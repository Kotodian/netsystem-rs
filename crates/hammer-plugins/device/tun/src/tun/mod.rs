use std::cell::RefCell;
use std::mem::transmute;
use std::sync::{Arc, Mutex};

use hammer_core::data_plane::{
    BufferFrame, BufferRef, DEFAULT_BUFFER_FRAME_CAPACITY, NodeId, NodeState,
};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, File, FileFunctions, Node, NodeProcessFn, NodeResult,
    NodeRuntimeData, TraceFormatter, add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::device::{
    DeviceInputNext, DeviceInputNode, DeviceMain, DeviceRxQueue, DeviceTxQueue, DriverScheduleMode,
};
use hammer_service::interface::{InterfaceConfig, configure_interfaces};
use hammer_service::opaque::NetworkOpaque;

/// TUN-owned configuration under `[plugin.tun]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
struct TunPluginConfig {
    #[serde(default)]
    interfaces: Vec<InterfaceConfig>,
}
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "macos")]
use darwin as platform;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;

thread_local! {
    static TUN_WORKER_RUNTIME: RefCell<Option<TunWorkerRuntime>> = const { RefCell::new(None) };
}

struct TunControl {
    device_main: Arc<DeviceMain>,
    devices: Mutex<Vec<Option<TunControlDevice>>>,
}

struct TunControlDevice {
    interface_index: u32,
    requested_name: String,
    mtu: u32,
}

struct TunWorkerRuntime {
    worker: DataWorkerId,
    rx_poll_vector: Vec<DeviceRxQueue>,
    tx_queues: Vec<DeviceTxQueue>,
    devices: Vec<TunWorkerDevice>,
}

struct TunWorkerDevice {
    device_instance: u32,
    interface_index: u32,
    file_index: hammer_infra::pool::Index,
}

impl TunControl {
    fn new(device_main: Arc<DeviceMain>) -> Arc<Self> {
        Arc::new(Self {
            device_main,
            devices: Mutex::new(Vec::new()),
        })
    }

    fn add_interface(
        &self,
        interface_name: &str,
        mtu: u32,
        interface_index: u32,
        worker_count: usize,
        input_node: NodeId,
        output_node: NodeId,
    ) -> RuntimeResult<()> {
        let worker_count = u32::try_from(worker_count)
            .map_err(|_| RuntimeError::invariant("worker count does not fit u32"))?;
        if worker_count == 0 {
            return Err(RuntimeError::invariant(
                "at least one data worker is required for a TUN interface",
            ));
        }
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| RuntimeError::invariant("TUN control devices poisoned"))?;
        let device = self
            .device_main
            .register_device(interface_index, input_node, output_node)?;
        let device_instance = device.instance;
        let owner = DataWorkerId::new(device_instance % worker_count);
        self.device_main.register_rx_queue(
            device_instance,
            0,
            owner,
            DriverScheduleMode::Interrupt,
        )?;
        self.device_main.register_tx_queue(
            interface_index,
            device_instance,
            0,
            owner,
            output_node,
        )?;
        devices.push(Some(TunControlDevice {
            interface_index,
            requested_name: interface_name.to_owned(),
            mtu,
        }));
        Ok(())
    }

    fn take_worker_runtime(
        &self,
        engine: &mut hammer_runtime::Engine,
        worker: DataWorkerId,
        tun_input: NodeId,
    ) -> RuntimeResult<TunWorkerRuntime> {
        let rx_poll_vector = self.device_main.rx_poll_vector(worker);
        let tx_queues = self.device_main.tx_queues_for_worker(worker);

        let mut control_devices = self
            .devices
            .lock()
            .map_err(|_| RuntimeError::invariant("TUN control devices poisoned"))?;
        let mut devices = Vec::with_capacity(rx_poll_vector.len());
        for queue in &rx_poll_vector {
            let device = control_devices
                .get_mut(queue.device_instance as usize)
                .and_then(Option::take)
                .ok_or_else(|| RuntimeError::invariant("TUN RX queue already has an owner"))?;
            let (fd, kernel_name) = platform::open(&device.requested_name, device.mtu)?;
            tracing::info!(
                logical_name = %device.requested_name,
                %kernel_name,
                owner = worker.slot(),
                "opened TUN file"
            );
            let file_index = engine.file_main_mut()?.add(File::new(
                fd,
                worker,
                format!("TUN interface {}", device.interface_index),
                u64::from(tun_input.slot()),
                FileFunctions {
                    read: Some(schedule_tun_input),
                    ..FileFunctions::default()
                },
            ))?;
            devices.push(TunWorkerDevice {
                device_instance: queue.device_instance,
                interface_index: device.interface_index,
                file_index,
            });
        }
        for queue in &tx_queues {
            if !devices
                .iter()
                .any(|device| device.device_instance == queue.device_instance)
            {
                return Err(RuntimeError::invariant(
                    "TUN TX queue has no same-worker RX-owned file",
                ));
            }
        }
        Ok(TunWorkerRuntime {
            worker,
            rx_poll_vector,
            tx_queues,
            devices,
        })
    }
}

#[hammer_component_macros::config_function(name = "tun_config", section = "plugin.tun")]
fn configure_tun(
    tun_cfg: TunPluginConfig,
    engine: &mut hammer_runtime::Engine,
    device_main: Arc<DeviceMain>,
) -> RuntimeResult<Arc<TunControl>> {
    let interface_main = configure_interfaces(&tun_cfg.interfaces)?;
    engine.registry.set(Arc::clone(&interface_main));
    let control = TunControl::new(device_main);
    let tun_input = engine
        .runtime
        .node_by_name(TunInputDriverNode::NODE_NAME)
        .ok_or_else(|| RuntimeError::invariant("tun-input is not registered"))?;
    let tun_output = engine
        .runtime
        .node_by_name(TunOutputDriverNode::NODE_NAME)
        .ok_or_else(|| RuntimeError::invariant("tun-output is not registered"))?;
    for interface in &tun_cfg.interfaces {
        let interface_index = interface_main
            .handle()
            .interface_index(&interface.name)
            .ok_or_else(|| {
                RuntimeError::config_validation(format!(
                    "plugin.tun interface `{}` was not registered",
                    interface.name
                ))
            })?;
        let mtu = interface_main
            .handle()
            .interface_mtu(interface_index)
            .ok_or_else(|| RuntimeError::invariant("TUN interface has no MTU"))?
            .l3();
        control.add_interface(
            &interface.name,
            mtu,
            interface_index,
            engine.configured_worker_count(),
            tun_input,
            tun_output,
        )?;
    }
    Ok(control)
}

#[hammer_component_macros::worker_init_function(name = "tun_worker_init")]
fn configure_tun_worker(
    engine: &mut hammer_runtime::Engine,
    control: Arc<TunControl>,
) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let tun_input = engine
        .runtime
        .node_by_name(TunInputDriverNode::NODE_NAME)
        .ok_or_else(|| RuntimeError::invariant("tun-input is not registered"))?;
    control.device_main.install_worker_output_runtime(engine)?;
    let runtime = control.take_worker_runtime(engine, worker, tun_input)?;
    engine.runtime.nodes().set_node_state(
        tun_input,
        if runtime.has_rx_queues() {
            NodeState::Interrupt
        } else {
            NodeState::Disabled
        },
    )?;
    TUN_WORKER_RUNTIME.with(|slot| {
        slot.replace(Some(runtime));
    });
    Ok(())
}

fn schedule_tun_input(file: &mut File) -> RuntimeResult<()> {
    let node = u32::try_from(file.private_data())
        .map(NodeId::new)
        .map_err(|_| RuntimeError::invariant("TUN input node id overflow"))?;
    hammer_runtime::Engine::with_current(|engine| engine.runtime.schedule_empty_frame(node))
        .ok_or_else(|| RuntimeError::invariant("TUN File callback has no current Engine"))??;
    Ok(())
}

#[hammer_component_macros::graph_node(
    graph = tun,
    name = "tun-input",
    kind = driver,
    state = disabled,
    sibling_of = DeviceInputNode,
)]
#[derive(Debug, Clone, Copy)]
pub struct TunInputDriverNode;

impl Node for TunInputDriverNode {
    #[inline(always)]
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tun_input_process
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(TunInputTrace))
    }
}

#[hammer_component_macros::graph_node(
    graph = tun,
    name = "tun-output",
    kind = internal,
)]
#[derive(Debug, Clone, Copy)]
pub struct TunOutputDriverNode;

impl Node for TunOutputDriverNode {
    #[inline(always)]
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tun_output_process
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(TunOutputTrace))
    }
}

fn tun_input_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    TUN_WORKER_RUNTIME.with(|worker| {
        let Ok(mut worker) = worker.try_borrow_mut() else {
            return NodeResult::drop();
        };
        let Some(worker) = worker.as_mut() else {
            return NodeResult::drop();
        };
        worker.process_input(runtime, frame)
    })
}

fn tun_output_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    TUN_WORKER_RUNTIME.with(|worker| {
        let Ok(mut worker) = worker.try_borrow_mut() else {
            return NodeResult::drop();
        };
        let Some(worker) = worker.as_mut() else {
            return NodeResult::drop();
        };
        worker.process_output(runtime, frame)
    })
}

impl TunWorkerRuntime {
    #[inline]
    fn has_rx_queues(&self) -> bool {
        !self.rx_poll_vector.is_empty()
    }

    fn process_input(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        self.receive_packets(runtime, frame);
        fanout_tun_input(runtime, frame);
        NodeResult::drop()
    }

    fn process_output(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let pending = frame.pending_len();
        for index in frame.pending_indices() {
            let _ = add_packet_trace!(
                runtime,
                *index,
                TunOutputTrace {
                    mode: TunDriverMode::Tun,
                    pending,
                },
            );
            self.send_packet(runtime, *index);
        }
        NodeResult::drop()
    }

    fn receive_packets(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) {
        for (queue, device) in self.rx_poll_vector.iter().zip(&mut self.devices) {
            debug_assert_eq!(queue.owner, self.worker);
            debug_assert_eq!(queue.queue_id, 0);
            let Some(fd) = hammer_runtime::Engine::with_current(|engine| {
                engine
                    .file_main()
                    .ok()
                    .and_then(|files| files.get(device.file_index))
                    .map(File::fd)
            })
            .flatten() else {
                return;
            };
            while frame.remaining_capacity() > 0 {
                let Ok(index) = runtime.alloc_index() else {
                    return;
                };
                let Ok(mut owner) = runtime.buffers().get_next_frame(NodeId::new(0)) else {
                    return;
                };
                if owner.push_index(index).is_err() {
                    return;
                }
                let received = {
                    let Ok(mut buffer) = runtime.get_buffer_mut(index) else {
                        return;
                    };
                    let writable = buffer.writable_tail_mut();
                    match platform::try_recv(fd, writable) {
                        Ok(Some(length)) => {
                            if buffer.commit_writable_tail(length).is_err() {
                                return;
                            }
                            // SAFETY: NetworkOpaque is the network subsystem view of the
                            // fixed-size primary opaque region and fits that region by its
                            // own compile-time layout assertion.
                            let network =
                                unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                            network.sw_if_index[0] = device.interface_index;
                            true
                        }
                        Ok(None) => false,
                        Err(_) => return,
                    }
                };
                if !received {
                    break;
                }
                if frame.push_index(index).is_err() {
                    return;
                }
                let _ = owner.retain_indices(|candidate| Ok(candidate != index));
                let _ =
                    runtime.try_mark_trace(runtime.current_node().unwrap_or(NodeId::new(0)), index);
                let _ = add_packet_trace!(
                    runtime,
                    index,
                    TunInputTrace {
                        interface_index: Some(device.interface_index),
                        mode: TunDriverMode::Tun,
                        received: 1,
                    },
                );
            }
        }
    }

    fn send_packet(&mut self, runtime: &DataPlaneRuntime, index: hammer_core::data_plane::Index) {
        let interface_index = runtime.get_buffer(index).ok().map(|buffer| {
            // SAFETY: NetworkOpaque is the established network view over the
            // fixed-size primary opaque region.
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
            network.sw_if_index[1]
        });
        let Some(device) = interface_index.and_then(|interface_index| {
            let queue = self
                .tx_queues
                .iter()
                .find(|queue| queue.interface_index == interface_index)?;
            self.devices
                .iter_mut()
                .find(|device| device.device_instance == queue.device_instance)
        }) else {
            return;
        };
        let mut buffers: Vec<BufferRef<'_>> = Vec::new();
        let mut chain = runtime.chain(index);
        while let Some(buffer) = chain.next() {
            let Ok(buffer) = buffer else {
                return;
            };
            buffers.push(buffer);
        }
        drop(chain);
        let Some(version) = buffers
            .first()
            .and_then(|buffer| buffer.current().first())
            .map(|first| first >> 4)
        else {
            return;
        };
        let mut segments: Vec<&[u8]> = Vec::with_capacity(buffers.len());
        for buffer in &buffers {
            segments.push(buffer.current());
        }
        let Some(fd) = hammer_runtime::Engine::with_current(|engine| {
            engine
                .file_main()
                .ok()
                .and_then(|files| files.get(device.file_index))
                .map(File::fd)
        })
        .flatten() else {
            return;
        };
        let _ = platform::try_send(fd, version, &segments);
    }
}

fn fanout_tun_input(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) {
    let count = frame.pending_len();
    if count == 0 {
        return;
    }
    debug_assert!(count <= DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut nexts = [DeviceInputNext::Drop; DEFAULT_BUFFER_FRAME_CAPACITY];
    for (next, index) in nexts.iter_mut().zip(frame.pending_indices()) {
        *next = match runtime
            .get_buffer(*index)
            .ok()
            .and_then(|buffer| buffer.current().first().copied())
            .map(|first| first >> 4)
        {
            Some(4) => DeviceInputNext::Ip4Input,
            Some(6) => DeviceInputNext::Ip6Input,
            _ => DeviceInputNext::Drop,
        };
    }
    runtime.enqueue_to_next(frame, &nexts[..count]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TunDriverMode {
    Tun,
    Tap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TunInputTrace {
    pub interface_index: Option<u32>,
    pub mode: TunDriverMode,
    pub received: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TunOutputTrace {
    pub mode: TunDriverMode,
    pub pending: usize,
}
