use std::cell::RefCell;
use std::mem::transmute;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::{Arc, Mutex};

use hammer_core::config::Config;
use hammer_core::config::network::Interface;
use hammer_core::data_plane::{
    BufferFrame, BufferRef, DEFAULT_BUFFER_FRAME_CAPACITY, NodeId, NodeState,
};
use hammer_core::error::{CoreError, CoreResult, HammerError, HammerResult};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, File, FileFunctions, Node, NodeProcessFn, NodeResult,
    NodeRuntimeData, PacketTrace, TraceFormatter, add_packet_trace,
};

use hammer_infra::vec::Vec;
use hammer_service::device::{
    DeviceInputNext, DeviceInputNode, DeviceMain, DeviceRxQueue, DriverScheduleMode,
};
use hammer_service::interface::InterfaceControlPlane;
use hammer_service::opaque::NetworkOpaque;

// External `mod tun;` cannot receive ownership injection from `#[plugin]`.
hammer_component_macros::declare_plugin!(name = "tun", load_after = []);

/// TUN-owned instance list under `[plugin.tun]`.
///
/// Names reference `[[network.interface]]` entries (L3/FIB). This driver
/// opens fds/queues for those instances only.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
struct TunPluginConfig {
    #[serde(default, alias = "interface")]
    interfaces: Vec<String>,
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
    fd: Arc<OwnedFd>,
}

struct TunWorkerRuntime {
    worker: DataWorkerId,
    rx_poll_vector: Vec<DeviceRxQueue>,
    devices: Vec<TunWorkerDevice>,
}

struct TunWorkerDevice {
    interface_index: u32,
    fd: Arc<OwnedFd>,
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
        interface: &Interface,
        interface_index: u32,
        worker_count: usize,
    ) -> HammerResult<()> {
        let worker_count = u32::try_from(worker_count)
            .map_err(|_| HammerError::internal("worker count does not fit u32"))?;
        if worker_count == 0 {
            return Err(HammerError::internal(
                "at least one data worker is required for a TUN interface",
            ));
        }
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| HammerError::internal("TUN control devices poisoned"))?;
        let device_instance = u32::try_from(devices.len())
            .map_err(|_| HammerError::internal("TUN device instance overflow"))?;
        let owner = DataWorkerId::new(device_instance % worker_count);
        let (fd, kernel_name) = platform::open(&interface.name, interface.mtu.l3)?;
        tracing::info!(
            logical_name = %interface.name,
            %kernel_name,
            owner = owner.slot(),
            "opened TUN file"
        );
        self.device_main.register_rx_queue(
            device_instance,
            0,
            owner,
            DriverScheduleMode::Interrupt,
        )?;
        self.device_main
            .register_tx_queue(device_instance, 0, owner)?;
        devices.push(Some(TunControlDevice {
            interface_index,
            fd: Arc::new(fd),
        }));
        Ok(())
    }

    fn take_worker_runtime(&self, worker: DataWorkerId) -> CoreResult<TunWorkerRuntime> {
        let rx_poll_vector = self.device_main.rx_poll_vector(worker);

        let mut control_devices = self
            .devices
            .lock()
            .map_err(|_| CoreError::internal("TUN control devices poisoned"))?;
        let mut devices = Vec::with_capacity(rx_poll_vector.len());
        for queue in &rx_poll_vector {
            let device = control_devices
                .get_mut(queue.device_instance as usize)
                .and_then(Option::take)
                .ok_or_else(|| CoreError::internal("TUN RX queue already has an owner"))?;
            devices.push(TunWorkerDevice {
                interface_index: device.interface_index,
                fd: device.fd,
            });
        }
        Ok(TunWorkerRuntime {
            worker,
            rx_poll_vector,
            devices,
        })
    }
}

#[hammer_component_macros::config_function(name = "tun_config", plugin = "tun")]
fn configure_tun(
    config: Arc<Config>,
    device_main: Arc<DeviceMain>,
    interface_main: Arc<InterfaceControlPlane>,
) -> HammerResult<Arc<TunControl>> {
    let tun_cfg = config.plugin_config::<TunPluginConfig>("tun")?;
    let control = TunControl::new(device_main);
    for name in &tun_cfg.interfaces {
        let interface = config
            .network
            .interface
            .iter()
            .find(|interface| interface.name == *name)
            .ok_or_else(|| {
                HammerError::config_validation(format!(
                    "plugin.tun interface `{name}` is not declared in [[network.interface]]"
                ))
            })?;
        let interface_index = interface_main
            .handle()
            .interface_index(&interface.name)
            .ok_or_else(|| HammerError::internal("TUN interface is not registered"))?;
        control.add_interface(interface, interface_index, config.worker.count)?;
    }
    Ok(control)
}

#[hammer_component_macros::worker_init_function(name = "tun_worker_init", plugin = "tun")]
fn configure_tun_worker(
    engine: &mut hammer_runtime::Engine,
    control: Arc<TunControl>,
) -> HammerResult<()> {
    let worker = engine.data_worker_id()?;
    let runtime = control.take_worker_runtime(worker)?;
    let tun_input = engine
        .runtime
        .node_by_name(TunInputDriverNode::NODE_NAME)
        .ok_or_else(|| CoreError::internal("tun-input is not registered"))?;
    for device in &runtime.devices {
        engine.file_main_mut()?.add(File::new(
            Arc::clone(&device.fd),
            worker,
            format!("TUN interface {}", device.interface_index),
            u64::from(tun_input.slot()),
            FileFunctions {
                read: Some(schedule_tun_input),
                ..FileFunctions::default()
            },
        ))?;
    }
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

fn schedule_tun_input(file: &mut File) -> HammerResult<()> {
    let node = u32::try_from(file.private_data())
        .map(NodeId::new)
        .map_err(|_| HammerError::internal("TUN input node id overflow"))?;
    hammer_runtime::Engine::with_current(|engine| engine.runtime.schedule_empty_frame(node))
        .ok_or_else(|| HammerError::internal("TUN File callback has no current Engine"))??;
    Ok(())
}

#[hammer_component_macros::graph_node(
    graph = tun,
    name = "tun-input",
    kind = driver,
    state = disabled,
    sibling_of = DeviceInputNode,
    plugin = "tun",
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
        Some(format_tun_input_trace)
    }
}

#[hammer_component_macros::graph_node(
    graph = tun,
    name = "tun-output",
    kind = internal,
    plugin = "tun",
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
        Some(format_tun_output_trace)
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
                    match platform::try_recv(device.fd.as_raw_fd(), writable) {
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
            self.devices
                .iter_mut()
                .find(|device| device.interface_index == interface_index)
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
        let _ = platform::try_send(device.fd.as_raw_fd(), version, &segments);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunDriverMode {
    Tun,
    Tap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TunInputTrace {
    pub interface_index: Option<u32>,
    pub mode: TunDriverMode,
    pub received: usize,
}

impl TunInputTrace {
    pub const ENCODED_LEN: usize = 14;

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let interface_index = match bytes[0] {
            0 => None,
            1 => Some(u32::from_le_bytes(bytes[1..5].try_into().ok()?)),
            _ => return None,
        };
        let mode = decode_tun_driver_mode(bytes[5])?;
        let received = usize::try_from(u64::from_le_bytes(bytes[6..14].try_into().ok()?)).ok()?;
        Some(Self {
            interface_index,
            mode,
            received,
        })
    }
}

impl PacketTrace for TunInputTrace {
    fn encode_trace(&self, out: &mut Vec<u8>) {
        out.push(u8::from(self.interface_index.is_some()));
        out.extend_from_slice(&self.interface_index.unwrap_or_default().to_le_bytes());
        out.push(encode_tun_driver_mode(self.mode));
        out.extend_from_slice(&(self.received as u64).to_le_bytes());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TunOutputTrace {
    pub mode: TunDriverMode,
    pub pending: usize,
}

impl TunOutputTrace {
    pub const ENCODED_LEN: usize = 9;

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let mode = decode_tun_driver_mode(bytes[0])?;
        let pending = usize::try_from(u64::from_le_bytes(bytes[1..9].try_into().ok()?)).ok()?;
        Some(Self { mode, pending })
    }
}

impl PacketTrace for TunOutputTrace {
    fn encode_trace(&self, out: &mut Vec<u8>) {
        out.push(encode_tun_driver_mode(self.mode));
        out.extend_from_slice(&(self.pending as u64).to_le_bytes());
    }
}

fn format_tun_input_trace(bytes: &[u8]) -> String {
    TunInputTrace::decode(bytes)
        .map(|trace| format!("{trace:?}"))
        .unwrap_or_else(|| format!("TunInputTrace invalid={bytes:?}"))
}

fn format_tun_output_trace(bytes: &[u8]) -> String {
    TunOutputTrace::decode(bytes)
        .map(|trace| format!("{trace:?}"))
        .unwrap_or_else(|| format!("TunOutputTrace invalid={bytes:?}"))
}

#[inline]
fn encode_tun_driver_mode(mode: TunDriverMode) -> u8 {
    match mode {
        TunDriverMode::Tun => 0,
        TunDriverMode::Tap => 1,
    }
}

#[inline]
fn decode_tun_driver_mode(value: u8) -> Option<TunDriverMode> {
    match value {
        0 => Some(TunDriverMode::Tun),
        1 => Some(TunDriverMode::Tap),
        _ => None,
    }
}
