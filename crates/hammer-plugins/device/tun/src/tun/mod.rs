use std::cell::RefCell;
use std::io;
use std::mem::transmute;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex};

use hammer_core::data_plane::{BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, NodeId, NodeState};
use hammer_infra::spinlock::Spinlock;
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, File, FileFunctions, Node, NodeProcessFn, NodeResult,
    NodeRuntimeData, TraceFormatter, add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::device::{
    DeviceError, DeviceInputNext, DeviceInputNode, DeviceMain, DeviceRxQueue, DeviceTxQueue,
    DriverScheduleMode,
};
use hammer_service::interface::{InterfaceConfig, InterfaceControlPlane, configure_interfaces};
use hammer_service::opaque::NetworkOpaque;

#[derive(Debug, thiserror::Error)]
enum TunError {
    #[error("{operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("invalid TUN interface name")]
    InvalidInterfaceName,
    #[cfg(target_os = "linux")]
    #[error("TUN interface name is not terminated")]
    InterfaceNameNotTerminated,
    #[cfg(target_os = "linux")]
    #[error("TUN interface name is empty")]
    InterfaceNameEmpty,
    #[error("TUN interface name is not UTF-8")]
    InterfaceNameNotUtf8,
    #[cfg(target_os = "macos")]
    #[error("TUN interface name length is invalid")]
    InterfaceNameLengthInvalid,
    #[error("TUN MTU is out of range")]
    MtuOutOfRange,
    #[error("TUN packet length is out of range")]
    PacketLengthOutOfRange,
    #[error("TUN packet has no L3 payload")]
    EmptyPacket,
    #[cfg(target_os = "macos")]
    #[error("TUN packet has unsupported address family {family}")]
    UnsupportedAddressFamily { family: u32 },
    #[error("TUN packet has unsupported L3 version {version}")]
    UnsupportedIpVersion { version: u8 },
    #[cfg(target_os = "macos")]
    #[error("TUN address family {family} does not match L3 version {version}")]
    AddressFamilyMismatch { family: u32, version: u8 },
    #[error("partial TUN packet write: wrote {actual} of {expected} bytes")]
    PartialWrite { expected: usize, actual: usize },
    #[error("configured worker count does not fit u32")]
    WorkerCountOutOfRange,
    #[error("a TUN interface requires at least one data worker")]
    NoDataWorkers,
    #[error("TUN control device registry is poisoned")]
    ControlRegistryPoisoned,
    #[error(transparent)]
    Device(#[from] DeviceError),
    #[error("TUN File for device {device_instance} is unavailable on worker {worker}")]
    WorkerFileUnavailable { device_instance: u32, worker: u32 },
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("required graph node `{name}` is not registered")]
    GraphNodeMissing { name: &'static str },
    #[error("configured TUN interface `{name}` is not registered")]
    InterfaceNotRegistered { name: String },
    #[error("TUN interface {interface_index} has no MTU")]
    InterfaceMtuMissing { interface_index: u32 },
    #[error("interface {interface_index} has no TUN TX queue on this worker")]
    TxQueueUnavailable { interface_index: u32 },
    #[error("TUN transmit packet is empty")]
    EmptyTxPacket,
    #[error("TUN transmit would block")]
    WouldBlock,
}

impl TunError {
    const fn code(&self) -> u16 {
        match self {
            Self::Runtime(_) => 1,
            Self::TxQueueUnavailable { .. } => 2,
            Self::WouldBlock => 3,
            Self::EmptyPacket
            | Self::EmptyTxPacket
            | Self::UnsupportedIpVersion { .. }
            | Self::PartialWrite { .. } => 4,
            #[cfg(target_os = "macos")]
            Self::UnsupportedAddressFamily { .. } | Self::AddressFamilyMismatch { .. } => 4,
            Self::Io { .. } => 5,
            _ => 6,
        }
    }
}

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
    devices: Mutex<Vec<TunControlDevice>>,
}

struct TunControlDevice {
    device_instance: u32,
    interface_index: u32,
    requested_name: String,
    kernel_name: String,
    worker_files: Vec<Option<OwnedFd>>,
    tx_lock: Arc<Spinlock<()>>,
}

struct TunWorkerRuntime {
    input_node: NodeId,
    rx_queues: Vec<TunRxQueue>,
    tx_queues: Vec<TunTxQueue>,
}

struct TunRxQueue {
    queue: DeviceRxQueue,
    interface_index: u32,
    file_index: hammer_infra::pool::Index,
    pending: bool,
}

struct TunTxQueue {
    queue: DeviceTxQueue,
    file_index: hammer_infra::pool::Index,
    tx_lock: Arc<Spinlock<()>>,
    tx_iovecs: Vec<libc::iovec>,
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
    ) -> Result<(), TunError> {
        let worker_count =
            u32::try_from(worker_count).map_err(|_| TunError::WorkerCountOutOfRange)?;
        if worker_count == 0 {
            return Err(TunError::NoDataWorkers);
        }
        let (fd, kernel_name) = platform::open(interface_name, mtu)?;
        let mut worker_files = Vec::with_capacity(worker_count as usize);
        for _ in 1..worker_count {
            let duplicate = fd.try_clone().map_err(|source| TunError::Io {
                operation: "duplicate TUN descriptor for data worker",
                source,
            })?;
            worker_files.push(Some(duplicate));
        }
        worker_files.insert(0, Some(fd));
        tracing::info!(
            logical_name = %interface_name,
            %kernel_name,
            "opened TUN interface"
        );
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| TunError::ControlRegistryPoisoned)?;
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
        self.device_main
            .register_tx_queue(device_instance, 0, owner)?;
        for worker in 0..worker_count {
            self.device_main.assign_tx_queue_to_worker(
                device_instance,
                0,
                DataWorkerId::new(worker),
            )?;
        }
        devices.push(TunControlDevice {
            device_instance,
            interface_index,
            requested_name: interface_name.to_owned(),
            kernel_name,
            worker_files,
            tx_lock: Arc::new(Spinlock::new(())),
        });
        Ok(())
    }

    fn take_worker_runtime(
        &self,
        engine: &mut hammer_runtime::Engine,
        worker: DataWorkerId,
        tun_input: NodeId,
    ) -> Result<TunWorkerRuntime, TunError> {
        let rx_queues = self.device_main.rx_poll_vector(worker);
        let assigned_tx_queues = self.device_main.tx_queues_for_worker(worker);

        let mut control_devices = self
            .devices
            .lock()
            .map_err(|_| TunError::ControlRegistryPoisoned)?;
        let mut worker_rx_queues = Vec::with_capacity(rx_queues.len());
        let mut worker_tx_queues = Vec::with_capacity(assigned_tx_queues.len());
        for device in control_devices.iter_mut() {
            let rx_queue = rx_queues
                .iter()
                .find(|queue| queue.device_instance == device.device_instance)
                .copied();
            let has_tx_queue = assigned_tx_queues
                .iter()
                .any(|queue| queue.device_instance == device.device_instance);
            if rx_queue.is_none() && !has_tx_queue {
                continue;
            }
            let fd = device
                .worker_files
                .get_mut(worker.slot())
                .and_then(Option::take)
                .ok_or(TunError::WorkerFileUnavailable {
                    device_instance: device.device_instance,
                    worker: worker.slot() as u32,
                })?;
            let queue_index = worker_rx_queues.len();
            let file_index = engine.file_main_mut().add(File::new(
                fd,
                format!(
                    "TUN interface {} (logical {}, kernel {})",
                    device.interface_index, device.requested_name, device.kernel_name
                ),
                if rx_queue.is_some() {
                    queue_index as u64
                } else {
                    0
                },
                FileFunctions {
                    read: if rx_queue.is_some() {
                        Some(schedule_tun_input)
                    } else {
                        None
                    },
                    ..FileFunctions::default()
                },
            ))?;
            if let Some(rx_queue) = rx_queue {
                worker_rx_queues.push(TunRxQueue {
                    queue: rx_queue,
                    interface_index: device.interface_index,
                    file_index,
                    pending: false,
                });
            }
            for queue in assigned_tx_queues
                .iter()
                .filter(|queue| queue.device_instance == device.device_instance)
            {
                worker_tx_queues.push(TunTxQueue {
                    queue: queue.clone(),
                    file_index,
                    tx_lock: Arc::clone(&device.tx_lock),
                    tx_iovecs: Vec::new(),
                });
            }
        }
        Ok(TunWorkerRuntime {
            input_node: tun_input,
            rx_queues: worker_rx_queues,
            tx_queues: worker_tx_queues,
        })
    }
}

#[hammer_component_macros::config_function(
    name = "tun_interfaces_config",
    section = "plugin.tun",
    early = true,
    runs_before = ["ip_config"]
)]
fn configure_tun_interfaces(tun_cfg: TunPluginConfig) -> RuntimeResult<Arc<InterfaceControlPlane>> {
    configure_interfaces(&tun_cfg.interfaces)
}

#[hammer_component_macros::config_function(name = "tun_config", section = "plugin.tun")]
fn configure_tun(
    tun_cfg: TunPluginConfig,
    engine: &mut hammer_runtime::Engine,
    device_main: Arc<DeviceMain>,
    interface_main: Arc<InterfaceControlPlane>,
) -> RuntimeResult<Arc<TunControl>> {
    (|| -> Result<Arc<TunControl>, TunError> {
        let control = TunControl::new(device_main);
        let tun_input = engine
            .runtime
            .node_by_name(TunInputDriverNode::NODE_NAME)
            .ok_or(TunError::GraphNodeMissing {
                name: TunInputDriverNode::NODE_NAME,
            })?;
        let tun_output = engine
            .runtime
            .node_by_name(TunOutputDriverNode::NODE_NAME)
            .ok_or(TunError::GraphNodeMissing {
                name: TunOutputDriverNode::NODE_NAME,
            })?;
        for interface in &tun_cfg.interfaces {
            let interface_index = interface_main
                .handle()
                .interface_index(&interface.name)
                .ok_or_else(|| TunError::InterfaceNotRegistered {
                    name: interface.name.clone(),
                })?;
            let mtu = interface_main
                .handle()
                .interface_mtu(interface_index)
                .ok_or(TunError::InterfaceMtuMissing { interface_index })?
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
    })()
    .map_err(|source| RuntimeError::subsystem("tun", source))
}

#[hammer_component_macros::worker_init_function(name = "tun_worker_init")]
fn configure_tun_worker(
    engine: &mut hammer_runtime::Engine,
    control: Arc<TunControl>,
) -> RuntimeResult<()> {
    (|| -> Result<(), TunError> {
        let worker = engine.data_worker_id()?;
        let tun_input = engine
            .runtime
            .node_by_name(TunInputDriverNode::NODE_NAME)
            .ok_or(TunError::GraphNodeMissing {
                name: TunInputDriverNode::NODE_NAME,
            })?;
        control.device_main.install_worker_output_runtime(worker);
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
    })()
    .map_err(|source| RuntimeError::subsystem("tun", source))
}

fn schedule_tun_input(graph: &hammer_runtime::NodeRuntime, file: &mut File) -> RuntimeResult<()> {
    let queue_index = usize::try_from(file.private_data()).expect("RX queue index fits usize");
    TUN_WORKER_RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        let runtime = runtime
            .as_mut()
            .expect("registered TUN File requires worker runtime");
        runtime
            .rx_queues
            .get_mut(queue_index)
            .expect("TUN File references a live RX queue")
            .pending = true;
        graph.mark_interrupt_pending(runtime.input_node).map(|_| ())
    })
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
        let mut worker = worker.borrow_mut();
        let worker = worker
            .as_mut()
            .expect("enabled TUN input node requires worker runtime");
        worker.process_input(runtime, frame)
    })
}

fn tun_output_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    TUN_WORKER_RUNTIME.with(|worker| {
        let mut worker = worker.borrow_mut();
        let worker = worker
            .as_mut()
            .expect("TUN output node requires worker runtime");
        worker.process_output(runtime, frame)
    })
}

impl TunWorkerRuntime {
    #[inline]
    fn has_rx_queues(&self) -> bool {
        !self.rx_queues.is_empty()
    }

    fn process_input(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        if let Err(error) = self.receive_packets(runtime, frame) {
            let code = error.code();
            if let Err(source) = runtime.record_current_node_error(code) {
                tracing::error!(%source, code, "failed to record TUN input error");
            }
            tracing::error!(%error, "TUN receive failed");
        }
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
            let _ = add_packet_trace!(runtime, *index, TunOutputTrace { pending },);
            if let Err(error) = self.send_packet(runtime, *index) {
                let code = error.code();
                if let Err(source) = runtime.record_current_node_error(code) {
                    tracing::error!(%source, code, "failed to record TUN output error");
                }
                tracing::error!(%error, ?index, "TUN transmit failed");
            }
        }
        NodeResult::drop()
    }

    fn receive_packets(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> Result<(), TunError> {
        let mut refill_pending = false;
        for queue in &mut self.rx_queues {
            if queue.queue.mode == DriverScheduleMode::Interrupt && !queue.pending {
                continue;
            }
            queue.pending = false;
            let mut drained = false;
            while frame.remaining_capacity() > 0 {
                let index = runtime.alloc_index()?;
                let received = (|| {
                    let mut buffer = runtime.get_buffer_mut(index)?;
                    let writable = buffer.writable_tail_mut();
                    let files = runtime.file_main();

                    #[cfg(target_os = "linux")]
                    let received = {
                        let mut vectors = [libc::iovec {
                            iov_base: writable.as_mut_ptr().cast(),
                            iov_len: writable.len(),
                        }];
                        // SAFETY: the iovec references the writable buffer tail
                        // for this synchronous FileMain call.
                        let received = unsafe { files.readv(queue.file_index, &mut vectors)? };
                        if let Some(length) = received {
                            if length == 0 {
                                return Err(TunError::EmptyPacket);
                            }
                            let version = writable[0] >> 4;
                            if version != 4 && version != 6 {
                                return Err(TunError::UnsupportedIpVersion { version });
                            }
                        }
                        received
                    };

                    #[cfg(target_os = "macos")]
                    let received = {
                        let mut family = [0u8; platform::UTUN_HEADER_LEN];
                        let mut vectors = [
                            libc::iovec {
                                iov_base: family.as_mut_ptr().cast(),
                                iov_len: family.len(),
                            },
                            libc::iovec {
                                iov_base: writable.as_mut_ptr().cast(),
                                iov_len: writable.len(),
                            },
                        ];
                        // SAFETY: both iovecs reference live writable arrays for
                        // this synchronous FileMain call.
                        let received = unsafe { files.readv(queue.file_index, &mut vectors)? };
                        match received {
                            None => None,
                            Some(length) if length <= platform::UTUN_HEADER_LEN => {
                                return Err(TunError::EmptyPacket);
                            }
                            Some(length) => {
                                let family = u32::from_be_bytes(family);
                                if family != libc::AF_INET as u32 && family != libc::AF_INET6 as u32
                                {
                                    return Err(TunError::UnsupportedAddressFamily { family });
                                }
                                let version = writable[0] >> 4;
                                let expected = if family == libc::AF_INET as u32 { 4 } else { 6 };
                                if version != expected {
                                    return Err(TunError::AddressFamilyMismatch {
                                        family,
                                        version,
                                    });
                                }
                                Some(length - platform::UTUN_HEADER_LEN)
                            }
                        }
                    };
                    match received {
                        Some(length) => {
                            buffer
                                .commit_writable_tail(length)
                                .map_err(RuntimeError::from)?;
                            // SAFETY: NetworkOpaque is the network subsystem view of the
                            // fixed-size primary opaque region and fits that region by its
                            // own compile-time layout assertion.
                            let network =
                                unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                            network.sw_if_index = [queue.interface_index, u32::MAX];
                            Ok(true)
                        }
                        None => Ok(false),
                    }
                })();
                let received = match received {
                    Ok(received) => received,
                    Err(error) => {
                        runtime.buffers().drop_index_owned_with_trace(index, |_| {});
                        return Err(error);
                    }
                };
                if !received {
                    runtime.buffers().drop_index_owned_with_trace(index, |_| {});
                    drained = true;
                    break;
                }
                if let Err(source) = frame.push_index(index) {
                    runtime.buffers().drop_index_owned_with_trace(index, |_| {});
                    return Err(RuntimeError::from(source).into());
                }
                if let Some(node) = runtime.current_node() {
                    let _ = runtime.try_mark_trace(node, index);
                }
                let _ = add_packet_trace!(
                    runtime,
                    index,
                    TunInputTrace {
                        interface_index: Some(queue.interface_index),
                        received: 1,
                    },
                );
            }
            if queue.queue.mode == DriverScheduleMode::Interrupt && !drained {
                queue.pending = true;
                refill_pending = true;
            }
        }
        if refill_pending {
            // The readiness edge is consumed: io_uring posts no new completion
            // until the peer transmits again, so a frame-limited pass must
            // schedule another drain. VPP af-packet similarly retains
            // `is_rx_pending` and keeps its input node polling when a frame
            // cannot consume the whole ready block.
            runtime.set_node_interrupt_pending(self.input_node)?;
        }
        Ok(())
    }

    fn send_packet(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: hammer_core::data_plane::Index,
    ) -> Result<(), TunError> {
        let interface_index = runtime.get_buffer(index).map(|buffer| {
            // SAFETY: NetworkOpaque is the established network view over the
            // fixed-size primary opaque region.
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
            network.sw_if_index[1]
        })?;
        let queue = self
            .tx_queues
            .iter_mut()
            .find(|queue| queue.queue.interface_index == interface_index)
            .ok_or(TunError::TxQueueUnavailable { interface_index })?;
        queue.tx_iovecs.clear();
        #[cfg(target_os = "macos")]
        queue.tx_iovecs.push(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        });
        let mut version = None;
        let mut chain = runtime.chain(index);
        for buffer in chain.by_ref() {
            let buffer = buffer.map_err(RuntimeError::from)?;
            if version.is_none() {
                version = buffer.current().first().map(|first| first >> 4);
            }
            queue.tx_iovecs.push(libc::iovec {
                iov_base: buffer.current().as_ptr().cast_mut().cast(),
                iov_len: buffer.current().len(),
            });
        }
        drop(chain);
        let version = version.ok_or(TunError::EmptyTxPacket)?;

        #[cfg(target_os = "linux")]
        if version != 4 && version != 6 {
            return Err(TunError::UnsupportedIpVersion { version });
        }

        #[cfg(target_os = "macos")]
        let family_header = match version {
            4 => (libc::AF_INET as u32).to_be_bytes(),
            6 => (libc::AF_INET6 as u32).to_be_bytes(),
            _ => return Err(TunError::UnsupportedIpVersion { version }),
        };
        #[cfg(target_os = "macos")]
        {
            let header = queue
                .tx_iovecs
                .first_mut()
                .expect("Darwin TUN TX queue reserves its family header iovec");
            *header = libc::iovec {
                iov_base: family_header.as_ptr().cast_mut().cast(),
                iov_len: family_header.len(),
            };
        }

        let expected = queue
            .tx_iovecs
            .iter()
            .try_fold(0usize, |length, vector| length.checked_add(vector.iov_len))
            .ok_or(TunError::PacketLengthOutOfRange)?;
        let files = runtime.file_main();
        let tx_guard = queue.queue.is_shared().then(|| queue.tx_lock.lock());
        // SAFETY: every payload iovec points into a live data-plane buffer
        // chain, the optional Darwin header remains live, and FileMain writes
        // synchronously before this function can release or mutate either.
        let written = unsafe { files.writev(queue.file_index, &queue.tx_iovecs)? };
        drop(tx_guard);
        match written {
            Some(written) if written == expected => Ok(()),
            Some(written) => Err(TunError::PartialWrite {
                expected,
                actual: written,
            }),
            None => Err(TunError::WouldBlock),
        }
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
pub struct TunInputTrace {
    pub interface_index: Option<u32>,
    pub received: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TunOutputTrace {
    pub pending: usize,
}
