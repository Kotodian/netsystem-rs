mod host;
mod vhost;

use std::cell::{RefCell, UnsafeCell};
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::OnceLock;

use hammer_core::buffer::{Buffer, DEFAULT_BUFFER_FRAME_CAPACITY};
use hammer_core::data_plane::{Frame, NodeId, NodeState};
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_plugin_ip::{Ip4InputNode, Ip6InputNode, IpInterfaceAddressError};
use hammer_runtime::file::{FILE_MAIN, File, FileFunctions};
use hammer_runtime::node::{NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity};
use hammer_runtime::{DataPlaneMain, DataWorkerId, Node, NodeRuntime, RuntimeError, RuntimeResult};
use hammer_service::data_plane::DropNode;
use hammer_service::feature::FeatureMain;
use hammer_service::interface::{
    HwClassFlags, HwInterfaceFlags, InterfaceMtu, SwInterfaceFlags, TxFrame,
};
use hammer_service::interface_model::DriverScheduleMode;
use hammer_service::net::NetMain;
use hammer_service::opaque::{NetworkFlags, NetworkOffloadFlags, NetworkOpaque};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use wide::{CmpEq, u16x8};

use vhost::{Descriptor, Virtqueue};

hammer_service::declare_interface_registration_image!();

#[derive(hammer_component_macros::DeviceClass)]
#[device_class(
    name = "tuntap",
    format_device_name = format_tun_interface_name,
    tx_function = tun_intfc_tx
)]
struct TunDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(name = "tun", flags = HwClassFlags::P2P, tx_hash = tun_ip_flow_hash)]
struct TunHwClass;

// VPP vnet/hash/crc32_5tuple.c:compute_ip4_key/compute_ip6_key. The
// callback sees the packet in its Buffer; no payload is materialized.
#[inline(always)]
fn tun_ip_flow_hash(buffer: &Buffer) -> u32 {
    let packet = buffer.current();
    let (address, transport, protocol) = match packet.first().map(|byte| byte >> 4) {
        Some(4) if packet.len() >= 20 => {
            let header_len = usize::from(packet[0] & 0x0f) * 4;
            if header_len < 20 {
                return 0;
            }
            (&packet[12..20], header_len, packet[9])
        }
        Some(6) if packet.len() >= 40 => (&packet[8..40], 40, packet[6]),
        _ => return 0,
    };
    let Some(ports) = packet.get(transport..transport + 4) else {
        return 0;
    };
    let mask = match protocol {
        1 | 58 => 0x0000_ffff,
        2 => 0x0000_00ff,
        6 | 17 | 50 | 51 => u32::MAX,
        _ => 0,
    };
    // VPP crc32_5tuple.c reads the four L4 key bytes directly from the packet.
    let l4 = unsafe { std::ptr::read_unaligned(ports.as_ptr().cast::<u32>()) } & mask;
    let key = (u64::from(protocol) << 32) | u64::from(l4);
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("sse4.2") {
        return unsafe { tun_crc32c_ip_tuple(address, key) };
    }
    let crc = tun_crc32c_bytes(0, address);
    tun_crc32c_bytes(crc, &key.to_ne_bytes())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn tun_crc32c_ip_tuple(address: &[u8], key: u64) -> u32 {
    let mut crc = 0u64;
    for bytes in address.chunks_exact(8) {
        let word = unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<u64>()) };
        crc = unsafe { std::arch::x86_64::_mm_crc32_u64(crc, word) };
    }
    unsafe { std::arch::x86_64::_mm_crc32_u64(crc, key) as u32 }
}

#[inline(always)]
fn tun_crc32c_bytes(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    crc
}

fn format_tun_interface_name(instance: u32, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(formatter, "tun-{instance}")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TunConfig {
    enabled: bool,
    name: String,
    mtu: u32,
    admin_up: bool,
    ip4_address: Option<Ipv4Net>,
    host_ip4_address: Option<Ipv4Net>,
    host_ip6_address: Option<Ipv6Net>,
    num_rx_queues: u16,
    num_tx_queues: Option<u16>,
    rx_ring_size: u16,
    tx_ring_size: u16,
    gso: bool,
    csum_offload: bool,
}

impl Default for TunConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "hammer0".to_owned(),
            mtu: 1500,
            admin_up: false,
            ip4_address: None,
            host_ip4_address: None,
            host_ip6_address: None,
            num_rx_queues: 1,
            num_tx_queues: None,
            rx_ring_size: 256,
            tx_ring_size: 256,
            gso: false,
            csum_offload: false,
        }
    }
}

#[derive(Clone, Copy)]
struct TunRxNext {
    drop: u16,
    ip4: u16,
    ip6: u16,
}

impl TunRxNext {
    fn resolve(runtime: &DataPlaneMain) -> Self {
        let nodes = runtime.nodes();
        let input = nodes
            .node_by_name(TunInputNode::NODE_NAME)
            .expect("tun-input is materialized before TUN config");
        let slot = |target| {
            nodes
                .node_next_slot_for_target(input, target)
                .expect("TUN input next lookup names live graph nodes")
                .expect("device-input sibling has IP and drop next slots")
        };
        Self {
            drop: slot(
                nodes
                    .node_by_name(DropNode::NODE_NAME)
                    .expect("drop node exists"),
            ),
            ip4: slot(
                nodes
                    .node_by_name(Ip4InputNode::NODE_NAME)
                    .expect("ip4-input exists"),
            ),
            ip6: slot(
                nodes
                    .node_by_name(Ip6InputNode::NODE_NAME)
                    .expect("ip6-input exists"),
            ),
        }
    }
}

#[repr(C)]
struct TunRxQueue {
    cacheline0: CacheLineAlignMark,
    ring: Virtqueue,
    buffers: Vec<Option<u32>>,
    kick: OwnedFd,
    call: OwnedFd,
    file_index: Option<u32>,
    last_used: u16,
    desc_next: u16,
    desc_in_use: u16,
    queue_id: u16,
    cacheline1: CacheLineAlignMark,
    queue_index: Option<u32>,
}

impl TunRxQueue {
    fn new(queue_id: u16, size: u16) -> Result<Self, TunConfigError> {
        let kick =
            vhost::eventfd().map_err(|source| TunConfigError::EventFd { queue_id, source })?;
        let call =
            vhost::eventfd().map_err(|source| TunConfigError::EventFd { queue_id, source })?;
        Ok(Self {
            cacheline0: CacheLineAlignMark,
            ring: Virtqueue::new(size),
            buffers: vec![None; size as usize],
            kick,
            call,
            file_index: None,
            last_used: 0,
            desc_next: 0,
            desc_in_use: 0,
            queue_id,
            cacheline1: CacheLineAlignMark,
            queue_index: None,
        })
    }

    // VPP tap/rx_node.c:30-87; descriptors are published only after Buffer
    // storage and the available ring entries are ready.
    #[inline(always)]
    fn refill(&mut self, runtime: &mut DataPlaneMain) -> usize {
        let size = self.ring.size;
        let mut unavailable = 0;
        while size - self.desc_in_use >= size / 8 {
            let requested = usize::from((size - self.desc_in_use).min(64));
            if requested == 0 {
                break;
            }
            let mut allocated = [0u32; 64];
            let count = runtime.buffer_alloc(&mut allocated[..requested]);
            unavailable += requested - count;
            if count == 0 {
                break;
            }
            let mut available = self.ring.available_index();
            for index in allocated[..count].iter().copied() {
                let slot = self.desc_next;
                runtime.buffer_chain_init(index);
                let buffer = runtime.buffer_mut(index);
                let memory =
                    buffer.make_headroom((vhost::TUN_DATA_OFFSET - vhost::NET_HEADER_LEN) as u8);
                self.ring.set_descriptor(
                    slot,
                    Descriptor {
                        addr: memory.as_ptr() as u64,
                        len: memory.len() as u32,
                        flags: vhost::DESC_WRITE,
                        next: 0,
                    },
                );
                assert!(
                    self.buffers[slot as usize].replace(index).is_none(),
                    "RX descriptor owns at most one Buffer"
                );
                self.ring.set_available(available, slot);
                available = available.wrapping_add(1);
                self.desc_next = (slot + 1) & (size - 1);
                self.desc_in_use += 1;
            }
            self.ring.publish_available(available);
            if self.ring.notify_enabled() {
                vhost::kick(&self.kick);
            }
            if count < requested {
                break;
            }
        }
        unavailable
    }

    #[inline(always)]
    fn has_used(&self) -> bool {
        self.ring.used_index() != self.last_used
    }
}

#[repr(C)]
struct TunTxQueue {
    cacheline0: CacheLineAlignMark,
    ring: Virtqueue,
    buffers: Vec<Option<u32>>,
    kick: OwnedFd,
    last_used: u16,
    desc_in_use: u16,
    free_head: u16,
    queue_id: u16,
    cacheline1: CacheLineAlignMark,
    queue_index: Option<u32>,
}

impl TunTxQueue {
    fn new(queue_id: u16, size: u16) -> Result<Self, TunConfigError> {
        let kick =
            vhost::eventfd().map_err(|source| TunConfigError::EventFd { queue_id, source })?;
        let ring = Virtqueue::new(size);
        for index in 0..size {
            ring.set_descriptor(
                index,
                Descriptor {
                    next: index.wrapping_sub(1),
                    ..Descriptor::default()
                },
            );
        }
        Ok(Self {
            cacheline0: CacheLineAlignMark,
            ring,
            buffers: vec![None; size as usize],
            kick,
            last_used: 0,
            desc_in_use: 0,
            free_head: size - 1,
            queue_id,
            cacheline1: CacheLineAlignMark,
            queue_index: None,
        })
    }

    // VPP tap/tx_node.c:61-88. The used index is the kernel's release
    // publication of descriptor ownership back to this worker.
    #[inline(always)]
    fn free_used(&mut self, runtime: &mut DataPlaneMain) {
        let used = self.ring.used_index();
        while self.last_used != used {
            let element = self.ring.used_element(self.last_used);
            let slot = u16::try_from(element.id).expect("vhost used descriptor fits ring index");
            assert!(
                slot < self.ring.size,
                "vhost used descriptor belongs to TX ring"
            );
            let buffer = self.buffers[slot as usize]
                .take()
                .expect("used TX descriptor owns a Buffer");
            runtime.buffer_free_one(buffer);
            self.ring.set_descriptor(
                slot,
                Descriptor {
                    next: self.free_head,
                    ..Descriptor::default()
                },
            );
            self.free_head = slot;
            self.desc_in_use -= 1;
            self.last_used = self.last_used.wrapping_add(1);
        }
    }
}

#[repr(C)]
struct TunWorker {
    cacheline0: CacheLineAlignMark,
    state: RefCell<TunWorkerState>,
}

#[derive(Default)]
struct TunWorkerState {
    rx: Vec<TunRxQueue>,
    tx: Vec<TunTxQueue>,
}

struct TunInterface {
    hw_if_index: u32,
    sw_if_index: u32,
    input_node: NodeId,
    next: TunRxNext,
    tun_fds: Vec<OwnedFd>,
    vhost_fds: Vec<OwnedFd>,
    workers: Vec<TunWorker>,
    gso_enabled: bool,
    csum_offload_enabled: bool,
}

struct TunMain {
    memory: vhost::MemoryTable,
    interface: UnsafeCell<Option<TunInterface>>,
}

// SAFETY: create_if publishes the sole interface before Data Worker launch.
// Each Data Worker subsequently borrows only its own immutable thread-index
// slot. Main-thread teardown runs after worker dispatch has stopped.
unsafe impl Sync for TunMain {}

static TUN_MAIN: OnceLock<TunMain> = OnceLock::new();

impl TunMain {
    fn init() -> &'static Self {
        TUN_MAIN.get_or_init(|| Self {
            memory: vhost::MemoryTable::new(),
            interface: UnsafeCell::new(None),
        })
    }

    fn global() -> &'static Self {
        TUN_MAIN
            .get()
            .expect("tun_init runs before TUN config or graph dispatch")
    }

    fn interface(&self) -> &TunInterface {
        unsafe { &*self.interface.get() }
            .as_ref()
            .expect("enabled TUN has a published interface")
    }

    fn create_if(&self, config: TunConfig, runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
        assert!(
            unsafe { &*self.interface.get() }.is_none(),
            "one TUN is configured only once"
        );
        let worker_count = hammer_runtime::config::worker::worker_count();
        let rx_count = config.num_rx_queues;
        let requested_tx = config.num_tx_queues.unwrap_or(worker_count as u16);
        if rx_count == 0 || requested_tx == 0 || worker_count == 0 {
            return Err(TunConfigError::QueueCount {
                rx: rx_count,
                tx: requested_tx,
            }
            .into());
        }
        let tx_count = requested_tx.max(worker_count as u16);
        for (queue, size) in [("rx", config.rx_ring_size), ("tx", config.tx_ring_size)] {
            if size == 0 || !size.is_power_of_two() || size > 32768 {
                return Err(TunConfigError::RingSize { queue, size }.into());
            }
        }
        if !(64..=65355).contains(&config.mtu) {
            return Err(TunConfigError::Mtu { mtu: config.mtu }.into());
        }
        if config.name.is_empty()
            || config.name.len() >= libc::IFNAMSIZ
            || config.name.as_bytes().contains(&0)
        {
            return Err(TunConfigError::InterfaceName { name: config.name }.into());
        }
        let (tun_fds, host_name) = open_tun_queues(
            &config.name,
            rx_count,
            tx_count,
            config.gso,
            config.csum_offload,
        )?;
        let host_ifindex = {
            let name = std::ffi::CString::new(host_name).expect("kernel TUN name is NUL-free");
            let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
            if index == 0 {
                return Err(TunConfigError::HostLink {
                    ifindex: 0,
                    source: io::Error::last_os_error(),
                }
                .into());
            }
            index
        };

        let mut vhost_fds = Vec::with_capacity(usize::from(rx_count.max(tx_count)));
        for queue_id in 0..rx_count.max(tx_count) {
            let descriptor = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
                .open("/dev/vhost-net")
                .map_err(|source| TunConfigError::OpenVhost { queue_id, source })?;
            let fd: OwnedFd = descriptor.into();
            vhost::set_owner(&fd)
                .map_err(|source| TunConfigError::VhostVring { queue_id, source })?;
            let features = vhost::get_features(&fd)
                .map_err(|source| TunConfigError::VhostVring { queue_id, source })?;
            let missing = vhost::FEATURES & !features;
            if missing != 0 {
                return Err(TunConfigError::VhostFeatureUnsupported { missing }.into());
            }
            vhost_fds.push(fd);
        }

        let mut rx = Vec::with_capacity(rx_count as usize);
        for queue_id in 0..rx_count {
            rx.push(TunRxQueue::new(queue_id, config.rx_ring_size)?);
        }
        let mut tx = Vec::with_capacity(tx_count as usize);
        for queue_id in 0..tx_count {
            tx.push(TunTxQueue::new(queue_id, config.tx_ring_size)?);
        }

        let mut host = host::HostLink::new().map_err(|source| TunConfigError::HostLink {
            ifindex: host_ifindex,
            source,
        })?;
        if let Some(address) = config.host_ip4_address {
            host.add_address(host_ifindex, IpNet::V4(address))
                .map_err(|source| TunConfigError::HostAddress {
                    ifindex: host_ifindex,
                    address: IpNet::V4(address),
                    source,
                })?;
        }
        if let Some(address) = config.host_ip6_address {
            host.add_address(host_ifindex, IpNet::V6(address))
                .map_err(|source| TunConfigError::HostAddress {
                    ifindex: host_ifindex,
                    address: IpNet::V6(address),
                    source,
                })?;
        }
        host.set_mtu(host_ifindex, config.mtu)
            .map_err(|source| TunConfigError::HostMtu {
                ifindex: host_ifindex,
                source,
            })?;
        host.set_up(host_ifindex)
            .map_err(|source| TunConfigError::HostLink {
                ifindex: host_ifindex,
                source,
            })?;

        for (queue_id, fd) in vhost_fds.iter().enumerate() {
            vhost::set_features(fd).map_err(|source| TunConfigError::VhostVring {
                queue_id: queue_id as u16,
                source,
            })?;
            self.memory
                .install(fd)
                .map_err(|source| TunConfigError::VhostMemoryTable { source })?;
        }

        let interfaces = NetMain::global()?.interface_main();
        let install = (|| -> Result<(), TunConfigError> {
            for queue in &rx {
                let queue_id = queue.queue_id;
                queue
                    .ring
                    .install(
                        &vhost_fds[queue_id as usize],
                        0,
                        &queue.kick,
                        Some(&queue.call),
                        &tun_fds[queue_id as usize],
                    )
                    .map_err(|source| TunConfigError::VhostVring { queue_id, source })?;
            }
            for queue in &tx {
                let queue_id = queue.queue_id;
                queue
                    .ring
                    .install(
                        &vhost_fds[queue_id as usize],
                        1,
                        &queue.kick,
                        None,
                        &tun_fds[queue_id as usize % tun_fds.len()],
                    )
                    .map_err(|source| TunConfigError::VhostVring { queue_id, source })?;
            }
            Ok(())
        })();
        if let Err(error) = install {
            drop(vhost_fds);
            return Err(error.into());
        }
        let hw_if_index = interfaces.register_interface(
            runtime,
            interfaces.device_class_index("tuntap"),
            0,
            interfaces.hw_class_index("tun"),
            0,
        );
        let sw_if_index = interfaces.hardware_interface(hw_if_index).sw_if_index();
        let next = TunRxNext::resolve(runtime);
        let input_node = runtime
            .nodes()
            .node_by_name(TunInputNode::NODE_NAME)
            .expect("tun-input is materialized before TUN config");
        let mut workers = (0..=worker_count)
            .map(|_| TunWorker {
                cacheline0: CacheLineAlignMark,
                state: RefCell::new(TunWorkerState::default()),
            })
            .collect::<Vec<_>>();
        for queue in rx {
            let worker = (queue.queue_id as usize % worker_count) + 1;
            workers[worker].state.get_mut().rx.push(queue);
        }
        for queue in tx {
            let worker = (queue.queue_id as usize % worker_count) + 1;
            workers[worker].state.get_mut().tx.push(queue);
        }

        let registration = (|| -> RuntimeResult<()> {
            interfaces.set_input_node(hw_if_index, input_node);
            interfaces.set_mtu(
                runtime,
                sw_if_index,
                InterfaceMtu::new(config.mtu, config.mtu, config.mtu, config.mtu),
            )?;
            interfaces.set_hardware_flags(runtime, hw_if_index, HwInterfaceFlags::LINK_UP)?;
            if config.admin_up {
                interfaces.set_software_flags(runtime, sw_if_index, SwInterfaceFlags::ADMIN_UP)?;
            }
            if let Some(node) = interfaces.hardware_interface(hw_if_index).tx_node_index {
                runtime.register_node_errors(node, &TUN_TX_ERRORS)?;
            }
            for (worker, slot) in workers.iter_mut().enumerate().skip(1) {
                for queue in &mut slot.state.get_mut().rx {
                    let index = interfaces.register_rx_queue(
                        hw_if_index,
                        queue.queue_id as u32,
                        DataWorkerId::new((worker - 1) as u32),
                        DriverScheduleMode::Interrupt,
                    )?;
                    queue.queue_index = Some(index);
                    let file_fd = queue.call.as_fd().try_clone_to_owned().map_err(|source| {
                        RuntimeError::FilePollerIo {
                            operation: "duplicate TUN call eventfd",
                            source,
                        }
                    })?;
                    let mut file = File::new(
                        file_fd,
                        format!("tun rx queue {}", queue.queue_id),
                        index as u64,
                        FileFunctions {
                            read: Some(tun_call_ready),
                            ..FileFunctions::default()
                        },
                    );
                    file.set_polling_thread_index(worker as u32);
                    let file_index = FILE_MAIN
                        .get()
                        .expect("FileMain is initialized before TUN config")
                        .add(file)?;
                    queue.file_index = Some(file_index);
                    interfaces.set_rx_queue_file_index(index, file_index);
                }
            }
            for (worker, slot) in workers.iter_mut().enumerate().skip(1) {
                for queue in &mut slot.state.get_mut().tx {
                    let index =
                        interfaces.register_tx_queue(hw_if_index, queue.queue_id as u32, false)?;
                    interfaces
                        .assign_tx_queue_to_worker(index, DataWorkerId::new((worker - 1) as u32))?;
                    queue.queue_index = Some(index);
                }
            }
            if let Some(address) = config.ip4_address {
                hammer_plugin_ip::ip4_add_del_interface_address(
                    runtime,
                    sw_if_index,
                    address.addr(),
                    address.prefix_len(),
                    false,
                )
                .map_err(|source| TunConfigError::Ip4Address {
                    sw_if_index,
                    address,
                    source,
                })?;
            }
            for worker in workers.iter_mut().skip(1) {
                for queue in &mut worker.state.get_mut().rx {
                    let unavailable = queue.refill(runtime);
                    if unavailable != 0 {
                        return Err(TunConfigError::RxBufferAlloc {
                            queue_id: queue.queue_id,
                            requested: queue.ring.size as usize,
                            available: queue.ring.size as usize - unavailable,
                        }
                        .into());
                    }
                }
            }
            Ok(())
        })();
        if let Err(startup_error) = registration {
            // Closing vhost fds first quiesces every backend, including queues
            // whose File or service registration failed partway through.
            drop(vhost_fds);
            for worker in workers.iter_mut().skip(1) {
                let state = worker.state.get_mut();
                for queue in &state.rx {
                    if let Some(file_index) = queue.file_index {
                        FILE_MAIN
                            .get()
                            .expect("FileMain survives TUN startup")
                            .delete(file_index)
                            .expect("registered TUN File is removable during startup");
                    }
                    for buffer in queue.buffers.iter().flatten() {
                        runtime.buffer_free_no_next(std::slice::from_ref(buffer));
                    }
                }
            }
            interfaces.delete_hardware_interface(runtime, hw_if_index);
            return Err(startup_error);
        }
        unsafe { &mut *self.interface.get() }.replace(TunInterface {
            hw_if_index,
            sw_if_index,
            input_node,
            next,
            tun_fds,
            vhost_fds,
            workers,
            gso_enabled: config.gso,
            csum_offload_enabled: config.csum_offload || config.gso,
        });
        Ok(())
    }
}

fn open_tun_queues(
    name: &str,
    rx: u16,
    tx: u16,
    gso: bool,
    csum_offload: bool,
) -> Result<(Vec<OwnedFd>, String), TunConfigError> {
    let mut fds = Vec::with_capacity(rx as usize);
    let mut actual_name = name.to_owned();
    for queue_id in 0..rx {
        let descriptor = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open("/dev/net/tun")
            .map_err(|source| TunConfigError::OpenTun { source })?;
        let fd: OwnedFd = descriptor.into();
        let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
        for (destination, source) in request.ifr_name.iter_mut().zip(actual_name.bytes()) {
            *destination = source as libc::c_char;
        }
        let mut features: libc::c_uint = 0;
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNGETFEATURES, &mut features) } < 0 {
            return Err(TunConfigError::TunFeatureQuery {
                source: io::Error::last_os_error(),
            });
        }
        if features & libc::IFF_VNET_HDR as u32 == 0 {
            return Err(TunConfigError::TunFeatureQuery {
                source: io::Error::new(io::ErrorKind::Unsupported, "IFF_VNET_HDR unavailable"),
            });
        }
        if rx.max(tx) > 1 && features & libc::IFF_MULTI_QUEUE as u32 == 0 {
            return Err(TunConfigError::MultiQueueUnsupported {
                requested: rx.max(tx),
            });
        }
        let mut flags = libc::IFF_TUN | libc::IFF_NO_PI | libc::IFF_VNET_HDR;
        // This plugin creates a new link; only later queue fds may attach to it.
        if queue_id == 0 {
            flags |= libc::IFF_TUN_EXCL;
        }
        if rx.max(tx) > 1 {
            flags |= libc::IFF_MULTI_QUEUE;
        }
        request.ifr_ifru.ifru_flags = flags as libc::c_short;
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETIFF, &mut request) } < 0 {
            return Err(TunConfigError::TunAttach {
                queue_id,
                source: io::Error::last_os_error(),
            });
        }
        if queue_id == 0 {
            actual_name = unsafe { CStr::from_ptr(request.ifr_name.as_ptr()) }
                .to_str()
                .map_err(|source| TunConfigError::TunAttach {
                    queue_id,
                    source: io::Error::new(io::ErrorKind::InvalidData, source),
                })?
                .to_owned();
        }
        let mut header_size = vhost::NET_HEADER_LEN as libc::c_int;
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETVNETHDRSZ, &mut header_size) } < 0 {
            return Err(TunConfigError::TunAttach {
                queue_id,
                source: io::Error::last_os_error(),
            });
        }
        let mut send_buffer = libc::c_int::MAX - 1;
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETSNDBUF, &mut send_buffer) } < 0 {
            return Err(TunConfigError::TunAttach {
                queue_id,
                source: io::Error::last_os_error(),
            });
        }
        let offload = if gso {
            libc::TUN_F_CSUM | libc::TUN_F_TSO4 | libc::TUN_F_TSO6
        } else if csum_offload {
            libc::TUN_F_CSUM
        } else {
            0
        };
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETOFFLOAD, offload) } < 0 {
            return Err(TunConfigError::TunAttach {
                queue_id,
                source: io::Error::last_os_error(),
            });
        }
        fds.push(fd);
    }
    Ok((fds, actual_name))
}

fn tun_call_ready(graph: &mut hammer_runtime::NodeMain, file: &mut File) -> RuntimeResult<()> {
    let mut count = 0u64;
    let read = unsafe {
        libc::read(
            file.fd(),
            (&mut count as *mut u64).cast(),
            std::mem::size_of::<u64>(),
        )
    };
    if read < 0 && io::Error::last_os_error().kind() != io::ErrorKind::WouldBlock {
        return Err(RuntimeError::FilePollerIo {
            operation: "read TUN call eventfd",
            source: io::Error::last_os_error(),
        });
    }
    NetMain::global()?
        .interface_main()
        .set_rx_queue_interrupt_pending(graph, file.private_data() as u32)
}

#[derive(Clone, Copy)]
enum TunRxError {
    BufferAlloc,
    FullRxQueue,
}

impl NodeErrorCode for TunRxError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

const TUN_RX_ERRORS: [NodeErrorDescriptor; 2] = [
    NodeErrorDescriptor::new(
        "buffer-alloc",
        NodeErrorSeverity::Error,
        "RX Buffer allocation failed",
    ),
    NodeErrorDescriptor::new("full-rx-queue", NodeErrorSeverity::Info, "RX queue filled"),
];

#[derive(Clone, Copy)]
enum TunTxError {
    NoFreeSlots,
    TruncPacket,
    IndirectDescAllocFailed,
    GsoPacketDrop,
    CsumOffloadPacketDrop,
}

impl NodeErrorCode for TunTxError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

const TUN_TX_ERRORS: [NodeErrorDescriptor; 5] = [
    NodeErrorDescriptor::new(
        "no-free-slots",
        NodeErrorSeverity::Error,
        "No free TX descriptors",
    ),
    NodeErrorDescriptor::new(
        "trunc-packet",
        NodeErrorSeverity::Error,
        "TX chain exceeds indirect descriptor capacity",
    ),
    NodeErrorDescriptor::new(
        "indirect-desc-alloc",
        NodeErrorSeverity::Error,
        "Indirect descriptor Buffer allocation failed",
    ),
    NodeErrorDescriptor::new(
        "gso-packet-drop",
        NodeErrorSeverity::Error,
        "GSO is disabled for TUN",
    ),
    NodeErrorDescriptor::new(
        "csum-offload-packet-drop",
        NodeErrorSeverity::Error,
        "Checksum offload is disabled for TUN",
    ),
];

#[hammer_component_macros::graph_node(
    graph = tuntap, init = register_tun_input, role = driver, name = "tun-input",
    sibling_of = hammer_service::device::DeviceInputNode,
)]
struct TunInputNode;

fn register_tun_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_driver(TunInputNode::new())?;
    runtime.nodes().set_node_state(node, NodeState::Interrupt)?;
    runtime.register_node_errors(node, &TUN_RX_ERRORS)?;
    Ok(node)
}

// VPP tap_rx_offloads, tap/rx_node.c:89-188. The virtio header is metadata;
// payload remains in the kernel-filled Buffer chain.
#[inline(always)]
fn tun_rx_offloads(runtime: &mut DataPlaneMain, head: u32, flags: u8, gso_type: u8, gso_size: u16) {
    if flags & vhost::NEEDS_CHECKSUM == 0 {
        return;
    }
    let packet = runtime.buffer(head).current();
    let Some(version) = packet.first().map(|byte| byte >> 4) else {
        return;
    };
    let (transport_offset, protocol) = match version {
        4 if packet.len() >= 20 => {
            let offset = usize::from(packet[0] & 0x0f) * 4;
            if offset < 20 || packet.len() < offset {
                return;
            }
            (offset, packet[9])
        }
        6 if packet.len() >= 40 => (40, packet[6]),
        _ => return,
    };
    let transport_header_len = match protocol {
        6 if packet.len() >= transport_offset + 20 => {
            let length = usize::from(packet[transport_offset + 12] >> 4) * 4;
            if length < 20 || packet.len() < transport_offset + length {
                return;
            }
            length
        }
        17 if packet.len() >= transport_offset + 8 => 8,
        _ => return,
    };
    let gso = matches!((version, gso_type & 0x7f, protocol), (4, 1, 6) | (6, 4, 6));
    let buffer = runtime.buffer_mut(head);
    let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
    if version == 4 {
        network.oflags.insert(NetworkOffloadFlags::IP4_CHECKSUM);
    }
    if protocol == 6 {
        network.oflags.insert(NetworkOffloadFlags::TCP_CHECKSUM);
    } else {
        network.oflags.insert(NetworkOffloadFlags::UDP_CHECKSUM);
    }
    if gso {
        network.flags.insert(NetworkFlags::GSO);
        network.ip_mut().set_gso_size(gso_size);
        network
            .ip_mut()
            .set_transport_header_len(transport_header_len as u16);
    }
}

// VPP tap_device_input_one_inline, tap/rx_node.c:302-316. The input lanes
// contain the first IP byte; the output lanes are tun-input local next slots.
#[inline(always)]
fn tun_ip_nexts(nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY], count: usize, layout: TunRxNext) {
    const {
        assert!(std::mem::size_of::<u16x8>() == 8 * std::mem::size_of::<u16>());
    }
    assert!(
        count <= nexts.len(),
        "RX next count fits the padded frame array"
    );
    for position in (0..count).step_by(8) {
        let chunk = &mut nexts[position..position + 8];
        // VPP uses u16x8u: the stack array need not have 16-byte alignment.
        // u16x8 consists solely of integer lanes, so every initialized u16
        // pattern in this padded range is a valid vector value.
        let versions = unsafe { std::ptr::read_unaligned(chunk.as_ptr().cast::<u16x8>()) } >> 4;
        let ip4 = versions.simd_eq(u16x8::splat(4));
        let ip6 = versions.simd_eq(u16x8::splat(6));
        let selected = (!(ip4 | ip6) & u16x8::splat(layout.drop))
            | (ip4 & u16x8::splat(layout.ip4))
            | (ip6 & u16x8::splat(layout.ip6));
        unsafe { std::ptr::write_unaligned(chunk.as_mut_ptr().cast::<u16x8>(), selected) };
    }
}

impl Node for TunInputNode {
    fn process(runtime: &mut DataPlaneMain, node: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let main = TunMain::global().interface();
        let worker_index = runtime.thread_index() as usize;
        let worker_count = main.workers.len() - 1;
        let worker = &main.workers[worker_index];
        let mut state = worker.state.borrow_mut();
        let mut packets = 0;
        let mut alloc_error = 0;
        let mut full_queue = 0;
        let interfaces = NetMain::global()
            .expect("network Main exists during TUN RX")
            .interface_main();
        let worker_id = DataWorkerId::try_from(runtime.thread_index())
            .expect("tun-input runs only on a Data Worker");
        assert!(
            frame.is_empty(),
            "tun-input starts with an empty driver Frame"
        );
        for poll in interfaces.rx_queue_poll_vector(worker_id, main.input_node) {
            assert_eq!(
                poll.device_instance, 0,
                "TUN input owns its device instance"
            );
            assert_eq!(
                poll.queue_id as usize % worker_count + 1,
                worker_index,
                "polled TUN queue belongs to this Data Worker"
            );
            let queue = state
                .rx
                .get_mut(poll.queue_id as usize / worker_count)
                .expect("polled TUN queue is registered on this Data Worker");
            assert_eq!(u32::from(queue.queue_id), poll.queue_id);
            if queue.ring.used_index().wrapping_sub(queue.last_used) == queue.ring.size {
                full_queue += 1;
            }
            let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
            let mut count = 0;
            let (buffers, _) = frame.next_args_mut::<u32, ()>(0);
            while queue.has_used() && count < DEFAULT_BUFFER_FRAME_CAPACITY {
                let used = queue.ring.used_element(queue.last_used);
                let slot = u16::try_from(used.id).expect("vhost RX used id fits u16");
                assert!(slot < queue.ring.size, "vhost RX used id belongs to ring");
                let head =
                    queue.buffers[slot as usize].expect("RX used descriptor retains its Buffer");
                let length = used.len as usize;
                assert!(
                    length >= vhost::NET_HEADER_LEN,
                    "RX packet includes virtio header"
                );
                let (num_buffers, flags, gso_type, gso_size) = {
                    let bytes = runtime.buffer_mut(head).make_headroom(0);
                    assert!(
                        length <= bytes.len(),
                        "vhost RX length fits Buffer capacity"
                    );
                    let header = &bytes[..vhost::NET_HEADER_LEN];
                    (
                        u16::from_le_bytes([header[10], header[11]]).max(1),
                        header[0],
                        header[1],
                        u16::from_le_bytes([header[4], header[5]]),
                    )
                };
                let available = queue.ring.used_index().wrapping_sub(queue.last_used);
                if available < num_buffers {
                    break;
                }
                assert!(
                    num_buffers as usize <= DEFAULT_BUFFER_FRAME_CAPACITY,
                    "vhost packet chain fits frame capacity"
                );
                let mut total = 0usize;
                let mut previous = None;
                for offset in 0..num_buffers {
                    let element = queue
                        .ring
                        .used_element(queue.last_used.wrapping_add(offset));
                    let id = usize::try_from(element.id).expect("vhost RX id fits usize");
                    let index = queue.buffers[id]
                        .take()
                        .expect("RX descriptor still owns Buffer");
                    let bytes = element.len as usize;
                    if let Some(previous) = previous {
                        runtime.buffer_chain_buffer(previous, index);
                    }
                    let buffer = runtime.buffer_mut(index);
                    if offset == 0 {
                        buffer.put_uninit(u16::try_from(bytes).expect("RX Buffer length fits u16"));
                        buffer.advance(vhost::NET_HEADER_LEN as isize);
                    } else {
                        buffer.put_uninit(u16::try_from(bytes).expect("RX Buffer length fits u16"));
                    }
                    previous = Some(index);
                    total += bytes
                        - if offset == 0 {
                            vhost::NET_HEADER_LEN
                        } else {
                            0
                        };
                }
                let tail_length = total - runtime.buffer(head).current_len();
                runtime
                    .buffer_mut(head)
                    .set_total_len_not_including_first(tail_length)
                    .expect("TUN packet length fits Buffer metadata");
                queue.last_used = queue.last_used.wrapping_add(num_buffers);
                queue.desc_in_use -= num_buffers;
                let mut network = NetworkOpaque::default();
                network.sw_if_index[0] = main.sw_if_index;
                network.l3_hdr_offset = vhost::TUN_DATA_OFFSET as _;
                *hammer_core::buffer_opaque!(mut runtime.buffer_mut(head) => NetworkOpaque) =
                    network;
                tun_rx_offloads(runtime, head, flags, gso_type, gso_size);
                buffers[count] = head;
                nexts[count] =
                    u16::from(runtime.buffer(head).current().first().copied().unwrap_or(0));
                count += 1;
            }
            tun_ip_nexts(&mut nexts, count, main.next);
            frame.set_vector_count(count);
            let is_up = interfaces
                .software_interface(main.sw_if_index)
                .expect("TUN interface remains live")
                .is_admin_up();
            let features = FeatureMain::global().expect("Feature Main exists during TUN RX");
            for (index, next) in frame.vector_args().iter().copied().zip(&mut nexts[..count]) {
                if !is_up {
                    *next = main.next.drop;
                }
                *next =
                    features.start_device_input(main.sw_if_index, runtime.buffer_mut(index), *next);
            }
            runtime.enqueue_to_next(node, frame, &nexts[..count]);
            frame.set_vector_count(0);
            packets += count;
            alloc_error += queue.refill(runtime);
        }
        for queue in &state.rx {
            if queue.has_used() {
                interfaces
                    .set_rx_queue_interrupt_pending(
                        runtime.nodes(),
                        queue.queue_index.expect("TUN RX queue is registered"),
                    )
                    .expect("TUN RX queue remains registered during dispatch");
            }
        }
        runtime
            .record_current_node_error_count(TunRxError::BufferAlloc, alloc_error as u64)
            .expect("tun-input error counters are registered");
        runtime
            .record_current_node_error_count(TunRxError::FullRxQueue, full_queue)
            .expect("tun-input error counters are registered");
        packets
    }
}

// VPP tap/tx_node.c:110-184. Virtio completes the L4 checksum from the
// pseudo-header seed; IPv4 header checksum remains the device's responsibility.
#[inline(always)]
fn set_checksum_offsets(buffer: &mut Buffer, header: &mut vhost::NetHeader) {
    let network = *hammer_core::buffer_opaque!(buffer => NetworkOpaque);
    let cursor = network.packet_cursor();
    let ip = cursor.network_header_offset();
    let packet = buffer.current_mut();
    assert!(
        packet.len() >= ip + 20,
        "offloaded IP header fits first Buffer"
    );
    let version = packet[ip] >> 4;
    if version == 4 && network.oflags.contains(NetworkOffloadFlags::IP4_CHECKSUM) {
        let length = usize::from(packet[ip] & 0x0f) * 4;
        assert!(
            length >= 20 && packet.len() >= ip + length,
            "IPv4 header length is valid"
        );
        packet[ip + 10..ip + 12].fill(0);
        let checksum = internet_checksum(&packet[ip..ip + length]);
        packet[ip + 10..ip + 12].copy_from_slice(&checksum.to_be_bytes());
    }
    let checksum_offset = if network.oflags.contains(NetworkOffloadFlags::TCP_CHECKSUM) {
        16
    } else if network.oflags.contains(NetworkOffloadFlags::UDP_CHECKSUM) {
        6
    } else {
        return;
    };
    let transport = cursor.transport_header_offset();
    let header_length = if checksum_offset == 16 {
        assert!(
            packet.len() >= transport + 20,
            "TCP header fits first Buffer"
        );
        let length = usize::from(packet[transport + 12] >> 4) * 4;
        assert!(
            length >= 20 && packet.len() >= transport + length,
            "TCP header length is valid"
        );
        length
    } else {
        assert!(
            packet.len() >= transport + 8,
            "UDP header fits first Buffer"
        );
        8
    };
    let pseudo = match version {
        4 => {
            let ip_length = usize::from(packet[ip] & 0x0f) * 4;
            let total = u16::from_be_bytes([packet[ip + 2], packet[ip + 3]]);
            let payload = total
                .checked_sub(ip_length as u16)
                .expect("IPv4 payload length is valid");
            !internet_checksum_parts(&[
                &packet[ip + 12..ip + 20],
                &[0, packet[ip + 9]],
                &payload.to_be_bytes(),
            ])
        }
        6 => {
            assert!(packet.len() >= ip + 40, "IPv6 header fits first Buffer");
            let payload = u16::from_be_bytes([packet[ip + 4], packet[ip + 5]]);
            !internet_checksum_parts(&[
                &packet[ip + 8..ip + 40],
                &u32::from(payload).to_be_bytes(),
                &[0, 0, 0, packet[ip + 6]],
            ])
        }
        _ => panic!("offloaded packet has an IP version"),
    };
    packet[transport + checksum_offset..transport + checksum_offset + 2]
        .copy_from_slice(&pseudo.to_be_bytes());
    header.flags = vhost::NEEDS_CHECKSUM;
    header.header_len = ((transport + header_length) as u16).to_le_bytes();
    header.checksum_start = (transport as u16).to_le_bytes();
    header.checksum_offset = (checksum_offset as u16).to_le_bytes();
}

// VPP tap/tx_node.c:186-218. The GSO producer owns the TCP partial checksum.
#[inline(always)]
fn set_gso_offsets(buffer: &mut Buffer, header: &mut vhost::NetHeader) {
    let network = *hammer_core::buffer_opaque!(buffer => NetworkOpaque);
    let cursor = network.packet_cursor();
    let ip = cursor.network_header_offset();
    let packet = buffer.current_mut();
    assert!(packet.len() >= ip + 20, "GSO IP header fits first Buffer");
    let version = packet[ip] >> 4;
    if version == 4 && network.oflags.contains(NetworkOffloadFlags::IP4_CHECKSUM) {
        let length = usize::from(packet[ip] & 0x0f) * 4;
        assert!(
            length >= 20 && packet.len() >= ip + length,
            "IPv4 header length is valid"
        );
        packet[ip + 10..ip + 12].fill(0);
        let checksum = internet_checksum(&packet[ip..ip + length]);
        packet[ip + 10..ip + 12].copy_from_slice(&checksum.to_be_bytes());
    }
    header.flags = vhost::NEEDS_CHECKSUM;
    header.gso_type = match version {
        4 => vhost::GSO_TCP4,
        6 => vhost::GSO_TCP6,
        _ => panic!("GSO packet has an IP version"),
    };
    let transport = cursor.transport_header_offset();
    assert!(
        cursor.transport_header_len() >= 20 && packet.len() >= cursor.transport_payload_offset(),
        "GSO TCP header fits first Buffer"
    );
    header.header_len = (cursor.transport_payload_offset() as u16).to_le_bytes();
    header.gso_size = network.ip().gso_size().to_le_bytes();
    header.checksum_start = (transport as u16).to_le_bytes();
    header.checksum_offset = 16u16.to_le_bytes();
}

// VPP tap/tx_node.c:220-374. A batch consumes only as many packets as it can
// publish or classify; a full vring leaves the rest for used-ring retries.
#[inline(always)]
fn tun_if_tx(
    runtime: &mut DataPlaneMain,
    queue: &mut TunTxQueue,
    buffers: &[u32],
    gso_enabled: bool,
    csum_offload_enabled: bool,
    drops: &mut [u64; 5],
) -> usize {
    let mut available = queue.ring.available_index();
    let mut published = 0;
    let mut consumed = 0;
    while consumed < buffers.len() && queue.desc_in_use < queue.ring.size {
        let head = buffers[consumed];
        consumed += 1;
        let mut count = 0usize;
        let mut exceeds_capacity = false;
        let mut current = Some(head);
        while let Some(index) = current {
            count += 1;
            current = runtime.buffer(index).next_buffer_slot();
            if count > 127
                || count * std::mem::size_of::<Descriptor>() > runtime.buffer_default_data_size()
            {
                exceeds_capacity = true;
                break;
            }
        }
        if exceeds_capacity {
            drops[TunTxError::TruncPacket as usize] += 1;
            runtime.buffer_free_one(head);
            continue;
        }
        let network = *hammer_core::buffer_opaque!(runtime.buffer(head) => NetworkOpaque);
        let mut header = vhost::NetHeader::default();
        if network.flags.contains(NetworkFlags::GSO) {
            if !gso_enabled {
                drops[TunTxError::GsoPacketDrop as usize] += 1;
                runtime.buffer_free_one(head);
                continue;
            }
            set_gso_offsets(runtime.buffer_mut(head), &mut header);
        } else if !network.oflags.is_empty() {
            if !csum_offload_enabled {
                drops[TunTxError::CsumOffloadPacketDrop as usize] += 1;
                runtime.buffer_free_one(head);
                continue;
            }
            set_checksum_offsets(runtime.buffer_mut(head), &mut header);
        }
        let first = runtime.buffer_mut(head);
        let headroom = first.current_data_offset() as isize
            + hammer_core::buffer::BUFFER_PRE_DATA_SIZE as isize;
        if headroom < vhost::NET_HEADER_LEN as isize {
            drops[TunTxError::TruncPacket as usize] += 1;
            runtime.buffer_free_one(head);
            continue;
        }
        let bytes = first.push_uninit(vhost::NET_HEADER_LEN as u8);
        header.write_to(bytes);
        let first_addr = bytes.as_ptr() as u64;
        first.advance(vhost::NET_HEADER_LEN as isize);
        let mut owned = head;
        let descriptor = if count == 1 {
            Descriptor {
                addr: first_addr,
                len: (runtime.buffer(head).current_len() + vhost::NET_HEADER_LEN) as u32,
                flags: 0,
                next: 0,
            }
        } else {
            let mut allocated = [0u32; 1];
            if runtime.buffer_alloc(&mut allocated) != 1 {
                drops[TunTxError::IndirectDescAllocFailed as usize] += 1;
                runtime.buffer_free_one(head);
                continue;
            }
            owned = allocated[0];
            runtime.buffer_chain_init(owned);
            let entries = runtime
                .buffer_mut(owned)
                .put_uninit((count * std::mem::size_of::<Descriptor>()) as u16);
            let entries_ptr = entries.as_mut_ptr();
            let address = entries_ptr as u64;
            let mut segment = Some(head);
            for position in 0..count {
                let index = segment.expect("count equals Buffer chain length");
                let buffer = runtime.buffer(index);
                let descriptor = Descriptor {
                    addr: if position == 0 {
                        first_addr
                    } else {
                        buffer.current().as_ptr() as u64
                    },
                    len: (buffer.current_len()
                        + if position == 0 {
                            vhost::NET_HEADER_LEN
                        } else {
                            0
                        }) as u32,
                    flags: if position + 1 < count {
                        vhost::DESC_NEXT
                    } else {
                        0
                    },
                    next: (position + 1) as u16,
                };
                // The indirect Buffer owns its descriptor table until used completion.
                unsafe {
                    std::ptr::write_unaligned(
                        entries_ptr.cast::<Descriptor>().add(position),
                        descriptor,
                    )
                };
                segment = buffer.next_buffer_slot();
            }
            runtime.buffer_chain_buffer(owned, head);
            Descriptor {
                addr: address,
                len: (count * std::mem::size_of::<Descriptor>()) as u32,
                flags: vhost::DESC_INDIRECT,
                next: 0,
            }
        };
        let slot = queue.free_head;
        queue.free_head = queue.ring.descriptor(slot).next;
        queue.ring.set_descriptor(slot, descriptor);
        assert!(
            queue.buffers[slot as usize].replace(owned).is_none(),
            "free TX descriptor owns no Buffer"
        );
        queue.ring.set_available(available, slot);
        available = available.wrapping_add(1);
        queue.desc_in_use += 1;
        published += 1;
    }
    if published != 0 {
        queue.ring.publish_available(available);
        if queue.ring.notify_enabled() {
            vhost::kick(&queue.kick);
        }
    }
    consumed
}

fn tun_intfc_tx(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
    let queue_id = frame.scalar_as::<TxFrame>().queue_id;
    let packets = frame.vectors_as::<TxFrame>();
    let main = TunMain::global().interface();
    let worker_index = runtime.thread_index() as usize;
    let worker_count = main.workers.len() - 1;
    assert_eq!(
        queue_id as usize % worker_count + 1,
        worker_index,
        "TX frame queue belongs to the executing worker"
    );
    let worker = &main.workers[worker_index];
    let mut state = worker.state.borrow_mut();
    // Queue ids are assigned in order and partitioned by worker modulo count.
    // VPP tap/internal.h:tap_get_tx_queue indexes the selected queue directly.
    let queue = state
        .tx
        .get_mut(queue_id as usize / worker_count)
        .expect("TX frame queue is registered on the executing worker");
    assert_eq!(u32::from(queue.queue_id), queue_id);
    let mut drops = [0u64; 5];
    let mut consumed = 0;
    let mut retries = 2;
    loop {
        queue.free_used(runtime);
        consumed += tun_if_tx(
            runtime,
            queue,
            &packets[consumed..],
            main.gso_enabled,
            main.csum_offload_enabled,
            &mut drops,
        );
        if consumed == packets.len() || retries == 0 {
            break;
        }
        retries -= 1;
    }
    let no_slots = packets.len() - consumed;
    if no_slots != 0 {
        drops[TunTxError::NoFreeSlots as usize] = no_slots as u64;
        runtime.buffer_free(&packets[consumed..]);
    }
    for (error, count) in [
        TunTxError::NoFreeSlots,
        TunTxError::TruncPacket,
        TunTxError::IndirectDescAllocFailed,
        TunTxError::GsoPacketDrop,
        TunTxError::CsumOffloadPacketDrop,
    ]
    .into_iter()
    .zip(drops)
    {
        if count != 0 {
            runtime
                .record_current_node_error_count(error, count)
                .expect("TUN TX error counters are registered");
        }
    }
    consumed
}

#[hammer_component_macros::runtime_error(subsystem = "tuntap")]
#[derive(Debug, thiserror::Error)]
enum TunConfigError {
    #[error("invalid TUN queue count: RX {rx}, TX {tx}")]
    QueueCount { rx: u16, tx: u16 },
    #[error("invalid {queue} ring size {size}")]
    RingSize { queue: &'static str, size: u16 },
    #[error("invalid TUN MTU {mtu}")]
    Mtu { mtu: u32 },
    #[error("invalid TUN interface name {name}")]
    InterfaceName { name: String },
    #[error("open /dev/net/tun")]
    OpenTun {
        #[source]
        source: io::Error,
    },
    #[error("query TUN capabilities")]
    TunFeatureQuery {
        #[source]
        source: io::Error,
    },
    #[error("TUN multi-queue is unavailable for {requested} queues")]
    MultiQueueUnsupported { requested: u16 },
    #[error("attach TUN queue {queue_id}")]
    TunAttach {
        queue_id: u16,
        #[source]
        source: io::Error,
    },
    #[error("open vhost-net queue {queue_id}")]
    OpenVhost {
        queue_id: u16,
        #[source]
        source: io::Error,
    },
    #[error("create queue {queue_id} eventfd")]
    EventFd {
        queue_id: u16,
        #[source]
        source: io::Error,
    },
    #[error("vhost-net is missing required features {missing:#x}")]
    VhostFeatureUnsupported { missing: u64 },
    #[error("install vhost-net memory table")]
    VhostMemoryTable {
        #[source]
        source: io::Error,
    },
    #[error("install vhost-net vring {queue_id}")]
    VhostVring {
        queue_id: u16,
        #[source]
        source: io::Error,
    },
    #[error("configure host link {ifindex}")]
    HostLink {
        ifindex: u32,
        #[source]
        source: io::Error,
    },
    #[error("configure {address} on host link {ifindex}")]
    HostAddress {
        ifindex: u32,
        address: IpNet,
        #[source]
        source: io::Error,
    },
    #[error("configure MTU on host link {ifindex}")]
    HostMtu {
        ifindex: u32,
        #[source]
        source: io::Error,
    },
    #[error("configure IPv4 address {address} on interface {sw_if_index}")]
    Ip4Address {
        sw_if_index: u32,
        address: Ipv4Net,
        #[source]
        source: IpInterfaceAddressError,
    },
    #[error(
        "RX Buffer allocation for queue {queue_id}: requested {requested}, available {available}"
    )]
    RxBufferAlloc {
        queue_id: u16,
        requested: usize,
        available: usize,
    },
}

#[hammer_component_macros::init_function(name = "tun_init", runs_after = ["interface_main_init"])]
fn tun_init(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    TunMain::init();
    Ok(())
}

#[hammer_component_macros::config_function(name = "tuntap_config", section = "plugin.tuntap")]
fn tuntap_config(config: TunConfig, runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    if !config.enabled {
        return Ok(());
    }
    TunMain::global().create_if(config, runtime)
}

#[hammer_component_macros::main_loop_exit_function(name = "tuntap_exit")]
fn tuntap_exit(runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    let Some(main) = TUN_MAIN.get() else {
        return Ok(());
    };
    let Some(mut interface) = (unsafe { &mut *main.interface.get() }).take() else {
        return Ok(());
    };
    // VPP tap_free closes vhost fds before freeing any vring or Buffer.
    drop(std::mem::take(&mut interface.vhost_fds));
    let mut file_error = None;
    for worker in interface.workers.iter_mut().skip(1) {
        let state = worker.state.get_mut();
        for queue in &mut state.rx {
            if let Some(file_index) = queue.file_index.take() {
                match FILE_MAIN
                    .get()
                    .expect("FileMain remains live during TUN shutdown")
                    .delete(file_index)
                {
                    Ok(deleted) => assert!(deleted, "TUN call File remains registered"),
                    Err(error) if file_error.is_none() => file_error = Some(error),
                    Err(error) => tracing::error!(%error, "additional TUN File shutdown error"),
                }
            }
            for index in queue.buffers.iter_mut().filter_map(Option::take) {
                runtime.buffer_free_no_next(std::slice::from_ref(&index));
            }
        }
        for queue in &mut state.tx {
            queue.free_used(runtime);
            for index in queue.buffers.iter_mut().filter_map(Option::take) {
                runtime.buffer_free_one(index);
            }
        }
    }
    NetMain::global()?
        .interface_main()
        .delete_hardware_interface(runtime, interface.hw_if_index);
    if let Some(error) = file_error {
        Err(error)
    } else {
        Ok(())
    }
}

hammer_component_macros::declare_plugin!(
    name = "tuntap",
    load_after = ["ip"],
    init_functions = [__INIT_FN_TUN_INIT],
    config_functions = [__CONFIG_FN_TUNTAP_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [__INIT_FN_TUNTAP_EXIT],
    worker_init_functions = [],
    graph_nodes = [__TUNTAP_GRAPH_NODE_TUN_INPUT_NODE],
    node_functions = [],
    process_nodes = [],
);
