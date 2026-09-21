use std::cell::{RefCell, RefMut, UnsafeCell};
use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::sync::OnceLock;

use hammer_core::buffer::DEFAULT_BUFFER_FRAME_CAPACITY;
use hammer_core::data_plane::{Frame, NodeId, NodeState};
use hammer_infra::align::CacheLineAlignMark;
use hammer_plugin_ip::{Ip4InputNode, Ip6InputNode, IpInterfaceAddressError};
use hammer_runtime::file::{FILE_MAIN, File, FileFunctions};
use hammer_runtime::{DataPlaneMain, Node, NodeRuntime, RuntimeError, RuntimeResult};
use hammer_service::data_plane::DropNode;
use hammer_service::feature::FeatureMain;
use hammer_service::interface::{HwClassFlags, HwInterfaceFlags, InterfaceMtu, SwInterfaceFlags};
use hammer_service::net::NetMain;
use hammer_service::opaque::NetworkOpaque;
use ipnet::Ipv4Net;

hammer_service::declare_interface_registration_image!();

#[derive(hammer_component_macros::DeviceClass)]
#[device_class(
    name = "tuntap",
    format_device_name = format_tuntap_interface_name,
    tx_function = tuntap_intfc_tx
)]
struct TuntapDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(name = "tuntap", flags = HwClassFlags::P2P)]
struct TuntapHwClass;

fn format_tuntap_interface_name(
    device_instance: u32,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(formatter, "tuntap-{device_instance}")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TuntapConfig {
    enabled: bool,
    name: String,
    mtu: u32,
    admin_up: bool,
    ip4_address: Option<Ipv4Net>,
}

impl Default for TuntapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "vnet".to_owned(),
            mtu: 4_096 + 256,
            admin_up: false,
            ip4_address: None,
        }
    }
}

enum TuntapFile {
    Active { control: OwnedFd, file_index: u32 },
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TuntapRxNext {
    drop: u16,
    ip4_input: u16,
    ip6_input: u16,
}

impl TuntapRxNext {
    fn resolve(nodes: &hammer_runtime::NodeMain) -> Self {
        let rx = nodes
            .node_by_name(TuntapRxNode::NODE_NAME)
            .expect("tuntap-rx is materialized before its next layout is resolved");
        let drop = nodes
            .node_by_name(DropNode::NODE_NAME)
            .expect("drop is materialized before tuntap input initialization");
        let ip4_input = nodes
            .node_by_name(Ip4InputNode::NODE_NAME)
            .expect("ip4-input is materialized before tuntap input initialization");
        let ip6_input = nodes
            .node_by_name(Ip6InputNode::NODE_NAME)
            .expect("ip6-input is materialized before tuntap input initialization");
        Self {
            drop: Self::slot(nodes, rx, drop),
            ip4_input: Self::slot(nodes, rx, ip4_input),
            ip6_input: Self::slot(nodes, rx, ip6_input),
        }
    }

    fn slot(nodes: &hammer_runtime::NodeMain, rx: NodeId, target: NodeId) -> u16 {
        nodes
            .node_next_slot_for_target(rx, target)
            .expect("tuntap sibling next lookup uses live nodes")
            .expect("IP owns the device-input next registration before tuntap initialization")
    }
}

struct TuntapThreadState {
    rx_buffers: Vec<u32>,
    iovecs: Vec<libc::iovec>,
}

// SAFETY: iovec pointers are created and consumed synchronously by the runtime
// thread that owns this state. The state is moved into TuntapMain only while
// its iovec vector is empty and never migrates after publication.
unsafe impl Send for TuntapThreadState {}

#[repr(C)]
struct TuntapThreadSlot {
    cacheline0: CacheLineAlignMark,
    state: RefCell<TuntapThreadState>,
}

struct TuntapMain {
    file: UnsafeCell<TuntapFile>,
    rx_next: OnceLock<TuntapRxNext>,
    threads: Box<[TuntapThreadSlot]>,
    provisioning_fd: OwnedFd,
    mtu_bytes: u32,
    hw_if_index: u32,
    sw_if_index: u32,
}

// SAFETY: every runtime thread permanently borrows only the slot selected by
// its immutable runtime thread index. File state and rx_next are changed only
// by thread zero during startup or after packet dispatch has stopped.
unsafe impl Sync for TuntapMain {}

static TUNTAP_MAIN: OnceLock<TuntapMain> = OnceLock::new();

impl TuntapMain {
    #[cfg(target_os = "linux")]
    fn init(config: TuntapConfig, data_plane: &mut DataPlaneMain) -> RuntimeResult<Self> {
        let descriptor = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .map_err(|source| TuntapConfigError::OpenDevNetTun { source })?;
        let control: OwnedFd = descriptor.into();
        let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
        for (destination, source) in request
            .ifr_name
            .iter_mut()
            .take(libc::IFNAMSIZ - 1)
            .zip(config.name.bytes())
        {
            *destination = source as libc::c_char;
        }
        request.ifr_ifru.ifru_flags = (libc::IFF_TUN | libc::IFF_NO_PI) as libc::c_short;
        if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETIFF, &mut request) } < 0 {
            return Err(TuntapConfigError::TunSetIff {
                source: io::Error::last_os_error(),
            }
            .into());
        }
        if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 1) } < 0 {
            return Err(TuntapConfigError::TunSetPersist {
                source: io::Error::last_os_error(),
            }
            .into());
        }
        let mut nonblocking = 1;
        if unsafe { libc::ioctl(control.as_raw_fd(), libc::FIONBIO, &mut nonblocking) } < 0 {
            let source = io::Error::last_os_error();
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            return Err(TuntapConfigError::SetNonblocking { source }.into());
        }

        let descriptor = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
        if descriptor < 0 {
            let source = io::Error::last_os_error();
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            return Err(TuntapConfigError::Socket { source }.into());
        }
        let provisioning = unsafe { OwnedFd::from_raw_fd(descriptor) };
        request.ifr_ifru.ifru_mtu = config.mtu as libc::c_int;
        if unsafe { libc::ioctl(provisioning.as_raw_fd(), libc::SIOCSIFMTU, &mut request) } < 0 {
            let source = io::Error::last_os_error();
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            return Err(TuntapConfigError::SetMtu { source }.into());
        }
        if unsafe { libc::ioctl(provisioning.as_raw_fd(), libc::SIOCGIFFLAGS, &mut request) } < 0 {
            let source = io::Error::last_os_error();
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            return Err(TuntapConfigError::GetInterfaceFlags { source }.into());
        }
        unsafe {
            request.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
        }
        if unsafe { libc::ioctl(provisioning.as_raw_fd(), libc::SIOCSIFFLAGS, &mut request) } < 0 {
            let source = io::Error::last_os_error();
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            return Err(TuntapConfigError::SetInterfaceFlags { source }.into());
        }

        let interfaces = NetMain::global()?.interface_main();
        let hw_if_index = interfaces.register_interface(
            data_plane,
            interfaces.device_class_index("tuntap"),
            0,
            interfaces.hw_class_index("tuntap"),
            0,
        );
        let sw_if_index = interfaces.hardware_interface(hw_if_index).sw_if_index();

        if let Err(startup_error) = interfaces.set_mtu(
            data_plane,
            sw_if_index,
            InterfaceMtu::new(config.mtu, config.mtu, config.mtu, config.mtu),
        ) {
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            interfaces.delete_hardware_interface(data_plane, hw_if_index);
            return Err(startup_error.into());
        }
        if let Err(startup_error) =
            interfaces.set_hardware_flags(data_plane, hw_if_index, HwInterfaceFlags::LINK_UP)
        {
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            interfaces.delete_hardware_interface(data_plane, hw_if_index);
            return Err(startup_error.into());
        }
        if config.admin_up
            && let Err(startup_error) =
                interfaces.set_software_flags(data_plane, sw_if_index, SwInterfaceFlags::ADMIN_UP)
        {
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            interfaces.delete_hardware_interface(data_plane, hw_if_index);
            return Err(startup_error.into());
        }

        let file = match register_tuntap_file(control) {
            Ok(file) => file,
            Err(startup_error) => {
                interfaces.delete_hardware_interface(data_plane, hw_if_index);
                return Err(startup_error);
            }
        };
        let thread_count = hammer_runtime::config::worker::worker_count() + 1;
        let threads = (0..thread_count)
            .map(|_| TuntapThreadSlot {
                cacheline0: CacheLineAlignMark,
                state: RefCell::new(TuntapThreadState {
                    rx_buffers: Vec::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY),
                    iovecs: Vec::new(),
                }),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let main = Self {
            file: UnsafeCell::new(file),
            rx_next: OnceLock::new(),
            threads,
            provisioning_fd: provisioning,
            mtu_bytes: config.mtu,
            hw_if_index,
            sw_if_index,
        };
        if let Some(address) = config.ip4_address
            && let Err(startup_error) = add_tuntap_ip4_address(data_plane, sw_if_index, address)
        {
            let TuntapFile::Active {
                control,
                file_index,
            } = main.file.into_inner()
            else {
                unreachable!("tuntap File is active before Main publication")
            };
            match FILE_MAIN
                .get()
                .expect("FileMain exists during tuntap startup cleanup")
                .delete(file_index)
            {
                Ok(deleted) => assert!(
                    deleted,
                    "published tuntap File index remains live during startup cleanup"
                ),
                Err(cleanup_error) => {
                    tracing::warn!(%cleanup_error, "tuntap File startup cleanup failed");
                }
            }
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            interfaces.delete_hardware_interface(data_plane, hw_if_index);
            drop(control);
            return Err(RuntimeError::from(startup_error));
        }
        Ok(main)
    }

    #[cfg(not(target_os = "linux"))]
    fn init(_: TuntapConfig, _: &mut DataPlaneMain) -> RuntimeResult<Self> {
        Err(TuntapConfigError::OpenDevNetTun {
            source: io::Error::new(io::ErrorKind::Unsupported, "Linux TUN is unavailable"),
        }
        .into())
    }

    fn file_index(&self) -> u32 {
        match unsafe { &*self.file.get() } {
            TuntapFile::Active { file_index, .. } => *file_index,
            TuntapFile::Closed => panic!("tuntap File remains active during graph dispatch"),
        }
    }

    fn rx_next(&self) -> TuntapRxNext {
        *self
            .rx_next
            .get()
            .expect("tuntap input next layout is published before graph dispatch")
    }

    fn thread(&self, runtime: &DataPlaneMain) -> RefMut<'_, TuntapThreadState> {
        self.threads[runtime.thread_index() as usize]
            .state
            .borrow_mut()
    }
}
fn register_tuntap_file(control: OwnedFd) -> RuntimeResult<TuntapFile> {
    let data =
        control
            .as_fd()
            .try_clone_to_owned()
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "duplicate tuntap descriptor",
                source,
            })?;
    let file = File::new(
        data,
        "vnet tuntap".to_owned(),
        0,
        FileFunctions {
            read: Some(tuntap_read_ready),
            ..FileFunctions::default()
        },
    );
    let file_index = match FILE_MAIN
        .get()
        .expect("FileMain exists before tuntap config")
        .add(file)
    {
        Ok(file_index) => file_index,
        Err(startup_error) => {
            #[cfg(target_os = "linux")]
            if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
            }
            return Err(startup_error);
        }
    };
    Ok(TuntapFile::Active {
        control,
        file_index,
    })
}

fn tuntap_read_ready(graph: &mut hammer_runtime::NodeMain, _: &mut File) -> RuntimeResult<()> {
    let node = graph
        .node_by_name(TuntapRxNode::NODE_NAME)
        .expect("tuntap-rx is materialized before File polling begins");
    graph.mark_interrupt_pending(node)?;
    Ok(())
}

fn add_tuntap_ip4_address(
    data_plane: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Net,
) -> Result<(), TuntapConfigError> {
    hammer_plugin_ip::ip4_add_del_interface_address(
        data_plane,
        sw_if_index,
        address.addr(),
        address.prefix_len(),
        false,
    )
    .map_err(|source| TuntapConfigError::Ip4Address {
        sw_if_index,
        address,
        source,
    })
}

#[hammer_component_macros::graph_node(
    graph = tuntap,
    init = register_tuntap_rx,
    role = driver,
    name = "tuntap-rx",
    sibling_of = hammer_service::device::DeviceInputNode,
)]
struct TuntapRxNode;

fn register_tuntap_rx(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_driver(TuntapRxNode::new())?;
    runtime.nodes().set_node_state(node, NodeState::Interrupt)?;
    Ok(node)
}

impl Node for TuntapRxNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        tuntap_rx(runtime, node_runtime, frame)
    }
}

fn tuntap_rx(runtime: &mut DataPlaneMain, node_runtime: &mut NodeRuntime, _: &mut Frame) -> usize {
    let main = TUNTAP_MAIN
        .get()
        .expect("tuntap main exists before graph dispatch");
    let mut state = main.thread(runtime);
    if state.rx_buffers.len() < DEFAULT_BUFFER_FRAME_CAPACITY / 2 {
        let mut allocated = [0_u32; DEFAULT_BUFFER_FRAME_CAPACITY];
        let requested = DEFAULT_BUFFER_FRAME_CAPACITY - state.rx_buffers.len();
        let count = runtime.buffer_alloc(&mut allocated[..requested]);
        state.rx_buffers.extend_from_slice(&allocated[..count]);
    }

    state.iovecs.clear();
    let cache_len = state.rx_buffers.len();
    let mut capacity = 0usize;
    while state.iovecs.len() < cache_len && capacity < main.mtu_bytes as usize {
        let offset = state.iovecs.len();
        let index = state.rx_buffers[cache_len - 1 - offset];
        runtime.buffer_chain_init(index);
        let buffer = runtime.buffer_mut(index);
        let segment_capacity = buffer.space_left_at_end().min(u16::MAX as usize);
        let segment = buffer.put_uninit(segment_capacity as u16);
        state.iovecs.push(libc::iovec {
            iov_base: segment.as_mut_ptr().cast(),
            iov_len: segment.len(),
        });
        capacity += segment.len();
    }
    if capacity < main.mtu_bytes as usize {
        for offset in 0..state.iovecs.len() {
            runtime.buffer_chain_init(state.rx_buffers[cache_len - 1 - offset]);
        }
        state.iovecs.clear();
        return 0;
    }

    let read = unsafe {
        FILE_MAIN
            .get()
            .expect("FileMain exists before graph dispatch")
            .readv(main.file_index(), &mut state.iovecs)
    };
    let bytes = match read {
        Ok(Some(bytes)) if bytes != 0 => bytes,
        Ok(Some(_)) | Ok(None) | Err(RuntimeError::FileRead { .. }) => {
            for offset in 0..state.iovecs.len() {
                runtime.buffer_chain_init(state.rx_buffers[cache_len - 1 - offset]);
            }
            state.iovecs.clear();
            return 0;
        }
        Err(RuntimeError::FileIndexInvalid { .. }) => {
            for offset in 0..state.iovecs.len() {
                runtime.buffer_chain_init(state.rx_buffers[cache_len - 1 - offset]);
            }
            state.iovecs.clear();
            panic!("published tuntap File index must remain live during graph dispatch")
        }
        Err(_) => panic!("FileMain::readv returned an undocumented error category"),
    };

    let prepared = state.iovecs.len();
    for offset in 0..prepared {
        runtime.buffer_chain_init(state.rx_buffers[cache_len - 1 - offset]);
    }
    let mut chain = [0_u32; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut lengths = [0_usize; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut remaining = bytes;
    let mut used = 0usize;
    while remaining != 0 {
        chain[used] = state.rx_buffers[cache_len - 1 - used];
        lengths[used] = remaining.min(state.iovecs[used].iov_len);
        remaining -= lengths[used];
        used += 1;
    }
    for pair in chain[..used].windows(2) {
        runtime.buffer_chain_buffer(pair[0], pair[1]);
    }
    for position in 0..used {
        runtime
            .buffer_mut(chain[position])
            .put_uninit(lengths[position] as u16);
    }
    let head = chain[0];
    runtime
        .buffer_mut(head)
        .set_total_len_not_including_first(bytes - lengths[0])
        .expect("TUN packet length fits Buffer chain metadata");
    state.iovecs.clear();
    state.rx_buffers.truncate(cache_len - used);

    let mut network = NetworkOpaque::default();
    network.sw_if_index[0] = main.sw_if_index;
    network.l3_hdr_offset = 0;
    *hammer_core::buffer_opaque!(mut runtime.buffer_mut(head) => NetworkOpaque) = network;

    let next = main.rx_next();
    let version = runtime.buffer(head).current()[0] >> 4;
    let mut next_index = match version {
        4 => next.ip4_input,
        6 => next.ip6_input,
        _ => next.drop,
    };
    let admin_up = NetMain::global()
        .expect("network Main exists before tuntap RX")
        .interface_main()
        .software_interface(main.sw_if_index)
        .expect("tuntap RX interface remains live")
        .is_admin_up();
    if !admin_up {
        next_index = next.drop;
    }
    next_index = FeatureMain::global()
        .expect("Feature Main exists before tuntap RX")
        .start_device_input(main.sw_if_index, runtime.buffer_mut(head), next_index);

    let vectors_left = {
        let (vectors, _) = runtime.get_next_frame::<u32, ()>(node_runtime, u32::from(next_index));
        vectors[0] = head;
        vectors.len() - 1
    };
    runtime.put_next_frame(node_runtime, u32::from(next_index), vectors_left);
    1
}

#[hammer_component_macros::graph_node(
    graph = tuntap,
    kind = internal,
    name = "tuntap-tx",
)]
struct TuntapTxNode;

impl Node for TuntapTxNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        tuntap_tx(runtime, node_runtime, frame)
    }
}

fn tuntap_tx(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
    let main = TUNTAP_MAIN
        .get()
        .expect("tuntap main exists before graph dispatch");
    let packet_count = frame.vector_args().len();
    let mut state = main.thread(runtime);
    for &head in frame.vector_args() {
        state.iovecs.clear();
        let mut packet_bytes = 0usize;
        let mut segment_index = Some(head);
        while let Some(index) = segment_index {
            let segment = runtime.buffer(index);
            let bytes = segment.current();
            state.iovecs.push(libc::iovec {
                iov_base: bytes.as_ptr().cast_mut().cast(),
                iov_len: bytes.len(),
            });
            packet_bytes += bytes.len();
            segment_index = segment.next_buffer_slot();
        }
        let write = unsafe {
            FILE_MAIN
                .get()
                .expect("FileMain exists before graph dispatch")
                .writev(main.file_index(), &state.iovecs)
        };
        match write {
            Ok(Some(written)) if written == packet_bytes => {}
            Ok(Some(_)) | Ok(None) | Err(RuntimeError::FileWrite { .. }) => {}
            Err(RuntimeError::FileIndexInvalid { .. }) => {
                panic!("published tuntap File index must remain live during graph dispatch")
            }
            Err(_) => panic!("FileMain::writev returned an undocumented error category"),
        }
        state.iovecs.clear();
    }
    runtime.buffer_free(frame.vector_args());
    packet_count
}

fn tuntap_intfc_tx(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    tuntap_tx(runtime, node_runtime, frame)
}

#[hammer_component_macros::main_loop_enter_function(
    name = "tuntap_input_init",
    runs_after = ["ip_feature_init"],
    runs_before = ["feature_arc_init"]
)]
fn tuntap_input_init(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let Some(tuntap) = TUNTAP_MAIN.get() else {
        return Ok(());
    };
    assert!(
        tuntap
            .rx_next
            .set(TuntapRxNext::resolve(main.nodes()))
            .is_ok(),
        "tuntap input next layout is published once"
    );
    Ok(())
}

#[hammer_component_macros::runtime_error(subsystem = "tuntap")]
#[derive(Debug, thiserror::Error)]
enum TuntapConfigError {
    #[error("open /dev/net/tun")]
    OpenDevNetTun {
        #[source]
        source: io::Error,
    },
    #[error("ioctl TUNSETIFF")]
    TunSetIff {
        #[source]
        source: io::Error,
    },
    #[error("ioctl TUNSETPERSIST")]
    TunSetPersist {
        #[source]
        source: io::Error,
    },
    #[error("open TUN provisioning socket")]
    Socket {
        #[source]
        source: io::Error,
    },
    #[error("ioctl FIONBIO")]
    SetNonblocking {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCSIFMTU")]
    SetMtu {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCGIFFLAGS")]
    GetInterfaceFlags {
        #[source]
        source: io::Error,
    },
    #[error("ioctl SIOCSIFFLAGS")]
    SetInterfaceFlags {
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
}

#[hammer_component_macros::config_function(name = "tuntap_config", section = "plugin.tuntap")]
fn tuntap_config(config: TuntapConfig, data_plane: &mut DataPlaneMain) -> RuntimeResult<()> {
    if !config.enabled {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    if unsafe { libc::geteuid() } != 0 {
        tracing::warn!("tuntap disabled: must be superuser");
        return Ok(());
    }
    let main = TuntapMain::init(config, data_plane)?;
    assert!(
        TUNTAP_MAIN.set(main).is_ok(),
        "tuntap config callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::main_loop_exit_function(name = "tuntap_exit")]
fn tuntap_exit(data_plane: &mut DataPlaneMain) -> RuntimeResult<()> {
    let Some(main) = TUNTAP_MAIN.get() else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    {
        let file = unsafe { &mut *main.file.get() };
        let active = std::mem::replace(file, TuntapFile::Closed);
        let TuntapFile::Active {
            control,
            file_index,
        } = active
        else {
            return Ok(());
        };

        let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
        if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNGETIFF, &mut request) } < 0 {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap interface identity cleanup failed");
        } else if unsafe {
            libc::ioctl(
                main.provisioning_fd.as_raw_fd(),
                libc::SIOCGIFFLAGS,
                &mut request,
            )
        } < 0
        {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap host flags cleanup failed");
        } else {
            unsafe {
                request.ifr_ifru.ifru_flags &=
                    !((libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short);
            }
            if unsafe {
                libc::ioctl(
                    main.provisioning_fd.as_raw_fd(),
                    libc::SIOCSIFFLAGS,
                    &mut request,
                )
            } < 0
            {
                tracing::warn!(source = %io::Error::last_os_error(), "tuntap host state cleanup failed");
            }
        }
        if unsafe { libc::ioctl(control.as_raw_fd(), libc::TUNSETPERSIST, 0) } < 0 {
            tracing::warn!(source = %io::Error::last_os_error(), "tuntap persistence cleanup failed");
        }

        let file_delete_error = match FILE_MAIN
            .get()
            .expect("FileMain exists during tuntap shutdown")
            .delete(file_index)
        {
            Ok(deleted) => {
                assert!(
                    deleted,
                    "published tuntap File index remains live until shutdown"
                );
                None
            }
            Err(error) => Some(error),
        };
        let rx_buffers = {
            let mut state = main.thread(data_plane);
            state.iovecs.clear();
            std::mem::take(&mut state.rx_buffers)
        };
        data_plane.buffer_free_no_next(&rx_buffers);
        drop(control);
        NetMain::global()
            .expect("network Main exists during tuntap shutdown")
            .interface_main()
            .delete_hardware_interface(data_plane, main.hw_if_index);
        if let Some(error) = file_delete_error {
            return Err(error);
        }
    }
    Ok(())
}

hammer_component_macros::declare_plugin!(
    name = "tuntap",
    load_after = ["ip"],
    init_functions = [],
    config_functions = [__CONFIG_FN_TUNTAP_CONFIG],
    main_loop_enter_functions = [__INIT_FN_TUNTAP_INPUT_INIT],
    main_loop_exit_functions = [__INIT_FN_TUNTAP_EXIT],
    worker_init_functions = [],
    graph_nodes = [
        __TUNTAP_GRAPH_NODE_TUNTAP_RX_NODE,
        __TUNTAP_GRAPH_NODE_TUNTAP_TX_NODE,
    ],
    node_functions = [],
    process_nodes = [],
    binary_api_methods = [],
);
