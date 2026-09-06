use crate::protocol::{IcmpBuildError, IcmpHeader, build_echo_reply};
use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext};
use hammer_plugin_ip::ip::ip_header;
use hammer_plugin_ip::protocol::icmp::IcmpErrorMetadata;
use hammer_plugin_ip::protocol::ip::{IpProtocol, IpVersion};
use hammer_plugin_ip::protocol::wire::read_header;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneMain, Node, NodeProcessFn, NodeRuntimeData, TraceFormatter, add_packet_trace,
    format_packet_trace,
};

use hammer_service::data_plane::set_index_node_error;
use hammer_service::opaque::NetworkOpaque;

#[hammer_component_macros::runtime_error(subsystem = "icmp")]
#[derive(Debug, thiserror::Error)]
enum IcmpControlError {
    #[error("ICMP type registration requires an attached input consumer")]
    ConsumerNotAttached,
}

const ICMP_HEADER_MIN_LEN: usize = 4;
const ICMP_ECHO_HEADER_LEN: usize = 8;
const ICMP4_ECHO_REQUEST: u8 = 8;
const ICMP6_ECHO_REQUEST: u8 = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum IcmpInputError {
    BadLength,
    WrongProtocol,
    UnknownType,
    BadCode,
    TooShort,
    HopLimit,
}

impl hammer_runtime::node::NodeErrorCode for IcmpInputError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl IcmpInputError {
    const DESCRIPTORS: [hammer_runtime::node::NodeErrorDescriptor; 6] = {
        use hammer_runtime::node::{NodeErrorDescriptor, NodeErrorSeverity};
        [
            NodeErrorDescriptor::new(
                "bad-length",
                NodeErrorSeverity::Error,
                "Invalid ICMP length",
            ),
            NodeErrorDescriptor::new(
                "wrong-protocol",
                NodeErrorSeverity::Error,
                "Not an ICMP message",
            ),
            NodeErrorDescriptor::new(
                "unknown-type",
                NodeErrorSeverity::Warn,
                "No consumer for ICMP type",
            ),
            NodeErrorDescriptor::new(
                "bad-code",
                NodeErrorSeverity::Error,
                "Invalid code for ICMP type",
            ),
            NodeErrorDescriptor::new(
                "too-short",
                NodeErrorSeverity::Error,
                "ICMP message too short for type",
            ),
            NodeErrorDescriptor::new(
                "hop-limit",
                NodeErrorSeverity::Error,
                "Invalid hop limit for ICMP type",
            ),
        ]
    };
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum IcmpNodeError {
    BadLength,
    WrongProtocol,
    WrongType,
}

impl hammer_runtime::node::NodeErrorCode for IcmpNodeError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IcmpInputTrace {
    pub version: Option<IpVersion>,
    pub icmp_type: Option<u8>,
    pub code: Option<u8>,
    pub error: Option<u16>,
    pub next: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IcmpEchoRequestTrace {
    pub packet_len: Option<usize>,
    pub error: Option<u16>,
    pub next: u16,
}

impl IcmpNodeError {
    const DESCRIPTORS: [hammer_runtime::node::NodeErrorDescriptor; 3] = {
        use hammer_runtime::node::{NodeErrorDescriptor, NodeErrorSeverity};
        [
            NodeErrorDescriptor::new(
                "bad-length",
                NodeErrorSeverity::Error,
                "Incomplete echo header",
            ),
            NodeErrorDescriptor::new(
                "wrong-protocol",
                NodeErrorSeverity::Error,
                "Not an ICMP message",
            ),
            NodeErrorDescriptor::new(
                "wrong-type",
                NodeErrorSeverity::Error,
                "Not an echo request",
            ),
        ]
    };
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

impl From<IcmpBuildError> for IcmpNodeError {
    #[inline(always)]
    fn from(error: IcmpBuildError) -> Self {
        match error {
            IcmpBuildError::BadLength => Self::BadLength,
            IcmpBuildError::WrongProtocol => Self::WrongProtocol,
            IcmpBuildError::WrongType => Self::WrongType,
        }
    }
}

#[hammer_component_macros::node_next]
pub enum Icmp4InputNext {
    #[next("ip4-drop")]
    Drop,
    #[next("ip4-punt")]
    Punt,
}

#[hammer_component_macros::node_next]
pub enum Icmp6InputNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-punt")]
    Punt,
}

#[derive(Debug, Clone)]
pub(crate) struct IcmpInputTable {
    entries: [IcmpInputEntry; 256],
}

impl IcmpInputTable {
    #[inline]
    pub(crate) fn new(default_next: u16) -> Self {
        Self {
            entries: [IcmpInputEntry::new(default_next); 256],
        }
    }

    #[inline(always)]
    fn register_type(&mut self, icmp_type: u8, next: u16) {
        let entry = &mut self.entries[icmp_type as usize];
        entry.next = next;
        entry.registered = true;
    }
}

#[derive(Debug, Clone, Copy)]
struct IcmpInputEntry {
    next: u16,
    spec: IcmpTypeSpec,
    registered: bool,
}

impl IcmpInputEntry {
    #[inline]
    fn new(default_next: u16) -> Self {
        Self {
            next: default_next,
            spec: IcmpTypeSpec::default(),
            registered: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IcmpTypeSpec {
    max_code: u8,
    min_len: u8,
    min_hop_limit: u8,
}

impl Default for IcmpTypeSpec {
    #[inline]
    fn default() -> Self {
        Self {
            max_code: u8::MAX,
            min_len: ICMP_HEADER_MIN_LEN as u8,
            min_hop_limit: 0,
        }
    }
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_icmp4_input,
    role = internal,
    name = "icmp4-input",
    next = Icmp4InputNext,
)]
pub struct Icmp4InputNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

fn register_icmp4_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = crate::IcmpMain::global()?;
    let node = runtime.nodes().try_register_internal_with_next_names(
        Icmp4InputNode::new(),
        &Icmp4InputNext::NEXT_NAMES,
    )?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IcmpInputError::DESCRIPTORS)?;
    hammer_plugin_ip::register_ip4_protocol(runtime.nodes(), IpProtocol::Icmpv4.into(), node)?;
    main.ip4_input_node
        .set(node)
        .expect("ICMP input node installed once");
    Ok(node)
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_icmp6_input,
    role = internal,
    name = "icmp6-input",
    next = Icmp6InputNext,
)]
pub struct Icmp6InputNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

fn register_icmp6_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = crate::IcmpMain::global()?;
    let node = runtime.nodes().try_register_internal_with_next_names(
        Icmp6InputNode::new(),
        &Icmp6InputNext::NEXT_NAMES,
    )?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IcmpInputError::DESCRIPTORS)?;
    hammer_plugin_ip::register_ip6_protocol(runtime.nodes(), IpProtocol::Icmpv6.into(), node)?;
    // SAFETY: initialization runs on main before publication; later changes
    // use the same main-thread barrier precondition in register_type.
    unsafe {
        let ip6 = &mut *main.ip6.get();
        for entry in &mut ip6.entries {
            entry.spec.max_code = 0;
        }
        for (icmp_type, max_code) in [(1, 6), (3, 1), (4, 3), (138, 1), (139, 2), (140, 2)] {
            ip6.entries[icmp_type].spec.max_code = max_code;
        }
        for (icmp_type, min_len) in [(133, 8), (134, 16), (135, 24), (136, 24), (137, 40)] {
            ip6.entries[icmp_type].spec.min_hop_limit = 255;
            ip6.entries[icmp_type].spec.min_len = min_len;
        }
    }
    main.ip6_input_node
        .set(node)
        .expect("ICMP input node installed once");
    Ok(node)
}

impl crate::IcmpMain {
    fn register_type(
        &self,
        nodes: &hammer_runtime::node::NodeRuntime,
        version: IpVersion,
        icmp_type: u8,
        node: NodeId,
    ) -> RuntimeResult<u16> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let consumer = *match version {
            IpVersion::V4 => self.ip4_input_node.get(),
            IpVersion::V6 => self.ip6_input_node.get(),
        }
        .ok_or(IcmpControlError::ConsumerNotAttached)?;
        let slot = nodes.add_node_next_slot(consumer, node)?;
        // SAFETY: the caller holds the publication scope; all fallible graph
        // work completed before the scalar dispatch entry changes.
        unsafe {
            match version {
                IpVersion::V4 => (*self.ip4.get()).register_type(icmp_type, slot),
                IpVersion::V6 => (*self.ip6.get()).register_type(icmp_type, slot),
            }
        }
        Ok(slot)
    }
}

impl Node for Icmp4InputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        icmp_input_process(runtime, self.runtime_data, frame, IpVersion::V4)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IcmpInputTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        |runtime, data, frame| icmp_input_process(runtime, data, frame, IpVersion::V4)
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl Node for Icmp6InputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        icmp_input_process(runtime, self.runtime_data, frame, IpVersion::V6)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IcmpInputTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        |runtime, data, frame| icmp_input_process(runtime, data, frame, IpVersion::V6)
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

#[hammer_component_macros::node_next]
pub enum Icmp4EchoRequestNext {
    #[next("ip4-lookup")]
    Lookup,
    #[next("drop")]
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_icmp4_echo_request,
    role = internal,
    name = "icmp4-echo-request",
    next = Icmp4EchoRequestNext,
)]
pub struct Icmp4EchoRequestNode;

fn register_icmp4_echo_request(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Icmp4EchoRequestNode::new(),
        &Icmp4EchoRequestNext::NEXT_NAMES,
    )?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IcmpNodeError::DESCRIPTORS)?;
    let main = crate::IcmpMain::global()?;
    main.register_type(runtime.nodes(), IpVersion::V4, ICMP4_ECHO_REQUEST, node)?;
    Ok(node)
}

impl Node for Icmp4EchoRequestNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        icmp_echo_request_process_frame(runtime, frame, IpVersion::V4)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IcmpEchoRequestTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| icmp_echo_request_process_frame(runtime, frame, IpVersion::V4)
    }
}

#[hammer_component_macros::node_next]
pub enum Icmp6EchoRequestNext {
    #[next("ip6-lookup")]
    Lookup,
    #[next("drop")]
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_icmp6_echo_request,
    role = internal,
    name = "icmp6-echo-request",
    next = Icmp6EchoRequestNext,
)]
pub struct Icmp6EchoRequestNode;

fn register_icmp6_echo_request(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Icmp6EchoRequestNode::new(),
        &Icmp6EchoRequestNext::NEXT_NAMES,
    )?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IcmpNodeError::DESCRIPTORS)?;
    let main = crate::IcmpMain::global()?;
    main.register_type(runtime.nodes(), IpVersion::V6, ICMP6_ECHO_REQUEST, node)?;
    Ok(node)
}

impl Node for Icmp6EchoRequestNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        icmp_echo_request_process_frame(runtime, frame, IpVersion::V6)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IcmpEchoRequestTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| icmp_echo_request_process_frame(runtime, frame, IpVersion::V6)
    }
}

fn icmp_input_process(
    runtime: &DataPlaneMain,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
    version: IpVersion,
) -> () {
    let drop_slot = match version {
        IpVersion::V4 => NodeNext::slot(Icmp4InputNext::Drop),
        IpVersion::V6 => NodeNext::slot(Icmp6InputNext::Drop),
    };
    let mut nexts = [0; hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY];
    for (index, slot) in frame.indices().iter().zip(&mut nexts) {
        *slot = match next_slot_for_index(runtime, *index, version) {
            Ok(slot) => slot,
            Err(_) => drop_slot,
        };
    }
    runtime.enqueue_to_next(frame, &nexts[..frame.len()]);
    ()
}

fn icmp_echo_request_process_frame(
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    version: IpVersion,
) -> () {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match next_for_echo_request_index(runtime, index, version) {
            Ok(next) => next,
            Err(_) => match version {
                IpVersion::V4 => NodeNext::slot(Icmp4EchoRequestNext::Drop),
                IpVersion::V6 => NodeNext::slot(Icmp6EchoRequestNext::Drop),
            },
        }
    })
}

#[inline(always)]
fn next_slot_for_index(
    runtime: &DataPlaneMain,
    index: Index,
    version: IpVersion,
) -> RuntimeResult<u16> {
    let main = crate::IcmpMain::global()?;
    let buffer = runtime.get_buffer(index)?;
    let current = buffer.current();
    // SAFETY: IP local initializes the network overlay before ICMP dispatch.
    let network = unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) };
    let default_next = match version {
        IpVersion::V4 => NodeNext::slot(Icmp4InputNext::Drop),
        IpVersion::V6 => NodeNext::slot(Icmp6InputNext::Punt),
    };
    let mut trace = IcmpInputTrace {
        version: None,
        icmp_type: None,
        code: None,
        error: None,
        next: default_next,
    };
    let selected = (|| {
        let parsed =
            ip_header(current, network.packet_cursor()).map_err(|_| IcmpInputError::BadLength)?;
        trace.version = Some(parsed.version);
        if parsed.version != version
            || !matches!(
                (parsed.version, parsed.protocol),
                (IpVersion::V4, IpProtocol::Icmpv4) | (IpVersion::V6, IpProtocol::Icmpv6)
            )
        {
            return Err(IcmpInputError::WrongProtocol);
        }
        let header = read_header::<IcmpHeader>(current, parsed.transport_header_offset)
            .map_err(|_| IcmpInputError::BadLength)?;
        let icmp_type = header.icmp_type();
        trace.icmp_type = Some(icmp_type);
        trace.code = Some(header.code());
        // SAFETY: only the main-thread publication scope writes these tables.
        // Copy one entry; no table reference escapes into graph or trace calls.
        let entry = unsafe {
            match parsed.version {
                IpVersion::V4 => (*main.ip4.get()).entries[usize::from(icmp_type)],
                IpVersion::V6 => (*main.ip6.get()).entries[usize::from(icmp_type)],
            }
        };
        let mut error = (!entry.registered).then_some(IcmpInputError::UnknownType);
        if parsed.version == IpVersion::V6 {
            if header.code() > entry.spec.max_code {
                error = Some(IcmpInputError::BadCode);
            }
            if current[parsed.network_header_offset + 7] < entry.spec.min_hop_limit {
                error = Some(IcmpInputError::HopLimit);
            }
            if parsed
                .packet_len
                .saturating_sub(parsed.transport_header_offset)
                < usize::from(entry.spec.min_len)
            {
                error = Some(IcmpInputError::TooShort);
            }
        }
        match error {
            Some(error) => Err(error),
            None => Ok(entry.next),
        }
    })();
    drop(buffer);
    match selected {
        Ok(next) => {
            runtime.get_buffer_mut(index)?.clear_node_error();
            trace.next = next;
        }
        Err(error) => {
            set_index_node_error(runtime, index, error)?;
            trace.error = Some(error.code());
            if matches!(error, IcmpInputError::UnknownType) {
                trace.next = match version {
                    IpVersion::V4 => NodeNext::slot(Icmp4InputNext::Punt),
                    IpVersion::V6 => NodeNext::slot(Icmp6InputNext::Punt),
                };
            }
        }
    }
    let next = trace.next;
    add_packet_trace!(runtime, index, trace)?;
    Ok(next)
}

#[inline(always)]
fn next_for_echo_request_index(
    runtime: &DataPlaneMain,
    index: Index,
    version: IpVersion,
) -> RuntimeResult<u16> {
    let net = hammer_service::net::NetMain::global()?;
    let mut buffer = runtime.get_buffer_mut(index)?;
    // SAFETY: IP local initialized the packet's network overlay before dispatch.
    let network = unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) };
    let parsed =
        ip_header(buffer.current(), network.packet_cursor()).map_err(|_| IcmpBuildError::BadLength);
    let reply = parsed.and_then(|parsed| {
        if parsed.version != version {
            return Err(IcmpBuildError::WrongProtocol);
        }
        build_echo_reply(buffer.current_mut(), &parsed)?;
        Ok(parsed)
    });
    match reply {
        Ok(parsed) => {
            let next = match parsed.version {
                IpVersion::V4 => NodeNext::slot(Icmp4EchoRequestNext::Lookup),
                IpVersion::V6 => NodeNext::slot(Icmp6EchoRequestNext::Lookup),
            };
            buffer.clear_node_error();
            IcmpErrorMetadata::clear(buffer.opaque2_mut());
            // SAFETY: same initialized overlay; the packet remains owned by this frame.
            let network = unsafe { &mut *(buffer.opaque_mut() as *mut _ as *mut NetworkOpaque) };
            if parsed.version == IpVersion::V4 {
                network.sw_if_index[0] = net.local_interface_sw_index();
            }
            if let (std::net::IpAddr::V6(source), std::net::IpAddr::V6(destination)) =
                (parsed.source, parsed.destination)
            {
                if destination.is_unicast_link_local() && !source.is_unicast_link_local() {
                    // The reply targets the original global source. Lookup must
                    // select the RX interface's FIB, not the request's LL table.
                    network.ip_mut().set_fib_index(None);
                    network.ip_mut().set_fib_index_override(None);
                }
            }
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(parsed.packet_len)
                    .with_network_header(parsed.network_header_offset, parsed.network_header_len)
                    .with_transport_header(parsed.transport_header_offset, ICMP_ECHO_HEADER_LEN)
                    .with_transport_payload_offset(
                        parsed.transport_header_offset + ICMP_ECHO_HEADER_LEN,
                    ),
            );
            drop(buffer);
            add_packet_trace!(
                runtime,
                index,
                IcmpEchoRequestTrace {
                    packet_len: Some(parsed.packet_len),
                    error: None,
                    next,
                },
            )?;
            Ok(next)
        }
        Err(error) => {
            drop(buffer);
            let error = IcmpNodeError::from(error);
            set_index_node_error(runtime, index, error)?;
            let next = match version {
                IpVersion::V4 => NodeNext::slot(Icmp4EchoRequestNext::Drop),
                IpVersion::V6 => NodeNext::slot(Icmp6EchoRequestNext::Drop),
            };
            add_packet_trace!(
                runtime,
                index,
                IcmpEchoRequestTrace {
                    packet_len: None,
                    error: Some(error.code()),
                    next,
                },
            )?;
            Ok(next)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_runtime::{DataPlaneBufferConfig, GlobalMain, RuntimeRegistry};

    #[test]
    fn input_dispatch_preserves_protocol_specific_validation() -> RuntimeResult<()> {
        hammer_runtime::config::Memory::default().ensure_main_heap()?;
        let mut main = GlobalMain::new(
            DataPlaneMain::new(DataPlaneBufferConfig::default()),
            RuntimeRegistry::new(),
        );
        main.init_control()?;
        main.install_current();
        main.plugin_main_mut()
            .register_builtin_image(hammer_service::registration_image());
        main.plugin_main_mut()
            .register_builtin_image(hammer_plugin_ip::plugin_module().registration_image().get());
        main.plugin_main_mut()
            .register_builtin_image(crate::plugin_module().registration_image().get());
        main.configure_early(&format!(
            "[stats]\nsocket_path = '/tmp/hammer-icmp-input-{}.sock'\n",
            std::process::id()
        ))?;
        hammer_runtime::init::run_init_functions(&mut main)?;
        let runtime = main.data_plane_main();

        // icmp6.c::icmp6_input applies code, hop-limit, then minimum-length
        // validation. Every classified error uses punt, including registered
        // types; a valid registered echo request uses its installed graph edge.
        // icmp4.c dispatches by type only; ping.c preserves the code while
        // replying. The same type number must not leak across IP tables.
        for (version, icmp_type, code, hop_limit, length, error) in [
            (IpVersion::V4, 8, 0, 64, 8, None),
            (IpVersion::V4, 8, 1, 64, 8, None),
            (
                IpVersion::V4,
                128,
                0,
                64,
                8,
                Some(IcmpInputError::UnknownType),
            ),
            (
                IpVersion::V6,
                8,
                0,
                64,
                8,
                Some(IcmpInputError::UnknownType),
            ),
            (IpVersion::V6, 128, 0, 64, 8, None),
            (IpVersion::V6, 128, 1, 64, 8, Some(IcmpInputError::BadCode)),
            (
                IpVersion::V6,
                135,
                0,
                64,
                24,
                Some(IcmpInputError::HopLimit),
            ),
            (IpVersion::V6, 135, 1, 64, 4, Some(IcmpInputError::TooShort)),
            (
                IpVersion::V6,
                200,
                0,
                64,
                4,
                Some(IcmpInputError::UnknownType),
            ),
        ] {
            let (input, punt, echo, header_len) = match version {
                IpVersion::V4 => ("icmp4-input", "ip4-punt", "icmp4-echo-request", 20),
                IpVersion::V6 => ("icmp6-input", "ip6-punt", "icmp6-echo-request", 40),
            };
            let input = runtime.node_by_name(input).unwrap();
            let punt = runtime.node_by_name(punt).unwrap();
            let echo = runtime.node_by_name(echo).unwrap();
            let mut packet = [0; 64];
            let packet_len = header_len + length;
            match version {
                IpVersion::V4 => {
                    packet[0] = 0x45;
                    packet[2..4].copy_from_slice(&(packet_len as u16).to_be_bytes());
                    packet[8] = hop_limit;
                    packet[9] = IpProtocol::Icmpv4.into();
                    packet[12..16].copy_from_slice(&[192, 0, 2, 2]);
                    packet[16..20].copy_from_slice(&[192, 0, 2, 1]);
                }
                IpVersion::V6 => {
                    packet[0] = 0x60;
                    packet[4..6].copy_from_slice(&(length as u16).to_be_bytes());
                    packet[6] = IpProtocol::Icmpv6.into();
                    packet[7] = hop_limit;
                    packet[8..12].copy_from_slice(&[0x20, 1, 0x0d, 0xb8]);
                    packet[23] = 2;
                    packet[24..28].copy_from_slice(&[0x20, 1, 0x0d, 0xb8]);
                    packet[39] = 1;
                }
            }
            packet[header_len] = icmp_type;
            packet[header_len + 1] = code;
            if version == IpVersion::V4 {
                let checksum =
                    hammer_infra::checksum::internet_checksum(&packet[header_len..packet_len]);
                packet[header_len + 2..header_len + 4].copy_from_slice(&checksum.to_be_bytes());
                let checksum = hammer_infra::checksum::internet_checksum(&packet[..header_len]);
                packet[10..12].copy_from_slice(&checksum.to_be_bytes());
            }
            let mut frame = runtime.buffers().get_next_frame(input)?;
            let index = runtime
                .buffers()
                .alloc_index_with_bytes(&packet[..packet_len])?;
            frame.push_index(index)?;
            {
                let mut buffer = runtime.get_buffer_mut(index)?;
                // SAFETY: this fixture installs the same initialized service
                // overlay that IP local supplies at the ICMP input boundary.
                let network =
                    unsafe { &mut *(buffer.opaque_mut() as *mut _ as *mut NetworkOpaque) };
                *network = NetworkOpaque::default();
                network.set_packet_cursor(
                    BufferPacketCursor::new()
                        .with_packet_len(packet_len)
                        .with_network_header(0, header_len)
                        .with_transport_header(header_len, 4)
                        .with_transport_payload_offset(header_len + 4),
                );
            }
            let next = runtime
                .with_current_node(input, || next_slot_for_index(runtime, index, version))?;
            assert_eq!(
                runtime.nodes().node_next_slot(input, usize::from(next))?,
                if error.is_some() { punt } else { echo },
            );
            let expected_error = error
                .map(|error| {
                    runtime.with_current_node(input, || runtime.record_current_node_error(error))
                })
                .transpose()?;
            assert_eq!(
                runtime.get_buffer(index)?.node_error_index(),
                expected_error
            );
            if version == IpVersion::V4 && error.is_none() {
                let next = runtime.with_current_node(echo, || {
                    next_for_echo_request_index(runtime, index, version)
                })?;
                assert_eq!(
                    runtime.nodes().node_next_slot(echo, usize::from(next))?,
                    runtime.node_by_name("ip4-lookup").unwrap()
                );
                let buffer = runtime.get_buffer(index)?;
                let reply = buffer.current();
                assert_eq!(&reply[12..16], &packet[16..20]);
                assert_eq!(&reply[16..20], &packet[12..16]);
                assert_eq!(&reply[20..22], &[0, code]);
                assert_eq!(hammer_infra::checksum::internet_checksum(&reply[..20]), 0);
                assert_eq!(hammer_infra::checksum::internet_checksum(&reply[20..]), 0);
            }
        }
        assert_eq!(runtime.buffers().in_use_buffers(), 0);
        main.close()?;
        GlobalMain::uninstall_current();
        Ok(())
    }
}
