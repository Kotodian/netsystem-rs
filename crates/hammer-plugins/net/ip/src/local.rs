use std::hash::Hasher;
use std::mem::transmute;
use std::net::IpAddr;

use crate::protocol::ip::{Ipv4Header, Ipv6Header};
use crate::protocol::wire::read_header;
use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext};
use hammer_infra::checksum::InternetChecksum;
use hammer_runtime::{
    DataPlaneMain, Node, NodeProcessFn, TraceFormatter, add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::data_plane::set_index_node_error;
use hammer_service::net::{DpoType, NetMain};
use hammer_service::opaque::NetworkOpaque;

use super::{IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket, ip_header};

const TCP_HEADER_MIN_LEN: usize = 20;
const ICMP_HEADER_MIN_LEN: usize = 4;

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct IcmpHeader {
    icmp_type: u8,
    code: u8,
    checksum: [u8; 2],
}

#[hammer_component_macros::node_next]
pub enum Ip4LocalNext {
    #[next("ip4-drop")]
    Drop,
    #[next("ip4-punt")]
    Punt,
    #[next("ip4-reassembly")]
    Reassembly,
}

#[hammer_component_macros::node_next]
pub enum Ip6LocalNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-punt")]
    Punt,
    #[next("ip6-reassembly")]
    Reassembly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum IpLocalError {
    BadLength,
    BadTransportHeader,
    BadChecksum,
    UnknownProtocol,
    SourceLookupMiss,
    SpoofedLocalPacket,
}

impl hammer_runtime::node::NodeErrorCode for IpLocalError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IpLocalTraceStage {
    Head,
    Receive,
    End,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IpLocalTrace {
    pub stage: IpLocalTraceStage,
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub transport_header_len: usize,
    pub error: Option<u16>,
    pub next: u16,
}

impl IpLocalError {
    const DESCRIPTORS: [hammer_runtime::node::NodeErrorDescriptor; 6] = {
        use hammer_runtime::node::{NodeErrorDescriptor, NodeErrorSeverity};
        [
            NodeErrorDescriptor::new(
                "bad-length",
                NodeErrorSeverity::Error,
                "Invalid IP packet length",
            ),
            NodeErrorDescriptor::new(
                "bad-transport-header",
                NodeErrorSeverity::Error,
                "Invalid transport header",
            ),
            NodeErrorDescriptor::new("bad-checksum", NodeErrorSeverity::Error, "Invalid checksum"),
            NodeErrorDescriptor::new(
                "unknown-protocol",
                NodeErrorSeverity::Warn,
                "Unknown local protocol",
            ),
            NodeErrorDescriptor::new(
                "source-lookup-miss",
                NodeErrorSeverity::Error,
                "No accepting interface for source",
            ),
            NodeErrorDescriptor::new(
                "spoofed-local-packet",
                NodeErrorSeverity::Error,
                "Source is a local receive address",
            ),
        ]
    };

    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[hammer_component_macros::feature_arc(name = "ip4-local", start_nodes = [Ip4LocalNode, Ip4ReceiveNode], last_in_arc = Ip4LocalEndOfArcNode)]
#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_local, role = internal, name = "ip4-local", next = Ip4LocalNext)]
pub struct Ip4LocalNode;

fn register_ip4_local(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4LocalNode::new(), &Ip4LocalNext::NEXT_NAMES)?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IpLocalError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip4LocalNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_frame(runtime, frame, LocalStage::Head, IpVersion::V4)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| process_frame(runtime, frame, LocalStage::Head, IpVersion::V4)
    }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_receive, role = internal, name = "ip4-receive", sibling_of = Ip4LocalNode)]
pub struct Ip4ReceiveNode;

fn register_ip4_receive(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip4ReceiveNode::new())?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IpLocalError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip4ReceiveNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_frame(runtime, frame, LocalStage::Receive, IpVersion::V4)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| process_frame(runtime, frame, LocalStage::Receive, IpVersion::V4)
    }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }
}

#[hammer_component_macros::feature(arc = Ip4LocalNode)]
#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_local_end_of_arc, role = internal, name = "ip4-local-end-of-arc", sibling_of = Ip4LocalNode)]
pub struct Ip4LocalEndOfArcNode;

fn register_ip4_local_end_of_arc(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip4LocalEndOfArcNode::new())?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IpLocalError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip4LocalEndOfArcNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_frame(runtime, frame, LocalStage::End, IpVersion::V4)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| process_frame(runtime, frame, LocalStage::End, IpVersion::V4)
    }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }
}

pub(crate) fn register_ip4_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = crate::lookup::IP4_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    let local = nodes.node_by_name(Ip4LocalNode::NODE_NAME).ok_or(
        crate::ip::IpControlError::NodeRuntimeUnavailable {
            operation: crate::ip::IpControlOperation::IpProtocolRegistration,
        },
    )?;
    let next = nodes.add_node_next_slot(local, node)?;
    // SAFETY: startup or the caller's worker barrier excludes packet readers.
    unsafe {
        (*main.local_next_by_ip_protocol.get())[usize::from(protocol)] = next;
    }
    Ok(())
}

pub fn unregister_ip4_protocol(protocol: u8) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = crate::lookup::IP4_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    // SAFETY: restore the owner table before the consumer retires under the barrier.
    unsafe {
        (*main.local_next_by_ip_protocol.get())[usize::from(protocol)] =
            NodeNext::slot(Ip4LocalNext::Punt);
    }
    Ok(())
}

#[hammer_component_macros::feature_arc(name = "ip6-local", start_nodes = [Ip6LocalNode, Ip6ReceiveNode], last_in_arc = Ip6LocalEndOfArcNode)]
#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_local, role = internal, name = "ip6-local", next = Ip6LocalNext)]
pub struct Ip6LocalNode;

fn register_ip6_local(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6LocalNode::new(), &Ip6LocalNext::NEXT_NAMES)?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IpLocalError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip6LocalNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_frame(runtime, frame, LocalStage::Head, IpVersion::V6)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| process_frame(runtime, frame, LocalStage::Head, IpVersion::V6)
    }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_receive, role = internal, name = "ip6-receive", sibling_of = Ip6LocalNode)]
pub struct Ip6ReceiveNode;

fn register_ip6_receive(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip6ReceiveNode::new())?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IpLocalError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip6ReceiveNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_frame(runtime, frame, LocalStage::Receive, IpVersion::V6)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| process_frame(runtime, frame, LocalStage::Receive, IpVersion::V6)
    }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }
}

#[hammer_component_macros::feature(arc = Ip6LocalNode)]
#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_local_end_of_arc, role = internal, name = "ip6-local-end-of-arc", sibling_of = Ip6LocalNode)]
pub struct Ip6LocalEndOfArcNode;

fn register_ip6_local_end_of_arc(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip6LocalEndOfArcNode::new())?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IpLocalError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip6LocalEndOfArcNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_frame(runtime, frame, LocalStage::End, IpVersion::V6)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| process_frame(runtime, frame, LocalStage::End, IpVersion::V6)
    }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }
}

pub(crate) fn register_ip6_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = crate::lookup::IP6_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    let local = nodes.node_by_name(Ip6LocalNode::NODE_NAME).ok_or(
        crate::ip::IpControlError::NodeRuntimeUnavailable {
            operation: crate::ip::IpControlOperation::IpProtocolRegistration,
        },
    )?;
    let next = nodes.add_node_next_slot(local, node)?;
    // SAFETY: startup or the caller's worker barrier excludes packet readers.
    unsafe {
        (*main.local_next_by_ip_protocol.get())[usize::from(protocol)] = next;
    }
    Ok(())
}

pub fn unregister_ip6_protocol(protocol: u8) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = crate::lookup::IP6_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    // SAFETY: restore the owner table before the consumer retires under the barrier.
    unsafe {
        (*main.local_next_by_ip_protocol.get())[usize::from(protocol)] =
            NodeNext::slot(Ip6LocalNext::Punt);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum LocalStage {
    Head,
    Receive,
    End,
}

impl LocalStage {
    #[inline(always)]
    fn is_head_of_feature_arc(self) -> bool {
        matches!(self, Self::Head | Self::Receive)
    }

    #[inline(always)]
    fn trace_stage(self) -> IpLocalTraceStage {
        match self {
            Self::Head => IpLocalTraceStage::Head,
            Self::Receive => IpLocalTraceStage::Receive,
            Self::End => IpLocalTraceStage::End,
        }
    }
}

#[inline(always)]
fn process_frame(
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    stage: LocalStage,
    version: IpVersion,
) -> () {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match process_index(runtime, index, stage, version) {
            Ok(slot) => slot,
            Err(_) => match version {
                IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
            },
        }
    })
}

#[inline(always)]
fn process_index(
    runtime: &DataPlaneMain,
    index: Index,
    stage: LocalStage,
    version: IpVersion,
) -> RuntimeResult<u16> {
    let buffer = runtime.get_buffer(index)?;
    let current = buffer.current();
    let network = *unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let header_offset = network.packet_cursor().network_header_offset();
    let protocol = match version {
        IpVersion::V4 => {
            let header = read_header::<Ipv4Header>(current, header_offset)?;
            if matches!(stage, LocalStage::End) && header.flags_fragment() & 0x3fff != 0 {
                return Ok(NodeNext::slot(Ip4LocalNext::Reassembly));
            }
            header.protocol()
        }
        IpVersion::V6 => read_header::<Ipv6Header>(current, header_offset)?.next_protocol(),
    };
    // SAFETY: the owning main thread mutates these slots only while workers
    // are stopped; only the copied next slot leaves this read.
    let protocol_next = unsafe {
        match version {
            IpVersion::V4 => {
                (*crate::lookup::IP4_MAIN
                    .get()
                    .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?
                    .local_next_by_ip_protocol
                    .get())[usize::from(protocol)]
            }
            IpVersion::V6 => {
                (*crate::lookup::IP6_MAIN
                    .get()
                    .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?
                    .local_next_by_ip_protocol
                    .get())[usize::from(protocol)]
            }
        }
    };
    if matches!(stage, LocalStage::End) {
        return Ok(protocol_next);
    }
    let parsed = match ip_header(current, network.packet_cursor()) {
        Ok(parsed) if parsed.version == version => parsed,
        _ => {
            drop(buffer);
            set_index_node_error(runtime, index, IpLocalError::BadLength)?;
            let resolved = match version {
                IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: None,
                    protocol: None,
                    transport_header_len: 0,
                    error: Some(IpLocalError::BadLength.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    };
    match parsed.input_target {
        IpInputTarget::Drop | IpInputTarget::IcmpError | IpInputTarget::Options => {
            let error = error_for_input(parsed.input_error);
            drop(buffer);
            set_index_node_error(runtime, index, error)?;
            let resolved = match version {
                IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: parsed.transport_header_len,
                    error: Some(error.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
        IpInputTarget::Reassembly => {
            drop(buffer);
            refresh_basic_metadata(runtime, index, &parsed, None)?;
            let resolved = match version {
                IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Reassembly),
                IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Reassembly),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: parsed.transport_header_len,
                    error: None,
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
        IpInputTarget::Punt | IpInputTarget::Lookup | IpInputTarget::LookupMulticast => {}
    }

    let first_len = current.len().min(parsed.packet_len);
    let packet = current
        .get(..first_len)
        .ok_or_else(|| RuntimeError::from(crate::protocol::ip::IpInputError::BadLength))?;
    let transport = match packet.get(parsed.transport_header_offset..) {
        Some(transport)
            if parsed.packet_len
                <= buffer.current_len() + buffer.total_len_not_including_first() =>
        {
            transport
        }
        _ => {
            drop(buffer);
            set_index_node_error(runtime, index, IpLocalError::BadLength)?;
            let resolved = match version {
                IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: 0,
                    error: Some(IpLocalError::BadLength.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    };

    let transport_len = match validate_transport(transport, &parsed) {
        Ok(transport_len) => transport_len,
        Err(error) => {
            drop(buffer);
            set_index_node_error(runtime, index, error)?;
            let resolved = match version {
                IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: 0,
                    error: Some(error.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    };
    let checksum_required = match parsed.protocol {
        IpProtocol::Tcp => parsed.version == IpVersion::V4,
        IpProtocol::Udp => transport[6..8] != [0, 0],
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => true,
        IpProtocol::Other(_) => false,
    };
    drop(buffer);
    if checksum_required && l4_checksum(runtime, index, &parsed)? != 0 {
        set_index_node_error(runtime, index, IpLocalError::BadChecksum)?;
        return Ok(match version {
            IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
            IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
        });
    }
    let net = NetMain::global()?;
    let source_error = match (parsed.source, parsed.destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            let source_dpo = network
                .ip()
                .fib_index_override()
                .or(network.ip().fib_index())
                .and_then(|fib| {
                    crate::lookup::IP4_MAIN
                        .get()
                        .and_then(|main| main.forwarding_dpo(fib, source))
                });
            if source_dpo
                .and_then(|dpo| net.select_load_balance(dpo, |_, _| Some(0)))
                .is_some_and(|dpo| dpo.class() == DpoType::RECEIVE)
            {
                Some(IpLocalError::SpoofedLocalPacket)
            } else if destination != std::net::Ipv4Addr::BROADCAST
                && !source_dpo
                    .and_then(|dpo| net.load_balance_urpf(dpo))
                    .and_then(|list| net.urpf_size(list))
                    .is_some_and(|count| count != 0)
            {
                Some(IpLocalError::SourceLookupMiss)
            } else {
                None
            }
        }
        (IpAddr::V6(source), _)
            if parsed.protocol != IpProtocol::Icmpv6 && !source.is_unicast_link_local() =>
        {
            let source_dpo = crate::lookup::IP6_MAIN.get().and_then(|main| {
                network
                    .ip()
                    .fib_index_override()
                    .or(network.ip().fib_index())
                    .and_then(|fib| main.forwarding_dpo(fib, source))
            });
            if source_dpo
                .and_then(|dpo| net.load_balance_urpf(dpo))
                .and_then(|list| net.urpf_size(list))
                .is_some_and(|count| count != 0)
            {
                None
            } else {
                Some(IpLocalError::SourceLookupMiss)
            }
        }
        _ => None,
    };
    if let Some(error) = source_error {
        set_index_node_error(runtime, index, error)?;
        return Ok(match version {
            IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Drop),
            IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Drop),
        });
    }
    refresh_basic_metadata(runtime, index, &parsed, transport_len)?;

    if stage.is_head_of_feature_arc() {
        // SAFETY: arc indices are installed before workers start and never change.
        let arc_index = unsafe {
            match version {
                IpVersion::V4 => *crate::lookup::IP4_MAIN
                    .get()
                    .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?
                    .local_feature_arc_index
                    .get(),
                IpVersion::V6 => *crate::lookup::IP6_MAIN
                    .get()
                    .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?
                    .local_feature_arc_index
                    .get(),
            }
        };
        let net = NetMain::global()?;
        let mut buffer = runtime.get_buffer_mut(index)?;
        let interface_index =
            unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }.sw_if_index[0];
        let resolved = net.interface_main().start_feature_arc(
            arc_index,
            interface_index,
            &mut buffer,
            protocol_next,
        );
        return Ok(resolved);
    }

    let resolved = protocol_next;
    let error = if resolved
        == match version {
            IpVersion::V4 => NodeNext::slot(Ip4LocalNext::Punt),
            IpVersion::V6 => NodeNext::slot(Ip6LocalNext::Punt),
        }
        && matches!(parsed.protocol, IpProtocol::Other(_))
    {
        set_index_node_error(runtime, index, IpLocalError::UnknownProtocol)?;
        Some(IpLocalError::UnknownProtocol.code())
    } else {
        None
    };
    let _ = add_packet_trace!(
        runtime,
        index,
        IpLocalTrace {
            stage: stage.trace_stage(),
            version: Some(parsed.version),
            protocol: Some(parsed.protocol),
            transport_header_len: transport_len.unwrap_or_default(),
            error,
            next: resolved,
        },
    );
    Ok(resolved)
}

#[inline(always)]
fn validate_transport(
    transport: &[u8],
    parsed: &ParsedIpPacket,
) -> Result<Option<usize>, IpLocalError> {
    match parsed.protocol {
        IpProtocol::Tcp => {
            let header_len = tcp_header_len(transport)?;
            Ok(Some(header_len))
        }
        IpProtocol::Udp => {
            let header = transport.get(..8).ok_or(IpLocalError::BadTransportHeader)?;
            let length = usize::from(u16::from_be_bytes([header[4], header[5]]));
            if length < 8 || length > parsed.packet_len - parsed.transport_header_offset {
                return Err(IpLocalError::BadLength);
            }
            Ok(Some(8))
        }
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => {
            read_header::<IcmpHeader>(transport, 0)
                .map_err(|_| IpLocalError::BadTransportHeader)?;
            Ok(Some(ICMP_HEADER_MIN_LEN))
        }
        IpProtocol::Other(_) => Ok(None),
    }
}

#[inline(always)]
fn tcp_header_len(transport: &[u8]) -> Result<usize, IpLocalError> {
    let data_offset = *transport.get(12).ok_or(IpLocalError::BadTransportHeader)?;
    let header_len = usize::from(data_offset >> 4) * 4;
    if header_len < TCP_HEADER_MIN_LEN || transport.len() < header_len {
        return Err(IpLocalError::BadTransportHeader);
    }
    Ok(header_len)
}

#[inline(always)]
fn refresh_basic_metadata(
    runtime: &DataPlaneMain,
    index: Index,
    parsed: &ParsedIpPacket,
    transport_header_len: Option<usize>,
) -> RuntimeResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let transport_header_len = transport_header_len.unwrap_or_default();
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(parsed.packet_len)
            .with_network_header(parsed.network_header_offset, parsed.network_header_len)
            .with_transport_header(parsed.transport_header_offset, transport_header_len)
            .with_transport_payload_offset(parsed.transport_header_offset + transport_header_len),
    );
    Ok(())
}

#[inline(always)]
fn error_for_input(error: IpInputError) -> IpLocalError {
    match error {
        IpInputError::BadChecksum => IpLocalError::BadChecksum,
        _ => IpLocalError::BadLength,
    }
}

#[inline(always)]
fn l4_checksum(
    runtime: &DataPlaneMain,
    index: Index,
    parsed: &ParsedIpPacket,
) -> RuntimeResult<u16> {
    let mut checksum: InternetChecksum = Default::default();
    let mut remaining = parsed.packet_len - parsed.transport_header_offset;
    match (parsed.source, parsed.destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) if parsed.protocol != IpProtocol::Icmpv4 => {
            checksum.write(&source.octets());
            checksum.write(&destination.octets());
            checksum.write(&[0, parsed.protocol.into()]);
            checksum.write(&(remaining as u16).to_be_bytes());
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            checksum.write(&source.octets());
            checksum.write(&destination.octets());
            checksum.write(&(remaining as u32).to_be_bytes());
            checksum.write(&[0, 0, 0, parsed.protocol.into()]);
        }
        _ => {}
    }
    let mut offset = parsed.transport_header_offset;
    for segment in runtime.chain(index) {
        let segment = segment?;
        let bytes = segment.current();
        if offset >= bytes.len() {
            offset -= bytes.len();
            continue;
        }
        let length = remaining.min(bytes.len() - offset);
        checksum.write(&bytes[offset..offset + length]);
        remaining -= length;
        offset = 0;
        if remaining == 0 {
            return Ok(checksum.finish() as u16);
        }
    }
    Err(crate::protocol::ip::IpInputError::BadLength.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::checksum::internet_checksum;
    use hammer_runtime::DataPlaneBufferConfig;

    #[test]
    fn icmp_checksum_spans_odd_buffer_boundary() -> RuntimeResult<()> {
        hammer_runtime::config::Memory::default().ensure_main_heap()?;
        let runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
        // test_ip4.py::TestICMPEcho uses ID 0xB, sequence 5 and 18 payload
        // bytes. Split that message across buffers to exercise the chain
        // checksum used by VPP ip_calculate_l4_checksum, including odd carry.
        let mut packet = [0x0a; 46];
        packet[..20].fill(0);
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&46u16.to_be_bytes());
        packet[9] = IpProtocol::Icmpv4.into();
        packet[12..16].copy_from_slice(&[192, 0, 2, 2]);
        packet[16..20].copy_from_slice(&[192, 0, 2, 1]);
        packet[20..28].copy_from_slice(&[8, 0, 0, 0, 0, 0x0b, 0, 5]);
        let checksum = internet_checksum(&packet[20..]);
        packet[22..24].copy_from_slice(&checksum.to_be_bytes());
        let mut frame = runtime.buffers().get_next_frame(NodeId::new(0))?;
        let head = runtime.buffers().alloc_index_with_bytes(&packet[..37])?;
        frame.push_index(head)?;
        let tail = runtime.buffers().alloc_index_with_bytes(&packet[37..])?;
        runtime.buffers().chain_buffer(head, tail)?;
        let parsed = ip_header(
            &packet,
            BufferPacketCursor::new()
                .with_packet_len(packet.len())
                .with_network_header(0, 20)
                .with_transport_header(20, 8),
        )?;
        assert_eq!(l4_checksum(&runtime, head, &parsed)?, 0);
        runtime.get_buffer_mut(tail)?.current_mut()[0] ^= 1;
        assert_ne!(l4_checksum(&runtime, head, &parsed)?, 0);
        drop(frame);
        assert_eq!(runtime.buffers().in_use_buffers(), 0);
        Ok(())
    }
}
