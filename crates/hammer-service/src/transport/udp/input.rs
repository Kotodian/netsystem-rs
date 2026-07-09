use std::mem::{size_of, transmute};
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeResult, PacketTrace, SecondaryOpaque,
    TraceFormatter, add_packet_trace,
};
use hammer_core::data_plane::{NodeId, NodeNextStorage};
use hammer_core::error::CoreResult;
use hammer_core::protocol::icmp::IcmpErrorMetadata;
use hammer_core::protocol::transport::UdpHeader;
use hammer_core::protocol::wire::read_header;
use hammer_infra::boxed::Slice;

use crate::data_plane::set_index_node_error_code;
use crate::net::NetworkOpaque;
use crate::net::ip::{IpProtocol, IpVersion};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_protocol, put_option_ip_version, put_option_u16,
};

const UDP_HEADER_LEN: usize = 8;
const UDP_PORT_COUNT: usize = u16::MAX as usize + 1;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct IcmpErrorOpaque {
    icmp_error: Option<IcmpErrorMetadata>,
    reserved: [u64; 6],
}

const _: () = assert!(size_of::<IcmpErrorOpaque>() == size_of::<SecondaryOpaque>());

#[hammer_component_macros::node_next]
pub enum UdpInputNext {
    Drop,
    Punt,
    IcmpError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpInputError {
    BadLength,
    WrongProtocol,
    UnknownPort,
}

impl UdpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub source_port: Option<u16>,
    pub destination_port: Option<u16>,
    pub error: Option<u16>,
    pub next: NodeId,
}

impl UdpInputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            version: cursor.read_option_ip_version()?,
            protocol: cursor.read_option_ip_protocol()?,
            source_port: cursor.read_option_u16()?,
            destination_port: cursor.read_option_u16()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for UdpInputTrace {
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_u16(out, self.source_port);
        put_option_u16(out, self.destination_port);
        put_option_u16(out, self.error);
        put_node(out, self.next);
    }
}

fn format_udp_input_trace(bytes: &[u8]) -> String {
    match UdpInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("UdpInputTrace invalid={bytes:?}"),
    }
}

pub struct UdpInputControlPlane {
    inner: Arc<ArcSwap<UdpInputSnapshot>>,
    next: [NodeId; UdpInputNext::COUNT],
}

impl UdpInputControlPlane {
    #[inline]
    pub fn new(nexts: [NodeId; UdpInputNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(UdpInputSnapshot::new())),
            next: nexts,
        }
    }

    #[inline]
    pub fn register_port(&self, port: u16, node: NodeId) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.register_port(port, node);
            next
        });
        Ok(())
    }

    #[inline]
    pub fn register_punt_port(&self, port: u16) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.register_punt_port(port);
            next
        });
        Ok(())
    }

    #[inline]
    pub fn unregister_port(&self, port: u16) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.unregister_port(port);
            next
        });
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> UdpInputNode {
        UdpInputNode::new(
            UdpInputSnapshotHandle::new(Arc::clone(&self.inner)),
            self.next,
        )
    }
}

#[derive(Clone)]
struct UdpInputSnapshot {
    ports: Slice<UdpPortAction>,
}

impl UdpInputSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            ports: Slice::from_elem(UDP_PORT_COUNT, UdpPortAction::IcmpError),
        }
    }

    #[inline(always)]
    fn register_port(&mut self, port: u16, node: NodeId) {
        self.ports[port as usize] = UdpPortAction::Dispatch(node);
    }

    #[inline(always)]
    fn register_punt_port(&mut self, port: u16) {
        self.ports[port as usize] = UdpPortAction::Punt;
    }

    #[inline(always)]
    fn unregister_port(&mut self, port: u16) {
        self.ports[port as usize] = UdpPortAction::IcmpError;
    }

    #[inline(always)]
    fn action(&self, port: u16) -> UdpPortAction {
        self.ports[port as usize].clone()
    }
}

#[derive(Debug, Clone)]
enum UdpPortAction {
    IcmpError,
    Punt,
    Dispatch(NodeId),
}

#[derive(Debug, Clone, Copy)]
enum UdpInputNextKey<'a> {
    Punt(&'a [NodeId; UdpInputNext::COUNT]),
    IcmpError(&'a [NodeId; UdpInputNext::COUNT]),
}

impl NodeNextStorage<UdpInputNextKey<'_>> for UdpInputSnapshot {
    #[inline(always)]
    fn next(&self, key: UdpInputNextKey<'_>) -> NodeId {
        match key {
            UdpInputNextKey::Punt(next) => next[UdpInputNext::Punt.slot()],
            UdpInputNextKey::IcmpError(next) => next[UdpInputNext::IcmpError.slot()],
        }
    }
}

#[derive(Clone)]
struct UdpInputSnapshotHandle {
    inner: Arc<ArcSwap<UdpInputSnapshot>>,
}

impl UdpInputSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<UdpInputSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<UdpInputSnapshot>> {
        self.inner.load()
    }
}

#[hammer_component_macros::node(role = internal, next = UdpInputNext)]
pub struct UdpInputNode {
    snapshot: UdpInputSnapshotHandle,
}

impl Node for UdpInputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let snapshot = self.snapshot.load();
        let next = match Self::runtime_nexts(runtime) {
            Ok(next) => next,
            Err(_) => return NodeResult::drop(),
        };
        udp_input_process_frame(runtime, frame, &snapshot, &next)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_udp_input_trace)
    }
}

#[inline(always)]
fn udp_input_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    snapshot: &UdpInputSnapshot,
    next: &[NodeId; UdpInputNext::COUNT],
) -> NodeResult {
    hammer_adapter::process_frame!(runtime, frame, |index| {
        match next_node_for_index(runtime, index, snapshot, next) {
            Ok(Some(node)) => node,
            _ => NodeNextStorage::next(next, UdpInputNext::Drop),
        }
    })
}

#[inline(always)]
fn next_node_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    snapshot: &UdpInputSnapshot,
    next: &[NodeId; UdpInputNext::COUNT],
) -> CoreResult<Option<NodeId>> {
    let (version, protocol, source_port, destination_port) = {
        let buffer = runtime.get_buffer(index)?;
        let current = buffer.current();
        let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
        let cursor = network.packet_cursor();
        let ip = network.ip();
        let version = match ip.ip_version() {
            Some(4) => IpVersion::V4,
            Some(6) => IpVersion::V6,
            _ => {
                drop(buffer);
                return resolve_drop_error(
                    runtime,
                    index,
                    next,
                    UdpInputError::BadLength,
                    None,
                    None,
                    None,
                    None,
                );
            }
        };
        let protocol = match ip.ip_protocol() {
            Some(17) => IpProtocol::Udp,
            Some(1) => IpProtocol::Icmpv4,
            Some(6) => IpProtocol::Tcp,
            Some(58) => IpProtocol::Icmpv6,
            Some(value) => IpProtocol::Other(value),
            None => {
                drop(buffer);
                return resolve_drop_error(
                    runtime,
                    index,
                    next,
                    UdpInputError::BadLength,
                    None,
                    None,
                    None,
                    None,
                );
            }
        };
        if protocol != IpProtocol::Udp {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                next,
                UdpInputError::WrongProtocol,
                Some(version),
                Some(protocol),
                None,
                None,
            );
        }
        if !valid_udp_cursor(cursor) {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                next,
                UdpInputError::BadLength,
                None,
                None,
                None,
                None,
            );
        }

        let header = match read_header::<UdpHeader>(current, cursor.transport_header_offset()) {
            Ok(header) => header,
            Err(_) => {
                drop(buffer);
                return resolve_drop_error(
                    runtime,
                    index,
                    next,
                    UdpInputError::BadLength,
                    None,
                    None,
                    None,
                    None,
                );
            }
        };
        let source_port = header.source_port();
        let destination_port = header.destination_port();
        if !valid_udp_len(
            cursor.transport_header_offset(),
            cursor.packet_len(),
            header.length(),
        ) {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                next,
                UdpInputError::BadLength,
                None,
                None,
                None,
                None,
            );
        }
        (version, protocol, source_port, destination_port)
    };

    match snapshot.action(destination_port) {
        UdpPortAction::Dispatch(node) => {
            clear_success_metadata(runtime, index)?;
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(version),
                    protocol: Some(protocol),
                    source_port: Some(source_port),
                    destination_port: Some(destination_port),
                    error: None,
                    next: node,
                },
            )?;
            Ok(Some(node))
        }
        UdpPortAction::Punt => {
            clear_success_metadata(runtime, index)?;
            let resolved = NodeNextStorage::next(snapshot, UdpInputNextKey::Punt(next));
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(version),
                    protocol: Some(protocol),
                    source_port: Some(source_port),
                    destination_port: Some(destination_port),
                    error: None,
                    next: resolved,
                },
            )?;
            Ok(Some(resolved))
        }
        UdpPortAction::IcmpError => resolve_unknown_port(
            runtime,
            index,
            next,
            snapshot,
            version,
            protocol,
            source_port,
            destination_port,
        ),
    }
}

#[inline(always)]
fn resolve_drop_error(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: &[NodeId; UdpInputNext::COUNT],
    error: UdpInputError,
    version: Option<IpVersion>,
    protocol: Option<IpProtocol>,
    source_port: Option<u16>,
    destination_port: Option<u16>,
) -> CoreResult<Option<NodeId>> {
    set_index_node_error_code(runtime, index, error.code())?;
    let resolved = NodeNextStorage::next(next, UdpInputNext::Drop);
    add_packet_trace!(
        runtime,
        index,
        UdpInputTrace {
            version,
            protocol,
            source_port,
            destination_port,
            error: Some(error.code()),
            next: resolved,
        },
    )?;
    Ok(Some(resolved))
}

#[inline(always)]
fn resolve_unknown_port(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: &[NodeId; UdpInputNext::COUNT],
    snapshot: &UdpInputSnapshot,
    version: IpVersion,
    protocol: IpProtocol,
    source_port: u16,
    destination_port: u16,
) -> CoreResult<Option<NodeId>> {
    set_index_node_error_code(runtime, index, UdpInputError::UnknownPort.code())?;
    let mut buffer = runtime.get_buffer_mut(index)?;
    let opaque = unsafe { transmute::<_, &mut IcmpErrorOpaque>(buffer.opaque2_mut()) };
    opaque.icmp_error = port_unreachable_metadata(version);
    let resolved = NodeNextStorage::next(snapshot, UdpInputNextKey::IcmpError(next));
    add_packet_trace!(
        runtime,
        index,
        UdpInputTrace {
            version: Some(version),
            protocol: Some(protocol),
            source_port: Some(source_port),
            destination_port: Some(destination_port),
            error: Some(UdpInputError::UnknownPort.code()),
            next: resolved,
        },
    )?;
    Ok(Some(resolved))
}

#[inline(always)]
fn clear_success_metadata(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.clear_node_error();
    let opaque = unsafe { transmute::<_, &mut IcmpErrorOpaque>(buffer.opaque2_mut()) };
    opaque.icmp_error = None;
    Ok(())
}

#[inline(always)]
fn valid_udp_cursor(cursor: hammer_adapter::BufferPacketCursor) -> bool {
    let transport_header_offset = cursor.transport_header_offset();
    let packet_len = cursor.packet_len();
    cursor.packet_len() != 0
        && cursor.transport_header_len() >= UDP_HEADER_LEN
        && cursor.transport_payload_offset() >= transport_header_offset + UDP_HEADER_LEN
        && transport_header_offset
            .checked_add(UDP_HEADER_LEN)
            .is_some_and(|end| end <= packet_len)
}

#[inline(always)]
fn valid_udp_len(transport_header_offset: usize, packet_len: usize, udp_len: usize) -> bool {
    udp_len >= UDP_HEADER_LEN
        && transport_header_offset
            .checked_add(udp_len)
            .is_some_and(|end| end <= packet_len)
}

#[inline(always)]
fn port_unreachable_metadata(version: IpVersion) -> Option<IcmpErrorMetadata> {
    match version {
        IpVersion::V4 => Some(IcmpErrorMetadata::ipv4_destination_unreachable(3, 0)),
        IpVersion::V6 => Some(IcmpErrorMetadata::ipv6_port_unreachable()),
    }
}
