use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, InternalNode, Node, NodeId,
    NodeNextFrames, NodeResult, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::data_plane::{FeatureArc, set_index_node_error_code};
use crate::net::{DpoType, FibLookupResult, FibSnapshotHandle};

use super::{IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket};
use super::{network_for_protocol, parse_ip_packet_with_chain_len};

const IP_PROTOCOL_ICMP: u8 = 1;
const IP_PROTOCOL_TCP: u8 = 6;
const IP_PROTOCOL_UDP: u8 = 17;
const IP_PROTOCOL_ICMP6: u8 = 58;
const TCP_HEADER_MIN_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const ICMP_HEADER_MIN_LEN: usize = 4;

#[hammer_component_macros::feature_arc]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpLocalArc {
    LocalForward,
}

#[hammer_component_macros::node_next]
pub enum IpLocalNext {
    Drop,
    Punt,
    Tcp,
    Udp,
    Icmp,
    Reassembly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpLocalError {
    BadLength,
    BadTransportHeader,
    BadChecksum,
    SourceCheckFailed,
    UnknownProtocol,
}

impl IpLocalError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone)]
pub enum IpLocalSourceCheck {
    Disabled,
    ReverseFib(FibSnapshotHandle),
}

impl Default for IpLocalSourceCheck {
    #[inline]
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct IpLocalControlPlane {
    inner: Arc<ArcSwap<IpLocalSnapshot>>,
}

impl IpLocalControlPlane {
    #[inline]
    pub fn new(next: [NodeId; IpLocalNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(IpLocalSnapshot::new(next))),
        }
    }

    #[inline]
    pub fn with_source_check(self, source_check: IpLocalSourceCheck) -> Self {
        self.publish_source_check(source_check);
        self
    }

    #[inline]
    pub fn node(&self) -> IpLocalNode {
        IpLocalNode::new(IpLocalSnapshotHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    pub fn receive_node(&self) -> IpReceiveNode {
        IpReceiveNode::new(IpLocalSnapshotHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    pub fn register_protocol(&self, protocol: u8, node: NodeId) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = IpLocalSnapshot::clone(current);
            next.protocol_nexts[protocol as usize] = node;
            next
        });
        Ok(())
    }

    #[inline]
    pub fn unregister_protocol(&self, protocol: u8) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = IpLocalSnapshot::clone(current);
            next.protocol_nexts[protocol as usize] = next.next[IpLocalNext::Punt.slot()];
            next
        });
        Ok(())
    }

    #[inline]
    pub fn publish_source_check(&self, source_check: IpLocalSourceCheck) {
        self.inner.rcu(|current| {
            let mut next = IpLocalSnapshot::clone(current);
            next.source_check = source_check.clone();
            next
        });
    }
}

#[derive(Debug, Clone)]
struct IpLocalSnapshot {
    next: [NodeId; IpLocalNext::COUNT],
    protocol_nexts: Box<[NodeId; 256]>,
    source_check: IpLocalSourceCheck,
}

impl IpLocalSnapshot {
    #[inline]
    fn new(next: [NodeId; IpLocalNext::COUNT]) -> Self {
        let punt = next[IpLocalNext::Punt.slot()];
        let mut protocol_nexts = Box::new([punt; 256]);
        protocol_nexts[IP_PROTOCOL_TCP as usize] = next[IpLocalNext::Tcp.slot()];
        protocol_nexts[IP_PROTOCOL_UDP as usize] = next[IpLocalNext::Udp.slot()];
        protocol_nexts[IP_PROTOCOL_ICMP as usize] = next[IpLocalNext::Icmp.slot()];
        protocol_nexts[IP_PROTOCOL_ICMP6 as usize] = next[IpLocalNext::Icmp.slot()];
        Self {
            next,
            protocol_nexts,
            source_check: IpLocalSourceCheck::Disabled,
        }
    }

    #[inline(always)]
    fn protocol_next(&self, protocol: IpProtocol) -> NodeId {
        self.protocol_nexts[ip_protocol_number(protocol) as usize]
    }

    #[inline(always)]
    fn punt_next(&self) -> NodeId {
        self.next[IpLocalNext::Punt.slot()]
    }

    #[inline(always)]
    fn drop_next(&self) -> NodeId {
        self.next[IpLocalNext::Drop.slot()]
    }

    #[inline(always)]
    fn reassembly_next(&self) -> NodeId {
        self.next[IpLocalNext::Reassembly.slot()]
    }
}

#[derive(Clone)]
struct IpLocalSnapshotHandle {
    inner: Arc<ArcSwap<IpLocalSnapshot>>,
}

impl IpLocalSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<IpLocalSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<IpLocalSnapshot>> {
        self.inner.load()
    }
}

#[hammer_component_macros::node(start_arc = IpLocalArc)]
pub struct IpLocalNode {
    snapshot: IpLocalSnapshotHandle,
}

#[hammer_component_macros::node(start_arc = IpLocalArc)]
pub struct IpReceiveNode {
    snapshot: IpLocalSnapshotHandle,
}

impl<G> Node<G> for IpLocalNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let snapshot = self.snapshot.load();
        process_frame(
            runtime,
            frame,
            &snapshot,
            LocalStage::Head,
            self.feature_arc.as_ref(),
        )
    }
}

impl<G> InternalNode<G> for IpLocalNode {}

impl<G> Node<G> for IpReceiveNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let snapshot = self.snapshot.load();
        process_frame(
            runtime,
            frame,
            &snapshot,
            LocalStage::Receive,
            self.feature_arc.as_ref(),
        )
    }
}

impl<G> InternalNode<G> for IpReceiveNode {}

#[derive(Debug, Clone, Copy)]
enum LocalStage {
    Head,
    Receive,
}

impl LocalStage {
    #[inline(always)]
    fn is_head_of_feature_arc(self) -> bool {
        matches!(self, Self::Head | Self::Receive)
    }
}

#[inline(always)]
fn process_frame<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
    snapshot: &IpLocalSnapshot,
    stage: LocalStage,
    feature_arc: Option<&FeatureArc<IpLocalArc>>,
) -> CoreResult<NodeResult> {
    let mut next_frames = NodeNextFrames::default();
    let mut current_next = None;
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        let next = process_index(runtime, index, snapshot, stage, feature_arc)?;
        emit_output(runtime, &mut next_frames, &mut current_next, next, index)
    })?;
    next_frames.schedule(runtime)?;
    if frame.has_pending()
        && let Some(node) = current_next
    {
        Ok(NodeResult::next_current(node))
    } else {
        Ok(NodeResult::drop())
    }
}

#[inline(always)]
fn process_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    snapshot: &IpLocalSnapshot,
    stage: LocalStage,
    feature_arc: Option<&FeatureArc<IpLocalArc>>,
) -> CoreResult<NodeId> {
    let packet = packet_bytes(runtime, index)?;
    let parsed = match parse_ip_packet_with_chain_len(&packet, 0) {
        Ok(parsed) => parsed,
        Err(_) => {
            set_index_node_error_code(runtime, index, IpLocalError::BadLength.code())?;
            return Ok(snapshot.drop_next());
        }
    };
    match parsed.input_target {
        IpInputTarget::Drop | IpInputTarget::IcmpError | IpInputTarget::Options => {
            set_index_node_error_code(runtime, index, error_for_input(parsed.input_error).code())?;
            return Ok(snapshot.drop_next());
        }
        IpInputTarget::Reassembly => {
            refresh_basic_metadata(runtime, index, &parsed, None)?;
            return Ok(snapshot.reassembly_next());
        }
        IpInputTarget::Punt | IpInputTarget::Lookup | IpInputTarget::LookupMulticast => {}
    }

    let packet = packet
        .get(..parsed.packet_len)
        .ok_or_else(|| CoreError::internal("invalid local packet length"))?;
    let transport = match packet.get(parsed.transport_header_offset..parsed.packet_len) {
        Some(transport) => transport,
        None => {
            set_index_node_error_code(runtime, index, IpLocalError::BadLength.code())?;
            return Ok(snapshot.drop_next());
        }
    };

    let transport_len = match validate_transport(packet, transport, &parsed, stage) {
        Ok(transport_len) => transport_len,
        Err(error) => {
            set_index_node_error_code(runtime, index, error.code())?;
            return Ok(snapshot.drop_next());
        }
    };
    refresh_basic_metadata(runtime, index, &parsed, transport_len)?;

    if stage.is_head_of_feature_arc() {
        if !source_check_passes(snapshot, &parsed) {
            set_index_node_error_code(runtime, index, IpLocalError::SourceCheckFailed.code())?;
            return Ok(snapshot.drop_next());
        }
        if let Some(feature_arc) = feature_arc {
            let next = snapshot.protocol_next(parsed.protocol);
            return runtime
                .with_metadata_mut(index, |metadata| feature_arc.start_or(metadata, next));
        }
    }

    let next = snapshot.protocol_next(parsed.protocol);
    if next == snapshot.punt_next() && matches!(parsed.protocol, IpProtocol::Other(_)) {
        set_index_node_error_code(runtime, index, IpLocalError::UnknownProtocol.code())?;
    }
    Ok(next)
}

#[inline(always)]
fn emit_output<G>(
    runtime: &DataPlaneRuntime<G>,
    next_frames: &mut NodeNextFrames,
    current_next: &mut Option<NodeId>,
    node: NodeId,
    index: BufferIndex,
) -> CoreResult<Option<BufferIndex>> {
    match *current_next {
        Some(current) if current == node => Ok(Some(index)),
        Some(_) => {
            next_frames.enqueue(runtime, node, index)?;
            Ok(None)
        }
        None => {
            *current_next = Some(node);
            Ok(Some(index))
        }
    }
}

#[inline(always)]
fn packet_bytes<G>(runtime: &DataPlaneRuntime<G>, index: BufferIndex) -> CoreResult<Vec<u8>> {
    let buffer = runtime.get_buffer(index)?;
    let packet_len = buffer.current_len() + buffer.total_len_not_including_first();
    drop(buffer);
    let mut packet = runtime.copy_current_chain(index)?;
    if packet.len() > packet_len {
        packet.truncate(packet_len);
    }
    Ok(packet.into_iter().collect())
}

#[inline(always)]
fn validate_transport(
    packet: &[u8],
    transport: &[u8],
    parsed: &ParsedIpPacket,
    stage: LocalStage,
) -> Result<Option<usize>, IpLocalError> {
    match parsed.protocol {
        IpProtocol::Tcp => {
            let header_len = tcp_header_len(transport)?;
            if stage.is_head_of_feature_arc()
                && l4_checksum(packet, parsed, IP_PROTOCOL_TCP, transport) != 0
            {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(header_len))
        }
        IpProtocol::Udp => {
            let datagram_len = udp_datagram_len(transport)?;
            let datagram = &transport[..datagram_len];
            if stage.is_head_of_feature_arc() {
                let checksum = u16::from_be_bytes([transport[6], transport[7]]);
                match parsed.version {
                    IpVersion::V4 if checksum == 0 => {}
                    IpVersion::V6 if checksum == 0 => return Err(IpLocalError::BadChecksum),
                    _ if l4_checksum(packet, parsed, IP_PROTOCOL_UDP, datagram) != 0 => {
                        return Err(IpLocalError::BadChecksum);
                    }
                    _ => {}
                }
            }
            Ok(Some(UDP_HEADER_LEN))
        }
        IpProtocol::Icmpv4 => {
            if transport.len() < ICMP_HEADER_MIN_LEN {
                return Err(IpLocalError::BadTransportHeader);
            }
            if matches!(stage, LocalStage::Head) && internet_checksum(transport) != 0 {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(ICMP_HEADER_MIN_LEN))
        }
        IpProtocol::Icmpv6 => {
            if transport.len() < ICMP_HEADER_MIN_LEN {
                return Err(IpLocalError::BadTransportHeader);
            }
            if matches!(stage, LocalStage::Head)
                && l4_checksum(packet, parsed, IP_PROTOCOL_ICMP6, transport) != 0
            {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(ICMP_HEADER_MIN_LEN))
        }
        IpProtocol::Other(_) => Ok(None),
    }
}

#[inline(always)]
fn tcp_header_len(transport: &[u8]) -> Result<usize, IpLocalError> {
    if transport.len() < TCP_HEADER_MIN_LEN {
        return Err(IpLocalError::BadTransportHeader);
    }
    let header_len = ((transport[12] >> 4) as usize) * 4;
    if header_len < TCP_HEADER_MIN_LEN || transport.len() < header_len {
        return Err(IpLocalError::BadTransportHeader);
    }
    Ok(header_len)
}

#[inline(always)]
fn udp_datagram_len(transport: &[u8]) -> Result<usize, IpLocalError> {
    if transport.len() < UDP_HEADER_LEN {
        return Err(IpLocalError::BadTransportHeader);
    }
    let len = u16::from_be_bytes([transport[4], transport[5]]) as usize;
    if len < UDP_HEADER_LEN || transport.len() < len {
        return Err(IpLocalError::BadTransportHeader);
    }
    Ok(len)
}

#[inline(always)]
fn refresh_basic_metadata<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    parsed: &ParsedIpPacket,
    transport_header_len: Option<usize>,
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let (source_port, destination_port) = match parsed.protocol {
        IpProtocol::Tcp | IpProtocol::Udp => {
            let current = buffer.current();
            let transport = current
                .get(parsed.transport_header_offset..parsed.packet_len)
                .ok_or_else(|| CoreError::internal("invalid transport packet cursor"))?;
            let source_port = u16::from_be_bytes([transport[0], transport[1]]);
            let destination_port = u16::from_be_bytes([transport[2], transport[3]]);
            (source_port, destination_port)
        }
        _ => (0, 0),
    };
    let metadata = buffer.metadata_mut();
    if let Some(network) = network_for_protocol(parsed.protocol) {
        metadata.network = network;
    }
    metadata.source = Some(SocksAddr::ip(parsed.source, source_port));
    metadata.destination = Some(SocksAddr::ip(parsed.destination, destination_port));
    let transport_header_len = transport_header_len.unwrap_or_default();
    buffer.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(parsed.packet_len)
            .with_network_header(parsed.network_header_offset, parsed.network_header_len)
            .with_transport_header(parsed.transport_header_offset, transport_header_len)
            .with_transport_payload_offset(parsed.transport_header_offset + transport_header_len),
    );
    Ok(())
}

#[inline(always)]
fn source_check_passes(snapshot: &IpLocalSnapshot, parsed: &ParsedIpPacket) -> bool {
    let IpLocalSourceCheck::ReverseFib(handle) = &snapshot.source_check else {
        return true;
    };
    let fib = handle.load();
    let result = match parsed.source {
        IpAddr::V4(source) => fib.lookup_ip4(source, 0),
        IpAddr::V6(source) => fib.lookup_ip6(source, 0),
    };
    result.is_some_and(source_lookup_result_is_usable)
}

#[inline(always)]
fn source_lookup_result_is_usable(result: FibLookupResult) -> bool {
    !matches!(
        result.dpo.kind(),
        DpoType::DROP | DpoType::PUNT | DpoType::RECEIVE
    )
}

#[inline(always)]
fn error_for_input(error: IpInputError) -> IpLocalError {
    match error {
        IpInputError::BadChecksum => IpLocalError::BadChecksum,
        _ => IpLocalError::BadLength,
    }
}

#[inline(always)]
fn ip_protocol_number(protocol: IpProtocol) -> u8 {
    match protocol {
        IpProtocol::Icmpv4 => IP_PROTOCOL_ICMP,
        IpProtocol::Tcp => IP_PROTOCOL_TCP,
        IpProtocol::Udp => IP_PROTOCOL_UDP,
        IpProtocol::Icmpv6 => IP_PROTOCOL_ICMP6,
        IpProtocol::Other(value) => value,
    }
}

#[inline(always)]
fn l4_checksum(_packet: &[u8], parsed: &ParsedIpPacket, protocol: u8, segment: &[u8]) -> u16 {
    match parsed.version {
        IpVersion::V4 => {
            let mut pseudo = Vec::with_capacity(12 + segment.len());
            match (parsed.source, parsed.destination) {
                (IpAddr::V4(source), IpAddr::V4(destination)) => {
                    pseudo.extend_from_slice(&source.octets());
                    pseudo.extend_from_slice(&destination.octets());
                }
                _ => return 1,
            }
            pseudo.push(0);
            pseudo.push(protocol);
            pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
            pseudo.extend_from_slice(segment);
            internet_checksum(&pseudo)
        }
        IpVersion::V6 => {
            let mut pseudo = Vec::with_capacity(40 + segment.len());
            match (parsed.source, parsed.destination) {
                (IpAddr::V6(source), IpAddr::V6(destination)) => {
                    pseudo.extend_from_slice(&source.octets());
                    pseudo.extend_from_slice(&destination.octets());
                }
                _ => return 1,
            }
            pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
            pseudo.extend_from_slice(&[0, 0, 0, protocol]);
            pseudo.extend_from_slice(segment);
            internet_checksum(&pseudo)
        }
    }
}

#[inline(always)]
fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
