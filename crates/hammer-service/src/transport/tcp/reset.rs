use std::cell::RefCell;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;

use super::TcpInputError;
use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, Node, NodeId,
    NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::ip::parse_ip_packet;
use hammer_core::protocol::tcp::TcpSegmentFlags;
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

#[hammer_component_macros::node_next]
pub enum TcpResetNext {
    Drop,
    Lookup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpResetReason {
    AckInvalid,
    ConnectionClosed,
    Other(u16),
    MissingNodeError,
}

impl TcpResetReason {
    #[inline]
    fn from_node_error_code(code: Option<u16>) -> Self {
        match code {
            Some(code) if code == TcpInputError::AckInvalid.code() => Self::AckInvalid,
            Some(code) if code == TcpInputError::ConnectionClosed.code() => Self::ConnectionClosed,
            Some(code) => Self::Other(code),
            None => Self::MissingNodeError,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResetObservation {
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub reason: TcpResetReason,
    pub synthesized_reset: Option<TcpSynthesizedReset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpSynthesizedReset {
    pub packet: std::vec::Vec<u8>,
}

pub trait TcpResetObserver: Send + Sync {
    fn observe_reset(&self, observation: TcpResetObservation) -> CoreResult<()>;
}

struct TcpResetObserverHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    observe: fn(*const (), TcpResetObservation) -> CoreResult<()>,
}

unsafe impl Send for TcpResetObserverHandle {}
unsafe impl Sync for TcpResetObserverHandle {}

impl Default for TcpResetObserverHandle {
    #[inline]
    fn default() -> Self {
        Self::noop()
    }
}

impl Clone for TcpResetObserverHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            observe: self.observe,
        }
    }
}

impl Drop for TcpResetObserverHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpResetObserverHandle {
    #[inline]
    fn noop() -> Self {
        Self {
            raw: std::ptr::null(),
            clone_raw: clone_noop_handle,
            drop_raw: drop_noop_handle,
            observe: observe_noop_reset,
        }
    }

    #[inline]
    fn new<O>(observer: Arc<O>) -> Self
    where
        O: TcpResetObserver + 'static,
    {
        Self {
            raw: Arc::into_raw(observer) as *const (),
            clone_raw: clone_arc_handle::<O>,
            drop_raw: drop_arc_handle::<O>,
            observe: observe_reset_with::<O>,
        }
    }

    #[inline]
    fn is_registered(&self) -> bool {
        !self.raw.is_null()
    }

    #[inline]
    fn observe_reset(&self, observation: TcpResetObservation) -> CoreResult<()> {
        (self.observe)(self.raw, observation)
    }
}

#[inline]
fn clone_noop_handle(_: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_handle(_: *const ()) {}

#[inline]
fn observe_noop_reset(_: *const (), _: TcpResetObservation) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn clone_arc_handle<O>(raw: *const ()) -> *const ()
where
    O: TcpResetObserver + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            Arc::increment_strong_count(raw);
        }
    }
    raw.cast()
}

#[inline]
fn drop_arc_handle<O>(raw: *const ())
where
    O: TcpResetObserver + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            drop(Arc::from_raw(raw));
        }
    }
}

#[inline]
fn observe_reset_with<O>(raw: *const (), observation: TcpResetObservation) -> CoreResult<()>
where
    O: TcpResetObserver + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_reset(observation) }
}

#[derive(Clone, Default)]
struct TcpResetRuntime {
    observer: TcpResetObserverHandle,
}

thread_local! {
    static TCP_RESET_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpResetRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

#[inline]
fn has_tcp_reset_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_reset_runtime(observer: TcpResetObserverHandle) -> CoreResult<NodeRuntimeData> {
    TCP_RESET_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpResetRuntime { observer });
        Ok(NodeRuntimeData::from_words([
            u64::try_from(slot)
                .map_err(|_| CoreError::internal("TCP reset runtime slot overflow"))?,
            1,
            0,
            0,
        ]))
    })
}

fn tcp_reset_runtime(data: NodeRuntimeData) -> CoreResult<TcpResetRuntime> {
    if !has_tcp_reset_runtime(data) {
        return Ok(TcpResetRuntime::default());
    }
    let slot = data.usize_word(0)?;
    TCP_RESET_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP reset runtime slot is invalid"))
    })
}

fn sync_tcp_reset_runtime(
    data: NodeRuntimeData,
    observer: TcpResetObserverHandle,
) -> CoreResult<()> {
    if !has_tcp_reset_runtime(data) {
        return Ok(());
    }
    let slot = data.usize_word(0)?;
    TCP_RESET_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP reset runtime slot is invalid"))?;
        runtime.observer = observer;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpResetNext)]
pub struct TcpResetNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    observer: TcpResetObserverHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl TcpResetNode {
    #[inline]
    pub fn with_observer<O>(mut self, observer: Arc<O>) -> CoreResult<Self>
    where
        O: TcpResetObserver + 'static,
    {
        let observer = TcpResetObserverHandle::new(observer);
        if has_tcp_reset_runtime(self.runtime_data) {
            sync_tcp_reset_runtime(self.runtime_data, observer.clone())?;
        } else {
            self.runtime_data = register_tcp_reset_runtime(observer.clone())?;
        }
        self.observer = observer;
        Ok(self)
    }
}

impl Node for TcpResetNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_reset_runtime(self.runtime_data, self.observer.clone())?;
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpResetNext::Drop as usize];
        let lookup_next = next[TcpResetNext::Lookup as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame(
            runtime,
            frame,
            prefetch_tcp_reset,
            |_, indices, nexts| {
                for (offset, index) in indices.iter().copied().enumerate() {
                    nexts[offset] = tcp_reset_next_for_index(
                        runtime,
                        index,
                        drop_next,
                        lookup_next,
                        &self.observer,
                    )?;
                }
                Ok(())
            },
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_reset_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_reset_runtime(self.runtime_data, self.observer.clone())?;
        Ok(self.runtime_data)
    }
}

fn tcp_reset_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_reset_runtime(data)?;
    let next = TcpResetNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpResetNext::Drop as usize];
    let lookup_next = next[TcpResetNext::Lookup as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame(
        runtime,
        frame,
        prefetch_tcp_reset,
        |_, indices, nexts| {
            for (offset, index) in indices.iter().copied().enumerate() {
                nexts[offset] = tcp_reset_next_for_index(
                    runtime,
                    index,
                    drop_next,
                    lookup_next,
                    &state.observer,
                )?;
            }
            Ok(())
        },
    )?;
    Ok(result)
}

#[inline(always)]
fn prefetch_tcp_reset(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
    for index in indices.iter().copied() {
        batch.prefetch_read(index);
    }
}

fn tcp_reset_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    lookup_next: NodeId,
    observer: &TcpResetObserverHandle,
) -> CoreResult<Option<NodeId>> {
    let observation = tcp_reset_observation(runtime, index)?;
    if observer.is_registered() {
        observer.observe_reset(observation.clone())?;
    }
    let Some(synthesized_reset) = observation.synthesized_reset else {
        return Ok(Some(drop_next));
    };
    replace_current_chain(runtime, index, &synthesized_reset.packet)?;
    refresh_synthesized_reset_metadata(runtime, index, &synthesized_reset)?;
    Ok(Some(lookup_next))
}

fn tcp_reset_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpResetObservation> {
    let packet = runtime.copy_current_chain(index)?;
    let cursor = runtime.packet_cursor(index)?;
    let (local, remote) = tcp_packet_addrs(&packet, cursor)?;
    let synthesized_reset = tcp_reset_reply_from_packet(&packet, cursor);
    let reason =
        TcpResetReason::from_node_error_code(runtime.node_error(index)?.map(|error| error.code()));
    Ok(TcpResetObservation {
        local,
        remote,
        reason,
        synthesized_reset,
    })
}

fn tcp_reset_reply_from_packet(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> Option<TcpSynthesizedReset> {
    let source_segment = parse_reset_source_segment(&packet, cursor)?;
    if source_segment.flags.contains(TcpSegmentFlags::RST) {
        return None;
    }
    match packet
        .get(cursor.network_header_offset())
        .copied()
        .map(|byte| byte >> 4)
    {
        Some(4) => synthesize_ipv4_tcp_reset(&packet, cursor, source_segment),
        Some(6) => synthesize_ipv6_tcp_reset(&packet, cursor, source_segment),
        _ => None,
    }
}

fn tcp_packet_addrs(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> CoreResult<(SocketAddr, SocketAddr)> {
    let parsed = parse_ip_packet(packet)
        .map_err(|_| CoreError::internal("tcp reset observation requires valid IP packet"))?;
    let transport = packet
        .get(parsed.transport_header_offset..parsed.packet_len)
        .ok_or_else(|| CoreError::internal("tcp reset observation requires transport header"))?;
    let segment = etherparse::TcpSlice::from_slice(transport)
        .map_err(|_| CoreError::internal("tcp reset observation requires TCP header"))?;
    if cursor.transport_header_offset() != parsed.transport_header_offset {
        return Err(CoreError::internal("tcp reset cursor mismatch"));
    }
    Ok((
        SocketAddr::new(parsed.destination, segment.destination_port()),
        SocketAddr::new(parsed.source, segment.source_port()),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResetSourceSegment {
    source_port: u16,
    destination_port: u16,
    sequence_number: u32,
    acknowledgment_number: Option<u32>,
    flags: TcpSegmentFlags,
    segment_len: usize,
}

fn parse_reset_source_segment(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> Option<ResetSourceSegment> {
    let available_len = packet.len().min(cursor.packet_len());
    let transport_offset = cursor.transport_header_offset();
    let payload_offset = cursor.transport_payload_offset();
    if transport_offset > available_len || payload_offset > available_len {
        return None;
    }
    let segment =
        etherparse::TcpSlice::from_slice(packet.get(transport_offset..available_len)?).ok()?;
    if payload_offset != transport_offset.checked_add(segment.header_len())? {
        return None;
    }
    let payload_len = available_len.checked_sub(payload_offset)?;
    let segment_len = payload_len
        .checked_add(usize::from(segment.syn()))?
        .checked_add(usize::from(segment.fin()))?;
    Some(ResetSourceSegment {
        source_port: segment.source_port(),
        destination_port: segment.destination_port(),
        sequence_number: segment.sequence_number(),
        acknowledgment_number: segment.ack().then(|| segment.acknowledgment_number()),
        flags: {
            let mut flags = TcpSegmentFlags::empty();
            flags.set(TcpSegmentFlags::NS, segment.ns());
            flags.set(TcpSegmentFlags::FIN, segment.fin());
            flags.set(TcpSegmentFlags::SYN, segment.syn());
            flags.set(TcpSegmentFlags::RST, segment.rst());
            flags.set(TcpSegmentFlags::PSH, segment.psh());
            flags.set(TcpSegmentFlags::ACK, segment.ack());
            flags.set(TcpSegmentFlags::URG, segment.urg());
            flags.set(TcpSegmentFlags::ECE, segment.ece());
            flags.set(TcpSegmentFlags::CWR, segment.cwr());
            flags
        },
        segment_len,
    })
}

fn synthesize_ipv4_tcp_reset(
    packet: &[u8],
    cursor: BufferPacketCursor,
    source_segment: ResetSourceSegment,
) -> Option<TcpSynthesizedReset> {
    const IPV4_MIN_HEADER_LEN: usize = 20;
    const TCP_MIN_HEADER_LEN: usize = 20;
    const TCP_FLAG_RST: u8 = 0x04;
    const TCP_FLAG_ACK: u8 = 0x10;

    if cursor.network_header_len() < IPV4_MIN_HEADER_LEN
        || cursor.transport_header_len() < TCP_MIN_HEADER_LEN
    {
        return None;
    }

    let available_len = packet.len().min(cursor.packet_len());
    let network_offset = cursor.network_header_offset();
    let transport_offset = cursor.transport_header_offset();
    if network_offset > available_len || transport_offset > available_len {
        return None;
    }

    let network_end = network_offset.checked_add(cursor.network_header_len())?;
    let transport_end = transport_offset.checked_add(cursor.transport_header_len())?;
    if network_end > available_len || transport_end > available_len {
        return None;
    }

    let version_ihl = *packet.get(network_offset)?;
    if version_ihl >> 4 != 4 {
        return None;
    }

    let ack_flag = source_segment.flags.contains(TcpSegmentFlags::ACK);
    let response_sequence = if ack_flag {
        source_segment.acknowledgment_number?
    } else {
        0
    };
    let response_acknowledgment = if ack_flag {
        0
    } else {
        source_segment
            .sequence_number
            .wrapping_add(u32::try_from(source_segment.segment_len).ok()?)
    };
    let response_flags = if ack_flag {
        TCP_FLAG_RST
    } else {
        TCP_FLAG_RST | TCP_FLAG_ACK
    };

    let total_len = IPV4_MIN_HEADER_LEN + TCP_MIN_HEADER_LEN;
    let mut reset = vec![0u8; total_len];
    reset[..IPV4_MIN_HEADER_LEN].copy_from_slice(&packet[network_offset..network_offset + 20]);
    reset[0] = 0x45;
    reset[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    reset[10] = 0;
    reset[11] = 0;
    reset[12..16].copy_from_slice(packet.get(network_offset + 16..network_offset + 20)?);
    reset[16..20].copy_from_slice(packet.get(network_offset + 12..network_offset + 16)?);

    reset[20..22].copy_from_slice(&source_segment.destination_port.to_be_bytes());
    reset[22..24].copy_from_slice(&source_segment.source_port.to_be_bytes());
    reset[24..28].copy_from_slice(&response_sequence.to_be_bytes());
    reset[28..32].copy_from_slice(&response_acknowledgment.to_be_bytes());
    reset[32] = 0x50;
    reset[33] = response_flags;
    reset[36] = 0;
    reset[37] = 0;

    let source = reset.get(12..16)?.try_into().ok()?;
    let destination = reset.get(16..20)?.try_into().ok()?;
    let checksum = ipv4_l4_checksum(source, destination, 6, &reset[20..]);
    reset[36..38].copy_from_slice(&checksum.to_be_bytes());
    let header_checksum = internet_checksum(&reset[..IPV4_MIN_HEADER_LEN]);
    reset[10..12].copy_from_slice(&header_checksum.to_be_bytes());

    Some(TcpSynthesizedReset { packet: reset })
}

fn synthesize_ipv6_tcp_reset(
    packet: &[u8],
    cursor: BufferPacketCursor,
    source_segment: ResetSourceSegment,
) -> Option<TcpSynthesizedReset> {
    const IPV6_HEADER_LEN: usize = 40;
    const TCP_MIN_HEADER_LEN: usize = 20;
    const TCP_FLAG_RST: u8 = 0x04;
    const TCP_FLAG_ACK: u8 = 0x10;

    if cursor.network_header_len() < IPV6_HEADER_LEN
        || cursor.transport_header_len() < TCP_MIN_HEADER_LEN
    {
        return None;
    }

    let available_len = packet.len().min(cursor.packet_len());
    let network_offset = cursor.network_header_offset();
    let transport_offset = cursor.transport_header_offset();
    if network_offset > available_len || transport_offset > available_len {
        return None;
    }

    let network_end = network_offset.checked_add(cursor.network_header_len())?;
    let transport_end = transport_offset.checked_add(cursor.transport_header_len())?;
    if network_end > available_len || transport_end > available_len {
        return None;
    }

    let version = *packet.get(network_offset)? >> 4;
    if version != 6 {
        return None;
    }

    let ack_flag = source_segment.flags.contains(TcpSegmentFlags::ACK);
    let response_sequence = if ack_flag {
        source_segment.acknowledgment_number?
    } else {
        0
    };
    let response_acknowledgment = if ack_flag {
        0
    } else {
        source_segment
            .sequence_number
            .wrapping_add(u32::try_from(source_segment.segment_len).ok()?)
    };
    let response_flags = if ack_flag {
        TCP_FLAG_RST
    } else {
        TCP_FLAG_RST | TCP_FLAG_ACK
    };

    let total_len = IPV6_HEADER_LEN + TCP_MIN_HEADER_LEN;
    let mut reset = vec![0u8; total_len];
    reset[..IPV6_HEADER_LEN].copy_from_slice(&packet[network_offset..network_offset + 40]);
    reset[0] = 0x60;
    reset[4..6].copy_from_slice(&(TCP_MIN_HEADER_LEN as u16).to_be_bytes());
    reset[8..24].copy_from_slice(packet.get(network_offset + 24..network_offset + 40)?);
    reset[24..40].copy_from_slice(packet.get(network_offset + 8..network_offset + 24)?);

    reset[40..42].copy_from_slice(&source_segment.destination_port.to_be_bytes());
    reset[42..44].copy_from_slice(&source_segment.source_port.to_be_bytes());
    reset[44..48].copy_from_slice(&response_sequence.to_be_bytes());
    reset[48..52].copy_from_slice(&response_acknowledgment.to_be_bytes());
    reset[52] = 0x50;
    reset[53] = response_flags;
    reset[56] = 0;
    reset[57] = 0;

    let source = Ipv6Addr::from(<[u8; 16]>::try_from(reset.get(8..24)?).ok()?);
    let destination = Ipv6Addr::from(<[u8; 16]>::try_from(reset.get(24..40)?).ok()?);
    let checksum = ipv6_l4_checksum(source, destination, 6, &reset[40..]);
    reset[56..58].copy_from_slice(&checksum.to_be_bytes());

    Some(TcpSynthesizedReset { packet: reset })
}

fn ipv4_l4_checksum(source: [u8; 4], destination: [u8; 4], protocol: u8, segment: &[u8]) -> u16 {
    internet_checksum_parts(&[
        &source,
        &destination,
        &[0, protocol],
        &(segment.len() as u16).to_be_bytes(),
        segment,
    ])
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    internet_checksum_parts(&[
        &source.octets(),
        &destination.octets(),
        &(segment.len() as u32).to_be_bytes(),
        &[0, 0, 0, protocol],
        segment,
    ])
}

#[inline(always)]
fn replace_current_chain(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    packet: &[u8],
) -> CoreResult<()> {
    runtime.truncate_chain(index, 0)?;
    runtime.append(index, packet)
}

#[inline(always)]
fn refresh_synthesized_reset_metadata(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    synthesized_reset: &TcpSynthesizedReset,
) -> CoreResult<()> {
    const IPV4_HEADER_LEN: usize = 20;
    const IPV6_HEADER_LEN: usize = 40;
    const TCP_HEADER_LEN: usize = 20;

    let network_header_len = match synthesized_reset
        .packet
        .first()
        .copied()
        .map(|byte| byte >> 4)
    {
        Some(4) => IPV4_HEADER_LEN,
        Some(6) => IPV6_HEADER_LEN,
        Some(other) => {
            return Err(CoreError::internal(format!(
                "tcp synthesized reset uses unsupported IP version {other}"
            )));
        }
        None => return Err(CoreError::internal("tcp synthesized reset packet is empty")),
    };

    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.clear_node_error();
    buffer.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(synthesized_reset.packet.len())
            .with_network_header(0, network_header_len)
            .with_transport_header(network_header_len, TCP_HEADER_LEN)
            .with_transport_payload_offset(network_header_len + TCP_HEADER_LEN),
    );
    Ok(())
}
