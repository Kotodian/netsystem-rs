use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, Node, NodeId, NodeProcessFn,
    NodeResult, NodeRuntimeData, NodeVectorDispatch, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec as InfraVec;

use super::TcpInputError;

#[hammer_component_macros::node_next]
pub enum TcpResetNext {
    Drop,
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
    pub metadata: RouteMetadata,
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
fn clone_noop_handle(_raw: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_handle(_raw: *const ()) {}

#[inline]
fn observe_noop_reset(_raw: *const (), _observation: TcpResetObservation) -> CoreResult<()> {
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
    static TCP_RESET_RUNTIMES: RefCell<InfraVec<TcpResetRuntime>> =
        const { RefCell::new(InfraVec::new()) };
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
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| tcp_reset_next_for_index(runtime, index, drop_next, &self.observer),
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
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_reset_next_for_index(runtime, index, drop_next, &state.observer)
    })?;
    Ok(result)
}

fn tcp_reset_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    observer: &TcpResetObserverHandle,
) -> CoreResult<Option<NodeId>> {
    if observer.is_registered() {
        observer.observe_reset(tcp_reset_observation(runtime, index)?)?;
    }
    Ok(Some(drop_next))
}

fn tcp_reset_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpResetObservation> {
    let metadata = runtime.metadata(index)?;
    let synthesized_reset = tcp_synthesized_reset(runtime, index, &metadata);
    let remote = socket_addr(
        metadata.source.clone(),
        "tcp reset observer requires remote source metadata",
    )?;
    let local = socket_addr(
        metadata.destination.clone(),
        "tcp reset observer requires local destination metadata",
    )?;
    let reason =
        TcpResetReason::from_node_error_code(runtime.node_error(index)?.map(|error| error.code()));
    Ok(TcpResetObservation {
        local,
        remote,
        reason,
        synthesized_reset,
    })
}

fn socket_addr(value: Option<SocksAddr>, missing: &'static str) -> CoreResult<SocketAddr> {
    let value = value.ok_or_else(|| CoreError::internal(missing))?;
    Ok(SocketAddr::new(value.host, value.port))
}

fn tcp_synthesized_reset(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    metadata: &RouteMetadata,
) -> Option<TcpSynthesizedReset> {
    let packet: std::vec::Vec<u8> = runtime.copy_current_chain(index).ok()?.into_iter().collect();
    let cursor = runtime.packet_cursor(index).ok()?;
    synthesize_ipv4_tcp_reset(&packet, cursor, metadata)
}

fn synthesize_ipv4_tcp_reset(
    packet: &[u8],
    cursor: BufferPacketCursor,
    metadata: &RouteMetadata,
) -> Option<TcpSynthesizedReset> {
    const IPV4_MIN_HEADER_LEN: usize = 20;
    const TCP_MIN_HEADER_LEN: usize = 20;
    const TCP_FLAG_FIN: u8 = 0x01;
    const TCP_FLAG_SYN: u8 = 0x02;
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
    let payload_offset = cursor.transport_payload_offset();
    if network_offset > available_len
        || transport_offset > available_len
        || payload_offset > available_len
    {
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

    let tcp = &packet[transport_offset..transport_end];
    let flags = *tcp.get(13)?;
    let ack_flag = flags & TCP_FLAG_ACK != 0;
    let sequence_number = u32::from_be_bytes(tcp.get(4..8)?.try_into().ok()?);
    let acknowledgment_number = u32::from_be_bytes(tcp.get(8..12)?.try_into().ok()?);
    let payload_len = available_len.checked_sub(payload_offset)?;
    let segment_len = payload_len
        .checked_add(usize::from(flags & TCP_FLAG_SYN != 0))?
        .checked_add(usize::from(flags & TCP_FLAG_FIN != 0))?;
    let response_sequence = if ack_flag { acknowledgment_number } else { 0 };
    let response_acknowledgment = if ack_flag {
        0
    } else {
        sequence_number.wrapping_add(u32::try_from(segment_len).ok()?)
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

    reset[20..22].copy_from_slice(tcp.get(2..4)?);
    reset[22..24].copy_from_slice(tcp.get(0..2)?);
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

    let mut response_metadata = metadata.clone();
    response_metadata.source = metadata.destination.clone();
    response_metadata.destination = metadata.source.clone();

    Some(TcpSynthesizedReset {
        metadata: response_metadata,
        packet: reset,
    })
}

fn ipv4_l4_checksum(source: [u8; 4], destination: [u8; 4], protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source);
    pseudo.extend_from_slice(&destination);
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = match chunk {
            [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
            [hi] => u16::from_be_bytes([*hi, 0]) as u32,
            _ => unreachable!(),
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
