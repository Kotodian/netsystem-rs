use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hammer_adapter::{BufferIndex, DataPlaneBuffers, Network, RouteMetadata, SocksAddr};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpSeq, TcpState};

use super::TcpLookupId;
use super::connection::TcpConnectionSnapshot;

pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
const IPV4_HEADER_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const TCP_HEADER_LEN: usize = 20;
pub const TCP_FLAG_FIN: u8 = 0x01;
pub const TCP_FLAG_SYN: u8 = 0x02;
pub const TCP_FLAG_PSH: u8 = 0x08;
pub const TCP_FLAG_ACK: u8 = 0x10;

#[inline]
pub const fn tcp_effective_output_payload_len(peer_max_segment_size: Option<u16>) -> usize {
    match peer_max_segment_size {
        Some(max_segment_size) if max_segment_size != 0 => {
            let max_segment_size = max_segment_size as usize;
            if max_segment_size < DEFAULT_TCP_OUTPUT_PAYLOAD_LEN {
                max_segment_size
            } else {
                DEFAULT_TCP_OUTPUT_PAYLOAD_LEN
            }
        }
        _ => DEFAULT_TCP_OUTPUT_PAYLOAD_LEN,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpOutputRecord {
    pub lookup_id: TcpLookupId,
    pub connection_id: TcpConnectionId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub sequence: u32,
    pub acknowledgment: u32,
    pub flags: u8,
    pub advertised_window: u16,
    pub payload_len: usize,
    pub metadata: RouteMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpQueuedPayload {
    pub id: u64,
    pub len: usize,
    pub offset: usize,
    pub tail_queued: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpOutputConnectionView {
    pub connection_id: Option<TcpConnectionId>,
    pub state: TcpState,
    pub local: Option<SocketAddr>,
    pub local_port: u16,
    pub remote: SocketAddr,
    pub send_state_initialized: bool,
    pub receive_state_initialized: bool,
    pub pending_fin: bool,
    pub output_payload_len: usize,
    pub next_output_at: Option<Instant>,
    pub persist_armed: bool,
    pub persist_deadline: Option<Instant>,
    pub send_view: TcpOutputSendView,
    pub iss: u32,
    pub snd_nxt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpOutputWorkItem {
    pub connection_id: TcpConnectionId,
    pub record: TcpOutputRecord,
    pub send_id: Option<u64>,
    pub send_queue_offset: usize,
    pub payload_len: usize,
    pub include_fin: bool,
    pub retransmit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpOutputDecision {
    Work(TcpOutputWorkItem),
    WaitUntil {
        connection_id: TcpConnectionId,
        deadline: Instant,
    },
    PersistUntil {
        connection_id: TcpConnectionId,
        deadline: Instant,
    },
    None,
}

impl TcpOutputRecord {
    #[inline]
    pub fn sequence_len(&self) -> u32 {
        let control_len =
            u32::from(self.flags & TCP_FLAG_SYN != 0) + u32::from(self.flags & TCP_FLAG_FIN != 0);
        self.payload_len as u32 + control_len
    }

    #[inline]
    pub fn consumes_sequence_space(&self) -> bool {
        self.sequence_len() != 0
    }

    #[inline]
    pub fn next_send_sequence(&self) -> u32 {
        TcpSeq::new(self.sequence)
            .advance(self.sequence_len())
            .raw()
    }

    #[inline]
    pub fn to_retransmit_record(&self) -> Option<TcpOutputRetransmitRecord> {
        self.consumes_sequence_space()
            .then(|| TcpOutputRetransmitRecord {
                record: self.clone(),
                next_sequence: self.next_send_sequence(),
                sent_at: None,
            })
    }

    #[inline]
    pub fn alloc_header_buffer(&self, buffers: &DataPlaneBuffers) -> CoreResult<BufferIndex> {
        let index = buffers.alloc_index(self.metadata.clone())?;
        let result = (|| {
            let mut buffer = buffers.get_buffer_mut(index)?;
            let output = buffer.writable_tail_mut();
            let written = write_packet_headers(
                output,
                self.local,
                self.remote,
                self.sequence,
                self.acknowledgment,
                self.flags,
                self.advertised_window,
                self.payload_len,
            )?;
            buffer.commit_writable_tail(written)?;
            Ok(())
        })();
        if let Err(err) = result {
            buffers.free_index(index);
            return Err(err);
        }
        Ok(index)
    }

    #[inline]
    pub fn finalize_buffer_checksums(
        &self,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> CoreResult<()> {
        finalize_packet_checksums(buffers, index, self.local, self.remote)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpOutputRetransmitRecord {
    pub record: TcpOutputRecord,
    pub next_sequence: u32,
    pub sent_at: Option<Instant>,
}

impl TcpOutputRetransmitRecord {
    #[inline]
    pub fn is_fully_acked_by(&self, acknowledgment: u32) -> bool {
        !TcpSeq::new(acknowledgment).before(TcpSeq::new(self.next_sequence))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpAckDeliverySample {
    pub bytes_acked: u32,
    pub latest_rtt: Option<Duration>,
    pub released_segments: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpOutputSendView {
    pub snd_una: u32,
    pub snd_nxt: u32,
    pub snd_wnd: u32,
    pub congestion_window: u32,
}

impl TcpOutputSendView {
    #[inline]
    pub fn from_snapshot(snapshot: TcpConnectionSnapshot) -> Self {
        Self {
            snd_una: snapshot.snd_una,
            snd_nxt: snapshot.snd_nxt,
            snd_wnd: snapshot.snd_wnd,
            congestion_window: snapshot.snd_wnd,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpOutputRetransmitQueue {
    records: VecDeque<TcpOutputRetransmitRecord>,
}

impl TcpOutputRetransmitQueue {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[inline]
    pub fn front(&self) -> Option<&TcpOutputRetransmitRecord> {
        self.records.front()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &TcpOutputRetransmitRecord> {
        self.records.iter()
    }

    #[inline]
    pub fn track_output(&mut self, record: &TcpOutputRecord) -> Option<&TcpOutputRetransmitRecord> {
        let retransmit = record.to_retransmit_record()?;
        if let Some(existing) = self.records.iter().position(|existing| {
            existing.record.sequence == retransmit.record.sequence
                && existing.next_sequence == retransmit.next_sequence
        }) {
            return self.records.get(existing);
        }
        self.records.push_back(retransmit);
        self.records.back()
    }

    #[inline]
    pub fn track_output_with_sent_at(
        &mut self,
        record: &TcpOutputRecord,
        sent_at: Instant,
    ) -> Option<&TcpOutputRetransmitRecord> {
        let mut retransmit = record.to_retransmit_record()?;
        retransmit.sent_at = Some(sent_at);
        if let Some(existing) = self.records.iter().position(|existing| {
            existing.record.sequence == retransmit.record.sequence
                && existing.next_sequence == retransmit.next_sequence
        }) {
            self.records
                .get_mut(existing)
                .expect("tracked output index should exist")
                .sent_at = Some(sent_at);
            return self.records.get(existing);
        }
        self.records.push_back(retransmit);
        self.records.back()
    }

    #[inline]
    pub fn acknowledge_through(&mut self, acknowledgment: u32) -> usize {
        let mut released = 0usize;
        while self
            .records
            .front()
            .is_some_and(|record| record.is_fully_acked_by(acknowledgment))
        {
            let _ = self.records.pop_front();
            released += 1;
        }
        released
    }

    #[inline]
    pub fn acknowledge_through_with_sample(
        &mut self,
        acknowledgment: u32,
        now: Instant,
    ) -> TcpAckDeliverySample {
        let mut sample = TcpAckDeliverySample::default();
        while self
            .records
            .front()
            .is_some_and(|record| record.is_fully_acked_by(acknowledgment))
        {
            let record = self
                .records
                .pop_front()
                .expect("front output record should be present after ACK check");
            sample.bytes_acked += record.record.sequence_len();
            if let Some(sent_at) = record.sent_at {
                sample.latest_rtt = Some(now.saturating_duration_since(sent_at));
            }
            sample.released_segments += 1;
        }
        sample
    }
}

pub trait TcpOutputBackend: Send + Sync {
    /// Borrow an already-built TCP output buffer chain.
    ///
    /// Ownership stays with the caller so TCP can keep application buffers
    /// alive for retransmission until ACK progress releases them.
    fn emit_buffer(&self, buffers: &DataPlaneBuffers, index: BufferIndex) -> CoreResult<()>;
}

#[derive(Debug, Default)]
pub struct NoopTcpOutputBackend;

impl TcpOutputBackend for NoopTcpOutputBackend {
    #[inline]
    fn emit_buffer(&self, _buffers: &DataPlaneBuffers, _index: BufferIndex) -> CoreResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct TcpOutputBackendSlot {
    inner: Arc<Mutex<Arc<dyn TcpOutputBackend>>>,
}

impl Default for TcpOutputBackendSlot {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl TcpOutputBackendSlot {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Arc::new(NoopTcpOutputBackend))),
        }
    }

    #[inline]
    pub fn install<O>(&self, backend: Arc<O>)
    where
        O: TcpOutputBackend + 'static,
    {
        let mut slot = self.inner.lock().expect("tcp output backend poisoned");
        *slot = backend;
    }

    #[inline]
    pub fn emit_buffer(&self, buffers: &DataPlaneBuffers, index: BufferIndex) -> CoreResult<()> {
        let backend = self
            .inner
            .lock()
            .map_err(|_| CoreError::internal("tcp output backend poisoned"))?
            .clone();
        backend.emit_buffer(buffers, index)
    }
}

impl TcpOutputBackend for TcpOutputBackendSlot {
    #[inline]
    fn emit_buffer(&self, buffers: &DataPlaneBuffers, index: BufferIndex) -> CoreResult<()> {
        TcpOutputBackendSlot::emit_buffer(self, buffers, index)
    }
}

pub fn tcp_output_packet(
    snapshot: TcpConnectionSnapshot,
    local: SocketAddr,
    payload: &[u8],
) -> CoreResult<TcpOutputRecord> {
    let flags = TCP_FLAG_ACK | u8::from(!payload.is_empty()) * TCP_FLAG_PSH;
    tcp_output_packet_flags(snapshot, local, payload, flags)
}

pub fn tcp_output_decision(
    snapshot: TcpConnectionSnapshot,
    view: TcpOutputConnectionView,
    queued: Option<TcpQueuedPayload>,
    retransmit_pending: bool,
    retransmit_record: Option<TcpOutputRecord>,
    now: Instant,
) -> CoreResult<TcpOutputDecision> {
    let Some(connection_id) = view.connection_id else {
        return Ok(TcpOutputDecision::None);
    };
    if retransmit_pending {
        let Some(record) = retransmit_record else {
            return Ok(TcpOutputDecision::None);
        };
        return Ok(TcpOutputDecision::Work(TcpOutputWorkItem {
            connection_id,
            payload_len: record.payload_len,
            include_fin: record.flags & TCP_FLAG_FIN != 0,
            retransmit: true,
            send_id: None,
            send_queue_offset: 0,
            record,
        }));
    }
    if view.state == TcpState::SynSent {
        if !view.send_state_initialized || view.snd_nxt != view.iss {
            return Ok(TcpOutputDecision::None);
        }
        let local = view.local.unwrap_or_else(|| {
            SocketAddr::new(unspecified_ip_for_output(view.remote.ip()), view.local_port)
        });
        let record = tcp_output_packet_len(snapshot, local, 0, TCP_FLAG_SYN)?;
        return Ok(TcpOutputDecision::Work(TcpOutputWorkItem {
            connection_id,
            record,
            send_id: None,
            send_queue_offset: 0,
            payload_len: 0,
            include_fin: false,
            retransmit: false,
        }));
    }
    if !tcp_state_allows_output(view.state) {
        return Ok(TcpOutputDecision::None);
    }
    let Some(local) = view.local else {
        return Ok(TcpOutputDecision::None);
    };
    if queued.is_none() && !view.pending_fin {
        return Ok(TcpOutputDecision::None);
    }
    if !(view.send_state_initialized && view.receive_state_initialized) {
        return Ok(TcpOutputDecision::None);
    }
    if let Some(next_output_at) = view.next_output_at
        && next_output_at > now
    {
        return Ok(TcpOutputDecision::WaitUntil {
            connection_id,
            deadline: next_output_at,
        });
    }
    let has_queued_payload = queued.is_some();
    if (has_queued_payload || view.pending_fin) && view.send_view.snd_wnd == 0 {
        if view.persist_armed {
            return Ok(TcpOutputDecision::None);
        }
        let Some(deadline) = view.persist_deadline else {
            return Ok(TcpOutputDecision::None);
        };
        return Ok(TcpOutputDecision::PersistUntil {
            connection_id,
            deadline,
        });
    }
    let (payload_len, include_fin, send_id, send_queue_offset) = if let Some(staged) = queued {
        let requested_payload_len = staged.len.min(view.output_payload_len);
        let can_drain_payload = requested_payload_len == staged.len && !staged.tail_queued;
        let payload_len_with_fin = if view.pending_fin && can_drain_payload {
            tcp_payload_len_in_send_window(view.send_view, requested_payload_len, 1)
        } else {
            0
        };
        let include_fin =
            view.pending_fin && can_drain_payload && payload_len_with_fin == requested_payload_len;
        let payload_len = if include_fin {
            payload_len_with_fin
        } else {
            tcp_payload_len_in_send_window(view.send_view, requested_payload_len, 0)
        };
        (payload_len, include_fin, Some(staged.id), staged.offset)
    } else {
        (
            0,
            view.pending_fin && tcp_available_send_window(view.send_view) != 0,
            None,
            0,
        )
    };
    if payload_len == 0 && !include_fin {
        return Ok(TcpOutputDecision::None);
    }
    let flags = TCP_FLAG_ACK
        | if payload_len == 0 { 0 } else { TCP_FLAG_PSH }
        | if include_fin { TCP_FLAG_FIN } else { 0 };
    let record = tcp_output_packet_len(snapshot, local, payload_len, flags)?;
    Ok(TcpOutputDecision::Work(TcpOutputWorkItem {
        connection_id,
        record,
        send_id,
        send_queue_offset,
        payload_len,
        include_fin,
        retransmit: false,
    }))
}

#[inline]
pub fn tcp_available_send_window(view: TcpOutputSendView) -> u32 {
    view.snd_wnd
        .min(view.congestion_window)
        .saturating_sub(tcp_inflight_sequence_len(view.snd_una, view.snd_nxt))
}

#[inline]
fn tcp_state_allows_payload_send(state: TcpState) -> bool {
    matches!(state, TcpState::Established | TcpState::CloseWait)
}

#[inline]
fn tcp_state_allows_output(state: TcpState) -> bool {
    tcp_state_allows_payload_send(state) || matches!(state, TcpState::FinWait1 | TcpState::LastAck)
}

#[inline]
fn unspecified_ip_for_output(remote: IpAddr) -> IpAddr {
    match remote {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

#[inline]
pub fn tcp_payload_len_in_send_window(
    view: TcpOutputSendView,
    requested_payload_len: usize,
    control_len: u32,
) -> usize {
    let available_payload_len =
        tcp_available_send_window(view).saturating_sub(control_len) as usize;
    available_payload_len.min(requested_payload_len)
}

pub fn tcp_output_packet_flags(
    snapshot: TcpConnectionSnapshot,
    local: SocketAddr,
    payload: &[u8],
    flags: u8,
) -> CoreResult<TcpOutputRecord> {
    build_packet(snapshot, local, payload.len(), payload.is_empty(), flags)
}

pub fn tcp_output_packet_len(
    snapshot: TcpConnectionSnapshot,
    local: SocketAddr,
    payload_len: usize,
    flags: u8,
) -> CoreResult<TcpOutputRecord> {
    build_packet(snapshot, local, payload_len, payload_len == 0, flags)
}

fn build_packet(
    snapshot: TcpConnectionSnapshot,
    local: SocketAddr,
    payload_len: usize,
    payload_empty: bool,
    flags: u8,
) -> CoreResult<TcpOutputRecord> {
    let connection_id = snapshot
        .connection_id
        .ok_or_else(|| CoreError::internal("tcp output requires an installed connection id"))?;
    let remote = snapshot.remote;
    validate_tcp_output_lengths(local.ip(), TCP_HEADER_LEN, payload_len)?;
    let sequence = tcp_output_sequence(snapshot);
    let acknowledgment = tcp_output_acknowledgment(snapshot);
    let advertised_window = snapshot.rcv_wnd.min(u32::from(u16::MAX)) as u16;
    let flags = normalize_tcp_output_flags(flags, payload_empty);
    validate_tcp_output_address_family(local, remote)?;
    Ok(TcpOutputRecord {
        lookup_id: snapshot.lookup_id,
        connection_id,
        local,
        remote,
        sequence,
        acknowledgment,
        flags,
        advertised_window,
        payload_len,
        metadata: RouteMetadata {
            network: Network::Tcp,
            source: Some(SocksAddr::ip(local.ip(), local.port())),
            destination: Some(SocksAddr::ip(remote.ip(), remote.port())),
            ..RouteMetadata::default()
        },
    })
}

fn validate_tcp_output_address_family(local: SocketAddr, remote: SocketAddr) -> CoreResult<()> {
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_)) => Ok(()),
        _ => Err(CoreError::internal(format!(
            "tcp output mixes IP versions: local={local} remote={remote}"
        ))),
    }
}

fn validate_tcp_output_lengths(
    source: IpAddr,
    tcp_header_len: usize,
    payload_len: usize,
) -> CoreResult<()> {
    let transport_len = tcp_header_len
        .checked_add(payload_len)
        .ok_or_else(|| CoreError::internal("tcp output segment length overflows"))?;
    let packet_len = match source {
        IpAddr::V4(_) => IPV4_HEADER_LEN
            .checked_add(transport_len)
            .ok_or_else(|| CoreError::internal("tcp output ipv4 packet length overflows"))?,
        IpAddr::V6(_) => transport_len,
    };
    if packet_len > usize::from(u16::MAX) {
        return Err(CoreError::internal(format!(
            "tcp output packet exceeds wire length field: len={packet_len}"
        )));
    }

    Ok(())
}

#[inline]
fn tcp_inflight_sequence_len(snd_una: u32, snd_nxt: u32) -> u32 {
    if snd_una != 0 && snd_nxt != 0 {
        TcpSeq::new(snd_una).distance_to(TcpSeq::new(snd_nxt))
    } else {
        0
    }
}

#[inline]
fn normalize_tcp_output_flags(flags: u8, payload_empty: bool) -> u8 {
    if payload_empty {
        flags & !TCP_FLAG_PSH
    } else {
        flags
    }
}

#[inline]
fn tcp_output_sequence(snapshot: TcpConnectionSnapshot) -> u32 {
    if snapshot.snd_nxt != 0 {
        snapshot.snd_nxt
    } else if snapshot.snd_una != 0 {
        snapshot.snd_una
    } else if snapshot.iss != 0 {
        TcpSeq::new(snapshot.iss).advance(1).raw()
    } else {
        1
    }
}

#[inline]
fn tcp_output_acknowledgment(snapshot: TcpConnectionSnapshot) -> u32 {
    if snapshot.rcv_nxt != 0 {
        snapshot.rcv_nxt
    } else if snapshot.irs != 0 {
        TcpSeq::new(snapshot.irs).advance(1).raw()
    } else {
        1
    }
}

fn write_packet_headers(
    output: &mut [u8],
    local: SocketAddr,
    remote: SocketAddr,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
    payload_len: usize,
) -> CoreResult<usize> {
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => write_ipv4_headers(
            output,
            source,
            local.port(),
            destination,
            remote.port(),
            sequence,
            acknowledgment,
            flags,
            advertised_window,
            payload_len,
        ),
        (IpAddr::V6(source), IpAddr::V6(destination)) => write_ipv6_headers(
            output,
            source,
            local.port(),
            destination,
            remote.port(),
            sequence,
            acknowledgment,
            flags,
            advertised_window,
            payload_len,
        ),
        _ => Err(CoreError::internal(format!(
            "tcp output mixes IP versions: local={local} remote={remote}"
        ))),
    }
}

fn write_ipv4_headers(
    output: &mut [u8],
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
    payload_len: usize,
) -> CoreResult<usize> {
    let header_len = IPV4_HEADER_LEN + TCP_HEADER_LEN;
    let total_len = header_len + payload_len;
    if output.len() < header_len {
        return Err(CoreError::internal(format!(
            "tcp output buffer too small for ipv4 headers: {} < {}",
            output.len(),
            header_len
        )));
    }
    let packet = &mut output[..header_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    write_tcp_header(
        &mut packet[IPV4_HEADER_LEN..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
        advertised_window,
    );
    Ok(header_len)
}

fn write_ipv6_headers(
    output: &mut [u8],
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
    payload_len: usize,
) -> CoreResult<usize> {
    let header_len = IPV6_HEADER_LEN + TCP_HEADER_LEN;
    let ipv6_payload_len = TCP_HEADER_LEN + payload_len;
    if output.len() < header_len {
        return Err(CoreError::internal(format!(
            "tcp output buffer too small for ipv6 headers: {} < {}",
            output.len(),
            header_len
        )));
    }
    let packet = &mut output[..header_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(ipv6_payload_len as u16).to_be_bytes());
    packet[6] = 6;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    write_tcp_header(
        &mut packet[IPV6_HEADER_LEN..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
        advertised_window,
    );
    Ok(header_len)
}

fn finalize_packet_checksums(
    buffers: &DataPlaneBuffers,
    index: BufferIndex,
    local: SocketAddr,
    remote: SocketAddr,
) -> CoreResult<()> {
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            {
                let mut buffer = buffers.get_buffer_mut(index)?;
                let packet = buffer.current_mut();
                packet[10..12].fill(0);
                packet[IPV4_HEADER_LEN + 16..IPV4_HEADER_LEN + 18].fill(0);
            }
            let checksum =
                buffers.with_current_chain_io_segments(index, |segments, total_len| {
                    let first = segments
                        .first()
                        .ok_or_else(|| CoreError::internal("tcp output chain is empty"))?;
                    let transport = first.get(IPV4_HEADER_LEN..).ok_or_else(|| {
                        CoreError::internal("tcp output ipv4 header is incomplete")
                    })?;
                    let transport_len = total_len
                        .checked_sub(IPV4_HEADER_LEN)
                        .ok_or_else(|| CoreError::internal("tcp output ipv4 length underflows"))?;
                    Ok(ipv4_l4_checksum_chain(
                        source,
                        destination,
                        6,
                        transport,
                        &segments[1..],
                        transport_len,
                    ))
                })?;
            let mut buffer = buffers.get_buffer_mut(index)?;
            let packet = buffer.current_mut();
            packet[IPV4_HEADER_LEN + 16..IPV4_HEADER_LEN + 18]
                .copy_from_slice(&checksum.to_be_bytes());
            let header_checksum = internet_checksum(&packet[..IPV4_HEADER_LEN]);
            packet[10..12].copy_from_slice(&header_checksum.to_be_bytes());
            Ok(())
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            {
                let mut buffer = buffers.get_buffer_mut(index)?;
                let packet = buffer.current_mut();
                packet[IPV6_HEADER_LEN + 16..IPV6_HEADER_LEN + 18].fill(0);
            }
            let checksum =
                buffers.with_current_chain_io_segments(index, |segments, total_len| {
                    let first = segments
                        .first()
                        .ok_or_else(|| CoreError::internal("tcp output chain is empty"))?;
                    let transport = first.get(IPV6_HEADER_LEN..).ok_or_else(|| {
                        CoreError::internal("tcp output ipv6 header is incomplete")
                    })?;
                    let transport_len = total_len
                        .checked_sub(IPV6_HEADER_LEN)
                        .ok_or_else(|| CoreError::internal("tcp output ipv6 length underflows"))?;
                    Ok(ipv6_l4_checksum_chain(
                        source,
                        destination,
                        6,
                        transport,
                        &segments[1..],
                        transport_len,
                    ))
                })?;
            let mut buffer = buffers.get_buffer_mut(index)?;
            let packet = buffer.current_mut();
            packet[IPV6_HEADER_LEN + 16..IPV6_HEADER_LEN + 18]
                .copy_from_slice(&checksum.to_be_bytes());
            Ok(())
        }
        _ => Err(CoreError::internal(format!(
            "tcp output mixes IP versions: local={local} remote={remote}"
        ))),
    }
}

fn write_tcp_header(
    out: &mut [u8],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
) {
    out[..2].copy_from_slice(&source_port.to_be_bytes());
    out[2..4].copy_from_slice(&destination_port.to_be_bytes());
    out[4..8].copy_from_slice(&sequence.to_be_bytes());
    out[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
    out[12] = ((TCP_HEADER_LEN / 4) as u8) << 4;
    out[13] = flags;
    out[14..16].copy_from_slice(&advertised_window.to_be_bytes());
}

fn ipv4_l4_checksum_chain(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    first_transport: &[u8],
    tail: &[&[u8]],
    transport_len: usize,
) -> u16 {
    let mut checksum = InternetChecksum::new();
    checksum.update(&source.octets());
    checksum.update(&destination.octets());
    checksum.update(&[0, protocol]);
    checksum.update(&(transport_len as u16).to_be_bytes());
    checksum.update(first_transport);
    for segment in tail {
        checksum.update(segment);
    }
    checksum.finish()
}

fn ipv6_l4_checksum_chain(
    source: Ipv6Addr,
    destination: Ipv6Addr,
    protocol: u8,
    first_transport: &[u8],
    tail: &[&[u8]],
    transport_len: usize,
) -> u16 {
    let mut checksum = InternetChecksum::new();
    checksum.update(&source.octets());
    checksum.update(&destination.octets());
    checksum.update(&(transport_len as u32).to_be_bytes());
    checksum.update(&[0, 0, 0, protocol]);
    checksum.update(first_transport);
    for segment in tail {
        checksum.update(segment);
    }
    checksum.finish()
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut checksum = InternetChecksum::new();
    checksum.update(bytes);
    checksum.finish()
}

#[derive(Debug, Default)]
struct InternetChecksum {
    sum: u32,
    pending: Option<u8>,
}

impl InternetChecksum {
    #[inline]
    fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn update(&mut self, mut bytes: &[u8]) {
        if let Some(high) = self.pending.take() {
            if let Some((&low, rest)) = bytes.split_first() {
                self.sum = self
                    .sum
                    .wrapping_add(u32::from(u16::from_be_bytes([high, low])));
                bytes = rest;
            } else {
                self.pending = Some(high);
                return;
            }
        }
        let mut chunks = bytes.chunks_exact(2);
        for chunk in &mut chunks {
            self.sum = self
                .sum
                .wrapping_add(u32::from(u16::from_be_bytes([chunk[0], chunk[1]])));
        }
        if let [byte] = chunks.remainder() {
            self.pending = Some(*byte);
        }
    }

    #[inline]
    fn finish(mut self) -> u16 {
        if let Some(high) = self.pending.take() {
            self.sum = self
                .sum
                .wrapping_add(u32::from(u16::from_be_bytes([high, 0])));
        }
        while (self.sum >> 16) != 0 {
            self.sum = (self.sum & 0xffff) + (self.sum >> 16);
        }
        !(self.sum as u16)
    }
}
