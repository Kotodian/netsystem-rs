use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hammer_adapter::{Network, RouteMetadata, SocksAddr};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpSeq};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpOutputSegment {
    pub lookup_id: TcpLookupId,
    pub connection_id: TcpConnectionId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub sequence: u32,
    pub acknowledgment: u32,
    pub flags: u8,
    pub advertised_window: u16,
    pub payload: std::vec::Vec<u8>,
    pub metadata: RouteMetadata,
    pub packet: std::vec::Vec<u8>,
}

impl TcpOutputSegment {
    #[inline]
    pub fn sequence_len(&self) -> u32 {
        let control_len =
            u32::from(self.flags & TCP_FLAG_SYN != 0) + u32::from(self.flags & TCP_FLAG_FIN != 0);
        self.payload.len() as u32 + control_len
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
    pub fn to_retransmit_segment(&self) -> Option<TcpOutputRetransmitSegment> {
        self.consumes_sequence_space()
            .then(|| TcpOutputRetransmitSegment {
                segment: self.clone(),
                next_sequence: self.next_send_sequence(),
                sent_at: None,
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpOutputRetransmitSegment {
    pub segment: TcpOutputSegment,
    pub next_sequence: u32,
    pub sent_at: Option<Instant>,
}

impl TcpOutputRetransmitSegment {
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
    segments: VecDeque<TcpOutputRetransmitSegment>,
}

impl TcpOutputRetransmitQueue {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    #[inline]
    pub fn front(&self) -> Option<&TcpOutputRetransmitSegment> {
        self.segments.front()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &TcpOutputRetransmitSegment> {
        self.segments.iter()
    }

    #[inline]
    pub fn track_segment(
        &mut self,
        segment: &TcpOutputSegment,
    ) -> Option<&TcpOutputRetransmitSegment> {
        let retransmit = segment.to_retransmit_segment()?;
        if let Some(existing) = self.segments.iter().position(|existing| {
            existing.segment.sequence == retransmit.segment.sequence
                && existing.next_sequence == retransmit.next_sequence
        }) {
            return self.segments.get(existing);
        }
        self.segments.push_back(retransmit);
        self.segments.back()
    }

    #[inline]
    pub fn track_segment_with_sent_at(
        &mut self,
        segment: &TcpOutputSegment,
        sent_at: Instant,
    ) -> Option<&TcpOutputRetransmitSegment> {
        let mut retransmit = segment.to_retransmit_segment()?;
        retransmit.sent_at = Some(sent_at);
        if let Some(existing) = self.segments.iter().position(|existing| {
            existing.segment.sequence == retransmit.segment.sequence
                && existing.next_sequence == retransmit.next_sequence
        }) {
            self.segments
                .get_mut(existing)
                .expect("tracked segment index should exist")
                .sent_at = Some(sent_at);
            return self.segments.get(existing);
        }
        self.segments.push_back(retransmit);
        self.segments.back()
    }

    #[inline]
    pub fn acknowledge_through(&mut self, acknowledgment: u32) -> usize {
        let mut released = 0usize;
        while self
            .segments
            .front()
            .is_some_and(|segment| segment.is_fully_acked_by(acknowledgment))
        {
            let _ = self.segments.pop_front();
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
            .segments
            .front()
            .is_some_and(|segment| segment.is_fully_acked_by(acknowledgment))
        {
            let segment = self
                .segments
                .pop_front()
                .expect("front segment should be present after ACK check");
            sample.bytes_acked += segment.segment.sequence_len();
            if let Some(sent_at) = segment.sent_at {
                sample.latest_rtt = Some(now.saturating_duration_since(sent_at));
            }
            sample.released_segments += 1;
        }
        sample
    }
}

pub trait TcpOutputBackend: Send + Sync {
    fn emit_segment(&self, segment: TcpOutputSegment) -> CoreResult<()>;
}

#[derive(Debug, Default)]
pub struct NoopTcpOutputBackend;

impl TcpOutputBackend for NoopTcpOutputBackend {
    #[inline]
    fn emit_segment(&self, _segment: TcpOutputSegment) -> CoreResult<()> {
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
    pub fn emit_segment(&self, segment: TcpOutputSegment) -> CoreResult<()> {
        let backend = self
            .inner
            .lock()
            .map_err(|_| CoreError::internal("tcp output backend poisoned"))?
            .clone();
        backend.emit_segment(segment)
    }
}

pub fn build_tcp_output_segment(
    snapshot: TcpConnectionSnapshot,
    local: SocketAddr,
    payload: &[u8],
) -> CoreResult<TcpOutputSegment> {
    let flags = TCP_FLAG_ACK | u8::from(!payload.is_empty()) * TCP_FLAG_PSH;
    build_tcp_output_segment_with_flags(snapshot, local, payload, flags)
}

#[inline]
pub fn tcp_available_send_window(view: TcpOutputSendView) -> u32 {
    view.snd_wnd
        .min(view.congestion_window)
        .saturating_sub(tcp_inflight_sequence_len(view.snd_una, view.snd_nxt))
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

pub fn build_tcp_output_segment_with_flags(
    snapshot: TcpConnectionSnapshot,
    local: SocketAddr,
    payload: &[u8],
    flags: u8,
) -> CoreResult<TcpOutputSegment> {
    let connection_id = snapshot
        .connection_id
        .ok_or_else(|| CoreError::internal("tcp output requires an installed connection id"))?;
    let remote = snapshot.remote;
    let sequence = tcp_output_sequence(snapshot);
    let acknowledgment = tcp_output_acknowledgment(snapshot);
    let advertised_window = snapshot.rcv_wnd.min(u32::from(u16::MAX)) as u16;
    let flags = normalize_tcp_output_flags(flags, payload);
    let packet = match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => build_ipv4_segment(
            local_ip,
            local.port(),
            remote_ip,
            remote.port(),
            sequence,
            acknowledgment,
            flags,
            advertised_window,
            payload,
        ),
        (IpAddr::V6(local_ip), IpAddr::V6(remote_ip)) => build_ipv6_segment(
            local_ip,
            local.port(),
            remote_ip,
            remote.port(),
            sequence,
            acknowledgment,
            flags,
            advertised_window,
            payload,
        ),
        _ => {
            return Err(CoreError::internal(format!(
                "tcp output mixes IP versions: local={local} remote={remote}"
            )));
        }
    };
    Ok(TcpOutputSegment {
        lookup_id: snapshot.lookup_id,
        connection_id,
        local,
        remote,
        sequence,
        acknowledgment,
        flags,
        advertised_window,
        payload: payload.to_vec(),
        metadata: RouteMetadata {
            network: Network::Tcp,
            source: Some(SocksAddr::ip(local.ip(), local.port())),
            destination: Some(SocksAddr::ip(remote.ip(), remote.port())),
            ..RouteMetadata::default()
        },
        packet,
    })
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
fn normalize_tcp_output_flags(flags: u8, payload: &[u8]) -> u8 {
    if payload.is_empty() {
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

fn build_ipv4_segment(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
    payload: &[u8],
) -> std::vec::Vec<u8> {
    let total_len = IPV4_HEADER_LEN + TCP_HEADER_LEN + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    write_tcp_segment(
        &mut packet[IPV4_HEADER_LEN..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
        advertised_window,
        payload,
    );
    let checksum = ipv4_l4_checksum(source, destination, 6, &packet[IPV4_HEADER_LEN..]);
    packet[IPV4_HEADER_LEN + 16..IPV4_HEADER_LEN + 18].copy_from_slice(&checksum.to_be_bytes());
    let header_checksum = internet_checksum(&packet[..IPV4_HEADER_LEN]);
    packet[10..12].copy_from_slice(&header_checksum.to_be_bytes());
    packet
}

fn build_ipv6_segment(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
    payload: &[u8],
) -> std::vec::Vec<u8> {
    let payload_len = TCP_HEADER_LEN + payload.len();
    let total_len = IPV6_HEADER_LEN + payload_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 6;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    write_tcp_segment(
        &mut packet[IPV6_HEADER_LEN..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
        advertised_window,
        payload,
    );
    let checksum = ipv6_l4_checksum(source, destination, 6, &packet[IPV6_HEADER_LEN..]);
    packet[IPV6_HEADER_LEN + 16..IPV6_HEADER_LEN + 18].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn write_tcp_segment(
    out: &mut [u8],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    advertised_window: u16,
    payload: &[u8],
) {
    out[..2].copy_from_slice(&source_port.to_be_bytes());
    out[2..4].copy_from_slice(&destination_port.to_be_bytes());
    out[4..8].copy_from_slice(&sequence.to_be_bytes());
    out[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
    out[12] = 0x50;
    out[13] = flags;
    out[14..16].copy_from_slice(&advertised_window.to_be_bytes());
    out[20..20 + payload.len()].copy_from_slice(payload);
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(40 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, protocol]);
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            u16::from_be_bytes([chunk[0], 0])
        };
        sum = sum.wrapping_add(u32::from(word));
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
