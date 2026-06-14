use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId, TcpNegotiatedOptions, TcpSeq};

use super::TcpState;
use super::congestion::TcpCongestionState;
use super::output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpOutputRetransmitQueue, TcpOutputSendView,
    tcp_effective_output_payload_len,
};

const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;
const TCP_MAX_WINDOW_SCALE: u8 = 14;
const TCP_CONNECTION_TIMER_RETRANSMIT_BIT: u8 = 1 << 0;
pub const TCP_INITIAL_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MIN_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MAX_RETRANSMIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpConnectionTimerKind {
    Retransmit,
}

impl TcpConnectionTimerKind {
    #[inline(always)]
    const fn bit(self) -> u8 {
        match self {
            Self::Retransmit => TCP_CONNECTION_TIMER_RETRANSMIT_BIT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpRetransmitTimeoutState {
    srtt: Option<Duration>,
    rttvar: Option<Duration>,
    rto: Duration,
    skip_next_sample: bool,
}

impl TcpRetransmitTimeoutState {
    #[inline]
    pub fn new() -> Self {
        Self {
            srtt: None,
            rttvar: None,
            rto: TCP_INITIAL_RETRANSMIT_TIMEOUT,
            skip_next_sample: false,
        }
    }

    #[inline]
    pub fn smoothed_rtt(&self) -> Option<Duration> {
        self.srtt
    }

    #[inline]
    pub fn rtt_variance(&self) -> Option<Duration> {
        self.rttvar
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> Duration {
        self.rto
    }

    pub fn observe_ack_sample(&mut self, rtt: Duration) -> Duration {
        if self.skip_next_sample {
            self.skip_next_sample = false;
            return self.rto;
        }
        match (self.srtt, self.rttvar) {
            (Some(srtt), Some(rttvar)) => {
                let rtt_delta = duration_abs_diff(srtt, rtt);
                let next_rttvar = duration_weighted_average(rttvar, 3, rtt_delta, 1, 4);
                let next_srtt = duration_weighted_average(srtt, 7, rtt, 1, 8);
                self.srtt = Some(next_srtt);
                self.rttvar = Some(next_rttvar);
            }
            _ => {
                self.srtt = Some(rtt);
                self.rttvar = Some(duration_div(rtt, 2));
            }
        }
        self.rto = retransmit_timeout_from_estimate(
            self.srtt
                .expect("smoothed RTT should be initialized by ACK sample"),
            self.rttvar
                .expect("RTT variance should be initialized by ACK sample"),
        );
        self.rto
    }

    #[inline]
    pub fn on_retransmission_timeout(&mut self) -> Duration {
        self.rto = clamp_retransmit_timeout(duration_mul(self.rto, 2));
        self.skip_next_sample = true;
        self.rto
    }
}

impl Default for TcpRetransmitTimeoutState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectionOptionState {
    local_capabilities: TcpCapabilities,
    remote_capabilities: Option<TcpCapabilities>,
    negotiated: TcpNegotiatedOptions,
}

impl TcpConnectionOptionState {
    #[inline]
    pub fn new(local_capabilities: TcpCapabilities) -> Self {
        Self {
            local_capabilities: normalize_tcp_capabilities(local_capabilities),
            remote_capabilities: None,
            negotiated: TcpNegotiatedOptions::default(),
        }
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        self.local_capabilities
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        self.remote_capabilities
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.negotiated
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.local_capabilities = normalize_tcp_capabilities(capabilities);
        self.recalculate_negotiated_options();
        self.negotiated
    }

    #[inline]
    pub fn apply_peer_handshake_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.remote_capabilities = Some(normalize_tcp_capabilities(capabilities));
        self.recalculate_negotiated_options();
        self.negotiated
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        tcp_window_scale(self.negotiated.send_window_scale)
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        tcp_window_scale(self.negotiated.receive_window_scale)
    }

    #[inline]
    pub fn effective_send_window(&self, advertised_window: u32) -> u32 {
        scaled_window_from_advertised(advertised_window, self.effective_send_window_scale())
    }

    #[inline]
    pub fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        advertised_window_from_receive(receive_window, self.effective_receive_window_scale())
    }

    #[inline]
    fn recalculate_negotiated_options(&mut self) {
        self.negotiated = self
            .remote_capabilities
            .map(|remote| tcp_negotiate_options(self.local_capabilities, remote))
            .unwrap_or_default();
    }
}

impl Default for TcpConnectionOptionState {
    #[inline]
    fn default() -> Self {
        Self::new(TcpCapabilities::default())
    }
}

#[derive(Debug, Clone)]
pub struct TcpConnectionState {
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    state: TcpState,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    iss: u32,
    irs: u32,
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
    options: TcpConnectionOptionState,
    output_payload_len: usize,
    retransmit_queue: TcpOutputRetransmitQueue,
    retransmit_timeout: TcpRetransmitTimeoutState,
    congestion: TcpCongestionState,
    active_timers: u8,
    pending_timers: u8,
    next_output_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectionView {
    pub connection_id: Option<TcpConnectionId>,
    pub owner_worker: DataWorkerId,
    pub state: TcpState,
    pub local_port: u16,
    pub local: Option<SocketAddr>,
    pub remote: SocketAddr,
    pub iss: u32,
    pub irs: u32,
    pub snd_una: u32,
    pub snd_nxt: u32,
    pub snd_wnd: u32,
    pub rcv_nxt: u32,
    pub rcv_wnd: u32,
}

impl TcpConnectionState {
    #[inline]
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        state: TcpState,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            connection_id,
            owner_worker,
            state,
            local_port,
            local,
            remote,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: DEFAULT_TCP_WINDOW,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            options: TcpConnectionOptionState::default(),
            output_payload_len: tcp_effective_output_payload_len(None),
            retransmit_queue: TcpOutputRetransmitQueue::new(),
            retransmit_timeout: TcpRetransmitTimeoutState::new(),
            congestion: TcpCongestionState::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            active_timers: 0,
            pending_timers: 0,
            next_output_at: None,
        }
    }

    #[inline]
    pub fn connection_id(&self) -> Option<TcpConnectionId> {
        self.connection_id
    }

    #[inline]
    pub fn set_connection_id(&mut self, connection_id: TcpConnectionId) {
        self.connection_id = Some(connection_id);
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn state(&self) -> TcpState {
        self.state
    }

    #[inline]
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    #[inline]
    pub fn local(&self) -> Option<SocketAddr> {
        self.local
    }

    #[inline]
    pub fn remote(&self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub fn iss(&self) -> u32 {
        self.iss
    }

    #[inline]
    pub fn irs(&self) -> u32 {
        self.irs
    }

    #[inline]
    pub fn snd_una(&self) -> u32 {
        self.snd_una
    }

    #[inline]
    pub fn snd_nxt(&self) -> u32 {
        self.snd_nxt
    }

    #[inline]
    pub fn snd_wnd(&self) -> u32 {
        self.snd_wnd
    }

    #[inline]
    pub fn rcv_nxt(&self) -> u32 {
        self.rcv_nxt
    }

    #[inline]
    pub fn rcv_wnd(&self) -> u32 {
        self.rcv_wnd
    }

    #[inline]
    pub fn output_payload_len(&self) -> usize {
        self.output_payload_len
    }

    #[inline]
    pub fn apply_peer_max_segment_size(&mut self, max_segment_size: Option<u16>) {
        if let Some(max_segment_size) =
            max_segment_size.filter(|max_segment_size| *max_segment_size != 0)
        {
            self.output_payload_len = tcp_effective_output_payload_len(Some(max_segment_size));
            self.congestion = TcpCongestionState::new(u32::from(max_segment_size));
        }
    }

    #[inline]
    pub fn option_state(&self) -> &TcpConnectionOptionState {
        &self.options
    }

    #[inline]
    pub fn option_state_mut(&mut self) -> &mut TcpConnectionOptionState {
        &mut self.options
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        self.options.local_capabilities()
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        self.options.remote_capabilities()
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.options.negotiated_options()
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.options.set_local_capabilities(capabilities)
    }

    #[inline]
    pub fn apply_peer_handshake_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        let negotiated = self.options.apply_peer_handshake_capabilities(capabilities);
        self.apply_peer_max_segment_size(negotiated.send_max_segment_size);
        negotiated
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        self.options.effective_send_window_scale()
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        self.options.effective_receive_window_scale()
    }

    #[inline]
    pub fn effective_send_window(&self, advertised_window: u32) -> u32 {
        self.options.effective_send_window(advertised_window)
    }

    #[inline]
    pub fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        self.options.advertised_receive_window(receive_window)
    }

    #[inline]
    pub fn congestion(&self) -> &TcpCongestionState {
        &self.congestion
    }

    #[inline]
    pub fn congestion_mut(&mut self) -> &mut TcpCongestionState {
        &mut self.congestion
    }

    #[inline]
    pub fn retransmit_queue(&self) -> &TcpOutputRetransmitQueue {
        &self.retransmit_queue
    }

    #[inline]
    pub fn retransmit_queue_mut(&mut self) -> &mut TcpOutputRetransmitQueue {
        &mut self.retransmit_queue
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> &TcpRetransmitTimeoutState {
        &self.retransmit_timeout
    }

    #[inline]
    pub fn retransmit_timeout_mut(&mut self) -> &mut TcpRetransmitTimeoutState {
        &mut self.retransmit_timeout
    }

    #[inline]
    pub fn tcp_timer_set(&mut self, timer: TcpConnectionTimerKind) {
        self.active_timers |= timer.bit();
    }

    #[inline]
    pub fn tcp_timer_reset(&mut self, timer: TcpConnectionTimerKind) {
        let bit = timer.bit();
        self.active_timers &= !bit;
        self.pending_timers &= !bit;
    }

    #[inline]
    pub fn tcp_timer_expire(&mut self, timer: TcpConnectionTimerKind) {
        let bit = timer.bit();
        self.active_timers &= !bit;
        self.pending_timers |= bit;
    }

    #[inline]
    pub fn tcp_timer_take_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        let bit = timer.bit();
        if self.pending_timers & bit == 0 {
            return false;
        }
        self.pending_timers &= !bit;
        true
    }

    #[inline]
    pub fn tcp_timer_dispatch_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        let bit = timer.bit();
        if self.pending_timers & bit == 0 {
            return false;
        }
        self.pending_timers &= !bit;
        self.active_timers & bit == 0
    }

    #[inline]
    pub fn tcp_timer_is_active(&self, timer: TcpConnectionTimerKind) -> bool {
        self.active_timers & timer.bit() != 0
    }

    #[inline]
    pub fn tcp_timer_is_pending(&self, timer: TcpConnectionTimerKind) -> bool {
        self.pending_timers & timer.bit() != 0
    }

    #[inline]
    pub fn tcp_timer_is_live(&self, timer: TcpConnectionTimerKind) -> bool {
        self.tcp_timer_is_active(timer) || self.tcp_timer_is_pending(timer)
    }

    #[inline]
    pub fn output_send_view(&self) -> TcpOutputSendView {
        TcpOutputSendView {
            snd_una: self.snd_una,
            snd_nxt: self.snd_nxt,
            snd_wnd: self.snd_wnd,
            congestion_window: self.congestion.congestion_window(),
        }
    }

    #[inline]
    pub fn view(&self) -> TcpConnectionView {
        TcpConnectionView {
            connection_id: self.connection_id,
            owner_worker: self.owner_worker,
            state: self.state,
            local_port: self.local_port,
            local: self.local,
            remote: self.remote,
            iss: self.iss,
            irs: self.irs,
            snd_una: self.snd_una,
            snd_nxt: self.snd_nxt,
            snd_wnd: self.snd_wnd,
            rcv_nxt: self.rcv_nxt,
            rcv_wnd: self.rcv_wnd,
        }
    }

    #[inline]
    pub fn set_send_state(&mut self, snd_una: u32, snd_nxt: u32, snd_wnd: u32) {
        self.snd_una = snd_una;
        self.snd_nxt = snd_nxt;
        self.snd_wnd = self.effective_send_window(snd_wnd);
    }

    #[inline]
    pub fn set_receive_state(&mut self, rcv_nxt: u32, rcv_wnd: u32) {
        self.rcv_nxt = rcv_nxt;
        self.rcv_wnd = rcv_wnd;
    }

    #[inline]
    pub fn set_sequence_state(
        &mut self,
        iss: u32,
        irs: u32,
        snd_una: u32,
        snd_nxt: u32,
        snd_wnd: u32,
        rcv_nxt: u32,
        rcv_wnd: u32,
    ) {
        self.iss = iss;
        self.irs = irs;
        self.snd_una = snd_una;
        self.snd_nxt = snd_nxt;
        self.snd_wnd = snd_wnd;
        self.rcv_nxt = rcv_nxt;
        self.rcv_wnd = rcv_wnd;
    }

    #[inline]
    pub fn set_state(&mut self, state: TcpState) {
        self.state = state;
    }

    #[inline]
    pub fn initialize_passive_open(&mut self, iss: u32, peer_sequence: u32, peer_window: u16) {
        self.iss = iss;
        self.irs = peer_sequence;
        self.snd_una = iss;
        self.snd_nxt = TcpSeq::new(iss).advance(1).raw();
        self.snd_wnd = self.effective_send_window(u32::from(peer_window));
        self.rcv_nxt = TcpSeq::new(peer_sequence).advance(1).raw();
        self.state = TcpState::SynRcvd;
    }

    #[inline]
    pub fn accepts_ack(&self, acknowledgment: u32) -> bool {
        !TcpSeq::new(acknowledgment).before(TcpSeq::new(self.snd_una))
            && !TcpSeq::new(acknowledgment).after(TcpSeq::new(self.snd_nxt))
    }

    #[inline]
    pub fn apply_ack(&mut self, acknowledgment: u32, advertised_window: u16) {
        if self.accepts_ack(acknowledgment) {
            self.snd_una = acknowledgment;
        }
        self.snd_wnd = self.effective_send_window(u32::from(advertised_window));
        self.retransmit_queue.acknowledge_through(acknowledgment);
    }

    #[inline]
    pub fn accept_in_order_payload(&mut self, sequence: u32, payload_len: usize) -> bool {
        if sequence != self.rcv_nxt {
            return false;
        }
        self.rcv_nxt = TcpSeq::new(self.rcv_nxt).advance(payload_len as u32).raw();
        true
    }

    #[inline]
    pub fn next_output_at(&self) -> Option<Instant> {
        self.next_output_at
    }

    #[inline]
    pub fn set_next_output_at(&mut self, deadline: Option<Instant>) {
        self.next_output_at = deadline;
    }
}

#[inline]
fn tcp_negotiate_options(local: TcpCapabilities, remote: TcpCapabilities) -> TcpNegotiatedOptions {
    let (send_window_scale, receive_window_scale) = match (local.window_scale, remote.window_scale)
    {
        (Some(local_scale), Some(remote_scale)) => (Some(remote_scale), Some(local_scale)),
        _ => (None, None),
    };
    TcpNegotiatedOptions {
        send_max_segment_size: remote.max_segment_size,
        receive_max_segment_size: local.max_segment_size,
        send_window_scale,
        receive_window_scale,
        sack: local.sack && remote.sack,
        timestamps: local.timestamps && remote.timestamps,
        ecn: local.ecn && remote.ecn,
    }
}

#[inline]
fn normalize_tcp_capabilities(capabilities: TcpCapabilities) -> TcpCapabilities {
    TcpCapabilities {
        max_segment_size: capabilities
            .max_segment_size
            .filter(|max_segment_size| *max_segment_size != 0),
        window_scale: capabilities
            .window_scale
            .map(|scale| scale.min(TCP_MAX_WINDOW_SCALE)),
        sack: capabilities.sack,
        timestamps: capabilities.timestamps,
        ecn: capabilities.ecn,
    }
}

#[inline]
fn tcp_window_scale(window_scale: Option<u8>) -> u8 {
    window_scale.unwrap_or_default().min(TCP_MAX_WINDOW_SCALE)
}

#[inline]
fn scaled_window_from_advertised(advertised_window: u32, window_scale: u8) -> u32 {
    advertised_window.saturating_mul(1_u32 << tcp_window_scale(Some(window_scale)))
}

#[inline]
fn advertised_window_from_receive(receive_window: u32, window_scale: u8) -> u16 {
    (receive_window >> tcp_window_scale(Some(window_scale))).min(u32::from(u16::MAX)) as u16
}

#[inline]
fn retransmit_timeout_from_estimate(srtt: Duration, rttvar: Duration) -> Duration {
    clamp_retransmit_timeout(duration_add(srtt, duration_mul(rttvar, 4)))
}

#[inline]
fn clamp_retransmit_timeout(timeout: Duration) -> Duration {
    if timeout < TCP_MIN_RETRANSMIT_TIMEOUT {
        TCP_MIN_RETRANSMIT_TIMEOUT
    } else if timeout > TCP_MAX_RETRANSMIT_TIMEOUT {
        TCP_MAX_RETRANSMIT_TIMEOUT
    } else {
        timeout
    }
}

#[inline]
fn duration_weighted_average(
    left: Duration,
    left_weight: u32,
    right: Duration,
    right_weight: u32,
    denominator: u32,
) -> Duration {
    duration_from_nanos_saturating(
        left.as_nanos()
            .saturating_mul(u128::from(left_weight))
            .saturating_add(right.as_nanos().saturating_mul(u128::from(right_weight)))
            / u128::from(denominator),
    )
}

#[inline]
fn duration_add(left: Duration, right: Duration) -> Duration {
    duration_from_nanos_saturating(left.as_nanos().saturating_add(right.as_nanos()))
}

#[inline]
fn duration_mul(duration: Duration, factor: u32) -> Duration {
    duration_from_nanos_saturating(duration.as_nanos().saturating_mul(u128::from(factor)))
}

#[inline]
fn duration_div(duration: Duration, divisor: u32) -> Duration {
    duration_from_nanos_saturating(duration.as_nanos() / u128::from(divisor))
}

#[inline]
fn duration_abs_diff(left: Duration, right: Duration) -> Duration {
    if left >= right {
        left - right
    } else {
        right - left
    }
}

#[inline]
fn duration_from_nanos_saturating(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = (nanos % NANOS_PER_SECOND) as u32;
    if seconds > u128::from(u64::MAX) {
        Duration::new(u64::MAX, 999_999_999)
    } else {
        Duration::new(seconds as u64, subsecond_nanos)
    }
}
