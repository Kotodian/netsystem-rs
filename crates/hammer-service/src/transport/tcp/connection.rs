use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use super::TcpInputNext;
use super::output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, tcp_effective_output_payload_len, tcp_send_goal_size,
};
use super::recovery::{TcpRecoveryAck, TcpRecoveryState};
use super::sack::TcpSackState;
use super::segment::TcpSegment;
use crate::transport::congestion::CongestionController;
use crossbeam_utils::CachePadded;
use hammer_adapter::DataWorkerId;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::ip_ecn::IpEcnCodepoint;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpError, TcpFastOpenCookie,
    TcpNegotiatedOptions, TcpPacket, TcpSackBlock, TcpSegmentFlags, TcpSeq, TcpState,
    TcpTimestampOption,
};
#[cfg(test)]
use hammer_infra::vec::Vec;
use thiserror::Error;

const TCP_MAX_WINDOW_SCALE: u8 = 14;
const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

pub const TCP_INITIAL_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MIN_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MAX_RETRANSMIT_TIMEOUT: Duration = Duration::from_secs(60);
/// 60s 2MSL TIME_WAIT. iOS VPN may use a shorter value (e.g. 120 ticks = 1.2s) if needed.
pub const TCP_TIME_WAIT_TICKS: u64 = 6_000;
const TCP_PAWS_IDLE: Duration = Duration::from_secs(24 * 86_400);
pub const TCP_TIMER_RETRANSMIT: u32 = 0;
pub const TCP_TIMER_RACK: u32 = 1;
pub const TCP_TIMER_TLP: u32 = 2;
pub const TCP_TIMER_DELAYED_ACK: u32 = 3;
pub const TCP_TIMER_PERSIST: u32 = 4;
pub const TCP_TIMER_KEEP_ALIVE: u32 = 5;
pub const TCP_TIMER_TIME_WAIT: u32 = 6;
pub const TCP_TIMER_PACING: u32 = 7;
pub const TCP_TIMER_COUNT: u32 = 8;
const TCP_DELAYED_ACK_TICKS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpKeepaliveConfig {
    idle: Duration,
    probe_interval: Duration,
    probe_limit: u8,
}

impl Default for TcpKeepaliveConfig {
    #[inline]
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(75),
            probe_interval: Duration::from_secs(75),
            probe_limit: 8,
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
enum TcpConnectionError {
    #[error("invalid connection")]
    InvalidState,
    #[error("invalid connection")]
    MissingLocalAddress,
    #[error("dispatch error")]
    PayloadLengthOverflow,
}

impl From<TcpConnectionError> for TcpError {
    #[inline]
    fn from(error: TcpConnectionError) -> Self {
        match error {
            TcpConnectionError::InvalidState | TcpConnectionError::MissingLocalAddress => {
                TcpError::InvalidConnection
            }
            TcpConnectionError::PayloadLengthOverflow => TcpError::Dispatch,
        }
    }
}

impl From<TcpConnectionError> for CoreError {
    #[inline]
    fn from(error: TcpConnectionError) -> Self {
        TcpError::from(error).into()
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    struct TcpPendingSignals: u8 {
        const ECE_ACK = 1 << 0;
        const ECN_CE = 1 << 1;
        const CWR = 1 << 2;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TcpEcnState {
    pending_signals: TcpPendingSignals,
    received_ce_counter: u64,
    pending_ce_feedback: u64,
    peer_ace_counter: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpTimerState {
    active: u16,
}

impl Default for TcpTimerState {
    #[inline]
    fn default() -> Self {
        Self { active: 0 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TcpTimestampState {
    recent_remote: Option<u32>,
    recent_remote_at: Option<Instant>,
    local_origin: Option<Instant>,
    last_local: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpKeepaliveState {
    last_activity_at: Instant,
    probes_sent: u8,
    config: TcpKeepaliveConfig,
}

impl TcpKeepaliveState {
    #[inline]
    fn new(now: Instant) -> Self {
        Self {
            last_activity_at: now,
            probes_sent: 0,
            config: TcpKeepaliveConfig::default(),
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
                let rtt_delta = srtt.abs_diff(rtt);
                let next_rttvar = (rttvar * 3 + rtt_delta) / 4;
                let next_srtt = (srtt * 7 + rtt) / 8;
                self.srtt = Some(next_srtt);
                self.rttvar = Some(next_rttvar);
            }
            _ => {
                self.srtt = Some(rtt);
                self.rttvar = Some(rtt / 2);
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
        self.rto = clamp_retransmit_timeout(self.rto * 2);
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

#[derive(Debug, Clone)]
pub struct TcpConnectionCacheline0 {
    state: TcpState,
    timers: TcpTimerState,
    pacing_ready: bool,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    iss: TcpSeq,
    irs: TcpSeq,
    snd_una: TcpSeq,
    snd_nxt: TcpSeq,
    snd_wnd: u32,
    rcv_nxt: TcpSeq,
    rcv_wnd: u32,
    negotiated_options: TcpNegotiatedOptions,
    tx_intent_sequence: Option<TcpSeq>,
    tx_intent_payload_len: u32,
    fast_open_syn_payload_len: u32,
    bytes_in_flight_cached: u32,
}

#[derive(Debug, Clone)]
struct TcpConnectionCacheline1 {
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    close_reason: Option<TcpCloseReason>,
    local_port: u16,
    fast_open_cookie: Option<TcpFastOpenCookie>,
    persist_attempts: u8,
}

#[derive(Debug, Clone)]
pub struct TcpConnection<C>
where
    C: CongestionController,
{
    cacheline0: CachePadded<TcpConnectionCacheline0>,
    cacheline1: CachePadded<TcpConnectionCacheline1>,
    retransmit_timeout: TcpRetransmitTimeoutState,
    timestamps: TcpTimestampState,
    keepalive: TcpKeepaliveState,
    ecn: TcpEcnState,
    congestion: C,
    recovery: TcpRecoveryState,
    sack: TcpSackState,
}

impl<C> Deref for TcpConnection<C>
where
    C: CongestionController,
{
    type Target = TcpConnectionCacheline0;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cacheline0
    }
}

impl<C> DerefMut for TcpConnection<C>
where
    C: CongestionController,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cacheline0
    }
}

impl<C> TcpConnection<C>
where
    C: CongestionController,
{
    #[inline(always)]
    pub fn timer_is_supported(&self, timer_id: u32) -> bool {
        matches!(
            (self.state, timer_id),
            (TcpState::SynSent, TCP_TIMER_RETRANSMIT)
                | (TcpState::Established, TCP_TIMER_RETRANSMIT)
                | (TcpState::FinWait1, TCP_TIMER_RETRANSMIT)
                | (TcpState::Closing, TCP_TIMER_RETRANSMIT)
                | (TcpState::LastAck, TCP_TIMER_RETRANSMIT)
                | (TcpState::Established, TCP_TIMER_RACK)
                | (TcpState::Established, TCP_TIMER_TLP)
                | (TcpState::Established, TCP_TIMER_DELAYED_ACK)
                | (TcpState::Established, TCP_TIMER_PERSIST)
                | (TcpState::Established, TCP_TIMER_KEEP_ALIVE)
                | (TcpState::Established, TCP_TIMER_PACING)
                | (TcpState::TimeWait, TCP_TIMER_TIME_WAIT)
        )
    }

    #[inline(always)]
    pub fn timer_is_active(&self, timer_id: u32) -> bool {
        timer_mask_contains(self.timers.active, timer_id)
    }

    /// Bulk read of the active-timer bitmask (`self.timers.active`).
    ///
    /// One load replaces up to 8 `timer_is_active` mask-and-test calls at
    /// call sites that need the whole mask (e.g. computing a timer-refresh
    /// keep-mask). The per-id `timer_is_active` predicate remains for sites
    /// that only test a single id.
    #[inline(always)]
    pub fn active_timer_mask(&self) -> u16 {
        self.timers.active
    }

    #[inline(always)]
    pub fn timer_set(&mut self, timer_id: u32) {
        self.timers.active |= timer_bit(timer_id);
    }

    #[inline]
    pub fn timer_reset(&mut self, timer_id: u32) {
        self.timers.active &= !timer_bit(timer_id);
    }

    #[inline]
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            cacheline0: CachePadded::new(TcpConnectionCacheline0 {
                state: TcpState::Closed,
                timers: TcpTimerState::default(),
                pacing_ready: false,
                local,
                remote,
                iss: TcpSeq::from(0),
                irs: TcpSeq::from(0),
                snd_una: TcpSeq::from(0),
                snd_nxt: TcpSeq::from(0),
                snd_wnd: DEFAULT_TCP_WINDOW,
                rcv_nxt: TcpSeq::from(0),
                rcv_wnd: DEFAULT_TCP_WINDOW,
                negotiated_options: TcpNegotiatedOptions::default(),
                tx_intent_sequence: None,
                tx_intent_payload_len: 0,
                fast_open_syn_payload_len: 0,
                bytes_in_flight_cached: 0,
            }),
            cacheline1: CachePadded::new(TcpConnectionCacheline1 {
                connection_id,
                owner_worker,
                close_reason: None,
                local_port,
                fast_open_cookie: None,
                persist_attempts: 0,
            }),
            retransmit_timeout: TcpRetransmitTimeoutState::new(),
            timestamps: TcpTimestampState::default(),
            keepalive: TcpKeepaliveState::new(Instant::now()),
            ecn: TcpEcnState::default(),
            congestion: C::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            recovery: TcpRecoveryState::new(),
            sack: TcpSackState::default(),
        }
    }

    /// Chain this after [`TcpConnection::new`] to override the keepalive policy.
    /// The default policy (75s idle, 75s probe interval, 8 probes, always-on for
    /// Established) is unchanged; this only paves the way for an opt-in policy.
    /// Unused in production until the opt-in keepalive plan lands; exercised by
    /// `tcp_keepalive_config_defaults_match_prior_constants_and_with_keepalive_overrides`.
    #[inline]
    #[allow(dead_code)]
    fn with_keepalive(mut self, config: TcpKeepaliveConfig) -> Self {
        self.keepalive.config = config;
        self
    }

    #[inline]
    pub fn state(&self) -> TcpState {
        self.state
    }

    #[inline]
    pub fn connection_id(&self) -> Option<TcpConnectionId> {
        self.cacheline1.connection_id
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.cacheline1.owner_worker
    }

    #[inline]
    pub fn local_port(&self) -> u16 {
        self.cacheline1.local_port
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
        self.iss.raw()
    }

    #[inline]
    pub fn irs(&self) -> u32 {
        self.irs.raw()
    }

    #[inline]
    pub fn snd_una(&self) -> u32 {
        self.snd_una.raw()
    }

    #[inline]
    pub fn snd_nxt(&self) -> u32 {
        self.snd_nxt.raw()
    }

    #[inline]
    pub fn snd_wnd(&self) -> u32 {
        self.snd_wnd
    }

    #[inline]
    pub fn rcv_nxt(&self) -> u32 {
        self.rcv_nxt.raw()
    }

    #[inline]
    pub fn rcv_wnd(&self) -> u32 {
        self.rcv_wnd
    }

    pub(crate) fn set_rcv_wnd(&mut self, available: usize) {
        self.rcv_wnd = u32::try_from(available).unwrap_or(u32::MAX);
    }

    #[inline]
    pub fn close_reason(&self) -> Option<TcpCloseReason> {
        self.cacheline1.close_reason
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.negotiated_options
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        tcp_window_scale(self.negotiated_options.send_window_scale)
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        tcp_window_scale(self.negotiated_options.receive_window_scale)
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
    pub fn output_payload_len(&self) -> usize {
        tcp_effective_output_payload_len(self.negotiated_options().send_max_segment_size)
    }

    #[inline]
    pub fn send_goal_size(&self) -> usize {
        tcp_send_goal_size(self.negotiated_options().send_max_segment_size)
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn pacing_ready(&self) -> bool {
        self.pacing_ready
    }

    #[inline]
    fn output_capabilities(&self) -> TcpCapabilities {
        let negotiated = self.negotiated_options();
        TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: negotiated.sack,
            timestamps: negotiated.timestamps,
            ecn: negotiated.ecn,
            accurate_ecn: negotiated.accurate_ecn,
            fast_open: false,
        }
    }

    #[inline]
    fn fast_open_cookie(&self) -> Option<TcpFastOpenCookie> {
        self.cacheline1.fast_open_cookie
    }

    #[inline]
    pub fn congestion(&self) -> &C {
        &self.congestion
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> &TcpRetransmitTimeoutState {
        &self.retransmit_timeout
    }

    #[inline]
    pub fn recovery(&self) -> &TcpRecoveryState {
        &self.recovery
    }

    #[inline]
    pub fn next_node(&self) -> TcpInputNext {
        match self.state {
            TcpState::Closed => TcpInputNext::Drop,
            TcpState::Listen => TcpInputNext::Listen,
            TcpState::SynSent => TcpInputNext::SynSent,
            TcpState::Established => TcpInputNext::Established,
            TcpState::SynRcvd
            | TcpState::FinWait1
            | TcpState::FinWait2
            | TcpState::CloseWait
            | TcpState::Closing
            | TcpState::LastAck
            | TcpState::TimeWait => TcpInputNext::RcvProcess,
        }
    }

    #[inline]
    pub(crate) fn timer_ticks(&self, timer_id: u32, now: Instant) -> Option<u64> {
        if !self.timer_is_active(timer_id) {
            return None;
        }
        match (self.state, timer_id) {
            (TcpState::Established, TCP_TIMER_RETRANSMIT)
            | (TcpState::FinWait1, TCP_TIMER_RETRANSMIT)
            | (TcpState::Closing, TCP_TIMER_RETRANSMIT)
            | (TcpState::LastAck, TCP_TIMER_RETRANSMIT)
            | (TcpState::SynSent, TCP_TIMER_RETRANSMIT) => Some(
                (self.retransmit_timeout().retransmit_timeout().as_millis() / 10).max(1) as u64,
            ),
            (TcpState::Established, TCP_TIMER_RACK) => self
                .recovery
                .rack_timeout(now)
                .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64),
            (TcpState::Established, TCP_TIMER_TLP) => self
                .recovery
                .tlp_timeout(
                    self.retransmit_timeout().smoothed_rtt(),
                    self.retransmit_timeout().retransmit_timeout(),
                )
                .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64),
            (TcpState::Established, TCP_TIMER_DELAYED_ACK) => Some(TCP_DELAYED_ACK_TICKS),
            (TcpState::Established, TCP_TIMER_PERSIST) => {
                let rto = self.retransmit_timeout().retransmit_timeout();
                let base_ticks = (rto.as_millis() / 10).max(1) as u64;
                let shift = u32::from(self.cacheline1.persist_attempts.min(9));
                let cap_ticks = TCP_MAX_RETRANSMIT_TIMEOUT.as_millis().div_ceil(10).max(1) as u64;
                let ticks = base_ticks
                    .checked_shl(shift)
                    .unwrap_or(u64::MAX)
                    .min(cap_ticks);
                Some(ticks.max(1))
            }
            (TcpState::Established, TCP_TIMER_KEEP_ALIVE) => {
                let interval = if self.keepalive.probes_sent == 0 {
                    self.keepalive.config.idle
                } else {
                    self.keepalive.config.probe_interval
                };
                Some(interval.as_millis().div_ceil(10).max(1) as u64)
            }
            (TcpState::Established, TCP_TIMER_PACING) => self
                .congestion
                .next_send_delay(self.output_payload_len() as u32)
                .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64),
            (TcpState::TimeWait, TCP_TIMER_TIME_WAIT) => Some(TCP_TIME_WAIT_TICKS),
            _ => None,
        }
    }

    #[inline]
    fn observe_activity(&mut self, now: Instant) {
        self.keepalive.last_activity_at = now;
        self.keepalive.probes_sent = 0;
        if self.state == TcpState::Established {
            self.timer_set(TCP_TIMER_KEEP_ALIVE);
        }
    }

    #[inline]
    pub(crate) fn tx_payload_sequence(&self) -> TcpSeq {
        self.tx_intent_sequence.unwrap_or_else(|| {
            if self.state == TcpState::SynSent {
                self.iss
            } else {
                self.snd_nxt
            }
        })
    }

    #[inline]
    pub(crate) fn clear_tx_intent(&mut self) {
        self.tx_intent_sequence = None;
        self.tx_intent_payload_len = 0;
    }

    #[inline]
    pub fn observe_retransmit_timeout(&mut self) -> Duration {
        self.retransmit_timeout.on_retransmission_timeout()
    }

    #[inline]
    fn accepts_ack(&self, acknowledgment: TcpSeq) -> bool {
        acknowledgment >= self.snd_una && acknowledgment <= self.snd_nxt
    }

    #[inline]
    pub(crate) fn apply_ack(&mut self, acknowledgment: TcpSeq, advertised_window: u16) {
        if self.accepts_ack(acknowledgment) {
            self.snd_una = acknowledgment;
        }
        self.snd_wnd = self.effective_send_window(u32::from(advertised_window));
        if self.snd_wnd > 0 {
            self.cacheline1.persist_attempts = 0;
        }
    }

    fn recovery_ack(&self, acknowledgment: TcpSeq) -> TcpRecoveryAck {
        let reordering_window = self
            .congestion
            .min_rtt()
            .or_else(|| self.retransmit_timeout().smoothed_rtt())
            .unwrap_or_else(|| self.retransmit_timeout().retransmit_timeout())
            / 4;
        TcpRecoveryAck {
            acknowledgment,
            now: Instant::now(),
            app_limited: false,
            ecn_ce_count: self.ecn.pending_ce_feedback,
            reordering_window: reordering_window.max(Duration::from_millis(1)),
        }
    }

    #[inline]
    fn pending_ece_ack(&self) -> bool {
        self.ecn
            .pending_signals
            .contains(TcpPendingSignals::ECE_ACK)
    }

    #[inline]
    #[cfg(test)]
    fn pending_ecn_ce(&self) -> bool {
        self.ecn.pending_signals.contains(TcpPendingSignals::ECN_CE)
    }

    #[inline]
    pub(crate) fn observe_peer_ecn_feedback(&mut self, packet: &TcpPacket) {
        if self.negotiated_options().accurate_ecn {
            self.observe_peer_accurate_ecn_feedback(packet);
            return;
        }
        if self.negotiated_options().ecn
            && packet.flags.contains(TcpSegmentFlags::ECE)
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            self.ecn
                .pending_signals
                .insert(TcpPendingSignals::ECN_CE | TcpPendingSignals::CWR);
            self.ecn.pending_ce_feedback = self.ecn.pending_ce_feedback.saturating_add(1);
        }
        if packet.flags.contains(TcpSegmentFlags::CWR) {
            self.ecn
                .pending_signals
                .remove(TcpPendingSignals::ECE_ACK | TcpPendingSignals::ECN_CE);
        }
        if matches!(packet.ip_ecn, Some(IpEcnCodepoint::Ce)) && self.negotiated_options().ecn {
            self.ecn.received_ce_counter = self.ecn.received_ce_counter.saturating_add(1);
            self.ecn.pending_signals.insert(TcpPendingSignals::ECE_ACK);
        }
    }

    fn observe_peer_accurate_ecn_feedback(&mut self, packet: &TcpPacket) {
        if packet.flags.contains(TcpSegmentFlags::ACK) {
            let next_ace = ace_counter(packet.flags);
            let ce_delta = ace_delta(self.ecn.peer_ace_counter, next_ace);
            self.ecn.peer_ace_counter = next_ace;
            self.ecn.pending_ce_feedback = self
                .ecn
                .pending_ce_feedback
                .saturating_add(u64::from(ce_delta));
            if ce_delta != 0 {
                self.ecn
                    .pending_signals
                    .insert(TcpPendingSignals::ECN_CE | TcpPendingSignals::CWR);
            } else {
                self.ecn.pending_signals.remove(TcpPendingSignals::ECN_CE);
            }
        }
        if matches!(packet.ip_ecn, Some(IpEcnCodepoint::Ce)) {
            self.ecn.received_ce_counter = self.ecn.received_ce_counter.saturating_add(1);
        }
    }

    #[inline]
    fn output_flags(&mut self, flags: TcpSegmentFlags) -> TcpSegmentFlags {
        let mut output = flags;
        if !output.contains(TcpSegmentFlags::ACK) {
            return output;
        }
        if self.negotiated_options().accurate_ecn {
            output.remove(TcpSegmentFlags::NS | TcpSegmentFlags::CWR | TcpSegmentFlags::ECE);
            output.insert(ace_flags(self.ecn.received_ce_counter));
            return output;
        }
        if self.pending_ece_ack() {
            output.insert(TcpSegmentFlags::ECE);
        }
        output
    }

    #[inline]
    fn output_ip_ecn(&self, payload_len: usize, retransmit: bool) -> Option<IpEcnCodepoint> {
        if !self.negotiated_options().ecn || payload_len == 0 || retransmit {
            return None;
        }
        Some(IpEcnCodepoint::Ect0)
    }

    #[inline]
    fn refresh_bytes_in_flight_cached(&mut self) {
        self.cacheline0.bytes_in_flight_cached = self.recovery.bytes_in_flight();
    }

    fn observe_ack_progress(
        &mut self,
        packet: &TcpPacket,
        acknowledgment: TcpSeq,
        sack_blocks: &[TcpSackBlock],
    ) -> Option<Duration> {
        if !self.accepts_ack(acknowledgment) {
            return None;
        }
        let advanced = acknowledgment > self.snd_una;
        let recovery_ack = self.recovery_ack(acknowledgment);
        let mut latest_rtt = if advanced {
            if sack_blocks.is_empty() {
                self.recovery.on_ack(recovery_ack, &mut self.congestion)
            } else {
                self.recovery
                    .on_sack_blocks(recovery_ack, sack_blocks, &mut self.congestion)
            }
        } else if !sack_blocks.is_empty() {
            self.recovery
                .on_sack_blocks(recovery_ack, sack_blocks, &mut self.congestion)
        } else {
            None
        };
        if advanced && latest_rtt.is_none() {
            latest_rtt = self.timestamp_rtt_sample(packet, Instant::now());
        }
        if advanced && let Some(latest_rtt) = latest_rtt {
            self.retransmit_timeout.observe_ack_sample(latest_rtt);
            self.ecn.pending_signals.remove(TcpPendingSignals::ECN_CE);
            self.ecn.pending_ce_feedback = 0;
        }
        self.refresh_bytes_in_flight_cached();
        latest_rtt
    }

    #[inline]
    fn next_local_timestamp(&mut self, enabled: bool) -> Option<TcpTimestampOption> {
        if !enabled {
            return None;
        }
        self.timestamps.last_local = self.local_timestamp_value(Instant::now());
        let timestamp = TcpTimestampOption {
            tsval: self.timestamps.last_local,
            tsecr: self.timestamps.recent_remote.unwrap_or(0),
        };
        Some(timestamp)
    }

    #[inline]
    fn local_timestamp_value(&mut self, now: Instant) -> u32 {
        let origin = self.timestamps.local_origin.get_or_insert(now);
        let mut tsval = now.saturating_duration_since(*origin).as_millis() as u32;
        if tsval == 0 {
            tsval = 1;
        }
        if TcpSeq::from(tsval) <= TcpSeq::from(self.timestamps.last_local) {
            tsval = self.timestamps.last_local.wrapping_add(1).max(1);
        }
        tsval
    }

    #[inline]
    fn timestamp_rtt_sample(&mut self, packet: &TcpPacket, now: Instant) -> Option<Duration> {
        if !self.negotiated_options().timestamps {
            return None;
        }
        let timestamp = packet.timestamp?;
        let tsecr = timestamp.tsecr;
        if tsecr == 0 {
            return None;
        }
        let local_now = self.local_timestamp_value(now);
        if TcpSeq::from(tsecr) > TcpSeq::from(local_now) {
            return None;
        }
        Some(Duration::from_millis(u64::from(
            local_now.wrapping_sub(tsecr).max(1),
        )))
    }

    #[inline]
    pub(crate) fn observe_inbound_timestamp(
        &mut self,
        flags: TcpSegmentFlags,
        timestamp: Option<TcpTimestampOption>,
        sequence: TcpSeq,
        payload_len: usize,
    ) -> bool {
        if flags.contains(TcpSegmentFlags::RST) {
            return true;
        }
        if !self.negotiated_options().timestamps {
            return true;
        }
        let Some(timestamp) = timestamp else {
            return false;
        };
        let now = Instant::now();
        if let Some(recent) = self.timestamps.recent_remote
            && TcpSeq::from(recent) > TcpSeq::from(timestamp.tsval)
        {
            let paws_expired = self
                .timestamps
                .recent_remote_at
                .is_some_and(|recent_at| now.saturating_duration_since(recent_at) > TCP_PAWS_IDLE);
            if paws_expired {
                self.timestamps.recent_remote = Some(timestamp.tsval);
                self.timestamps.recent_remote_at = Some(now);
            } else {
                return false;
            }
        }
        let sequence_len = payload_len as u32
            + u32::from(flags.contains(TcpSegmentFlags::SYN))
            + u32::from(flags.contains(TcpSegmentFlags::FIN));
        let segment_end = sequence.advance(sequence_len).raw();
        if flags.contains(TcpSegmentFlags::SYN)
            || (self.rcv_nxt >= sequence && TcpSeq::from(segment_end) >= self.rcv_nxt)
        {
            self.timestamps.recent_remote = Some(timestamp.tsval);
            self.timestamps.recent_remote_at = Some(now);
        }
        true
    }

    #[inline]
    pub(crate) fn on_clean_in_order_payload(&mut self) -> bool {
        if self.timer_is_active(TCP_TIMER_DELAYED_ACK) {
            self.timer_reset(TCP_TIMER_DELAYED_ACK);
            return true;
        }
        self.timer_set(TCP_TIMER_DELAYED_ACK);
        false
    }

    #[inline]
    fn apply_peer_handshake_capabilities(
        &mut self,
        local_capabilities: TcpCapabilities,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        let local_capabilities = normalize_tcp_capabilities(local_capabilities);
        let capabilities = normalize_tcp_capabilities(capabilities);
        self.negotiated_options = tcp_negotiate_options(local_capabilities, capabilities);
        let negotiated = self.negotiated_options;
        if let Some(max_segment_size) = negotiated.send_max_segment_size.filter(|max| *max != 0) {
            self.congestion.on_mtu_update(u32::from(max_segment_size));
        }
        negotiated
    }

    #[inline]
    pub(crate) fn control_segment(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
        flags: TcpSegmentFlags,
        reset_sequence: Option<u32>,
        local_capabilities: TcpCapabilities,
    ) -> TcpSegment {
        let flags = self.output_flags(flags);
        let local_capabilities = normalize_tcp_capabilities(local_capabilities);
        let capabilities = if flags.contains(TcpSegmentFlags::SYN) {
            local_capabilities
        } else {
            self.output_capabilities()
        };
        let sack_blocks = self.sack.take_output(
            self.negotiated_options().sack,
            self.negotiated_options().timestamps,
            flags,
        );
        TcpSegment::new(
            local,
            remote,
            reset_sequence.unwrap_or_else(|| self.output_sequence(flags)),
            self.output_acknowledgment(flags),
            self.advertised_receive_window(self.rcv_wnd),
            flags,
            capabilities,
            sack_blocks.as_ref().map(|(blocks, len)| &blocks[..*len]),
            self.next_local_timestamp(capabilities.timestamps),
            flags
                .contains(TcpSegmentFlags::SYN)
                .then(|| self.fast_open_cookie())
                .flatten(),
            None,
            0,
        )
    }

    #[inline]
    fn output_sequence(&self, flags: TcpSegmentFlags) -> u32 {
        if flags.contains(TcpSegmentFlags::SYN) && self.iss != TcpSeq::from(0) {
            self.iss.raw()
        } else if self.snd_nxt != TcpSeq::from(0) {
            self.snd_nxt.raw()
        } else if self.snd_una != TcpSeq::from(0) {
            self.snd_una.raw()
        } else if self.iss != TcpSeq::from(0) {
            self.iss.advance(1).raw()
        } else {
            1
        }
    }

    #[inline]
    fn output_acknowledgment(&self, flags: TcpSegmentFlags) -> u32 {
        if !flags.contains(TcpSegmentFlags::ACK) {
            return 0;
        }
        if self.rcv_nxt != TcpSeq::from(0) {
            self.rcv_nxt.raw()
        } else if self.irs != TcpSeq::from(0) {
            self.irs.advance(1).raw()
        } else {
            1
        }
    }

    #[inline]
    pub(crate) fn ensure_state(&self, state: TcpState) -> CoreResult<()> {
        if self.state != state {
            return Err(TcpConnectionError::InvalidState.into());
        }
        Ok(())
    }

    #[inline]
    pub fn connect_state(&mut self, initial_sequence: u32) {
        self.cacheline1.close_reason = None;
        self.iss = TcpSeq::from(initial_sequence);
        self.snd_una = self.iss;
        self.snd_nxt = self.iss.advance(1);
        self.fast_open_syn_payload_len = 0;
        self.state = TcpState::SynSent;
    }

    pub fn set_fast_open_cookie(&mut self, cookie: Option<TcpFastOpenCookie>) {
        self.cacheline1.fast_open_cookie = cookie.filter(|cookie| !cookie.is_empty());
    }

    pub(crate) fn receive_syn(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
        flags: TcpSegmentFlags,
        sequence: TcpSeq,
        advertised_window: u16,
        capabilities: TcpCapabilities,
        timestamp: Option<TcpTimestampOption>,
        accepted_payload_len: usize,
        local_capabilities: TcpCapabilities,
    ) -> CoreResult<Option<TcpSegment>> {
        if !flags.contains(TcpSegmentFlags::SYN)
            || flags.intersects(TcpSegmentFlags::ACK | TcpSegmentFlags::RST)
        {
            return Ok(None);
        }
        self.apply_peer_handshake_capabilities(local_capabilities, capabilities);
        let _ = self.observe_inbound_timestamp(flags, timestamp, sequence, accepted_payload_len);
        if self.iss == TcpSeq::from(0) {
            self.iss = TcpSeq::from(1);
        }
        self.state = TcpState::SynRcvd;
        self.irs = sequence;
        self.snd_una = self.iss;
        self.snd_nxt = self.iss.advance(1);
        self.snd_wnd = self.effective_send_window(u32::from(advertised_window));
        self.rcv_nxt = sequence.advance(1 + accepted_payload_len as u32);
        let flags = if self.negotiated_options().ecn {
            TcpSegmentFlags::SYN | TcpSegmentFlags::ACK | TcpSegmentFlags::ECE
        } else {
            TcpSegmentFlags::SYN | TcpSegmentFlags::ACK
        };
        Ok(Some(self.control_segment(
            local,
            remote,
            flags,
            None,
            local_capabilities,
        )))
    }

    pub(crate) fn receive_open_reply(
        &mut self,
        packet: &TcpPacket,
        local_capabilities: TcpCapabilities,
    ) -> CoreResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::SynSent)?;

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && (acknowledgment <= self.iss || acknowledgment > self.snd_nxt)
        {
            if packet.flags.contains(TcpSegmentFlags::RST) {
                return Ok(None);
            }
            return Ok(Some(self.control_segment(
                packet.local,
                packet.remote,
                TcpSegmentFlags::RST,
                Some(acknowledgment.raw()),
                local_capabilities,
            )));
        }

        if packet.flags.contains(TcpSegmentFlags::RST) {
            if let Some(acknowledgment) = packet.acknowledgment
                && self.accepts_ack(acknowledgment)
            {
                self.cacheline1.close_reason = Some(TcpCloseReason::RemoteReset);
                self.state = TcpState::Closed;
                self.timer_reset(TCP_TIMER_RETRANSMIT);
            }
            return Ok(None);
        }

        if !packet.flags.contains(TcpSegmentFlags::SYN) {
            return Ok(None);
        }

        self.apply_peer_handshake_capabilities(local_capabilities, packet.capabilities);
        if !self.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ) {
            return Ok(None);
        }
        self.irs = packet.sequence;
        self.snd_wnd = self.effective_send_window(u32::from(packet.advertised_window));
        self.rcv_nxt = packet.sequence.advance(1);
        if let Some(cookie) = packet.fast_open_cookie.filter(|cookie| !cookie.is_empty()) {
            self.set_fast_open_cookie(Some(cookie));
        }

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            if !self.accepts_ack(acknowledgment) {
                return Ok(None);
            }
            self.snd_una = acknowledgment;
            self.state = TcpState::Established;
            self.timer_reset(TCP_TIMER_RETRANSMIT);
            self.observe_activity(Instant::now());
            if packet.payload_len != 0 {
                self.rcv_nxt = packet.sequence.advance(1 + packet.payload_len as u32);
            }
            return Ok(Some(self.control_segment(
                packet.local,
                packet.remote,
                TcpSegmentFlags::ACK,
                None,
                local_capabilities,
            )));
        }

        self.state = TcpState::SynRcvd;
        let flags = if self.negotiated_options().ecn {
            TcpSegmentFlags::SYN | TcpSegmentFlags::ACK | TcpSegmentFlags::ECE
        } else {
            TcpSegmentFlags::SYN | TcpSegmentFlags::ACK
        };
        Ok(Some(self.control_segment(
            packet.local,
            packet.remote,
            flags,
            None,
            local_capabilities,
        )))
    }

    #[inline]
    pub(crate) fn take_acked_tx_len(&mut self, previous_snd_una: u32) -> u32 {
        let previous_snd_una = TcpSeq::from(previous_snd_una);
        let acked = previous_snd_una.distance_to(self.snd_una);
        if acked == 0 {
            return 0;
        }
        let syn_control =
            u32::from(previous_snd_una == self.iss && self.snd_una != previous_snd_una);
        let payload_acked = acked.saturating_sub(syn_control);
        let fast_open_acked = payload_acked.min(self.fast_open_syn_payload_len);
        self.fast_open_syn_payload_len = self
            .fast_open_syn_payload_len
            .saturating_sub(fast_open_acked);
        payload_acked
    }

    pub(crate) fn receive_final_ack(
        &mut self,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::SynRcvd)?;
        if !self.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ) {
            return Ok(None);
        }
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.cacheline1.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        let Some(acknowledgment) = packet.acknowledgment else {
            return Ok(None);
        };
        if !packet.flags.contains(TcpSegmentFlags::ACK) || !self.accepts_ack(acknowledgment) {
            return Ok(Some(self.control_segment(
                packet.local,
                packet.remote,
                TcpSegmentFlags::RST,
                Some(acknowledgment.raw()),
                TcpCapabilities::default(),
            )));
        }
        self.apply_ack(acknowledgment, packet.advertised_window);
        self.state = TcpState::Established;
        self.timer_reset(TCP_TIMER_RETRANSMIT);
        self.observe_activity(Instant::now());
        Ok(None)
    }

    #[inline]
    pub(crate) fn tx_payload_budget(
        &self,
        pending_len: usize,
        _: Instant,
        local_capabilities: TcpCapabilities,
    ) -> usize {
        let local_capabilities = normalize_tcp_capabilities(local_capabilities);
        if self.tx_intent_sequence.is_some() {
            return pending_len.min(self.tx_intent_payload_len as usize);
        }
        if pending_len == 0 {
            return 0;
        }
        if self.state == TcpState::SynSent {
            if !local_capabilities.fast_open || self.cacheline1.fast_open_cookie.is_none() {
                return 0;
            }
            let syn_payload_len =
                tcp_effective_output_payload_len(local_capabilities.max_segment_size);
            return pending_len.min(syn_payload_len);
        }
        if self.state != TcpState::Established {
            return 0;
        }
        let mss = self.output_payload_len();
        let bytes_in_flight = self.bytes_in_flight_cached;
        let peer_remaining = self.snd_wnd.saturating_sub(bytes_in_flight) as usize;
        let cc_remaining = if let Some(recovery_remaining) = self
            .recovery
            .recovery_send_space(bytes_in_flight, self.congestion.max_datagram_size())
        {
            recovery_remaining as usize
        } else {
            self.congestion
                .congestion_window()
                .saturating_sub(bytes_in_flight) as usize
        };
        let allowed = pending_len.min(mss).min(peer_remaining).min(cc_remaining);
        if allowed == 0 {
            return 0;
        }
        if !self.pacing_ready && self.congestion.next_send_delay(allowed as u32).is_some() {
            return 0;
        }
        allowed
    }

    pub(crate) fn tx_segment(
        &mut self,
        payload_len: usize,
        local_capabilities: TcpCapabilities,
    ) -> CoreResult<TcpSegment> {
        if self.state == TcpState::SynSent {
            let local = self
                .local()
                .ok_or(TcpConnectionError::MissingLocalAddress)?;
            let capabilities = normalize_tcp_capabilities(local_capabilities);
            let flags = if capabilities.ecn {
                TcpSegmentFlags::SYN | TcpSegmentFlags::ECE | TcpSegmentFlags::CWR
            } else {
                TcpSegmentFlags::SYN
            };
            let sequence = self.tx_payload_sequence();
            return Ok(TcpSegment::new(
                local,
                self.remote(),
                sequence.raw(),
                self.rcv_nxt(),
                self.advertised_receive_window(self.rcv_wnd),
                flags,
                capabilities,
                None,
                self.next_local_timestamp(capabilities.timestamps),
                self.fast_open_cookie(),
                self.output_ip_ecn(payload_len, self.tx_intent_sequence.is_some()),
                payload_len,
            ));
        }
        self.ensure_state(TcpState::Established)?;
        let local = self
            .local()
            .ok_or(TcpConnectionError::MissingLocalAddress)?;
        let mut flags = self.output_flags(TcpSegmentFlags::ACK | TcpSegmentFlags::PSH);
        if self.ecn.pending_signals.contains(TcpPendingSignals::CWR) {
            flags.insert(TcpSegmentFlags::CWR);
            self.ecn.pending_signals.remove(TcpPendingSignals::CWR);
        }
        let sack_blocks = self.sack.take_output(
            self.negotiated_options().sack,
            self.negotiated_options().timestamps,
            flags,
        );
        let sequence = self.tx_payload_sequence();
        let retransmit = self
            .tx_intent_sequence
            .is_some_and(|sequence| sequence != self.snd_nxt);
        Ok(TcpSegment::new(
            local,
            self.remote(),
            sequence.raw(),
            self.rcv_nxt(),
            self.advertised_receive_window(self.rcv_wnd),
            flags,
            self.output_capabilities(),
            sack_blocks.as_ref().map(|(blocks, len)| &blocks[..*len]),
            self.next_local_timestamp(self.negotiated_options().timestamps),
            None,
            self.output_ip_ecn(payload_len, retransmit),
            payload_len,
        ))
    }

    pub(crate) fn commit_payload_tx(
        &mut self,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<u16> {
        if self.state == TcpState::SynSent {
            if self.tx_intent_sequence.is_some() {
                self.clear_tx_intent();
                return Ok(timer_bit(TCP_TIMER_RETRANSMIT));
            }
            let payload_len = u32::try_from(payload_len)
                .map_err(|_| TcpConnectionError::PayloadLengthOverflow)?;
            let sequence = self.iss;
            let end_sequence = TcpSeq::from(sequence)
                .advance(payload_len.saturating_add(1))
                .raw();
            let bytes_in_flight = self.bytes_in_flight_cached;
            let packet_number = self.recovery.next_packet_number();
            self.recovery.record_sent(
                packet_number,
                TcpSeq::from(sequence),
                TcpSeq::from(end_sequence),
                payload_len,
                payload_len,
                now,
            );
            self.refresh_bytes_in_flight_cached();
            self.congestion.on_packet_sent(
                packet_number,
                payload_len.saturating_add(1),
                bytes_in_flight,
                now,
            );
            self.fast_open_syn_payload_len = payload_len;
            self.snd_nxt = TcpSeq::from(end_sequence);
            self.timer_set(TCP_TIMER_RETRANSMIT);
            self.pacing_ready = false;
            return Ok(0);
        }
        self.ensure_state(TcpState::Established)?;
        if self.tx_intent_sequence.is_some() {
            self.clear_tx_intent();
            let mut timers = 0;
            self.timer_set(TCP_TIMER_RETRANSMIT);
            timers |= timer_bit(TCP_TIMER_RETRANSMIT);
            if self
                .recovery
                .rack_timeout(now)
                .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64)
                .is_some()
            {
                self.timer_set(TCP_TIMER_RACK);
                timers |= timer_bit(TCP_TIMER_RACK);
            } else {
                self.timer_reset(TCP_TIMER_RACK);
            }
            if self
                .recovery
                .tlp_timeout(
                    self.retransmit_timeout().smoothed_rtt(),
                    self.retransmit_timeout().retransmit_timeout(),
                )
                .is_some()
            {
                self.timer_set(TCP_TIMER_TLP);
                timers |= timer_bit(TCP_TIMER_TLP);
            } else {
                self.timer_reset(TCP_TIMER_TLP);
            }
            if self.snd_wnd == 0 && self.recovery.has_unacked_data() {
                self.timer_set(TCP_TIMER_PERSIST);
                timers |= timer_bit(TCP_TIMER_PERSIST);
            } else {
                self.timer_reset(TCP_TIMER_PERSIST);
            }
            return Ok(timers);
        }
        let payload_len =
            u32::try_from(payload_len).map_err(|_| TcpConnectionError::PayloadLengthOverflow)?;
        let sequence = self.snd_nxt;
        let end_sequence = TcpSeq::from(sequence).advance(payload_len).raw();
        let bytes_in_flight = self.bytes_in_flight_cached;
        let packet_number = self.recovery.next_packet_number();
        self.recovery.record_sent(
            packet_number,
            TcpSeq::from(sequence),
            TcpSeq::from(end_sequence),
            payload_len,
            payload_len,
            now,
        );
        self.recovery.on_new_data_sent(payload_len);
        self.refresh_bytes_in_flight_cached();
        self.congestion
            .on_packet_sent(packet_number, payload_len, bytes_in_flight, now);
        self.snd_nxt = TcpSeq::from(end_sequence);
        self.pacing_ready = false;
        self.observe_activity(now);
        let mut timers = 0;
        self.timer_set(TCP_TIMER_RETRANSMIT);
        timers |= timer_bit(TCP_TIMER_RETRANSMIT);
        if self
            .recovery
            .rack_timeout(now)
            .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64)
            .is_some()
        {
            self.timer_set(TCP_TIMER_RACK);
            timers |= timer_bit(TCP_TIMER_RACK);
        } else {
            self.timer_reset(TCP_TIMER_RACK);
        }
        if self
            .recovery
            .tlp_timeout(
                self.retransmit_timeout().smoothed_rtt(),
                self.retransmit_timeout().retransmit_timeout(),
            )
            .is_some()
        {
            self.timer_set(TCP_TIMER_TLP);
            timers |= timer_bit(TCP_TIMER_TLP);
        } else {
            self.timer_reset(TCP_TIMER_TLP);
        }
        if self
            .congestion
            .next_send_delay(self.output_payload_len() as u32)
            .is_some()
        {
            self.timer_set(TCP_TIMER_PACING);
            timers |= timer_bit(TCP_TIMER_PACING);
        } else {
            self.timer_reset(TCP_TIMER_PACING);
        }
        Ok(timers)
    }

    #[inline]
    pub(crate) fn receive_ack(
        &mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
        advertised_window: u16,
        sack_blocks: &[TcpSackBlock],
    ) -> u16 {
        let now = Instant::now();
        let acknowledgment = TcpSeq::from(acknowledgment);
        let _ = self.observe_ack_progress(packet, acknowledgment, sack_blocks);
        self.apply_ack(acknowledgment, advertised_window);
        self.observe_activity(now);
        self.timer_reset(TCP_TIMER_DELAYED_ACK);
        let mut timers = 0;
        if self.recovery.has_unacked_data() {
            self.timer_set(TCP_TIMER_RETRANSMIT);
            timers |= timer_bit(TCP_TIMER_RETRANSMIT);
        } else {
            self.timer_reset(TCP_TIMER_RETRANSMIT);
        }
        if self.snd_wnd == 0 && self.recovery.has_unacked_data() {
            self.timer_set(TCP_TIMER_PERSIST);
            self.timer_reset(TCP_TIMER_RACK);
            self.timer_reset(TCP_TIMER_TLP);
            self.timer_reset(TCP_TIMER_PACING);
        } else {
            self.timer_reset(TCP_TIMER_PERSIST);
            if self
                .recovery
                .rack_timeout(now)
                .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64)
                .is_some()
            {
                self.timer_set(TCP_TIMER_RACK);
                timers |= timer_bit(TCP_TIMER_RACK);
            } else {
                self.timer_reset(TCP_TIMER_RACK);
            }
            if self
                .recovery
                .tlp_timeout(
                    self.retransmit_timeout().smoothed_rtt(),
                    self.retransmit_timeout().retransmit_timeout(),
                )
                .is_some()
            {
                self.timer_set(TCP_TIMER_TLP);
                timers |= timer_bit(TCP_TIMER_TLP);
            } else {
                self.timer_reset(TCP_TIMER_TLP);
            }
            if self
                .congestion
                .next_send_delay(self.output_payload_len() as u32)
                .is_some()
            {
                self.timer_set(TCP_TIMER_PACING);
                timers |= timer_bit(TCP_TIMER_PACING);
            } else {
                self.timer_reset(TCP_TIMER_PACING);
            }
        }
        timers
    }

    pub(crate) fn receive_established(
        &mut self,
        packet: &TcpPacket,
    ) -> CoreResult<(Option<TcpSegment>, u16)> {
        self.ensure_state(TcpState::Established)?;
        if !self.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ) {
            return Ok((None, 0));
        }
        self.observe_activity(Instant::now());
        self.observe_peer_ecn_feedback(packet);
        let mut timers = 0;
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            timers |= self.receive_ack(
                packet,
                acknowledgment.raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
        }
        if let Some(segment) = self.receive_fin_in_established(packet)? {
            return Ok((Some(segment), timers));
        }
        Ok((None, timers))
    }

    fn receive_fin_in_established(&mut self, packet: &TcpPacket) -> CoreResult<Option<TcpSegment>> {
        if packet.flags.contains(TcpSegmentFlags::FIN)
            && packet.sequence.advance(packet.payload_len as u32) == self.rcv_nxt
        {
            self.rcv_nxt = self.rcv_nxt.advance(1);
            self.state = TcpState::CloseWait;
            return Ok(Some(self.control_segment(
                packet.local,
                packet.remote,
                TcpSegmentFlags::ACK,
                None,
                TcpCapabilities::default(),
            )));
        }
        Ok(None)
    }

    pub(crate) fn receive_close_side(
        &mut self,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
        if !self.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ) {
            return Ok(None);
        }
        self.observe_activity(Instant::now());
        self.observe_peer_ecn_feedback(packet);

        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.cacheline1.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let _ = self.receive_ack(
                packet,
                acknowledgment.raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
        }

        match self.state {
            TcpState::SynRcvd => self.receive_final_ack(packet),
            TcpState::FinWait1 => {
                if packet.flags.contains(TcpSegmentFlags::FIN) && packet.sequence == self.rcv_nxt {
                    self.rcv_nxt = self.rcv_nxt.advance(1);
                    self.state = if packet
                        .acknowledgment
                        .filter(|ack| {
                            packet.flags.contains(TcpSegmentFlags::ACK)
                                && self.accepts_ack(*ack)
                                && *ack == self.snd_nxt
                        })
                        .is_some()
                    {
                        self.timer_set(TCP_TIMER_TIME_WAIT);
                        TcpState::TimeWait
                    } else {
                        TcpState::Closing
                    };
                    return Ok(Some(self.control_segment(
                        packet.local,
                        packet.remote,
                        TcpSegmentFlags::ACK,
                        None,
                        TcpCapabilities::default(),
                    )));
                }
                if let Some(acknowledgment) = packet.acknowledgment
                    && packet.flags.contains(TcpSegmentFlags::ACK)
                    && self.accepts_ack(acknowledgment)
                    && acknowledgment == self.snd_nxt
                {
                    self.state = TcpState::FinWait2;
                }
                Ok(None)
            }
            TcpState::FinWait2 => {
                if packet.flags.contains(TcpSegmentFlags::FIN) && packet.sequence == self.rcv_nxt {
                    self.rcv_nxt = self.rcv_nxt.advance(1);
                    self.state = TcpState::TimeWait;
                    self.timer_set(TCP_TIMER_TIME_WAIT);
                    return Ok(Some(self.control_segment(
                        packet.local,
                        packet.remote,
                        TcpSegmentFlags::ACK,
                        None,
                        TcpCapabilities::default(),
                    )));
                }
                Ok(None)
            }
            TcpState::CloseWait => Ok(None),
            TcpState::Closing => {
                if let Some(acknowledgment) = packet.acknowledgment
                    && packet.flags.contains(TcpSegmentFlags::ACK)
                    && self.accepts_ack(acknowledgment)
                    && acknowledgment == self.snd_nxt
                {
                    self.state = TcpState::TimeWait;
                    self.timer_set(TCP_TIMER_TIME_WAIT);
                }
                Ok(None)
            }
            TcpState::LastAck => {
                if let Some(acknowledgment) = packet.acknowledgment
                    && packet.flags.contains(TcpSegmentFlags::ACK)
                    && self.accepts_ack(acknowledgment)
                    && acknowledgment == self.snd_nxt
                {
                    self.state = TcpState::Closed;
                }
                Ok(None)
            }
            TcpState::TimeWait => {
                if packet.flags.contains(TcpSegmentFlags::FIN) {
                    self.timer_set(TCP_TIMER_TIME_WAIT);
                    return Ok(Some(self.control_segment(
                        packet.local,
                        packet.remote,
                        TcpSegmentFlags::ACK,
                        None,
                        TcpCapabilities::default(),
                    )));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    #[inline]
    pub(crate) fn accept_payload(&self, packet: &TcpPacket) -> Option<(usize, u32)> {
        if packet.payload_len == 0 {
            return None;
        }
        let sequence = packet.sequence;
        let end_sequence = sequence.advance(packet.payload_len as u32);
        if end_sequence <= self.rcv_nxt {
            return None;
        }
        if sequence < self.rcv_nxt {
            let trim = sequence.distance_to(self.rcv_nxt);
            return Some((trim as usize, 0));
        }
        Some((0, self.rcv_nxt.distance_to(sequence)))
    }

    #[inline]
    pub(crate) fn receive_payload(
        &mut self,
        sequence: TcpSeq,
        trim: u32,
        delivered_len: u32,
        newest_ooo_start: Option<u32>,
        newest_ooo_len: u32,
    ) {
        if trim != 0 {
            self.sack
                .set_duplicate(self.negotiated_options().sack, sequence, self.rcv_nxt);
        }
        self.rcv_nxt = self.rcv_nxt.advance(delivered_len);
        if let Some(start) = newest_ooo_start {
            let left = self.rcv_nxt.advance(start);
            let right = left.advance(newest_ooo_len);
            self.sack
                .update_range(self.negotiated_options().sack, self.rcv_nxt, left, right);
            return;
        }
        self.sack.update_range(
            self.negotiated_options().sack,
            self.rcv_nxt,
            self.rcv_nxt,
            self.rcv_nxt,
        );
    }

    #[inline]
    pub(crate) fn observe_duplicate_payload(&mut self, sequence: TcpSeq, end_sequence: TcpSeq) {
        self.sack
            .set_duplicate(self.negotiated_options().sack, sequence, end_sequence);
    }

    #[inline]
    pub(crate) fn has_pending_sack_output(&self) -> bool {
        self.sack
            .has_pending_output(self.negotiated_options().timestamps)
    }

    #[inline]
    pub(crate) fn on_tcp_ready(
        &mut self,
        has_pending_tx: bool,
        local_capabilities: TcpCapabilities,
    ) -> Option<TcpSegment> {
        if self.state == TcpState::SynSent && !has_pending_tx && !self.recovery.has_unacked_data() {
            return self.tx_segment(0, local_capabilities).ok();
        }
        if self.state == TcpState::Established {
            if self.recovery.has_unacked_data() || has_pending_tx {
                self.timer_set(TCP_TIMER_RETRANSMIT);
            } else {
                self.timer_reset(TCP_TIMER_RETRANSMIT);
            }
            if self.snd_wnd == 0 && has_pending_tx {
                self.timer_set(TCP_TIMER_PERSIST);
            } else if self.snd_wnd != 0 || !self.recovery.has_unacked_data() {
                self.timer_reset(TCP_TIMER_PERSIST);
            }
            if has_pending_tx
                && self
                    .congestion
                    .next_send_delay(self.output_payload_len() as u32)
                    .is_some()
            {
                self.timer_set(TCP_TIMER_PACING);
            } else {
                self.timer_reset(TCP_TIMER_PACING);
            }
            self.timer_set(TCP_TIMER_KEEP_ALIVE);
        }
        let local = self.local?;
        match self.state {
            TcpState::Established if !has_pending_tx && self.has_pending_sack_output() => {
                Some(TcpSegment::new(
                    local,
                    self.remote(),
                    self.output_sequence(TcpSegmentFlags::ACK),
                    self.rcv_nxt(),
                    self.advertised_receive_window(self.rcv_wnd),
                    self.output_flags(TcpSegmentFlags::ACK),
                    self.output_capabilities(),
                    self.sack
                        .take_output(
                            self.negotiated_options().sack,
                            self.negotiated_options().timestamps,
                            TcpSegmentFlags::ACK,
                        )
                        .as_ref()
                        .map(|(blocks, len)| &blocks[..*len]),
                    self.next_local_timestamp(self.negotiated_options().timestamps),
                    None,
                    None,
                    0,
                ))
            }
            TcpState::FinWait1 | TcpState::LastAck if self.snd_una == self.snd_nxt => {
                self.snd_nxt = self.snd_nxt.advance(1);
                self.timer_set(TCP_TIMER_RETRANSMIT);
                Some(TcpSegment::new(
                    local,
                    self.remote(),
                    self.snd_una(),
                    self.rcv_nxt(),
                    self.advertised_receive_window(self.rcv_wnd),
                    TcpSegmentFlags::ACK | TcpSegmentFlags::FIN,
                    self.output_capabilities(),
                    None,
                    self.next_local_timestamp(self.negotiated_options().timestamps),
                    None,
                    None,
                    0,
                ))
            }
            TcpState::FinWait1 | TcpState::LastAck => Some(TcpSegment::new(
                local,
                self.remote(),
                self.snd_una(),
                self.rcv_nxt(),
                self.advertised_receive_window(self.rcv_wnd),
                TcpSegmentFlags::ACK | TcpSegmentFlags::FIN,
                self.output_capabilities(),
                None,
                self.next_local_timestamp(self.negotiated_options().timestamps),
                None,
                None,
                0,
            )),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn on_session_close(&mut self) {
        match self.state {
            TcpState::Established => {
                self.cacheline1.close_reason = Some(TcpCloseReason::LocalRequest);
                self.state = TcpState::FinWait1;
                self.timer_reset(TCP_TIMER_KEEP_ALIVE);
                self.timer_reset(TCP_TIMER_PACING);
            }
            TcpState::CloseWait => {
                self.cacheline1.close_reason = Some(TcpCloseReason::LocalRequest);
                self.state = TcpState::LastAck;
                self.timer_reset(TCP_TIMER_KEEP_ALIVE);
                self.timer_reset(TCP_TIMER_PACING);
            }
            _ => {}
        }
    }

    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer_id: u32,
        local_capabilities: TcpCapabilities,
    ) -> Option<TcpSegment> {
        if !self.timer_is_supported(timer_id) {
            return None;
        }
        self.timer_reset(timer_id);
        if !self.timer_dispatch_pending(timer_id) {
            return None;
        }
        match timer_id {
            TCP_TIMER_RETRANSMIT => self.on_retransmit_timer_expiry(local_capabilities),
            TCP_TIMER_RACK => self.on_rack_timer_expiry(),
            TCP_TIMER_TLP => self.on_tlp_timer_expiry(),
            TCP_TIMER_DELAYED_ACK => self.on_delayed_ack_timer_expiry(),
            TCP_TIMER_PERSIST => self.on_persist_timer_expiry(),
            TCP_TIMER_KEEP_ALIVE => self.on_keepalive_timer_expiry(),
            TCP_TIMER_TIME_WAIT => self.on_time_wait_timer_expiry(),
            TCP_TIMER_PACING => self.on_pacing_timer_expiry(),
            _ => None,
        }
    }

    /// Dispatch gate: the connection owns timer dispatch and only forwards the
    /// exact `timer_id` supplied by the runtime. Returns whether the pending
    /// timer may be dispatched in the current state.
    fn timer_dispatch_pending(&self, timer_id: u32) -> bool {
        match (self.state, timer_id) {
            (TcpState::SynSent, _)
            | (TcpState::Established, _)
            | (TcpState::FinWait1, TCP_TIMER_RETRANSMIT)
            | (TcpState::Closing, TCP_TIMER_RETRANSMIT)
            | (TcpState::LastAck, TCP_TIMER_RETRANSMIT)
            | (TcpState::TimeWait, _) => true,
            _ => false,
        }
    }

    fn on_retransmit_timer_expiry(
        &mut self,
        local_capabilities: TcpCapabilities,
    ) -> Option<TcpSegment> {
        if self.state == TcpState::SynSent {
            self.observe_retransmit_timeout();
            self.timer_set(TCP_TIMER_RETRANSMIT);
            let local = self.local?;
            let capabilities = normalize_tcp_capabilities(local_capabilities);
            let flags = if capabilities.ecn {
                TcpSegmentFlags::SYN | TcpSegmentFlags::ECE | TcpSegmentFlags::CWR
            } else {
                TcpSegmentFlags::SYN
            };
            return Some(TcpSegment::new(
                local,
                self.remote,
                self.iss.raw(),
                self.rcv_nxt.raw(),
                self.advertised_receive_window(self.rcv_wnd),
                flags,
                capabilities,
                None,
                self.next_local_timestamp(capabilities.timestamps),
                self.fast_open_cookie(),
                None,
                self.fast_open_syn_payload_len as usize,
            ));
        }
        match self.state {
            TcpState::Established => {
                let now = Instant::now();
                let sample = self.recovery.on_retransmission_timeout(
                    now,
                    self.snd_nxt,
                    &mut self.congestion,
                )?;
                self.observe_retransmit_timeout();
                self.timer_set(TCP_TIMER_RETRANSMIT);
                self.timer_reset(TCP_TIMER_RACK);
                self.timer_reset(TCP_TIMER_TLP);
                self.timer_reset(TCP_TIMER_PACING);
                self.tx_intent_sequence = Some(sample.sequence);
                self.tx_intent_payload_len = sample.payload_len;
                self.pacing_ready = true;
                None
            }
            TcpState::FinWait1 | TcpState::Closing | TcpState::LastAck => {
                self.observe_retransmit_timeout();
                self.timer_set(TCP_TIMER_RETRANSMIT);
                let local = self.local?;
                Some(TcpSegment::new(
                    local,
                    self.remote,
                    self.snd_una.raw(),
                    self.rcv_nxt.raw(),
                    self.advertised_receive_window(self.rcv_wnd),
                    TcpSegmentFlags::ACK | TcpSegmentFlags::FIN,
                    self.output_capabilities(),
                    None,
                    self.next_local_timestamp(self.negotiated_options().timestamps),
                    None,
                    None,
                    0,
                ))
            }
            _ => None,
        }
    }

    fn on_rack_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        let now = Instant::now();
        self.recovery
            .on_rack_timeout(now, self.snd_nxt, &mut self.congestion);
        let sample = self.recovery.take_rack_retransmit()?;
        if self
            .recovery
            .rack_timeout(now)
            .map(|duration| duration.as_millis().div_ceil(10).max(1) as u64)
            .is_some()
        {
            self.timer_set(TCP_TIMER_RACK);
        }
        self.recovery.on_retransmit_sent(sample.bytes);
        self.tx_intent_sequence = Some(sample.sequence);
        self.tx_intent_payload_len = sample.payload_len;
        self.pacing_ready = true;
        None
    }

    fn on_tlp_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        let sample = self.recovery.take_tlp_probe()?;
        if self
            .recovery
            .tlp_timeout(
                self.retransmit_timeout().smoothed_rtt(),
                self.retransmit_timeout().retransmit_timeout(),
            )
            .is_some()
        {
            self.timer_set(TCP_TIMER_TLP);
        }
        if self.recovery.in_recovery() {
            self.recovery.on_retransmit_sent(sample.bytes);
        }
        self.tx_intent_sequence = Some(sample.sequence);
        self.tx_intent_payload_len = sample.payload_len;
        self.pacing_ready = true;
        None
    }

    fn on_delayed_ack_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        let local = self.local?;
        Some(TcpSegment::new(
            local,
            self.remote(),
            self.snd_nxt(),
            self.rcv_nxt(),
            self.advertised_receive_window(self.rcv_wnd),
            self.output_flags(TcpSegmentFlags::ACK),
            self.output_capabilities(),
            None,
            self.next_local_timestamp(self.negotiated_options().timestamps),
            None,
            None,
            0,
        ))
    }

    fn on_persist_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        if self.snd_wnd != 0 {
            self.cacheline1.persist_attempts = 0;
            return None;
        }
        self.cacheline1.persist_attempts = self.cacheline1.persist_attempts.saturating_add(1);
        if self.tx_intent_sequence.is_some() {
            self.timer_set(TCP_TIMER_PERSIST);
            return None;
        }
        self.timer_set(TCP_TIMER_PERSIST);
        self.tx_intent_sequence = Some(self.snd_una);
        self.tx_intent_payload_len = 1;
        self.pacing_ready = true;
        None
    }

    fn on_time_wait_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.cacheline1.close_reason = Some(TcpCloseReason::LocalRequest);
        self.state = TcpState::Closed;
        None
    }

    fn on_keepalive_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        if self.keepalive.probes_sent >= self.keepalive.config.probe_limit {
            self.cacheline1.close_reason = Some(TcpCloseReason::KeepAliveTimeout);
            self.state = TcpState::Closed;
            self.timer_reset(TCP_TIMER_KEEP_ALIVE);
            return None;
        }
        self.keepalive.probes_sent = self.keepalive.probes_sent.saturating_add(1);
        self.timer_set(TCP_TIMER_KEEP_ALIVE);
        let local = self.local?;
        Some(TcpSegment::new(
            local,
            self.remote(),
            self.snd_nxt().wrapping_sub(1),
            self.rcv_nxt(),
            self.advertised_receive_window(self.rcv_wnd),
            self.output_flags(TcpSegmentFlags::ACK),
            self.output_capabilities(),
            None,
            self.next_local_timestamp(self.negotiated_options().timestamps),
            None,
            None,
            0,
        ))
    }

    fn on_pacing_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        if self
            .congestion
            .next_send_delay(self.output_payload_len() as u32)
            .is_none()
        {
            return None;
        }
        self.timer_set(TCP_TIMER_PACING);
        self.pacing_ready = true;
        if self.tx_intent_sequence.is_none() {
            self.tx_intent_sequence = Some(self.snd_nxt);
            self.tx_intent_payload_len = self.output_payload_len() as u32;
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn established_for_test(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        let mut connection = Self::new(connection_id, owner_worker, local_port, local, remote);
        let packet = TcpPacket {
            local: remote,
            remote: local.unwrap_or(remote),
            flags: TcpSegmentFlags::SYN,
            sequence: 7_000.into(),
            acknowledgment: None,
            advertised_window: u16::MAX,
            payload_offset: 0,
            payload_len: 0,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
        };
        let _ = connection
            .receive_syn(
                packet.local,
                packet.remote,
                packet.flags,
                packet.sequence,
                packet.advertised_window,
                packet.capabilities,
                packet.timestamp,
                0,
                TcpCapabilities::default(),
            )
            .expect("accept syn");
        let final_packet = TcpPacket {
            acknowledgment: Some(connection.snd_nxt().into()),
            flags: TcpSegmentFlags::ACK,
            ..packet
        };
        let _ = connection
            .receive_final_ack(&final_packet)
            .expect("accept final ack");
        connection
    }

    #[cfg(test)]
    pub(crate) fn established_with_capabilities_for_test(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        local_capabilities: TcpCapabilities,
        remote_capabilities: TcpCapabilities,
    ) -> Self {
        let mut connection =
            Self::established_for_test(connection_id, owner_worker, local_port, local, remote);
        let _ =
            connection.apply_peer_handshake_capabilities(local_capabilities, remote_capabilities);
        connection
    }

    #[cfg(test)]
    pub(crate) fn established_with_sack_for_test(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self::established_with_capabilities_for_test(
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            TcpCapabilities {
                max_segment_size: None,
                window_scale: None,
                sack: true,
                timestamps: false,
                ecn: false,
                accurate_ecn: false,
                fast_open: false,
            },
            TcpCapabilities {
                max_segment_size: None,
                window_scale: None,
                sack: true,
                timestamps: false,
                ecn: false,
                accurate_ecn: false,
                fast_open: false,
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn established_for_time_wait_test(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        let mut connection = Self::new(connection_id, owner_worker, local_port, local, remote);
        connection.state = TcpState::Established;
        connection.iss = TcpSeq::from(1_000);
        connection.irs = TcpSeq::from(7_000);
        connection.snd_una = TcpSeq::from(1_500);
        connection.snd_nxt = TcpSeq::from(1_500);
        connection.rcv_nxt = TcpSeq::from(7_000);
        connection
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
        accurate_ecn: local.accurate_ecn && remote.accurate_ecn,
        fast_open: local.fast_open && remote.fast_open,
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
        accurate_ecn: capabilities.accurate_ecn,
        fast_open: capabilities.fast_open,
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
    clamp_retransmit_timeout(srtt.saturating_add(rttvar.saturating_mul(4)))
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

#[inline(always)]
const fn timer_bit(timer_id: u32) -> u16 {
    1u16 << timer_id
}

#[inline(always)]
const fn timer_mask_contains(mask: u16, timer_id: u32) -> bool {
    (mask & timer_bit(timer_id)) != 0
}

#[inline]
fn ace_counter(flags: TcpSegmentFlags) -> u8 {
    ((flags.contains(TcpSegmentFlags::NS) as u8) << 2)
        | ((flags.contains(TcpSegmentFlags::CWR) as u8) << 1)
        | (flags.contains(TcpSegmentFlags::ECE) as u8)
}

#[inline]
fn ace_flags(counter: u64) -> TcpSegmentFlags {
    let counter = (counter & 0x07) as u8;
    let mut flags = TcpSegmentFlags::empty();
    if counter & 0b100 != 0 {
        flags.insert(TcpSegmentFlags::NS);
    }
    if counter & 0b010 != 0 {
        flags.insert(TcpSegmentFlags::CWR);
    }
    if counter & 0b001 != 0 {
        flags.insert(TcpSegmentFlags::ECE);
    }
    flags
}

#[inline]
fn ace_delta(previous: u8, next: u8) -> u8 {
    next.wrapping_sub(previous) & 0x07
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::congestion::BbrController;
    use crate::transport::congestion::{
        AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample,
    };
    fn established_connection() -> TcpConnection<BbrController> {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        TcpConnection::established_with_sack_for_test(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    #[derive(Clone, Copy, Debug)]
    struct TestCongestionController {
        max_datagram_size: u32,
        congestion_window: u32,
    }

    impl CongestionController for TestCongestionController {
        fn new(max_datagram_size: u32) -> Self {
            Self {
                max_datagram_size,
                congestion_window: max_datagram_size.saturating_mul(4),
            }
        }

        fn metrics(&self) -> CongestionMetrics {
            CongestionMetrics {
                congestion_window: self.congestion_window,
                pacing_rate_bytes_per_second: None,
                delivered: 0,
                max_bandwidth_bytes_per_second: 0,
                min_rtt: None,
            }
        }

        fn max_datagram_size(&self) -> u32 {
            self.max_datagram_size
        }

        fn congestion_window(&self) -> u32 {
            self.congestion_window
        }

        fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
            None
        }

        fn delivered(&self) -> u64 {
            0
        }

        fn min_rtt(&self) -> Option<Duration> {
            None
        }

        fn max_bandwidth_bytes_per_second(&self) -> u64 {
            0
        }

        fn on_packet_sent(&mut self, _: PacketNumber, _: u32, _: u32, _: Instant) {}

        fn on_ack(&mut self, _: Instant, _: AckedPacket, _: RttSample, _: u32) {}

        fn on_end_acks(&mut self, _: Instant, _: u32, _: bool, _: PacketNumber) {}

        fn on_loss(&mut self, _: Instant, lost: LostPacket, _: bool) {
            let halved = self.congestion_window / 2;
            self.congestion_window = halved.max(self.max_datagram_size.max(lost.bytes));
        }

        fn on_mtu_update(&mut self, max_datagram_size: u32) {
            self.max_datagram_size = max_datagram_size;
            self.congestion_window = self.congestion_window.max(max_datagram_size);
        }

        fn next_send_delay(&self, _: u32) -> Option<Duration> {
            None
        }
    }

    fn established_connection_with_test_controller() -> TcpConnection<TestCongestionController> {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        TcpConnection::established_for_test(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    #[derive(Clone, Copy, Debug)]
    struct PacingTestCongestionController {
        max_datagram_size: u32,
        congestion_window: u32,
        send_delay: Duration,
    }

    impl CongestionController for PacingTestCongestionController {
        fn new(max_datagram_size: u32) -> Self {
            Self {
                max_datagram_size,
                congestion_window: max_datagram_size.saturating_mul(4),
                send_delay: Duration::from_millis(25),
            }
        }

        fn metrics(&self) -> CongestionMetrics {
            CongestionMetrics {
                congestion_window: self.congestion_window,
                pacing_rate_bytes_per_second: Some(40_000),
                delivered: 0,
                max_bandwidth_bytes_per_second: 40_000,
                min_rtt: None,
            }
        }

        fn max_datagram_size(&self) -> u32 {
            self.max_datagram_size
        }

        fn congestion_window(&self) -> u32 {
            self.congestion_window
        }

        fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
            Some(40_000)
        }

        fn delivered(&self) -> u64 {
            0
        }

        fn min_rtt(&self) -> Option<Duration> {
            None
        }

        fn max_bandwidth_bytes_per_second(&self) -> u64 {
            40_000
        }

        fn on_packet_sent(&mut self, _: PacketNumber, _: u32, _: u32, _: Instant) {}

        fn on_ack(&mut self, _: Instant, _: AckedPacket, _: RttSample, _: u32) {}

        fn on_end_acks(&mut self, _: Instant, _: u32, _: bool, _: PacketNumber) {}

        fn on_loss(&mut self, _: Instant, _: LostPacket, _: bool) {}

        fn on_mtu_update(&mut self, max_datagram_size: u32) {
            self.max_datagram_size = max_datagram_size;
            self.congestion_window = self.congestion_window.max(max_datagram_size);
        }

        fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
            (pending_bytes != 0).then_some(self.send_delay)
        }
    }

    fn established_connection_with_pacing_controller()
    -> TcpConnection<PacingTestCongestionController> {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        TcpConnection::established_for_test(
            Some(TcpConnectionId::new(11)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    #[test]
    fn tcp_negotiated_options_preserve_accurate_ecn_when_both_peers_support_it() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: true,
            fast_open: false,
        };
        let negotiated = connection.apply_peer_handshake_capabilities(local_caps, local_caps);

        assert!(negotiated.ecn);
        assert!(negotiated.accurate_ecn);
    }

    #[test]
    fn tcp_syn_retransmit_sets_classic_ecn_handshake_flags() {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::<BbrController>::new(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: true,
            fast_open: false,
        };
        connection.connect_state(100);

        let segment = connection
            .on_tcp_ready(false, local_caps)
            .expect("initial syn should be emitted");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");

        assert!(parsed.syn());
        assert!(parsed.ece());
        assert!(parsed.cwr());
    }

    #[test]
    fn tcp_syn_data_retransmit_preserves_payload_len_and_cookie() {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::<BbrController>::new(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: false,
            accurate_ecn: false,
            fast_open: true,
        };
        connection.set_fast_open_cookie((&[1, 2, 3, 4][..]).try_into().ok());
        connection.connect_state(100);
        let now = Instant::now();
        let _ = connection
            .commit_payload_tx(5, now)
            .expect("commit syn payload");

        let segment = connection
            .on_tcp_timer_expiry(TCP_TIMER_RETRANSMIT, local_caps)
            .expect("retransmit");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        let options = hammer_core::protocol::tcp::tcp_options_from_bytes(parsed.options());

        assert_eq!(segment.payload_len(), 5);
        assert!(parsed.syn());
        assert_eq!(options.fast_open_cookie.as_deref(), Some(&[1, 2, 3, 4][..]));
    }

    #[test]
    fn tcp_syn_data_ack_releases_payload_without_consuming_syn_control_byte() {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::<BbrController>::new(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        connection.set_fast_open_cookie((&[1, 2, 3, 4][..]).try_into().ok());
        connection.connect_state(1000);
        connection.fast_open_syn_payload_len = 5;
        connection.snd_nxt = connection.iss.advance(6);
        connection.snd_una = connection.iss;
        connection.apply_ack(connection.iss.advance(6), u16::MAX);

        assert_eq!(connection.take_acked_tx_len(connection.iss.raw()), 5);
        assert_eq!(connection.fast_open_syn_payload_len, 0);
    }

    #[test]
    fn tcp_established_supports_pacing_and_keepalive_timers() {
        let mut connection = established_connection_with_pacing_controller();

        assert!(connection.timer_is_supported(TCP_TIMER_PACING));
        assert!(connection.timer_is_supported(TCP_TIMER_KEEP_ALIVE));
        assert_eq!(
            connection.timer_ticks(TCP_TIMER_PACING, Instant::now()),
            None
        );

        let _ = connection.on_tcp_ready(true, TcpCapabilities::default());

        assert!(connection.timer_is_active(TCP_TIMER_PACING));
        assert_eq!(
            connection.timer_ticks(TCP_TIMER_PACING, Instant::now()),
            Some(3)
        );
        assert_eq!(
            connection.timer_ticks(TCP_TIMER_KEEP_ALIVE, Instant::now()),
            Some(7_500)
        );
    }

    #[test]
    fn tcp_pacing_timer_expiry_rearms_and_requests_tx_dispatch() {
        let mut connection = established_connection_with_pacing_controller();

        let _ = connection.on_tcp_ready(true, TcpCapabilities::default());

        let pacing_segment =
            connection.on_tcp_timer_expiry(TCP_TIMER_PACING, TcpCapabilities::default());
        assert!(pacing_segment.is_none());

        assert!(connection.timer_is_active(TCP_TIMER_PACING));
        assert!(connection.pacing_ready());
    }

    #[test]
    fn tcp_keepalive_timer_expiry_probes_then_closes_idle_connection() {
        let mut connection = established_connection();

        connection.timer_set(TCP_TIMER_KEEP_ALIVE);
        connection.keepalive.last_activity_at = Instant::now()
            .checked_sub(connection.keepalive.config.idle)
            .expect("move keepalive activity into the past");

        for probe in 0..connection.keepalive.config.probe_limit {
            let segment = connection
                .on_tcp_timer_expiry(TCP_TIMER_KEEP_ALIVE, TcpCapabilities::default())
                .expect("keepalive probe");
            let mut header = [0u8; 64];
            let header_len = segment.write_header(&mut header).expect("write header");
            let parsed =
                etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");

            assert!(parsed.ack());
            assert_eq!(
                parsed.sequence_number(),
                connection.snd_nxt().wrapping_sub(1),
            );
            assert_eq!(connection.keepalive.probes_sent, probe + 1);
            assert!(connection.timer_is_active(TCP_TIMER_KEEP_ALIVE));
        }

        assert!(
            connection
                .on_tcp_timer_expiry(TCP_TIMER_KEEP_ALIVE, TcpCapabilities::default())
                .is_none()
        );
        assert_eq!(connection.state(), TcpState::Closed);
        assert_eq!(
            connection.close_reason(),
            Some(TcpCloseReason::KeepAliveTimeout)
        );
    }

    #[test]
    fn tcp_keepalive_config_defaults_match_prior_constants_and_with_keepalive_overrides() {
        let default_config = TcpKeepaliveConfig::default();
        assert_eq!(default_config.idle, Duration::from_secs(75));
        assert_eq!(default_config.probe_interval, Duration::from_secs(75));
        assert_eq!(default_config.probe_limit, 8);

        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let connection = TcpConnection::<BbrController>::new(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        assert_eq!(connection.keepalive.config, default_config);

        let custom = TcpKeepaliveConfig {
            idle: Duration::from_secs(30),
            probe_interval: Duration::from_secs(10),
            probe_limit: 3,
        };
        let customized = connection.with_keepalive(custom);
        assert_eq!(customized.keepalive.config, custom);
    }

    #[test]
    fn tcp_local_close_tracks_fin_in_sequence_space_and_retransmit_timer() {
        let mut connection = established_connection();
        let initial_snd_nxt = connection.snd_nxt();

        connection.on_session_close();
        let segment = connection
            .on_tcp_ready(false, TcpCapabilities::default())
            .expect("fin");

        assert_eq!(connection.state(), TcpState::FinWait1);
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        assert_eq!(parsed.sequence_number(), initial_snd_nxt);
        assert_eq!(connection.snd_nxt(), initial_snd_nxt.wrapping_add(1));
        assert!(connection.timer_is_active(TCP_TIMER_RETRANSMIT));
    }

    #[test]
    fn tcp_finwait1_ack_of_local_fin_advances_to_finwait2() {
        let mut connection = established_connection();
        connection.on_session_close();
        let _ = connection
            .on_tcp_ready(false, TcpCapabilities::default())
            .expect("fin");
        let packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let control = connection.receive_close_side(&packet).expect("receive ack");

        assert!(control.is_none());
        assert_eq!(connection.state(), TcpState::FinWait2);
    }

    #[test]
    fn tcp_simultaneous_close_moves_to_closing_until_fin_is_acked() {
        let mut connection = established_connection();
        connection.on_session_close();
        let _ = connection
            .on_tcp_ready(false, TcpCapabilities::default())
            .expect("fin");
        let fin = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_una().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let ack = connection.receive_close_side(&fin).expect("receive fin");
        assert!(ack.is_some());
        assert_eq!(connection.state(), TcpState::Closing);

        let final_ack = TcpPacket {
            acknowledgment: Some(connection.snd_nxt().into()),
            flags: TcpSegmentFlags::ACK,
            ..fin
        };
        let _ = connection
            .receive_close_side(&final_ack)
            .expect("receive fin ack");

        assert_eq!(connection.state(), TcpState::TimeWait);
        assert!(connection.timer_is_active(TCP_TIMER_TIME_WAIT));
    }

    #[test]
    fn tcp_receive_syn_echoes_peer_timestamp_in_syn_ack() {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::<BbrController>::new(
            Some(TcpConnectionId::new(9)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let packet = TcpPacket {
            local,
            remote,
            sequence: 10.into(),
            acknowledgment: None,
            advertised_window: 4096,
            flags: TcpSegmentFlags::SYN,
            capabilities: TcpCapabilities {
                max_segment_size: Some(1460),
                window_scale: None,
                sack: true,
                timestamps: true,
                ecn: false,
                accurate_ecn: false,
                fast_open: false,
            },
            sack_blocks: Vec::new().into(),
            timestamp: Some(TcpTimestampOption {
                tsval: 55,
                tsecr: 0,
            }),
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let segment = connection
            .receive_syn(
                packet.local,
                packet.remote,
                packet.flags,
                packet.sequence,
                packet.advertised_window,
                packet.capabilities,
                packet.timestamp,
                0,
                local_caps,
            )
            .expect("receive syn")
            .expect("syn ack");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        let options = hammer_core::protocol::tcp::tcp_options_from_bytes(parsed.options());
        let timestamp = options.timestamp.expect("timestamp");

        assert_eq!(timestamp.tsecr, 55);
        assert_ne!(timestamp.tsval, 0);
    }

    #[test]
    fn tcp_established_ack_echoes_latest_peer_timestamp() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        let packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: Some(TcpTimestampOption {
                tsval: 88,
                tsecr: 0,
            }),
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        assert!(connection.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ));
        let segment = connection.control_segment(
            packet.local,
            packet.remote,
            TcpSegmentFlags::ACK,
            None,
            local_caps,
        );
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        let options = hammer_core::protocol::tcp::tcp_options_from_bytes(parsed.options());
        let timestamp = options.timestamp.expect("timestamp");

        assert_eq!(timestamp.tsecr, 88);
        assert_ne!(timestamp.tsval, 0);
    }

    #[test]
    fn tcp_timestamp_is_required_when_negotiated() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        let packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        assert!(!connection.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ));
    }

    #[test]
    fn tcp_paws_rejects_stale_timestamp() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        let first = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: Some(TcpTimestampOption {
                tsval: 88,
                tsecr: 0,
            }),
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let stale = TcpPacket {
            timestamp: Some(TcpTimestampOption {
                tsval: 77,
                tsecr: 0,
            }),
            ..first.clone()
        };

        assert!(connection.observe_inbound_timestamp(
            first.flags,
            first.timestamp,
            first.sequence,
            first.payload_len,
        ));
        assert!(!connection.observe_inbound_timestamp(
            stale.flags,
            stale.timestamp,
            stale.sequence,
            stale.payload_len,
        ));
    }

    #[test]
    fn tcp_paws_accepts_stale_timestamp_after_idle_window() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        let first = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: Some(TcpTimestampOption {
                tsval: 88,
                tsecr: 0,
            }),
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let stale = TcpPacket {
            timestamp: Some(TcpTimestampOption {
                tsval: 77,
                tsecr: 0,
            }),
            ..first.clone()
        };

        assert!(connection.observe_inbound_timestamp(
            first.flags,
            first.timestamp,
            first.sequence,
            first.payload_len,
        ));
        connection.timestamps.recent_remote_at = Some(
            Instant::now()
                .checked_sub(TCP_PAWS_IDLE + Duration::from_secs(1))
                .expect("recent remote age"),
        );
        assert!(connection.observe_inbound_timestamp(
            stale.flags,
            stale.timestamp,
            stale.sequence,
            stale.payload_len,
        ));
        assert_eq!(connection.timestamps.recent_remote, Some(77));
    }

    #[test]
    fn tcp_timestamp_recent_only_moves_when_segment_covers_rcv_nxt() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        let in_order = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: Some(TcpTimestampOption {
                tsval: 88,
                tsecr: 0,
            }),
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let out_of_order = TcpPacket {
            sequence: TcpSeq::from(connection.rcv_nxt()).advance(32),
            timestamp: Some(TcpTimestampOption {
                tsval: 99,
                tsecr: 0,
            }),
            ..in_order.clone()
        };

        assert!(connection.observe_inbound_timestamp(
            in_order.flags,
            in_order.timestamp,
            in_order.sequence,
            in_order.payload_len,
        ));
        assert!(connection.observe_inbound_timestamp(
            out_of_order.flags,
            out_of_order.timestamp,
            out_of_order.sequence,
            out_of_order.payload_len,
        ));
        let segment = connection.control_segment(
            in_order.local,
            in_order.remote,
            TcpSegmentFlags::ACK,
            None,
            local_caps,
        );
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        let options = hammer_core::protocol::tcp::tcp_options_from_bytes(parsed.options());
        let timestamp = options.timestamp.expect("timestamp");

        assert_eq!(timestamp.tsecr, 88);
    }

    #[test]
    fn tcp_retransmitted_ack_sample_does_not_update_rto() {
        let now = Instant::now();
        let mut connection = established_connection();
        let baseline = connection.retransmit_timeout().retransmit_timeout();

        connection
            .recovery
            .record_sent(1, TcpSeq::from(1_000), TcpSeq::from(2_000), 1_000, 0, now);
        let _ = connection.recovery.take_tlp_probe().expect("tlp probe");
        let packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(2_000.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let _ = connection.observe_ack_progress(&packet, TcpSeq::from(2_000), &[]);

        assert_eq!(
            connection.retransmit_timeout().retransmit_timeout(),
            baseline
        );
    }

    #[test]
    fn tcp_timestamp_echo_ack_updates_rto_for_retransmitted_ack() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: Some(1460),
            window_scale: None,
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        let now = Instant::now();
        let sequence = connection.snd_nxt();
        let end_sequence = TcpSeq::from(sequence).advance(1).raw();
        connection.snd_nxt = TcpSeq::from(end_sequence);
        let sent = connection
            .next_local_timestamp(true)
            .expect("local timestamp for outbound packet");
        connection.recovery.record_sent(
            1,
            TcpSeq::from(sequence),
            TcpSeq::from(end_sequence),
            1,
            0,
            now,
        );
        let _ = connection.recovery.take_tlp_probe().expect("tlp probe");
        let packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(end_sequence.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: Some(TcpTimestampOption {
                tsval: 123,
                tsecr: sent.tsval,
            }),
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        std::thread::sleep(Duration::from_millis(60));
        let baseline = connection.retransmit_timeout().retransmit_timeout();
        let _ = connection.observe_ack_progress(&packet, TcpSeq::from(end_sequence), &[]);
        assert!(connection.retransmit_timeout().smoothed_rtt().is_some());
        assert!(connection.retransmit_timeout().rtt_variance().is_some());
        assert!(connection.retransmit_timeout().retransmit_timeout() > baseline);
    }

    #[test]
    fn bytes_in_flight_cached_matches_recovery_after_record_and_take() {
        let mut connection = established_connection_with_test_controller();
        let capabilities = TcpCapabilities {
            max_segment_size: Some(1_000),
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(capabilities, capabilities);
        connection.snd_wnd = 8_000;
        connection.snd_una = TcpSeq::from(1_000);
        connection.snd_nxt = TcpSeq::from(1_000);

        let now = Instant::now();
        for i in 0..5u32 {
            let _ = connection
                .commit_payload_tx(1_000, now + Duration::from_millis(i as u64))
                .expect("commit payload tx");
            assert_eq!(
                connection.bytes_in_flight_cached,
                connection.recovery.bytes_in_flight(),
                "mirror must match recovery after commit {i}"
            );
        }
        assert_eq!(connection.snd_nxt, TcpSeq::from(6_000));
        assert_eq!(connection.recovery.bytes_in_flight(), 5_000);

        let sack_packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(1_000.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: std::vec::Vec::from([TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }])
            .into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let _ = connection.receive_ack(&sack_packet, 1_000, 8_000, &sack_packet.sack_blocks);
        assert_eq!(
            connection.bytes_in_flight_cached,
            connection.recovery.bytes_in_flight(),
            "mirror must match recovery after sack"
        );
        assert_eq!(connection.recovery.bytes_in_flight(), 4_000);

        let local = connection.local().expect("local");
        let remote = connection.remote();
        let rcv_nxt = connection.rcv_nxt().into();
        let ack_packet = |ack: u32| TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt,
            acknowledgment: Some(ack.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: std::vec::Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let _ = connection.receive_ack(&ack_packet(3_000), 3_000, 8_000, &[]);
        assert_eq!(
            connection.bytes_in_flight_cached,
            connection.recovery.bytes_in_flight(),
            "mirror must match recovery after partial ack to 3k"
        );
        assert_eq!(connection.recovery.bytes_in_flight(), 3_000);

        for i in 0..3u32 {
            let _ = connection
                .commit_payload_tx(500, now + Duration::from_millis(10 + i as u64))
                .expect("commit payload tx mid-recovery");
            assert_eq!(
                connection.bytes_in_flight_cached,
                connection.recovery.bytes_in_flight(),
                "mirror must match recovery after mid-recovery commit {i}"
            );
        }

        let _ = connection.receive_ack(&ack_packet(4_000), 4_000, 8_000, &[]);
        assert_eq!(
            connection.bytes_in_flight_cached,
            connection.recovery.bytes_in_flight(),
            "mirror must match recovery after ack to 4k"
        );
    }

    #[test]
    fn tcp_tx_payload_budget_is_limited_by_prr_during_recovery() {
        let mut connection = established_connection_with_test_controller();
        let capabilities = TcpCapabilities {
            max_segment_size: Some(1_000),
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(capabilities, capabilities);
        connection.snd_wnd = 8_000;
        connection.snd_una = TcpSeq::from(1_000);
        connection.snd_nxt = TcpSeq::from(1_000);

        let now = Instant::now();
        for i in 0..5u32 {
            let _ = connection
                .commit_payload_tx(1_000, now + Duration::from_millis(i as u64))
                .expect("commit payload tx");
        }
        assert_eq!(connection.snd_nxt, TcpSeq::from(6_000));
        assert_eq!(connection.recovery.bytes_in_flight(), 5_000);

        let sack_blocks = [TcpSackBlock {
            left_edge: TcpSeq::from(2_000),
            right_edge: TcpSeq::from(3_000),
        }];
        let sack_packet = TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(1_000.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: std::vec::Vec::from(sack_blocks).into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let _ = connection.receive_ack(&sack_packet, 1_000, 8_000, &sack_blocks);
        assert_eq!(connection.recovery.bytes_in_flight(), 4_000);
        assert_eq!(connection.bytes_in_flight_cached, 4_000);

        connection.recovery.on_rack_timeout(
            now + Duration::from_secs(60),
            TcpSeq::from(connection.snd_nxt),
            &mut connection.congestion,
        );
        connection.refresh_bytes_in_flight_cached();
        connection.recovery.on_retransmit_sent(1_000);
        assert_eq!(connection.bytes_in_flight_cached, 4_000);
        assert!(connection.recovery.in_recovery());

        assert_eq!(
            connection.tx_payload_budget(1_000, now, TcpCapabilities::default()),
            0
        );

        let local = connection.local().expect("local");
        let remote = connection.remote();
        let rcv_nxt = connection.rcv_nxt().into();
        let ack_packet = |ack: u32| TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt,
            acknowledgment: Some(ack.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: std::vec::Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let _ = connection.receive_ack(&ack_packet(3_000), 3_000, 8_000, &[]);
        assert_eq!(connection.bytes_in_flight_cached, 3_000);

        assert_eq!(
            connection.tx_payload_budget(1_000, now, TcpCapabilities::default()),
            0
        );

        let _ = connection.receive_ack(&ack_packet(4_000), 4_000, 8_000, &[]);
        assert_eq!(connection.bytes_in_flight_cached, 2_000);

        assert_eq!(
            connection.tx_payload_budget(2_000, now, TcpCapabilities::default()),
            connection
                .recovery
                .recovery_send_space(
                    connection.recovery.bytes_in_flight(),
                    connection.congestion.max_datagram_size(),
                )
                .expect("recovery send space") as usize
        );
    }

    #[test]
    fn tcp_cwr_clears_pending_ece_echo() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);
        connection.observe_peer_ecn_feedback(&TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            flags: TcpSegmentFlags::ACK,
            sequence: 1.into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: Some(IpEcnCodepoint::Ce),
            payload_offset: 0,
            payload_len: 0,
        });
        assert!(connection.pending_ece_ack());

        connection.observe_peer_ecn_feedback(&TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            flags: TcpSegmentFlags::ACK | TcpSegmentFlags::CWR,
            sequence: 2.into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        });

        assert!(!connection.pending_ece_ack());
    }

    #[test]
    fn tcp_ack_ece_sets_congestion_feedback_without_enabling_ack_echo() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);

        connection.observe_peer_ecn_feedback(&TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            flags: TcpSegmentFlags::ACK | TcpSegmentFlags::ECE,
            sequence: 1.into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        });

        assert!(!connection.pending_ece_ack());
        assert!(connection.pending_ecn_ce());
    }

    #[test]
    fn tcp_ip_ce_sets_ack_echo_without_marking_congestion_feedback() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: false,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);

        connection.observe_peer_ecn_feedback(&TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            flags: TcpSegmentFlags::ACK,
            sequence: 1.into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: Some(IpEcnCodepoint::Ce),
            payload_offset: 0,
            payload_len: 0,
        });

        assert!(connection.pending_ece_ack());
        assert!(!connection.pending_ecn_ce());
    }

    #[test]
    fn tcp_accecn_ack_uses_ace_bits_for_ce_feedback() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: true,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);

        connection.observe_peer_ecn_feedback(&TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            flags: TcpSegmentFlags::ACK,
            sequence: 1.into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: Some(IpEcnCodepoint::Ce),
            payload_offset: 0,
            payload_len: 0,
        });

        let flags = connection.output_flags(TcpSegmentFlags::ACK);
        assert_eq!(ace_counter(flags), 1);
        assert!(!connection.pending_ece_ack());
    }

    #[test]
    fn tcp_accecn_ack_updates_pending_feedback_from_peer_ace_delta() {
        let mut connection = established_connection();
        let local_caps = TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: true,
            accurate_ecn: true,
            fast_open: false,
        };
        let _ = connection.apply_peer_handshake_capabilities(local_caps, local_caps);

        connection.observe_peer_ecn_feedback(&TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local"),
            flags: ace_flags(3) | TcpSegmentFlags::ACK,
            sequence: 1.into(),
            acknowledgment: Some(connection.snd_nxt().into()),
            advertised_window: u16::MAX,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        });

        assert_eq!(connection.ecn.pending_ce_feedback, 3);
        assert!(connection.pending_ecn_ce());
    }

    #[test]
    fn tcp_ace_helpers_round_trip_and_wrap_modulo_eight() {
        for value in 0_u64..8 {
            assert_eq!(u64::from(ace_counter(ace_flags(value))), value);
        }
        assert_eq!(ace_delta(6, 1), 3);
    }

    #[test]
    fn time_wait_ticks_is_60s() {
        assert_eq!(TCP_TIME_WAIT_TICKS, 6_000);
    }

    #[test]
    fn persist_timer_backoff_doubles_interval_each_attempt() {
        let now = Instant::now();
        let mut connection = established_connection();

        // Default RTO for a fresh connection is TCP_INITIAL_RETRANSMIT_TIMEOUT (50ms),
        // so the persist base interval is 50ms / 10 = 5 ticks.
        let base_ticks: u64 = (connection
            .retransmit_timeout()
            .retransmit_timeout()
            .as_millis()
            / 10) as u64;
        assert_eq!(base_ticks, 5);

        connection.snd_wnd = 0;
        connection.timer_set(TCP_TIMER_PERSIST);

        // Each persist expiry re-arms the timer and increments persist_attempts, so
        // the interval read before the N-th arm uses shift=N: base, base*2, base*4.
        let expected = [base_ticks, base_ticks * 2, base_ticks * 4];
        for (step, expected_ticks) in expected.iter().enumerate() {
            assert_eq!(
                connection.timer_ticks(TCP_TIMER_PERSIST, now),
                Some(*expected_ticks),
                "persist interval before arm {step}"
            );
            let _ = connection.on_tcp_timer_expiry(TCP_TIMER_PERSIST, TcpCapabilities::default());
        }

        // Drive a large RTO so base << 9 exceeds the 60s cap and verify the cap binds.
        while connection.retransmit_timeout().retransmit_timeout() < TCP_MAX_RETRANSMIT_TIMEOUT {
            connection.observe_retransmit_timeout();
        }
        assert_eq!(
            connection.retransmit_timeout().retransmit_timeout(),
            TCP_MAX_RETRANSMIT_TIMEOUT
        );
        let maxed_base_ticks: u64 = (TCP_MAX_RETRANSMIT_TIMEOUT.as_millis() / 10) as u64;

        // Reset attempts so the shift starts from 0 with the maxed RTO.
        connection.snd_wnd = 8_000;
        let _ = connection.on_tcp_timer_expiry(TCP_TIMER_PERSIST, TcpCapabilities::default());
        assert_eq!(connection.timer_ticks(TCP_TIMER_PERSIST, now), None);

        connection.snd_wnd = 0;
        connection.timer_set(TCP_TIMER_PERSIST);
        assert_eq!(
            connection.timer_ticks(TCP_TIMER_PERSIST, now),
            Some(maxed_base_ticks)
        );

        // Climb attempts until the shift cap (9) plus the 60s interval cap both bind.
        for _ in 0..15 {
            let _ = connection.on_tcp_timer_expiry(TCP_TIMER_PERSIST, TcpCapabilities::default());
        }
        let capped_ticks: u64 = TCP_MAX_RETRANSMIT_TIMEOUT.as_millis().div_ceil(10).max(1) as u64;
        assert_eq!(
            connection.timer_ticks(TCP_TIMER_PERSIST, now),
            Some(capped_ticks),
            "persist interval capped at 60s"
        );

        // Window opening resets the backoff so the next probe uses the base interval.
        connection.snd_wnd = 8_000;
        let _ = connection.on_tcp_timer_expiry(TCP_TIMER_PERSIST, TcpCapabilities::default());
        connection.snd_wnd = 0;
        connection.timer_set(TCP_TIMER_PERSIST);
        assert_eq!(
            connection.timer_ticks(TCP_TIMER_PERSIST, now),
            Some(maxed_base_ticks),
            "persist backoff resets when the window opens"
        );
    }

    use crate::session::SessionId;
    use crate::session::runtime::SessionDriverRuntime;
    use crate::transport::tcp::closing_session_for_test;

    fn peer_fin_packet(
        local: SocketAddr,
        remote: SocketAddr,
        rcv_nxt: u32,
        snd_nxt: u32,
    ) -> TcpPacket {
        TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt.into(),
            acknowledgment: Some(snd_nxt.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        }
    }

    fn drive_fin_ack_to_time_wait<C>(
        driver: &mut SessionDriverRuntime<TcpConnection<C>>,
        session_id: SessionId,
        local: SocketAddr,
        remote: SocketAddr,
    ) where
        C: CongestionController + 'static,
    {
        {
            let connection = driver.session_mut(session_id).expect("connection");
            connection.on_session_close();
            let _ = connection.on_tcp_ready(false, TcpCapabilities::default());
        }
        let (rcv_nxt, snd_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt.into(),
            acknowledgment: Some(snd_nxt.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let now = {
            let connection = driver.session_mut(session_id).expect("connection");
            let _ = connection
                .receive_close_side(&packet)
                .expect("receive fin ack");
            std::time::Instant::now()
        };
        let session = session_id.pool_index();
        let connection: *const TcpConnection<C> =
            driver.session(session_id).expect("connection") as *const _;
        crate::transport::tcp::sync_all_tcp_timers(
            driver.timers_mut(),
            unsafe { &*connection },
            session,
            now,
        )
        .expect("sync time wait timer");
    }

    fn enter_close_wait_for_passive_close_test(
        driver: &mut SessionDriverRuntime<TcpConnection<BbrController>>,
        session_id: SessionId,
        local: SocketAddr,
        remote: SocketAddr,
    ) {
        let (rcv_nxt, snd_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = peer_fin_packet(local, remote, rcv_nxt, snd_nxt);
        let connection = driver.session_mut(session_id).expect("connection");
        let (segment, _) = connection
            .receive_established(&packet)
            .expect("receive peer fin");
        assert!(
            segment.is_some(),
            "expected ack segment for in-sequence fin"
        );
        assert_eq!(connection.state(), TcpState::CloseWait);
        assert_eq!(connection.rcv_nxt(), rcv_nxt.wrapping_add(1));
    }

    #[test]
    fn tcp_passive_close_fin_in_established_enters_close_wait_and_acks() {
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        let (rcv_nxt, snd_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = peer_fin_packet(local, remote, rcv_nxt, snd_nxt);
        let connection = driver.session_mut(session_id).expect("connection");
        let (segment, _) = connection
            .receive_established(&packet)
            .expect("receive peer fin");

        let segment = segment.expect("ack segment");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed = etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp");
        assert!(parsed.ack());
        assert!(!parsed.fin());

        assert_eq!(connection.state(), TcpState::CloseWait);
        assert_eq!(connection.rcv_nxt(), rcv_nxt.wrapping_add(1));
    }

    #[test]
    fn tcp_passive_close_local_close_after_peer_fin_enters_last_ack() {
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        enter_close_wait_for_passive_close_test(&mut driver, session_id, local, remote);

        let connection = driver.session_mut(session_id).expect("connection");
        connection.on_session_close();
        assert_eq!(connection.state(), TcpState::LastAck);
    }

    #[test]
    fn tcp_passive_close_peer_ack_final_fin_closes() {
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        enter_close_wait_for_passive_close_test(&mut driver, session_id, local, remote);

        let connection = driver.session_mut(session_id).expect("connection");
        connection.on_session_close();
        assert_eq!(connection.state(), TcpState::LastAck);

        let snd_nxt = connection.snd_nxt();
        let ack_packet = TcpPacket {
            local: remote,
            remote: local,
            sequence: connection.rcv_nxt().into(),
            acknowledgment: Some(snd_nxt.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        connection
            .receive_close_side(&ack_packet)
            .expect("receive final ack");
        assert_eq!(connection.state(), TcpState::Closed);
    }

    #[test]
    fn tcp_fin_path_enters_time_wait_and_retains_session_until_expiry() {
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        drive_fin_ack_to_time_wait(&mut driver, session_id, local, remote);

        let connection = driver.session(session_id).expect("session");
        assert_eq!(connection.state(), TcpState::TimeWait);
        assert!(connection.timer_is_active(TCP_TIMER_TIME_WAIT));
    }

    #[test]
    fn tcp_time_wait_duplicate_fin_reacks_and_rearms_timer() {
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        drive_fin_ack_to_time_wait(&mut driver, session_id, local, remote);

        let (rcv_nxt, snd_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let duplicate_fin = TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt.into(),
            acknowledgment: Some(snd_nxt.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let segment = driver
            .session_mut(session_id)
            .expect("connection")
            .receive_close_side(&duplicate_fin)
            .expect("receive duplicate fin")
            .expect("ack segment");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed = etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp");

        assert!(parsed.ack());
        let connection = driver.session(session_id).expect("session");
        assert_eq!(connection.state(), TcpState::TimeWait);
        assert!(connection.timer_is_active(TCP_TIMER_TIME_WAIT));
    }
}
