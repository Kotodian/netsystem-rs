use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use super::TcpInputNext;
use super::congestion::State;
use super::output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, tcp_effective_output_payload_len, tcp_send_goal_size,
};
use super::recovery::{TcpRecoveryAck, TcpRecoveryState};
use super::sack::TcpSackState;
use super::segment::TcpSegment;
use super::timers::{TcpTimerKind, TcpTimerState, TcpTimers};
use crate::protocol::TcpEcnCodepoint;
use crate::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpError, TcpFastOpenCookie,
    TcpNegotiatedOptions, TcpPacket, TcpSackBlock, TcpSegmentFlags, TcpSeq, TcpState,
    TcpTimestampOption,
};
use crossbeam_utils::CachePadded;
use hammer_infra::pool::Index as PoolIndex;
use hammer_runtime::DataWorkerId;
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::runtime::RxDelivery;
use hammer_service::transport::congestion::{CongestionController, CongestionMetrics};
use thiserror::Error;

use hammer_service::session::SessionId;

const TCP_MAX_WINDOW_SCALE: u8 = 14;
const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

pub const TCP_INITIAL_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MIN_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MAX_RETRANSMIT_TIMEOUT: Duration = Duration::from_secs(60);
const TCP_DELAYED_ACK_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpKeepaliveConfig {
    idle: Duration,
    probe_interval: Duration,
    probe_limit: u8,
}

impl Default for TcpKeepaliveConfig {
    #[inline]
    fn default() -> Self {
        let policy = crate::active_tcp_policy();
        Self {
            idle: policy.keepalive_idle,
            probe_interval: policy.keepalive_probe_interval,
            probe_limit: policy.keepalive_probe_limit,
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

impl From<TcpConnectionError> for RuntimeError {
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
    min: Duration,
    max: Duration,
    skip_next_sample: bool,
}

impl TcpRetransmitTimeoutState {
    #[inline]
    pub fn new() -> Self {
        let policy = crate::active_tcp_policy();
        Self {
            srtt: None,
            rttvar: None,
            rto: policy.retransmit_initial,
            min: policy.retransmit_min,
            max: policy.retransmit_max,
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

    #[inline]
    fn clamp_timeout(&self, timeout: Duration) -> Duration {
        if timeout < self.min {
            self.min
        } else if timeout > self.max {
            self.max
        } else {
            timeout
        }
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
        self.rto = self.clamp_timeout(retransmit_timeout_from_estimate(
            self.srtt
                .expect("smoothed RTT should be initialized by ACK sample"),
            self.rttvar
                .expect("RTT variance should be initialized by ACK sample"),
        ));
        self.rto
    }

    #[inline]
    pub fn on_retransmission_timeout(&mut self) -> Duration {
        self.rto = self.clamp_timeout(self.rto * 2);
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
    session_id: SessionId,
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    close_reason: Option<TcpCloseReason>,
    local_port: u16,
    fast_open_cookie: Option<TcpFastOpenCookie>,
    persist_attempts: u8,
}

#[derive(Debug, Clone)]
pub struct TcpConnection {
    cacheline0: CachePadded<TcpConnectionCacheline0>,
    cacheline1: CachePadded<TcpConnectionCacheline1>,
    retransmit_timeout: TcpRetransmitTimeoutState,
    timestamps: TcpTimestampState,
    keepalive: TcpKeepaliveState,
    ecn: TcpEcnState,
    congestion: State,
    recovery: TcpRecoveryState,
    sack: TcpSackState,
    time_wait: Duration,
    nagle: bool,
    paws_idle: Duration,
    pmtu_enabled: bool,
    path_mtu_retransmit: bool,
}

impl Deref for TcpConnection {
    type Target = TcpConnectionCacheline0;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cacheline0
    }
}

impl DerefMut for TcpConnection {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cacheline0
    }
}

impl TcpConnection {
    #[inline]
    pub(super) fn timer_state(&self) -> &TcpTimerState {
        &self.timers
    }

    #[inline]
    pub(super) fn timer_state_mut(&mut self) -> &mut TcpTimerState {
        &mut self.timers
    }

    #[inline]
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        let policy = crate::active_tcp_policy();
        let mss = policy.mss as u32;
        let session_id = SessionId::new(connection_id.map_or(0, TcpConnectionId::get));
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
                snd_wnd: policy.receive_window,
                rcv_nxt: TcpSeq::from(0),
                rcv_wnd: policy.receive_window,
                negotiated_options: TcpNegotiatedOptions::default(),
                tx_intent_sequence: None,
                tx_intent_payload_len: 0,
                fast_open_syn_payload_len: 0,
                bytes_in_flight_cached: 0,
            }),
            cacheline1: CachePadded::new(TcpConnectionCacheline1 {
                session_id,
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
            congestion: State::new(policy.congestion, mss.max(1)),
            recovery: TcpRecoveryState::new(),
            sack: TcpSackState::default(),
            time_wait: policy.time_wait,
            nagle: policy.nagle,
            paws_idle: policy.paws_idle,
            pmtu_enabled: policy.pmtu_enabled,
            path_mtu_retransmit: false,
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
    pub(crate) fn session_id(&self) -> SessionId {
        self.cacheline1.session_id
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
    pub(crate) fn congestion(&self) -> &State {
        &self.congestion
    }

    #[inline]
    pub fn congestion_metrics(&self) -> CongestionMetrics {
        self.congestion.metrics()
    }

    #[inline]
    pub fn nagle(&self) -> bool {
        self.nagle
    }

    #[inline]
    pub fn time_wait(&self) -> Duration {
        self.time_wait
    }

    #[inline]
    pub fn paws_idle(&self) -> Duration {
        self.paws_idle
    }

    #[inline]
    pub fn keepalive_idle(&self) -> Duration {
        self.keepalive.config.idle
    }

    #[inline]
    pub fn keepalive_probe_interval(&self) -> Duration {
        self.keepalive.config.probe_interval
    }

    #[inline]
    pub fn keepalive_probe_limit(&self) -> u8 {
        self.keepalive.config.probe_limit
    }

    /// Clamp send MSS from an IP-side path MTU when PMTU is enabled.
    ///
    /// Returns `true` when the connection MSS was reduced. If unacked data is
    /// present, also marks a path-MTU retransmit request and arms a TX intent
    /// from `snd_una` under the new MSS so oversized in-flight work is retried.
    pub fn apply_path_mtu(&mut self, path_mtu: u16) -> bool {
        if !self.pmtu_enabled {
            return false;
        }
        let mss = hammer_service::net::pmtu::ipv4_path_mtu_to_mss(path_mtu);
        let current = self
            .negotiated_options
            .send_max_segment_size
            .filter(|value| *value != 0)
            .unwrap_or(u16::MAX);
        if mss >= current {
            return false;
        }
        self.negotiated_options.send_max_segment_size = Some(mss);
        self.congestion.on_mtu_update(u32::from(mss));
        if self.recovery.has_unacked_data() {
            self.path_mtu_retransmit = true;
            let unacked = self.snd_una.distance_to(self.snd_nxt);
            let intent_len = u32::from(mss).min(unacked).max(1);
            self.tx_intent_sequence = Some(self.snd_una);
            self.tx_intent_payload_len = intent_len;
            self.pacing_ready = true;
        }
        true
    }

    /// Take and clear the path-MTU retransmit request raised by [`Self::apply_path_mtu`].
    #[inline]
    pub fn take_path_mtu_retransmit(&mut self) -> bool {
        let pending = self.path_mtu_retransmit;
        self.path_mtu_retransmit = false;
        pending
    }

    /// Pull the IP-owned path MTU for this connection's remote and clamp MSS.
    pub fn refresh_path_mtu_from_cache(&mut self) -> bool {
        if !self.pmtu_enabled {
            return false;
        }
        let IpAddr::V4(remote) = self.remote.ip() else {
            return false;
        };
        let Some(cache) = hammer_service::net::pmtu::path_mtu_cache() else {
            return false;
        };
        let Some(path_mtu) = cache.path_mtu(IpAddr::V4(remote)) else {
            return false;
        };
        self.apply_path_mtu(path_mtu)
    }

    /// Test helper: record `bytes` of unacked payload so PMTU shrink can request retransmit.
    #[cfg(test)]
    pub(crate) fn mark_unacked_for_path_mtu_test(&mut self, bytes: u32) {
        let sequence = self.snd_una;
        let end = sequence.advance(bytes);
        self.snd_nxt = end;
        let packet_number = self.recovery.next_packet_number();
        self.recovery
            .record_sent(packet_number, sequence, end, bytes, bytes, Instant::now());
        self.refresh_bytes_in_flight_cached();
    }

    #[cfg(test)]
    pub(crate) fn set_nagle_for_test(&mut self, enabled: bool) {
        self.nagle = enabled;
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
    fn observe_activity(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.keepalive.last_activity_at = now;
        self.keepalive.probes_sent = 0;
        if self.state == TcpState::Established {
            let idle = self.keepalive.config.idle;
            timers.update(index, &mut self.timers, TcpTimerKind::KeepAlive, idle)?;
        }
        Ok(())
    }

    #[inline]
    fn persist_interval(&self) -> Duration {
        let shift = u32::from(self.cacheline1.persist_attempts.min(9));
        self.retransmit_timeout()
            .retransmit_timeout()
            .saturating_mul(1_u32 << shift)
            .min(self.retransmit_timeout.max)
    }

    #[inline]
    fn pacing_interval(&self) -> Option<Duration> {
        self.congestion
            .next_send_delay(self.output_payload_len() as u32)
    }

    fn sync_recovery_timers(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        now: Instant,
        recovery_timing_changed: bool,
    ) -> RuntimeResult<()> {
        if self.snd_wnd == 0 && self.recovery.has_unacked_data() {
            let interval = self.persist_interval();
            timers.set(index, &mut self.timers, TcpTimerKind::Persist, interval)?;
            timers.reset(index, &mut self.timers, TcpTimerKind::Rack);
            timers.reset(index, &mut self.timers, TcpTimerKind::Tlp);
            timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
            return Ok(());
        }

        timers.reset(index, &mut self.timers, TcpTimerKind::Persist);
        if recovery_timing_changed {
            if let Some(interval) = self.recovery.rack_timeout(now) {
                timers.update(index, &mut self.timers, TcpTimerKind::Rack, interval)?;
            } else {
                timers.reset(index, &mut self.timers, TcpTimerKind::Rack);
            }
            if let Some(interval) = self.recovery.tlp_timeout(
                self.retransmit_timeout().smoothed_rtt(),
                self.retransmit_timeout().retransmit_timeout(),
            ) {
                timers.update(index, &mut self.timers, TcpTimerKind::Tlp, interval)?;
            } else {
                timers.reset(index, &mut self.timers, TcpTimerKind::Tlp);
            }
        }
        if let Some(interval) = self.pacing_interval() {
            timers.set(index, &mut self.timers, TcpTimerKind::Pacing, interval)?;
        } else {
            timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
        }
        Ok(())
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
        if !self.accepts_ack(acknowledgment) {
            return;
        }
        self.snd_una = acknowledgment;
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
        if matches!(packet.ip_ecn, Some(TcpEcnCodepoint::Ce)) && self.negotiated_options().ecn {
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
        if matches!(packet.ip_ecn, Some(TcpEcnCodepoint::Ce)) {
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
    fn output_ip_ecn(&self, payload_len: usize, retransmit: bool) -> Option<TcpEcnCodepoint> {
        if !self.negotiated_options().ecn || payload_len == 0 || retransmit {
            return None;
        }
        Some(TcpEcnCodepoint::Ect0)
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
                .is_some_and(|recent_at| now.saturating_duration_since(recent_at) > self.paws_idle);
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
    pub(super) fn on_clean_in_order_payload(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
    ) -> RuntimeResult<bool> {
        if self.timers.is_active(TcpTimerKind::DelayedAck) {
            timers.reset(index, &mut self.timers, TcpTimerKind::DelayedAck);
            return Ok(true);
        }
        timers.set(
            index,
            &mut self.timers,
            TcpTimerKind::DelayedAck,
            TCP_DELAYED_ACK_INTERVAL,
        )?;
        Ok(false)
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
    pub(crate) fn ensure_state(&self, state: TcpState) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<Option<TcpSegment>> {
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

    pub(super) fn receive_open_reply(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        packet: &TcpPacket,
        local_capabilities: TcpCapabilities,
        now: Instant,
    ) -> RuntimeResult<Option<TcpSegment>> {
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
                timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
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
            timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
            self.observe_activity(index, timers, now)?;
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

    pub(super) fn receive_final_ack(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        packet: &TcpPacket,
        now: Instant,
    ) -> RuntimeResult<Option<TcpSegment>> {
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
            timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
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
        timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
        self.observe_activity(index, timers, now)?;
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
        let allowed = pending_len.min(peer_remaining).min(cc_remaining);
        if allowed == 0 {
            return 0;
        }
        let pacing_probe_len = allowed.min(self.send_goal_size());
        if !self.pacing_ready
            && self
                .congestion
                .next_send_delay(pacing_probe_len as u32)
                .is_some()
        {
            return 0;
        }
        // Nagle (orthogonal to pacing): while unacked data is in flight, only
        // emit full MSS multiples; hold a trailing partial segment. Skip during
        // recovery so PRR/limited transmit can send sub-MSS repair/new data.
        if self.nagle && bytes_in_flight > 0 && !self.recovery.in_recovery() {
            let mss = self.send_goal_size().max(1);
            return (allowed / mss) * mss;
        }
        allowed
    }

    pub(crate) fn tx_segment(
        &mut self,
        payload_len: usize,
        local_capabilities: TcpCapabilities,
    ) -> RuntimeResult<TcpSegment> {
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
    ) -> RuntimeResult<()> {
        if self.state == TcpState::SynSent {
            if self.tx_intent_sequence.is_some() {
                self.clear_tx_intent();
                return Ok(());
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
            self.pacing_ready = false;
            return Ok(());
        }
        self.ensure_state(TcpState::Established)?;
        if self.tx_intent_sequence.is_some() {
            self.clear_tx_intent();
            return Ok(());
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
        self.keepalive.last_activity_at = now;
        self.keepalive.probes_sent = 0;
        Ok(())
    }

    pub(super) fn sync_payload_tx_timers(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        now: Instant,
    ) -> RuntimeResult<()> {
        let retransmit = self.retransmit_timeout().retransmit_timeout();
        timers.validate_interval(retransmit)?;
        if self.state != TcpState::Established {
            return timers.set(
                index,
                &mut self.timers,
                TcpTimerKind::Retransmit,
                retransmit,
            );
        }
        let rack = self.recovery.rack_timeout(now);
        let tlp = self.recovery.tlp_timeout(
            self.retransmit_timeout().smoothed_rtt(),
            self.retransmit_timeout().retransmit_timeout(),
        );
        let persist = (self.snd_wnd == 0 && self.recovery.has_unacked_data())
            .then(|| self.persist_interval());
        let pacing = self.pacing_interval();
        let keepalive = self.keepalive.config.idle;
        for interval in [rack, tlp, persist, pacing, Some(keepalive)]
            .into_iter()
            .flatten()
        {
            timers.validate_interval(interval)?;
        }

        timers.set(
            index,
            &mut self.timers,
            TcpTimerKind::Retransmit,
            retransmit,
        )?;
        if let Some(interval) = rack {
            timers.update(index, &mut self.timers, TcpTimerKind::Rack, interval)?;
        } else {
            timers.reset(index, &mut self.timers, TcpTimerKind::Rack);
        }
        if let Some(interval) = tlp {
            timers.update(index, &mut self.timers, TcpTimerKind::Tlp, interval)?;
        } else {
            timers.reset(index, &mut self.timers, TcpTimerKind::Tlp);
        }
        if let Some(interval) = persist {
            timers.set(index, &mut self.timers, TcpTimerKind::Persist, interval)?;
        } else {
            timers.reset(index, &mut self.timers, TcpTimerKind::Persist);
        }
        if let Some(interval) = pacing {
            timers.update(index, &mut self.timers, TcpTimerKind::Pacing, interval)?;
        } else {
            timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
        }
        timers.update(index, &mut self.timers, TcpTimerKind::KeepAlive, keepalive)
    }

    pub(super) fn receive_ack_with_timers(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        packet: &TcpPacket,
        acknowledgment: u32,
        advertised_window: u16,
        sack_blocks: &[TcpSackBlock],
        now: Instant,
    ) -> RuntimeResult<()> {
        let acknowledgment = TcpSeq::from(acknowledgment);
        let ack_accepted = self.accepts_ack(acknowledgment);
        let snd_una_before = self.snd_una;
        let bytes_in_flight_before = self.recovery.bytes_in_flight();
        let rack_timeout_before = self.recovery.rack_timeout(now);
        let zero_window_before = self.snd_wnd == 0;
        let _ = self.observe_ack_progress(packet, acknowledgment, sack_blocks);
        self.apply_ack(acknowledgment, advertised_window);
        let recovery_progress = ack_accepted
            && (self.snd_una != snd_una_before
                || self.recovery.bytes_in_flight() != bytes_in_flight_before
                || self.recovery.rack_timeout(now) != rack_timeout_before);
        let recovery_timing_changed =
            recovery_progress || (ack_accepted && zero_window_before != (self.snd_wnd == 0));
        self.observe_activity(index, timers, now)?;
        timers.reset(index, &mut self.timers, TcpTimerKind::DelayedAck);
        if self.recovery.has_unacked_data() {
            let interval = self.retransmit_timeout().retransmit_timeout();
            if self.snd_una != snd_una_before {
                timers.update(index, &mut self.timers, TcpTimerKind::Retransmit, interval)?;
            } else {
                timers.set(index, &mut self.timers, TcpTimerKind::Retransmit, interval)?;
            }
        } else {
            timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
        }
        self.sync_recovery_timers(index, timers, now, recovery_timing_changed)
    }

    pub(super) fn receive_established_with_timers(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        packet: &TcpPacket,
        now: Instant,
    ) -> RuntimeResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::Established)?;
        if !self.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ) {
            return Ok(None);
        }
        self.observe_peer_ecn_feedback(packet);
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            self.receive_ack_with_timers(
                index,
                timers,
                packet,
                acknowledgment.raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                now,
            )?;
        } else {
            self.observe_activity(index, timers, now)?;
        }
        self.receive_fin_in_established(packet)
    }

    fn receive_fin_in_established(
        &mut self,
        packet: &TcpPacket,
    ) -> RuntimeResult<Option<TcpSegment>> {
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

    pub(super) fn receive_close_side(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        packet: &TcpPacket,
        now: Instant,
    ) -> RuntimeResult<Option<TcpSegment>> {
        if !self.observe_inbound_timestamp(
            packet.flags,
            packet.timestamp,
            packet.sequence,
            packet.payload_len,
        ) {
            return Ok(None);
        }
        self.observe_activity(index, timers, now)?;
        self.observe_peer_ecn_feedback(packet);

        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.cacheline1.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            self.receive_ack_with_timers(
                index,
                timers,
                packet,
                acknowledgment.raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                now,
            )?;
        }

        match self.state {
            TcpState::SynRcvd => self.receive_final_ack(index, timers, packet, now),
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
                        let time_wait = self.time_wait;
                        timers.update(
                            index,
                            &mut self.timers,
                            TcpTimerKind::TimeWait,
                            time_wait,
                        )?;
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
                    let time_wait = self.time_wait;
                    timers.update(index, &mut self.timers, TcpTimerKind::TimeWait, time_wait)?;
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
                    let time_wait = self.time_wait;
                    timers.update(index, &mut self.timers, TcpTimerKind::TimeWait, time_wait)?;
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
                    let time_wait = self.time_wait;
                    timers.update(index, &mut self.timers, TcpTimerKind::TimeWait, time_wait)?;
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
    pub(crate) fn receive_payload(&mut self, sequence: TcpSeq, trim: u32, delivery: RxDelivery) {
        if trim != 0 {
            self.sack
                .set_duplicate(self.negotiated_options().sack, sequence, self.rcv_nxt);
        }
        match delivery {
            RxDelivery::NotAccepted { .. } => {}
            RxDelivery::InOrder {
                accepted, promoted, ..
            } => {
                self.rcv_nxt = self
                    .rcv_nxt
                    .advance(accepted.get().saturating_add(promoted));
            }
            RxDelivery::OutOfOrder { newest, .. } => {
                let left = self.rcv_nxt.advance(newest.start());
                let right = left.advance(newest.len().get());
                self.sack
                    .update_range(self.negotiated_options().sack, self.rcv_nxt, left, right);
                return;
            }
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

    fn ready_segment(
        &mut self,
        has_pending_tx: bool,
        local_capabilities: TcpCapabilities,
    ) -> Option<TcpSegment> {
        if self.state == TcpState::SynSent && !has_pending_tx && !self.recovery.has_unacked_data() {
            return self.tx_segment(0, local_capabilities).ok();
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

    pub(super) fn on_tcp_ready(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        has_pending_tx: bool,
        local_capabilities: TcpCapabilities,
        _: Instant,
    ) -> RuntimeResult<Option<TcpSegment>> {
        if self.state == TcpState::Established {
            if self.recovery.has_unacked_data() || has_pending_tx {
                let interval = self.retransmit_timeout().retransmit_timeout();
                timers.set(index, &mut self.timers, TcpTimerKind::Retransmit, interval)?;
            } else {
                timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
            }
            if self.snd_wnd == 0 && has_pending_tx {
                let interval = self.persist_interval();
                timers.set(index, &mut self.timers, TcpTimerKind::Persist, interval)?;
            } else if self.snd_wnd != 0 || !self.recovery.has_unacked_data() {
                timers.reset(index, &mut self.timers, TcpTimerKind::Persist);
            }
            if has_pending_tx {
                if let Some(interval) = self.pacing_interval() {
                    timers.set(index, &mut self.timers, TcpTimerKind::Pacing, interval)?;
                } else {
                    timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
                }
            } else {
                timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
            }
            let keepalive = if self.keepalive.probes_sent == 0 {
                self.keepalive.config.idle
            } else {
                self.keepalive.config.probe_interval
            };
            timers.set(index, &mut self.timers, TcpTimerKind::KeepAlive, keepalive)?;
        }
        let segment = self.ready_segment(has_pending_tx, local_capabilities);
        if matches!(
            self.state,
            TcpState::SynSent | TcpState::FinWait1 | TcpState::Closing | TcpState::LastAck
        ) && (self.state == TcpState::SynSent || self.snd_una != self.snd_nxt)
        {
            let interval = self.retransmit_timeout().retransmit_timeout();
            timers.set(index, &mut self.timers, TcpTimerKind::Retransmit, interval)?;
        }
        if self.state != TcpState::Established {
            timers.reset(index, &mut self.timers, TcpTimerKind::KeepAlive);
            timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
        }
        Ok(segment)
    }

    #[inline]
    pub(super) fn on_session_close(&mut self, index: PoolIndex, timers: &mut TcpTimers) {
        match self.state {
            TcpState::Established => {
                self.cacheline1.close_reason = Some(TcpCloseReason::LocalRequest);
                self.state = TcpState::FinWait1;
                timers.reset(index, &mut self.timers, TcpTimerKind::KeepAlive);
                timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
            }
            TcpState::CloseWait => {
                self.cacheline1.close_reason = Some(TcpCloseReason::LocalRequest);
                self.state = TcpState::LastAck;
                timers.reset(index, &mut self.timers, TcpTimerKind::KeepAlive);
                timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
            }
            _ => {}
        }
    }

    pub(super) fn on_typed_timer_expiry(
        &mut self,
        index: PoolIndex,
        timers: &mut TcpTimers,
        kind: TcpTimerKind,
        local_capabilities: TcpCapabilities,
        now: Instant,
    ) -> RuntimeResult<Option<TcpSegment>> {
        if !self.timer_dispatch_pending(kind) {
            return Ok(None);
        }
        let segment = match kind {
            TcpTimerKind::Retransmit => self.on_retransmit_timer_expiry(local_capabilities),
            TcpTimerKind::Rack => self.on_rack_timer_expiry(),
            TcpTimerKind::Tlp => self.on_tlp_timer_expiry(),
            TcpTimerKind::DelayedAck => self.on_delayed_ack_timer_expiry(),
            TcpTimerKind::Persist => self.on_persist_timer_expiry(),
            TcpTimerKind::KeepAlive => self.on_keepalive_timer_expiry(),
            TcpTimerKind::TimeWait => self.on_time_wait_timer_expiry(),
            TcpTimerKind::Pacing => self.on_pacing_timer_expiry(),
        };

        match kind {
            TcpTimerKind::Retransmit => {
                timers.reset(index, &mut self.timers, TcpTimerKind::Rack);
                timers.reset(index, &mut self.timers, TcpTimerKind::Tlp);
                timers.reset(index, &mut self.timers, TcpTimerKind::Pacing);
                if matches!(
                    self.state,
                    TcpState::SynSent
                        | TcpState::Established
                        | TcpState::FinWait1
                        | TcpState::Closing
                        | TcpState::LastAck
                ) {
                    let interval = self.retransmit_timeout().retransmit_timeout();
                    timers.update(index, &mut self.timers, TcpTimerKind::Retransmit, interval)?;
                } else {
                    timers.reset(index, &mut self.timers, TcpTimerKind::Retransmit);
                }
            }
            TcpTimerKind::Rack => {
                if let Some(interval) = self.recovery.rack_timeout(now) {
                    timers.update(index, &mut self.timers, TcpTimerKind::Rack, interval)?;
                } else {
                    timers.reset(index, &mut self.timers, TcpTimerKind::Rack);
                }
            }
            TcpTimerKind::Tlp => {
                if let Some(interval) = self.recovery.tlp_timeout(
                    self.retransmit_timeout().smoothed_rtt(),
                    self.retransmit_timeout().retransmit_timeout(),
                ) {
                    timers.update(index, &mut self.timers, TcpTimerKind::Tlp, interval)?;
                } else {
                    timers.reset(index, &mut self.timers, TcpTimerKind::Tlp);
                }
            }
            TcpTimerKind::Persist => {
                if self.state == TcpState::Established && self.snd_wnd == 0 {
                    let interval = self.persist_interval();
                    timers.update(index, &mut self.timers, TcpTimerKind::Persist, interval)?;
                }
            }
            TcpTimerKind::KeepAlive => {
                if self.state == TcpState::Established {
                    let interval = self.keepalive.config.probe_interval;
                    timers.update(index, &mut self.timers, TcpTimerKind::KeepAlive, interval)?;
                } else {
                    timers.reset(index, &mut self.timers, TcpTimerKind::KeepAlive);
                }
            }
            TcpTimerKind::Pacing => {
                if let Some(interval) = self.pacing_interval() {
                    timers.update(index, &mut self.timers, TcpTimerKind::Pacing, interval)?;
                }
            }
            TcpTimerKind::DelayedAck | TcpTimerKind::TimeWait => {}
        }
        Ok(segment)
    }

    fn timer_dispatch_pending(&self, kind: TcpTimerKind) -> bool {
        match (self.state, kind) {
            (TcpState::SynSent, _)
            | (TcpState::Established, _)
            | (TcpState::FinWait1, TcpTimerKind::Retransmit)
            | (TcpState::Closing, TcpTimerKind::Retransmit)
            | (TcpState::LastAck, TcpTimerKind::Retransmit)
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
                self.tx_intent_sequence = Some(sample.sequence);
                self.tx_intent_payload_len = sample.payload_len;
                self.pacing_ready = true;
                None
            }
            TcpState::FinWait1 | TcpState::Closing | TcpState::LastAck => {
                self.observe_retransmit_timeout();
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
        self.recovery.on_retransmit_sent(sample.bytes);
        self.tx_intent_sequence = Some(sample.sequence);
        self.tx_intent_payload_len = sample.payload_len;
        self.pacing_ready = true;
        None
    }

    fn on_tlp_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        let sample = self.recovery.take_tlp_probe()?;
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
            return None;
        }
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
            return None;
        }
        self.keepalive.probes_sent = self.keepalive.probes_sent.saturating_add(1);
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
        let now = Instant::now();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
        let _ = connection
            .receive_final_ack(PoolIndex::new(0, 0), &mut timers, &final_packet, now)
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
    srtt.saturating_add(rttvar.saturating_mul(4))
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
    use crate::congestion::{Algorithm, State};
    use crate::timers::TcpTimers;
    use hammer_infra::pool::Pool;
    use hammer_service::session::runtime::{OooSpan, RxDelivery};
    use hammer_service::transport::congestion::{
        AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample,
    };
    use std::num::NonZeroU32;
    fn established_connection() -> TcpConnection {
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

    fn timer_policy_connection() -> (
        Pool<TcpConnection>,
        hammer_infra::pool::Index,
        TcpTimers,
        Instant,
    ) {
        let now = Instant::now();
        let mut connections = Pool::with_capacity(1);
        let index = connections
            .insert(established_connection())
            .expect("insert TCP connection");
        (
            connections,
            index,
            TcpTimers::new(now, Duration::from_millis(10)),
            now,
        )
    }

    fn receive_close_side_for_test(
        connection: &mut TcpConnection,
        packet: &TcpPacket,
    ) -> RuntimeResult<Option<TcpSegment>> {
        let mut timers = TcpTimers::new(Instant::now(), Duration::from_millis(10));
        connection.receive_close_side(PoolIndex::new(0, 0), &mut timers, packet, Instant::now())
    }

    fn start_local_close_for_test(connection: &mut TcpConnection) -> TcpSegment {
        let now = Instant::now();
        let index = PoolIndex::new(0, 0);
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
        connection.on_session_close(index, &mut timers);
        connection
            .on_tcp_ready(index, &mut timers, false, TcpCapabilities::default(), now)
            .expect("prepare close output")
            .expect("fin")
    }

    #[test]
    fn tcp_delayed_ack_expiry_emits_one_ack_without_moving_keepalive_deadline() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        {
            let connection = connections.get_mut(index).expect("connection");
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::KeepAlive,
                    Duration::from_millis(30),
                )
                .expect("arm keepalive");
            assert!(
                !connection
                    .on_clean_in_order_payload(index, &mut timers)
                    .expect("delay first ACK")
            );
        }

        timers.advance(now + Duration::from_millis(10), &mut connections);
        let token = timers
            .take_pending(&mut connections)
            .expect("delayed ACK expiry");
        assert_eq!(token.kind, TcpTimerKind::DelayedAck);
        let ack = connections
            .get_mut(index)
            .expect("connection")
            .on_typed_timer_expiry(
                index,
                &mut timers,
                token.kind,
                TcpCapabilities::default(),
                now + Duration::from_millis(10),
            )
            .expect("dispatch delayed ACK")
            .expect("ACK output");
        assert_eq!(ack.payload_len(), 0);
        assert!(timers.take_pending(&mut connections).is_none());

        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert!(timers.take_pending(&mut connections).is_none());
        timers.advance(now + Duration::from_millis(30), &mut connections);
        assert_eq!(
            timers
                .take_pending(&mut connections)
                .expect("keepalive expiry")
                .kind,
            TcpTimerKind::KeepAlive
        );
    }

    #[test]
    fn tcp_keepalive_activity_updates_only_keepalive_deadline() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        {
            let connection = connections.get_mut(index).expect("connection");
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(20),
                )
                .expect("arm retransmit");
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::KeepAlive,
                    Duration::from_millis(10),
                )
                .expect("arm old keepalive");
            connection
                .observe_activity(index, &mut timers, now)
                .expect("refresh keepalive activity");
        }

        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert_eq!(
            timers
                .take_pending(&mut connections)
                .expect("retransmit expiry")
                .kind,
            TcpTimerKind::Retransmit
        );
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_armed(TcpTimerKind::KeepAlive)
        );
    }

    #[test]
    fn tcp_time_wait_duplicate_fin_rearms_only_time_wait() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        let packet = {
            let connection = connections.get(index).expect("connection");
            TcpPacket {
                local: connection.remote(),
                remote: connection.local().expect("local"),
                sequence: connection.rcv_nxt().into(),
                acknowledgment: None,
                advertised_window: u16::MAX,
                flags: TcpSegmentFlags::FIN,
                capabilities: TcpCapabilities::default(),
                sack_blocks: Vec::new(),
                timestamp: None,
                fast_open_cookie: None,
                ip_ecn: None,
                payload_offset: 0,
                payload_len: 0,
            }
        };
        {
            let connection = connections.get_mut(index).expect("connection");
            connection.state = TcpState::TimeWait;
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::TimeWait,
                    Duration::from_millis(10),
                )
                .expect("arm old time wait");
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(20),
                )
                .expect("arm unrelated retransmit");
            assert!(
                connection
                    .receive_close_side(index, &mut timers, &packet, now)
                    .expect("receive duplicate FIN")
                    .is_some()
            );
        }

        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert_eq!(
            timers
                .take_pending(&mut connections)
                .expect("unrelated retransmit expiry")
                .kind,
            TcpTimerKind::Retransmit
        );
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_armed(TcpTimerKind::TimeWait)
        );
    }

    #[test]
    fn tcp_persist_window_reopen_cancels_pending_probe() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        {
            let connection = connections.get_mut(index).expect("connection");
            connection.snd_wnd = 0;
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Persist,
                    Duration::from_millis(10),
                )
                .expect("arm persist");
        }
        timers.advance(now + Duration::from_millis(10), &mut connections);
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_pending(TcpTimerKind::Persist)
        );

        let packet = {
            let connection = connections.get(index).expect("connection");
            TcpPacket {
                local: connection.remote(),
                remote: connection.local().expect("local"),
                sequence: connection.rcv_nxt().into(),
                acknowledgment: Some(connection.snd_una().into()),
                advertised_window: u16::MAX,
                flags: TcpSegmentFlags::ACK,
                capabilities: TcpCapabilities::default(),
                sack_blocks: Vec::new(),
                timestamp: None,
                fast_open_cookie: None,
                ip_ecn: None,
                payload_offset: 0,
                payload_len: 0,
            }
        };
        connections
            .get_mut(index)
            .expect("connection")
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                packet.acknowledgment.expect("acknowledgment").raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                now + Duration::from_millis(10),
            )
            .expect("process reopened window");

        assert!(timers.take_pending(&mut connections).is_none());
        assert!(
            !connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_active(TcpTimerKind::Persist)
        );
    }

    #[test]
    fn tcp_pacing_expiry_schedules_only_when_pacing_is_active() {
        let now = Instant::now();
        let mut connections = Pool::with_capacity(1);
        let index = connections
            .insert(established_connection_with_pacing_controller())
            .expect("insert TCP connection");
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
        {
            let connection = connections.get_mut(index).expect("connection");
            assert!(
                connection
                    .on_tcp_ready(index, &mut timers, true, TcpCapabilities::default(), now)
                    .expect("schedule pacing")
                    .is_none()
            );
            assert!(!connection.pacing_ready());
        }

        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert!(timers.take_pending(&mut connections).is_none());
        timers.advance(now + Duration::from_millis(30), &mut connections);
        let token = timers
            .take_pending(&mut connections)
            .expect("pacing expiry");
        assert_eq!(token.kind, TcpTimerKind::Pacing);
        connections
            .get_mut(index)
            .expect("connection")
            .on_typed_timer_expiry(
                index,
                &mut timers,
                token.kind,
                TcpCapabilities::default(),
                now + Duration::from_millis(30),
            )
            .expect("dispatch pacing expiry");
        assert!(connections.get(index).expect("connection").pacing_ready());
    }

    #[test]
    fn tcp_unrelated_ack_does_not_move_retransmit_deadline() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        let packet = {
            let connection = connections.get_mut(index).expect("connection");
            connection.recovery.record_sent(
                1,
                TcpSeq::from(connection.snd_una()),
                TcpSeq::from(connection.snd_una()).advance(1),
                1,
                1,
                now,
            );
            connection.snd_nxt = TcpSeq::from(connection.snd_una()).advance(1);
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Retransmit,
                    Duration::from_millis(20),
                )
                .expect("arm retransmit");
            TcpPacket {
                local: connection.remote(),
                remote: connection.local().expect("local"),
                sequence: connection.rcv_nxt().into(),
                acknowledgment: Some(connection.snd_una().into()),
                advertised_window: u16::MAX,
                flags: TcpSegmentFlags::ACK,
                capabilities: TcpCapabilities::default(),
                sack_blocks: Vec::new(),
                timestamp: None,
                fast_open_cookie: None,
                ip_ecn: None,
                payload_offset: 0,
                payload_len: 0,
            }
        };
        connections
            .get_mut(index)
            .expect("connection")
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                packet.acknowledgment.expect("acknowledgment").raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                now + Duration::from_millis(10),
            )
            .expect("process duplicate ACK");

        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert_eq!(
            timers
                .take_pending(&mut connections)
                .expect("original retransmit deadline")
                .kind,
            TcpTimerKind::Retransmit
        );
    }

    #[test]
    fn tcp_ack_progress_restarts_retransmit_deadline_at_current_rto() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        let (packet, original_rto) = {
            let connection = connections.get_mut(index).expect("connection");
            let snd_una = TcpSeq::from(connection.snd_una());
            connection.snd_nxt = snd_una.advance(2);
            connection
                .recovery
                .record_sent(1, snd_una, snd_una.advance(1), 1, 1, now);
            connection
                .recovery
                .record_sent(2, snd_una.advance(1), snd_una.advance(2), 1, 1, now);
            let rto = connection.retransmit_timeout().retransmit_timeout();
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Retransmit,
                    rto,
                )
                .expect("arm retransmit");
            (
                TcpPacket {
                    local: connection.remote(),
                    remote: connection.local().expect("local"),
                    sequence: connection.rcv_nxt().into(),
                    acknowledgment: Some(snd_una.advance(1)),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    timestamp: None,
                    fast_open_cookie: None,
                    ip_ecn: None,
                    payload_offset: 0,
                    payload_len: 0,
                },
                rto,
            )
        };
        let ack_at = now + original_rto / 2;
        timers.advance(ack_at, &mut connections);
        connections
            .get_mut(index)
            .expect("connection")
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                packet.acknowledgment.expect("acknowledgment").raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                ack_at,
            )
            .expect("process advancing ACK");
        let current_rto = connections
            .get(index)
            .expect("connection")
            .retransmit_timeout()
            .retransmit_timeout();

        timers.advance(
            ack_at + current_rto - Duration::from_millis(10),
            &mut connections,
        );
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_armed(TcpTimerKind::Retransmit)
        );
        timers.advance(ack_at + current_rto, &mut connections);
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_pending(TcpTimerKind::Retransmit)
        );
    }

    #[test]
    fn tcp_payload_timer_sync_validation_preserves_existing_timer_transaction() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        let timer_state_before = {
            let connection = connections.get_mut(index).expect("connection");
            let snd_una = TcpSeq::from(connection.snd_una());
            connection.snd_nxt = snd_una.advance(1);
            connection
                .recovery
                .record_sent(1, snd_una, snd_una.advance(1), 1, 1, now);
            connection.keepalive.config.idle = Duration::from_secs(24 * 60 * 60);
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Tlp,
                    Duration::from_millis(20),
                )
                .expect("arm original TLP");
            *connection.timer_state()
        };

        connections
            .get_mut(index)
            .expect("connection")
            .sync_payload_tx_timers(index, &mut timers, now)
            .expect_err("over-horizon keepalive must reject timer plan");

        assert_eq!(
            *connections.get(index).expect("connection").timer_state(),
            timer_state_before
        );
        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_pending(TcpTimerKind::Tlp)
        );
    }

    #[test]
    fn tcp_unrelated_ack_does_not_move_rack_or_tlp_deadlines() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        let packet = {
            let connection = connections.get_mut(index).expect("connection");
            let snd_una = TcpSeq::from(connection.snd_una());
            connection.snd_nxt = snd_una.advance(2_000);
            connection.recovery.record_sent(
                1,
                snd_una,
                snd_una.advance(1_000),
                1_000,
                1_000,
                now - Duration::from_secs(2),
            );
            connection.recovery.record_sent(
                2,
                snd_una.advance(1_000),
                snd_una.advance(2_000),
                1_000,
                1_000,
                now - Duration::from_secs(1),
            );
            connection.recovery.on_sack_blocks(
                TcpRecoveryAck {
                    acknowledgment: snd_una,
                    now,
                    app_limited: false,
                    ecn_ce_count: 0,
                    reordering_window: Duration::from_millis(20),
                },
                &[TcpSackBlock {
                    left_edge: snd_una.advance(1_000),
                    right_edge: snd_una.advance(2_000),
                }],
                &mut connection.congestion,
            );
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Rack,
                    Duration::from_millis(20),
                )
                .expect("arm RACK");
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Tlp,
                    Duration::from_millis(20),
                )
                .expect("arm TLP");
            TcpPacket {
                local: connection.remote(),
                remote: connection.local().expect("local"),
                sequence: connection.rcv_nxt().into(),
                acknowledgment: Some(connection.snd_una().into()),
                advertised_window: u16::MAX / 2,
                flags: TcpSegmentFlags::ACK,
                capabilities: TcpCapabilities::default(),
                sack_blocks: Vec::from([TcpSackBlock {
                    left_edge: snd_una.advance(1_000),
                    right_edge: snd_una.advance(2_000),
                }])
                .into(),
                timestamp: None,
                fast_open_cookie: None,
                ip_ecn: None,
                payload_offset: 0,
                payload_len: 0,
            }
        };
        connections
            .get_mut(index)
            .expect("connection")
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                packet.acknowledgment.expect("acknowledgment").raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                now + Duration::from_millis(10),
            )
            .expect("process duplicate ACK");

        timers.advance(now + Duration::from_millis(20), &mut connections);
        let state = connections.get(index).expect("connection").timer_state();
        assert!(state.is_pending(TcpTimerKind::Rack));
        assert!(state.is_pending(TcpTimerKind::Tlp));
    }

    #[test]
    fn tcp_out_of_range_ack_does_not_change_window_or_recovery_timers() {
        let (mut connections, index, mut timers, now) = timer_policy_connection();
        let (packet, send_window_before) = {
            let connection = connections.get_mut(index).expect("connection");
            let snd_una = TcpSeq::from(connection.snd_una());
            connection.snd_nxt = snd_una.advance(1);
            connection
                .recovery
                .record_sent(1, snd_una, snd_una.advance(1), 1, 1, now);
            timers
                .update(
                    index,
                    connection.timer_state_mut(),
                    TcpTimerKind::Tlp,
                    Duration::from_millis(20),
                )
                .expect("arm TLP");
            (
                TcpPacket {
                    local: connection.remote(),
                    remote: connection.local().expect("local"),
                    sequence: connection.rcv_nxt().into(),
                    acknowledgment: Some(connection.snd_nxt.advance(1)),
                    advertised_window: 0,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    timestamp: None,
                    fast_open_cookie: None,
                    ip_ecn: None,
                    payload_offset: 0,
                    payload_len: 0,
                },
                connection.snd_wnd,
            )
        };
        connections
            .get_mut(index)
            .expect("connection")
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                packet.acknowledgment.expect("acknowledgment").raw(),
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
                now + Duration::from_millis(10),
            )
            .expect("process out-of-range ACK");

        let connection = connections.get(index).expect("connection");
        assert_eq!(connection.snd_wnd, send_window_before);
        assert!(!connection.timer_state().is_active(TcpTimerKind::Persist));
        timers.advance(now + Duration::from_millis(20), &mut connections);
        assert!(
            connections
                .get(index)
                .expect("connection")
                .timer_state()
                .is_pending(TcpTimerKind::Tlp)
        );
    }

    #[test]
    fn tcp_rack_and_tlp_expiry_schedule_exact_recovery_work() {
        let now = Instant::now();
        let mut connections = Pool::with_capacity(2);
        let mut rack_connection = established_connection_with_test_controller();
        rack_connection.snd_una = TcpSeq::from(1_000);
        rack_connection.snd_nxt = TcpSeq::from(3_000);
        rack_connection.recovery.record_sent(
            1,
            TcpSeq::from(1_000),
            TcpSeq::from(2_000),
            1_000,
            1_000,
            now - Duration::from_secs(2),
        );
        rack_connection.recovery.record_sent(
            2,
            TcpSeq::from(2_000),
            TcpSeq::from(3_000),
            1_000,
            1_000,
            now - Duration::from_secs(1),
        );
        rack_connection.recovery.on_sack_blocks(
            TcpRecoveryAck {
                acknowledgment: TcpSeq::from(1_000),
                now: now - Duration::from_millis(100),
                app_limited: false,
                ecn_ce_count: 0,
                reordering_window: Duration::from_millis(10),
            },
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut rack_connection.congestion,
        );
        let rack_index = connections
            .insert(rack_connection)
            .expect("insert RACK connection");

        let mut tlp_connection = established_connection_with_test_controller();
        let tlp_sequence = TcpSeq::from(tlp_connection.snd_nxt());
        tlp_connection.snd_nxt = tlp_sequence.advance(1_000);
        tlp_connection.recovery.record_sent(
            1,
            tlp_sequence,
            tlp_sequence.advance(1_000),
            1_000,
            1_000,
            now,
        );
        let tlp_index = connections
            .insert(tlp_connection)
            .expect("insert TLP connection");
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
        timers
            .update(
                rack_index,
                connections
                    .get_mut(rack_index)
                    .expect("RACK connection")
                    .timer_state_mut(),
                TcpTimerKind::Rack,
                Duration::from_millis(10),
            )
            .expect("arm RACK");
        timers
            .update(
                tlp_index,
                connections
                    .get_mut(tlp_index)
                    .expect("TLP connection")
                    .timer_state_mut(),
                TcpTimerKind::Tlp,
                Duration::from_millis(20),
            )
            .expect("arm TLP");

        timers.advance(now + Duration::from_millis(10), &mut connections);
        let rack = timers.take_pending(&mut connections).expect("RACK expiry");
        assert_eq!(rack.kind, TcpTimerKind::Rack);
        connections
            .get_mut(rack.index)
            .expect("RACK connection")
            .on_typed_timer_expiry(
                rack.index,
                &mut timers,
                rack.kind,
                TcpCapabilities::default(),
                now + Duration::from_millis(10),
            )
            .expect("dispatch RACK");
        assert_eq!(
            connections
                .get(rack_index)
                .expect("RACK connection")
                .tx_payload_sequence(),
            TcpSeq::from(1_000)
        );

        timers.advance(now + Duration::from_millis(20), &mut connections);
        let tlp = timers.take_pending(&mut connections).expect("TLP expiry");
        assert_eq!(tlp.kind, TcpTimerKind::Tlp);
        connections
            .get_mut(tlp.index)
            .expect("TLP connection")
            .on_typed_timer_expiry(
                tlp.index,
                &mut timers,
                tlp.kind,
                TcpCapabilities::default(),
                now + Duration::from_millis(20),
            )
            .expect("dispatch TLP");
        assert_eq!(
            connections
                .get(tlp_index)
                .expect("TLP connection")
                .tx_payload_sequence(),
            tlp_sequence
        );
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

    static TEST_CONGESTION: Algorithm = Algorithm::for_test::<TestCongestionController>("test");

    fn established_connection_with_test_controller() -> TcpConnection {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::established_for_test(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        connection.congestion = State::new(&TEST_CONGESTION, DEFAULT_TCP_MAX_SEGMENT_SIZE);
        connection
    }

    #[test]
    fn tcp_receive_payload_in_order_rx_delivery_advances_rcv_nxt() {
        let mut connection = established_connection();
        let sequence = TcpSeq::from(connection.rcv_nxt());

        connection.receive_payload(
            sequence,
            0,
            RxDelivery::InOrder {
                accepted: NonZeroU32::new(5).expect("accepted bytes"),
                promoted: 0,
                rx_available: 4096,
            },
        );

        assert_eq!(connection.rcv_nxt(), sequence.advance(5).raw());
        assert!(!connection.has_pending_sack_output());
    }

    #[test]
    fn tcp_receive_payload_in_order_rx_delivery_advances_promoted_bytes() {
        let mut connection = established_connection();
        let sequence = TcpSeq::from(connection.rcv_nxt());

        connection.receive_payload(
            sequence,
            0,
            RxDelivery::InOrder {
                accepted: NonZeroU32::new(5).expect("accepted bytes"),
                promoted: 5,
                rx_available: 4096,
            },
        );

        assert_eq!(connection.rcv_nxt(), sequence.advance(10).raw());
        assert!(!connection.has_pending_sack_output());
    }

    #[test]
    fn tcp_receive_payload_ooo_rx_delivery_keeps_rcv_nxt_and_stages_sack() {
        let mut connection = established_connection();
        let base = TcpSeq::from(connection.rcv_nxt());
        let sequence = base.advance(5);

        connection.receive_payload(
            sequence,
            0,
            RxDelivery::OutOfOrder {
                accepted: NonZeroU32::new(5).expect("accepted bytes"),
                newest: OooSpan::new(5, NonZeroU32::new(5).expect("ooo len")),
                rx_available: 4096,
            },
        );

        assert_eq!(connection.rcv_nxt(), base.raw());
        let (blocks, len) = connection
            .sack
            .take_output(true, false, TcpSegmentFlags::ACK)
            .expect("sack output");
        assert_eq!(len, 1);
        assert_eq!(blocks[0].left_edge, base.advance(5));
        assert_eq!(blocks[0].right_edge, base.advance(10));
    }

    #[test]
    fn tcp_receive_payload_not_accepted_rx_delivery_leaves_receive_state_unchanged() {
        let mut connection = established_connection();
        let sequence = TcpSeq::from(connection.rcv_nxt());

        connection.receive_payload(sequence, 0, RxDelivery::NotAccepted { rx_available: 0 });

        assert_eq!(connection.rcv_nxt(), sequence.raw());
        assert!(!connection.has_pending_sack_output());
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

    static PACING_CONGESTION: Algorithm =
        Algorithm::for_test::<PacingTestCongestionController>("pacing-test");

    fn established_connection_with_pacing_controller() -> TcpConnection {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::established_for_test(
            Some(TcpConnectionId::new(11)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        connection.congestion = State::new(&PACING_CONGESTION, DEFAULT_TCP_MAX_SEGMENT_SIZE);
        connection
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
        let mut connection = TcpConnection::new(
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

        let now = Instant::now();
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
        let segment = connection
            .on_tcp_ready(PoolIndex::new(0, 0), &mut timers, false, local_caps, now)
            .expect("prepare syn")
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
        let mut connection = TcpConnection::new(
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
        let index = PoolIndex::new(0, 0);
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
        connection
            .sync_payload_tx_timers(index, &mut timers, now)
            .expect("arm SYN retransmit");

        let segment = connection
            .on_typed_timer_expiry(
                index,
                &mut timers,
                TcpTimerKind::Retransmit,
                local_caps,
                now,
            )
            .expect("dispatch retransmit")
            .expect("retransmit");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        let options = crate::tcp_options_from_bytes(parsed.options());

        assert_eq!(segment.payload_len(), 5);
        assert!(parsed.syn());
        assert_eq!(options.fast_open_cookie.as_deref(), Some(&[1, 2, 3, 4][..]));
    }

    #[test]
    fn tcp_syn_data_ack_releases_payload_without_consuming_syn_control_byte() {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::new(
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
    fn tcp_keepalive_config_defaults_match_prior_constants_and_with_keepalive_overrides() {
        let default_config = TcpKeepaliveConfig::default();
        assert_eq!(default_config.idle, Duration::from_secs(75));
        assert_eq!(default_config.probe_interval, Duration::from_secs(75));
        assert_eq!(default_config.probe_limit, 8);

        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let connection = TcpConnection::new(
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

        let segment = start_local_close_for_test(&mut connection);

        assert_eq!(connection.state(), TcpState::FinWait1);
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed =
            etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");
        assert_eq!(parsed.sequence_number(), initial_snd_nxt);
        assert_eq!(connection.snd_nxt(), initial_snd_nxt.wrapping_add(1));
        assert!(connection.timer_state().is_active(TcpTimerKind::Retransmit));
    }

    #[test]
    fn tcp_finwait1_ack_of_local_fin_advances_to_finwait2() {
        let mut connection = established_connection();
        let _ = start_local_close_for_test(&mut connection);
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

        let control = receive_close_side_for_test(&mut connection, &packet).expect("receive ack");

        assert!(control.is_none());
        assert_eq!(connection.state(), TcpState::FinWait2);
    }

    #[test]
    fn tcp_simultaneous_close_moves_to_closing_until_fin_is_acked() {
        let mut connection = established_connection();
        let _ = start_local_close_for_test(&mut connection);
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

        let ack = receive_close_side_for_test(&mut connection, &fin).expect("receive fin");
        assert!(ack.is_some());
        assert_eq!(connection.state(), TcpState::Closing);

        let final_ack = TcpPacket {
            acknowledgment: Some(connection.snd_nxt().into()),
            flags: TcpSegmentFlags::ACK,
            ..fin
        };
        let _ = receive_close_side_for_test(&mut connection, &final_ack).expect("receive fin ack");

        assert_eq!(connection.state(), TcpState::TimeWait);
        assert!(connection.timer_state().is_active(TcpTimerKind::TimeWait));
    }

    #[test]
    fn tcp_receive_syn_echoes_peer_timestamp_in_syn_ack() {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::new(
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
        let options = crate::tcp_options_from_bytes(parsed.options());
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
        let options = crate::tcp_options_from_bytes(parsed.options());
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
                .checked_sub(connection.paws_idle + Duration::from_secs(1))
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
        let options = crate::tcp_options_from_bytes(parsed.options());
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
        let index = PoolIndex::new(0, 0);
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
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
            sack_blocks: Vec::from([TcpSackBlock {
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
        connection
            .receive_ack_with_timers(
                index,
                &mut timers,
                &sack_packet,
                1_000,
                8_000,
                &sack_packet.sack_blocks,
                now + Duration::from_millis(10),
            )
            .expect("process sack");
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
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let packet = ack_packet(3_000);
        connection
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                3_000,
                8_000,
                &[],
                now + Duration::from_millis(20),
            )
            .expect("process partial ack to 3k");
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

        let packet = ack_packet(4_000);
        connection
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                4_000,
                8_000,
                &[],
                now + Duration::from_millis(30),
            )
            .expect("process ack to 4k");
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
        let index = PoolIndex::new(0, 0);
        let mut timers = TcpTimers::new(now, Duration::from_millis(10));
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
            sack_blocks: Vec::from(sack_blocks).into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        connection
            .receive_ack_with_timers(
                index,
                &mut timers,
                &sack_packet,
                1_000,
                8_000,
                &sack_blocks,
                now + Duration::from_millis(10),
            )
            .expect("process recovery sack");
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
            sack_blocks: Vec::new().into(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };

        let packet = ack_packet(3_000);
        connection
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                3_000,
                8_000,
                &[],
                now + Duration::from_millis(20),
            )
            .expect("process recovery ack to 3k");
        assert_eq!(connection.bytes_in_flight_cached, 3_000);

        assert_eq!(
            connection.tx_payload_budget(1_000, now, TcpCapabilities::default()),
            0
        );

        let packet = ack_packet(4_000);
        connection
            .receive_ack_with_timers(
                index,
                &mut timers,
                &packet,
                4_000,
                8_000,
                &[],
                now + Duration::from_millis(30),
            )
            .expect("process recovery ack to 4k");
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
            ip_ecn: Some(TcpEcnCodepoint::Ce),
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
            ip_ecn: Some(TcpEcnCodepoint::Ce),
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
            ip_ecn: Some(TcpEcnCodepoint::Ce),
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

    use crate::{TcpWorkerState, closing_session_for_test};
    use hammer_infra::segment::Local;
    use hammer_service::session::SessionId;

    fn session(state: &TcpWorkerState<Local>, session_id: SessionId) -> Option<&TcpConnection> {
        let (_, index) = state.sessions.session_transport(session_id)?;
        state.tcp.connections.get(index)
    }

    fn session_mut(
        state: &mut TcpWorkerState<Local>,
        session_id: SessionId,
    ) -> Option<&mut TcpConnection> {
        let (_, index) = state.sessions.session_transport(session_id)?;
        state.tcp.connections.get_mut(index)
    }

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
            sack_blocks: Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        }
    }

    fn drive_fin_ack_to_time_wait(
        driver: &mut TcpWorkerState<Local>,
        session_id: SessionId,
        local: SocketAddr,
        remote: SocketAddr,
    ) {
        let (_, index) = driver
            .sessions
            .session_transport(session_id)
            .expect("session transport");
        {
            let worker = &mut driver.tcp;
            let connection = worker.connections.get_mut(index).expect("connection");
            connection.on_session_close(index, &mut worker.timers);
            let _ = connection
                .on_tcp_ready(
                    index,
                    &mut worker.timers,
                    false,
                    TcpCapabilities::default(),
                    Instant::now(),
                )
                .expect("prepare final output");
        }
        let (rcv_nxt, snd_nxt) = {
            let connection = session(driver, session_id).expect("connection");
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
            sack_blocks: Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        driver
            .tcp
            .receive_close_side_for_test(index, &packet)
            .expect("receive fin ack");
    }

    fn enter_close_wait_for_passive_close_test(
        driver: &mut TcpWorkerState<Local>,
        session_id: SessionId,
        local: SocketAddr,
        remote: SocketAddr,
    ) {
        let (rcv_nxt, snd_nxt) = {
            let connection = session(driver, session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = peer_fin_packet(local, remote, rcv_nxt, snd_nxt);
        let (_, index) = driver
            .sessions
            .session_transport(session_id)
            .expect("session transport");
        let now = Instant::now();
        let worker = &mut driver.tcp;
        let connection = worker.connections.get_mut(index).expect("connection");
        let segment = connection
            .receive_established_with_timers(index, &mut worker.timers, &packet, now)
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
        let (mut driver, session_id, local, remote) = closing_session_for_test();

        let (rcv_nxt, snd_nxt) = {
            let connection = session(&driver, session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = peer_fin_packet(local, remote, rcv_nxt, snd_nxt);
        let (_, index) = driver
            .sessions
            .session_transport(session_id)
            .expect("session transport");
        let now = Instant::now();
        let worker = &mut driver.tcp;
        let connection = worker.connections.get_mut(index).expect("connection");
        let segment = connection
            .receive_established_with_timers(index, &mut worker.timers, &packet, now)
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
        let (mut driver, session_id, local, remote) = closing_session_for_test();

        enter_close_wait_for_passive_close_test(&mut driver, session_id, local, remote);

        let (_, index) = driver
            .sessions
            .session_transport(session_id)
            .expect("session transport");
        let worker = &mut driver.tcp;
        let connection = worker.connections.get_mut(index).expect("connection");
        connection.on_session_close(index, &mut worker.timers);
        assert_eq!(connection.state(), TcpState::LastAck);
    }

    #[test]
    fn tcp_passive_close_peer_ack_final_fin_closes() {
        let (mut driver, session_id, local, remote) = closing_session_for_test();

        enter_close_wait_for_passive_close_test(&mut driver, session_id, local, remote);

        let (_, index) = driver
            .sessions
            .session_transport(session_id)
            .expect("session transport");
        let worker = &mut driver.tcp;
        let connection = worker.connections.get_mut(index).expect("connection");
        connection.on_session_close(index, &mut worker.timers);
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
            sack_blocks: Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        receive_close_side_for_test(connection, &ack_packet).expect("receive final ack");
        assert_eq!(connection.state(), TcpState::Closed);
    }

    #[test]
    fn tcp_fin_path_enters_time_wait_and_retains_session_until_expiry() {
        let (mut driver, session_id, local, remote) = closing_session_for_test();

        drive_fin_ack_to_time_wait(&mut driver, session_id, local, remote);

        let connection = session(&driver, session_id).expect("session");
        assert_eq!(connection.state(), TcpState::TimeWait);
        assert!(connection.timer_state().is_active(TcpTimerKind::TimeWait));
    }

    #[test]
    fn tcp_time_wait_duplicate_fin_reacks_and_rearms_timer() {
        let (mut driver, session_id, local, remote) = closing_session_for_test();

        drive_fin_ack_to_time_wait(&mut driver, session_id, local, remote);

        let (rcv_nxt, snd_nxt) = {
            let connection = session(&driver, session_id).expect("connection");
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
            sack_blocks: Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let segment = receive_close_side_for_test(
            session_mut(&mut driver, session_id).expect("connection"),
            &duplicate_fin,
        )
        .expect("receive duplicate fin")
        .expect("ack segment");
        let mut header = [0u8; 64];
        let header_len = segment.write_header(&mut header).expect("write header");
        let parsed = etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp");

        assert!(parsed.ack());
        let connection = session(&driver, session_id).expect("session");
        assert_eq!(connection.state(), TcpState::TimeWait);
        assert!(connection.timer_state().is_active(TcpTimerKind::TimeWait));
    }

    #[test]
    fn nagle_holds_small_send_while_unacked_data_in_flight() {
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
        assert!(connection.nagle());

        let now = Instant::now();
        connection
            .commit_payload_tx(200, now)
            .expect("seed unacked");
        assert!(connection.bytes_in_flight_cached > 0);

        assert_eq!(
            connection.tx_payload_budget(100, now, TcpCapabilities::default()),
            0
        );
    }

    #[test]
    fn nagle_allows_full_mss_multiples_while_unacked() {
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
        connection.snd_wnd = 16_000;

        let now = Instant::now();
        connection
            .commit_payload_tx(200, now)
            .expect("seed unacked");

        // 2_500 pending → only 2 full MSS while unacked data is in flight.
        assert_eq!(
            connection.tx_payload_budget(2_500, now, TcpCapabilities::default()),
            2_000
        );
    }

    #[test]
    fn nagle_allows_small_send_when_nothing_unacked() {
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
        assert_eq!(connection.bytes_in_flight_cached, 0);

        assert_eq!(
            connection.tx_payload_budget(100, Instant::now(), TcpCapabilities::default()),
            100
        );
    }

    #[test]
    fn nagle_disabled_allows_small_send_with_unacked_data() {
        let mut connection = established_connection_with_test_controller();
        connection.set_nagle_for_test(false);
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

        let now = Instant::now();
        connection
            .commit_payload_tx(200, now)
            .expect("seed unacked");
        assert!(connection.bytes_in_flight_cached > 0);

        assert_eq!(
            connection.tx_payload_budget(100, now, TcpCapabilities::default()),
            100
        );
    }

    #[test]
    fn pacing_still_holds_when_nagle_is_disabled() {
        let mut connection = established_connection_with_pacing_controller();
        connection.set_nagle_for_test(false);
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
        assert!(!connection.pacing_ready());
        assert!(connection.congestion().next_send_delay(100).is_some());

        assert_eq!(
            connection.tx_payload_budget(100, Instant::now(), TcpCapabilities::default()),
            0
        );
    }
}
