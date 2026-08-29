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
use hammer_infra::align::CacheLineAlignMark;
use hammer_runtime::DataWorkerId;
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::runtime::RxDelivery;
use hammer_service::transport::congestion::{CongestionController, CongestionMetrics};
use thiserror::Error;

pub(crate) const TCP_MAX_WINDOW_SCALE: u8 = 14;
const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;
const IPV4_TCP_BASE_HEADER_BYTES: u16 = 40;

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

/// Effective work a timer expiry actually performed: a recovery sample was
/// selected or a probe segment/TX intent was produced. Stale tokens, missing
/// samples, and state mismatches yield no action and must not be counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TcpTimerAction {
    RtoRetransmit,
    RackRetransmit,
    TlpProbe,
    PersistProbe,
    KeepaliveProbe,
}

#[derive(Debug)]
pub(crate) struct TcpTimerOutcome {
    pub(crate) segment: Option<TcpSegment>,
    pub(crate) action: Option<TcpTimerAction>,
}

impl TcpTimerOutcome {
    const fn none() -> Self {
        Self {
            segment: None,
            action: None,
        }
    }

    const fn segment(segment: Option<TcpSegment>) -> Self {
        Self {
            segment,
            action: None,
        }
    }

    const fn acted(action: TcpTimerAction, segment: Option<TcpSegment>) -> Self {
        Self {
            segment,
            action: Some(action),
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
#[repr(C)]
pub struct TcpConnectionCacheline0 {
    cacheline0: CacheLineAlignMark,
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
    zero_receive_window_sent: bool,
    negotiated_options: TcpNegotiatedOptions,
    tx_intent_sequence: Option<TcpSeq>,
    tx_intent_payload_len: u32,
    fast_open_syn_payload_len: u32,
    bytes_in_flight_cached: u32,
}

#[derive(Debug, Clone)]
#[repr(C)]
struct TcpConnectionCacheline1 {
    cacheline1: CacheLineAlignMark,
    session_id: u32,
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    close_reason: Option<TcpCloseReason>,
    local_port: u16,
    fast_open_cookie: Option<TcpFastOpenCookie>,
    persist_attempts: u8,
}

#[derive(Debug, Clone)]
pub struct TcpConnection {
    cacheline0: TcpConnectionCacheline0,
    cacheline1: TcpConnectionCacheline1,
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
        let session_id = 0;
        Self {
            cacheline0: TcpConnectionCacheline0 {
                cacheline0: CacheLineAlignMark,
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
                zero_receive_window_sent: false,
                negotiated_options: TcpNegotiatedOptions::default(),
                tx_intent_sequence: None,
                tx_intent_payload_len: 0,
                fast_open_syn_payload_len: 0,
                bytes_in_flight_cached: 0,
            },
            cacheline1: TcpConnectionCacheline1 {
                cacheline1: CacheLineAlignMark,
                session_id,
                connection_id,
                owner_worker,
                close_reason: None,
                local_port,
                fast_open_cookie: None,
                persist_attempts: 0,
            },
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
    pub(crate) fn session_id(&self) -> u32 {
        self.cacheline1.session_id
    }

    pub(crate) fn attach_session(&mut self, session_id: u32) -> RuntimeResult<()> {
        if self.cacheline1.connection_id.is_some() {
            return Err(TcpConnectionError::InvalidState.into());
        }
        self.cacheline1.session_id = session_id;
        self.cacheline1.connection_id = Some(TcpConnectionId::new(u64::from(session_id)));
        Ok(())
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
    pub(crate) fn zero_receive_window_sent(&self) -> bool {
        self.zero_receive_window_sent
    }

    #[inline]
    fn output_receive_window(&mut self, flags: TcpSegmentFlags) -> u16 {
        let advertised = self.advertised_receive_window(self.rcv_wnd);
        if flags.contains(TcpSegmentFlags::ACK) {
            self.zero_receive_window_sent = advertised == 0;
        }
        advertised
    }

    #[inline]
    pub fn output_payload_len(&self) -> usize {
        tcp_effective_output_payload_len(self.negotiated_options().send_max_segment_size)
    }

    #[inline]
    pub fn send_goal_size(&self) -> usize {
        tcp_send_goal_size(self.negotiated_options().send_max_segment_size)
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
        let mss = path_mtu.saturating_sub(IPV4_TCP_BASE_HEADER_BYTES).max(1);
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
        index: u32,
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
        index: u32,
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
    ) -> RuntimeResult<Option<Duration>> {
        if !self.accepts_ack(acknowledgment) {
            return Ok(None);
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
        Ok(latest_rtt)
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
        index: u32,
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
        let advertised_window = self.output_receive_window(flags);
        TcpSegment::new(
            local,
            remote,
            reset_sequence.unwrap_or_else(|| self.output_sequence(flags)),
            self.output_acknowledgment(flags),
            advertised_window,
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

    pub(crate) fn receive_window_update_segment(
        &mut self,
        rx_available: usize,
    ) -> RuntimeResult<TcpSegment> {
        self.ensure_state(TcpState::Established)?;
        let local = self
            .local()
            .ok_or(TcpConnectionError::MissingLocalAddress)?;
        self.set_rcv_wnd(rx_available);
        Ok(self.control_segment(
            local,
            self.remote(),
            TcpSegmentFlags::ACK,
            None,
            TcpCapabilities::default(),
        ))
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
        index: u32,
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
        index: u32,
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
            let advertised_window = self.output_receive_window(flags);
            return Ok(TcpSegment::new(
                local,
                self.remote(),
                sequence.raw(),
                self.rcv_nxt(),
                advertised_window,
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
        let advertised_window = self.output_receive_window(flags);
        Ok(TcpSegment::new(
            local,
            self.remote(),
            sequence.raw(),
            self.rcv_nxt(),
            advertised_window,
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
        if let Some(intent_sequence) = self.tx_intent_sequence {
            self.clear_tx_intent();
            // An intent below `snd_nxt` retransmits scoreboard-tracked bytes;
            // like VPP's retransmit path it must not re-record or move
            // `snd_nxt`. An intent at `snd_nxt` (persist probe with nothing in
            // flight) is a first transmission and falls through to be recorded
            // like any new data, otherwise the probe byte leaves the stack
            // unaccounted and the ack for it is rejected as a future ack.
            if intent_sequence != self.snd_nxt {
                return Ok(());
            }
        }
        let payload_len =
            u32::try_from(payload_len).map_err(|_| TcpConnectionError::PayloadLengthOverflow)?;
        let sequence = self.snd_nxt;
        let end_sequence = TcpSeq::from(sequence).advance(payload_len).raw();
        let bytes_in_flight = self.bytes_in_flight_cached;
        let packet_number = self.recovery.next_packet_number();
        let _ = self.recovery.record_sent(
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
        index: u32,
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
        index: u32,
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
        self.observe_ack_progress(packet, acknowledgment, sack_blocks)?;
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
        index: u32,
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
        Ok(None)
    }

    pub(super) fn process_fin_after_payload(
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
        index: u32,
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
    pub(crate) fn receive_payload(
        &mut self,
        sequence: TcpSeq,
        trim: u32,
        delivery: RxDelivery,
    ) -> RuntimeResult<()> {
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
                return Ok(());
            }
        }
        self.sack.update_range(
            self.negotiated_options().sack,
            self.rcv_nxt,
            self.rcv_nxt,
            self.rcv_nxt,
        );
        Ok(())
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
                let flags = self.output_flags(TcpSegmentFlags::ACK);
                let sequence = self.output_sequence(flags);
                let acknowledgment = self.rcv_nxt();
                let advertised_window = self.output_receive_window(flags);
                let capabilities = self.output_capabilities();
                let sack_blocks = self.sack.take_output(
                    self.negotiated_options().sack,
                    self.negotiated_options().timestamps,
                    flags,
                );
                let timestamp = self.next_local_timestamp(self.negotiated_options().timestamps);
                Some(TcpSegment::new(
                    local,
                    self.remote(),
                    sequence,
                    acknowledgment,
                    advertised_window,
                    flags,
                    capabilities,
                    sack_blocks.as_ref().map(|(blocks, len)| &blocks[..*len]),
                    timestamp,
                    None,
                    None,
                    0,
                ))
            }
            TcpState::FinWait1 | TcpState::LastAck if self.snd_una == self.snd_nxt => {
                self.snd_nxt = self.snd_nxt.advance(1);
                let flags = TcpSegmentFlags::ACK | TcpSegmentFlags::FIN;
                let advertised_window = self.output_receive_window(flags);
                Some(TcpSegment::new(
                    local,
                    self.remote(),
                    self.snd_una(),
                    self.rcv_nxt(),
                    advertised_window,
                    flags,
                    self.output_capabilities(),
                    None,
                    self.next_local_timestamp(self.negotiated_options().timestamps),
                    None,
                    None,
                    0,
                ))
            }
            TcpState::FinWait1 | TcpState::LastAck => {
                let flags = TcpSegmentFlags::ACK | TcpSegmentFlags::FIN;
                let advertised_window = self.output_receive_window(flags);
                Some(TcpSegment::new(
                    local,
                    self.remote(),
                    self.snd_una(),
                    self.rcv_nxt(),
                    advertised_window,
                    flags,
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

    pub(super) fn on_tcp_ready(
        &mut self,
        index: u32,
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
    pub(super) fn on_session_close(&mut self, index: u32, timers: &mut TcpTimers) {
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
        index: u32,
        timers: &mut TcpTimers,
        kind: TcpTimerKind,
        local_capabilities: TcpCapabilities,
        now: Instant,
    ) -> RuntimeResult<TcpTimerOutcome> {
        if !self.timer_dispatch_pending(kind) {
            return Ok(TcpTimerOutcome::none());
        }
        let outcome = match kind {
            TcpTimerKind::Retransmit => self.on_retransmit_timer_expiry(local_capabilities)?,
            TcpTimerKind::Rack => self.on_rack_timer_expiry()?,
            TcpTimerKind::Tlp => self.on_tlp_timer_expiry()?,
            TcpTimerKind::DelayedAck => {
                TcpTimerOutcome::segment(self.on_delayed_ack_timer_expiry())
            }
            TcpTimerKind::Persist => self.on_persist_timer_expiry(),
            TcpTimerKind::KeepAlive => self.on_keepalive_timer_expiry(),
            TcpTimerKind::TimeWait => TcpTimerOutcome::segment(self.on_time_wait_timer_expiry()),
            TcpTimerKind::Pacing => TcpTimerOutcome::segment(self.on_pacing_timer_expiry()),
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
        Ok(outcome)
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
    ) -> RuntimeResult<TcpTimerOutcome> {
        if self.state == TcpState::SynSent {
            self.observe_retransmit_timeout();
            let Some(local) = self.local else {
                return Ok(TcpTimerOutcome::none());
            };
            let capabilities = normalize_tcp_capabilities(local_capabilities);
            let flags = if capabilities.ecn {
                TcpSegmentFlags::SYN | TcpSegmentFlags::ECE | TcpSegmentFlags::CWR
            } else {
                TcpSegmentFlags::SYN
            };
            let advertised_window = self.output_receive_window(flags);
            return Ok(TcpTimerOutcome::acted(
                TcpTimerAction::RtoRetransmit,
                Some(TcpSegment::new(
                    local,
                    self.remote,
                    self.iss.raw(),
                    self.rcv_nxt.raw(),
                    advertised_window,
                    flags,
                    capabilities,
                    None,
                    self.next_local_timestamp(capabilities.timestamps),
                    self.fast_open_cookie(),
                    None,
                    self.fast_open_syn_payload_len as usize,
                )),
            ));
        }
        match self.state {
            TcpState::Established => {
                let now = Instant::now();
                let Some(sample) = self.recovery.on_retransmission_timeout(
                    now,
                    self.snd_nxt,
                    &mut self.congestion,
                ) else {
                    return Ok(TcpTimerOutcome::none());
                };
                self.observe_retransmit_timeout();
                self.tx_intent_sequence = Some(sample.sequence);
                self.tx_intent_payload_len = sample.payload_len;
                self.pacing_ready = true;
                Ok(TcpTimerOutcome::acted(TcpTimerAction::RtoRetransmit, None))
            }
            TcpState::FinWait1 | TcpState::Closing | TcpState::LastAck => {
                self.observe_retransmit_timeout();
                let Some(local) = self.local else {
                    return Ok(TcpTimerOutcome::none());
                };
                let flags = TcpSegmentFlags::ACK | TcpSegmentFlags::FIN;
                let advertised_window = self.output_receive_window(flags);
                Ok(TcpTimerOutcome::acted(
                    TcpTimerAction::RtoRetransmit,
                    Some(TcpSegment::new(
                        local,
                        self.remote,
                        self.snd_una.raw(),
                        self.rcv_nxt.raw(),
                        advertised_window,
                        flags,
                        self.output_capabilities(),
                        None,
                        self.next_local_timestamp(self.negotiated_options().timestamps),
                        None,
                        None,
                        0,
                    )),
                ))
            }
            _ => Ok(TcpTimerOutcome::none()),
        }
    }

    fn on_rack_timer_expiry(&mut self) -> RuntimeResult<TcpTimerOutcome> {
        if self.ensure_state(TcpState::Established).is_err() {
            return Ok(TcpTimerOutcome::none());
        }
        let now = Instant::now();
        self.recovery
            .on_rack_timeout(now, self.snd_nxt, &mut self.congestion);
        let Some(sample) = self.recovery.take_rack_retransmit() else {
            return Ok(TcpTimerOutcome::none());
        };
        self.recovery.on_retransmit_sent(sample.bytes);
        self.tx_intent_sequence = Some(sample.sequence);
        self.tx_intent_payload_len = sample.payload_len;
        self.pacing_ready = true;
        Ok(TcpTimerOutcome::acted(TcpTimerAction::RackRetransmit, None))
    }

    fn on_tlp_timer_expiry(&mut self) -> RuntimeResult<TcpTimerOutcome> {
        if self.ensure_state(TcpState::Established).is_err() {
            return Ok(TcpTimerOutcome::none());
        }
        let Some(sample) = self.recovery.take_tlp_probe() else {
            return Ok(TcpTimerOutcome::none());
        };
        if self.recovery.in_recovery() {
            self.recovery.on_retransmit_sent(sample.bytes);
        }
        self.tx_intent_sequence = Some(sample.sequence);
        self.tx_intent_payload_len = sample.payload_len;
        self.pacing_ready = true;
        Ok(TcpTimerOutcome::acted(TcpTimerAction::TlpProbe, None))
    }

    fn on_delayed_ack_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.ensure_state(TcpState::Established).ok()?;
        let local = self.local?;
        let flags = self.output_flags(TcpSegmentFlags::ACK);
        let advertised_window = self.output_receive_window(flags);
        Some(TcpSegment::new(
            local,
            self.remote(),
            self.snd_nxt(),
            self.rcv_nxt(),
            advertised_window,
            flags,
            self.output_capabilities(),
            None,
            self.next_local_timestamp(self.negotiated_options().timestamps),
            None,
            None,
            0,
        ))
    }

    fn on_persist_timer_expiry(&mut self) -> TcpTimerOutcome {
        if self.ensure_state(TcpState::Established).is_err() {
            return TcpTimerOutcome::none();
        }
        if self.snd_wnd != 0 {
            self.cacheline1.persist_attempts = 0;
            return TcpTimerOutcome::none();
        }
        self.cacheline1.persist_attempts = self.cacheline1.persist_attempts.saturating_add(1);
        if self.tx_intent_sequence.is_some() {
            return TcpTimerOutcome::none();
        }
        self.tx_intent_sequence = Some(self.snd_una);
        self.tx_intent_payload_len = 1;
        self.pacing_ready = true;
        TcpTimerOutcome::acted(TcpTimerAction::PersistProbe, None)
    }

    fn on_time_wait_timer_expiry(&mut self) -> Option<TcpSegment> {
        self.cacheline1.close_reason = Some(TcpCloseReason::LocalRequest);
        self.state = TcpState::Closed;
        None
    }

    fn on_keepalive_timer_expiry(&mut self) -> TcpTimerOutcome {
        if self.ensure_state(TcpState::Established).is_err() {
            return TcpTimerOutcome::none();
        }
        if self.keepalive.probes_sent >= self.keepalive.config.probe_limit {
            self.cacheline1.close_reason = Some(TcpCloseReason::KeepAliveTimeout);
            self.state = TcpState::Closed;
            return TcpTimerOutcome::none();
        }
        self.keepalive.probes_sent = self.keepalive.probes_sent.saturating_add(1);
        let Some(local) = self.local else {
            return TcpTimerOutcome::none();
        };
        let flags = self.output_flags(TcpSegmentFlags::ACK);
        let advertised_window = self.output_receive_window(flags);
        TcpTimerOutcome::acted(
            TcpTimerAction::KeepaliveProbe,
            Some(TcpSegment::new(
                local,
                self.remote(),
                self.snd_nxt().wrapping_sub(1),
                self.rcv_nxt(),
                advertised_window,
                flags,
                self.output_capabilities(),
                None,
                self.next_local_timestamp(self.negotiated_options().timestamps),
                None,
                None,
                0,
            )),
        )
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
        // VPP releases pacer-gated bytes through the normal send path
        // (`tcp_available_cc_snd_space` + pacer), never through the
        // retransmit machinery. Raising `pacing_ready` lets the next
        // `tx_payload_budget` call pass the pacing gate and send new data
        // from `snd_nxt` with full loss-recovery accounting; routing it
        // through a tx intent would transmit bytes the scoreboard never
        // records, freezing `snd_una` on the ack for them.
        self.pacing_ready = true;
        None
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
