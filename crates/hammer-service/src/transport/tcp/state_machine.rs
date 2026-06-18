use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpNegotiatedOptions, TcpSegmentFlags,
    TcpSeq, TcpState,
};

use crate::transport::congestion::CongestionController;

use super::TcpInputNext;
use super::connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TcpConnectionOptionState, TcpConnectionTimerKind,
    TcpRetransmitTimeoutState,
};
use super::output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, tcp_effective_output_payload_len};
use super::recovery::{TcpRecoveryAck, TcpRecoveryState, TcpSentSegment};
use super::segment::{TcpPacket, TcpSegment};

const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Listen {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SynSent {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SynRcvd {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Established {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseWait {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinWait1 {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinWait2 {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closing {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastAck {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWait {
    _private: (),
}

trait TcpInitialState {
    fn initial_state() -> Self;
}

impl TcpInitialState for Closed {
    #[inline]
    fn initial_state() -> Self {
        Self { _private: () }
    }
}

impl TcpInitialState for Listen {
    #[inline]
    fn initial_state() -> Self {
        Self { _private: () }
    }
}

#[derive(Debug, Clone)]
pub struct TcpConnection<S, C>
where
    C: CongestionController,
{
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    close_reason: Option<TcpCloseReason>,
    iss: u32,
    irs: u32,
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
    options: TcpConnectionOptionState,
    retransmit_timeout: TcpRetransmitTimeoutState,
    congestion: C,
    recovery: TcpRecoveryState,
    active_timers: TcpConnectionTimerKind,
    pending_timers: TcpConnectionTimerKind,
    _state: S,
}

#[allow(private_bounds)]
impl<S, C> TcpConnection<S, C>
where
    S: TcpInitialState,
    C: CongestionController,
{
    #[inline]
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            close_reason: None,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: DEFAULT_TCP_WINDOW,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            options: TcpConnectionOptionState::default(),
            retransmit_timeout: TcpRetransmitTimeoutState::new(),
            congestion: C::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            recovery: TcpRecoveryState::new(),
            active_timers: TcpConnectionTimerKind::empty(),
            pending_timers: TcpConnectionTimerKind::empty(),
            _state: S::initial_state(),
        }
    }
}

impl<S, C> TcpConnection<S, C>
where
    C: CongestionController,
{
    #[inline]
    pub const fn connection_id(&self) -> Option<TcpConnectionId> {
        self.connection_id
    }

    #[inline]
    pub const fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub const fn local_port(&self) -> u16 {
        self.local_port
    }

    #[inline]
    pub const fn local(&self) -> Option<SocketAddr> {
        self.local
    }

    #[inline]
    pub const fn remote(&self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub const fn iss(&self) -> u32 {
        self.iss
    }

    #[inline]
    pub const fn irs(&self) -> u32 {
        self.irs
    }

    #[inline]
    pub const fn snd_una(&self) -> u32 {
        self.snd_una
    }

    #[inline]
    pub const fn snd_nxt(&self) -> u32 {
        self.snd_nxt
    }

    #[inline]
    pub const fn snd_wnd(&self) -> u32 {
        self.snd_wnd
    }

    #[inline]
    pub const fn rcv_nxt(&self) -> u32 {
        self.rcv_nxt
    }

    #[inline]
    pub const fn rcv_wnd(&self) -> u32 {
        self.rcv_wnd
    }

    #[inline]
    pub fn close_reason(&self) -> Option<TcpCloseReason> {
        self.close_reason
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
    pub fn output_payload_len(&self) -> usize {
        tcp_effective_output_payload_len(self.negotiated_options().send_max_segment_size)
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

    pub(crate) fn tcp_timer_ticks(&self, timer: TcpConnectionTimerKind) -> Option<u64> {
        if timer == TcpConnectionTimerKind::RACK {
            return self.recovery.rack_timeout_ticks();
        }
        if timer == TcpConnectionTimerKind::TLP {
            return self.recovery.tlp_timeout_ticks();
        }
        None
    }

    #[inline]
    pub fn tcp_timer_is_active(&self, timer: TcpConnectionTimerKind) -> bool {
        self.active_timers.contains(timer)
    }

    #[inline]
    pub fn tcp_timer_is_pending(&self, timer: TcpConnectionTimerKind) -> bool {
        self.pending_timers.contains(timer)
    }

    #[inline]
    pub fn tcp_timer_is_live(&self, timer: TcpConnectionTimerKind) -> bool {
        self.tcp_timer_is_active(timer) || self.tcp_timer_is_pending(timer)
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        let negotiated = self.options.set_local_capabilities(capabilities);
        if let Some(max_segment_size) = negotiated.send_max_segment_size.filter(|max| *max != 0) {
            self.congestion.on_mtu_update(u32::from(max_segment_size));
        }
        negotiated
    }

    #[inline]
    pub fn tcp_timer_set(&mut self, timer: TcpConnectionTimerKind) {
        self.active_timers.insert(timer);
    }

    #[inline]
    pub fn tcp_timer_reset(&mut self, timer: TcpConnectionTimerKind) {
        self.active_timers.remove(timer);
        self.pending_timers.remove(timer);
    }

    #[inline]
    pub fn tcp_timer_expire(&mut self, timer: TcpConnectionTimerKind) {
        self.active_timers.remove(timer);
        self.pending_timers.insert(timer);
    }

    #[inline]
    pub fn tcp_timer_take_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        if !self.pending_timers.contains(timer) {
            return false;
        }
        self.pending_timers.remove(timer);
        true
    }

    #[inline]
    pub fn tcp_timer_dispatch_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        if !self.pending_timers.contains(timer) {
            return false;
        }
        self.pending_timers.remove(timer);
        !self.active_timers.contains(timer)
    }

    #[inline]
    pub fn observe_retransmit_timeout(&mut self) -> Duration {
        self.retransmit_timeout.on_retransmission_timeout()
    }

    #[inline]
    fn accepts_ack(&self, acknowledgment: u32) -> bool {
        !TcpSeq::new(acknowledgment).before(TcpSeq::new(self.snd_una))
            && !TcpSeq::new(acknowledgment).after(TcpSeq::new(self.snd_nxt))
    }

    #[inline]
    fn apply_ack(&mut self, acknowledgment: u32, advertised_window: u16) {
        if self.accepts_ack(acknowledgment) {
            if TcpSeq::new(acknowledgment).after(TcpSeq::new(self.snd_una)) {
                self.ack_sent_data(acknowledgment);
            }
            self.snd_una = acknowledgment;
        }
        self.snd_wnd = self.effective_send_window(u32::from(advertised_window));
    }

    fn ack_sent_data(&mut self, acknowledgment: u32) {
        let now = Instant::now();
        let latest_rtt = self
            .retransmit_timeout
            .smoothed_rtt()
            .unwrap_or(TCP_INITIAL_RETRANSMIT_TIMEOUT);
        let min_rtt = latest_rtt;
        self.recovery.on_ack(
            TcpRecoveryAck {
                acknowledgment,
                now,
                latest_rtt,
                min_rtt,
                app_limited: false,
                ecn_ce: false,
            },
            &mut self.congestion,
        );
        self.retransmit_timeout.observe_ack_sample(latest_rtt);
    }

    #[inline]
    fn receive_in_order(&mut self, sequence: u32, payload_len: usize) -> bool {
        if sequence != self.rcv_nxt {
            return false;
        }
        self.rcv_nxt = TcpSeq::new(self.rcv_nxt).advance(payload_len as u32).raw();
        true
    }

    #[inline]
    fn apply_peer_handshake_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        let negotiated = self.options.apply_peer_handshake_capabilities(capabilities);
        if let Some(max_segment_size) = negotiated.send_max_segment_size.filter(|max| *max != 0) {
            self.congestion.on_mtu_update(u32::from(max_segment_size));
        }
        negotiated
    }

    #[inline]
    pub(crate) fn control_segment(
        &self,
        packet: &TcpPacket,
        flags: TcpSegmentFlags,
        reset_sequence: Option<u32>,
    ) -> TcpSegment {
        let flags_bits = flags.bits();
        TcpSegment::new(
            packet.local,
            packet.remote,
            reset_sequence.unwrap_or_else(|| self.output_sequence(flags_bits)),
            self.output_acknowledgment(flags_bits),
            self.advertised_receive_window(self.rcv_wnd),
            flags,
            self.options.local_capabilities(),
            0,
        )
    }

    #[inline]
    fn output_sequence(&self, flags: u8) -> u32 {
        if flags & super::output::TCP_FLAG_SYN != 0 && self.iss != 0 {
            self.iss
        } else if self.snd_nxt != 0 {
            self.snd_nxt
        } else if self.snd_una != 0 {
            self.snd_una
        } else if self.iss != 0 {
            TcpSeq::new(self.iss).advance(1).raw()
        } else {
            1
        }
    }

    #[inline]
    fn output_acknowledgment(&self, flags: u8) -> u32 {
        if flags & super::output::TCP_FLAG_ACK == 0 {
            return 0;
        }
        if self.rcv_nxt != 0 {
            self.rcv_nxt
        } else if self.irs != 0 {
            TcpSeq::new(self.irs).advance(1).raw()
        } else {
            1
        }
    }
}

macro_rules! impl_state_view {
    ($state:ty, $tcp_state:expr, $next:expr) => {
        impl<C> TcpConnection<$state, C>
        where
            C: CongestionController,
        {
            #[inline]
            pub const fn state(&self) -> TcpState {
                $tcp_state
            }

            #[inline]
            pub const fn next_node(&self) -> TcpInputNext {
                $next
            }
        }
    };
}

impl_state_view!(Closed, TcpState::Closed, TcpInputNext::Drop);
impl_state_view!(Listen, TcpState::Listen, TcpInputNext::Listen);
impl_state_view!(SynSent, TcpState::SynSent, TcpInputNext::SynSent);
impl_state_view!(SynRcvd, TcpState::SynRcvd, TcpInputNext::SynRcvd);
impl_state_view!(
    Established,
    TcpState::Established,
    TcpInputNext::Established
);
impl_state_view!(CloseWait, TcpState::CloseWait, TcpInputNext::CloseWait);
impl_state_view!(FinWait1, TcpState::FinWait1, TcpInputNext::FinWait1);
impl_state_view!(FinWait2, TcpState::FinWait2, TcpInputNext::FinWait2);
impl_state_view!(Closing, TcpState::Closing, TcpInputNext::Closing);
impl_state_view!(LastAck, TcpState::LastAck, TcpInputNext::LastAck);
impl_state_view!(TimeWait, TcpState::TimeWait, TcpInputNext::TimeWait);

macro_rules! impl_state_constructor {
    ($target:ident, $method:ident, $source:ty) => {
        impl<C> TcpConnection<$target, C>
        where
            C: CongestionController,
        {
            #[inline]
            fn $method(current: TcpConnection<$source, C>) -> Self {
                let TcpConnection {
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                    close_reason,
                    iss,
                    irs,
                    snd_una,
                    snd_nxt,
                    snd_wnd,
                    rcv_nxt,
                    rcv_wnd,
                    options,
                    retransmit_timeout,
                    congestion,
                    recovery,
                    active_timers,
                    pending_timers,
                    _state: _,
                } = current;
                Self {
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                    close_reason,
                    iss,
                    irs,
                    snd_una,
                    snd_nxt,
                    snd_wnd,
                    rcv_nxt,
                    rcv_wnd,
                    options,
                    retransmit_timeout,
                    congestion,
                    recovery,
                    active_timers,
                    pending_timers,
                    _state: $target { _private: () },
                }
            }
        }
    };
}

impl_state_constructor!(SynSent, syn_sent_from_closed, Closed);
impl_state_constructor!(SynRcvd, syn_rcvd_from_listen, Listen);
impl_state_constructor!(Closed, closed_from_syn_sent, SynSent);
impl_state_constructor!(SynRcvd, syn_rcvd_from_syn_sent, SynSent);
impl_state_constructor!(Established, established_from_syn_sent, SynSent);
impl_state_constructor!(Closed, closed_from_syn_rcvd, SynRcvd);
impl_state_constructor!(Established, established_from_syn_rcvd, SynRcvd);
impl_state_constructor!(Closed, closed_from_established, Established);
impl_state_constructor!(CloseWait, close_wait_from_established, Established);
impl_state_constructor!(Closed, closed_from_close_wait, CloseWait);
impl_state_constructor!(Closed, closed_from_fin_wait1, FinWait1);
impl_state_constructor!(FinWait2, fin_wait2_from_fin_wait1, FinWait1);
impl_state_constructor!(Closing, closing_from_fin_wait1, FinWait1);
impl_state_constructor!(TimeWait, time_wait_from_fin_wait1, FinWait1);
impl_state_constructor!(Closed, closed_from_fin_wait2, FinWait2);
impl_state_constructor!(TimeWait, time_wait_from_fin_wait2, FinWait2);
impl_state_constructor!(Closed, closed_from_closing, Closing);
impl_state_constructor!(TimeWait, time_wait_from_closing, Closing);
impl_state_constructor!(Closed, closed_from_last_ack, LastAck);
impl_state_constructor!(Closed, closed_from_time_wait, TimeWait);

impl<C> TcpConnection<Closed, C>
where
    C: CongestionController,
{
    #[inline]
    pub fn connect_state(mut self, initial_sequence: u32) -> TcpConnection<SynSent, C> {
        self.close_reason = None;
        self.iss = initial_sequence;
        self.snd_una = initial_sequence;
        self.snd_nxt = TcpSeq::new(initial_sequence).advance(1).raw();
        TcpConnection::syn_sent_from_closed(self)
    }
}

impl<C> TcpConnection<Listen, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn accept_syn(
        mut self,
        packet: &TcpPacket,
    ) -> (TcpConnection<SynRcvd, C>, TcpSegment) {
        self.apply_peer_handshake_capabilities(packet.capabilities);
        if self.iss == 0 {
            self.iss = 1;
        }
        self.irs = packet.sequence;
        self.snd_una = self.iss;
        self.snd_nxt = TcpSeq::new(self.iss).advance(1).raw();
        self.snd_wnd = self.effective_send_window(u32::from(packet.advertised_window));
        self.rcv_nxt = TcpSeq::new(packet.sequence).advance(1).raw();
        let next = TcpConnection::syn_rcvd_from_listen(self);
        let segment =
            next.control_segment(packet, TcpSegmentFlags::SYN | TcpSegmentFlags::ACK, None);
        (next, segment)
    }
}

impl<C> TcpConnection<SynSent, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn unacceptable_ack(&self, acknowledgment: u32) -> bool {
        !TcpSeq::new(acknowledgment).after(TcpSeq::new(self.iss))
            || TcpSeq::new(acknowledgment).after(TcpSeq::new(self.snd_nxt))
    }

    #[inline]
    pub(crate) fn accepts_segment_ack(&self, acknowledgment: u32) -> bool {
        self.accepts_ack(acknowledgment)
    }

    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_syn_sent(self)
    }

    pub(crate) fn accept_syn_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> (TcpConnection<Established, C>, TcpSegment) {
        self.apply_peer_handshake_capabilities(packet.capabilities);
        self.irs = packet.sequence;
        self.snd_wnd = self.effective_send_window(u32::from(packet.advertised_window));
        self.rcv_nxt = TcpSeq::new(packet.sequence).advance(1).raw();
        self.snd_una = acknowledgment;
        let next = TcpConnection::established_from_syn_sent(self);
        let segment = next.control_segment(packet, TcpSegmentFlags::ACK, None);
        (next, segment)
    }

    pub(crate) fn accept_simultaneous_syn(
        mut self,
        packet: &TcpPacket,
    ) -> (TcpConnection<SynRcvd, C>, TcpSegment) {
        self.apply_peer_handshake_capabilities(packet.capabilities);
        self.irs = packet.sequence;
        self.snd_wnd = self.effective_send_window(u32::from(packet.advertised_window));
        self.rcv_nxt = TcpSeq::new(packet.sequence).advance(1).raw();
        let next = TcpConnection::syn_rcvd_from_syn_sent(self);
        let segment =
            next.control_segment(packet, TcpSegmentFlags::SYN | TcpSegmentFlags::ACK, None);
        (next, segment)
    }

    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<TcpSegment> {
        if timer != TcpConnectionTimerKind::RETRANSMIT {
            return None;
        }
        let retransmit = self.tcp_timer_dispatch_pending(timer);
        let first_syn = !self.tcp_timer_is_active(timer)
            && self.snd_una == self.iss
            && self.snd_nxt == TcpSeq::new(self.iss).advance(1).raw();
        if !retransmit && !first_syn {
            return None;
        }
        if retransmit {
            self.observe_retransmit_timeout();
        }
        self.tcp_timer_set(timer);
        let local = self.local?;
        Some(TcpSegment::new(
            local,
            self.remote,
            self.iss,
            self.rcv_nxt,
            self.advertised_receive_window(self.rcv_wnd),
            TcpSegmentFlags::SYN,
            self.options.local_capabilities(),
            0,
        ))
    }
}

impl<C> TcpConnection<SynRcvd, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn accepts_final_ack(&self, acknowledgment: u32) -> bool {
        self.accepts_ack(acknowledgment)
    }

    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_syn_rcvd(self)
    }

    pub(crate) fn accept_final_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> (TcpConnection<Established, C>, TcpSegment) {
        self.apply_ack(acknowledgment, packet.advertised_window);
        let next = TcpConnection::established_from_syn_rcvd(self);
        let segment = next.control_segment(packet, TcpSegmentFlags::ACK, None);
        (next, segment)
    }
}

impl<C> TcpConnection<Established, C>
where
    C: CongestionController,
{
    pub(crate) fn tx_payload_len(&self, pending_len: usize, _: Instant) -> usize {
        if pending_len == 0 {
            return 0;
        }
        let mss = self.output_payload_len();
        let bytes_in_flight = self.recovery.bytes_in_flight();
        let peer_remaining = self.snd_wnd.saturating_sub(bytes_in_flight) as usize;
        let cc_remaining = self
            .congestion
            .congestion_window()
            .saturating_sub(bytes_in_flight) as usize;
        let allowed = pending_len.min(mss).min(peer_remaining).min(cc_remaining);
        if allowed == 0 {
            return 0;
        }
        if self.congestion.next_send_delay(allowed as u32).is_some() {
            return 0;
        }
        allowed
    }

    pub(crate) fn tx_segment(&self, payload_len: usize) -> CoreResult<TcpSegment> {
        let local = self.local().ok_or_else(|| {
            CoreError::internal("established tcp connection missing local address")
        })?;
        Ok(TcpSegment::new(
            local,
            self.remote(),
            self.snd_nxt(),
            self.rcv_nxt(),
            self.advertised_receive_window(self.rcv_wnd),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            self.local_capabilities(),
            payload_len,
        ))
    }

    pub(crate) fn commit_payload_tx(
        &mut self,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<TcpConnectionTimerKind> {
        let payload_len = u32::try_from(payload_len)
            .map_err(|_| CoreError::internal("tcp payload length exceeds u32"))?;
        let sequence = self.snd_nxt;
        let end_sequence = TcpSeq::new(sequence).advance(payload_len).raw();
        let bytes_in_flight = self.recovery.bytes_in_flight();
        let packet_number = self.recovery.next_packet_number();
        self.recovery.record_sent(TcpSentSegment {
            packet_number,
            sequence,
            end_sequence,
            bytes: payload_len,
            sent_at: now,
            retransmitted: false,
            is_probe: false,
        });
        self.congestion
            .on_packet_sent(packet_number, payload_len, bytes_in_flight, now);
        self.snd_nxt = end_sequence;
        let mut timers = TcpConnectionTimerKind::empty();
        if self.recovery.rack_timeout_ticks().is_some() {
            timers.insert(TcpConnectionTimerKind::RACK);
        }
        if self.recovery.tlp_timeout_ticks().is_some() {
            timers.insert(TcpConnectionTimerKind::TLP);
        }
        Ok(timers)
    }

    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_established(self)
    }

    #[inline]
    pub(crate) fn receive_ack(&mut self, acknowledgment: u32, advertised_window: u16) {
        self.apply_ack(acknowledgment, advertised_window);
    }

    #[inline]
    pub(crate) fn accept_payload(&mut self, packet: &TcpPacket) -> Option<usize> {
        if packet.payload_len == 0 {
            return None;
        }
        self.receive_in_order(packet.sequence, packet.payload_len)
            .then_some(packet.payload_len)
    }

    pub(crate) fn accept_fin(
        mut self,
        packet: &TcpPacket,
    ) -> (TcpConnection<CloseWait, C>, TcpSegment) {
        if let Some(acknowledgment) = packet.acknowledgment {
            self.apply_ack(acknowledgment, packet.advertised_window);
        }
        let fin_sequence = packet.sequence.wrapping_add(packet.payload_len as u32);
        self.receive_in_order(fin_sequence, 1);
        self.close_reason = Some(TcpCloseReason::RemoteFin);
        let next = TcpConnection::close_wait_from_established(self);
        let segment = next.control_segment(packet, TcpSegmentFlags::ACK, None);
        (next, segment)
    }
}

impl<C> TcpConnection<CloseWait, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_close_wait(self)
    }

    #[inline]
    pub(crate) fn receive_ack(&mut self, acknowledgment: u32, advertised_window: u16) {
        self.apply_ack(acknowledgment, advertised_window);
    }

    #[inline]
    pub(crate) fn accept_repeated_fin(mut self, packet: &TcpPacket) -> (Self, TcpSegment) {
        self.receive_in_order(packet.sequence, 1);
        let segment = self.control_segment(packet, TcpSegmentFlags::ACK, None);
        (self, segment)
    }
}

impl<C> TcpConnection<FinWait1, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_fin_wait1(self)
    }

    pub(crate) fn accept_fin_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> (TcpConnection<TimeWait, C>, TcpSegment) {
        self.apply_ack(acknowledgment, packet.advertised_window);
        self.receive_in_order(packet.sequence, 1);
        let next = TcpConnection::time_wait_from_fin_wait1(self);
        let segment = next.control_segment(packet, TcpSegmentFlags::ACK, None);
        (next, segment)
    }

    pub(crate) fn accept_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> TcpConnection<FinWait2, C> {
        self.apply_ack(acknowledgment, packet.advertised_window);
        TcpConnection::fin_wait2_from_fin_wait1(self)
    }

    pub(crate) fn accept_fin(
        mut self,
        packet: &TcpPacket,
    ) -> (TcpConnection<Closing, C>, TcpSegment) {
        self.apply_ack(self.snd_una, packet.advertised_window);
        self.receive_in_order(packet.sequence, 1);
        let next = TcpConnection::closing_from_fin_wait1(self);
        let segment = next.control_segment(packet, TcpSegmentFlags::ACK, None);
        (next, segment)
    }
}

impl<C> TcpConnection<FinWait2, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_fin_wait2(self)
    }

    pub(crate) fn accept_fin(
        mut self,
        packet: &TcpPacket,
    ) -> (TcpConnection<TimeWait, C>, TcpSegment) {
        self.apply_ack(self.snd_una, packet.advertised_window);
        self.receive_in_order(packet.sequence, 1);
        let next = TcpConnection::time_wait_from_fin_wait2(self);
        let segment = next.control_segment(packet, TcpSegmentFlags::ACK, None);
        (next, segment)
    }
}

impl<C> TcpConnection<Closing, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_closing(self)
    }

    pub(crate) fn accept_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> TcpConnection<TimeWait, C> {
        self.apply_ack(acknowledgment, packet.advertised_window);
        TcpConnection::time_wait_from_closing(self)
    }
}

impl<C> TcpConnection<LastAck, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_last_ack(self)
    }

    pub(crate) fn accept_ack(
        mut self,
        packet: &TcpPacket,
        acknowledgment: u32,
    ) -> TcpConnection<Closed, C> {
        self.apply_ack(acknowledgment, packet.advertised_window);
        self.close_reason = Some(TcpCloseReason::LocalRequest);
        TcpConnection::closed_from_last_ack(self)
    }
}

impl<C> TcpConnection<TimeWait, C>
where
    C: CongestionController,
{
    #[inline]
    pub(crate) fn close_remote_reset(mut self) -> TcpConnection<Closed, C> {
        self.close_reason = Some(TcpCloseReason::RemoteReset);
        TcpConnection::closed_from_time_wait(self)
    }
}

impl<C> TcpConnection<Established, C>
where
    C: CongestionController,
{
    #[cfg(test)]
    pub(crate) fn established_for_test(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        let connection: TcpConnection<Listen, C> =
            TcpConnection::new(connection_id, owner_worker, local_port, local, remote);
        let packet = TcpPacket {
            local: remote,
            remote: local.unwrap_or(remote),
            flags: TcpSegmentFlags::SYN,
            sequence: 7_000,
            acknowledgment: None,
            advertised_window: u16::MAX,
            payload_offset: 0,
            payload_len: 0,
            capabilities: TcpCapabilities::default(),
        };
        let (syn_rcvd, _) = connection.accept_syn(&packet);
        let final_packet = TcpPacket {
            acknowledgment: Some(syn_rcvd.snd_nxt()),
            flags: TcpSegmentFlags::ACK,
            ..packet
        };
        let (established, _) =
            syn_rcvd.accept_final_ack(&final_packet, final_packet.acknowledgment.unwrap());
        established
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::transport::congestion::{
        AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample,
    };

    use super::*;

    #[derive(Clone, Debug)]
    struct RecordingController {
        congestion_window: u32,
        delay: Option<Duration>,
        sent: hammer_infra::vec::Vec<(PacketNumber, u32, u32, Instant)>,
        acked: hammer_infra::vec::Vec<AckedPacket>,
        end_acks: u32,
        last_bytes_in_flight: u32,
        mtu: u32,
    }

    impl CongestionController for RecordingController {
        fn new(max_datagram_size: u32) -> Self {
            Self {
                congestion_window: 65_535,
                delay: None,
                sent: hammer_infra::vec::Vec::new(),
                acked: hammer_infra::vec::Vec::new(),
                end_acks: 0,
                last_bytes_in_flight: 0,
                mtu: max_datagram_size,
            }
        }

        fn metrics(&self) -> CongestionMetrics {
            CongestionMetrics {
                congestion_window: self.congestion_window,
                pacing_rate_bytes_per_second: None,
                delivered: self.acked.len() as u64,
                max_bandwidth_bytes_per_second: 0,
                min_rtt: None,
            }
        }

        fn max_datagram_size(&self) -> u32 {
            self.mtu
        }

        fn congestion_window(&self) -> u32 {
            self.congestion_window
        }

        fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
            None
        }

        fn delivered(&self) -> u64 {
            self.acked.len() as u64
        }

        fn min_rtt(&self) -> Option<Duration> {
            None
        }

        fn max_bandwidth_bytes_per_second(&self) -> u64 {
            0
        }

        fn on_packet_sent(
            &mut self,
            packet_number: PacketNumber,
            bytes_sent: u32,
            bytes_in_flight: u32,
            now: Instant,
        ) {
            self.sent
                .push((packet_number, bytes_sent, bytes_in_flight, now));
            self.last_bytes_in_flight = bytes_in_flight;
        }

        fn on_ack(&mut self, _: Instant, acked: AckedPacket, _: RttSample, bytes_in_flight: u32) {
            self.last_bytes_in_flight = bytes_in_flight;
            self.acked.push(acked);
        }

        fn on_end_acks(&mut self, _: Instant, bytes_in_flight: u32, _: bool, _: PacketNumber) {
            self.last_bytes_in_flight = bytes_in_flight;
            self.end_acks += 1;
        }

        fn on_loss(&mut self, _: Instant, _: LostPacket, _: bool) {}

        fn on_mtu_update(&mut self, max_datagram_size: u32) {
            self.mtu = max_datagram_size;
        }

        fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
            (pending_bytes != 0).then_some(self.delay).flatten()
        }
    }

    fn established_connection() -> TcpConnection<Established, RecordingController> {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
        TcpConnection::established_for_test(
            Some(TcpConnectionId::new(900)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    #[test]
    fn established_tx_payload_len_uses_window_congestion_and_mss() {
        let now = Instant::now();
        let mut connection = established_connection();
        connection.snd_wnd = 8;
        connection.congestion.congestion_window = 6;

        assert_eq!(connection.tx_payload_len(32, now), 6);

        connection.congestion.delay = Some(Duration::from_millis(1));
        assert_eq!(connection.tx_payload_len(32, now), 0);
    }

    #[test]
    fn established_commit_records_sent_segment_and_congestion_send() {
        let now = Instant::now();
        let mut connection = established_connection();
        let start = connection.snd_nxt();

        let timers = connection.commit_payload_tx(5, now).expect("commit tx");

        assert!(timers.contains(TcpConnectionTimerKind::TLP));
        assert_eq!(connection.snd_nxt(), TcpSeq::new(start).advance(5).raw());
        assert_eq!(connection.recovery().bytes_in_flight(), 5);
        assert_eq!(connection.congestion.sent.as_slice(), &[(1, 5, 0, now)]);
    }

    #[test]
    fn established_ack_cleans_sent_segment_and_updates_congestion() {
        let now = Instant::now();
        let mut connection = established_connection();
        let end = TcpSeq::new(connection.snd_nxt()).advance(5).raw();

        connection.commit_payload_tx(5, now).expect("commit tx");
        connection.receive_ack(end, u16::MAX);

        assert_eq!(connection.snd_una(), end);
        assert_eq!(connection.recovery().bytes_in_flight(), 0);
        assert_eq!(connection.congestion.acked.len(), 1);
        assert_eq!(connection.congestion.end_acks, 1);
    }
}
