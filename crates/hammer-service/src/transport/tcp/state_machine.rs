use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpNegotiatedOptions, TcpSegmentFlags, TcpSegmentHeader,
    TcpSeq, TcpState,
};

use super::TcpInputNext;
use super::congestion::{TcpCongestionAckSample, TcpCongestionState};
use super::connection::{
    TcpConnection, TcpConnectionOptionState, TcpConnectionTimerKind, TcpRetransmitTimeoutState,
};
use super::output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, tcp_effective_output_payload_len};
use super::segment::TcpPacket;

const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

#[derive(Debug, Clone)]
struct TcpProtocolState {
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
    congestion: TcpCongestionState,
    active_timers: u8,
    pending_timers: u8,
    next_output_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct TcpStateMachine<S> {
    protocol: TcpProtocolState,
    state: S,
}

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
pub struct LastAck {
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
pub struct TimeWait {
    _private: (),
}

pub trait TcpPhase: Sized {
    const TCP_STATE: TcpState;
    const NEXT_NODE: TcpInputNext;
}

pub(super) trait TcpConstructPhase: TcpPhase {
    fn new() -> Self;
}

pub trait TcpRootPhase: TcpPhase {
    fn new_root() -> Self;
}

macro_rules! impl_phase {
    ($state_type:ty, $tcp_state:expr, $next_node:expr) => {
        impl TcpPhase for $state_type {
            const TCP_STATE: TcpState = $tcp_state;
            const NEXT_NODE: TcpInputNext = $next_node;
        }

        impl TcpConstructPhase for $state_type {
            #[inline]
            fn new() -> Self {
                Self { _private: () }
            }
        }
    };
}

impl_phase!(Closed, TcpState::Closed, TcpInputNext::Drop);
impl_phase!(Listen, TcpState::Listen, TcpInputNext::Listen);
impl_phase!(SynSent, TcpState::SynSent, TcpInputNext::SynSent);
impl_phase!(SynRcvd, TcpState::SynRcvd, TcpInputNext::RcvProcess);
impl_phase!(
    Established,
    TcpState::Established,
    TcpInputNext::Established
);
impl_phase!(CloseWait, TcpState::CloseWait, TcpInputNext::RcvProcess);
impl_phase!(LastAck, TcpState::LastAck, TcpInputNext::RcvProcess);
impl_phase!(FinWait1, TcpState::FinWait1, TcpInputNext::RcvProcess);
impl_phase!(FinWait2, TcpState::FinWait2, TcpInputNext::RcvProcess);
impl_phase!(Closing, TcpState::Closing, TcpInputNext::RcvProcess);
impl_phase!(TimeWait, TcpState::TimeWait, TcpInputNext::RcvProcess);

impl TcpRootPhase for Closed {
    #[inline]
    fn new_root() -> Self {
        <Self as TcpConstructPhase>::new()
    }
}

impl TcpRootPhase for Listen {
    #[inline]
    fn new_root() -> Self {
        <Self as TcpConstructPhase>::new()
    }
}

impl TcpProtocolState {
    #[inline]
    fn new() -> Self {
        Self {
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
            congestion: TcpCongestionState::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            active_timers: 0,
            pending_timers: 0,
            next_output_at: None,
        }
    }

    #[inline]
    fn transition<S: TcpConstructPhase>(self) -> TcpStateMachine<S> {
        TcpStateMachine {
            protocol: self,
            state: S::new(),
        }
    }

    #[inline]
    fn accepts_ack(&self, acknowledgment: u32) -> bool {
        !TcpSeq::new(acknowledgment).before(TcpSeq::new(self.snd_una))
            && !TcpSeq::new(acknowledgment).after(TcpSeq::new(self.snd_nxt))
    }

    #[inline]
    fn apply_ack(&mut self, acknowledgment: u32, advertised_window: u16) {
        if self.accepts_ack(acknowledgment) {
            self.snd_una = acknowledgment;
        }
        self.snd_wnd = self.effective_send_window(u32::from(advertised_window));
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
            self.congestion = TcpCongestionState::new(u32::from(max_segment_size));
        }
        negotiated
    }

    #[inline]
    fn configure_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        let negotiated = self.options.set_local_capabilities(capabilities);
        if let Some(max_segment_size) = negotiated.send_max_segment_size.filter(|max| *max != 0) {
            self.congestion = TcpCongestionState::new(u32::from(max_segment_size));
        }
        negotiated
    }

    #[inline]
    fn effective_send_window(&self, advertised_window: u32) -> u32 {
        self.options.effective_send_window(advertised_window)
    }

    #[inline]
    fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        self.options.advertised_receive_window(receive_window)
    }

    #[inline]
    fn control(
        &self,
        packet: &TcpPacket,
        flags: TcpSegmentFlags,
        reset_sequence: Option<u32>,
    ) -> TcpSegmentHeader {
        let flags_bits = flags.bits();
        TcpSegmentHeader {
            source_port: packet.local.port(),
            destination_port: packet.remote.port(),
            sequence_number: reset_sequence.unwrap_or_else(|| self.output_sequence(flags_bits)),
            acknowledgment_number: self.output_acknowledgment(flags_bits),
            flags,
            advertised_window: self.advertised_receive_window(self.rcv_wnd),
            capabilities: self.options.local_capabilities(),
        }
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

impl<S: TcpRootPhase> TcpStateMachine<S> {
    #[inline]
    pub(super) fn new() -> Self {
        TcpStateMachine {
            protocol: TcpProtocolState::new(),
            state: S::new_root(),
        }
    }
}

impl TcpStateMachine<Closed> {
    #[inline]
    pub(super) fn connect(mut self, iss: u32) -> TcpStateMachine<SynSent> {
        self.protocol.close_reason = None;
        self.protocol.iss = iss;
        self.protocol.snd_una = iss;
        self.protocol.snd_nxt = TcpSeq::new(iss).advance(1).raw();
        self.protocol.transition()
    }
}

impl TcpStateMachine<Listen> {
    #[inline]
    pub(super) fn on_syn(
        mut self,
        sequence: u32,
        advertised_window: u16,
        capabilities: TcpCapabilities,
    ) -> TcpStateMachine<SynRcvd> {
        self.protocol
            .apply_peer_handshake_capabilities(capabilities);
        if self.protocol.iss == 0 {
            self.protocol.iss = 1;
        }
        self.protocol.irs = sequence;
        self.protocol.snd_una = self.protocol.iss;
        self.protocol.snd_nxt = TcpSeq::new(self.protocol.iss).advance(1).raw();
        self.protocol.snd_wnd = self
            .protocol
            .effective_send_window(u32::from(advertised_window));
        self.protocol.rcv_nxt = TcpSeq::new(sequence).advance(1).raw();
        self.protocol.transition()
    }
}

impl TcpStateMachine<SynRcvd> {
    #[inline]
    pub(super) fn on_final_ack(
        mut self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Established> {
        self.protocol.apply_ack(acknowledgment, advertised_window);
        self.protocol.transition()
    }
}

impl TcpStateMachine<Established> {
    #[inline]
    pub(super) fn on_fin(
        mut self,
        sequence: u32,
        payload_len: usize,
        acknowledgment: Option<u32>,
        advertised_window: u16,
    ) -> TcpStateMachine<CloseWait> {
        if let Some(acknowledgment) = acknowledgment {
            self.protocol.apply_ack(acknowledgment, advertised_window);
        }
        let fin_sequence = sequence.wrapping_add(payload_len as u32);
        self.protocol.receive_in_order(fin_sequence, 1);
        self.protocol.close_reason = Some(TcpCloseReason::RemoteFin);
        self.protocol.transition()
    }

    #[inline]
    pub(super) fn close(mut self) -> TcpStateMachine<FinWait1> {
        self.protocol.close_reason = Some(TcpCloseReason::LocalRequest);
        self.protocol.snd_nxt = TcpSeq::new(self.protocol.snd_nxt).advance(1).raw();
        self.protocol.transition()
    }
}

impl TcpStateMachine<CloseWait> {
    #[inline]
    pub(super) fn close(mut self) -> TcpStateMachine<LastAck> {
        self.protocol.close_reason = Some(TcpCloseReason::LocalRequest);
        self.protocol.snd_nxt = TcpSeq::new(self.protocol.snd_nxt).advance(1).raw();
        self.protocol.transition()
    }
}

impl TcpStateMachine<FinWait1> {
    #[inline]
    pub(super) fn on_ack(
        mut self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<FinWait2> {
        self.protocol.apply_ack(acknowledgment, advertised_window);
        self.protocol.transition()
    }

    #[inline]
    pub(super) fn on_fin(
        mut self,
        sequence: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Closing> {
        self.protocol
            .apply_ack(self.protocol.snd_una, advertised_window);
        self.protocol.receive_in_order(sequence, 1);
        self.protocol.transition()
    }

    #[inline]
    pub(super) fn on_fin_ack(
        mut self,
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<TimeWait> {
        self.protocol.apply_ack(acknowledgment, advertised_window);
        self.protocol.receive_in_order(sequence, 1);
        self.protocol.transition()
    }
}

impl TcpStateMachine<FinWait2> {
    #[inline]
    pub(super) fn on_fin(
        mut self,
        sequence: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<TimeWait> {
        self.protocol
            .apply_ack(self.protocol.snd_una, advertised_window);
        self.protocol.receive_in_order(sequence, 1);
        self.protocol.transition()
    }
}

impl TcpStateMachine<Closing> {
    #[inline]
    pub(super) fn on_ack(
        mut self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<TimeWait> {
        self.protocol.apply_ack(acknowledgment, advertised_window);
        self.protocol.transition()
    }
}

impl TcpStateMachine<LastAck> {
    #[inline]
    pub(super) fn on_ack(
        mut self,
        acknowledgment: u32,
        advertised_window: u16,
    ) -> TcpStateMachine<Closed> {
        self.protocol.apply_ack(acknowledgment, advertised_window);
        self.protocol.close_reason = Some(TcpCloseReason::LocalRequest);
        self.protocol.transition()
    }
}

impl TcpStateMachine<TimeWait> {
    #[inline]
    pub(super) fn on_timeout(self) -> TcpStateMachine<Closed> {
        self.protocol.transition()
    }
}

impl<S: TcpPhase> TcpStateMachine<S> {
    #[inline]
    pub const fn tcp_state(&self) -> TcpState {
        S::TCP_STATE
    }

    #[inline]
    pub const fn next_node(&self) -> TcpInputNext {
        S::NEXT_NODE
    }

    #[inline]
    pub const fn iss(&self) -> u32 {
        self.protocol.iss
    }

    #[inline]
    pub const fn irs(&self) -> u32 {
        self.protocol.irs
    }

    #[inline]
    pub const fn snd_una(&self) -> u32 {
        self.protocol.snd_una
    }

    #[inline]
    pub const fn snd_nxt(&self) -> u32 {
        self.protocol.snd_nxt
    }

    #[inline]
    pub const fn snd_wnd(&self) -> u32 {
        self.protocol.snd_wnd
    }

    #[inline]
    pub const fn rcv_nxt(&self) -> u32 {
        self.protocol.rcv_nxt
    }

    #[inline]
    pub const fn rcv_wnd(&self) -> u32 {
        self.protocol.rcv_wnd
    }

    #[inline]
    pub fn close_reason(&self) -> Option<TcpCloseReason> {
        self.protocol.close_reason
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        self.protocol.options.local_capabilities()
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        self.protocol.options.remote_capabilities()
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.protocol.options.negotiated_options()
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        self.protocol.options.effective_send_window_scale()
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        self.protocol.options.effective_receive_window_scale()
    }

    #[inline]
    pub fn effective_send_window(&self, advertised_window: u32) -> u32 {
        self.protocol.effective_send_window(advertised_window)
    }

    #[inline]
    pub fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        self.protocol.advertised_receive_window(receive_window)
    }

    #[inline]
    pub fn output_payload_len(&self) -> usize {
        tcp_effective_output_payload_len(self.negotiated_options().send_max_segment_size)
    }

    #[inline]
    pub fn congestion(&self) -> &TcpCongestionState {
        &self.protocol.congestion
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> &TcpRetransmitTimeoutState {
        &self.protocol.retransmit_timeout
    }

    #[inline]
    pub const fn tcp_timer_is_active(&self, timer: TcpConnectionTimerKind) -> bool {
        self.protocol.active_timers & timer.bit() != 0
    }

    #[inline]
    pub const fn tcp_timer_is_pending(&self, timer: TcpConnectionTimerKind) -> bool {
        self.protocol.pending_timers & timer.bit() != 0
    }

    #[inline]
    pub const fn tcp_timer_is_live(&self, timer: TcpConnectionTimerKind) -> bool {
        self.tcp_timer_is_active(timer) || self.tcp_timer_is_pending(timer)
    }

    #[inline]
    pub const fn next_output_at(&self) -> Option<Instant> {
        self.protocol.next_output_at
    }

    #[inline]
    pub(super) fn configure_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.protocol.configure_local_capabilities(capabilities)
    }

    #[inline]
    pub(super) fn observe_congestion_ack(&mut self, sample: TcpCongestionAckSample) {
        self.protocol.congestion.on_ack(sample);
    }

    #[inline]
    pub(super) fn observe_congestion_send(
        &mut self,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    ) {
        self.protocol
            .congestion
            .on_packet_sent(bytes_sent, bytes_in_flight);
        self.protocol.next_output_at = self
            .protocol
            .congestion
            .next_send_delay(bytes_sent)
            .map(|delay| now + delay);
    }

    #[inline]
    pub(super) fn observe_congestion_loss(&mut self, bytes_lost: u32) {
        self.protocol.congestion.on_loss(bytes_lost);
        self.protocol.next_output_at = None;
    }

    #[inline]
    pub(super) fn schedule_next_output(&mut self, deadline: Option<Instant>) {
        self.protocol.next_output_at = deadline;
    }

    #[inline]
    pub(super) fn arm_tcp_timer(&mut self, timer: TcpConnectionTimerKind) {
        self.protocol.active_timers |= timer.bit();
    }

    #[inline]
    pub(super) fn reset_tcp_timer(&mut self, timer: TcpConnectionTimerKind) {
        let bit = timer.bit();
        self.protocol.active_timers &= !bit;
        self.protocol.pending_timers &= !bit;
    }

    #[inline]
    pub(super) fn expire_tcp_timer(&mut self, timer: TcpConnectionTimerKind) {
        let bit = timer.bit();
        self.protocol.active_timers &= !bit;
        self.protocol.pending_timers |= bit;
    }

    #[inline]
    pub(super) fn take_pending_tcp_timer(&mut self, timer: TcpConnectionTimerKind) -> bool {
        let bit = timer.bit();
        if self.protocol.pending_timers & bit == 0 {
            return false;
        }
        self.protocol.pending_timers &= !bit;
        true
    }

    #[inline]
    pub(super) fn dispatch_pending_tcp_timer(&mut self, timer: TcpConnectionTimerKind) -> bool {
        let bit = timer.bit();
        if self.protocol.pending_timers & bit == 0 {
            return false;
        }
        self.protocol.pending_timers &= !bit;
        self.protocol.active_timers & bit == 0
    }

    #[inline]
    pub(super) fn observe_retransmit_timeout(&mut self) -> Duration {
        self.protocol.retransmit_timeout.on_retransmission_timeout()
    }

    #[inline]
    pub(super) fn syn_header(&self, source_port: u16, destination_port: u16) -> TcpSegmentHeader {
        TcpSegmentHeader {
            source_port,
            destination_port,
            sequence_number: self.protocol.iss,
            acknowledgment_number: self.protocol.rcv_nxt,
            flags: TcpSegmentFlags::SYN,
            advertised_window: self
                .protocol
                .advertised_receive_window(self.protocol.rcv_wnd),
            capabilities: self.protocol.options.local_capabilities(),
        }
    }

    #[inline]
    fn into_next<T: TcpConstructPhase>(self) -> TcpStateMachine<T> {
        self.protocol.transition()
    }
}

impl TcpConnection<Listen> {
    pub(crate) fn receive_syn<Out>(self, packet: &TcpPacket) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<Listen>> + From<TcpConnection<SynRcvd>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        if !packet.flags.contains(TcpSegmentFlags::SYN)
            || packet
                .flags
                .intersects(TcpSegmentFlags::ACK | TcpSegmentFlags::RST)
        {
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        let next = machine.on_syn(
            packet.sequence,
            packet.advertised_window,
            packet.capabilities,
        );
        let control =
            next.protocol
                .control(packet, TcpSegmentFlags::SYN | TcpSegmentFlags::ACK, None);
        (
            connection_from_machine(next, connection_id, owner_worker, local_port, local, remote),
            Some(control),
        )
    }
}

impl TcpConnection<SynSent> {
    pub(crate) fn receive_open_reply<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<SynSent>>
            + From<TcpConnection<SynRcvd>>
            + From<TcpConnection<Established>>
            + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && (!TcpSeq::new(acknowledgment).after(TcpSeq::new(machine.protocol.iss))
                || TcpSeq::new(acknowledgment).after(TcpSeq::new(machine.protocol.snd_nxt)))
        {
            if !packet.flags.contains(TcpSegmentFlags::RST) {
                let control =
                    machine
                        .protocol
                        .control(packet, TcpSegmentFlags::RST, Some(acknowledgment));
                return (
                    connection_from_machine(
                        machine,
                        connection_id,
                        owner_worker,
                        local_port,
                        local,
                        remote,
                    ),
                    Some(control),
                );
            }
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }

        if packet.flags.contains(TcpSegmentFlags::RST) {
            let Some(acknowledgment) = packet.acknowledgment else {
                return (
                    connection_from_machine(
                        machine,
                        connection_id,
                        owner_worker,
                        local_port,
                        local,
                        remote,
                    ),
                    None,
                );
            };
            if machine.protocol.accepts_ack(acknowledgment) {
                machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
                let closed = machine.into_next::<Closed>();
                return (
                    connection_from_machine(
                        closed,
                        connection_id,
                        owner_worker,
                        local_port,
                        local,
                        remote,
                    ),
                    None,
                );
            }
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }

        if !packet.flags.contains(TcpSegmentFlags::SYN) {
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }

        machine
            .protocol
            .apply_peer_handshake_capabilities(packet.capabilities);
        machine.protocol.irs = packet.sequence;
        machine.protocol.snd_wnd = machine
            .protocol
            .effective_send_window(u32::from(packet.advertised_window));
        machine.protocol.rcv_nxt = TcpSeq::new(packet.sequence).advance(1).raw();

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            if !TcpSeq::new(acknowledgment).after(TcpSeq::new(machine.protocol.iss))
                || !machine.protocol.accepts_ack(acknowledgment)
            {
                return (
                    connection_from_machine(
                        machine,
                        connection_id,
                        owner_worker,
                        local_port,
                        local,
                        remote,
                    ),
                    None,
                );
            }
            machine.protocol.snd_una = acknowledgment;
            let next = machine.into_next::<Established>();
            let control = next.protocol.control(packet, TcpSegmentFlags::ACK, None);
            return (
                connection_from_machine(
                    next,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                Some(control),
            );
        }

        let next = machine.into_next::<SynRcvd>();
        let control =
            next.protocol
                .control(packet, TcpSegmentFlags::SYN | TcpSegmentFlags::ACK, None);
        (
            connection_from_machine(next, connection_id, owner_worker, local_port, local, remote),
            Some(control),
        )
    }
}

impl TcpConnection<SynRcvd> {
    pub(crate) fn receive_final_ack<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<SynRcvd>>
            + From<TcpConnection<Established>>
            + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        let Some(acknowledgment) = packet.acknowledgment else {
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        };
        if !packet.flags.contains(TcpSegmentFlags::ACK)
            || !machine.protocol.accepts_ack(acknowledgment)
        {
            let control =
                machine
                    .protocol
                    .control(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                Some(control),
            );
        }
        let next = machine.on_final_ack(acknowledgment, packet.advertised_window);
        let control = next.protocol.control(packet, TcpSegmentFlags::ACK, None);
        (
            connection_from_machine(next, connection_id, owner_worker, local_port, local, remote),
            Some(control),
        )
    }
}

impl TcpConnection<Established> {
    pub(crate) fn receive_data<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>, Option<usize>, bool)
    where
        Out: From<TcpConnection<Established>>
            + From<TcpConnection<CloseWait>>
            + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
                None,
                false,
            );
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            machine
                .protocol
                .apply_ack(acknowledgment, packet.advertised_window);
        }

        let mut ack = false;
        let mut accepted_payload_len = None;
        if packet.payload_len != 0 {
            ack = true;
            if machine
                .protocol
                .receive_in_order(packet.sequence, packet.payload_len)
            {
                accepted_payload_len = Some(packet.payload_len);
            }
        }

        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let next = machine.on_fin(
                packet.sequence,
                packet.payload_len,
                packet.acknowledgment,
                packet.advertised_window,
            );
            let control = next.protocol.control(packet, TcpSegmentFlags::ACK, None);
            return (
                connection_from_machine(
                    next,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                Some(control),
                accepted_payload_len,
                true,
            );
        }

        let control = ack.then(|| machine.protocol.control(packet, TcpSegmentFlags::ACK, None));
        (
            connection_from_machine(
                machine,
                connection_id,
                owner_worker,
                local_port,
                local,
                remote,
            ),
            control,
            accepted_payload_len,
            false,
        )
    }
}

impl TcpConnection<CloseWait> {
    pub(crate) fn receive_close_wait<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<CloseWait>> + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            machine
                .protocol
                .apply_ack(acknowledgment, packet.advertised_window);
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            machine.protocol.receive_in_order(packet.sequence, 1);
            let control = machine.protocol.control(packet, TcpSegmentFlags::ACK, None);
            return (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                Some(control),
            );
        }
        (
            connection_from_machine(
                machine,
                connection_id,
                owner_worker,
                local_port,
                local,
                remote,
            ),
            None,
        )
    }
}

impl TcpConnection<FinWait1> {
    pub(crate) fn receive_fin_wait1<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<FinWait1>>
            + From<TcpConnection<FinWait2>>
            + From<TcpConnection<Closing>>
            + From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        let fin = packet.flags.contains(TcpSegmentFlags::FIN);
        let ack = packet
            .acknowledgment
            .filter(|_| packet.flags.contains(TcpSegmentFlags::ACK));
        match (fin, ack) {
            (true, Some(acknowledgment)) if machine.protocol.accepts_ack(acknowledgment) => {
                let next =
                    machine.on_fin_ack(packet.sequence, acknowledgment, packet.advertised_window);
                let control = next.protocol.control(packet, TcpSegmentFlags::ACK, None);
                (
                    connection_from_machine(
                        next,
                        connection_id,
                        owner_worker,
                        local_port,
                        local,
                        remote,
                    ),
                    Some(control),
                )
            }
            (false, Some(acknowledgment)) if machine.protocol.accepts_ack(acknowledgment) => (
                connection_from_machine(
                    machine.on_ack(acknowledgment, packet.advertised_window),
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            ),
            (true, _) => {
                let next = machine.on_fin(packet.sequence, packet.advertised_window);
                let control = next.protocol.control(packet, TcpSegmentFlags::ACK, None);
                (
                    connection_from_machine(
                        next,
                        connection_id,
                        owner_worker,
                        local_port,
                        local,
                        remote,
                    ),
                    Some(control),
                )
            }
            _ => (
                connection_from_machine(
                    machine,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            ),
        }
    }
}

impl TcpConnection<FinWait2> {
    pub(crate) fn receive_fin_wait2<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<FinWait2>>
            + From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let next = machine.on_fin(packet.sequence, packet.advertised_window);
            let control = next.protocol.control(packet, TcpSegmentFlags::ACK, None);
            return (
                connection_from_machine(
                    next,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                Some(control),
            );
        }
        (
            connection_from_machine(
                machine,
                connection_id,
                owner_worker,
                local_port,
                local,
                remote,
            ),
            None,
        )
    }
}

impl TcpConnection<Closing> {
    pub(crate) fn receive_closing<Out>(self, packet: &TcpPacket) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<Closing>>
            + From<TcpConnection<TimeWait>>
            + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && machine.protocol.accepts_ack(acknowledgment)
        {
            let next = machine.on_ack(acknowledgment, packet.advertised_window);
            return (
                connection_from_machine(
                    next,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        (
            connection_from_machine(
                machine,
                connection_id,
                owner_worker,
                local_port,
                local,
                remote,
            ),
            None,
        )
    }
}

impl TcpConnection<LastAck> {
    pub(crate) fn receive_last_ack<Out>(self, packet: &TcpPacket) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<LastAck>> + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && machine.protocol.accepts_ack(acknowledgment)
        {
            let next = machine.on_ack(acknowledgment, packet.advertised_window);
            return (
                connection_from_machine(
                    next,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        (
            connection_from_machine(
                machine,
                connection_id,
                owner_worker,
                local_port,
                local,
                remote,
            ),
            None,
        )
    }
}

impl TcpConnection<TimeWait> {
    pub(crate) fn receive_time_wait<Out>(
        self,
        packet: &TcpPacket,
    ) -> (Out, Option<TcpSegmentHeader>)
    where
        Out: From<TcpConnection<TimeWait>> + From<TcpConnection<Closed>>,
    {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let mut machine = machine;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            machine.protocol.close_reason = Some(TcpCloseReason::RemoteReset);
            let closed = machine.into_next::<Closed>();
            return (
                connection_from_machine(
                    closed,
                    connection_id,
                    owner_worker,
                    local_port,
                    local,
                    remote,
                ),
                None,
            );
        }
        (
            connection_from_machine(
                machine,
                connection_id,
                owner_worker,
                local_port,
                local,
                remote,
            ),
            None,
        )
    }
}

#[inline]
fn connection_from_machine<S, Out>(
    machine: TcpStateMachine<S>,
    connection_id: Option<hammer_core::protocol::tcp::TcpConnectionId>,
    owner_worker: hammer_adapter::DataWorkerId,
    local_port: u16,
    local: Option<std::net::SocketAddr>,
    remote: std::net::SocketAddr,
) -> Out
where
    S: TcpPhase,
    Out: From<TcpConnection<S>>,
{
    Out::from(TcpConnection::from_parts(
        connection_id,
        owner_worker,
        local_port,
        local,
        remote,
        machine,
    ))
}

#[cfg(test)]
mod tests {
    use hammer_core::protocol::tcp::{TcpCapabilities, TcpState};

    use super::{CloseWait, Closed, Established, Listen, SynRcvd, SynSent, TcpStateMachine};
    use crate::transport::tcp::TcpInputNext;

    fn assert_machine<S>(machine: TcpStateMachine<S>) -> TcpStateMachine<S> {
        machine
    }

    #[test]
    fn closed_connect_returns_syn_sent_typestate() {
        let closed: TcpStateMachine<Closed> = TcpStateMachine::new();

        let syn_sent = assert_machine::<SynSent>(closed.connect(7_000));

        assert_eq!(syn_sent.tcp_state(), TcpState::SynSent);
        assert_eq!(syn_sent.next_node(), TcpInputNext::SynSent);
        assert_eq!(syn_sent.iss(), 7_000);
        assert_eq!(syn_sent.snd_una(), 7_000);
        assert_eq!(syn_sent.snd_nxt(), 7_001);
    }

    #[test]
    fn listen_syn_returns_syn_rcvd_typestate() {
        let listen: TcpStateMachine<Listen> = TcpStateMachine::new();

        let syn_rcvd =
            assert_machine::<SynRcvd>(listen.on_syn(7_000, u16::MAX, TcpCapabilities::default()));

        assert_eq!(syn_rcvd.tcp_state(), TcpState::SynRcvd);
        assert_eq!(syn_rcvd.next_node(), TcpInputNext::RcvProcess);
        assert_eq!(syn_rcvd.irs(), 7_000);
        assert_eq!(syn_rcvd.rcv_nxt(), 7_001);
    }

    #[test]
    fn syn_rcvd_final_ack_returns_established_typestate() {
        let listen: TcpStateMachine<Listen> = TcpStateMachine::new();
        let syn_rcvd = listen.on_syn(7_000, u16::MAX, TcpCapabilities::default());
        let final_ack = syn_rcvd.snd_nxt();

        let established = assert_machine::<Established>(syn_rcvd.on_final_ack(final_ack, u16::MAX));

        assert_eq!(established.tcp_state(), TcpState::Established);
        assert_eq!(established.next_node(), TcpInputNext::Established);
    }

    #[test]
    fn established_fin_returns_close_wait_typestate() {
        let listen: TcpStateMachine<Listen> = TcpStateMachine::new();
        let syn_rcvd = listen.on_syn(7_000, u16::MAX, TcpCapabilities::default());
        let final_ack = syn_rcvd.snd_nxt();
        let established = syn_rcvd.on_final_ack(final_ack, u16::MAX);
        let fin_ack = established.snd_nxt();

        let close_wait =
            assert_machine::<CloseWait>(established.on_fin(7_001, 0, Some(fin_ack), u16::MAX));

        assert_eq!(close_wait.tcp_state(), TcpState::CloseWait);
        assert_eq!(close_wait.next_node(), TcpInputNext::RcvProcess);
    }

    #[test]
    fn listen_syn_negotiates_tcp_options() {
        let local = TcpCapabilities {
            window_scale: Some(4),
            sack: true,
            timestamps: true,
            ecn: true,
            ..TcpCapabilities::default()
        };
        let remote = TcpCapabilities {
            window_scale: Some(6),
            sack: true,
            timestamps: true,
            ecn: false,
            ..TcpCapabilities::default()
        };
        let mut listen: TcpStateMachine<Listen> = TcpStateMachine::new();
        listen.configure_local_capabilities(local);

        let syn_rcvd = listen.on_syn(7_000, u16::MAX, remote);
        let negotiated = syn_rcvd.negotiated_options();

        assert_eq!(syn_rcvd.remote_capabilities(), Some(remote));
        assert_eq!(negotiated.send_window_scale, Some(6));
        assert_eq!(negotiated.receive_window_scale, Some(4));
        assert!(negotiated.sack);
        assert!(negotiated.timestamps);
        assert!(!negotiated.ecn);
    }

    #[test]
    fn negotiated_window_scaling_is_applied() {
        let mut listen: TcpStateMachine<Listen> = TcpStateMachine::new();
        listen.configure_local_capabilities(TcpCapabilities {
            window_scale: Some(20),
            ..TcpCapabilities::default()
        });

        let syn_rcvd = listen.on_syn(
            7_000,
            u16::MAX,
            TcpCapabilities {
                window_scale: Some(20),
                ..TcpCapabilities::default()
            },
        );

        assert_eq!(syn_rcvd.effective_send_window_scale(), 14);
        assert_eq!(syn_rcvd.effective_receive_window_scale(), 14);
        assert_eq!(
            syn_rcvd.effective_send_window(u32::from(u16::MAX)),
            u32::from(u16::MAX) << 14
        );
        assert_eq!(syn_rcvd.effective_send_window(u32::MAX), u32::MAX);
        assert_eq!(
            syn_rcvd.advertised_receive_window(u32::from(u16::MAX) << 14),
            u16::MAX
        );
        assert_eq!(syn_rcvd.advertised_receive_window(u32::MAX), u16::MAX);
    }
}
