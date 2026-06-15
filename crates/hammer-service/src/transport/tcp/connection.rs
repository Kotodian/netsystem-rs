use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpNegotiatedOptions, TcpSegmentHeader,
    TcpState,
};

use super::TcpInputNext;
use super::congestion::{TcpCongestionAckSample, TcpCongestionState};
use super::state_machine::{
    CloseWait, Closed, Closing, Established, FinWait1, FinWait2, LastAck, Listen, SynRcvd, SynSent,
    TcpPhase, TcpRootPhase, TcpStateMachine, TimeWait,
};

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
    pub(super) const fn bit(self) -> u8 {
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
    pub(super) fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.local_capabilities = normalize_tcp_capabilities(capabilities);
        self.recalculate_negotiated_options();
        self.negotiated
    }

    #[inline]
    pub(super) fn apply_peer_handshake_capabilities(
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
pub struct TcpConnection<S> {
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    machine: TcpStateMachine<S>,
}

#[derive(Debug, Clone)]
pub enum TcpConnectionState {
    Closed(TcpConnection<Closed>),
    Listen(TcpConnection<Listen>),
    SynSent(TcpConnection<SynSent>),
    SynRcvd(TcpConnection<SynRcvd>),
    Established(TcpConnection<Established>),
    CloseWait(TcpConnection<CloseWait>),
    LastAck(TcpConnection<LastAck>),
    FinWait1(TcpConnection<FinWait1>),
    FinWait2(TcpConnection<FinWait2>),
    Closing(TcpConnection<Closing>),
    TimeWait(TcpConnection<TimeWait>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectionView {
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
}

impl TcpConnectionView {
    #[inline]
    pub const fn connection_id(self) -> Option<TcpConnectionId> {
        self.connection_id
    }

    #[inline]
    pub const fn owner_worker(self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub const fn state(self) -> TcpState {
        self.state
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        self.local_port
    }

    #[inline]
    pub const fn local(self) -> Option<SocketAddr> {
        self.local
    }

    #[inline]
    pub const fn remote(self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub const fn iss(self) -> u32 {
        self.iss
    }

    #[inline]
    pub const fn irs(self) -> u32 {
        self.irs
    }

    #[inline]
    pub const fn snd_una(self) -> u32 {
        self.snd_una
    }

    #[inline]
    pub const fn snd_nxt(self) -> u32 {
        self.snd_nxt
    }

    #[inline]
    pub const fn snd_wnd(self) -> u32 {
        self.snd_wnd
    }

    #[inline]
    pub const fn rcv_nxt(self) -> u32 {
        self.rcv_nxt
    }

    #[inline]
    pub const fn rcv_wnd(self) -> u32 {
        self.rcv_wnd
    }
}

impl<S: TcpRootPhase> TcpConnection<S> {
    pub fn new(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        let machine: TcpStateMachine<S> = TcpStateMachine::new();
        Self::from_parts(
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            machine,
        )
    }
}

impl<S: TcpPhase> TcpConnection<S> {
    #[inline]
    pub(super) fn from_parts(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        machine: TcpStateMachine<S>,
    ) -> Self {
        Self {
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            machine,
        }
    }

    #[inline]
    pub(super) fn into_metadata(
        self,
    ) -> (
        Option<TcpConnectionId>,
        DataWorkerId,
        u16,
        Option<SocketAddr>,
        SocketAddr,
        TcpStateMachine<S>,
    ) {
        (
            self.connection_id,
            self.owner_worker,
            self.local_port,
            self.local,
            self.remote,
            self.machine,
        )
    }

    #[inline]
    pub const fn tcp_state(&self) -> TcpState {
        self.machine.tcp_state()
    }

    #[inline]
    pub const fn state(&self) -> TcpState {
        self.tcp_state()
    }

    #[inline]
    pub const fn next_node(&self) -> TcpInputNext {
        self.machine.next_node()
    }

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
        self.machine.iss()
    }

    #[inline]
    pub const fn irs(&self) -> u32 {
        self.machine.irs()
    }

    #[inline]
    pub const fn snd_una(&self) -> u32 {
        self.machine.snd_una()
    }

    #[inline]
    pub const fn snd_nxt(&self) -> u32 {
        self.machine.snd_nxt()
    }

    #[inline]
    pub const fn snd_wnd(&self) -> u32 {
        self.machine.snd_wnd()
    }

    #[inline]
    pub const fn rcv_nxt(&self) -> u32 {
        self.machine.rcv_nxt()
    }

    #[inline]
    pub const fn rcv_wnd(&self) -> u32 {
        self.machine.rcv_wnd()
    }

    #[inline]
    pub fn close_reason(&self) -> Option<TcpCloseReason> {
        self.machine.close_reason()
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        self.machine.local_capabilities()
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        self.machine.remote_capabilities()
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.machine.negotiated_options()
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        self.machine.effective_send_window_scale()
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        self.machine.effective_receive_window_scale()
    }

    #[inline]
    pub fn effective_send_window(&self, advertised_window: u32) -> u32 {
        self.machine.effective_send_window(advertised_window)
    }

    #[inline]
    pub fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        self.machine.advertised_receive_window(receive_window)
    }

    #[inline]
    pub fn output_payload_len(&self) -> usize {
        self.machine.output_payload_len()
    }

    #[inline]
    pub fn congestion(&self) -> &TcpCongestionState {
        self.machine.congestion()
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> &TcpRetransmitTimeoutState {
        self.machine.retransmit_timeout()
    }

    #[inline]
    pub const fn tcp_timer_is_active(&self, timer: TcpConnectionTimerKind) -> bool {
        self.machine.tcp_timer_is_active(timer)
    }

    #[inline]
    pub const fn tcp_timer_is_pending(&self, timer: TcpConnectionTimerKind) -> bool {
        self.machine.tcp_timer_is_pending(timer)
    }

    #[inline]
    pub const fn tcp_timer_is_live(&self, timer: TcpConnectionTimerKind) -> bool {
        self.machine.tcp_timer_is_live(timer)
    }

    #[inline]
    pub const fn next_output_at(&self) -> Option<Instant> {
        self.machine.next_output_at()
    }

    #[inline]
    pub(super) fn view(&self) -> TcpConnectionView {
        TcpConnectionView {
            connection_id: self.connection_id,
            owner_worker: self.owner_worker,
            state: self.tcp_state(),
            local_port: self.local_port,
            local: self.local,
            remote: self.remote,
            iss: self.iss(),
            irs: self.irs(),
            snd_una: self.snd_una(),
            snd_nxt: self.snd_nxt(),
            snd_wnd: self.snd_wnd(),
            rcv_nxt: self.rcv_nxt(),
            rcv_wnd: self.rcv_wnd(),
        }
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.machine.configure_local_capabilities(capabilities)
    }

    #[inline]
    pub fn observe_congestion_ack(&mut self, sample: TcpCongestionAckSample) {
        self.machine.observe_congestion_ack(sample);
    }

    #[inline]
    pub fn observe_congestion_send(&mut self, bytes_sent: u32, bytes_in_flight: u32, now: Instant) {
        self.machine
            .observe_congestion_send(bytes_sent, bytes_in_flight, now);
    }

    #[inline]
    pub fn observe_congestion_loss(&mut self, bytes_lost: u32) {
        self.machine.observe_congestion_loss(bytes_lost);
    }

    #[inline]
    pub fn schedule_next_output(&mut self, deadline: Option<Instant>) {
        self.machine.schedule_next_output(deadline);
    }

    #[inline]
    pub fn tcp_timer_set(&mut self, timer: TcpConnectionTimerKind) {
        self.machine.arm_tcp_timer(timer);
    }

    #[inline]
    pub fn tcp_timer_reset(&mut self, timer: TcpConnectionTimerKind) {
        self.machine.reset_tcp_timer(timer);
    }

    #[inline]
    pub fn tcp_timer_expire(&mut self, timer: TcpConnectionTimerKind) {
        self.machine.expire_tcp_timer(timer);
    }

    #[inline]
    pub fn tcp_timer_take_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        self.machine.take_pending_tcp_timer(timer)
    }

    #[inline]
    pub fn tcp_timer_dispatch_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        self.machine.dispatch_pending_tcp_timer(timer)
    }

    #[inline]
    pub fn observe_retransmit_timeout(&mut self) -> Duration {
        self.machine.observe_retransmit_timeout()
    }

    #[inline]
    pub fn syn_header(&self) -> CoreResult<TcpSegmentHeader> {
        let local = self
            .local
            .ok_or_else(|| CoreError::internal("syn-sent tcp session missing local address"))?;
        Ok(self.machine.syn_header(local.port(), self.remote.port()))
    }
}

impl TcpConnection<Closed> {
    #[inline]
    pub fn connect(self, initial_sequence: u32) -> TcpConnection<SynSent> {
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            self.into_metadata();
        let next = machine.connect(initial_sequence);
        TcpConnection::from_parts(connection_id, owner_worker, local_port, local, remote, next)
    }
}

impl TcpConnectionState {
    #[cfg(test)]
    pub(crate) fn established_for_test(
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        let connection: TcpConnection<Listen> =
            TcpConnection::new(connection_id, owner_worker, local_port, local, remote);
        let (connection_id, owner_worker, local_port, local, remote, machine) =
            connection.into_metadata();
        let machine = machine.on_syn(7_000, u16::MAX, TcpCapabilities::default());
        let final_ack = machine.snd_nxt();
        let machine = machine.on_final_ack(final_ack, u16::MAX);
        TcpConnection::from_parts(
            connection_id,
            owner_worker,
            local_port,
            local,
            remote,
            machine,
        )
        .into()
    }

    #[inline]
    pub fn connection_id(&self) -> Option<TcpConnectionId> {
        self.view().connection_id()
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.view().owner_worker()
    }

    #[inline]
    pub fn state(&self) -> TcpState {
        self.view().state()
    }

    #[inline]
    pub fn next_node(&self) -> TcpInputNext {
        match self {
            Self::Closed(connection) => connection.next_node(),
            Self::Listen(connection) => connection.next_node(),
            Self::SynSent(connection) => connection.next_node(),
            Self::SynRcvd(connection) => connection.next_node(),
            Self::Established(connection) => connection.next_node(),
            Self::CloseWait(connection) => connection.next_node(),
            Self::LastAck(connection) => connection.next_node(),
            Self::FinWait1(connection) => connection.next_node(),
            Self::FinWait2(connection) => connection.next_node(),
            Self::Closing(connection) => connection.next_node(),
            Self::TimeWait(connection) => connection.next_node(),
        }
    }

    #[inline]
    pub fn local_port(&self) -> u16 {
        self.view().local_port()
    }

    #[inline]
    pub fn local(&self) -> Option<SocketAddr> {
        self.view().local()
    }

    #[inline]
    pub fn remote(&self) -> SocketAddr {
        self.view().remote()
    }

    #[inline]
    pub fn iss(&self) -> u32 {
        self.view().iss()
    }

    #[inline]
    pub fn irs(&self) -> u32 {
        self.view().irs()
    }

    #[inline]
    pub fn snd_una(&self) -> u32 {
        self.view().snd_una()
    }

    #[inline]
    pub fn snd_nxt(&self) -> u32 {
        self.view().snd_nxt()
    }

    #[inline]
    pub fn snd_wnd(&self) -> u32 {
        self.view().snd_wnd()
    }

    #[inline]
    pub fn rcv_nxt(&self) -> u32 {
        self.view().rcv_nxt()
    }

    #[inline]
    pub fn rcv_wnd(&self) -> u32 {
        self.view().rcv_wnd()
    }

    #[inline]
    pub fn close_reason(&self) -> Option<TcpCloseReason> {
        match self {
            Self::Closed(connection) => connection.close_reason(),
            Self::Listen(connection) => connection.close_reason(),
            Self::SynSent(connection) => connection.close_reason(),
            Self::SynRcvd(connection) => connection.close_reason(),
            Self::Established(connection) => connection.close_reason(),
            Self::CloseWait(connection) => connection.close_reason(),
            Self::LastAck(connection) => connection.close_reason(),
            Self::FinWait1(connection) => connection.close_reason(),
            Self::FinWait2(connection) => connection.close_reason(),
            Self::Closing(connection) => connection.close_reason(),
            Self::TimeWait(connection) => connection.close_reason(),
        }
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        match self {
            Self::Closed(connection) => connection.local_capabilities(),
            Self::Listen(connection) => connection.local_capabilities(),
            Self::SynSent(connection) => connection.local_capabilities(),
            Self::SynRcvd(connection) => connection.local_capabilities(),
            Self::Established(connection) => connection.local_capabilities(),
            Self::CloseWait(connection) => connection.local_capabilities(),
            Self::LastAck(connection) => connection.local_capabilities(),
            Self::FinWait1(connection) => connection.local_capabilities(),
            Self::FinWait2(connection) => connection.local_capabilities(),
            Self::Closing(connection) => connection.local_capabilities(),
            Self::TimeWait(connection) => connection.local_capabilities(),
        }
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        match self {
            Self::Closed(connection) => connection.remote_capabilities(),
            Self::Listen(connection) => connection.remote_capabilities(),
            Self::SynSent(connection) => connection.remote_capabilities(),
            Self::SynRcvd(connection) => connection.remote_capabilities(),
            Self::Established(connection) => connection.remote_capabilities(),
            Self::CloseWait(connection) => connection.remote_capabilities(),
            Self::LastAck(connection) => connection.remote_capabilities(),
            Self::FinWait1(connection) => connection.remote_capabilities(),
            Self::FinWait2(connection) => connection.remote_capabilities(),
            Self::Closing(connection) => connection.remote_capabilities(),
            Self::TimeWait(connection) => connection.remote_capabilities(),
        }
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        match self {
            Self::Closed(connection) => connection.negotiated_options(),
            Self::Listen(connection) => connection.negotiated_options(),
            Self::SynSent(connection) => connection.negotiated_options(),
            Self::SynRcvd(connection) => connection.negotiated_options(),
            Self::Established(connection) => connection.negotiated_options(),
            Self::CloseWait(connection) => connection.negotiated_options(),
            Self::LastAck(connection) => connection.negotiated_options(),
            Self::FinWait1(connection) => connection.negotiated_options(),
            Self::FinWait2(connection) => connection.negotiated_options(),
            Self::Closing(connection) => connection.negotiated_options(),
            Self::TimeWait(connection) => connection.negotiated_options(),
        }
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        self.negotiated_options()
            .send_window_scale
            .unwrap_or_default()
            .min(TCP_MAX_WINDOW_SCALE)
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        self.negotiated_options()
            .receive_window_scale
            .unwrap_or_default()
            .min(TCP_MAX_WINDOW_SCALE)
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
        match self {
            Self::Closed(connection) => connection.output_payload_len(),
            Self::Listen(connection) => connection.output_payload_len(),
            Self::SynSent(connection) => connection.output_payload_len(),
            Self::SynRcvd(connection) => connection.output_payload_len(),
            Self::Established(connection) => connection.output_payload_len(),
            Self::CloseWait(connection) => connection.output_payload_len(),
            Self::LastAck(connection) => connection.output_payload_len(),
            Self::FinWait1(connection) => connection.output_payload_len(),
            Self::FinWait2(connection) => connection.output_payload_len(),
            Self::Closing(connection) => connection.output_payload_len(),
            Self::TimeWait(connection) => connection.output_payload_len(),
        }
    }

    #[inline]
    pub fn congestion(&self) -> &TcpCongestionState {
        match self {
            Self::Closed(connection) => connection.congestion(),
            Self::Listen(connection) => connection.congestion(),
            Self::SynSent(connection) => connection.congestion(),
            Self::SynRcvd(connection) => connection.congestion(),
            Self::Established(connection) => connection.congestion(),
            Self::CloseWait(connection) => connection.congestion(),
            Self::LastAck(connection) => connection.congestion(),
            Self::FinWait1(connection) => connection.congestion(),
            Self::FinWait2(connection) => connection.congestion(),
            Self::Closing(connection) => connection.congestion(),
            Self::TimeWait(connection) => connection.congestion(),
        }
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> &TcpRetransmitTimeoutState {
        match self {
            Self::Closed(connection) => connection.retransmit_timeout(),
            Self::Listen(connection) => connection.retransmit_timeout(),
            Self::SynSent(connection) => connection.retransmit_timeout(),
            Self::SynRcvd(connection) => connection.retransmit_timeout(),
            Self::Established(connection) => connection.retransmit_timeout(),
            Self::CloseWait(connection) => connection.retransmit_timeout(),
            Self::LastAck(connection) => connection.retransmit_timeout(),
            Self::FinWait1(connection) => connection.retransmit_timeout(),
            Self::FinWait2(connection) => connection.retransmit_timeout(),
            Self::Closing(connection) => connection.retransmit_timeout(),
            Self::TimeWait(connection) => connection.retransmit_timeout(),
        }
    }

    #[inline]
    pub fn tcp_timer_set(&mut self, timer: TcpConnectionTimerKind) {
        match self {
            Self::Closed(connection) => connection.tcp_timer_set(timer),
            Self::Listen(connection) => connection.tcp_timer_set(timer),
            Self::SynSent(connection) => connection.tcp_timer_set(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_set(timer),
            Self::Established(connection) => connection.tcp_timer_set(timer),
            Self::CloseWait(connection) => connection.tcp_timer_set(timer),
            Self::LastAck(connection) => connection.tcp_timer_set(timer),
            Self::FinWait1(connection) => connection.tcp_timer_set(timer),
            Self::FinWait2(connection) => connection.tcp_timer_set(timer),
            Self::Closing(connection) => connection.tcp_timer_set(timer),
            Self::TimeWait(connection) => connection.tcp_timer_set(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_reset(&mut self, timer: TcpConnectionTimerKind) {
        match self {
            Self::Closed(connection) => connection.tcp_timer_reset(timer),
            Self::Listen(connection) => connection.tcp_timer_reset(timer),
            Self::SynSent(connection) => connection.tcp_timer_reset(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_reset(timer),
            Self::Established(connection) => connection.tcp_timer_reset(timer),
            Self::CloseWait(connection) => connection.tcp_timer_reset(timer),
            Self::LastAck(connection) => connection.tcp_timer_reset(timer),
            Self::FinWait1(connection) => connection.tcp_timer_reset(timer),
            Self::FinWait2(connection) => connection.tcp_timer_reset(timer),
            Self::Closing(connection) => connection.tcp_timer_reset(timer),
            Self::TimeWait(connection) => connection.tcp_timer_reset(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_expire(&mut self, timer: TcpConnectionTimerKind) {
        match self {
            Self::Closed(connection) => connection.tcp_timer_expire(timer),
            Self::Listen(connection) => connection.tcp_timer_expire(timer),
            Self::SynSent(connection) => connection.tcp_timer_expire(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_expire(timer),
            Self::Established(connection) => connection.tcp_timer_expire(timer),
            Self::CloseWait(connection) => connection.tcp_timer_expire(timer),
            Self::LastAck(connection) => connection.tcp_timer_expire(timer),
            Self::FinWait1(connection) => connection.tcp_timer_expire(timer),
            Self::FinWait2(connection) => connection.tcp_timer_expire(timer),
            Self::Closing(connection) => connection.tcp_timer_expire(timer),
            Self::TimeWait(connection) => connection.tcp_timer_expire(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_take_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        match self {
            Self::Closed(connection) => connection.tcp_timer_take_pending(timer),
            Self::Listen(connection) => connection.tcp_timer_take_pending(timer),
            Self::SynSent(connection) => connection.tcp_timer_take_pending(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_take_pending(timer),
            Self::Established(connection) => connection.tcp_timer_take_pending(timer),
            Self::CloseWait(connection) => connection.tcp_timer_take_pending(timer),
            Self::LastAck(connection) => connection.tcp_timer_take_pending(timer),
            Self::FinWait1(connection) => connection.tcp_timer_take_pending(timer),
            Self::FinWait2(connection) => connection.tcp_timer_take_pending(timer),
            Self::Closing(connection) => connection.tcp_timer_take_pending(timer),
            Self::TimeWait(connection) => connection.tcp_timer_take_pending(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_dispatch_pending(&mut self, timer: TcpConnectionTimerKind) -> bool {
        match self {
            Self::Closed(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::Listen(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::SynSent(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::Established(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::CloseWait(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::LastAck(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::FinWait1(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::FinWait2(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::Closing(connection) => connection.tcp_timer_dispatch_pending(timer),
            Self::TimeWait(connection) => connection.tcp_timer_dispatch_pending(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_is_active(&self, timer: TcpConnectionTimerKind) -> bool {
        match self {
            Self::Closed(connection) => connection.tcp_timer_is_active(timer),
            Self::Listen(connection) => connection.tcp_timer_is_active(timer),
            Self::SynSent(connection) => connection.tcp_timer_is_active(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_is_active(timer),
            Self::Established(connection) => connection.tcp_timer_is_active(timer),
            Self::CloseWait(connection) => connection.tcp_timer_is_active(timer),
            Self::LastAck(connection) => connection.tcp_timer_is_active(timer),
            Self::FinWait1(connection) => connection.tcp_timer_is_active(timer),
            Self::FinWait2(connection) => connection.tcp_timer_is_active(timer),
            Self::Closing(connection) => connection.tcp_timer_is_active(timer),
            Self::TimeWait(connection) => connection.tcp_timer_is_active(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_is_pending(&self, timer: TcpConnectionTimerKind) -> bool {
        match self {
            Self::Closed(connection) => connection.tcp_timer_is_pending(timer),
            Self::Listen(connection) => connection.tcp_timer_is_pending(timer),
            Self::SynSent(connection) => connection.tcp_timer_is_pending(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_is_pending(timer),
            Self::Established(connection) => connection.tcp_timer_is_pending(timer),
            Self::CloseWait(connection) => connection.tcp_timer_is_pending(timer),
            Self::LastAck(connection) => connection.tcp_timer_is_pending(timer),
            Self::FinWait1(connection) => connection.tcp_timer_is_pending(timer),
            Self::FinWait2(connection) => connection.tcp_timer_is_pending(timer),
            Self::Closing(connection) => connection.tcp_timer_is_pending(timer),
            Self::TimeWait(connection) => connection.tcp_timer_is_pending(timer),
        }
    }

    #[inline]
    pub fn tcp_timer_is_live(&self, timer: TcpConnectionTimerKind) -> bool {
        self.tcp_timer_is_active(timer) || self.tcp_timer_is_pending(timer)
    }

    #[inline]
    pub fn next_output_at(&self) -> Option<Instant> {
        match self {
            Self::Closed(connection) => connection.next_output_at(),
            Self::Listen(connection) => connection.next_output_at(),
            Self::SynSent(connection) => connection.next_output_at(),
            Self::SynRcvd(connection) => connection.next_output_at(),
            Self::Established(connection) => connection.next_output_at(),
            Self::CloseWait(connection) => connection.next_output_at(),
            Self::LastAck(connection) => connection.next_output_at(),
            Self::FinWait1(connection) => connection.next_output_at(),
            Self::FinWait2(connection) => connection.next_output_at(),
            Self::Closing(connection) => connection.next_output_at(),
            Self::TimeWait(connection) => connection.next_output_at(),
        }
    }

    #[inline]
    pub fn schedule_next_output(&mut self, deadline: Option<Instant>) {
        match self {
            Self::Closed(connection) => connection.schedule_next_output(deadline),
            Self::Listen(connection) => connection.schedule_next_output(deadline),
            Self::SynSent(connection) => connection.schedule_next_output(deadline),
            Self::SynRcvd(connection) => connection.schedule_next_output(deadline),
            Self::Established(connection) => connection.schedule_next_output(deadline),
            Self::CloseWait(connection) => connection.schedule_next_output(deadline),
            Self::LastAck(connection) => connection.schedule_next_output(deadline),
            Self::FinWait1(connection) => connection.schedule_next_output(deadline),
            Self::FinWait2(connection) => connection.schedule_next_output(deadline),
            Self::Closing(connection) => connection.schedule_next_output(deadline),
            Self::TimeWait(connection) => connection.schedule_next_output(deadline),
        }
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        match self {
            Self::Closed(connection) => connection.set_local_capabilities(capabilities),
            Self::Listen(connection) => connection.set_local_capabilities(capabilities),
            Self::SynSent(connection) => connection.set_local_capabilities(capabilities),
            Self::SynRcvd(connection) => connection.set_local_capabilities(capabilities),
            Self::Established(connection) => connection.set_local_capabilities(capabilities),
            Self::CloseWait(connection) => connection.set_local_capabilities(capabilities),
            Self::LastAck(connection) => connection.set_local_capabilities(capabilities),
            Self::FinWait1(connection) => connection.set_local_capabilities(capabilities),
            Self::FinWait2(connection) => connection.set_local_capabilities(capabilities),
            Self::Closing(connection) => connection.set_local_capabilities(capabilities),
            Self::TimeWait(connection) => connection.set_local_capabilities(capabilities),
        }
    }

    #[inline]
    pub fn observe_congestion_ack(&mut self, sample: TcpCongestionAckSample) {
        match self {
            Self::Closed(connection) => connection.observe_congestion_ack(sample),
            Self::Listen(connection) => connection.observe_congestion_ack(sample),
            Self::SynSent(connection) => connection.observe_congestion_ack(sample),
            Self::SynRcvd(connection) => connection.observe_congestion_ack(sample),
            Self::Established(connection) => connection.observe_congestion_ack(sample),
            Self::CloseWait(connection) => connection.observe_congestion_ack(sample),
            Self::LastAck(connection) => connection.observe_congestion_ack(sample),
            Self::FinWait1(connection) => connection.observe_congestion_ack(sample),
            Self::FinWait2(connection) => connection.observe_congestion_ack(sample),
            Self::Closing(connection) => connection.observe_congestion_ack(sample),
            Self::TimeWait(connection) => connection.observe_congestion_ack(sample),
        }
    }

    #[inline]
    pub fn observe_congestion_send(&mut self, bytes_sent: u32, bytes_in_flight: u32, now: Instant) {
        match self {
            Self::Closed(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::Listen(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::SynSent(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::SynRcvd(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::Established(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::CloseWait(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::LastAck(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::FinWait1(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::FinWait2(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::Closing(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
            Self::TimeWait(connection) => {
                connection.observe_congestion_send(bytes_sent, bytes_in_flight, now)
            }
        }
    }

    #[inline]
    pub fn observe_congestion_loss(&mut self, bytes_lost: u32) {
        match self {
            Self::Closed(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::Listen(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::SynSent(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::SynRcvd(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::Established(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::CloseWait(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::LastAck(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::FinWait1(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::FinWait2(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::Closing(connection) => connection.observe_congestion_loss(bytes_lost),
            Self::TimeWait(connection) => connection.observe_congestion_loss(bytes_lost),
        }
    }

    #[inline]
    pub fn observe_retransmit_timeout(&mut self) -> Duration {
        match self {
            Self::Closed(connection) => connection.observe_retransmit_timeout(),
            Self::Listen(connection) => connection.observe_retransmit_timeout(),
            Self::SynSent(connection) => connection.observe_retransmit_timeout(),
            Self::SynRcvd(connection) => connection.observe_retransmit_timeout(),
            Self::Established(connection) => connection.observe_retransmit_timeout(),
            Self::CloseWait(connection) => connection.observe_retransmit_timeout(),
            Self::LastAck(connection) => connection.observe_retransmit_timeout(),
            Self::FinWait1(connection) => connection.observe_retransmit_timeout(),
            Self::FinWait2(connection) => connection.observe_retransmit_timeout(),
            Self::Closing(connection) => connection.observe_retransmit_timeout(),
            Self::TimeWait(connection) => connection.observe_retransmit_timeout(),
        }
    }

    #[inline]
    pub fn view(&self) -> TcpConnectionView {
        match self {
            Self::Closed(connection) => connection.view(),
            Self::Listen(connection) => connection.view(),
            Self::SynSent(connection) => connection.view(),
            Self::SynRcvd(connection) => connection.view(),
            Self::Established(connection) => connection.view(),
            Self::CloseWait(connection) => connection.view(),
            Self::LastAck(connection) => connection.view(),
            Self::FinWait1(connection) => connection.view(),
            Self::FinWait2(connection) => connection.view(),
            Self::Closing(connection) => connection.view(),
            Self::TimeWait(connection) => connection.view(),
        }
    }
}

macro_rules! impl_connection_storage {
    ($state_type:ty, $variant:ident) => {
        impl TryFrom<TcpConnectionState> for TcpConnection<$state_type> {
            type Error = CoreError;

            #[inline]
            fn try_from(state: TcpConnectionState) -> Result<Self, Self::Error> {
                match state {
                    TcpConnectionState::$variant(connection) => Ok(connection),
                    other => Err(CoreError::internal(format!(
                        "tcp connection state mismatch: expected {}, got {:?}",
                        stringify!($variant),
                        other.state()
                    ))),
                }
            }
        }

        impl From<TcpConnection<$state_type>> for TcpConnectionState {
            #[inline]
            fn from(connection: TcpConnection<$state_type>) -> Self {
                Self::$variant(connection)
            }
        }
    };
}

impl_connection_storage!(Closed, Closed);
impl_connection_storage!(Listen, Listen);
impl_connection_storage!(SynSent, SynSent);
impl_connection_storage!(SynRcvd, SynRcvd);
impl_connection_storage!(Established, Established);
impl_connection_storage!(CloseWait, CloseWait);
impl_connection_storage!(LastAck, LastAck);
impl_connection_storage!(FinWait1, FinWait1);
impl_connection_storage!(FinWait2, FinWait2);
impl_connection_storage!(Closing, Closing);
impl_connection_storage!(TimeWait, TimeWait);

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
