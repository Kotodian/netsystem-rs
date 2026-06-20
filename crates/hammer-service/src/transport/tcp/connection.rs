use std::time::{Duration, Instant};

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpCapabilities, TcpNegotiatedOptions};

use crate::session::{
    SessionId, SessionQueueNext, SessionTimerExpiry, SessionTimerToken,
    node::SessionQueueOutput,
    protocol::SessionQueueControlContext,
    runtime::{SessionDriverRuntime, SessionQueueProtocol},
};
use crate::transport::congestion::CongestionController;

use super::lookup::TcpWorkerOwnedState;
use super::segment::TcpSegment;
pub use super::state_machine::TcpConnection;
use super::state_machine::{
    CloseWait, Closed, Closing, Established, FinWait1, FinWait2, LastAck, Listen, SynRcvd, SynSent,
    TimeWait,
};

const TCP_MAX_WINDOW_SCALE: u8 = 14;
pub const TCP_INITIAL_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MIN_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MAX_RETRANSMIT_TIMEOUT: Duration = Duration::from_secs(60);

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TcpConnectionTimerKind: u16 {
        const RETRANSMIT = 1 << 0;
        const RACK = 1 << 1;
        const TLP = 1 << 2;
        const PACING = 1 << 3;
        const DELAYED_ACK = 1 << 4;
        const PERSIST = 1 << 5;
        const KEEP_ALIVE = 1 << 6;
        const TIME_WAIT = 1 << 7;
    }
}

impl TcpConnectionTimerKind {
    #[inline(always)]
    pub(crate) const fn from_timer_bit(bit: u16) -> Option<Self> {
        Self::from_bits(bit)
    }

    #[inline]
    pub(crate) fn session_timer_token(self) -> Option<SessionTimerToken> {
        let bits = self.bits();
        if bits == 0 || bits.count_ones() != 1 {
            return None;
        }
        Some(SessionTimerToken::new(bits.trailing_zeros() + 1))
    }

    #[inline]
    pub(crate) fn from_session_timer_token(token: SessionTimerToken) -> Option<Self> {
        let ordinal = token.get();
        if ordinal == 0 || ordinal > u16::BITS {
            return None;
        }
        Self::from_timer_bit(1u16 << (ordinal - 1))
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
pub enum TcpConnectionState<C>
where
    C: CongestionController,
{
    Closed(TcpConnection<Closed, C>),
    Listen(TcpConnection<Listen, C>),
    SynSent(TcpConnection<SynSent, C>),
    SynRcvd(TcpConnection<SynRcvd, C>),
    Established(TcpConnection<Established, C>),
    CloseWait(TcpConnection<CloseWait, C>),
    LastAck(TcpConnection<LastAck, C>),
    FinWait1(TcpConnection<FinWait1, C>),
    FinWait2(TcpConnection<FinWait2, C>),
    Closing(TcpConnection<Closing, C>),
    TimeWait(TcpConnection<TimeWait, C>),
}

impl<C> TcpConnectionState<C>
where
    C: CongestionController,
{
    #[inline]
    #[cfg(test)]
    pub(crate) fn state(&self) -> super::TcpState {
        match self {
            Self::Closed(connection) => connection.state(),
            Self::Listen(connection) => connection.state(),
            Self::SynSent(connection) => connection.state(),
            Self::SynRcvd(connection) => connection.state(),
            Self::Established(connection) => connection.state(),
            Self::CloseWait(connection) => connection.state(),
            Self::LastAck(connection) => connection.state(),
            Self::FinWait1(connection) => connection.state(),
            Self::FinWait2(connection) => connection.state(),
            Self::Closing(connection) => connection.state(),
            Self::TimeWait(connection) => connection.state(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn connection_id(&self) -> Option<hammer_core::protocol::tcp::TcpConnectionId> {
        match self {
            Self::Closed(connection) => connection.connection_id(),
            Self::Listen(connection) => connection.connection_id(),
            Self::SynSent(connection) => connection.connection_id(),
            Self::SynRcvd(connection) => connection.connection_id(),
            Self::Established(connection) => connection.connection_id(),
            Self::CloseWait(connection) => connection.connection_id(),
            Self::LastAck(connection) => connection.connection_id(),
            Self::FinWait1(connection) => connection.connection_id(),
            Self::FinWait2(connection) => connection.connection_id(),
            Self::Closing(connection) => connection.connection_id(),
            Self::TimeWait(connection) => connection.connection_id(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn owner_worker(&self) -> hammer_adapter::DataWorkerId {
        match self {
            Self::Closed(connection) => connection.owner_worker(),
            Self::Listen(connection) => connection.owner_worker(),
            Self::SynSent(connection) => connection.owner_worker(),
            Self::SynRcvd(connection) => connection.owner_worker(),
            Self::Established(connection) => connection.owner_worker(),
            Self::CloseWait(connection) => connection.owner_worker(),
            Self::LastAck(connection) => connection.owner_worker(),
            Self::FinWait1(connection) => connection.owner_worker(),
            Self::FinWait2(connection) => connection.owner_worker(),
            Self::Closing(connection) => connection.owner_worker(),
            Self::TimeWait(connection) => connection.owner_worker(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn local(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::Closed(connection) => connection.local(),
            Self::Listen(connection) => connection.local(),
            Self::SynSent(connection) => connection.local(),
            Self::SynRcvd(connection) => connection.local(),
            Self::Established(connection) => connection.local(),
            Self::CloseWait(connection) => connection.local(),
            Self::LastAck(connection) => connection.local(),
            Self::FinWait1(connection) => connection.local(),
            Self::FinWait2(connection) => connection.local(),
            Self::Closing(connection) => connection.local(),
            Self::TimeWait(connection) => connection.local(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn remote(&self) -> std::net::SocketAddr {
        match self {
            Self::Closed(connection) => connection.remote(),
            Self::Listen(connection) => connection.remote(),
            Self::SynSent(connection) => connection.remote(),
            Self::SynRcvd(connection) => connection.remote(),
            Self::Established(connection) => connection.remote(),
            Self::CloseWait(connection) => connection.remote(),
            Self::LastAck(connection) => connection.remote(),
            Self::FinWait1(connection) => connection.remote(),
            Self::FinWait2(connection) => connection.remote(),
            Self::Closing(connection) => connection.remote(),
            Self::TimeWait(connection) => connection.remote(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn next_node(&self) -> super::TcpInputNext {
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

    #[cfg(test)]
    pub(crate) fn established_for_test(
        connection_id: Option<hammer_core::protocol::tcp::TcpConnectionId>,
        owner_worker: hammer_adapter::DataWorkerId,
        local_port: u16,
        local: Option<std::net::SocketAddr>,
        remote: std::net::SocketAddr,
    ) -> Self {
        TcpConnection::established_for_test(connection_id, owner_worker, local_port, local, remote)
            .into()
    }

    #[inline]
    pub(crate) fn on_tcp_timer(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<(TcpConnectionTimerKind, TcpSegment)> {
        match self {
            Self::SynSent(connection) => {
                let segment = connection.on_tcp_timer_expiry(timer)?;
                Some((timer, segment))
            }
            Self::Closed(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::Listen(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::SynRcvd(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::Established(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::CloseWait(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::LastAck(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::FinWait1(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::FinWait2(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::Closing(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
            Self::TimeWait(connection) => {
                connection.tcp_timer_take_pending(timer);
                None
            }
        }
    }

    #[inline]
    pub(crate) fn on_tcp_ready(&mut self) -> Option<TcpSegment> {
        match self {
            Self::SynSent(connection) => {
                connection.on_tcp_timer_expiry(TcpConnectionTimerKind::RETRANSMIT)
            }
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn on_session_close(self) -> Self {
        match self {
            Self::Established(connection) => connection.close_local().into(),
            Self::CloseWait(connection) => connection.close_local().into(),
            state => state,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn tcp_timer_is_active(&self, timer: TcpConnectionTimerKind) -> bool {
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
    pub(crate) fn tcp_timer_expire(&mut self, timer: TcpConnectionTimerKind) {
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
    pub(crate) fn tcp_timer_ticks(&self, timer: TcpConnectionTimerKind) -> Option<u64> {
        match self {
            Self::Closed(connection) => connection.tcp_timer_ticks(timer),
            Self::Listen(connection) => connection.tcp_timer_ticks(timer),
            Self::SynSent(connection) => connection.tcp_timer_ticks(timer),
            Self::SynRcvd(connection) => connection.tcp_timer_ticks(timer),
            Self::Established(connection) => connection.tcp_timer_ticks(timer),
            Self::CloseWait(connection) => connection.tcp_timer_ticks(timer),
            Self::LastAck(connection) => connection.tcp_timer_ticks(timer),
            Self::FinWait1(connection) => connection.tcp_timer_ticks(timer),
            Self::FinWait2(connection) => connection.tcp_timer_ticks(timer),
            Self::Closing(connection) => connection.tcp_timer_ticks(timer),
            Self::TimeWait(connection) => connection.tcp_timer_ticks(timer),
        }
    }
}

impl<C> SessionQueueProtocol<TcpWorkerOwnedState> for TcpConnectionState<C>
where
    C: CongestionController + 'static,
{
    fn handle_timer_expiry(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        expiry: SessionTimerExpiry,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        let Some(kind) = TcpConnectionTimerKind::from_session_timer_token(expiry.token()) else {
            return Ok(());
        };
        self.tcp_timer_expire(kind);
        let Some((timer, segment)) = self.on_tcp_timer(kind) else {
            return Ok(());
        };
        emit_segment(runtime, context.buffers(), output_next, output, segment)?;
        if let Some(ticks) = self.tcp_timer_ticks(timer)
            && let Some(token) = timer.session_timer_token()
        {
            context.arm_timer_ticks(expiry.session_id(), token, ticks)?;
        }
        Ok(())
    }

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        close_requested: bool,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        if close_requested {
            *self = self.clone().on_session_close();
        }
        if matches!(self, Self::Established(_)) {
            context.flush_session_rx(context.session_id())?;
        }
        if let Some(segment) = self.on_tcp_ready() {
            emit_segment(runtime, context.buffers(), output_next, output, segment)?;
        }
        Ok(())
    }

    fn tx_payload_len(
        &mut self,
        _: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<usize> {
        Ok(match self {
            Self::Established(connection) => connection.tx_payload_len(pending_len, now),
            _ => 0,
        })
    }

    fn prepare_tx(
        &mut self,
        context: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()> {
        let Self::Established(connection) = self else {
            return Err(CoreError::internal(
                "tcp tx prepare requires established connection",
            ));
        };
        let budget = connection.tx_payload_len(payload_len, now);
        if payload_len > budget {
            return Err(CoreError::internal("tcp tx payload exceeds send budget"));
        }
        let segment = connection.tx_segment(payload_len)?;
        write_segment(context.buffers(), index, &segment)?;
        Ok(())
    }

    fn cancel_tx(&mut self, _: &mut TcpWorkerOwnedState, _: BufferIndex) {}

    fn commit_tx(
        &mut self,
        context: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        _: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()> {
        let Self::Established(connection) = self else {
            return Err(CoreError::internal("tcp tx commit state is missing"));
        };
        let timers = connection.commit_payload_tx(payload_len, now)?;
        for timer in [TcpConnectionTimerKind::RACK, TcpConnectionTimerKind::TLP] {
            if !timers.contains(timer) {
                continue;
            }
            if let Some(ticks) = connection.tcp_timer_ticks(timer)
                && let Some(token) = timer.session_timer_token()
            {
                context.arm_timer_ticks(context.session_id(), token, ticks)?;
            }
        }
        Ok(())
    }
}

fn write_segment(buffers: &DataPlaneBuffers, index: BufferIndex, segment: &TcpSegment) -> CoreResult<()> {
    let mut buffer = buffers.get_buffer_mut(index)?;
    buffer.opaque_mut().write(segment);
    buffer.opaque2_mut().write(segment);
    Ok(())
}

fn emit_segment(
    runtime: &DataPlaneRuntime,
    buffers: &DataPlaneBuffers,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
    segment: TcpSegment,
) -> CoreResult<()> {
    let index = buffers.alloc_index(Default::default())?;
    if let Err(error) = write_segment(buffers, index, &segment) {
        buffers.free_index(index);
        return Err(error);
    }
    if let Err(error) = output.enqueue(runtime, output_next.node(), index) {
        buffers.free_index(index);
        return Err(error);
    }
    Ok(())
}

fn release_acked_tx<C>(
    queue: &mut SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
    session_id: SessionId,
    previous_snd_una: u32,
    snd_una: u32,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let acked = hammer_core::protocol::tcp::TcpSeq::from(previous_snd_una)
        .distance_to(hammer_core::protocol::tcp::TcpSeq::from(snd_una));
    if acked != 0 {
        queue.release_tx_up_to(session_id, acked as usize)?;
    }
    Ok(())
}

impl<C> TcpConnection<Listen, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_syn(
        self,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        if !packet.flags.contains(hammer_core::protocol::tcp::TcpSegmentFlags::SYN)
            || packet.flags.intersects(
                hammer_core::protocol::tcp::TcpSegmentFlags::ACK
                    | hammer_core::protocol::tcp::TcpSegmentFlags::RST,
            )
        {
            return Ok((self.into(), None));
        }
        let (connection, segment) = self.accept_syn(packet);
        Ok((connection.into(), Some(segment)))
    }
}

impl<C> TcpConnection<SynSent, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_open_reply(
        mut self,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && self.unacceptable_ack(acknowledgment)
        {
            if packet.flags.contains(TcpSegmentFlags::RST) {
                return Ok((self.into(), None));
            }
            let segment = self.control_segment(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            return Ok((self.into(), Some(segment)));
        }

        if packet.flags.contains(TcpSegmentFlags::RST) {
            if let Some(acknowledgment) = packet.acknowledgment
                && self.accepts_segment_ack(acknowledgment)
            {
                let mut connection = self.close_remote_reset();
                connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
                return Ok((connection.into(), None));
            }
            return Ok((self.into(), None));
        }

        if !packet.flags.contains(TcpSegmentFlags::SYN) {
            return Ok((self.into(), None));
        }

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            if !self.accepts_segment_ack(acknowledgment) {
                return Ok((self.into(), None));
            }
            let (mut connection, segment) = self.accept_syn_ack(packet, acknowledgment);
            connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
            return Ok((connection.into(), Some(segment)));
        }

        let (connection, segment) = self.accept_simultaneous_syn(packet);
        Ok((connection.into(), Some(segment)))
    }
}

impl<C> TcpConnection<SynRcvd, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_final_ack(
        mut self,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        let Some(acknowledgment) = packet.acknowledgment else {
            return Ok((self.into(), None));
        };
        if !packet.flags.contains(TcpSegmentFlags::ACK) || !self.accepts_final_ack(acknowledgment) {
            let segment = self.control_segment(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            return Ok((self.into(), Some(segment)));
        }
        let (mut connection, segment) = self.accept_final_ack(packet, acknowledgment);
        connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
        Ok((connection.into(), Some(segment)))
    }
}

impl<C> TcpConnection<Established, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_data(
        mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        queue: &mut SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            if packet.payload_len != 0 {
                runtime.free_index(index);
            }
            return Ok((self.close_remote_reset().into(), None));
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            self.receive_ack(
                acknowledgment,
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
            release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
        }

        let mut ack = false;
        if packet.payload_len != 0 {
            ack = true;
            runtime.packet_buffers().advance(index, packet.payload_offset)?;
            if self.accept_payload(packet).is_some() {
                runtime.truncate_chain(index, packet.payload_len)?;
                let _ = queue.enqueue_rx(session_id, index, false)?;
                self.update_ack_sack_blocks(&[], None);
            } else {
                runtime.free_index(index);
            }
        }

        if packet.flags.contains(TcpSegmentFlags::FIN)
            && packet.sequence.wrapping_add(packet.payload_len as u32) == self.rcv_nxt()
        {
            let (connection, segment) = self.accept_fin(packet);
            return Ok((connection.into(), Some(segment)));
        }

        let segment = ack.then(|| self.control_segment(packet, TcpSegmentFlags::ACK, None));
        Ok((self.into(), segment))
    }
}

impl<C> TcpConnection<CloseWait, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_close_wait(
        mut self,
        queue: &mut SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            self.receive_ack(
                acknowledgment,
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
            release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let (connection, segment) = self.accept_repeated_fin(packet);
            return Ok((connection.into(), Some(segment)));
        }
        Ok((self.into(), None))
    }
}

impl<C> TcpConnection<FinWait1, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_fin_wait1(
        self,
        queue: &mut SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        let fin = packet.flags.contains(TcpSegmentFlags::FIN);
        let ack = packet
            .acknowledgment
            .filter(|_| packet.flags.contains(TcpSegmentFlags::ACK));
        match (fin, ack) {
            (true, Some(acknowledgment)) => {
                let previous_snd_una = self.snd_una();
                let (connection, segment) = self.accept_fin_ack(packet, acknowledgment);
                release_acked_tx(queue, session_id, previous_snd_una, connection.snd_una())?;
                Ok((connection.into(), Some(segment)))
            }
            (false, Some(acknowledgment)) => {
                let previous_snd_una = self.snd_una();
                let connection = self.accept_ack(packet, acknowledgment);
                release_acked_tx(queue, session_id, previous_snd_una, connection.snd_una())?;
                Ok((connection.into(), None))
            }
            (true, _) => {
                let (connection, segment) = self.accept_fin(packet);
                Ok((connection.into(), Some(segment)))
            }
            _ => Ok((self.into(), None)),
        }
    }
}

impl<C> TcpConnection<FinWait2, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_fin_wait2(
        self,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let (connection, segment) = self.accept_fin(packet);
            return Ok((connection.into(), Some(segment)));
        }
        Ok((self.into(), None))
    }
}

impl<C> TcpConnection<Closing, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_closing(
        self,
        queue: &mut SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            let connection = self.accept_ack(packet, acknowledgment);
            release_acked_tx(queue, session_id, previous_snd_una, connection.snd_una())?;
            return Ok((connection.into(), None));
        }
        Ok((self.into(), None))
    }
}

impl<C> TcpConnection<LastAck, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_last_ack(
        self,
        queue: &mut SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            let connection = self.accept_ack(packet, acknowledgment);
            release_acked_tx(queue, session_id, previous_snd_una, connection.snd_una())?;
            return Ok((connection.into(), None));
        }
        Ok((self.into(), None))
    }
}

impl<C> TcpConnection<TimeWait, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_time_wait(
        self,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<(TcpConnectionState<C>, Option<TcpSegment>)> {
        use hammer_core::protocol::tcp::TcpSegmentFlags;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            return Ok((self.close_remote_reset().into(), None));
        }
        Ok((self.into(), None))
    }
}

macro_rules! impl_connection_storage {
    ($state_type:ty, $variant:ident) => {
        impl<C> TryFrom<TcpConnectionState<C>> for TcpConnection<$state_type, C>
        where
            C: CongestionController,
        {
            type Error = CoreError;

            #[inline]
            fn try_from(state: TcpConnectionState<C>) -> Result<Self, Self::Error> {
                match state {
                    TcpConnectionState::$variant(connection) => Ok(connection),
                    _ => Err(CoreError::internal(concat!(
                        "tcp connection state mismatch: expected ",
                        stringify!($variant)
                    ))),
                }
            }
        }

        impl<C> From<TcpConnection<$state_type, C>> for TcpConnectionState<C>
        where
            C: CongestionController,
        {
            #[inline]
            fn from(connection: TcpConnection<$state_type, C>) -> Self {
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
