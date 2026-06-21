use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpNegotiatedOptions, TcpSackBlock,
    TcpSegmentFlags, TcpSeq, TcpState,
};

use crate::session::{
    SessionId, SessionQueueNext, SessionTimerExpiry, SessionTimerToken,
    node::SessionQueueOutput,
    protocol::SessionQueueControlContext,
    runtime::{SessionDriverRuntime, SessionQueueProtocol},
};
use crate::transport::congestion::CongestionController;

use super::lookup::TcpWorkerOwnedState;
use super::output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, tcp_effective_output_payload_len};
use super::recovery::{TcpRecoveryAck, TcpRecoveryState};
use super::segment::{TcpPacket, TcpSegment};
use super::TcpInputNext;

const TCP_MAX_WINDOW_SCALE: u8 = 14;
const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

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
pub struct TcpConnection<C>
where
    C: CongestionController,
{
    state: TcpState,
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
    sack_blocks: [TcpSackBlock; 4],
    sack_block_count: u8,
    pending_dsack: Option<TcpSackBlock>,
    active_timers: TcpConnectionTimerKind,
    pending_timers: TcpConnectionTimerKind,
}

impl<C> TcpConnection<C>
where
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
            state: TcpState::Closed,
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
            sack_blocks: [TcpSackBlock {
                left_edge: 0,
                right_edge: 0,
            }; 4],
            sack_block_count: 0,
            pending_dsack: None,
            active_timers: TcpConnectionTimerKind::empty(),
            pending_timers: TcpConnectionTimerKind::empty(),
        }
    }

    #[inline]
    pub const fn state(&self) -> TcpState {
        self.state
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

    #[inline]
    pub const fn next_node(&self) -> TcpInputNext {
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
        !TcpSeq::from(acknowledgment).before(TcpSeq::from(self.snd_una))
            && !TcpSeq::from(acknowledgment).after(TcpSeq::from(self.snd_nxt))
    }

    #[inline]
    fn apply_ack(&mut self, acknowledgment: u32, advertised_window: u16) {
        if self.accepts_ack(acknowledgment) {
            self.snd_una = acknowledgment;
        }
        self.snd_wnd = self.effective_send_window(u32::from(advertised_window));
    }

    fn recovery_ack(&self, acknowledgment: u32) -> TcpRecoveryAck {
        let now = Instant::now();
        let latest_rtt = self
            .retransmit_timeout
            .smoothed_rtt()
            .unwrap_or(TCP_INITIAL_RETRANSMIT_TIMEOUT);
        let min_rtt = latest_rtt;
        TcpRecoveryAck {
            acknowledgment,
            now,
            latest_rtt,
            min_rtt,
            app_limited: false,
            ecn_ce: false,
        }
    }

    fn observe_ack_progress(&mut self, acknowledgment: u32, sack_blocks: &[TcpSackBlock]) {
        if !self.accepts_ack(acknowledgment) {
            return;
        }
        let advanced = TcpSeq::from(acknowledgment).after(TcpSeq::from(self.snd_una));
        let recovery_ack = self.recovery_ack(acknowledgment);
        let latest_rtt = recovery_ack.latest_rtt;
        if advanced {
            if sack_blocks.is_empty() {
                self.recovery.on_ack(recovery_ack, &mut self.congestion);
            } else {
                self.recovery
                    .on_sack_blocks(recovery_ack, sack_blocks, &mut self.congestion);
            }
        } else if !sack_blocks.is_empty() {
            self.recovery
                .on_sack_blocks(recovery_ack, sack_blocks, &mut self.congestion);
        }
        if advanced {
            self.retransmit_timeout.observe_ack_sample(latest_rtt);
        }
    }

    #[inline]
    fn receive_in_order(&mut self, sequence: u32, payload_len: usize) -> bool {
        if sequence != self.rcv_nxt {
            return false;
        }
        self.rcv_nxt = TcpSeq::from(self.rcv_nxt).advance(payload_len as u32).raw();
        true
    }

    fn update_ack_sack_block(&mut self, left_edge: u32, right_edge: u32) {
        if !self.negotiated_options().sack {
            self.sack_block_count = 0;
            return;
        }

        let mut blocks = [TcpSackBlock {
            left_edge: 0,
            right_edge: 0,
        }; 4];
        let mut count = 0usize;
        let mut current = TcpSackBlock {
            left_edge,
            right_edge,
        };

        if TcpSeq::from(self.rcv_nxt).before(TcpSeq::from(left_edge)) {
            blocks[count] = current;
            count += 1;
        }

        for block in self.sack_blocks.iter().take(self.sack_block_count as usize) {
            if !TcpSeq::from(self.rcv_nxt).before(TcpSeq::from(block.left_edge)) {
                continue;
            }
            if count != 0
                && sack_blocks_overlap_or_touch(current, *block)
            {
                if TcpSeq::from(block.left_edge).before(TcpSeq::from(current.left_edge)) {
                    current.left_edge = block.left_edge;
                }
                if TcpSeq::from(current.right_edge).before(TcpSeq::from(block.right_edge)) {
                    current.right_edge = block.right_edge;
                }
                blocks[0] = current;
                continue;
            }
            if count == blocks.len() {
                break;
            }
            blocks[count] = *block;
            count += 1;
        }

        self.sack_block_count = count as u8;
        for (slot, block) in self
            .sack_blocks
            .iter_mut()
            .zip(blocks.into_iter())
        {
            *slot = block;
        }
    }

    #[inline]
    pub(crate) fn update_ack_sack_blocks(
        &mut self,
        sack_blocks: &[TcpSackBlock],
        dsack: Option<TcpSackBlock>,
    ) {
        if !self.negotiated_options().sack {
            self.sack_block_count = 0;
            self.pending_dsack = None;
            return;
        }
        self.sack_block_count = 0;
        for block in sack_blocks.iter().take(self.sack_blocks.len()) {
            self.sack_blocks[self.sack_block_count as usize] = *block;
            self.sack_block_count += 1;
        }
        self.pending_dsack = dsack;
    }

    #[inline]
    fn set_duplicate_sack(&mut self, left_edge: u32, right_edge: u32) {
        if !self.negotiated_options().sack {
            self.pending_dsack = None;
            return;
        }
        self.pending_dsack = Some(TcpSackBlock {
            left_edge,
            right_edge,
        });
    }

    fn output_sack_blocks(&mut self, flags: TcpSegmentFlags) -> Option<([TcpSackBlock; 4], usize)> {
        if !flags.contains(TcpSegmentFlags::ACK) || !self.negotiated_options().sack {
            return None;
        }
        let mut blocks = [TcpSackBlock {
            left_edge: 0,
            right_edge: 0,
        }; 4];
        let mut count = 0usize;
        if let Some(dsack) = self.pending_dsack {
            blocks[count] = dsack;
            count += 1;
        }
        for block in self
            .sack_blocks
            .iter()
            .take(self.sack_block_count as usize)
            .take(blocks.len().saturating_sub(count))
        {
            blocks[count] = *block;
            count += 1;
        }
        self.pending_dsack = None;
        (count != 0).then_some((blocks, count))
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
        &mut self,
        packet: &TcpPacket,
        flags: TcpSegmentFlags,
        reset_sequence: Option<u32>,
    ) -> TcpSegment {
        let flags_bits = flags.bits();
        let sack_blocks = self.output_sack_blocks(flags);
        TcpSegment::new(
            packet.local,
            packet.remote,
            reset_sequence.unwrap_or_else(|| self.output_sequence(flags_bits)),
            self.output_acknowledgment(flags_bits),
            self.advertised_receive_window(self.rcv_wnd),
            flags,
            self.options.local_capabilities(),
            sack_blocks.as_ref().map(|(blocks, len)| &blocks[..*len]),
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
            TcpSeq::from(self.iss).advance(1).raw()
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
            TcpSeq::from(self.irs).advance(1).raw()
        } else {
            1
        }
    }

    #[inline]
    fn ensure_state(&self, state: TcpState, message: &'static str) -> CoreResult<()> {
        if self.state != state {
            return Err(CoreError::internal(message));
        }
        Ok(())
    }

    #[inline]
    pub fn connect_state(&mut self, initial_sequence: u32) {
        self.close_reason = None;
        self.iss = initial_sequence;
        self.snd_una = initial_sequence;
        self.snd_nxt = TcpSeq::from(initial_sequence).advance(1).raw();
        self.state = TcpState::SynSent;
    }

    pub(crate) fn receive_syn(&mut self, packet: &TcpPacket) -> CoreResult<Option<TcpSegment>> {
        if !packet.flags.contains(TcpSegmentFlags::SYN)
            || packet
                .flags
                .intersects(TcpSegmentFlags::ACK | TcpSegmentFlags::RST)
        {
            return Ok(None);
        }
        self.apply_peer_handshake_capabilities(packet.capabilities);
        if self.iss == 0 {
            self.iss = 1;
        }
        self.state = TcpState::SynRcvd;
        self.irs = packet.sequence;
        self.snd_una = self.iss;
        self.snd_nxt = TcpSeq::from(self.iss).advance(1).raw();
        self.snd_wnd = self.effective_send_window(u32::from(packet.advertised_window));
        self.rcv_nxt = TcpSeq::from(packet.sequence).advance(1).raw();
        Ok(Some(
            self.control_segment(packet, TcpSegmentFlags::SYN | TcpSegmentFlags::ACK, None),
        ))
    }

    pub(crate) fn receive_open_reply(
        &mut self,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::SynSent, "tcp open reply requires syn-sent state")?;

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && (!TcpSeq::from(acknowledgment).after(TcpSeq::from(self.iss))
                || TcpSeq::from(acknowledgment).after(TcpSeq::from(self.snd_nxt)))
        {
            if packet.flags.contains(TcpSegmentFlags::RST) {
                return Ok(None);
            }
            return Ok(Some(self.control_segment(
                packet,
                TcpSegmentFlags::RST,
                Some(acknowledgment),
            )));
        }

        if packet.flags.contains(TcpSegmentFlags::RST) {
            if let Some(acknowledgment) = packet.acknowledgment && self.accepts_ack(acknowledgment)
            {
                self.close_reason = Some(TcpCloseReason::RemoteReset);
                self.state = TcpState::Closed;
                self.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
            }
            return Ok(None);
        }

        if !packet.flags.contains(TcpSegmentFlags::SYN) {
            return Ok(None);
        }

        self.apply_peer_handshake_capabilities(packet.capabilities);
        self.irs = packet.sequence;
        self.snd_wnd = self.effective_send_window(u32::from(packet.advertised_window));
        self.rcv_nxt = TcpSeq::from(packet.sequence).advance(1).raw();

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            if !self.accepts_ack(acknowledgment) {
                return Ok(None);
            }
            self.snd_una = acknowledgment;
            self.state = TcpState::Established;
            self.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
            return Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)));
        }

        self.state = TcpState::SynRcvd;
        Ok(Some(
            self.control_segment(packet, TcpSegmentFlags::SYN | TcpSegmentFlags::ACK, None),
        ))
    }

    fn receive_final_ack(&mut self, packet: &TcpPacket) -> CoreResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::SynRcvd, "tcp final ack requires syn-rcvd state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        let Some(acknowledgment) = packet.acknowledgment else {
            return Ok(None);
        };
        if !packet.flags.contains(TcpSegmentFlags::ACK) || !self.accepts_ack(acknowledgment) {
            return Ok(Some(self.control_segment(
                packet,
                TcpSegmentFlags::RST,
                Some(acknowledgment),
            )));
        }
        self.apply_ack(acknowledgment, packet.advertised_window);
        self.state = TcpState::Established;
        self.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
        Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)))
    }

    #[inline]
    pub(crate) fn tx_payload_budget(&self, pending_len: usize, _: Instant) -> usize {
        if self.state != TcpState::Established || pending_len == 0 {
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

    pub(crate) fn tx_segment(&mut self, payload_len: usize) -> CoreResult<TcpSegment> {
        self.ensure_state(TcpState::Established, "tcp tx segment requires established state")?;
        let local = self
            .local()
            .ok_or_else(|| CoreError::internal("established tcp connection missing local address"))?;
        let sack_blocks = self.output_sack_blocks(TcpSegmentFlags::ACK | TcpSegmentFlags::PSH);
        Ok(TcpSegment::new(
            local,
            self.remote(),
            self.snd_nxt(),
            self.rcv_nxt(),
            self.advertised_receive_window(self.rcv_wnd),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            self.local_capabilities(),
            sack_blocks.as_ref().map(|(blocks, len)| &blocks[..*len]),
            payload_len,
        ))
    }

    pub(crate) fn commit_payload_tx(
        &mut self,
        payload: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<TcpConnectionTimerKind> {
        self.ensure_state(TcpState::Established, "tcp tx commit requires established state")?;
        let payload_len = u32::try_from(payload_len)
            .map_err(|_| CoreError::internal("tcp payload length exceeds u32"))?;
        let sequence = self.snd_nxt;
        let end_sequence = TcpSeq::from(sequence).advance(payload_len).raw();
        let bytes_in_flight = self.recovery.bytes_in_flight();
        let packet_number = self.recovery.next_packet_number();
        self.recovery.record_sent(
            packet_number,
            sequence,
            end_sequence,
            payload_len,
            Some(payload),
            now,
            false,
            false,
        );
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
    pub(crate) fn receive_ack(
        &mut self,
        acknowledgment: u32,
        advertised_window: u16,
        sack_blocks: &[TcpSackBlock],
    ) -> TcpConnectionTimerKind {
        self.observe_ack_progress(acknowledgment, sack_blocks);
        self.apply_ack(acknowledgment, advertised_window);
        let mut timers = TcpConnectionTimerKind::empty();
        if self.recovery.rack_timeout_ticks().is_some() {
            timers.insert(TcpConnectionTimerKind::RACK);
        }
        if self.recovery.tlp_timeout_ticks().is_some() {
            timers.insert(TcpConnectionTimerKind::TLP);
        }
        timers
    }

    #[inline]
    pub(crate) fn accept_payload(&mut self, packet: &TcpPacket) -> Option<usize> {
        if packet.payload_len == 0 {
            return None;
        }
        self.receive_in_order(packet.sequence, packet.payload_len)
            .then_some(packet.payload_len)
    }

    fn payload_offset_from_rcv_nxt(&self, sequence: u32) -> Option<u32> {
        let sequence = TcpSeq::from(sequence);
        let rcv_nxt = TcpSeq::from(self.rcv_nxt);
        if sequence.before(rcv_nxt) {
            return None;
        }
        Some(rcv_nxt.distance_to(sequence))
    }

    pub(crate) fn receive_data(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: 'static,
    {
        self.ensure_state(TcpState::Established, "tcp data receive requires established state")?;

        if packet.flags.contains(TcpSegmentFlags::RST) {
            if packet.payload_len != 0 {
                runtime.free_index(index);
            }
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            let timers = self.receive_ack(
                acknowledgment,
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
            sync_recovery_timers(queue, session_id, timers, self)?;
            release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
        }

        let mut ack = false;
        if packet.payload_len != 0 {
            ack = true;
            runtime.packet_buffers().advance(index, packet.payload_offset)?;
            runtime.packet_buffers().truncate_chain(index, packet.payload_len)?;
            if self.accept_payload(packet).is_some() {
                let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
                if enqueue.delivered_len != 0 {
                    self.rcv_nxt = TcpSeq::from(packet.sequence)
                        .advance(enqueue.delivered_len)
                        .raw();
                }
                if let Some(start) = enqueue.newest_ooo_start {
                    let left_edge = TcpSeq::from(self.rcv_nxt).advance(start).raw();
                    let right_edge = TcpSeq::from(left_edge).advance(enqueue.newest_ooo_len).raw();
                    self.update_ack_sack_block(left_edge, right_edge);
                } else {
                    self.update_ack_sack_blocks(&[], None);
                }
            } else if let Some(offset) = self.payload_offset_from_rcv_nxt(packet.sequence) {
                let enqueue = queue.enqueue_rx(session_id, index, offset, false)?;
                if let Some(start) = enqueue.newest_ooo_start {
                    let left_edge = TcpSeq::from(self.rcv_nxt).advance(start).raw();
                    let right_edge = TcpSeq::from(left_edge).advance(enqueue.newest_ooo_len).raw();
                    self.update_ack_sack_block(left_edge, right_edge);
                }
            } else {
                let right_edge = packet.sequence.wrapping_add(packet.payload_len as u32);
                self.set_duplicate_sack(packet.sequence, right_edge);
                runtime.free_index(index);
            }
        }

        if packet.flags.contains(TcpSegmentFlags::FIN)
            && packet.sequence.wrapping_add(packet.payload_len as u32) == self.rcv_nxt()
        {
            let fin_sequence = packet.sequence.wrapping_add(packet.payload_len as u32);
            self.receive_in_order(fin_sequence, 1);
            self.close_reason = Some(TcpCloseReason::RemoteFin);
            self.state = TcpState::CloseWait;
            return Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)));
        }

        Ok(ack.then(|| self.control_segment(packet, TcpSegmentFlags::ACK, None)))
    }

    fn receive_close_wait(
        &mut self,
        queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: 'static,
    {
        self.ensure_state(TcpState::CloseWait, "tcp close-wait receive requires close-wait state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            let timers = self.receive_ack(
                acknowledgment,
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
            sync_recovery_timers(queue, session_id, timers, self)?;
            release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            self.receive_in_order(packet.sequence, 1);
            return Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)));
        }
        Ok(None)
    }

    fn receive_fin_wait1(
        &mut self,
        queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: 'static,
    {
        self.ensure_state(TcpState::FinWait1, "tcp fin-wait1 receive requires fin-wait1 state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        let fin = packet.flags.contains(TcpSegmentFlags::FIN);
        let ack = packet
            .acknowledgment
            .filter(|_| packet.flags.contains(TcpSegmentFlags::ACK));
        match (fin, ack) {
            (true, Some(acknowledgment)) => {
                let previous_snd_una = self.snd_una();
                self.apply_ack(acknowledgment, packet.advertised_window);
                self.receive_in_order(packet.sequence, 1);
                self.state = TcpState::TimeWait;
                release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
                Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)))
            }
            (false, Some(acknowledgment)) => {
                let previous_snd_una = self.snd_una();
                self.apply_ack(acknowledgment, packet.advertised_window);
                self.state = TcpState::FinWait2;
                release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
                Ok(None)
            }
            (true, _) => {
                self.apply_ack(self.snd_una, packet.advertised_window);
                self.receive_in_order(packet.sequence, 1);
                self.state = TcpState::Closing;
                Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)))
            }
            _ => Ok(None),
        }
    }

    fn receive_fin_wait2(&mut self, packet: &TcpPacket) -> CoreResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::FinWait2, "tcp fin-wait2 receive requires fin-wait2 state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            self.apply_ack(self.snd_una, packet.advertised_window);
            self.receive_in_order(packet.sequence, 1);
            self.state = TcpState::TimeWait;
            return Ok(Some(self.control_segment(packet, TcpSegmentFlags::ACK, None)));
        }
        Ok(None)
    }

    fn receive_closing(
        &mut self,
        queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: 'static,
    {
        self.ensure_state(TcpState::Closing, "tcp closing receive requires closing state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            self.apply_ack(acknowledgment, packet.advertised_window);
            self.state = TcpState::TimeWait;
            release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
        }
        Ok(None)
    }

    fn receive_last_ack(
        &mut self,
        queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: 'static,
    {
        self.ensure_state(TcpState::LastAck, "tcp last-ack receive requires last-ack state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let previous_snd_una = self.snd_una();
            self.apply_ack(acknowledgment, packet.advertised_window);
            self.close_reason = Some(TcpCloseReason::LocalRequest);
            self.state = TcpState::Closed;
            release_acked_tx(queue, session_id, previous_snd_una, self.snd_una())?;
        }
        Ok(None)
    }

    fn receive_time_wait(&mut self, packet: &TcpPacket) -> CoreResult<Option<TcpSegment>> {
        self.ensure_state(TcpState::TimeWait, "tcp time-wait receive requires time-wait state")?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            self.close_reason = Some(TcpCloseReason::RemoteReset);
            self.state = TcpState::Closed;
        }
        Ok(None)
    }

    pub(crate) fn receive_rcv_process(
        &mut self,
        queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
        session_id: SessionId,
        packet: &TcpPacket,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: 'static,
    {
        match self.state {
            TcpState::SynRcvd => self.receive_final_ack(packet),
            TcpState::CloseWait => self.receive_close_wait(queue, session_id, packet),
            TcpState::FinWait1 => self.receive_fin_wait1(queue, session_id, packet),
            TcpState::FinWait2 => self.receive_fin_wait2(packet),
            TcpState::Closing => self.receive_closing(queue, session_id, packet),
            TcpState::LastAck => self.receive_last_ack(queue, session_id, packet),
            TcpState::TimeWait => self.receive_time_wait(packet),
            _ => Err(CoreError::internal("tcp rcv-process state is invalid")),
        }
    }

    pub(crate) fn on_tcp_timer(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<(TcpConnectionTimerKind, TcpSegment)> {
        if self.state == TcpState::SynSent {
            let segment = self.on_tcp_timer_expiry(timer)?;
            return Some((timer, segment));
        }
        self.tcp_timer_take_pending(timer);
        None
    }

    #[inline]
    pub(crate) fn on_tcp_ready(&mut self) -> Option<TcpSegment> {
        if self.state == TcpState::SynSent {
            return self.on_tcp_timer_expiry(TcpConnectionTimerKind::RETRANSMIT);
        }
        None
    }

    #[inline]
    pub(crate) fn on_session_close(&mut self) {
        match self.state {
            TcpState::Established => {
                self.close_reason = Some(TcpCloseReason::LocalRequest);
                self.state = TcpState::FinWait1;
            }
            TcpState::CloseWait => {
                self.close_reason = Some(TcpCloseReason::LocalRequest);
                self.state = TcpState::LastAck;
            }
            _ => {}
        }
    }

    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<TcpSegment> {
        if self.state != TcpState::SynSent || timer != TcpConnectionTimerKind::RETRANSMIT {
            return None;
        }
        let retransmit = self.tcp_timer_dispatch_pending(timer);
        let first_syn = !self.tcp_timer_is_active(timer)
            && self.snd_una == self.iss
            && self.snd_nxt == TcpSeq::from(self.iss).advance(1).raw();
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
            None,
            0,
        ))
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
            sequence: 7_000,
            acknowledgment: None,
            advertised_window: u16::MAX,
            payload_offset: 0,
            payload_len: 0,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new(),
        };
        let _ = connection.receive_syn(&packet).expect("accept syn");
        let final_packet = TcpPacket {
            acknowledgment: Some(connection.snd_nxt()),
            flags: TcpSegmentFlags::ACK,
            ..packet
        };
        let _ = connection
            .receive_final_ack(&final_packet)
            .expect("accept final ack");
        connection
    }
}

impl<C> SessionQueueProtocol<TcpWorkerOwnedState> for TcpConnection<C>
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
        if self.state == TcpState::Established {
            match kind {
                TcpConnectionTimerKind::RACK => {
                    self.recovery.on_rack_timeout(Instant::now(), &mut self.congestion);
                    if let Some((sequence, bytes, payload)) = self.recovery.take_rack_retransmit() {
                        emit_retained_segment(
                            runtime,
                            context.buffers(),
                            output_next,
                            output,
                            self,
                            sequence,
                            bytes,
                            payload,
                        )?;
                        if let Some(ticks) = self.tcp_timer_ticks(kind)
                            && let Some(token) = kind.session_timer_token()
                        {
                            context.arm_timer_ticks(expiry.session_id(), token, ticks)?;
                        }
                        return Ok(());
                    }
                }
                TcpConnectionTimerKind::TLP => {
                    if let Some((_, sequence, bytes, payload)) = self.recovery.take_tlp_probe() {
                        emit_retained_segment(
                            runtime,
                            context.buffers(),
                            output_next,
                            output,
                            self,
                            sequence,
                            bytes,
                            payload,
                        )?;
                        if let Some(ticks) = self.tcp_timer_ticks(kind)
                            && let Some(token) = kind.session_timer_token()
                        {
                            context.arm_timer_ticks(expiry.session_id(), token, ticks)?;
                        }
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
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
            self.on_session_close();
        }
        if self.state == TcpState::Established {
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
        Ok(self.tx_payload_budget(pending_len, now))
    }

    fn prepare_tx(
        &mut self,
        context: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()> {
        let budget = self.tx_payload_budget(payload_len, now);
        if payload_len > budget {
            return Err(CoreError::internal("tcp tx payload exceeds send budget"));
        }
        let segment = self.tx_segment(payload_len)?;
        write_segment(context.buffers(), index, &segment)?;
        Ok(())
    }

    fn cancel_tx(&mut self, _: &mut TcpWorkerOwnedState, _: BufferIndex) {}

    fn commit_tx(
        &mut self,
        context: &mut SessionQueueControlContext<'_, TcpWorkerOwnedState>,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()> {
        let payload = context
            .buffers()
            .get_buffer_mut(index)?
            .next_buffer()
            .ok_or_else(|| CoreError::internal("tcp output head is missing retained payload"))?;
        let timers = self.commit_payload_tx(payload, payload_len, now)?;
        for timer in [TcpConnectionTimerKind::RACK, TcpConnectionTimerKind::TLP] {
            if !timers.contains(timer) {
                continue;
            }
            if let Some(ticks) = self.tcp_timer_ticks(timer)
                && let Some(token) = timer.session_timer_token()
            {
                context.arm_timer_ticks(context.session_id(), token, ticks)?;
            }
        }
        Ok(())
    }
}

fn timer_segment_header<C>(
    connection: &mut TcpConnection<C>,
    sequence: u32,
    bytes: u32,
) -> CoreResult<TcpSegment>
where
    C: CongestionController + 'static,
{
    let local = connection
        .local()
        .ok_or_else(|| CoreError::internal("established tcp connection missing local address"))?;
    let sack_blocks = connection.output_sack_blocks(TcpSegmentFlags::ACK | TcpSegmentFlags::PSH);
    Ok(TcpSegment::new(
        local,
        connection.remote(),
        sequence,
        connection.rcv_nxt(),
        connection.advertised_receive_window(connection.rcv_wnd),
        TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
        connection.local_capabilities(),
        sack_blocks.as_ref().map(|(blocks, len)| &blocks[..*len]),
        bytes as usize,
    ))
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

fn emit_retained_segment<C>(
    runtime: &DataPlaneRuntime,
    buffers: &DataPlaneBuffers,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
    connection: &mut TcpConnection<C>,
    sequence: u32,
    bytes: u32,
    payload: Option<BufferIndex>,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let header = timer_segment_header(connection, sequence, bytes)?;
    let payload = payload
        .ok_or_else(|| CoreError::internal("tcp timer segment is missing retained payload"))?;
    let index = buffers.alloc_index(Default::default())?;
    if let Err(error) = buffers.attach_clone(index, payload) {
        buffers.free_index(index);
        return Err(error);
    }
    if let Err(error) = write_segment(buffers, index, &header) {
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
    queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
    session_id: SessionId,
    previous_snd_una: u32,
    snd_una: u32,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let acked = TcpSeq::from(previous_snd_una).distance_to(TcpSeq::from(snd_una));
    if acked != 0 {
        queue.release_tx_up_to(session_id, acked as usize)?;
    }
    Ok(())
}

fn sync_recovery_timers<C>(
    queue: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
    session_id: SessionId,
    timers: TcpConnectionTimerKind,
    connection: &TcpConnection<C>,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    for timer in [TcpConnectionTimerKind::RACK, TcpConnectionTimerKind::TLP] {
        let Some(token) = timer.session_timer_token() else {
            continue;
        };
        if timers.contains(timer) {
            if let Some(ticks) = connection.tcp_timer_ticks(timer) {
                queue.arm_timer_ticks(session_id, token, ticks)?;
            }
            continue;
        }
        let _ = queue.cancel_timer(session_id, token);
    }
    Ok(())
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

#[inline]
fn sack_blocks_overlap_or_touch(left: TcpSackBlock, right: TcpSackBlock) -> bool {
    !TcpSeq::from(left.right_edge).before(TcpSeq::from(right.left_edge))
        && !TcpSeq::from(right.right_edge).before(TcpSeq::from(left.left_edge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::congestion::BbrController;

    fn established_connection() -> TcpConnection<BbrController> {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let mut connection = TcpConnection::established_for_test(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        let _ = connection.set_local_capabilities(TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: false,
        });
        let _ = connection.apply_peer_handshake_capabilities(TcpCapabilities {
            max_segment_size: None,
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: false,
        });
        connection
    }

    #[test]
    fn tcp_update_sack_block_merges_and_drops_delivered_ranges() {
        let mut connection = established_connection();
        connection.rcv_nxt = 1_000;

        connection.update_ack_sack_block(1_020, 1_030);
        connection.update_ack_sack_block(1_040, 1_050);
        connection.update_ack_sack_block(1_028, 1_045);

        assert_eq!(connection.sack_block_count, 1);
        assert_eq!(
            connection.sack_blocks[0],
            TcpSackBlock {
                left_edge: 1_020,
                right_edge: 1_050,
            }
        );

        connection.rcv_nxt = 1_050;
        connection.update_ack_sack_block(connection.rcv_nxt, connection.rcv_nxt);

        assert_eq!(connection.sack_block_count, 0);
    }

    #[test]
    fn tcp_output_sack_blocks_emits_pending_dsack_first() {
        let mut connection = established_connection();
        connection.update_ack_sack_block(8_000, 8_100);
        connection.set_duplicate_sack(6_500, 6_550);

        let (blocks, count) = connection
            .output_sack_blocks(TcpSegmentFlags::ACK)
            .expect("sack blocks");

        assert_eq!(count, 2);
        assert_eq!(
            blocks[0],
            TcpSackBlock {
                left_edge: 6_500,
                right_edge: 6_550,
            }
        );
        assert_eq!(
            blocks[1],
            TcpSackBlock {
                left_edge: 8_000,
                right_edge: 8_100,
            }
        );
        assert!(connection.pending_dsack.is_none());
    }
}
