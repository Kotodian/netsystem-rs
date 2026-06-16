use std::net::SocketAddr;
use std::time::Duration;

use hammer_core::error::CoreError;
use hammer_core::protocol::tcp::{TcpCapabilities, TcpNegotiatedOptions, TcpSegmentHeader};

use crate::transport::congestion::CongestionController;

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
    pub(crate) fn on_tcp_timer_expiry(
        &mut self,
    ) -> Option<(
        TcpConnectionTimerKind,
        SocketAddr,
        SocketAddr,
        TcpSegmentHeader,
    )> {
        for timer in TcpConnectionTimerKind::all().iter() {
            if let Some(output) = self.on_tcp_timer(timer) {
                return Some(output);
            }
        }
        None
    }

    #[inline]
    fn on_tcp_timer(
        &mut self,
        timer: TcpConnectionTimerKind,
    ) -> Option<(
        TcpConnectionTimerKind,
        SocketAddr,
        SocketAddr,
        TcpSegmentHeader,
    )> {
        match self {
            Self::SynSent(connection) => {
                let header = connection.on_tcp_timer_expiry(timer)?;
                Some((timer, connection.local()?, connection.remote(), header))
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
