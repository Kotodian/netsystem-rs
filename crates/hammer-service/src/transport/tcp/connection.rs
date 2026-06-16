use std::net::SocketAddr;
use std::time::Duration;

use hammer_core::error::CoreError;
use hammer_core::protocol::tcp::{TcpCapabilities, TcpNegotiatedOptions, TcpSegmentHeader};

pub use super::state_machine::TcpConnection;
use super::state_machine::{
    CloseWait, Closed, Closing, Established, FinWait1, FinWait2, LastAck, Listen, SynRcvd, SynSent,
    TimeWait,
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

impl TcpConnectionState {
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
        timer: TcpConnectionTimerKind,
    ) -> Option<(SocketAddr, SocketAddr, TcpSegmentHeader)> {
        match self {
            Self::SynSent(connection) => {
                let header = connection.on_tcp_timer_expiry(timer)?;
                Some((connection.local()?, connection.remote(), header))
            }
            _ => None,
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
                    _ => Err(CoreError::internal(concat!(
                        "tcp connection state mismatch: expected ",
                        stringify!($variant)
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
