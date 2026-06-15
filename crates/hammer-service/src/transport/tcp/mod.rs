pub use hammer_core::protocol::tcp::TcpState;

pub mod accept;
pub mod congestion;
pub mod congestion_control;
pub mod connection;
pub mod established;
pub mod input;
pub mod listen;
pub mod lookup;
pub mod output;
pub mod rcv_process;
pub mod reply;
pub mod reset;
mod segment;
pub mod session;
pub mod session_index;
pub mod state;
pub mod state_machine;
pub mod syn_sent;

pub use accept::{TcpAcceptNext, TcpAcceptNode};
pub use congestion::{TcpCongestionAckSample, TcpCongestionState};
pub use congestion_control::{
    TcpCongestionAckObservation, TcpCongestionControlNode, TcpCongestionLossObservation,
    TcpCongestionSendObservation,
};
pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TcpConnectionOptionState, TcpConnectionState, TcpConnectionTimerKind, TcpConnectionView,
    TcpRetransmitTimeoutState,
};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputHandoff, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerAddress, TcpListenerKey,
    TcpListenerLookup, TcpListenerLookupAccess, TcpListenerTable, TcpLookupId, TcpLookupSnapshot,
    TcpLookupValue, TcpV4ListenerKey, TcpV6ListenerKey, TcpWorkerOwnedState,
};
pub use output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN,
    TcpOutputNext, TcpOutputNode,
};
pub use rcv_process::{TcpRcvProcessControlPlane, TcpRcvProcessNext, TcpRcvProcessNode};
pub use reply::{
    TcpControlFlags, queue_tcp_control_packet, synthesize_ipv4_tcp_control, tcp_control_metadata,
};
pub use reset::{TcpResetNext, TcpResetNode};
pub use session::TcpSessionProtocol;
pub use session_index::{TcpPendingIndex, TcpSessionConnectionIndex};
pub use state::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionConfigState, TcpInputFlags,
};
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

#[hammer_component_macros::node_next]
pub enum TcpInputNext {
    Drop,
    Punt,
    Listen,
    RcvProcess,
    SynSent,
    Established,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpInputError {
    BadLength,
    WrongProtocol,
    AckInvalid,
    ConnectionClosed,
}

impl TcpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}
