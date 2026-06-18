pub use hammer_core::protocol::tcp::TcpState;

pub mod accept;
pub mod close_wait;
pub mod closing;
pub mod congestion;
pub mod connection;
pub mod established;
pub mod fin_wait1;
pub mod fin_wait2;
pub mod input;
pub mod last_ack;
pub mod listen;
pub mod lookup;
pub mod output;
pub mod recovery;
pub mod reply;
pub mod reset;
pub mod segment;
pub mod session;
pub mod session_index;
pub mod state;
pub mod state_machine;
pub mod syn_rcvd;
pub mod syn_sent;
pub mod time_wait;

pub use accept::{TcpAcceptNext, TcpAcceptNode};
pub use close_wait::{TcpCloseWaitNext, TcpCloseWaitNode};
pub use closing::{TcpClosingNext, TcpClosingNode};
pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TcpConnectionOptionState, TcpConnectionState, TcpConnectionTimerKind,
    TcpRetransmitTimeoutState,
};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use fin_wait1::{TcpFinWait1Next, TcpFinWait1Node};
pub use fin_wait2::{TcpFinWait2Next, TcpFinWait2Node};
pub use input::{TcpInputControlPlane, TcpInputHandoff, TcpInputNode, TcpInputTrace};
pub use last_ack::{TcpLastAckNext, TcpLastAckNode};
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
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reply::{
    TcpControlFlags, queue_tcp_control_packet, synthesize_ipv4_tcp_control, tcp_control_metadata,
};
pub use reset::{TcpResetNext, TcpResetNode};
pub use session::TcpSessionProtocol;
pub use session_index::{TcpPendingIndex, TcpSessionConnectionIndex};
pub use state::TcpInputFlags;
pub use syn_rcvd::{TcpSynRcvdNext, TcpSynRcvdNode};
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};
pub use time_wait::{TcpTimeWaitNext, TcpTimeWaitNode};

#[hammer_component_macros::node_next]
pub enum TcpInputNext {
    Drop,
    Punt,
    Listen,
    SynSent,
    SynRcvd,
    Established,
    CloseWait,
    FinWait1,
    FinWait2,
    Closing,
    LastAck,
    TimeWait,
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
