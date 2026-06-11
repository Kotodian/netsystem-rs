pub use hammer_core::protocol::tcp::TcpState;

pub mod accept;
pub mod app;
pub mod congestion;
pub mod congestion_control;
pub mod connection;
pub mod established;
pub mod input;
pub mod listen;
pub mod lookup;
mod options;
pub mod output;
pub mod rcv_process;
pub mod reset;
pub mod state;
pub mod syn_sent;

pub use accept::{
    TcpAcceptBackend, TcpAcceptControlPlane, TcpAcceptNext, TcpAcceptNode, TcpAcceptRegistration,
};
pub use app::TcpAppIngress;
pub use congestion::{TcpCongestionAckSample, TcpCongestionState};
pub use congestion_control::{
    TcpCongestionAckObservation, TcpCongestionControlNode, TcpCongestionLossObservation,
    TcpCongestionSendObservation,
};
pub use connection::{
    TcpConnectionSnapshot, TcpConnectionSnapshotPool, TcpConnectionTable, TcpDataPlaneConnection,
    TcpEstablishedControlPlane, TcpWorkerOwnedConnectionState,
};
pub use established::{
    TcpEstablishedAckObservation, TcpEstablishedBackend, TcpEstablishedNext, TcpEstablishedNode,
    TcpEstablishedObservation,
};
pub use input::{TcpInputControlPlane, TcpInputHandoff, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use lookup::{
    TcpLookupId, TcpLookupKind, TcpLookupSnapshot, TcpLookupValue, TcpV4ConnectionKey,
    TcpV4ListenerKey, TcpV4PendingConnectionKey, TcpV6ConnectionKey, TcpV6ListenerKey,
    TcpV6PendingConnectionKey, TcpWorkerOwnedState,
};
pub use output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, NoopTcpOutputBackend, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH,
    TCP_FLAG_SYN, TcpOutputBackend, TcpOutputBackendSlot, TcpOutputRetransmitQueue,
    TcpOutputRetransmitSegment, TcpOutputSegment, TcpOutputSendView, build_tcp_output_segment,
    build_tcp_output_segment_with_flags,
};
pub use rcv_process::{TcpRcvProcessControlPlane, TcpRcvProcessNext, TcpRcvProcessNode};
pub use reset::{TcpResetNext, TcpResetNode};
pub use state::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionState, TcpDispatchEntry,
    TcpDispatchTable, TcpInputFlags, TcpListenerConfig,
};
pub use syn_sent::{
    TcpSynSentBackend, TcpSynSentControlPlane, TcpSynSentNext, TcpSynSentNode,
    TcpSynSentObservation, TcpSynSentRegistration,
};

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
