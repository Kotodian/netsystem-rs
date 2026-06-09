pub use hammer_core::protocol::tcp::TcpState;

pub mod accept;
pub mod app;
pub mod established;
pub mod input;
pub mod listen;
pub mod lookup;
pub mod rcv_process;
pub mod reset;
pub mod state;
pub mod syn_sent;

pub use accept::{
    TcpAcceptBackend, TcpAcceptControlPlane, TcpAcceptNext, TcpAcceptNode, TcpAcceptRegistration,
};
pub use app::TcpAppIngress;
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputHandoff, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use lookup::{
    TcpLookupId, TcpLookupKind, TcpLookupSnapshot, TcpLookupValue, TcpV4ConnectionKey,
    TcpV4ListenerKey, TcpV4PendingConnectionKey, TcpV6ConnectionKey, TcpV6ListenerKey,
    TcpV6PendingConnectionKey, TcpWorkerOwnedState,
};
pub use rcv_process::{TcpRcvProcessControlPlane, TcpRcvProcessNext, TcpRcvProcessNode};
pub use reset::{TcpResetNext, TcpResetNode};
pub use state::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionState, TcpDispatchEntry,
    TcpDispatchTable, TcpInputFlags, TcpListenerConfig,
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
