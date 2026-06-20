pub use hammer_core::protocol::tcp::TcpState;

use hammer_adapter::{DataPlaneRuntime, DataWorkerId, NodeRuntimeData};
use hammer_core::error::CoreResult;

use crate::session::{
    SessionId, SessionQueueHandle, SessionQueueNext,
    node::SessionQueueDispatchFn,
    runtime::{SessionDriverRuntime, dispatch_session_queue_once_at},
};
use crate::transport::congestion::CongestionController;

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
    TcpConnectionRouteIndex, TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerAddress,
    TcpListenerKey, TcpListenerLookup, TcpListenerLookupAccess, TcpListenerTable, TcpLookupId,
    TcpLookupSnapshot, TcpLookupValue, TcpPendingRouteIndex, TcpV4ListenerKey, TcpV6ListenerKey,
    TcpWorkerOwnedState,
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
pub use state::TcpInputFlags;
pub use syn_rcvd::{TcpSynRcvdNext, TcpSynRcvdNode};
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};
pub use time_wait::{TcpTimeWaitNext, TcpTimeWaitNode};

pub(crate) type TcpQueueHandle<C> =
    SessionQueueHandle<SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>>;

pub(crate) fn tcp_session_queue_dispatch_fn<C>() -> SessionQueueDispatchFn
where
    C: CongestionController + 'static,
{
    tcp_session_queue_dispatch::<C>
}

fn tcp_session_queue_dispatch<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let mut driver = SessionQueueHandle::<
        SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
    >::new(data)
    .borrow_mut()?;
    dispatch_session_queue_once_at(runtime, &mut driver, now, output_next, output)?;
    Ok(())
}

impl<C> SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>
where
    C: CongestionController + 'static,
{
    #[inline]
    pub(crate) fn session_route_by_tuple(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.aux().session_route_by_tuple(local, remote)
    }

    #[inline]
    pub(crate) fn pending_route_by_tuple(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.aux().pending_route_by_tuple(local, remote)
    }
}

#[cfg(test)]
pub(crate) fn register_tcp_session_queue<C>(
    worker: DataWorkerId,
    buffers: DataPlaneBuffers,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    crate::session::node::register_session_queue(SessionDriverRuntime::new(
        worker,
        buffers,
        TcpWorkerOwnedState::new(worker),
    ))
}

#[cfg(test)]
pub(crate) fn register_tcp_session_queue_with_connection_for_test<C>(
    worker: DataWorkerId,
    buffers: DataPlaneBuffers,
    connection: TcpConnectionState<C>,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let mut driver = SessionDriverRuntime::new(worker, buffers, TcpWorkerOwnedState::new(worker));
    let session_id = driver.insert_session(connection.clone());
    driver.aux_mut().remember_session(
        session_id,
        connection.connection_id(),
        connection.local(),
        connection.remote(),
        connection.owner_worker(),
        connection.next_node(),
    );
    crate::session::node::register_session_queue(driver)
}

#[cfg(test)]
pub(crate) fn register_session_queue_for_test<C>(
    queue: SessionDriverRuntime<TcpConnectionState<C>, TcpWorkerOwnedState>,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    crate::session::node::register_session_queue(queue)
}

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
