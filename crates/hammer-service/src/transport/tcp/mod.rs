use std::cell::RefMut;
use std::marker::PhantomData;

pub use hammer_core::protocol::tcp::TcpState;

use hammer_adapter::{DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData};
use hammer_core::error::CoreResult;

use crate::session::{
    SessionId, SessionQueueHandle, SessionQueueNext,
    node::SessionQueueDispatchFn,
    runtime::{SessionDriverRuntime, dispatch_session_queue_once_at},
};
use crate::transport::congestion::CongestionController;

pub mod congestion;
pub mod connection;
pub mod established;
pub mod input;
pub mod listen;
pub mod lookup;
pub mod output;
pub mod rcv_process;
pub mod recovery;
pub mod reply;
pub mod reset;
pub mod segment;
pub mod state;
pub mod syn_sent;

pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TcpConnection, TcpConnectionOptionState, TcpConnectionTimerKind, TcpRetransmitTimeoutState,
};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN,
    TcpOutputNext, TcpOutputNode,
};
pub use rcv_process::{TcpRcvProcessNext, TcpRcvProcessNode};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reply::{
    TcpControlFlags, queue_tcp_control_packet, synthesize_ipv4_tcp_control, tcp_control_metadata,
};
pub use reset::{TcpResetNext, TcpResetNode};
pub use state::TcpInputFlags;
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

pub(crate) use lookup::TcpWorkerOwnedState;

#[derive(Debug, PartialEq, Eq)]
pub struct TcpQueueHandle<C>
where
    C: CongestionController + 'static,
{
    runtime_data: NodeRuntimeData,
    _controller: PhantomData<fn() -> C>,
}

impl<C> TcpQueueHandle<C>
where
    C: CongestionController + 'static,
{
    #[inline]
    pub(crate) const fn new(runtime_data: NodeRuntimeData) -> Self {
        Self {
            runtime_data,
            _controller: PhantomData,
        }
    }

    #[inline]
    pub(crate) const fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }

    #[inline]
    pub(crate) fn borrow_mut(
        self,
    ) -> CoreResult<RefMut<'static, SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>>>
    {
        SessionQueueHandle::<SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>>::new(
            self.runtime_data,
        )
        .borrow_mut()
    }
}

impl<C> Copy for TcpQueueHandle<C> where C: CongestionController + 'static {}

impl<C> Clone for TcpQueueHandle<C>
where
    C: CongestionController + 'static,
{
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

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
        SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
    >::new(data)
    .borrow_mut()?;
    dispatch_session_queue_once_at(runtime, &mut driver, now, output_next, output)?;
    Ok(())
}

impl<C> SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>
where
    C: CongestionController + 'static,
{
    pub(crate) fn connect(
        &mut self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> CoreResult<SessionId> {
        let owner = self.worker();
        let initial_sequence = self.aux_mut().next_initial_sequence(local, remote);
        let session_id = self.insert_session_with_id(|session_id: SessionId| {
            let connection_id = hammer_core::protocol::tcp::TcpConnectionId::new(session_id.get());
            let mut connection =
                TcpConnection::new(Some(connection_id), owner, local.port(), Some(local), remote);
            connection.connect_state(initial_sequence);
            connection
        });
        self.aux_mut().remember_pending_open(
            session_id,
            Some(local),
            remote,
            owner,
            TcpInputNext::SynSent,
        );
        if let Some(connection) = self.session_mut(session_id) {
            connection.tcp_timer_set(TcpConnectionTimerKind::RETRANSMIT);
        }
        self.mark_ready(session_id);
        Ok(session_id)
    }

    pub(crate) fn refresh_session_route(&mut self, session_id: SessionId) -> CoreResult<()> {
        let state = self
            .session(session_id)
            .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?
            .clone();
        let completed_active_open = state
            .local()
            .and_then(|local| self.pending_route_by_tuple(local, state.remote()))
            .is_some_and(|(pending_session_id, _, _)| pending_session_id == session_id)
            && state.state() == TcpState::Established;
        self.aux_mut().forget_session(session_id);
        self.aux_mut().forget_pending_open(session_id);
        match state.state() {
            TcpState::Closed => {
                let _ = self.close_session(session_id)?;
                return Ok(());
            }
            TcpState::SynSent => self.aux_mut().remember_pending_open(
                session_id,
                state.local(),
                state.remote(),
                state.owner_worker(),
                state.next_node(),
            ),
            _ => self.aux_mut().remember_session(
                session_id,
                state.connection_id(),
                state.local(),
                state.remote(),
                state.owner_worker(),
                state.next_node(),
            ),
        }
        if completed_active_open
            && let Some(op) = self.session_app_op(session_id)
        {
            self.app().complete_connected(op)?;
        }
        Ok(())
    }

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

pub(crate) fn register_tcp_session_queue<C>(
    worker: DataWorkerId,
    buffers: DataPlaneBuffers,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let handle = crate::session::node::register_session_queue(
        SessionDriverRuntime::<TcpConnection<C>, TcpWorkerOwnedState>::new(
            worker,
            buffers,
            TcpWorkerOwnedState::new(worker),
        ),
    )?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

pub(crate) fn register_tcp_session_queue_with_connection_for_test<C>(
    worker: DataWorkerId,
    buffers: DataPlaneBuffers,
    connection: TcpConnection<C>,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let mut driver = SessionDriverRuntime::new(worker, buffers, TcpWorkerOwnedState::new(worker));
    let session_id = driver.insert_session_with_id(|_| connection.clone());
    driver.refresh_session_route(session_id)?;
    let handle = crate::session::node::register_session_queue(driver)?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

#[cfg(test)]
pub(crate) fn register_session_queue_for_test<C>(
    queue: SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let handle = crate::session::node::register_session_queue(queue)?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

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
