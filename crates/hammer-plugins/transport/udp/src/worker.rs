use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, mpsc};

use hammer_core::data_plane::{BufferFrame, Index as BufferIndex, NodeHandle, NodeId, NodeState};
use hammer_infra::align::CacheLine;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::thread_owned::{ThreadOwned, ThreadOwnedError};
use hammer_runtime::app::SessionDgramHeader;
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Engine, NodeRuntimeData, RuntimeError, RuntimeResult,
    SessionConnectEndpoint, SessionConnectionId, SessionListenEndpoint, SessionListenerId,
    with_data_plane_runtime,
};
use hammer_service::session::node::{SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionDgramArgs, SessionMain, SessionMigrateResult, SessionSwitchPoolArgs,
    SessionSwitchPoolClosed, SessionSwitchPoolCompletion, SessionSwitchPoolCompletionStatus,
    SessionSwitchPoolReply, SessionSwitchPoolStatus, SessionTransport, SessionTransportId,
    SessionWorker, TransportInternalTransport, TransportInternalTx, dispatch_session_queue_events,
};
use hammer_service::session::{SessionId, SessionQueueNext};

use crate::UdpIpVersion;
use crate::connection::{UdpConnection, UdpListener};
use crate::lookup::UdpLookup;
use crate::output::UdpOutputNode;
use crate::wire::write_udp_header;

const UDP_CONNECTION_CAPACITY: usize = 1024;
const UDP_LISTENER_CAPACITY: usize = 256;
const UDP_HEADER_LEN: usize = 8;

#[hammer_component_macros::runtime_error(subsystem = "udp")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum UdpTransportError {
    #[error("required UDP graph node `{name}` is not registered")]
    NodeMissing { name: &'static str },
    #[error("runtime thread {thread_index} is not a data worker")]
    WorkerUnavailable { thread_index: u32 },
    #[error("UDP worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("UDP worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("UDP worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
    #[error("UDP connection pool capacity {capacity} is exhausted")]
    ConnectionCapacityExhausted { capacity: usize },
    #[error("UDP connection {index:?} is missing")]
    ConnectionMissing { index: PoolIndex },
    #[error("UDP listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionListenerId },
    #[error("UDP endpoint {endpoint} is already in use")]
    EndpointInUse { endpoint: SocketAddr },
    #[error("UDP session {session_id:?} is missing")]
    SessionMissing { session_id: SessionId },
    #[error("UDP connection requires compatible IPv4 or IPv6 endpoints")]
    InvalidConnection,
    #[error("UDP output header could not be written")]
    OutputHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpDelivery {
    Delivered,
    FifoFull,
    MigrationQueued,
    WrongWorker,
    NotUdp,
}

struct UdpListenerCell {
    value: UnsafeCell<Vec<UdpListener>>,
}

impl UdpListenerCell {
    fn new() -> Self {
        Self {
            value: UnsafeCell::new(Vec::with_capacity(UDP_LISTENER_CAPACITY)),
        }
    }

    #[inline]
    fn get(&self) -> &[UdpListener] {
        // SAFETY: listener reads happen on Data Workers while the main thread
        // holds the worker barrier before mutation.
        unsafe { &*self.value.get() }
    }

    #[allow(clippy::mut_from_ref)]
    #[inline]
    fn get_mut(&self) -> &mut Vec<UdpListener> {
        // SAFETY: only the main/control thread mutates this list and only
        // while Data Workers are stopped at the worker barrier.
        unsafe { &mut *self.value.get() }
    }
}

// SAFETY: shared listener reads are ordered by the worker barrier; mutable
// access is confined to the main/control thread during the barrier phase.
unsafe impl Send for UdpListenerCell {}
unsafe impl Sync for UdpListenerCell {}

pub struct UdpMain {
    listeners: Arc<UdpListenerCell>,
    sessions: Arc<SessionMain>,
    workers: Box<[CacheLine<ThreadOwned<UdpWorker>>]>,
}

impl UdpMain {
    fn new(worker_count: usize, sessions: Arc<SessionMain>) -> Self {
        let workers = (0..worker_count)
            .map(|_| CacheLine::new(ThreadOwned::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            listeners: Arc::new(UdpListenerCell::new()),
            sessions,
            workers,
        }
    }

    fn worker(&self, worker: DataWorkerId) -> RuntimeResult<&ThreadOwned<UdpWorker>> {
        self.workers
            .get(worker.slot())
            .map(|slot| &**slot)
            .ok_or_else(|| {
                UdpTransportError::WorkerOutOfRange {
                    worker: worker.slot(),
                }
                .into()
            })
    }

    fn with_worker<R>(
        &self,
        runtime: &DataPlaneRuntime,
        operation: impl FnOnce(&mut SessionWorker<PoolIndex>, &mut UdpWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        self.sessions.with_worker_mut(runtime, |sessions| {
            let worker = runtime
                .thread_index()
                .checked_sub(1)
                .map(DataWorkerId::new)
                .ok_or(UdpTransportError::WorkerUnavailable {
                    thread_index: runtime.thread_index(),
                })?;
            self.worker(worker)?
                .with_mut(|udp| operation(sessions, udp))
                .map_err(|source| UdpTransportError::WorkerAccess {
                    worker: worker.slot(),
                    source,
                })?
        })
    }

    pub(crate) fn deliver_datagram(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        local: SocketAddr,
        remote: SocketAddr,
        payload_offset: usize,
        payload_len: usize,
        urgent: bool,
        return_node: NodeId,
    ) -> RuntimeResult<UdpDelivery> {
        let listener = find_udp_listener(self.listeners.get(), local);
        self.with_worker(runtime, |sessions, udp| {
            udp.deliver_datagram(
                sessions,
                runtime,
                index,
                local,
                remote,
                payload_offset,
                payload_len,
                urgent,
                listener,
                return_node,
            )
        })
    }

}

pub(crate) static UDP_MAIN: OnceLock<Arc<UdpMain>> = OnceLock::new();

#[hammer_component_macros::session_transport(
    name = "udp",
    start_listen = crate::worker::start_listen,
    stop_listen = crate::worker::stop_listen,
    connect = crate::worker::connect
)]
pub struct UdpWorker {
    worker: DataWorkerId,
    connections: Pool<UdpConnection>,
    lookup: UdpLookup,
    session_switch_pool_replies: VecDeque<SessionSwitchPoolReply>,
    session_switch_pool_completions: VecDeque<SessionSwitchPoolCompletion>,
    session_switch_pool_closed: VecDeque<SessionSwitchPoolClosed>,
}

impl UdpWorker {
    pub(crate) fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            connections: Pool::with_capacity(UDP_CONNECTION_CAPACITY),
            lookup: UdpLookup::new(),
            session_switch_pool_replies: VecDeque::new(),
            session_switch_pool_completions: VecDeque::new(),
            session_switch_pool_closed: VecDeque::new(),
        }
    }

    fn insert_connection(&mut self, connection: UdpConnection) -> RuntimeResult<PoolIndex> {
        self.connections.insert(connection).ok_or_else(|| {
            UdpTransportError::ConnectionCapacityExhausted {
                capacity: self.connections.capacity(),
            }
            .into()
        })
    }

    fn connection(&self, index: PoolIndex) -> Option<&UdpConnection> {
        self.connections.get(index)
    }

    fn connection_mut(&mut self, index: PoolIndex) -> Option<&mut UdpConnection> {
        self.connections.get_mut(index)
    }

    fn remove_connection(&mut self, index: PoolIndex) -> Option<UdpConnection> {
        self.connections.remove(index)
    }

    fn accept_datagram(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        listener: UdpListener,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> RuntimeResult<(PoolIndex, SessionId)> {
        let connection = UdpConnection::connected(self.worker, local, remote)
            .ok_or(UdpTransportError::InvalidConnection)?;
        let index = self.insert_connection(connection)?;
        let session_id =
            match sessions.stream_accept(UdpWorker::ID, index, listener.session_listener()) {
                Ok(session_id) => session_id,
                Err(error) => {
                    self.remove_connection(index);
                    return Err(error);
                }
            };
        if !self
            .connection_mut(index)
            .expect("new UDP connection remains installed")
            .attach_session(session_id)
        {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::InvalidConnection.into());
        }
        self.lookup.insert_tuple(index, local, remote);
        if !sessions.insert_session_endpoint(session_id, UdpWorker::ID, local, remote)? {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::EndpointInUse { endpoint: local }.into());
        }
        let rollback = |sessions: &mut SessionWorker<PoolIndex>,
                        udp: &mut UdpWorker,
                        session_id,
                        index,
                        local,
                        remote| {
            udp.rollback_accept(sessions, session_id, index, local, remote)
        };
        if let Err(error) = sessions.complete_stream_connect(session_id) {
            if let Err(cleanup_error) = rollback(sessions, self, session_id, index, local, remote) {
                tracing::error!(
                    ?session_id,
                    %cleanup_error,
                    "UDP accept App publication rollback failed"
                );
            }
            return Err(error);
        }
        let _ = sessions.mark_session_endpoint_ready(session_id, UdpWorker::ID, local, remote)?;
        Ok((index, session_id))
    }

    fn active_connect(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        connection: SessionConnectionId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> RuntimeResult<SessionId> {
        if self
            .lookup
            .find_tuple(&self.connections, local, remote)
            .is_some()
            || crate::input::is_external_port_registered(UdpIpVersion::from(local), local.port())
        {
            return Err(UdpTransportError::EndpointInUse { endpoint: local }.into());
        }
        let connection_state = UdpConnection::connected(self.worker, local, remote)
            .ok_or(UdpTransportError::InvalidConnection)?;
        let index = self.insert_connection(connection_state)?;
        let session_id = match sessions.stream_connect_pending(UdpWorker::ID, index, connection) {
            Ok(session_id) => session_id,
            Err(error) => {
                self.remove_connection(index);
                return Err(error);
            }
        };
        if !self
            .connection_mut(index)
            .expect("new UDP connection remains installed")
            .attach_session(session_id)
        {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::InvalidConnection.into());
        }
        self.lookup.insert_tuple(index, local, remote);
        if !sessions.insert_session_endpoint(session_id, UdpWorker::ID, local, remote)? {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::EndpointInUse { endpoint: local }.into());
        }
        if let Err(error) = sessions.complete_stream_connect(session_id) {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(error);
        }
        Ok(session_id)
    }

    fn rollback_accept(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        session_id: SessionId,
        index: PoolIndex,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> RuntimeResult<()> {
        self.lookup.remove_tuple(local, remote);
        let _ = sessions.remove_session_endpoint(session_id, UdpWorker::ID, local, remote)?;
        self.remove_connection(index);
        sessions.rollback_session_creation(session_id)?;
        Ok(())
    }

    fn deliver_datagram(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        local: SocketAddr,
        remote: SocketAddr,
        payload_offset: usize,
        payload_len: usize,
        urgent: bool,
        listener: Option<UdpListener>,
        return_node: NodeId,
    ) -> RuntimeResult<UdpDelivery> {
        let endpoint = sessions.lookup_session_endpoint(UdpWorker::ID, local, remote)?;
        if endpoint.is_some()
            && sessions.session_endpoint_is_migrating(UdpWorker::ID, local, remote)?
        {
            // VPP's wrong-thread path does not enqueue on the old owner while
            // migration is outstanding; the input node accounts and drops it.
            return Ok(UdpDelivery::WrongWorker);
        }
        if let Some(connection_index) = self.lookup.find_tuple(&self.connections, local, remote) {
            if endpoint.is_some_and(|handle| handle.worker_index() != self.worker.slot() as u32) {
                return Ok(UdpDelivery::WrongWorker);
            }
            let session_id = self
                .connection(connection_index)
                .and_then(|connection| connection.session())
                .ok_or(UdpTransportError::SessionMissing {
                    session_id: SessionId::from(connection_index),
                })?;
            let _ =
                sessions.mark_session_endpoint_ready(session_id, UdpWorker::ID, local, remote)?;
            let header = SessionDgramHeader::new(local, remote, payload_len)
                .ok_or(UdpTransportError::InvalidConnection)?;
            let written = sessions.enqueue_datagram_rx_from_buffer_at(
                runtime.buffers(),
                session_id,
                index,
                payload_offset,
                header,
                urgent,
            )?;
            return Ok(if written == 0 {
                UdpDelivery::FifoFull
            } else {
                UdpDelivery::Delivered
            });
        }
        if let Some(handle) = sessions.lookup_session_endpoint(UdpWorker::ID, local, remote)? {
            if handle.worker_index() != self.worker.slot() as u32 {
                let result = sessions.program_thread_migration(
                    runtime,
                    self.worker,
                    handle,
                    (UdpWorker::ID, local, remote),
                    SessionDgramArgs {
                        index,
                        payload_offset,
                        payload_len,
                        urgent,
                        return_node,
                    },
                );
                return Ok(match result {
                    SessionMigrateResult::Queued => UdpDelivery::MigrationQueued,
                    SessionMigrateResult::Handoff
                    | SessionMigrateResult::Busy
                    | SessionMigrateResult::QueueFull
                    | SessionMigrateResult::Unavailable => UdpDelivery::WrongWorker,
                });
            }
            return Ok(UdpDelivery::NotUdp);
        }
        let Some(listener) = listener else {
            return Ok(UdpDelivery::NotUdp);
        };
        let (_connection_index, session_id) =
            self.accept_datagram(sessions, listener, local, remote)?;
        let header = SessionDgramHeader::new(local, remote, payload_len)
            .ok_or(UdpTransportError::InvalidConnection)?;
        let written = sessions.enqueue_datagram_rx_from_buffer_at(
            runtime.buffers(),
            session_id,
            index,
            payload_offset,
            header,
            urgent,
        )?;
        Ok(if written == 0 {
            UdpDelivery::FifoFull
        } else {
            UdpDelivery::Delivered
        })
    }

    fn migration_request_is_current(
        &self,
        sessions: &SessionWorker<PoolIndex>,
        args: &SessionSwitchPoolArgs,
    ) -> bool {
        let Some(session_id) = sessions.session_id_from_handle(args.old_sh) else {
            return false;
        };
        let Some((transport, index)) = sessions.session_transport(session_id) else {
            return false;
        };
        let (tuple_transport, local, remote) = args.tuple;
        transport == tuple_transport
            && transport == UdpWorker::ID
            && self.lookup.find_tuple(&self.connections, local, remote) == Some(index)
            && self
                .connection(index)
                .is_some_and(|connection| connection.session() == Some(session_id))
    }

    fn rejected_migration_reply(
        &self,
        sessions: &SessionWorker<PoolIndex>,
        mut reply: SessionSwitchPoolReply,
    ) -> SessionSwitchPoolReply {
        let _ = sessions.cancel_thread_migration(reply.old_sh, reply.tuple);
        reply.new_sh = None;
        reply.state = None;
        reply.status = SessionSwitchPoolStatus::Rejected;
        reply
    }

    fn prepared_migration_reply(
        &self,
        sessions: &SessionWorker<PoolIndex>,
        args: SessionSwitchPoolArgs,
    ) -> Option<SessionSwitchPoolReply> {
        let session_id = sessions.session_id_from_handle(args.old_sh)?;
        let state = sessions.migration_snapshot(session_id)?;
        Some(SessionSwitchPoolReply {
            old_sh: args.old_sh,
            new_sh: None,
            old_thread: args.old_thread,
            new_thread: args.new_thread,
            tuple: args.tuple,
            status: SessionSwitchPoolStatus::Prepared,
            state: Some(state),
            dgram: args.dgram,
        })
    }

    fn publish_migration_reply(
        &mut self,
        sessions: &SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        reply: SessionSwitchPoolReply,
    ) {
        if let Err(reply) = sessions.push_session_switch_pool_reply(runtime, reply) {
            self.session_switch_pool_replies.push_back(reply);
        }
    }

    fn publish_migration_completion(
        &mut self,
        sessions: &SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        completion: SessionSwitchPoolCompletion,
    ) {
        if let Err(completion) = sessions.push_session_switch_pool_completion(runtime, completion) {
            self.session_switch_pool_completions.push_back(completion);
        }
    }

    fn publish_migration_closed(
        &mut self,
        sessions: &SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        closed: SessionSwitchPoolClosed,
    ) {
        if let Err(closed) = sessions.push_session_switch_pool_closed(runtime, closed) {
            self.session_switch_pool_closed.push_back(closed);
        }
    }

    fn handoff_migration_datagram(
        &self,
        runtime: &DataPlaneRuntime,
        reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        let worker = if reply.status == SessionSwitchPoolStatus::Rejected {
            reply.old_thread
        } else {
            reply.new_thread
        };
        runtime
            .handoff_index(
                worker,
                NodeHandle::new(reply.dgram.return_node.slot()),
                reply.dgram.index,
                None::<crate::input::UdpInputNext>,
            )
            .map(|_| ())
            .map_err(|_| reply)
    }

    fn handoff_migration_datagram_or_drop(
        &self,
        runtime: &DataPlaneRuntime,
        reply: SessionSwitchPoolReply,
    ) {
        if let Err(reply) = self.handoff_migration_datagram(runtime, reply) {
            runtime
                .buffers()
                .drop_index_owned_with_trace(reply.dgram.index, |_| {});
        }
    }

    fn process_migration_reply(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        mut reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        if reply.status == SessionSwitchPoolStatus::Rejected {
            self.handoff_migration_datagram_or_drop(runtime, reply);
            return Ok(());
        }

        let (transport, local, remote) = reply.tuple;
        if reply.new_sh.is_none() {
            let Some(state) = reply.state.take() else {
                let reply = self.rejected_migration_reply(sessions, reply);
                self.handoff_migration_datagram_or_drop(runtime, reply);
                return Ok(());
            };
            let Some(connection) = UdpConnection::connected(self.worker, local, remote)
            else {
                let reply = self.rejected_migration_reply(sessions, reply);
                self.handoff_migration_datagram_or_drop(runtime, reply);
                return Ok(());
            };
            let Ok(connection_index) = self.insert_connection(connection) else {
                let reply = self.rejected_migration_reply(sessions, reply);
                self.handoff_migration_datagram_or_drop(runtime, reply);
                return Ok(());
            };
            let Ok((session_id, new_handle)) =
                sessions.install_migrated_session(state, connection_index)
            else {
                let _ = self.remove_connection(connection_index);
                let reply = self.rejected_migration_reply(sessions, reply);
                self.handoff_migration_datagram_or_drop(runtime, reply);
                return Ok(());
            };
            let attached = self
                .connection_mut(connection_index)
                .is_some_and(|connection| connection.attach_session(session_id));
            let inserted = attached
                && self
                    .lookup
                    .insert_tuple(connection_index, local, remote);
            let published = inserted
                && sessions
                    .publish_session_migration(
                        reply.old_sh,
                        new_handle,
                        transport,
                        local,
                        remote,
                    )
                    .unwrap_or(false);
            let accepted = published && sessions.accept_migrated_session(session_id).is_ok();
            if !accepted {
                if published {
                    let _ = sessions.replace_session_endpoint(
                        new_handle,
                        reply.old_sh,
                        transport,
                        local,
                        remote,
                    );
                }
                self.lookup.remove_tuple(local, remote);
                let _ = self.remove_connection(connection_index);
                let _ = sessions.remove_migrated_session(session_id);
                let reply = self.rejected_migration_reply(sessions, reply);
                self.handoff_migration_datagram_or_drop(runtime, reply);
                return Ok(());
            }
            reply.new_sh = Some(new_handle);
        }

        let Some(new_handle) = reply.new_sh else {
            let reply = self.rejected_migration_reply(sessions, reply);
            self.handoff_migration_datagram_or_drop(runtime, reply);
            return Ok(());
        };
        let completion = SessionSwitchPoolCompletion {
            old_sh: reply.old_sh,
            new_sh: new_handle,
            old_thread: reply.old_thread,
            new_thread: reply.new_thread,
            tuple: reply.tuple,
            status: SessionSwitchPoolCompletionStatus::Accepted,
        };
        self.handoff_migration_datagram_or_drop(runtime, reply);
        self.publish_migration_completion(sessions, runtime, completion);
        Ok(())
    }

    fn process_migration_completion(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        completion: SessionSwitchPoolCompletion,
    ) -> bool {
        if completion.status != SessionSwitchPoolCompletionStatus::Accepted {
            return true;
        }
        let (tuple_transport, local, remote) = completion.tuple;
        let closed = SessionSwitchPoolClosed {
            new_sh: completion.new_sh,
        };
        let Some(session_id) = sessions.session_id_from_handle(completion.old_sh) else {
            self.publish_migration_closed(sessions, runtime, closed);
            return true;
        };
        let Some((transport, index)) = sessions.session_transport(session_id) else {
            self.publish_migration_closed(sessions, runtime, closed);
            return true;
        };
        if transport != tuple_transport
            || self
                .lookup
                .find_tuple(&self.connections, local, remote)
                != Some(index)
            || !self
                .connection(index)
                .is_some_and(|connection| connection.session() == Some(session_id))
        {
            self.publish_migration_closed(sessions, runtime, closed);
            return true;
        }
        if sessions
            .notify_migrated_session(session_id, completion.new_sh)
            .is_err()
            || sessions.remove_migrated_session(session_id).is_err()
            || sessions
                .remove_session_endpoint(
                    session_id,
                    transport,
                    local,
                    remote,
                )
                .is_err()
        {
            return false;
        }
        self.lookup
            .remove_tuple(local, remote);
        let _ = self.remove_connection(index);
        true
    }

    fn process_migration_closed(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        closed: SessionSwitchPoolClosed,
    ) -> bool {
        let Some(session_id) = sessions.session_id_from_handle(closed.new_sh) else {
            return true;
        };
        let Some((transport, index)) = sessions.session_transport(session_id) else {
            return true;
        };
        let Some(connection) = self.connection(index) else {
            return true;
        };
        let local = connection.local();
        let remote = connection.remote();
        if connection.session() != Some(session_id)
            || self.lookup.find_tuple(&self.connections, local, remote) != Some(index)
        {
            return true;
        }
        if sessions
            .remove_session_endpoint(session_id, transport, local, remote)
            .is_err()
        {
            return false;
        }
        self.lookup.remove_tuple(local, remote);
        let _ = self.remove_connection(index);
        sessions.remove_migrated_session(session_id).is_ok()
    }

    fn drain_migration_closed(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        while let Some(closed) = self.session_switch_pool_closed.pop_front() {
            if let Err(closed) = sessions.push_session_switch_pool_closed(runtime, closed) {
                self.session_switch_pool_closed.push_front(closed);
                return;
            }
        }
        while let Some(closed) = sessions.pop_session_switch_pool_closed() {
            if !self.process_migration_closed(sessions, closed) {
                self.session_switch_pool_closed.push_front(closed);
                return;
            }
        }
    }

    fn drop_migration_datagram(runtime: &DataPlaneRuntime, dgram: SessionDgramArgs) {
        runtime
            .buffers()
            .drop_index_owned_with_trace(dgram.index, |_| {});
    }

    fn drain_migration_shutdown_requests(
        &mut self,
        sessions: &SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        while let Some(args) = sessions.pop_session_migrate_request() {
            let _ = sessions.cancel_thread_migration(args.old_sh, args.tuple);
            Self::drop_migration_datagram(runtime, args.dgram);
        }
    }

    fn drain_migration_shutdown_replies(
        &mut self,
        sessions: &SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        while let Some(reply) = self.session_switch_pool_replies.pop_front() {
            let _ = sessions.cancel_thread_migration(reply.old_sh, reply.tuple);
            Self::drop_migration_datagram(runtime, reply.dgram);
        }
        while let Some(reply) = sessions.pop_session_switch_pool_reply() {
            let _ = sessions.cancel_thread_migration(reply.old_sh, reply.tuple);
            Self::drop_migration_datagram(runtime, reply.dgram);
        }
    }

    fn drain_migration_shutdown(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        sessions.wait_session_migration_shutdown_phase();
        self.drain_migration_shutdown_requests(sessions, runtime);
        self.drain_migration_shutdown_replies(sessions, runtime);
        self.drain_migration_completions(sessions, runtime);

        sessions.wait_session_migration_shutdown_phase();
        self.drain_migration_closed(sessions, runtime);

        sessions.wait_session_migration_shutdown_phase();
        self.drain_migration_closed(sessions, runtime);

        sessions.wait_session_migration_shutdown_phase();
        self.drain_migration_closed(sessions, runtime);
    }

    fn drain_migration_completions(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        let local_count = self.session_switch_pool_completions.len();
        for _ in 0..local_count {
            let Some(completion) = self.session_switch_pool_completions.pop_front() else {
                break;
            };
            if !self.process_migration_completion(sessions, runtime, completion) {
                self.session_switch_pool_completions.push_back(completion);
            }
        }
        while let Some(completion) = sessions.pop_session_switch_pool_completion() {
            if !self.process_migration_completion(sessions, runtime, completion) {
                self.session_switch_pool_completions.push_back(completion);
            }
        }
    }

    fn drain_migration_replies(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        while let Some(reply) = self.session_switch_pool_replies.pop_front() {
            if let Err(reply) = sessions.push_session_switch_pool_reply(runtime, reply) {
                self.session_switch_pool_replies.push_front(reply);
                return;
            }
        }
        while let Some(reply) = sessions.pop_session_switch_pool_reply() {
            if let Err(reply) = self.process_migration_reply(sessions, runtime, reply) {
                self.session_switch_pool_replies.push_front(reply);
                return;
            }
        }
    }

    fn drain_migration_requests(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
    ) {
        while let Some(args) = sessions.pop_session_migrate_request() {
            let reply = if self.migration_request_is_current(sessions, &args) {
                self.prepared_migration_reply(sessions, args)
                    .unwrap_or_else(|| {
                        self.rejected_migration_reply(
                            sessions,
                            SessionSwitchPoolReply {
                                old_sh: args.old_sh,
                                new_sh: None,
                                old_thread: args.old_thread,
                                new_thread: args.new_thread,
                                tuple: args.tuple,
                                status: SessionSwitchPoolStatus::Rejected,
                                state: None,
                                dgram: args.dgram,
                            },
                        )
                    })
            } else {
                self.rejected_migration_reply(
                    sessions,
                    SessionSwitchPoolReply {
                        old_sh: args.old_sh,
                        new_sh: None,
                        old_thread: args.old_thread,
                        new_thread: args.new_thread,
                        tuple: args.tuple,
                        status: SessionSwitchPoolStatus::Rejected,
                        state: None,
                        dgram: args.dgram,
                    },
                )
            };
            self.publish_migration_reply(sessions, runtime, reply);
        }
    }
}

pub(crate) fn start_listen(
    listener: SessionListenerId,
    _: hammer_runtime::app::ApplicationId,
    _: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    let udp_listener = UdpListener::new(endpoint.local(), listener, endpoint.worker())
        .ok_or(UdpTransportError::InvalidConnection)?;
    if crate::input::is_external_port_registered(
        UdpIpVersion::from(endpoint.local()),
        endpoint.local().port(),
    ) {
        return Err(UdpTransportError::EndpointInUse {
            endpoint: endpoint.local(),
        }
        .into());
    }
    if main
        .listeners
        .get()
        .iter()
        .any(|candidate| candidate.local() == endpoint.local())
    {
        return Err(UdpTransportError::EndpointInUse {
            endpoint: endpoint.local(),
        }
        .into());
    }
    main.listeners.get_mut().push(udp_listener);
    Ok(())
}

fn find_udp_listener(listeners: &[UdpListener], local: SocketAddr) -> Option<UdpListener> {
    let mut wildcard = None;
    for listener in listeners.iter().copied() {
        if !listener.accepts(local) {
            continue;
        }
        if listener.local().ip().is_unspecified() {
            wildcard = wildcard.or(Some(listener));
        } else {
            return Some(listener);
        }
    }
    wildcard
}

pub(crate) fn stop_listen(listener: SessionListenerId) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    let listeners = main.listeners.get_mut();
    let slot = listeners
        .iter()
        .position(|candidate| candidate.session_listener() == listener)
        .ok_or(UdpTransportError::ListenerMissing { listener })?;
    listeners.remove(slot);
    Ok(())
}

pub(crate) fn connect(endpoint: SessionConnectEndpoint) -> RuntimeResult<()> {
    let local = endpoint.local.ok_or(UdpTransportError::InvalidConnection)?;
    if local.is_ipv4() != endpoint.remote.is_ipv4() || local.port() == 0 {
        return Err(UdpTransportError::InvalidConnection.into());
    }
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    let worker = endpoint.worker;
    let worker_slot = worker.slot();
    let (completion, completed) = mpsc::sync_channel(1);
    Engine::with_current(|engine| {
        engine.schedule_on_worker(worker, move || {
            let result = with_data_plane_runtime(|runtime| {
                main.with_worker(runtime, |sessions, udp| {
                    udp.active_connect(sessions, endpoint.connection, local, endpoint.remote)
                })
            });
            if completion.send(result).is_err() {
                return;
            }
        })
    })
    .ok_or(RuntimeError::WorkerControlRequiresMainEngine)??;
    let _ = completed
        .recv()
        .map_err(|_| RuntimeError::DataWorkerCallCanceled {
            worker: worker_slot,
        })??;
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "udp_init",
    runs_after = ["session_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_udp(engine: &mut Engine, sessions: Arc<SessionMain>) -> RuntimeResult<()> {
    let main = Arc::new(UdpMain::new(engine.configured_worker_count(), sessions));
    UDP_MAIN
        .set(Arc::clone(&main))
        .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    Ok(())
}

fn bind_worker_graph(engine: &mut Engine) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue =
        engine
            .runtime
            .node_by_name("session-queue")
            .ok_or(UdpTransportError::NodeMissing {
                name: "session-queue",
            })?;
    let udp_output = engine
        .runtime
        .node_by_name(UdpOutputNode::NODE_NAME)
        .ok_or(UdpTransportError::NodeMissing {
            name: UdpOutputNode::NODE_NAME,
        })?;
    let session_queue_data = engine.runtime.nodes().node_runtime_data(session_queue)?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(&engine.runtime, session_queue, udp_output)?;
    SessionQueueNode::install_worker_attachment(
        &engine.runtime,
        session_queue_data,
        session_queue_output,
        udp_session_queue_update_time,
        udp_session_queue_dispatch,
    )?;
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    if main
        .worker(worker)?
        .install(UdpWorker::new(worker))
        .is_err()
    {
        return Err(UdpTransportError::WorkerAlreadyInstalled {
            worker: worker.slot(),
        }
        .into());
    }
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Polling)?;
    Ok(())
}

#[hammer_component_macros::worker_init_function(
    name = "udp_worker_init",
    runs_after = ["session_worker_init"]
)]
fn init_udp_worker(engine: &mut Engine) -> RuntimeResult<()> {
    UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    bind_worker_graph(engine)?;
    engine.register_worker_exit_function(udp_worker_exit);
    Ok(())
}

fn udp_worker_exit(engine: &mut Engine) -> RuntimeResult<()> {
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    let runtime = engine.runtime.clone();
    main.with_worker(&runtime, |sessions, udp| {
        udp.drain_migration_shutdown(sessions, &runtime);
        Ok(())
    })
}

fn udp_session_queue_update_time(
    runtime: &DataPlaneRuntime,
    _: &mut SessionWorker<PoolIndex>,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    frame: &mut BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    main.with_worker(runtime, |sessions, udp| {
        udp.update_time(sessions, runtime, output_next, frame, output, now)
    })
}

fn udp_session_queue_dispatch(
    runtime: &DataPlaneRuntime,
    _: &mut SessionWorker<PoolIndex>,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    frame: &mut BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    main.with_worker(runtime, |sessions, udp| {
        dispatch_session_queue_events(runtime, sessions, udp, output_next, frame, output, now)
            .map(|_| ())
    })
}

impl SessionTransport<PoolIndex> for UdpWorker {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(2);

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        runtime: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        _: std::time::Instant,
    ) -> RuntimeResult<()> {
        self.drain_migration_completions(sessions, runtime);
        self.drain_migration_closed(sessions, runtime);
        self.drain_migration_replies(sessions, runtime);
        self.drain_migration_requests(sessions, runtime);
        Ok(())
    }

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        index: PoolIndex,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        _: std::time::Instant,
    ) -> RuntimeResult<()> {
        let Some(connection) = self.connections.get(index) else {
            return Ok(());
        };
        let Some(session_id) = connection.session() else {
            return Ok(());
        };
        self.lookup
            .remove_tuple(connection.local(), connection.remote());
        let _ = sessions.remove_session_endpoint(
            session_id,
            UdpWorker::ID,
            connection.local(),
            connection.remote(),
        )?;
        self.connections.remove(index);
        sessions.notify_transport_deleted(session_id, index)?;
        Ok(())
    }
}

impl TransportInternalTransport<PoolIndex> for UdpWorker {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<PoolIndex>,
        session_id: SessionId,
        index: PoolIndex,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut SessionQueueOutput,
        _: std::time::Instant,
    ) -> RuntimeResult<()> {
        while output.remaining_io_budget() != 0 {
            let Some(header) = sessions.peek_tx_datagram(session_id)? else {
                break;
            };
            let data_length = header.data_length() as usize;
            let payload_len = data_length
                .checked_sub(header.data_offset() as usize)
                .ok_or(UdpTransportError::InvalidConnection)?;
            let (local, remote) = {
                let connection = self
                    .connections
                    .get(index)
                    .ok_or(UdpTransportError::ConnectionMissing { index })?;
                (connection.local(), connection.remote())
            };

            let buffer = runtime.buffers().alloc_index()?;
            if let Err(error) =
                sessions.copy_tx_datagram_to_buffer(runtime.buffers(), session_id, header, buffer)
            {
                runtime
                    .buffers()
                    .drop_index_owned_with_trace(buffer, |_| {});
                return Err(error);
            }
            {
                let mut output_buffer = runtime.buffers().get_buffer_mut(buffer)?;
                let header_slice = output_buffer.prepend_mut(UDP_HEADER_LEN)?;
                if write_udp_header(header_slice, local.port(), remote.port(), payload_len)
                    .is_none()
                {
                    return Err(UdpTransportError::OutputHeader.into());
                }
                crate::output::write_udp_egress_endpoints(
                    output_buffer.opaque2_mut(),
                    local.ip(),
                    remote.ip(),
                );
            }
            if !output.try_enqueue_io(frame, output_next, buffer)? {
                runtime
                    .buffers()
                    .drop_index_owned_with_trace(buffer, |_| {});
                sessions.mark_ready(session_id);
                break;
            }
            sessions.dequeue_tx_datagram(session_id, header)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use hammer_core::data_plane::BufferFrame;
    use hammer_runtime::app::AppSessionConfig;
    use hammer_runtime::{
        DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, RuntimeRegistry, SessionListenEndpoint,
        SessionTransportRegistration,
    };
    use hammer_service::session::ApplicationMain;

    use super::*;

    fn noop_start_listen(
        _: SessionListenerId,
        _: hammer_runtime::app::ApplicationId,
        _: Option<u64>,
        _: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn noop_stop_listen(_: SessionListenerId) -> RuntimeResult<()> {
        Ok(())
    }

    fn worker_state() -> (
        SessionWorker<PoolIndex>,
        UdpWorker,
        Arc<ApplicationMain>,
        Arc<SessionMain>,
    ) {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )
        .expect("Session worker");
        sessions.set_listener_main(Arc::clone(&main));
        (
            sessions,
            UdpWorker::new(DataWorkerId::new(0)),
            applications,
            main,
        )
    }

    fn test_endpoints() -> (std::net::SocketAddr, std::net::SocketAddr) {
        (
            "127.0.0.1:9000".parse().expect("local UDP endpoint"),
            "127.0.0.1:50000".parse().expect("remote UDP endpoint"),
        )
    }

    #[test]
    fn udp_transport_registration_has_callable_vft() {
        let registration = super::__SESSION_TRANSPORT_UDP_WORKER;
        assert_eq!(registration.name(), "udp");
        assert!(registration.start_listen().is_some());
        assert!(registration.stop_listen().is_some());
        assert!(registration.connect().is_some());
    }

    #[test]
    fn udp_session_control_path_registers_and_unregisters_listener() -> RuntimeResult<()> {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        engine.install_current();
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach test Application");
        let application_listener = applications
            .register_listener(application, None, Some(0xfeed))
            .expect("register test listener");
        let session_main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let udp_main = Arc::new(UdpMain::new(1, Arc::clone(&session_main)));
        if UDP_MAIN.set(udp_main).is_err() {
            panic!("UDP test main is set more than once");
        }
        let (local, _remote) = test_endpoints();

        let listener = session_main.listen(
            application_listener,
            super::__SESSION_TRANSPORT_UDP_WORKER,
            SessionListenEndpoint::new(local, DataWorkerId::new(0)),
        )?;
        assert_eq!(
            UDP_MAIN.get().expect("UDP test main").listeners.get().len(),
            1
        );
        session_main.unlisten(listener)?;
        assert!(
            UDP_MAIN
                .get()
                .expect("UDP test main")
                .listeners
                .get()
                .is_empty()
        );
        Engine::uninstall_current();
        Ok(())
    }

    #[test]
    fn udp_listener_lookup_prefers_exact_address_over_wildcard() {
        let local = "127.0.0.1:9000".parse().expect("local endpoint");
        let wildcard = UdpListener::new(
            "0.0.0.0:9000".parse().expect("wildcard endpoint"),
            SessionListenerId::new(1, 1),
            DataWorkerId::new(0),
        )
        .expect("wildcard listener");
        let exact = UdpListener::new(local, SessionListenerId::new(2, 1), DataWorkerId::new(0))
            .expect("exact listener");
        let found = find_udp_listener(&[wildcard, exact], local).expect("listener");
        assert_eq!(found.session_listener(), SessionListenerId::new(2, 1));
    }

    #[test]
    fn udp_accept_delivers_datagram_into_exact_session_fifo()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut sessions, mut udp, applications, main) = worker_state();
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_listener = applications.register_listener(application, None, None)?;
        let (local, remote) = test_endpoints();
        let listener = main.listen(
            application_listener,
            SessionTransportRegistration::new(
                "udp-test",
                Some(noop_start_listen),
                Some(noop_stop_listen),
                None,
            ),
            SessionListenEndpoint::new(local, DataWorkerId::new(0)),
        )?;
        let udp_listener =
            UdpListener::new(local, listener, DataWorkerId::new(0)).expect("UDP listener");
        let (_index, session_id) =
            udp.accept_datagram(&mut sessions, udp_listener, local, remote)?;

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        let buffer = runtime.alloc_index_with_bytes(b"hello")?;
        udp.deliver_datagram(
            &mut sessions,
            &runtime,
            buffer,
            local,
            remote,
            0,
            5,
            false,
            None,
            NodeId::new(0),
        )?;

        let app = sessions
            .app_session(session_id)
            .expect("UDP Session owns an AppSession")
            .clone();
        let mut out = [0_u8; 16];
        let (header, copied) = app.recv_datagram(&mut out)?.expect("received datagram");
        assert_eq!(copied, 5);
        assert_eq!(&out[..5], b"hello");
        assert_eq!(header.local(), local);
        assert_eq!(header.remote(), remote);
        Ok(())
    }

    #[test]
    fn udp_active_connect_registers_exact_tuple_session() -> Result<(), Box<dyn std::error::Error>>
    {
        let (mut sessions, mut udp, applications, _main) = worker_state();
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());

        let session_id = udp.active_connect(&mut sessions, connection_id, local, remote)?;

        assert!(sessions.has_session(session_id));
        assert!(
            udp.lookup
                .find_tuple(&udp.connections, local, remote)
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn udp_wrong_worker_classifies_connected_tuple() -> Result<(), Box<dyn std::error::Error>> {
        let (mut sessions, mut udp, applications, _main) = worker_state();
        let mut udp_other = UdpWorker::new(DataWorkerId::new(1));
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        udp.active_connect(&mut sessions, connection_id, local, remote)?;

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(2, 1)
            .expect("worker runtime");
        let buffer = runtime.alloc_index_with_bytes(b"x")?;
        let delivery = udp_other.deliver_datagram(
            &mut sessions,
            &runtime,
            buffer,
            local,
            remote,
            0,
            1,
            false,
            None,
            NodeId::new(0),
        )?;
        assert_eq!(delivery, UdpDelivery::WrongWorker);
        Ok(())
    }

    #[test]
    fn udp_foreign_opened_tuple_queues_first_datagram_for_migration()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        sessions.set_listener_main(Arc::clone(&main));
        let mut udp = UdpWorker::new(DataWorkerId::new(0));
        let mut udp_other = UdpWorker::new(DataWorkerId::new(1));
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        udp.active_connect(&mut sessions, connection_id, local, remote)?;

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(2, 1)
            .expect("worker runtime");
        let buffer = runtime.alloc_index_with_bytes(b"x")?;
        let delivery = udp_other.deliver_datagram(
            &mut sessions,
            &runtime,
            buffer,
            local,
            remote,
            0,
            1,
            false,
            None,
            NodeId::new(0),
        )?;

        assert_eq!(delivery, UdpDelivery::MigrationQueued);
        Ok(())
    }

    #[test]
    fn udp_owner_first_datagram_prevents_later_foreign_migration()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        sessions.set_listener_main(Arc::clone(&main));
        let mut source_udp = UdpWorker::new(DataWorkerId::new(0));
        let mut target_udp = UdpWorker::new(DataWorkerId::new(1));
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        source_udp.active_connect(&mut sessions, connection_id, local, remote)?;

        let source_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 0)?;
        let first = source_runtime.alloc_index_with_bytes(b"x")?;
        assert_eq!(
            source_udp.deliver_datagram(
                &mut sessions,
                &source_runtime,
                first,
                local,
                remote,
                0,
                1,
                false,
                None,
                NodeId::new(0),
            )?,
            UdpDelivery::Delivered
        );

        let target_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 1)?;
        let second = target_runtime.alloc_index_with_bytes(b"y")?;
        assert_eq!(
            target_udp.deliver_datagram(
                &mut sessions,
                &target_runtime,
                second,
                local,
                remote,
                0,
                1,
                false,
                None,
                NodeId::new(0),
            )?,
            UdpDelivery::WrongWorker
        );
        Ok(())
    }

    #[test]
    fn udp_accepted_session_does_not_migrate_on_foreign_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        sessions.set_listener_main(Arc::clone(&main));
        let mut source_udp = UdpWorker::new(DataWorkerId::new(0));
        let mut target_udp = UdpWorker::new(DataWorkerId::new(1));
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_listener = applications.register_listener(application, None, None)?;
        let (local, remote) = test_endpoints();
        let listener = main.listen(
            application_listener,
            SessionTransportRegistration::new(
                "udp-test",
                Some(noop_start_listen),
                Some(noop_stop_listen),
                None,
            ),
            SessionListenEndpoint::new(local, DataWorkerId::new(0)),
        )?;
        let udp_listener =
            UdpListener::new(local, listener, DataWorkerId::new(0)).ok_or("invalid listener")?;
        source_udp.accept_datagram(&mut sessions, udp_listener, local, remote)?;

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 1)?;
        let buffer = runtime.alloc_index_with_bytes(b"x")?;
        let delivery = target_udp.deliver_datagram(
            &mut sessions,
            &runtime,
            buffer,
            local,
            remote,
            0,
            1,
            false,
            None,
            NodeId::new(0),
        )?;
        assert_eq!(delivery, UdpDelivery::WrongWorker);
        Ok(())
    }

    #[test]
    fn udp_old_owner_does_not_process_packet_while_migration_is_pending()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        sessions.set_listener_main(Arc::clone(&main));
        let mut source_udp = UdpWorker::new(DataWorkerId::new(0));
        let mut target_udp = UdpWorker::new(DataWorkerId::new(1));
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        source_udp.active_connect(&mut sessions, connection_id, local, remote)?;

        let target_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 1)?;
        let first = target_runtime.alloc_index_with_bytes(b"x")?;
        assert_eq!(
            target_udp.deliver_datagram(
                &mut sessions,
                &target_runtime,
                first,
                local,
                remote,
                0,
                1,
                false,
                None,
                NodeId::new(0),
            )?,
            UdpDelivery::MigrationQueued
        );

        let source_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 0)?;
        let second = source_runtime.alloc_index_with_bytes(b"y")?;
        assert_eq!(
            source_udp.deliver_datagram(
                &mut sessions,
                &source_runtime,
                second,
                local,
                remote,
                0,
                1,
                false,
                None,
                NodeId::new(0),
            )?,
            UdpDelivery::WrongWorker
        );
        let source_session = sessions
            .lookup_session_endpoint(UdpWorker::ID, local, remote)?
            .and_then(|handle| sessions.session_id_from_handle(handle))
            .ok_or("missing source UDP Session")?;
        assert_eq!(
            sessions
                .fifo_pair(source_session)
                .ok_or("missing source FIFO pair")?
                .0
                .max_dequeue(),
            0
        );
        Ok(())
    }

    #[test]
    fn udp_completion_retry_does_not_block_later_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut sessions, mut udp, applications, main) = worker_state();
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, first_remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        let first_session =
            udp.active_connect(&mut sessions, connection_id, local, first_remote)?;
        let first_handle = main
            .lookup_endpoint(local, first_remote, UdpWorker::ID)
            .ok_or("missing application Session route")?;

        let second_remote: std::net::SocketAddr = "127.0.0.1:50001".parse()?;
        let second_connection =
            UdpConnection::connected(DataWorkerId::new(0), local, second_remote)
                .ok_or("bare UDP connection")?;
        let second_index = udp.insert_connection(second_connection)?;
        let second_session =
            sessions.insert_unbound_transport_session_for_test(UdpWorker::ID, second_index)?;
        if !udp
            .connection_mut(second_index)
            .is_some_and(|connection| connection.attach_session(second_session))
        {
            return Err("bare UDP Session attachment failed".into());
        }
        assert!(udp.lookup.insert_tuple(second_index, local, second_remote));
        assert!(sessions.insert_session_endpoint(
            second_session,
            UdpWorker::ID,
            local,
            second_remote,
        )?);
        let second_handle = main
            .lookup_endpoint(local, second_remote, UdpWorker::ID)
            .ok_or("missing bare Session route")?;

        udp.session_switch_pool_completions.push_back(SessionSwitchPoolCompletion {
            old_sh: first_handle,
            new_sh: second_handle,
            old_thread: DataWorkerId::new(0),
            new_thread: DataWorkerId::new(0),
            tuple: (UdpWorker::ID, local, first_remote),
            status: SessionSwitchPoolCompletionStatus::Accepted,
        });
        udp.session_switch_pool_completions.push_back(SessionSwitchPoolCompletion {
            old_sh: second_handle,
            new_sh: first_handle,
            old_thread: DataWorkerId::new(0),
            new_thread: DataWorkerId::new(0),
            tuple: (UdpWorker::ID, local, second_remote),
            status: SessionSwitchPoolCompletionStatus::Accepted,
        });

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(1, 0)?;
        udp.drain_migration_completions(&mut sessions, &runtime);

        assert_eq!(
            main.lookup_endpoint(local, first_remote, UdpWorker::ID),
            Some(first_handle)
        );
        assert!(
            main.lookup_endpoint(local, second_remote, UdpWorker::ID)
                .is_none()
        );
        assert!(sessions.has_session(first_session));
        assert!(!sessions.has_session(second_session));
        Ok(())
    }

    #[test]
    fn udp_source_migration_processing_rejects_external_application_without_losing_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        sessions.set_listener_main(Arc::clone(&main));
        let mut udp = UdpWorker::new(DataWorkerId::new(0));
        let mut udp_other = UdpWorker::new(DataWorkerId::new(1));
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        udp.active_connect(&mut sessions, connection_id, local, remote)?;
        let old_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing UDP Session route")?;

        let target_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 1)?;
        let buffer = target_runtime.alloc_index_with_bytes(b"x")?;
        let delivery = udp_other.deliver_datagram(
            &mut sessions,
            &target_runtime,
            buffer,
            local,
            remote,
            0,
            1,
            false,
            None,
            NodeId::new(0),
        )?;
        assert_eq!(delivery, UdpDelivery::MigrationQueued);

        let source_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 0)?;
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut udp,
            &mut sessions,
            &source_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        assert_eq!(
            main.lookup_endpoint(local, remote, UdpWorker::ID),
            Some(old_handle)
        );
        let reply = main
            .pop_session_switch_pool_reply(DataWorkerId::new(1))
            .ok_or("missing migration rejection")?;
        assert_eq!(reply.old_sh, old_handle);
        assert_eq!(reply.status, SessionSwitchPoolStatus::Rejected);
        Ok(())
    }

    #[test]
    fn udp_target_migration_installs_session_before_pending_datagram_handoff()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut source_sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        let mut target_sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(1),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        source_sessions.set_listener_main(Arc::clone(&main));
        target_sessions.set_listener_main(Arc::clone(&main));
        let mut source_udp = UdpWorker::new(DataWorkerId::new(0));
        let mut target_udp = UdpWorker::new(DataWorkerId::new(1));
        let (local, remote) = test_endpoints();
        let source_connection = UdpConnection::connected(DataWorkerId::new(0), local, remote)
            .ok_or("source UDP connection")?;
        let source_index = source_udp.insert_connection(source_connection)?;
        let source_session = source_sessions
            .insert_unbound_transport_session_for_test(UdpWorker::ID, source_index)?;
        if !source_udp
            .connection_mut(source_index)
            .is_some_and(|connection| connection.attach_session(source_session))
        {
            return Err("source UDP Session attachment failed".into());
        }
        assert!(source_udp.lookup.insert_tuple(source_index, local, remote));
        assert!(source_sessions.insert_session_endpoint(
            source_session,
            UdpWorker::ID,
            local,
            remote,
        )?);
        let old_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing source UDP Session route")?;

        let target_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 1)?;
        let buffer = target_runtime.alloc_index_with_bytes(b"x")?;
        assert_eq!(
            target_udp.deliver_datagram(
                &mut target_sessions,
                &target_runtime,
                buffer,
                local,
                remote,
                0,
                1,
                false,
                None,
                NodeId::new(0),
            )?,
            UdpDelivery::MigrationQueued
        );

        let source_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 0)?;
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut source_udp,
            &mut source_sessions,
            &source_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut target_udp,
            &mut target_sessions,
            &target_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        let new_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing target UDP Session route")?;
        assert_ne!(new_handle, old_handle);
        assert_eq!(new_handle.worker_index(), 1);
        assert!(source_sessions.session_id_from_handle(old_handle).is_some());
        let target_session = target_sessions
            .session_id_from_handle(new_handle)
            .ok_or("target Session was not installed")?;
        assert!(
            target_udp
                .lookup
                .find_tuple(&target_udp.connections, local, remote)
                .is_some()
        );
        assert!(target_sessions.has_session(target_session));
        assert_eq!(target_udp.session_switch_pool_replies.len(), 0);
        assert!(target_runtime.get_buffer(buffer).is_err());
        Ok(())
    }

    #[test]
    fn udp_source_close_after_publication_cleans_target_clone_idempotently()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut source_sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        let mut target_sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(1),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        source_sessions.set_listener_main(Arc::clone(&main));
        target_sessions.set_listener_main(Arc::clone(&main));
        let mut source_udp = UdpWorker::new(DataWorkerId::new(0));
        let mut target_udp = UdpWorker::new(DataWorkerId::new(1));
        let (local, remote) = test_endpoints();
        let source_connection = UdpConnection::connected(DataWorkerId::new(0), local, remote)
            .ok_or("source UDP connection")?;
        let source_index = source_udp.insert_connection(source_connection)?;
        let source_session = source_sessions
            .insert_unbound_transport_session_for_test(UdpWorker::ID, source_index)?;
        if !source_udp
            .connection_mut(source_index)
            .is_some_and(|connection| connection.attach_session(source_session))
        {
            return Err("source UDP Session attachment failed".into());
        }
        assert!(source_udp.lookup.insert_tuple(source_index, local, remote));
        assert!(source_sessions.insert_session_endpoint(
            source_session,
            UdpWorker::ID,
            local,
            remote,
        )?);
        let old_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing source UDP Session route")?;

        let target_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 1)?;
        let buffer = target_runtime.alloc_index_with_bytes(b"x")?;
        assert_eq!(
            target_udp.deliver_datagram(
                &mut target_sessions,
                &target_runtime,
                buffer,
                local,
                remote,
                0,
                1,
                false,
                None,
                NodeId::new(0),
            )?,
            UdpDelivery::MigrationQueued
        );

        let source_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 0)?;
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut source_udp,
            &mut source_sessions,
            &source_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut target_udp,
            &mut target_sessions,
            &target_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        let new_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing target UDP Session route")?;
        assert_ne!(new_handle, old_handle);
        assert_eq!(new_handle.worker_index(), 1);
        assert!(target_sessions.session_id_from_handle(new_handle).is_some());
        assert!(
            target_udp
                .lookup
                .find_tuple(&target_udp.connections, local, remote)
                .is_some()
        );

        source_sessions.schedule_disconnect(source_session);
        dispatch_session_queue_events(
            &source_runtime,
            &mut source_sessions,
            &mut source_udp,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;
        assert!(source_sessions.session_id_from_handle(old_handle).is_none());
        assert!(source_udp
            .lookup
            .find_tuple(&source_udp.connections, local, remote)
            .is_none());

        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut source_udp,
            &mut source_sessions,
            &source_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;
        let closed = main
            .pop_session_switch_pool_closed(DataWorkerId::new(1))
            .ok_or("missing closed notification")?;
        assert!(main.push_session_switch_pool_closed(&target_runtime, closed).is_ok());
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut target_udp,
            &mut target_sessions,
            &target_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;
        assert!(main.lookup_endpoint(local, remote, UdpWorker::ID).is_none());
        assert!(target_sessions.session_id_from_handle(new_handle).is_none());
        assert!(
            target_udp
                .lookup
                .find_tuple(&target_udp.connections, local, remote)
                .is_none()
        );

        assert!(main
            .push_session_switch_pool_closed(
                &target_runtime,
                SessionSwitchPoolClosed { new_sh: new_handle },
            )
            .is_ok());
        <UdpWorker as SessionTransport<PoolIndex>>::update_time(
            &mut target_udp,
            &mut target_sessions,
            &target_runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;
        assert!(main.lookup_endpoint(local, remote, UdpWorker::ID).is_none());
        assert!(target_sessions.session_id_from_handle(new_handle).is_none());
        Ok(())
    }

    #[test]
    fn udp_worker_shutdown_drops_pending_migration_datagrams_and_clears_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        sessions.set_listener_main(Arc::clone(&main));
        let mut udp = UdpWorker::new(DataWorkerId::new(0));
        let (local, remote) = test_endpoints();
        let connection = UdpConnection::connected(DataWorkerId::new(0), local, remote)
            .ok_or("source UDP connection")?;
        let connection_index = udp.insert_connection(connection)?;
        let session_id = sessions
            .insert_unbound_transport_session_for_test(UdpWorker::ID, connection_index)?;
        if !udp
            .connection_mut(connection_index)
            .is_some_and(|connection| connection.attach_session(session_id))
        {
            return Err("source UDP Session attachment failed".into());
        }
        assert!(udp.lookup.insert_tuple(connection_index, local, remote));
        assert!(sessions.insert_session_endpoint(
            session_id,
            UdpWorker::ID,
            local,
            remote,
        )?);
        let old_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing source UDP Session route")?;
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(2, 0)?;
        let request_buffer = runtime.alloc_index_with_bytes(b"request")?;
        assert_eq!(
            sessions.program_thread_migration(
                &runtime,
                DataWorkerId::new(1),
                old_handle,
                (UdpWorker::ID, local, remote),
                SessionDgramArgs {
                    index: request_buffer,
                    payload_offset: 0,
                    payload_len: 7,
                    urgent: false,
                    return_node: NodeId::new(0),
                },
            ),
            SessionMigrateResult::Queued
        );
        let reply_buffer = runtime.alloc_index_with_bytes(b"reply")?;
        assert!(main
            .push_session_switch_pool_reply(
                &runtime,
                SessionSwitchPoolReply {
                    old_sh: old_handle,
                    new_sh: None,
                    old_thread: DataWorkerId::new(0),
                    new_thread: DataWorkerId::new(0),
                    tuple: (UdpWorker::ID, local, remote),
                    status: SessionSwitchPoolStatus::Rejected,
                    state: None,
                    dgram: SessionDgramArgs {
                        index: reply_buffer,
                        payload_offset: 0,
                        payload_len: 5,
                        urgent: false,
                        return_node: NodeId::new(0),
                    },
                },
            )
            .is_ok());

        main.begin_session_migration_shutdown();
        let rendezvous_main = Arc::clone(&main);
        let rendezvous = std::thread::spawn(move || {
            for _ in 0..4 {
                rendezvous_main.wait_session_migration_shutdown_phase();
            }
        });
        udp.drain_migration_shutdown(&mut sessions, &runtime);
        rendezvous.join().expect("shutdown phase rendezvous");

        assert!(!main.endpoint_is_migrating(local, remote, UdpWorker::ID));
        assert_eq!(
            main.lookup_endpoint(local, remote, UdpWorker::ID),
            Some(old_handle)
        );
        assert!(runtime.get_buffer(request_buffer).is_err());
        assert!(runtime.get_buffer(reply_buffer).is_err());
        Ok(())
    }

    #[test]
    fn udp_source_reply_retry_republishes_to_target_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let applications = ApplicationMain::new(1024);
        let main = Arc::new(SessionMain::new(2, Arc::clone(&applications)));
        let mut source_sessions = SessionWorker::<PoolIndex>::new(
            DataWorkerId::new(0),
            2,
            AppSessionConfig::default(),
            1024,
            Arc::clone(&applications),
            None,
        )?;
        source_sessions.set_listener_main(Arc::clone(&main));
        let mut source_udp = UdpWorker::new(DataWorkerId::new(0));
        let (local, remote) = test_endpoints();
        let source_connection = UdpConnection::connected(DataWorkerId::new(0), local, remote)
            .ok_or("source UDP connection")?;
        let source_index = source_udp.insert_connection(source_connection)?;
        let source_session = source_sessions
            .insert_unbound_transport_session_for_test(UdpWorker::ID, source_index)?;
        if !source_udp
            .connection_mut(source_index)
            .is_some_and(|connection| connection.attach_session(source_session))
        {
            return Err("source UDP Session attachment failed".into());
        }
        assert!(source_udp.lookup.insert_tuple(source_index, local, remote));
        assert!(source_sessions.insert_session_endpoint(
            source_session,
            UdpWorker::ID,
            local,
            remote,
        )?);
        let old_handle = main
            .lookup_endpoint(local, remote, UdpWorker::ID)
            .ok_or("missing source UDP Session route")?;
        let source_runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(2, 0)?;
        let buffer = source_runtime.alloc_index_with_bytes(b"x")?;
        assert_eq!(
            source_sessions.program_thread_migration(
                &source_runtime,
                DataWorkerId::new(1),
                old_handle,
                (UdpWorker::ID, local, remote),
                SessionDgramArgs {
                    index: buffer,
                    payload_offset: 0,
                    payload_len: 1,
                    urgent: false,
                    return_node: NodeId::new(0),
                },
            ),
            SessionMigrateResult::Queued
        );
        assert!(source_sessions.pop_session_migrate_request().is_some());
        let state = source_sessions
            .migration_snapshot(source_session)
            .ok_or("missing source migration state")?;
        let dgram = SessionDgramArgs {
            index: buffer,
            payload_offset: 0,
            payload_len: 1,
            urgent: false,
            return_node: NodeId::new(0),
        };
        for _ in 0..1024 {
            assert!(main
                .push_session_switch_pool_reply(
                    &source_runtime,
                    SessionSwitchPoolReply {
                        old_sh: old_handle,
                        new_sh: None,
                        old_thread: DataWorkerId::new(0),
                        new_thread: DataWorkerId::new(1),
                        tuple: (UdpWorker::ID, local, remote),
                        status: SessionSwitchPoolStatus::Rejected,
                        state: None,
                        dgram,
                    },
                )
                .is_ok());
        }
        source_udp.session_switch_pool_replies.push_back(SessionSwitchPoolReply {
            old_sh: old_handle,
            new_sh: None,
            old_thread: DataWorkerId::new(0),
            new_thread: DataWorkerId::new(1),
            tuple: (UdpWorker::ID, local, remote),
            status: SessionSwitchPoolStatus::Prepared,
            state: Some(state),
            dgram,
        });

        source_udp.drain_migration_replies(&mut source_sessions, &source_runtime);

        assert_eq!(source_udp.session_switch_pool_replies.len(), 1);
        assert_eq!(
            main.lookup_endpoint(local, remote, UdpWorker::ID),
            Some(old_handle)
        );
        Ok(())
    }

    #[test]
    fn udp_disconnect_removes_connection_and_session_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut sessions, mut udp, applications, _main) = worker_state();
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, None, None, None)?;
        let (local, remote) = test_endpoints();
        let connection_id =
            hammer_runtime::SessionConnectionId::from_raw(application_connection.raw());
        let session_id = udp.active_connect(&mut sessions, connection_id, local, remote)?;
        let (transport_id, index) = sessions
            .session_transport(session_id)
            .expect("UDP Session transport");
        assert_eq!(transport_id, UdpWorker::ID);

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        <UdpWorker as SessionTransport<PoolIndex>>::disconnect(
            &mut udp,
            &mut sessions,
            index,
            &runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        assert!(udp.connections.get(index).is_none());
        assert!(
            sessions
                .lookup_session_endpoint(UdpWorker::ID, local, remote)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn udp_tx_writes_datagram_buffer_without_consuming_untill_output_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut sessions, mut udp, applications, main) = worker_state();
        let application = applications.attach()?;
        sessions.install_application_mq_for_test(application)?;
        let application_listener = applications.register_listener(application, None, None)?;
        let (local, remote) = test_endpoints();
        let listener = main.listen(
            application_listener,
            SessionTransportRegistration::new(
                "udp-test",
                Some(noop_start_listen),
                Some(noop_stop_listen),
                None,
            ),
            SessionListenEndpoint::new(local, DataWorkerId::new(0)),
        )?;
        let udp_listener =
            UdpListener::new(local, listener, DataWorkerId::new(0)).expect("UDP listener");
        let (index, session_id) =
            udp.accept_datagram(&mut sessions, udp_listener, local, remote)?;
        let app = sessions
            .app_session(session_id)
            .expect("UDP Session owns an AppSession")
            .clone();
        app.send_datagram_to(local, remote, b"reply")?;

        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        udp.internal_tx(
            &mut sessions,
            session_id,
            index,
            &runtime,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        let buffer = frame
            .pending_indices()
            .iter()
            .copied()
            .next()
            .expect("UDP TX buffer");
        let packet = runtime.buffers().get_buffer(buffer)?.current().to_vec();
        assert_eq!(packet.len(), 13);
        assert_eq!(&packet[..2], &9000_u16.to_be_bytes());
        assert_eq!(&packet[2..4], &50000_u16.to_be_bytes());
        assert_eq!(&packet[4..6], &13_u16.to_be_bytes());
        assert_eq!(&packet[8..], b"reply");
        assert!(
            sessions.peek_tx_datagram(session_id)?.is_none(),
            "UDP TX FIFO is consumed after output enqueue"
        );
        Ok(())
    }
}
