use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::{Arc, OnceLock, mpsc};

use hammer_core::data_plane::{BufferFrame, Index as BufferIndex, NodeHandle, NodeId, NodeState};
use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::{ThreadOwned, ThreadOwnedError};
use hammer_runtime::app::{SessionDgramHeader, SessionHandle};
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, GlobalMain, NodeRuntimeData, RuntimeError, RuntimeResult,
    SessionConnectEndpoint, SessionListenEndpoint, with_data_plane_main,
};
use hammer_service::session::SessionQueueNext;
use hammer_service::session::node::{SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionDgramArgs, SessionMigrateResult, SessionSwitchPoolArgs, SessionSwitchPoolClosed,
    SessionSwitchPoolCompletion, SessionSwitchPoolCompletionStatus, SessionSwitchPoolReply,
    SessionSwitchPoolStatus, SessionTransport, SessionWorker, TransportInternalTransport,
    TransportInternalTx, dispatch_session_queue_events, session_main,
};
use hammer_service::transport::{TransportVft, register_transport};

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
    ConnectionMissing { index: u32 },
    #[error("UDP listener {listener:?} is missing")]
    ListenerMissing { listener: SessionHandle },
    #[error("UDP endpoint {endpoint} is already in use")]
    EndpointInUse { endpoint: SocketAddr },
    #[error("UDP session {session_id:?} is missing")]
    SessionMissing { session_id: u32 },
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

#[repr(C)]
struct UdpWorkerSlot {
    cacheline0: CacheLineAlignMark,
    owner: ThreadOwned<UdpWorker>,
}

impl UdpWorkerSlot {
    fn new() -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            owner: ThreadOwned::new(),
        }
    }
}

impl Deref for UdpWorkerSlot {
    type Target = ThreadOwned<UdpWorker>;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

pub struct UdpMain {
    protocol: u8,
    listeners: Arc<UdpListenerCell>,
    workers: Box<[UdpWorkerSlot]>,
}

impl UdpMain {
    fn new(protocol: u8, worker_count: usize) -> Self {
        let workers = (0..worker_count)
            .map(|_| UdpWorkerSlot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            protocol,
            listeners: Arc::new(UdpListenerCell::new()),
            workers,
        }
    }

    pub const fn protocol(&self) -> u8 {
        self.protocol
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
        runtime: &DataPlaneMain,
        operation: impl FnOnce(&mut SessionWorker, &mut UdpWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        session_main().with_worker_mut(runtime, |sessions| {
            let thread_index = runtime.thread_index();
            let worker = DataWorkerId::try_from(thread_index)
                .map_err(|_| UdpTransportError::WorkerUnavailable { thread_index })?;
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
        runtime: &DataPlaneMain,
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

pub(crate) static UDP_MAIN: OnceLock<UdpMain> = OnceLock::new();

pub fn protocol() -> RuntimeResult<u8> {
    UDP_MAIN
        .get()
        .map(UdpMain::protocol)
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })
}

pub struct UdpWorker {
    protocol: u8,
    worker: DataWorkerId,
    connections: Pool<UdpConnection>,
    lookup: UdpLookup,
    session_switch_pool_replies: VecDeque<SessionSwitchPoolReply>,
    session_switch_pool_completions: VecDeque<SessionSwitchPoolCompletion>,
    session_switch_pool_closed: VecDeque<SessionSwitchPoolClosed>,
}

impl UdpWorker {
    pub(crate) fn new(worker: DataWorkerId, protocol: u8) -> Self {
        Self {
            protocol,
            worker,
            connections: Pool::with_capacity(UDP_CONNECTION_CAPACITY),
            lookup: UdpLookup::new(),
            session_switch_pool_replies: VecDeque::new(),
            session_switch_pool_completions: VecDeque::new(),
            session_switch_pool_closed: VecDeque::new(),
        }
    }

    fn insert_connection(&mut self, connection: UdpConnection) -> RuntimeResult<u32> {
        Ok(self.connections.insert(connection))
    }

    fn connection(&self, index: u32) -> Option<&UdpConnection> {
        self.connections.get(index)
    }

    fn connection_mut(&mut self, index: u32) -> Option<&mut UdpConnection> {
        self.connections.get_mut(index)
    }

    fn remove_connection(&mut self, index: u32) -> Option<UdpConnection> {
        self.connections.remove(index)
    }

    fn accept_datagram(
        &mut self,
        sessions: &mut SessionWorker,
        listener: UdpListener,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> RuntimeResult<(u32, u32)> {
        let connection = UdpConnection::connected(self.worker, local, remote)
            .ok_or(UdpTransportError::InvalidConnection)?;
        let index = self.insert_connection(connection)?;
        let session_id =
            match sessions.stream_accept(self.protocol, index, listener.session_listener()) {
                Ok(session_id) => session_id,
                Err(error) => {
                    self.remove_connection(index);
                    return Err(error);
                }
            };
        let connection = self
            .connection_mut(index)
            .ok_or(UdpTransportError::ConnectionMissing { index })?;
        if !connection.attach_session(session_id) {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::InvalidConnection.into());
        }
        self.lookup.insert_tuple(index, local, remote);
        if !sessions.insert_session_endpoint(session_id, self.protocol, local, remote)? {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::EndpointInUse { endpoint: local }.into());
        }
        let rollback = |sessions: &mut SessionWorker,
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
        Ok((index, session_id))
    }

    fn active_connect(
        &mut self,
        sessions: &mut SessionWorker,
        connection: u32,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> RuntimeResult<u32> {
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
        let session_id = match sessions.stream_connect_pending(self.protocol, index, connection) {
            Ok(session_id) => session_id,
            Err(error) => {
                self.remove_connection(index);
                return Err(error);
            }
        };
        let connection = self
            .connection_mut(index)
            .ok_or(UdpTransportError::ConnectionMissing { index })?;
        if !connection.attach_session(session_id) {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::InvalidConnection.into());
        }
        self.lookup.insert_tuple(index, local, remote);
        if !sessions.insert_session_endpoint(session_id, self.protocol, local, remote)? {
            self.rollback_accept(sessions, session_id, index, local, remote)?;
            return Err(UdpTransportError::EndpointInUse { endpoint: local }.into());
        }
        if let Err(error) = sessions.complete_stream_connect(session_id) {
            if let Err(cleanup_error) =
                self.rollback_accept(sessions, session_id, index, local, remote)
            {
                tracing::error!(
                    ?session_id,
                    %cleanup_error,
                    "UDP connect App publication rollback failed"
                );
            }
            return Err(error);
        }
        Ok(session_id)
    }

    fn rollback_accept(
        &mut self,
        sessions: &mut SessionWorker,
        session_id: u32,
        index: u32,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> RuntimeResult<()> {
        self.lookup.remove_tuple(local, remote);
        let _ = sessions.remove_session_endpoint(self.protocol, local, remote)?;
        self.remove_connection(index);
        sessions.rollback_session_creation(session_id)?;
        Ok(())
    }

    fn deliver_datagram(
        &mut self,
        sessions: &mut SessionWorker,
        runtime: &DataPlaneMain,
        index: BufferIndex,
        local: SocketAddr,
        remote: SocketAddr,
        payload_offset: usize,
        payload_len: usize,
        urgent: bool,
        listener: Option<UdpListener>,
        return_node: NodeId,
    ) -> RuntimeResult<UdpDelivery> {
        let endpoint = sessions.lookup_session_endpoint(self.protocol, local, remote)?;
        if let Some(connection_index) = self.lookup.find_tuple(&self.connections, local, remote) {
            if endpoint.is_some_and(|handle| handle.thread_index != self.worker.thread_index()) {
                return Ok(UdpDelivery::WrongWorker);
            }
            let session_id = self
                .connection(connection_index)
                .and_then(|connection| connection.session())
                .ok_or(UdpTransportError::SessionMissing {
                    session_id: u32::from(connection_index),
                })?;
            let header = SessionDgramHeader::new(local, remote, payload_len)
                .ok_or(UdpTransportError::InvalidConnection)?;
            let written = sessions.enqueue_datagram_rx_from_buffer_at(
                runtime.buffers(),
                session_id,
                index,
                payload_offset,
                header,
            )?;
            return Ok(if written == 0 {
                UdpDelivery::FifoFull
            } else {
                UdpDelivery::Delivered
            });
        }
        if let Some(handle) = sessions.lookup_session_endpoint(self.protocol, local, remote)? {
            if handle.thread_index != self.worker.thread_index() {
                let result = sessions.program_thread_migration(
                    runtime,
                    self.worker,
                    handle,
                    (self.protocol, local, remote),
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
        )?;
        Ok(if written == 0 {
            UdpDelivery::FifoFull
        } else {
            UdpDelivery::Delivered
        })
    }

    fn migration_request_is_current(
        &self,
        sessions: &SessionWorker,
        args: &SessionSwitchPoolArgs,
    ) -> bool {
        let Some(session_id) = sessions.session_id_from_handle(args.old_sh) else {
            return false;
        };
        let Some(index) = sessions.transport_connection_index(session_id) else {
            return false;
        };
        let (tuple_transport, local, remote) = args.tuple;
        tuple_transport == self.protocol
            && self.lookup.find_tuple(&self.connections, local, remote) == Some(index)
            && self
                .connection(index)
                .is_some_and(|connection| connection.session() == Some(session_id))
    }

    fn rejected_migration_reply(
        &self,
        sessions: &SessionWorker,
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
        sessions: &SessionWorker,
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
        sessions: &SessionWorker,
        runtime: &DataPlaneMain,
        reply: SessionSwitchPoolReply,
    ) {
        if let Err(reply) = sessions.push_session_switch_pool_reply(runtime, reply) {
            self.session_switch_pool_replies.push_back(reply);
        }
    }

    fn publish_migration_completion(
        &mut self,
        sessions: &SessionWorker,
        runtime: &DataPlaneMain,
        completion: SessionSwitchPoolCompletion,
    ) {
        if let Err(completion) = sessions.push_session_switch_pool_completion(runtime, completion) {
            self.session_switch_pool_completions.push_back(completion);
        }
    }

    fn publish_migration_closed(
        &mut self,
        sessions: &SessionWorker,
        runtime: &DataPlaneMain,
        closed: SessionSwitchPoolClosed,
    ) {
        if let Err(closed) = sessions.push_session_switch_pool_closed(runtime, closed) {
            self.session_switch_pool_closed.push_back(closed);
        }
    }

    fn handoff_migration_datagram(
        &self,
        runtime: &DataPlaneMain,
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
        runtime: &DataPlaneMain,
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
        sessions: &mut SessionWorker,
        runtime: &DataPlaneMain,
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
            let Some(connection) = UdpConnection::connected(self.worker, local, remote) else {
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
            let inserted = attached && self.lookup.insert_tuple(connection_index, local, remote);
            let published = inserted
                && sessions
                    .publish_session_migration(new_handle, transport, local, remote)
                    .unwrap_or(false);
            let accepted = published && sessions.accept_migrated_session(session_id).is_ok();
            if !accepted {
                if published {
                    let _ =
                        sessions.replace_session_endpoint(reply.old_sh, transport, local, remote);
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
        sessions: &mut SessionWorker,
        runtime: &DataPlaneMain,
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
        let Some(index) = sessions.transport_connection_index(session_id) else {
            self.publish_migration_closed(sessions, runtime, closed);
            return true;
        };
        if tuple_transport != self.protocol
            || !sessions.owns_transport_session(session_id, self.protocol)
            || self.lookup.find_tuple(&self.connections, local, remote) != Some(index)
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
                .remove_session_endpoint(self.protocol, local, remote)
                .is_err()
        {
            return false;
        }
        self.lookup.remove_tuple(local, remote);
        let _ = self.remove_connection(index);
        true
    }

    fn process_migration_closed(
        &mut self,
        sessions: &mut SessionWorker,
        closed: SessionSwitchPoolClosed,
    ) -> bool {
        let Some(session_id) = sessions.session_id_from_handle(closed.new_sh) else {
            return true;
        };
        let Some(index) = sessions.transport_connection_index(session_id) else {
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
            .remove_session_endpoint(self.protocol, local, remote)
            .is_err()
        {
            return false;
        }
        self.lookup.remove_tuple(local, remote);
        let _ = self.remove_connection(index);
        sessions.remove_migrated_session(session_id).is_ok()
    }

    fn drain_migration_closed(&mut self, sessions: &mut SessionWorker, runtime: &DataPlaneMain) {
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

    fn drop_migration_datagram(runtime: &DataPlaneMain, dgram: SessionDgramArgs) {
        runtime
            .buffers()
            .drop_index_owned_with_trace(dgram.index, |_| {});
    }

    fn drain_migration_shutdown_requests(
        &mut self,
        sessions: &SessionWorker,
        runtime: &DataPlaneMain,
    ) {
        while let Some(args) = sessions.pop_session_migrate_request() {
            let _ = sessions.cancel_thread_migration(args.old_sh, args.tuple);
            Self::drop_migration_datagram(runtime, args.dgram);
        }
    }

    fn drain_migration_shutdown_replies(
        &mut self,
        sessions: &SessionWorker,
        runtime: &DataPlaneMain,
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

    fn drain_migration_shutdown(&mut self, sessions: &mut SessionWorker, runtime: &DataPlaneMain) {
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
        sessions: &mut SessionWorker,
        runtime: &DataPlaneMain,
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

    fn drain_migration_replies(&mut self, sessions: &mut SessionWorker, runtime: &DataPlaneMain) {
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

    fn drain_migration_requests(&mut self, sessions: &mut SessionWorker, runtime: &DataPlaneMain) {
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
    listener: SessionHandle,
    _: u32,
    _: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<u32> {
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
    let connection_index = listener.session_index;
    main.listeners.get_mut().push(udp_listener);
    Ok(connection_index)
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

pub(crate) fn stop_listen(connection_index: u32) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    let listeners = main.listeners.get_mut();
    let slot = listeners
        .iter()
        .position(|candidate| candidate.session_listener().session_index == connection_index)
        .ok_or(UdpTransportError::ListenerMissing {
            listener: SessionHandle::new(connection_index, 0),
        })?;
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
    GlobalMain::with_current(|engine| {
        engine.schedule_on_worker(worker, move || {
            let result = with_data_plane_main(|runtime| {
                main.with_worker(runtime, |sessions, udp| {
                    udp.active_connect(sessions, endpoint.connection, local, endpoint.remote)
                })
            });
            if completion.send(result).is_err() {
                return;
            }
        })
    })
    .ok_or(RuntimeError::WorkerControlRequiresGlobalMain)??;
    let _ = completed
        .recv()
        .map_err(|_| RuntimeError::DataWorkerCallCanceled {
            worker: worker_slot,
        })??;
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "udp_init",
    runs_after = ["transport_main_init", "session_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_udp(engine: &mut GlobalMain) -> RuntimeResult<()> {
    if UDP_MAIN.get().is_some() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "udp" });
    }
    let protocol = register_transport(TransportVft::new(
        Some(start_listen),
        Some(stop_listen),
        Some(connect),
        None,
        None,
        None,
        None,
        None,
    ))
    .map_err(RuntimeError::from)?;
    let main = UdpMain::new(protocol, engine.configured_worker_count());
    UDP_MAIN
        .set(main)
        .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    Ok(())
}

fn bind_worker_graph(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue =
        engine
            .node_by_name("session-queue")
            .ok_or(UdpTransportError::NodeMissing {
                name: "session-queue",
            })?;
    let udp_output =
        engine
            .node_by_name(UdpOutputNode::NODE_NAME)
            .ok_or(UdpTransportError::NodeMissing {
                name: UdpOutputNode::NODE_NAME,
            })?;
    let session_queue_data = engine.nodes().node_runtime_data(session_queue)?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(engine, session_queue, udp_output)?;
    SessionQueueNode::install_worker_attachment(
        engine,
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
        .install(UdpWorker::new(worker, main.protocol))
        .is_err()
    {
        return Err(UdpTransportError::WorkerAlreadyInstalled {
            worker: worker.slot(),
        }
        .into());
    }
    engine
        .nodes()
        .set_node_state(session_queue, NodeState::Polling)?;
    Ok(())
}

#[hammer_component_macros::worker_init_function(
    name = "udp_worker_init",
    runs_after = ["session_worker_init"]
)]
fn init_udp_worker(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    bind_worker_graph(engine)?;
    engine.register_worker_exit_function(udp_worker_exit);
    Ok(())
}

fn udp_worker_exit(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let main = UDP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    let runtime = engine.clone();
    main.with_worker(&runtime, |sessions, udp| {
        udp.drain_migration_shutdown(sessions, &runtime);
        Ok(())
    })
}

fn udp_session_queue_update_time(
    runtime: &DataPlaneMain,
    _: &mut SessionWorker,
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
    runtime: &DataPlaneMain,
    _: &mut SessionWorker,
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

impl SessionTransport for UdpWorker {
    type Tx = TransportInternalTx;

    #[inline]
    fn protocol(&self) -> u8 {
        self.protocol
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker,
        runtime: &DataPlaneMain,
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
        sessions: &mut SessionWorker,
        index: u32,
        _: &DataPlaneMain,
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
            self.protocol,
            connection.local(),
            connection.remote(),
        )?;
        self.connections.remove(index);
        sessions.notify_transport_deleted(session_id, index)?;
        Ok(())
    }
}

impl TransportInternalTransport for UdpWorker {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker,
        session_id: u32,
        index: u32,
        runtime: &DataPlaneMain,
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
