use std::cell::UnsafeCell;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, mpsc};

use hammer_core::data_plane::{BufferFrame, Index as BufferIndex, NodeState};
use hammer_infra::align::CacheLine;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::thread_owned::{ThreadOwned, ThreadOwnedError};
use hammer_runtime::app::{SessionDgramHeader, SessionHandle};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Engine, NodeRuntimeData, RuntimeError, RuntimeResult,
    SessionConnectEndpoint, SessionConnectionId, SessionListenEndpoint, SessionListenerId,
    with_data_plane_runtime,
};
use hammer_service::session::node::{SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionMain, SessionTransport, SessionTransportId, SessionWorker, TransportInternalTransport,
    TransportInternalTx, dispatch_session_queue_events,
};
use hammer_service::session::{SessionId, SessionQueueNext};

use crate::UdpIpVersion;
use crate::connection::{UdpConnection, UdpListener};
use crate::lookup::{UdpLookup, UdpSessionLookup};
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
    session_lookup: Arc<UdpSessionLookup>,
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
            session_lookup: Arc::new(UdpSessionLookup::new()),
            workers,
        }
    }

    pub(crate) fn session_lookup(&self) -> Arc<UdpSessionLookup> {
        Arc::clone(&self.session_lookup)
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
    session_lookup: Arc<UdpSessionLookup>,
}

impl UdpWorker {
    pub(crate) fn new(worker: DataWorkerId, session_lookup: Arc<UdpSessionLookup>) -> Self {
        Self {
            worker,
            connections: Pool::with_capacity(UDP_CONNECTION_CAPACITY),
            lookup: UdpLookup::new(),
            session_lookup,
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

    fn insert_session_lookup(&self, session_id: SessionId, local: SocketAddr, remote: SocketAddr) {
        let handle = SessionHandle::new(session_id.pool_index().slot(), self.worker.slot() as u32);
        self.session_lookup.insert(local, remote, handle.raw());
    }

    fn remove_session_lookup(&self, local: SocketAddr, remote: SocketAddr) {
        self.session_lookup.remove(local, remote);
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
        self.insert_session_lookup(session_id, local, remote);
        let rollback = |sessions: &mut SessionWorker<PoolIndex>,
                        udp: &mut UdpWorker,
                        session_id,
                        index,
                        local,
                        remote| {
            udp.rollback_accept(sessions, session_id, index, local, remote)
        };
        let initial = match sessions.connection_published(session_id) {
            Ok(initial) => initial,
            Err(error) => {
                if let Err(cleanup_error) =
                    rollback(sessions, self, session_id, index, local, remote)
                {
                    tracing::error!(
                        ?session_id,
                        %cleanup_error,
                        "UDP accept publication rollback failed"
                    );
                }
                return Err(error);
            }
        };
        if initial {
            if let Err(error) = sessions.connected(session_id) {
                if let Err(cleanup_error) =
                    rollback(sessions, self, session_id, index, local, remote)
                {
                    tracing::error!(
                        ?session_id,
                        %cleanup_error,
                        "UDP accept App publication rollback failed"
                    );
                }
                return Err(error);
            }
        }
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
        let session_id = match sessions.stream_connect(UdpWorker::ID, index, connection) {
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
        self.insert_session_lookup(session_id, local, remote);
        let initial = match sessions.connection_published(session_id) {
            Ok(initial) => initial,
            Err(error) => {
                self.rollback_accept(sessions, session_id, index, local, remote)?;
                return Err(error);
            }
        };
        if initial {
            if let Err(error) = sessions.connected(session_id) {
                self.rollback_accept(sessions, session_id, index, local, remote)?;
                return Err(error);
            }
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
        self.remove_session_lookup(local, remote);
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
    ) -> RuntimeResult<UdpDelivery> {
        if let Some(connection_index) = self.lookup.find_tuple(&self.connections, local, remote) {
            let session_id = self
                .connection(connection_index)
                .and_then(|connection| connection.session())
                .ok_or(UdpTransportError::SessionMissing {
                    session_id: SessionId::from(connection_index),
                })?;
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
        if let Some(handle) = self.session_lookup.lookup(local, remote) {
            let handle = SessionHandle::from(handle);
            if handle.worker_index() != self.worker.slot() as u32 {
                return Ok(UdpDelivery::WrongWorker);
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
    let session_lookup = main.session_lookup();
    if main
        .worker(worker)?
        .install(UdpWorker::new(worker, session_lookup))
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
    bind_worker_graph(engine)
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
        _: &mut SessionWorker<PoolIndex>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        _: std::time::Instant,
    ) -> RuntimeResult<()> {
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
        self.remove_session_lookup(connection.local(), connection.remote());
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
        let udp_main = UdpMain::new(1, Arc::clone(&main));
        let session_lookup = udp_main.session_lookup();
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
            UdpWorker::new(DataWorkerId::new(0), session_lookup),
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
        let mut udp_other = UdpWorker::new(DataWorkerId::new(1), Arc::clone(&udp.session_lookup));
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
        )?;
        assert_eq!(delivery, UdpDelivery::WrongWorker);
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
        assert!(udp.session_lookup.lookup(local, remote).is_none());
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
