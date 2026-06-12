use std::cell::RefCell;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, Node, NodeProcessFn, NodeRegistration, NodeResult,
    NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_infra::map::FlatHashTable;
use hammer_infra::timer_wheel::{TimerHandle, TimerStartError, TimerWheel2t1w2048};
use hammer_runtime::app::{
    AppBackend, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppFlowId, AppObjectRef,
    AppRegisteredBuffer, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown,
};

use super::{TcpConnectionTable, TcpDataPlaneConnection, TcpLookupId};

const DEFAULT_TCP_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpSessionTimerKind {
    Retransmit,
    Persist,
    OutputPacing,
}

#[derive(Debug)]
pub enum TcpAppCommand {
    Send(TcpAppSend),
    Recv(TcpAppRecv),
    Close(TcpAppClose),
    Shutdown(TcpAppShutdownCommand),
}

#[derive(Debug)]
pub struct TcpAppSend {
    connection_id: TcpConnectionId,
    descriptor: AppSqeDescriptor,
    registered: AppRegisteredBuffer,
}

impl TcpAppSend {
    #[inline]
    pub fn connection_id(&self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn descriptor(&self) -> &AppSqeDescriptor {
        &self.descriptor
    }

    #[inline]
    pub fn registered(&self) -> &AppRegisteredBuffer {
        &self.registered
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAppRecv {
    connection_id: TcpConnectionId,
    descriptor: AppSqeDescriptor,
    max_len: u32,
}

impl TcpAppRecv {
    #[inline]
    pub fn connection_id(self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }

    #[inline]
    pub fn max_len(self) -> u32 {
        self.max_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAppClose {
    connection_id: TcpConnectionId,
    descriptor: AppSqeDescriptor,
}

impl TcpAppClose {
    #[inline]
    pub fn connection_id(self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAppShutdownCommand {
    connection_id: TcpConnectionId,
    shutdown: AppTcpShutdown,
}

impl TcpAppShutdownCommand {
    #[inline]
    pub fn connection_id(self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn shutdown(self) -> AppTcpShutdown {
        self.shutdown
    }
}

#[derive(Clone, Debug)]
struct TcpSessionAppBackend {
    connection_id: TcpConnectionId,
    flow: AppFlowId,
    backend: AppBackend,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpSessionStep {
    pub app_submissions: usize,
    pub expired_timers: usize,
    pub ready_connections: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpSessionTimerSlot {
    connection_id: TcpConnectionId,
    kind: TcpSessionTimerKind,
    handle: TimerHandle,
    live: bool,
}

pub struct TcpSessionRuntime {
    worker: DataWorkerId,
    connections: TcpConnectionTable,
    ready_connections: hammer_infra::vec::Vec<TcpConnectionId>,
    ready_slots: FlatHashTable<u64, usize>,
    timer_wheel: TimerWheel2t1w2048,
    timer_slots: hammer_infra::vec::Vec<TcpSessionTimerSlot>,
    expired_timer_slots: hammer_infra::vec::Vec<u32>,
    pending_timers: hammer_infra::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)>,
    app_backends: hammer_infra::vec::Vec<TcpSessionAppBackend>,
    app_backend_slots: FlatHashTable<u64, usize>,
    app_commands: hammer_infra::vec::Vec<TcpAppCommand>,
    timer_tick_duration: Duration,
    last_timer_tick: Instant,
}

impl TcpSessionRuntime {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self::with_timer_clock(worker, DEFAULT_TCP_SESSION_TIMER_TICK, Instant::now())
    }

    pub fn with_timer_clock(
        worker: DataWorkerId,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            worker,
            connections: TcpConnectionTable::empty(),
            ready_connections: hammer_infra::vec::Vec::new(),
            ready_slots: FlatHashTable::new(),
            timer_wheel: TimerWheel2t1w2048::new(0),
            timer_slots: hammer_infra::vec::Vec::new(),
            expired_timer_slots: hammer_infra::vec::Vec::new(),
            pending_timers: hammer_infra::vec::Vec::new(),
            app_backends: hammer_infra::vec::Vec::new(),
            app_backend_slots: FlatHashTable::new(),
            app_commands: hammer_infra::vec::Vec::new(),
            timer_tick_duration,
            last_timer_tick,
        }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    pub fn install_connection(&mut self, connection: TcpDataPlaneConnection) -> CoreResult<()> {
        let connection_id = connection
            .connection_id()
            .ok_or_else(|| CoreError::internal("TCP session connection is missing id"))?;
        if connection.owner_worker() != self.worker {
            return Err(CoreError::internal(format!(
                "TCP session worker mismatch: connection owner={} session worker={}",
                connection.owner_worker().slot(),
                self.worker.slot()
            )));
        }
        self.connections.insert(connection);
        self.mark_connection_ready(connection_id);
        Ok(())
    }

    #[inline]
    pub fn connection(&self, connection_id: TcpConnectionId) -> Option<&TcpDataPlaneConnection> {
        self.connections.lookup_by_connection_id(connection_id)
    }

    #[inline]
    pub fn lookup_connection(&self, lookup_id: TcpLookupId) -> Option<&TcpDataPlaneConnection> {
        self.connections.lookup_by_lookup_id(lookup_id)
    }

    pub fn mark_connection_ready(&mut self, connection_id: TcpConnectionId) {
        if self.ready_slots.lookup(&connection_id.get()).is_some() {
            return;
        }
        let slot = self.ready_connections.len();
        self.ready_connections.push(connection_id);
        self.ready_slots.insert(connection_id.get(), slot);
    }

    pub fn take_ready_connections(&mut self) -> std::vec::Vec<TcpConnectionId> {
        let ready = self.ready_connections.iter().copied().collect();
        self.ready_connections.clear();
        self.ready_slots = FlatHashTable::new();
        ready
    }

    pub fn arm_timer_ticks(
        &mut self,
        connection_id: TcpConnectionId,
        kind: TcpSessionTimerKind,
        ticks: u64,
    ) -> CoreResult<()> {
        if self.connection(connection_id).is_none() {
            return Err(CoreError::internal(format!(
                "TCP session timer connection {} is missing",
                connection_id.get()
            )));
        }
        self.cancel_timer(connection_id, kind);
        let user_handle = u32::try_from(self.timer_slots.len())
            .map_err(|_| CoreError::internal("TCP session timer slot overflow"))?;
        let handle = self
            .timer_wheel
            .start(user_handle, ticks)
            .map_err(timer_start_error)?;
        self.timer_slots.push(TcpSessionTimerSlot {
            connection_id,
            kind,
            handle,
            live: true,
        });
        Ok(())
    }

    pub fn cancel_timer(
        &mut self,
        connection_id: TcpConnectionId,
        kind: TcpSessionTimerKind,
    ) -> bool {
        let Some(slot) = self.live_timer_slot(connection_id, kind) else {
            return false;
        };
        let timer = self
            .timer_slots
            .get_mut(slot)
            .expect("live timer slot should be valid");
        timer.live = false;
        self.timer_wheel.stop(timer.handle)
    }

    pub fn expire_timers(&mut self, ticks: u32) -> CoreResult<usize> {
        self.expired_timer_slots.clear();
        let expired = self
            .timer_wheel
            .expire(ticks, &mut self.expired_timer_slots);
        let expired_slots: std::vec::Vec<u32> = self.expired_timer_slots.iter().copied().collect();
        for slot in expired_slots {
            let Some(timer) = self.timer_slots.get_mut(slot as usize) else {
                return Err(CoreError::internal("TCP session timer slot is invalid"));
            };
            if !timer.live {
                continue;
            }
            timer.live = false;
            let connection_id = timer.connection_id;
            let kind = timer.kind;
            self.pending_timers.push((connection_id, kind));
            self.mark_connection_ready(connection_id);
        }
        Ok(expired)
    }

    pub fn dispatch_pending_timers_for_test(
        &mut self,
    ) -> std::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)> {
        self.pending_timers.drain(..).collect()
    }

    pub fn attach_app_backend(
        &mut self,
        connection_id: TcpConnectionId,
        backend: AppBackend,
    ) -> CoreResult<()> {
        if self.connection(connection_id).is_none() {
            return Err(CoreError::internal(format!(
                "TCP session app backend connection {} is missing",
                connection_id.get()
            )));
        }
        if self
            .app_backend_slots
            .lookup(&connection_id.get())
            .is_some()
        {
            return Err(CoreError::internal(format!(
                "TCP session app backend already attached for connection {}",
                connection_id.get()
            )));
        }
        let slot = self.app_backends.len();
        self.app_backends.push(TcpSessionAppBackend {
            connection_id,
            flow: backend.flow(),
            backend,
        });
        self.app_backend_slots.insert(connection_id.get(), slot);
        Ok(())
    }

    pub fn take_app_commands(&mut self) -> std::vec::Vec<TcpAppCommand> {
        self.app_commands.drain(..).collect()
    }

    pub fn poll_app_rings(&mut self) -> CoreResult<usize> {
        let backends: std::vec::Vec<TcpSessionAppBackend> =
            self.app_backends.iter().cloned().collect();
        let mut polled = 0usize;

        for app_backend in backends {
            while let Some(entry) = app_backend.backend.try_pop_submission_entry() {
                self.handle_app_submission(&app_backend, entry)?;
                polled += 1;
            }
            while let Some(shutdown) = app_backend.backend.try_pop_tcp_shutdown() {
                if shutdown.flow() != app_backend.flow {
                    return Err(CoreError::internal(format!(
                        "TCP app shutdown flow {} does not match attached flow {}",
                        shutdown.flow().value(),
                        app_backend.flow.value()
                    )));
                }
                self.app_commands
                    .push(TcpAppCommand::Shutdown(TcpAppShutdownCommand {
                        connection_id: app_backend.connection_id,
                        shutdown,
                    }));
                self.mark_connection_ready(app_backend.connection_id);
                polled += 1;
            }
        }

        Ok(polled)
    }

    fn handle_app_submission(
        &mut self,
        app_backend: &TcpSessionAppBackend,
        entry: AppSubmissionEntry,
    ) -> CoreResult<()> {
        let (descriptor, registered) = entry.into_parts();
        if descriptor.object() != AppObjectRef::Flow(app_backend.flow) {
            return Err(CoreError::internal(format!(
                "TCP app submission object {:?} does not match attached flow {}",
                descriptor.object(),
                app_backend.flow.value()
            )));
        }

        match descriptor.payload() {
            AppSqeData::Send { .. } => {
                let registered = registered.ok_or_else(|| {
                    CoreError::internal("TCP app send submission is missing registered buffer")
                })?;
                self.app_commands.push(TcpAppCommand::Send(TcpAppSend {
                    connection_id: app_backend.connection_id,
                    descriptor,
                    registered,
                }));
                self.mark_connection_ready(app_backend.connection_id);
            }
            AppSqeData::Recv { max_len } => {
                self.app_commands.push(TcpAppCommand::Recv(TcpAppRecv {
                    connection_id: app_backend.connection_id,
                    descriptor,
                    max_len,
                }));
                self.mark_connection_ready(app_backend.connection_id);
            }
            AppSqeData::Close => {
                self.app_commands.push(TcpAppCommand::Close(TcpAppClose {
                    connection_id: app_backend.connection_id,
                    descriptor,
                }));
                self.mark_connection_ready(app_backend.connection_id);
            }
            AppSqeData::Nop => {
                app_backend
                    .backend
                    .try_push_cqe_descriptor(AppCqeDescriptor::new(
                        descriptor.user_data(),
                        0,
                        AppCqeFlags::NONE,
                        AppObjectRef::Flow(app_backend.flow),
                        AppCqeData::None,
                    ))
                    .map_err(CoreError::from)?;
            }
            AppSqeData::Accept | AppSqeData::RecvFrom { .. } | AppSqeData::SendTo { .. } => {
                return Err(CoreError::internal(format!(
                    "unsupported TCP app submission opcode {:?}",
                    descriptor.opcode()
                )));
            }
        }

        Ok(())
    }

    pub fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<TcpSessionStep> {
        let app_submissions = self.poll_app_rings()?;
        let expired_timers = self.expire_timers(timer_ticks)?;
        let ready_connections = self.ready_connections.len();
        Ok(TcpSessionStep {
            app_submissions,
            expired_timers,
            ready_connections,
        })
    }

    pub fn poll_once_at(&mut self, now: Instant) -> CoreResult<TcpSessionStep> {
        let timer_ticks = self.elapsed_timer_ticks(now);
        self.poll_once_for_ticks(timer_ticks)
    }

    fn elapsed_timer_ticks(&mut self, now: Instant) -> u32 {
        if self.timer_tick_duration.is_zero() {
            self.last_timer_tick = now;
            return 0;
        }

        let elapsed = now.saturating_duration_since(self.last_timer_tick);
        let tick_nanos = self.timer_tick_duration.as_nanos();
        let elapsed_ticks = elapsed.as_nanos() / tick_nanos;
        let ticks = elapsed_ticks.min(u32::MAX as u128) as u32;
        if ticks == 0 {
            return 0;
        }

        if let Some(advance) = self.timer_tick_duration.checked_mul(ticks) {
            self.last_timer_tick += advance;
        } else {
            self.last_timer_tick = now;
        }
        ticks
    }
}

#[derive(Clone, Debug)]
pub struct TcpSessionNode {
    worker: DataWorkerId,
}

impl TcpSessionNode {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self { worker }
    }
}

impl Node for TcpSessionNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.clear();
        Ok(NodeResult::drop())
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_session_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(register_tcp_session_runtime(self.worker))
    }
}

impl DriverNode for TcpSessionNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("tcp-session-node", 0)
    }
}

thread_local! {
    static TCP_SESSION_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpSessionRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_tcp_session_runtime(worker: DataWorkerId) -> NodeRuntimeData {
    TCP_SESSION_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpSessionRuntime::new(worker));
        NodeRuntimeData::from_usize(slot).expect("TCP session runtime slot overflow")
    })
}

fn with_tcp_session_runtime<R>(
    data: NodeRuntimeData,
    f: impl FnOnce(&mut TcpSessionRuntime) -> CoreResult<R>,
) -> CoreResult<R> {
    let slot = data.usize_word(0)?;
    TCP_SESSION_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("TCP session runtimes borrowed"))?;
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP session runtime slot is invalid"))?;
        f(runtime)
    })
}

fn tcp_session_process(
    _runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    with_tcp_session_runtime(data, |session| {
        session.poll_once_at(Instant::now())?;
        Ok(NodeResult::drop())
    })
}

impl TcpSessionRuntime {
    fn live_timer_slot(
        &self,
        connection_id: TcpConnectionId,
        kind: TcpSessionTimerKind,
    ) -> Option<usize> {
        self.timer_slots.iter().position(|timer| {
            timer.live && timer.connection_id == connection_id && timer.kind == kind
        })
    }
}

fn timer_start_error(error: TimerStartError) -> CoreError {
    CoreError::internal(format!("start TCP session timer: {error:?}"))
}
