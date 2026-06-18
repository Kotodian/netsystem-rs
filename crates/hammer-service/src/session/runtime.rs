use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, RouteMetadata,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Pool;
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::session::{
    SessionAppRuntime, SessionId, SessionReadyQueue, SessionTimerExpiry, SessionTimerToken,
    SessionTimerWheel,
};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionQueueStep {
    pub(crate) expired_timers: usize,
    pub(crate) ready_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    ready: SessionReadyQueue,
    timers: SessionTimerWheel,
    pending_timer_expiries: hammer_infra::vec::Vec<SessionTimerExpiry>,
    timer_tick_duration: Duration,
    last_timer_tick: Instant,
}

impl WorkerSessionRuntime {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self::with_timer_clock(worker, DEFAULT_SESSION_TIMER_TICK, Instant::now())
    }

    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            worker,
            ready: SessionReadyQueue::new(),
            timers: SessionTimerWheel::new(),
            pending_timer_expiries: hammer_infra::vec::Vec::new(),
            timer_tick_duration,
            last_timer_tick,
        }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: SessionId) {
        self.ready.mark_ready(session_id);
    }

    #[inline]
    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        self.ready.take_ready_sessions()
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.clear_pending_timer_expiry(session_id, token);
        self.timers.arm_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.clear_pending_timer_expiry(session_id, token);
        self.timers.cancel(session_id, token)
    }

    pub(crate) fn expire_timers(&mut self, ticks: u32) -> CoreResult<usize> {
        let expired = self.timers.expire(ticks, &mut self.ready)?;
        self.pending_timer_expiries
            .extend(self.timers.take_expiries());
        Ok(expired)
    }

    pub(crate) fn take_timer_expiries(&mut self) -> hammer_infra::vec::Vec<SessionTimerExpiry> {
        self.pending_timer_expiries.drain(..).collect()
    }

    fn clear_pending_timer_expiry(&mut self, session_id: SessionId, token: SessionTimerToken) {
        let pending = self
            .pending_timer_expiries
            .drain(..)
            .collect::<hammer_infra::vec::Vec<_>>();
        for expiry in pending {
            if expiry.session_id() != session_id || expiry.token() != token {
                self.pending_timer_expiries.push(expiry);
            }
        }
    }

    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let expired_timers = self.expire_timers(timer_ticks)?;
        Ok(SessionQueueStep {
            expired_timers,
            ready_sessions: self.ready.len(),
        })
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

pub(crate) struct SessionDriverRuntime<S> {
    sessions: WorkerSessionRuntime,
    entries: Pool<SessionEntry<S>>,
    app: SessionAppRuntime,
    buffers: DataPlaneBuffers,
}

pub(crate) trait SessionQueueProtocol<S> {
    fn handle_timer_expiry(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, S>,
        expiry: SessionTimerExpiry,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<()>;

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, S>,
        session_id: SessionId,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<()>;

    fn tx_payload_len(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, S>,
        session_id: SessionId,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<usize>;

    fn prepare_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, S>,
        session_id: SessionId,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;

    fn cancel_tx(&mut self, index: BufferIndex);

    fn commit_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, S>,
        session_id: SessionId,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;
}

pub(crate) trait SessionStateFactory<S> {
    fn build(self, session_id: SessionId) -> S;
}

impl<S, F> SessionStateFactory<S> for F
where
    F: FnOnce(SessionId) -> S,
{
    #[inline]
    fn build(self, session_id: SessionId) -> S {
        self(session_id)
    }
}

#[derive(Debug)]
pub(crate) struct SessionEntry<S> {
    app_op: Option<AppOpId>,
    state: S,
}

impl<S> SessionEntry<S> {
    #[inline]
    pub(crate) fn new(state: S) -> Self {
        Self {
            app_op: None,
            state,
        }
    }

    #[inline]
    pub(crate) fn app_op(&self) -> Option<AppOpId> {
        self.app_op
    }

    #[inline]
    pub(crate) fn bind_app_op(&mut self, op: AppOpId) {
        self.app_op = Some(op);
    }

    #[inline]
    pub(crate) fn state(&self) -> &S {
        &self.state
    }

    #[inline]
    pub(crate) fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }
}

impl<S> SessionDriverRuntime<S> {
    #[inline]
    pub(crate) fn new(worker: DataWorkerId, buffers: DataPlaneBuffers) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            app: SessionAppRuntime::new(),
            buffers,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            sessions: WorkerSessionRuntime::with_timer_clock(
                worker,
                timer_tick_duration,
                last_timer_tick,
            ),
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            app: SessionAppRuntime::new(),
            buffers,
        }
    }

    #[inline]
    pub(crate) fn worker(&self) -> DataWorkerId {
        self.sessions.worker()
    }

    #[inline]
    pub(crate) fn mark_ready(&mut self, session_id: SessionId) {
        self.sessions.mark_ready(session_id);
    }

    #[inline]
    pub(crate) fn insert_session(&mut self, state: S) -> SessionId {
        let index = self
            .entries
            .insert(SessionEntry::new(state))
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> SessionId
    where
        F: SessionStateFactory<S>,
    {
        let index = self
            .entries
            .insert_with(|index| {
                let session_id = SessionId::from(index);
                SessionEntry::new(f.build(session_id))
            })
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub(crate) fn session(&self, id: SessionId) -> Option<&SessionEntry<S>> {
        self.entries.get(id.pool_index())
    }

    #[inline]
    pub(crate) fn session_mut(&mut self, id: SessionId) -> Option<&mut SessionEntry<S>> {
        self.entries.get_mut(id.pool_index())
    }

    #[inline]
    pub(crate) fn session_state(&self, id: SessionId) -> Option<&S> {
        self.session(id).map(SessionEntry::state)
    }

    #[inline]
    pub(crate) fn session_state_mut(&mut self, id: SessionId) -> Option<&mut S> {
        self.session_mut(id).map(SessionEntry::state_mut)
    }

    pub(crate) fn replace_session_state(&mut self, id: SessionId, state: S) -> Option<S> {
        let entry = self.session_mut(id)?;
        Some(std::mem::replace(entry.state_mut(), state))
    }

    pub(crate) fn remove_session(&mut self, id: SessionId) -> Option<SessionEntry<S>> {
        let removed = self.entries.remove(id.pool_index())?;
        if let Some(op) = removed.app_op() {
            self.app.unbind_ring(op);
        }
        Some(removed)
    }

    pub(crate) fn close_session(&mut self, id: SessionId) -> CoreResult<Option<SessionEntry<S>>> {
        let Some(entry) = self.session(id) else {
            return Ok(None);
        };
        if let Some(op) = entry.app_op() {
            self.app.complete_closed(op)?;
        }
        Ok(self.remove_session(id))
    }

    #[inline]
    pub(crate) fn bind_session_app_ring(
        &mut self,
        id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        let Some(entry) = self.session_mut(id) else {
            return false;
        };
        entry.bind_app_op(op);
        self.app.bind_ring(id, op, ring);
        true
    }

    #[inline]
    pub(crate) fn app(&self) -> &SessionAppRuntime {
        &self.app
    }

    #[inline]
    pub(crate) fn app_mut(&mut self) -> &mut SessionAppRuntime {
        &mut self.app
    }

    #[inline]
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    pub(crate) fn poll_app(&mut self) -> CoreResult<()> {
        self.app.drain_submissions()?;
        let mut ready = hammer_infra::vec::Vec::new();
        self.app.take_ready_send_sessions(&mut ready);
        for close in self.app.pending_closes() {
            if !ready
                .iter()
                .any(|session_id| *session_id == close.session_id())
            {
                ready.push(close.session_id());
            }
        }
        for session_id in ready {
            self.mark_ready(session_id);
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn enqueue_rx(
        &self,
        session_id: SessionId,
        index: BufferIndex,
        fin: bool,
    ) -> CoreResult<bool> {
        let Some(op) = self.session(session_id).and_then(SessionEntry::app_op) else {
            self.buffers.free_index(index);
            return Ok(false);
        };
        self.app
            .complete_recv(op, self.buffers.clone(), index, fin)?;
        Ok(true)
    }

    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        self.sessions.poll_once_for_ticks(timer_ticks)
    }

    pub(crate) fn poll_once_at(&mut self, now: Instant) -> CoreResult<SessionQueueStep> {
        let timer_ticks = self.sessions.elapsed_timer_ticks(now);
        self.poll_once_for_ticks(timer_ticks)
    }

    pub(crate) fn take_timer_expiries(&mut self) -> hammer_infra::vec::Vec<SessionTimerExpiry> {
        self.sessions.take_timer_expiries()
    }

    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        self.sessions.take_ready_sessions()
    }

    pub(crate) fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.sessions.arm_timer_ticks(session_id, token, ticks)
    }

    pub(crate) fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.sessions.cancel_timer(session_id, token)
    }
}

fn flush_one_session_tx<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    session_id: SessionId,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
) -> CoreResult<bool>
where
    P: SessionQueueProtocol<S>,
{
    let Some(pending_len) = driver.app().pending_send_len(session_id)? else {
        return Ok(false);
    };
    let payload_len = {
        let mut context = crate::session::protocol::SessionQueueControlContext::new(driver);
        protocol
            .tx_payload_len(&mut context, session_id, pending_len, now)?
            .min(pending_len)
    };
    if payload_len == 0 {
        return Ok(false);
    }
    let Some(payload) = driver
        .app()
        .copy_pending_send_bytes(session_id, payload_len)?
    else {
        return Err(CoreError::internal("session app tx progress is missing"));
    };
    if payload.is_empty() {
        let completed = driver.app_mut().commit_pending_send_bytes(session_id, 0)?;
        return Ok(completed);
    }

    let index = driver.buffers().alloc_index(RouteMetadata::default())?;
    if let Err(err) = driver.buffers().append(index, payload.as_slice()) {
        driver.buffers().free_index(index);
        return Err(err);
    }
    let payload_len = payload.len();

    {
        let mut context = crate::session::protocol::SessionQueueControlContext::new(driver);
        if let Err(err) = protocol.prepare_tx(&mut context, session_id, index, payload_len, now) {
            context.buffers().free_index(index);
            return Err(err);
        }
    }

    if let Err(err) = output.enqueue(runtime, output_next.node(), index) {
        protocol.cancel_tx(index);
        driver.buffers().free_index(index);
        return Err(err);
    }

    let commit_result = {
        let mut context = crate::session::protocol::SessionQueueControlContext::new(driver);
        protocol.commit_tx(&mut context, session_id, index, payload_len, now)
    };
    if let Err(err) = commit_result {
        protocol.cancel_tx(index);
        return Err(err);
    }
    let completed = driver
        .app_mut()
        .commit_pending_send_bytes(session_id, payload_len)?;
    if !completed {
        driver.mark_ready(session_id);
    }
    Ok(true)
}

#[cfg(test)]
pub(crate) fn dispatch_session_queue_for_ticks<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    timer_ticks: u32,
    output_next: crate::session::SessionQueueNext,
) -> CoreResult<SessionQueueStep>
where
    P: SessionQueueProtocol<S>,
{
    let mut step = driver.poll_once_for_ticks(timer_ticks)?;
    let now = Instant::now();
    let mut output = crate::session::node::SessionQueueOutput::default();
    dispatch_session_queue_pending(
        runtime,
        driver,
        protocol,
        output_next,
        &mut output,
        &mut step,
        now,
    )?;
    output.schedule(runtime)?;
    Ok(step)
}

pub(crate) fn dispatch_session_queue_once_at<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    now: Instant,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<SessionQueueStep>
where
    P: SessionQueueProtocol<S>,
{
    let mut step = driver.poll_once_at(now)?;
    dispatch_session_queue_pending(
        runtime,
        driver,
        protocol,
        output_next,
        output,
        &mut step,
        now,
    )?;
    Ok(step)
}

pub(crate) fn dispatch_session_queue_pending<S, P>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S>,
    protocol: &mut P,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    step: &mut SessionQueueStep,
    now: Instant,
) -> CoreResult<()>
where
    P: SessionQueueProtocol<S>,
{
    driver.poll_app()?;
    let expiries = driver.take_timer_expiries();
    for expiry in expiries {
        let mut context = crate::session::protocol::SessionQueueControlContext::new(driver);
        protocol.handle_timer_expiry(runtime, &mut context, expiry, output_next, output)?;
    }
    let ready_sessions = driver.take_ready_sessions();
    step.ready_sessions = ready_sessions.len();
    for session_id in ready_sessions {
        while flush_one_session_tx(
            runtime,
            driver,
            protocol,
            session_id,
            output_next,
            output,
            now,
        )? {}
        let mut context = crate::session::protocol::SessionQueueControlContext::new(driver);
        protocol.handle_ready_session(runtime, &mut context, session_id, output_next, output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferFrame, InternalNode, Node, NodeId, NodeProcessFn, NodeRegistration, NodeResult,
        NodeRuntimeData,
    };
    use hammer_runtime::app::{AppRingHandle, AppSendData};

    use crate::session::protocol::SessionQueueControlContext;

    fn infra_vec<T>(items: impl IntoIterator<Item = T>) -> hammer_infra::vec::Vec<T> {
        let mut values = hammer_infra::vec::Vec::new();
        for item in items {
            values.push(item);
        }
        values
    }

    #[derive(Debug, Clone)]
    struct FakeTxState {
        prepared: usize,
        committed: usize,
        canceled: usize,
    }

    fn fake_tx_state() -> FakeTxState {
        FakeTxState {
            prepared: 0,
            committed: 0,
            canceled: 0,
        }
    }

    struct FakeTxProtocol;

    impl SessionQueueProtocol<FakeTxState> for FakeTxProtocol {
        fn handle_timer_expiry(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionTimerExpiry,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionId,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn tx_payload_len(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionId,
            pending_len: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(pending_len.min(4))
        }

        fn prepare_tx(
            &mut self,
            context: &mut SessionQueueControlContext<'_, FakeTxState>,
            session_id: SessionId,
            _: BufferIndex,
            payload_len: usize,
            _: Instant,
        ) -> CoreResult<()> {
            let state = context
                .session_state_mut(session_id)
                .ok_or_else(|| CoreError::internal("missing fake state"))?;
            state.prepared += payload_len;
            Ok(())
        }

        fn cancel_tx(&mut self, _: BufferIndex) {}

        fn commit_tx(
            &mut self,
            context: &mut SessionQueueControlContext<'_, FakeTxState>,
            session_id: SessionId,
            _: BufferIndex,
            payload_len: usize,
            _: Instant,
        ) -> CoreResult<()> {
            let state = context
                .session_state_mut(session_id)
                .ok_or_else(|| CoreError::internal("missing fake state"))?;
            state.committed += payload_len;
            Ok(())
        }
    }

    struct NoTxPayloadProtocol;

    impl SessionQueueProtocol<FakeTxState> for NoTxPayloadProtocol {
        fn handle_timer_expiry(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionTimerExpiry,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionId,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn tx_payload_len(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionId,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(0)
        }

        fn prepare_tx(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionId,
            _: BufferIndex,
            _: usize,
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx prepare must not run"))
        }

        fn cancel_tx(&mut self, _: BufferIndex) {}

        fn commit_tx(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionId,
            _: BufferIndex,
            _: usize,
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx commit must not run"))
        }
    }

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must use descriptor process",
            ))
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {
        fn node_registration(&self) -> NodeRegistration
        where
            Self: Sized,
        {
            NodeRegistration::Plain
        }
    }

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let slot = data.usize_word(0)?;
        let state = {
            let states = capture_states().lock().expect("capture registry");
            Arc::clone(
                states
                    .get(slot)
                    .ok_or_else(|| CoreError::internal("capture slot is invalid"))?,
            )
        };
        let mut state = state.lock().expect("capture state");
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index)?;
            state.packets.push(packet.to_vec());
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    #[test]
    fn worker_session_runtime_expires_timer_into_expiry_and_ready_session() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(9);
        let token = SessionTimerToken::new(17);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 3)
            .expect("arm session timer");

        assert_eq!(runtime.expire_timers(2).expect("expire before deadline"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(runtime.expire_timers(1).expect("expire at deadline"), 1);
        assert_eq!(
            runtime.take_timer_expiries(),
            infra_vec([SessionTimerExpiry::new(session_id, token)])
        );
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
    }

    #[test]
    fn worker_session_runtime_rearming_same_timer_suppresses_stale_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(10);
        let token = SessionTimerToken::new(3);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 2)
            .expect("arm first timer");
        runtime
            .arm_timer_ticks(session_id, token, 5)
            .expect("rearm timer");

        assert_eq!(runtime.expire_timers(2).expect("expire stale timer"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(runtime.expire_timers(3).expect("expire rearmed timer"), 1);
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
    }

    #[test]
    fn worker_session_runtime_cancel_timer_suppresses_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(11);
        let token = SessionTimerToken::new(4);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 2)
            .expect("arm timer");
        assert!(runtime.cancel_timer(session_id, token));

        assert_eq!(runtime.expire_timers(2).expect("expire canceled timer"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());
    }

    #[test]
    fn worker_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(32);
        let token = SessionTimerToken::new(5);
        let start = Instant::now();
        let mut runtime =
            WorkerSessionRuntime::with_timer_clock(worker, Duration::from_millis(10), start);

        runtime
            .arm_timer_ticks(session_id, token, 2)
            .expect("arm timer");

        let first_ticks = runtime.elapsed_timer_ticks(start + Duration::from_millis(10));
        let first = runtime
            .poll_once_for_ticks(first_ticks)
            .expect("first poll");
        assert_eq!(first.expired_timers, 0);
        assert!(runtime.take_timer_expiries().is_empty());

        let second_ticks = runtime.elapsed_timer_ticks(start + Duration::from_millis(20));
        let second = runtime
            .poll_once_for_ticks(second_ticks)
            .expect("second poll");
        assert_eq!(second.expired_timers, 1);
        assert_eq!(
            runtime.take_timer_expiries(),
            infra_vec([SessionTimerExpiry::new(session_id, token)])
        );
    }

    #[test]
    fn session_tx_copies_app_send_and_commits_progress() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), buffers.clone());
        let session_id = driver.insert_session(fake_tx_state());
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
            .try_into()
            .expect("transfer");
        driver.app_mut().push_pending_send(session_id, send);
        driver.mark_ready(session_id);

        let mut protocol = FakeTxProtocol;
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let output_node = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let next = crate::session::SessionQueueNext::from_node(output_node);
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            &mut protocol,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch");

        let state = driver.session_state(session_id).expect("state");
        assert_eq!(state.prepared, 6);
        assert_eq!(state.committed, 6);
        assert_eq!(state.canceled, 0);
        assert!(!driver.app().has_pending_send(session_id));

        output.schedule(&runtime).expect("schedule output");
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 1);
        let packets = &capture.lock().expect("capture").packets;
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].as_slice(), b"abcd");
        assert_eq!(packets[1].as_slice(), b"ef");
    }

    #[test]
    fn session_tx_does_not_call_transport_when_app_has_no_pending_send() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), buffers.clone());
        let session_id = driver.insert_session(fake_tx_state());
        driver.mark_ready(session_id);

        let mut protocol = FakeTxProtocol;
        let next = crate::session::SessionQueueNext::from_node(NodeId::new(9));
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            &mut protocol,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch without app tx");

        let state = driver.session_state(session_id).expect("state");
        assert_eq!(state.prepared, 0);
        assert_eq!(state.committed, 0);
        assert_eq!(state.canceled, 0);
    }

    #[test]
    fn session_tx_keeps_pending_send_when_transport_has_no_capacity() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), buffers.clone());
        let session_id = driver.insert_session(fake_tx_state());
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"pending").expect("data"))
            .try_into()
            .expect("transfer");
        driver.app_mut().push_pending_send(session_id, send);
        driver.mark_ready(session_id);

        let mut protocol = NoTxPayloadProtocol;
        let next = crate::session::SessionQueueNext::from_node(NodeId::new(9));
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            &mut protocol,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch without capacity");

        let state = driver.session_state(session_id).expect("state");
        assert_eq!(state.prepared, 0);
        assert_eq!(state.committed, 0);
        assert_eq!(state.canceled, 0);
        assert!(driver.app().has_pending_send(session_id));
    }
}
