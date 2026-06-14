use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;
use hammer_infra::pool::Pool;
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::session::protocol::SessionProtocolContext;
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
        self.timers.arm_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
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

pub(crate) trait SessionQueueProgram: 'static {
    type Session: 'static;

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionProtocolContext<'_, Self::Session>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()>;

    fn handle_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_, Self::Session>,
        session_id: SessionId,
    ) -> CoreResult<()>;
}

pub(crate) struct SessionQueueRuntime<P: SessionQueueProgram> {
    sessions: WorkerSessionRuntime,
    entries: Pool<SessionEntry<P::Session>>,
    app: SessionAppRuntime,
    program: P,
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
    pub(crate) fn clear_app_op(&mut self) -> Option<AppOpId> {
        self.app_op.take()
    }

    #[inline]
    pub(crate) fn state(&self) -> &S {
        &self.state
    }

    #[inline]
    pub(crate) fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    #[inline]
    pub(crate) fn into_state(self) -> S {
        self.state
    }
}

impl<P> SessionQueueRuntime<P>
where
    P: SessionQueueProgram,
{
    #[inline]
    pub(crate) fn new(worker: DataWorkerId, protocol: P) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            app: SessionAppRuntime::new(),
            program: protocol,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        protocol: P,
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
            program: protocol,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn sessions_mut(&mut self) -> &mut WorkerSessionRuntime {
        &mut self.sessions
    }

    #[inline]
    pub(crate) fn insert_session(&mut self, state: P::Session) -> SessionId {
        let index = self
            .entries
            .insert(SessionEntry::new(state))
            .expect("session pool capacity exhausted");
        SessionId::from_pool_index(index)
    }

    #[inline]
    pub(crate) fn session(&self, id: SessionId) -> Option<&SessionEntry<P::Session>> {
        self.entries.get(id.pool_index())
    }

    #[inline]
    pub(crate) fn session_mut(&mut self, id: SessionId) -> Option<&mut SessionEntry<P::Session>> {
        self.entries.get_mut(id.pool_index())
    }

    #[inline]
    pub(crate) fn session_state(&self, id: SessionId) -> Option<&P::Session> {
        self.session(id).map(SessionEntry::state)
    }

    #[inline]
    pub(crate) fn session_state_mut(&mut self, id: SessionId) -> Option<&mut P::Session> {
        self.session_mut(id).map(SessionEntry::state_mut)
    }

    pub(crate) fn remove_session(&mut self, id: SessionId) -> Option<SessionEntry<P::Session>> {
        let removed = self.entries.remove(id.pool_index())?;
        if let Some(op) = removed.app_op() {
            self.app.unbind_ring(op);
        }
        Some(removed)
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
    pub(crate) fn program_mut(&mut self) -> &mut P {
        &mut self.program
    }

    #[inline]
    pub(crate) fn program(&self) -> &P {
        &self.program
    }
}

impl<P> SessionQueueRuntime<P>
where
    P: SessionQueueProgram,
{
    pub(crate) fn run_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let mut step = self.sessions.poll_once_for_ticks(timer_ticks)?;
        let expiries = self.sessions.take_timer_expiries();
        let worker = self.sessions.worker();

        for expiry in expiries {
            let mut context = crate::session::SessionProtocolContext::new(
                worker,
                &mut self.sessions,
                &mut self.entries,
                &mut self.app,
            );
            self.program.handle_timer_expiry(&mut context, expiry)?;
        }

        let ready_sessions = self.sessions.take_ready_sessions();
        step.ready_sessions = ready_sessions.len();
        for session_id in ready_sessions {
            let mut context = crate::session::SessionProtocolContext::new(
                worker,
                &mut self.sessions,
                &mut self.entries,
                &mut self.app,
            );
            self.program.handle_ready(&mut context, session_id)?;
        }

        Ok(step)
    }

    pub(crate) fn run_once_at(&mut self, now: Instant) -> CoreResult<SessionQueueStep> {
        let timer_ticks = self.sessions.elapsed_timer_ticks(now);
        self.run_once_for_ticks(timer_ticks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn infra_vec<T>(items: impl IntoIterator<Item = T>) -> hammer_infra::vec::Vec<T> {
        let mut values = hammer_infra::vec::Vec::new();
        for item in items {
            values.push(item);
        }
        values
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
}
