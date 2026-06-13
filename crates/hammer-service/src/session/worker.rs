use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;

use crate::session::{
    AppSessionAppIngress, AppSessionCompletion, AppSessionId, AppSessionReadyQueue,
    AppSessionSubmission, AppSessionTimerExpiry, AppSessionTimerToken, AppSessionTimerWheel,
    SessionProtocolRegistry,
};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionQueueStep {
    pub(crate) app_submissions: usize,
    pub(crate) expired_timers: usize,
    pub(crate) ready_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    app: AppSessionAppIngress,
    ready: AppSessionReadyQueue,
    timers: AppSessionTimerWheel,
    pending_submissions: hammer_infra::vec::Vec<AppSessionSubmission>,
    pending_timer_expiries: hammer_infra::vec::Vec<AppSessionTimerExpiry>,
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
            app: AppSessionAppIngress::new(),
            ready: AppSessionReadyQueue::new(),
            timers: AppSessionTimerWheel::new(),
            pending_submissions: hammer_infra::vec::Vec::new(),
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
    pub fn attach_app_backend(
        &mut self,
        session_id: AppSessionId,
        backend: hammer_runtime::app::AppBackend,
    ) -> CoreResult<()> {
        self.app.attach_backend(session_id, backend)
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: AppSessionId) {
        self.ready.mark_ready(session_id);
    }

    #[inline]
    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<AppSessionId> {
        self.ready.take_ready_sessions()
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: AppSessionId,
        token: AppSessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.timers.arm_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: AppSessionId, token: AppSessionTimerToken) -> bool {
        self.timers.cancel(session_id, token)
    }

    #[inline]
    pub fn complete(&mut self, completion: AppSessionCompletion) -> CoreResult<()> {
        self.app.complete(completion)
    }

    pub(crate) fn poll_app_submissions(&mut self) -> CoreResult<usize> {
        self.app.poll_submissions(&mut self.pending_submissions)
    }

    pub(crate) fn expire_timers(&mut self, ticks: u32) -> CoreResult<usize> {
        let expired = self.timers.expire(ticks, &mut self.ready)?;
        self.pending_timer_expiries
            .extend(self.timers.take_expiries());
        Ok(expired)
    }

    pub(crate) fn take_submissions(&mut self) -> hammer_infra::vec::Vec<AppSessionSubmission> {
        self.pending_submissions.drain(..).collect()
    }

    pub(crate) fn take_timer_expiries(&mut self) -> hammer_infra::vec::Vec<AppSessionTimerExpiry> {
        self.pending_timer_expiries.drain(..).collect()
    }

    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let app_submissions = self.poll_app_submissions()?;
        let expired_timers = self.expire_timers(timer_ticks)?;
        Ok(SessionQueueStep {
            app_submissions,
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

pub(crate) struct SessionQueueRuntime {
    sessions: WorkerSessionRuntime,
    protocols: SessionProtocolRegistry,
}

impl SessionQueueRuntime {
    #[inline]
    pub(crate) fn new(worker: DataWorkerId) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            protocols: SessionProtocolRegistry::new(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_protocols(worker: DataWorkerId, protocols: SessionProtocolRegistry) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            protocols,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        protocols: SessionProtocolRegistry,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            sessions: WorkerSessionRuntime::with_timer_clock(
                worker,
                timer_tick_duration,
                last_timer_tick,
            ),
            protocols,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn sessions_mut(&mut self) -> &mut WorkerSessionRuntime {
        &mut self.sessions
    }

    pub(crate) fn run_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let mut step = self.sessions.poll_once_for_ticks(timer_ticks)?;
        let submissions = self.sessions.take_submissions();
        let expiries = self.sessions.take_timer_expiries();
        let worker = self.sessions.worker();

        for submission in submissions {
            self.protocols
                .dispatch_submission(worker, &mut self.sessions, submission)?;
        }

        for expiry in expiries {
            self.protocols
                .dispatch_timer_expiry(worker, &mut self.sessions, expiry)?;
        }

        let ready_sessions = self.sessions.take_ready_sessions();
        step.ready_sessions = ready_sessions.len();
        for session_id in ready_sessions {
            self.protocols
                .dispatch_ready(worker, &mut self.sessions, session_id)?;
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
    use std::net::Shutdown;

    use hammer_runtime::app::{
        AppBackend, AppBufferLease, AppCqeData, AppCqeFlags, AppObjectRef, AppOpcode,
        AppRegisteredBuffer, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown,
        AppUserData,
    };
    use hammer_runtime::spawn::with_data_plane_buffers;

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
        let session_id = AppSessionId::new(9);
        let token = AppSessionTimerToken::new(17);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 3)
            .expect("arm app session timer");

        assert_eq!(runtime.expire_timers(2).expect("expire before deadline"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(runtime.expire_timers(1).expect("expire at deadline"), 1);
        assert_eq!(
            runtime.take_timer_expiries(),
            infra_vec([AppSessionTimerExpiry::new(session_id, token)])
        );
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
    }

    #[test]
    fn worker_session_runtime_rearming_same_timer_suppresses_stale_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(10);
        let token = AppSessionTimerToken::new(3);
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
        let session_id = AppSessionId::new(11);
        let token = AppSessionTimerToken::new(4);
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
    fn worker_session_runtime_polls_app_send_submission() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(21);
        let mut runtime = WorkerSessionRuntime::new(worker);
        let backend = AppBackend::new(4);
        let flow = backend.flow();
        runtime
            .attach_app_backend(session_id, backend.clone())
            .expect("attach app backend");

        let buffers = with_data_plane_buffers(Clone::clone);
        let index = buffers
            .alloc_index_with_bytes(Default::default(), b"l5-session-send")
            .expect("alloc app send buffer");
        let registered =
            AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
                .expect("registered buffer");
        let descriptor = AppSqeDescriptor::new(
            AppOpcode::Send,
            AppUserData::new(21),
            AppObjectRef::Flow(flow),
            AppSqeData::Send {
                buffer: registered.index(),
            },
        );

        backend
            .try_push_submission_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
            .expect("push app send entry");

        assert_eq!(runtime.poll_app_submissions().expect("poll app ring"), 1);
        let submissions = runtime.take_submissions();
        assert_eq!(submissions.len(), 1);

        match &submissions[0] {
            AppSessionSubmission::Send(send) => {
                assert_eq!(send.session_id(), session_id);
                assert_eq!(send.descriptor().user_data(), AppUserData::new(21));
                assert_eq!(
                    send.registered()
                        .lease()
                        .copy_current()
                        .expect("copy send payload"),
                    b"l5-session-send"
                );
            }
            other => panic!("unexpected submission: {other:?}"),
        }
    }

    #[test]
    fn worker_session_runtime_polls_app_shutdown_submission() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(22);
        let mut runtime = WorkerSessionRuntime::new(worker);
        let backend = AppBackend::new(4);
        let flow = backend.flow();
        runtime
            .attach_app_backend(session_id, backend.clone())
            .expect("attach app backend");

        backend
            .try_push_tcp_shutdown(AppTcpShutdown::new(flow, Shutdown::Write))
            .expect("push app shutdown");

        assert_eq!(runtime.poll_app_submissions().expect("poll app ring"), 1);
        let submissions = runtime.take_submissions();
        assert_eq!(submissions.len(), 1);

        match submissions[0] {
            AppSessionSubmission::Shutdown(shutdown) => {
                assert_eq!(shutdown.session_id(), session_id);
                assert_eq!(shutdown.shutdown().flow(), flow);
                assert_eq!(shutdown.how(), Shutdown::Write);
            }
            ref other => panic!("unexpected submission: {other:?}"),
        }
    }

    #[test]
    fn worker_session_runtime_completion_writes_cqe_descriptor() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(31);
        let mut runtime = WorkerSessionRuntime::new(worker);
        let backend = AppBackend::new(4);
        let flow = backend.flow();
        runtime
            .attach_app_backend(session_id, backend.clone())
            .expect("attach app backend");

        runtime
            .complete(AppSessionCompletion::new(
                session_id,
                AppUserData::new(31),
                7,
                AppCqeFlags::NONE,
                AppCqeData::None,
            ))
            .expect("complete app session");

        let descriptor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(async { backend.next_cqe_descriptor().await.expect("cqe descriptor") });

        assert_eq!(descriptor.user_data(), AppUserData::new(31));
        assert_eq!(descriptor.result(), 7);
        assert_eq!(descriptor.object(), AppObjectRef::Flow(flow));
        assert_eq!(descriptor.payload(), AppCqeData::None);
    }

    #[test]
    fn worker_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(32);
        let token = AppSessionTimerToken::new(5);
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
            infra_vec([AppSessionTimerExpiry::new(session_id, token)])
        );
    }
}
