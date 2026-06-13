use std::cell::RefCell;
use std::time::Instant;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DataWorkerId, DriverNode, Node, NodeProcessFn, NodeRegistration,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::session::worker::SessionQueueRuntime;

#[derive(Clone, Debug)]
pub struct SessionQueueNode {
    worker: DataWorkerId,
    runtime_data: Option<NodeRuntimeData>,
}

impl SessionQueueNode {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            runtime_data: None,
        }
    }
}

impl Node for SessionQueueNode {
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
        session_queue_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        match self.runtime_data {
            Some(runtime_data) => Ok(runtime_data),
            None => register_session_queue_runtime(SessionQueueRuntime::new(self.worker)),
        }
    }
}

impl DriverNode for SessionQueueNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("session-queue", 0)
    }
}

thread_local! {
    static SESSION_QUEUE_RUNTIMES: RefCell<hammer_infra::vec::Vec<SessionQueueRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_session_queue_runtime(runtime: SessionQueueRuntime) -> CoreResult<NodeRuntimeData> {
    SESSION_QUEUE_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        let runtime_data = NodeRuntimeData::from_usize(slot)?;
        runtimes.push(runtime);
        Ok(runtime_data)
    })
}

fn with_session_queue_runtime<R>(
    data: NodeRuntimeData,
    f: impl FnOnce(&mut SessionQueueRuntime) -> CoreResult<R>,
) -> CoreResult<R> {
    let slot = data.usize_word(0)?;
    SESSION_QUEUE_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queue runtimes borrowed"))?;
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("session queue runtime slot is invalid"))?;
        f(runtime)
    })
}

fn session_queue_process(
    _runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    with_session_queue_runtime(data, |session| {
        session.run_once_at(Instant::now())?;
        Ok(NodeResult::drop())
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    use hammer_adapter::NodeState;
    use hammer_runtime::app::{
        AppBackend, AppBufferLease, AppObjectRef, AppOpcode, AppRegisteredBuffer, AppSqeData,
        AppSqeDescriptor, AppSubmissionEntry, AppUserData,
    };
    use hammer_runtime::spawn::with_data_plane_buffers;

    use crate::session::{
        AppSessionId, AppSessionSubmission, AppSessionTimerExpiry, AppSessionTimerToken,
        SessionProtocolContext, SessionProtocolOps, SessionProtocolRegistry, WorkerSessionRuntime,
    };

    use super::*;

    #[derive(Default)]
    struct RecordingState {
        submissions: hammer_infra::vec::Vec<AppSessionId>,
        timers: hammer_infra::vec::Vec<AppSessionTimerExpiry>,
        ready: hammer_infra::vec::Vec<AppSessionId>,
    }

    struct RecordingProtocol {
        state: Rc<RefCell<RecordingState>>,
    }

    fn infra_vec<T>(items: impl IntoIterator<Item = T>) -> hammer_infra::vec::Vec<T> {
        let mut values = hammer_infra::vec::Vec::new();
        for item in items {
            values.push(item);
        }
        values
    }

    impl SessionProtocolOps for RecordingProtocol {
        fn handle_submission(
            &mut self,
            context: &mut SessionProtocolContext<'_>,
            submission: AppSessionSubmission,
        ) -> CoreResult<()> {
            let session_id = submission.session_id();
            self.state.borrow_mut().submissions.push(session_id);
            context.mark_ready(session_id);
            Ok(())
        }

        fn handle_timer_expiry(
            &mut self,
            _context: &mut SessionProtocolContext<'_>,
            expiry: AppSessionTimerExpiry,
        ) -> CoreResult<()> {
            self.state.borrow_mut().timers.push(expiry);
            Ok(())
        }

        fn handle_ready(
            &mut self,
            _context: &mut SessionProtocolContext<'_>,
            session_id: AppSessionId,
        ) -> CoreResult<()> {
            self.state.borrow_mut().ready.push(session_id);
            Ok(())
        }
    }

    fn session_queue_node_with_runtime(
        worker: DataWorkerId,
        runtime: SessionQueueRuntime,
    ) -> CoreResult<(SessionQueueNode, NodeRuntimeData)> {
        let runtime_data = register_session_queue_runtime(runtime)?;
        Ok((
            SessionQueueNode {
                worker,
                runtime_data: Some(runtime_data),
            },
            runtime_data,
        ))
    }

    fn build_session_queue_node(
        worker: DataWorkerId,
        protocols: SessionProtocolRegistry,
        configure: impl FnOnce(&mut WorkerSessionRuntime) -> CoreResult<()>,
    ) -> CoreResult<(SessionQueueNode, NodeRuntimeData)> {
        let mut runtime = SessionQueueRuntime::with_protocols(worker, protocols);
        configure(runtime.sessions_mut())?;
        session_queue_node_with_runtime(worker, runtime)
    }

    fn build_session_queue_node_with_clock(
        worker: DataWorkerId,
        protocols: SessionProtocolRegistry,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
        configure: impl FnOnce(&mut WorkerSessionRuntime) -> CoreResult<()>,
    ) -> CoreResult<(SessionQueueNode, NodeRuntimeData)> {
        let mut runtime = SessionQueueRuntime::with_timer_clock(
            worker,
            protocols,
            timer_tick_duration,
            last_timer_tick,
        );
        configure(runtime.sessions_mut())?;
        session_queue_node_with_runtime(worker, runtime)
    }

    fn run_session_queue_node(runtime: &DataPlaneRuntime, node: hammer_adapter::NodeId) {
        runtime
            .nodes()
            .set_node_state(node, NodeState::Interrupt)
            .expect("set session queue interrupt state");
        assert!(
            runtime
                .set_node_interrupt_pending(node)
                .expect("wake session queue node")
        );
        assert_eq!(
            runtime.run_ready_nodes().expect("run session queue node"),
            1
        );
    }

    #[test]
    fn session_queue_node_dispatches_submission_to_registered_protocol() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(41);
        let state = Rc::new(RefCell::new(RecordingState::default()));
        let mut registry = SessionProtocolRegistry::new();
        let protocol_id = registry
            .register(
                "recording",
                Box::new(RecordingProtocol {
                    state: state.clone(),
                }),
            )
            .expect("register protocol");
        registry
            .bind_session(session_id, protocol_id)
            .expect("bind session protocol");

        let backend = AppBackend::new(4);
        let flow = backend.flow();

        let buffers = with_data_plane_buffers(Clone::clone);
        let index = buffers
            .alloc_index_with_bytes(Default::default(), b"dispatch-send")
            .expect("alloc app send buffer");
        let registered =
            AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
                .expect("registered buffer");
        let descriptor = AppSqeDescriptor::new(
            AppOpcode::Send,
            AppUserData::new(41),
            AppObjectRef::Flow(flow),
            AppSqeData::Send {
                buffer: registered.index(),
            },
        );
        backend
            .try_push_submission_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
            .expect("push app send entry");

        let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
        let (node, _data) = build_session_queue_node(worker, registry, |sessions| {
            sessions
                .attach_app_backend(session_id, backend.clone())
                .expect("attach app backend");
            Ok(())
        })
        .expect("build session queue node with runtime");
        let driver = runtime.nodes().register_driver(node);
        run_session_queue_node(&runtime, driver);

        let state = state.borrow();
        assert_eq!(state.submissions, infra_vec([session_id]));
        assert_eq!(state.ready, infra_vec([session_id]));
    }

    #[test]
    fn session_queue_node_dispatches_timer_to_registered_protocol() {
        let worker = DataWorkerId::new(0);
        let session_id = AppSessionId::new(42);
        let token = AppSessionTimerToken::new(99);
        let state = Rc::new(RefCell::new(RecordingState::default()));
        let mut registry = SessionProtocolRegistry::new();
        let protocol_id = registry
            .register(
                "recording",
                Box::new(RecordingProtocol {
                    state: state.clone(),
                }),
            )
            .expect("register protocol");
        registry
            .bind_session(session_id, protocol_id)
            .expect("bind session protocol");

        let start = Instant::now() - Duration::from_millis(10);

        let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
        let (node, _data) = build_session_queue_node_with_clock(
            worker,
            registry,
            Duration::from_millis(10),
            start,
            |sessions| {
                sessions
                    .arm_timer_ticks(session_id, token, 1)
                    .expect("arm timer");
                Ok(())
            },
        )
        .expect("build session queue node with runtime");
        let driver = runtime.nodes().register_driver(node);
        run_session_queue_node(&runtime, driver);

        let state = state.borrow();
        assert_eq!(
            state.timers,
            infra_vec([AppSessionTimerExpiry::new(session_id, token)])
        );
        assert_eq!(state.ready, infra_vec([session_id]));
    }
}
