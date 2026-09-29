use hammer_core::data_plane::{Frame, NodeId, NodeRegistration, NodeState};
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, DriverNode, File, Node, NodeMain, NodeRuntime, RuntimeError,
    RuntimeResult,
};
use hammer_runtime::node::{NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity};

use crate::session::{SessionQueueError, app, core};

pub const SESSION_QUEUE_IO_BUDGET: usize = 128;

/// VPP: session_node.c, `SESSION_QUEUE_ERROR_NO_BUFFER`.
#[derive(Clone, Copy)]
pub(crate) enum SessionQueueNodeError {
    Tx,
    Timer,
    NoBuffer,
}

impl NodeErrorCode for SessionQueueNodeError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

const SESSION_QUEUE_ERROR_DESCRIPTORS: [NodeErrorDescriptor; 3] = [
    NodeErrorDescriptor::new("tx", NodeErrorSeverity::Info, "Packets transmitted"),
    NodeErrorDescriptor::new("timer", NodeErrorSeverity::Info, "Timer events"),
    NodeErrorDescriptor::new("no-buffer", NodeErrorSeverity::Error, "No Buffer available"),
];

/// VPP: session_input.c:350-405.
#[hammer_component_macros::graph_node(
    graph = session,
    init = crate::session::node::register_session_input_node,
    name = "session-input",
    kind = driver,
)]
#[derive(Clone, Copy, Default)]
pub struct SessionInputNode;

pub fn register_session_input_node(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("session-input") {
        return Ok(node);
    }
    let node = runtime.nodes().try_register_driver(SessionInputNode)?;
    runtime.nodes().set_node_state(node, NodeState::Disabled)?;
    Ok(node)
}

impl Node for SessionInputNode {
    fn process(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let vectors = frame.len();
        let session_main = core::SessionMain::global()
            .expect("Session Main initializes before session-input executes");
        let session_worker = unsafe { session_main.worker_mut(runtime) }
            .expect("session-input runs only on a configured Data Worker");
        let pending = match app::ApplicationMain::global() {
            Some(application_main) => match application_main.flush_worker_events(session_worker) {
                Ok(pending) => pending,
                Err(_) => true,
            },
            None => false,
        };
        if pending {
            let node = runtime
                .current_node()
                .expect("session-input is executing as a Graph Node");
            runtime
                .set_node_interrupt_pending(node)
                .expect("session-input can reschedule pending events");
        }
        vectors
    }
}

impl DriverNode for SessionInputNode {
    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        Some(NodeRegistration::next("session-input", 0))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionQueueNext(u16);

impl SessionQueueNext {
    #[inline]
    pub const fn from_slot(slot: u16) -> Self {
        Self(slot)
    }

    #[inline]
    pub const fn slot(self) -> u16 {
        self.0
    }
}

/// VPP: session_node.c:2148-2154.
#[hammer_component_macros::graph_node(
    graph = session,
    init = crate::session::node::register_session_queue_node,
    name = "session-queue",
    kind = driver,
)]
#[derive(Clone, Default)]
pub struct SessionQueueNode;

pub fn register_session_queue_node(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("session-queue") {
        return Ok(node);
    }
    let node = runtime.nodes().try_register_driver(SessionQueueNode)?;
    runtime.register_node_errors(node, &SESSION_QUEUE_ERROR_DESCRIPTORS)?;
    runtime.nodes().set_node_state(node, NodeState::Disabled)?;
    Ok(node)
}

/// VPP: session_node.c:2180-2188. The File stores the queue NodeId resolved
/// at worker init; its polling thread identifies the owning Session worker.
pub(crate) fn session_queue_timer_ready(graph: &mut NodeMain, file: &mut File) -> RuntimeResult<()> {
    let queue = NodeId::new(
        u32::try_from(file.private_data()).expect("session-queue NodeId fits File private data"),
    );
    graph.mark_interrupt_pending(queue)?;
    let worker = u32::try_from(DataWorkerId::try_from(file.polling_thread_index())?.slot())
        .expect("configured worker slot fits u32");
    let mut expirations = 0_u64;
    loop {
        // SAFETY: FileMain owns the descriptor for this callback, and timerfd
        // produces one u64 counter for each successful nonblocking read.
        let read = unsafe {
            libc::read(
                file.fd(),
                std::ptr::from_mut(&mut expirations).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if read >= 0 {
            assert_eq!(
                read,
                std::mem::size_of::<u64>() as isize,
                "timerfd returns one complete expiry counter"
            );
            return Ok(());
        }
        let source = std::io::Error::last_os_error();
        match source.kind() {
            std::io::ErrorKind::Interrupted => continue,
            std::io::ErrorKind::WouldBlock => return Ok(()),
            _ => return Err(SessionQueueError::TimerRead { worker, source }.into()),
        }
    }
}

impl SessionQueueNode {
    pub fn compile_output_next(
        runtime: &DataPlaneMain,
        consumer: NodeId,
        output_node: NodeId,
    ) -> RuntimeResult<SessionQueueNext> {
        let slot = runtime.nodes().add_node_next_slot(consumer, output_node)?;
        Ok(SessionQueueNext::from_slot(slot))
    }

    pub fn existing_output_next(
        runtime: &DataPlaneMain,
        consumer: NodeId,
        output_node: NodeId,
    ) -> RuntimeResult<SessionQueueNext> {
        let mut slot = 0usize;
        loop {
            match runtime.nodes().node_next_slot(consumer, slot) {
                Ok(node) if node == output_node => {
                    return u16::try_from(slot)
                        .map(SessionQueueNext::from_slot)
                        .map_err(|_| RuntimeError::NodeNextCountOverflow {
                            count: slot.saturating_add(1),
                        });
                }
                Ok(_) => slot += 1,
                Err(_) => {
                    return Err(SessionQueueError::OutputMissing {
                        consumer,
                        output_node,
                    }
                    .into());
                }
            }
        }
    }
}

impl Node for SessionQueueNode {
    fn process(runtime: &mut DataPlaneMain, data: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let session_main = core::SessionMain::global()
            .expect("Session Main initializes before session-queue executes");
        let session_worker = unsafe { session_main.worker_mut(runtime) }
            .expect("session-queue executes on its owning Data Worker");
        let now = session_main.now();
        session_worker.update_time(now);
        session_worker
            .update_transport_time(runtime, session_main, now)
            .expect("registered Session transport updates its worker time");
        let event_queue = session_main
            .event_queue(session_worker.worker_index())
            .expect("Session worker retains its message queue");
        session_worker
            .drain_event_queue(event_queue)
            .expect("Session worker message queue retains valid event records");
        session_worker
            .dispatch_control_events(runtime, session_main)
            .expect("registered Session control handlers accept their events");
        let transmitted = session_worker
            .dispatch_io_events(runtime, data, session_main)
            .expect("registered Session transport handles its IO events");
        session_worker.schedule_pending_app_events(runtime);
        session_worker.flush_pending_tx_buffers(runtime, data, frame);
        runtime
            .record_current_node_error_count(SessionQueueNodeError::Tx, transmitted as u64)
            .expect("session-queue registers its TX counter");
        session_worker.update_state(runtime);
        transmitted
    }
}

impl DriverNode for SessionQueueNode {
    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        Some(NodeRegistration::next("session-queue", 0))
    }
}
