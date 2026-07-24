use std::cell::{Cell, RefCell};
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, Index, NodeId, NodeRegistration};
use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};
use hammer_runtime::{
    DataPlaneRuntime, DriverNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
};

use crate::session::SessionQueueError;

/// Shared Session Queue IO allowance for normal and custom TX in one dispatch.
pub const SESSION_QUEUE_IO_BUDGET: usize = 128;

/// Current-node-local next slot for Session Queue generated output.
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

pub type SessionQueueDispatchFn = fn(
    &DataPlaneRuntime,
    NodeRuntimeData,
    SessionQueueNext,
    Instant,
    &mut BufferFrame,
    &mut SessionQueueOutput,
) -> RuntimeResult<()>;

/// Accumulates Session Queue TX indexes on the driver Frame and records one
/// local next per entry. Graph Fanout runs once at [`Self::flush`].
pub struct SessionQueueOutput {
    nexts: Vec<u16>,
    io_count: usize,
}

impl Default for SessionQueueOutput {
    #[inline]
    fn default() -> Self {
        Self::seeded(0)
    }
}

impl SessionQueueOutput {
    #[inline]
    pub fn seeded(pending_seed: usize) -> Self {
        Self {
            nexts: Vec::new(),
            io_count: pending_seed.min(SESSION_QUEUE_IO_BUDGET),
        }
    }

    #[inline]
    pub fn remaining_io_budget(&self) -> usize {
        SESSION_QUEUE_IO_BUDGET.saturating_sub(self.io_count)
    }

    #[inline]
    pub fn io_count(&self) -> usize {
        self.io_count
    }

    /// Enqueue one normal/custom IO index into the driver Frame.
    ///
    /// Returns `Ok(false)` when the shared IO budget is exhausted; the caller
    /// keeps ownership of `index` and should leave unserved work pending.
    #[inline]
    pub fn try_enqueue_io(
        &mut self,
        frame: &mut BufferFrame,
        next: SessionQueueNext,
        index: Index,
    ) -> RuntimeResult<bool> {
        if self.io_count >= SESSION_QUEUE_IO_BUDGET {
            return Ok(false);
        }
        frame.push_index(index)?;
        self.nexts.push(next.slot());
        self.io_count += 1;
        Ok(true)
    }

    /// One Graph Fanout flush for every index recorded on `frame` this dispatch.
    #[inline]
    pub fn flush(self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) {
        debug_assert_eq!(frame.len(), self.nexts.len());
        if self.nexts.is_empty() {
            return;
        }
        runtime.enqueue_to_next(frame, self.nexts.as_slice());
    }
}

#[derive(Clone, Copy)]
struct SessionQueueAttachment {
    output_next: SessionQueueNext,
    dispatch: SessionQueueDispatchFn,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::session::node::register_session_queue_node,
    name = "session-queue",
    kind = driver,
)]
#[derive(Clone)]
pub struct SessionQueueNode {
    runtime_data: NodeRuntimeData,
}

thread_local! {
    static SESSION_QUEUE_NODES: RefCell<Vec<Vec<SessionQueueAttachment>>> =
        const { RefCell::new(Vec::new()) };
}

pub fn register_session_queue_node(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("session-queue") {
        return Ok(node);
    }
    let node = SessionQueueNode::new()?;
    runtime.nodes().try_register_driver(node)
}

impl SessionQueueNode {
    pub fn new() -> RuntimeResult<Self> {
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes.borrow_mut();
            let slot = nodes.len();
            let runtime_data = NodeRuntimeData::from_usize(slot)?;
            nodes.push(Vec::new());
            Ok(Self { runtime_data })
        })
    }

    /// Compiles the session queue's output edge in the main graph.
    pub fn compile_output_next(
        runtime: &DataPlaneRuntime,
        consumer: NodeId,
        output_node: NodeId,
    ) -> RuntimeResult<SessionQueueNext> {
        let slot = runtime.nodes().add_node_next_slot(consumer, output_node)?;
        Ok(SessionQueueNext::from_slot(slot))
    }

    /// Resolves an already-compiled output edge in a worker graph clone.
    ///
    /// This only observes graph identity. Worker initialization must never add
    /// or change a next arc.
    pub fn existing_output_next(
        runtime: &DataPlaneRuntime,
        consumer: NodeId,
        output_node: NodeId,
    ) -> RuntimeResult<SessionQueueNext> {
        let mut slot = 0usize;
        loop {
            match runtime.nodes().node_next_slot(consumer, slot) {
                Ok(node) if node == output_node => {
                    return u16::try_from(slot)
                        .map(SessionQueueNext::from_slot)
                        .map_err(|_| {
                            RuntimeError::invariant("session queue next slot overflows u16")
                        });
                }
                Ok(_) => slot += 1,
                Err(_) => {
                    return Err(RuntimeError::invariant(
                        "session queue output is not registered",
                    ));
                }
            }
        }
    }

    /// Installs one worker-local transport dispatch attachment.
    ///
    /// The graph edge is compiled by [`Self::compile_output_next`] on the main
    /// thread. This method owns only the worker's dispatch table.
    pub fn install_worker_attachment(
        runtime_data: NodeRuntimeData,
        output_next: SessionQueueNext,
        dispatch: SessionQueueDispatchFn,
    ) -> RuntimeResult<()> {
        let attachment_slot = runtime_data.usize_word(0)?;
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes
                .try_borrow_mut()
                .map_err(|_| RuntimeError::invariant("session queue nodes borrowed"))?;
            let node = nodes
                .get_mut(attachment_slot)
                .ok_or_else(|| RuntimeError::invariant("session queue node slot is invalid"))?;
            node.push(SessionQueueAttachment {
                output_next,
                dispatch,
            });
            Ok(())
        })
    }
}

impl Node for SessionQueueNode {
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        session_queue_node_process(runtime, self.runtime_data, frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        session_queue_node_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
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

fn session_queue_node_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let slot = match data.usize_word(0) {
        Ok(slot) => slot,
        Err(_) => return NodeResult::drop(),
    };
    let now = Instant::now();
    let mut output = SessionQueueOutput::default();
    let mut index = 0usize;
    loop {
        let attachment = SESSION_QUEUE_NODES.with(|nodes| {
            let nodes = nodes.try_borrow().ok()?;
            nodes.get(slot)?.get(index).copied()
        });
        let Some(attachment) = attachment else { break };
        if (attachment.dispatch)(
            runtime,
            data,
            attachment.output_next,
            now,
            frame,
            &mut output,
        )
        .map_err(RuntimeError::from)
        .map_err(|err| {
            let _ = runtime.record_current_node_error(SessionQueueError::DispatchFailed.code());
            err
        })
        .and_then(|_| Ok(()))
        .is_err()
        {
            output.flush(runtime, frame);
            return NodeResult::drop();
        }
        index += 1;
    }
    output.flush(runtime, frame);
    NodeResult::drop()
}

#[cfg(test)]
#[path = "node/tests.rs"]
mod tests;
