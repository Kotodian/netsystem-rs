use std::cell::{Cell, RefCell, RefMut};
use std::fmt;
use std::marker::PhantomData;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, Index, NodeId, NodeRegistration};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::{
    DataPlaneRuntime, DriverNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
};

use crate::session::SessionQueueError;
use hammer_infra::vec::Vec;

/// Shared Session Queue IO allowance for normal and custom TX in one dispatch.
pub const SESSION_QUEUE_IO_BUDGET: usize = 128;

#[derive(PartialEq, Eq)]
pub struct SessionQueueHandle<Q> {
    runtime_data: NodeRuntimeData,
    _queue: PhantomData<fn() -> Q>,
}

impl<Q> SessionQueueHandle<Q> {
    #[inline]
    pub const fn new(runtime_data: NodeRuntimeData) -> Self {
        Self {
            runtime_data,
            _queue: PhantomData,
        }
    }

    #[inline]
    pub const fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }

    #[inline]
    pub fn borrow_mut(self) -> CoreResult<RefMut<'static, Q>>
    where
        Q: 'static,
    {
        let queue = session_queue_cell::<Q>(self.runtime_data)?;
        queue
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queue borrowed"))
    }
}

impl<Q> Copy for SessionQueueHandle<Q> {}

impl<Q> Clone for SessionQueueHandle<Q> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Q> fmt::Debug for SessionQueueHandle<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionQueueHandle")
            .field("runtime_data", &self.runtime_data)
            .finish()
    }
}

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
) -> CoreResult<()>;

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
    ) -> CoreResult<bool> {
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
    runtime_data: NodeRuntimeData,
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
    static SESSION_QUEUE_NODE_RUNTIME_DATA: Cell<Option<NodeRuntimeData>> = const { Cell::new(None) };
    static SESSION_QUEUE_NODE_ID: Cell<Option<NodeId>> = const { Cell::new(None) };
}

pub fn register_session_queue_node(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    let node = SessionQueueNode::new()?;
    let runtime_data = node.runtime_data;
    let id = runtime.nodes().try_register_driver(node)?;
    SESSION_QUEUE_NODE_RUNTIME_DATA.with(|data| data.set(Some(runtime_data)));
    SESSION_QUEUE_NODE_ID.with(|data| data.set(Some(id)));
    Ok(id)
}

impl SessionQueueNode {
    pub fn new() -> CoreResult<Self> {
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes.borrow_mut();
            let slot = nodes.len();
            let runtime_data = NodeRuntimeData::from_usize(slot)?;
            nodes.push(Vec::new());
            Ok(Self { runtime_data })
        })
    }

    pub fn attach_queue_by_runtime_data<Q>(
        runtime: &DataPlaneRuntime,
        consumer: NodeId,
        runtime_data: NodeRuntimeData,
        handle: SessionQueueHandle<Q>,
        output_node: NodeId,
        dispatch: SessionQueueDispatchFn,
    ) -> CoreResult<()> {
        let slot = runtime.nodes().add_node_next_slot(consumer, output_node)?;
        let output_next = SessionQueueNext::from_slot(slot);
        let attachment_slot = runtime_data.usize_word(0)?;
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes
                .try_borrow_mut()
                .map_err(|_| CoreError::internal("session queue nodes borrowed"))?;
            let node = nodes
                .get_mut(attachment_slot)
                .ok_or_else(|| CoreError::internal("session queue node slot is invalid"))?;
            node.push(SessionQueueAttachment {
                runtime_data: handle.runtime_data(),
                output_next,
                dispatch,
            });
            Ok(())
        })
    }

    pub fn registered_runtime_data() -> CoreResult<NodeRuntimeData> {
        SESSION_QUEUE_NODE_RUNTIME_DATA
            .with(|data| data.get())
            .ok_or_else(|| CoreError::internal("session queue node not registered"))
    }

    pub fn registered_node_id() -> CoreResult<NodeId> {
        SESSION_QUEUE_NODE_ID
            .with(|data| data.get())
            .ok_or_else(|| CoreError::internal("session queue node not registered"))
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
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
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
            attachment.runtime_data,
            attachment.output_next,
            now,
            frame,
            &mut output,
        )
        .map_err(CoreError::from)
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

pub fn register_session_queue<Q: 'static>(queue: Q) -> CoreResult<SessionQueueHandle<Q>> {
    let queue = Box::leak(Box::new(RefCell::new(queue)));
    let runtime_data = session_queue_node_runtime_data(queue)?;
    Ok(SessionQueueHandle::new(runtime_data))
}

fn session_queue_node_runtime_data<Q>(queue: &'static RefCell<Q>) -> CoreResult<NodeRuntimeData> {
    Ok(NodeRuntimeData::from_words([
        u64::try_from(queue as *const _ as usize)
            .map_err(|_| CoreError::internal("session queue pointer overflow"))?,
        0,
        0,
        0,
    ]))
}

fn session_queue_cell<Q>(data: NodeRuntimeData) -> CoreResult<&'static RefCell<Q>> {
    let raw = usize::try_from(data.word(0))
        .map_err(|_| CoreError::internal("session queue pointer is invalid"))?;
    let ptr = raw as *const RefCell<Q>;
    // SAFETY: `register_session_queue` leaks a `RefCell<Q>` and stores the exact
    // pointer in `NodeRuntimeData`. The typed handle preserves the same `Q` on
    // recovery.
    unsafe { ptr.as_ref() }.ok_or_else(|| CoreError::internal("session queue is missing"))
}

#[cfg(test)]
#[path = "node/tests.rs"]
mod tests;
