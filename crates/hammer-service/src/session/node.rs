use std::cell::RefCell;
use std::thread::LocalKey;
use std::time::Instant;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, DriverNode, Node, NodeId, NodeNextFrames,
    NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionQueueHandle {
    runtime_data: NodeRuntimeData,
}

impl SessionQueueHandle {
    #[inline]
    pub(crate) const fn new(runtime_data: NodeRuntimeData) -> Self {
        Self { runtime_data }
    }

    #[inline]
    pub(crate) const fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionQueueNext(NodeId);

impl SessionQueueNext {
    #[inline]
    pub const fn from_node(node: NodeId) -> Self {
        Self(node)
    }

    #[inline]
    pub const fn node(self) -> NodeId {
        self.0
    }
}

pub type SessionQueueDispatchFn =
    fn(&DataPlaneRuntime, SessionQueueHandle, SessionQueueNext, Instant) -> CoreResult<()>;

#[derive(Default)]
pub(crate) struct SessionQueueOutput {
    frames: NodeNextFrames,
}

impl SessionQueueOutput {
    #[inline]
    pub(crate) fn enqueue(
        &mut self,
        runtime: &DataPlaneRuntime,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<()> {
        self.frames.enqueue(runtime, node, index)
    }

    #[inline]
    pub(crate) fn schedule(self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        self.frames.schedule(runtime)
    }
}

#[derive(Clone, Copy)]
struct SessionQueueAttachment {
    handle: SessionQueueHandle,
    output_next: SessionQueueNext,
    dispatch: SessionQueueDispatchFn,
}

#[derive(Clone)]
pub struct SessionQueueNode {
    runtime_data: NodeRuntimeData,
}

thread_local! {
    static SESSION_QUEUE_NODES: RefCell<hammer_infra::vec::Vec<hammer_infra::vec::Vec<SessionQueueAttachment>>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

impl SessionQueueNode {
    pub fn new() -> CoreResult<Self> {
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes.borrow_mut();
            let slot = nodes.len();
            let runtime_data = NodeRuntimeData::from_usize(slot)?;
            nodes.push(hammer_infra::vec::Vec::new());
            Ok(Self { runtime_data })
        })
    }

    pub fn attach_queue(
        &self,
        handle: SessionQueueHandle,
        output_next: SessionQueueNext,
        dispatch: SessionQueueDispatchFn,
    ) -> CoreResult<()> {
        let slot = self.runtime_data.usize_word(0)?;
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes
                .try_borrow_mut()
                .map_err(|_| CoreError::internal("session queue nodes borrowed"))?;
            let node = nodes
                .get_mut(slot)
                .ok_or_else(|| CoreError::internal("session queue node slot is invalid"))?;
            node.push(SessionQueueAttachment {
                handle,
                output_next,
                dispatch,
            });
            Ok(())
        })
    }
}

impl Node for SessionQueueNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
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
) -> CoreResult<NodeResult> {
    frame.clear();
    let slot = data.usize_word(0)?;
    let attachments = SESSION_QUEUE_NODES.with(|nodes| {
        let nodes = nodes
            .try_borrow()
            .map_err(|_| CoreError::internal("session queue nodes borrowed"))?;
        let node = nodes
            .get(slot)
            .ok_or_else(|| CoreError::internal("session queue node slot is invalid"))?;
        Ok::<_, CoreError>(node.clone())
    })?;
    let now = Instant::now();
    for attachment in attachments {
        (attachment.dispatch)(runtime, attachment.handle, attachment.output_next, now)?;
    }
    Ok(NodeResult::drop())
}

pub(crate) fn register_session_queue<Q>(
    store: &'static LocalKey<RefCell<hammer_infra::vec::Vec<Q>>>,
    queue: Q,
) -> CoreResult<SessionQueueHandle> {
    store.with(|queues| {
        let mut queues = queues.borrow_mut();
        let slot = queues.len();
        let runtime_data = NodeRuntimeData::from_usize(slot)?;
        queues.push(queue);
        Ok(SessionQueueHandle::new(runtime_data))
    })
}

pub(crate) fn with_session_queue<Q, R>(
    store: &'static LocalKey<RefCell<hammer_infra::vec::Vec<Q>>>,
    handle: SessionQueueHandle,
    f: impl FnOnce(&mut Q) -> CoreResult<R>,
) -> CoreResult<R> {
    let slot = handle.runtime_data().usize_word(0)?;
    store.with(|queues| {
        let mut queues = queues
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queues borrowed"))?;
        let queue = queues
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("session queue slot is invalid"))?;
        f(queue)
    })
}
