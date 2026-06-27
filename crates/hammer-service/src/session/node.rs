use std::cell::{Cell, RefCell, RefMut};
use std::fmt;
use std::marker::PhantomData;
use std::time::Instant;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, DriverNode, Node, NodeId, NodeNextFrames,
    NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

#[derive(PartialEq, Eq)]
pub struct SessionQueueHandle<Q> {
    runtime_data: NodeRuntimeData,
    _queue: PhantomData<fn() -> Q>,
}

impl<Q> SessionQueueHandle<Q> {
    #[inline]
    pub(crate) const fn new(runtime_data: NodeRuntimeData) -> Self {
        Self {
            runtime_data,
            _queue: PhantomData,
        }
    }

    #[inline]
    pub(crate) const fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }

    #[inline]
    pub(crate) fn borrow_mut(self) -> CoreResult<RefMut<'static, Q>>
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionQueueNext(NodeId);

impl SessionQueueNext {
    #[inline]
    pub const fn node(self) -> NodeId {
        self.0
    }
}

impl From<NodeId> for SessionQueueNext {
    #[inline]
    fn from(node: NodeId) -> Self {
        Self(node)
    }
}

pub(crate) type SessionQueueDispatchFn = fn(
    &DataPlaneRuntime,
    NodeRuntimeData,
    SessionQueueNext,
    Instant,
    &mut SessionQueueOutput,
) -> CoreResult<()>;

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
    runtime_data: NodeRuntimeData,
    output_next: SessionQueueNext,
    dispatch: SessionQueueDispatchFn,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::session::node::register_session_queue_node,
    name = "session-queue",
    kind = driver,
)]
#[derive(Clone)]
pub struct SessionQueueNode {
    runtime_data: NodeRuntimeData,
}

thread_local! {
    static SESSION_QUEUE_NODES: RefCell<hammer_infra::vec::Vec<hammer_infra::vec::Vec<SessionQueueAttachment>>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
    static SESSION_QUEUE_NODE_RUNTIME_DATA: Cell<Option<NodeRuntimeData>> = const { Cell::new(None) };
}

pub fn register_session_queue_node(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    let node = SessionQueueNode::new()?;
    let runtime_data = node.runtime_data;
    let id = runtime.nodes().try_register_driver(node)?;
    SESSION_QUEUE_NODE_RUNTIME_DATA.with(|data| data.set(Some(runtime_data)));
    Ok(id)
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

    pub(crate) fn attach_queue_by_runtime_data<Q>(
        runtime_data: NodeRuntimeData,
        handle: SessionQueueHandle<Q>,
        output_next: SessionQueueNext,
        dispatch: SessionQueueDispatchFn,
    ) -> CoreResult<()> {
        let slot = runtime_data.usize_word(0)?;
        SESSION_QUEUE_NODES.with(|nodes| {
            let mut nodes = nodes
                .try_borrow_mut()
                .map_err(|_| CoreError::internal("session queue nodes borrowed"))?;
            let node = nodes
                .get_mut(slot)
                .ok_or_else(|| CoreError::internal("session queue node slot is invalid"))?;
            node.push(SessionQueueAttachment {
                runtime_data: handle.runtime_data(),
                output_next,
                dispatch,
            });
            Ok(())
        })
    }

    pub(crate) fn registered_runtime_data() -> CoreResult<NodeRuntimeData> {
        SESSION_QUEUE_NODE_RUNTIME_DATA
            .with(|data| data.get())
            .ok_or_else(|| CoreError::internal("session queue node not registered"))
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
    _: &mut BufferFrame,
) -> CoreResult<NodeResult> {
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
    let mut output = SessionQueueOutput::default();
    for attachment in attachments {
        (attachment.dispatch)(
            runtime,
            attachment.runtime_data,
            attachment.output_next,
            now,
            &mut output,
        )?;
    }
    output.schedule(runtime)?;
    Ok(NodeResult::drop())
}

pub(crate) fn register_session_queue<Q: 'static>(queue: Q) -> CoreResult<SessionQueueHandle<Q>> {
    let queue = Box::leak(Box::new(RefCell::new(queue)));
    let runtime_data = session_queue_runtime_data(queue)?;
    Ok(SessionQueueHandle::new(runtime_data))
}

fn session_queue_runtime_data<Q>(queue: &'static RefCell<Q>) -> CoreResult<NodeRuntimeData> {
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
