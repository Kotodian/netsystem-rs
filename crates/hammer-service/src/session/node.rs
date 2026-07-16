use std::any::Any;
use std::cell::{Cell, RefCell, RefMut};
use std::fmt;
use std::marker::PhantomData;
use std::os::fd::BorrowedFd;
use std::rc::Rc;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, Index, NodeId, NodeRegistration};
use hammer_core::error::{AttachError, CoreError, CoreResult};
use hammer_runtime::app::{SessionEventQueue, SessionSegment};
use hammer_runtime::{
    DataPlaneRuntime, DriverNode, Engine, File, FileFunctions, Node, NodeProcessFn, NodeResult,
    NodeRuntimeData,
};

use crate::session::SessionQueueError;
use crate::session::runtime::SessionDriverRuntime;

/// Shared Session Queue IO allowance for normal and custom TX in one dispatch.
pub const SESSION_QUEUE_IO_BUDGET: usize = 128;

#[derive(PartialEq, Eq)]
pub struct SessionQueueHandle<Q> {
    runtime_data: NodeRuntimeData,
    _queue: PhantomData<Rc<Q>>,
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
        let queue = queue
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queue borrowed"))?;
        RefMut::filter_map(queue, Option::as_mut).map_err(|_| CoreError::service_closed())
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

struct SessionQueueOwner {
    attachment_slot: usize,
    queue: &'static dyn Any,
    release: fn(&'static dyn Any),
    readiness_file: Option<hammer_infra::pool::Index>,
}

impl Drop for SessionQueueOwner {
    fn drop(&mut self) {
        (self.release)(self.queue);
    }
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
    static SESSION_QUEUE_OWNERS: RefCell<Vec<SessionQueueOwner>> = const { RefCell::new(Vec::new()) };
    static SESSION_QUEUE_NODE_RUNTIME_DATA: Cell<Option<NodeRuntimeData>> = const { Cell::new(None) };
    static SESSION_QUEUE_NODE_ID: Cell<Option<NodeId>> = const { Cell::new(None) };
}

pub fn register_session_queue_node(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("session-queue") {
        return Ok(node);
    }
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

pub fn register_session_queue<T, Seg, QueueIndex>(
    engine: &mut Engine,
    session_queue: NodeId,
    node_runtime_data: NodeRuntimeData,
    queue: SessionDriverRuntime<T, Seg, QueueIndex>,
) -> CoreResult<SessionQueueHandle<SessionDriverRuntime<T, Seg, QueueIndex>>>
where
    T: 'static,
    Seg: SessionSegment,
    QueueIndex: Copy + Eq + 'static,
{
    type Queue<T, Seg, QueueIndex> = RefCell<Option<SessionDriverRuntime<T, Seg, QueueIndex>>>;

    let attachment_slot = node_runtime_data.usize_word(0)?;
    SESSION_QUEUE_OWNERS.with(|owners| {
        let mut owners = owners
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queue owners borrowed"))?;
        if let Some(owner) = owners
            .iter_mut()
            .find(|owner| owner.attachment_slot == attachment_slot)
        {
            let Some(queue_cell) = owner.queue.downcast_ref::<Queue<T, Seg, QueueIndex>>() else {
                return Err(CoreError::service_closed());
            };
            if let Some(file) = owner.readiness_file {
                let _ = engine.file_main_mut()?.delete(file)?;
                owner.readiness_file = None;
            }
            *queue_cell.borrow_mut() = Some(queue);
            let queue = queue_cell.borrow();
            let Some(queue) = queue.as_ref() else {
                return Err(CoreError::service_closed());
            };
            owner.readiness_file = register_session_queue_readiness(engine, session_queue, queue)?;
            return Ok(SessionQueueHandle::new(session_queue_node_runtime_data(
                queue_cell,
            )?));
        }

        let queue = Box::leak(Box::new(RefCell::new(Some(queue))));
        let runtime_data = session_queue_node_runtime_data(queue)?;
        owners.push(SessionQueueOwner {
            attachment_slot,
            queue,
            release: release_session_queue::<SessionDriverRuntime<T, Seg, QueueIndex>>,
            readiness_file: None,
        });
        let queue = queue.borrow();
        let Some(queue) = queue.as_ref() else {
            return Err(CoreError::service_closed());
        };
        let owner_index = owners.len() - 1;
        owners[owner_index].readiness_file =
            register_session_queue_readiness(engine, session_queue, queue)?;
        Ok(SessionQueueHandle::new(runtime_data))
    })
}

fn release_session_queue<Q: 'static>(queue: &'static dyn Any) {
    let Some(queue) = queue.downcast_ref::<RefCell<Option<Q>>>() else {
        return;
    };
    if let Ok(mut queue) = queue.try_borrow_mut() {
        queue.take();
    }
}

fn register_session_queue_readiness<T, Seg, QueueIndex>(
    engine: &mut Engine,
    session_queue: NodeId,
    queue: &SessionDriverRuntime<T, Seg, QueueIndex>,
) -> CoreResult<Option<hammer_infra::pool::Index>>
where
    Seg: SessionSegment,
    QueueIndex: Copy + Eq,
{
    let Some(signal_read_fd) = queue.app().tx_evt_q().read_fd() else {
        return Ok(None);
    };
    // SAFETY: the Session Runtime queue remains owned by SESSION_QUEUE_OWNERS
    // until its File registration is removed. Cloning does not consume it.
    let signal_read = unsafe { BorrowedFd::borrow_raw(signal_read_fd) }
        .try_clone_to_owned()
        .map_err(|source| AttachError::SessionSignalDuplicate { source })?;
    let worker = engine.data_worker_id()?;
    engine
        .file_main_mut()?
        .add(File::new(
            signal_read,
            worker,
            "SVM session queue app-to-dataplane signal".to_owned(),
            u64::from(session_queue.slot()),
            FileFunctions {
                read: Some(schedule_session_queue),
                ..FileFunctions::default()
            },
        ))
        .map(Some)
}

fn schedule_session_queue(file: &mut File) -> CoreResult<()> {
    let node = NodeId::new(file.private_data() as u32);
    Engine::with_current(|engine| engine.runtime.schedule_empty_frame(node))
        .expect("File callbacks run on their polling Engine")?;
    Ok(())
}

fn session_queue_node_runtime_data<Q>(
    queue: &'static RefCell<Option<Q>>,
) -> CoreResult<NodeRuntimeData> {
    Ok(NodeRuntimeData::from_words([
        u64::try_from(queue as *const _ as usize)
            .map_err(|_| CoreError::internal("session queue pointer overflow"))?,
        0,
        0,
        0,
    ]))
}

fn session_queue_cell<Q>(data: NodeRuntimeData) -> CoreResult<&'static RefCell<Option<Q>>> {
    let raw = usize::try_from(data.word(0))
        .map_err(|_| CoreError::internal("session queue pointer is invalid"))?;
    let ptr = raw as *const RefCell<Option<Q>>;
    // SAFETY: register_session_queue leaks the emptyable RefCell allocation and
    // stores its exact pointer in NodeRuntimeData. SessionQueueOwner releases Q
    // at worker teardown without invalidating the allocation, and the typed
    // handle preserves the same Q on recovery.
    unsafe { ptr.as_ref() }.ok_or_else(|| CoreError::internal("session queue is missing"))
}

#[cfg(test)]
#[path = "node/tests.rs"]
mod tests;
