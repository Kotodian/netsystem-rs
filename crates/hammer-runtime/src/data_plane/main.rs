use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};

use crate::error::{RuntimeError, RuntimeResult};
use crate::file::{FILE_MAIN, FileMain};
use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, BufferFrame, BufferPoolArena, BufferRef, BufferRefMut,
    DEFAULT_BUFFER_FRAME_POOL_SIZE, DataPlaneBuffers, Frame, FrameBatchWidth, Index, Next,
    NodeErrorIndex, NodeHandle, NodeId, NodeKind, NodeNext, NodeRegistration, Pending,
};
use hammer_core::error::{DataPlaneError, DataPlaneResult};
use hammer_infra::PageSize;

use crate::barrier::WorkerBarrier;
use crate::config::Worker;
use crate::global_main::WorkerPublication;
use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
use crate::init::WorkerInitFunction;
use crate::node::{
    NodeEntry, NodeErrorCode, NodeFunctionRegistration, NodeRuntime, NodeRuntimeData,
    NodeRuntimeInner,
};
use crate::registry::RuntimeRegistry;
use crate::runtime_simd::{native_simd_bytes, preferred_frame_batch_width};
use crate::spawn::DataRemoteLocalQueue;
use crate::trace::{DataPlaneTrace, PacketTrace, TraceControlHandle};

mod buffer_pool;
mod config;
mod dispatch;
mod frame_queue;
mod handoff;
mod trace;
mod worker;

pub use config::DataPlaneBufferConfig;

pub struct DataPlaneMain {
    buffers: DataPlaneBuffers,
    nodes: NodeRuntime,
    current_node: Rc<Cell<Option<NodeId>>>,
    /// Worker-local appendable Next Frame per (current node × local slot).
    pub(crate) appendable_next_frames: RefCell<Vec<(NodeId, u16, Frame<Next>)>>,
    handoff: Option<DataPlaneHandoffWorker>,
    handoff_node_handle: Option<NodeHandle>,
    active_numa_node: u32,
    trace: DataPlaneTrace,
    simd_bytes: usize,
    registry: Arc<RuntimeRegistry>,
    barrier: WorkerBarrier,
    main_loop_exit_now: Arc<AtomicBool>,
    main_loop_exit_status: Arc<Mutex<i32>>,
    publication: Arc<WorkerPublication>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_config: Worker,
    worker_init_functions: Vec<WorkerInitFunction>,
    worker_exit_functions: Vec<fn(&mut DataPlaneMain) -> RuntimeResult<()>>,
    called_worker_init_functions: HashSet<&'static str>,
    main_loop_count: AtomicU32,
    worker_control_queues: Arc<[DataRemoteLocalQueue]>,
}

impl fmt::Debug for DataPlaneMain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneMain")
            .field("buffers", &self.buffers)
            .field("nodes", &self.nodes)
            .field("current_node", &self.current_node.get())
            .field(
                "appendable_next_frames",
                &self.appendable_next_frames.borrow().len(),
            )
            .field("handoff", &self.handoff)
            .field("handoff_node_handle", &self.handoff_node_handle)
            .field("active_numa_node", &self.active_numa_node)
            .field("trace", &self.trace)
            .field("simd_bytes", &self.simd_bytes)
            .field("thread_index", &self.thread_index())
            .finish()
    }
}

struct HandoffSlotGuard<'runtime> {
    runtime: &'runtime DataPlaneMain,
    slot: Option<HandoffSlot>,
}

impl<'runtime> HandoffSlotGuard<'runtime> {
    #[inline]
    fn new(runtime: &'runtime DataPlaneMain, slot: HandoffSlot) -> Self {
        Self {
            runtime,
            slot: Some(slot),
        }
    }

    #[inline]
    fn push_into_frame(&mut self, frame: &mut Frame<Next>) -> RuntimeResult<()> {
        match self.slot.as_ref() {
            Some(slot) => frame.push_indices(slot.iter())?,
            None => return Ok(()),
        }
        self.slot = None;
        Ok(())
    }
}

impl Drop for HandoffSlotGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.runtime.drop_handoff_slot_owned(slot);
        }
    }
}

impl Clone for DataPlaneMain {
    fn clone(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            nodes: self.nodes.clone(),
            current_node: Rc::clone(&self.current_node),
            appendable_next_frames: RefCell::new(Vec::with_capacity(
                hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY,
            )),
            handoff: self.handoff.clone(),
            handoff_node_handle: self.handoff_node_handle,
            active_numa_node: self.active_numa_node,
            trace: self.trace.clone(),
            simd_bytes: self.simd_bytes,
            registry: Arc::clone(&self.registry),
            barrier: self.barrier.clone(),
            main_loop_exit_now: Arc::clone(&self.main_loop_exit_now),
            main_loop_exit_status: Arc::clone(&self.main_loop_exit_status),
            publication: Arc::clone(&self.publication),
            workers_updating_graph: Arc::clone(&self.workers_updating_graph),
            worker_config: self.worker_config.clone(),
            worker_init_functions: self.worker_init_functions.clone(),
            worker_exit_functions: self.worker_exit_functions.clone(),
            called_worker_init_functions: self.called_worker_init_functions.clone(),
            main_loop_count: AtomicU32::new(
                self.main_loop_count
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            worker_control_queues: Arc::clone(&self.worker_control_queues),
        }
    }
}
