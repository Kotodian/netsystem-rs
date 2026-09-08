use rand::{SeedableRng, rngs::SmallRng};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};

use crate::error::{RuntimeError, RuntimeResult};
use crate::file::{FILE_MAIN, FileMain};
use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, BufferFrame, BufferPoolArena, BufferRef,
    DEFAULT_BUFFER_FRAME_POOL_SIZE, DataPlaneBuffers, Frame, FrameBatchWidth, Next, NodeErrorIndex,
    NodeId, NodeKind, NodeRegistration, Pending,
};
use hammer_core::error::{DataPlaneError, DataPlaneResult};
use hammer_infra::PageSize;

use crate::config::Worker;
use crate::global_main::WorkerPublication;
use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
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
    random: Rc<RefCell<SmallRng>>,
    buffers: DataPlaneBuffers,
    nodes: NodeRuntime,
    current_node: Rc<Cell<Option<NodeId>>>,
    /// Worker-local appendable Next Frame per (current node × local slot).
    pub(crate) appendable_next_frames: RefCell<Vec<(NodeId, u16, Frame<Next>)>>,
    handoff: Option<DataPlaneHandoffWorker>,
    active_numa_node: u32,
    trace: DataPlaneTrace,
    simd_bytes: usize,
    registry: Arc<RuntimeRegistry>,
    main_loop_exit_now: Arc<AtomicBool>,
    main_loop_exit_status: Arc<Mutex<i32>>,
    publication: Arc<WorkerPublication>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_config: Worker,
    worker_exit_functions: Vec<fn(&mut DataPlaneMain) -> RuntimeResult<()>>,
    called_worker_init_functions: HashSet<&'static str>,
    main_loop_count: AtomicU32,
    worker_control_queues: Arc<[DataRemoteLocalQueue]>,
}

impl DataPlaneMain {
    /// Worker-local, non-cryptographic randomness seeded at worker construction.
    #[inline]
    pub fn random(&self) -> std::cell::RefMut<'_, SmallRng> {
        self.random.borrow_mut()
    }
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
