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
    BUFFER_CACHE_LINE_SIZE, DEFAULT_BUFFER_FRAME_POOL_SIZE, Frame, FrameBatchWidth, NodeErrorIndex,
    NodeId, NodeKind, NodeRegistration,
};
use hammer_core::error::DataPlaneError;
use hammer_infra::PageSize;

use crate::config::Worker;
use crate::global_main::WorkerPublication;
use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
use crate::node::{
    NodeEntry, NodeErrorCode, NodeFunctionRegistration, NodeMain, NodeRuntime, NodeRuntimeInner,
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
    thread_index: u32,
    pub(crate) nodes: NodeMain,
    current_node: Rc<Cell<Option<NodeId>>>,
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
            .field("thread_index", &self.thread_index)
            .field("nodes", &self.nodes)
            .field("current_node", &self.current_node.get())
            .field("handoff", &self.handoff)
            .field("active_numa_node", &self.active_numa_node)
            .field("trace", &self.trace)
            .field("simd_bytes", &self.simd_bytes)
            .field("thread_index", &self.thread_index())
            .finish()
    }
}
