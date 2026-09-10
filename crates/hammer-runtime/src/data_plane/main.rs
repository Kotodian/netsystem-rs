use rand::{SeedableRng, rngs::SmallRng};
use std::cell::Cell;
use std::fmt;

use crate::error::{RuntimeError, RuntimeResult};
use crate::file::{FILE_MAIN, FileMode};
use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, DEFAULT_BUFFER_FRAME_POOL_SIZE, Frame, FrameBatchWidth, NodeErrorIndex,
    NodeId, NodeKind, NodeRegistration,
};
use hammer_core::error::DataPlaneError;
use hammer_infra::PageSize;
use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::bitmap::Bitmap;

use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
use crate::node::{
    NodeEntry, NodeErrorCode, NodeFunctionRegistration, NodeMain, NodeRuntime, NodeRuntimeInner,
};
use crate::runtime_simd::{native_simd_bytes, preferred_frame_batch_width};
use crate::trace::{DataPlaneTrace, PacketTrace, TraceControlHandle};

mod buffer_pool;
mod config;
mod dispatch;
mod frame_queue;
mod handoff;
mod trace;
mod worker;

pub use config::DataPlaneBufferConfig;

#[repr(C)]
pub struct DataPlaneMain {
    cacheline0: CacheLineAlignMark,
    random: SmallRng,
    thread_index: u32,
    buffer_main: &'static hammer_core::buffer::BufferMain,
    pub(crate) nodes: NodeMain,
    current_node: Cell<Option<NodeId>>,
    handoff: Option<DataPlaneHandoffWorker>,
    active_numa_node: u32,
    trace: DataPlaneTrace,
    simd_bytes: usize,
    file_main: FileMode,
    main_loop_count: u32,
    main_loop_exit_now: bool,
    main_loop_exit_status: i32,
    pub(crate) worker_init_functions_called: Bitmap,
}

const _: () = {
    assert!(core::mem::align_of::<DataPlaneMain>() == CACHE_LINE);
    assert!(core::mem::offset_of!(DataPlaneMain, cacheline0) == 0);
    assert!(core::mem::offset_of!(DataPlaneMain, random) == 0);
};

impl DataPlaneMain {
    /// Worker-local, non-cryptographic randomness seeded at worker construction.
    #[inline]
    pub fn random(&mut self) -> &mut SmallRng {
        &mut self.random
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
            .field("main_loop_count", &self.main_loop_count)
            .field("main_loop_exit_now", &self.main_loop_exit_now)
            .field("main_loop_exit_status", &self.main_loop_exit_status)
            .finish()
    }
}
