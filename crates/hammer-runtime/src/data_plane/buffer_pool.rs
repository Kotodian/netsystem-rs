use super::*;

impl DataPlaneMain {
    pub fn new(config: DataPlaneBufferConfig) -> Self {
        Self::try_new(config).expect("create ordinary-page data-plane runtime")
    }

    #[inline]
    pub fn try_new(config: DataPlaneBufferConfig) -> RuntimeResult<Self> {
        Self::from_buffers(config.try_into()?, native_simd_bytes())
    }

    #[inline]
    pub(crate) fn from_buffers(
        buffers: DataPlaneBuffers,
        simd_bytes: usize,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            appendable_next_frames: RefCell::new(Vec::with_capacity(
                hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY,
            )),
            handoff: None,
            handoff_node_handle: None,
            trace: DataPlaneTrace::default(),
            simd_bytes,
            registry: crate::RuntimeRegistry::new(),
            barrier: crate::barrier::WorkerBarrier::new(0),
            main_loop_exit_now: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            main_loop_exit_status: std::sync::Arc::new(std::sync::Mutex::new(0)),
            publication: std::sync::Arc::new(crate::global_main::WorkerPublication::new()),
            workers_updating_graph: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
            worker_config: Worker::default(),
            worker_exit_functions: Vec::new(),
            called_worker_init_functions: std::collections::HashSet::new(),
            main_loop_count: std::sync::atomic::AtomicU32::new(0),
            worker_control_queues: std::sync::Arc::from([]),
        })
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    /// VPP-style runtime thread index: zero for main, one-based for workers.
    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.buffers.thread_index()
    }

    #[inline]
    pub fn alloc_index(&self) -> RuntimeResult<Index> {
        Ok(self.buffers.alloc_index()?)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> RuntimeResult<Index> {
        Ok(self.buffers.alloc_index_with_bytes(bytes)?)
    }

    #[inline]
    pub(crate) fn drop_index_owned(&self, index: Index) {
        self.buffers
            .drop_index_owned_with_trace(index, |handle| self.trace.finalize(handle));
    }

    #[inline]
    pub(crate) fn drop_pending_frame_owned(&self, frame: Frame<Pending>) {
        frame.return_with_trace_release(|handle| self.trace.finalize(handle));
    }

    #[inline]
    pub fn prefetch_header(&self, index: Index) {
        self.buffers.prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: Index) {
        self.buffers.prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: Index) {
        self.buffers.prefetch_write(index);
    }

    #[inline]
    pub fn chain(
        &self,
        index: Index,
    ) -> impl Iterator<Item = Result<BufferRef<'_>, DataPlaneError>> + '_ {
        self.buffers.chain(index)
    }

    #[inline]
    pub fn current_config(&self, index: Index) -> RuntimeResult<NodeId> {
        Ok(self.buffers.current_config(index)?)
    }

    #[inline]
    pub fn put_next_frame(&self, frame: Frame<Next>) -> RuntimeResult<()> {
        let next = frame.next();
        let pending = frame.into_pending()?;
        if pending.is_empty() {
            return Ok(());
        }
        self.nodes.schedule_frame(next, pending, false)
    }

    #[inline]
    pub fn get_buffer(&self, index: Index) -> RuntimeResult<BufferRef<'_>> {
        Ok(self.buffers.get_buffer(index)?)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: Index) -> RuntimeResult<BufferRefMut<'_>> {
        Ok(self.buffers.get_buffer_mut(index)?)
    }

    #[inline]
    pub fn nodes(&self) -> &NodeRuntime {
        &self.nodes
    }

    pub fn file_main(&self) -> &'static FileMain {
        FILE_MAIN
            .get()
            .expect("FileMain is initialized before data-plane use")
    }
}
