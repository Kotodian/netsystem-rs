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
        // Like vlib_main's clock seed, this is not cryptographic entropy.
        // Seed once at runtime construction, never on the packet path.
        let elapsed = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => elapsed,
            Err(error) => error.duration(),
        };
        let seed = elapsed.as_nanos() as u64 ^ u64::from(buffers.thread_index());
        Ok(Self {
            random: Rc::new(RefCell::new(SmallRng::seed_from_u64(seed))),
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            appendable_next_frames: RefCell::new(Vec::with_capacity(
                hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY,
            )),
            handoff: None,
            trace: DataPlaneTrace::default(),
            simd_bytes,
            registry: crate::RuntimeRegistry::new(),
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
    pub fn alloc_index(&self) -> RuntimeResult<u32> {
        Ok(self.buffers.alloc_index()?)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> RuntimeResult<u32> {
        Ok(self.buffers.alloc_index_with_bytes(bytes)?)
    }

    #[inline]
    pub(crate) fn drop_index_owned(&self, index: u32) {
        self.buffers
            .drop_index_owned_with_trace(index, |handle| self.trace.finalize(handle));
    }

    #[inline]
    pub(crate) fn drop_pending_frame_owned(&self, frame: Frame<Pending>) {
        frame.return_with_trace_release(|handle| self.trace.finalize(handle));
    }

    #[inline]
    pub fn prefetch_header(&self, index: u32) {
        self.buffers.prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: u32) {
        self.buffers.prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: u32) {
        self.buffers.prefetch_write(index);
    }

    #[inline]
    pub fn chain(
        &self,
        index: u32,
    ) -> impl Iterator<Item = Result<BufferRef<'_>, DataPlaneError>> + '_ {
        self.buffers.chain(index)
    }

    #[inline]
    pub fn current_config_index(&self, index: u32) -> RuntimeResult<u32> {
        Ok(self.buffers.current_config_index(index)?)
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
    pub fn buffer(&self, index: u32) -> &hammer_core::data_plane::Buffer {
        // SAFETY: the graph owns this live index on the calling Worker. The
        // returned borrow is bounded by this runtime borrow, not BufferMain's
        // process lifetime. Shared chain tails remain immutable.
        unsafe { hammer_core::buffer::BufferMain::global().buffer(index) }
    }

    #[inline]
    pub fn buffer_mut(&mut self, index: u32) -> &mut hammer_core::data_plane::Buffer {
        // SAFETY: Node invocation borrows its Worker runtime exclusively. The
        // graph owns the index throughout this borrow; core rejects shared
        // tails. The borrow ends before the Node transfers the index onward.
        unsafe { hammer_core::buffer::BufferMain::global().buffer_mut(index) }
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
