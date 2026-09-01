use super::*;

impl DataPlaneMain {
    pub(crate) fn set_worker_config(&mut self, worker_config: Worker) {
        self.worker_config = worker_config;
    }

    pub(crate) fn install_global_control(
        &mut self,
        registry: Arc<RuntimeRegistry>,
        barrier: WorkerBarrier,
        main_loop_exit_now: Arc<AtomicBool>,
        main_loop_exit_status: Arc<Mutex<i32>>,
        publication: Arc<WorkerPublication>,
        workers_updating_graph: Arc<AtomicU32>,
        worker_config: Worker,
        worker_control_queues: Arc<[DataRemoteLocalQueue]>,
    ) {
        self.registry = registry;
        self.barrier = barrier;
        self.main_loop_exit_now = main_loop_exit_now;
        self.main_loop_exit_status = main_loop_exit_status;
        self.publication = publication;
        self.workers_updating_graph = workers_updating_graph;
        self.worker_config = worker_config;
        self.worker_control_queues = worker_control_queues;
    }

    #[inline]
    pub fn registry(&self) -> &RuntimeRegistry {
        &self.registry
    }

    #[inline]
    pub fn worker_barrier(&self) -> WorkerBarrier {
        self.barrier.clone()
    }

    #[inline]
    pub fn main_loop_exit_requested(&self) -> bool {
        self.main_loop_exit_now
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn data_worker_id(&self) -> RuntimeResult<DataWorkerId> {
        DataWorkerId::try_from(self.thread_index())
    }

    #[inline]
    pub fn configured_worker_count(&self) -> usize {
        self.worker_config.count
    }

    #[inline]
    pub(crate) fn worker_config(&self) -> &Worker {
        &self.worker_config
    }

    #[inline]
    pub(crate) fn increment_main_loop_count(&self) {
        self.main_loop_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn main_loop_exit_status(&self) -> i32 {
        *self
            .main_loop_exit_status
            .lock()
            .expect("DataPlaneMain exit status mutex poisoned")
    }

    pub(crate) fn poll_file_readiness(&self) -> RuntimeResult<usize> {
        self.file_main()
            .poll_for_worker(self.thread_index(), self.nodes())
    }

    #[inline]
    pub fn register_worker_exit_function(
        &mut self,
        function: fn(&mut DataPlaneMain) -> RuntimeResult<()>,
    ) {
        self.worker_exit_functions.push(function);
    }

    pub(crate) fn take_worker_exit_functions(
        &mut self,
    ) -> Vec<fn(&mut DataPlaneMain) -> RuntimeResult<()>> {
        std::mem::take(&mut self.worker_exit_functions)
    }

    pub(crate) fn take_called_worker_init_functions(&mut self) -> HashSet<&'static str> {
        std::mem::take(&mut self.called_worker_init_functions)
    }

    pub(crate) fn restore_called_worker_init_functions(&mut self, called: HashSet<&'static str>) {
        self.called_worker_init_functions = called;
    }

    pub fn set_worker_node_runtime_data(
        &mut self,
        node: NodeId,
        data: NodeRuntimeData,
    ) -> RuntimeResult<()> {
        self.data_worker_id()?;
        self.nodes.set_node_runtime_data(node, data)
    }

    pub(crate) fn refork_worker_graph(&mut self) {
        use std::sync::atomic::Ordering;

        if self.workers_updating_graph.load(Ordering::Acquire) == 0 {
            return;
        }

        // SAFETY: GlobalMain publishes this value before releasing the worker
        // barrier and retains it until every worker completes the refork.
        let graph = unsafe { self.publication.graph() }
            .as_ref()
            .expect("published worker graph must be present")
            .clone();
        self.nodes.refork(graph);

        let updating = self.workers_updating_graph.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(updating, 0, "worker graph completion count underflow");
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            core::hint::spin_loop();
        }
    }
}

impl DataPlaneMain {
    pub(crate) fn worker_parts(
        &self,
    ) -> (
        Vec<BufferPoolArena>,
        usize,
        NodeRuntimeInner,
        usize,
        Option<DataPlaneHandoffWorker>,
        Option<NodeHandle>,
        Option<TraceControlHandle>,
    ) {
        (
            self.buffers.buffer_arenas().collect(),
            self.buffers.frame_slots(),
            self.nodes.snapshot(),
            self.simd_bytes,
            self.handoff.clone(),
            self.handoff_node_handle,
            self.trace.control(),
        )
    }

    pub(crate) fn from_worker_parts(
        buffer_arenas: Vec<BufferPoolArena>,
        frame_slots: usize,
        nodes: NodeRuntimeInner,
        simd_bytes: usize,
        handoff: Option<DataPlaneHandoffWorker>,
        handoff_node_handle: Option<NodeHandle>,
        trace_control: Option<TraceControlHandle>,
        thread_index: u32,
        numa_node: u32,
    ) -> RuntimeResult<Self> {
        let buffers =
            DataPlaneBuffers::from_arenas(buffer_arenas, frame_slots, thread_index, numa_node);
        let mut runtime = Self::from_buffers(buffers, simd_bytes)?;
        runtime.nodes = nodes.into();
        runtime.handoff = handoff;
        runtime.handoff_node_handle = handoff_node_handle;
        runtime.trace.set_control(trace_control);
        if let Some(arena) = runtime
            .handoff
            .as_ref()
            .and_then(DataPlaneHandoffWorker::configured_buffer_arena)
        {
            runtime.buffers = runtime.buffers.with_active_buffer_arena(arena);
            runtime.active_numa_node = runtime.buffers.active_numa_node();
        }
        Ok(runtime)
    }

    pub fn for_worker(&self, thread_index: u32, numa_node: u32) -> RuntimeResult<Self> {
        let (arenas, frame_slots, nodes, simd_bytes, handoff, handoff_node_handle, trace_control) =
            self.worker_parts();
        Self::from_worker_parts(
            arenas,
            frame_slots,
            nodes,
            simd_bytes,
            handoff,
            handoff_node_handle,
            trace_control,
            thread_index,
            numa_node,
        )
    }

    #[inline]
    pub fn attach_handoff_worker(mut runtime: Self, handoff: DataPlaneHandoffWorker) -> Self {
        if let Some(arena) = handoff.configured_buffer_arena() {
            runtime.buffers = runtime.buffers.with_active_buffer_arena(arena);
            runtime.active_numa_node = runtime.buffers.active_numa_node();
        }
        runtime.handoff = Some(handoff);
        runtime
    }

    #[inline]
    pub fn set_handoff_node_handle(&mut self, handle: NodeHandle) {
        self.handoff_node_handle = Some(handle);
    }

    #[inline]
    pub fn handoff_node_handle(&self) -> RuntimeResult<NodeHandle> {
        self.handoff_node_handle
            .ok_or(DataPlaneError::HandoffNodeHandleMissing.into())
    }
}
