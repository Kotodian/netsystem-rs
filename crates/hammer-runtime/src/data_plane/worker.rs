use super::*;

impl DataPlaneMain {
    pub(crate) fn set_worker_config(&mut self, worker_config: Worker) {
        self.worker_config = worker_config;
    }

    pub(crate) fn install_global_control(
        &mut self,
        registry: Arc<RuntimeRegistry>,
        main_loop_exit_now: Arc<AtomicBool>,
        main_loop_exit_status: Arc<Mutex<i32>>,
        publication: Arc<WorkerPublication>,
        workers_updating_graph: Arc<AtomicU32>,
        worker_config: Worker,
        worker_control_queues: Arc<[DataRemoteLocalQueue]>,
    ) {
        self.registry = registry;
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
    pub fn worker_barrier(&self) -> crate::barrier::WorkerBarrier {
        crate::barrier::global().expect("worker barrier is not installed")
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

    pub(crate) fn take_called_worker_init_functions(&mut self) -> HashSet<&'static str> {
        std::mem::take(&mut self.called_worker_init_functions)
    }

    pub(crate) fn restore_called_worker_init_functions(&mut self, called: HashSet<&'static str>) {
        self.called_worker_init_functions = called;
    }

    pub fn set_worker_node_runtime_data(
        &mut self,
        node: NodeId,
        data: NodeRuntime,
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
        Option<TraceControlHandle>,
    ) {
        (
            self.buffers.buffer_arenas().collect(),
            self.buffers.frame_slots(),
            self.nodes.snapshot(),
            self.simd_bytes,
            self.handoff.clone(),
            self.trace.control(),
        )
    }

    pub(crate) fn from_worker_parts(
        buffer_arenas: Vec<BufferPoolArena>,
        frame_slots: usize,
        nodes: NodeRuntimeInner,
        simd_bytes: usize,
        handoff: Option<DataPlaneHandoffWorker>,
        trace_control: Option<TraceControlHandle>,
        thread_index: u32,
        numa_node: u32,
    ) -> RuntimeResult<Self> {
        let buffers =
            DataPlaneBuffers::from_arenas(buffer_arenas, frame_slots, thread_index, numa_node);
        let mut runtime = Self::from_buffers(buffers, simd_bytes)?;
        runtime.nodes = nodes.into();
        runtime.handoff = handoff;
        runtime.trace.set_control(trace_control);
        Ok(runtime)
    }

    pub fn for_worker(&self, thread_index: u32, numa_node: u32) -> RuntimeResult<Self> {
        let (arenas, frame_slots, nodes, simd_bytes, handoff, trace_control) = self.worker_parts();
        Self::from_worker_parts(
            arenas,
            frame_slots,
            nodes,
            simd_bytes,
            handoff,
            trace_control,
            thread_index,
            numa_node,
        )
    }

    #[inline]
    pub fn attach_handoff_worker(mut runtime: Self, handoff: DataPlaneHandoffWorker) -> Self {
        runtime.handoff = Some(handoff);
        runtime
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{NodeDescriptor, NodeRuntime};
    use hammer_core::data_plane::{Frame, NodeKind, NodeRegistration};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    fn packet_output(_: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        frame.len()
    }

    fn packet_input(
        worker: &mut DataPlaneMain,
        state: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        assert!(worker.nodes().node_by_name("packet-added").is_none());
        let output = worker.nodes().node_by_name("packet-output").unwrap();
        for index in 1..=40 {
            let mut next = worker.get_frame_to_node(output).unwrap();
            next.set_vector_count(1);
            next.vector_args_mut()[0] = index;
            worker.put_frame_to_node(output, next).unwrap();
        }
        *state = NodeRuntime::from_words([state.word(0) + 1, 0, 0, 0]);
        frame.len()
    }

    fn packet_count(
        worker: &mut DataPlaneMain,
        state: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        assert!(worker.nodes().node_by_name("packet-added").is_none());
        assert_eq!(frame.vector_args(), &[state.word(0) as u32 + 1]);
        *state = NodeRuntime::from_words([state.word(0) + 1, 0, 0, 0]);
        frame.len()
    }

    #[test]
    fn worker_reforks_after_pending_dispatch() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                2,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let main = DataPlaneMain::new(DataPlaneBufferConfig::default());
        let output = main
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_count,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-output", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        let input = main
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_input,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-input", 1)),
                    &[output],
                    None,
                ),
            )
            .unwrap();
        let mut worker = DataPlaneMain::new(DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        worker.nodes = main.nodes().snapshot().into();
        main.nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-added", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        // SAFETY: publication and Worker are local to this test's OS thread;
        // no writer accesses the publication after this point.
        unsafe { worker.publication.set_graph(main.nodes().snapshot()) };
        worker.workers_updating_graph.store(1, Ordering::Release);
        let mut frame = worker.get_frame_to_node(input).unwrap();
        frame.set_vector_count(1);
        frame.vector_args_mut()[0] = 1;
        worker.put_frame_to_node(input, frame).unwrap();
        // main.c dispatches until the dynamically growing Pending vector is
        // exhausted. The loop-top refork is a separate operation afterward.
        assert_eq!(worker.run_ready_nodes().unwrap(), 41);
        assert_eq!(worker.run_ready_nodes().unwrap(), 0);
        assert_eq!(worker.nodes().node_runtime_data(input).unwrap().word(0), 1);
        assert_eq!(
            worker.nodes().node_runtime_data(output).unwrap().word(0),
            40
        );
        assert_eq!(worker.workers_updating_graph.load(Ordering::Acquire), 1);
        worker.refork_worker_graph();
        assert!(worker.nodes().node_by_name("packet-added").is_some());
        assert_eq!(worker.workers_updating_graph.load(Ordering::Acquire), 0);
        assert_eq!(worker.run_ready_nodes().unwrap(), 0);
        assert_eq!(worker.nodes().node_runtime_data(input).unwrap().word(0), 1);
        assert_eq!(
            worker.nodes().node_runtime_data(output).unwrap().word(0),
            40
        );
    }

    #[test]
    fn refork_completion_follows_clone_replacement() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                2,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let main = DataPlaneMain::new(DataPlaneBufferConfig::default());
        let output = main
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-output", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        let input = main
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("packet-input", 1)),
                    &[output],
                    None,
                ),
            )
            .unwrap();
        let graph = main.nodes().snapshot();
        let added = main
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    packet_output,
                    NodeRuntime::from_words([29, 0, 0, 0]),
                    Some(NodeRegistration::next("packet-added", 0)),
                    &[],
                    None,
                ),
            )
            .unwrap();
        // SAFETY: no Workers exist yet; publication remains unchanged until
        // both joins. Thread creation publishes these writes to both readers.
        unsafe { main.publication.set_graph(main.nodes().snapshot()) };
        main.workers_updating_graph.store(2, Ordering::Release);
        let mut workers = Vec::new();
        for worker_index in 1..=2 {
            let graph = graph.clone();
            let publication = Arc::clone(&main.publication);
            let updating = Arc::clone(&main.workers_updating_graph);
            workers.push(std::thread::spawn(move || {
                let mut worker = DataPlaneMain::new(DataPlaneBufferConfig {
                    thread_index: worker_index,
                    ..Default::default()
                });
                worker.nodes = graph.into();
                worker.publication = publication;
                worker.workers_updating_graph = updating;
                worker
                    .set_worker_node_runtime_data(
                        output,
                        NodeRuntime::from_words([u64::from(worker_index), 0, 0, 0]),
                    )
                    .unwrap();
                worker.with_current_node(input, |worker| {
                    worker.get_next_frame::<u32, ()>(&mut NodeRuntime::empty(), 0);
                });
                assert_eq!(worker.nodes().frames_in_use(), 1);
                if worker_index == 2 {
                    // threads.h decrements only after replacing the clone.
                    // Hold this Worker's old clone until its peer has finished
                    // replacement and is waiting on the real completion count.
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while worker.workers_updating_graph.load(Ordering::Acquire) != 1 {
                        assert!(Instant::now() < deadline);
                        std::thread::yield_now();
                    }
                    assert_eq!(worker.nodes().frames_in_use(), 1);
                    assert!(worker.nodes().node_by_name("packet-added").is_none());
                }
                worker.refork_worker_graph();
                assert_eq!(worker.workers_updating_graph.load(Ordering::Acquire), 0);
                assert_eq!(worker.nodes().frames_in_use(), 0);
                assert_eq!(
                    worker.nodes().node_runtime_data(output).unwrap().word(0),
                    u64::from(worker_index)
                );
                assert_eq!(worker.nodes().node_runtime_data(added).unwrap().word(0), 29);
                worker.refork_worker_graph();
                assert_eq!(worker.workers_updating_graph.load(Ordering::Acquire), 0);
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(main.workers_updating_graph.load(Ordering::Acquire), 0);
    }
}
