use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::*;
use crate::ThreadMain;

impl DataPlaneMain {
    #[inline]
    pub fn main_loop_exit_requested(&self) -> bool {
        self.main_loop_exit_now
    }

    #[inline]
    pub fn data_worker_id(&self) -> RuntimeResult<DataWorkerId> {
        DataWorkerId::try_from(self.thread_index())
    }

    #[inline]
    pub(crate) fn request_exit(&mut self, status: i32) {
        self.main_loop_exit_status = status;
        self.main_loop_exit_now = true;
    }

    /// VPP's two adjacent main-loop steps: publish the counter increment
    /// (`vlib_increment_main_loop_counter`) and, when the reporting interval is
    /// over, update the damped loops-per-second value.
    ///
    /// The counter goes into this thread's descriptor because the collector
    /// runs on another thread; the rate window stays in this worker's own
    /// fields. Both values have a single writer, so the relaxed orderings carry
    /// no ownership or publication claim.
    #[inline]
    pub(crate) fn increment_main_loop_count(&mut self) {
        let threads = ThreadMain::global();
        threads
            .worker_main_loop_count(self.thread_index())
            .fetch_add(1, Ordering::Relaxed);
        self.loops_this_reporting_interval += 1;

        let now = Instant::now();
        if now < self.loop_interval_end {
            return;
        }
        if let Some(start) = self.loop_interval_start {
            let elapsed = now.duration_since(start).as_secs_f64();
            if elapsed > 0.0 {
                let interval_rate = self.loops_this_reporting_interval as f64 / elapsed;
                self.loops_per_second = self.loops_per_second * self.damping_constant
                    + (1.0 - self.damping_constant) * interval_rate;
                threads
                    .worker_loops_per_second(self.thread_index())
                    .store(self.loops_per_second as u64, Ordering::Relaxed);
            }
        }
        self.loop_interval_start = Some(now);
        self.loop_interval_end = now + Duration::from_micros(200);
        self.loops_this_reporting_interval = 0;
    }

    #[inline]
    pub(crate) fn main_loop_exit_status(&self) -> i32 {
        self.main_loop_exit_status
    }

    pub(crate) fn poll_file_readiness(&mut self) -> RuntimeResult<usize> {
        let thread_index = self.thread_index();
        match &self.file_main {
            FileMode::Sync => FILE_MAIN
                .get()
                .expect("FileMain is initialized before data-plane use")
                .poll_for_worker(thread_index, &mut self.nodes),
            FileMode::Async(_) => panic!("thread zero awaits AsyncFileMain readiness"),
        }
    }

    pub async fn next_file_readiness(&mut self) -> RuntimeResult<usize> {
        let Self {
            file_main, nodes, ..
        } = self;
        match file_main {
            FileMode::Async(file_main) => file_main.next_ready(nodes).await,
            FileMode::Sync => panic!("Data Workers poll synchronous File readiness"),
        }
    }

    pub(crate) fn take_worker_init_functions_called(&mut self) -> Bitmap {
        std::mem::take(&mut self.worker_init_functions_called)
    }

    pub(crate) fn restore_worker_init_functions_called(&mut self, called: Bitmap) {
        self.worker_init_functions_called = called;
    }

    pub fn set_worker_node_runtime_data(
        &mut self,
        node: NodeId,
        data: NodeRuntime,
    ) -> RuntimeResult<()> {
        self.data_worker_id()?;
        self.nodes.set_node_runtime_data(node, data)
    }
}

impl DataPlaneMain {
    pub(crate) fn worker_parts(
        &self,
    ) -> (
        NodeMain,
        usize,
        Option<DataPlaneHandoffWorker>,
        Option<TraceControlHandle>,
    ) {
        (
            self.nodes.clone(),
            self.simd_bytes,
            self.handoff.clone(),
            self.trace.control(),
        )
    }

    pub(crate) fn new_worker(
        nodes: NodeMain,
        simd_bytes: usize,
        handoff: Option<DataPlaneHandoffWorker>,
        trace_control: Option<TraceControlHandle>,
        thread_index: u32,
        numa_node: u32,
    ) -> RuntimeResult<Self> {
        let mut runtime = Self::from_config(
            DataPlaneBufferConfig {
                thread_index,
                active_numa_node: numa_node,
                ..Default::default()
            },
            simd_bytes,
        )?;
        runtime.nodes = nodes;
        runtime.handoff = handoff;
        runtime.trace.set_control(trace_control);
        Ok(runtime)
    }

    pub fn for_worker(&self, thread_index: u32, numa_node: u32) -> RuntimeResult<Self> {
        let (nodes, simd_bytes, handoff, trace_control) = self.worker_parts();
        Self::new_worker(
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
