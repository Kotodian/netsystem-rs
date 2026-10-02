use core::hint::spin_loop;
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, OwnedFd};
use std::panic::Location;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hammer_core::data_plane::NodeId;
use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};

use crate::config::worker::WorkerScheduler;
use crate::error::{RuntimeError, RuntimeResult};

#[cfg(debug_assertions)]
const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_millis(600_100);
#[cfg(not(debug_assertions))]
const BARRIER_SYNC_TIMEOUT: Duration = Duration::from_secs(1);
const BARRIER_MINIMUM_OPEN_LIMIT: Duration = Duration::from_millis(1);
const BARRIER_MINIMUM_OPEN_FACTOR: u32 = 3;

struct BarrierScope {
    thread: &'static WorkerThread,
    caller: &'static Location<'static>,
    _not_send: PhantomData<std::rc::Rc<()>>,
}

impl Drop for BarrierScope {
    fn drop(&mut self) {
        self.thread.release(self.caller);
    }
}

fn data_worker_entry(_: u32) -> RuntimeResult<()> {
    Ok(())
}

#[repr(C)]
pub struct WorkerThread {
    cacheline0: CacheLineAlignMark,
    cacheline1: CacheLineAlignMark,
    thread_index: u32,
    name: &'static str,
    instance_index: u32,
    cpu_index: Option<u32>,
    numa_node: Option<u32>,
    stack_size: usize,
    scheduler: WorkerScheduler,
    numa_memory_binding: bool,
    entry: fn(u32) -> RuntimeResult<()>,
    no_data_structure_clone: bool,
    join_handle: UnsafeCell<Option<JoinHandle<RuntimeResult<()>>>>,
    join_handle_ready: AtomicBool,
    #[cfg(target_os = "linux")]
    file_wake: OnceLock<OwnedFd>,
    node_interrupts: UnsafeCell<Option<Box<[AtomicU64]>>>,
    cacheline2: CacheLineAlignMark,
    main_loop_count: AtomicU64,
    cacheline3: CacheLineAlignMark,
    loops_per_second: AtomicU64,
    cacheline4: CacheLineAlignMark,
    last_vectors_per_main_loop: AtomicUsize,
    cacheline5: CacheLineAlignMark,
    wait_at_barrier: AtomicU32,
    cacheline6: CacheLineAlignMark,
    workers_at_barrier: AtomicU32,
    cacheline7: CacheLineAlignMark,
    node_reforks_required: AtomicU32,
    barrier_recursion: Cell<u32>,
    barrier_sync_count: Cell<u64>,
    barrier_initialized: AtomicBool,
    startup_cancelled: AtomicBool,
    node_runtime: UnsafeCell<Option<crate::node::NodeRuntimeInner>>,
    barrier_epoch: Cell<Instant>,
    barrier_no_close_before: Cell<Instant>,
    main_thread: std::thread::ThreadId,
}

const _: () = {
    assert!(core::mem::align_of::<WorkerThread>() == CACHE_LINE);
    assert!(core::mem::offset_of!(WorkerThread, cacheline0) == 0);
    assert!(core::mem::offset_of!(WorkerThread, cacheline1) % CACHE_LINE == 0);
    assert!(
        core::mem::offset_of!(WorkerThread, thread_index)
            == core::mem::offset_of!(WorkerThread, cacheline1)
    );
    assert!(core::mem::offset_of!(WorkerThread, main_loop_count) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, loops_per_second) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, last_vectors_per_main_loop) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, wait_at_barrier) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, workers_at_barrier) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, node_reforks_required) % CACHE_LINE == 0);
};

// SAFETY: node_interrupts are installed before worker launch. join_handle has
// one main-thread writer and its release-ready flag gates concurrent reads.
// The thread-zero node_runtime slot is written only while every participant
// has acknowledged the barrier and is read after release/acquire publication.
// Its recursion and timing cells are accessed only by the installed main thread.
unsafe impl Sync for WorkerThread {}

impl WorkerThread {
    /// Thread zero owns the process barrier fields. Before worker startup
    /// there is no active barrier, as in VPP's zero-worker path.
    #[inline]
    pub fn main() -> Option<&'static Self> {
        crate::thread_main::THREAD_MAIN.get().and_then(|threads| {
            let main = threads.thread_by_index(0)?;
            main.barrier_initialized
                .load(Ordering::Acquire)
                .then_some(main)
        })
    }

    #[doc(hidden)]
    #[track_caller]
    pub fn __sync_guard(main: &mut crate::DataPlaneMain) -> impl Drop + use<> {
        assert_eq!(
            main.thread_index(),
            0,
            "worker barrier sync requires main thread"
        );
        Self::__main_sync_guard()
    }

    #[doc(hidden)]
    #[track_caller]
    pub fn __main_sync_guard() -> impl Drop + use<> {
        crate::ensure_main_thread().expect("worker barrier sync requires main thread");
        let thread = Self::main().expect("worker barrier is not installed");
        let caller = Location::caller();
        thread.sync(caller);
        BarrierScope {
            thread,
            caller,
            _not_send: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn __assert_held() {
        let Some(thread) = Self::main() else {
            crate::ensure_main_thread().expect("worker barrier scope requires main thread");
            return;
        };
        thread.assert_held();
    }

    pub(crate) fn initialize_barrier(&self) {
        assert_eq!(self.thread_index, 0, "thread zero owns worker barrier");
        assert!(
            !self.barrier_initialized.load(Ordering::Relaxed),
            "worker barrier initializes once"
        );
        self.barrier_initialized.store(true, Ordering::Release);
    }

    #[inline]
    pub fn worker_count(&self) -> u32 {
        crate::ThreadMain::global().worker_count()
    }

    #[inline]
    pub fn is_pending(&self) -> bool {
        self.wait_at_barrier.load(Ordering::Acquire) != 0
    }

    fn assert_held(&self) {
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "worker barrier scope requires main thread"
        );
        if crate::ThreadMain::global().thread_count() < 2 {
            return;
        }
        assert_ne!(
            self.barrier_recursion.get(),
            0,
            "worker barrier scope is required"
        );
        assert!(self.is_pending(), "worker barrier scope is required");
        assert_eq!(
            self.workers_at_barrier.load(Ordering::Acquire),
            crate::ThreadMain::global().thread_count() - 1,
            "worker barrier requires every participant to acknowledge"
        );
    }

    /// Arm the startup handshake before any worker is launched.
    #[track_caller]
    pub(crate) fn arm(&self) {
        assert_eq!(
            self.barrier_recursion.get(),
            0,
            "startup barrier is outside a scope"
        );
        let previous = self.wait_at_barrier.swap(1, Ordering::Release);
        if previous != 0 {
            barrier_deadlock("arm startup barrier", 0, previous, Location::caller());
        }
    }

    #[inline]
    pub(crate) fn cancel_startup(&self) {
        self.startup_cancelled.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn startup_cancelled(&self) -> bool {
        self.startup_cancelled.load(Ordering::Acquire)
    }

    #[track_caller]
    pub(crate) fn release_startup(&self) {
        assert_eq!(
            self.barrier_recursion.get(),
            0,
            "startup has no active scope"
        );
        let previous = self.wait_at_barrier.swap(0, Ordering::Release);
        assert_eq!(previous, 1, "startup barrier releases once");
        wait_for_count(
            &self.workers_at_barrier,
            0,
            Instant::now() + BARRIER_SYNC_TIMEOUT,
            "startup barrier release",
            Location::caller(),
        );
    }

    /// VPP `vlib_worker_thread_initial_barrier_sync_and_release`, called
    /// after main-loop-enter and configuration, before Process scheduling.
    #[track_caller]
    pub(crate) fn initial_barrier_sync_and_release(&self) {
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "initial barrier requires main thread"
        );
        let count = crate::ThreadMain::global().thread_count() - 1;
        if count == 0 {
            return;
        }
        let deadline = Instant::now() + BARRIER_SYNC_TIMEOUT;
        self.wait_at_barrier.store(1, Ordering::Release);
        for thread_index in 1..crate::ThreadMain::global().thread_count() {
            crate::ThreadMain::global()
                .thread_by_index(thread_index)
                .expect("barrier participant has a thread descriptor")
                .wake_for_barrier();
        }
        wait_for_count(
            &self.workers_at_barrier,
            count,
            deadline,
            "initial barrier",
            Location::caller(),
        );
        self.wait_at_barrier.store(0, Ordering::Release);
    }

    /// VPP `vlib_worker_thread_barrier_sync_int`: the outer entry applies the
    /// vector-rate hold-down, publishes one request, and waits for every
    /// runtime thread to acknowledge it.
    #[track_caller]
    pub(crate) fn sync(&self, caller: &'static Location<'static>) {
        if crate::ThreadMain::global().thread_count() < 2 {
            return;
        }
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "worker barrier sync requires main thread"
        );
        let recursion = self.barrier_recursion.get() + 1;
        self.barrier_recursion.set(recursion);
        if recursion > 1 {
            return;
        }

        let mut now = Instant::now();
        let max_vector_rate = crate::ThreadMain::global()
            .data_workers()
            .map(|worker| worker.last_vectors_per_main_loop.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0);
        self.barrier_sync_count
            .set(self.barrier_sync_count.get() + 1);
        let no_close_before = self.barrier_no_close_before.get();
        debug_assert!(no_close_before <= now + BARRIER_MINIMUM_OPEN_LIMIT);
        if max_vector_rate > 10 {
            while now < no_close_before {
                if no_close_before.duration_since(now)
                    > BARRIER_MINIMUM_OPEN_LIMIT.saturating_mul(2)
                {
                    break;
                }
                spin_loop();
                now = Instant::now();
            }
        }
        self.barrier_epoch.set(now);
        let deadline = now + BARRIER_SYNC_TIMEOUT;

        self.wait_at_barrier.store(1, Ordering::Release);
        for thread_index in 1..crate::ThreadMain::global().thread_count() {
            crate::ThreadMain::global()
                .thread_by_index(thread_index)
                .expect("barrier participant has a thread descriptor")
                .wake_for_barrier();
        }
        wait_for_count(
            &self.workers_at_barrier,
            crate::ThreadMain::global().thread_count() - 1,
            deadline,
            "barrier sync",
            caller,
        );
    }

    /// VPP `vlib_worker_thread_barrier_release`: publish main-thread writes,
    /// wait until all participants resume, then await any requested refork.
    fn release(&self, caller: &'static Location<'static>) {
        if crate::ThreadMain::global().thread_count() < 2 {
            return;
        }
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "worker barrier release requires main thread"
        );
        let recursion = self.barrier_recursion.get();
        if recursion == 0 {
            barrier_deadlock("barrier release without matching sync", 1, 0, caller);
        }
        self.barrier_recursion.set(recursion - 1);
        if recursion > 1 {
            return;
        }
        // SAFETY: thread zero writes this slot while all participants are parked.
        let refork_requested = unsafe { (&*self.node_runtime.get()).is_some() };
        let stats_segment = match hammer_stats::StatsMain::global() {
            Ok(stats) if refork_requested => Some(&stats.segment),
            Ok(_) | Err(hammer_stats::StatsError::NotInitialized) => None,
            Err(error) => panic!("stats segment lookup failed during graph refork: {error}"),
        };
        let stats_guard = stats_segment.map(|segment| segment.lock_refork());
        if refork_requested {
            // SAFETY: the main thread owns the published graph while all Data
            // Workers are parked, and the stats segment stays locked until
            // their reforks complete.
            let graph = unsafe {
                (&*self.node_runtime.get())
                    .as_ref()
                    .expect("requested refork retains its graph")
            };
            crate::node_stats::NodeCounterRows::global().refresh_graph(graph.node_names());
            let previous = self
                .node_reforks_required
                .fetch_add(self.worker_count(), Ordering::Release);
            assert_eq!(previous, 0, "graph refork counter must start at zero");
        }
        assert_eq!(
            self.wait_at_barrier.load(Ordering::Relaxed),
            1,
            "outer worker barrier release is unique"
        );
        self.wait_at_barrier.store(0, Ordering::Release);
        wait_for_count(
            &self.workers_at_barrier,
            0,
            Instant::now() + BARRIER_SYNC_TIMEOUT,
            "barrier release",
            caller,
        );
        if refork_requested {
            wait_for_count(
                &self.node_reforks_required,
                0,
                Instant::now() + BARRIER_SYNC_TIMEOUT,
                "graph refork",
                caller,
            );
            // SAFETY: every worker completed its graph clone before the
            // acquire observation of the zero completion count.
            unsafe { drop(self.node_runtime.get().replace(None)) };
        }
        drop(stats_guard);
        let now = Instant::now();
        let minimum_open = now
            .saturating_duration_since(self.barrier_epoch.get())
            .saturating_mul(BARRIER_MINIMUM_OPEN_FACTOR)
            .min(BARRIER_MINIMUM_OPEN_LIMIT);
        self.barrier_no_close_before.set(now + minimum_open);
        self.barrier_epoch.set(now);
    }

    /// Workers acknowledge a request only after ending their mutable main
    /// borrow; the acquire observation after release publishes graph changes.
    pub(crate) fn check(&self) -> bool {
        if !self.is_pending() {
            return false;
        }
        self.workers_at_barrier.fetch_add(1, Ordering::Release);
        while self.wait_at_barrier.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        self.workers_at_barrier.fetch_sub(1, Ordering::Release);
        self.node_reforks_required.load(Ordering::Acquire) != 0
    }

    pub(crate) fn request_node_refork(&self, graph: crate::node::NodeRuntimeInner) {
        self.assert_held();
        assert_eq!(
            self.node_reforks_required.load(Ordering::Acquire),
            0,
            "previous graph refork must complete before publication"
        );
        // SAFETY: all participants are parked; only thread zero writes.
        unsafe { *self.node_runtime.get() = Some(graph) };
    }

    pub(crate) fn refork(&self, nodes: &mut crate::NodeMain) {
        assert_ne!(
            self.node_reforks_required.load(Ordering::Acquire),
            0,
            "worker refork requires a published graph"
        );
        // SAFETY: acquire observes the graph published before release, and
        // thread zero retains it until every worker completes its clone.
        let graph = unsafe {
            (&*self.node_runtime.get())
                .as_ref()
                .expect("refork count and graph stay paired")
                .clone()
        };
        nodes.refork(graph);
        let previous = self.node_reforks_required.fetch_sub(1, Ordering::Release);
        assert_ne!(previous, 0, "graph refork completion underflow");
        while self.node_reforks_required.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
    }

    /// VPP `vlib_worker_wait_one_loop`: wait for each Data Worker to advance
    /// once, unless a barrier request stops its progress.
    pub fn wait_one_loop(&self) {
        assert_eq!(self.thread_index, 0, "only thread zero waits for workers");
        assert_eq!(
            std::thread::current().id(),
            self.main_thread,
            "wait_one_loop requires main thread"
        );
        if self.worker_count() == 0 || self.is_pending() {
            return;
        }
        let workers: Vec<_> = crate::ThreadMain::global()
            .data_workers()
            .map(|worker| (worker, worker.main_loop_count.load(Ordering::Relaxed)))
            .collect();
        for (worker, count) in workers {
            while worker.main_loop_count.load(Ordering::Relaxed) == count {
                if self.is_pending() {
                    return;
                }
                spin_loop();
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        thread_index: u32,
        name: &'static str,
        instance_index: u32,
        cpu_index: Option<u32>,
        numa_node: Option<u32>,
        stack_size: usize,
        scheduler: WorkerScheduler,
        numa_memory_binding: bool,
        entry: Option<fn(u32) -> RuntimeResult<()>>,
        no_data_structure_clone: bool,
    ) -> Self {
        assert_eq!(
            entry.is_some(),
            no_data_structure_clone,
            "only a no-data-structure-clone registration supplies its own entry"
        );
        Self {
            cacheline0: CacheLineAlignMark,
            cacheline1: CacheLineAlignMark,
            thread_index,
            name,
            instance_index,
            cpu_index,
            numa_node,
            stack_size,
            scheduler,
            numa_memory_binding,
            entry: entry.unwrap_or(data_worker_entry),
            no_data_structure_clone,
            join_handle: UnsafeCell::new(None),
            join_handle_ready: AtomicBool::new(false),
            #[cfg(target_os = "linux")]
            file_wake: OnceLock::new(),
            node_interrupts: UnsafeCell::new(None),
            cacheline2: CacheLineAlignMark,
            main_loop_count: AtomicU64::new(0),
            cacheline3: CacheLineAlignMark,
            loops_per_second: AtomicU64::new(0),
            cacheline4: CacheLineAlignMark,
            last_vectors_per_main_loop: AtomicUsize::new(0),
            cacheline5: CacheLineAlignMark,
            wait_at_barrier: AtomicU32::new(0),
            cacheline6: CacheLineAlignMark,
            workers_at_barrier: AtomicU32::new(0),
            cacheline7: CacheLineAlignMark,
            node_reforks_required: AtomicU32::new(0),
            barrier_recursion: Cell::new(0),
            barrier_sync_count: Cell::new(0),
            barrier_initialized: AtomicBool::new(false),
            startup_cancelled: AtomicBool::new(false),
            node_runtime: UnsafeCell::new(None),
            barrier_epoch: Cell::new(Instant::now()),
            barrier_no_close_before: Cell::new(Instant::now()),
            main_thread: std::thread::current().id(),
        }
    }

    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.thread_index
    }

    /// The main-loop count this thread publishes.
    ///
    /// Only the owning thread writes it and only the round collector reads it,
    /// so the load the collector performs needs no synchronization beyond the
    /// relaxed ordering statistics use.
    #[inline]
    pub(crate) fn main_loop_count(&self) -> &AtomicU64 {
        &self.main_loop_count
    }

    /// The latest loops-per-second value this thread published.
    ///
    /// The window itself stays in the owning `DataPlaneMain`; this record only
    /// carries the value the collector copies into `/sys/loops_per_worker`.
    #[inline]
    pub(crate) fn loops_per_second(&self) -> &AtomicU64 {
        &self.loops_per_second
    }

    /// Last completed loop's maximum internal frame size. The main thread
    /// reads this scalar only to decide whether to honor barrier hold-down.
    #[inline]
    pub(crate) fn last_vectors_per_main_loop(&self) -> &AtomicUsize {
        &self.last_vectors_per_main_loop
    }

    #[inline]
    pub fn cpu_index(&self) -> Option<u32> {
        self.cpu_index
    }

    #[inline]
    pub fn numa_node(&self) -> Option<u32> {
        self.numa_node
    }

    #[inline]
    pub fn no_data_structure_clone(&self) -> bool {
        self.no_data_structure_clone
    }

    pub(crate) fn install_node_interrupts(&self, node_count: usize) {
        // SAFETY: start_workers calls this before launching any worker. No
        // producer has a published Session queue NodeId at this point.
        let interrupts = unsafe { &mut *self.node_interrupts.get() };
        assert!(
            interrupts.is_none(),
            "Data Worker node interrupts install once"
        );
        *interrupts = Some(
            (0..node_count.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
        );
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn install_file_wake(&self, wake: OwnedFd) {
        assert!(self.file_wake.set(wake).is_ok(), "worker File wake installs once");
    }

    #[inline]
    pub(crate) fn interrupt_node(&self, node: NodeId) {
        // SAFETY: the allocation was installed before worker launch and is
        // immutable for the rest of the process; only its atomics change.
        let pending = unsafe { &*self.node_interrupts.get() }
            .as_ref()
            .expect("Data Worker node interrupts install before event publication");
        let bit = pending
            .get(node.slot() as usize / 64)
            .expect("published node is within the worker graph");
        let mask = 1_u64 << (node.slot() % 64);
        if bit.fetch_or(mask, Ordering::Release) & mask == 0 {
            self.wake_for_barrier();
        }
    }

    #[inline]
    pub(crate) fn wake_for_barrier(&self) {
        #[cfg(target_os = "linux")]
        if let Some(wake) = self.file_wake.get() {
            let increment = 1u64;
            // SAFETY: eventfd accepts one u64; Poller owns the descriptor and
            // drains the wake hint before consuming CQEs.
            let written = unsafe {
                libc::write(
                    wake.as_raw_fd(),
                    (&increment as *const u64).cast(),
                    core::mem::size_of::<u64>(),
                )
            };
            if written < 0 {
                let source = std::io::Error::last_os_error();
                assert_eq!(
                    source.raw_os_error(),
                    Some(libc::EAGAIN),
                    "worker File wake failed: {source}"
                );
            }
        }
        if self.join_handle_ready.load(Ordering::Acquire) {
            // SAFETY: the acquire load observes the main thread's handle install.
            let thread = unsafe { &*self.join_handle.get() }
                .as_ref()
                .expect("ready WorkerThread has a launch handle");
            thread.thread().unpark();
        }
    }

    #[inline]
    pub(crate) fn take_node_interrupts(&self, word: usize) -> Option<u64> {
        // SAFETY: the allocation remains immutable after prelaunch install.
        unsafe { &*self.node_interrupts.get() }
            .as_ref()
            .expect("Data Worker node interrupts install before dispatch")
            .get(word)
            .map(|pending| pending.swap(0, Ordering::Acquire))
    }

    pub(crate) fn has_node_interrupts(&self) -> bool {
        // SAFETY: the installed slice stays at a fixed address after launch.
        unsafe { &*self.node_interrupts.get() }
            .as_ref()
            .expect("Data Worker node interrupts install before dispatch")
            .iter()
            .any(|pending| pending.load(Ordering::Acquire) != 0)
    }

    #[inline]
    pub(crate) fn is_current(&self) -> bool {
        if !self.join_handle_ready.load(Ordering::Acquire) {
            return false;
        }
        // SAFETY: the acquire load observes the immutable launch handle.
        unsafe { &*self.join_handle.get() }
            .as_ref()
            .expect("ready WorkerThread has a launch handle")
            .thread()
            .id()
            == std::thread::current().id()
    }

    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn launch(
        &self,
        init_functions: Arc<[&'static crate::init::InitFunction]>,
    ) -> RuntimeResult<()> {
        assert_ne!(self.thread_index, 0);
        assert!(
            unsafe { &*self.join_handle.get() }.is_none(),
            "WorkerThread launches once"
        );
        let barrier = Self::main().expect("worker barrier installs before thread launch");
        let thread_index = self.thread_index;
        let name = self.name;
        let instance_index = self.instance_index;
        let cpu_index = self.cpu_index;
        let numa_node = self.numa_node;
        let scheduler = self.scheduler.clone();
        let numa_memory_binding = self.numa_memory_binding;
        let stack_size = self.stack_size;
        let entry = self.entry;
        let thread = std::thread::Builder::new()
            .name(format!("hammer-{name}-{instance_index}"))
            .stack_size(stack_size)
            .spawn(move || -> RuntimeResult<()> {
                // Runtime worker threads live for the process lifetime and use
                // the process Main Heap for ordinary allocations.
                unsafe { hammer_infra::MemThreadMain::register_current(thread_index) };
                if let Err(source) = apply_current_thread_setup(
                    thread_index,
                    cpu_index,
                    numa_node,
                    &scheduler,
                    numa_memory_binding,
                ) {
                    let error = RuntimeError::ThreadSetup {
                        thread_index,
                        source: Box::new(source),
                    };
                    eprintln!("{error:?}");
                    std::process::abort();
                }
                let refork_required = barrier.check();
                if barrier.startup_cancelled() {
                    assert!(
                        !refork_required,
                        "startup cancellation cannot publish a graph refork"
                    );
                    return Ok(());
                }

                let descriptor = crate::ThreadMain::global()
                    .thread_by_index(thread_index)
                    .expect("launched Worker has a descriptor");
                if descriptor.no_data_structure_clone {
                    assert!(
                        !refork_required,
                        "a no-data-structure-clone thread cannot refork a graph"
                    );
                    return entry(thread_index);
                }
                {
                    // SAFETY: startup barrier has released; only this Worker
                    // accesses its main until the next barrier check.
                    let main =
                        unsafe { crate::ThreadMain::global().worker_main_on_worker(descriptor) };
                    if refork_required {
                        barrier.refork(&mut main.nodes);
                    }
                    main.select_architecture_functions(
                        cfg!(target_os = "linux") && cpu_index.is_some(),
                    )?;
                    crate::init::run_worker_init_functions(main, &init_functions)?;
                }
                let exit_status = crate::main_loop::data_plane_main_loop(descriptor);
                if exit_status == 0 {
                    Ok(())
                } else {
                    Err(RuntimeError::DataWorkerExited {
                        worker: thread_index,
                        status: exit_status,
                    })
                }
            })
            .map_err(|source| RuntimeError::ThreadSpawn {
                thread_index,
                name,
                source,
            })?;
        // SAFETY: only the main thread launches workers, and the startup
        // barrier prevents worker execution until this handle is installed.
        let handle = unsafe { &mut *self.join_handle.get() };
        assert!(
            handle.replace(thread).is_none(),
            "WorkerThread launches once"
        );
        self.join_handle_ready.store(true, Ordering::Release);
        Ok(())
    }
}

fn wait_for_count(
    counter: &AtomicU32,
    expected: u32,
    deadline: Instant,
    phase: &'static str,
    caller: &'static Location<'static>,
) {
    loop {
        let observed = counter.load(Ordering::Acquire);
        if observed == expected {
            return;
        }
        if Instant::now() > deadline {
            barrier_deadlock(phase, expected, observed, caller);
        }
        spin_loop();
    }
}

#[cold]
#[inline(never)]
fn barrier_deadlock(
    phase: &'static str,
    expected: u32,
    observed: u32,
    caller: &'static Location<'static>,
) -> ! {
    eprintln!(
        "{phase}: worker thread deadlock; caller={caller}; workers_at_barrier={observed}; expected={expected}"
    );
    std::process::abort();
}

pub(crate) fn apply_current_thread_setup(
    thread_index: u32,
    cpu_index: Option<u32>,
    numa_node: Option<u32>,
    scheduler: &WorkerScheduler,
    numa_memory_binding: bool,
) -> RuntimeResult<u32> {
    #[cfg(target_os = "linux")]
    {
        if let Some(cpu) = cpu_index {
            let cpu = cpu as usize;
            if !core_affinity::set_for_current(core_affinity::CoreId { id: cpu }) {
                return Err(RuntimeError::WorkerCpuAffinity { thread_index, cpu });
            }
        }
        apply_scheduler(scheduler)?;
        let numa_node = numa_node
            .or_else(crate::numa::current_numa_node)
            .unwrap_or(0);
        if numa_memory_binding {
            crate::numa::bind_current_thread_memory_to_numa(numa_node)?;
        }
        return Ok(numa_node);
    }

    #[cfg(target_os = "macos")]
    {
        let _ = (thread_index, cpu_index, numa_node, numa_memory_binding);
        apply_qos(scheduler)?;
        Ok(0)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (
            thread_index,
            cpu_index,
            numa_node,
            scheduler,
            numa_memory_binding,
        );
        Ok(0)
    }
}

#[cfg(target_os = "linux")]
fn apply_scheduler(scheduler: &WorkerScheduler) -> RuntimeResult<()> {
    use crate::config::worker::SchedulerPolicy;
    use thread_priority::{
        NormalThreadSchedulePolicy, RealtimeThreadSchedulePolicy, ThreadPriority,
        ThreadPriorityOsValue, ThreadPriorityValue, ThreadSchedulePolicy,
        set_thread_priority_and_policy, thread_native_id,
    };

    let policy = match scheduler.policy {
        SchedulerPolicy::Other => ThreadSchedulePolicy::Normal(NormalThreadSchedulePolicy::Other),
        SchedulerPolicy::Batch => ThreadSchedulePolicy::Normal(NormalThreadSchedulePolicy::Batch),
        SchedulerPolicy::Idle => ThreadSchedulePolicy::Normal(NormalThreadSchedulePolicy::Idle),
        SchedulerPolicy::Fifo => ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::Fifo),
        SchedulerPolicy::Rr => {
            ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::RoundRobin)
        }
    };
    let priority = match policy {
        ThreadSchedulePolicy::Normal(_) => ThreadPriority::Os(ThreadPriorityOsValue::default()),
        ThreadSchedulePolicy::Realtime(_) => u8::try_from(scheduler.priority)
            .ok()
            .and_then(|value| ThreadPriorityValue::try_from(value).ok())
            .map(ThreadPriority::Crossplatform)
            .unwrap_or(ThreadPriority::Min),
    };
    set_thread_priority_and_policy(thread_native_id(), priority, policy).map_err(|source| {
        RuntimeError::WorkerScheduler {
            source: Box::new(source),
        }
    })
}

#[cfg(target_os = "macos")]
fn apply_qos(scheduler: &WorkerScheduler) -> RuntimeResult<()> {
    use crate::config::worker::QosClass;

    let qos = match scheduler.qos {
        QosClass::UserInteractive => libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
        QosClass::UserInitiated => libc::qos_class_t::QOS_CLASS_USER_INITIATED,
        QosClass::Default => libc::qos_class_t::QOS_CLASS_DEFAULT,
        QosClass::Utility => libc::qos_class_t::QOS_CLASS_UTILITY,
        QosClass::Background => libc::qos_class_t::QOS_CLASS_BACKGROUND,
    };
    let result = unsafe { libc::pthread_set_qos_class_self_np(qos, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(RuntimeError::WorkerQos {
            source: std::io::Error::from_raw_os_error(result),
        })
    }
}
