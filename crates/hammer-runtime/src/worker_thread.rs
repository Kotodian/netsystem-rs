use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use hammer_core::data_plane::NodeId;
use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};

use crate::config::WorkerScheduler;
use crate::error::{RuntimeError, RuntimeResult};

fn data_worker_entry(_: u32) -> RuntimeResult<()> {
    Ok(())
}

#[repr(C)]
pub struct WorkerThread {
    cacheline0: CacheLineAlignMark,
    barrier: OnceLock<crate::WorkerBarrier>,
    cacheline1: CacheLineAlignMark,
    thread_index: u32,
    name: &'static str,
    instance_index: u32,
    cpu_index: Option<u32>,
    numa_node: Option<u32>,
    stack_size: usize,
    idle_slice: Duration,
    scheduler: WorkerScheduler,
    numa_memory_binding: bool,
    entry: fn(u32) -> RuntimeResult<()>,
    no_data_structure_clone: bool,
    join_handle: OnceLock<JoinHandle<RuntimeResult<()>>>,
    node_interrupts: UnsafeCell<Option<Box<[AtomicU64]>>>,
    cacheline2: CacheLineAlignMark,
    main_loop_count: AtomicU64,
    cacheline3: CacheLineAlignMark,
    loops_per_second: AtomicU64,
}

const _: () = {
    assert!(core::mem::align_of::<WorkerThread>() == CACHE_LINE);
    assert!(core::mem::offset_of!(WorkerThread, cacheline0) == 0);
    assert!(core::mem::offset_of!(WorkerThread, barrier) == 0);
    assert!(core::mem::offset_of!(WorkerThread, cacheline1) % CACHE_LINE == 0);
    assert!(
        core::mem::offset_of!(WorkerThread, thread_index)
            == core::mem::offset_of!(WorkerThread, cacheline1)
    );
    assert!(core::mem::offset_of!(WorkerThread, main_loop_count) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(WorkerThread, loops_per_second) % CACHE_LINE == 0);
};

// SAFETY: node_interrupts is installed once before any worker launch or
// producer publication. Its allocation is never replaced afterward; only the
// contained atomics are accessed concurrently.
unsafe impl Sync for WorkerThread {}

impl WorkerThread {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        thread_index: u32,
        name: &'static str,
        instance_index: u32,
        cpu_index: Option<u32>,
        numa_node: Option<u32>,
        stack_size: usize,
        idle_slice: Duration,
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
            barrier: OnceLock::new(),
            cacheline1: CacheLineAlignMark,
            thread_index,
            name,
            instance_index,
            cpu_index,
            numa_node,
            stack_size,
            idle_slice,
            scheduler,
            numa_memory_binding,
            entry: entry.unwrap_or(data_worker_entry),
            no_data_structure_clone,
            join_handle: OnceLock::new(),
            node_interrupts: UnsafeCell::new(None),
            cacheline2: CacheLineAlignMark,
            main_loop_count: AtomicU64::new(0),
            cacheline3: CacheLineAlignMark,
            loops_per_second: AtomicU64::new(0),
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

    pub(crate) fn install_barrier(&self, barrier: crate::WorkerBarrier) {
        assert!(self.barrier.set(barrier).is_ok(), "barrier installs once");
    }

    pub(crate) fn install_node_interrupts(&self, node_count: usize) {
        // SAFETY: start_workers calls this before launching any worker. No
        // producer has a published Session queue NodeId at this point.
        let interrupts = unsafe { &mut *self.node_interrupts.get() };
        assert!(interrupts.is_none(), "Data Worker node interrupts install once");
        *interrupts = Some(
            (0..node_count.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
        );
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
        bit.fetch_or(1_u64 << (node.slot() % 64), Ordering::Release);
        if let Some(thread) = self.join_handle.get() {
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

    #[inline]
    pub(crate) fn is_current(&self) -> bool {
        self.join_handle
            .get()
            .is_some_and(|thread| thread.thread().id() == std::thread::current().id())
    }

    pub(crate) fn launch(
        &self,
        main: Option<Box<crate::DataPlaneMain>>,
        init_functions: Arc<[&'static crate::init::InitFunction]>,
    ) -> RuntimeResult<()> {
        assert_ne!(self.thread_index, 0);
        assert_eq!(
            main.is_none(),
            self.no_data_structure_clone,
            "no-data-structure-clone registration controls DataPlaneMain ownership"
        );
        assert!(
            self.join_handle.get().is_none(),
            "WorkerThread launches once"
        );
        let barrier = self
            .barrier
            .get()
            .expect("worker barrier installs before thread launch")
            .clone();
        let thread_index = self.thread_index;
        let name = self.name;
        let instance_index = self.instance_index;
        let cpu_index = self.cpu_index;
        let numa_node = self.numa_node;
        let scheduler = self.scheduler.clone();
        let numa_memory_binding = self.numa_memory_binding;
        let stack_size = self.stack_size;
        let idle_slice = self.idle_slice;
        let entry = self.entry;
        let thread = std::thread::Builder::new()
            .name(format!("hammer-{name}-{instance_index}"))
            .stack_size(stack_size)
            .spawn(move || -> RuntimeResult<()> {
                // Runtime worker threads live for the process lifetime and use
                // the process Main Heap for ordinary allocations.
                unsafe { hammer_infra::MemThreadMain::register_current(thread_index) };
                apply_current_thread_setup(
                    thread_index,
                    cpu_index,
                    numa_node,
                    &scheduler,
                    numa_memory_binding,
                )
                .map_err(|source| RuntimeError::ThreadSetup {
                    thread_index,
                    source: Box::new(source),
                })?;
                let refork_required = barrier.check_for_refork();
                if barrier.startup_cancelled() {
                    assert!(
                        !refork_required,
                        "startup cancellation cannot publish a graph refork"
                    );
                    return Ok(());
                }

                let Some(mut main) = main else {
                    assert!(
                        !refork_required,
                        "a no-data-structure-clone thread cannot refork a graph"
                    );
                    return entry(thread_index);
                };
                if refork_required {
                    barrier.refork(&mut main.nodes);
                }
                crate::init::run_worker_init_functions(&mut main, &init_functions)?;
                let exit_status = crate::main_loop::data_plane_main_loop(&mut main, idle_slice);
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
        assert!(
            self.join_handle.set(thread).is_ok(),
            "WorkerThread launch handle installs once"
        );
        Ok(())
    }
}

fn apply_current_thread_setup(
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
    use crate::config::SchedulerPolicy;
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
    use crate::config::QosClass;

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
