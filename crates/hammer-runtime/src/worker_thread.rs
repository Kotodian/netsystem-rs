use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};

use crate::config::WorkerScheduler;
use crate::error::{RuntimeError, RuntimeResult};

fn data_worker_entry(_: u32) -> RuntimeResult<()> {
    Ok(())
}

#[repr(C)]
pub struct WorkerThread {
    cacheline0: CacheLineAlignMark,
    barrier: Option<crate::WorkerBarrier>,
    startup_acknowledged: bool,
    cacheline1: CacheLineAlignMark,
    thread_index: u32,
    cpu_index: Option<u32>,
    numa_node: Option<u32>,
    stack_size: usize,
    max_blocking_threads: usize,
    idle_slice: Duration,
    scheduler: WorkerScheduler,
    numa_memory_binding: bool,
    entry: fn(u32) -> RuntimeResult<()>,
    no_data_structure_clone: bool,
    refork_pending: bool,
    join_handle: Option<JoinHandle<RuntimeResult<()>>>,
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
};

impl WorkerThread {
    pub(crate) fn new_main(
        cpu_index: Option<u32>,
        numa_node: Option<u32>,
        scheduler: WorkerScheduler,
    ) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            barrier: None,
            startup_acknowledged: true,
            cacheline1: CacheLineAlignMark,
            thread_index: 0,
            cpu_index,
            numa_node,
            stack_size: 0,
            max_blocking_threads: 0,
            idle_slice: Duration::ZERO,
            scheduler,
            numa_memory_binding: false,
            entry: data_worker_entry,
            no_data_structure_clone: false,
            refork_pending: false,
            join_handle: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_data_worker(
        thread_index: u32,
        cpu_index: Option<u32>,
        numa_node: Option<u32>,
        stack_size: usize,
        max_blocking_threads: usize,
        idle_slice: Duration,
        scheduler: WorkerScheduler,
        numa_memory_binding: bool,
    ) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            barrier: None,
            startup_acknowledged: false,
            cacheline1: CacheLineAlignMark,
            thread_index,
            cpu_index,
            numa_node,
            stack_size,
            max_blocking_threads,
            idle_slice,
            scheduler,
            numa_memory_binding,
            entry: data_worker_entry,
            no_data_structure_clone: false,
            refork_pending: false,
            join_handle: None,
        }
    }

    pub(crate) fn new_auxiliary(
        thread_index: u32,
        stack_size: usize,
        entry: fn(u32) -> RuntimeResult<()>,
    ) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            barrier: None,
            startup_acknowledged: false,
            cacheline1: CacheLineAlignMark,
            thread_index,
            cpu_index: None,
            numa_node: None,
            stack_size,
            max_blocking_threads: 0,
            idle_slice: Duration::ZERO,
            scheduler: WorkerScheduler::default(),
            numa_memory_binding: false,
            entry,
            no_data_structure_clone: true,
            refork_pending: false,
            join_handle: None,
        }
    }

    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.thread_index
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

    pub(crate) fn apply_current_thread_setup(&self) -> RuntimeResult<u32> {
        #[cfg(target_os = "linux")]
        {
            if let Some(cpu) = self.cpu_index {
                let cpu = cpu as usize;
                if !core_affinity::set_for_current(core_affinity::CoreId { id: cpu }) {
                    return Err(RuntimeError::WorkerCpuAffinity {
                        thread_index: self.thread_index,
                        cpu,
                    });
                }
            }
            apply_scheduler(&self.scheduler)?;
            let numa_node = self
                .numa_node
                .or_else(crate::numa::current_numa_node)
                .unwrap_or(0);
            if self.numa_memory_binding {
                crate::numa::bind_current_thread_memory_to_numa(numa_node)?;
            }
            return Ok(numa_node);
        }

        #[cfg(target_os = "macos")]
        {
            apply_qos(&self.scheduler)?;
            Ok(0)
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Ok(0)
        }
    }

    pub(crate) fn launch_auxiliary(&mut self) -> RuntimeResult<()> {
        assert!(self.no_data_structure_clone);
        let thread_index = self.thread_index;
        let entry = self.entry;
        let thread = std::thread::Builder::new()
            .name(format!("hammer-auxiliary-{thread_index}"))
            .stack_size(self.stack_size)
            .spawn(move || entry(thread_index))
            .map_err(|source| RuntimeError::DataWorkerThreadSpawn {
                worker: thread_index as usize,
                source,
            })?;
        self.join_handle = Some(thread);
        Ok(())
    }

    pub(crate) fn launch_data_worker(
        &mut self,
        mut main: crate::DataPlaneMain,
        remote_local: crate::spawn::DataRemoteLocalQueue,
        init_functions: Vec<&'static crate::init::InitFunction>,
        barrier: crate::WorkerBarrier,
        startup_cancelled: Arc<AtomicBool>,
    ) -> RuntimeResult<()> {
        assert!(!self.no_data_structure_clone);
        assert_ne!(self.thread_index, 0);
        let thread_index = self.thread_index;
        let cpu_index = self.cpu_index;
        let numa_node = self.numa_node;
        let scheduler = self.scheduler.clone();
        let numa_memory_binding = self.numa_memory_binding;
        let stack_size = self.stack_size;
        let max_blocking_threads = self.max_blocking_threads;
        let idle_slice = self.idle_slice;
        let thread = std::thread::Builder::new()
            .name(format!("hammer-worker-{thread_index}"))
            .stack_size(stack_size)
            .spawn(move || -> RuntimeResult<()> {
                barrier.check();
                if startup_cancelled.load(Ordering::Acquire) {
                    return Ok(());
                }
                apply_current_thread_setup(
                    thread_index,
                    cpu_index,
                    numa_node,
                    &scheduler,
                    numa_memory_binding,
                )?;
                remote_local.attach_current_thread();
                if let Err(error) =
                    crate::init::run_worker_init_functions(&mut main, &init_functions)
                {
                    tracing::error!(worker = thread_index, %error, "worker initialization failed");
                }
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .max_blocking_threads(max_blocking_threads)
                    .enable_all()
                    .build()
                    .map_err(|source| RuntimeError::DataWorkerRuntime {
                        worker: thread_index,
                        source,
                    })?;
                let exit_status = crate::main_loop::data_plane_main_loop(
                    &mut main,
                    &runtime,
                    &remote_local,
                    idle_slice,
                );
                remote_local.close();
                if exit_status == 0 {
                    Ok(())
                } else {
                    Err(RuntimeError::DataWorkerExited {
                        worker: thread_index,
                        status: exit_status,
                    })
                }
            })
            .map_err(|source| RuntimeError::DataWorkerThreadSpawn {
                worker: thread_index as usize - 1,
                source,
            })?;
        self.join_handle = Some(thread);
        Ok(())
    }

    pub(crate) fn acknowledge_startup(&mut self) {
        self.startup_acknowledged = true;
    }

    pub(crate) fn install_barrier(&mut self, barrier: crate::WorkerBarrier) {
        self.barrier = Some(barrier);
    }

    pub(crate) fn request_refork(&mut self) {
        self.refork_pending = true;
    }

    pub(crate) fn complete_refork(&mut self) {
        self.refork_pending = false;
    }

    pub(crate) fn retain_join_handle(&mut self, handle: JoinHandle<RuntimeResult<()>>) {
        assert!(self.join_handle.is_none(), "WorkerThread launches once");
        self.join_handle = Some(handle);
    }

    pub(crate) fn join(&mut self) -> RuntimeResult<()> {
        let Some(handle) = self.join_handle.take() else {
            return Ok(());
        };
        match handle.join() {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.join_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
    }

    pub(crate) fn stack_size(&self) -> usize {
        self.stack_size
    }

    pub(crate) fn max_blocking_threads(&self) -> usize {
        self.max_blocking_threads
    }

    pub(crate) fn idle_slice(&self) -> Duration {
        self.idle_slice
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
