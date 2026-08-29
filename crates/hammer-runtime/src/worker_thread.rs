//! Per-worker thread setup: CPU affinity, scheduler policy, QoS, NUMA.

use crate::config::Worker;

#[cfg(target_os = "linux")]
use crate::config::WorkerCpu;
#[cfg(target_os = "linux")]
use crate::numa;

impl Worker {
    /// Apply this worker's platform thread setup before its dataplane loop runs
    /// and return the NUMA node selected for its worker-local runtime view.
    #[cfg(target_os = "linux")]
    pub(crate) fn apply_current_thread_setup(&self, index: usize) -> crate::RuntimeResult<u32> {
        apply_cpu_affinity(index, &self.cpu)?;
        apply_scheduler(&self.scheduler)?;
        if self.numa.enabled {
            let numa_node = numa::current_numa_node().ok_or_else(|| {
                crate::RuntimeError::lifecycle(
                    "probe data worker NUMA node",
                    "getcpu did not return a NUMA node",
                )
            })?;
            numa::bind_current_thread_memory_to_numa(numa_node)?;
            Ok(numa_node)
        } else {
            Ok(numa::current_numa_node().unwrap_or(0))
        }
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn apply_current_thread_setup(&self, _: usize) -> crate::RuntimeResult<u32> {
        apply_qos(&self.scheduler)?;
        Ok(0)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn apply_current_thread_setup(&self, _: usize) -> crate::RuntimeResult<u32> {
        Ok(0)
    }
}

#[cfg(target_os = "linux")]
fn apply_cpu_affinity(index: usize, cpu: &WorkerCpu) -> crate::RuntimeResult<()> {
    use core_affinity::{CoreId, set_for_current};

    let core = worker_core(index, cpu);
    if let Some(id) = core
        && !set_for_current(CoreId { id })
    {
        return Err(crate::RuntimeError::lifecycle(
            "set data worker CPU affinity",
            format!("failed to select CPU core {id}"),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn worker_core(index: usize, cpu: &WorkerCpu) -> Option<usize> {
    if !cpu.worker_cores.is_empty() {
        return cpu.worker_cores.get(index).copied();
    }
    let cores = core_affinity::get_core_ids()?;
    let reserved: std::collections::HashSet<usize> =
        cpu.main_core.into_iter().chain(cpu.app_core).collect();
    cores
        .into_iter()
        .map(|core| core.id)
        .filter(|id| !reserved.contains(id))
        .nth(index)
}

#[cfg(target_os = "linux")]
fn apply_scheduler(scheduler: &crate::config::WorkerScheduler) -> crate::RuntimeResult<()> {
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
    set_thread_priority_and_policy(thread_native_id(), priority, policy).map_err(|error| {
        crate::RuntimeError::lifecycle("set data worker scheduler", error.to_string())
    })
}

#[cfg(target_os = "macos")]
fn apply_qos(scheduler: &crate::config::WorkerScheduler) -> crate::RuntimeResult<()> {
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
        Err(crate::RuntimeError::lifecycle(
            "set data worker QoS",
            std::io::Error::from_raw_os_error(result).to_string(),
        ))
    }
}
