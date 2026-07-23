//! Per-worker thread setup: CPU affinity, scheduler policy, QoS, NUMA.

use crate::config::Worker;

#[cfg(target_os = "linux")]
use crate::config::WorkerCpu;
#[cfg(target_os = "linux")]
use crate::numa;

impl Worker {
    /// Apply this worker's platform thread setup before its dataplane loop runs.
    pub(crate) fn apply_current_thread_setup(&self, index: usize) -> crate::RuntimeResult<()> {
        #[cfg(target_os = "linux")]
        {
            apply_cpu_affinity(index, &self.cpu);
            apply_scheduler(&self.scheduler);
            if self.numa.enabled {
                if let Some(node) = numa::current_numa_node() {
                    numa::bind_current_thread_memory_to_numa(node)?;
                }
            }
            let _ = index;
        }
        #[cfg(target_os = "macos")]
        {
            apply_qos(&self.scheduler);
            let _ = index;
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (self, index);
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn apply_cpu_affinity(index: usize, cpu: &WorkerCpu) {
    use core_affinity::{CoreId, set_for_current};

    let core = worker_core(index, cpu);
    if let Some(id) = core {
        set_for_current(CoreId { id });
    }
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
fn apply_scheduler(scheduler: &crate::config::WorkerScheduler) {
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
    let _ = set_thread_priority_and_policy(thread_native_id(), priority, policy);
}

#[cfg(target_os = "macos")]
fn apply_qos(scheduler: &crate::config::WorkerScheduler) {
    use crate::config::QosClass;

    let qos = match scheduler.qos {
        QosClass::UserInteractive => libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
        QosClass::UserInitiated => libc::qos_class_t::QOS_CLASS_USER_INITIATED,
        QosClass::Default => libc::qos_class_t::QOS_CLASS_DEFAULT,
        QosClass::Utility => libc::qos_class_t::QOS_CLASS_UTILITY,
        QosClass::Background => libc::qos_class_t::QOS_CLASS_BACKGROUND,
    };
    unsafe {
        libc::pthread_set_qos_class_self_np(qos, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Worker;

    #[test]
    fn worker_thread_setup_accepts_default_worker() {
        Worker::default()
            .apply_current_thread_setup(0)
            .expect("apply default worker setup");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_pinned_to_configured_core() {
        use core_affinity::get_core_ids;

        let cores = get_core_ids().expect("cpu topology");
        if cores.len() < 2 {
            return;
        }
        let target = cores[1].id;
        let mut worker = Worker::default();
        worker.count = 1;
        worker.cpu.worker_cores = vec![target];
        worker
            .apply_current_thread_setup(0)
            .expect("apply pinned worker setup");
        assert_eq!(
            get_core_ids()
                .map(|cores| { cores.into_iter().map(|core| core.id).collect::<Vec<_>>() }),
            Some(vec![target])
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn worker_qos_set_does_not_panic() {
        use crate::config::QosClass;

        let mut worker = Worker::default();
        worker.scheduler.qos = QosClass::UserInteractive;
        worker
            .apply_current_thread_setup(0)
            .expect("apply worker QoS");
    }
}
