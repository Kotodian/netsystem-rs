//! Per-worker thread setup: CPU affinity, scheduler policy, QoS, NUMA.

use hammer_core::config::Worker;

#[cfg(target_os = "linux")]
use crate::numa;
#[cfg(target_os = "linux")]
use hammer_core::config::WorkerCpu;

/// Apply platform worker-thread setup before the dataplane loop runs.
pub fn apply_worker_thread_setup(worker: &Worker, index: usize) {
    #[cfg(target_os = "linux")]
    {
        apply_linux_cpu_affinity(index, &worker.cpu);
        apply_linux_scheduler(&worker.scheduler);
        if worker.numa.enabled {
            if let Some(node) = numa::current_numa_node() {
                let _ = numa::bind_current_thread_memory_to_numa(node);
            }
        }
        let _ = index;
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        apply_apple_qos(&worker.scheduler);
        let _ = index;
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    {
        let _ = (worker, index);
    }
}

#[cfg(target_os = "linux")]
fn apply_linux_cpu_affinity(index: usize, cpu: &WorkerCpu) {
    use core_affinity::{CoreId, get_core_ids, set_for_current};

    let core = if !cpu.worker_cores.is_empty() {
        cpu.worker_cores.get(index).copied()
    } else {
        auto_worker_core(index, cpu)
    };
    if let Some(id) = core {
        set_for_current(CoreId { id });
    }
}

#[cfg(target_os = "linux")]
fn auto_worker_core(index: usize, cpu: &WorkerCpu) -> Option<usize> {
    let cores = get_core_ids()?;
    let reserved: std::collections::HashSet<usize> =
        cpu.main_core.into_iter().chain(cpu.app_core).collect();
    cores
        .into_iter()
        .map(|core| core.id)
        .filter(|id| !reserved.contains(id))
        .nth(index)
}

#[cfg(target_os = "linux")]
fn apply_linux_scheduler(scheduler: &hammer_core::config::WorkerScheduler) {
    use hammer_core::config::SchedulerPolicy;
    use thread_priority::{
        RealtimeThreadSchedulePolicy, ThreadPriority, ThreadPriorityValue, ThreadSchedulePolicy,
    };

    match scheduler.policy {
        SchedulerPolicy::Other => {
            let _ = ThreadSchedulePolicy::Other.set_for_current();
        }
        SchedulerPolicy::Batch => {
            let _ = ThreadSchedulePolicy::Batch.set_for_current();
        }
        SchedulerPolicy::Idle => {
            let _ = ThreadSchedulePolicy::Idle.set_for_current();
        }
        SchedulerPolicy::Fifo => {
            let priority = ThreadPriorityValue::try_from(scheduler.priority)
                .unwrap_or(ThreadPriorityValue::Min);
            let _ = ThreadPriority::Crossplatform(priority).set_for_current();
        }
        SchedulerPolicy::Rr => {
            let priority = ThreadPriorityValue::try_from(scheduler.priority)
                .unwrap_or(ThreadPriorityValue::Min);
            let _ = RealtimeThreadSchedulePolicy::RoundRobin
                .with_priority(priority)
                .set_for_current();
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn apply_apple_qos(scheduler: &hammer_core::config::WorkerScheduler) {
    use hammer_core::config::QosClass;

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
    use hammer_core::config::Worker;

    #[test]
    fn worker_thread_setup_accepts_default_worker() {
        apply_worker_thread_setup(&Worker::default(), 0);
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
        apply_worker_thread_setup(&worker, 0);
        assert_eq!(
            core_affinity::get_for_current().map(|core| core.id),
            Some(target)
        );
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn worker_qos_set_does_not_panic() {
        use hammer_core::config::QosClass;

        let mut worker = Worker::default();
        worker.scheduler.qos = QosClass::UserInteractive;
        apply_worker_thread_setup(&worker, 0);
    }
}
