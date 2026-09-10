use std::time::Duration;
use std::{sync::OnceLock, thread::ThreadId};

use hammer_infra::bitmap::Bitmap;

use crate::config::WorkerScheduler;
#[cfg(target_os = "linux")]
use crate::config::{WorkerCpu, WorkerNuma};
use crate::error::{RuntimeError, RuntimeResult};
use crate::worker_thread::WorkerThread;

static MAIN_THREAD_ID: OnceLock<ThreadId> = OnceLock::new();
pub(crate) static THREAD_MAIN: OnceLock<ThreadMain> = OnceLock::new();

pub(crate) fn install_main_thread() -> RuntimeResult<()> {
    let current = std::thread::current().id();
    if let Some(owner) = MAIN_THREAD_ID.get() {
        if *owner != current {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
    } else {
        MAIN_THREAD_ID
            .set(current)
            .expect("Main Thread identity is installed once");
    }
    Ok(())
}

pub fn ensure_main_thread() -> RuntimeResult<()> {
    if MAIN_THREAD_ID
        .get()
        .is_some_and(|owner| *owner == std::thread::current().id())
    {
        Ok(())
    } else {
        Err(RuntimeError::ControlRequiresMainThread)
    }
}

pub fn ensure_main_thread_with_barrier() -> RuntimeResult<()> {
    ensure_main_thread()?;
    if crate::barrier::global()
        .as_ref()
        .is_some_and(|barrier| barrier.worker_count() != 0 && !barrier.is_pending())
    {
        return Err(RuntimeError::ControlRequiresWorkerBarrier);
    }
    Ok(())
}

pub struct ThreadMain {
    thread_count: u32,
    worker_count: u32,
    cpu_core_bitmap: Bitmap,
    cpu_socket_bitmap: Bitmap,
    worker_threads: Vec<WorkerThread>,
}

impl ThreadMain {
    pub fn new() -> RuntimeResult<Self> {
        install_main_thread()?;
        let mut cpu_core_bitmap = Bitmap::new();
        #[cfg(target_os = "linux")]
        let cores = core_affinity::get_core_ids().ok_or(RuntimeError::CpuInventoryUnavailable)?;
        #[cfg(target_os = "linux")]
        for core in cores {
            cpu_core_bitmap.set(core.id);
        }
        #[cfg(not(target_os = "linux"))]
        for cpu in 0..std::thread::available_parallelism()
            .map_err(|_| RuntimeError::CpuInventoryUnavailable)?
            .get()
        {
            cpu_core_bitmap.set(cpu);
        }

        let mut cpu_socket_bitmap = Bitmap::new();
        #[cfg(target_os = "linux")]
        for cpu in cpu_core_bitmap.iter_set() {
            cpu_socket_bitmap.set(crate::numa::node_for_cpu(cpu)? as usize);
        }
        #[cfg(not(target_os = "linux"))]
        cpu_socket_bitmap.set(0);

        Ok(Self {
            thread_count: 0,
            worker_count: 0,
            cpu_core_bitmap,
            cpu_socket_bitmap,
            worker_threads: Vec::new(),
        })
    }

    pub(crate) fn global() -> &'static Self {
        THREAD_MAIN
            .get()
            .expect("ThreadMain is published before worker startup")
    }

    pub fn configure(&mut self) -> RuntimeResult<()> {
        let worker_count = u32::try_from(crate::config::worker::worker_count()).map_err(|_| {
            RuntimeError::WorkerCountOverflow {
                count: crate::config::worker::worker_count(),
            }
        })?;
        #[cfg(target_os = "linux")]
        {
            self.configure_fields(
                worker_count,
                crate::config::worker::stack_size(),
                crate::config::worker::max_blocking_threads(),
                crate::config::worker::idle_slice(),
                crate::config::worker::cpu(),
                crate::config::worker::scheduler(),
                crate::config::worker::numa(),
            )
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.configure_fields(
                worker_count,
                crate::config::worker::stack_size(),
                crate::config::worker::max_blocking_threads(),
                crate::config::worker::idle_slice(),
                crate::config::worker::scheduler(),
            )
        }
    }

    #[cfg(target_os = "linux")]
    #[allow(clippy::too_many_arguments)]
    fn configure_fields(
        &mut self,
        worker_count: u32,
        stack_size: usize,
        max_blocking_threads: usize,
        idle_slice: Duration,
        cpu: &WorkerCpu,
        scheduler: &WorkerScheduler,
        numa: &WorkerNuma,
    ) -> RuntimeResult<()> {
        assert!(
            self.worker_threads.is_empty(),
            "ThreadMain is configured once"
        );
        let worker_count_usize = worker_count as usize;
        Self::validate_thread_fields(worker_count_usize, stack_size, max_blocking_threads)?;
        cpu.validate(worker_count_usize)?;
        scheduler.validate()?;
        numa.validate()?;
        let cores = self.worker_cpu_indices(worker_count_usize, cpu)?;
        let main_cpu = cpu
            .main_core
            .or_else(|| usize::try_from(unsafe { libc::sched_getcpu() }).ok());
        self.worker_threads.push(WorkerThread::new(
            0,
            "main",
            0,
            main_cpu.and_then(|cpu| u32::try_from(cpu).ok()),
            main_cpu.map(crate::numa::node_for_cpu).transpose()?,
            0,
            Duration::ZERO,
            scheduler.clone(),
            false,
            None,
            false,
        ));
        for (slot, cpu_index) in cores.into_iter().enumerate() {
            let thread_index =
                u32::try_from(slot + 1).map_err(|_| RuntimeError::WorkerCountOverflow {
                    count: worker_count_usize,
                })?;
            let numa_node = crate::numa::node_for_cpu(cpu_index)?;
            self.worker_threads.push(WorkerThread::new(
                thread_index,
                "workers",
                u32::try_from(slot).expect("worker slot fits configured u32 count"),
                Some(
                    u32::try_from(cpu_index)
                        .map_err(|_| RuntimeError::CpuIndexOverflow { cpu: cpu_index })?,
                ),
                Some(numa_node),
                stack_size,
                idle_slice,
                scheduler.clone(),
                numa.enabled,
                None,
                false,
            ));
        }
        self.worker_count = worker_count;
        self.thread_count = worker_count
            .checked_add(1)
            .expect("validated worker count leaves room for thread zero");
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn configure_fields(
        &mut self,
        worker_count: u32,
        stack_size: usize,
        max_blocking_threads: usize,
        idle_slice: Duration,
        scheduler: &WorkerScheduler,
    ) -> RuntimeResult<()> {
        assert!(
            self.worker_threads.is_empty(),
            "ThreadMain is configured once"
        );
        Self::validate_thread_fields(worker_count as usize, stack_size, max_blocking_threads)?;
        scheduler.validate()?;
        self.worker_threads.push(WorkerThread::new(
            0,
            "main",
            0,
            None,
            Some(0),
            0,
            Duration::ZERO,
            scheduler.clone(),
            false,
            None,
            false,
        ));
        for slot in 0..worker_count as usize {
            let thread_index =
                u32::try_from(slot + 1).map_err(|_| RuntimeError::WorkerCountOverflow {
                    count: worker_count as usize,
                })?;
            self.worker_threads.push(WorkerThread::new(
                thread_index,
                "workers",
                u32::try_from(slot).expect("worker slot fits configured u32 count"),
                None,
                Some(0),
                stack_size,
                idle_slice,
                scheduler.clone(),
                false,
                None,
                false,
            ));
        }
        self.worker_count = worker_count;
        self.thread_count = worker_count
            .checked_add(1)
            .expect("validated worker count leaves room for thread zero");
        Ok(())
    }

    fn validate_thread_fields(
        worker_count: usize,
        stack_size: usize,
        max_blocking_threads: usize,
    ) -> RuntimeResult<()> {
        if worker_count == 0 {
            return Err(RuntimeError::WorkerCountZero);
        }
        if stack_size == 0 {
            return Err(RuntimeError::WorkerStackSizeZero);
        }
        if max_blocking_threads == 0 {
            return Err(RuntimeError::WorkerBlockingThreadCountZero);
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn worker_cpu_indices(&self, count: usize, cpu: &WorkerCpu) -> RuntimeResult<Vec<usize>> {
        let mut available = self.cpu_core_bitmap.clone();
        for reserved in [cpu.main_core, cpu.app_core].into_iter().flatten() {
            if !available.clear(reserved) {
                return Err(RuntimeError::CpuUnavailable { cpu: reserved });
            }
        }

        if !cpu.worker_cores.is_empty() {
            for (worker, &configured) in cpu.worker_cores.iter().enumerate() {
                if !available.clear(configured) {
                    return Err(RuntimeError::WorkerCpuUnavailable {
                        worker,
                        cpu: configured,
                    });
                }
            }
            return Ok(cpu.worker_cores.clone());
        }

        let mut cores = Vec::with_capacity(count);
        for worker in 0..count {
            let cpu = available
                .first_set()
                .ok_or(RuntimeError::WorkerCpuExhausted { worker })?;
            available.clear(cpu);
            cores.push(cpu);
        }
        Ok(cores)
    }

    #[inline]
    pub fn thread_count(&self) -> u32 {
        self.thread_count
    }

    #[inline]
    pub fn worker_count(&self) -> u32 {
        self.worker_count
    }

    #[inline]
    pub fn cpu_core_bitmap(&self) -> &Bitmap {
        &self.cpu_core_bitmap
    }

    #[inline]
    pub fn cpu_socket_bitmap(&self) -> &Bitmap {
        &self.cpu_socket_bitmap
    }

    pub fn thread_by_index(&self, index: u32) -> Option<&WorkerThread> {
        self.worker_threads
            .iter()
            .find(|thread| thread.thread_index() == index)
    }

    pub fn register_thread(
        &mut self,
        name: &'static str,
        count: u32,
        stack_size: usize,
        entry: fn(u32) -> RuntimeResult<()>,
    ) -> RuntimeResult<()> {
        assert!(!name.is_empty(), "runtime thread registration has a name");
        assert!(stack_size != 0, "runtime thread registration has a stack");
        let next_thread_index = self
            .thread_count
            .checked_add(count)
            .ok_or(RuntimeError::ThreadCountOverflow)?;
        for instance_index in 0..count {
            let thread_index = self
                .thread_count
                .checked_add(instance_index)
                .expect("validated runtime thread count");
            self.worker_threads.push(WorkerThread::new(
                thread_index,
                name,
                instance_index,
                None,
                None,
                stack_size,
                Duration::ZERO,
                WorkerScheduler::default(),
                false,
                Some(entry),
                true,
            ));
        }
        self.thread_count = next_thread_index;
        Ok(())
    }
}
