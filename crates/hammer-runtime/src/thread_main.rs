use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU64;
use std::{sync::OnceLock, thread::ThreadId};

use hammer_core::data_plane::NodeId;
use hammer_infra::bitmap::Bitmap;

use crate::DataWorkerId;
use crate::config::worker::WorkerScheduler;
#[cfg(target_os = "linux")]
use crate::config::{CpuConfig, WorkerNuma};
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
    if crate::WorkerThread::main()
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
    worker_mains: UnsafeCell<Vec<UnsafeCell<Box<crate::DataPlaneMain>>>>,
}

// SAFETY: worker_mains is installed once before launch and its Vec never
// changes afterward. Each Data Worker exclusively borrows its own entry
// between barrier checks; thread zero borrows entries only while all workers
// have acknowledged the barrier and released those borrows.
unsafe impl Send for ThreadMain {}
unsafe impl Sync for ThreadMain {}

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
            worker_mains: UnsafeCell::new(Vec::new()),
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
        let cpu = crate::config::worker::cpu();
        let scheduler = WorkerScheduler::default();
        #[cfg(target_os = "linux")]
        {
            self.configure_fields(
                worker_count,
                crate::config::worker::stack_size(),
                crate::config::worker::max_blocking_threads(),
                cpu,
                &scheduler,
                crate::config::worker::numa(),
            )
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.configure_fields(
                worker_count,
                crate::config::worker::stack_size(),
                crate::config::worker::max_blocking_threads(),
                &scheduler,
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
        cpu: &CpuConfig,
        scheduler: &WorkerScheduler,
        numa: &WorkerNuma,
    ) -> RuntimeResult<()> {
        assert!(
            self.worker_threads.is_empty(),
            "ThreadMain is configured once"
        );
        let worker_count_usize = worker_count as usize;
        Self::validate_thread_fields(worker_count_usize, stack_size, max_blocking_threads)?;
        cpu.validate()?;
        scheduler.validate()?;
        numa.validate()?;
        let (main_cpu, cores) = self.worker_cpu_indices(worker_count_usize, cpu)?;
        self.worker_threads.push(WorkerThread::new(
            0,
            "main",
            0,
            main_cpu.and_then(|cpu| u32::try_from(cpu).ok()),
            main_cpu.map(crate::numa::node_for_cpu).transpose()?,
            0,
            scheduler.clone(),
            false,
            None,
            false,
        ));
        crate::worker_thread::apply_current_thread_setup(
            0,
            main_cpu.and_then(|cpu| u32::try_from(cpu).ok()),
            main_cpu.map(crate::numa::node_for_cpu).transpose()?,
            scheduler,
            false,
        )?;
        for (slot, cpu_index) in cores.iter_set().enumerate() {
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
    fn worker_cpu_indices(
        &self,
        count: usize,
        cpu: &CpuConfig,
    ) -> RuntimeResult<(Option<usize>, Bitmap)> {
        let mut available = self.cpu_core_bitmap.clone();
        for _ in 0..cpu.skip_cores {
            let skipped = available
                .first_set()
                .ok_or(RuntimeError::WorkerCpuExhausted { worker: 0 })?;
            available.clear(skipped);
        }

        let main_cpu = cpu
            .main_core
            .map(|configured| self.resolve_cpu(configured, cpu.relative))
            .transpose()?
            .or_else(|| usize::try_from(unsafe { libc::sched_getcpu() }).ok());
        if let Some(main_cpu) = main_cpu {
            if !available.clear(main_cpu) {
                return Err(RuntimeError::CpuUnavailable { cpu: main_cpu });
            }
        }

        if !cpu.corelist_workers.is_empty() {
            let mut workers = Bitmap::new();
            for (worker, configured) in cpu.corelist_workers.iter_set().enumerate() {
                let configured = self.resolve_cpu(configured, cpu.relative)?;
                if !available.clear(configured) {
                    return Err(RuntimeError::WorkerCpuUnavailable {
                        worker,
                        cpu: configured,
                    });
                }
                workers.set(configured);
            }
            return Ok((main_cpu, workers));
        }

        let mut cores = Bitmap::new();
        for worker in 0..count {
            let cpu_index_zero_available = available.clear(0);
            let cpu_index = available
                .first_set()
                .or_else(|| (!cpu.relative && cpu_index_zero_available).then_some(0));
            let cpu_index = cpu_index.ok_or(RuntimeError::WorkerCpuExhausted { worker })?;
            available.clear(cpu_index);
            if cpu_index_zero_available && cpu_index != 0 {
                available.set(0);
            }
            cores.set(cpu_index);
        }
        Ok((main_cpu, cores))
    }

    #[cfg(target_os = "linux")]
    fn resolve_cpu(&self, configured: usize, relative: bool) -> RuntimeResult<usize> {
        if !relative {
            return Ok(configured);
        }
        self.cpu_core_bitmap
            .iter_set()
            .nth(configured)
            .ok_or(RuntimeError::CpuUnavailable { cpu: configured })
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

    pub(crate) fn data_workers(&self) -> impl Iterator<Item = &WorkerThread> {
        self.worker_threads
            .iter()
            .skip(1)
            .take(self.worker_count as usize)
    }

    pub(crate) fn install_worker_mains(&self, mains: Vec<UnsafeCell<Box<crate::DataPlaneMain>>>) {
        crate::ensure_main_thread().expect("Data Worker mains install on thread zero");
        assert_eq!(mains.len(), self.worker_count as usize);
        for (slot, main) in mains.iter().enumerate() {
            // SAFETY: these cells are not published until installation ends.
            assert_eq!(unsafe { &*main.get() }.thread_index(), slot as u32 + 1);
        }
        // SAFETY: installation precedes every Worker launch, and this Vec is
        // never structurally modified after publication to the workers.
        let worker_mains = unsafe { &mut *self.worker_mains.get() };
        assert!(worker_mains.is_empty(), "Data Worker mains install once");
        *worker_mains = mains;
    }

    /// # Safety
    /// The caller is this Worker and holds no other borrow of its main. Its
    /// preceding main borrow ended before the latest barrier check.
    pub(crate) unsafe fn worker_main_on_worker(
        &self,
        worker: &WorkerThread,
    ) -> &mut crate::DataPlaneMain {
        assert!(
            worker.is_current(),
            "only the owning Worker borrows its main"
        );
        assert!(!worker.no_data_structure_clone());
        let index = worker.thread_index() as usize - 1;
        let mains = unsafe { &*self.worker_mains.get() };
        let main = mains
            .get(index)
            .expect("Data Worker mains install before launch");
        unsafe { &mut **main.get() }
    }

    /// # Safety
    /// Thread zero holds WorkerBarrier, this Worker has released its mutable
    /// borrow, and no other borrow of this main overlaps the returned one.
    pub(crate) unsafe fn worker_main_at_barrier(
        &self,
        worker: &WorkerThread,
    ) -> &mut crate::DataPlaneMain {
        crate::WorkerThread::__assert_held();
        assert!(!worker.no_data_structure_clone());
        let index = worker.thread_index() as usize - 1;
        let mains = unsafe { &*self.worker_mains.get() };
        let main = mains
            .get(index)
            .expect("Data Worker mains install before barrier CLI");
        unsafe { &mut **main.get() }
    }

    /// The main-loop count of the Data Worker with `thread_index`.
    ///
    /// `configure_fields` pushes thread zero first and then every Data Worker
    /// in ascending thread index order, and `register_thread` only appends, so
    /// the thread index indexes the descriptor directly and no second index
    /// mapping is needed.
    #[inline]
    pub(crate) fn worker_main_loop_count(&self, thread_index: u32) -> &AtomicU64 {
        self.worker_descriptor(thread_index).main_loop_count()
    }

    /// The latest published loops-per-second value of the Data Worker with
    /// `thread_index`.
    #[inline]
    pub(crate) fn worker_loops_per_second(&self, thread_index: u32) -> &AtomicU64 {
        self.worker_descriptor(thread_index).loops_per_second()
    }

    /// Resolves one Data Worker descriptor by thread index.
    #[inline]
    fn worker_descriptor(&self, thread_index: u32) -> &WorkerThread {
        self.worker_threads
            .get(thread_index as usize)
            .filter(|descriptor| descriptor.thread_index() == thread_index)
            .expect("Data Worker descriptors are stored by thread index")
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

/// Request an interrupt on the target worker after its domain queue commits.
#[inline]
pub fn interrupt_worker_node(worker: DataWorkerId, node: NodeId) {
    ThreadMain::global()
        .worker_descriptor(worker.thread_index())
        .interrupt_node(node);
}

/// Whether this call runs on the selected Data Worker.
#[inline]
pub fn is_current_worker(worker: DataWorkerId) -> bool {
    ThreadMain::global()
        .worker_descriptor(worker.thread_index())
        .is_current()
}

/// Declares the worker-count gauge `/sys/num_worker_threads`.
///
/// The count belongs to the worker-thread domain: VPP creates this gauge in
/// `threads.c` and sets it to the number of Data Workers, so the entry is
/// declared here rather than by the stats mechanism or a heap owner. It is not
/// a collector: `start_workers` writes it once, the way VPP's thread startup
/// does.
#[derive(hammer_component_macros::Stats)]
pub(crate) struct WorkerThreadCount {
    #[stats(path = "/sys/num_worker_threads")]
    pub(crate) worker_threads: hammer_stats::Gauge,
}
