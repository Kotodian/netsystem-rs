//! VPP-shaped Crypto Engine lifecycle and synchronized engine epochs.
//!
//! [`Main`] is the sole publication authority, matching VPP's
//! `vnet_crypto_main_t`. Each Runtime Engine owns one thread-bound [`Thread`],
//! matching `vnet_crypto_thread_t`. Runtime supplies only its generic worker
//! barrier and worker-main-loop callback ordering; it has no Crypto knowledge.

use core::hint::spin_loop;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::thread;

use arc_swap::{ArcSwap, ArcSwapOption};
use hammer_infra::crypto::InstructionSet;
use hammer_runtime::{Barrier, DataWorkerId, RuntimeError, RuntimeResult};

use super::{Engine, RegistryError};

fn worker_id(slot: usize) -> DataWorkerId {
    DataWorkerId::new(u32::try_from(slot).expect("validated worker count must fit u32"))
}

/// One completely prepared Crypto Engine publication identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Epoch(u64);

impl Epoch {
    const INITIAL: Self = Self(1);

    /// Returns the monotonically increasing epoch number.
    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// A replayable VPP-style Crypto Engine registration.
///
/// The registration crosses threads; each resulting [`Engine`] does not. The
/// `thread_index` follows VPP: zero is Main Thread and Data Workers start at
/// one.
#[derive(Clone, Copy)]
pub struct EngineRegistration {
    name: &'static str,
    create: fn(u32, InstructionSet) -> Result<Engine, RegistryError>,
}

impl EngineRegistration {
    /// Declares one replayable Crypto Engine registration.
    pub const fn new(
        name: &'static str,
        create: fn(u32, InstructionSet) -> Result<Engine, RegistryError>,
    ) -> Self {
        Self { name, create }
    }

    fn create(
        self,
        thread_index: u32,
        instructions: InstructionSet,
    ) -> Result<Engine, RegistryError> {
        (self.create)(thread_index, instructions)
    }
}

impl fmt::Debug for EngineRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineRegistration")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn create_builtin_engine(_: u32, instructions: InstructionSet) -> Result<Engine, RegistryError> {
    Engine::with_builtins(instructions)
}

/// Hammer's built-in Crypto Engine registration.
pub const BUILTIN_ENGINE_REGISTRATION: EngineRegistration =
    EngineRegistration::new("builtin", create_builtin_engine);

struct EngineEpoch {
    epoch: Epoch,
    registration: EngineRegistration,
}

impl fmt::Debug for EngineEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineEpoch")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

struct PrepareError {
    epoch: Epoch,
    source: RegistryError,
}

struct ThreadSlot {
    alive: AtomicBool,
    prepared: AtomicU64,
    active: AtomicU64,
    activation_error: AtomicU64,
    prepare_error: Barrier<Option<PrepareError>>,
}

impl ThreadSlot {
    fn new() -> Self {
        Self {
            alive: AtomicBool::new(false),
            prepared: AtomicU64::new(0),
            active: AtomicU64::new(0),
            activation_error: AtomicU64::new(0),
            prepare_error: Barrier::new(None),
        }
    }
}

/// Thread-bound Crypto Engine state owned by one Runtime Engine.
pub struct Thread {
    thread_index: u32,
    epoch: Epoch,
    engine: Engine,
    candidate: Option<(Epoch, Engine)>,
    main: Arc<Main>,
}

impl fmt::Debug for Thread {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Thread")
            .field("thread_index", &self.thread_index)
            .field("epoch", &self.epoch)
            .field(
                "candidate_epoch",
                &self.candidate.as_ref().map(|(epoch, _)| epoch),
            )
            .finish_non_exhaustive()
    }
}

impl Thread {
    /// Returns this thread's active epoch.
    #[inline]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Borrows the thread-bound Crypto Engine.
    #[inline]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Mutably borrows the thread-bound Crypto Engine.
    #[inline]
    pub fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }

    fn worker(&self) -> Option<DataWorkerId> {
        self.thread_index.checked_sub(1).map(DataWorkerId::new)
    }

    fn prepare(
        &mut self,
        epoch: Epoch,
        registration: EngineRegistration,
    ) -> Result<(), RegistryError> {
        let engine = registration.create(self.thread_index, InstructionSet::detect())?;
        self.candidate = Some((epoch, engine));
        Ok(())
    }

    fn activate(&mut self, epoch: Epoch) -> Result<(), UpdateError> {
        let observed = self
            .candidate
            .as_ref()
            .map_or(self.epoch, |(candidate, _)| *candidate);
        if observed != epoch {
            return Err(UpdateError::StaleEpoch {
                expected: epoch,
                observed,
                worker: self.worker(),
            });
        }
        let (_, engine) = self
            .candidate
            .take()
            .expect("matching candidate epoch must retain its Engine");
        self.engine = engine;
        self.epoch = epoch;
        Ok(())
    }

    fn poll(&mut self) -> Result<(), UpdateError> {
        self.prepare_pending();
        self.activate_published()?;
        if self.main.pending.load().is_none()
            && self
                .candidate
                .as_ref()
                .is_some_and(|(epoch, _)| *epoch > self.epoch)
        {
            self.candidate = None;
        }
        Ok(())
    }

    fn prepare_pending(&mut self) {
        let Some(pending) = self.main.pending.load_full() else {
            return;
        };
        let worker = self.worker().expect("only Data Workers poll Crypto Main");
        let main = Arc::clone(&self.main);
        let slot = &main.threads[worker.slot()];
        if pending.epoch <= Epoch(slot.prepared.load(Ordering::Acquire)) {
            return;
        }

        let result = self.prepare(pending.epoch, pending.registration);
        // SAFETY: this Data Worker owns exactly one result slot. Main Thread
        // reads it only after `preparing` reaches zero.
        unsafe {
            slot.prepare_error.with_mut_unchecked(|error| {
                *error = result.err().map(|source| PrepareError {
                    epoch: pending.epoch,
                    source,
                });
            });
        }
        slot.prepared.store(pending.epoch.0, Ordering::Release);
        let preparing = main.preparing.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(preparing, 0, "Crypto preparation count underflow");
    }

    fn activate_published(&mut self) -> Result<(), UpdateError> {
        let published = Epoch(self.main.published.load(Ordering::Acquire));
        if published <= self.epoch {
            return Ok(());
        }
        let worker = self.worker().expect("only Data Workers poll Crypto Main");
        let activation = self.activate(published);
        let slot = &self.main.threads[worker.slot()];
        match &activation {
            Ok(()) => slot.active.store(published.0, Ordering::Release),
            Err(UpdateError::StaleEpoch { observed, .. }) => {
                slot.activation_error.store(observed.0, Ordering::Release);
            }
            Err(_) => unreachable!("Crypto Thread activation has only stale-epoch failures"),
        }
        let remaining = self.main.activating.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(remaining, 0, "Crypto activation count underflow");
        while self.main.activating.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        activation?;
        if let Some((slot, thread)) = self
            .main
            .threads
            .iter()
            .enumerate()
            .find(|(_, thread)| thread.activation_error.load(Ordering::Acquire) != 0)
        {
            return Err(UpdateError::StaleEpoch {
                expected: published,
                observed: Epoch(thread.activation_error.load(Ordering::Acquire)),
                worker: Some(worker_id(slot)),
            });
        }
        Ok(())
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        if let Some(worker) = self.worker() {
            self.main.threads[worker.slot()]
                .alive
                .store(false, Ordering::Release);
        }
    }
}

/// Main Thread authority for Crypto Engine registrations and active epochs.
pub struct Main {
    active: ArcSwap<EngineEpoch>,
    pending: ArcSwapOption<EngineEpoch>,
    published: AtomicU64,
    threads: Box<[ThreadSlot]>,
    next_epoch: AtomicU64,
    updating: AtomicBool,
    cancel_pending: AtomicBool,
    preparing: AtomicU32,
    activating: AtomicU32,
    closed: AtomicBool,
}

impl fmt::Debug for Main {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Main")
            .field("epoch", &self.epoch())
            .field("worker_count", &self.threads.len())
            .finish_non_exhaustive()
    }
}

impl Main {
    /// Creates Crypto Main with Hammer's built-in Engine registration.
    pub fn new(worker_count: usize) -> Arc<Self> {
        Arc::new(Self {
            active: ArcSwap::from_pointee(EngineEpoch {
                epoch: Epoch::INITIAL,
                registration: BUILTIN_ENGINE_REGISTRATION,
            }),
            pending: ArcSwapOption::empty(),
            published: AtomicU64::new(Epoch::INITIAL.0),
            threads: (0..worker_count)
                .map(|_| ThreadSlot::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_epoch: AtomicU64::new(Epoch::INITIAL.0 + 1),
            updating: AtomicBool::new(false),
            cancel_pending: AtomicBool::new(false),
            preparing: AtomicU32::new(0),
            activating: AtomicU32::new(0),
            closed: AtomicBool::new(false),
        })
    }

    /// Returns the globally published Engine epoch.
    #[inline]
    pub fn epoch(&self) -> Epoch {
        self.active.load().epoch
    }

    /// Requests cancellation of the update currently waiting for preparation.
    pub fn cancel_update(&self) {
        self.cancel_pending.store(true, Ordering::Release);
    }

    /// Prepares and publishes one complete Engine registration on every thread.
    pub fn register_engine(
        &self,
        runtime: &mut hammer_runtime::Engine,
        registration: EngineRegistration,
    ) -> Result<Epoch, UpdateError> {
        if runtime.thread_index != 0 {
            return Err(UpdateError::MainThreadRequired {
                thread_index: runtime.thread_index,
            });
        }
        runtime
            .thread_state::<Thread>()
            .map_err(|source| UpdateError::MainThreadUnavailable { source })?;

        let guard = UpdateGuard::enter(self)?;
        let epoch = Epoch(
            self.next_epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    epoch.checked_add(1)
                })
                .map_err(|_| UpdateError::EpochExhausted)?,
        );

        let main_thread = runtime
            .thread_state_mut::<Thread>()
            .map_err(|source| UpdateError::MainThreadUnavailable { source })?;
        main_thread
            .prepare(epoch, registration)
            .map_err(|source| UpdateError::MainPrepare { epoch, source })?;

        let pending = Arc::new(EngineEpoch {
            epoch,
            registration,
        });
        let worker_count =
            u32::try_from(self.threads.len()).map_err(|_| UpdateError::BarrierWorkerCount {
                expected: self.threads.len(),
                active: runtime.configured_worker_count(),
            })?;
        let prepare = runtime
            .synchronize_workers(|_| {
                for slot in &self.threads {
                    // SAFETY: the previous preparation completed before a new
                    // update may enter, so no worker can access these slots.
                    unsafe {
                        slot.prepare_error.with_mut_unchecked(|error| *error = None);
                    }
                }
                self.preparing.store(worker_count, Ordering::Release);
                self.pending.store(Some(Arc::clone(&pending)));
            })
            .map_err(|source| UpdateError::Barrier { epoch, source });
        if let Err(error) = prepare {
            runtime
                .thread_state_mut::<Thread>()
                .expect("Main Thread Crypto state was already resolved")
                .candidate = None;
            return Err(error);
        }

        if let Err(error) = self.wait_prepared(epoch) {
            self.pending.store(None);
            runtime
                .thread_state_mut::<Thread>()
                .expect("Main Thread Crypto state was already resolved")
                .candidate = None;
            return Err(error);
        }

        let active = Arc::clone(&pending);
        let commit = match runtime.synchronize_workers(|runtime| {
            let result = (|| {
                runtime
                    .thread_state_mut::<Thread>()
                    .map_err(|source| UpdateError::MainThreadUnavailable { source })?
                    .activate(epoch)?;
                for slot in &self.threads {
                    slot.activation_error.store(0, Ordering::Relaxed);
                }
                self.activating.store(worker_count, Ordering::Release);
                self.active.store(Arc::clone(&active));
                self.published.store(epoch.0, Ordering::Release);
                Ok::<(), UpdateError>(())
            })();
            result
        }) {
            Ok(result) => result,
            Err(source) => Err(UpdateError::Barrier { epoch, source }),
        };
        if let Err(error) = commit {
            self.activating.store(0, Ordering::Release);
            self.pending.store(None);
            runtime
                .thread_state_mut::<Thread>()
                .expect("Main Thread Crypto state was already resolved")
                .candidate = None;
            return Err(error);
        }
        if let Err(error) = self.wait_activated(epoch) {
            self.pending.store(None);
            return Err(error);
        }
        self.pending.store(None);
        drop(guard);
        Ok(epoch)
    }

    fn install_thread(self: &Arc<Self>, thread_index: u32) -> Result<Thread, UpdateError> {
        let active = self.active.load_full();
        let engine = active
            .registration
            .create(thread_index, InstructionSet::detect())
            .map_err(
                |source| match thread_index.checked_sub(1).map(DataWorkerId::new) {
                    Some(worker) => UpdateError::WorkerPrepare {
                        epoch: active.epoch,
                        worker,
                        source,
                    },
                    None => UpdateError::MainPrepare {
                        epoch: active.epoch,
                        source,
                    },
                },
            )?;
        if let Some(worker) = thread_index.checked_sub(1).map(DataWorkerId::new) {
            let slot = self
                .threads
                .get(worker.slot())
                .ok_or(UpdateError::WorkerOutOfRange {
                    worker,
                    worker_count: self.threads.len(),
                })?;
            if slot.alive.swap(true, Ordering::AcqRel) {
                return Err(UpdateError::WorkerAlreadyInstalled { worker });
            }
            slot.prepared.store(active.epoch.0, Ordering::Release);
            slot.active.store(active.epoch.0, Ordering::Release);
        }
        Ok(Thread {
            thread_index,
            epoch: active.epoch,
            engine,
            candidate: None,
            main: Arc::clone(self),
        })
    }

    fn wait_prepared(&self, epoch: Epoch) -> Result<(), UpdateError> {
        self.wait_for_count(&self.preparing, epoch, true)?;
        for (slot, thread) in self.threads.iter().enumerate() {
            let worker = worker_id(slot);
            let observed = Epoch(thread.prepared.load(Ordering::Acquire));
            if observed != epoch {
                return Err(UpdateError::StaleEpoch {
                    expected: epoch,
                    observed,
                    worker: Some(worker),
                });
            }
            // SAFETY: `preparing == 0` means every worker finished writing its
            // owned result slot before the Main Thread reads it.
            let error = unsafe { thread.prepare_error.with_mut_unchecked(Option::take) };
            if let Some(error) = error {
                if error.epoch != epoch {
                    return Err(UpdateError::StaleEpoch {
                        expected: epoch,
                        observed: error.epoch,
                        worker: Some(worker),
                    });
                }
                return Err(UpdateError::WorkerPrepare {
                    epoch,
                    worker,
                    source: error.source,
                });
            }
        }
        Ok(())
    }

    fn wait_activated(&self, epoch: Epoch) -> Result<(), UpdateError> {
        self.wait_for_count(&self.activating, epoch, false)?;
        for (slot, thread) in self.threads.iter().enumerate() {
            let activation_error = thread.activation_error.load(Ordering::Acquire);
            if activation_error != 0 {
                return Err(UpdateError::StaleEpoch {
                    expected: epoch,
                    observed: Epoch(activation_error),
                    worker: Some(worker_id(slot)),
                });
            }
            let observed = Epoch(thread.active.load(Ordering::Acquire));
            if observed != epoch {
                return Err(UpdateError::StaleEpoch {
                    expected: epoch,
                    observed,
                    worker: Some(worker_id(slot)),
                });
            }
        }
        Ok(())
    }

    fn wait_for_count(
        &self,
        count: &AtomicU32,
        epoch: Epoch,
        cancellable: bool,
    ) -> Result<(), UpdateError> {
        loop {
            if count.load(Ordering::Acquire) == 0 {
                return if cancellable && self.cancel_pending.load(Ordering::Acquire) {
                    Err(UpdateError::Cancelled { epoch })
                } else {
                    Ok(())
                };
            }
            if cancellable && self.cancel_pending.load(Ordering::Acquire) {
                if let Some((slot, _)) = self
                    .threads
                    .iter()
                    .enumerate()
                    .find(|(_, thread)| !thread.alive.load(Ordering::Acquire))
                {
                    return Err(UpdateError::WorkerExit {
                        epoch,
                        worker: worker_id(slot),
                    });
                }
                thread::yield_now();
                continue;
            }
            if let Some((slot, _)) = self
                .threads
                .iter()
                .enumerate()
                .find(|(_, thread)| !thread.alive.load(Ordering::Acquire))
            {
                return Err(UpdateError::WorkerExit {
                    epoch,
                    worker: worker_id(slot),
                });
            }
            thread::yield_now();
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancel_pending.store(true, Ordering::Release);
    }
}

struct UpdateGuard<'a> {
    main: &'a Main,
}

impl<'a> UpdateGuard<'a> {
    fn enter(main: &'a Main) -> Result<Self, UpdateError> {
        if main.closed.load(Ordering::Acquire) {
            return Err(UpdateError::Closed);
        }
        main.updating
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| UpdateError::UpdateAlreadyPending)?;
        main.cancel_pending.store(false, Ordering::Release);
        Ok(Self { main })
    }
}

impl Drop for UpdateGuard<'_> {
    fn drop(&mut self) {
        self.main.updating.store(false, Ordering::Release);
    }
}

/// Failure while preparing or publishing a Crypto Engine epoch.
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("Crypto Engine epoch space is exhausted")]
    EpochExhausted,
    #[error("a Crypto Engine update is already pending")]
    UpdateAlreadyPending,
    #[error("Crypto Main has shut down")]
    Closed,
    #[error("Crypto Engine update {epoch:?} was cancelled")]
    Cancelled { epoch: Epoch },
    #[error("thread {thread_index} cannot publish Crypto Engine registrations")]
    MainThreadRequired { thread_index: u32 },
    #[error("Crypto worker barrier has {active} workers, expected {expected}")]
    BarrierWorkerCount { expected: usize, active: usize },
    #[error("Crypto Engine barrier operation failed for epoch {epoch:?}")]
    Barrier {
        epoch: Epoch,
        #[source]
        source: RuntimeError,
    },
    #[error("Crypto Thread {worker:?} is outside configured worker count {worker_count}")]
    WorkerOutOfRange {
        worker: DataWorkerId,
        worker_count: usize,
    },
    #[error("Crypto Thread {worker:?} is already installed")]
    WorkerAlreadyInstalled { worker: DataWorkerId },
    #[error("Crypto Thread {worker:?} exited while updating epoch {epoch:?}")]
    WorkerExit { epoch: Epoch, worker: DataWorkerId },
    #[error("Main Thread Crypto Engine is unavailable")]
    MainThreadUnavailable {
        #[source]
        source: RuntimeError,
    },
    #[error("Main Thread failed to prepare Crypto Engine epoch {epoch:?}")]
    MainPrepare {
        epoch: Epoch,
        #[source]
        source: RegistryError,
    },
    #[error("Crypto Thread {worker:?} failed to prepare Engine epoch {epoch:?}")]
    WorkerPrepare {
        epoch: Epoch,
        worker: DataWorkerId,
        #[source]
        source: RegistryError,
    },
    #[error("Crypto Thread {worker:?} observed stale epoch {observed:?}, expected {expected:?}")]
    StaleEpoch {
        expected: Epoch,
        observed: Epoch,
        worker: Option<DataWorkerId>,
    },
}

impl From<UpdateError> for RuntimeError {
    fn from(source: UpdateError) -> Self {
        Self::subsystem("crypto", source)
    }
}

#[hammer_component_macros::init_function(name = "crypto_init")]
fn crypto_init(runtime: &mut hammer_runtime::Engine) -> RuntimeResult<Arc<Main>> {
    let main = Main::new(runtime.configured_worker_count());
    let thread = main.install_thread(runtime.thread_index)?;
    runtime.install_thread_state(thread)?;
    Ok(main)
}

#[hammer_component_macros::worker_init_function(name = "crypto_worker_init")]
fn crypto_worker_init(runtime: &mut hammer_runtime::Engine, main: Arc<Main>) -> RuntimeResult<()> {
    let thread = main.install_thread(runtime.thread_index)?;
    runtime.install_thread_state(thread)?;
    runtime.register_worker_main_loop_callback(crypto_worker_main_loop)
}

fn crypto_worker_main_loop(runtime: &mut hammer_runtime::Engine) -> RuntimeResult<()> {
    runtime
        .thread_state_mut::<Thread>()?
        .poll()
        .map_err(Into::into)
}

#[hammer_component_macros::main_loop_exit_function]
fn crypto_exit(runtime: &mut hammer_runtime::Engine, main: Arc<Main>) -> RuntimeResult<()> {
    main.close();
    drop(runtime.remove_thread_state::<Thread>());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use hammer_runtime::config::Worker;
    use hammer_runtime::engine::EnginePool;
    use hammer_runtime::{RuntimeRegistry, start_workers::start_workers};

    use super::*;
    use crate::crypto::{Hash, HashOperation, Input};

    hammer_runtime::__declare_registration_image!(
        init_functions = [super::__INIT_FN_CRYPTO_INIT];
        config_functions = [];
        early_config_functions = [];
        main_loop_enter_functions = [];
        main_loop_exit_functions = [super::__INIT_FN_CRYPTO_EXIT];
        worker_init_functions = [super::__INIT_FN_CRYPTO_WORKER_INIT];
        graph_nodes = [];
        node_functions = [];
        process_nodes = [];
    );

    fn create_test_engine(_: u32, instructions: InstructionSet) -> Result<Engine, RegistryError> {
        Engine::with_builtins(instructions)
    }

    fn create_with_worker_two_failure(
        thread_index: u32,
        instructions: InstructionSet,
    ) -> Result<Engine, RegistryError> {
        if thread_index == 2 {
            return Err(RegistryError::MalformedAlgorithmName {
                name: "injected-thread-2".to_owned(),
            });
        }
        Engine::with_builtins(instructions)
    }

    fn create_delayed_test_engine(
        _: u32,
        instructions: InstructionSet,
    ) -> Result<Engine, RegistryError> {
        thread::sleep(Duration::from_millis(30));
        Engine::with_builtins(instructions)
    }

    const TEST_REGISTRATION: EngineRegistration =
        EngineRegistration::new("test", create_test_engine);
    const FAIL_WORKER_TWO_REGISTRATION: EngineRegistration =
        EngineRegistration::new("fail-worker-two", create_with_worker_two_failure);
    const DELAYED_TEST_REGISTRATION: EngineRegistration =
        EngineRegistration::new("delayed-test", create_delayed_test_engine);

    fn running_crypto(worker_count: usize) -> (EnginePool, Arc<Main>) {
        let mut worker = Worker::default();
        worker.count = worker_count;
        worker.idle_slice = Duration::from_millis(1);
        let mut runtime = hammer_runtime::Engine::new_configured(RuntimeRegistry::new(), worker)
            .expect("construct configured Runtime Engine");
        runtime
            .plugin_main_mut()
            .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
        hammer_runtime::init::run_init_functions(&mut runtime)
            .expect("initialize Crypto Main Thread");
        let main = runtime
            .registry
            .require::<Main>()
            .expect("Crypto Main capability");
        start_workers(&mut runtime).expect("start Crypto Data Workers");
        (EnginePool::new(runtime), main)
    }

    fn close(pool: &mut EnginePool) {
        pool.close().expect("close Crypto worker pool");
    }

    #[test]
    fn engine_registration_activates_on_main_and_every_worker() {
        let (mut pool, main) = running_crypto(2);
        let epoch = main
            .register_engine(pool.main_engine_mut(), TEST_REGISTRATION)
            .expect("publish complete Engine registration");
        assert_eq!(epoch, Epoch(2));
        assert_eq!(main.epoch(), epoch);
        assert!(
            main.threads
                .iter()
                .all(|thread| thread.active.load(Ordering::Acquire) == epoch.0)
        );
        close(&mut pool);
    }

    #[test]
    fn worker_prepare_failure_preserves_epoch_and_retry_succeeds() {
        let (mut pool, main) = running_crypto(2);
        let error = main
            .register_engine(pool.main_engine_mut(), FAIL_WORKER_TWO_REGISTRATION)
            .expect_err("worker preparation must fail");
        assert!(matches!(
            error,
            UpdateError::WorkerPrepare {
                epoch: Epoch(2),
                worker,
                ..
            } if worker == DataWorkerId::new(1)
        ));
        assert_eq!(main.epoch(), Epoch::INITIAL);
        assert!(
            main.threads
                .iter()
                .all(|thread| thread.active.load(Ordering::Acquire) == Epoch::INITIAL.0)
        );

        let epoch = main
            .register_engine(pool.main_engine_mut(), TEST_REGISTRATION)
            .expect("retry complete Engine registration");
        assert_eq!(epoch, Epoch(3));
        close(&mut pool);
    }

    #[test]
    fn existing_context_remains_bound_after_engine_epoch_replacement() {
        let (mut pool, main) = running_crypto(1);
        let mut context = {
            let thread = pool
                .main_engine()
                .thread_state::<Thread>()
                .expect("Main Thread Crypto state");
            let algorithm = thread
                .engine()
                .algorithm::<Hash>("sha-256")
                .expect("SHA-256 registration");
            thread.engine().context(algorithm).expect("prepare Context")
        };
        main.register_engine(pool.main_engine_mut(), TEST_REGISTRATION)
            .expect("replace Engine epoch");
        let mut output = [0_u8; 32];
        let mut operations = [HashOperation::new(
            Input::Contiguous(b"pinned"),
            &mut output,
        )];
        context
            .execute(&mut operations)
            .expect("old Context remains executable");
        assert_eq!(operations[0].status(), Some(Ok(32)));
        close(&mut pool);
    }

    #[test]
    fn cancellation_preserves_current_epoch_and_allows_retry() {
        let (mut pool, main) = running_crypto(1);
        let cancel = Arc::clone(&main);
        let controller = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            cancel.cancel_update();
        });
        let error = main
            .register_engine(pool.main_engine_mut(), DELAYED_TEST_REGISTRATION)
            .expect_err("cancel pending update");
        controller.join().expect("cancellation controller");
        assert!(matches!(error, UpdateError::Cancelled { epoch: Epoch(2) }));
        assert_eq!(main.epoch(), Epoch::INITIAL);
        assert_eq!(
            main.register_engine(pool.main_engine_mut(), TEST_REGISTRATION,)
                .expect("retry after cancellation"),
            Epoch(3)
        );
        close(&mut pool);
    }

    #[test]
    fn stale_epoch_and_worker_exit_are_distinct() {
        let main = Main::new(1);
        let thread = main.install_thread(1).expect("install Crypto Thread");
        main.threads[0].prepared.store(7, Ordering::Release);
        main.preparing.store(0, Ordering::Release);
        assert!(matches!(
            main.wait_prepared(Epoch(8)),
            Err(UpdateError::StaleEpoch {
                expected: Epoch(8),
                observed: Epoch(7),
                ..
            })
        ));
        drop(thread);
        main.preparing.store(1, Ordering::Release);
        assert!(matches!(
            main.wait_prepared(Epoch(9)),
            Err(UpdateError::WorkerExit {
                epoch: Epoch(9),
                worker,
            }) if worker == DataWorkerId::new(0)
        ));
    }
}
