//! Crypto Engine lifecycle declarations.
//!
//! Runtime owns lifecycle ordering only. [`CryptoEngineMain`] owns the
//! immutable Crypto Engine registration selected during service initialization,
//! and each execution-thread owner constructs and retains its own
//! [`CryptoEngineThread`]. Runtime does not store, erase, poll, or synchronize
//! Crypto Engine state.

use std::fmt;
use std::sync::Arc;

use hammer_infra::crypto::InstructionSet;
use hammer_runtime::RuntimeResult;

use super::{Engine, RegistryError};

/// The identity of one immutable Crypto Engine catalog.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CryptoEngineEpoch(u64);

impl CryptoEngineEpoch {
    /// The catalog installed during service initialization.
    pub const INITIAL: Self = Self(1);

    /// Returns the monotonically increasing catalog identity.
    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// A replayable VPP-style Crypto Engine registration.
///
/// The registration is shared across execution threads. The resulting
/// [`Engine`] remains owned by the thread that creates it.
#[derive(Clone, Copy)]
pub struct CryptoEngineRegistration {
    name: &'static str,
    create: fn(u32, InstructionSet) -> Result<Engine, RegistryError>,
}

impl CryptoEngineRegistration {
    /// Declares a Crypto Engine registration applied during thread
    /// initialization.
    pub const fn new(
        name: &'static str,
        create: fn(u32, InstructionSet) -> Result<Engine, RegistryError>,
    ) -> Self {
        Self { name, create }
    }

    fn create(self, thread_index: u32) -> Result<Engine, RegistryError> {
        (self.create)(thread_index, InstructionSet::detect())
    }
}

impl fmt::Debug for CryptoEngineRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CryptoEngineRegistration")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn create_builtin_engine(_: u32, instructions: InstructionSet) -> Result<Engine, RegistryError> {
    Engine::with_builtins(instructions)
}

/// Hammer's built-in Crypto Engine registration.
const BUILTIN_CRYPTO_ENGINE_REGISTRATION: CryptoEngineRegistration =
    CryptoEngineRegistration::new("builtin", create_builtin_engine);

/// Main Thread authority for the Crypto Engine catalog selected at startup.
#[derive(Debug)]
pub struct CryptoEngineMain {
    epoch: CryptoEngineEpoch,
    registration: CryptoEngineRegistration,
}

impl CryptoEngineMain {
    /// Creates the immutable startup catalog.
    #[inline]
    pub const fn new(registration: CryptoEngineRegistration) -> Self {
        Self {
            epoch: CryptoEngineEpoch::INITIAL,
            registration,
        }
    }

    /// Returns the installed catalog identity.
    #[inline]
    pub const fn epoch(&self) -> CryptoEngineEpoch {
        self.epoch
    }
}

/// Crypto Engine state owned by one execution thread.
pub struct CryptoEngineThread {
    thread_index: u32,
    epoch: CryptoEngineEpoch,
    engine: Engine,
}

impl fmt::Debug for CryptoEngineThread {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CryptoEngineThread")
            .field("thread_index", &self.thread_index)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

impl CryptoEngineThread {
    /// Applies the selected registration directly on the calling thread.
    pub fn new(main: &CryptoEngineMain, thread_index: u32) -> Result<Self, RegistryError> {
        let engine = main.registration.create(thread_index)?;
        Ok(Self {
            thread_index,
            epoch: main.epoch,
            engine,
        })
    }

    /// Returns the catalog used to initialize this thread.
    #[inline]
    pub const fn epoch(&self) -> CryptoEngineEpoch {
        self.epoch
    }

    /// Borrows the thread-owned Crypto Engine.
    #[inline]
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Mutably borrows the thread-owned Crypto Engine.
    #[inline]
    pub const fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }
}

#[hammer_component_macros::init_function(name = "crypto_init")]
fn crypto_init() -> RuntimeResult<Arc<CryptoEngineMain>> {
    Ok(Arc::new(CryptoEngineMain::new(
        BUILTIN_CRYPTO_ENGINE_REGISTRATION,
    )))
}

#[cfg(test)]
mod tests {
    use hammer_runtime::{Engine as RuntimeEngine, RuntimeRegistry};

    use super::*;
    use crate::crypto::{Hash, HashOperation, Input};

    hammer_runtime::__declare_registration_image!(
        init_functions = [super::__INIT_FN_CRYPTO_INIT];
        config_functions = [];
        early_config_functions = [];
        main_loop_enter_functions = [];
        main_loop_exit_functions = [];
        worker_init_functions = [];
        graph_nodes = [];
        node_functions = [];
        process_nodes = [];
    );

    #[test]
    fn lifecycle_registers_crypto_engine_main() {
        let mut runtime = RuntimeEngine::new(
            hammer_runtime::DataPlaneRuntime::new(Default::default()),
            RuntimeRegistry::new(),
        );
        runtime
            .plugin_main_mut()
            .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);

        hammer_runtime::init::run_init_functions(&mut runtime)
            .expect("initialize Crypto Engine catalog");

        let main = runtime
            .registry
            .require::<CryptoEngineMain>()
            .expect("Crypto Engine Main capability");
        assert_eq!(main.epoch(), CryptoEngineEpoch::INITIAL);
    }

    #[test]
    fn registration_is_applied_on_the_owning_thread() {
        let main = CryptoEngineMain::new(BUILTIN_CRYPTO_ENGINE_REGISTRATION);
        let thread = CryptoEngineThread::new(&main, 3).expect("initialize Crypto Engine Thread");
        let algorithm = thread
            .engine()
            .algorithm::<Hash>("sha-256")
            .expect("SHA-256 registration");
        let mut context = thread
            .engine()
            .context(algorithm)
            .expect("prepare hash Context");
        let mut output = [0_u8; 32];
        let mut operations = [HashOperation::new(
            Input::Contiguous(b"thread-owned"),
            &mut output,
        )];

        context
            .execute(&mut operations)
            .expect("execute registered implementation");

        assert_eq!(thread.epoch(), CryptoEngineEpoch::INITIAL);
        assert_eq!(operations[0].status(), Some(Ok(32)));
    }

    #[test]
    fn registration_error_is_returned_by_thread_initialization() {
        fn reject_thread(_: u32, _: InstructionSet) -> Result<Engine, RegistryError> {
            Err(RegistryError::MalformedAlgorithmName {
                name: "rejected".to_owned(),
            })
        }

        let main = CryptoEngineMain::new(CryptoEngineRegistration::new("reject", reject_thread));
        let error =
            CryptoEngineThread::new(&main, 1).expect_err("registration failure must remain typed");

        assert!(matches!(
            error,
            RegistryError::MalformedAlgorithmName { name } if name == "rejected"
        ));
    }
}
