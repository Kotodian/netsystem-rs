//! Synchronous typed cryptographic execution.
//!
//! `hammer-service` owns algorithm identity, implementation selection, and
//! operation lifecycle. Portable algorithm semantics remain in
//! `hammer-infra`; `hammer-runtime` does not participate in this boundary.

use std::marker::PhantomData;
use std::rc::Rc;

/// A closed cryptographic operation family.
pub trait Family: private::Sealed + Sized + 'static {
    /// One operation accepted by this family.
    type Operation<'a>;
    /// One batch-level implementation entry point.
    type Dispatch: Copy;

    #[doc(hidden)]
    fn algorithm(engine: &Engine, name: &str) -> Option<AlgorithmId<Self>>;

    #[doc(hidden)]
    fn dispatch(engine: &Engine, algorithm: AlgorithmId<Self>) -> Option<Self::Dispatch>;
}

mod private {
    pub trait Sealed {}
}

/// The hash operation family.
#[derive(Debug)]
pub struct Hash;

impl private::Sealed for Hash {}

impl Family for Hash {
    type Operation<'a> = HashOperation<'a>;
    type Dispatch = for<'a> fn(&mut [HashOperation<'a>]);

    fn algorithm(engine: &Engine, name: &str) -> Option<AlgorithmId<Self>> {
        (engine.sha256_name == name).then_some(AlgorithmId::new(0))
    }

    fn dispatch(engine: &Engine, algorithm: AlgorithmId<Self>) -> Option<Self::Dispatch> {
        (algorithm.slot == 0).then_some(engine.sha256_dispatch)
    }
}

/// A process-local algorithm identity typed by its operation family.
#[derive(Debug, Eq, PartialEq)]
pub struct AlgorithmId<F: Family> {
    slot: u32,
    family: PhantomData<fn() -> F>,
}

impl<F: Family> AlgorithmId<F> {
    const fn new(slot: u32) -> Self {
        Self {
            slot,
            family: PhantomData,
        }
    }
}

impl<F: Family> Clone for AlgorithmId<F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F: Family> Copy for AlgorithmId<F> {}

/// Caller-owned hash input in contiguous or scatter-gather form.
#[derive(Clone, Copy, Debug)]
pub enum HashInput<'a> {
    /// One contiguous byte slice.
    Contiguous(&'a [u8]),
    /// Ordered fragments treated as one logical message.
    Scatter(&'a [&'a [u8]]),
}

/// The independent completion state of one hash operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashStatus {
    /// The operation has not yet been executed.
    Pending,
    /// The complete digest was written to caller memory.
    Complete {
        /// Number of output bytes written.
        written: usize,
    },
    /// Caller output was too short and remained unchanged.
    OutputTooSmall {
        /// Digest size required by the algorithm.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
    },
}

/// One synchronous hash request and its caller-owned output.
#[derive(Debug)]
pub struct HashOperation<'a> {
    input: HashInput<'a>,
    output: &'a mut [u8],
    status: HashStatus,
}

impl<'a> HashOperation<'a> {
    /// Creates a pending operation over caller-owned memory.
    pub fn new(input: HashInput<'a>, output: &'a mut [u8]) -> Self {
        Self {
            input,
            output,
            status: HashStatus::Pending,
        }
    }

    /// Returns the operation's current completion state.
    pub fn status(&self) -> HashStatus {
        self.status
    }
}

/// A typed set of operations dispatched synchronously as one unit.
#[derive(Debug)]
pub struct Batch<'batch, 'data, F: Family> {
    operations: &'batch mut [F::Operation<'data>],
}

impl<'batch, 'data, F: Family> Batch<'batch, 'data, F> {
    /// Borrows the operations that comprise this batch.
    pub fn new(operations: &'batch mut [F::Operation<'data>]) -> Self {
        Self { operations }
    }
}

/// The owner of cryptographic registries and implementation selection.
#[derive(Debug)]
pub struct Engine {
    sha256_name: String,
    sha256_dispatch: <Hash as Family>::Dispatch,
}

impl Engine {
    /// Creates an Engine containing Hammer's standard built-in algorithms.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::MalformedAlgorithmName`] if a built-in name no
    /// longer satisfies the canonical algorithm-name contract.
    pub fn with_builtins() -> Result<Self, RegistryError> {
        let sha256_name = validate_standard_algorithm_name("sha-256")?.to_owned();
        Ok(Self {
            sha256_name,
            sha256_dispatch: execute_sha256,
        })
    }

    /// Resolves a canonical name to a family-typed process-local identity.
    pub fn algorithm<F: Family>(&self, name: &str) -> Option<AlgorithmId<F>> {
        F::algorithm(self, name)
    }

    /// Creates a thread-bound Context and selects its implementation once.
    ///
    /// # Errors
    ///
    /// Returns [`ContextError::AlgorithmUnavailable`] when no implementation
    /// currently supports the supplied identity.
    pub fn context<F: Family>(
        &self,
        algorithm: AlgorithmId<F>,
    ) -> Result<Context<F>, ContextError> {
        let dispatch = F::dispatch(self, algorithm).ok_or(ContextError::AlgorithmUnavailable {
            algorithm: algorithm.slot,
        })?;
        Ok(Context {
            algorithm,
            dispatch,
            thread_bound: PhantomData,
        })
    }
}

/// A prepared, implementation-bound cryptographic context.
///
/// Contexts are deliberately confined to their creating thread:
///
/// ```compile_fail
/// use hammer_service::crypto::{Engine, Hash};
///
/// let engine = Engine::with_builtins().unwrap();
/// let algorithm = engine.algorithm::<Hash>("sha-256").unwrap();
/// let context = engine.context(algorithm).unwrap();
/// std::thread::spawn(move || drop(context));
/// ```
#[derive(Debug)]
pub struct Context<F: Family> {
    algorithm: AlgorithmId<F>,
    dispatch: F::Dispatch,
    // Rc is deliberately part of the marker: Context must be neither Send nor Sync.
    thread_bound: PhantomData<Rc<()>>,
}

impl Context<Hash> {
    /// Executes all operations synchronously through one batch dispatch.
    pub fn execute(&mut self, batch: &mut Batch<'_, '_, Hash>) {
        (self.dispatch)(batch.operations);
    }

    /// Returns the algorithm permanently bound to this Context.
    pub fn algorithm(&self) -> AlgorithmId<Hash> {
        self.algorithm
    }
}

/// A failure while publishing the built-in registry.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum RegistryError {
    /// A built-in algorithm violated the canonical naming contract.
    #[error("malformed standard algorithm name `{name}`")]
    MalformedAlgorithmName {
        /// Rejected name.
        name: String,
    },
}

/// A failure while creating an implementation-bound Context.
#[derive(Clone, Copy, Debug, thiserror::Error, Eq, PartialEq)]
pub enum ContextError {
    /// No currently available implementation supports the algorithm.
    #[error("algorithm slot {algorithm} has no available implementation")]
    AlgorithmUnavailable {
        /// Process-local algorithm slot.
        algorithm: u32,
    },
}

fn validate_standard_algorithm_name(name: &str) -> Result<&str, RegistryError> {
    let valid = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--");
    if valid {
        Ok(name)
    } else {
        Err(RegistryError::MalformedAlgorithmName {
            name: name.to_owned(),
        })
    }
}

fn execute_sha256(operations: &mut [HashOperation<'_>]) {
    for operation in operations {
        let result = match operation.input {
            HashInput::Contiguous(input) => {
                hammer_infra::crypto::sha256(&[input], operation.output)
            }
            HashInput::Scatter(input) => hammer_infra::crypto::sha256(input, operation.output),
        };
        operation.status = match result {
            Ok(written) => HashStatus::Complete { written },
            Err(hammer_infra::crypto::HashError::OutputTooSmall { required, provided }) => {
                HashStatus::OutputTooSmall { required, provided }
            }
        };
    }
}
