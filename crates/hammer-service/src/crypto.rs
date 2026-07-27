//! Synchronous typed cryptographic execution.
//!
//! `hammer-service` owns algorithm identity, implementation selection, and
//! operation lifecycle. Portable algorithm semantics remain in
//! `hammer-infra`; `hammer-runtime` does not participate in this boundary.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

use hammer_infra::pool::{Index as PoolIndex, Pool};
use zeroize::Zeroizing;

/// A closed cryptographic operation family.
pub trait Family: private::Sealed + Sized + 'static {
    /// One operation accepted by this family.
    type Operation<'a>;
    /// Prepared implementation state owned by a Context.
    type Prepared: fmt::Debug;
    /// One batch-level implementation entry point.
    type Dispatch: Copy;

    #[doc(hidden)]
    const KEY_FAMILY: u8;

    #[doc(hidden)]
    fn algorithm(engine: &Engine, name: &str) -> Option<AlgorithmId<Self>>;

    #[doc(hidden)]
    fn unkeyed_context(
        engine: &Engine,
        algorithm: AlgorithmId<Self>,
    ) -> Option<(Self::Dispatch, Self::Prepared)>;
}

mod private {
    pub trait Sealed {}
}

/// The hash operation family.
#[derive(Debug)]
pub struct Hash;

/// Prepared state for a hash Context.
#[derive(Debug)]
pub struct HashPrepared;

impl private::Sealed for Hash {}

impl Family for Hash {
    type Operation<'a> = HashOperation<'a>;
    type Prepared = HashPrepared;
    type Dispatch = for<'a> fn(&mut HashPrepared, &mut [HashOperation<'a>]);

    const KEY_FAMILY: u8 = 2;

    fn algorithm(engine: &Engine, name: &str) -> Option<AlgorithmId<Self>> {
        (engine.sha256_name == name).then_some(AlgorithmId::new(0))
    }

    fn unkeyed_context(
        engine: &Engine,
        algorithm: AlgorithmId<Self>,
    ) -> Option<(Self::Dispatch, Self::Prepared)> {
        (algorithm.slot == 0).then_some((engine.sha256_dispatch, HashPrepared))
    }
}

/// The authenticated-encryption operation family.
#[derive(Debug)]
pub struct Aead;

/// Prepared state for an authenticated-encryption Context.
#[derive(Debug)]
pub struct AeadPrepared {
    cipher: hammer_infra::crypto::Aes128Gcm,
    operations: KeyOperations,
}

impl private::Sealed for Aead {}

impl Family for Aead {
    type Operation<'a> = AeadOperation<'a>;
    type Prepared = AeadPrepared;
    type Dispatch = for<'a> fn(&mut AeadPrepared, &mut [AeadOperation<'a>]);

    const KEY_FAMILY: u8 = 1;

    fn algorithm(engine: &Engine, name: &str) -> Option<AlgorithmId<Self>> {
        (engine.aes128_gcm_name == name).then_some(AlgorithmId::new(0))
    }

    fn unkeyed_context(
        _: &Engine,
        _: AlgorithmId<Self>,
    ) -> Option<(Self::Dispatch, Self::Prepared)> {
        None
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

/// Caller-owned AEAD input in contiguous or scatter-gather form.
#[derive(Clone, Copy, Debug)]
pub enum AeadInput<'a> {
    /// One contiguous byte slice.
    Contiguous(&'a [u8]),
    /// Ordered fragments treated as one logical payload.
    Scatter(&'a [&'a [u8]]),
}

impl AeadInput<'_> {
    fn with_fragments<T>(self, operation: impl FnOnce(&[&[u8]]) -> T) -> T {
        match self {
            Self::Contiguous(input) => operation(&[input]),
            Self::Scatter(input) => operation(input),
        }
    }
}

/// The authenticated-encryption operation being requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AeadDirection {
    /// Encrypt and generate an authentication tag.
    Seal,
    /// Authenticate and decrypt.
    Open,
}

/// The independent completion state of one AEAD operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AeadStatus {
    /// The operation has not yet been executed.
    Pending,
    /// The complete payload was written or transformed in place.
    Complete {
        /// Number of payload bytes written.
        written: usize,
    },
    /// Caller output was too short and remained unchanged.
    OutputTooSmall {
        /// Payload size required by the operation.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
    },
    /// The nonce length is invalid for the selected algorithm.
    InvalidNonceLength {
        /// Nonce size required by the algorithm.
        required: usize,
        /// Nonce size supplied by the caller.
        provided: usize,
    },
    /// The detached tag memory has an invalid length.
    InvalidTagLength {
        /// Tag size required by the algorithm.
        required: usize,
        /// Tag size supplied by the caller.
        provided: usize,
    },
    /// The authentication tag did not validate and no plaintext was retained.
    AuthenticationFailed,
    /// The payload exceeded the selected algorithm's size limit.
    InputTooLong,
    /// The key policy denied this operation.
    PolicyDenied {
        /// Operation denied by the immutable policy.
        operation: AeadDirection,
    },
}

#[derive(Debug)]
enum AeadPayload<'a> {
    OutOfPlace {
        input: AeadInput<'a>,
        output: &'a mut [u8],
    },
    InPlace(&'a mut [u8]),
}

#[derive(Debug)]
enum AeadTag<'a> {
    Input(&'a [u8]),
    Output(&'a mut [u8]),
}

/// One synchronous authenticated-encryption request.
#[derive(Debug)]
pub struct AeadOperation<'a> {
    direction: AeadDirection,
    nonce: &'a [u8],
    associated_data: &'a [u8],
    payload: AeadPayload<'a>,
    tag: AeadTag<'a>,
    status: AeadStatus,
}

impl<'a> AeadOperation<'a> {
    /// Creates an out-of-place seal operation.
    pub fn seal(
        input: AeadInput<'a>,
        nonce: &'a [u8],
        associated_data: &'a [u8],
        output: &'a mut [u8],
        tag: &'a mut [u8],
    ) -> Self {
        Self {
            direction: AeadDirection::Seal,
            nonce,
            associated_data,
            payload: AeadPayload::OutOfPlace { input, output },
            tag: AeadTag::Output(tag),
            status: AeadStatus::Pending,
        }
    }

    /// Creates an out-of-place open operation.
    pub fn open(
        input: AeadInput<'a>,
        nonce: &'a [u8],
        associated_data: &'a [u8],
        tag: &'a [u8],
        output: &'a mut [u8],
    ) -> Self {
        Self {
            direction: AeadDirection::Open,
            nonce,
            associated_data,
            payload: AeadPayload::OutOfPlace { input, output },
            tag: AeadTag::Input(tag),
            status: AeadStatus::Pending,
        }
    }

    /// Creates an in-place seal operation.
    pub fn seal_in_place(
        payload: &'a mut [u8],
        nonce: &'a [u8],
        associated_data: &'a [u8],
        tag: &'a mut [u8],
    ) -> Self {
        Self {
            direction: AeadDirection::Seal,
            nonce,
            associated_data,
            payload: AeadPayload::InPlace(payload),
            tag: AeadTag::Output(tag),
            status: AeadStatus::Pending,
        }
    }

    /// Creates an in-place open operation.
    pub fn open_in_place(
        payload: &'a mut [u8],
        nonce: &'a [u8],
        associated_data: &'a [u8],
        tag: &'a [u8],
    ) -> Self {
        Self {
            direction: AeadDirection::Open,
            nonce,
            associated_data,
            payload: AeadPayload::InPlace(payload),
            tag: AeadTag::Input(tag),
            status: AeadStatus::Pending,
        }
    }

    /// Returns the operation's current completion state.
    pub fn status(&self) -> AeadStatus {
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

bitflags::bitflags! {
    /// Operations permitted by an immutable Key Policy.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct KeyOperations: u16 {
        /// Generate authenticated ciphertext.
        const AEAD_SEAL = 1 << 0;
        /// Authenticate and decrypt ciphertext.
        const AEAD_OPEN = 1 << 1;
        /// Derive new Engine-owned secret material.
        const DERIVE = 1 << 2;
    }
}

/// Immutable permissions attached to one Engine-owned key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyPolicy {
    family: u8,
    algorithm: u32,
    operations: KeyOperations,
    secret_export: bool,
}

impl KeyPolicy {
    /// Creates a policy for one family-typed algorithm.
    pub fn new<F: Family>(
        algorithm: AlgorithmId<F>,
        operations: KeyOperations,
        secret_export: bool,
    ) -> Self {
        Self {
            family: F::KEY_FAMILY,
            algorithm: algorithm.slot,
            operations,
            secret_export,
        }
    }

    fn permits<F: Family>(&self, algorithm: AlgorithmId<F>) -> bool {
        self.family == F::KEY_FAMILY && self.algorithm == algorithm.slot
    }
}

/// A generation-bearing opaque identity for Engine-owned key material.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KeyHandle {
    index: PoolIndex,
}

struct KeyEntry {
    material: Zeroizing<Vec<u8>>,
    policy: KeyPolicy,
    contexts: usize,
}

impl fmt::Debug for KeyEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyEntry")
            .field("material_len", &self.material.len())
            .field("policy", &self.policy)
            .field("contexts", &self.contexts)
            .finish()
    }
}

#[derive(Debug)]
struct ContextKeyRef {
    keys: Rc<RefCell<Pool<KeyEntry>>>,
    key: KeyHandle,
}

/// The owner of cryptographic registries and implementation selection.
pub struct Engine {
    sha256_name: String,
    sha256_dispatch: <Hash as Family>::Dispatch,
    aes128_gcm_name: String,
    aes128_gcm_dispatch: <Aead as Family>::Dispatch,
    keys: Rc<RefCell<Pool<KeyEntry>>>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("key_count", &self.keys.borrow().len())
            .finish_non_exhaustive()
    }
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
        let aes128_gcm_name = validate_standard_algorithm_name("aes-128-gcm")?.to_owned();
        Ok(Self {
            sha256_name,
            sha256_dispatch: execute_sha256,
            aes128_gcm_name,
            aes128_gcm_dispatch: execute_aes128_gcm,
            keys: Rc::new(RefCell::new(Pool::with_capacity(1024))),
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
        let (dispatch, prepared) =
            F::unkeyed_context(self, algorithm).ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;
        Ok(Context {
            algorithm,
            dispatch,
            prepared,
            key_ref: None,
            thread_bound: PhantomData,
        })
    }

    /// Creates a generation-bearing opaque key from caller-provided material.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError::PoolFull`] when the fixed-capacity key pool is
    /// exhausted.
    pub fn create_key(&self, material: &[u8], policy: KeyPolicy) -> Result<KeyHandle, KeyError> {
        let mut keys = self.keys.borrow_mut();
        let capacity = keys.capacity();
        let index = keys
            .insert(KeyEntry {
                material: Zeroizing::new(material.to_vec()),
                policy,
                contexts: 0,
            })
            .ok_or(KeyError::PoolFull { capacity })?;
        Ok(KeyHandle { index })
    }

    /// Destroys a key that is no longer referenced by any Context.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError::StaleKey`] for an invalid generation and
    /// [`KeyError::KeyInUse`] while live Contexts retain the key.
    pub fn destroy_key(&self, key: KeyHandle) -> Result<(), KeyError> {
        let mut keys = self.keys.borrow_mut();
        let contexts = keys
            .get(key.index)
            .ok_or(KeyError::StaleKey { key })?
            .contexts;
        if contexts != 0 {
            return Err(KeyError::KeyInUse { key, contexts });
        }
        let removed = keys.remove(key.index);
        assert!(
            removed.is_some(),
            "validated key must remain present until removal"
        );
        Ok(())
    }

    /// Explicitly exports secret material when permitted by Key Policy.
    ///
    /// # Errors
    ///
    /// Returns a concrete stale-key, policy-denial, or output-capacity error.
    pub fn export_secret(&self, key: KeyHandle, output: &mut [u8]) -> Result<usize, KeyError> {
        let keys = self.keys.borrow();
        let entry = keys.get(key.index).ok_or(KeyError::StaleKey { key })?;
        if !entry.policy.secret_export {
            return Err(KeyError::SecretExportDenied { key });
        }
        if output.len() < entry.material.len() {
            return Err(KeyError::OutputTooSmall {
                key,
                required: entry.material.len(),
                provided: output.len(),
            });
        }
        output[..entry.material.len()].copy_from_slice(&entry.material);
        Ok(entry.material.len())
    }

    /// Creates a thread-bound AEAD Context using an opaque key.
    ///
    /// # Errors
    ///
    /// Returns a concrete stale-key, policy, availability, or key-preparation
    /// failure without changing key lifecycle state.
    pub fn context_with_key(
        &self,
        algorithm: AlgorithmId<Aead>,
        key: KeyHandle,
    ) -> Result<Context<Aead>, ContextError> {
        if algorithm.slot != 0 {
            return Err(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            });
        }

        let mut keys = self.keys.borrow_mut();
        let entry = keys
            .get_mut(key.index)
            .ok_or(ContextError::StaleKey { key })?;
        if !entry.policy.permits(algorithm) {
            return Err(ContextError::AlgorithmDenied {
                key,
                algorithm: algorithm.slot,
            });
        }
        let required = KeyOperations::AEAD_SEAL | KeyOperations::AEAD_OPEN;
        if !entry.policy.operations.intersects(required) {
            return Err(ContextError::OperationsDenied { key, required });
        }
        let cipher = hammer_infra::crypto::Aes128Gcm::new(&entry.material).map_err(|source| {
            ContextError::InvalidKeyLength {
                key,
                required: 16,
                provided: entry.material.len(),
                source,
            }
        })?;
        let prepared = AeadPrepared {
            cipher,
            operations: entry.policy.operations,
        };
        entry.contexts = entry
            .contexts
            .checked_add(1)
            .expect("key context reference count overflow");
        drop(keys);

        Ok(Context {
            algorithm,
            dispatch: self.aes128_gcm_dispatch,
            prepared,
            key_ref: Some(ContextKeyRef {
                keys: Rc::clone(&self.keys),
                key,
            }),
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
    prepared: F::Prepared,
    key_ref: Option<ContextKeyRef>,
    // Rc is deliberately part of the marker: Context must be neither Send nor Sync.
    thread_bound: PhantomData<Rc<()>>,
}

impl Context<Hash> {
    /// Executes all operations synchronously through one batch dispatch.
    pub fn execute(&mut self, batch: &mut Batch<'_, '_, Hash>) {
        (self.dispatch)(&mut self.prepared, batch.operations);
    }

    /// Returns the algorithm permanently bound to this Context.
    pub fn algorithm(&self) -> AlgorithmId<Hash> {
        self.algorithm
    }
}

impl Context<Aead> {
    /// Executes all operations synchronously through one batch dispatch.
    pub fn execute(&mut self, batch: &mut Batch<'_, '_, Aead>) {
        (self.dispatch)(&mut self.prepared, batch.operations);
    }

    /// Returns the algorithm permanently bound to this Context.
    pub fn algorithm(&self) -> AlgorithmId<Aead> {
        self.algorithm
    }
}

impl<F: Family> Drop for Context<F> {
    fn drop(&mut self) {
        let Some(key_ref) = &self.key_ref else {
            return;
        };
        let mut keys = key_ref.keys.borrow_mut();
        let entry = keys
            .get_mut(key_ref.key.index)
            .expect("live Context must retain a live key generation");
        entry.contexts = entry
            .contexts
            .checked_sub(1)
            .expect("key Context reference count must be positive");
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
    /// The Key Handle generation is absent or has already been destroyed.
    #[error("key {key:?} is stale")]
    StaleKey {
        /// Rejected opaque key identity.
        key: KeyHandle,
    },
    /// The immutable Key Policy does not permit this algorithm.
    #[error("key {key:?} does not permit algorithm slot {algorithm}")]
    AlgorithmDenied {
        /// Rejected opaque key identity.
        key: KeyHandle,
        /// Process-local algorithm slot.
        algorithm: u32,
    },
    /// The immutable Key Policy permits none of the operations needed by the Context.
    #[error("key {key:?} does not permit any required operation {required:?}")]
    OperationsDenied {
        /// Rejected opaque key identity.
        key: KeyHandle,
        /// Operations of which at least one must be permitted.
        required: KeyOperations,
    },
    /// Key material has the wrong length for the selected algorithm.
    #[error("key {key:?} has {provided} bytes but the selected algorithm requires {required}")]
    InvalidKeyLength {
        /// Rejected opaque key identity.
        key: KeyHandle,
        /// Key size required by the algorithm.
        required: usize,
        /// Key size held by the Engine.
        provided: usize,
        /// Portable algorithm preparation failure.
        #[source]
        source: hammer_infra::crypto::AeadError,
    },
}

/// A failure owned by the Engine's opaque key lifecycle.
#[derive(Clone, Copy, Debug, thiserror::Error, Eq, PartialEq)]
pub enum KeyError {
    /// The fixed-capacity key authority has no free slot.
    #[error("key pool capacity {capacity} is exhausted")]
    PoolFull {
        /// Configured fixed capacity.
        capacity: usize,
    },
    /// The Key Handle generation is absent or has already been destroyed.
    #[error("key {key:?} is stale")]
    StaleKey {
        /// Rejected opaque key identity.
        key: KeyHandle,
    },
    /// Live Contexts still retain the key.
    #[error("key {key:?} is retained by {contexts} Contexts")]
    KeyInUse {
        /// Retained opaque key identity.
        key: KeyHandle,
        /// Number of live Context references.
        contexts: usize,
    },
    /// The immutable Key Policy denies Secret Export.
    #[error("key {key:?} denies Secret Export")]
    SecretExportDenied {
        /// Non-exportable opaque key identity.
        key: KeyHandle,
    },
    /// Caller output cannot hold the complete exported secret.
    #[error(
        "Secret Export for key {key:?} requires {required} bytes but caller provided {provided}"
    )]
    OutputTooSmall {
        /// Opaque key identity being exported.
        key: KeyHandle,
        /// Secret size held by the Engine.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
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

fn execute_sha256(_: &mut HashPrepared, operations: &mut [HashOperation<'_>]) {
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

fn execute_aes128_gcm(prepared: &mut AeadPrepared, operations: &mut [AeadOperation<'_>]) {
    for operation in operations {
        let required = match operation.direction {
            AeadDirection::Seal => KeyOperations::AEAD_SEAL,
            AeadDirection::Open => KeyOperations::AEAD_OPEN,
        };
        if !prepared.operations.contains(required) {
            operation.status = AeadStatus::PolicyDenied {
                operation: operation.direction,
            };
            continue;
        }

        let result = match (
            operation.direction,
            &mut operation.payload,
            &mut operation.tag,
        ) {
            (
                AeadDirection::Seal,
                AeadPayload::OutOfPlace { input, output },
                AeadTag::Output(tag),
            ) => (*input).with_fragments(|fragments| {
                prepared.cipher.seal(
                    fragments,
                    operation.nonce,
                    operation.associated_data,
                    output,
                    tag,
                )
            }),
            (
                AeadDirection::Open,
                AeadPayload::OutOfPlace { input, output },
                AeadTag::Input(tag),
            ) => (*input).with_fragments(|fragments| {
                prepared.cipher.open(
                    fragments,
                    operation.nonce,
                    operation.associated_data,
                    tag,
                    output,
                )
            }),
            (AeadDirection::Seal, AeadPayload::InPlace(payload), AeadTag::Output(tag)) => prepared
                .cipher
                .seal_in_place(payload, operation.nonce, operation.associated_data, tag),
            (AeadDirection::Open, AeadPayload::InPlace(payload), AeadTag::Input(tag)) => prepared
                .cipher
                .open_in_place(payload, operation.nonce, operation.associated_data, tag),
            _ => unreachable!("AEAD constructors preserve direction, payload, and tag shape"),
        };
        operation.status = aead_status(result);
    }
}

fn aead_status(result: Result<usize, hammer_infra::crypto::AeadError>) -> AeadStatus {
    match result {
        Ok(written) => AeadStatus::Complete { written },
        Err(hammer_infra::crypto::AeadError::OutputTooSmall { required, provided }) => {
            AeadStatus::OutputTooSmall { required, provided }
        }
        Err(hammer_infra::crypto::AeadError::InvalidNonceLength { required, provided }) => {
            AeadStatus::InvalidNonceLength { required, provided }
        }
        Err(hammer_infra::crypto::AeadError::InvalidTagLength { required, provided }) => {
            AeadStatus::InvalidTagLength { required, provided }
        }
        Err(hammer_infra::crypto::AeadError::AuthenticationFailed) => {
            AeadStatus::AuthenticationFailed
        }
        Err(
            hammer_infra::crypto::AeadError::InputLengthOverflow
            | hammer_infra::crypto::AeadError::InputTooLong,
        ) => AeadStatus::InputTooLong,
        Err(hammer_infra::crypto::AeadError::InvalidKeyLength { .. }) => {
            unreachable!("AEAD key length is validated while preparing the Context")
        }
    }
}
