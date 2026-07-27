//! Synchronous typed cryptographic execution.
//!
//! `hammer-service` owns algorithm identity, implementation selection, and
//! operation lifecycle. Portable algorithm semantics remain in
//! `hammer-infra`; `hammer-runtime` does not participate in this boundary.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
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
    fn registry(engine: &Engine) -> &FamilyRegistry<Self>;

    #[doc(hidden)]
    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self>;

    #[doc(hidden)]
    fn prepare_unkeyed() -> Option<Self::Prepared>;
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

    fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
        &engine.hashes
    }

    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
        &mut engine.hashes
    }

    fn prepare_unkeyed() -> Option<Self::Prepared> {
        Some(HashPrepared)
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

    fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
        &engine.aeads
    }

    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
        &mut engine.aeads
    }

    fn prepare_unkeyed() -> Option<Self::Prepared> {
        None
    }
}

macro_rules! define_pending_family {
    ($family:ident, $field:ident, $key:expr) => {
        #[doc = concat!("The ", stringify!($family), " operation family.")]
        #[derive(Debug)]
        pub struct $family;

        impl private::Sealed for $family {}

        impl Family for $family {
            type Operation<'a> = ();
            type Prepared = ();
            type Dispatch = for<'a> fn(&mut (), &mut [()]);

            const KEY_FAMILY: u8 = $key;

            fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
                &engine.$field
            }

            fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
                &mut engine.$field
            }

            fn prepare_unkeyed() -> Option<Self::Prepared> {
                None
            }
        }
    };
}

define_pending_family!(Cipher, ciphers, 3);
define_pending_family!(Mac, macs, 4);
define_pending_family!(Kdf, kdfs, 5);
define_pending_family!(Kx, key_exchanges, 6);
define_pending_family!(Sign, signers, 7);
define_pending_family!(Verify, verifiers, 8);

bitflags::bitflags! {
    /// Input and output shapes supported by a cryptographic implementation.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct Capabilities: u16 {
        /// Accept one contiguous input slice.
        const CONTIGUOUS_INPUT = 1 << 0;
        /// Accept ordered scatter-gather input.
        const SCATTER_INPUT = 1 << 1;
        /// Transform caller-owned memory in place.
        const IN_PLACE = 1 << 2;
        /// Write results to separate caller-owned memory.
        const OUT_OF_PLACE = 1 << 3;
        /// Authenticate caller-provided associated data.
        const ASSOCIATED_DATA = 1 << 4;
    }
}

/// One algorithm declaration awaiting failure-atomic publication.
pub struct AlgorithmRegistration<F: Family> {
    name: String,
    required: Capabilities,
    family: PhantomData<fn() -> F>,
}

impl<F: Family> AlgorithmRegistration<F> {
    /// Declares canonical algorithm semantics and required operation shapes.
    pub fn new(name: impl Into<String>, required: Capabilities) -> Self {
        Self {
            name: name.into(),
            required,
            family: PhantomData,
        }
    }
}

/// One implementation declaration awaiting failure-atomic publication.
pub struct ImplementationRegistration<F: Family> {
    name: String,
    algorithms: Vec<String>,
    capabilities: Capabilities,
    priority: i32,
    available: bool,
    dispatch: F::Dispatch,
}

impl<F: Family> ImplementationRegistration<F> {
    /// Declares one implementation and the algorithms it can execute.
    pub fn new(
        name: impl Into<String>,
        algorithms: &[&str],
        capabilities: Capabilities,
        priority: i32,
        available: bool,
        dispatch: F::Dispatch,
    ) -> Self {
        Self {
            name: name.into(),
            algorithms: algorithms.iter().map(|name| (*name).to_owned()).collect(),
            capabilities,
            priority,
            available,
            dispatch,
        }
    }
}

/// A family-typed algorithm and implementation publication bundle.
pub struct Registration<F: Family> {
    algorithms: Vec<AlgorithmRegistration<F>>,
    implementations: Vec<ImplementationRegistration<F>>,
}

impl<F: Family> Registration<F> {
    /// Creates an empty publication bundle.
    pub fn new() -> Self {
        Self {
            algorithms: Vec::new(),
            implementations: Vec::new(),
        }
    }

    /// Adds one algorithm declaration to the bundle.
    pub fn with_algorithm(mut self, algorithm: AlgorithmRegistration<F>) -> Self {
        self.algorithms.push(algorithm);
        self
    }

    /// Adds one implementation declaration to the bundle.
    pub fn with_implementation(mut self, implementation: ImplementationRegistration<F>) -> Self {
        self.implementations.push(implementation);
        self
    }
}

impl<F: Family> Default for Registration<F> {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable implementation admission policy used for new Contexts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SelectionPolicy {
    allowed: Option<BTreeSet<String>>,
}

impl SelectionPolicy {
    /// Allows every registered implementation.
    pub fn allow_all() -> Self {
        Self::default()
    }

    /// Allows only the named implementations.
    pub fn only<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            allowed: Some(names.into_iter().map(Into::into).collect()),
        }
    }

    fn permits(&self, name: &str) -> bool {
        self.allowed
            .as_ref()
            .is_none_or(|allowed| allowed.contains(name))
    }
}

#[derive(Debug)]
struct AlgorithmRecord {
    name: String,
    required: Capabilities,
}

struct ImplementationRecord<F: Family> {
    name: String,
    algorithms: Vec<u32>,
    capabilities: Capabilities,
    priority: i32,
    available: bool,
    dispatch: F::Dispatch,
}

/// Family-private registry storage exposed only to the sealed [`Family`] contract.
#[doc(hidden)]
pub struct FamilyRegistry<F: Family> {
    algorithms: Vec<AlgorithmRecord>,
    algorithm_names: HashMap<String, u32>,
    implementations: Vec<ImplementationRecord<F>>,
    implementation_names: HashMap<String, usize>,
}

impl<F: Family> FamilyRegistry<F> {
    fn new() -> Self {
        Self {
            algorithms: Vec::new(),
            algorithm_names: HashMap::new(),
            implementations: Vec::new(),
            implementation_names: HashMap::new(),
        }
    }

    fn algorithm(&self, name: &str) -> Option<AlgorithmId<F>> {
        self.algorithm_names
            .get(name)
            .copied()
            .map(AlgorithmId::new)
    }

    fn algorithm_record(&self, algorithm: AlgorithmId<F>) -> Option<&AlgorithmRecord> {
        self.algorithms.get(algorithm.slot as usize)
    }

    fn select(
        &self,
        algorithm: AlgorithmId<F>,
        policy: &SelectionPolicy,
    ) -> Option<(&str, F::Dispatch)> {
        let required = self.algorithm_record(algorithm)?.required;
        self.implementations
            .iter()
            .filter(|implementation| {
                implementation.available
                    && policy.permits(&implementation.name)
                    && implementation.algorithms.contains(&algorithm.slot)
                    && implementation.capabilities.contains(required)
            })
            .min_by(|left, right| {
                right
                    .priority
                    .cmp(&left.priority)
                    .then_with(|| left.name.cmp(&right.name))
            })
            .map(|implementation| (implementation.name.as_str(), implementation.dispatch))
    }

    fn set_availability(&mut self, name: &str, available: bool) -> Result<(), RegistryError> {
        let index = self
            .implementation_names
            .get(name)
            .copied()
            .ok_or_else(|| RegistryError::ImplementationUnknown {
                name: name.to_owned(),
            })?;
        self.implementations[index].available = available;
        Ok(())
    }

    fn publish(&mut self, registration: Registration<F>) {
        let first_slot = self.algorithms.len();
        for (offset, algorithm) in registration.algorithms.into_iter().enumerate() {
            let slot = u32::try_from(first_slot + offset)
                .expect("registry capacity was validated before publication");
            self.algorithm_names.insert(algorithm.name.clone(), slot);
            self.algorithms.push(AlgorithmRecord {
                name: algorithm.name,
                required: algorithm.required,
            });
        }

        for implementation in registration.implementations {
            let algorithms = implementation
                .algorithms
                .iter()
                .map(|name| {
                    *self
                        .algorithm_names
                        .get(name)
                        .expect("implementation algorithms were validated before publication")
                })
                .collect();
            let index = self.implementations.len();
            self.implementation_names
                .insert(implementation.name.clone(), index);
            self.implementations.push(ImplementationRecord {
                name: implementation.name,
                algorithms,
                capabilities: implementation.capabilities,
                priority: implementation.priority,
                available: implementation.available,
                dispatch: implementation.dispatch,
            });
        }
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
    aeads: FamilyRegistry<Aead>,
    ciphers: FamilyRegistry<Cipher>,
    hashes: FamilyRegistry<Hash>,
    macs: FamilyRegistry<Mac>,
    kdfs: FamilyRegistry<Kdf>,
    key_exchanges: FamilyRegistry<Kx>,
    signers: FamilyRegistry<Sign>,
    verifiers: FamilyRegistry<Verify>,
    selection_policy: SelectionPolicy,
    keys: Rc<RefCell<Pool<KeyEntry>>>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("aead_algorithms", &self.aeads.algorithms.len())
            .field("hash_algorithms", &self.hashes.algorithms.len())
            .field("key_count", &self.keys.borrow().len())
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Creates an Engine with empty family registries.
    pub fn new() -> Self {
        Self {
            aeads: FamilyRegistry::new(),
            ciphers: FamilyRegistry::new(),
            hashes: FamilyRegistry::new(),
            macs: FamilyRegistry::new(),
            kdfs: FamilyRegistry::new(),
            key_exchanges: FamilyRegistry::new(),
            signers: FamilyRegistry::new(),
            verifiers: FamilyRegistry::new(),
            selection_policy: SelectionPolicy::allow_all(),
            keys: Rc::new(RefCell::new(Pool::with_capacity(1024))),
        }
    }

    /// Creates an Engine containing Hammer's standard built-in algorithms.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::MalformedAlgorithmName`] if a built-in name no
    /// longer satisfies the canonical algorithm-name contract.
    pub fn with_builtins() -> Result<Self, RegistryError> {
        let hash_capabilities = Capabilities::CONTIGUOUS_INPUT
            | Capabilities::SCATTER_INPUT
            | Capabilities::OUT_OF_PLACE;
        let aead_capabilities =
            hash_capabilities | Capabilities::IN_PLACE | Capabilities::ASSOCIATED_DATA;
        let mut engine = Self::new();
        engine.publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "sha-256",
                    hash_capabilities,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "hammer:sha-256-portable",
                    &["sha-256"],
                    hash_capabilities,
                    0,
                    true,
                    execute_sha256,
                )),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Aead>::new(
                    "aes-128-gcm",
                    aead_capabilities,
                ))
                .with_implementation(ImplementationRegistration::<Aead>::new(
                    "hammer:aes-128-gcm-portable",
                    &["aes-128-gcm"],
                    aead_capabilities,
                    0,
                    true,
                    execute_aes128_gcm,
                )),
        )?;
        Ok(engine)
    }

    /// Publishes one family-typed registration as a failure-atomic bundle.
    ///
    /// # Errors
    ///
    /// Returns a concrete naming, collision, capability, or capacity failure
    /// without changing any registry.
    pub fn publish<F: Family>(
        &mut self,
        registration: Registration<F>,
    ) -> Result<(), RegistryError> {
        validate_registration(self, &registration)?;
        F::registry_mut(self).publish(registration);
        Ok(())
    }

    /// Resolves a canonical name to a family-typed process-local identity.
    pub fn algorithm<F: Family>(&self, name: &str) -> Option<AlgorithmId<F>> {
        F::registry(self).algorithm(name)
    }

    /// Replaces the immutable policy used for subsequent Context creation.
    pub fn set_selection_policy(&mut self, policy: SelectionPolicy) {
        self.selection_policy = policy;
    }

    /// Changes whether one implementation may be selected by new Contexts.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::ImplementationUnknown`] when `name` is absent
    /// from this operation family.
    pub fn set_implementation_availability<F: Family>(
        &mut self,
        name: &str,
        available: bool,
    ) -> Result<(), RegistryError> {
        F::registry_mut(self).set_availability(name, available)
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
        let (implementation, dispatch) = F::registry(self)
            .select(algorithm, &self.selection_policy)
            .ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;
        let prepared = F::prepare_unkeyed().ok_or(ContextError::KeyRequired {
            algorithm: algorithm.slot,
        })?;
        Ok(Context {
            algorithm,
            implementation: implementation.to_owned(),
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
        let registry = Aead::registry(self);
        let algorithm_record =
            registry
                .algorithm_record(algorithm)
                .ok_or(ContextError::AlgorithmUnavailable {
                    algorithm: algorithm.slot,
                })?;
        let (implementation, dispatch) = registry.select(algorithm, &self.selection_policy).ok_or(
            ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            },
        )?;
        if algorithm_record.name != "aes-128-gcm" {
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
            implementation: implementation.to_owned(),
            dispatch,
            prepared,
            key_ref: Some(ContextKeyRef {
                keys: Rc::clone(&self.keys),
                key,
            }),
            thread_bound: PhantomData,
        })
    }

    fn algorithm_name_exists(&self, name: &str) -> bool {
        self.aeads.algorithm_names.contains_key(name)
            || self.ciphers.algorithm_names.contains_key(name)
            || self.hashes.algorithm_names.contains_key(name)
            || self.macs.algorithm_names.contains_key(name)
            || self.kdfs.algorithm_names.contains_key(name)
            || self.key_exchanges.algorithm_names.contains_key(name)
            || self.signers.algorithm_names.contains_key(name)
            || self.verifiers.algorithm_names.contains_key(name)
    }

    fn implementation_name_exists(&self, name: &str) -> bool {
        self.aeads.implementation_names.contains_key(name)
            || self.ciphers.implementation_names.contains_key(name)
            || self.hashes.implementation_names.contains_key(name)
            || self.macs.implementation_names.contains_key(name)
            || self.kdfs.implementation_names.contains_key(name)
            || self.key_exchanges.implementation_names.contains_key(name)
            || self.signers.implementation_names.contains_key(name)
            || self.verifiers.implementation_names.contains_key(name)
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
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
    implementation: String,
    dispatch: F::Dispatch,
    prepared: F::Prepared,
    key_ref: Option<ContextKeyRef>,
    // Rc is deliberately part of the marker: Context must be neither Send nor Sync.
    thread_bound: PhantomData<Rc<()>>,
}

impl<F: Family> Context<F> {
    /// Returns the implementation permanently bound to this Context.
    pub fn implementation_name(&self) -> &str {
        &self.implementation
    }
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

/// A failure while publishing a cryptographic registry.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum RegistryError {
    /// An algorithm violated the canonical naming contract.
    #[error("malformed algorithm name `{name}`")]
    MalformedAlgorithmName {
        /// Rejected name.
        name: String,
    },
    /// An implementation name omitted or violated its namespace contract.
    #[error("malformed implementation name `{name}`")]
    MalformedImplementationName {
        /// Rejected name.
        name: String,
    },
    /// An Algorithm Name is already published or repeated in one bundle.
    #[error("algorithm `{name}` is already registered")]
    AlgorithmCollision {
        /// Colliding canonical name.
        name: String,
    },
    /// A Crypto Implementation Name is already published or repeated.
    #[error("implementation `{name}` is already registered")]
    ImplementationCollision {
        /// Colliding canonical name.
        name: String,
    },
    /// An implementation advertises an algorithm absent from the bundle and registry.
    #[error("implementation `{implementation}` references unknown algorithm `{algorithm}`")]
    UnknownAlgorithm {
        /// Rejected implementation.
        implementation: String,
        /// Missing Algorithm Name.
        algorithm: String,
    },
    /// An implementation does not support every shape required by an algorithm.
    #[error(
        "implementation `{implementation}` capabilities {provided:?} do not satisfy algorithm `{algorithm}` requirements {required:?}"
    )]
    CapabilityMismatch {
        /// Rejected implementation.
        implementation: String,
        /// Algorithm whose contract was not satisfied.
        algorithm: String,
        /// Shapes required by the algorithm.
        required: Capabilities,
        /// Shapes advertised by the implementation.
        provided: Capabilities,
    },
    /// An implementation declared no executable algorithm.
    #[error("implementation `{name}` declares no algorithms")]
    ImplementationWithoutAlgorithms {
        /// Rejected implementation.
        name: String,
    },
    /// The named implementation is absent from the selected family.
    #[error("implementation `{name}` is not registered")]
    ImplementationUnknown {
        /// Missing implementation name.
        name: String,
    },
    /// The process-local Algorithm ID space cannot represent another bundle.
    #[error("algorithm registry capacity is exhausted")]
    AlgorithmCapacityExhausted,
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
    /// The selected operation family requires an opaque key.
    #[error("algorithm slot {algorithm} requires a Key Handle")]
    KeyRequired {
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

fn validate_registration<F: Family>(
    engine: &Engine,
    registration: &Registration<F>,
) -> Result<(), RegistryError> {
    let registry = F::registry(engine);
    if registry
        .algorithms
        .len()
        .checked_add(registration.algorithms.len())
        .is_none_or(|len| u32::try_from(len.saturating_sub(1)).is_err())
    {
        return Err(RegistryError::AlgorithmCapacityExhausted);
    }

    let mut algorithms = HashMap::new();
    for algorithm in &registration.algorithms {
        validate_algorithm_name(&algorithm.name)?;
        if engine.algorithm_name_exists(&algorithm.name)
            || algorithms
                .insert(algorithm.name.as_str(), algorithm.required)
                .is_some()
        {
            return Err(RegistryError::AlgorithmCollision {
                name: algorithm.name.clone(),
            });
        }
    }

    let mut implementations = BTreeSet::new();
    for implementation in &registration.implementations {
        validate_implementation_name(&implementation.name)?;
        if engine.implementation_name_exists(&implementation.name)
            || !implementations.insert(implementation.name.as_str())
        {
            return Err(RegistryError::ImplementationCollision {
                name: implementation.name.clone(),
            });
        }
        if implementation.algorithms.is_empty() {
            return Err(RegistryError::ImplementationWithoutAlgorithms {
                name: implementation.name.clone(),
            });
        }
        for algorithm_name in &implementation.algorithms {
            let required = algorithms
                .get(algorithm_name.as_str())
                .copied()
                .or_else(|| {
                    registry
                        .algorithm(algorithm_name)
                        .and_then(|algorithm| registry.algorithm_record(algorithm))
                        .map(|algorithm| algorithm.required)
                });
            let required = required.ok_or_else(|| RegistryError::UnknownAlgorithm {
                implementation: implementation.name.clone(),
                algorithm: algorithm_name.clone(),
            })?;
            if !implementation.capabilities.contains(required) {
                return Err(RegistryError::CapabilityMismatch {
                    implementation: implementation.name.clone(),
                    algorithm: algorithm_name.clone(),
                    required,
                    provided: implementation.capabilities,
                });
            }
        }
    }
    Ok(())
}

fn validate_algorithm_name(name: &str) -> Result<(), RegistryError> {
    let mut components = name.split(':');
    let first = components.next().unwrap_or_default();
    let second = components.next();
    let valid = valid_name_component(first)
        && second.is_none_or(valid_name_component)
        && components.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(RegistryError::MalformedAlgorithmName {
            name: name.to_owned(),
        })
    }
}

fn validate_implementation_name(name: &str) -> Result<(), RegistryError> {
    let mut components = name.split(':');
    let valid = components.next().is_some_and(valid_name_component)
        && components.next().is_some_and(valid_name_component)
        && components.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(RegistryError::MalformedImplementationName {
            name: name.to_owned(),
        })
    }
}

fn valid_name_component(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
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
