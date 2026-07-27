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
    /// One implementation-specific Context preparation entry point.
    type Prepare: Copy;
    /// One batch-level implementation entry point.
    type Dispatch: Copy;

    #[doc(hidden)]
    const KEY_FAMILY: u8;

    #[doc(hidden)]
    fn registry(engine: &Engine) -> &FamilyRegistry<Self>;

    #[doc(hidden)]
    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self>;

    #[doc(hidden)]
    fn prepare_unkeyed(prepare: Self::Prepare) -> Option<Self::Prepared>;

    #[doc(hidden)]
    fn key_operations() -> KeyOperations {
        KeyOperations::empty()
    }

    #[doc(hidden)]
    fn prepare_keyed(
        _: Self::Prepare,
        _: &Engine,
        algorithm: AlgorithmId<Self>,
        _: KeyHandle,
        _: &[u8],
        _: &KeyPolicy,
    ) -> Result<Self::Prepared, ContextError> {
        Err(ContextError::KeyUnsupported {
            algorithm: algorithm.slot,
        })
    }
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
    type Prepared = ();
    type Prepare = ();
    type Dispatch = for<'a> fn(&mut (), &mut [HashOperation<'a>]);

    const KEY_FAMILY: u8 = 2;

    fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
        &engine.hashes
    }

    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
        &mut engine.hashes
    }

    fn prepare_unkeyed(_: Self::Prepare) -> Option<Self::Prepared> {
        Some(())
    }
}

/// The authenticated-encryption operation family.
#[derive(Debug)]
pub struct Aead;

/// Prepared state for an authenticated-encryption Context.
#[derive(Debug)]
pub struct AeadPrepared {
    cipher: hammer_infra::crypto::AeadCipher,
    operations: KeyOperations,
}

impl AeadPrepared {
    fn new(
        algorithm: hammer_infra::crypto::AeadAlgorithm,
        key: KeyHandle,
        material: &[u8],
        operations: KeyOperations,
    ) -> Result<Self, ContextError> {
        let cipher =
            hammer_infra::crypto::AeadCipher::new(algorithm, material).map_err(|source| {
                let hammer_infra::crypto::AeadError::InvalidKeyLength { required, provided } =
                    source
                else {
                    unreachable!("AEAD Context preparation only validates key length")
                };
                ContextError::InvalidKeyLength {
                    key,
                    required,
                    provided,
                    source,
                }
            })?;
        Ok(Self { cipher, operations })
    }
}

impl private::Sealed for Aead {}

impl Family for Aead {
    type Operation<'a> = AeadOperation<'a>;
    type Prepared = AeadPrepared;
    type Prepare = fn(KeyHandle, &[u8], KeyOperations) -> Result<AeadPrepared, ContextError>;
    type Dispatch = for<'a> fn(&mut AeadPrepared, &mut [AeadOperation<'a>]);

    const KEY_FAMILY: u8 = 1;

    fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
        &engine.aeads
    }

    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
        &mut engine.aeads
    }

    fn prepare_unkeyed(_: Self::Prepare) -> Option<Self::Prepared> {
        None
    }

    fn key_operations() -> KeyOperations {
        KeyOperations::AEAD_SEAL | KeyOperations::AEAD_OPEN
    }

    fn prepare_keyed(
        prepare: Self::Prepare,
        _: &Engine,
        _: AlgorithmId<Self>,
        key: KeyHandle,
        material: &[u8],
        policy: &KeyPolicy,
    ) -> Result<Self::Prepared, ContextError> {
        prepare(key, material, policy.operations)
    }
}

/// The message-authentication operation family.
#[derive(Debug)]
pub struct Mac;

/// Prepared state for a message-authentication Context.
#[derive(Debug)]
pub struct MacPrepared {
    mac: hammer_infra::crypto::Hmac,
}

impl MacPrepared {
    fn new(algorithm: hammer_infra::crypto::Sha2Algorithm, material: &[u8]) -> Self {
        Self {
            mac: hammer_infra::crypto::Hmac::new(algorithm, material),
        }
    }
}

impl private::Sealed for Mac {}

impl Family for Mac {
    type Operation<'a> = MacOperation<'a>;
    type Prepared = MacPrepared;
    type Prepare = fn(&[u8]) -> MacPrepared;
    type Dispatch = for<'a> fn(&mut MacPrepared, &mut [MacOperation<'a>]);

    const KEY_FAMILY: u8 = 4;

    fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
        &engine.macs
    }

    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
        &mut engine.macs
    }

    fn prepare_unkeyed(_: Self::Prepare) -> Option<Self::Prepared> {
        None
    }

    fn key_operations() -> KeyOperations {
        KeyOperations::MAC_AUTHENTICATE
    }

    fn prepare_keyed(
        prepare: Self::Prepare,
        _: &Engine,
        _: AlgorithmId<Self>,
        _: KeyHandle,
        material: &[u8],
        _: &KeyPolicy,
    ) -> Result<Self::Prepared, ContextError> {
        Ok(prepare(material))
    }
}

/// The key-derivation operation family.
#[derive(Debug)]
pub struct Kdf;

/// Prepared state for a key-derivation Context.
#[derive(Debug)]
pub struct KdfPrepared {
    algorithm: hammer_infra::crypto::Sha2Algorithm,
    material: Zeroizing<Vec<u8>>,
    policy: KeyPolicy,
    keys: Rc<RefCell<Pool<KeyEntry>>>,
}

impl KdfPrepared {
    fn new(
        algorithm: hammer_infra::crypto::Sha2Algorithm,
        material: &[u8],
        policy: &KeyPolicy,
        keys: Rc<RefCell<Pool<KeyEntry>>>,
    ) -> Self {
        Self {
            algorithm,
            material: Zeroizing::new(material.to_vec()),
            policy: policy.clone(),
            keys,
        }
    }
}

impl private::Sealed for Kdf {}

impl Family for Kdf {
    type Operation<'a> = KdfOperation<'a>;
    type Prepared = KdfPrepared;
    type Prepare = hammer_infra::crypto::Sha2Algorithm;
    type Dispatch = for<'a> fn(&mut KdfPrepared, &mut [KdfOperation<'a>]);

    const KEY_FAMILY: u8 = 5;

    fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
        &engine.kdfs
    }

    fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
        &mut engine.kdfs
    }

    fn prepare_unkeyed(_: Self::Prepare) -> Option<Self::Prepared> {
        None
    }

    fn key_operations() -> KeyOperations {
        KeyOperations::DERIVE
    }

    fn prepare_keyed(
        algorithm: Self::Prepare,
        engine: &Engine,
        _: AlgorithmId<Self>,
        _: KeyHandle,
        material: &[u8],
        policy: &KeyPolicy,
    ) -> Result<Self::Prepared, ContextError> {
        Ok(KdfPrepared::new(
            algorithm,
            material,
            policy,
            Rc::clone(&engine.keys),
        ))
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
            type Prepare = fn() -> ();
            type Dispatch = for<'a> fn(&mut (), &mut [()]);

            const KEY_FAMILY: u8 = $key;

            fn registry(engine: &Engine) -> &FamilyRegistry<Self> {
                &engine.$field
            }

            fn registry_mut(engine: &mut Engine) -> &mut FamilyRegistry<Self> {
                &mut engine.$field
            }

            fn prepare_unkeyed(_: Self::Prepare) -> Option<Self::Prepared> {
                None
            }
        }
    };
}

define_pending_family!(Cipher, ciphers, 3);
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
    algorithms: Vec<AlgorithmImplementationRegistration<F>>,
    priority: i32,
    available: bool,
}

struct AlgorithmImplementationRegistration<F: Family> {
    name: String,
    capabilities: Capabilities,
    prepare: F::Prepare,
    dispatch: F::Dispatch,
}

impl<F: Family> ImplementationRegistration<F> {
    /// Declares one implementation before adding its algorithm function tables.
    pub fn new(name: impl Into<String>, priority: i32, available: bool) -> Self {
        Self {
            name: name.into(),
            algorithms: Vec::new(),
            priority,
            available,
        }
    }

    /// Adds the direct Context preparation and batch dispatch table for one algorithm.
    pub fn with_algorithm(
        mut self,
        name: impl Into<String>,
        capabilities: Capabilities,
        prepare: F::Prepare,
        dispatch: F::Dispatch,
    ) -> Self {
        self.algorithms.push(AlgorithmImplementationRegistration {
            name: name.into(),
            capabilities,
            prepare,
            dispatch,
        });
        self
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
    required: Capabilities,
}

struct ImplementationRecord<F: Family> {
    name: String,
    algorithms: Vec<AlgorithmImplementation<F>>,
    priority: i32,
    available: bool,
}

struct AlgorithmImplementation<F: Family> {
    algorithm: u32,
    capabilities: Capabilities,
    prepare: F::Prepare,
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
    ) -> Option<(&str, F::Prepare, F::Dispatch)> {
        let required = self.algorithm_record(algorithm)?.required;
        self.implementations
            .iter()
            .filter_map(|implementation| {
                if !implementation.available || !policy.permits(&implementation.name) {
                    return None;
                }
                let functions = implementation.algorithms.iter().find(|functions| {
                    functions.algorithm == algorithm.slot
                        && functions.capabilities.contains(required)
                })?;
                Some((implementation, functions))
            })
            .min_by(|left, right| {
                right
                    .0
                    .priority
                    .cmp(&left.0.priority)
                    .then_with(|| left.0.name.cmp(&right.0.name))
            })
            .map(|(implementation, functions)| {
                (
                    implementation.name.as_str(),
                    functions.prepare,
                    functions.dispatch,
                )
            })
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
                required: algorithm.required,
            });
        }

        for implementation in registration.implementations {
            let algorithms = implementation
                .algorithms
                .into_iter()
                .map(|functions| AlgorithmImplementation {
                    algorithm: *self
                        .algorithm_names
                        .get(&functions.name)
                        .expect("implementation algorithms were validated before publication"),
                    capabilities: functions.capabilities,
                    prepare: functions.prepare,
                    dispatch: functions.dispatch,
                })
                .collect();
            let index = self.implementations.len();
            self.implementation_names
                .insert(implementation.name.clone(), index);
            self.implementations.push(ImplementationRecord {
                name: implementation.name,
                algorithms,
                priority: implementation.priority,
                available: implementation.available,
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

/// Caller-owned cryptographic input in contiguous or scatter-gather form.
#[derive(Clone, Copy, Debug)]
pub enum Input<'a> {
    /// One contiguous byte slice.
    Contiguous(&'a [u8]),
    /// Ordered fragments treated as one logical message.
    Scatter(&'a [&'a [u8]]),
}

impl Input<'_> {
    fn with_fragments<T>(self, operation: impl FnOnce(&[&[u8]]) -> T) -> T {
        match self {
            Self::Contiguous(input) => operation(&[input]),
            Self::Scatter(input) => operation(input),
        }
    }
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
    input: Input<'a>,
    output: &'a mut [u8],
    status: HashStatus,
}

impl<'a> HashOperation<'a> {
    /// Creates a pending operation over caller-owned memory.
    pub fn new(input: Input<'a>, output: &'a mut [u8]) -> Self {
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

/// The independent completion state of one message-authentication operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MacStatus {
    /// The operation has not yet been executed.
    Pending,
    /// The complete authenticator was written to caller memory.
    Complete {
        /// Number of output bytes written.
        written: usize,
    },
    /// Caller output cannot hold the complete authenticator.
    OutputTooSmall {
        /// Required output size.
        required: usize,
        /// Caller-provided output size.
        provided: usize,
    },
}

/// One synchronous message-authentication request.
#[derive(Debug)]
pub struct MacOperation<'a> {
    input: Input<'a>,
    output: &'a mut [u8],
    status: MacStatus,
}

impl<'a> MacOperation<'a> {
    /// Creates a pending authentication operation over caller-owned memory.
    pub fn authenticate(input: Input<'a>, output: &'a mut [u8]) -> Self {
        Self {
            input,
            output,
            status: MacStatus::Pending,
        }
    }

    /// Returns this operation's independent completion state.
    pub fn status(&self) -> MacStatus {
        self.status
    }
}

/// The independent completion state of one key-derivation operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KdfStatus {
    /// The operation has not yet been executed.
    Pending,
    /// The derived secret was installed in the Engine key authority.
    Complete {
        /// Opaque identity of the derived secret.
        key: KeyHandle,
    },
    /// The parent Key Policy does not permit the requested derived-key policy.
    DerivationDenied,
    /// The Engine key authority has no free slot for the derived secret.
    KeyPoolFull {
        /// Configured fixed capacity.
        capacity: usize,
    },
    /// The selected KDF cannot produce the requested length.
    OutputTooLong {
        /// Requested output size.
        requested: usize,
        /// Algorithm maximum output size.
        maximum: usize,
    },
}

/// One synchronous key-derivation request.
#[derive(Debug)]
pub struct KdfOperation<'a> {
    salt: Option<&'a [u8]>,
    info: Input<'a>,
    length: usize,
    target: PolicyAlgorithm,
    status: KdfStatus,
}

impl<'a> KdfOperation<'a> {
    /// Creates a pending HKDF extract-and-expand operation.
    pub fn derive<F: Family>(
        salt: Option<&'a [u8]>,
        info: Input<'a>,
        length: usize,
        algorithm: AlgorithmId<F>,
    ) -> Self {
        Self {
            salt,
            info,
            length,
            target: PolicyAlgorithm::new(algorithm),
            status: KdfStatus::Pending,
        }
    }

    /// Returns this operation's independent completion state.
    pub fn status(&self) -> KdfStatus {
        self.status
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
        input: Input<'a>,
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
        input: Input<'a>,
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
        input: Input<'a>,
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
        /// Compute a message authenticator.
        const MAC_AUTHENTICATE = 1 << 3;
    }
}

/// Immutable permissions attached to one Engine-owned key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PolicyAlgorithm {
    family: u8,
    algorithm: u32,
}

impl PolicyAlgorithm {
    fn new<F: Family>(algorithm: AlgorithmId<F>) -> Self {
        Self {
            family: F::KEY_FAMILY,
            algorithm: algorithm.slot,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DerivedKeyPolicy {
    target: PolicyAlgorithm,
    operations: KeyOperations,
    secret_export: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyPolicy {
    family: u8,
    algorithm: u32,
    operations: KeyOperations,
    secret_export: bool,
    derivations: Vec<DerivedKeyPolicy>,
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
            derivations: Vec::new(),
        }
    }

    /// Permits derivation of a key for one family-typed algorithm.
    pub fn with_derivation<F: Family>(
        mut self,
        algorithm: AlgorithmId<F>,
        operations: KeyOperations,
        secret_export: bool,
    ) -> Self {
        let derivation = DerivedKeyPolicy {
            target: PolicyAlgorithm::new(algorithm),
            operations,
            secret_export,
        };
        if let Some(existing) = self
            .derivations
            .iter_mut()
            .find(|existing| existing.target == derivation.target)
        {
            *existing = derivation;
        } else {
            self.derivations.push(derivation);
        }
        self
    }

    fn permits<F: Family>(&self, algorithm: AlgorithmId<F>) -> bool {
        self.family == F::KEY_FAMILY && self.algorithm == algorithm.slot
    }

    fn derived_policy(&self, target: PolicyAlgorithm) -> Option<Self> {
        let policy = self
            .derivations
            .iter()
            .find(|policy| policy.target == target)?;
        Some(Self {
            family: target.family,
            algorithm: target.algorithm,
            operations: policy.operations,
            secret_export: policy.secret_export,
            derivations: Vec::new(),
        })
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
        let kdf_capabilities = Capabilities::CONTIGUOUS_INPUT | Capabilities::SCATTER_INPUT;
        let mut engine = Self::new();
        engine.publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "sha-256",
                    hash_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "sha-384",
                    hash_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "sha-512",
                    hash_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "blake2s-256",
                    hash_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "blake2b-512",
                    hash_capabilities,
                ))
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("hammer:hash-portable", 0, true)
                        .with_algorithm("sha-256", hash_capabilities, (), execute_sha256)
                        .with_algorithm("sha-384", hash_capabilities, (), execute_sha384)
                        .with_algorithm("sha-512", hash_capabilities, (), execute_sha512)
                        .with_algorithm("blake2s-256", hash_capabilities, (), execute_blake2s)
                        .with_algorithm("blake2b-512", hash_capabilities, (), execute_blake2b),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Aead>::new(
                    "aes-128-gcm",
                    aead_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Aead>::new(
                    "aes-256-gcm",
                    aead_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Aead>::new(
                    "chacha20-poly1305",
                    aead_capabilities,
                ))
                .with_implementation(
                    ImplementationRegistration::<Aead>::new("hammer:aead-portable", 0, true)
                        .with_algorithm(
                            "aes-128-gcm",
                            aead_capabilities,
                            prepare_aes128_gcm,
                            execute_aead,
                        )
                        .with_algorithm(
                            "aes-256-gcm",
                            aead_capabilities,
                            prepare_aes256_gcm,
                            execute_aead,
                        )
                        .with_algorithm(
                            "chacha20-poly1305",
                            aead_capabilities,
                            prepare_chacha20_poly1305,
                            execute_aead,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Mac>::new(
                    "hmac-sha-256",
                    hash_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Mac>::new(
                    "hmac-sha-384",
                    hash_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Mac>::new(
                    "hmac-sha-512",
                    hash_capabilities,
                ))
                .with_implementation(
                    ImplementationRegistration::<Mac>::new("hammer:hmac-portable", 0, true)
                        .with_algorithm(
                            "hmac-sha-256",
                            hash_capabilities,
                            prepare_hmac_sha256,
                            execute_hmac,
                        )
                        .with_algorithm(
                            "hmac-sha-384",
                            hash_capabilities,
                            prepare_hmac_sha384,
                            execute_hmac,
                        )
                        .with_algorithm(
                            "hmac-sha-512",
                            hash_capabilities,
                            prepare_hmac_sha512,
                            execute_hmac,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Kdf>::new(
                    "hkdf-sha-256",
                    kdf_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Kdf>::new(
                    "hkdf-sha-384",
                    kdf_capabilities,
                ))
                .with_algorithm(AlgorithmRegistration::<Kdf>::new(
                    "hkdf-sha-512",
                    kdf_capabilities,
                ))
                .with_implementation(
                    ImplementationRegistration::<Kdf>::new("hammer:hkdf-portable", 0, true)
                        .with_algorithm(
                            "hkdf-sha-256",
                            kdf_capabilities,
                            hammer_infra::crypto::Sha2Algorithm::Sha256,
                            execute_hkdf,
                        )
                        .with_algorithm(
                            "hkdf-sha-384",
                            kdf_capabilities,
                            hammer_infra::crypto::Sha2Algorithm::Sha384,
                            execute_hkdf,
                        )
                        .with_algorithm(
                            "hkdf-sha-512",
                            kdf_capabilities,
                            hammer_infra::crypto::Sha2Algorithm::Sha512,
                            execute_hkdf,
                        ),
                ),
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
        let (implementation, prepare, dispatch) = F::registry(self)
            .select(algorithm, &self.selection_policy)
            .ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;
        let prepared = F::prepare_unkeyed(prepare).ok_or(ContextError::KeyRequired {
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

    /// Creates a thread-bound family-typed Context using an opaque key.
    ///
    /// # Errors
    ///
    /// Returns a concrete stale-key, policy, availability, or key-preparation
    /// failure without changing key lifecycle state.
    pub fn context_with_key<F: Family>(
        &self,
        algorithm: AlgorithmId<F>,
        key: KeyHandle,
    ) -> Result<Context<F>, ContextError> {
        let registry = F::registry(self);
        let (implementation, prepare, dispatch) = registry
            .select(algorithm, &self.selection_policy)
            .ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;

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
        let required = F::key_operations();
        if required.is_empty() {
            return Err(ContextError::KeyUnsupported {
                algorithm: algorithm.slot,
            });
        }
        if !entry.policy.operations.intersects(required) {
            return Err(ContextError::OperationsDenied { key, required });
        }
        let prepared = F::prepare_keyed(
            prepare,
            self,
            algorithm,
            key,
            &entry.material,
            &entry.policy,
        )?;
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

impl Context<Mac> {
    /// Executes all operations synchronously through one batch dispatch.
    pub fn execute(&mut self, batch: &mut Batch<'_, '_, Mac>) {
        (self.dispatch)(&mut self.prepared, batch.operations);
    }

    /// Returns the algorithm permanently bound to this Context.
    pub fn algorithm(&self) -> AlgorithmId<Mac> {
        self.algorithm
    }
}

impl Context<Kdf> {
    /// Executes all operations synchronously through one batch dispatch.
    pub fn execute(&mut self, batch: &mut Batch<'_, '_, Kdf>) {
        (self.dispatch)(&mut self.prepared, batch.operations);
    }

    /// Returns the algorithm permanently bound to this Context.
    pub fn algorithm(&self) -> AlgorithmId<Kdf> {
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
    /// One implementation repeats the same Algorithm Name in its function table.
    #[error(
        "implementation `{implementation}` repeats algorithm `{algorithm}` in one registration"
    )]
    ImplementationAlgorithmCollision {
        /// Rejected implementation.
        implementation: String,
        /// Repeated Algorithm Name.
        algorithm: String,
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
    /// The selected operation family does not accept a Key Handle.
    #[error("algorithm slot {algorithm} does not accept a Key Handle")]
    KeyUnsupported {
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
        let mut implementation_algorithms = BTreeSet::new();
        for functions in &implementation.algorithms {
            if !implementation_algorithms.insert(functions.name.as_str()) {
                return Err(RegistryError::ImplementationAlgorithmCollision {
                    implementation: implementation.name.clone(),
                    algorithm: functions.name.clone(),
                });
            }
            let required = algorithms
                .get(functions.name.as_str())
                .copied()
                .or_else(|| {
                    registry
                        .algorithm(&functions.name)
                        .and_then(|algorithm| registry.algorithm_record(algorithm))
                        .map(|algorithm| algorithm.required)
                });
            let required = required.ok_or_else(|| RegistryError::UnknownAlgorithm {
                implementation: implementation.name.clone(),
                algorithm: functions.name.clone(),
            })?;
            if !functions.capabilities.contains(required) {
                return Err(RegistryError::CapabilityMismatch {
                    implementation: implementation.name.clone(),
                    algorithm: functions.name.clone(),
                    required,
                    provided: functions.capabilities,
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

fn prepare_aes128_gcm(
    key: KeyHandle,
    material: &[u8],
    operations: KeyOperations,
) -> Result<AeadPrepared, ContextError> {
    AeadPrepared::new(
        hammer_infra::crypto::AeadAlgorithm::Aes128Gcm,
        key,
        material,
        operations,
    )
}

fn prepare_aes256_gcm(
    key: KeyHandle,
    material: &[u8],
    operations: KeyOperations,
) -> Result<AeadPrepared, ContextError> {
    AeadPrepared::new(
        hammer_infra::crypto::AeadAlgorithm::Aes256Gcm,
        key,
        material,
        operations,
    )
}

fn prepare_chacha20_poly1305(
    key: KeyHandle,
    material: &[u8],
    operations: KeyOperations,
) -> Result<AeadPrepared, ContextError> {
    AeadPrepared::new(
        hammer_infra::crypto::AeadAlgorithm::ChaCha20Poly1305,
        key,
        material,
        operations,
    )
}

fn prepare_hmac_sha256(material: &[u8]) -> MacPrepared {
    MacPrepared::new(hammer_infra::crypto::Sha2Algorithm::Sha256, material)
}

fn prepare_hmac_sha384(material: &[u8]) -> MacPrepared {
    MacPrepared::new(hammer_infra::crypto::Sha2Algorithm::Sha384, material)
}

fn prepare_hmac_sha512(material: &[u8]) -> MacPrepared {
    MacPrepared::new(hammer_infra::crypto::Sha2Algorithm::Sha512, material)
}

fn execute_hash(
    algorithm: hammer_infra::crypto::HashAlgorithm,
    operations: &mut [HashOperation<'_>],
) {
    for operation in operations {
        let result = operation
            .input
            .with_fragments(|input| hammer_infra::crypto::hash(algorithm, input, operation.output));
        operation.status = match result {
            Ok(written) => HashStatus::Complete { written },
            Err(hammer_infra::crypto::HashError::OutputTooSmall { required, provided }) => {
                HashStatus::OutputTooSmall { required, provided }
            }
        };
    }
}

fn execute_sha256(_: &mut (), operations: &mut [HashOperation<'_>]) {
    execute_hash(hammer_infra::crypto::HashAlgorithm::Sha256, operations);
}

fn execute_sha384(_: &mut (), operations: &mut [HashOperation<'_>]) {
    execute_hash(hammer_infra::crypto::HashAlgorithm::Sha384, operations);
}

fn execute_sha512(_: &mut (), operations: &mut [HashOperation<'_>]) {
    execute_hash(hammer_infra::crypto::HashAlgorithm::Sha512, operations);
}

fn execute_blake2s(_: &mut (), operations: &mut [HashOperation<'_>]) {
    execute_hash(hammer_infra::crypto::HashAlgorithm::Blake2s, operations);
}

fn execute_blake2b(_: &mut (), operations: &mut [HashOperation<'_>]) {
    execute_hash(hammer_infra::crypto::HashAlgorithm::Blake2b, operations);
}

fn execute_hmac(prepared: &mut MacPrepared, operations: &mut [MacOperation<'_>]) {
    for operation in operations {
        let result = operation
            .input
            .with_fragments(|input| prepared.mac.authenticate(input, operation.output));
        operation.status = match result {
            Ok(written) => MacStatus::Complete { written },
            Err(hammer_infra::crypto::MacError::OutputTooSmall { required, provided }) => {
                MacStatus::OutputTooSmall { required, provided }
            }
        };
    }
}

fn execute_hkdf(prepared: &mut KdfPrepared, operations: &mut [KdfOperation<'_>]) {
    for operation in operations {
        let Some(policy) = prepared.policy.derived_policy(operation.target) else {
            operation.status = KdfStatus::DerivationDenied;
            continue;
        };
        let maximum = 255 * prepared.algorithm.output_len();
        if operation.length > maximum {
            operation.status = KdfStatus::OutputTooLong {
                requested: operation.length,
                maximum,
            };
            continue;
        }

        let mut material = Zeroizing::new(vec![0; operation.length]);
        let hkdf =
            hammer_infra::crypto::Hkdf::new(prepared.algorithm, operation.salt, &prepared.material);
        let result = operation
            .info
            .with_fragments(|info| hkdf.expand(info, operation.length, &mut material));
        match result {
            Ok(_) => {}
            Err(hammer_infra::crypto::KdfError::OutputTooLong { requested, maximum }) => {
                operation.status = KdfStatus::OutputTooLong { requested, maximum };
                continue;
            }
            Err(hammer_infra::crypto::KdfError::OutputTooSmall { .. }) => {
                unreachable!("HKDF output storage has the exact requested length")
            }
        }

        let mut keys = prepared.keys.borrow_mut();
        let capacity = keys.capacity();
        let Some(index) = keys.insert(KeyEntry {
            material,
            policy,
            contexts: 0,
        }) else {
            operation.status = KdfStatus::KeyPoolFull { capacity };
            continue;
        };
        operation.status = KdfStatus::Complete {
            key: KeyHandle { index },
        };
    }
}

fn execute_aead(prepared: &mut AeadPrepared, operations: &mut [AeadOperation<'_>]) {
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
