//! Synchronous typed cryptographic execution.
//!
//! `hammer-service` owns algorithm identity, implementation selection, and
//! operation lifecycle. Portable algorithm semantics remain in
//! `hammer-infra`; `hammer-runtime` does not participate in this boundary.

pub mod exchange;
pub mod main;

use std::any::{TypeId, type_name};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

use hammer_infra::crypto::InstructionSet;
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
    fn prepare_unkeyed(
        prepare: Self::Prepare,
        engine: &Engine,
        algorithm: AlgorithmId<Self>,
    ) -> Option<Self::Prepared>;

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

mod families {
    use super::*;
    use hammer_component_macros::{Aead, Cipher, Hash, Kdf, Kx, Mac, Sign, Verify};

    /// The hash operation family.
    #[Hash]
    #[derive(Debug)]
    pub struct Hash;

    /// The authenticated-encryption operation family.
    #[Aead]
    #[derive(Debug)]
    pub struct Aead;

    /// The message-authentication operation family.
    #[Mac]
    #[derive(Debug)]
    pub struct Mac;

    /// The key-derivation operation family.
    #[Kdf]
    #[derive(Debug)]
    pub struct Kdf;

    /// The key-establishment operation family.
    #[Kx]
    #[derive(Debug)]
    pub struct Kx;

    /// The digital-signing operation family.
    #[Sign]
    #[derive(Debug)]
    pub struct Sign;

    /// The digital-signature verification operation family.
    #[Verify]
    #[derive(Debug)]
    pub struct Verify;

    /// The unauthenticated-cipher operation family.
    #[Cipher]
    #[derive(Debug)]
    pub struct Cipher;
}

pub use families::{Aead, Cipher, Hash, Kdf, Kx, Mac, Sign, Verify};

struct OwnedState {
    value: NonNull<()>,
    value_type: TypeId,
    type_name: &'static str,
    release: unsafe fn(NonNull<()>),
}

impl OwnedState {
    fn new<T: 'static>(value: T) -> Self {
        Self {
            value: NonNull::from(Box::leak(Box::new(value))).cast(),
            value_type: TypeId::of::<T>(),
            type_name: type_name::<T>(),
            release: Self::release_value::<T>,
        }
    }

    fn value<T: 'static>(&self) -> Option<&T> {
        (self.value_type == TypeId::of::<T>()).then(|| {
            // SAFETY: `value_type` is recorded with the allocation in `new`, and
            // `&self` keeps the allocation alive and prevents mutable access.
            unsafe { self.value.cast::<T>().as_ref() }
        })
    }

    fn value_mut<T: 'static>(&mut self) -> Option<&mut T> {
        (self.value_type == TypeId::of::<T>()).then(|| {
            // SAFETY: `value_type` is recorded with the allocation in `new`, and
            // `&mut self` provides exclusive access to the allocation.
            unsafe { self.value.cast::<T>().as_mut() }
        })
    }

    unsafe fn release_value<T>(value: NonNull<()>) {
        // SAFETY: `new` allocated this pointer as `Box<T>`, and `OwnedState::drop`
        // invokes the matching monomorphized release function exactly once.
        unsafe { drop(Box::from_raw(value.cast::<T>().as_ptr())) };
    }
}

impl fmt::Debug for OwnedState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedState")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedState {
    fn drop(&mut self) {
        // SAFETY: `release` was paired with this allocation by `new` and this is
        // the unique owning value, so the allocation has not been released yet.
        unsafe { (self.release)(self.value) };
    }
}

/// Prepared state for a hash Context.
#[derive(Debug)]
pub struct HashPrepared;

impl HashPrepared {
    /// Executes a batch through one statically selected digest implementation.
    pub fn execute<A>(&mut self, operations: &mut [HashOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::hash::Algorithm,
    {
        for operation in operations {
            operation.result = Some(
                operation
                    .input
                    .with_fragments(|input| A::default().digest(input, operation.output)),
            );
        }
        Ok(())
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
    fn execute_sha2(&mut self, operations: &mut [HashOperation<'_>]) -> Result<(), ContextError> {
        let available = InstructionSet::detect();
        if !available.contains(InstructionSet::SHA2) {
            return Err(ContextError::InstructionsUnavailable {
                required: InstructionSet::SHA2,
                available,
            });
        }
        let algorithm = hammer_infra::crypto::hash::Sha256;
        for operation in operations {
            operation.result = Some(
                operation
                    .input
                    .with_fragments(|input| algorithm.digest_sha2(input, operation.output)),
            );
        }
        Ok(())
    }
}

/// Prepared state for an authenticated-encryption Context.
#[derive(Debug)]
pub struct AeadPrepared {
    key: Zeroizing<Vec<u8>>,
    operations: KeyOperations,
}

impl AeadPrepared {
    fn new(
        required: usize,
        key: KeyHandle,
        material: &[u8],
        operations: KeyOperations,
    ) -> Result<Self, ContextError> {
        if material.len() != required {
            return Err(ContextError::InvalidKeyLength {
                key,
                required,
                provided: material.len(),
                source: hammer_infra::crypto::aead::Error::InvalidKeyLength {
                    required,
                    provided: material.len(),
                },
            });
        }
        Ok(Self {
            key: Zeroizing::new(material.to_vec()),
            operations,
        })
    }

    /// Executes a batch through one statically selected AEAD implementation.
    pub fn execute<A>(&mut self, operations: &mut [AeadOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::aead::Algorithm,
    {
        let cipher =
            A::new(&self.key).expect("AEAD key length was validated while preparing the Context");
        for operation in operations {
            let (direction, required) = match &operation.request {
                AeadRequest::Seal { .. } | AeadRequest::SealInPlace { .. } => {
                    (AeadDirection::Seal, KeyOperations::AEAD_SEAL)
                }
                AeadRequest::Open { .. } | AeadRequest::OpenInPlace { .. } => {
                    (AeadDirection::Open, KeyOperations::AEAD_OPEN)
                }
            };
            if !self.operations.contains(required) {
                operation.status = AeadStatus::PolicyDenied {
                    operation: direction,
                };
                continue;
            }

            let result = match &mut operation.request {
                AeadRequest::Seal {
                    input,
                    nonce,
                    associated_data,
                    output,
                    tag,
                } => (*input).with_fragments(|fragments| {
                    cipher.seal(fragments, nonce, associated_data, output, tag)
                }),
                AeadRequest::Open {
                    input,
                    nonce,
                    associated_data,
                    tag,
                    output,
                } => (*input).with_fragments(|fragments| {
                    cipher.open(fragments, nonce, associated_data, tag, output)
                }),
                AeadRequest::SealInPlace {
                    payload,
                    nonce,
                    associated_data,
                    tag,
                } => cipher.seal_in_place(payload, nonce, associated_data, tag),
                AeadRequest::OpenInPlace {
                    payload,
                    nonce,
                    associated_data,
                    tag,
                } => cipher.open_in_place(payload, nonce, associated_data, tag),
            };
            operation.status = AeadStatus::Executed(result);
        }
        Ok(())
    }
}

/// Prepared state for a message-authentication Context.
#[derive(Debug)]
pub struct MacPrepared {
    key: Zeroizing<Vec<u8>>,
}

impl MacPrepared {
    /// Executes a batch through one statically selected MAC implementation.
    pub fn execute<A>(&mut self, operations: &mut [MacOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::mac::Algorithm,
    {
        let mac = A::new(&self.key);
        for operation in operations {
            operation.result = Some(
                operation
                    .input
                    .with_fragments(|input| mac.authenticate(input, operation.output)),
            );
        }
        Ok(())
    }
}

/// Prepared state for a key-derivation Context.
#[derive(Debug)]
pub struct KdfPrepared {
    material: Zeroizing<Vec<u8>>,
    policy: KeyPolicy,
    keys: Rc<RefCell<Pool<KeyEntry>>>,
}

impl KdfPrepared {
    /// Executes a batch through one statically selected KDF implementation.
    pub fn execute<A>(&mut self, operations: &mut [KdfOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::kdf::Algorithm,
    {
        for operation in operations {
            let Some(policy) = self.policy.derived_policy(operation.target) else {
                operation.status = KdfStatus::DerivationDenied;
                continue;
            };
            let maximum = 255 * A::OUTPUT_LEN;
            if operation.length > maximum {
                operation.status =
                    KdfStatus::Algorithm(hammer_infra::crypto::kdf::Error::OutputTooLong {
                        requested: operation.length,
                        maximum,
                    });
                continue;
            }

            let mut material = Zeroizing::new(vec![0; operation.length]);
            let hkdf = A::new(operation.salt, &self.material);
            operation
                .info
                .with_fragments(|info| hkdf.expand(info, operation.length, &mut material))
                .expect("KDF length and output storage were validated before expansion");

            let mut keys = self.keys.borrow_mut();
            let capacity = keys.capacity();
            let Some(index) = keys.insert(KeyEntry {
                state: OwnedState::new(material),
                policy,
                contexts: 0,
                provenance: None,
            }) else {
                operation.status = KdfStatus::KeyPoolFull { capacity };
                continue;
            };
            operation.status = KdfStatus::Complete {
                key: KeyHandle { index },
            };
        }
        Ok(())
    }
}

/// Prepared state for a key-establishment Context.
#[derive(Debug)]
pub struct KxPrepared {
    policy_algorithm: PolicyAlgorithm,
    keys: Rc<RefCell<Pool<KeyEntry>>>,
}

impl KxPrepared {
    /// Executes a batch through one statically selected key-establishment implementation.
    pub fn execute<A>(&mut self, operations: &mut [KxOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::key_establishment::Algorithm,
    {
        let algorithm = A::default();
        for operation in operations {
            operation.status = match &mut operation.request {
                KxRequest::Generate { policy, public_key } => {
                    self.generate_keypair(&algorithm, policy, public_key)
                }
                KxRequest::Agree {
                    private_key,
                    peer_public_key,
                    target,
                } => self.agree(&algorithm, *private_key, peer_public_key, *target),
                KxRequest::Encapsulate {
                    peer_public_key,
                    policy,
                    ciphertext,
                } => self.encapsulate(&algorithm, peer_public_key, policy, ciphertext),
                KxRequest::Decapsulate {
                    private_key,
                    ciphertext,
                    target,
                } => self.decapsulate(&algorithm, *private_key, ciphertext, *target),
            };
        }
        Ok(())
    }

    fn generate_keypair<A>(
        &self,
        algorithm: &A,
        policy: &KeyPolicy,
        public_key: &mut [u8],
    ) -> KxStatus
    where
        A: hammer_infra::crypto::key_establishment::Algorithm,
    {
        if !policy.applies_to(self.policy_algorithm) {
            return KxStatus::GenerationPolicyDenied;
        }

        let public_len = A::PUBLIC_KEY_LEN;
        if public_key.len() < public_len {
            return KxStatus::Algorithm(
                hammer_infra::crypto::key_establishment::Error::OutputTooSmall {
                    output: hammer_infra::crypto::key_establishment::Output::PublicKey,
                    required: public_len,
                    provided: public_key.len(),
                },
            );
        }
        {
            let keys = self.keys.borrow();
            if keys.len() == keys.capacity() {
                return KxStatus::KeyPoolFull {
                    capacity: keys.capacity(),
                };
            }
        }

        let mut entropy = Zeroizing::new(vec![0; A::PRIVATE_KEY_LEN]);
        let mut private_key = Zeroizing::new(vec![0; A::PRIVATE_KEY_LEN]);
        let mut public_key_result = vec![0; public_len];
        loop {
            if let Err(source) = getrandom::getrandom(&mut entropy) {
                return KxStatus::EntropyUnavailable { source };
            }
            match algorithm.generate_keypair(&entropy, &mut private_key, &mut public_key_result) {
                Ok(()) => break,
                Err(hammer_infra::crypto::key_establishment::Error::InvalidPrivateKey) => continue,
                Err(error) => return KxStatus::Algorithm(error),
            }
        }

        let key = match self.install_key(private_key, policy.clone()) {
            Ok(key) => key,
            Err(status) => return status,
        };
        public_key[..public_len].copy_from_slice(&public_key_result);
        KxStatus::Generated {
            key,
            public_written: public_len,
        }
    }

    fn agree<A>(
        &self,
        algorithm: &A,
        private_key: KeyHandle,
        peer_public_key: &[u8],
        target: PolicyAlgorithm,
    ) -> KxStatus
    where
        A: hammer_infra::crypto::key_establishment::Algorithm,
    {
        let mut shared_secret = Zeroizing::new(vec![0; A::SHARED_SECRET_LEN]);
        let policy = {
            let keys = self.keys.borrow();
            let Some(entry) = keys.get(private_key.index) else {
                return KxStatus::StaleKey { key: private_key };
            };
            if !entry.policy.applies_to(self.policy_algorithm)
                || !entry.policy.operations.contains(KeyOperations::KX_AGREE)
            {
                return KxStatus::PolicyDenied { key: private_key };
            }
            let Some(policy) = entry.policy.derived_policy(target) else {
                return KxStatus::DerivationDenied { key: private_key };
            };
            if keys.len() == keys.capacity() {
                return KxStatus::KeyPoolFull {
                    capacity: keys.capacity(),
                };
            }
            let material = entry
                .state
                .value::<Zeroizing<Vec<u8>>>()
                .map(|material| material.as_slice())
                .expect("portable key establishment receives software keys");
            if let Err(error) = algorithm.agree(material, peer_public_key, &mut shared_secret) {
                return KxStatus::Algorithm(error);
            }
            policy
        };

        match self.install_key(shared_secret, policy) {
            Ok(key) => KxStatus::SharedSecret { key },
            Err(status) => status,
        }
    }

    fn encapsulate<A>(
        &self,
        algorithm: &A,
        peer_public_key: &[u8],
        policy: &KeyPolicy,
        ciphertext: &mut [u8],
    ) -> KxStatus
    where
        A: hammer_infra::crypto::key_establishment::Algorithm,
    {
        let Some(ciphertext_len) = A::CIPHERTEXT_LEN else {
            return KxStatus::Algorithm(
                hammer_infra::crypto::key_establishment::Error::OperationUnsupported,
            );
        };
        if ciphertext.len() < ciphertext_len {
            return KxStatus::Algorithm(
                hammer_infra::crypto::key_establishment::Error::OutputTooSmall {
                    output: hammer_infra::crypto::key_establishment::Output::Ciphertext,
                    required: ciphertext_len,
                    provided: ciphertext.len(),
                },
            );
        }
        {
            let keys = self.keys.borrow();
            if keys.len() == keys.capacity() {
                return KxStatus::KeyPoolFull {
                    capacity: keys.capacity(),
                };
            }
        }

        let mut entropy = Zeroizing::new(vec![0; A::ENCAPSULATION_ENTROPY_LEN]);
        if let Err(source) = getrandom::getrandom(&mut entropy) {
            return KxStatus::EntropyUnavailable { source };
        }
        let mut ciphertext_result = vec![0; ciphertext_len];
        let mut shared_secret = Zeroizing::new(vec![0; A::SHARED_SECRET_LEN]);
        if let Err(error) = algorithm.encapsulate(
            peer_public_key,
            &entropy,
            &mut ciphertext_result,
            &mut shared_secret,
        ) {
            return KxStatus::Algorithm(error);
        }

        let key = match self.install_key(shared_secret, policy.clone()) {
            Ok(key) => key,
            Err(status) => return status,
        };
        ciphertext[..ciphertext_len].copy_from_slice(&ciphertext_result);
        KxStatus::Encapsulated {
            key,
            ciphertext_written: ciphertext_len,
        }
    }

    fn decapsulate<A>(
        &self,
        algorithm: &A,
        private_key: KeyHandle,
        ciphertext: &[u8],
        target: PolicyAlgorithm,
    ) -> KxStatus
    where
        A: hammer_infra::crypto::key_establishment::Algorithm,
    {
        let mut shared_secret = Zeroizing::new(vec![0; A::SHARED_SECRET_LEN]);
        let policy = {
            let keys = self.keys.borrow();
            let Some(entry) = keys.get(private_key.index) else {
                return KxStatus::StaleKey { key: private_key };
            };
            if !entry.policy.applies_to(self.policy_algorithm)
                || !entry
                    .policy
                    .operations
                    .contains(KeyOperations::KX_DECAPSULATE)
            {
                return KxStatus::PolicyDenied { key: private_key };
            }
            let Some(policy) = entry.policy.derived_policy(target) else {
                return KxStatus::DerivationDenied { key: private_key };
            };
            if keys.len() == keys.capacity() {
                return KxStatus::KeyPoolFull {
                    capacity: keys.capacity(),
                };
            }
            let material = entry
                .state
                .value::<Zeroizing<Vec<u8>>>()
                .map(|material| material.as_slice())
                .expect("portable key establishment receives software keys");
            if let Err(error) = algorithm.decapsulate(material, ciphertext, &mut shared_secret) {
                return KxStatus::Algorithm(error);
            }
            policy
        };

        match self.install_key(shared_secret, policy) {
            Ok(key) => KxStatus::SharedSecret { key },
            Err(status) => status,
        }
    }

    fn install_key(
        &self,
        material: Zeroizing<Vec<u8>>,
        policy: KeyPolicy,
    ) -> Result<KeyHandle, KxStatus> {
        let mut keys = self.keys.borrow_mut();
        let capacity = keys.capacity();
        let index = keys
            .insert(KeyEntry {
                state: OwnedState::new(material),
                policy,
                contexts: 0,
                provenance: None,
            })
            .ok_or(KxStatus::KeyPoolFull { capacity })?;
        Ok(KeyHandle { index })
    }
}

/// Prepared state for a signing Context.
pub struct SignPrepared {
    private_key: Zeroizing<Vec<u8>>,
}

impl fmt::Debug for SignPrepared {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignPrepared")
            .field("private_key_len", &self.private_key.len())
            .finish_non_exhaustive()
    }
}

impl SignPrepared {
    /// Executes a batch through one statically selected signature implementation.
    pub fn execute<A>(&mut self, operations: &mut [SignOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::signature::Algorithm,
    {
        for operation in operations {
            operation.result = Some(match &mut operation.request {
                SignRequest::PublicKey { output } => {
                    A::default().public_key(&self.private_key, output)
                }
                SignRequest::Sign { input, output } => input.with_fragments(|message| {
                    A::default().sign(&self.private_key, message, output)
                }),
            });
        }
        Ok(())
    }
}

/// Prepared state for a verification Context.
#[derive(Debug)]
pub struct VerifyPrepared;

impl VerifyPrepared {
    /// Executes a batch through one statically selected signature implementation.
    pub fn execute<A>(&mut self, operations: &mut [VerifyOperation<'_>]) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::signature::Algorithm,
    {
        for operation in operations {
            operation.result = Some(operation.input.with_fragments(|message| {
                A::default().verify(operation.public_key, message, operation.signature)
            }));
        }
        Ok(())
    }
}

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

type PrepareContext<F> = for<'a> fn(
    &OwnedState,
    &RefCell<OwnedState>,
    &Engine,
    AlgorithmId<F>,
    Option<(KeyHandle, &'a OwnedState, &'a KeyPolicy)>,
) -> Result<OwnedState, ContextError>;
type DispatchBatch<F> = for<'a> fn(
    &OwnedState,
    &RefCell<OwnedState>,
    &mut OwnedState,
    &mut [<F as Family>::Operation<'a>],
) -> Result<(), ContextError>;
type GenerateKey = fn(&OwnedState, &RefCell<OwnedState>) -> Result<OwnedState, KeyError>;

/// Prepares implementation-private state for an unkeyed Context.
pub type UnkeyedContextPrepare<F, I, P> =
    fn(&mut I, &Engine, AlgorithmId<F>) -> Result<P, ContextError>;
/// Prepares implementation-private state for a keyed Context.
pub type KeyedContextPrepare<F, I, K, P> =
    fn(&mut I, &Engine, AlgorithmId<F>, KeyHandle, &K, &KeyPolicy) -> Result<P, ContextError>;
/// Executes one typed batch through implementation-private Context state.
pub type ContextDispatch<F, I, P> =
    for<'a> fn(&mut I, &mut P, &mut [<F as Family>::Operation<'a>]) -> Result<(), ContextError>;

struct AlgorithmFunctions<F: Family> {
    state: OwnedState,
    prepare: PrepareContext<F>,
    dispatch: DispatchBatch<F>,
    key_type: Option<TypeId>,
}

struct FamilyFunctions<F: Family> {
    prepare: F::Prepare,
    dispatch: F::Dispatch,
}

impl<F: Family> FamilyFunctions<F>
where
    F::Prepared: 'static,
    F::Prepare: 'static,
    F::Dispatch:
        'static + for<'a> Fn(&mut F::Prepared, &mut [F::Operation<'a>]) -> Result<(), ContextError>,
{
    fn prepare(
        functions: &OwnedState,
        _: &RefCell<OwnedState>,
        engine: &Engine,
        algorithm: AlgorithmId<F>,
        key: Option<(KeyHandle, &OwnedState, &KeyPolicy)>,
    ) -> Result<OwnedState, ContextError> {
        let functions = functions
            .value::<Self>()
            .expect("published family functions retain their concrete type");
        let prepared = match key {
            Some((handle, state, policy)) => {
                let key = state
                    .value::<Zeroizing<Vec<u8>>>()
                    .expect("built-in keyed implementations receive software keys");
                F::prepare_keyed(
                    functions.prepare,
                    engine,
                    algorithm,
                    handle,
                    key.as_slice(),
                    policy,
                )?
            }
            None => F::prepare_unkeyed(functions.prepare, engine, algorithm).ok_or(
                ContextError::KeyRequired {
                    algorithm: algorithm.slot,
                },
            )?,
        };
        Ok(OwnedState::new(prepared))
    }

    fn dispatch<'data>(
        functions: &OwnedState,
        _: &RefCell<OwnedState>,
        prepared: &mut OwnedState,
        operations: &mut [F::Operation<'data>],
    ) -> Result<(), ContextError> {
        let functions = functions
            .value::<Self>()
            .expect("published family functions retain their concrete type");
        let prepared = prepared
            .value_mut::<F::Prepared>()
            .expect("built-in Context retains its concrete prepared state");
        (functions.dispatch)(prepared, operations)
    }
}

struct UnkeyedFunctions<F: Family, I, P> {
    prepare: UnkeyedContextPrepare<F, I, P>,
    dispatch: ContextDispatch<F, I, P>,
}

impl<F: Family, I: 'static, P: 'static> UnkeyedFunctions<F, I, P> {
    fn prepare(
        functions: &OwnedState,
        implementation: &RefCell<OwnedState>,
        engine: &Engine,
        algorithm: AlgorithmId<F>,
        key: Option<(KeyHandle, &OwnedState, &KeyPolicy)>,
    ) -> Result<OwnedState, ContextError> {
        if key.is_some() {
            return Err(ContextError::KeyUnsupported {
                algorithm: algorithm.slot,
            });
        }
        let functions = functions
            .value::<Self>()
            .expect("published functions retain their concrete type");
        let mut implementation = implementation.borrow_mut();
        let implementation = implementation
            .value_mut::<I>()
            .expect("published implementation state retains its concrete type");
        (functions.prepare)(implementation, engine, algorithm).map(OwnedState::new)
    }

    fn dispatch<'data>(
        functions: &OwnedState,
        implementation: &RefCell<OwnedState>,
        prepared: &mut OwnedState,
        operations: &mut [F::Operation<'data>],
    ) -> Result<(), ContextError> {
        let functions = functions
            .value::<Self>()
            .expect("published functions retain their concrete type");
        let mut implementation = implementation.borrow_mut();
        let implementation = implementation
            .value_mut::<I>()
            .expect("published implementation state retains its concrete type");
        let prepared = prepared
            .value_mut::<P>()
            .expect("Context retains its implementation-private prepared type");
        (functions.dispatch)(implementation, prepared, operations)
    }
}

struct KeyedFunctions<F: Family, I, K, P> {
    prepare: KeyedContextPrepare<F, I, K, P>,
    dispatch: ContextDispatch<F, I, P>,
}

impl<F: Family, I: 'static, K: 'static, P: 'static> KeyedFunctions<F, I, K, P> {
    fn prepare(
        functions: &OwnedState,
        implementation: &RefCell<OwnedState>,
        engine: &Engine,
        algorithm: AlgorithmId<F>,
        key: Option<(KeyHandle, &OwnedState, &KeyPolicy)>,
    ) -> Result<OwnedState, ContextError> {
        let (handle, key, policy) = key.ok_or(ContextError::KeyRequired {
            algorithm: algorithm.slot,
        })?;
        let functions = functions
            .value::<Self>()
            .expect("published functions retain their concrete type");
        let key = key
            .value::<K>()
            .expect("selected implementation receives its registered key type");
        let mut implementation = implementation.borrow_mut();
        let implementation = implementation
            .value_mut::<I>()
            .expect("published implementation state retains its concrete type");
        (functions.prepare)(implementation, engine, algorithm, handle, key, policy)
            .map(OwnedState::new)
    }

    fn dispatch<'data>(
        functions: &OwnedState,
        implementation: &RefCell<OwnedState>,
        prepared: &mut OwnedState,
        operations: &mut [F::Operation<'data>],
    ) -> Result<(), ContextError> {
        let functions = functions
            .value::<Self>()
            .expect("published functions retain their concrete type");
        let mut implementation = implementation.borrow_mut();
        let implementation = implementation
            .value_mut::<I>()
            .expect("published implementation state retains its concrete type");
        let prepared = prepared
            .value_mut::<P>()
            .expect("Context retains its implementation-private prepared type");
        (functions.dispatch)(implementation, prepared, operations)
    }
}

struct KeyGenerationFunctions<I, K> {
    generate: fn(&mut I) -> Result<K, KeyError>,
}

impl<I: 'static, K: 'static> KeyGenerationFunctions<I, K> {
    fn generate(
        functions: &OwnedState,
        implementation: &RefCell<OwnedState>,
    ) -> Result<OwnedState, KeyError> {
        let functions = functions
            .value::<Self>()
            .expect("published key functions retain their concrete type");
        let mut implementation = implementation.borrow_mut();
        let implementation = implementation
            .value_mut::<I>()
            .expect("published implementation state retains its concrete type");
        (functions.generate)(implementation).map(OwnedState::new)
    }
}

struct KeyGenerationRegistration {
    functions: OwnedState,
    generate: GenerateKey,
    key_type: TypeId,
    operations: KeyOperations,
}

/// One implementation declaration awaiting failure-atomic publication.
pub struct ImplementationRegistration<F: Family, I: 'static = ()> {
    name: String,
    algorithms: Vec<AlgorithmImplementationRegistration<F>>,
    priority: i32,
    available: bool,
    instructions: InstructionSet,
    state: I,
    key_generation: Option<KeyGenerationRegistration>,
}

struct AlgorithmImplementationRegistration<F: Family> {
    name: String,
    capabilities: Capabilities,
    functions: AlgorithmFunctions<F>,
}

impl<F: Family> ImplementationRegistration<F, ()> {
    /// Declares one implementation before adding its algorithm function tables.
    pub fn new(name: impl Into<String>, priority: i32, available: bool) -> Self {
        Self {
            name: name.into(),
            algorithms: Vec::new(),
            priority,
            available,
            instructions: InstructionSet::empty(),
            state: (),
            key_generation: None,
        }
    }

    /// Installs the state shared by this implementation's key and Context functions.
    pub fn with_state<I: 'static>(self, state: I) -> ImplementationRegistration<F, I> {
        ImplementationRegistration {
            name: self.name,
            algorithms: self.algorithms,
            priority: self.priority,
            available: self.available,
            instructions: self.instructions,
            state,
            key_generation: self.key_generation,
        }
    }
}

impl<F: Family, I: 'static> ImplementationRegistration<F, I> {
    /// Declares the CPU instructions required before this implementation may be selected.
    pub fn with_instruction_set(mut self, instructions: InstructionSet) -> Self {
        self.instructions = instructions;
        self
    }

    /// Adds the direct Context preparation and batch dispatch table for one algorithm.
    pub fn with_algorithm(
        mut self,
        name: impl Into<String>,
        capabilities: Capabilities,
        prepare: F::Prepare,
        dispatch: F::Dispatch,
    ) -> Self
    where
        F::Prepared: 'static,
        F::Prepare: 'static,
        F::Dispatch: 'static
            + for<'a> Fn(&mut F::Prepared, &mut [F::Operation<'a>]) -> Result<(), ContextError>,
    {
        let key_type =
            (!F::key_operations().is_empty()).then_some(TypeId::of::<Zeroizing<Vec<u8>>>());
        self.algorithms.push(AlgorithmImplementationRegistration {
            name: name.into(),
            capabilities,
            functions: AlgorithmFunctions {
                state: OwnedState::new(FamilyFunctions::<F> { prepare, dispatch }),
                prepare: FamilyFunctions::<F>::prepare,
                dispatch: FamilyFunctions::<F>::dispatch,
                key_type,
            },
        });
        self
    }

    /// Adds an unkeyed algorithm backed by implementation-private prepared state.
    pub fn with_unkeyed_algorithm<P: 'static>(
        mut self,
        name: impl Into<String>,
        capabilities: Capabilities,
        prepare: UnkeyedContextPrepare<F, I, P>,
        dispatch: ContextDispatch<F, I, P>,
    ) -> Self {
        self.algorithms.push(AlgorithmImplementationRegistration {
            name: name.into(),
            capabilities,
            functions: AlgorithmFunctions {
                state: OwnedState::new(UnkeyedFunctions::<F, I, P> { prepare, dispatch }),
                prepare: UnkeyedFunctions::<F, I, P>::prepare,
                dispatch: UnkeyedFunctions::<F, I, P>::dispatch,
                key_type: None,
            },
        });
        self
    }

    /// Adds a keyed algorithm backed by implementation-private prepared state.
    pub fn with_keyed_algorithm<K: 'static, P: 'static>(
        mut self,
        name: impl Into<String>,
        capabilities: Capabilities,
        prepare: KeyedContextPrepare<F, I, K, P>,
        dispatch: ContextDispatch<F, I, P>,
    ) -> Self {
        self.algorithms.push(AlgorithmImplementationRegistration {
            name: name.into(),
            capabilities,
            functions: AlgorithmFunctions {
                state: OwnedState::new(KeyedFunctions::<F, I, K, P> { prepare, dispatch }),
                prepare: KeyedFunctions::<F, I, K, P>::prepare,
                dispatch: KeyedFunctions::<F, I, K, P>::dispatch,
                key_type: Some(TypeId::of::<K>()),
            },
        });
        self
    }

    /// Enables implementation-owned, non-exportable key generation.
    pub fn with_key_generation<K: 'static>(
        mut self,
        operations: KeyOperations,
        generate: fn(&mut I) -> Result<K, KeyError>,
    ) -> Self {
        self.key_generation = Some(KeyGenerationRegistration {
            functions: OwnedState::new(KeyGenerationFunctions::<I, K> { generate }),
            generate: KeyGenerationFunctions::<I, K>::generate,
            key_type: TypeId::of::<K>(),
            operations,
        });
        self
    }

    fn into_declaration(self) -> ImplementationDeclaration<F> {
        ImplementationDeclaration {
            name: self.name,
            algorithms: self.algorithms,
            priority: self.priority,
            available: self.available,
            instructions: self.instructions,
            state: OwnedState::new(self.state),
            key_generation: self.key_generation,
        }
    }
}

struct ImplementationDeclaration<F: Family> {
    name: String,
    algorithms: Vec<AlgorithmImplementationRegistration<F>>,
    priority: i32,
    available: bool,
    instructions: InstructionSet,
    state: OwnedState,
    key_generation: Option<KeyGenerationRegistration>,
}

/// A family-typed algorithm and implementation publication bundle.
pub struct Registration<F: Family> {
    algorithms: Vec<(String, Capabilities)>,
    implementations: Vec<ImplementationDeclaration<F>>,
}

impl<F: Family> Registration<F> {
    /// Creates an empty publication bundle.
    pub fn new() -> Self {
        Self {
            algorithms: Vec::new(),
            implementations: Vec::new(),
        }
    }

    /// Adds one canonical algorithm declaration to the bundle.
    pub fn with_algorithm(mut self, name: impl Into<String>, required: Capabilities) -> Self {
        self.algorithms.push((name.into(), required));
        self
    }

    /// Adds one implementation declaration to the bundle.
    pub fn with_implementation<I: 'static>(
        mut self,
        implementation: ImplementationRegistration<F, I>,
    ) -> Self {
        self.implementations.push(implementation.into_declaration());
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

struct ImplementationRecord<F: Family> {
    name: String,
    algorithms: Vec<AlgorithmImplementation<F>>,
    priority: i32,
    available: Rc<Cell<bool>>,
    instructions: InstructionSet,
    state: Rc<RefCell<OwnedState>>,
    key_generation: Option<KeyGenerationRegistration>,
}

struct AlgorithmImplementation<F: Family> {
    algorithm: u32,
    capabilities: Capabilities,
    functions: Rc<AlgorithmFunctions<F>>,
}

/// Family-private registry storage exposed only to the sealed [`Family`] contract.
#[doc(hidden)]
pub struct FamilyRegistry<F: Family> {
    algorithms: Vec<Capabilities>,
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

    fn select(
        &self,
        algorithm: AlgorithmId<F>,
        policy: &SelectionPolicy,
        instructions: InstructionSet,
        key_type: Option<TypeId>,
        owner: Option<&Rc<RefCell<OwnedState>>>,
    ) -> Option<(&ImplementationRecord<F>, &AlgorithmImplementation<F>)> {
        let required = *self.algorithms.get(algorithm.slot as usize)?;
        self.implementations
            .iter()
            .filter_map(|implementation| {
                if !implementation.available.get()
                    || !instructions.contains(implementation.instructions)
                    || !policy.permits(&implementation.name)
                    || owner.is_some_and(|owner| !Rc::ptr_eq(owner, &implementation.state))
                {
                    return None;
                }
                let functions = implementation.algorithms.iter().find(|functions| {
                    functions.algorithm == algorithm.slot
                        && functions.capabilities.contains(required)
                        && functions.functions.key_type == key_type
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
    }

    fn select_key_generation(
        &self,
        algorithm: AlgorithmId<F>,
        policy: &SelectionPolicy,
        instructions: InstructionSet,
    ) -> Option<(&ImplementationRecord<F>, &KeyGenerationRegistration)> {
        let required = *self.algorithms.get(algorithm.slot as usize)?;
        self.implementations
            .iter()
            .filter_map(|implementation| {
                if !implementation.available.get()
                    || !instructions.contains(implementation.instructions)
                    || !policy.permits(&implementation.name)
                {
                    return None;
                }
                let key_generation = implementation.key_generation.as_ref()?;
                implementation.algorithms.iter().find(|functions| {
                    functions.algorithm == algorithm.slot
                        && functions.capabilities.contains(required)
                        && functions.functions.key_type == Some(key_generation.key_type)
                })?;
                Some((implementation, key_generation))
            })
            .min_by(|left, right| {
                right
                    .0
                    .priority
                    .cmp(&left.0.priority)
                    .then_with(|| left.0.name.cmp(&right.0.name))
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
        self.implementations[index].available.set(available);
        Ok(())
    }

    fn set_priority(&mut self, name: &str, priority: i32) -> Result<(), RegistryError> {
        let index = self
            .implementation_names
            .get(name)
            .copied()
            .ok_or_else(|| RegistryError::ImplementationUnknown {
                name: name.to_owned(),
            })?;
        self.implementations[index].priority = priority;
        Ok(())
    }

    fn publish(&mut self, registration: Registration<F>) {
        let first_slot = self.algorithms.len();
        for (offset, (name, required)) in registration.algorithms.into_iter().enumerate() {
            let slot = u32::try_from(first_slot + offset)
                .expect("registry capacity was validated before publication");
            self.algorithm_names.insert(name, slot);
            self.algorithms.push(required);
        }

        for implementation in registration.implementations {
            let state = Rc::new(RefCell::new(implementation.state));
            let algorithms = implementation
                .algorithms
                .into_iter()
                .map(|functions| AlgorithmImplementation {
                    algorithm: *self
                        .algorithm_names
                        .get(&functions.name)
                        .expect("implementation algorithms were validated before publication"),
                    capabilities: functions.capabilities,
                    functions: Rc::new(functions.functions),
                })
                .collect();
            let index = self.implementations.len();
            self.implementation_names
                .insert(implementation.name.clone(), index);
            self.implementations.push(ImplementationRecord {
                name: implementation.name,
                algorithms,
                priority: implementation.priority,
                available: Rc::new(Cell::new(implementation.available)),
                instructions: implementation.instructions,
                state,
                key_generation: implementation.key_generation,
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
    /// Presents contiguous and scatter-gather input as one ordered fragment slice.
    pub fn with_fragments<T>(self, operation: impl FnOnce(&[&[u8]]) -> T) -> T {
        match self {
            Self::Contiguous(input) => operation(&[input]),
            Self::Scatter(input) => operation(input),
        }
    }
}

/// One synchronous hash request and its caller-owned output.
#[derive(Debug)]
pub struct HashOperation<'a> {
    input: Input<'a>,
    output: &'a mut [u8],
    result: Option<Result<usize, hammer_infra::crypto::hash::Error>>,
}

impl<'a> HashOperation<'a> {
    /// Creates a pending operation over caller-owned memory.
    pub fn new(input: Input<'a>, output: &'a mut [u8]) -> Self {
        Self {
            input,
            output,
            result: None,
        }
    }

    /// Returns `None` until execution, then the exact digest result.
    pub fn status(&self) -> Option<Result<usize, hammer_infra::crypto::hash::Error>> {
        self.result
    }
}

/// One synchronous message-authentication request.
#[derive(Debug)]
pub struct MacOperation<'a> {
    input: Input<'a>,
    output: &'a mut [u8],
    result: Option<Result<usize, hammer_infra::crypto::mac::Error>>,
}

impl<'a> MacOperation<'a> {
    /// Creates a pending authentication operation over caller-owned memory.
    pub fn authenticate(input: Input<'a>, output: &'a mut [u8]) -> Self {
        Self {
            input,
            output,
            result: None,
        }
    }

    /// Returns `None` until execution, then the exact authentication result.
    pub fn status(&self) -> Option<Result<usize, hammer_infra::crypto::mac::Error>> {
        self.result
    }

    /// Returns the caller-owned input selected for this operation.
    pub fn input(&self) -> Input<'a> {
        self.input
    }

    /// Returns the caller-owned output selected for this operation.
    pub fn output(&mut self) -> &mut [u8] {
        self.output
    }

    /// Records this operation's independent completion result.
    pub fn complete(&mut self, result: Result<usize, hammer_infra::crypto::mac::Error>) {
        self.result = Some(result);
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
    /// The selected algorithm rejected the requested derivation.
    Algorithm(hammer_infra::crypto::kdf::Error),
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

/// The independent completion state of one key-establishment operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KxStatus {
    /// The operation has not yet been executed.
    Pending,
    /// A private key was installed and its public key was written.
    Generated {
        /// Opaque identity of the generated private key.
        key: KeyHandle,
        /// Number of public-key bytes written.
        public_written: usize,
    },
    /// A shared secret was installed in the Engine key authority.
    SharedSecret {
        /// Opaque identity of the established secret.
        key: KeyHandle,
    },
    /// An ML-KEM secret was installed and its ciphertext was written.
    Encapsulated {
        /// Opaque identity of the encapsulated secret.
        key: KeyHandle,
        /// Number of ciphertext bytes written.
        ciphertext_written: usize,
    },
    /// A generated private-key policy names a different algorithm.
    GenerationPolicyDenied,
    /// A referenced Key Handle is stale.
    StaleKey {
        /// Rejected key identity.
        key: KeyHandle,
    },
    /// A private-key policy does not permit the requested operation.
    PolicyDenied {
        /// Rejected private-key identity.
        key: KeyHandle,
    },
    /// A private-key policy does not permit the requested shared-secret policy.
    DerivationDenied {
        /// Rejected private-key identity.
        key: KeyHandle,
    },
    /// The selected algorithm rejected an operation or its caller-owned memory.
    Algorithm(hammer_infra::crypto::key_establishment::Error),
    /// The operating system could not provide cryptographic entropy.
    EntropyUnavailable {
        /// Original entropy-source failure.
        source: getrandom::Error,
    },
    /// The Engine key authority has no free slot for the result.
    KeyPoolFull {
        /// Configured fixed capacity.
        capacity: usize,
    },
}

#[derive(Debug)]
enum KxRequest<'a> {
    Generate {
        policy: KeyPolicy,
        public_key: &'a mut [u8],
    },
    Agree {
        private_key: KeyHandle,
        peer_public_key: &'a [u8],
        target: PolicyAlgorithm,
    },
    Encapsulate {
        peer_public_key: &'a [u8],
        policy: KeyPolicy,
        ciphertext: &'a mut [u8],
    },
    Decapsulate {
        private_key: KeyHandle,
        ciphertext: &'a [u8],
        target: PolicyAlgorithm,
    },
}

/// One synchronous key-establishment request.
#[derive(Debug)]
pub struct KxOperation<'a> {
    request: KxRequest<'a>,
    status: KxStatus,
}

impl<'a> KxOperation<'a> {
    /// Creates a pending private-key generation operation.
    pub fn generate_keypair(policy: KeyPolicy, public_key: &'a mut [u8]) -> Self {
        Self {
            request: KxRequest::Generate { policy, public_key },
            status: KxStatus::Pending,
        }
    }

    /// Creates a pending ECDH agreement operation.
    pub fn agree<F: Family>(
        private_key: KeyHandle,
        peer_public_key: &'a [u8],
        target: AlgorithmId<F>,
    ) -> Self {
        Self {
            request: KxRequest::Agree {
                private_key,
                peer_public_key,
                target: PolicyAlgorithm::new(target),
            },
            status: KxStatus::Pending,
        }
    }

    /// Creates a pending ML-KEM encapsulation operation.
    pub fn encapsulate(
        peer_public_key: &'a [u8],
        policy: KeyPolicy,
        ciphertext: &'a mut [u8],
    ) -> Self {
        Self {
            request: KxRequest::Encapsulate {
                peer_public_key,
                policy,
                ciphertext,
            },
            status: KxStatus::Pending,
        }
    }

    /// Creates a pending ML-KEM decapsulation operation.
    pub fn decapsulate<F: Family>(
        private_key: KeyHandle,
        ciphertext: &'a [u8],
        target: AlgorithmId<F>,
    ) -> Self {
        Self {
            request: KxRequest::Decapsulate {
                private_key,
                ciphertext,
                target: PolicyAlgorithm::new(target),
            },
            status: KxStatus::Pending,
        }
    }

    /// Returns this operation's independent completion state.
    pub fn status(&self) -> KxStatus {
        self.status
    }
}

#[derive(Debug)]
enum SignRequest<'a> {
    PublicKey {
        output: &'a mut [u8],
    },
    Sign {
        input: Input<'a>,
        output: &'a mut [u8],
    },
}

/// One synchronous signing request.
#[derive(Debug)]
pub struct SignOperation<'a> {
    request: SignRequest<'a>,
    result: Option<Result<usize, hammer_infra::crypto::signature::SignError>>,
}

impl<'a> SignOperation<'a> {
    /// Creates a pending public-key derivation request.
    pub fn public_key(output: &'a mut [u8]) -> Self {
        Self {
            request: SignRequest::PublicKey { output },
            result: None,
        }
    }

    /// Creates a pending signature request.
    pub fn sign(input: Input<'a>, output: &'a mut [u8]) -> Self {
        Self {
            request: SignRequest::Sign { input, output },
            result: None,
        }
    }

    /// Returns `None` until execution, then the written length or signing error.
    pub fn status(&self) -> Option<Result<usize, hammer_infra::crypto::signature::SignError>> {
        self.result
    }
}

/// One synchronous signature-verification request.
#[derive(Debug)]
pub struct VerifyOperation<'a> {
    public_key: &'a [u8],
    input: Input<'a>,
    signature: &'a [u8],
    result: Option<Result<(), hammer_infra::crypto::signature::VerifyError>>,
}

impl<'a> VerifyOperation<'a> {
    /// Creates a pending signature-verification request.
    pub fn verify(public_key: &'a [u8], input: Input<'a>, signature: &'a [u8]) -> Self {
        Self {
            public_key,
            input,
            signature,
            result: None,
        }
    }

    /// Returns `None` until execution, then the verification result.
    pub fn status(&self) -> Option<Result<(), hammer_infra::crypto::signature::VerifyError>> {
        self.result
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
    /// The selected implementation completed and retained its exact typed result.
    Executed(Result<usize, hammer_infra::crypto::aead::Error>),
    /// The key policy denied this operation.
    PolicyDenied {
        /// Operation denied by the immutable policy.
        operation: AeadDirection,
    },
}

#[derive(Debug)]
enum AeadRequest<'a> {
    Seal {
        input: Input<'a>,
        nonce: &'a [u8],
        associated_data: &'a [u8],
        output: &'a mut [u8],
        tag: &'a mut [u8],
    },
    Open {
        input: Input<'a>,
        nonce: &'a [u8],
        associated_data: &'a [u8],
        tag: &'a [u8],
        output: &'a mut [u8],
    },
    SealInPlace {
        payload: &'a mut [u8],
        nonce: &'a [u8],
        associated_data: &'a [u8],
        tag: &'a mut [u8],
    },
    OpenInPlace {
        payload: &'a mut [u8],
        nonce: &'a [u8],
        associated_data: &'a [u8],
        tag: &'a [u8],
    },
}

/// One synchronous authenticated-encryption request.
#[derive(Debug)]
pub struct AeadOperation<'a> {
    request: AeadRequest<'a>,
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
            request: AeadRequest::Seal {
                input,
                nonce,
                associated_data,
                output,
                tag,
            },
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
            request: AeadRequest::Open {
                input,
                nonce,
                associated_data,
                tag,
                output,
            },
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
            request: AeadRequest::SealInPlace {
                payload,
                nonce,
                associated_data,
                tag,
            },
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
            request: AeadRequest::OpenInPlace {
                payload,
                nonce,
                associated_data,
                tag,
            },
            status: AeadStatus::Pending,
        }
    }

    /// Returns the operation's current completion state.
    pub fn status(&self) -> AeadStatus {
        self.status
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
        /// Establish an ECDH shared secret.
        const KX_AGREE = 1 << 4;
        /// Decapsulate an ML-KEM shared secret.
        const KX_DECAPSULATE = 1 << 5;
        /// Produce digital signatures with a private key.
        const SIGN = 1 << 6;
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

    fn applies_to(&self, algorithm: PolicyAlgorithm) -> bool {
        self.family == algorithm.family && self.algorithm == algorithm.algorithm
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

    fn restrict_to(&mut self, operations: KeyOperations) {
        self.operations &= operations;
        self.secret_export = false;
        if !self.operations.contains(KeyOperations::DERIVE) {
            self.derivations.clear();
        }
    }
}

/// A generation-bearing opaque identity for Engine-owned key material.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KeyHandle {
    index: PoolIndex,
}

impl From<PoolIndex> for KeyHandle {
    #[inline]
    fn from(index: PoolIndex) -> Self {
        Self { index }
    }
}

impl From<KeyHandle> for PoolIndex {
    #[inline]
    fn from(key: KeyHandle) -> Self {
        key.index
    }
}

struct KeyProvenance {
    name: String,
    state: Rc<RefCell<OwnedState>>,
}

struct KeyEntry {
    state: OwnedState,
    policy: KeyPolicy,
    contexts: usize,
    provenance: Option<KeyProvenance>,
}

impl fmt::Debug for KeyEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyEntry")
            .field("state", &self.state)
            .field("policy", &self.policy)
            .field("contexts", &self.contexts)
            .field(
                "implementation",
                &self
                    .provenance
                    .as_ref()
                    .map(|implementation| implementation.name.as_str()),
            )
            .finish()
    }
}

#[derive(Debug)]
struct KeyRetention {
    keys: Rc<RefCell<Pool<KeyEntry>>>,
    key: KeyHandle,
}

impl Drop for KeyRetention {
    fn drop(&mut self) {
        let mut keys = self.keys.borrow_mut();
        let entry = keys
            .get_mut(self.key.index)
            .expect("live Context must retain a live key generation");
        entry.contexts = entry
            .contexts
            .checked_sub(1)
            .expect("key Context reference count must be positive");
    }
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
    instructions: InstructionSet,
    selection_policy: SelectionPolicy,
    keys: Rc<RefCell<Pool<KeyEntry>>>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("aead_algorithms", &self.aeads.algorithms.len())
            .field("hash_algorithms", &self.hashes.algorithms.len())
            .field("instructions", &self.instructions)
            .field("key_count", &self.keys.borrow().len())
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Creates an Engine with empty family registries.
    pub fn new(instructions: InstructionSet) -> Self {
        Self {
            aeads: FamilyRegistry::new(),
            ciphers: FamilyRegistry::new(),
            hashes: FamilyRegistry::new(),
            macs: FamilyRegistry::new(),
            kdfs: FamilyRegistry::new(),
            key_exchanges: FamilyRegistry::new(),
            signers: FamilyRegistry::new(),
            verifiers: FamilyRegistry::new(),
            instructions,
            selection_policy: SelectionPolicy::default(),
            keys: Rc::new(RefCell::new(Pool::with_capacity(1024))),
        }
    }

    /// Creates an Engine containing Hammer's standard built-in algorithms.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::MalformedAlgorithmName`] if a built-in name no
    /// longer satisfies the canonical algorithm-name contract.
    pub fn with_builtins(instructions: InstructionSet) -> Result<Self, RegistryError> {
        let hash_capabilities = Capabilities::CONTIGUOUS_INPUT
            | Capabilities::SCATTER_INPUT
            | Capabilities::OUT_OF_PLACE;
        let aead_capabilities =
            hash_capabilities | Capabilities::IN_PLACE | Capabilities::ASSOCIATED_DATA;
        let kdf_capabilities = Capabilities::CONTIGUOUS_INPUT | Capabilities::SCATTER_INPUT;
        let kx_capabilities = Capabilities::CONTIGUOUS_INPUT | Capabilities::OUT_OF_PLACE;
        let mut engine = Self::new(instructions);
        let hash_registration = Registration::new()
            .with_algorithm("sha-256", hash_capabilities)
            .with_algorithm("sha-384", hash_capabilities)
            .with_algorithm("sha-512", hash_capabilities)
            .with_algorithm("blake2s-256", hash_capabilities)
            .with_algorithm("blake2b-512", hash_capabilities)
            .with_implementation(
                ImplementationRegistration::<Hash>::new("hammer:hash-portable", 0, true)
                    .with_algorithm(
                        "sha-256",
                        hash_capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Sha256>,
                    )
                    .with_algorithm(
                        "sha-384",
                        hash_capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Sha384>,
                    )
                    .with_algorithm(
                        "sha-512",
                        hash_capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Sha512>,
                    )
                    .with_algorithm(
                        "blake2s-256",
                        hash_capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Blake2s256>,
                    )
                    .with_algorithm(
                        "blake2b-512",
                        hash_capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Blake2b512>,
                    ),
            );
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        let hash_registration = hash_registration.with_implementation(
            ImplementationRegistration::<Hash>::new("hammer:sha-256-sha-ni", 100, true)
                .with_instruction_set(InstructionSet::SHA2)
                .with_algorithm("sha-256", hash_capabilities, (), HashPrepared::execute_sha2),
        );
        #[cfg(target_arch = "aarch64")]
        let hash_registration = hash_registration.with_implementation(
            ImplementationRegistration::<Hash>::new("hammer:sha-256-armv8", 100, true)
                .with_instruction_set(InstructionSet::SHA2)
                .with_algorithm("sha-256", hash_capabilities, (), HashPrepared::execute_sha2),
        );
        engine.publish(hash_registration)?;
        engine.publish(
            Registration::new()
                .with_algorithm("aes-128-gcm", aead_capabilities)
                .with_algorithm("aes-256-gcm", aead_capabilities)
                .with_algorithm("chacha20-poly1305", aead_capabilities)
                .with_implementation(
                    ImplementationRegistration::<Aead>::new("hammer:aead-portable", 0, true)
                        .with_algorithm(
                            "aes-128-gcm",
                            aead_capabilities,
                            <hammer_infra::crypto::aead::Aes128Gcm as hammer_infra::crypto::aead::Algorithm>::KEY_LEN,
                            AeadPrepared::execute::<hammer_infra::crypto::aead::Aes128Gcm>,
                        )
                        .with_algorithm(
                            "aes-256-gcm",
                            aead_capabilities,
                            <hammer_infra::crypto::aead::Aes256Gcm as hammer_infra::crypto::aead::Algorithm>::KEY_LEN,
                            AeadPrepared::execute::<hammer_infra::crypto::aead::Aes256Gcm>,
                        )
                        .with_algorithm(
                            "chacha20-poly1305",
                            aead_capabilities,
                            <hammer_infra::crypto::aead::ChaCha20Poly1305 as hammer_infra::crypto::aead::Algorithm>::KEY_LEN,
                            AeadPrepared::execute::<hammer_infra::crypto::aead::ChaCha20Poly1305>,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm("hmac-sha-256", hash_capabilities)
                .with_algorithm("hmac-sha-384", hash_capabilities)
                .with_algorithm("hmac-sha-512", hash_capabilities)
                .with_implementation(
                    ImplementationRegistration::<Mac>::new("hammer:hmac-portable", 0, true)
                        .with_algorithm(
                            "hmac-sha-256",
                            hash_capabilities,
                            (),
                            MacPrepared::execute::<hammer_infra::crypto::mac::HmacSha256>,
                        )
                        .with_algorithm(
                            "hmac-sha-384",
                            hash_capabilities,
                            (),
                            MacPrepared::execute::<hammer_infra::crypto::mac::HmacSha384>,
                        )
                        .with_algorithm(
                            "hmac-sha-512",
                            hash_capabilities,
                            (),
                            MacPrepared::execute::<hammer_infra::crypto::mac::HmacSha512>,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm("hkdf-sha-256", kdf_capabilities)
                .with_algorithm("hkdf-sha-384", kdf_capabilities)
                .with_algorithm("hkdf-sha-512", kdf_capabilities)
                .with_implementation(
                    ImplementationRegistration::<Kdf>::new("hammer:hkdf-portable", 0, true)
                        .with_algorithm(
                            "hkdf-sha-256",
                            kdf_capabilities,
                            (),
                            KdfPrepared::execute::<hammer_infra::crypto::kdf::HkdfSha256>,
                        )
                        .with_algorithm(
                            "hkdf-sha-384",
                            kdf_capabilities,
                            (),
                            KdfPrepared::execute::<hammer_infra::crypto::kdf::HkdfSha384>,
                        )
                        .with_algorithm(
                            "hkdf-sha-512",
                            kdf_capabilities,
                            (),
                            KdfPrepared::execute::<hammer_infra::crypto::kdf::HkdfSha512>,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm("x25519", kx_capabilities)
                .with_algorithm("p-256", kx_capabilities)
                .with_algorithm("p-384", kx_capabilities)
                .with_algorithm("ml-kem-768", kx_capabilities)
                .with_implementation(
                    ImplementationRegistration::<Kx>::new("hammer:kx-portable", 0, true)
                        .with_algorithm(
                            "x25519",
                            kx_capabilities,
                            (),
                            KxPrepared::execute::<hammer_infra::crypto::key_establishment::X25519>,
                        )
                        .with_algorithm(
                            "p-256",
                            kx_capabilities,
                            (),
                            KxPrepared::execute::<hammer_infra::crypto::key_establishment::P256>,
                        )
                        .with_algorithm(
                            "p-384",
                            kx_capabilities,
                            (),
                            KxPrepared::execute::<hammer_infra::crypto::key_establishment::P384>,
                        )
                        .with_algorithm(
                            "ml-kem-768",
                            kx_capabilities,
                            (),
                            KxPrepared::execute::<hammer_infra::crypto::key_establishment::MlKem768>,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm("ed25519", hash_capabilities)
                .with_algorithm("ecdsa-p-256-sha-256", hash_capabilities)
                .with_algorithm("ecdsa-p-384-sha-384", hash_capabilities)
                .with_algorithm("rsa-pss-sha-256", hash_capabilities)
                .with_algorithm("rsa-pss-sha-384", hash_capabilities)
                .with_algorithm("rsa-pss-sha-512", hash_capabilities)
                .with_implementation(
                    ImplementationRegistration::<Sign>::new("hammer:sign-portable", 0, true)
                        .with_algorithm(
                            "ed25519",
                            hash_capabilities,
                            (),
                            SignPrepared::execute::<hammer_infra::crypto::signature::Ed25519>,
                        )
                        .with_algorithm(
                            "ecdsa-p-256-sha-256",
                            hash_capabilities,
                            (),
                            SignPrepared::execute::<
                                hammer_infra::crypto::signature::EcdsaP256Sha256,
                            >,
                        )
                        .with_algorithm(
                            "ecdsa-p-384-sha-384",
                            hash_capabilities,
                            (),
                            SignPrepared::execute::<
                                hammer_infra::crypto::signature::EcdsaP384Sha384,
                            >,
                        )
                        .with_algorithm(
                            "rsa-pss-sha-256",
                            hash_capabilities,
                            (),
                            SignPrepared::execute::<
                                hammer_infra::crypto::signature::RsaPssSha256,
                            >,
                        )
                        .with_algorithm(
                            "rsa-pss-sha-384",
                            hash_capabilities,
                            (),
                            SignPrepared::execute::<
                                hammer_infra::crypto::signature::RsaPssSha384,
                            >,
                        )
                        .with_algorithm(
                            "rsa-pss-sha-512",
                            hash_capabilities,
                            (),
                            SignPrepared::execute::<
                                hammer_infra::crypto::signature::RsaPssSha512,
                            >,
                        ),
                ),
        )?;
        engine.publish(
            Registration::new()
                .with_algorithm("ed25519", hash_capabilities)
                .with_algorithm("ecdsa-p-256-sha-256", hash_capabilities)
                .with_algorithm("ecdsa-p-384-sha-384", hash_capabilities)
                .with_algorithm("rsa-pss-sha-256", hash_capabilities)
                .with_algorithm("rsa-pss-sha-384", hash_capabilities)
                .with_algorithm("rsa-pss-sha-512", hash_capabilities)
                .with_implementation(
                    ImplementationRegistration::<Verify>::new(
                        "hammer:verify-portable",
                        0,
                        true,
                    )
                    .with_algorithm(
                        "ed25519",
                        hash_capabilities,
                        (),
                        VerifyPrepared::execute::<hammer_infra::crypto::signature::Ed25519>,
                    )
                    .with_algorithm(
                        "ecdsa-p-256-sha-256",
                        hash_capabilities,
                        (),
                        VerifyPrepared::execute::<
                            hammer_infra::crypto::signature::EcdsaP256Sha256,
                        >,
                    )
                    .with_algorithm(
                        "ecdsa-p-384-sha-384",
                        hash_capabilities,
                        (),
                        VerifyPrepared::execute::<
                            hammer_infra::crypto::signature::EcdsaP384Sha384,
                        >,
                    )
                    .with_algorithm(
                        "rsa-pss-sha-256",
                        hash_capabilities,
                        (),
                        VerifyPrepared::execute::<
                            hammer_infra::crypto::signature::RsaPssSha256,
                        >,
                    )
                    .with_algorithm(
                        "rsa-pss-sha-384",
                        hash_capabilities,
                        (),
                        VerifyPrepared::execute::<
                            hammer_infra::crypto::signature::RsaPssSha384,
                        >,
                    )
                    .with_algorithm(
                        "rsa-pss-sha-512",
                        hash_capabilities,
                        (),
                        VerifyPrepared::execute::<
                            hammer_infra::crypto::signature::RsaPssSha512,
                        >,
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
        self.validate_registration(&registration)?;
        F::registry_mut(self).publish(registration);
        Ok(())
    }

    /// Resolves a canonical name to a family-typed process-local identity.
    pub fn algorithm<F: Family>(&self, name: &str) -> Option<AlgorithmId<F>> {
        F::registry(self)
            .algorithm_names
            .get(name)
            .copied()
            .map(AlgorithmId::new)
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

    /// Changes selection priority for new Contexts without migrating existing Contexts.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::ImplementationUnknown`] when `name` is absent
    /// from this operation family.
    pub fn set_implementation_priority<F: Family>(
        &mut self,
        name: &str,
        priority: i32,
    ) -> Result<(), RegistryError> {
        F::registry_mut(self).set_priority(name, priority)
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
        let (implementation, algorithm_functions) = F::registry(self)
            .select(
                algorithm,
                &self.selection_policy,
                self.instructions,
                None,
                None,
            )
            .ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;
        let functions = Rc::clone(&algorithm_functions.functions);
        let prepared = (functions.prepare)(
            &functions.state,
            &implementation.state,
            self,
            algorithm,
            None,
        )?;
        Ok(Context {
            algorithm,
            implementation: implementation.name.clone(),
            implementation_state: Rc::clone(&implementation.state),
            functions,
            prepared,
            key_retention: None,
            available: Rc::clone(&implementation.available),
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
                state: OwnedState::new(Zeroizing::new(material.to_vec())),
                policy,
                contexts: 0,
                provenance: None,
            })
            .ok_or(KeyError::PoolFull { capacity })?;
        Ok(KeyHandle { index })
    }

    /// Generates a non-exportable key through one selected implementation.
    ///
    /// # Errors
    ///
    /// Returns a typed policy, availability, implementation-resource, or Engine
    /// key-capacity failure without publishing a partial key.
    pub fn generate_key<F: Family>(
        &self,
        algorithm: AlgorithmId<F>,
        mut policy: KeyPolicy,
    ) -> Result<KeyHandle, KeyError> {
        if !policy.applies_to(PolicyAlgorithm::new(algorithm)) {
            return Err(KeyError::GenerationPolicyDenied {
                algorithm: algorithm.slot,
            });
        }
        let (implementation, key_generation) = F::registry(self)
            .select_key_generation(algorithm, &self.selection_policy, self.instructions)
            .ok_or(KeyError::GenerationUnavailable {
                algorithm: algorithm.slot,
            })?;
        let state = (key_generation.generate)(&key_generation.functions, &implementation.state)?;
        policy.restrict_to(key_generation.operations);

        let mut keys = self.keys.borrow_mut();
        let capacity = keys.capacity();
        let index = keys
            .insert(KeyEntry {
                state,
                policy,
                contexts: 0,
                provenance: Some(KeyProvenance {
                    name: implementation.name.clone(),
                    state: Rc::clone(&implementation.state),
                }),
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
        let material = entry
            .state
            .value::<Zeroizing<Vec<u8>>>()
            .expect("exportable keys retain Engine-owned software material");
        if output.len() < material.len() {
            return Err(KeyError::OutputTooSmall {
                key,
                required: material.len(),
                provided: output.len(),
            });
        }
        output[..material.len()].copy_from_slice(material);
        Ok(material.len())
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
        let mut keys = self.keys.borrow_mut();
        let entry = keys
            .get_mut(key.index)
            .ok_or(ContextError::StaleKey { key })?;
        if !entry.policy.applies_to(PolicyAlgorithm::new(algorithm)) {
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
        let owner = entry
            .provenance
            .as_ref()
            .map(|implementation| &implementation.state);
        let selection = F::registry(self).select(
            algorithm,
            &self.selection_policy,
            self.instructions,
            Some(entry.state.value_type),
            owner,
        );
        let (implementation, algorithm_functions) = match selection {
            Some(selection) => selection,
            None => {
                return Err(match &entry.provenance {
                    Some(implementation) => ContextError::ImplementationUnavailable {
                        implementation: implementation.name.clone(),
                    },
                    None => ContextError::AlgorithmUnavailable {
                        algorithm: algorithm.slot,
                    },
                });
            }
        };
        let functions = Rc::clone(&algorithm_functions.functions);
        let prepared = (functions.prepare)(
            &functions.state,
            &implementation.state,
            self,
            algorithm,
            Some((key, &entry.state, &entry.policy)),
        )?;
        entry.contexts = entry
            .contexts
            .checked_add(1)
            .expect("key context reference count overflow");
        drop(keys);

        Ok(Context {
            algorithm,
            implementation: implementation.name.clone(),
            implementation_state: Rc::clone(&implementation.state),
            functions,
            prepared,
            key_retention: Some(KeyRetention {
                keys: Rc::clone(&self.keys),
                key,
            }),
            available: Rc::clone(&implementation.available),
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
/// let engine = Engine::with_builtins(hammer_infra::crypto::InstructionSet::detect()).unwrap();
/// let algorithm = engine.algorithm::<Hash>("sha-256").unwrap();
/// let context = engine.context(algorithm).unwrap();
/// std::thread::spawn(move || drop(context));
/// ```
pub struct Context<F: Family> {
    algorithm: AlgorithmId<F>,
    implementation: String,
    prepared: OwnedState,
    key_retention: Option<KeyRetention>,
    implementation_state: Rc<RefCell<OwnedState>>,
    functions: Rc<AlgorithmFunctions<F>>,
    available: Rc<Cell<bool>>,
}

impl<F: Family> fmt::Debug for Context<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Context")
            .field("algorithm", &self.algorithm.slot)
            .field("implementation", &self.implementation)
            .field("prepared", &self.prepared)
            .field("key_retention", &self.key_retention)
            .finish_non_exhaustive()
    }
}

impl<F: Family> Context<F> {
    /// Returns the implementation permanently bound to this Context.
    pub fn implementation_name(&self) -> &str {
        &self.implementation
    }

    /// Returns the algorithm permanently bound to this Context.
    pub fn algorithm(&self) -> AlgorithmId<F> {
        self.algorithm
    }

    /// Executes all operations synchronously through one batch dispatch.
    pub fn execute<'data>(
        &mut self,
        operations: &mut [F::Operation<'data>],
    ) -> Result<(), ContextError> {
        if !self.available.get() {
            return Err(ContextError::ImplementationUnavailable {
                implementation: self.implementation.clone(),
            });
        }
        (self.functions.dispatch)(
            &self.functions.state,
            &self.implementation_state,
            &mut self.prepared,
            operations,
        )
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
    /// Key generation permits operations outside this operation family.
    #[error(
        "implementation `{implementation}` key generation permits {provided:?}, outside family operations {supported:?}"
    )]
    KeyGenerationOperationsUnsupported {
        /// Rejected Crypto Implementation Name.
        implementation: String,
        /// Operations declared by the implementation.
        provided: KeyOperations,
        /// Operations owned by this family.
        supported: KeyOperations,
    },
    /// Key generation produces a key type accepted by none of the implementation's algorithms.
    #[error("implementation `{implementation}` key generation has no matching keyed algorithm")]
    KeyGenerationWithoutAlgorithm {
        /// Rejected Crypto Implementation Name.
        implementation: String,
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
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum ContextError {
    /// No currently available implementation supports the algorithm.
    #[error("algorithm slot {algorithm} has no available implementation")]
    AlgorithmUnavailable {
        /// Process-local algorithm slot.
        algorithm: u32,
    },
    /// A Context's permanently bound implementation is no longer available.
    #[error("implementation `{implementation}` is unavailable for its bound Context")]
    ImplementationUnavailable {
        /// Crypto Implementation Name selected when the Context was created.
        implementation: String,
    },
    /// Injected capability facts selected an instruction implementation that this CPU cannot run.
    #[error(
        "required cryptographic instructions {required:?} are absent from detected set {available:?}"
    )]
    InstructionsUnavailable {
        /// Instructions required by the selected implementation.
        required: InstructionSet,
        /// Instructions detected immediately before dispatch.
        available: InstructionSet,
    },
    /// The selected implementation cannot currently open or use its session.
    #[error("implementation `{implementation}` has no available cryptographic session")]
    SessionUnavailable {
        /// Permanently selected Crypto Implementation Name.
        implementation: String,
    },
    /// The selected implementation rejected this batch without executing it.
    #[error("implementation `{implementation}` rejected the cryptographic operation batch")]
    OperationRejected {
        /// Permanently selected Crypto Implementation Name.
        implementation: String,
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
        source: hammer_infra::crypto::aead::Error,
    },
}

/// A failure owned by the Engine's opaque key lifecycle.
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum KeyError {
    /// The fixed-capacity key authority has no free slot.
    #[error("key pool capacity {capacity} is exhausted")]
    PoolFull {
        /// Configured fixed capacity.
        capacity: usize,
    },
    /// No selected implementation can generate a key for this algorithm.
    #[error("algorithm slot {algorithm} has no available key-generating implementation")]
    GenerationUnavailable {
        /// Process-local algorithm slot.
        algorithm: u32,
    },
    /// The supplied policy belongs to another family or algorithm.
    #[error("key-generation policy does not permit algorithm slot {algorithm}")]
    GenerationPolicyDenied {
        /// Rejected process-local algorithm slot.
        algorithm: u32,
    },
    /// The selected implementation exhausted its own key resources.
    #[error(
        "implementation `{implementation}` exhausted its cryptographic resource capacity {capacity}"
    )]
    ImplementationResourcesExhausted {
        /// Selected Crypto Implementation Name.
        implementation: String,
        /// Implementation-owned capacity.
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

impl Engine {
    fn validate_registration<F: Family>(
        &self,
        registration: &Registration<F>,
    ) -> Result<(), RegistryError> {
        let registry = F::registry(self);
        let valid_name_component = |name: &str| {
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && !name.starts_with('-')
                && !name.ends_with('-')
                && !name.contains("--")
        };
        if registry
            .algorithms
            .len()
            .checked_add(registration.algorithms.len())
            .is_none_or(|len| u32::try_from(len.saturating_sub(1)).is_err())
        {
            return Err(RegistryError::AlgorithmCapacityExhausted);
        }

        let mut algorithms = HashMap::new();
        for (name, required) in &registration.algorithms {
            let mut components = name.split(':');
            let first = components.next().unwrap_or_default();
            let second = components.next();
            let valid = valid_name_component(first)
                && second.is_none_or(valid_name_component)
                && components.next().is_none();
            if !valid {
                return Err(RegistryError::MalformedAlgorithmName { name: name.clone() });
            }
            if registry.algorithm_names.contains_key(name)
                || algorithms.insert(name.as_str(), *required).is_some()
            {
                return Err(RegistryError::AlgorithmCollision { name: name.clone() });
            }
        }

        let mut implementations = BTreeSet::new();
        for implementation in &registration.implementations {
            let mut components = implementation.name.split(':');
            let valid = components.next().is_some_and(valid_name_component)
                && components.next().is_some_and(valid_name_component)
                && components.next().is_none();
            if !valid {
                return Err(RegistryError::MalformedImplementationName {
                    name: implementation.name.clone(),
                });
            }
            if self
                .aeads
                .implementation_names
                .contains_key(&implementation.name)
                || self
                    .ciphers
                    .implementation_names
                    .contains_key(&implementation.name)
                || self
                    .hashes
                    .implementation_names
                    .contains_key(&implementation.name)
                || self
                    .macs
                    .implementation_names
                    .contains_key(&implementation.name)
                || self
                    .kdfs
                    .implementation_names
                    .contains_key(&implementation.name)
                || self
                    .key_exchanges
                    .implementation_names
                    .contains_key(&implementation.name)
                || self
                    .signers
                    .implementation_names
                    .contains_key(&implementation.name)
                || self
                    .verifiers
                    .implementation_names
                    .contains_key(&implementation.name)
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
                        self.algorithm::<F>(&functions.name)
                            .and_then(|algorithm| registry.algorithms.get(algorithm.slot as usize))
                            .copied()
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
            if let Some(key_generation) = &implementation.key_generation {
                let supported = F::key_operations();
                if key_generation.operations.is_empty()
                    || !supported.contains(key_generation.operations)
                {
                    return Err(RegistryError::KeyGenerationOperationsUnsupported {
                        implementation: implementation.name.clone(),
                        provided: key_generation.operations,
                        supported,
                    });
                }
                if !implementation
                    .algorithms
                    .iter()
                    .any(|algorithm| algorithm.functions.key_type == Some(key_generation.key_type))
                {
                    return Err(RegistryError::KeyGenerationWithoutAlgorithm {
                        implementation: implementation.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}
