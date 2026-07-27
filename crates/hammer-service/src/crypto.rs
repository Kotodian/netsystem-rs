//! Synchronous typed cryptographic execution.
//!
//! `hammer-service` owns algorithm identity, implementation selection, and
//! operation lifecycle. Portable algorithm semantics remain in
//! `hammer-infra`; `hammer-runtime` does not participate in this boundary.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::marker::PhantomData;
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

/// The hash operation family.
#[hammer_component_macros::Hash]
#[derive(Debug)]
pub struct Hash;

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

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    fn execute_sha_ni<A>(
        &mut self,
        operations: &mut [HashOperation<'_>],
    ) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::hash::Algorithm,
    {
        let available = InstructionSet::detect();
        if !available.contains(InstructionSet::SHA2) {
            return Err(ContextError::InstructionsUnavailable {
                required: InstructionSet::SHA2,
                available,
            });
        }
        // SAFETY: SHA-NI support was detected on this CPU immediately above.
        unsafe { self.execute_sha_ni_unchecked::<A>(operations) }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "sha")]
    unsafe fn execute_sha_ni_unchecked<A>(
        &mut self,
        operations: &mut [HashOperation<'_>],
    ) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::hash::Algorithm,
    {
        for operation in operations {
            operation.result = Some(operation.input.with_fragments(|input| {
                // SAFETY: the safe batch entry point detects SHA-NI before calling this method.
                unsafe { A::default().digest_sha_ni(input, operation.output) }
            }));
        }
        Ok(())
    }

    #[cfg(target_arch = "aarch64")]
    fn execute_sha2_armv8<A>(
        &mut self,
        operations: &mut [HashOperation<'_>],
    ) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::hash::Algorithm,
    {
        let available = InstructionSet::detect();
        if !available.contains(InstructionSet::SHA2) {
            return Err(ContextError::InstructionsUnavailable {
                required: InstructionSet::SHA2,
                available,
            });
        }
        // SAFETY: Armv8 SHA-2 support was detected on this CPU immediately above.
        unsafe { self.execute_sha2_armv8_unchecked::<A>(operations) }
    }

    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "sha2")]
    unsafe fn execute_sha2_armv8_unchecked<A>(
        &mut self,
        operations: &mut [HashOperation<'_>],
    ) -> Result<(), ContextError>
    where
        A: hammer_infra::crypto::hash::Algorithm,
    {
        for operation in operations {
            operation.result = Some(operation.input.with_fragments(|input| {
                // SAFETY: the safe batch entry point detects Armv8 SHA-2 before calling this method.
                unsafe { A::default().digest_sha2_armv8(input, operation.output) }
            }));
        }
        Ok(())
    }
}

/// The authenticated-encryption operation family.
#[hammer_component_macros::Aead]
#[derive(Debug)]
pub struct Aead;

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
            let required = match operation.direction {
                AeadDirection::Seal => KeyOperations::AEAD_SEAL,
                AeadDirection::Open => KeyOperations::AEAD_OPEN,
            };
            if !self.operations.contains(required) {
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
                    cipher.seal(
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
                    cipher.open(
                        fragments,
                        operation.nonce,
                        operation.associated_data,
                        tag,
                        output,
                    )
                }),
                (AeadDirection::Seal, AeadPayload::InPlace(payload), AeadTag::Output(tag)) => {
                    cipher.seal_in_place(payload, operation.nonce, operation.associated_data, tag)
                }
                (AeadDirection::Open, AeadPayload::InPlace(payload), AeadTag::Input(tag)) => {
                    cipher.open_in_place(payload, operation.nonce, operation.associated_data, tag)
                }
                _ => unreachable!("AEAD constructors preserve direction, payload, and tag shape"),
            };
            operation.status = AeadStatus::Executed(result);
        }
        Ok(())
    }
}

/// The message-authentication operation family.
#[hammer_component_macros::Mac]
#[derive(Debug)]
pub struct Mac;

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

/// The key-derivation operation family.
#[hammer_component_macros::Kdf]
#[derive(Debug)]
pub struct Kdf;

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
        Ok(())
    }
}

/// The key-establishment operation family.
#[hammer_component_macros::Kx]
#[derive(Debug)]
pub struct Kx;

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
            if let Err(error) =
                algorithm.agree(&entry.material, peer_public_key, &mut shared_secret)
            {
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
            if let Err(error) =
                algorithm.decapsulate(&entry.material, ciphertext, &mut shared_secret)
            {
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
                material,
                policy,
                contexts: 0,
            })
            .ok_or(KxStatus::KeyPoolFull { capacity })?;
        Ok(KeyHandle { index })
    }
}

/// The digital-signing operation family.
#[hammer_component_macros::Sign]
#[derive(Debug)]
pub struct Sign;

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

/// The digital-signature verification operation family.
#[hammer_component_macros::Verify]
#[derive(Debug)]
pub struct Verify;

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

/// The unauthenticated-cipher operation family.
#[hammer_component_macros::Cipher]
#[derive(Debug)]
pub struct Cipher;

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

/// One implementation declaration awaiting failure-atomic publication.
pub struct ImplementationRegistration<F: Family> {
    name: String,
    algorithms: Vec<AlgorithmImplementationRegistration<F>>,
    priority: i32,
    available: bool,
    instructions: InstructionSet,
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
            instructions: InstructionSet::empty(),
        }
    }

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
    algorithms: Vec<(String, Capabilities)>,
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

    /// Adds one canonical algorithm declaration to the bundle.
    pub fn with_algorithm(mut self, name: impl Into<String>, required: Capabilities) -> Self {
        self.algorithms.push((name.into(), required));
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
    ) -> Option<(&str, F::Prepare, F::Dispatch, Rc<Cell<bool>>)> {
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
                    Rc::clone(&implementation.available),
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
                available: Rc::new(Cell::new(implementation.available)),
                instructions: implementation.instructions,
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
                .with_algorithm(
                    "sha-256",
                    hash_capabilities,
                    (),
                    HashPrepared::execute_sha_ni::<hammer_infra::crypto::hash::Sha256>,
                ),
        );
        #[cfg(target_arch = "aarch64")]
        let hash_registration = hash_registration.with_implementation(
            ImplementationRegistration::<Hash>::new("hammer:sha-256-armv8", 100, true)
                .with_instruction_set(InstructionSet::SHA2)
                .with_algorithm(
                    "sha-256",
                    hash_capabilities,
                    (),
                    HashPrepared::execute_sha2_armv8::<hammer_infra::crypto::hash::Sha256>,
                ),
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
        let (implementation, prepare, dispatch, available) = F::registry(self)
            .select(algorithm, &self.selection_policy, self.instructions)
            .ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;
        let prepared =
            F::prepare_unkeyed(prepare, self, algorithm).ok_or(ContextError::KeyRequired {
                algorithm: algorithm.slot,
            })?;
        Ok(Context {
            algorithm,
            implementation: implementation.to_owned(),
            dispatch,
            prepared,
            key_ref: None,
            available,
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
        let (implementation, prepare, dispatch, available) = registry
            .select(algorithm, &self.selection_policy, self.instructions)
            .ok_or(ContextError::AlgorithmUnavailable {
                algorithm: algorithm.slot,
            })?;

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
            available,
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
/// let engine = Engine::with_builtins(hammer_infra::crypto::InstructionSet::detect()).unwrap();
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
    available: Rc<Cell<bool>>,
    // Rc is deliberately part of the marker: Context must be neither Send nor Sync.
    thread_bound: PhantomData<Rc<()>>,
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
    ) -> Result<(), ContextError>
    where
        F::Dispatch: Fn(&mut F::Prepared, &mut [F::Operation<'data>]) -> Result<(), ContextError>,
    {
        if !self.available.get() {
            return Err(ContextError::ImplementationUnavailable {
                implementation: self.implementation.clone(),
            });
        }
        (self.dispatch)(&mut self.prepared, operations)
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

impl Engine {
    fn validate_registration<F: Family>(
        &self,
        registration: &Registration<F>,
    ) -> Result<(), RegistryError> {
        let registry = F::registry(self);
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
        }
        Ok(())
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
