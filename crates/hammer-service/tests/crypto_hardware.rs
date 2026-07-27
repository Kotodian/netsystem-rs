use std::cell::RefCell;
use std::rc::Rc;

use hammer_infra::crypto::{InstructionSet, mac::Algorithm as _};
use hammer_service::crypto::{
    AlgorithmId, Capabilities, Context, ContextError, Engine, ImplementationRegistration, Input,
    KeyError, KeyHandle, KeyOperations, KeyPolicy, Mac, MacOperation, Registration, RegistryError,
    SelectionPolicy,
};

const IMPLEMENTATION: &str = "test:hardware-mac";
const CAPABILITIES: Capabilities = Capabilities::CONTIGUOUS_INPUT
    .union(Capabilities::SCATTER_INPUT)
    .union(Capabilities::OUT_OF_PLACE);
const HMAC_SHA256: [u8; 32] = [
    0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b, 0xf1, 0x2b,
    0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37, 0x6c, 0x2e, 0x32, 0xcf, 0xf7,
];

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    ContextReleased(u32),
    ContextCleanupFailed(u32),
    KeyReleased(u32),
}

#[derive(Debug)]
struct HardwareState {
    next_key: u32,
    live_keys: usize,
    key_capacity: usize,
    session_available: bool,
    reject_batches: bool,
    fail_context_cleanup: bool,
    events: Vec<Event>,
}

#[derive(Clone, Debug)]
struct Hardware(Rc<RefCell<HardwareState>>);

impl Hardware {
    fn new(key_capacity: usize) -> Self {
        Self(Rc::new(RefCell::new(HardwareState {
            next_key: 1,
            live_keys: 0,
            key_capacity,
            session_available: true,
            reject_batches: false,
            fail_context_cleanup: false,
            events: Vec::new(),
        })))
    }

    fn generate_key(&mut self) -> Result<HardwareKey, KeyError> {
        let mut state = self.0.borrow_mut();
        if state.live_keys == state.key_capacity {
            return Err(KeyError::ImplementationResourcesExhausted {
                implementation: IMPLEMENTATION.to_owned(),
                capacity: state.key_capacity,
            });
        }
        let key = state.next_key;
        state.next_key += 1;
        state.live_keys += 1;
        drop(state);
        Ok(HardwareKey {
            key,
            hardware: self.clone(),
        })
    }

    fn prepare(
        &mut self,
        _: &Engine,
        _: AlgorithmId<Mac>,
        _: KeyHandle,
        key: &HardwareKey,
        _: &KeyPolicy,
    ) -> Result<HardwareContext, ContextError> {
        if !self.0.borrow().session_available {
            return Err(ContextError::SessionUnavailable {
                implementation: IMPLEMENTATION.to_owned(),
            });
        }
        Ok(HardwareContext {
            key: key.key,
            hardware: self.clone(),
        })
    }

    fn execute(
        &mut self,
        _: &mut HardwareContext,
        operations: &mut [MacOperation<'_>],
    ) -> Result<(), ContextError> {
        if !self.0.borrow().session_available {
            return Err(ContextError::SessionUnavailable {
                implementation: IMPLEMENTATION.to_owned(),
            });
        }
        if self.0.borrow().reject_batches {
            return Err(ContextError::OperationRejected {
                implementation: IMPLEMENTATION.to_owned(),
            });
        }

        let algorithm = hammer_infra::crypto::mac::HmacSha256::new(&[0x0b; 20]);
        for operation in operations {
            let input = operation.input();
            let result = input
                .with_fragments(|fragments| algorithm.authenticate(fragments, operation.output()));
            operation.complete(result);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct HardwareKey {
    key: u32,
    hardware: Hardware,
}

#[derive(Debug)]
struct ForeignKey;

impl Drop for HardwareKey {
    fn drop(&mut self) {
        let mut state = self.hardware.0.borrow_mut();
        state.live_keys -= 1;
        state.events.push(Event::KeyReleased(self.key));
    }
}

#[derive(Debug)]
struct HardwareContext {
    key: u32,
    hardware: Hardware,
}

impl Drop for HardwareContext {
    fn drop(&mut self) {
        let mut state = self.hardware.0.borrow_mut();
        let event = if state.fail_context_cleanup {
            Event::ContextCleanupFailed(self.key)
        } else {
            Event::ContextReleased(self.key)
        };
        state.events.push(event);
    }
}

fn engine(hardware: Hardware, operations: KeyOperations) -> (Engine, AlgorithmId<Mac>) {
    let mut engine = Engine::with_builtins(InstructionSet::empty()).expect("built-ins publish");
    engine
        .publish(
            Registration::new().with_implementation(
                ImplementationRegistration::<Mac>::new(IMPLEMENTATION, 100, true)
                    .with_state(hardware)
                    .with_key_generation(operations, Hardware::generate_key)
                    .with_keyed_algorithm(
                        "hmac-sha-256",
                        CAPABILITIES,
                        Hardware::prepare,
                        Hardware::execute,
                    ),
            ),
        )
        .expect("fake hardware implementation publishes");
    let algorithm = engine
        .algorithm::<Mac>("hmac-sha-256")
        .expect("HMAC-SHA-256 is built in");
    (engine, algorithm)
}

fn assert_hmac_sha256_conformance(context: &mut Context<Mac>) {
    let mut singleton_output = [0u8; 32];
    let mut singleton = [MacOperation::authenticate(
        Input::Contiguous(b"Hi There"),
        &mut singleton_output,
    )];
    context
        .execute(&mut singleton)
        .expect("singleton batch dispatches synchronously");
    assert_eq!(singleton[0].status(), Some(Ok(32)));
    assert_eq!(singleton_output, HMAC_SHA256);

    let fragments: &[&[u8]] = &[b"Hi ", b"There"];
    let mut scatter_output = [0u8; 32];
    let mut short_output = [0xa5; 31];
    let mut batch = [
        MacOperation::authenticate(Input::Scatter(fragments), &mut scatter_output),
        MacOperation::authenticate(Input::Contiguous(b"Hi There"), &mut short_output),
    ];
    context
        .execute(&mut batch)
        .expect("mixed multi-operation batch dispatches synchronously");
    assert_eq!(batch[0].status(), Some(Ok(32)));
    assert_eq!(
        batch[1].status(),
        Some(Err(hammer_infra::crypto::mac::Error::OutputTooSmall {
            required: 32,
            provided: 31,
        }))
    );
    assert_eq!(scatter_output, HMAC_SHA256);
    assert_eq!(short_output, [0xa5; 31]);
}

#[test]
fn portable_and_hardware_hmac_sha256_obey_one_conformance_suite() {
    let hardware = Hardware::new(1);
    let (mut engine, algorithm) = engine(hardware, KeyOperations::MAC_AUTHENTICATE);

    engine.set_selection_policy(SelectionPolicy::only(["hammer:hmac-portable"]));
    let software_key = engine
        .create_key(
            &[0x0b; 20],
            KeyPolicy::new(algorithm, KeyOperations::MAC_AUTHENTICATE, false),
        )
        .expect("software key installs");
    let mut portable = engine
        .context_with_key(algorithm, software_key)
        .expect("portable HMAC Context");

    engine.set_selection_policy(SelectionPolicy::only([IMPLEMENTATION]));
    let hardware_key = engine
        .generate_key(
            algorithm,
            KeyPolicy::new(algorithm, KeyOperations::MAC_AUTHENTICATE, false),
        )
        .expect("hardware key generation succeeds");
    let mut hardware = engine
        .context_with_key(algorithm, hardware_key)
        .expect("hardware HMAC Context");

    assert_hmac_sha256_conformance(&mut portable);
    assert_hmac_sha256_conformance(&mut hardware);
}

#[test]
fn hardware_policy_can_only_remove_requested_permissions() {
    let hardware = Hardware::new(2);
    let (engine, algorithm) = engine(hardware, KeyOperations::MAC_AUTHENTICATE);
    let denied = engine
        .generate_key(
            algorithm,
            KeyPolicy::new(algorithm, KeyOperations::SIGN, true),
        )
        .expect("provider may own a policy-restricted key");
    assert_eq!(
        engine
            .context_with_key(algorithm, denied)
            .expect_err("provider cannot add MAC permission absent from the request"),
        ContextError::OperationsDenied {
            key: denied,
            required: KeyOperations::MAC_AUTHENTICATE,
        }
    );
    assert_eq!(
        engine.export_secret(denied, &mut [0u8; 1]),
        Err(KeyError::SecretExportDenied { key: denied })
    );
}

#[test]
fn registration_rejects_key_permissions_owned_by_another_family_atomically() {
    let hardware = Hardware::new(1);
    let mut engine = Engine::with_builtins(InstructionSet::empty()).expect("built-ins publish");
    let error = engine
        .publish(
            Registration::new().with_implementation(
                ImplementationRegistration::<Mac>::new(IMPLEMENTATION, 100, true)
                    .with_state(hardware)
                    .with_key_generation(KeyOperations::SIGN, Hardware::generate_key)
                    .with_keyed_algorithm(
                        "hmac-sha-256",
                        CAPABILITIES,
                        Hardware::prepare,
                        Hardware::execute,
                    ),
            ),
        )
        .expect_err("MAC implementation cannot grant signing permission");

    assert_eq!(
        error,
        RegistryError::KeyGenerationOperationsUnsupported {
            implementation: IMPLEMENTATION.to_owned(),
            provided: KeyOperations::SIGN,
            supported: KeyOperations::MAC_AUTHENTICATE,
        }
    );
    assert!(engine.algorithm::<Mac>("hmac-sha-256").is_some());
}

#[test]
fn registration_rejects_unusable_generated_key_type_atomically() {
    let hardware = Hardware::new(1);
    let mut engine = Engine::with_builtins(InstructionSet::empty()).expect("built-ins publish");
    let error = engine
        .publish(
            Registration::new().with_implementation(
                ImplementationRegistration::<Mac>::new(IMPLEMENTATION, 100, true)
                    .with_state(hardware.clone())
                    .with_key_generation(KeyOperations::MAC_AUTHENTICATE, |_| Ok(ForeignKey))
                    .with_keyed_algorithm(
                        "hmac-sha-256",
                        CAPABILITIES,
                        Hardware::prepare,
                        Hardware::execute,
                    ),
            ),
        )
        .expect_err("generated key type must match a keyed algorithm");

    assert_eq!(
        error,
        RegistryError::KeyGenerationWithoutAlgorithm {
            implementation: IMPLEMENTATION.to_owned(),
        }
    );
    engine
        .publish(
            Registration::new().with_implementation(
                ImplementationRegistration::<Mac>::new(IMPLEMENTATION, 100, true)
                    .with_state(hardware)
                    .with_key_generation(KeyOperations::MAC_AUTHENTICATE, Hardware::generate_key)
                    .with_keyed_algorithm(
                        "hmac-sha-256",
                        CAPABILITIES,
                        Hardware::prepare,
                        Hardware::execute,
                    ),
            ),
        )
        .expect("failed registration leaves no implementation-name residue");
}

#[test]
fn hardware_key_is_non_exportable_and_shared_by_multiple_contexts() {
    let hardware = Hardware::new(1);
    let (engine, algorithm) = engine(hardware.clone(), KeyOperations::MAC_AUTHENTICATE);
    let key = engine
        .generate_key(
            algorithm,
            KeyPolicy::new(
                algorithm,
                KeyOperations::MAC_AUTHENTICATE | KeyOperations::SIGN,
                true,
            ),
        )
        .expect("hardware key generation succeeds");

    assert_eq!(
        engine.export_secret(key, &mut [0u8; 32]),
        Err(KeyError::SecretExportDenied { key })
    );

    let first = engine
        .context_with_key(algorithm, key)
        .expect("first Context shares the hardware key");
    let second = engine
        .context_with_key(algorithm, key)
        .expect("second Context shares the hardware key");
    assert_eq!(
        engine.destroy_key(key),
        Err(KeyError::KeyInUse { key, contexts: 2 })
    );

    drop(first);
    assert_eq!(
        engine.destroy_key(key),
        Err(KeyError::KeyInUse { key, contexts: 1 })
    );
    drop(second);
    engine.destroy_key(key).expect("released key is destroyed");
    assert_eq!(
        hardware.0.borrow().events,
        [
            Event::ContextReleased(1),
            Event::ContextReleased(1),
            Event::KeyReleased(1),
        ]
    );
}

#[test]
fn provider_failures_are_typed_and_bound_context_never_falls_back() {
    let hardware = Hardware::new(1);
    let (mut engine, algorithm) = engine(hardware.clone(), KeyOperations::MAC_AUTHENTICATE);
    let key = engine
        .generate_key(
            algorithm,
            KeyPolicy::new(algorithm, KeyOperations::MAC_AUTHENTICATE, false),
        )
        .expect("hardware key generation succeeds");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("hardware Context is prepared");
    assert_eq!(context.implementation_name(), IMPLEMENTATION);

    hardware.0.borrow_mut().session_available = false;
    let mut output = [0u8; 1];
    let mut operations = [MacOperation::authenticate(
        Input::Contiguous(b"session"),
        &mut output,
    )];
    assert_eq!(
        context.execute(&mut operations),
        Err(ContextError::SessionUnavailable {
            implementation: IMPLEMENTATION.to_owned(),
        })
    );
    assert_eq!(operations[0].status(), None);

    hardware.0.borrow_mut().session_available = true;
    hardware.0.borrow_mut().reject_batches = true;
    assert_eq!(
        context.execute(&mut operations),
        Err(ContextError::OperationRejected {
            implementation: IMPLEMENTATION.to_owned(),
        })
    );
    assert_eq!(operations[0].status(), None);

    engine
        .set_implementation_availability::<Mac>(IMPLEMENTATION, false)
        .expect("hardware implementation is registered");
    assert_eq!(
        context.execute(&mut operations),
        Err(ContextError::ImplementationUnavailable {
            implementation: IMPLEMENTATION.to_owned(),
        })
    );
    assert_eq!(context.implementation_name(), IMPLEMENTATION);
    assert_eq!(operations[0].status(), None);
    assert_eq!(
        engine
            .context_with_key(algorithm, key)
            .expect_err("provider-owned key cannot fall back"),
        ContextError::ImplementationUnavailable {
            implementation: IMPLEMENTATION.to_owned(),
        }
    );
}

#[test]
fn resource_and_cleanup_failures_preserve_existing_state_and_primary_error() {
    let hardware = Hardware::new(1);
    let (engine, algorithm) = engine(hardware.clone(), KeyOperations::MAC_AUTHENTICATE);
    let policy = KeyPolicy::new(algorithm, KeyOperations::MAC_AUTHENTICATE, false);
    let key = engine
        .generate_key(algorithm, policy.clone())
        .expect("first hardware key fits");
    assert_eq!(
        engine.generate_key(algorithm, policy),
        Err(KeyError::ImplementationResourcesExhausted {
            implementation: IMPLEMENTATION.to_owned(),
            capacity: 1,
        })
    );

    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("failed second generation did not damage the first key");
    hardware.0.borrow_mut().reject_batches = true;
    hardware.0.borrow_mut().fail_context_cleanup = true;
    let mut output = [0u8; 1];
    let mut operations = [MacOperation::authenticate(
        Input::Contiguous(b"reject"),
        &mut output,
    )];
    let operation_error = context
        .execute(&mut operations)
        .expect_err("hardware rejects this batch");
    assert_eq!(
        operation_error,
        ContextError::OperationRejected {
            implementation: IMPLEMENTATION.to_owned(),
        }
    );

    drop(context);
    assert_eq!(hardware.0.borrow().events, [Event::ContextCleanupFailed(1)]);
    engine
        .destroy_key(key)
        .expect("observable cleanup failure does not leak the key reference");
    assert_eq!(
        hardware.0.borrow().events,
        [Event::ContextCleanupFailed(1), Event::KeyReleased(1)]
    );
}
