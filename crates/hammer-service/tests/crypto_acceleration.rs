use hammer_infra::crypto::InstructionSet;
use hammer_service::crypto::{
    Capabilities, Context, ContextError, Engine, Hash, HashOperation, HashPrepared,
    ImplementationRegistration, Input, Registration, SelectionPolicy,
};

const SHA256_ABC: [u8; 32] = [
    0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
    0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
];
const HASH_CAPABILITIES: Capabilities = Capabilities::CONTIGUOUS_INPUT
    .union(Capabilities::SCATTER_INPUT)
    .union(Capabilities::OUT_OF_PLACE);

fn assert_sha256_conformance(context: &mut Context<Hash>) {
    let mut singleton_output = [0u8; 32];
    let mut singleton = [HashOperation::new(
        Input::Contiguous(b"abc"),
        &mut singleton_output,
    )];
    context
        .execute(&mut singleton)
        .expect("singleton batch dispatches synchronously");
    assert_eq!(singleton[0].status(), Some(Ok(32)));
    assert_eq!(singleton_output, SHA256_ABC);

    let fragments: &[&[u8]] = &[b"a", b"b", b"c"];
    let mut scatter_output = [0u8; 32];
    let mut short_output = [0xa5; 31];
    let mut batch = [
        HashOperation::new(Input::Scatter(fragments), &mut scatter_output),
        HashOperation::new(Input::Contiguous(b"abc"), &mut short_output),
    ];
    context
        .execute(&mut batch)
        .expect("mixed multi-operation batch dispatches synchronously");
    assert_eq!(batch[0].status(), Some(Ok(32)));
    assert_eq!(
        batch[1].status(),
        Some(Err(hammer_infra::crypto::hash::Error::OutputTooSmall {
            required: 32,
            provided: 31,
        }))
    );
    assert_eq!(scatter_output, SHA256_ABC);
    assert_eq!(short_output, [0xa5; 31]);
}

#[test]
fn injected_instruction_set_controls_implementation_selection() {
    let portable = Engine::with_builtins(InstructionSet::empty())
        .expect("portable built-ins publish without CPU instructions");
    let algorithm = portable
        .algorithm::<Hash>("sha-256")
        .expect("standard SHA-256 algorithm");
    let context = portable
        .context(algorithm)
        .expect("portable SHA-256 Context");
    assert_eq!(context.implementation_name(), "hammer:hash-portable");

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    let expected = "hammer:sha-256-sha-ni";
    #[cfg(target_arch = "aarch64")]
    let expected = "hammer:sha-256-armv8";
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    let expected = "hammer:hash-portable";

    let accelerated = Engine::with_builtins(InstructionSet::SHA2)
        .expect("injected SHA-2 capability publishes built-ins");
    let algorithm = accelerated
        .algorithm::<Hash>("sha-256")
        .expect("standard SHA-256 algorithm");
    let context = accelerated
        .context(algorithm)
        .expect("instruction implementation is selectable");
    assert_eq!(context.implementation_name(), expected);
}

#[test]
fn bound_context_reports_implementation_loss_without_fallback() {
    let capabilities = HASH_CAPABILITIES;
    let mut engine = Engine::new(InstructionSet::empty());
    engine
        .publish(
            Registration::new()
                .with_algorithm("test:hash", capabilities)
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:preferred", 10, true)
                        .with_algorithm(
                            "test:hash",
                            capabilities,
                            (),
                            HashPrepared::execute::<hammer_infra::crypto::hash::Sha256>,
                        ),
                )
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:fallback", 0, true)
                        .with_algorithm(
                            "test:hash",
                            capabilities,
                            (),
                            HashPrepared::execute::<hammer_infra::crypto::hash::Sha256>,
                        ),
                ),
        )
        .expect("test implementations publish");

    let algorithm = engine
        .algorithm::<Hash>("test:hash")
        .expect("test hash algorithm");
    let mut bound = engine.context(algorithm).expect("preferred Context");
    assert_eq!(bound.implementation_name(), "test:preferred");

    engine
        .set_implementation_availability::<Hash>("test:preferred", false)
        .expect("preferred implementation exists");

    let mut output = [0u8; 32];
    let mut operations = [HashOperation::new(Input::Contiguous(b"lost"), &mut output)];
    assert_eq!(
        bound.execute(&mut operations),
        Err(ContextError::ImplementationUnavailable {
            implementation: "test:preferred".to_owned(),
        })
    );
    assert_eq!(operations[0].status(), None);

    let replacement = engine.context(algorithm).expect("fallback Context");
    assert_eq!(replacement.implementation_name(), "test:fallback");
}

#[test]
fn priority_changes_affect_only_new_contexts() {
    let capabilities = HASH_CAPABILITIES;
    let mut engine = Engine::new(InstructionSet::empty());
    engine
        .publish(
            Registration::new()
                .with_algorithm("test:hash", capabilities)
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:first", 10, true).with_algorithm(
                        "test:hash",
                        capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Sha256>,
                    ),
                )
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:second", 0, true).with_algorithm(
                        "test:hash",
                        capabilities,
                        (),
                        HashPrepared::execute::<hammer_infra::crypto::hash::Sha256>,
                    ),
                ),
        )
        .expect("test implementations publish");
    let algorithm = engine
        .algorithm::<Hash>("test:hash")
        .expect("test hash algorithm");
    let existing = engine.context(algorithm).expect("first Context");

    engine
        .set_implementation_priority::<Hash>("test:second", 20)
        .expect("second implementation exists");
    let new = engine.context(algorithm).expect("reprioritized Context");

    assert_eq!(existing.implementation_name(), "test:first");
    assert_eq!(new.implementation_name(), "test:second");
}

#[test]
fn sha256_implementations_obey_one_conformance_suite() {
    let instructions = InstructionSet::detect();
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    let accelerated_name = "hammer:sha-256-sha-ni";
    #[cfg(target_arch = "aarch64")]
    let accelerated_name = "hammer:sha-256-armv8";

    let mut engine = Engine::with_builtins(instructions).expect("built-ins publish");
    let algorithm = engine
        .algorithm::<Hash>("sha-256")
        .expect("standard SHA-256 algorithm");

    engine.set_selection_policy(SelectionPolicy::only(["hammer:hash-portable"]));
    let mut portable = engine.context(algorithm).expect("portable Context");
    assert_sha256_conformance(&mut portable);

    #[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
    if instructions.contains(InstructionSet::SHA2) {
        engine.set_selection_policy(SelectionPolicy::only([accelerated_name]));
        let mut accelerated = engine.context(algorithm).expect("accelerated Context");
        assert_sha256_conformance(&mut accelerated);
    }
}
