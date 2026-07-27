use hammer_infra::crypto::InstructionSet;
use hammer_service::crypto::{
    Capabilities, ContextError, Engine, Hash, HashOperation, HashPrepared,
    ImplementationRegistration, Input, Registration, SelectionPolicy,
};

fn hash_capabilities() -> Capabilities {
    Capabilities::CONTIGUOUS_INPUT | Capabilities::SCATTER_INPUT | Capabilities::OUT_OF_PLACE
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
    let capabilities = hash_capabilities();
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
    let capabilities = hash_capabilities();
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
fn accelerated_and_portable_hashes_have_identical_results_and_failures() {
    let instructions = InstructionSet::detect();
    if !instructions.contains(InstructionSet::SHA2) {
        return;
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    let accelerated_name = "hammer:sha-256-sha-ni";
    #[cfg(target_arch = "aarch64")]
    let accelerated_name = "hammer:sha-256-armv8";
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    return;

    let mut engine = Engine::with_builtins(instructions).expect("built-ins publish");
    let algorithm = engine
        .algorithm::<Hash>("sha-256")
        .expect("standard SHA-256 algorithm");

    engine.set_selection_policy(SelectionPolicy::only(["hammer:hash-portable"]));
    let mut portable = engine.context(algorithm).expect("portable Context");
    engine.set_selection_policy(SelectionPolicy::only([accelerated_name]));
    let mut accelerated = engine.context(algorithm).expect("accelerated Context");

    let fragments: &[&[u8]] = &[b"cross-", b"implementation"];
    let mut portable_output = [0u8; 32];
    let mut accelerated_output = [0u8; 32];
    let mut portable_operations = [HashOperation::new(
        Input::Scatter(fragments),
        &mut portable_output,
    )];
    let mut accelerated_operations = [HashOperation::new(
        Input::Scatter(fragments),
        &mut accelerated_output,
    )];
    portable
        .execute(&mut portable_operations)
        .expect("portable execution");
    accelerated
        .execute(&mut accelerated_operations)
        .expect("accelerated execution");
    assert_eq!(portable_operations[0].status(), Ok(32).into());
    assert_eq!(
        accelerated_operations[0].status(),
        portable_operations[0].status()
    );
    assert_eq!(accelerated_output, portable_output);

    let mut portable_short = [0u8; 31];
    let mut accelerated_short = [0u8; 31];
    let mut portable_operations = [HashOperation::new(
        Input::Contiguous(b"failure"),
        &mut portable_short,
    )];
    let mut accelerated_operations = [HashOperation::new(
        Input::Contiguous(b"failure"),
        &mut accelerated_short,
    )];
    portable
        .execute(&mut portable_operations)
        .expect("portable dispatch");
    accelerated
        .execute(&mut accelerated_operations)
        .expect("accelerated dispatch");
    assert_eq!(
        accelerated_operations[0].status(),
        portable_operations[0].status()
    );
    assert_eq!(accelerated_short, portable_short);
}
