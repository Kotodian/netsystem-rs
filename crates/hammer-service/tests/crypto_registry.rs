use hammer_service::crypto::{
    Aead, AlgorithmRegistration, Capabilities, Cipher, Engine, Family, Hash, HashOperation,
    HashPrepared, ImplementationRegistration, Kdf, Kx, Mac, Registration, RegistryError,
    SelectionPolicy, Sign, Verify,
};

const HASH_CAPABILITIES: Capabilities = Capabilities::CONTIGUOUS_INPUT
    .union(Capabilities::SCATTER_INPUT)
    .union(Capabilities::OUT_OF_PLACE);

fn implementation_a(_: &mut HashPrepared, _: &mut [HashOperation<'_>]) {}

fn implementation_b(_: &mut HashPrepared, _: &mut [HashOperation<'_>]) {}

fn publish_algorithm<F: Family>(engine: &mut Engine, name: &str) {
    engine
        .publish(
            Registration::<F>::new()
                .with_algorithm(AlgorithmRegistration::new(name, Capabilities::empty())),
        )
        .expect("family accepts an algorithm registration");
    assert!(engine.algorithm::<F>(name).is_some());
}

#[test]
fn registry_supports_each_closed_operation_family() {
    let mut engine = Engine::new();

    publish_algorithm::<Aead>(&mut engine, "test:aead");
    publish_algorithm::<Cipher>(&mut engine, "test:cipher");
    publish_algorithm::<Hash>(&mut engine, "test:hash");
    publish_algorithm::<Mac>(&mut engine, "test:mac");
    publish_algorithm::<Kdf>(&mut engine, "test:kdf");
    publish_algorithm::<Kx>(&mut engine, "test:kx");
    publish_algorithm::<Sign>(&mut engine, "test:sign");
    publish_algorithm::<Verify>(&mut engine, "test:verify");
}

#[test]
fn registration_bundle_rolls_back_on_implementation_name_failure() {
    let mut engine = Engine::new();
    let registration = Registration::new()
        .with_algorithm(AlgorithmRegistration::<Hash>::new(
            "test-hash",
            HASH_CAPABILITIES,
        ))
        .with_implementation(ImplementationRegistration::<Hash>::new(
            "malformed",
            &["test-hash"],
            HASH_CAPABILITIES,
            10,
            true,
            implementation_a,
        ));

    let error = engine
        .publish(registration)
        .expect_err("implementation names require a namespace");

    assert_eq!(
        error,
        RegistryError::MalformedImplementationName {
            name: "malformed".to_owned(),
        }
    );
    assert!(engine.algorithm::<Hash>("test-hash").is_none());
}

#[test]
fn algorithm_collision_rolls_back_other_bundle_declarations() {
    let mut engine = Engine::new();
    engine
        .publish(
            Registration::<Hash>::new().with_algorithm(AlgorithmRegistration::new(
                "existing-hash",
                HASH_CAPABILITIES,
            )),
        )
        .expect("initial algorithm publishes");

    let error = engine
        .publish(
            Registration::<Hash>::new()
                .with_algorithm(AlgorithmRegistration::new("new-hash", HASH_CAPABILITIES))
                .with_algorithm(AlgorithmRegistration::new(
                    "existing-hash",
                    HASH_CAPABILITIES,
                )),
        )
        .expect_err("published names cannot collide");

    assert_eq!(
        error,
        RegistryError::AlgorithmCollision {
            name: "existing-hash".to_owned(),
        }
    );
    assert!(engine.algorithm::<Hash>("new-hash").is_none());
}

#[test]
fn registration_rejects_capability_mismatch_without_partial_publication() {
    let mut engine = Engine::new();
    let registration = Registration::new()
        .with_algorithm(AlgorithmRegistration::<Hash>::new(
            "test-hash",
            HASH_CAPABILITIES,
        ))
        .with_implementation(ImplementationRegistration::<Hash>::new(
            "test:incomplete",
            &["test-hash"],
            Capabilities::CONTIGUOUS_INPUT,
            10,
            true,
            implementation_a,
        ));

    let error = engine
        .publish(registration)
        .expect_err("implementation lacks required operation shapes");

    assert_eq!(
        error,
        RegistryError::CapabilityMismatch {
            implementation: "test:incomplete".to_owned(),
            algorithm: "test-hash".to_owned(),
            required: HASH_CAPABILITIES,
            provided: Capabilities::CONTIGUOUS_INPUT,
        }
    );
    assert!(engine.algorithm::<Hash>("test-hash").is_none());
}

#[test]
fn deterministic_selection_uses_priority_then_implementation_name() {
    let mut engine = Engine::new();
    engine
        .publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "test-hash",
                    HASH_CAPABILITIES,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "test:z-low",
                    &["test-hash"],
                    HASH_CAPABILITIES,
                    10,
                    true,
                    implementation_a,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "test:b-high",
                    &["test-hash"],
                    HASH_CAPABILITIES,
                    20,
                    true,
                    implementation_b,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "test:a-high",
                    &["test-hash"],
                    HASH_CAPABILITIES,
                    20,
                    true,
                    implementation_a,
                )),
        )
        .expect("valid registration publishes");
    let algorithm = engine
        .algorithm::<Hash>("test-hash")
        .expect("algorithm is published");
    let context = engine
        .context(algorithm)
        .expect("implementation is selected");

    assert_eq!(context.implementation_name(), "test:a-high");
}

#[test]
fn availability_and_policy_changes_affect_only_new_contexts() {
    let mut engine = Engine::new();
    engine
        .publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "test-hash",
                    HASH_CAPABILITIES,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "test:preferred",
                    &["test-hash"],
                    HASH_CAPABILITIES,
                    20,
                    true,
                    implementation_b,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "test:portable",
                    &["test-hash"],
                    HASH_CAPABILITIES,
                    10,
                    true,
                    implementation_a,
                )),
        )
        .expect("valid registration publishes");
    let algorithm = engine
        .algorithm::<Hash>("test-hash")
        .expect("algorithm is published");
    let existing = engine.context(algorithm).expect("preferred is available");

    engine
        .set_implementation_availability::<Hash>("test:preferred", false)
        .expect("implementation exists");
    engine.set_selection_policy(SelectionPolicy::only(["test:portable"]));
    let new_context = engine
        .context(algorithm)
        .expect("portable remains selectable");

    assert_eq!(existing.implementation_name(), "test:preferred");
    assert_eq!(new_context.implementation_name(), "test:portable");
}

#[test]
fn names_enforce_algorithm_and_implementation_namespaces() {
    let mut engine = Engine::new();
    let malformed_algorithm = engine
        .publish(
            Registration::<Hash>::new()
                .with_algorithm(AlgorithmRegistration::new("Vendor:Hash", HASH_CAPABILITIES)),
        )
        .expect_err("algorithm name is not canonical");
    assert_eq!(
        malformed_algorithm,
        RegistryError::MalformedAlgorithmName {
            name: "Vendor:Hash".to_owned(),
        }
    );

    engine
        .publish(
            Registration::new()
                .with_algorithm(AlgorithmRegistration::<Hash>::new(
                    "vendor:test-hash",
                    HASH_CAPABILITIES,
                ))
                .with_implementation(ImplementationRegistration::<Hash>::new(
                    "vendor:portable",
                    &["vendor:test-hash"],
                    HASH_CAPABILITIES,
                    0,
                    true,
                    implementation_a,
                )),
        )
        .expect("canonical names publish");
}
