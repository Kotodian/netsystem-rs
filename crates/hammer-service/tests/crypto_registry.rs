use hammer_infra::crypto::HashAlgorithm;
use hammer_service::crypto::{
    Aead, AlgorithmRegistration, Capabilities, Cipher, Engine, Hash, HashOperation, HashPrepared,
    ImplementationRegistration, Kdf, Kx, Mac, Registration, RegistryError, SelectionPolicy, Sign,
    Verify,
};

const HASH_CAPABILITIES: Capabilities = Capabilities::CONTIGUOUS_INPUT
    .union(Capabilities::SCATTER_INPUT)
    .union(Capabilities::OUT_OF_PLACE);

fn implementation_a(_: &mut HashPrepared, _: &mut [HashOperation<'_>]) {}

fn implementation_b(_: &mut HashPrepared, _: &mut [HashOperation<'_>]) {}

#[test]
fn registry_supports_each_closed_operation_family() {
    let mut engine = Engine::new();

    engine
        .publish(
            Registration::<Aead>::new().with_algorithm(AlgorithmRegistration::new(
                "test:aead",
                Capabilities::empty(),
            )),
        )
        .expect("AEAD family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Cipher>::new().with_algorithm(AlgorithmRegistration::new(
                "test:cipher",
                Capabilities::empty(),
            )),
        )
        .expect("Cipher family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Hash>::new().with_algorithm(AlgorithmRegistration::new(
                "test:hash",
                Capabilities::empty(),
            )),
        )
        .expect("Hash family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Mac>::new().with_algorithm(AlgorithmRegistration::new(
                "test:mac",
                Capabilities::empty(),
            )),
        )
        .expect("MAC family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Kdf>::new().with_algorithm(AlgorithmRegistration::new(
                "test:kdf",
                Capabilities::empty(),
            )),
        )
        .expect("KDF family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Kx>::new()
                .with_algorithm(AlgorithmRegistration::new("test:kx", Capabilities::empty())),
        )
        .expect("KX family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Sign>::new().with_algorithm(AlgorithmRegistration::new(
                "test:sign",
                Capabilities::empty(),
            )),
        )
        .expect("Sign family accepts an algorithm registration");
    engine
        .publish(
            Registration::<Verify>::new().with_algorithm(AlgorithmRegistration::new(
                "test:verify",
                Capabilities::empty(),
            )),
        )
        .expect("Verify family accepts an algorithm registration");

    assert!(engine.algorithm::<Aead>("test:aead").is_some());
    assert!(engine.algorithm::<Cipher>("test:cipher").is_some());
    assert!(engine.algorithm::<Hash>("test:hash").is_some());
    assert!(engine.algorithm::<Mac>("test:mac").is_some());
    assert!(engine.algorithm::<Kdf>("test:kdf").is_some());
    assert!(engine.algorithm::<Kx>("test:kx").is_some());
    assert!(engine.algorithm::<Sign>("test:sign").is_some());
    assert!(engine.algorithm::<Verify>("test:verify").is_some());
}

#[test]
fn algorithm_names_are_unique_within_each_family() {
    let mut engine = Engine::new();

    engine
        .publish(
            Registration::<Sign>::new().with_algorithm(AlgorithmRegistration::new(
                "test:signature",
                Capabilities::empty(),
            )),
        )
        .expect("Sign family publishes its canonical signature name");
    engine
        .publish(
            Registration::<Verify>::new().with_algorithm(AlgorithmRegistration::new(
                "test:signature",
                Capabilities::empty(),
            )),
        )
        .expect("Verify family independently publishes the same canonical signature name");

    assert!(engine.algorithm::<Sign>("test:signature").is_some());
    assert!(engine.algorithm::<Verify>("test:signature").is_some());
}

#[test]
fn registration_bundle_rolls_back_on_implementation_name_failure() {
    let mut engine = Engine::new();
    let registration = Registration::new()
        .with_algorithm(AlgorithmRegistration::<Hash>::new(
            "test-hash",
            HASH_CAPABILITIES,
        ))
        .with_implementation(
            ImplementationRegistration::<Hash>::new("malformed", 10, true).with_algorithm(
                "test-hash",
                HASH_CAPABILITIES,
                HashAlgorithm::Sha256,
                implementation_a,
            ),
        );

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
        .with_implementation(
            ImplementationRegistration::<Hash>::new("test:incomplete", 10, true).with_algorithm(
                "test-hash",
                Capabilities::CONTIGUOUS_INPUT,
                HashAlgorithm::Sha256,
                implementation_a,
            ),
        );

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
fn registration_rejects_repeated_algorithm_function_tables() {
    let mut engine = Engine::new();
    let registration = Registration::new()
        .with_algorithm(AlgorithmRegistration::<Hash>::new(
            "test-hash",
            HASH_CAPABILITIES,
        ))
        .with_implementation(
            ImplementationRegistration::<Hash>::new("test:duplicate", 10, true)
                .with_algorithm(
                    "test-hash",
                    HASH_CAPABILITIES,
                    HashAlgorithm::Sha256,
                    implementation_a,
                )
                .with_algorithm(
                    "test-hash",
                    HASH_CAPABILITIES,
                    HashAlgorithm::Sha256,
                    implementation_b,
                ),
        );

    let error = engine
        .publish(registration)
        .expect_err("one implementation cannot publish two tables for one algorithm");

    assert_eq!(
        error,
        RegistryError::ImplementationAlgorithmCollision {
            implementation: "test:duplicate".to_owned(),
            algorithm: "test-hash".to_owned(),
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
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:z-low", 10, true).with_algorithm(
                        "test-hash",
                        HASH_CAPABILITIES,
                        HashAlgorithm::Sha256,
                        implementation_a,
                    ),
                )
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:b-high", 20, true)
                        .with_algorithm(
                            "test-hash",
                            HASH_CAPABILITIES,
                            HashAlgorithm::Sha256,
                            implementation_b,
                        ),
                )
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:a-high", 20, true)
                        .with_algorithm(
                            "test-hash",
                            HASH_CAPABILITIES,
                            HashAlgorithm::Sha256,
                            implementation_a,
                        ),
                ),
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
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:preferred", 20, true)
                        .with_algorithm(
                            "test-hash",
                            HASH_CAPABILITIES,
                            HashAlgorithm::Sha256,
                            implementation_b,
                        ),
                )
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("test:portable", 10, true)
                        .with_algorithm(
                            "test-hash",
                            HASH_CAPABILITIES,
                            HashAlgorithm::Sha256,
                            implementation_a,
                        ),
                ),
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
                .with_implementation(
                    ImplementationRegistration::<Hash>::new("vendor:portable", 0, true)
                        .with_algorithm(
                            "vendor:test-hash",
                            HASH_CAPABILITIES,
                            HashAlgorithm::Sha256,
                            implementation_a,
                        ),
                ),
        )
        .expect("canonical names publish");
}
