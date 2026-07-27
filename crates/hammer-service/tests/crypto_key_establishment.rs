use hammer_infra::crypto::key_establishment::{Error as KeyEstablishmentError, Output};
use hammer_service::crypto::{
    Engine, Kdf, KeyError, KeyOperations, KeyPolicy, Kx, KxOperation, KxStatus,
};

const X25519_PRIVATE: [u8; 32] = [
    0x77, 0x07, 0x6d, 0x0a, 0x73, 0x18, 0xa5, 0x7d, 0x3c, 0x16, 0xc1, 0x72, 0x51, 0xb2, 0x66, 0x45,
    0xdf, 0x4c, 0x2f, 0x87, 0xeb, 0xc0, 0x99, 0x2a, 0xb1, 0x77, 0xfb, 0xa5, 0x1d, 0xb9, 0x2c, 0x2a,
];
const X25519_PEER_PUBLIC: [u8; 32] = [
    0xde, 0x9e, 0xdb, 0x7d, 0x7b, 0x7d, 0xc1, 0xb4, 0xd3, 0x5b, 0x61, 0xc2, 0xec, 0xe4, 0x35, 0x37,
    0x3f, 0x83, 0x43, 0xc8, 0x5b, 0x78, 0x67, 0x4d, 0xad, 0xfc, 0x7e, 0x14, 0x6f, 0x88, 0x2b, 0x4f,
];
const X25519_SHARED_SECRET: [u8; 32] = [
    0x4a, 0x5d, 0x9d, 0x5b, 0xa4, 0xce, 0x2d, 0xe1, 0x72, 0x8e, 0x3b, 0xf4, 0x80, 0x35, 0x0f, 0x25,
    0xe0, 0x7e, 0x21, 0xc9, 0x47, 0xd1, 0x9e, 0x33, 0x76, 0xf0, 0x9b, 0x3c, 0x1e, 0x16, 0x17, 0x42,
];

#[test]
fn builtins_publish_the_common_key_establishment_catalog() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");

    for name in ["x25519", "p-256", "p-384", "ml-kem-768"] {
        assert!(engine.algorithm::<Kx>(name).is_some(), "missing {name}");
    }
}

#[test]
fn x25519_context_matches_the_rfc_7748_shared_secret() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_key = engine
        .create_key(
            &X25519_PRIVATE,
            KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false).with_derivation(
                target,
                KeyOperations::DERIVE,
                true,
            ),
        )
        .expect("private key is installed");
    let mut context = engine
        .context(algorithm)
        .expect("X25519 Context is prepared");
    let mut operations = [KxOperation::agree(private_key, &X25519_PEER_PUBLIC, target)];

    context.execute(&mut operations);

    let status = operations[0].status();
    let KxStatus::SharedSecret { key: shared_key } = status else {
        panic!("agreement did not return an opaque shared secret: {status:?}")
    };
    let mut shared_secret = [0_u8; 32];
    engine
        .export_secret(shared_key, &mut shared_secret)
        .expect("test policy permits export");
    assert_eq!(shared_secret, X25519_SHARED_SECRET);
}

#[test]
fn nist_curve_contexts_generate_opaque_keys_and_agree_symmetrically() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-384")
        .expect("HKDF-SHA-384 is registered");

    for (name, public_len, shared_len) in [("p-256", 65, 32), ("p-384", 97, 48)] {
        let algorithm = engine
            .algorithm::<Kx>(name)
            .expect("NIST curve is registered");
        let private_policy = KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false)
            .with_derivation(target, KeyOperations::DERIVE, true);
        let mut context = engine
            .context(algorithm)
            .expect("NIST curve Context is prepared");
        let mut alice_public = vec![0_u8; public_len];
        let mut bob_public = vec![0_u8; public_len];

        let alice_key = {
            let mut operations = [KxOperation::generate_keypair(
                private_policy.clone(),
                &mut alice_public,
            )];
            context.execute(&mut operations);
            let status = operations[0].status();
            let KxStatus::Generated { key, .. } = status else {
                panic!("key generation did not return an opaque private key: {status:?}")
            };
            key
        };
        let bob_key = {
            let mut operations = [KxOperation::generate_keypair(
                private_policy.clone(),
                &mut bob_public,
            )];
            context.execute(&mut operations);
            let status = operations[0].status();
            let KxStatus::Generated { key, .. } = status else {
                panic!("key generation did not return an opaque private key: {status:?}")
            };
            key
        };
        let alice_shared = {
            let mut operations = [KxOperation::agree(alice_key, &bob_public, target)];
            context.execute(&mut operations);
            let status = operations[0].status();
            let KxStatus::SharedSecret { key } = status else {
                panic!("agreement did not return an opaque shared secret: {status:?}")
            };
            key
        };
        let bob_shared = {
            let mut operations = [KxOperation::agree(bob_key, &alice_public, target)];
            context.execute(&mut operations);
            let status = operations[0].status();
            let KxStatus::SharedSecret { key } = status else {
                panic!("agreement did not return an opaque shared secret: {status:?}")
            };
            key
        };
        let mut alice_secret = vec![0_u8; shared_len];
        let mut bob_secret = vec![0_u8; shared_len];
        engine
            .export_secret(alice_shared, &mut alice_secret)
            .expect("test policy permits export");
        engine
            .export_secret(bob_shared, &mut bob_secret)
            .expect("test policy permits export");

        assert_eq!(alice_secret, bob_secret, "{name} shared secrets differ");
    }
}

#[test]
fn ml_kem_context_round_trips_without_returning_secret_bytes() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("ml-kem-768")
        .expect("ML-KEM-768 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_policy = KeyPolicy::new(algorithm, KeyOperations::KX_DECAPSULATE, false)
        .with_derivation(target, KeyOperations::DERIVE, true);
    let shared_policy = KeyPolicy::new(target, KeyOperations::DERIVE, true);
    let mut context = engine
        .context(algorithm)
        .expect("ML-KEM Context is prepared");
    let mut public_key = vec![0_u8; 1184];
    let private_key = {
        let mut operations = [KxOperation::generate_keypair(
            private_policy,
            &mut public_key,
        )];
        context.execute(&mut operations);
        let status = operations[0].status();
        let KxStatus::Generated { key, .. } = status else {
            panic!("key generation did not return an opaque private key: {status:?}")
        };
        key
    };
    let mut ciphertext = vec![0_u8; 1088];
    let sender_key = {
        let mut operations = [KxOperation::encapsulate(
            &public_key,
            shared_policy,
            &mut ciphertext,
        )];
        context.execute(&mut operations);
        let KxStatus::Encapsulated { key, .. } = operations[0].status() else {
            panic!("encapsulation did not return an opaque secret")
        };
        key
    };
    let receiver_key = {
        let mut operations = [KxOperation::decapsulate(private_key, &ciphertext, target)];
        context.execute(&mut operations);
        let status = operations[0].status();
        let KxStatus::SharedSecret { key } = status else {
            panic!("decapsulation did not return an opaque shared secret: {status:?}")
        };
        key
    };
    let mut sender_secret = [0_u8; 32];
    let mut receiver_secret = [0_u8; 32];
    engine
        .export_secret(sender_key, &mut sender_secret)
        .expect("test policy permits export");
    engine
        .export_secret(receiver_key, &mut receiver_secret)
        .expect("test policy permits export");

    assert_eq!(sender_secret, receiver_secret);
}

#[test]
fn key_agreement_reports_policy_denial_without_creating_a_secret() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_key = engine
        .create_key(
            &X25519_PRIVATE,
            KeyPolicy::new(algorithm, KeyOperations::empty(), false).with_derivation(
                target,
                KeyOperations::DERIVE,
                true,
            ),
        )
        .expect("private key is installed");
    let mut context = engine
        .context(algorithm)
        .expect("X25519 Context is prepared");
    let mut operations = [KxOperation::agree(private_key, &X25519_PEER_PUBLIC, target)];

    context.execute(&mut operations);

    assert_eq!(
        operations[0].status(),
        KxStatus::PolicyDenied { key: private_key }
    );
}

#[test]
fn key_agreement_reports_a_stale_private_key_handle() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_key = engine
        .create_key(
            &X25519_PRIVATE,
            KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false),
        )
        .expect("private key is installed");
    engine
        .destroy_key(private_key)
        .expect("unreferenced private key is destroyed");
    let mut context = engine
        .context(algorithm)
        .expect("X25519 Context is prepared");
    let mut operations = [KxOperation::agree(private_key, &X25519_PEER_PUBLIC, target)];

    context.execute(&mut operations);

    assert_eq!(
        operations[0].status(),
        KxStatus::StaleKey { key: private_key }
    );
}

#[test]
fn key_generation_capacity_failure_leaves_public_output_unchanged() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let mut context = engine
        .context(algorithm)
        .expect("X25519 Context is prepared");
    let mut public_key = [0xa5_u8; 31];
    let mut operations = [KxOperation::generate_keypair(
        KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false),
        &mut public_key,
    )];

    context.execute(&mut operations);

    assert_eq!(
        operations[0].status(),
        KxStatus::Algorithm(KeyEstablishmentError::OutputTooSmall {
            output: Output::PublicKey,
            required: 32,
            provided: 31,
        })
    );
    assert_eq!(public_key, [0xa5; 31]);
}

#[test]
fn key_generation_rejects_a_policy_for_another_algorithm() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let x25519 = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let p256 = engine
        .algorithm::<Kx>("p-256")
        .expect("P-256 is registered");
    let mut context = engine.context(x25519).expect("X25519 Context is prepared");
    let mut public_key = [0xa5_u8; 32];
    let mut operations = [KxOperation::generate_keypair(
        KeyPolicy::new(p256, KeyOperations::KX_AGREE, false),
        &mut public_key,
    )];

    context.execute(&mut operations);

    assert_eq!(operations[0].status(), KxStatus::GenerationPolicyDenied);
    assert_eq!(public_key, [0xa5; 32]);
}

#[test]
fn ml_kem_ciphertext_capacity_failure_leaves_output_unchanged() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("ml-kem-768")
        .expect("ML-KEM-768 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let mut context = engine
        .context(algorithm)
        .expect("ML-KEM Context is prepared");
    let mut public_key = vec![0_u8; 1184];
    {
        let mut operations = [KxOperation::generate_keypair(
            KeyPolicy::new(algorithm, KeyOperations::KX_DECAPSULATE, false),
            &mut public_key,
        )];
        context.execute(&mut operations);
        let status = operations[0].status();
        assert!(
            matches!(status, KxStatus::Generated { .. }),
            "key generation did not return an opaque private key: {status:?}"
        );
    }
    let mut ciphertext = vec![0xa5_u8; 1087];
    let mut operations = [KxOperation::encapsulate(
        &public_key,
        KeyPolicy::new(target, KeyOperations::DERIVE, false),
        &mut ciphertext,
    )];

    context.execute(&mut operations);

    assert_eq!(
        operations[0].status(),
        KxStatus::Algorithm(KeyEstablishmentError::OutputTooSmall {
            output: Output::Ciphertext,
            required: 1088,
            provided: 1087,
        })
    );
    assert_eq!(ciphertext, vec![0xa5; 1087]);
}

#[test]
fn ml_kem_decapsulation_reports_invalid_ciphertext_length() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("ml-kem-768")
        .expect("ML-KEM-768 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_policy = KeyPolicy::new(algorithm, KeyOperations::KX_DECAPSULATE, false)
        .with_derivation(target, KeyOperations::DERIVE, false);
    let mut context = engine
        .context(algorithm)
        .expect("ML-KEM Context is prepared");
    let mut public_key = vec![0_u8; 1184];
    let private_key = {
        let mut operations = [KxOperation::generate_keypair(
            private_policy,
            &mut public_key,
        )];
        context.execute(&mut operations);
        let status = operations[0].status();
        let KxStatus::Generated { key, .. } = status else {
            panic!("key generation did not return an opaque private key: {status:?}")
        };
        key
    };
    let mut operations = [KxOperation::decapsulate(private_key, &[0; 1087], target)];

    context.execute(&mut operations);

    assert_eq!(
        operations[0].status(),
        KxStatus::Algorithm(KeyEstablishmentError::InvalidCiphertextLength {
            required: 1088,
            provided: 1087,
        })
    );
}

#[test]
fn x25519_small_order_peer_key_is_rejected() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_key = engine
        .create_key(
            &X25519_PRIVATE,
            KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false).with_derivation(
                target,
                KeyOperations::DERIVE,
                false,
            ),
        )
        .expect("private key is installed");
    let mut context = engine
        .context(algorithm)
        .expect("X25519 Context is prepared");
    let mut operations = [KxOperation::agree(private_key, &[0; 32], target)];

    context.execute(&mut operations);

    assert_eq!(
        operations[0].status(),
        KxStatus::Algorithm(KeyEstablishmentError::SmallOrderPublicKey)
    );
}

#[test]
fn established_secret_remains_non_exportable_when_target_policy_denies_export() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is registered");
    let target = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let private_key = engine
        .create_key(
            &X25519_PRIVATE,
            KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false).with_derivation(
                target,
                KeyOperations::DERIVE,
                false,
            ),
        )
        .expect("private key is installed");
    let mut context = engine
        .context(algorithm)
        .expect("X25519 Context is prepared");
    let mut operations = [KxOperation::agree(private_key, &X25519_PEER_PUBLIC, target)];
    context.execute(&mut operations);
    let status = operations[0].status();
    let KxStatus::SharedSecret { key: shared_key } = status else {
        panic!("agreement did not return an opaque shared secret: {status:?}")
    };
    let mut output = [0xa5_u8; 32];

    let error = engine
        .export_secret(shared_key, &mut output)
        .expect_err("derived policy denies Secret Export");

    assert_eq!(error, KeyError::SecretExportDenied { key: shared_key });
    assert_eq!(output, [0xa5; 32]);
}
