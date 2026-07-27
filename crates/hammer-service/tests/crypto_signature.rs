use hammer_infra::crypto::signature::{SignError, VerifyError};
use hammer_service::crypto::{
    ContextError, Engine, Input, KeyOperations, KeyPolicy, Sign, SignOperation, Verify,
    VerifyOperation,
};

#[test]
fn builtin_signature_catalog_resolves_in_both_operation_families() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");

    for name in [
        "ed25519",
        "ecdsa-p-256-sha-256",
        "ecdsa-p-384-sha-384",
        "rsa-pss-sha-256",
        "rsa-pss-sha-384",
        "rsa-pss-sha-512",
    ] {
        assert!(
            engine.algorithm::<Sign>(name).is_some(),
            "missing Sign {name}"
        );
        assert!(
            engine.algorithm::<Verify>(name).is_some(),
            "missing Verify {name}"
        );
    }
}

#[test]
fn builtin_ed25519_signs_and_verifies_through_registered_contexts() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let sign_algorithm = engine
        .algorithm::<Sign>("ed25519")
        .expect("Ed25519 signing is built in");
    let verify_algorithm = engine
        .algorithm::<Verify>("ed25519")
        .expect("Ed25519 verification is built in");
    let private_key = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60,
    ];
    let key = engine
        .create_key(
            &private_key,
            KeyPolicy::new(sign_algorithm, KeyOperations::SIGN, false),
        )
        .expect("key pool has capacity");
    let mut sign_context = engine
        .context_with_key(sign_algorithm, key)
        .expect("key permits Ed25519 signing");
    let message: [&[u8]; 2] = [b"registered ", b"batch"];
    let mut public_key = [0_u8; 32];
    let mut signature = [0_u8; 64];
    {
        let mut sign_operations = [
            SignOperation::public_key(&mut public_key),
            SignOperation::sign(Input::Scatter(&message), &mut signature),
        ];
        assert_eq!(sign_operations[0].status(), None);
        assert_eq!(sign_operations[1].status(), None);

        sign_context.execute(&mut sign_operations);

        assert_eq!(sign_operations[0].status(), Some(Ok(32)));
        assert_eq!(sign_operations[1].status(), Some(Ok(64)));
    }

    let mut verify_context = engine
        .context(verify_algorithm)
        .expect("portable Ed25519 verification is available");
    {
        let mut verify_operations = [VerifyOperation::verify(
            &public_key,
            Input::Scatter(&message),
            &signature,
        )];
        verify_context.execute(&mut verify_operations);
        assert_eq!(verify_operations[0].status(), Some(Ok(())));
    }

    signature[0] ^= 1;
    let mut invalid_operations = [VerifyOperation::verify(
        &public_key,
        Input::Scatter(&message),
        &signature,
    )];
    verify_context.execute(&mut invalid_operations);
    assert_eq!(
        invalid_operations[0].status(),
        Some(Err(VerifyError::SignatureMismatch))
    );
}

#[test]
fn signing_context_preserves_policy_stale_key_and_key_encoding_failures() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let ed25519 = engine
        .algorithm::<Sign>("ed25519")
        .expect("Ed25519 signing is built in");
    let denied_key = engine
        .create_key(
            &[1; 32],
            KeyPolicy::new(ed25519, KeyOperations::empty(), false),
        )
        .expect("key pool has capacity");
    assert_eq!(
        engine
            .context_with_key(ed25519, denied_key)
            .expect_err("key policy does not permit signing"),
        ContextError::OperationsDenied {
            key: denied_key,
            required: KeyOperations::SIGN,
        }
    );

    let stale_key = engine
        .create_key(
            &[2; 32],
            KeyPolicy::new(ed25519, KeyOperations::SIGN, false),
        )
        .expect("key pool has capacity");
    engine
        .destroy_key(stale_key)
        .expect("unreferenced key can be destroyed");
    assert_eq!(
        engine
            .context_with_key(ed25519, stale_key)
            .expect_err("destroyed key generation is stale"),
        ContextError::StaleKey { key: stale_key }
    );

    let p256 = engine
        .algorithm::<Sign>("ecdsa-p-256-sha-256")
        .expect("P-256 signing is built in");
    let invalid_key = engine
        .create_key(&[0; 32], KeyPolicy::new(p256, KeyOperations::SIGN, false))
        .expect("key pool has capacity");
    let mut context = engine
        .context_with_key(p256, invalid_key)
        .expect("key material remains opaque until the selected operation executes");
    let mut public_key = [0_u8; 65];
    let mut operations = [SignOperation::public_key(&mut public_key)];
    context.execute(&mut operations);
    assert_eq!(
        operations[0].status(),
        Some(Err(SignError::InvalidPrivateKey))
    );
}
