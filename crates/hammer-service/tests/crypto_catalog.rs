use hammer_service::crypto::{
    Aead, AeadOperation, AeadStatus, Engine, Hash, Input, Kdf, KdfOperation, KdfStatus,
    KeyOperations, KeyPolicy, Mac, MacOperation,
};

#[test]
fn builtins_publish_the_common_symmetric_digest_mac_and_kdf_catalog() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");

    for name in ["aes-128-gcm", "aes-256-gcm", "chacha20-poly1305"] {
        assert!(engine.algorithm::<Aead>(name).is_some(), "missing {name}");
    }
    for name in [
        "sha-256",
        "sha-384",
        "sha-512",
        "blake2s-256",
        "blake2b-512",
    ] {
        assert!(engine.algorithm::<Hash>(name).is_some(), "missing {name}");
    }
    for name in ["hmac-sha-256", "hmac-sha-384", "hmac-sha-512"] {
        assert!(engine.algorithm::<Mac>(name).is_some(), "missing {name}");
    }
    for name in ["hkdf-sha-256", "hkdf-sha-384", "hkdf-sha-512"] {
        assert!(engine.algorithm::<Kdf>(name).is_some(), "missing {name}");
    }
}

#[test]
fn all_builtin_aead_algorithms_execute_through_resolved_function_tables() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");

    for (name, key) in [
        ("aes-128-gcm", &[0_u8; 16][..]),
        ("aes-256-gcm", &[0_u8; 32][..]),
        ("chacha20-poly1305", &[0_u8; 32][..]),
    ] {
        let algorithm = engine
            .algorithm::<Aead>(name)
            .expect("algorithm is registered");
        let key = engine
            .create_key(
                key,
                KeyPolicy::new(
                    algorithm,
                    KeyOperations::AEAD_SEAL | KeyOperations::AEAD_OPEN,
                    false,
                ),
            )
            .expect("key is installed");
        let mut context = engine
            .context_with_key(algorithm, key)
            .expect("algorithm-specific preparation succeeds");
        let mut ciphertext = [0_u8; 3];
        let mut tag = [0_u8; 16];
        let mut operations = [AeadOperation::seal(
            Input::Contiguous(b"abc"),
            &[0; 12],
            b"aad",
            &mut ciphertext,
            &mut tag,
        )];
        context.execute(&mut operations);
        assert_eq!(operations[0].status(), AeadStatus::Executed(Ok(3)));
    }
}

#[test]
fn hmac_sha256_executes_as_a_keyed_batch() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let algorithm = engine
        .algorithm::<Mac>("hmac-sha-256")
        .expect("HMAC-SHA-256 is registered");
    let key = engine
        .create_key(
            &[0x0b; 20],
            KeyPolicy::new(algorithm, KeyOperations::MAC_AUTHENTICATE, false),
        )
        .expect("key is installed");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("HMAC Context is prepared");
    let mut output = [0_u8; 32];
    let mut operations = [MacOperation::authenticate(
        Input::Scatter(&[b"Hi ", b"There"]),
        &mut output,
    )];

    context.execute(&mut operations);

    assert_eq!(operations[0].status(), Some(Ok(32)));
    assert_eq!(
        output,
        [
            0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b,
            0xf1, 0x2b, 0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37, 0x6c,
            0x2e, 0x32, 0xcf, 0xf7,
        ]
    );
}

#[test]
fn hkdf_installs_policy_limited_derived_keys_without_exposing_secret_bytes() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let kdf = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let target = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is registered");
    let parent_policy = KeyPolicy::new(kdf, KeyOperations::DERIVE, false).with_derivation(
        target,
        KeyOperations::AEAD_SEAL,
        true,
    );
    let parent = engine
        .create_key(&[0x0b; 22], parent_policy)
        .expect("input key material is installed");
    let mut context = engine
        .context_with_key(kdf, parent)
        .expect("HKDF Context is prepared");
    let info = [0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9];
    let salt = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
    ];
    let mut operations = [KdfOperation::derive(
        Some(&salt),
        Input::Contiguous(&info),
        16,
        target,
    )];

    context.execute(&mut operations);

    let KdfStatus::Complete { key } = operations[0].status() else {
        panic!("derivation must return an opaque key")
    };
    let mut exported = [0_u8; 16];
    assert_eq!(
        engine
            .export_secret(key, &mut exported)
            .expect("derived policy explicitly permits export"),
        exported.len()
    );
    assert_eq!(
        exported,
        [
            0x3c, 0xb2, 0x5f, 0x25, 0xfa, 0xac, 0xd5, 0x7a, 0x90, 0x43, 0x4f, 0x64, 0xd0, 0x36,
            0x2f, 0x2a,
        ]
    );
}

#[test]
fn hkdf_rejects_a_derived_algorithm_absent_from_the_parent_policy() {
    let engine = Engine::with_builtins().expect("built-in catalog publishes");
    let kdf = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is registered");
    let allowed = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is registered");
    let denied = engine
        .algorithm::<Aead>("aes-256-gcm")
        .expect("AES-256-GCM is registered");
    let parent = engine
        .create_key(
            &[0x0b; 22],
            KeyPolicy::new(kdf, KeyOperations::DERIVE, false).with_derivation(
                allowed,
                KeyOperations::AEAD_SEAL,
                false,
            ),
        )
        .expect("input key material is installed");
    let mut context = engine
        .context_with_key(kdf, parent)
        .expect("HKDF Context is prepared");
    let mut operations = [KdfOperation::derive(
        None,
        Input::Contiguous(b"info"),
        32,
        denied,
    )];

    context.execute(&mut operations);

    assert_eq!(operations[0].status(), KdfStatus::DerivationDenied);
}
