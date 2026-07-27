use hammer_infra::crypto::InstructionSet;
use hammer_service::crypto::{
    Aead, AeadOperation, AeadStatus, Engine, Hash, HashOperation, Input, Kdf, KdfOperation,
    KdfStatus, KeyOperations, KeyPolicy, Mac, MacOperation, SelectionPolicy,
};

struct AeadVector {
    algorithm: &'static str,
    key: &'static str,
    nonce: &'static str,
    associated_data: &'static str,
    plaintext: &'static str,
    ciphertext: &'static str,
    tag: &'static str,
}

#[test]
fn builtins_publish_the_common_symmetric_digest_mac_and_kdf_catalog() {
    let engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in catalog publishes");

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
fn portable_aead_catalog_matches_nist_and_rfc_vectors_in_both_output_modes() {
    let mut engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in catalog publishes");
    engine.set_selection_policy(SelectionPolicy::only(["hammer:aead-portable"]));

    for vector in [
        AeadVector {
            algorithm: "aes-128-gcm",
            key: "00000000000000000000000000000000",
            nonce: "000000000000000000000000",
            associated_data: "",
            plaintext: "00000000000000000000000000000000",
            ciphertext: "0388dace60b6a392f328c2b971b2fe78",
            tag: "ab6e47d42cec13bdf53a67b21257bddf",
        },
        AeadVector {
            algorithm: "aes-256-gcm",
            key: "0000000000000000000000000000000000000000000000000000000000000000",
            nonce: "000000000000000000000000",
            associated_data: "",
            plaintext: "00000000000000000000000000000000",
            ciphertext: "cea7403d4d606b6e074ec5d3baf39d18",
            tag: "d0d1c8a799996bf0265b98b5d48ab919",
        },
        AeadVector {
            algorithm: "chacha20-poly1305",
            key: "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f",
            nonce: "070000004041424344454647",
            associated_data: "50515253c0c1c2c3c4c5c6c7",
            plaintext: "4c616469657320616e642047656e746c656d656e206f662074686520636c617373206f66202739393a204966204920636f756c64206f6666657220796f75206f6e6c79206f6e652074697020666f7220746865206675747572652c2073756e73637265656e20776f756c642062652069742e",
            ciphertext: "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d63dbea45e8ca9671282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b3692ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7bc3ff4def08e4b7a9de576d26586cec64b6116",
            tag: "1ae10b594f09e26a7e902ecbd0600691",
        },
    ] {
        let key = hex::decode(vector.key).expect("standard key vector is valid hexadecimal");
        let nonce = hex::decode(vector.nonce).expect("standard nonce vector is valid hexadecimal");
        let associated_data = hex::decode(vector.associated_data)
            .expect("standard associated-data vector is valid hexadecimal");
        let plaintext =
            hex::decode(vector.plaintext).expect("standard plaintext vector is valid hexadecimal");
        let expected_ciphertext = hex::decode(vector.ciphertext)
            .expect("standard ciphertext vector is valid hexadecimal");
        let expected_tag =
            hex::decode(vector.tag).expect("standard tag vector is valid hexadecimal");
        let algorithm = engine
            .algorithm::<Aead>(vector.algorithm)
            .expect("algorithm is registered");
        let key = engine
            .create_key(
                &key,
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
        let split = plaintext.len() / 2;
        let fragments: &[&[u8]] = &[&plaintext[..split], &plaintext[split..]];
        let mut ciphertext = vec![0u8; plaintext.len()];
        let mut tag = vec![0u8; expected_tag.len()];
        let mut operations = [AeadOperation::seal(
            Input::Scatter(fragments),
            &nonce,
            &associated_data,
            &mut ciphertext,
            &mut tag,
        )];
        context
            .execute(&mut operations)
            .expect("Context remains available");
        assert_eq!(
            operations[0].status(),
            AeadStatus::Executed(Ok(plaintext.len()))
        );
        assert_eq!(ciphertext, expected_ciphertext);
        assert_eq!(tag, expected_tag);

        let mut payload = plaintext.clone();
        let mut tag = vec![0u8; expected_tag.len()];
        let mut operations = [AeadOperation::seal_in_place(
            &mut payload,
            &nonce,
            &associated_data,
            &mut tag,
        )];
        context
            .execute(&mut operations)
            .expect("in-place seal dispatches through the same Context");
        assert_eq!(
            operations[0].status(),
            AeadStatus::Executed(Ok(plaintext.len()))
        );
        assert_eq!(payload, expected_ciphertext);
        assert_eq!(tag, expected_tag);

        let mut opened = vec![0u8; plaintext.len()];
        let mut operations = [AeadOperation::open(
            Input::Contiguous(&expected_ciphertext),
            &nonce,
            &associated_data,
            &expected_tag,
            &mut opened,
        )];
        context
            .execute(&mut operations)
            .expect("out-of-place open dispatches through the same Context");
        assert_eq!(
            operations[0].status(),
            AeadStatus::Executed(Ok(plaintext.len()))
        );
        assert_eq!(opened, plaintext);
    }
}

#[test]
fn portable_hash_catalog_matches_standard_empty_message_vectors() {
    let mut engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in catalog publishes");
    engine.set_selection_policy(SelectionPolicy::only(["hammer:hash-portable"]));

    for (name, expected) in [
        (
            "sha-256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        (
            "sha-384",
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
        ),
        (
            "sha-512",
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
        ),
        (
            "blake2s-256",
            "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9",
        ),
        (
            "blake2b-512",
            "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce",
        ),
    ] {
        let expected = hex::decode(expected).expect("standard digest vector is valid hexadecimal");
        let algorithm = engine
            .algorithm::<Hash>(name)
            .expect("portable digest is registered");
        let mut context = engine
            .context(algorithm)
            .expect("portable digest Context is available");
        let fragments: &[&[u8]] = &[b"", b""];
        let mut output = vec![0u8; expected.len()];
        let mut operations = [HashOperation::new(Input::Scatter(fragments), &mut output)];
        context
            .execute(&mut operations)
            .expect("portable digest dispatches synchronously");
        assert_eq!(operations[0].status(), Some(Ok(expected.len())));
        assert_eq!(output, expected, "{name} selected the wrong function table");
    }
}

#[test]
fn portable_hmac_catalog_matches_rfc_4231_case_one() {
    let mut engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in catalog publishes");
    engine.set_selection_policy(SelectionPolicy::only(["hammer:hmac-portable"]));

    for (name, expected) in [
        (
            "hmac-sha-256",
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        ),
        (
            "hmac-sha-384",
            "afd03944d84895626b0825f4ab46907f15f9dadbe4101ec682aa034c7cebc59cfaea9ea9076ede7f4af152e8b2fa9cb6",
        ),
        (
            "hmac-sha-512",
            "87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cdedaa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854",
        ),
    ] {
        let expected = hex::decode(expected).expect("standard HMAC vector is valid hexadecimal");
        let algorithm = engine
            .algorithm::<Mac>(name)
            .expect("portable HMAC is registered");
        let key = engine
            .create_key(
                &[0x0b; 20],
                KeyPolicy::new(algorithm, KeyOperations::MAC_AUTHENTICATE, false),
            )
            .expect("HMAC key installs");
        let mut context = engine
            .context_with_key(algorithm, key)
            .expect("portable HMAC Context is available");
        let fragments: &[&[u8]] = &[b"Hi ", b"There"];
        let mut output = vec![0u8; expected.len()];
        let mut operations = [MacOperation::authenticate(
            Input::Scatter(fragments),
            &mut output,
        )];
        context
            .execute(&mut operations)
            .expect("portable HMAC dispatches synchronously");
        assert_eq!(operations[0].status(), Some(Ok(expected.len())));
        assert_eq!(output, expected, "{name} selected the wrong function table");
    }
}

#[test]
fn hkdf_installs_policy_limited_derived_keys_without_exposing_secret_bytes() {
    let engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in catalog publishes");
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

    context
        .execute(&mut operations)
        .expect("Context remains available");

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
    let engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in catalog publishes");
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

    context
        .execute(&mut operations)
        .expect("Context remains available");

    assert_eq!(operations[0].status(), KdfStatus::DerivationDenied);
}
