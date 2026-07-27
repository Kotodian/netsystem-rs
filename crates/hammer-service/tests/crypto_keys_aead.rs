use std::error::Error;

use hammer_infra::crypto::AeadError;
use hammer_service::crypto::{
    Aead, AeadDirection, AeadInput, AeadOperation, AeadStatus, Batch, ContextError, Engine, Hash,
    KeyError, KeyOperations, KeyPolicy,
};

const CIPHERTEXT: [u8; 16] = [
    0x03, 0x88, 0xda, 0xce, 0x60, 0xb6, 0xa3, 0x92, 0xf3, 0x28, 0xc2, 0xb9, 0x71, 0xb2, 0xfe, 0x78,
];
const TAG: [u8; 16] = [
    0xab, 0x6e, 0x47, 0xd4, 0x2c, 0xec, 0x13, 0xbd, 0xf5, 0x3a, 0x67, 0xb2, 0x12, 0x57, 0xbd, 0xdf,
];

#[test]
fn aead_context_executes_scatter_gather_seal_batch() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let policy = KeyPolicy::new(
        algorithm,
        KeyOperations::AEAD_SEAL | KeyOperations::AEAD_OPEN,
        false,
    );
    let key = engine
        .create_key(&[0; 16], policy)
        .expect("key pool has capacity");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("key permits AES-128-GCM");
    let fragments: [&[u8]; 2] = [&[0; 8], &[0; 8]];
    let mut output = [0; 16];
    let mut tag = [0; 16];
    let mut operations = [AeadOperation::seal(
        AeadInput::Scatter(&fragments),
        &[0; 12],
        &[],
        &mut output,
        &mut tag,
    )];
    let mut batch = Batch::new(&mut operations);

    context.execute(&mut batch);

    assert_eq!(operations[0].status(), AeadStatus::Complete { written: 16 });
    assert_eq!((output, tag), (CIPHERTEXT, TAG));
}

#[test]
fn aead_authentication_failure_is_per_operation_and_exposes_no_plaintext() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let policy = KeyPolicy::new(algorithm, KeyOperations::AEAD_OPEN, false);
    let key = engine
        .create_key(&[0; 16], policy)
        .expect("key pool has capacity");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("key permits AES-128-GCM open");
    let mut invalid_tag = TAG;
    invalid_tag[0] ^= 1;
    let mut output = [0xa5; 16];
    let mut operations = [AeadOperation::open(
        AeadInput::Contiguous(&CIPHERTEXT),
        &[0; 12],
        &[],
        &invalid_tag,
        &mut output,
    )];
    let mut batch = Batch::new(&mut operations);

    context.execute(&mut batch);

    assert_eq!(operations[0].status(), AeadStatus::AuthenticationFailed);
    assert_eq!(output, [0; 16]);
}

#[test]
fn key_destruction_waits_for_context_release_and_rejects_stale_handle() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let policy = KeyPolicy::new(algorithm, KeyOperations::AEAD_SEAL, false);
    let key = engine
        .create_key(&[0; 16], policy)
        .expect("key pool has capacity");
    let context = engine
        .context_with_key(algorithm, key)
        .expect("key permits AES-128-GCM seal");

    let in_use = engine
        .destroy_key(key)
        .expect_err("live context retains the key");
    assert_eq!(in_use, KeyError::KeyInUse { key, contexts: 1 });
    drop(context);
    engine
        .destroy_key(key)
        .expect("released key can be destroyed");

    let stale = engine
        .context_with_key(algorithm, key)
        .expect_err("destroyed generation must be stale");
    assert_eq!(stale, ContextError::StaleKey { key });
}

#[test]
fn secret_export_requires_explicit_policy_permission() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let denied_policy = KeyPolicy::new(algorithm, KeyOperations::AEAD_SEAL, false);
    let denied_key = engine
        .create_key(&[7; 16], denied_policy)
        .expect("key pool has capacity");
    let mut output = [0; 16];

    let denied = engine
        .export_secret(denied_key, &mut output)
        .expect_err("policy does not allow Secret Export");
    assert_eq!(denied, KeyError::SecretExportDenied { key: denied_key });

    let allowed_policy = KeyPolicy::new(algorithm, KeyOperations::AEAD_SEAL, true);
    let allowed_key = engine
        .create_key(&[7; 16], allowed_policy)
        .expect("key pool has capacity");
    let written = engine
        .export_secret(allowed_key, &mut output)
        .expect("explicit Secret Export is allowed");
    assert_eq!((written, output), (16, [7; 16]));
}

#[test]
fn aead_context_round_trips_in_place_with_associated_data() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let policy = KeyPolicy::new(
        algorithm,
        KeyOperations::AEAD_SEAL | KeyOperations::AEAD_OPEN,
        false,
    );
    let key = engine
        .create_key(&[3; 16], policy)
        .expect("key pool has capacity");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("key permits AES-128-GCM");
    let original = *b"caller-owned-data";
    let mut payload = original;
    let mut tag = [0; 16];

    {
        let mut operations = [AeadOperation::seal_in_place(
            &mut payload,
            &[5; 12],
            b"associated-data",
            &mut tag,
        )];
        context.execute(&mut Batch::new(&mut operations));
        assert_eq!(operations[0].status(), AeadStatus::Complete { written: 17 });
    }
    {
        let mut operations = [AeadOperation::open_in_place(
            &mut payload,
            &[5; 12],
            b"associated-data",
            &tag,
        )];
        context.execute(&mut Batch::new(&mut operations));
        assert_eq!(operations[0].status(), AeadStatus::Complete { written: 17 });
    }

    assert_eq!(payload, original);
}

#[test]
fn aead_batch_reports_operation_policy_denial_without_mutating_output() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let policy = KeyPolicy::new(algorithm, KeyOperations::AEAD_SEAL, false);
    let key = engine
        .create_key(&[0; 16], policy)
        .expect("key pool has capacity");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("key permits AES-128-GCM seal");
    let mut output = [0xa5; 16];
    let mut operations = [AeadOperation::open(
        AeadInput::Contiguous(&CIPHERTEXT),
        &[0; 12],
        &[],
        &TAG,
        &mut output,
    )];

    context.execute(&mut Batch::new(&mut operations));

    assert_eq!(
        operations[0].status(),
        AeadStatus::PolicyDenied {
            operation: AeadDirection::Open,
        }
    );
    assert_eq!(output, [0xa5; 16]);
}

#[test]
fn key_policy_rejects_an_algorithm_from_another_family() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let hash = engine
        .algorithm::<Hash>("sha-256")
        .expect("SHA-256 is built in");
    let aead = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let key = engine
        .create_key(
            &[0; 16],
            KeyPolicy::new(hash, KeyOperations::AEAD_SEAL, false),
        )
        .expect("key pool has capacity");

    let error = engine
        .context_with_key(aead, key)
        .expect_err("hash-only policy cannot create an AEAD Context");

    assert_eq!(error, ContextError::AlgorithmDenied { key, algorithm: 0 });
}

#[test]
fn invalid_key_context_creation_is_failure_atomic() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let policy = KeyPolicy::new(algorithm, KeyOperations::AEAD_SEAL, false);
    let key = engine
        .create_key(&[0; 15], policy)
        .expect("key pool has capacity");

    let error = engine
        .context_with_key(algorithm, key)
        .expect_err("AES-128-GCM rejects a 15-byte key");

    assert_eq!(
        error.source().and_then(|source| source.downcast_ref()),
        Some(&AeadError::InvalidKeyLength {
            required: 16,
            provided: 15,
        })
    );
    assert_eq!(
        error,
        ContextError::InvalidKeyLength {
            key,
            required: 16,
            provided: 15,
            source: AeadError::InvalidKeyLength {
                required: 16,
                provided: 15,
            },
        }
    );
    engine
        .destroy_key(key)
        .expect("failed Context creation retained no key reference");
}

#[test]
fn opaque_key_handle_can_cross_threads_without_key_material() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Aead>("aes-128-gcm")
        .expect("AES-128-GCM is built in");
    let key = engine
        .create_key(
            &[9; 16],
            KeyPolicy::new(algorithm, KeyOperations::AEAD_SEAL, false),
        )
        .expect("key pool has capacity");

    let returned = std::thread::spawn(move || key)
        .join()
        .expect("KeyHandle is a thread-safe identity");

    assert_eq!(returned, key);
}
