use hammer_infra::crypto::key_establishment::{
    Algorithm, Error, MlKem768, Output, P256, P384, X25519,
};

#[test]
fn x25519_matches_rfc_7748_alice_vector() {
    let private = hex::decode("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
        .expect("valid vector");
    let peer_public =
        hex::decode("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f")
            .expect("valid vector");
    let expected_public =
        hex::decode("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
            .expect("valid vector");
    let expected_shared =
        hex::decode("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742")
            .expect("valid vector");
    let mut actual_public = [0_u8; 32];
    let mut actual_shared = [0_u8; 32];
    let algorithm = X25519;

    assert_eq!(
        algorithm.public_key(&private, &mut actual_public),
        Ok(actual_public.len())
    );
    assert_eq!(
        algorithm.agree(&private, &peer_public, &mut actual_shared),
        Ok(actual_shared.len())
    );
    assert_eq!(actual_public.as_slice(), expected_public);
    assert_eq!(actual_shared.as_slice(), expected_shared);
}

fn assert_nist_curve_agreement<A: Algorithm>() {
    let algorithm = A::default();
    let mut alice_private = vec![0_u8; A::PRIVATE_KEY_LEN];
    alice_private[A::PRIVATE_KEY_LEN - 1] = 1;
    let mut bob_private = vec![0_u8; A::PRIVATE_KEY_LEN];
    bob_private[A::PRIVATE_KEY_LEN - 1] = 2;
    let mut alice_public = vec![0_u8; A::PUBLIC_KEY_LEN];
    let mut bob_public = vec![0_u8; A::PUBLIC_KEY_LEN];
    let mut alice_shared = vec![0_u8; A::SHARED_SECRET_LEN];
    let mut bob_shared = vec![0_u8; A::SHARED_SECRET_LEN];

    algorithm
        .public_key(&alice_private, &mut alice_public)
        .expect("valid Alice key");
    algorithm
        .public_key(&bob_private, &mut bob_public)
        .expect("valid Bob key");
    algorithm
        .agree(&alice_private, &bob_public, &mut alice_shared)
        .expect("Alice agrees");
    algorithm
        .agree(&bob_private, &alice_public, &mut bob_shared)
        .expect("Bob agrees");
    assert_eq!(alice_shared, bob_shared);
    assert!(alice_shared.iter().any(|byte| *byte != 0));
}

#[test]
fn nist_curve_key_agreement_is_symmetric_for_canonical_public_keys() {
    assert_nist_curve_agreement::<P256>();
    assert_nist_curve_agreement::<P384>();
}

#[test]
fn ml_kem_768_deterministic_round_trip() {
    let algorithm = MlKem768;
    let key_entropy: Vec<u8> = (0..MlKem768::PRIVATE_KEY_LEN)
        .map(|value| value as u8)
        .collect();
    let mut private_key = vec![0; MlKem768::PRIVATE_KEY_LEN];
    let mut public_key = vec![0; MlKem768::PUBLIC_KEY_LEN];
    let mut ciphertext = vec![0; MlKem768::CIPHERTEXT_LEN.expect("ciphertext length")];
    let mut sender_secret = vec![0; MlKem768::SHARED_SECRET_LEN];
    let mut receiver_secret = vec![0; MlKem768::SHARED_SECRET_LEN];

    algorithm
        .generate_keypair(&key_entropy, &mut private_key, &mut public_key)
        .expect("key pair is generated");
    algorithm
        .encapsulate(
            &public_key,
            &[0x5a; 32],
            &mut ciphertext,
            &mut sender_secret,
        )
        .expect("secret is encapsulated");
    algorithm
        .decapsulate(&private_key, &ciphertext, &mut receiver_secret)
        .expect("secret is decapsulated");
    assert_eq!(sender_secret, receiver_secret);
    assert!(sender_secret.iter().any(|byte| *byte != 0));
}

#[test]
fn key_establishment_failures_leave_outputs_unchanged() {
    let mut shared = [0x55; 32];
    assert_eq!(
        X25519.agree(&[7; 32], &[0; 32], &mut shared),
        Err(Error::SmallOrderPublicKey)
    );
    assert_eq!(shared, [0x55; 32]);

    let mut short = [0x55; 31];
    assert_eq!(
        X25519.public_key(&[7; 32], &mut short),
        Err(Error::OutputTooSmall {
            output: Output::PublicKey,
            required: 32,
            provided: 31,
        })
    );
    assert_eq!(short, [0x55; 31]);

    assert_eq!(
        P256.agree(&[1; 32], &[4; 65], &mut shared),
        Err(Error::InvalidPublicKey)
    );
    assert_eq!(shared, [0x55; 32]);
}

#[test]
fn generation_and_decapsulation_validate_before_writing() {
    let mut private_key = [0x55; 32];
    let mut public_key = [0x55; 65];
    assert_eq!(
        P256.generate_keypair(&[0; 32], &mut private_key, &mut public_key),
        Err(Error::InvalidPrivateKey)
    );
    assert_eq!(private_key, [0x55; 32]);
    assert_eq!(public_key, [0x55; 65]);

    let mut shared_secret = [0x55; 32];
    assert_eq!(
        MlKem768.decapsulate(&[0; 64], &[0; 1087], &mut shared_secret),
        Err(Error::InvalidCiphertextLength {
            required: 1088,
            provided: 1087,
        })
    );
    assert_eq!(shared_secret, [0x55; 32]);
}
