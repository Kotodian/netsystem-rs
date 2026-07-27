use hammer_infra::crypto::{
    KeyEstablishmentAlgorithm, KeyEstablishmentError, KeyEstablishmentOutput, agree, decapsulate,
    encapsulate, generate_keypair, public_key,
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

    assert_eq!(
        public_key(
            KeyEstablishmentAlgorithm::X25519,
            &private,
            &mut actual_public,
        ),
        Ok(actual_public.len())
    );
    assert_eq!(
        agree(
            KeyEstablishmentAlgorithm::X25519,
            &private,
            &peer_public,
            &mut actual_shared,
        ),
        Ok(actual_shared.len())
    );
    assert_eq!(actual_public.as_slice(), expected_public);
    assert_eq!(actual_shared.as_slice(), expected_shared);
}

#[test]
fn nist_curve_key_agreement_is_symmetric_for_canonical_public_keys() {
    for algorithm in [
        KeyEstablishmentAlgorithm::P256,
        KeyEstablishmentAlgorithm::P384,
    ] {
        let mut alice_private = vec![0_u8; algorithm.private_key_len()];
        alice_private[algorithm.private_key_len() - 1] = 1;
        let mut bob_private = vec![0_u8; algorithm.private_key_len()];
        bob_private[algorithm.private_key_len() - 1] = 2;
        let mut alice_public = vec![0_u8; algorithm.public_key_len()];
        let mut bob_public = vec![0_u8; algorithm.public_key_len()];
        let mut alice_shared = vec![0_u8; algorithm.shared_secret_len()];
        let mut bob_shared = vec![0_u8; algorithm.shared_secret_len()];

        public_key(algorithm, &alice_private, &mut alice_public).expect("valid Alice key");
        public_key(algorithm, &bob_private, &mut bob_public).expect("valid Bob key");
        agree(algorithm, &alice_private, &bob_public, &mut alice_shared).expect("Alice agrees");
        agree(algorithm, &bob_private, &alice_public, &mut bob_shared).expect("Bob agrees");

        assert_eq!(alice_shared, bob_shared);
        assert!(alice_shared.iter().any(|byte| *byte != 0));
    }
}

#[test]
fn ml_kem_768_deterministic_keypair_encapsulation_and_decapsulation_round_trip() {
    let algorithm = KeyEstablishmentAlgorithm::MlKem768;
    let key_entropy: Vec<u8> = (0..algorithm.key_generation_entropy_len())
        .map(|value| value as u8)
        .collect();
    let encapsulation_entropy = [0x5a; 32];
    let mut private_key = vec![0_u8; algorithm.private_key_len()];
    let mut public_key = vec![0_u8; algorithm.public_key_len()];
    let mut ciphertext = vec![0_u8; algorithm.ciphertext_len().expect("ML-KEM ciphertext")];
    let mut sender_secret = vec![0_u8; algorithm.shared_secret_len()];
    let mut receiver_secret = vec![0_u8; algorithm.shared_secret_len()];

    generate_keypair(algorithm, &key_entropy, &mut private_key, &mut public_key)
        .expect("ML-KEM key pair is generated");
    encapsulate(
        algorithm,
        &public_key,
        &encapsulation_entropy,
        &mut ciphertext,
        &mut sender_secret,
    )
    .expect("ML-KEM secret is encapsulated");
    decapsulate(algorithm, &private_key, &ciphertext, &mut receiver_secret)
        .expect("ML-KEM secret is decapsulated");

    assert_eq!(sender_secret, receiver_secret);
    assert!(sender_secret.iter().any(|byte| *byte != 0));
}

#[test]
fn key_establishment_rejects_invalid_inputs_without_modifying_output() {
    let mut shared = [0x55_u8; 32];
    assert_eq!(
        agree(
            KeyEstablishmentAlgorithm::X25519,
            &[7; 32],
            &[0; 32],
            &mut shared,
        ),
        Err(KeyEstablishmentError::SmallOrderPublicKey)
    );
    assert_eq!(shared, [0x55; 32]);

    let mut short = [0x55_u8; 31];
    assert_eq!(
        public_key(KeyEstablishmentAlgorithm::X25519, &[7; 32], &mut short),
        Err(KeyEstablishmentError::OutputTooSmall {
            output: KeyEstablishmentOutput::PublicKey,
            required: 32,
            provided: 31,
        })
    );
    assert_eq!(short, [0x55; 31]);

    assert_eq!(
        agree(
            KeyEstablishmentAlgorithm::P256,
            &[1; 32],
            &[4; 65],
            &mut shared,
        ),
        Err(KeyEstablishmentError::InvalidPublicKey)
    );
    assert_eq!(shared, [0x55; 32]);
}

#[test]
fn key_generation_rejects_an_invalid_scalar_without_modifying_either_output() {
    let mut private_key = [0x55_u8; 32];
    let mut public_key = [0x55_u8; 65];

    assert_eq!(
        generate_keypair(
            KeyEstablishmentAlgorithm::P256,
            &[0; 32],
            &mut private_key,
            &mut public_key,
        ),
        Err(KeyEstablishmentError::InvalidPrivateKey)
    );
    assert_eq!(private_key, [0x55; 32]);
    assert_eq!(public_key, [0x55; 65]);
}

#[test]
fn ml_kem_rejects_a_short_ciphertext_without_modifying_shared_secret_output() {
    let mut shared_secret = [0x55_u8; 32];

    assert_eq!(
        decapsulate(
            KeyEstablishmentAlgorithm::MlKem768,
            &[0; 64],
            &[0; 1087],
            &mut shared_secret,
        ),
        Err(KeyEstablishmentError::InvalidCiphertextLength {
            required: 1088,
            provided: 1087,
        })
    );
    assert_eq!(shared_secret, [0x55; 32]);
}
