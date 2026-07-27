use hammer_infra::crypto::{AeadAlgorithm, AeadCipher, AeadError};

const CIPHERTEXT: [u8; 16] = [
    0x03, 0x88, 0xda, 0xce, 0x60, 0xb6, 0xa3, 0x92, 0xf3, 0x28, 0xc2, 0xb9, 0x71, 0xb2, 0xfe, 0x78,
];
const TAG: [u8; 16] = [
    0xab, 0x6e, 0x47, 0xd4, 0x2c, 0xec, 0x13, 0xbd, 0xf5, 0x3a, 0x67, 0xb2, 0x12, 0x57, 0xbd, 0xdf,
];

#[test]
fn aes_128_gcm_seals_scatter_input_to_the_known_vector() {
    let cipher =
        AeadCipher::new(AeadAlgorithm::Aes128Gcm, &[0; 16]).expect("AES-128 key has 16 bytes");
    let mut output = [0; 16];
    let mut tag = [0; 16];

    cipher
        .seal(&[&[0; 8], &[0; 8]], &[0; 12], &[], &mut output, &mut tag)
        .expect("known vector has valid capacities");

    assert_eq!((output, tag), (CIPHERTEXT, TAG));
}

#[test]
fn aes_128_gcm_authentication_failure_clears_out_of_place_output() {
    let cipher =
        AeadCipher::new(AeadAlgorithm::Aes128Gcm, &[0; 16]).expect("AES-128 key has 16 bytes");
    let mut output = [0xa5; 16];
    let mut invalid_tag = TAG;
    invalid_tag[0] ^= 1;

    let error = cipher
        .open(&[&CIPHERTEXT], &[0; 12], &[], &invalid_tag, &mut output)
        .expect_err("modified tag must be rejected");

    assert_eq!(error, AeadError::AuthenticationFailed);
    assert_eq!(output, [0; 16]);
}

#[test]
fn aes_128_gcm_round_trips_in_place() {
    let cipher =
        AeadCipher::new(AeadAlgorithm::Aes128Gcm, &[0; 16]).expect("AES-128 key has 16 bytes");
    let mut payload = [0; 16];
    let mut tag = [0; 16];

    cipher
        .seal_in_place(&mut payload, &[0; 12], &[], &mut tag)
        .expect("known vector has valid capacities");
    cipher
        .open_in_place(&mut payload, &[0; 12], &[], &tag)
        .expect("matching tag authenticates");

    assert_eq!(payload, [0; 16]);
}
