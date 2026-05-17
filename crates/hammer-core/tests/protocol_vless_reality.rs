#![cfg(feature = "vless")]

use hammer_core::config::{RealityOptions, RealityPublicKey, RealityShortId};
use hammer_core::protocol::vless::reality::{
    RealityAuthKey, RealityClientVersion, derive_auth_key, seal_session_id,
    verify_temporary_certificate_signature,
};

#[test]
fn reality_auth_key_derivation_matches_hkdf_layout() {
    let client_random = std::array::from_fn(|idx| idx as u8);
    let shared_secret = [0x42_u8; 32];

    let auth_key = derive_auth_key(&shared_secret, &client_random).expect("auth key");

    assert_eq!(
        auth_key.as_bytes(),
        &hex_bytes("a0f7148b3431834ec80ea3c06812e78c670c5359af2f702ecc9ae775c44e3940")
    );
}

#[test]
fn reality_session_id_seals_version_time_short_id_with_client_hello_aad() {
    let options = RealityOptions {
        public_key: RealityPublicKey([9_u8; 32]),
        short_id: RealityShortId(vec![0xaa, 0xbb]),
    };
    let client_random = std::array::from_fn(|idx| idx as u8);
    let shared_secret = [0x42_u8; 32];
    let auth_key = derive_auth_key(&shared_secret, &client_random).expect("auth key");
    let mut client_hello_raw = vec![0_u8; 96];
    client_hello_raw[39..55]
        .copy_from_slice(&[0, 1, 0, 0, 0x65, 0, 0, 1, 0xaa, 0xbb, 0, 0, 0, 0, 0, 0]);

    let session_id = seal_session_id(
        &options,
        &auth_key,
        &client_random,
        &client_hello_raw,
        RealityClientVersion::new(0, 1, 0),
        0x6500_0001,
    )
    .expect("session id");

    assert_eq!(
        session_id.as_bytes(),
        &hex_bytes("a91644933fe88eeb9f515209b4070da78ace9f7199cb97d7ba0e8bdc080ff04f")
    );
}

#[test]
fn reality_temporary_certificate_signature_verifies_hmac_sha512() {
    let auth_key = RealityAuthKey::from_bytes([0x33_u8; 32]);
    let public_key = [0x44_u8; 32];
    let signature: [u8; 64] = hex_bytes(
        "2a8307b3de0990e78df273eabc178a61022f1051702e83f1929649dd1c24bc9e\
         b4abbf898658e557184eef8e7d222ed1b5f5dac717aedd1a7bee2fa92a9755fc",
    );

    assert!(verify_temporary_certificate_signature(
        &auth_key,
        &public_key,
        &signature
    ));
    assert!(!verify_temporary_certificate_signature(
        &auth_key,
        &public_key,
        &signature[..63]
    ));
}

fn hex_bytes<const N: usize>(hex: &str) -> [u8; N] {
    let hex = hex
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(hex.len(), N * 2);
    let mut out = [0_u8; N];
    for (idx, chunk) in hex.chunks_exact(2).enumerate() {
        out[idx] = (hex_nibble(chunk[0]) << 4) | hex_nibble(chunk[1]);
    }
    out
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex byte: {byte}"),
    }
}
