use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::hmac;

use crate::config::{RealityOptions, RealityShortId};
use crate::error::{HammerError, HammerResult};

const SESSION_ID_LEN: usize = 32;
const SESSION_ID_PLAINTEXT_LEN: usize = 16;
const CLIENT_RANDOM_SALT_LEN: usize = 20;
const AES_GCM_NONCE_LEN: usize = 12;
const REALITY_HKDF_INFO: &[u8] = b"REALITY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealityClientVersion([u8; 4]);

impl RealityClientVersion {
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self([major, minor, patch, 0])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealityAuthKey([u8; 32]);

impl RealityAuthKey {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealitySessionId([u8; SESSION_ID_LEN]);

impl RealitySessionId {
    pub fn as_bytes(&self) -> &[u8; SESSION_ID_LEN] {
        &self.0
    }
}

pub fn derive_auth_key(
    shared_secret: &[u8; 32],
    client_random: &[u8; 32],
) -> HammerResult<RealityAuthKey> {
    let prk = hmac_sha256(&client_random[..CLIENT_RANDOM_SALT_LEN], shared_secret);
    let mut info = Vec::with_capacity(REALITY_HKDF_INFO.len() + 1);
    info.extend_from_slice(REALITY_HKDF_INFO);
    info.push(1);
    Ok(RealityAuthKey(hmac_sha256(&prk, &info)))
}

pub fn seal_session_id(
    options: &RealityOptions,
    auth_key: &RealityAuthKey,
    client_random: &[u8; 32],
    client_hello_raw: &[u8],
    version: RealityClientVersion,
    unix_time: u32,
) -> HammerResult<RealitySessionId> {
    let mut ciphertext = session_id_plaintext(&options.short_id, version, unix_time)?;
    let key = UnboundKey::new(&AES_256_GCM, auth_key.as_bytes())
        .map(LessSafeKey::new)
        .map_err(|_| HammerError::internal("Reality AES-GCM key"))?;
    let tag = key
        .seal_in_place_separate_tag(
            Nonce::assume_unique_for_key(reality_nonce(client_random)),
            Aad::from(client_hello_raw),
            &mut ciphertext,
        )
        .map_err(|_| HammerError::internal("seal Reality session id"))?;
    let tag: &[u8; SESSION_ID_PLAINTEXT_LEN] = tag
        .as_ref()
        .try_into()
        .map_err(|_| HammerError::internal("Reality AES-GCM tag length"))?;

    let mut session_id = [0_u8; SESSION_ID_LEN];
    session_id[..SESSION_ID_PLAINTEXT_LEN].copy_from_slice(&ciphertext);
    session_id[SESSION_ID_PLAINTEXT_LEN..].copy_from_slice(tag);
    Ok(RealitySessionId(session_id))
}

pub fn verify_temporary_certificate_signature(
    auth_key: &RealityAuthKey,
    ed25519_public_key: &[u8],
    signature: &[u8],
) -> bool {
    if ed25519_public_key.len() != 32 || signature.len() != 64 {
        return false;
    }
    let key = hmac::Key::new(hmac::HMAC_SHA512, auth_key.as_bytes());
    hmac::verify(&key, ed25519_public_key, signature).is_ok()
}

fn session_id_plaintext(
    short_id: &RealityShortId,
    version: RealityClientVersion,
    unix_time: u32,
) -> HammerResult<[u8; SESSION_ID_PLAINTEXT_LEN]> {
    if short_id.0.len() > 8 {
        return Err(HammerError::config_validation(
            "tls.reality.short_id must be at most 8 bytes",
        ));
    }

    let mut plaintext = [0_u8; SESSION_ID_PLAINTEXT_LEN];
    plaintext[..4].copy_from_slice(&version.0);
    plaintext[4..8].copy_from_slice(&unix_time.to_be_bytes());
    plaintext[8..8 + short_id.0.len()].copy_from_slice(&short_id.0);
    Ok(plaintext)
}

fn reality_nonce(client_random: &[u8; 32]) -> [u8; AES_GCM_NONCE_LEN] {
    client_random[CLIENT_RANDOM_SALT_LEN..]
        .try_into()
        .expect("client random suffix is 12-byte AES-GCM nonce")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, message)
        .as_ref()
        .try_into()
        .expect("HMAC-SHA256 output is 32 bytes")
}
