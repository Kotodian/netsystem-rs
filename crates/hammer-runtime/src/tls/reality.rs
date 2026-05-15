use btls::aead::{AeadCtx, Algorithm};
use btls::hash::{hmac_sha256, hmac_sha512};
use btls::memcmp;
use hammer_core::config::{RealityOptions, RealityShortId};
use hammer_core::error::{HammerError, HammerResult};

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
    let prk = hmac_sha256(&client_random[..CLIENT_RANDOM_SALT_LEN], shared_secret)
        .map_err(|err| HammerError::internal(format!("derive Reality auth key: {err}")))?;
    let mut info = Vec::with_capacity(REALITY_HKDF_INFO.len() + 1);
    info.extend_from_slice(REALITY_HKDF_INFO);
    info.push(1);
    let auth_key = hmac_sha256(&prk, &info)
        .map_err(|err| HammerError::internal(format!("expand Reality auth key: {err}")))?;
    Ok(RealityAuthKey(auth_key))
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
    let algorithm = Algorithm::aes_256_gcm();
    let cipher = AeadCtx::new_default_tag(&algorithm, auth_key.as_bytes())
        .map_err(|err| HammerError::internal(format!("Reality AES-GCM key: {err}")))?;
    let mut tag = [0u8; SESSION_ID_PLAINTEXT_LEN];
    let tag = cipher
        .seal_in_place(
            &reality_nonce(client_random),
            &mut ciphertext,
            &mut tag,
            client_hello_raw,
        )
        .map_err(|err| HammerError::internal(format!("seal Reality session id: {err}")))?;
    if tag.len() != SESSION_ID_PLAINTEXT_LEN {
        return Err(HammerError::internal("Reality AES-GCM tag length"));
    }

    let mut session_id = [0u8; SESSION_ID_LEN];
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
    let Ok(expected) = hmac_sha512(auth_key.as_bytes(), ed25519_public_key) else {
        return false;
    };
    memcmp::eq(&expected, signature)
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

    let mut plaintext = [0u8; SESSION_ID_PLAINTEXT_LEN];
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

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::config::{RealityOptions, RealityPublicKey, RealityShortId};

    #[test]
    fn session_id_plaintext_encodes_version_time_and_short_id() {
        let short_id = RealityShortId(vec![0x0a, 0x0b, 0x0c]);
        let plaintext =
            session_id_plaintext(&short_id, RealityClientVersion::new(1, 2, 3), 0x0102_0304)
                .expect("plaintext");

        assert_eq!(
            plaintext,
            [1, 2, 3, 0, 1, 2, 3, 4, 0x0a, 0x0b, 0x0c, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn seal_session_id_matches_reality_aad_layout() {
        let options = RealityOptions {
            public_key: RealityPublicKey([9u8; 32]),
            short_id: RealityShortId(vec![0xaa, 0xbb]),
        };
        let client_random = std::array::from_fn(|idx| idx as u8);
        let shared_secret = [0x42u8; 32];
        let auth_key = derive_auth_key(&shared_secret, &client_random).expect("auth key");
        let version = RealityClientVersion::new(0, 1, 0);
        let plaintext =
            session_id_plaintext(&options.short_id, version, 0x6500_0001).expect("plaintext");

        let mut initial_session_id = [0u8; SESSION_ID_LEN];
        initial_session_id[..SESSION_ID_PLAINTEXT_LEN].copy_from_slice(&plaintext);
        let mut client_hello_raw = vec![0u8; 96];
        client_hello_raw[39..39 + SESSION_ID_LEN].copy_from_slice(&initial_session_id);

        let sealed = seal_session_id(
            &options,
            &auth_key,
            &client_random,
            &client_hello_raw,
            version,
            0x6500_0001,
        )
        .expect("sealed session id");
        assert_ne!(
            &sealed.as_bytes()[..SESSION_ID_PLAINTEXT_LEN],
            plaintext.as_slice()
        );

        let opened = open_session_id_for_test(&auth_key, &client_random, &client_hello_raw, sealed)
            .expect("open sealed session id");
        assert_eq!(opened, plaintext);

        client_hello_raw[40] ^= 1;
        assert!(
            open_session_id_for_test(&auth_key, &client_random, &client_hello_raw, sealed).is_err()
        );
    }

    #[test]
    fn temporary_certificate_signature_uses_reality_hmac() {
        let auth_key = RealityAuthKey([0x33u8; 32]);
        let public_key = [0x44u8; 32];
        let signature = hmac_sha512(auth_key.as_bytes(), &public_key).expect("signature");

        assert!(verify_temporary_certificate_signature(
            &auth_key,
            &public_key,
            &signature
        ));

        let mut wrong_signature = signature;
        wrong_signature[0] ^= 1;
        assert!(!verify_temporary_certificate_signature(
            &auth_key,
            &public_key,
            &wrong_signature
        ));
    }

    fn open_session_id_for_test(
        auth_key: &RealityAuthKey,
        client_random: &[u8; 32],
        client_hello_raw: &[u8],
        session_id: RealitySessionId,
    ) -> Result<[u8; SESSION_ID_PLAINTEXT_LEN], btls::error::ErrorStack> {
        let algorithm = Algorithm::aes_256_gcm();
        let cipher = AeadCtx::new_default_tag(&algorithm, auth_key.as_bytes())?;
        let mut plaintext: [u8; SESSION_ID_PLAINTEXT_LEN] = session_id.as_bytes()
            [..SESSION_ID_PLAINTEXT_LEN]
            .try_into()
            .expect("Reality session id prefix is 16 bytes");
        let tag = &session_id.as_bytes()[SESSION_ID_PLAINTEXT_LEN..];
        cipher.open_in_place(
            &reality_nonce(client_random),
            &mut plaintext,
            tag,
            client_hello_raw,
        )?;
        Ok(plaintext)
    }
}
