use blake2::{Blake2b512, Digest};
use hammer_core::error::HammerError;
use rand::RngCore;

const SALT_LEN: usize = 8;
const KEY_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct Salamander {
    password: Vec<u8>,
}

impl Salamander {
    pub fn new(password: Vec<u8>) -> Self {
        Self { password }
    }

    pub fn seal(&self, payload: &[u8]) -> Vec<u8> {
        let mut salt = [0; SALT_LEN];
        rand::thread_rng().fill_bytes(&mut salt);
        let key = self.key(&salt);
        let mut out = Vec::with_capacity(SALT_LEN + payload.len());
        out.extend_from_slice(&salt);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ key[i % KEY_LEN]),
        );
        out
    }

    pub fn open(&self, packet: &[u8]) -> Result<Vec<u8>, HammerError> {
        if packet.len() <= SALT_LEN {
            return Err(HammerError::internal("short salamander packet"));
        }
        let salt = &packet[..SALT_LEN];
        let key = self.key(salt);
        Ok(packet[SALT_LEN..]
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % KEY_LEN])
            .collect())
    }

    fn key(&self, salt: &[u8]) -> [u8; KEY_LEN] {
        let mut hasher = Blake2b512::new();
        hasher.update(&self.password);
        hasher.update(salt);
        let digest = hasher.finalize();
        let mut key = [0; KEY_LEN];
        key.copy_from_slice(&digest[..KEY_LEN]);
        key
    }
}
