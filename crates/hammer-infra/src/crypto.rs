//! Protocol-neutral portable cryptographic algorithms.
//!
//! This layer accepts raw caller-owned memory and implements algorithm
//! semantics only. Key policy, implementation selection, prepared contexts,
//! and operation lifecycle belong to `hammer-service`.

pub mod aead;
pub mod hash;
pub mod kdf;
pub mod key_establishment;
pub mod mac;
pub mod signature;
