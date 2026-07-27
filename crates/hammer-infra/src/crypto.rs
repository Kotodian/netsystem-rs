//! Protocol-neutral portable cryptographic algorithms.
//!
//! This layer accepts raw caller-provided memory and implements algorithm
//! semantics only. Key policy, implementation selection, prepared contexts,
//! and operation lifecycle belong to `hammer-service`.

use std::fmt;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes128Gcm as RustCryptoAes128Gcm, Nonce, Tag};
use sha2::{Digest, Sha256};

const AES_GCM_NONCE_LEN: usize = 12;
const AES_GCM_TAG_LEN: usize = 16;

/// A failure produced by a portable AEAD implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AeadError {
    /// The supplied key length is not valid for the selected algorithm.
    InvalidKeyLength {
        /// Key size required by the algorithm.
        required: usize,
        /// Key size supplied by the caller.
        provided: usize,
    },
    /// The supplied nonce length is not valid for the selected algorithm.
    InvalidNonceLength {
        /// Nonce size required by the algorithm.
        required: usize,
        /// Nonce size supplied by the caller.
        provided: usize,
    },
    /// The supplied tag memory has an invalid length.
    InvalidTagLength {
        /// Tag size required by the algorithm.
        required: usize,
        /// Tag size supplied by the caller.
        provided: usize,
    },
    /// The caller-provided output cannot hold the payload.
    OutputTooSmall {
        /// Output size required for the complete payload.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
    },
    /// Fragment lengths overflow the addressable input size.
    InputLengthOverflow,
    /// The authentication tag did not validate.
    AuthenticationFailed,
    /// The algorithm rejected an otherwise structurally valid input size.
    InputTooLong,
}

impl fmt::Display for AeadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyLength { required, provided } => write!(
                formatter,
                "AEAD key requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidNonceLength { required, provided } => write!(
                formatter,
                "AEAD nonce requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidTagLength { required, provided } => write!(
                formatter,
                "AEAD tag requires {required} bytes but caller provided {provided}"
            ),
            Self::OutputTooSmall { required, provided } => write!(
                formatter,
                "AEAD output requires {required} bytes but caller provided {provided}"
            ),
            Self::InputLengthOverflow => formatter.write_str("AEAD input length overflow"),
            Self::AuthenticationFailed => formatter.write_str("AEAD authentication failed"),
            Self::InputTooLong => formatter.write_str("AEAD input exceeds the algorithm limit"),
        }
    }
}

impl std::error::Error for AeadError {}

/// Portable AES-128-GCM prepared algorithm state.
pub struct Aes128Gcm(RustCryptoAes128Gcm);

impl fmt::Debug for Aes128Gcm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Aes128Gcm").finish_non_exhaustive()
    }
}

impl Aes128Gcm {
    /// Prepares AES-128-GCM state from raw key material.
    ///
    /// # Errors
    ///
    /// Returns [`AeadError::InvalidKeyLength`] unless `key` contains 16 bytes.
    pub fn new(key: &[u8]) -> Result<Self, AeadError> {
        RustCryptoAes128Gcm::new_from_slice(key)
            .map(Self)
            .map_err(|_| AeadError::InvalidKeyLength {
                required: 16,
                provided: key.len(),
            })
    }

    /// Encrypts ordered input fragments into separate caller-owned memory.
    ///
    /// # Errors
    ///
    /// Returns a typed length failure without modifying output, or
    /// [`AeadError::InputTooLong`] if the algorithm rejects the payload size.
    pub fn seal(
        &self,
        input: &[&[u8]],
        nonce: &[u8],
        associated_data: &[u8],
        output: &mut [u8],
        tag: &mut [u8],
    ) -> Result<usize, AeadError> {
        let input_len = validate_aead_memory(input, nonce, output, tag)?;
        copy_fragments(input, &mut output[..input_len]);
        let generated = self
            .0
            .encrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                &mut output[..input_len],
            )
            .map_err(|_| AeadError::InputTooLong)?;
        tag[..AES_GCM_TAG_LEN].copy_from_slice(generated.as_slice());
        Ok(input_len)
    }

    /// Authenticates and decrypts ordered input fragments into separate memory.
    ///
    /// Authentication failure clears the payload portion of `output`.
    ///
    /// # Errors
    ///
    /// Returns typed length failures before modifying output, or
    /// [`AeadError::AuthenticationFailed`] after clearing unauthenticated data.
    pub fn open(
        &self,
        input: &[&[u8]],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
        output: &mut [u8],
    ) -> Result<usize, AeadError> {
        let input_len = validate_aead_memory(input, nonce, output, tag)?;
        copy_fragments(input, &mut output[..input_len]);
        if self
            .0
            .decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                &mut output[..input_len],
                Tag::from_slice(tag),
            )
            .is_err()
        {
            output[..input_len].fill(0);
            return Err(AeadError::AuthenticationFailed);
        }
        Ok(input_len)
    }

    /// Encrypts a caller-owned payload in place and writes a detached tag.
    ///
    /// # Errors
    ///
    /// Returns a typed nonce or tag length failure before modifying the payload,
    /// or [`AeadError::InputTooLong`] for an algorithm size rejection.
    pub fn seal_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &mut [u8],
    ) -> Result<usize, AeadError> {
        validate_nonce_and_tag(nonce, tag)?;
        let generated = self
            .0
            .encrypt_in_place_detached(Nonce::from_slice(nonce), associated_data, payload)
            .map_err(|_| AeadError::InputTooLong)?;
        tag[..AES_GCM_TAG_LEN].copy_from_slice(generated.as_slice());
        Ok(payload.len())
    }

    /// Authenticates and decrypts a caller-owned payload in place.
    ///
    /// Authentication failure clears the complete payload.
    ///
    /// # Errors
    ///
    /// Returns a typed nonce or tag length failure before modifying the payload,
    /// or [`AeadError::AuthenticationFailed`] after clearing it.
    pub fn open_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
    ) -> Result<usize, AeadError> {
        validate_nonce_and_tag(nonce, tag)?;
        if self
            .0
            .decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                payload,
                Tag::from_slice(tag),
            )
            .is_err()
        {
            payload.fill(0);
            return Err(AeadError::AuthenticationFailed);
        }
        Ok(payload.len())
    }
}

fn validate_aead_memory(
    input: &[&[u8]],
    nonce: &[u8],
    output: &[u8],
    tag: &[u8],
) -> Result<usize, AeadError> {
    validate_nonce_and_tag(nonce, tag)?;
    let input_len = input.iter().try_fold(0usize, |length, fragment| {
        length.checked_add(fragment.len())
    });
    let input_len = input_len.ok_or(AeadError::InputLengthOverflow)?;
    if output.len() < input_len {
        return Err(AeadError::OutputTooSmall {
            required: input_len,
            provided: output.len(),
        });
    }
    Ok(input_len)
}

fn validate_nonce_and_tag(nonce: &[u8], tag: &[u8]) -> Result<(), AeadError> {
    if nonce.len() != AES_GCM_NONCE_LEN {
        return Err(AeadError::InvalidNonceLength {
            required: AES_GCM_NONCE_LEN,
            provided: nonce.len(),
        });
    }
    if tag.len() != AES_GCM_TAG_LEN {
        return Err(AeadError::InvalidTagLength {
            required: AES_GCM_TAG_LEN,
            provided: tag.len(),
        });
    }
    Ok(())
}

fn copy_fragments(input: &[&[u8]], output: &mut [u8]) {
    let mut written = 0;
    for fragment in input {
        let next = written + fragment.len();
        output[written..next].copy_from_slice(fragment);
        written = next;
    }
}

/// A failure produced by a portable hash implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashError {
    /// The caller-provided output cannot hold the complete digest.
    OutputTooSmall {
        /// Digest size required by the selected algorithm.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
    },
}

impl fmt::Display for HashError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputTooSmall { required, provided } => write!(
                formatter,
                "hash output requires {required} bytes but caller provided {provided}"
            ),
        }
    }
}

impl std::error::Error for HashError {}

/// Computes SHA-256 over ordered input fragments into caller-provided memory.
///
/// # Errors
///
/// Returns [`HashError::OutputTooSmall`] without modifying `output` when it is
/// shorter than the 32-byte SHA-256 digest.
pub fn sha256(input: &[&[u8]], output: &mut [u8]) -> Result<usize, HashError> {
    const OUTPUT_LEN: usize = 32;

    if output.len() < OUTPUT_LEN {
        return Err(HashError::OutputTooSmall {
            required: OUTPUT_LEN,
            provided: output.len(),
        });
    }

    let mut digest = Sha256::new();
    for fragment in input {
        digest.update(fragment);
    }
    output[..OUTPUT_LEN].copy_from_slice(&digest.finalize());
    Ok(OUTPUT_LEN)
}
