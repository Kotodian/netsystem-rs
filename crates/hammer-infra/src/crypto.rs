//! Protocol-neutral portable cryptographic algorithms.
//!
//! This layer accepts raw caller-provided memory and implements algorithm
//! semantics only. Key policy, implementation selection, prepared contexts,
//! and operation lifecycle belong to `hammer-service`.

use std::fmt;

use sha2::{Digest, Sha256};

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
