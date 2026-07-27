//! Portable key-derivation implementations.

use std::fmt;

use hmac::digest::{Digest, core_api::BlockSizeUser};

/// A failure produced while expanding derived key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Caller output cannot hold the requested bytes.
    OutputTooSmall {
        /// Requested output bytes.
        required: usize,
        /// Supplied output bytes.
        provided: usize,
    },
    /// The algorithm cannot produce the requested number of bytes.
    OutputTooLong {
        /// Requested output bytes.
        requested: usize,
        /// Maximum output bytes.
        maximum: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputTooSmall { required, provided } => write!(
                formatter,
                "KDF output requires {required} bytes but caller provided {provided}"
            ),
            Self::OutputTooLong { requested, maximum } => write!(
                formatter,
                "KDF requested {requested} bytes but the algorithm limit is {maximum}"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Complete semantics supplied by one key-derivation algorithm.
pub trait Algorithm: Sized + fmt::Debug + 'static {
    /// Underlying digest length in bytes.
    const OUTPUT_LEN: usize;

    /// Extracts derivation state from salt and input key material.
    fn new(salt: Option<&[u8]>, input_key_material: &[u8]) -> Self;

    /// Expands ordered info fragments into caller-owned output.
    fn expand(&self, info: &[&[u8]], requested: usize, output: &mut [u8]) -> Result<usize, Error>;
}

/// HKDF semantics shared by digest algorithms.
pub struct Hkdf<D, const OUTPUT_LEN: usize>(hkdf::SimpleHkdf<D>)
where
    D: Digest + BlockSizeUser + Clone;

impl<D, const OUTPUT_LEN: usize> fmt::Debug for Hkdf<D, OUTPUT_LEN>
where
    D: Digest + BlockSizeUser + Clone,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(std::any::type_name::<D>())
    }
}

impl<D, const OUTPUT_LEN: usize> Algorithm for Hkdf<D, OUTPUT_LEN>
where
    D: Digest + BlockSizeUser + Clone + 'static,
{
    const OUTPUT_LEN: usize = OUTPUT_LEN;

    fn new(salt: Option<&[u8]>, input_key_material: &[u8]) -> Self {
        Self(hkdf::SimpleHkdf::new(salt, input_key_material))
    }

    fn expand(&self, info: &[&[u8]], requested: usize, output: &mut [u8]) -> Result<usize, Error> {
        if output.len() < requested {
            return Err(Error::OutputTooSmall {
                required: requested,
                provided: output.len(),
            });
        }
        let maximum = 255 * OUTPUT_LEN;
        if requested > maximum {
            return Err(Error::OutputTooLong { requested, maximum });
        }
        self.0
            .expand_multi_info(info, &mut output[..requested])
            .map_err(|_| Error::OutputTooLong { requested, maximum })?;
        Ok(requested)
    }
}

/// HKDF-SHA-256.
pub type HkdfSha256 = Hkdf<sha2::Sha256, 32>;
/// HKDF-SHA-384.
pub type HkdfSha384 = Hkdf<sha2::Sha384, 48>;
/// HKDF-SHA-512.
pub type HkdfSha512 = Hkdf<sha2::Sha512, 64>;
