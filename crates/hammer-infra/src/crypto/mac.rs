//! Portable message-authentication implementations.

use std::fmt;

use hmac::digest::{Digest, core_api::BlockSizeUser};

/// A failure produced while computing an authenticator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Caller output cannot hold the complete authenticator.
    OutputTooSmall {
        /// Required output bytes.
        required: usize,
        /// Supplied output bytes.
        provided: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputTooSmall { required, provided } => write!(
                formatter,
                "MAC output requires {required} bytes but caller provided {provided}"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Complete semantics supplied by one message-authentication algorithm.
pub trait Algorithm: Sized + fmt::Debug + 'static {
    /// Authenticator length in bytes.
    const OUTPUT_LEN: usize;

    /// Prepares authentication state from key material.
    fn new(key: &[u8]) -> Self;

    /// Authenticates ordered fragments into caller-owned output.
    fn authenticate(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, Error>;
}

/// HMAC semantics shared by digest algorithms.
pub struct Hmac<D, const OUTPUT_LEN: usize>(hmac::SimpleHmac<D>)
where
    D: Digest + BlockSizeUser;

impl<D, const OUTPUT_LEN: usize> fmt::Debug for Hmac<D, OUTPUT_LEN>
where
    D: Digest + BlockSizeUser,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(std::any::type_name::<D>())
    }
}

impl<D, const OUTPUT_LEN: usize> Algorithm for Hmac<D, OUTPUT_LEN>
where
    D: Digest + BlockSizeUser + Clone + 'static,
{
    const OUTPUT_LEN: usize = OUTPUT_LEN;

    fn new(key: &[u8]) -> Self {
        Self(
            <hmac::SimpleHmac<D> as hmac::Mac>::new_from_slice(key)
                .expect("HMAC accepts keys of every length"),
        )
    }

    fn authenticate(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, Error> {
        if output.len() < OUTPUT_LEN {
            return Err(Error::OutputTooSmall {
                required: OUTPUT_LEN,
                provided: output.len(),
            });
        }
        let mut state = self.0.clone();
        for fragment in input {
            hmac::Mac::update(&mut state, fragment);
        }
        let authenticator = hmac::Mac::finalize(state).into_bytes();
        assert_eq!(
            authenticator.len(),
            OUTPUT_LEN,
            "HMAC output length must match its algorithm declaration"
        );
        output[..OUTPUT_LEN].copy_from_slice(&authenticator);
        Ok(OUTPUT_LEN)
    }
}

/// HMAC-SHA-256.
pub type HmacSha256 = Hmac<sha2::Sha256, 32>;
/// HMAC-SHA-384.
pub type HmacSha384 = Hmac<sha2::Sha384, 48>;
/// HMAC-SHA-512.
pub type HmacSha512 = Hmac<sha2::Sha512, 64>;
