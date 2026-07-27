//! Portable authenticated-encryption implementations.

use std::fmt;

use aes_gcm::aead::consts::{U12, U16};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Nonce, Tag};

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// A failure produced by an authenticated-encryption implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// The supplied key length is invalid.
    InvalidKeyLength {
        /// Required key bytes.
        required: usize,
        /// Supplied key bytes.
        provided: usize,
    },
    /// The supplied nonce length is invalid.
    InvalidNonceLength {
        /// Required nonce bytes.
        required: usize,
        /// Supplied nonce bytes.
        provided: usize,
    },
    /// The supplied tag length is invalid.
    InvalidTagLength {
        /// Required tag bytes.
        required: usize,
        /// Supplied tag bytes.
        provided: usize,
    },
    /// Caller output cannot hold the payload.
    OutputTooSmall {
        /// Required output bytes.
        required: usize,
        /// Supplied output bytes.
        provided: usize,
    },
    /// Fragment lengths overflow the addressable input size.
    InputLengthOverflow,
    /// The authentication tag did not validate.
    AuthenticationFailed,
    /// The algorithm rejected the payload size.
    InputTooLong,
}

impl fmt::Display for Error {
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

impl std::error::Error for Error {}

/// Complete semantics supplied by one authenticated-encryption algorithm.
pub trait Algorithm: Sized + fmt::Debug + 'static {
    /// Required key bytes.
    const KEY_LEN: usize;

    /// Prepares algorithm state from key material.
    fn new(key: &[u8]) -> Result<Self, Error>;

    /// Encrypts ordered fragments into separate output and a detached tag.
    fn seal(
        &self,
        input: &[&[u8]],
        nonce: &[u8],
        associated_data: &[u8],
        output: &mut [u8],
        tag: &mut [u8],
    ) -> Result<usize, Error>;

    /// Authenticates and decrypts ordered fragments into separate output.
    fn open(
        &self,
        input: &[&[u8]],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
        output: &mut [u8],
    ) -> Result<usize, Error>;

    /// Encrypts a caller-owned payload in place and writes a detached tag.
    fn seal_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &mut [u8],
    ) -> Result<usize, Error>;

    /// Authenticates and decrypts a caller-owned payload in place.
    fn open_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
    ) -> Result<usize, Error>;
}

/// Authenticated-encryption semantics shared by fixed-nonce, fixed-tag ciphers.
pub struct Cipher<C, const KEY_LEN: usize>(Box<C>);

impl<C, const KEY_LEN: usize> fmt::Debug for Cipher<C, KEY_LEN> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(std::any::type_name::<C>())
    }
}

impl<C, const KEY_LEN: usize> Algorithm for Cipher<C, KEY_LEN>
where
    C: AeadInPlace<NonceSize = U12, TagSize = U16> + KeyInit + 'static,
{
    const KEY_LEN: usize = KEY_LEN;

    fn new(key: &[u8]) -> Result<Self, Error> {
        C::new_from_slice(key)
            .map(Box::new)
            .map(Self)
            .map_err(|_| Error::InvalidKeyLength {
                required: KEY_LEN,
                provided: key.len(),
            })
    }

    fn seal(
        &self,
        input: &[&[u8]],
        nonce: &[u8],
        associated_data: &[u8],
        output: &mut [u8],
        tag: &mut [u8],
    ) -> Result<usize, Error> {
        if nonce.len() != NONCE_LEN {
            return Err(Error::InvalidNonceLength {
                required: NONCE_LEN,
                provided: nonce.len(),
            });
        }
        if tag.len() != TAG_LEN {
            return Err(Error::InvalidTagLength {
                required: TAG_LEN,
                provided: tag.len(),
            });
        }
        let input_len = input.iter().try_fold(0usize, |length, fragment| {
            length.checked_add(fragment.len())
        });
        let input_len = input_len.ok_or(Error::InputLengthOverflow)?;
        if output.len() < input_len {
            return Err(Error::OutputTooSmall {
                required: input_len,
                provided: output.len(),
            });
        }
        let mut written = 0;
        for fragment in input {
            let next = written + fragment.len();
            output[written..next].copy_from_slice(fragment);
            written = next;
        }
        let generated = self
            .0
            .encrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                &mut output[..input_len],
            )
            .map_err(|_| Error::InputTooLong)?;
        tag.copy_from_slice(generated.as_slice());
        Ok(input_len)
    }

    fn open(
        &self,
        input: &[&[u8]],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
        output: &mut [u8],
    ) -> Result<usize, Error> {
        if nonce.len() != NONCE_LEN {
            return Err(Error::InvalidNonceLength {
                required: NONCE_LEN,
                provided: nonce.len(),
            });
        }
        if tag.len() != TAG_LEN {
            return Err(Error::InvalidTagLength {
                required: TAG_LEN,
                provided: tag.len(),
            });
        }
        let input_len = input.iter().try_fold(0usize, |length, fragment| {
            length.checked_add(fragment.len())
        });
        let input_len = input_len.ok_or(Error::InputLengthOverflow)?;
        if output.len() < input_len {
            return Err(Error::OutputTooSmall {
                required: input_len,
                provided: output.len(),
            });
        }
        let mut written = 0;
        for fragment in input {
            let next = written + fragment.len();
            output[written..next].copy_from_slice(fragment);
            written = next;
        }
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
            return Err(Error::AuthenticationFailed);
        }
        Ok(input_len)
    }

    fn seal_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &mut [u8],
    ) -> Result<usize, Error> {
        if nonce.len() != NONCE_LEN {
            return Err(Error::InvalidNonceLength {
                required: NONCE_LEN,
                provided: nonce.len(),
            });
        }
        if tag.len() != TAG_LEN {
            return Err(Error::InvalidTagLength {
                required: TAG_LEN,
                provided: tag.len(),
            });
        }
        let generated = self
            .0
            .encrypt_in_place_detached(Nonce::from_slice(nonce), associated_data, payload)
            .map_err(|_| Error::InputTooLong)?;
        tag.copy_from_slice(generated.as_slice());
        Ok(payload.len())
    }

    fn open_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
    ) -> Result<usize, Error> {
        if nonce.len() != NONCE_LEN {
            return Err(Error::InvalidNonceLength {
                required: NONCE_LEN,
                provided: nonce.len(),
            });
        }
        if tag.len() != TAG_LEN {
            return Err(Error::InvalidTagLength {
                required: TAG_LEN,
                provided: tag.len(),
            });
        }
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
            return Err(Error::AuthenticationFailed);
        }
        Ok(payload.len())
    }
}

/// AES-128-GCM.
pub type Aes128Gcm = Cipher<aes_gcm::Aes128Gcm, 16>;
/// AES-256-GCM.
pub type Aes256Gcm = Cipher<aes_gcm::Aes256Gcm, 32>;
/// ChaCha20-Poly1305.
pub type ChaCha20Poly1305 = Cipher<chacha20poly1305::ChaCha20Poly1305, 32>;
