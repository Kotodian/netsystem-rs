//! Portable digital-signature implementations.

use std::fmt;
use std::marker::PhantomData;

use ed25519_dalek::ed25519::signature::{MultipartSigner, MultipartVerifier};
use p256::ecdsa::signature::{DigestVerifier, SignatureEncoding};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePublicKey};
use rsa::traits::PublicKeyParts;

/// A caller-owned output used by a signature operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Output {
    /// Canonical public-key encoding.
    PublicKey,
    /// Canonical signature encoding.
    Signature,
}

/// A failure produced while deriving a public key or creating a signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignError {
    /// Private-key input has the wrong fixed length.
    InvalidPrivateKeyLength {
        /// Required private-key bytes.
        required: usize,
        /// Supplied private-key bytes.
        provided: usize,
    },
    /// Private-key input is not a valid scalar, seed, or PKCS#8 encoding.
    InvalidPrivateKey,
    /// Caller output cannot hold the complete public result.
    OutputTooSmall {
        /// Rejected output role.
        output: Output,
        /// Required output bytes.
        required: usize,
        /// Supplied output bytes.
        provided: usize,
    },
    /// An RSA modulus cannot encode the selected digest and PSS salt.
    KeyTooSmall {
        /// Minimum encoded-message bytes required by the parameter set.
        required: usize,
        /// RSA modulus bytes supplied by the key.
        provided: usize,
    },
    /// RSA-PSS signing could not obtain cryptographic randomness.
    EntropyUnavailable,
}

impl fmt::Display for SignError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrivateKeyLength { required, provided } => write!(
                formatter,
                "private key requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidPrivateKey => formatter.write_str("private key encoding is invalid"),
            Self::OutputTooSmall {
                output,
                required,
                provided,
            } => write!(
                formatter,
                "{output:?} output requires {required} bytes but caller provided {provided}"
            ),
            Self::KeyTooSmall { required, provided } => write!(
                formatter,
                "RSA-PSS parameters require {required} modulus bytes but key provides {provided}"
            ),
            Self::EntropyUnavailable => formatter.write_str("cryptographic entropy is unavailable"),
        }
    }
}

impl std::error::Error for SignError {}

/// A failure produced while verifying a signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifyError {
    /// Public-key input has the wrong fixed length.
    InvalidPublicKeyLength {
        /// Required public-key bytes.
        required: usize,
        /// Supplied public-key bytes.
        provided: usize,
    },
    /// Public-key input is not a valid point or SPKI encoding.
    InvalidPublicKey,
    /// Signature input has the wrong canonical length.
    InvalidSignatureLength {
        /// Required signature bytes.
        required: usize,
        /// Supplied signature bytes.
        provided: usize,
    },
    /// Signature input is not a valid canonical encoding.
    InvalidSignature,
    /// An RSA modulus cannot encode the selected digest and PSS salt.
    KeyTooSmall {
        /// Minimum encoded-message bytes required by the parameter set.
        required: usize,
        /// RSA modulus bytes supplied by the key.
        provided: usize,
    },
    /// The signature is well formed but does not authenticate the message.
    SignatureMismatch,
}

impl fmt::Display for VerifyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPublicKeyLength { required, provided } => write!(
                formatter,
                "public key requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidPublicKey => formatter.write_str("public key encoding is invalid"),
            Self::InvalidSignatureLength { required, provided } => write!(
                formatter,
                "signature requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidSignature => formatter.write_str("signature encoding is invalid"),
            Self::KeyTooSmall { required, provided } => write!(
                formatter,
                "RSA-PSS parameters require {required} modulus bytes but key provides {provided}"
            ),
            Self::SignatureMismatch => formatter.write_str("signature does not match"),
        }
    }
}

impl std::error::Error for VerifyError {}

/// Complete semantics supplied by one signature algorithm implementation.
pub trait Algorithm: Default + fmt::Debug + 'static {
    /// Derives the canonical public key from private key material.
    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, SignError>;

    /// Signs ordered message fragments into canonical caller-owned output.
    fn sign(
        &self,
        private_key: &[u8],
        message: &[&[u8]],
        output: &mut [u8],
    ) -> Result<usize, SignError>;

    /// Verifies a canonical signature over ordered message fragments.
    fn verify(
        &self,
        public_key: &[u8],
        message: &[&[u8]],
        signature: &[u8],
    ) -> Result<(), VerifyError>;
}

/// Ed25519 as specified by RFC 8032.
#[derive(Clone, Copy, Debug, Default)]
pub struct Ed25519;

impl Algorithm for Ed25519 {
    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, SignError> {
        const KEY_LEN: usize = 32;
        if private_key.len() != KEY_LEN {
            return Err(SignError::InvalidPrivateKeyLength {
                required: KEY_LEN,
                provided: private_key.len(),
            });
        }
        let private_key: &[u8; KEY_LEN] = private_key
            .try_into()
            .expect("Ed25519 private-key length was validated");
        let public_key = ed25519_dalek::SigningKey::from_bytes(private_key)
            .verifying_key()
            .to_bytes();
        if output.len() < public_key.len() {
            return Err(SignError::OutputTooSmall {
                output: Output::PublicKey,
                required: public_key.len(),
                provided: output.len(),
            });
        }
        output[..public_key.len()].copy_from_slice(&public_key);
        Ok(public_key.len())
    }

    fn sign(
        &self,
        private_key: &[u8],
        message: &[&[u8]],
        output: &mut [u8],
    ) -> Result<usize, SignError> {
        const KEY_LEN: usize = 32;
        if private_key.len() != KEY_LEN {
            return Err(SignError::InvalidPrivateKeyLength {
                required: KEY_LEN,
                provided: private_key.len(),
            });
        }
        let private_key: &[u8; KEY_LEN] = private_key
            .try_into()
            .expect("Ed25519 private-key length was validated");
        let signature = ed25519_dalek::SigningKey::from_bytes(private_key)
            .try_multipart_sign(message)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let signature = signature.to_bytes();
        if output.len() < signature.len() {
            return Err(SignError::OutputTooSmall {
                output: Output::Signature,
                required: signature.len(),
                provided: output.len(),
            });
        }
        output[..signature.len()].copy_from_slice(&signature);
        Ok(signature.len())
    }

    fn verify(
        &self,
        public_key: &[u8],
        message: &[&[u8]],
        signature: &[u8],
    ) -> Result<(), VerifyError> {
        const PUBLIC_KEY_LEN: usize = 32;
        const SIGNATURE_LEN: usize = 64;
        if public_key.len() != PUBLIC_KEY_LEN {
            return Err(VerifyError::InvalidPublicKeyLength {
                required: PUBLIC_KEY_LEN,
                provided: public_key.len(),
            });
        }
        if signature.len() != SIGNATURE_LEN {
            return Err(VerifyError::InvalidSignatureLength {
                required: SIGNATURE_LEN,
                provided: signature.len(),
            });
        }
        let public_key: &[u8; PUBLIC_KEY_LEN] = public_key
            .try_into()
            .expect("Ed25519 public-key length was validated");
        let public_key = ed25519_dalek::VerifyingKey::from_bytes(public_key)
            .map_err(|_| VerifyError::InvalidPublicKey)?;
        let signature = ed25519_dalek::Signature::try_from(signature)
            .map_err(|_| VerifyError::InvalidSignature)?;
        public_key
            .multipart_verify(message, &signature)
            .map_err(|_| VerifyError::SignatureMismatch)
    }
}

/// ECDSA over P-256 with SHA-256 and fixed-width signatures.
#[derive(Clone, Copy, Debug, Default)]
pub struct EcdsaP256Sha256;

impl Algorithm for EcdsaP256Sha256 {
    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, SignError> {
        const KEY_LEN: usize = 32;
        if private_key.len() != KEY_LEN {
            return Err(SignError::InvalidPrivateKeyLength {
                required: KEY_LEN,
                provided: private_key.len(),
            });
        }
        let private_key = p256::ecdsa::SigningKey::from_slice(private_key)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let public_key = private_key.verifying_key().to_sec1_point(false);
        if output.len() < public_key.as_bytes().len() {
            return Err(SignError::OutputTooSmall {
                output: Output::PublicKey,
                required: public_key.as_bytes().len(),
                provided: output.len(),
            });
        }
        output[..public_key.as_bytes().len()].copy_from_slice(public_key.as_bytes());
        Ok(public_key.as_bytes().len())
    }

    fn sign(
        &self,
        private_key: &[u8],
        message: &[&[u8]],
        output: &mut [u8],
    ) -> Result<usize, SignError> {
        const KEY_LEN: usize = 32;
        if private_key.len() != KEY_LEN {
            return Err(SignError::InvalidPrivateKeyLength {
                required: KEY_LEN,
                provided: private_key.len(),
            });
        }
        let private_key = p256::ecdsa::SigningKey::from_slice(private_key)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let signature: p256::ecdsa::Signature = private_key
            .try_multipart_sign(message)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let signature = signature.to_bytes();
        if output.len() < signature.len() {
            return Err(SignError::OutputTooSmall {
                output: Output::Signature,
                required: signature.len(),
                provided: output.len(),
            });
        }
        output[..signature.len()].copy_from_slice(&signature);
        Ok(signature.len())
    }

    fn verify(
        &self,
        public_key: &[u8],
        message: &[&[u8]],
        signature: &[u8],
    ) -> Result<(), VerifyError> {
        const PUBLIC_KEY_LEN: usize = 65;
        const SIGNATURE_LEN: usize = 64;
        if public_key.len() != PUBLIC_KEY_LEN {
            return Err(VerifyError::InvalidPublicKeyLength {
                required: PUBLIC_KEY_LEN,
                provided: public_key.len(),
            });
        }
        if signature.len() != SIGNATURE_LEN {
            return Err(VerifyError::InvalidSignatureLength {
                required: SIGNATURE_LEN,
                provided: signature.len(),
            });
        }
        let public_key = p256::ecdsa::VerifyingKey::from_sec1_bytes(public_key)
            .map_err(|_| VerifyError::InvalidPublicKey)?;
        let signature = p256::ecdsa::Signature::from_slice(signature)
            .map_err(|_| VerifyError::InvalidSignature)?;
        public_key
            .multipart_verify(message, &signature)
            .map_err(|_| VerifyError::SignatureMismatch)
    }
}

/// ECDSA over P-384 with SHA-384 and fixed-width signatures.
#[derive(Clone, Copy, Debug, Default)]
pub struct EcdsaP384Sha384;

impl Algorithm for EcdsaP384Sha384 {
    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, SignError> {
        const KEY_LEN: usize = 48;
        if private_key.len() != KEY_LEN {
            return Err(SignError::InvalidPrivateKeyLength {
                required: KEY_LEN,
                provided: private_key.len(),
            });
        }
        let private_key = p384::ecdsa::SigningKey::from_slice(private_key)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let public_key = private_key.verifying_key().to_sec1_point(false);
        if output.len() < public_key.as_bytes().len() {
            return Err(SignError::OutputTooSmall {
                output: Output::PublicKey,
                required: public_key.as_bytes().len(),
                provided: output.len(),
            });
        }
        output[..public_key.as_bytes().len()].copy_from_slice(public_key.as_bytes());
        Ok(public_key.as_bytes().len())
    }

    fn sign(
        &self,
        private_key: &[u8],
        message: &[&[u8]],
        output: &mut [u8],
    ) -> Result<usize, SignError> {
        const KEY_LEN: usize = 48;
        if private_key.len() != KEY_LEN {
            return Err(SignError::InvalidPrivateKeyLength {
                required: KEY_LEN,
                provided: private_key.len(),
            });
        }
        let private_key = p384::ecdsa::SigningKey::from_slice(private_key)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let signature: p384::ecdsa::Signature = private_key
            .try_multipart_sign(message)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let signature = signature.to_bytes();
        if output.len() < signature.len() {
            return Err(SignError::OutputTooSmall {
                output: Output::Signature,
                required: signature.len(),
                provided: output.len(),
            });
        }
        output[..signature.len()].copy_from_slice(&signature);
        Ok(signature.len())
    }

    fn verify(
        &self,
        public_key: &[u8],
        message: &[&[u8]],
        signature: &[u8],
    ) -> Result<(), VerifyError> {
        const PUBLIC_KEY_LEN: usize = 97;
        const SIGNATURE_LEN: usize = 96;
        if public_key.len() != PUBLIC_KEY_LEN {
            return Err(VerifyError::InvalidPublicKeyLength {
                required: PUBLIC_KEY_LEN,
                provided: public_key.len(),
            });
        }
        if signature.len() != SIGNATURE_LEN {
            return Err(VerifyError::InvalidSignatureLength {
                required: SIGNATURE_LEN,
                provided: signature.len(),
            });
        }
        let public_key = p384::ecdsa::VerifyingKey::from_sec1_bytes(public_key)
            .map_err(|_| VerifyError::InvalidPublicKey)?;
        let signature = p384::ecdsa::Signature::from_slice(signature)
            .map_err(|_| VerifyError::InvalidSignature)?;
        public_key
            .multipart_verify(message, &signature)
            .map_err(|_| VerifyError::SignatureMismatch)
    }
}

/// RSA-PSS with a digest-sized salt.
pub struct RsaPss<D> {
    digest: PhantomData<fn() -> D>,
}

/// RSA-PSS with SHA-256 and a 32-byte salt.
pub type RsaPssSha256 = RsaPss<rsa::sha2::Sha256>;

/// RSA-PSS with SHA-384 and a 48-byte salt.
pub type RsaPssSha384 = RsaPss<rsa::sha2::Sha384>;

/// RSA-PSS with SHA-512 and a 64-byte salt.
pub type RsaPssSha512 = RsaPss<rsa::sha2::Sha512>;

impl<D> Default for RsaPss<D> {
    fn default() -> Self {
        Self {
            digest: PhantomData,
        }
    }
}

impl<D> fmt::Debug for RsaPss<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RsaPss")
    }
}

impl<D> Algorithm for RsaPss<D>
where
    D: rsa::sha2::Digest + rsa::sha2::digest::FixedOutputReset + 'static,
{
    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, SignError> {
        let private_key = rsa::RsaPrivateKey::from_pkcs8_der(private_key)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let required = 2 * <D as rsa::sha2::Digest>::output_size() + 2;
        let provided = private_key.size();
        if provided < required {
            return Err(SignError::KeyTooSmall { required, provided });
        }
        let public_key = rsa::RsaPublicKey::from(&private_key)
            .to_public_key_der()
            .map_err(|_| SignError::InvalidPrivateKey)?;
        if output.len() < public_key.as_bytes().len() {
            return Err(SignError::OutputTooSmall {
                output: Output::PublicKey,
                required: public_key.as_bytes().len(),
                provided: output.len(),
            });
        }
        output[..public_key.as_bytes().len()].copy_from_slice(public_key.as_bytes());
        Ok(public_key.as_bytes().len())
    }

    fn sign(
        &self,
        private_key: &[u8],
        message: &[&[u8]],
        output: &mut [u8],
    ) -> Result<usize, SignError> {
        let private_key = rsa::RsaPrivateKey::from_pkcs8_der(private_key)
            .map_err(|_| SignError::InvalidPrivateKey)?;
        let required = 2 * <D as rsa::sha2::Digest>::output_size() + 2;
        let provided = private_key.size();
        if provided < required {
            return Err(SignError::KeyTooSmall { required, provided });
        }
        if output.len() < private_key.size() {
            return Err(SignError::OutputTooSmall {
                output: Output::Signature,
                required: private_key.size(),
                provided: output.len(),
            });
        }
        let signature: rsa::pss::Signature = rsa::pss::SigningKey::<D>::new(private_key)
            .try_multipart_sign(message)
            .map_err(|_| SignError::EntropyUnavailable)?;
        let signature = signature.to_bytes();
        output[..signature.len()].copy_from_slice(&signature);
        Ok(signature.len())
    }

    fn verify(
        &self,
        public_key: &[u8],
        message: &[&[u8]],
        signature: &[u8],
    ) -> Result<(), VerifyError> {
        let public_key = rsa::RsaPublicKey::from_public_key_der(public_key)
            .map_err(|_| VerifyError::InvalidPublicKey)?;
        let required = 2 * <D as rsa::sha2::Digest>::output_size() + 2;
        let provided = public_key.size();
        if provided < required {
            return Err(VerifyError::KeyTooSmall { required, provided });
        }
        if signature.len() != public_key.size() {
            return Err(VerifyError::InvalidSignatureLength {
                required: public_key.size(),
                provided: signature.len(),
            });
        }
        let signature =
            rsa::pss::Signature::try_from(signature).map_err(|_| VerifyError::InvalidSignature)?;
        rsa::pss::VerifyingKey::<D>::new(public_key)
            .verify_digest(
                |digest: &mut D| {
                    for fragment in message {
                        rsa::sha2::digest::Update::update(digest, fragment);
                    }
                    Ok(())
                },
                &signature,
            )
            .map_err(|_| VerifyError::SignatureMismatch)
    }
}
