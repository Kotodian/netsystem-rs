//! Protocol-neutral portable cryptographic algorithms.
//!
//! This layer accepts raw caller-provided memory and implements algorithm
//! semantics only. Key policy, implementation selection, prepared contexts,
//! and operation lifecycle belong to `hammer-service`.

use std::fmt;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes128Gcm, Aes256Gcm, Nonce, Tag};
use blake2::{Blake2b512, Blake2s256};
use chacha20poly1305::ChaCha20Poly1305;
use sha2::{Digest, Sha256, Sha384, Sha512};

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

/// One authenticated-encryption algorithm from the standard catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AeadAlgorithm {
    /// AES with a 128-bit key in Galois/Counter Mode.
    Aes128Gcm,
    /// AES with a 256-bit key in Galois/Counter Mode.
    Aes256Gcm,
    /// ChaCha20 with a Poly1305 authenticator.
    ChaCha20Poly1305,
}

impl AeadAlgorithm {
    fn key_len(self) -> usize {
        match self {
            Self::Aes128Gcm => 16,
            Self::Aes256Gcm | Self::ChaCha20Poly1305 => 32,
        }
    }
}

enum AeadState {
    Aes128Gcm(Box<Aes128Gcm>),
    Aes256Gcm(Box<Aes256Gcm>),
    ChaCha20Poly1305(Box<ChaCha20Poly1305>),
}

/// Prepared state for one standard authenticated-encryption algorithm.
pub struct AeadCipher {
    algorithm: AeadAlgorithm,
    state: AeadState,
}

impl fmt::Debug for AeadCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AeadCipher")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl AeadCipher {
    /// Prepares the selected algorithm from raw key material.
    ///
    /// # Errors
    ///
    /// Returns [`AeadError::InvalidKeyLength`] unless `key` has the selected
    /// algorithm's required length.
    pub fn new(algorithm: AeadAlgorithm, key: &[u8]) -> Result<Self, AeadError> {
        let state = match algorithm {
            AeadAlgorithm::Aes128Gcm => Aes128Gcm::new_from_slice(key)
                .map(Box::new)
                .map(AeadState::Aes128Gcm),
            AeadAlgorithm::Aes256Gcm => Aes256Gcm::new_from_slice(key)
                .map(Box::new)
                .map(AeadState::Aes256Gcm),
            AeadAlgorithm::ChaCha20Poly1305 => ChaCha20Poly1305::new_from_slice(key)
                .map(Box::new)
                .map(AeadState::ChaCha20Poly1305),
        }
        .map_err(|_| AeadError::InvalidKeyLength {
            required: algorithm.key_len(),
            provided: key.len(),
        })?;
        Ok(Self { algorithm, state })
    }

    /// Encrypts ordered input fragments into separate caller-owned memory.
    ///
    /// # Errors
    ///
    /// Returns a typed length failure before modifying output, or
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
        self.state
            .seal(nonce, associated_data, &mut output[..input_len], tag)?;
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
            .state
            .open(nonce, associated_data, &mut output[..input_len], tag)
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
    /// Returns a typed nonce, tag, or input-length failure.
    pub fn seal_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &mut [u8],
    ) -> Result<usize, AeadError> {
        validate_nonce_and_tag(nonce, tag)?;
        self.state.seal(nonce, associated_data, payload, tag)?;
        Ok(payload.len())
    }

    /// Authenticates and decrypts a caller-owned payload in place.
    ///
    /// Authentication failure clears the complete payload.
    ///
    /// # Errors
    ///
    /// Returns a typed nonce or tag failure before modification, or
    /// [`AeadError::AuthenticationFailed`] after clearing unauthenticated data.
    pub fn open_in_place(
        &self,
        payload: &mut [u8],
        nonce: &[u8],
        associated_data: &[u8],
        tag: &[u8],
    ) -> Result<usize, AeadError> {
        validate_nonce_and_tag(nonce, tag)?;
        if self
            .state
            .open(nonce, associated_data, payload, tag)
            .is_err()
        {
            payload.fill(0);
            return Err(AeadError::AuthenticationFailed);
        }
        Ok(payload.len())
    }
}

impl AeadState {
    fn seal(
        &self,
        nonce: &[u8],
        associated_data: &[u8],
        payload: &mut [u8],
        tag: &mut [u8],
    ) -> Result<(), AeadError> {
        let generated = match self {
            Self::Aes128Gcm(cipher) => {
                cipher.encrypt_in_place_detached(Nonce::from_slice(nonce), associated_data, payload)
            }
            Self::Aes256Gcm(cipher) => {
                cipher.encrypt_in_place_detached(Nonce::from_slice(nonce), associated_data, payload)
            }
            Self::ChaCha20Poly1305(cipher) => {
                cipher.encrypt_in_place_detached(Nonce::from_slice(nonce), associated_data, payload)
            }
        }
        .map_err(|_| AeadError::InputTooLong)?;
        tag.copy_from_slice(generated.as_slice());
        Ok(())
    }

    fn open(
        &self,
        nonce: &[u8],
        associated_data: &[u8],
        payload: &mut [u8],
        tag: &[u8],
    ) -> Result<(), AeadError> {
        let result = match self {
            Self::Aes128Gcm(cipher) => cipher.decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                payload,
                Tag::from_slice(tag),
            ),
            Self::Aes256Gcm(cipher) => cipher.decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                payload,
                Tag::from_slice(tag),
            ),
            Self::ChaCha20Poly1305(cipher) => cipher.decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                associated_data,
                payload,
                Tag::from_slice(tag),
            ),
        };
        result.map_err(|_| AeadError::AuthenticationFailed)
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

/// One digest algorithm from the standard catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashAlgorithm {
    /// SHA-256.
    Sha256,
    /// SHA-384.
    Sha384,
    /// SHA-512.
    Sha512,
    /// BLAKE2s with a 256-bit digest.
    Blake2s,
    /// BLAKE2b with a 512-bit digest.
    Blake2b,
}

impl HashAlgorithm {
    /// Returns the digest length in bytes.
    pub fn output_len(self) -> usize {
        match self {
            Self::Sha256 | Self::Blake2s => 32,
            Self::Sha384 => 48,
            Self::Sha512 | Self::Blake2b => 64,
        }
    }
}

/// Computes a standard digest over ordered fragments into caller memory.
///
/// # Errors
///
/// Returns [`HashError::OutputTooSmall`] without modifying `output` when it is
/// shorter than the selected algorithm's digest.
pub fn hash(
    algorithm: HashAlgorithm,
    input: &[&[u8]],
    output: &mut [u8],
) -> Result<usize, HashError> {
    let output_len = algorithm.output_len();
    if output.len() < output_len {
        return Err(HashError::OutputTooSmall {
            required: output_len,
            provided: output.len(),
        });
    }

    match algorithm {
        HashAlgorithm::Sha256 => write_digest::<Sha256>(input, output),
        HashAlgorithm::Sha384 => write_digest::<Sha384>(input, output),
        HashAlgorithm::Sha512 => write_digest::<Sha512>(input, output),
        HashAlgorithm::Blake2s => write_digest::<Blake2s256>(input, output),
        HashAlgorithm::Blake2b => write_digest::<Blake2b512>(input, output),
    }
    Ok(output_len)
}

fn write_digest<D: Digest>(input: &[&[u8]], output: &mut [u8]) {
    let mut digest = D::new();
    for fragment in input {
        digest.update(fragment);
    }
    let digest = digest.finalize();
    output[..digest.len()].copy_from_slice(&digest);
}

/// One SHA-2 digest size used by HMAC and HKDF.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sha2Algorithm {
    /// SHA-256.
    Sha256,
    /// SHA-384.
    Sha384,
    /// SHA-512.
    Sha512,
}

impl Sha2Algorithm {
    /// Returns the digest length in bytes.
    pub fn output_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha384 => 48,
            Self::Sha512 => 64,
        }
    }
}

/// A failure produced by an HMAC implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MacError {
    /// Caller output cannot hold the complete authenticator.
    OutputTooSmall {
        /// Authenticator length required by the algorithm.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
    },
}

impl fmt::Display for MacError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputTooSmall { required, provided } => write!(
                formatter,
                "MAC output requires {required} bytes but caller provided {provided}"
            ),
        }
    }
}

impl std::error::Error for MacError {}

enum HmacState {
    Sha256(hmac::Hmac<Sha256>),
    Sha384(hmac::Hmac<Sha384>),
    Sha512(hmac::Hmac<Sha512>),
}

/// Prepared state for HMAC with one standard SHA-2 algorithm.
pub struct Hmac {
    algorithm: Sha2Algorithm,
    state: HmacState,
}

impl fmt::Debug for Hmac {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Hmac")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl Hmac {
    /// Prepares HMAC state from caller-provided key material.
    pub fn new(algorithm: Sha2Algorithm, key: &[u8]) -> Self {
        let state = match algorithm {
            Sha2Algorithm::Sha256 => HmacState::Sha256(
                <hmac::Hmac<Sha256> as hmac::Mac>::new_from_slice(key)
                    .expect("HMAC accepts keys of every length"),
            ),
            Sha2Algorithm::Sha384 => HmacState::Sha384(
                <hmac::Hmac<Sha384> as hmac::Mac>::new_from_slice(key)
                    .expect("HMAC accepts keys of every length"),
            ),
            Sha2Algorithm::Sha512 => HmacState::Sha512(
                <hmac::Hmac<Sha512> as hmac::Mac>::new_from_slice(key)
                    .expect("HMAC accepts keys of every length"),
            ),
        };
        Self { algorithm, state }
    }

    /// Authenticates ordered input fragments into caller-provided memory.
    ///
    /// # Errors
    ///
    /// Returns [`MacError::OutputTooSmall`] without modifying `output`.
    pub fn authenticate(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, MacError> {
        let output_len = self.algorithm.output_len();
        if output.len() < output_len {
            return Err(MacError::OutputTooSmall {
                required: output_len,
                provided: output.len(),
            });
        }
        match &self.state {
            HmacState::Sha256(state) => write_mac(state, input, output),
            HmacState::Sha384(state) => write_mac(state, input, output),
            HmacState::Sha512(state) => write_mac(state, input, output),
        }
        Ok(output_len)
    }
}

fn write_mac<M>(state: &M, input: &[&[u8]], output: &mut [u8])
where
    M: hmac::Mac + Clone,
{
    let mut state = state.clone();
    for fragment in input {
        state.update(fragment);
    }
    let authenticator = state.finalize().into_bytes();
    output[..authenticator.len()].copy_from_slice(&authenticator);
}

/// A failure produced by HKDF expansion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KdfError {
    /// Caller output cannot hold the requested derived bytes.
    OutputTooSmall {
        /// Number of bytes requested by the caller.
        required: usize,
        /// Capacity supplied by the caller.
        provided: usize,
    },
    /// HKDF cannot produce the requested number of bytes.
    OutputTooLong {
        /// Number of bytes requested by the caller.
        requested: usize,
        /// Maximum length permitted by the selected hash.
        maximum: usize,
    },
}

impl fmt::Display for KdfError {
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

impl std::error::Error for KdfError {}

enum HkdfState {
    Sha256(hkdf::Hkdf<Sha256>),
    Sha384(hkdf::Hkdf<Sha384>),
    Sha512(hkdf::Hkdf<Sha512>),
}

/// Extracted state for HKDF with one standard SHA-2 algorithm.
pub struct Hkdf {
    algorithm: Sha2Algorithm,
    state: HkdfState,
}

impl fmt::Debug for Hkdf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Hkdf")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl Hkdf {
    /// Extracts HKDF state from input key material and an optional salt.
    pub fn new(algorithm: Sha2Algorithm, salt: Option<&[u8]>, input_key_material: &[u8]) -> Self {
        let state = match algorithm {
            Sha2Algorithm::Sha256 => HkdfState::Sha256(hkdf::Hkdf::new(salt, input_key_material)),
            Sha2Algorithm::Sha384 => HkdfState::Sha384(hkdf::Hkdf::new(salt, input_key_material)),
            Sha2Algorithm::Sha512 => HkdfState::Sha512(hkdf::Hkdf::new(salt, input_key_material)),
        };
        Self { algorithm, state }
    }

    /// Expands ordered info fragments into caller-provided memory.
    ///
    /// # Errors
    ///
    /// Returns a typed capacity or algorithm-limit failure without modifying
    /// `output`.
    pub fn expand(
        &self,
        info: &[&[u8]],
        length: usize,
        output: &mut [u8],
    ) -> Result<usize, KdfError> {
        if output.len() < length {
            return Err(KdfError::OutputTooSmall {
                required: length,
                provided: output.len(),
            });
        }
        let maximum = 255 * self.algorithm.output_len();
        if length > maximum {
            return Err(KdfError::OutputTooLong {
                requested: length,
                maximum,
            });
        }
        let result = match &self.state {
            HkdfState::Sha256(state) => state.expand_multi_info(info, &mut output[..length]),
            HkdfState::Sha384(state) => state.expand_multi_info(info, &mut output[..length]),
            HkdfState::Sha512(state) => state.expand_multi_info(info, &mut output[..length]),
        };
        result.expect("HKDF length was validated before expansion");
        Ok(length)
    }
}

/// One standard key-establishment algorithm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyEstablishmentAlgorithm {
    /// X25519 Diffie-Hellman.
    X25519,
    /// ECDH over NIST P-256.
    P256,
    /// ECDH over NIST P-384.
    P384,
    /// ML-KEM-768 encapsulation.
    MlKem768,
}

impl KeyEstablishmentAlgorithm {
    /// Returns the serialized private-key length.
    pub fn private_key_len(self) -> usize {
        match self {
            Self::X25519 | Self::P256 => 32,
            Self::P384 => 48,
            Self::MlKem768 => 64,
        }
    }

    /// Returns the canonical serialized public-key length.
    pub fn public_key_len(self) -> usize {
        match self {
            Self::X25519 => 32,
            Self::P256 => 65,
            Self::P384 => 97,
            Self::MlKem768 => 1184,
        }
    }

    /// Returns the established shared-secret length.
    pub fn shared_secret_len(self) -> usize {
        match self {
            Self::X25519 | Self::P256 | Self::MlKem768 => 32,
            Self::P384 => 48,
        }
    }

    /// Returns the ML-KEM ciphertext length, if this is an encapsulation algorithm.
    pub fn ciphertext_len(self) -> Option<usize> {
        match self {
            Self::MlKem768 => Some(1088),
            Self::X25519 | Self::P256 | Self::P384 => None,
        }
    }
}

/// One output role used to report a key-establishment capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyEstablishmentOutput {
    /// Serialized private key.
    PrivateKey,
    /// Serialized public key.
    PublicKey,
    /// Established secret.
    SharedSecret,
    /// Encapsulated ML-KEM value.
    Ciphertext,
}

/// A failure produced by a key-establishment implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyEstablishmentError {
    /// Random input has the wrong length for the selected algorithm.
    InvalidEntropyLength {
        /// Required random bytes.
        required: usize,
        /// Supplied random bytes.
        provided: usize,
    },
    /// Private-key input has the wrong serialized length.
    InvalidPrivateKeyLength {
        /// Required private-key bytes.
        required: usize,
        /// Supplied private-key bytes.
        provided: usize,
    },
    /// Private-key input is not a valid scalar or seed.
    InvalidPrivateKey,
    /// Public-key input has the wrong serialized length.
    InvalidPublicKeyLength {
        /// Required public-key bytes.
        required: usize,
        /// Supplied public-key bytes.
        provided: usize,
    },
    /// Public-key input is not a valid encoding for the selected algorithm.
    InvalidPublicKey,
    /// X25519 rejected a non-contributory small-order public key.
    SmallOrderPublicKey,
    /// ML-KEM ciphertext input has the wrong serialized length.
    InvalidCiphertextLength {
        /// Required ciphertext bytes.
        required: usize,
        /// Supplied ciphertext bytes.
        provided: usize,
    },
    /// Caller output cannot hold the complete result.
    OutputTooSmall {
        /// Output whose capacity was rejected.
        output: KeyEstablishmentOutput,
        /// Required output bytes.
        required: usize,
        /// Supplied output bytes.
        provided: usize,
    },
    /// The selected algorithm does not define the requested operation.
    OperationUnsupported {
        /// Selected algorithm.
        algorithm: KeyEstablishmentAlgorithm,
    },
}

impl fmt::Display for KeyEstablishmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEntropyLength { required, provided } => write!(
                formatter,
                "key generation requires {required} random bytes but caller provided {provided}"
            ),
            Self::InvalidPrivateKeyLength { required, provided } => write!(
                formatter,
                "private key requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidPrivateKey => formatter.write_str("private key is invalid"),
            Self::InvalidPublicKeyLength { required, provided } => write!(
                formatter,
                "public key requires {required} bytes but caller provided {provided}"
            ),
            Self::InvalidPublicKey => formatter.write_str("public key encoding is invalid"),
            Self::SmallOrderPublicKey => {
                formatter.write_str("X25519 public key is non-contributory")
            }
            Self::InvalidCiphertextLength { required, provided } => write!(
                formatter,
                "ML-KEM ciphertext requires {required} bytes but caller provided {provided}"
            ),
            Self::OutputTooSmall {
                output,
                required,
                provided,
            } => write!(
                formatter,
                "{output:?} output requires {required} bytes but caller provided {provided}"
            ),
            Self::OperationUnsupported { algorithm } => {
                write!(formatter, "operation is not defined for {algorithm:?}")
            }
        }
    }
}

impl std::error::Error for KeyEstablishmentError {}

/// Generates one standard key pair from caller-supplied random bytes.
///
/// # Errors
///
/// Returns a typed input or capacity failure before modifying either output.
pub fn generate_keypair(
    algorithm: KeyEstablishmentAlgorithm,
    entropy: &[u8],
    private_key_output: &mut [u8],
    public_key_output: &mut [u8],
) -> Result<(), KeyEstablishmentError> {
    let entropy_len = algorithm.private_key_len();
    if entropy.len() != entropy_len {
        return Err(KeyEstablishmentError::InvalidEntropyLength {
            required: entropy_len,
            provided: entropy.len(),
        });
    }
    validate_key_establishment_output(
        KeyEstablishmentOutput::PrivateKey,
        algorithm.private_key_len(),
        private_key_output,
    )?;
    validate_key_establishment_output(
        KeyEstablishmentOutput::PublicKey,
        algorithm.public_key_len(),
        public_key_output,
    )?;

    public_key(algorithm, entropy, public_key_output)?;
    private_key_output[..entropy_len].copy_from_slice(entropy);
    Ok(())
}

/// Derives the canonical public key for caller-supplied private-key bytes.
///
/// # Errors
///
/// Returns a typed key or capacity failure without modifying `output`.
pub fn public_key(
    algorithm: KeyEstablishmentAlgorithm,
    private_key: &[u8],
    output: &mut [u8],
) -> Result<usize, KeyEstablishmentError> {
    validate_private_key_len(algorithm, private_key)?;
    let output_len = algorithm.public_key_len();
    validate_key_establishment_output(KeyEstablishmentOutput::PublicKey, output_len, output)?;

    match algorithm {
        KeyEstablishmentAlgorithm::X25519 => {
            let secret = x25519_dalek::StaticSecret::from(array_32(private_key));
            output[..output_len].copy_from_slice(x25519_dalek::PublicKey::from(&secret).as_bytes());
        }
        KeyEstablishmentAlgorithm::P256 => {
            let secret = p256::SecretKey::from_slice(private_key)
                .map_err(|_| KeyEstablishmentError::InvalidPrivateKey)?;
            let encoded = p256::Sec1Point::from(secret.public_key());
            output[..output_len].copy_from_slice(encoded.as_bytes());
        }
        KeyEstablishmentAlgorithm::P384 => {
            let secret = p384::SecretKey::from_slice(private_key)
                .map_err(|_| KeyEstablishmentError::InvalidPrivateKey)?;
            let encoded = p384::Sec1Point::from(secret.public_key());
            output[..output_len].copy_from_slice(encoded.as_bytes());
        }
        KeyEstablishmentAlgorithm::MlKem768 => {
            use ml_kem::KeyExport;

            let seed = ml_kem::Seed::try_from(private_key)
                .map_err(|_| KeyEstablishmentError::InvalidPrivateKey)?;
            let private_key = ml_kem::ml_kem_768::DecapsulationKey::from_seed(seed);
            let encoded = private_key.encapsulation_key().to_bytes();
            output[..output_len].copy_from_slice(encoded.as_slice());
        }
    }
    Ok(output_len)
}

/// Establishes an ECDH secret from private and peer-public key bytes.
///
/// # Errors
///
/// Returns typed length, encoding, small-order, or capacity failures without
/// exposing a partial secret.
pub fn agree(
    algorithm: KeyEstablishmentAlgorithm,
    private_key: &[u8],
    peer_public_key: &[u8],
    output: &mut [u8],
) -> Result<usize, KeyEstablishmentError> {
    validate_private_key_len(algorithm, private_key)?;
    let output_len = algorithm.shared_secret_len();
    validate_key_establishment_output(KeyEstablishmentOutput::SharedSecret, output_len, output)?;

    match algorithm {
        KeyEstablishmentAlgorithm::X25519 => {
            if peer_public_key.len() != 32 {
                return Err(KeyEstablishmentError::InvalidPublicKeyLength {
                    required: 32,
                    provided: peer_public_key.len(),
                });
            }
            let secret = x25519_dalek::StaticSecret::from(array_32(private_key));
            let peer = x25519_dalek::PublicKey::from(array_32(peer_public_key));
            let shared = secret.diffie_hellman(&peer);
            if !shared.was_contributory() {
                return Err(KeyEstablishmentError::SmallOrderPublicKey);
            }
            output[..output_len].copy_from_slice(shared.as_bytes());
        }
        KeyEstablishmentAlgorithm::P256 => {
            let secret = p256::SecretKey::from_slice(private_key)
                .map_err(|_| KeyEstablishmentError::InvalidPrivateKey)?;
            let peer = p256::PublicKey::from_sec1_bytes(peer_public_key)
                .map_err(|_| KeyEstablishmentError::InvalidPublicKey)?;
            let shared = p256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), peer.as_affine());
            output[..output_len].copy_from_slice(shared.raw_secret_bytes());
        }
        KeyEstablishmentAlgorithm::P384 => {
            let secret = p384::SecretKey::from_slice(private_key)
                .map_err(|_| KeyEstablishmentError::InvalidPrivateKey)?;
            let peer = p384::PublicKey::from_sec1_bytes(peer_public_key)
                .map_err(|_| KeyEstablishmentError::InvalidPublicKey)?;
            let shared = p384::ecdh::diffie_hellman(secret.to_nonzero_scalar(), peer.as_affine());
            output[..output_len].copy_from_slice(shared.raw_secret_bytes());
        }
        KeyEstablishmentAlgorithm::MlKem768 => {
            return Err(KeyEstablishmentError::OperationUnsupported { algorithm });
        }
    }
    Ok(output_len)
}

/// Encapsulates an ML-KEM secret using caller-supplied random bytes.
///
/// # Errors
///
/// Returns typed input or capacity failures before modifying either output.
pub fn encapsulate(
    algorithm: KeyEstablishmentAlgorithm,
    peer_public_key: &[u8],
    entropy: &[u8],
    ciphertext: &mut [u8],
    shared_secret: &mut [u8],
) -> Result<(), KeyEstablishmentError> {
    if algorithm != KeyEstablishmentAlgorithm::MlKem768 {
        return Err(KeyEstablishmentError::OperationUnsupported { algorithm });
    }
    if peer_public_key.len() != algorithm.public_key_len() {
        return Err(KeyEstablishmentError::InvalidPublicKeyLength {
            required: algorithm.public_key_len(),
            provided: peer_public_key.len(),
        });
    }
    if entropy.len() != 32 {
        return Err(KeyEstablishmentError::InvalidEntropyLength {
            required: 32,
            provided: entropy.len(),
        });
    }
    let ciphertext_len = algorithm
        .ciphertext_len()
        .expect("ML-KEM defines a ciphertext length");
    validate_key_establishment_output(
        KeyEstablishmentOutput::Ciphertext,
        ciphertext_len,
        ciphertext,
    )?;
    validate_key_establishment_output(
        KeyEstablishmentOutput::SharedSecret,
        algorithm.shared_secret_len(),
        shared_secret,
    )?;

    use ml_kem::TryKeyInit;

    let peer = ml_kem::ml_kem_768::EncapsulationKey::new_from_slice(peer_public_key)
        .map_err(|_| KeyEstablishmentError::InvalidPublicKey)?;
    let entropy =
        ml_kem::B32::try_from(entropy).expect("ML-KEM encapsulation entropy length was validated");
    let (encapsulated, secret) = peer.encapsulate_deterministic(&entropy);
    ciphertext[..ciphertext_len].copy_from_slice(encapsulated.as_slice());
    shared_secret[..secret.len()].copy_from_slice(secret.as_slice());
    Ok(())
}

/// Decapsulates an ML-KEM secret from serialized private key and ciphertext bytes.
///
/// # Errors
///
/// Returns typed length or capacity failures without modifying `shared_secret`.
pub fn decapsulate(
    algorithm: KeyEstablishmentAlgorithm,
    private_key: &[u8],
    ciphertext: &[u8],
    shared_secret: &mut [u8],
) -> Result<usize, KeyEstablishmentError> {
    if algorithm != KeyEstablishmentAlgorithm::MlKem768 {
        return Err(KeyEstablishmentError::OperationUnsupported { algorithm });
    }
    validate_private_key_len(algorithm, private_key)?;
    let ciphertext_len = algorithm
        .ciphertext_len()
        .expect("ML-KEM defines a ciphertext length");
    if ciphertext.len() != ciphertext_len {
        return Err(KeyEstablishmentError::InvalidCiphertextLength {
            required: ciphertext_len,
            provided: ciphertext.len(),
        });
    }
    let output_len = algorithm.shared_secret_len();
    validate_key_establishment_output(
        KeyEstablishmentOutput::SharedSecret,
        output_len,
        shared_secret,
    )?;

    use ml_kem::Decapsulate;

    let seed = ml_kem::Seed::try_from(private_key)
        .map_err(|_| KeyEstablishmentError::InvalidPrivateKey)?;
    let private_key = ml_kem::ml_kem_768::DecapsulationKey::from_seed(seed);
    let ciphertext = ml_kem::ml_kem_768::Ciphertext::try_from(ciphertext)
        .expect("ML-KEM ciphertext length was validated");
    let secret = private_key.decapsulate(&ciphertext);
    shared_secret[..output_len].copy_from_slice(secret.as_slice());
    Ok(output_len)
}

fn validate_private_key_len(
    algorithm: KeyEstablishmentAlgorithm,
    private_key: &[u8],
) -> Result<(), KeyEstablishmentError> {
    let required = algorithm.private_key_len();
    if private_key.len() != required {
        return Err(KeyEstablishmentError::InvalidPrivateKeyLength {
            required,
            provided: private_key.len(),
        });
    }
    Ok(())
}

fn validate_key_establishment_output(
    role: KeyEstablishmentOutput,
    required: usize,
    output: &[u8],
) -> Result<(), KeyEstablishmentError> {
    if output.len() < required {
        return Err(KeyEstablishmentError::OutputTooSmall {
            output: role,
            required,
            provided: output.len(),
        });
    }
    Ok(())
}

fn array_32(input: &[u8]) -> [u8; 32] {
    input
        .try_into()
        .expect("32-byte input length was validated before conversion")
}
