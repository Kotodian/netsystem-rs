//! Portable key-establishment implementations.

use std::fmt;

/// One caller-owned output produced during key establishment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Output {
    /// Serialized private key.
    PrivateKey,
    /// Canonical public key.
    PublicKey,
    /// Established shared secret.
    SharedSecret,
    /// Encapsulated KEM value.
    Ciphertext,
}

/// A failure produced by a key-establishment algorithm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Random input has the wrong length.
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
    /// Public-key input is not a valid encoding.
    InvalidPublicKey,
    /// X25519 rejected a non-contributory peer public key.
    SmallOrderPublicKey,
    /// KEM ciphertext input has the wrong serialized length.
    InvalidCiphertextLength {
        /// Required ciphertext bytes.
        required: usize,
        /// Supplied ciphertext bytes.
        provided: usize,
    },
    /// Caller output cannot hold the complete result.
    OutputTooSmall {
        /// Rejected output role.
        output: Output,
        /// Required output bytes.
        required: usize,
        /// Supplied output bytes.
        provided: usize,
    },
    /// The algorithm does not define the requested operation.
    OperationUnsupported,
}

impl fmt::Display for Error {
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
                "ciphertext requires {required} bytes but caller provided {provided}"
            ),
            Self::OutputTooSmall {
                output,
                required,
                provided,
            } => write!(
                formatter,
                "{output:?} output requires {required} bytes but caller provided {provided}"
            ),
            Self::OperationUnsupported => {
                formatter.write_str("operation is not defined for this algorithm")
            }
        }
    }
}

impl std::error::Error for Error {}

/// Complete semantics supplied by one key-establishment algorithm.
pub trait Algorithm: Default + fmt::Debug + 'static {
    /// Serialized private-key length.
    const PRIVATE_KEY_LEN: usize;
    /// Canonical public-key length.
    const PUBLIC_KEY_LEN: usize;
    /// Established shared-secret length.
    const SHARED_SECRET_LEN: usize;
    /// Encapsulated ciphertext length, when the algorithm is a KEM.
    const CIPHERTEXT_LEN: Option<usize>;
    /// Random bytes consumed by encapsulation.
    const ENCAPSULATION_ENTROPY_LEN: usize = 0;

    /// Derives the canonical public key from private key material.
    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, Error>;

    /// Generates a key pair from caller-supplied random bytes.
    fn generate_keypair(
        &self,
        entropy: &[u8],
        private_key: &mut [u8],
        public_key: &mut [u8],
    ) -> Result<(), Error> {
        if entropy.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidEntropyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: entropy.len(),
            });
        }
        if private_key.len() < Self::PRIVATE_KEY_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::PrivateKey,
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if public_key.len() < Self::PUBLIC_KEY_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::PublicKey,
                required: Self::PUBLIC_KEY_LEN,
                provided: public_key.len(),
            });
        }
        self.public_key(entropy, public_key)?;
        private_key[..Self::PRIVATE_KEY_LEN].copy_from_slice(entropy);
        Ok(())
    }

    /// Establishes a Diffie-Hellman secret.
    fn agree(&self, _: &[u8], _: &[u8], _: &mut [u8]) -> Result<usize, Error> {
        Err(Error::OperationUnsupported)
    }

    /// Encapsulates a shared secret.
    fn encapsulate(&self, _: &[u8], _: &[u8], _: &mut [u8], _: &mut [u8]) -> Result<(), Error> {
        Err(Error::OperationUnsupported)
    }

    /// Decapsulates a shared secret.
    fn decapsulate(&self, _: &[u8], _: &[u8], _: &mut [u8]) -> Result<usize, Error> {
        Err(Error::OperationUnsupported)
    }
}

/// X25519 Diffie-Hellman.
#[derive(Clone, Copy, Debug, Default)]
pub struct X25519;

impl Algorithm for X25519 {
    const PRIVATE_KEY_LEN: usize = 32;
    const PUBLIC_KEY_LEN: usize = 32;
    const SHARED_SECRET_LEN: usize = 32;
    const CIPHERTEXT_LEN: Option<usize> = None;

    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if output.len() < Self::PUBLIC_KEY_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::PublicKey,
                required: Self::PUBLIC_KEY_LEN,
                provided: output.len(),
            });
        }
        let private_key: [u8; 32] = private_key
            .try_into()
            .expect("X25519 private-key length was validated");
        let private_key = x25519_dalek::StaticSecret::from(private_key);
        output[..Self::PUBLIC_KEY_LEN]
            .copy_from_slice(x25519_dalek::PublicKey::from(&private_key).as_bytes());
        Ok(Self::PUBLIC_KEY_LEN)
    }

    fn agree(
        &self,
        private_key: &[u8],
        peer_public_key: &[u8],
        output: &mut [u8],
    ) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if peer_public_key.len() != Self::PUBLIC_KEY_LEN {
            return Err(Error::InvalidPublicKeyLength {
                required: Self::PUBLIC_KEY_LEN,
                provided: peer_public_key.len(),
            });
        }
        if output.len() < Self::SHARED_SECRET_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::SharedSecret,
                required: Self::SHARED_SECRET_LEN,
                provided: output.len(),
            });
        }
        let private_key: [u8; 32] = private_key
            .try_into()
            .expect("X25519 private-key length was validated");
        let peer_public_key: [u8; 32] = peer_public_key
            .try_into()
            .expect("X25519 public-key length was validated");
        let private_key = x25519_dalek::StaticSecret::from(private_key);
        let peer_public_key = x25519_dalek::PublicKey::from(peer_public_key);
        let shared_secret = private_key.diffie_hellman(&peer_public_key);
        if !shared_secret.was_contributory() {
            return Err(Error::SmallOrderPublicKey);
        }
        output[..Self::SHARED_SECRET_LEN].copy_from_slice(shared_secret.as_bytes());
        Ok(Self::SHARED_SECRET_LEN)
    }
}

/// ECDH over NIST P-256.
#[derive(Clone, Copy, Debug, Default)]
pub struct P256;

impl Algorithm for P256 {
    const PRIVATE_KEY_LEN: usize = 32;
    const PUBLIC_KEY_LEN: usize = 65;
    const SHARED_SECRET_LEN: usize = 32;
    const CIPHERTEXT_LEN: Option<usize> = None;

    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if output.len() < Self::PUBLIC_KEY_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::PublicKey,
                required: Self::PUBLIC_KEY_LEN,
                provided: output.len(),
            });
        }
        let private_key =
            p256::SecretKey::from_slice(private_key).map_err(|_| Error::InvalidPrivateKey)?;
        let public_key = p256::Sec1Point::from(private_key.public_key());
        output[..Self::PUBLIC_KEY_LEN].copy_from_slice(public_key.as_bytes());
        Ok(Self::PUBLIC_KEY_LEN)
    }

    fn agree(
        &self,
        private_key: &[u8],
        peer_public_key: &[u8],
        output: &mut [u8],
    ) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if peer_public_key.len() != Self::PUBLIC_KEY_LEN {
            return Err(Error::InvalidPublicKeyLength {
                required: Self::PUBLIC_KEY_LEN,
                provided: peer_public_key.len(),
            });
        }
        if output.len() < Self::SHARED_SECRET_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::SharedSecret,
                required: Self::SHARED_SECRET_LEN,
                provided: output.len(),
            });
        }
        let private_key =
            p256::SecretKey::from_slice(private_key).map_err(|_| Error::InvalidPrivateKey)?;
        let peer_public_key = p256::PublicKey::from_sec1_bytes(peer_public_key)
            .map_err(|_| Error::InvalidPublicKey)?;
        let shared_secret = p256::ecdh::diffie_hellman(
            private_key.to_nonzero_scalar(),
            peer_public_key.as_affine(),
        );
        output[..Self::SHARED_SECRET_LEN].copy_from_slice(shared_secret.raw_secret_bytes());
        Ok(Self::SHARED_SECRET_LEN)
    }
}

/// ECDH over NIST P-384.
#[derive(Clone, Copy, Debug, Default)]
pub struct P384;

impl Algorithm for P384 {
    const PRIVATE_KEY_LEN: usize = 48;
    const PUBLIC_KEY_LEN: usize = 97;
    const SHARED_SECRET_LEN: usize = 48;
    const CIPHERTEXT_LEN: Option<usize> = None;

    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if output.len() < Self::PUBLIC_KEY_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::PublicKey,
                required: Self::PUBLIC_KEY_LEN,
                provided: output.len(),
            });
        }
        let private_key =
            p384::SecretKey::from_slice(private_key).map_err(|_| Error::InvalidPrivateKey)?;
        let public_key = p384::Sec1Point::from(private_key.public_key());
        output[..Self::PUBLIC_KEY_LEN].copy_from_slice(public_key.as_bytes());
        Ok(Self::PUBLIC_KEY_LEN)
    }

    fn agree(
        &self,
        private_key: &[u8],
        peer_public_key: &[u8],
        output: &mut [u8],
    ) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if peer_public_key.len() != Self::PUBLIC_KEY_LEN {
            return Err(Error::InvalidPublicKeyLength {
                required: Self::PUBLIC_KEY_LEN,
                provided: peer_public_key.len(),
            });
        }
        if output.len() < Self::SHARED_SECRET_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::SharedSecret,
                required: Self::SHARED_SECRET_LEN,
                provided: output.len(),
            });
        }
        let private_key =
            p384::SecretKey::from_slice(private_key).map_err(|_| Error::InvalidPrivateKey)?;
        let peer_public_key = p384::PublicKey::from_sec1_bytes(peer_public_key)
            .map_err(|_| Error::InvalidPublicKey)?;
        let shared_secret = p384::ecdh::diffie_hellman(
            private_key.to_nonzero_scalar(),
            peer_public_key.as_affine(),
        );
        output[..Self::SHARED_SECRET_LEN].copy_from_slice(shared_secret.raw_secret_bytes());
        Ok(Self::SHARED_SECRET_LEN)
    }
}

/// ML-KEM-768.
#[derive(Clone, Copy, Debug, Default)]
pub struct MlKem768;

impl Algorithm for MlKem768 {
    const PRIVATE_KEY_LEN: usize = 64;
    const PUBLIC_KEY_LEN: usize = 1184;
    const SHARED_SECRET_LEN: usize = 32;
    const CIPHERTEXT_LEN: Option<usize> = Some(1088);
    const ENCAPSULATION_ENTROPY_LEN: usize = 32;

    fn public_key(&self, private_key: &[u8], output: &mut [u8]) -> Result<usize, Error> {
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if output.len() < Self::PUBLIC_KEY_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::PublicKey,
                required: Self::PUBLIC_KEY_LEN,
                provided: output.len(),
            });
        }
        use ml_kem::KeyExport;

        let seed = ml_kem::Seed::try_from(private_key).map_err(|_| Error::InvalidPrivateKey)?;
        let private_key = ml_kem::ml_kem_768::DecapsulationKey::from_seed(seed);
        let public_key = private_key.encapsulation_key().to_bytes();
        output[..Self::PUBLIC_KEY_LEN].copy_from_slice(public_key.as_slice());
        Ok(Self::PUBLIC_KEY_LEN)
    }

    fn encapsulate(
        &self,
        peer_public_key: &[u8],
        entropy: &[u8],
        ciphertext: &mut [u8],
        shared_secret: &mut [u8],
    ) -> Result<(), Error> {
        let ciphertext_len = Self::CIPHERTEXT_LEN.expect("ML-KEM defines ciphertext output");
        if peer_public_key.len() != Self::PUBLIC_KEY_LEN {
            return Err(Error::InvalidPublicKeyLength {
                required: Self::PUBLIC_KEY_LEN,
                provided: peer_public_key.len(),
            });
        }
        if entropy.len() != Self::ENCAPSULATION_ENTROPY_LEN {
            return Err(Error::InvalidEntropyLength {
                required: Self::ENCAPSULATION_ENTROPY_LEN,
                provided: entropy.len(),
            });
        }
        if ciphertext.len() < ciphertext_len {
            return Err(Error::OutputTooSmall {
                output: Output::Ciphertext,
                required: ciphertext_len,
                provided: ciphertext.len(),
            });
        }
        if shared_secret.len() < Self::SHARED_SECRET_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::SharedSecret,
                required: Self::SHARED_SECRET_LEN,
                provided: shared_secret.len(),
            });
        }
        use ml_kem::TryKeyInit;

        let peer_public_key = ml_kem::ml_kem_768::EncapsulationKey::new_from_slice(peer_public_key)
            .map_err(|_| Error::InvalidPublicKey)?;
        let entropy = ml_kem::B32::try_from(entropy)
            .expect("ML-KEM encapsulation entropy length was validated");
        let (encapsulated, secret) = peer_public_key.encapsulate_deterministic(&entropy);
        ciphertext[..ciphertext_len].copy_from_slice(encapsulated.as_slice());
        shared_secret[..Self::SHARED_SECRET_LEN].copy_from_slice(secret.as_slice());
        Ok(())
    }

    fn decapsulate(
        &self,
        private_key: &[u8],
        ciphertext: &[u8],
        shared_secret: &mut [u8],
    ) -> Result<usize, Error> {
        let ciphertext_len = Self::CIPHERTEXT_LEN.expect("ML-KEM defines ciphertext input");
        if private_key.len() != Self::PRIVATE_KEY_LEN {
            return Err(Error::InvalidPrivateKeyLength {
                required: Self::PRIVATE_KEY_LEN,
                provided: private_key.len(),
            });
        }
        if ciphertext.len() != ciphertext_len {
            return Err(Error::InvalidCiphertextLength {
                required: ciphertext_len,
                provided: ciphertext.len(),
            });
        }
        if shared_secret.len() < Self::SHARED_SECRET_LEN {
            return Err(Error::OutputTooSmall {
                output: Output::SharedSecret,
                required: Self::SHARED_SECRET_LEN,
                provided: shared_secret.len(),
            });
        }
        use ml_kem::Decapsulate;

        let seed = ml_kem::Seed::try_from(private_key).map_err(|_| Error::InvalidPrivateKey)?;
        let private_key = ml_kem::ml_kem_768::DecapsulationKey::from_seed(seed);
        let ciphertext = ml_kem::ml_kem_768::Ciphertext::try_from(ciphertext)
            .expect("ML-KEM ciphertext length was validated");
        let secret = private_key.decapsulate(&ciphertext);
        shared_secret[..Self::SHARED_SECRET_LEN].copy_from_slice(secret.as_slice());
        Ok(Self::SHARED_SECRET_LEN)
    }
}
