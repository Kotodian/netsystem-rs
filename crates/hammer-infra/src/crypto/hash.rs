//! Portable digest implementations.

use std::fmt;
use std::marker::PhantomData;

use sha2::Digest as DigestTrait;

/// A failure produced while computing a digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Caller output cannot hold the complete digest.
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
                "hash output requires {required} bytes but caller provided {provided}"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Complete semantics supplied by one digest algorithm.
pub trait Algorithm: Default + fmt::Debug + 'static {
    /// Digest length in bytes.
    const OUTPUT_LEN: usize;

    /// Computes a digest over ordered fragments into caller-owned output.
    fn digest(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, Error>;
}

/// Digest semantics shared by algorithms implemented through the Rust digest traits.
pub struct Digest<D, const OUTPUT_LEN: usize>(PhantomData<fn() -> D>);

impl<D, const OUTPUT_LEN: usize> Clone for Digest<D, OUTPUT_LEN> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D, const OUTPUT_LEN: usize> Copy for Digest<D, OUTPUT_LEN> {}

impl<D, const OUTPUT_LEN: usize> Default for Digest<D, OUTPUT_LEN> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<D, const OUTPUT_LEN: usize> fmt::Debug for Digest<D, OUTPUT_LEN> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(std::any::type_name::<D>())
    }
}

impl<D, const OUTPUT_LEN: usize> Algorithm for Digest<D, OUTPUT_LEN>
where
    D: DigestTrait + 'static,
{
    const OUTPUT_LEN: usize = OUTPUT_LEN;

    fn digest(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, Error> {
        if output.len() < OUTPUT_LEN {
            return Err(Error::OutputTooSmall {
                required: OUTPUT_LEN,
                provided: output.len(),
            });
        }
        let mut digest = D::new();
        for fragment in input {
            DigestTrait::update(&mut digest, fragment);
        }
        let digest = digest.finalize();
        assert_eq!(
            digest.len(),
            OUTPUT_LEN,
            "digest output length must match its algorithm declaration"
        );
        output[..OUTPUT_LEN].copy_from_slice(&digest);
        Ok(OUTPUT_LEN)
    }
}

/// SHA-256 with separate portable and instruction-selected execution methods.
#[derive(Clone, Copy, Debug, Default)]
pub struct Sha256;

impl Algorithm for Sha256 {
    const OUTPUT_LEN: usize = 32;

    fn digest(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, Error> {
        Digest::<sha2::Sha256, 32>::default().digest(input, output)
    }
}

impl Sha256 {
    /// Computes SHA-256 through the architecture-selected SHA-2 instruction backend.
    #[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
    pub fn digest_sha2(&self, input: &[&[u8]], output: &mut [u8]) -> Result<usize, Error> {
        const OUTPUT_LEN: usize = 32;
        if output.len() < OUTPUT_LEN {
            return Err(Error::OutputTooSmall {
                required: OUTPUT_LEN,
                provided: output.len(),
            });
        }
        use sha2_accelerated::Digest as _;
        let mut digest = sha2_accelerated::Sha256::new();
        for fragment in input {
            digest.update(fragment);
        }
        output[..OUTPUT_LEN].copy_from_slice(&digest.finalize());
        Ok(OUTPUT_LEN)
    }
}

/// SHA-384.
pub type Sha384 = Digest<sha2::Sha384, 48>;
/// SHA-512.
pub type Sha512 = Digest<sha2::Sha512, 64>;
/// BLAKE2s-256.
pub type Blake2s256 = Digest<blake2::Blake2s256, 32>;
/// BLAKE2b-512.
pub type Blake2b512 = Digest<blake2::Blake2b512, 64>;
