use core::mem::{size_of, transmute};

use thiserror::Error;

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Prefix {
    signature_scheme: [u8; 2],
    signature_length: [u8; 2],
}

const _: () = assert!(size_of::<Prefix>() == 4);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CertificateVerify<'a> {
    signature_scheme: [u8; 2],
    signature: &'a [u8],
}

impl<'a> CertificateVerify<'a> {
    pub(crate) fn new(signature_scheme: [u8; 2], signature: &'a [u8]) -> Self {
        Self {
            signature_scheme,
            signature,
        }
    }

    pub(crate) fn decode(input: &'a [u8]) -> Result<Self, CertificateVerifyError> {
        let prefix_bytes =
            input
                .get(..size_of::<Prefix>())
                .ok_or(CertificateVerifyError::PrefixTruncated {
                    available: input.len(),
                })?;
        // SAFETY: `prefix_bytes` contains a complete packed `Prefix` and is
        // only read unaligned.
        let prefix =
            unsafe { transmute::<_, *const Prefix>(prefix_bytes.as_ptr()).read_unaligned() };
        let declared = usize::from(u16::from_be_bytes(prefix.signature_length));
        let signature = input
            .get(size_of::<Prefix>()..)
            .expect("CertificateVerify prefix exists");
        if signature.len() != declared {
            return Err(CertificateVerifyError::SignatureLength {
                declared,
                available: signature.len(),
            });
        }
        Ok(Self {
            signature_scheme: prefix.signature_scheme,
            signature,
        })
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<usize, CertificateVerifyError> {
        let required = size_of::<Prefix>() + self.signature.len();
        if output.len() < required {
            return Err(CertificateVerifyError::OutputTooSmall {
                required,
                available: output.len(),
            });
        }
        let signature_length = u16::try_from(self.signature.len()).map_err(|_| {
            CertificateVerifyError::SignatureLengthLimit {
                length: self.signature.len(),
            }
        })?;
        let prefix = Prefix {
            signature_scheme: self.signature_scheme,
            signature_length: signature_length.to_be_bytes(),
        };
        // SAFETY: the output length check covers the complete packed prefix;
        // the pointer is only written unaligned.
        unsafe { transmute::<_, *mut Prefix>(output.as_mut_ptr()).write_unaligned(prefix) };
        output[size_of::<Prefix>()..required].copy_from_slice(self.signature);
        Ok(required)
    }

    pub(crate) fn signature_scheme(self) -> [u8; 2] {
        self.signature_scheme
    }

    pub(crate) fn signature(self) -> &'a [u8] {
        self.signature
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CertificateVerifyError {
    #[error("TLS CertificateVerify prefix is truncated: received {available} bytes")]
    PrefixTruncated { available: usize },
    #[error("TLS CertificateVerify signature declares {declared} bytes, received {available}")]
    SignatureLength { declared: usize, available: usize },
    #[error("TLS CertificateVerify signature length {length} exceeds the u16 wire limit")]
    SignatureLengthLimit { length: usize },
    #[error("TLS CertificateVerify output requires {required} bytes, received {available}")]
    OutputTooSmall { required: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_verify_borrows_and_round_trips_signature() {
        let input = [0x08, 0x07, 0x00, 0x03, 1, 2, 3];
        let message = CertificateVerify::decode(&input).expect("CertificateVerify");
        let mut output = [0u8; 7];

        assert_eq!(message.signature_scheme(), [0x08, 0x07]);
        assert_eq!(message.signature().as_ptr(), input[4..].as_ptr());
        assert_eq!(message.encode(&mut output), Ok(input.len()));
        assert_eq!(output, input);
    }
}
