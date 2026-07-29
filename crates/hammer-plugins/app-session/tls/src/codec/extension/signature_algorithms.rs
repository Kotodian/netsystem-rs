use thiserror::Error;

use super::Extension;

const SIGNATURE_ALGORITHMS: u16 = 13;
pub(crate) const ED25519: [u8; 2] = [0x08, 0x07];
pub(crate) const RSA_PSS_RSAE_SHA256: [u8; 2] = [0x08, 0x04];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SignatureAlgorithms<'a> {
    schemes: &'a [u8],
}

impl SignatureAlgorithms<'_> {
    pub(crate) fn contains(self, scheme: [u8; 2]) -> bool {
        self.schemes
            .chunks_exact(2)
            .any(|candidate| candidate == scheme)
    }
}

impl<'a> Extension<'a> for SignatureAlgorithms<'a> {
    type Error = SignatureAlgorithmsError;

    const TYPE: u16 = SIGNATURE_ALGORITHMS;

    fn decode_body(body: &'a [u8]) -> Result<Self, Self::Error> {
        let length_bytes = body
            .get(..2)
            .ok_or(SignatureAlgorithmsError::LengthTruncated)?;
        let declared = usize::from(u16::from_be_bytes([length_bytes[0], length_bytes[1]]));
        let schemes = body
            .get(2..)
            .expect("two-byte signature algorithms length exists");
        if declared == 0 || declared % 2 != 0 {
            return Err(SignatureAlgorithmsError::Length { length: declared });
        }
        if schemes.len() != declared {
            return Err(SignatureAlgorithmsError::BodyLength {
                declared,
                available: schemes.len(),
            });
        }
        Ok(Self { schemes })
    }

    fn body_len(&self) -> usize {
        2 + self.schemes.len()
    }

    fn encode_body(&self, output: &mut [u8]) {
        let length = u16::try_from(self.schemes.len())
            .expect("validated signature_algorithms length fits u16");
        output[..2].copy_from_slice(&length.to_be_bytes());
        output[2..].copy_from_slice(self.schemes);
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SignatureAlgorithmsError {
    #[error("TLS signature_algorithms list length is truncated")]
    LengthTruncated,
    #[error("TLS signature_algorithms list length {length} must be nonzero and even")]
    Length { length: usize },
    #[error("TLS signature_algorithms list declares {declared} bytes, received {available}")]
    BodyLength { declared: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::super::find;
    use super::*;

    #[test]
    fn rfc8448_signature_algorithms_are_typed_without_central_dispatch() {
        let input = [
            0x00, 0x0d, 0x00, 0x08, 0x00, 0x06, 0x08, 0x04, 0x08, 0x07, 0x04, 0x03,
        ];

        let algorithms = find::<SignatureAlgorithms<'_>>(&input)
            .expect("signature_algorithms body")
            .expect("signature_algorithms extension");

        assert!(algorithms.contains(ED25519));
        assert!(algorithms.contains(RSA_PSS_RSAE_SHA256));
    }
}
