use core::mem::{size_of, transmute};

use thiserror::Error;

use super::extension::{self, Extension, ExtensionError};

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ExtensionsLength {
    length: [u8; 2],
}

const _: () = assert!(size_of::<ExtensionsLength>() == 2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncryptedExtensions<'a> {
    extensions: &'a [u8],
}

impl<'a> EncryptedExtensions<'a> {
    pub(crate) fn decode(input: &'a [u8]) -> Result<Self, EncryptedExtensionsError> {
        let length_bytes = input.get(..size_of::<ExtensionsLength>()).ok_or(
            EncryptedExtensionsError::LengthTruncated {
                available: input.len(),
            },
        )?;
        // SAFETY: `length_bytes` contains a complete packed `ExtensionsLength`
        // and is only read unaligned.
        let length = unsafe {
            transmute::<_, *const ExtensionsLength>(length_bytes.as_ptr()).read_unaligned()
        };
        let declared = usize::from(u16::from_be_bytes(length.length));
        let extensions = input
            .get(size_of::<ExtensionsLength>()..)
            .expect("two-byte encrypted extensions length exists");
        if extensions.len() != declared {
            return Err(EncryptedExtensionsError::Length {
                declared,
                available: extensions.len(),
            });
        }
        extension::validate(extensions)
            .map_err(|source| EncryptedExtensionsError::Extension { source })?;
        Ok(Self { extensions })
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<usize, EncryptedExtensionsError> {
        let required = size_of::<ExtensionsLength>() + self.extensions.len();
        if output.len() < required {
            return Err(EncryptedExtensionsError::OutputTooSmall {
                required,
                available: output.len(),
            });
        }
        let length = u16::try_from(self.extensions.len()).map_err(|_| {
            EncryptedExtensionsError::LengthLimit {
                length: self.extensions.len(),
            }
        })?;
        let wire = ExtensionsLength {
            length: length.to_be_bytes(),
        };
        // SAFETY: the output length check covers the complete packed header;
        // the pointer is only written unaligned.
        unsafe { transmute::<_, *mut ExtensionsLength>(output.as_mut_ptr()).write_unaligned(wire) };
        output[size_of::<ExtensionsLength>()..required].copy_from_slice(self.extensions);
        Ok(required)
    }

    pub(crate) fn extension<T>(self) -> Result<Option<T>, T::Error>
    where
        T: Extension<'a>,
    {
        extension::find(self.extensions)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum EncryptedExtensionsError {
    #[error("TLS EncryptedExtensions length is truncated: received {available} bytes")]
    LengthTruncated { available: usize },
    #[error("TLS EncryptedExtensions declares {declared} bytes, received {available}")]
    Length { declared: usize, available: usize },
    #[error("TLS EncryptedExtensions length {length} exceeds the u16 wire limit")]
    LengthLimit { length: usize },
    #[error("TLS EncryptedExtensions extension framing failed")]
    Extension {
        #[source]
        source: ExtensionError,
    },
    #[error("TLS EncryptedExtensions output requires {required} bytes, received {available}")]
    OutputTooSmall { required: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc8448_encrypted_extensions_round_trip_without_body_copies() {
        let input = [
            0x00, 0x22, 0x00, 0x0a, 0x00, 0x14, 0x00, 0x12, 0x00, 0x1d, 0x00, 0x17, 0x00, 0x18,
            0x00, 0x19, 0x01, 0x00, 0x01, 0x01, 0x01, 0x02, 0x01, 0x03, 0x01, 0x04, 0x00, 0x1c,
            0x00, 0x02, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00,
        ];

        let message = EncryptedExtensions::decode(&input).expect("RFC 8448 extensions");
        let mut output = [0u8; 36];

        assert_eq!(message.extensions.as_ptr(), input[2..].as_ptr());
        assert_eq!(message.encode(&mut output), Ok(input.len()));
        assert_eq!(output, input);
    }
}
