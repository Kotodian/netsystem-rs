use core::mem::{size_of, transmute};

use thiserror::Error;

use super::Extension;

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Header {
    extension_type: [u8; 2],
    length: [u8; 2],
}

const _: () = assert!(size_of::<Header>() == 4);

fn decode(input: &[u8]) -> Result<(u16, &[u8], usize), ExtensionError> {
    let header_bytes = input
        .get(..size_of::<Header>())
        .ok_or(ExtensionError::HeaderTruncated {
            available: input.len(),
        })?;
    // SAFETY: `header_bytes` contains a complete packed `Header` and is only
    // read unaligned.
    let header = unsafe { transmute::<_, *const Header>(header_bytes.as_ptr()).read_unaligned() };

    let extension_type = u16::from_be_bytes(header.extension_type);
    let body_length = usize::from(u16::from_be_bytes(header.length));
    let consumed =
        size_of::<Header>()
            .checked_add(body_length)
            .ok_or(ExtensionError::BodyTruncated {
                extension_type,
                declared: body_length,
                available: input.len().saturating_sub(size_of::<Header>()),
            })?;
    let body = input
        .get(size_of::<Header>()..consumed)
        .ok_or(ExtensionError::BodyTruncated {
            extension_type,
            declared: body_length,
            available: input.len().saturating_sub(size_of::<Header>()),
        })?;

    Ok((extension_type, body, consumed))
}

pub(crate) fn encode<'a, T>(extension: &T, output: &mut [u8]) -> Result<usize, ExtensionError>
where
    T: Extension<'a>,
{
    let body_length = extension.body_len();
    let length = u16::try_from(body_length).map_err(|_| ExtensionError::BodyLength {
        extension_type: T::TYPE,
        length: body_length,
    })?;
    let required = size_of::<Header>() + body_length;
    if output.len() < required {
        return Err(ExtensionError::OutputTooSmall {
            extension_type: T::TYPE,
            required,
            available: output.len(),
        });
    }

    let header = Header {
        extension_type: T::TYPE.to_be_bytes(),
        length: length.to_be_bytes(),
    };
    // SAFETY: the output length check covers the complete packed header and
    // the pointer is only written unaligned.
    unsafe { transmute::<_, *mut Header>(output.as_mut_ptr()).write_unaligned(header) };
    extension.encode_body(&mut output[size_of::<Header>()..required]);
    Ok(required)
}

pub(crate) fn validate(mut input: &[u8]) -> Result<(), ExtensionError> {
    while !input.is_empty() {
        let (extension_type, _, consumed) = decode(input)?;
        let mut remaining = &input[consumed..];
        while !remaining.is_empty() {
            let (candidate_type, _, candidate_length) = decode(remaining)?;
            if candidate_type == extension_type {
                return Err(ExtensionError::Duplicate { extension_type });
            }
            remaining = &remaining[candidate_length..];
        }
        input = &input[consumed..];
    }
    Ok(())
}

pub(crate) fn find<'a, T>(mut input: &'a [u8]) -> Result<Option<T>, T::Error>
where
    T: Extension<'a>,
{
    while !input.is_empty() {
        let (extension_type, body, consumed) =
            decode(input).expect("TLS extensions were validated by the containing message");
        if extension_type == T::TYPE {
            return T::decode_body(body).map(Some);
        }
        input = &input[consumed..];
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ExtensionError {
    #[error("TLS extension header is truncated: received {available} bytes")]
    HeaderTruncated { available: usize },
    #[error(
        "TLS extension {extension_type} is truncated: declared {declared}, received {available}"
    )]
    BodyTruncated {
        extension_type: u16,
        declared: usize,
        available: usize,
    },
    #[error("TLS extension {extension_type} body length {length} exceeds the u16 wire limit")]
    BodyLength { extension_type: u16, length: usize },
    #[error(
        "TLS extension {extension_type} output requires {required} bytes, received {available}"
    )]
    OutputTooSmall {
        extension_type: u16,
        required: usize,
        available: usize,
    },
    #[error("TLS handshake message contains duplicate extension {extension_type}")]
    Duplicate { extension_type: u16 },
}

#[cfg(test)]
mod tests {
    use core::convert::Infallible;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct PrivateExtension<'a>(&'a [u8]);

    impl<'a> Extension<'a> for PrivateExtension<'a> {
        type Error = Infallible;

        const TYPE: u16 = 0xfe0d;

        fn decode_body(bytes: &'a [u8]) -> Result<Self, Self::Error> {
            Ok(Self(bytes))
        }

        fn body_len(&self) -> usize {
            self.0.len()
        }

        fn encode_body(&self, output: &mut [u8]) {
            output.copy_from_slice(self.0);
        }
    }

    #[test]
    fn decode_preserves_unknown_extension_type_and_borrows_body() {
        let input = [0xfe, 0x0d, 0x00, 0x03, 1, 2, 3];

        let (extension_type, body, consumed) = decode(&input).expect("unknown extension");

        assert_eq!((extension_type, body, consumed), (0xfe0d, &input[4..], 7));
        assert_eq!(body.as_ptr(), input[4..].as_ptr());
    }

    #[test]
    fn decode_reports_typed_body_truncation() {
        let input = [0x00, 0x2b, 0x00, 0x02, 0x03];

        assert_eq!(
            decode(&input),
            Err(ExtensionError::BodyTruncated {
                extension_type: 43,
                declared: 2,
                available: 1,
            })
        );
    }

    #[test]
    fn validate_rejects_duplicate_unknown_extensions() {
        let input = [0xfe, 0x0d, 0x00, 0x01, 1, 0xfe, 0x0d, 0x00, 0x01, 2];

        assert_eq!(
            validate(&input),
            Err(ExtensionError::Duplicate {
                extension_type: 0xfe0d,
            })
        );
    }

    #[test]
    fn concrete_extension_decodes_without_central_registration() {
        let input = [0xfe, 0x0d, 0x00, 0x03, 1, 2, 3];

        let extension = find::<PrivateExtension<'_>>(&input)
            .expect("infallible private extension")
            .expect("private extension");

        assert_eq!(extension, PrivateExtension(&input[4..]));
    }

    #[test]
    fn concrete_extension_encodes_without_central_registration() {
        let body = [1, 2, 3];
        let extension = PrivateExtension(&body);
        let mut output = [0u8; 7];

        assert_eq!(encode(&extension, &mut output), Ok(7));
        assert_eq!(output, [0xfe, 0x0d, 0x00, 0x03, 1, 2, 3]);
    }
}
