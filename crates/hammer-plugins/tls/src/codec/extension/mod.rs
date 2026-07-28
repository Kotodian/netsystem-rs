//! TLS extension framing is independent of the handshake messages that carry
//! extensions. Each extension implements `Extension`; protocol state requests
//! its concrete type and never dispatches through a central extension enum.

use core::mem::{size_of, transmute};

use thiserror::Error;

pub(crate) mod key_share;
pub(crate) mod signature_algorithms;
pub(crate) mod supported_versions;

pub(crate) trait Extension<'a>: Sized {
    type Error;

    const TYPE: u16;

    fn decode(bytes: &'a [u8]) -> Result<Self, Self::Error>;
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ExtensionHeader {
    extension_type: [u8; 2],
    length: [u8; 2],
}

const _: () = assert!(size_of::<ExtensionHeader>() == 4);

fn decode(input: &[u8]) -> Result<(u16, &[u8], usize), ExtensionError> {
    let header_bytes =
        input
            .get(..size_of::<ExtensionHeader>())
            .ok_or(ExtensionError::HeaderTruncated {
                available: input.len(),
            })?;
    // SAFETY: `header_bytes` contains a complete packed `ExtensionHeader` and
    // is only read unaligned.
    let header =
        unsafe { transmute::<_, *const ExtensionHeader>(header_bytes.as_ptr()).read_unaligned() };

    let extension_type = u16::from_be_bytes(header.extension_type);
    let body_length = usize::from(u16::from_be_bytes(header.length));
    let consumed = size_of::<ExtensionHeader>()
        .checked_add(body_length)
        .ok_or(ExtensionError::BodyTruncated {
            extension_type,
            declared: body_length,
            available: input.len().saturating_sub(size_of::<ExtensionHeader>()),
        })?;
    let body =
        input
            .get(size_of::<ExtensionHeader>()..consumed)
            .ok_or(ExtensionError::BodyTruncated {
                extension_type,
                declared: body_length,
                available: input.len().saturating_sub(size_of::<ExtensionHeader>()),
            })?;

    Ok((extension_type, body, consumed))
}

pub(super) fn validate(mut input: &[u8]) -> Result<(), ExtensionError> {
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

pub(super) fn find<'a, T>(mut input: &'a [u8]) -> Result<Option<T>, T::Error>
where
    T: Extension<'a>,
{
    while !input.is_empty() {
        let (extension_type, body, consumed) = decode(input)
            .expect("TLS extensions were validated by the containing handshake message");
        if extension_type == T::TYPE {
            return T::decode(body).map(Some);
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
    #[error("TLS handshake message contains duplicate extension {extension_type}")]
    Duplicate { extension_type: u16 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct PrivateExtension<'a>(&'a [u8]);

    impl<'a> Extension<'a> for PrivateExtension<'a> {
        type Error = core::convert::Infallible;

        const TYPE: u16 = 0xfe0d;

        fn decode(bytes: &'a [u8]) -> Result<Self, Self::Error> {
            Ok(Self(bytes))
        }
    }

    #[test]
    fn framing_preserves_unknown_extension_type_and_borrows_body() {
        let input = [0xfe, 0x0d, 0x00, 0x03, 1, 2, 3];

        let (extension_type, body, consumed) = decode(&input).expect("unknown extension");

        assert_eq!(extension_type, 0xfe0d);
        assert_eq!(body, &[1, 2, 3]);
        assert_eq!(body.as_ptr(), input[4..].as_ptr());
        assert_eq!(consumed, input.len());
    }

    #[test]
    fn framing_reports_typed_body_truncation() {
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
    fn framing_rejects_duplicate_unknown_extensions() {
        let input = [0xfe, 0x0d, 0x00, 0x01, 1, 0xfe, 0x0d, 0x00, 0x01, 2];

        assert_eq!(
            validate(&input),
            Err(ExtensionError::Duplicate {
                extension_type: 0xfe0d,
            })
        );
    }

    #[test]
    fn body_module_decodes_without_central_registration() {
        let input = [0xfe, 0x0d, 0x00, 0x03, 1, 2, 3];

        let extension = find::<PrivateExtension<'_>>(&input)
            .expect("infallible private extension")
            .expect("private extension");

        assert_eq!(extension, PrivateExtension(&input[4..]));
    }
}
