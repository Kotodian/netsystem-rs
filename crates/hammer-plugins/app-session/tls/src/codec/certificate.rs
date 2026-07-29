use core::mem::{size_of, transmute};

use thiserror::Error;

use super::extension::{self, ExtensionError};

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ListLength {
    length: [u8; 3],
}

const _: () = assert!(size_of::<ListLength>() == 3);

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct EntryLength {
    length: [u8; 3],
}

const _: () = assert!(size_of::<EntryLength>() == 3);

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ExtensionsLength {
    length: [u8; 2],
}

const _: () = assert!(size_of::<ExtensionsLength>() == 2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Certificate<'a> {
    request_context: &'a [u8],
    entries: &'a [u8],
}

impl<'a> Certificate<'a> {
    pub(crate) fn decode(input: &'a [u8]) -> Result<Self, CertificateError> {
        let (&context_length, remaining) = input
            .split_first()
            .ok_or(CertificateError::ContextLengthTruncated)?;
        let context_length = usize::from(context_length);
        let request_context =
            remaining
                .get(..context_length)
                .ok_or(CertificateError::ContextTruncated {
                    declared: context_length,
                    available: remaining.len(),
                })?;
        let list_start = 1 + context_length;
        let list_length_bytes = input
            .get(list_start..list_start + size_of::<ListLength>())
            .ok_or(CertificateError::ListLengthTruncated)?;
        // SAFETY: `list_length_bytes` contains a complete packed `ListLength`
        // and is only read unaligned.
        let list_length = unsafe {
            transmute::<_, *const ListLength>(list_length_bytes.as_ptr()).read_unaligned()
        };
        let declared = decode_u24(list_length.length);
        let entries_start = list_start + size_of::<ListLength>();
        let entries = input
            .get(entries_start..)
            .expect("Certificate list length exists");
        if entries.len() != declared {
            return Err(CertificateError::ListLength {
                declared,
                available: entries.len(),
            });
        }
        if entries.is_empty() {
            return Err(CertificateError::ListEmpty);
        }
        let mut unparsed = entries;
        while !unparsed.is_empty() {
            let (_, consumed) = decode_entry(unparsed)?;
            unparsed = &unparsed[consumed..];
        }
        Ok(Self {
            request_context,
            entries,
        })
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<usize, CertificateError> {
        let required =
            1 + self.request_context.len() + size_of::<ListLength>() + self.entries.len();
        if output.len() < required {
            return Err(CertificateError::OutputTooSmall {
                required,
                available: output.len(),
            });
        }
        let context_length = u8::try_from(self.request_context.len()).map_err(|_| {
            CertificateError::ContextLengthLimit {
                length: self.request_context.len(),
            }
        })?;
        let list_length = encode_u24(self.entries.len())?;
        output[0] = context_length;
        let mut offset = 1;
        output[offset..offset + self.request_context.len()].copy_from_slice(self.request_context);
        offset += self.request_context.len();
        let wire = ListLength {
            length: list_length,
        };
        // SAFETY: the output length check covers the complete packed list
        // length at `offset`; the pointer is only written unaligned.
        unsafe {
            transmute::<_, *mut ListLength>(output.as_mut_ptr().add(offset)).write_unaligned(wire)
        };
        offset += size_of::<ListLength>();
        output[offset..required].copy_from_slice(self.entries);
        Ok(required)
    }

    pub(crate) fn leaf(self) -> &'a [u8] {
        decode_entry(self.entries)
            .expect("Certificate entries were validated during decode")
            .0
    }
}

fn decode_entry(input: &[u8]) -> Result<(&[u8], usize), CertificateError> {
    let length_bytes =
        input
            .get(..size_of::<EntryLength>())
            .ok_or(CertificateError::EntryLengthTruncated {
                available: input.len(),
            })?;
    // SAFETY: `length_bytes` contains a complete packed `EntryLength` and is
    // only read unaligned.
    let length =
        unsafe { transmute::<_, *const EntryLength>(length_bytes.as_ptr()).read_unaligned() };
    let declared = decode_u24(length.length);
    if declared == 0 {
        return Err(CertificateError::EntryEmpty);
    }
    let certificate_start = size_of::<EntryLength>();
    let certificate_end =
        certificate_start
            .checked_add(declared)
            .ok_or(CertificateError::EntryTruncated {
                declared,
                available: input.len().saturating_sub(certificate_start),
            })?;
    let certificate =
        input
            .get(certificate_start..certificate_end)
            .ok_or(CertificateError::EntryTruncated {
                declared,
                available: input.len().saturating_sub(certificate_start),
            })?;
    let extensions_length_bytes = input
        .get(certificate_end..certificate_end + size_of::<ExtensionsLength>())
        .ok_or(CertificateError::ExtensionsLengthTruncated)?;
    // SAFETY: `extensions_length_bytes` contains a complete packed
    // `ExtensionsLength` and is only read unaligned.
    let extensions_length = unsafe {
        transmute::<_, *const ExtensionsLength>(extensions_length_bytes.as_ptr()).read_unaligned()
    };
    let extensions_length = usize::from(u16::from_be_bytes(extensions_length.length));
    let extensions_start = certificate_end + size_of::<ExtensionsLength>();
    let consumed = extensions_start.checked_add(extensions_length).ok_or(
        CertificateError::ExtensionsTruncated {
            declared: extensions_length,
            available: input.len().saturating_sub(extensions_start),
        },
    )?;
    let extensions =
        input
            .get(extensions_start..consumed)
            .ok_or(CertificateError::ExtensionsTruncated {
                declared: extensions_length,
                available: input.len().saturating_sub(extensions_start),
            })?;
    extension::validate(extensions).map_err(|source| CertificateError::Extension { source })?;
    Ok((certificate, consumed))
}

fn decode_u24(bytes: [u8; 3]) -> usize {
    usize::try_from(u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]))
        .expect("u24 certificate length fits in usize")
}

fn encode_u24(length: usize) -> Result<[u8; 3], CertificateError> {
    if length > 0x00ff_ffff {
        return Err(CertificateError::ListLengthLimit { length });
    }
    let bytes = u32::try_from(length)
        .expect("validated Certificate length fits in u32")
        .to_be_bytes();
    Ok([bytes[1], bytes[2], bytes[3]])
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CertificateError {
    #[error("TLS Certificate request context length is truncated")]
    ContextLengthTruncated,
    #[error("TLS Certificate request context declares {declared} bytes, received {available}")]
    ContextTruncated { declared: usize, available: usize },
    #[error("TLS Certificate list length is truncated")]
    ListLengthTruncated,
    #[error("TLS Certificate list declares {declared} bytes, received {available}")]
    ListLength { declared: usize, available: usize },
    #[error("TLS Certificate list is empty")]
    ListEmpty,
    #[error("TLS Certificate entry length is truncated: received {available} bytes")]
    EntryLengthTruncated { available: usize },
    #[error("TLS Certificate entry must not be empty")]
    EntryEmpty,
    #[error("TLS Certificate entry declares {declared} bytes, received {available}")]
    EntryTruncated { declared: usize, available: usize },
    #[error("TLS Certificate entry extensions length is truncated")]
    ExtensionsLengthTruncated,
    #[error("TLS Certificate entry extensions declare {declared} bytes, received {available}")]
    ExtensionsTruncated { declared: usize, available: usize },
    #[error("TLS Certificate entry extension framing failed")]
    Extension {
        #[source]
        source: ExtensionError,
    },
    #[error("TLS Certificate request context length {length} exceeds the u8 wire limit")]
    ContextLengthLimit { length: usize },
    #[error("TLS Certificate list length {length} exceeds the u24 wire limit")]
    ListLengthLimit { length: usize },
    #[error("TLS Certificate output requires {required} bytes, received {available}")]
    OutputTooSmall { required: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_borrows_leaf_and_round_trips_entries() {
        let input = [0, 0, 0, 7, 0, 0, 2, 0xaa, 0xbb, 0, 0];
        let message = Certificate::decode(&input).expect("one Certificate entry");
        let mut output = [0u8; 11];

        assert_eq!(message.leaf(), &[0xaa, 0xbb]);
        assert_eq!(message.leaf().as_ptr(), input[7..].as_ptr());
        assert_eq!(message.encode(&mut output), Ok(input.len()));
        assert_eq!(output, input);
    }

    #[test]
    fn certificate_rejects_truncated_entry_extensions() {
        let input = [0, 0, 0, 7, 0, 0, 2, 0xaa, 0xbb, 0, 1];

        assert_eq!(
            Certificate::decode(&input),
            Err(CertificateError::ExtensionsTruncated {
                declared: 1,
                available: 0,
            })
        );
    }
}
