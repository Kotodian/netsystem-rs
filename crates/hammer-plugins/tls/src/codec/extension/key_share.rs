use core::mem::{size_of, transmute};

use thiserror::Error;

use super::Extension;

const KEY_SHARE: u16 = 51;
pub(crate) const X25519: [u8; 2] = [0x00, 0x1d];

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct EntryHeader {
    group: [u8; 2],
    length: [u8; 2],
}

const _: () = assert!(size_of::<EntryHeader>() == 4);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OfferedKeyShares<'a> {
    entries: &'a [u8],
}

impl<'a> OfferedKeyShares<'a> {
    pub(crate) fn key_exchange(self, group: [u8; 2]) -> Result<Option<&'a [u8]>, KeyShareError> {
        let mut entries = self.entries;
        while !entries.is_empty() {
            let (entry_group, key_exchange, consumed) = decode_entry(entries)?;
            if entry_group == group {
                return Ok(Some(key_exchange));
            }
            entries = &entries[consumed..];
        }
        Ok(None)
    }
}

impl<'a> Extension<'a> for OfferedKeyShares<'a> {
    type Error = KeyShareError;

    const TYPE: u16 = KEY_SHARE;

    fn decode_body(body: &'a [u8]) -> Result<Self, Self::Error> {
        let length_bytes = body.get(..2).ok_or(KeyShareError::ListLengthTruncated)?;
        let declared = usize::from(u16::from_be_bytes([length_bytes[0], length_bytes[1]]));
        let entries = body.get(2..).expect("two-byte key share length exists");
        if entries.len() != declared {
            return Err(KeyShareError::ListLength {
                declared,
                available: entries.len(),
            });
        }
        let mut remaining = entries;
        while !remaining.is_empty() {
            let (_, _, consumed) = decode_entry(remaining)?;
            remaining = &remaining[consumed..];
        }
        Ok(Self { entries })
    }

    fn body_len(&self) -> usize {
        2 + self.entries.len()
    }

    fn encode_body(&self, output: &mut [u8]) {
        let length =
            u16::try_from(self.entries.len()).expect("validated key_share list length fits u16");
        output[..2].copy_from_slice(&length.to_be_bytes());
        output[2..].copy_from_slice(self.entries);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedKeyShare<'a> {
    group: [u8; 2],
    key_exchange: &'a [u8],
}

impl<'a> SelectedKeyShare<'a> {
    pub(crate) fn group(self) -> [u8; 2] {
        self.group
    }

    pub(crate) fn key_exchange(self) -> &'a [u8] {
        self.key_exchange
    }
}

impl<'a> Extension<'a> for SelectedKeyShare<'a> {
    type Error = KeyShareError;

    const TYPE: u16 = KEY_SHARE;

    fn decode_body(body: &'a [u8]) -> Result<Self, Self::Error> {
        let (group, key_exchange, consumed) = decode_entry(body)?;
        if consumed != body.len() {
            return Err(KeyShareError::TrailingData {
                trailing: body.len() - consumed,
            });
        }
        Ok(Self {
            group,
            key_exchange,
        })
    }

    fn body_len(&self) -> usize {
        size_of::<EntryHeader>() + self.key_exchange.len()
    }

    fn encode_body(&self, output: &mut [u8]) {
        let length = u16::try_from(self.key_exchange.len())
            .expect("validated key_share key exchange length fits u16");
        let header = EntryHeader {
            group: self.group,
            length: length.to_be_bytes(),
        };
        // SAFETY: framing passes an exact body slice large enough for the
        // packed header and the pointer is only written unaligned.
        unsafe { transmute::<_, *mut EntryHeader>(output.as_mut_ptr()).write_unaligned(header) };
        output[size_of::<EntryHeader>()..].copy_from_slice(self.key_exchange);
    }
}

fn decode_entry(input: &[u8]) -> Result<([u8; 2], &[u8], usize), KeyShareError> {
    let header_bytes =
        input
            .get(..size_of::<EntryHeader>())
            .ok_or(KeyShareError::EntryHeaderTruncated {
                available: input.len(),
            })?;
    // SAFETY: `header_bytes` contains a complete packed `EntryHeader` and is
    // only read unaligned.
    let header =
        unsafe { transmute::<_, *const EntryHeader>(header_bytes.as_ptr()).read_unaligned() };
    let declared = usize::from(u16::from_be_bytes(header.length));
    let consumed = size_of::<EntryHeader>().checked_add(declared).ok_or(
        KeyShareError::KeyExchangeTruncated {
            declared,
            available: input.len().saturating_sub(size_of::<EntryHeader>()),
        },
    )?;
    let key_exchange = input.get(size_of::<EntryHeader>()..consumed).ok_or(
        KeyShareError::KeyExchangeTruncated {
            declared,
            available: input.len().saturating_sub(size_of::<EntryHeader>()),
        },
    )?;
    Ok((header.group, key_exchange, consumed))
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum KeyShareError {
    #[error("TLS ClientHello key_share list length is truncated")]
    ListLengthTruncated,
    #[error("TLS ClientHello key_share list declares {declared} bytes, received {available}")]
    ListLength { declared: usize, available: usize },
    #[error("TLS key_share entry header is truncated: received {available} bytes")]
    EntryHeaderTruncated { available: usize },
    #[error("TLS key_share key exchange declares {declared} bytes, received {available}")]
    KeyExchangeTruncated { declared: usize, available: usize },
    #[error("TLS ServerHello key_share contains {trailing} trailing bytes")]
    TrailingData { trailing: usize },
}

#[cfg(test)]
mod tests {
    use super::super::find;
    use super::*;

    #[test]
    fn rfc8448_client_key_share_borrows_x25519_public_key() {
        let input = [
            0x00, 0x33, 0x00, 0x26, 0x00, 0x24, 0x00, 0x1d, 0x00, 0x20, 0x99, 0x38, 0x1d, 0xe5,
            0x60, 0xe4, 0xbd, 0x43, 0xd2, 0x3d, 0x8e, 0x43, 0x5a, 0x7d, 0xba, 0xfe, 0xb3, 0xc0,
            0x6e, 0x51, 0xc1, 0x3c, 0xae, 0x4d, 0x54, 0x13, 0x69, 0x1e, 0x52, 0x9a, 0xaf, 0x2c,
        ];

        let shares = find::<OfferedKeyShares<'_>>(&input)
            .expect("key_share body")
            .expect("key_share extension");
        let key = shares
            .key_exchange(X25519)
            .expect("valid key share list")
            .expect("x25519 key share");

        assert_eq!(key.len(), 32);
        assert_eq!(key.as_ptr(), input[10..].as_ptr());
    }

    #[test]
    fn server_key_share_requires_exactly_one_entry() {
        let body = [0x00, 0x1d, 0x00, 0x01, 7, 8];

        assert_eq!(
            SelectedKeyShare::decode_body(&body),
            Err(KeyShareError::TrailingData { trailing: 1 })
        );
    }
}
