use thiserror::Error;

use core::mem::{size_of, transmute};

const LEGACY_RECORD_VERSION: [u8; 2] = [0x03, 0x03];
const MAX_PLAINTEXT_LEN: usize = 1 << 14;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ContentType {
    ChangeCipherSpec = 20,
    Alert = 21,
    Handshake = 22,
    ApplicationData = 23,
}

impl TryFrom<u8> for ContentType {
    type Error = RecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            20 => Ok(Self::ChangeCipherSpec),
            21 => Ok(Self::Alert),
            22 => Ok(Self::Handshake),
            23 => Ok(Self::ApplicationData),
            content_type => Err(RecordError::ContentType { content_type }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C, packed)]
pub(crate) struct RecordHeader {
    content_type: u8,
    legacy_version: [u8; 2],
    length: [u8; 2],
}

const _: () = assert!(size_of::<RecordHeader>() == 5);

impl RecordHeader {
    pub(crate) fn new(content_type: ContentType, length: usize) -> Result<Self, RecordError> {
        if length > MAX_PLAINTEXT_LEN {
            return Err(RecordError::PlaintextLength { length });
        }
        Ok(Self {
            content_type: content_type as u8,
            legacy_version: LEGACY_RECORD_VERSION,
            length: u16::try_from(length)
                .expect("TLS plaintext length fits in u16")
                .to_be_bytes(),
        })
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<(), RecordError> {
        let output_len = output.len();
        let bytes = output
            .get_mut(..size_of::<Self>())
            .ok_or(RecordError::OutputTooSmall { output_len })?;
        // SAFETY: `bytes` covers a complete `RecordHeader`. TLS output may be
        // unaligned, so the packed wire value is written with `write_unaligned`.
        let header = unsafe { transmute::<_, *mut Self>(bytes.as_mut_ptr()) };
        // SAFETY: the range check above proves that `header` points to a full
        // writable `RecordHeader`; unaligned access is intentional.
        unsafe { header.write_unaligned(self) };
        Ok(())
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, RecordError> {
        let input_len = input.len();
        let bytes = input
            .get(..size_of::<Self>())
            .ok_or(RecordError::HeaderTruncated { input_len })?;
        // SAFETY: `bytes` contains a complete `RecordHeader`. The resulting raw
        // pointer may be unaligned and is therefore only read unaligned.
        let header = unsafe { transmute::<_, *const Self>(bytes.as_ptr()) };
        // SAFETY: the range check above proves that `header` points to a full
        // initialized `RecordHeader`; unaligned access is intentional.
        let header = unsafe { header.read_unaligned() };
        if header.legacy_version != LEGACY_RECORD_VERSION {
            return Err(RecordError::LegacyVersion {
                version: header.legacy_version,
            });
        }
        ContentType::try_from(header.content_type)?;
        let length = usize::from(u16::from_be_bytes(header.length));
        if length > MAX_PLAINTEXT_LEN {
            return Err(RecordError::PlaintextLength { length });
        }
        Ok(header)
    }

    #[inline(always)]
    fn content_type(self) -> ContentType {
        ContentType::try_from(self.content_type)
            .expect("validated TLS record header retains a known content type")
    }

    #[inline(always)]
    fn length(self) -> usize {
        usize::from(u16::from_be_bytes(self.length))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Record<'a> {
    header: RecordHeader,
    payload: &'a [u8],
}

impl<'a> Record<'a> {
    pub(crate) fn decode(input: &'a [u8]) -> Result<(Self, usize), RecordError> {
        let header = RecordHeader::decode(input)?;
        let payload_start = size_of::<RecordHeader>();
        let payload_end =
            payload_start
                .checked_add(header.length())
                .ok_or(RecordError::PayloadTruncated {
                    declared: header.length(),
                    available: input.len().saturating_sub(payload_start),
                })?;
        let payload =
            input
                .get(payload_start..payload_end)
                .ok_or(RecordError::PayloadTruncated {
                    declared: header.length(),
                    available: input.len().saturating_sub(payload_start),
                })?;
        Ok((Self { header, payload }, payload_end))
    }

    #[inline(always)]
    pub(crate) fn content_type(self) -> ContentType {
        self.header.content_type()
    }

    #[inline(always)]
    pub(crate) fn payload(self) -> &'a [u8] {
        self.payload
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum RecordError {
    #[error("TLS record header is truncated: received {input_len} bytes")]
    HeaderTruncated { input_len: usize },
    #[error("TLS record header output is too small: received {output_len} bytes")]
    OutputTooSmall { output_len: usize },
    #[error("unsupported TLS record content type {content_type}")]
    ContentType { content_type: u8 },
    #[error("TLS 1.3 record uses invalid legacy version {version:02x?}")]
    LegacyVersion { version: [u8; 2] },
    #[error("TLS plaintext length {length} exceeds 16384 bytes")]
    PlaintextLength { length: usize },
    #[error(
        "TLS record payload is truncated: declared {declared} bytes, received {available} bytes"
    )]
    PayloadTruncated { declared: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls13_record_header_matches_rfc_wire_bytes() {
        let header = RecordHeader::new(ContentType::Handshake, 512).expect("valid record length");
        let mut output = [0u8; 5];

        header.encode(&mut output).expect("record header output");

        assert_eq!(output, [22, 0x03, 0x03, 0x02, 0x00]);
        assert_eq!(RecordHeader::decode(&output), Ok(header));
    }

    #[test]
    fn record_borrows_exact_payload_and_reports_consumed_bytes() {
        let input = [22, 0x03, 0x03, 0, 4, 1, 0, 0, 0, 0xaa, 0xbb];

        let (record, consumed) = Record::decode(&input).expect("complete TLS record");

        assert_eq!(record.content_type(), ContentType::Handshake);
        assert_eq!(record.payload(), &[1, 0, 0, 0]);
        assert_eq!(record.payload().as_ptr(), input[5..].as_ptr());
        assert_eq!(consumed, 9);
    }
}
