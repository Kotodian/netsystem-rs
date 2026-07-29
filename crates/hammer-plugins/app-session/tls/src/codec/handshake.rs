use core::mem::{size_of, transmute};

use thiserror::Error;

const MAX_HANDSHAKE_LEN: usize = 0x00ff_ffff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum HandshakeType {
    ClientHello = 1,
    ServerHello = 2,
    NewSessionTicket = 4,
    EncryptedExtensions = 8,
    Certificate = 11,
    CertificateRequest = 13,
    CertificateVerify = 15,
    Finished = 20,
    KeyUpdate = 24,
    MessageHash = 254,
}

impl TryFrom<u8> for HandshakeType {
    type Error = HandshakeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::ClientHello),
            2 => Ok(Self::ServerHello),
            4 => Ok(Self::NewSessionTicket),
            8 => Ok(Self::EncryptedExtensions),
            11 => Ok(Self::Certificate),
            13 => Ok(Self::CertificateRequest),
            15 => Ok(Self::CertificateVerify),
            20 => Ok(Self::Finished),
            24 => Ok(Self::KeyUpdate),
            254 => Ok(Self::MessageHash),
            handshake_type => Err(HandshakeError::Type { handshake_type }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C, packed)]
pub(crate) struct HandshakeHeader {
    handshake_type: u8,
    length: [u8; 3],
}

const _: () = assert!(size_of::<HandshakeHeader>() == 4);

impl HandshakeHeader {
    pub(crate) fn new(
        handshake_type: HandshakeType,
        length: usize,
    ) -> Result<Self, HandshakeError> {
        if length > MAX_HANDSHAKE_LEN {
            return Err(HandshakeError::Length { length });
        }
        let length = u32::try_from(length)
            .expect("TLS handshake length fits in u32")
            .to_be_bytes();
        Ok(Self {
            handshake_type: handshake_type as u8,
            length: [length[1], length[2], length[3]],
        })
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<(), HandshakeError> {
        let output_len = output.len();
        let bytes = output
            .get_mut(..size_of::<Self>())
            .ok_or(HandshakeError::OutputTooSmall { output_len })?;
        // SAFETY: `bytes` covers a complete packed `HandshakeHeader`. The raw
        // output pointer may be unaligned and is therefore only written unaligned.
        let header = unsafe { transmute::<_, *mut Self>(bytes.as_mut_ptr()) };
        // SAFETY: the range check above proves that `header` points to a full
        // writable `HandshakeHeader`; unaligned access is intentional.
        unsafe { header.write_unaligned(self) };
        Ok(())
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, HandshakeError> {
        let input_len = input.len();
        let bytes = input
            .get(..size_of::<Self>())
            .ok_or(HandshakeError::HeaderTruncated { input_len })?;
        // SAFETY: `bytes` contains a complete packed `HandshakeHeader`. The raw
        // pointer may be unaligned and is therefore only read unaligned.
        let header = unsafe { transmute::<_, *const Self>(bytes.as_ptr()) };
        // SAFETY: the range check above proves that `header` points to a full
        // initialized `HandshakeHeader`; unaligned access is intentional.
        let header = unsafe { header.read_unaligned() };
        HandshakeType::try_from(header.handshake_type)?;
        Ok(header)
    }

    #[inline(always)]
    fn handshake_type(self) -> HandshakeType {
        HandshakeType::try_from(self.handshake_type)
            .expect("validated TLS handshake header retains a known message type")
    }

    #[inline(always)]
    fn length(self) -> usize {
        usize::try_from(u32::from_be_bytes([
            0,
            self.length[0],
            self.length[1],
            self.length[2],
        ]))
        .expect("u24 TLS handshake length fits in usize")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HandshakeMessage<'a> {
    header: HandshakeHeader,
    body: &'a [u8],
}

impl<'a> HandshakeMessage<'a> {
    pub(crate) fn decode(input: &'a [u8]) -> Result<(Self, usize), HandshakeError> {
        let header = HandshakeHeader::decode(input)?;
        let body_start = size_of::<HandshakeHeader>();
        let body_end =
            body_start
                .checked_add(header.length())
                .ok_or(HandshakeError::BodyTruncated {
                    declared: header.length(),
                    available: input.len().saturating_sub(body_start),
                })?;
        let body = input
            .get(body_start..body_end)
            .ok_or(HandshakeError::BodyTruncated {
                declared: header.length(),
                available: input.len().saturating_sub(body_start),
            })?;
        Ok((Self { header, body }, body_end))
    }

    #[inline(always)]
    pub(crate) fn handshake_type(self) -> HandshakeType {
        self.header.handshake_type()
    }

    #[inline(always)]
    pub(crate) fn body(self) -> &'a [u8] {
        self.body
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum HandshakeError {
    #[error("TLS handshake header is truncated: received {input_len} bytes")]
    HeaderTruncated { input_len: usize },
    #[error("TLS handshake header output is too small: received {output_len} bytes")]
    OutputTooSmall { output_len: usize },
    #[error("unsupported TLS handshake type {handshake_type}")]
    Type { handshake_type: u8 },
    #[error("TLS handshake length {length} exceeds the u24 wire limit")]
    Length { length: usize },
    #[error(
        "TLS handshake body is truncated: declared {declared} bytes, received {available} bytes"
    )]
    BodyTruncated { declared: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finished_header_reads_and_writes_u24_length() {
        let header = HandshakeHeader::new(HandshakeType::Finished, 0x01_0203)
            .expect("valid handshake length");
        let mut output = [0u8; 4];

        header.encode(&mut output).expect("handshake header output");

        assert_eq!(output, [20, 0x01, 0x02, 0x03]);
        assert_eq!(HandshakeHeader::decode(&output), Ok(header));
    }

    #[test]
    fn message_borrows_exact_body_and_reports_consumed_bytes() {
        let input = [20, 0, 0, 3, 0xaa, 0xbb, 0xcc, 0xdd];

        let (message, consumed) = HandshakeMessage::decode(&input).expect("complete handshake");

        assert_eq!(message.handshake_type(), HandshakeType::Finished);
        assert_eq!(message.body(), &[0xaa, 0xbb, 0xcc]);
        assert_eq!(message.body().as_ptr(), input[4..].as_ptr());
        assert_eq!(consumed, 7);
    }
}
