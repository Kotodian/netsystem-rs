use core::mem::{size_of, transmute};

use thiserror::Error;

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct AlertWire {
    level: u8,
    description: u8,
}

const _: () = assert!(size_of::<AlertWire>() == 2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum AlertLevel {
    Warning = 1,
    Fatal = 2,
}

impl TryFrom<u8> for AlertLevel {
    type Error = AlertError;

    fn try_from(level: u8) -> Result<Self, Self::Error> {
        match level {
            1 => Ok(Self::Warning),
            2 => Ok(Self::Fatal),
            level => Err(AlertError::Level { level }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum AlertDescription {
    CloseNotify = 0,
    UnexpectedMessage = 10,
    BadRecordMac = 20,
    RecordOverflow = 22,
    HandshakeFailure = 40,
    BadCertificate = 42,
    UnsupportedCertificate = 43,
    CertificateRevoked = 44,
    CertificateExpired = 45,
    CertificateUnknown = 46,
    IllegalParameter = 47,
    UnknownCa = 48,
    AccessDenied = 49,
    DecodeError = 50,
    DecryptError = 51,
    ProtocolVersion = 70,
    InsufficientSecurity = 71,
    InternalError = 80,
    UserCanceled = 90,
    MissingExtension = 109,
    UnsupportedExtension = 110,
    UnrecognizedName = 112,
    BadCertificateStatusResponse = 113,
    UnknownPskIdentity = 115,
    CertificateRequired = 116,
    NoApplicationProtocol = 120,
}

impl TryFrom<u8> for AlertDescription {
    type Error = AlertError;

    fn try_from(description: u8) -> Result<Self, Self::Error> {
        match description {
            0 => Ok(Self::CloseNotify),
            10 => Ok(Self::UnexpectedMessage),
            20 => Ok(Self::BadRecordMac),
            22 => Ok(Self::RecordOverflow),
            40 => Ok(Self::HandshakeFailure),
            42 => Ok(Self::BadCertificate),
            43 => Ok(Self::UnsupportedCertificate),
            44 => Ok(Self::CertificateRevoked),
            45 => Ok(Self::CertificateExpired),
            46 => Ok(Self::CertificateUnknown),
            47 => Ok(Self::IllegalParameter),
            48 => Ok(Self::UnknownCa),
            49 => Ok(Self::AccessDenied),
            50 => Ok(Self::DecodeError),
            51 => Ok(Self::DecryptError),
            70 => Ok(Self::ProtocolVersion),
            71 => Ok(Self::InsufficientSecurity),
            80 => Ok(Self::InternalError),
            90 => Ok(Self::UserCanceled),
            109 => Ok(Self::MissingExtension),
            110 => Ok(Self::UnsupportedExtension),
            112 => Ok(Self::UnrecognizedName),
            113 => Ok(Self::BadCertificateStatusResponse),
            115 => Ok(Self::UnknownPskIdentity),
            116 => Ok(Self::CertificateRequired),
            120 => Ok(Self::NoApplicationProtocol),
            description => Err(AlertError::Description { description }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Alert {
    pub(crate) level: AlertLevel,
    pub(crate) description: AlertDescription,
}

impl Alert {
    pub(crate) fn decode(input: &[u8]) -> Result<Self, AlertError> {
        if input.len() != size_of::<AlertWire>() {
            return Err(AlertError::Length {
                length: input.len(),
            });
        }
        // SAFETY: the exact `AlertWire` byte range was checked above. TLS
        // payloads may be unaligned, so the packed value is read unaligned.
        let wire = unsafe { transmute::<_, *const AlertWire>(input.as_ptr()).read_unaligned() };
        let alert = Self {
            level: AlertLevel::try_from(wire.level)?,
            description: AlertDescription::try_from(wire.description)?,
        };
        if alert.level == AlertLevel::Warning
            && !matches!(
                alert.description,
                AlertDescription::CloseNotify | AlertDescription::UserCanceled
            )
        {
            return Err(AlertError::Tls13Warning {
                description: alert.description,
            });
        }
        Ok(alert)
    }

    pub(crate) fn encode(self, output: &mut [u8]) -> Result<usize, AlertError> {
        if output.len() < size_of::<AlertWire>() {
            return Err(AlertError::OutputTooSmall {
                available: output.len(),
            });
        }
        let wire = AlertWire {
            level: self.level as u8,
            description: self.description as u8,
        };
        // SAFETY: the output range contains a complete writable `AlertWire`;
        // unaligned access is intentional for FIFO-backed bytes.
        unsafe { transmute::<_, *mut AlertWire>(output.as_mut_ptr()).write_unaligned(wire) };
        Ok(size_of::<AlertWire>())
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum AlertError {
    #[error("TLS alert must contain exactly 2 bytes, received {length}")]
    Length { length: usize },
    #[error("TLS alert level {level} is invalid")]
    Level { level: u8 },
    #[error("TLS alert description {description} is unknown")]
    Description { description: u8 },
    #[error("TLS 1.3 forbids warning alert {description:?}")]
    Tls13Warning { description: AlertDescription },
    #[error("TLS alert output requires 2 bytes, received {available}")]
    OutputTooSmall { available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_decode_error_round_trips_wire_value() {
        let alert = Alert::decode(&[2, 50]).expect("fatal decode_error");
        assert_eq!(alert.level, AlertLevel::Fatal);
        assert_eq!(alert.description, AlertDescription::DecodeError);

        let mut output = [0u8; 2];
        assert_eq!(alert.encode(&mut output), Ok(2));
        assert_eq!(output, [2, 50]);
    }

    #[test]
    fn tls13_rejects_non_close_warning_alert() {
        assert_eq!(
            Alert::decode(&[1, 10]),
            Err(AlertError::Tls13Warning {
                description: AlertDescription::UnexpectedMessage,
            })
        );
    }
}
