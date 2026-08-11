//! HTTP message validation of decoded field sections (RFC 9114 Sections 4.2,
//! 4.3): field names and values, pseudo-header placement and duplicates, and
//! the required fields of requests and responses.
//!
//! Wire-level port of the validation in `third_party/h3/h3/src/proto/headers.rs`
//! (`Field::parse`) without the `http` crate, plus the ordering and
//! duplicate rules of RFC 9114 Section 4.3.

use bytes::Bytes;

use super::error::ErrorCode;

/// Whether the field section belongs to a request or a response; determines
/// which pseudo-headers are defined and required.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MessageKind {
    Request,
    Response,
}

/// Streaming validator for one decoded field section. The caller owns the
/// validator and feeds it each decoded field line in order; `finish` verifies
/// the required pseudo-headers and the authority/Host consistency rule
/// (RFC 9114 Section 4.3.1).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FieldSectionValidator {
    kind: MessageKind,
    seen_regular: bool,
    seen: Seen,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
struct Seen {
    method: bool,
    scheme: bool,
    path: bool,
    status: bool,
    protocol: bool,
    /// `:method` value was `CONNECT`; plain CONNECT carries no `:scheme` or
    /// `:path` (RFC 9114 Section 4.4), unlike extended CONNECT.
    connect: bool,
    authority_seen: bool,
    /// `:authority` value, if seen.
    authority: Option<Bytes>,
    /// `Host` field value, if seen.
    host: Option<Bytes>,
}

impl FieldSectionValidator {
    pub fn new(kind: MessageKind) -> FieldSectionValidator {
        FieldSectionValidator { kind, seen_regular: false, seen: Seen::default() }
    }

    /// Validate one field line. Field names and values are checked per
    /// RFC 9114 Sections 4.2 and 10.3, pseudo-headers against the message
    /// kind, and pseudo-after-regular ordering is rejected.
    pub fn on_field(&mut self, name: &[u8], value: &[u8]) -> Result<(), MessageError> {
        if name.is_empty() {
            return Err(MessageError::EmptyName);
        }
        if name[0] == b':' {
            if self.seen_regular {
                return Err(MessageError::PseudoAfterRegular(Bytes::copy_from_slice(name)));
            }
            self.on_pseudo(name, value)
        } else {
            self.seen_regular = true;
            self.on_regular(name, value)
        }
    }

    /// Verify the field section is a complete, well-formed message: requests
    /// need exactly one of each of :method, :scheme, :path and either
    /// :authority or Host (with matching values when both are present);
    /// responses need :status.
    ///
    /// Plain CONNECT (RFC 9114 Section 4.4) is exempt from :scheme and :path;
    /// extended CONNECT (:protocol, RFC 9220) requires them again, mirroring
    /// VPP http3.c.
    pub fn finish(&self) -> Result<(), MessageError> {
        match self.kind {
            MessageKind::Request => {
                if !self.seen.method {
                    return Err(MessageError::MissingMethod);
                }
                // Plain CONNECT is the only request kind without :scheme and
                // :path; extended CONNECT (:protocol) requires them again.
                let needs_scheme_path = !self.seen.connect || self.seen.protocol;
                if !self.seen.scheme && needs_scheme_path {
                    return Err(MessageError::MissingScheme);
                }
                if !self.seen.path && needs_scheme_path {
                    return Err(MessageError::MissingPath);
                }
                match (&self.seen.authority, &self.seen.host) {
                    (None, None) => return Err(MessageError::MissingAuthority),
                    (Some(a), Some(h)) if a != h => return Err(MessageError::ContradictedAuthority),
                    _ => {}
                }
            }
            MessageKind::Response => {
                if !self.seen.status {
                    return Err(MessageError::MissingStatus);
                }
            }
        }
        Ok(())
    }

    fn on_regular(&mut self, name: &[u8], value: &[u8]) -> Result<(), MessageError> {
        if !name.iter().all(|c| c.is_ascii_lowercase() || is_token_char(*c)) {
            return Err(MessageError::InvalidFieldName(Bytes::copy_from_slice(name)));
        }
        if !value.iter().all(is_valid_value_byte) {
            return Err(MessageError::InvalidFieldValue(
                Bytes::copy_from_slice(name),
                Bytes::copy_from_slice(value),
            ));
        }
        if name == b"host" {
            if value.is_empty() {
                // RFC 9114 Section 4.3.1: Host, if present, MUST NOT be empty.
                return Err(MessageError::EmptyHost);
            }
            self.seen.host = Some(Bytes::copy_from_slice(value));
        }
        Ok(())
    }

    fn on_pseudo(&mut self, name: &[u8], value: &[u8]) -> Result<(), MessageError> {
        if !value.iter().all(is_valid_value_byte) {
            return Err(MessageError::InvalidFieldValue(
                Bytes::copy_from_slice(name),
                Bytes::copy_from_slice(value),
            ));
        }
        if value.is_empty() {
            return Err(MessageError::EmptyPseudo(Bytes::copy_from_slice(name)));
        }
        let slot = match name {
            b":method" => &mut self.seen.method,
            b":scheme" => &mut self.seen.scheme,
            b":path" => &mut self.seen.path,
            b":authority" => &mut self.seen.authority_seen,
            b":status" => &mut self.seen.status,
            // RFC 8441 :protocol is defined for CONNECT requests (h3 accepts it)
            b":protocol" => &mut self.seen.protocol,
            _ => return Err(MessageError::UnknownPseudo(Bytes::copy_from_slice(name))),
        };
        let expected_kind = if name == b":status" { MessageKind::Response } else { MessageKind::Request };
        if self.kind != expected_kind {
            return Err(MessageError::WrongKind(Bytes::copy_from_slice(name), self.kind));
        }
        if *slot {
            return Err(MessageError::DuplicatePseudo(Bytes::copy_from_slice(name)));
        }
        *slot = true;
        if name == b":method" && value == b"CONNECT" {
            self.seen.connect = true;
        }
        if name == b":status" {
            let valid = value.len() == 3
                && value.iter().all(u8::is_ascii_digit)
                && (100..=599).contains(&status_code(value));
            if !valid {
                return Err(MessageError::InvalidStatus(Bytes::copy_from_slice(value)));
            }
        }
        if name == b":authority" {
            self.seen.authority = Some(Bytes::copy_from_slice(value));
        }
        Ok(())
    }
}

fn status_code(value: &[u8]) -> u16 {
    value.iter().fold(0u16, |acc, c| acc * 10 + u16::from(c - b'0'))
}

/// RFC 9110 field-name tokens (tchar), excluding letters: the caller accepts
/// lowercase letters via `is_ascii_lowercase` and rejects uppercase ones.
fn is_token_char(c: u8) -> bool {
    matches!(c, b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~' | b'0'..=b'9')
}

/// A byte permitted in a field value: HTAB, SP, VCHAR, or obs-text
/// (RFC 9110 Section 5.5; RFC 9114 Section 10.3).
fn is_valid_value_byte(c: &u8) -> bool {
    matches!(c, 0x09 | 0x20..=0x7e | 0x80..=0xff)
}

/// Errors that make a decoded field section a malformed message; every one
/// maps to H3_MESSAGE_ERROR (RFC 9114 Section 4.1.2).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MessageError {
    /// The field name is empty.
    EmptyName,
    /// A regular field name contains uppercase or non-token characters.
    InvalidFieldName(Bytes),
    /// A field value contains bytes that are not permitted.
    InvalidFieldValue(Bytes, Bytes),
    /// A pseudo-header not defined by RFC 9114/RFC 8441.
    UnknownPseudo(Bytes),
    /// A pseudo-header with an empty value.
    EmptyPseudo(Bytes),
    /// `:status` is not a three-digit code in 100..=599.
    InvalidStatus(Bytes),
    /// A pseudo-header appeared more than once.
    DuplicatePseudo(Bytes),
    /// A pseudo-header appeared after a regular field.
    PseudoAfterRegular(Bytes),
    /// A pseudo-header defined for the other message kind.
    WrongKind(Bytes, MessageKind),
    /// A request without `:method`.
    MissingMethod,
    /// A request without `:scheme`.
    MissingScheme,
    /// A request without `:path`.
    MissingPath,
    /// A response without `:status`.
    MissingStatus,
    /// A request without either `:authority` or `Host`.
    MissingAuthority,
    /// `:authority` and `Host` disagree.
    ContradictedAuthority,
    /// A `Host` field with an empty value.
    EmptyHost,
}

impl MessageError {
    /// The connection error code to send: all malformed messages use
    /// H3_MESSAGE_ERROR.
    pub const fn error_code(&self) -> ErrorCode {
        ErrorCode::MessageError
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http3::proto::error::ErrorCode;

    fn req() -> FieldSectionValidator {
        FieldSectionValidator::new(MessageKind::Request)
    }

    fn resp() -> FieldSectionValidator {
        FieldSectionValidator::new(MessageKind::Response)
    }

    fn valid_request() -> FieldSectionValidator {
        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        v.on_field(b":scheme", b"https").unwrap();
        v.on_field(b":authority", b"example.com").unwrap();
        v.on_field(b":path", b"/").unwrap();
        v
    }

    #[test]
    fn valid_request_is_ok() {
        let mut v = valid_request();
        v.on_field(b"content-length", b"0").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn request_has_no_authority_nor_host() {
        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        v.on_field(b":scheme", b"https").unwrap();
        v.on_field(b":path", b"/").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingAuthority));
    }

    #[test]
    fn host_stands_in_for_authority() {
        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        v.on_field(b":scheme", b"https").unwrap();
        v.on_field(b":path", b"/").unwrap();
        v.on_field(b"host", b"example.com").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn contradicted_authority() {
        let mut v = valid_request();
        v.on_field(b"host", b"other.example").unwrap();
        assert_eq!(v.finish(), Err(MessageError::ContradictedAuthority));

        let mut v = valid_request();
        v.on_field(b"host", b"example.com").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn missing_required_request_pseudos() {
        let mut v = req();
        v.on_field(b":scheme", b"https").unwrap();
        v.on_field(b":path", b"/").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingMethod));

        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        v.on_field(b":path", b"/").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingScheme));

        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        v.on_field(b":scheme", b"https").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingPath));
    }

    #[test]
    fn response_requires_status() {
        let mut v = resp();
        v.on_field(b"content-type", b"text/plain").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingStatus));

        let mut v = resp();
        v.on_field(b":status", b"200").unwrap();
        v.on_field(b"content-type", b"text/plain").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn uppercase_field_name_is_malformed() {
        let mut v = valid_request();
        assert!(matches!(
            v.on_field(b"Content-Length", b"0"),
            Err(MessageError::InvalidFieldName(_))
        ));
    }

    #[test]
    fn invalid_field_name_chars_are_malformed() {
        let mut v = valid_request();
        assert!(matches!(
            v.on_field(b"foo bar", b"0"),
            Err(MessageError::InvalidFieldName(_))
        ));
        let mut v = valid_request();
        assert_eq!(v.on_field(b"", b"0"), Err(MessageError::EmptyName));
    }

    #[test]
    fn invalid_field_value_bytes_are_malformed() {
        // NUL and LF are not permitted in field values
        let mut v = valid_request();
        assert!(matches!(
            v.on_field(b"foo", b"a\x00b"),
            Err(MessageError::InvalidFieldValue(_, _))
        ));
        let mut v = valid_request();
        assert!(matches!(
            v.on_field(b"foo", b"a\nb"),
            Err(MessageError::InvalidFieldValue(_, _))
        ));
        // obs-text and HTAB are permitted
        let mut v = valid_request();
        assert!(v.on_field(b"foo", b"a\xff\tb").is_ok());
    }

    #[test]
    fn unknown_pseudo_is_malformed() {
        let mut v = valid_request();
        assert!(matches!(
            v.on_field(b":bogus", b"x"),
            Err(MessageError::UnknownPseudo(_))
        ));
    }

    #[test]
    fn pseudo_after_regular_field_is_malformed() {
        let mut v = valid_request();
        v.on_field(b"content-length", b"0").unwrap();
        assert!(matches!(
            v.on_field(b":path", b"/"),
            Err(MessageError::PseudoAfterRegular(_))
        ));
    }

    #[test]
    fn duplicate_pseudo_is_malformed() {
        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        assert!(matches!(
            v.on_field(b":method", b"POST"),
            Err(MessageError::DuplicatePseudo(_))
        ));
    }

    #[test]
    fn pseudo_of_wrong_message_kind_is_malformed() {
        let mut v = req();
        assert!(matches!(
            v.on_field(b":status", b"200"),
            Err(MessageError::WrongKind(_, MessageKind::Request))
        ));
        let mut v = resp();
        assert!(matches!(
            v.on_field(b":method", b"GET"),
            Err(MessageError::WrongKind(_, MessageKind::Response))
        ));
    }

    #[test]
    fn empty_pseudo_value_is_malformed() {
        let mut v = valid_request();
        assert!(matches!(
            v.on_field(b":path", b""),
            Err(MessageError::EmptyPseudo(_))
        ));
        let mut v = resp();
        assert!(matches!(
            v.on_field(b":status", b""),
            Err(MessageError::EmptyPseudo(_))
        ));
    }

    #[test]
    fn invalid_status_value() {
        let mut v = resp();
        assert!(matches!(
            v.on_field(b":status", b"abc"),
            Err(MessageError::InvalidStatus(_))
        ));
        let mut v = resp();
        assert!(matches!(
            v.on_field(b":status", b"99"),
            Err(MessageError::InvalidStatus(_))
        ));
        let mut v = resp();
        assert!(matches!(
            v.on_field(b":status", b"700"),
            Err(MessageError::InvalidStatus(_))
        ));
    }

    #[test]
    fn protocol_pseudo_is_accepted_in_requests() {
        let mut v = valid_request();
        assert!(v.on_field(b":protocol", b"websocket").is_ok());
        assert!(v.finish().is_ok());
    }

    #[test]
    fn connect_request_skips_scheme_and_path() {
        // Plain CONNECT (RFC 9114 Section 4.4) has no :scheme or :path.
        let mut v = req();
        v.on_field(b":method", b"CONNECT").unwrap();
        v.on_field(b":authority", b"example.com:443").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn extended_connect_requires_scheme_and_path() {
        // Extended CONNECT (:protocol, RFC 9220) needs :scheme and :path
        // again, mirroring VPP http3.c (http3_stream_prepare_request).
        let mut v = req();
        v.on_field(b":method", b"CONNECT").unwrap();
        v.on_field(b":authority", b"example.com:443").unwrap();
        v.on_field(b":protocol", b"websocket").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingScheme));

        let mut v = req();
        v.on_field(b":method", b"CONNECT").unwrap();
        v.on_field(b":authority", b"example.com:443").unwrap();
        v.on_field(b":protocol", b"websocket").unwrap();
        v.on_field(b":scheme", b"https").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingPath));

        let mut v = req();
        v.on_field(b":method", b"CONNECT").unwrap();
        v.on_field(b":authority", b"example.com:443").unwrap();
        v.on_field(b":protocol", b"websocket").unwrap();
        v.on_field(b":scheme", b"https").unwrap();
        v.on_field(b":path", b"/").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn connect_still_requires_authority() {
        let mut v = req();
        v.on_field(b":method", b"CONNECT").unwrap();
        assert_eq!(v.finish(), Err(MessageError::MissingAuthority));
    }

    #[test]
    fn empty_host_value_is_malformed() {
        // RFC 9114 Section 4.3.1: an :authority or Host field, if present,
        // MUST NOT be empty.
        let mut v = req();
        v.on_field(b":method", b"GET").unwrap();
        assert_eq!(v.on_field(b"host", b""), Err(MessageError::EmptyHost));
        let mut v = valid_request();
        assert!(v.on_field(b"host", b"example.com").is_ok());
    }

    #[test]
    fn unknown_regular_fields_are_tolerated() {
        let mut v = valid_request();
        v.on_field(b"x-whatever", b"value").unwrap();
        assert!(v.finish().is_ok());
    }

    #[test]
    fn errors_map_to_message_error_code() {
        assert_eq!(MessageError::MissingMethod.error_code(), ErrorCode::MessageError);
        assert_eq!(MessageError::ContradictedAuthority.error_code(), ErrorCode::MessageError);
    }
}
