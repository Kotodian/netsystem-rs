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
        FieldSectionValidator {
            kind,
            seen_regular: false,
            seen: Seen::default(),
        }
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
                return Err(MessageError::PseudoAfterRegular(Bytes::copy_from_slice(
                    name,
                )));
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
                    (Some(a), Some(h)) if a != h => {
                        return Err(MessageError::ContradictedAuthority);
                    }
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
        if !name
            .iter()
            .all(|c| c.is_ascii_lowercase() || is_token_char(*c))
        {
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
        let expected_kind = if name == b":status" {
            MessageKind::Response
        } else {
            MessageKind::Request
        };
        if self.kind != expected_kind {
            return Err(MessageError::WrongKind(
                Bytes::copy_from_slice(name),
                self.kind,
            ));
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
    value
        .iter()
        .fold(0u16, |acc, c| acc * 10 + u16::from(c - b'0'))
}

/// RFC 9110 field-name tokens (tchar), excluding letters: the caller accepts
/// lowercase letters via `is_ascii_lowercase` and rejects uppercase ones.
fn is_token_char(c: u8) -> bool {
    matches!(
        c,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
            | b'0'..=b'9'
    )
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
