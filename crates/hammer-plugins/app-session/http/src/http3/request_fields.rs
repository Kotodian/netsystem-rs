//! Request field-line validation (RFC 9114 Section 4.2), applied to an
//! already-decoded field section.
//!
//! `proto::headers::FieldSectionValidator` already checks that field names
//! are lowercase tchar and that values are valid bytes; this seam adds the
//! request-only semantics of Section 4.2: a request carrying a
//! connection-specific field (`connection`, `keep-alive`,
//! `proxy-connection`, `transfer-encoding`, or `upgrade`) is malformed,
//! and `te` must have exactly the value `trailers`. VPP http3.c likewise
//! terminates the request stream with `HTTP3_ERROR_MESSAGE_ERROR` when
//! header validation fails.
//!
//! Reference: RFC 9114 Section 4.2; `third_party/vpp/src/plugins/http/
//! http3/http3.c` (`http3_stream_terminate`, `HTTP3_ERROR_MESSAGE_ERROR`).

use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::qpack::field::HeaderField;

/// Validate a single regular field line of a decoded request field section.
///
/// Rejects connection-specific field names and any `te` value other than
/// exactly `trailers` (RFC 9114 Section 4.2); no allocation.
pub(crate) fn validate_request_field_line(
    field: &HeaderField,
) -> Result<(), RequestFieldLineError<'_>> {
    let name = field.name.as_ref();
    if is_connection_specific(name) {
        return Err(RequestFieldLineError::ForbiddenField(name));
    }
    if name == b"te" && field.value.as_ref() != b"trailers" {
        return Err(RequestFieldLineError::InvalidTe(field.value.as_ref()));
    }
    Ok(())
}

/// Validate the regular field lines of a decoded request field section.
///
/// Walks the slice once, delegating to [`validate_request_field_line`], and
/// stops at the first offending field; no allocation on success.
pub(crate) fn validate_request_field_lines(
    fields: &[HeaderField],
) -> Result<(), RequestFieldLineError<'_>> {
    for field in fields {
        validate_request_field_line(field)?;
    }
    Ok(())
}

/// The connection-specific field names a request MUST NOT carry
/// (RFC 9114 Section 4.2).
fn is_connection_specific(name: &[u8]) -> bool {
    matches!(
        name,
        b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding" | b"upgrade"
    )
}

/// A malformed request field line; every variant maps to
/// `ErrorCode::MessageError` (RFC 9114 Section 4.1.2).
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RequestFieldLineError<'a> {
    /// A connection-specific field appeared; carries the field name.
    ForbiddenField(&'a [u8]),
    /// `te` carried a value other than exactly `trailers`; carries the
    /// value.
    InvalidTe(&'a [u8]),
}

impl RequestFieldLineError<'_> {
    /// The connection error code to send: malformed field sections use
    /// H3_MESSAGE_ERROR.
    pub(crate) const fn error_code(&self) -> ErrorCode {
        ErrorCode::MessageError
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> HeaderField {
        HeaderField::new(name, value)
    }

    #[test]
    fn rejects_every_connection_specific_field() {
        let cases = [
            ("connection", "close"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "h2c"),
        ];
        for (name, value) in cases {
            let fields = [field(name, value)];
            let err = validate_request_field_lines(&fields).unwrap_err();
            assert_eq!(
                err,
                RequestFieldLineError::ForbiddenField(name.as_bytes()),
                "field {name}"
            );
            assert_eq!(err.error_code(), ErrorCode::MessageError);
        }
    }

    #[test]
    fn accepts_te_with_value_trailers() {
        let fields = [field("te", "trailers"), field("content-type", "text/plain")];
        assert!(validate_request_field_lines(&fields).is_ok());
    }

    #[test]
    fn rejects_te_with_another_value() {
        let fields = [field("te", "gzip")];
        let err = validate_request_field_lines(&fields).unwrap_err();
        assert_eq!(err, RequestFieldLineError::InvalidTe(b"gzip"));
        assert_eq!(err.error_code(), ErrorCode::MessageError);
    }

    #[test]
    fn accepts_ordinary_fields() {
        let fields = [
            field("content-type", "text/plain"),
            field("accept", "application/json"),
        ];
        assert!(validate_request_field_lines(&fields).is_ok());
    }

    #[test]
    fn singular_accepts_ordinary_field() {
        assert!(validate_request_field_line(&field("content-type", "text/plain")).is_ok());
    }

    #[test]
    fn singular_rejects_connection_specific_field() {
        let field = field("connection", "close");
        let err = validate_request_field_line(&field).unwrap_err();
        assert_eq!(err, RequestFieldLineError::ForbiddenField(b"connection"));
        assert_eq!(err.error_code(), ErrorCode::MessageError);
    }

    #[test]
    fn singular_accepts_te_with_value_trailers() {
        assert!(validate_request_field_line(&field("te", "trailers")).is_ok());
    }

    #[test]
    fn singular_rejects_te_with_another_value() {
        let field = field("te", "gzip");
        let err = validate_request_field_line(&field).unwrap_err();
        assert_eq!(err, RequestFieldLineError::InvalidTe(b"gzip"));
    }
}
