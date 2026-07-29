mod client;
mod server;

use core::mem::size_of;

use thiserror::Error;

use super::extension::ExtensionError;

pub(crate) use client::ClientHello;
pub(crate) use server::ServerHello;

const LEGACY_HELLO_VERSION: [u8; 2] = [0x03, 0x03];

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct HelloPrefix {
    legacy_version: [u8; 2],
    random: [u8; 32],
    session_id_length: u8,
}

const _: () = assert!(size_of::<HelloPrefix>() == 35);

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct ExtensionsLength {
    length: [u8; 2],
}

const _: () = assert!(size_of::<ExtensionsLength>() == 2);

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum HelloError {
    #[error("TLS ClientHello fixed prefix is truncated: received {available} bytes")]
    ClientPrefixTruncated { available: usize },
    #[error("TLS ServerHello fixed prefix is truncated: received {available} bytes")]
    ServerPrefixTruncated { available: usize },
    #[error("TLS ClientHello session id is truncated: declared {declared}, received {available}")]
    ClientSessionIdTruncated { declared: usize, available: usize },
    #[error("TLS ServerHello session id is truncated: declared {declared}, received {available}")]
    ServerSessionIdTruncated { declared: usize, available: usize },
    #[error("TLS cipher suites length is truncated")]
    CipherSuitesLengthTruncated,
    #[error("TLS cipher suites length {length} must be nonzero, even, and fit u16")]
    CipherSuitesLength { length: usize },
    #[error("TLS cipher suites are truncated: declared {declared}, received {available}")]
    CipherSuitesTruncated { declared: usize, available: usize },
    #[error("TLS compression methods length is truncated")]
    CompressionMethodsLengthTruncated,
    #[error("TLS compression methods list is empty")]
    CompressionMethodsEmpty,
    #[error("TLS compression methods length {length} does not fit u8")]
    CompressionMethodsLength { length: usize },
    #[error("TLS compression methods are truncated: declared {declared}, received {available}")]
    CompressionMethodsTruncated { declared: usize, available: usize },
    #[error("TLS 1.3 ClientHello must offer only the null compression method")]
    CompressionMethods,
    #[error("TLS extensions length is truncated")]
    ExtensionsLengthTruncated,
    #[error("TLS extensions length {length} does not fit u16")]
    ExtensionsLength { length: usize },
    #[error("TLS extensions are truncated: declared {declared}, received {available}")]
    ExtensionsTruncated { declared: usize, available: usize },
    #[error("TLS hello extension framing failed")]
    Extension {
        #[source]
        source: ExtensionError,
    },
    #[error("TLS hello has {trailing} trailing bytes")]
    TrailingData { trailing: usize },
    #[error("TLS 1.3 hello uses invalid legacy version {version:02x?}")]
    LegacyVersion { version: [u8; 2] },
    #[error("TLS 1.3 ServerHello selected unsupported cipher suite {cipher_suite:02x?}")]
    CipherSuite { cipher_suite: [u8; 2] },
    #[error("TLS 1.3 ServerHello selected compression method {compression_method}")]
    CompressionMethod { compression_method: u8 },
    #[error("TLS session id length {length} does not fit u8")]
    SessionIdLength { length: usize },
    #[error("TLS hello output requires {required} bytes, received {available}")]
    OutputTooSmall { required: usize, available: usize },
    #[error("TLS ServerHello suffix is truncated")]
    ServerSuffixTruncated,
}
