use thiserror::Error;

/// Session-owned error reported when an active open fails before the
/// CONNECTED message is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SessionConnectError {
    #[error("TLS alert {alert}")]
    TlsAlert { alert: u8 },
    #[error("QUIC version is unsupported")]
    QuicVersionUnsupported,
    #[error("QUIC handshake timed out")]
    TimedOut,
    #[error("the peer refused the connection")]
    ConnectionRefused,
    #[error("the peer reset the connection")]
    ConnectionReset,
    #[error("the peer closed the connection with code {code}")]
    PeerClosed { code: u64 },
    #[error("QUIC transport error {code}")]
    QuicTransportError { code: u64 },
    #[error("local QUIC connection resources are exhausted")]
    LocalResourceExhausted,
    #[error("Session control failed: {error}")]
    Control {
        error: super::control::SessionControlError,
    },
    #[error("the local connection closed during the handshake")]
    LocalClosed,
}
