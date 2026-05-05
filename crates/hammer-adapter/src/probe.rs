//! Outbound-level latency probing.
//!
//! `ProbeProtocol` is the plug-in seam: implementations measure
//! round-trip time to an `Outbound`'s own probe endpoint using
//! whichever transport surface fits. The `ProbeManager` (in the
//! runtime crate) is the orchestrator that fans probes out across
//! every registered outbound.
//!
//! V1 ships an ICMP echo probe for server-backed outbounds. Future
//! HTTP / QUIC probes drop in by adding a new `impl ProbeProtocol`.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hammer_core::error::CoreError;

use crate::outbound::Outbound;

/// One-shot latency probe. Implementations measure round-trip time to
/// `outbound`'s own probe endpoint, returning the elapsed `Duration`
/// on success.
///
/// Implementations must be cheap to clone-via-`Arc` and stateless
/// across calls — the manager may invoke `measure` concurrently for
/// every registered outbound from a single `Arc<dyn ProbeProtocol>`.
#[async_trait]
pub trait ProbeProtocol: Send + Sync + 'static {
    /// Short identifier (`"tcp"`, `"icmp"`, …) used in `ProbeReport`
    /// and FFI surfaces. Must be a stable, machine-friendly slug.
    fn name(&self) -> &'static str;

    /// Run one probe attempt against `outbound`. Implementations must
    /// honour `timeout` themselves (typically by wrapping their I/O in
    /// `tokio::time::timeout`); the manager does not enforce it on
    /// their behalf because the natural cancellation point varies by
    /// transport.
    async fn measure(
        &self,
        outbound: &Arc<dyn Outbound>,
        timeout: Duration,
    ) -> Result<Duration, CoreError>;
}

/// Result of one probe attempt against one outbound.
///
/// `result.is_ok()` means the probe completed within `timeout` and the
/// returned `Duration` is the measured RTT. `Err` carries the error
/// returned by the outbound (timeout, connection refused, unsupported
/// network, …) verbatim, so callers can surface a useful message to
/// the user without losing detail.
#[derive(Debug)]
pub struct ProbeReport {
    pub outbound_id: String,
    pub protocol: String,
    pub result: Result<Duration, CoreError>,
}
