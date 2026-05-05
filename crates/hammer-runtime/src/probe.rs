//! Outbound-level latency probe runtime glue.
//!
//! `ProbeManager` is a thin orchestrator that picks outbounds out of
//! the `OutboundManager` and runs a [`ProbeProtocol`] against each. It
//! holds no state of its own — every call re-runs the probe — so the
//! manager safely composes with the existing service lifecycle without
//! adding a background task.
//!
//! V1 ships exactly one concrete probe protocol, [`TcpConnectProbe`],
//! which measures the time `outbound.dial(Network::Tcp, target, &[])`
//! takes to return a connected stream. ICMP / HTTP / QUIC variants
//! plug in by adding new `ProbeProtocol` implementations under the
//! same trait surface.
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hammer_adapter::{
    Network, Outbound, OutboundManager as OutboundManagerTrait, ProbeProtocol, ProbeReport,
    SocksAddr,
};
use hammer_core::error::HammerError;
use tokio::time::timeout;

use crate::OutboundManager;

/// Stateless orchestrator that runs a probe against one or every
/// registered outbound. Lives next to the other runtime managers
/// (network, dns, outbound) but is **not** registered with
/// `LIFECYCLE_ORDER` — there is no background task to start or stop.
pub struct ProbeManager {
    outbound: Arc<OutboundManager>,
}

impl ProbeManager {
    pub fn new(outbound: Arc<OutboundManager>) -> Self {
        Self { outbound }
    }

    /// Run `probe` once against `outbound_id`. Returns `Err` only when
    /// the outbound id is unknown; transport-level probe failures are
    /// surfaced inside the returned [`ProbeReport`]'s `result`.
    pub async fn probe(
        &self,
        outbound_id: &str,
        probe: Arc<dyn ProbeProtocol>,
        timeout: Duration,
    ) -> Result<ProbeReport, HammerError> {
        let outbound = self
            .outbound
            .get(outbound_id)
            .ok_or_else(|| HammerError::internal(format!("outbound not found: {outbound_id}")))?;
        let protocol = probe.name().to_owned();
        let result = probe.measure(&outbound, timeout).await;
        Ok(ProbeReport {
            outbound_id: outbound_id.to_owned(),
            protocol,
            result,
        })
    }

    /// Run `probe` concurrently against every registered outbound.
    /// Order of returned reports matches `OutboundManager::list()`.
    /// `timeout` applies per outbound, not to the batch as a whole.
    pub async fn probe_all(
        &self,
        probe: Arc<dyn ProbeProtocol>,
        timeout: Duration,
    ) -> Vec<ProbeReport> {
        let outbounds = self.outbound.list();
        if outbounds.is_empty() {
            return Vec::new();
        }
        let protocol = probe.name().to_owned();
        let mut handles = Vec::with_capacity(outbounds.len());
        for outbound in outbounds {
            let probe = Arc::clone(&probe);
            let outbound_id = outbound.id().to_owned();
            let protocol = protocol.clone();
            handles.push(crate::spawn::spawn(async move {
                let result = probe.measure(&outbound, timeout).await;
                ProbeReport {
                    outbound_id,
                    protocol,
                    result,
                }
            }));
        }
        let mut reports = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(report) => reports.push(report),
                Err(err) => reports.push(ProbeReport {
                    outbound_id: String::new(),
                    protocol: protocol.clone(),
                    result: Err(HammerError::internal(format!("probe task panicked: {err}"))),
                }),
            }
        }
        reports
    }
}

/// V1 probe: open a TCP connection through `outbound` and measure the
/// time until `dial(Network::Tcp, target, &[])` returns Ok.
///
/// The returned stream is dropped immediately; we only care about the
/// underlying handshake / proxy-connect latency. Connection refused,
/// TLS errors, and proxy auth failures all surface as the inner
/// `Err(CoreError)` — useful diagnostic for the caller.
pub struct TcpConnectProbe {
    pub target: SocksAddr,
}

impl TcpConnectProbe {
    pub fn new(target: SocksAddr) -> Self {
        Self { target }
    }
}

#[async_trait]
impl ProbeProtocol for TcpConnectProbe {
    fn name(&self) -> &'static str {
        "tcp"
    }

    async fn measure(
        &self,
        outbound: &Arc<dyn Outbound>,
        deadline: Duration,
    ) -> Result<Duration, HammerError> {
        let started = Instant::now();
        let fut = outbound.dial(Network::Tcp, self.target.clone(), &[]);
        match timeout(deadline, fut).await {
            Ok(Ok(_stream)) => Ok(started.elapsed()),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(HammerError::internal(format!(
                "tcp probe timed out after {deadline:?}"
            ))),
        }
    }
}
