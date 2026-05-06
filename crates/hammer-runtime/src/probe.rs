//! Outbound-level latency probe runtime glue.
//!
//! `ProbeManager` is a thin orchestrator that picks outbounds out of
//! the `OutboundManager` and runs a [`ProbeProtocol`] against each. It
//! holds no state of its own — every call re-runs the probe — so the
//! manager safely composes with the existing service lifecycle without
//! adding a background task.
//!
//! V1 ships exactly one concrete probe protocol, [`IcmpOutboundProbe`],
//! which asks each outbound to measure latency to its own ICMP probe
//! endpoint. HTTP / QUIC variants plug in by adding new
//! `ProbeProtocol` implementations under the same trait surface.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hammer_adapter::{
    Outbound, OutboundManager as OutboundManagerTrait, ProbeProtocol, ProbeReport,
};
use hammer_core::error::HammerError;

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

/// Probe that measures latency to each outbound's own ICMP probe endpoint.
#[derive(Default)]
pub struct IcmpOutboundProbe;

impl IcmpOutboundProbe {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProbeProtocol for IcmpOutboundProbe {
    fn name(&self) -> &'static str {
        "icmp"
    }

    async fn measure(
        &self,
        outbound: &Arc<dyn Outbound>,
        timeout: Duration,
    ) -> Result<Duration, HammerError> {
        outbound.probe_latency(self.name(), timeout).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use hammer_adapter::{Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
    use hammer_core::error::HammerError;

    use super::*;

    #[derive(Default)]
    struct ProbeOnlyOutbound {
        calls: Mutex<Vec<(String, Duration)>>,
    }

    impl ProbeOnlyOutbound {
        fn calls(&self) -> Vec<(String, Duration)> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl Outbound for ProbeOnlyOutbound {
        fn type_name(&self) -> &str {
            "probe-only"
        }

        fn id(&self) -> &str {
            "probe-only"
        }

        fn networks(&self) -> &[Network] {
            &[]
        }

        fn dependencies(&self) -> &[String] {
            &[]
        }

        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> Result<Box<dyn ProxyStream>, HammerError> {
            Err(HammerError::internal("not used"))
        }

        async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
            Err(HammerError::internal("not used"))
        }

        async fn probe_latency(
            &self,
            protocol: &str,
            timeout: Duration,
        ) -> Result<Duration, HammerError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((protocol.to_owned(), timeout));
            Ok(Duration::from_millis(7))
        }
    }

    #[tokio::test]
    async fn icmp_probe_delegates_to_outbound_probe_latency() {
        let outbound = Arc::new(ProbeOnlyOutbound::default());
        let erased = Arc::clone(&outbound) as Arc<dyn Outbound>;
        let probe = IcmpOutboundProbe::new();

        let elapsed = probe
            .measure(&erased, Duration::from_millis(250))
            .await
            .expect("icmp probe should delegate");

        assert_eq!(elapsed, Duration::from_millis(7));
        assert_eq!(
            outbound.calls(),
            vec![("icmp".to_owned(), Duration::from_millis(250))]
        );
    }
}
