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
    ComponentMetadata, OutboundComponent, OutboundManager as _, ProbeProtocol,
    ProbeProtocolComponent, ProbeReport, RuntimeComponent,
};
use hammer_core::error::{HammerError, HammerResult};

use crate::OutboundManager;
use crate::component_registry::register_components;

pub(crate) type ProbeProtocolBuilder = fn() -> ProbeProtocolComponent;

/// Stateless orchestrator that runs a probe against one or every
/// registered outbound. Lives next to the other runtime managers
/// (network, dns, outbound) but is **not** registered with
/// `LIFECYCLE_ORDER` — there is no background task to start or stop.
pub struct ProbeManager {
    outbound: Arc<OutboundManager>,
}

pub struct ProbeProtocolRegistration(ProbeProtocolComponent);

impl From<ProbeProtocolComponent> for ProbeProtocolRegistration {
    fn from(probe: ProbeProtocolComponent) -> Self {
        Self(probe)
    }
}

impl<T> From<Arc<T>> for ProbeProtocolRegistration
where
    T: ProbeProtocol + ComponentMetadata + 'static,
{
    fn from(probe: Arc<T>) -> Self {
        let meta = ComponentMetadata::component_meta(probe.as_ref());
        let runtime: Arc<dyn ProbeProtocol> = probe;
        Self(RuntimeComponent::new(meta, runtime))
    }
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
        probe: impl Into<ProbeProtocolRegistration>,
        timeout: Duration,
    ) -> HammerResult<ProbeReport> {
        let probe = probe.into().0;
        let outbound = self
            .outbound
            .get(outbound_id)
            .ok_or_else(|| HammerError::internal(format!("outbound not found: {outbound_id}")))?;
        let protocol = probe.meta().type_name().to_owned();
        let result = probe.runtime().measure(&outbound, timeout).await;
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
        probe: impl Into<ProbeProtocolRegistration>,
        timeout: Duration,
    ) -> Vec<ProbeReport> {
        let probe = probe.into().0;
        let outbounds = self.outbound.list();
        if outbounds.is_empty() {
            return Vec::new();
        }
        let protocol = probe.meta().type_name().to_owned();
        let mut handles = Vec::with_capacity(outbounds.len());
        for outbound in outbounds {
            let probe = probe.clone();
            let outbound_id = outbound.meta().id().to_owned();
            let protocol = protocol.clone();
            handles.push(crate::spawn::spawn(async move {
                let result = probe.runtime().measure(&outbound, timeout).await;
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

#[derive(Clone)]
pub struct ProbeProtocolFactorySet {
    builders: Arc<std::collections::HashMap<&'static str, ProbeProtocolBuilder>>,
}

impl ProbeProtocolFactorySet {
    pub fn standard() -> Self {
        let mut builders = std::collections::HashMap::new();
        register_components!(probe, &mut builders, [IcmpOutboundProbe]);
        Self {
            builders: Arc::new(builders),
        }
    }

    pub fn build(&self, protocol: &str) -> HammerResult<ProbeProtocolComponent> {
        let builder = self
            .builders
            .get(protocol)
            .ok_or_else(|| HammerError::internal(format!("unknown probe protocol: {protocol}")))?;
        Ok(builder())
    }
}

/// Probe that measures latency to each outbound's own ICMP probe endpoint.
#[hammer_component_macros::hammer_component(probe, name = "icmp", builder = build_icmp_probe)]
#[derive(Default)]
pub struct IcmpOutboundProbe;

impl IcmpOutboundProbe {
    pub fn new() -> Self {
        Self
    }
}

fn build_icmp_probe() -> Arc<IcmpOutboundProbe> {
    Arc::new(IcmpOutboundProbe::new())
}

#[async_trait]
impl ProbeProtocol for IcmpOutboundProbe {
    async fn measure(
        &self,
        outbound: &OutboundComponent,
        timeout: Duration,
    ) -> HammerResult<Duration> {
        outbound.runtime().probe_latency("icmp", timeout).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use hammer_adapter::{
        ComponentMeta, Network, Outbound, ProxyPacketConn, ProxyStream, RuntimeComponent, SocksAddr,
    };
    use hammer_core::error::{HammerError, HammerResult};

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
        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> HammerResult<Box<dyn ProxyStream>> {
            Err(HammerError::internal("not used"))
        }

        async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
            Err(HammerError::internal("not used"))
        }

        async fn probe_latency(&self, protocol: &str, timeout: Duration) -> HammerResult<Duration> {
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
        let erased: Arc<dyn Outbound> = Arc::clone(&outbound) as Arc<dyn Outbound>;
        let outbound_component = RuntimeComponent::new(
            ComponentMeta::new(
                "outbound",
                "probe-only",
                "probe-only",
                Vec::new(),
                Vec::new(),
                None,
            ),
            erased,
        );
        let probe = IcmpOutboundProbe::new();

        let elapsed = probe
            .measure(&outbound_component, Duration::from_millis(250))
            .await
            .expect("icmp probe should delegate");

        assert_eq!(elapsed, Duration::from_millis(7));
        assert_eq!(
            outbound.calls(),
            vec![("icmp".to_owned(), Duration::from_millis(250))]
        );
    }
}
