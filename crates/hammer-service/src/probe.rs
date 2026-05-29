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
use hammer_core::error::{HammerError, HammerResult};
use hammer_runtime::OutboundManager;
use hammer_runtime::adapter::{
    ComponentMeta, ComponentMetadata, OutboundComponent, OutboundManager as _, ProbeProtocol,
    ProbeProtocolComponent, ProbeReport, RuntimeComponent,
};

type ProbeProtocolBuilder = fn() -> ProbeProtocolComponent;

/// Stateless orchestrator that prepares probe work from control-plane
/// state. Service code triggers it on the control thread, then moves the
/// returned batch onto the data runtime before probe packet I/O starts.
pub struct ProbeManager {
    outbound: Arc<OutboundManager>,
}

pub struct ProbeProtocolRegistration(ProbeProtocolComponent);

pub struct ProbeBatch {
    protocol: String,
    targets: Vec<ProbeTarget>,
}

struct ProbeTarget {
    outbound_id: String,
    outbound: OutboundComponent,
    probe: ProbeProtocolComponent,
}

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

    pub fn prepare(
        &self,
        outbound_id: &str,
        probe: impl Into<ProbeProtocolRegistration>,
    ) -> HammerResult<ProbeBatch> {
        let probe = probe.into().0;
        let outbound = self
            .outbound
            .get(outbound_id)
            .ok_or_else(|| HammerError::internal(format!("outbound not found: {outbound_id}")))?;
        let protocol = probe.meta().type_name().to_owned();
        Ok(ProbeBatch {
            protocol,
            targets: vec![ProbeTarget {
                outbound_id: outbound_id.to_owned(),
                outbound,
                probe,
            }],
        })
    }

    pub fn prepare_all(&self, probe: impl Into<ProbeProtocolRegistration>) -> ProbeBatch {
        let probe = probe.into().0;
        let outbounds = self.outbound.list();
        let protocol = probe.meta().type_name().to_owned();
        let targets = outbounds
            .into_iter()
            .map(|outbound| ProbeTarget {
                outbound_id: outbound.meta().id().to_owned(),
                outbound,
                probe: probe.clone(),
            })
            .collect();
        ProbeBatch { protocol, targets }
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
        let batch = self.prepare(outbound_id, probe)?;
        let mut reports = batch.run(timeout).await;
        reports
            .pop()
            .ok_or_else(|| HammerError::internal("probe batch returned no reports"))
    }

    /// Run `probe` concurrently against every registered outbound.
    /// Order of returned reports matches `OutboundManager::list()`.
    /// `timeout` applies per outbound, not to the batch as a whole.
    pub async fn probe_all(
        &self,
        probe: impl Into<ProbeProtocolRegistration>,
        timeout: Duration,
    ) -> Vec<ProbeReport> {
        self.prepare_all(probe).run(timeout).await
    }
}

impl ProbeBatch {
    pub async fn run(self, timeout: Duration) -> Vec<ProbeReport> {
        if self.targets.is_empty() {
            return Vec::new();
        }

        let mut handles = Vec::with_capacity(self.targets.len());
        for target in self.targets {
            let protocol = self.protocol.clone();
            let ProbeTarget {
                outbound_id,
                outbound,
                probe,
            } = target;
            handles.push((
                outbound_id.clone(),
                protocol.clone(),
                hammer_runtime::spawn::spawn(async move {
                    let result = probe.runtime().measure(&outbound, timeout).await;
                    ProbeReport {
                        outbound_id,
                        protocol,
                        result,
                    }
                }),
            ));
        }

        let mut reports = Vec::with_capacity(handles.len());
        for (outbound_id, protocol, handle) in handles {
            match handle.await {
                Ok(report) => reports.push(report),
                Err(err) => reports.push(ProbeReport {
                    outbound_id,
                    protocol,
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
        builders.insert("icmp", build_icmp_probe_component as ProbeProtocolBuilder);
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

fn build_icmp_probe_component() -> ProbeProtocolComponent {
    let runtime = build_icmp_probe();
    let meta = runtime.component_meta();
    let runtime: Arc<dyn ProbeProtocol> = runtime;
    RuntimeComponent::new(meta, runtime)
}

impl ComponentMetadata for IcmpOutboundProbe {
    fn component_meta(&self) -> ComponentMeta {
        ComponentMeta::new("probe", "icmp", "icmp", Vec::new(), Vec::new(), None)
    }
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
    use hammer_core::error::{HammerError, HammerResult};
    use hammer_core::log::{DiscardWriter, Factory, Logger};
    use hammer_runtime::adapter::{
        ComponentMeta, ComponentMetadata, Network, Outbound, ProxyPacketConn, ProxyStream,
        RuntimeComponent, SocksAddr,
    };

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

    #[derive(Default)]
    struct DataThreadProbeOutbound {
        threads: Mutex<Vec<String>>,
    }

    impl DataThreadProbeOutbound {
        fn threads(&self) -> Vec<String> {
            self.threads.lock().expect("threads lock").clone()
        }
    }

    impl ComponentMetadata for DataThreadProbeOutbound {
        fn component_meta(&self) -> ComponentMeta {
            ComponentMeta::new(
                "outbound",
                "data-thread-probe",
                "data-thread-probe",
                Vec::new(),
                Vec::new(),
                None,
            )
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

    #[async_trait]
    impl Outbound for DataThreadProbeOutbound {
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

        async fn probe_latency(
            &self,
            _protocol: &str,
            _timeout: Duration,
        ) -> HammerResult<Duration> {
            self.threads
                .lock()
                .expect("threads lock")
                .push(std::thread::current().name().unwrap_or_default().to_owned());
            Ok(Duration::from_millis(5))
        }
    }

    fn test_logger(id: &str) -> Logger {
        Factory::new(std::time::Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
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

    #[test]
    fn probe_all_runs_outbound_probe_on_data_worker() {
        let outbound = Arc::new(DataThreadProbeOutbound::default());
        let manager = Arc::new(OutboundManager::new(
            test_logger("outbound"),
            "data-thread-probe",
        ));
        manager
            .register_outbound(Arc::clone(&outbound))
            .expect("register outbound");
        let probe_manager = ProbeManager::new(manager);
        let data_runtime = hammer_runtime::spawn::DataRuntime::new(1, "probe-data", 512 * 1024, 2)
            .expect("data runtime");
        let context = data_runtime.context();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let reports = context.enter(|| {
            driver.block_on(async {
                probe_manager
                    .probe_all(
                        Arc::new(IcmpOutboundProbe::new()),
                        Duration::from_millis(250),
                    )
                    .await
            })
        });

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outbound_id, "data-thread-probe");
        assert_eq!(outbound.threads(), vec!["probe-data-0".to_owned()]);
        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[test]
    fn prepared_probe_batch_runs_outbound_probe_on_data_worker() {
        let outbound = Arc::new(DataThreadProbeOutbound::default());
        let manager = Arc::new(OutboundManager::new(
            test_logger("outbound"),
            "data-thread-probe",
        ));
        manager
            .register_outbound(Arc::clone(&outbound))
            .expect("register outbound");
        let probe_manager = ProbeManager::new(manager);
        let batch = probe_manager.prepare_all(Arc::new(IcmpOutboundProbe::new()));
        let data_runtime =
            hammer_runtime::spawn::DataRuntime::new(1, "probe-batch-data", 512 * 1024, 2)
                .expect("data runtime");
        let data = data_runtime.executor();
        let driver = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");

        let reports = driver.block_on(async {
            data.execute(async move { batch.run(Duration::from_millis(250)).await })
                .await
                .expect("probe batch task")
        });

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outbound_id, "data-thread-probe");
        assert_eq!(outbound.threads(), vec!["probe-batch-data-0".to_owned()]);
        data_runtime.shutdown_timeout(Duration::from_secs(1));
    }
}
