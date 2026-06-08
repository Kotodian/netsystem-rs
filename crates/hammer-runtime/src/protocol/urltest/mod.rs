//! `urltest` outbound group — probes child outbounds via HTTP HEAD over the
//! proxied stream and routes traffic through the lowest-latency child.
//!
//! Mirrors sing-box's `protocol/group/urltest.go` design but trimmed for V1:
//! no automatic ticker, no idle-timeout, no `interrupt_exist_connections`.
//! Probe sweeps are triggered explicitly through the FFI surface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use async_trait::async_trait;
use hammer_adapter::{
    Network, Outbound, OutboundComponent, OutboundManager as OutboundManagerTrait, ProbeReport,
    ProxyPacketConn, ProxyStream, SocksAddr,
};
use hammer_core::config::{OutboundKind, UrltestOutboundOptions};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::Logger;

use crate::socket_protector::SocketProtector;

mod probe;

pub use probe::HttpUrltestProbe;

const TCP_AND_UDP: [Network; 2] = [Network::Tcp, Network::Udp];

/// Latency sample for one child outbound.
#[derive(Clone, Copy)]
struct Sample {
    delay: Duration,
}

/// Mutable urltest state. The outer `Mutex` is taken only for short
/// synchronous critical sections — never held across `.await`.
#[derive(Default)]
struct UrltestState {
    selected_tcp: Option<String>,
    selected_udp: Option<String>,
    history: HashMap<String, Sample>,
}

/// One probe report carried back to the FFI surface.
pub struct UrltestSample {
    pub id: String,
    pub result: HammerResult<Duration>,
}

/// Aggregate outbound that selects the lowest-latency child each probe
/// sweep. Implements [`Outbound`] so the router treats it like any other
/// outbound — children are looked up dynamically through the
/// [`OutboundManager`](OutboundManagerTrait) the runtime binds in via
/// [`Outbound::bind_resolver`].
#[hammer_component_macros::hammer_component(
    outbound,
    name = "urltest",
    builder = build_outbound,
    dependencies = children_ids,
    metrics = ("outbound", "outbound")
)]
pub struct UrltestOutbound {
    id: String,
    children_ids: Vec<String>,
    networks: Vec<Network>,
    probe: Arc<HttpUrltestProbe>,
    timeout: Duration,
    tolerance: Duration,
    state: Mutex<UrltestState>,
    resolver: OnceLock<Weak<dyn OutboundManagerTrait>>,
    logger: Logger,
}

impl UrltestOutbound {
    fn new(
        logger: Logger,
        id: String,
        options: &UrltestOutboundOptions,
        protector: SocketProtector,
    ) -> HammerResult<Arc<Self>> {
        let probe = if let Some(platform) = protector.platform() {
            HttpUrltestProbe::new_with_platform(options.url.clone(), platform)?
        } else {
            HttpUrltestProbe::new(options.url.clone())?
        };
        Ok(Arc::new(Self {
            id,
            children_ids: options.outbounds.clone(),
            networks: TCP_AND_UDP.to_vec(),
            probe: Arc::new(probe),
            timeout: options.timeout,
            tolerance: options.tolerance,
            state: Mutex::new(UrltestState::default()),
            resolver: OnceLock::new(),
            logger,
        }))
    }

    /// Per-probe timeout configured at construction time. Callers that
    /// don't want to override should pass this to [`Self::run_probe`].
    pub fn default_timeout(&self) -> Duration {
        self.timeout
    }

    fn resolver(&self) -> HammerResult<Arc<dyn OutboundManagerTrait>> {
        self.resolver.get().and_then(Weak::upgrade).ok_or_else(|| {
            HammerError::internal(format!(
                "urltest '{}' is not bound to an OutboundManager",
                self.id
            ))
        })
    }

    /// Run one probe sweep against every child concurrently. Updates the
    /// internal history + selection and returns one [`UrltestSample`] per
    /// child in declaration order. `per_probe_timeout` overrides the
    /// configured per-probe timeout for this sweep — pass
    /// [`Self::default_timeout`] to use the configured value.
    pub async fn run_probe(&self, per_probe_timeout: Duration) -> Vec<UrltestSample> {
        let resolver = match self.resolver() {
            Ok(r) => r,
            Err(err) => {
                self.logger.warn(err.to_string());
                return Vec::new();
            }
        };

        let mut handles: Vec<(String, crate::spawn::JoinHandle<HammerResult<Duration>>)> =
            Vec::with_capacity(self.children_ids.len());
        let mut missing: Vec<String> = Vec::new();
        for child_id in &self.children_ids {
            let Some(child) = resolver.get(child_id) else {
                self.logger.warn(format!(
                    "urltest '{}': child '{child_id}' missing during probe",
                    self.id
                ));
                missing.push(child_id.clone());
                continue;
            };
            let probe = Arc::clone(&self.probe);
            let timeout = per_probe_timeout;
            let id = child_id.clone();
            handles.push((
                id,
                crate::spawn::spawn(async move { probe.measure(&child, timeout).await }),
            ));
        }

        let mut samples = Vec::with_capacity(handles.len() + missing.len());
        for (id, handle) in handles {
            let result = match handle.await {
                Ok(r) => r,
                Err(err) => Err(HammerError::internal(format!(
                    "urltest probe task panicked for '{id}': {err}"
                ))),
            };
            samples.push(UrltestSample { id, result });
        }
        for id in missing {
            samples.push(UrltestSample {
                id,
                result: Err(HammerError::internal("child outbound not registered")),
            });
        }

        // Update history and re-pick selected outbounds. Mutex is taken
        // synchronously and dropped before the function returns.
        let mut state = self.state.lock().expect("urltest state poisoned");
        for sample in &samples {
            match &sample.result {
                Ok(delay) => {
                    state
                        .history
                        .insert(sample.id.clone(), Sample { delay: *delay });
                }
                Err(_) => {
                    state.history.remove(&sample.id);
                }
            }
        }
        self.recompute_selection(&mut state, &resolver);
        drop(state);

        if let Some(now_id) = self.now() {
            self.logger
                .info(format!("urltest '{}' selected '{now_id}'", self.id));
        }
        samples
    }

    fn recompute_selection(
        &self,
        state: &mut UrltestState,
        resolver: &Arc<dyn OutboundManagerTrait>,
    ) {
        state.selected_tcp = self.select(state, resolver, Network::Tcp);
        state.selected_udp = self.select(state, resolver, Network::Udp);
    }

    /// Sing-box selection: prefer the current pick unless a candidate is
    /// faster by at least `tolerance`. Falls back to the first
    /// network-compatible child when no probe has succeeded yet so a
    /// dial coming in before [`run_probe`] still gets a sensible target.
    fn select(
        &self,
        state: &UrltestState,
        resolver: &Arc<dyn OutboundManagerTrait>,
        network: Network,
    ) -> Option<String> {
        if !matches!(network, Network::Tcp | Network::Udp) {
            return None;
        }
        let current = match network {
            Network::Tcp => state.selected_tcp.as_deref(),
            Network::Udp => state.selected_udp.as_deref(),
            _ => None,
        };
        let mut best: Option<(String, Duration)> =
            current.and_then(|id| state.history.get(id).map(|s| (id.to_owned(), s.delay)));

        for child_id in &self.children_ids {
            let Some(child) = resolver.get(child_id) else {
                continue;
            };
            if !child.meta().networks().contains(&network) {
                continue;
            }
            let Some(sample) = state.history.get(child_id) else {
                continue;
            };
            match &best {
                None => best = Some((child_id.clone(), sample.delay)),
                Some((cur_id, cur_delay))
                    if cur_id != child_id && sample.delay + self.tolerance <= *cur_delay =>
                {
                    best = Some((child_id.clone(), sample.delay));
                }
                _ => {}
            }
        }

        best.map(|(id, _)| id).or_else(|| {
            // No history yet — pick the first child whose `networks()`
            // covers the requested protocol so the runtime can still dial.
            self.children_ids
                .iter()
                .find(|id| {
                    resolver
                        .get(id)
                        .is_some_and(|c| c.meta().networks().contains(&network))
                })
                .cloned()
        })
    }

    fn pick_for(&self, network: Network) -> HammerResult<OutboundComponent> {
        let resolver = self.resolver()?;
        let cached = {
            let state = self.state.lock().expect("urltest state poisoned");
            match network {
                Network::Tcp => state.selected_tcp.clone(),
                Network::Udp => state.selected_udp.clone(),
                _ => {
                    return Err(HammerError::internal(format!(
                        "urltest '{}': unsupported network {network}",
                        self.id
                    )));
                }
            }
        };
        if let Some(id) = cached.as_deref()
            && let Some(child) = resolver.get(id)
        {
            return Ok(child);
        }

        // Cache miss (first dial before probe completes, or selected was
        // dropped on failure). Recompute synchronously and try again.
        let id = {
            let mut state = self.state.lock().expect("urltest state poisoned");
            self.recompute_selection(&mut state, &resolver);
            match network {
                Network::Tcp => state.selected_tcp.clone(),
                Network::Udp => state.selected_udp.clone(),
                _ => None,
            }
        };
        let id = id.ok_or_else(|| {
            HammerError::internal(format!(
                "urltest '{}': no child supports {network}",
                self.id
            ))
        })?;
        resolver.get(&id).ok_or_else(|| {
            HammerError::internal(format!(
                "urltest '{}': selected child '{id}' disappeared",
                self.id
            ))
        })
    }

    fn drop_child(&self, id: &str) {
        let mut state = self.state.lock().expect("urltest state poisoned");
        state.history.remove(id);
        if state.selected_tcp.as_deref() == Some(id) {
            state.selected_tcp = None;
        }
        if state.selected_udp.as_deref() == Some(id) {
            state.selected_udp = None;
        }
    }
}

#[async_trait]
impl Outbound for UrltestOutbound {
    fn reset(&self) {
        // Selected outbounds may carry stale connections after a network
        // flip; clear them so the next dial recomputes against the
        // children's fresh networks.
        let mut state = self.state.lock().expect("urltest state poisoned");
        state.selected_tcp = None;
        state.selected_udp = None;
    }

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> HammerResult<Box<dyn ProxyStream>> {
        let child = self.pick_for(network)?;
        match child
            .runtime()
            .dial(network, destination, initial_payload)
            .await
        {
            Ok(stream) => Ok(stream),
            Err(err) => {
                self.drop_child(child.meta().id());
                Err(err)
            }
        }
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        let child = self.pick_for(Network::Udp)?;
        match child.runtime().listen_packet().await {
            Ok(conn) => Ok(conn),
            Err(err) => {
                self.drop_child(child.meta().id());
                Err(err)
            }
        }
    }

    async fn post_start(&self) -> HammerResult<()> {
        // The demo drives urltest explicitly from the UI. Do not warm or probe
        // during service startup; that can compete with the first real flow.
        Ok(())
    }

    async fn probe_group(&self, timeout: Duration) -> HammerResult<Vec<ProbeReport>> {
        let effective = self.probe_group_timeout(timeout);
        let samples = self.run_probe(effective).await;
        Ok(samples
            .into_iter()
            .map(|sample| ProbeReport {
                outbound_id: sample.id,
                protocol: hammer_core::config::constants::TYPE_URLTEST.to_owned(),
                result: sample.result,
            })
            .collect())
    }

    fn now(&self) -> Option<String> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.selected_tcp.clone())
    }

    fn probe_group_timeout(&self, timeout: Duration) -> Duration {
        if timeout.is_zero() {
            self.timeout
        } else {
            timeout
        }
    }

    fn bind_resolver(&self, resolver: Weak<dyn OutboundManagerTrait>) {
        let _ = self.resolver.set(resolver);
    }
}

/// Builder slot called by [`OutboundManager`](crate::OutboundManager) when it
/// encounters a `urltest` entry in the config.
pub(crate) fn build_outbound(
    logger: Logger,
    id: String,
    kind: &OutboundKind,
    protector: SocketProtector,
    _control_handle: Option<Arc<crate::ControlThreadHandle>>,
) -> HammerResult<Arc<UrltestOutbound>> {
    let OutboundKind::Urltest(options) = kind else {
        return Err(HammerError::internal(format!(
            "urltest builder invoked for non-urltest outbound: {id}"
        )));
    };
    let outbound = UrltestOutbound::new(logger, id, options, protector)?;
    Ok(outbound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_adapter::{ComponentMeta, RuntimeComponent};
    use hammer_core::log::{DiscardWriter, Factory};
    use url::Url;

    struct LeafOutbound;

    #[async_trait]
    impl Outbound for LeafOutbound {
        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> HammerResult<Box<dyn ProxyStream>> {
            Err(HammerError::internal("leaf-test: dial not used"))
        }

        async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
            Err(HammerError::internal("leaf-test: listen_packet not used"))
        }
    }

    fn logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn leaf(id: &str) -> OutboundComponent {
        let outbound = Arc::new(LeafOutbound);
        let runtime: Arc<dyn Outbound> = outbound;
        RuntimeComponent::new(
            ComponentMeta::new(
                "outbound",
                "leaf-test",
                id,
                vec![Network::Tcp],
                Vec::new(),
                None,
            ),
            runtime,
        )
    }

    #[test]
    fn select_switches_when_candidate_is_exactly_tolerance_faster() {
        let manager = Arc::new(crate::outbounds::OutboundManager::new(
            logger("manager"),
            "current",
        ));
        manager
            .register_outbound(leaf("current"))
            .expect("register current");
        manager
            .register_outbound(leaf("candidate"))
            .expect("register candidate");
        let resolver: Arc<dyn OutboundManagerTrait> = manager;

        let options = UrltestOutboundOptions {
            outbounds: vec!["current".to_owned(), "candidate".to_owned()],
            url: Url::parse("http://urltest.example/probe").expect("valid URL"),
            tolerance: Duration::from_millis(50),
            timeout: Duration::from_secs(1),
        };
        let urltest = UrltestOutbound::new(
            logger("urltest"),
            "auto".to_owned(),
            &options,
            SocketProtector::default(),
        )
        .expect("urltest outbound");
        let state = UrltestState {
            selected_tcp: Some("current".to_owned()),
            selected_udp: None,
            history: HashMap::from([
                (
                    "current".to_owned(),
                    Sample {
                        delay: Duration::from_millis(100),
                    },
                ),
                (
                    "candidate".to_owned(),
                    Sample {
                        delay: Duration::from_millis(50),
                    },
                ),
            ]),
        };

        assert_eq!(
            urltest.select(&state, &resolver, Network::Tcp).as_deref(),
            Some("candidate")
        );
    }

    #[test]
    fn probe_group_timeout_uses_configured_timeout_for_zero() {
        let options = UrltestOutboundOptions {
            outbounds: vec!["direct".to_owned()],
            url: Url::parse("http://urltest.example/probe").expect("valid URL"),
            tolerance: Duration::from_millis(50),
            timeout: Duration::from_secs(12),
        };
        let urltest = UrltestOutbound::new(
            logger("urltest"),
            "auto".to_owned(),
            &options,
            SocketProtector::default(),
        )
        .expect("urltest outbound");

        assert_eq!(
            urltest.probe_group_timeout(Duration::ZERO),
            Duration::from_secs(12)
        );
        assert_eq!(
            urltest.probe_group_timeout(Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }
}
