#![cfg(feature = "outbound-urltest")]
//! Integration tests for the urltest aggregate outbound. The whole file is
//! gated on `outbound-urltest`; without the feature `cargo test` compiles
//! it to nothing rather than fight the optional deps.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hammer_adapter::{
    ComponentMeta, Lifecycle, Network, Outbound as AdapterOutbound, OutboundComponent,
    OutboundManager as _, ProxyPacketConn, ProxyStream, RuntimeComponent, SocksAddr,
};
use hammer_core::config::{Outbound, OutboundKind, UrltestOutboundOptions};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::OutboundManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

/// Outbound mock that, on `dial`, hands back one half of a duplex pair and
/// spawns a task that reads the HEAD request, sleeps `delay`, then replies
/// with `204 No Content`. Latency seen by `HttpUrltestProbe::measure` is
/// dominated by `delay`.
struct LatencyOutbound {
    id: String,
    networks: Vec<Network>,
    delay: Duration,
    fail_dial: AtomicUsize,
    dial_attempts: AtomicUsize,
}

impl LatencyOutbound {
    fn new(id: &str, networks: Vec<Network>, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            id: id.to_owned(),
            networks,
            delay,
            fail_dial: AtomicUsize::new(0),
            dial_attempts: AtomicUsize::new(0),
        })
    }

    fn fail_next_dials(&self, count: usize) {
        self.fail_dial.fetch_add(count, Ordering::SeqCst);
    }

    fn dial_attempts(&self) -> usize {
        self.dial_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AdapterOutbound for LatencyOutbound {
    async fn dial(
        &self,
        _network: Network,
        _destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        self.dial_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_dial
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n == 0 { None } else { Some(n - 1) }
            })
            .is_ok()
        {
            return Err(HammerError::internal("latency-mock: forced dial failure"));
        }
        let (client, mut server) = tokio::io::duplex(4096);
        let delay = self.delay;
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1024];
            let mut total = 0;
            loop {
                let Ok(n) = server.read(&mut buffer[total..]).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                total += n;
                if buffer[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
                if total == buffer.len() {
                    return;
                }
            }
            tokio::time::sleep(delay).await;
            let _ = server
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await;
            let _ = server.shutdown().await;
        });
        Ok(Box::new(client))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        Err(HammerError::internal("latency-mock: udp not supported"))
    }
}

fn latency_component(outbound: Arc<LatencyOutbound>) -> OutboundComponent {
    let id = outbound.id.clone();
    let networks = outbound.networks.clone();
    let runtime: Arc<dyn AdapterOutbound> = outbound;
    RuntimeComponent::new(
        ComponentMeta::new("outbound", "latency-mock", id, networks, Vec::new(), None),
        runtime,
    )
}

/// Build an OutboundManager preloaded with a slice of leaf children plus a
/// single urltest entry referencing them in declaration order.
fn build_manager(
    children: Vec<Arc<LatencyOutbound>>,
    tolerance: Duration,
    timeout: Duration,
) -> Arc<OutboundManager> {
    let manager = OutboundManager::new(logger("outbound"), "auto");
    for child in &children {
        manager
            .register_outbound(latency_component(Arc::clone(child)))
            .expect("register child");
    }
    let urltest_options = UrltestOutboundOptions {
        outbounds: children.iter().map(|c| c.id.clone()).collect(),
        url: Url::parse("http://urltest.example/probe").expect("valid url"),
        tolerance,
        timeout,
    };
    manager
        .register_descriptor(&Outbound {
            id: "auto".to_owned(),
            kind: OutboundKind::Urltest(urltest_options),
        })
        .expect("register urltest");
    let manager = Arc::new(manager);
    manager.bind_aggregates();
    manager
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_start_does_not_probe_until_group_probe_is_requested() {
    let child = LatencyOutbound::new("hysteria2", vec![Network::Tcp], Duration::from_millis(20));
    let manager = build_manager(
        vec![Arc::clone(&child)],
        Duration::from_millis(50),
        Duration::from_secs(2),
    );
    let urltest = manager.get("auto").expect("urltest registered");

    manager
        .start(StartStage::PostStart)
        .expect("post-start hooks");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        child.dial_attempts(),
        0,
        "urltest post-start must not probe before the user requests it"
    );
    assert_eq!(urltest.now(), None);

    let reports = urltest
        .probe_group(Duration::from_secs(2))
        .await
        .expect("manual urltest probe");

    assert_eq!(reports.len(), 1);
    assert!(reports[0].result.is_ok());
    assert_eq!(child.dial_attempts(), 1);
    assert_eq!(urltest.now().as_deref(), Some("hysteria2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn picks_lowest_latency_child() {
    // Slow / fast / medium so the middle child by declaration is the
    // fastest — exercises the "switch on first-faster candidate" branch.
    let a = LatencyOutbound::new("a", vec![Network::Tcp], Duration::from_millis(220));
    let b = LatencyOutbound::new("b", vec![Network::Tcp], Duration::from_millis(40));
    let c = LatencyOutbound::new("c", vec![Network::Tcp], Duration::from_millis(120));
    let manager = build_manager(
        vec![Arc::clone(&a), Arc::clone(&b), Arc::clone(&c)],
        Duration::from_millis(50),
        Duration::from_secs(2),
    );

    let urltest = manager.get("auto").expect("urltest registered");
    let reports = urltest
        .probe_group(Duration::from_secs(2))
        .await
        .expect("probe sweep");
    assert_eq!(reports.len(), 3);
    assert!(reports.iter().all(|r| r.result.is_ok()));

    assert_eq!(urltest.now().as_deref(), Some("b"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tolerance_keeps_first_when_others_close_enough() {
    // All three respond within ~50 ms of each other; declaration order
    // wins because the tolerance keeps the first declared child.
    let a = LatencyOutbound::new("a", vec![Network::Tcp], Duration::from_millis(80));
    let b = LatencyOutbound::new("b", vec![Network::Tcp], Duration::from_millis(60));
    let c = LatencyOutbound::new("c", vec![Network::Tcp], Duration::from_millis(40));
    let manager = build_manager(
        vec![Arc::clone(&a), Arc::clone(&b), Arc::clone(&c)],
        Duration::from_millis(80),
        Duration::from_secs(2),
    );

    let urltest = manager.get("auto").expect("urltest registered");
    let reports = urltest
        .probe_group(Duration::from_secs(2))
        .await
        .expect("probe sweep");
    assert!(reports.iter().all(|r| r.result.is_ok()));
    // First declared child stays selected because no candidate beat it
    // by the configured tolerance.
    assert_eq!(urltest.now().as_deref(), Some("a"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dial_failure_drops_child_from_history() {
    let a = LatencyOutbound::new("a", vec![Network::Tcp], Duration::from_millis(20));
    let b = LatencyOutbound::new("b", vec![Network::Tcp], Duration::from_millis(40));
    let manager = build_manager(
        vec![Arc::clone(&a), Arc::clone(&b)],
        Duration::from_millis(50),
        Duration::from_secs(2),
    );

    let urltest = manager.get("auto").expect("urltest registered");
    let reports = urltest
        .probe_group(Duration::from_secs(2))
        .await
        .expect("probe sweep");
    assert!(reports.iter().all(|r| r.result.is_ok()));
    assert_eq!(urltest.now().as_deref(), Some("a"));

    // Force the next dial against `a` to fail. The urltest should drop
    // `a` from its history and the subsequent dial should fall through
    // to `b`.
    a.fail_next_dials(1);
    let dest = SocksAddr::ip("127.0.0.1".parse().unwrap(), 80);
    let _ = urltest.dial(Network::Tcp, dest.clone(), &[]).await;

    // After the failure `a`'s history is cleared, so the next pick
    // re-selects `b` (only remaining child with history).
    let _ = urltest.dial(Network::Tcp, dest, &[]).await.ok();
    assert_eq!(urltest.now().as_deref(), Some("b"));
}
