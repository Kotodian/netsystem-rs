use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use hammer_adapter::{ConnectionHandle, Network, PlatformInterface, RouteDecision, RouteMetadata};
use hammer_core::config::{self, DomainStrategy, Options};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::{ConnectionManager, NetworkManager, OutboundManager, PauseManager, Router};

fn logger(tag: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(tag)
}

fn options() -> Options {
    config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]
sniff = true
hijack_dns = true
block_quic = true
domain_strategy = "prefer_ipv4"
udp_disable_domain_unmapping = true

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "hysteria2"
"#,
    )
    .expect("parse config")
}

fn router_from_options(options: &Options) -> Router {
    let outbound = Arc::new(OutboundManager::from_options(
        logger("outbound"),
        options.route.final_.clone(),
        &options.outbounds,
    ));
    Router::from_options(logger("router"), options.route.clone(), outbound)
}

#[test]
fn router_matches_hijack_dns_before_default_outbound() {
    let router = router_from_options(&options());
    let mut metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Udp,
        protocol: "dns".to_owned(),
        ..Default::default()
    };

    let decision = router.match_route(&mut metadata).expect("match route");

    assert_eq!(decision, RouteDecision::HijackDns);
}

#[test]
fn router_rejects_quic_with_default_method() {
    let router = router_from_options(&options());
    let mut metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Udp,
        protocol: "quic".to_owned(),
        ..Default::default()
    };

    let decision = router.match_route(&mut metadata).expect("match route");

    assert_eq!(
        decision,
        RouteDecision::Reject {
            method: "default".to_owned()
        }
    );
}

#[test]
fn router_applies_non_terminal_actions_then_uses_default_outbound() {
    let router = router_from_options(&options());
    let mut metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Udp,
        protocol: "https".to_owned(),
        ..Default::default()
    };

    let decision = router.match_route(&mut metadata).expect("match route");

    assert_eq!(
        decision,
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
    assert_eq!(metadata.domain_strategy, Some(DomainStrategy::PreferIpv4));
    assert!(metadata.udp_disable_domain_unmapping);
}

#[derive(Default)]
struct FakeHandle {
    closed: AtomicBool,
}

impl ConnectionHandle for FakeHandle {
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

#[test]
fn connection_manager_tracks_removes_and_closes_connections() {
    let manager = ConnectionManager::new(logger("connection"));
    let first = Arc::new(FakeHandle::default());
    let second = Arc::new(FakeHandle::default());

    let first_id = manager.track(Arc::clone(&first) as Arc<dyn ConnectionHandle>);
    let second_id = manager.track(Arc::clone(&second) as Arc<dyn ConnectionHandle>);

    assert_eq!(manager.count(), 2);
    assert!(manager.remove(first_id));
    assert!(!manager.remove(first_id), "remove must be idempotent");
    assert_eq!(manager.count(), 1);

    manager.close_all();

    assert!(!first.closed.load(Ordering::SeqCst));
    assert!(second.closed.load(Ordering::SeqCst));
    assert_eq!(manager.count(), 0);
    assert!(!manager.remove(second_id));
}

#[derive(Default)]
struct MockPlatform {
    listener: std::sync::Mutex<Option<Arc<dyn hammer_adapter::DefaultInterfaceUpdateListener>>>,
    started: AtomicBool,
    closed: AtomicBool,
}

impl MockPlatform {
    fn emit(&self, name: &str, index: i32, expensive: bool, constrained: bool) {
        let listener = self
            .listener
            .lock()
            .expect("MockPlatform poisoned")
            .as_ref()
            .cloned()
            .expect("listener registered");
        listener.update_default_interface(name.to_owned(), index, expensive, constrained);
    }
}

impl PlatformInterface for MockPlatform {
    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }

    fn auto_detect_interface_control(&self, _fd: i32) -> Result<(), HammerError> {
        Ok(())
    }

    fn start_default_interface_monitor(
        &self,
        listener: Arc<dyn hammer_adapter::DefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError> {
        self.started.store(true, Ordering::SeqCst);
        *self.listener.lock().expect("MockPlatform poisoned") = Some(listener);
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _listener: Arc<dyn hammer_adapter::DefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn get_interfaces(&self) -> Result<Vec<hammer_adapter::NetworkInterface>, HammerError> {
        Ok(vec![hammer_adapter::NetworkInterface {
            index: 7,
            mtu: 1500,
            name: "en0".to_owned(),
            type_: 0,
            flags: 1,
            expensive: false,
            constrained: false,
            addresses: vec!["192.0.2.2/24".to_owned()],
            dns_servers: vec!["1.1.1.1".to_owned()],
        }])
    }

    fn read_wifi_state(&self) -> Option<hammer_adapter::WifiState> {
        Some(hammer_adapter::WifiState {
            ssid: "office".to_owned(),
            bssid: "aabbccddeeff".to_owned(),
        })
    }
}

#[test]
fn network_manager_tracks_platform_default_interface_and_pause_state() {
    let platform = Arc::new(MockPlatform::default());
    let pause = Arc::new(PauseManager::new());
    let connection = Arc::new(ConnectionManager::new(logger("connection")));
    let tracked = Arc::new(FakeHandle::default());
    connection.track(Arc::clone(&tracked) as Arc<dyn ConnectionHandle>);
    let network = NetworkManager::with_platform(
        logger("network"),
        true,
        Arc::clone(&platform) as Arc<dyn PlatformInterface>,
        Arc::clone(&pause),
        Arc::clone(&connection),
    );

    network
        .start(StartStage::Initialize)
        .expect("start monitor");
    network.start(StartStage::Started).expect("mark started");

    platform.emit("en0", 7, true, false);

    let default = network
        .default_network_interface()
        .expect("default interface should be set");
    assert_eq!(default.name, "en0");
    assert!(default.expensive);
    assert_eq!(network.network_interfaces().len(), 1);
    assert!(!pause.is_network_paused());
    assert_eq!(network.wifi_state().unwrap().ssid, "office");
    assert!(tracked.closed.load(Ordering::SeqCst));

    platform.emit("", -1, false, false);

    assert!(network.default_network_interface().is_none());
    assert!(pause.is_network_paused());
    network.close().expect("close monitor");
    assert!(platform.closed.load(Ordering::SeqCst));
}
