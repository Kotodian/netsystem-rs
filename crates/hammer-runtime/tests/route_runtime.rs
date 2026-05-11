use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use hammer_adapter::{
    ConnectionHandle, Network, PlatformInterface, RouteDecision, RouteMetadata, SocksAddr,
};
use hammer_core::config::{self, DomainStrategy, Options, RuleActionKind};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_runtime::{
    ConnectionManager, MetricsRegistry, NetworkManager, OutboundManager, PauseManager, Router,
};

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn options() -> Options {
    config::parse_config(
        r#"
[tun]
address = ["172.19.0.1/30"]
sniff = true
sniff_override_destination = true
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

fn demo_like_options_without_tcp_sniff_rules() -> Options {
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
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            options.route.final_.clone(),
            &options.outbounds,
        )
        .expect("outbound manager"),
    );
    Router::from_options(logger("router"), options.route.clone(), outbound).expect("router")
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
    assert_eq!(metadata.domain_strategy, Some(DomainStrategy::PreferIpv4));
    assert!(metadata.udp_disable_domain_unmapping);
}

#[test]
fn router_does_not_record_success_metrics() {
    let opts = options();
    let metrics = MetricsRegistry::new();
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            opts.route.final_.clone(),
            &opts.outbounds,
        )
        .expect("outbound manager"),
    );
    let router = Router::from_options_with_metrics(
        logger("router"),
        opts.route.clone(),
        outbound,
        Arc::clone(&metrics),
    )
    .expect("router");
    let mut metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Udp,
        protocol: "dns".to_owned(),
        ..Default::default()
    };

    let _ = router.match_route(&mut metadata).expect("match route");

    let samples = metrics.snapshot();
    assert!(
        samples
            .iter()
            .all(|sample| sample.name != "match_total" && sample.name != "decision_total"),
        "samples = {samples:?}"
    );
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
fn router_skips_tcp_sniff_wait_when_no_tcp_rule_needs_sniffed_metadata() {
    let router = router_from_options(&demo_like_options_without_tcp_sniff_rules());
    let metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Tcp,
        destination: Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 443)),
        ..Default::default()
    };

    assert!(
        router.sniff_timeout(&metadata).is_some(),
        "generic sniff stays enabled for UDP DNS/QUIC handling"
    );
    assert_eq!(router.tcp_sniff_timeout(&metadata), None);
}

#[test]
fn router_keeps_tcp_sniff_wait_for_domain_route_rules() {
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"example.com\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);
    let metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Tcp,
        destination: Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
            443,
        )),
        ..Default::default()
    };

    assert!(router.tcp_sniff_timeout(&metadata).is_some());
}

#[test]
fn router_keeps_tcp_sniff_wait_when_sniff_overrides_destination() {
    let router = router_from_options(&options());
    let metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Tcp,
        destination: Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
            443,
        )),
        ..Default::default()
    };

    assert!(router.tcp_sniff_timeout(&metadata).is_some());
}

fn options_with_user_rules(rules_block: &str) -> Options {
    let cfg = format!(
        r#"
[tun]
address = ["172.19.0.1/30"]
sniff = true

[hysteria2]
server = "example.com"
password = "secret"
sni = "example.com"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "direct"
{rules_block}
"#
    );
    config::parse_config(&cfg).expect("parse config")
}

fn metadata_with_domain(domain: &str) -> RouteMetadata {
    RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Tcp,
        protocol: "https".to_owned(),
        domain: Some(domain.to_owned()),
        ..Default::default()
    }
}

fn metadata_with_destination(host: IpAddr) -> RouteMetadata {
    RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Tcp,
        protocol: "https".to_owned(),
        destination: Some(SocksAddr::ip(host, 443)),
        ..Default::default()
    }
}

#[test]
fn router_routes_domain_suffix_match_to_named_outbound() {
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"google.com\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    let mut hit = metadata_with_domain("www.google.com");
    assert_eq!(
        router.match_route(&mut hit).expect("match"),
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );

    let mut miss = metadata_with_domain("baidu.com");
    assert_eq!(
        router.match_route(&mut miss).expect("match"),
        RouteDecision::Route {
            outbound: "direct".to_owned()
        }
    );
}

#[test]
fn router_caches_decision_for_repeated_metadata() {
    // UDP/ICMP dispatch invokes match_route once per packet — same 5-tuple
    // hits the cache so the second walk is a single hashmap lookup.
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"google.com\"]\noutbound = \"hysteria2\"\n",
    );
    let metrics = MetricsRegistry::new();
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            opts.route.final_.clone(),
            &opts.outbounds,
        )
        .expect("outbound manager"),
    );
    let router = Router::from_options_with_metrics(
        logger("router"),
        opts.route.clone(),
        outbound,
        Arc::clone(&metrics),
    )
    .expect("router");

    let make_metadata = || metadata_with_domain("www.google.com");

    // First call: miss + walk + insert.
    let mut first = make_metadata();
    let decision_first = router.match_route(&mut first).expect("first");

    // Second call with semantically identical metadata: hit.
    let mut second = make_metadata();
    let decision_second = router.match_route(&mut second).expect("second");
    assert_eq!(decision_first, decision_second);

    let samples = metrics.snapshot();
    let count = |name: &str| -> u64 {
        samples
            .iter()
            .filter(|s| s.name == name)
            .map(|s| s.value as u64)
            .sum()
    };
    assert_eq!(count("cache_hit_total"), 1, "samples = {samples:?}");
    assert_eq!(count("cache_miss_total"), 1, "samples = {samples:?}");
}

#[test]
fn router_reset_network_clears_cache() {
    // reset_network is the runtime's hook for "network conditions changed";
    // any cached decisions might be stale, so flushing keeps us correct
    // when the dispatch path next asks.
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"google.com\"]\noutbound = \"hysteria2\"\n",
    );
    let metrics = MetricsRegistry::new();
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            opts.route.final_.clone(),
            &opts.outbounds,
        )
        .expect("outbound manager"),
    );
    let router = Router::from_options_with_metrics(
        logger("router"),
        opts.route.clone(),
        outbound,
        Arc::clone(&metrics),
    )
    .expect("router");

    let mut metadata = metadata_with_domain("www.google.com");
    let _ = router.match_route(&mut metadata).expect("warm-up miss");

    use hammer_adapter::Router as _; // bring `reset_network` into scope
    router.reset_network();

    let mut metadata = metadata_with_domain("www.google.com");
    let _ = router.match_route(&mut metadata).expect("post-reset");

    let samples = metrics.snapshot();
    let count = |name: &str| -> u64 {
        samples
            .iter()
            .filter(|s| s.name == name)
            .map(|s| s.value as u64)
            .sum()
    };
    assert_eq!(count("cache_hit_total"), 0, "samples = {samples:?}");
    assert_eq!(count("cache_miss_total"), 2, "samples = {samples:?}");
}

#[test]
fn router_domain_match_is_case_insensitive_via_config_normalize() {
    // Rule written in mixed case should still match the lowercase form
    // that sniff_http / sniff_tls_sni / apply_reverse_dns_mapping write
    // into metadata.domain.
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain = [\"Example.COM\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    let mut metadata = metadata_with_domain("example.com");
    assert_eq!(
        router.match_route(&mut metadata).expect("match"),
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
}

#[test]
fn router_domain_match_strips_trailing_dot_in_config() {
    // Hickory-style FQDN ("ifconfig.so.") in the rule should match the
    // bare form metadata always carries.
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain = [\"ifconfig.so.\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    let mut metadata = metadata_with_domain("ifconfig.so");
    assert_eq!(
        router.match_route(&mut metadata).expect("match"),
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
}

#[test]
fn router_domain_suffix_match_is_case_insensitive_via_config_normalize() {
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"Google.COM\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    let mut metadata = metadata_with_domain("www.google.com");
    assert_eq!(
        router.match_route(&mut metadata).expect("match"),
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
}

#[test]
fn router_domain_suffix_does_not_match_partial_label() {
    let opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"google.com\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    // "evilgoogle.com" must not be treated as a subdomain of "google.com".
    let mut metadata = metadata_with_domain("evilgoogle.com");
    assert_eq!(
        router.match_route(&mut metadata).expect("match"),
        RouteDecision::Route {
            outbound: "direct".to_owned()
        }
    );
}

#[test]
fn router_routes_ipv4_cidr_match_to_named_outbound() {
    let opts = options_with_user_rules(
        "[[route.rules]]\nip_cidr = [\"10.0.0.0/8\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    let mut hit = metadata_with_destination(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)));
    assert_eq!(
        router.match_route(&mut hit).expect("match"),
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );

    let mut miss = metadata_with_destination(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
    assert_eq!(
        router.match_route(&mut miss).expect("match"),
        RouteDecision::Route {
            outbound: "direct".to_owned()
        }
    );
}

#[test]
fn router_routes_ipv6_cidr_match_to_named_outbound() {
    let opts = options_with_user_rules(
        "[[route.rules]]\nip_cidr = [\"2001:db8::/32\"]\noutbound = \"hysteria2\"\n",
    );
    let router = router_from_options(&opts);

    let mut hit =
        metadata_with_destination(IpAddr::V6("2001:db8:1234::1".parse::<Ipv6Addr>().unwrap()));
    assert_eq!(
        router.match_route(&mut hit).expect("match"),
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );

    let mut miss =
        metadata_with_destination(IpAddr::V6("2001:dead::1".parse::<Ipv6Addr>().unwrap()));
    assert_eq!(
        router.match_route(&mut miss).expect("match"),
        RouteDecision::Route {
            outbound: "direct".to_owned()
        }
    );
}

#[test]
fn router_rejects_unknown_outbound_at_construction() {
    let mut opts = options_with_user_rules(
        "[[route.rules]]\ndomain_suffix = [\"google.com\"]\noutbound = \"direct\"\n",
    );
    let RuleActionKind::Route(action) = &mut opts
        .route
        .rules
        .last_mut()
        .expect("user route rule")
        .default_options
        .action
    else {
        panic!("last rule should be a route action");
    };
    action.outbound = "typo".to_owned();
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            opts.route.final_.clone(),
            &opts.outbounds,
        )
        .expect("outbound manager"),
    );
    let err = match Router::from_options(logger("router"), opts.route.clone(), outbound) {
        Ok(_) => panic!("unknown outbound should fail router construction"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("\"typo\""),
        "error should mention the unknown outbound id: {err}"
    );
}

#[test]
fn router_rejects_unknown_default_outbound_at_construction() {
    let mut opts = options();
    opts.route.final_ = "typo".to_owned();
    let outbound = Arc::new(
        OutboundManager::from_options(
            logger("outbound"),
            opts.route.final_.clone(),
            &opts.outbounds,
        )
        .expect("outbound manager"),
    );
    let err = match Router::from_options(logger("router"), opts.route.clone(), outbound) {
        Ok(_) => panic!("unknown default outbound should fail router construction"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("\"typo\""),
        "error should mention the unknown default outbound id: {err}"
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

#[test]
fn router_applies_sniff_override_destination_option() {
    let router = router_from_options(&options());
    let mut metadata = RouteMetadata {
        inbound: "tun".to_owned(),
        network: Network::Tcp,
        protocol: "tls".to_owned(),
        domain: Some("example.com".to_owned()),
        destination: Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
            443,
        )),
        ..Default::default()
    };

    let decision = router.match_route(&mut metadata).expect("match route");

    assert_eq!(
        decision,
        RouteDecision::Route {
            outbound: "hysteria2".to_owned()
        }
    );
    assert!(metadata.override_destination);
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
    let manager = ConnectionManager::new();
    let first = Arc::new(FakeHandle::default());
    let second = Arc::new(FakeHandle::default());

    let first_id = manager.track(Arc::clone(&first));
    let second_id = manager.track(Arc::clone(&second));

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

#[test]
fn connection_manager_lifecycle_close_closes_all_connections() {
    let manager = ConnectionManager::new();
    let first = Arc::new(FakeHandle::default());
    let second = Arc::new(FakeHandle::default());

    manager.track(Arc::clone(&first));
    manager.track(Arc::clone(&second));

    Lifecycle::close(&manager).expect("lifecycle close");

    assert!(first.closed.load(Ordering::SeqCst));
    assert!(second.closed.load(Ordering::SeqCst));
    assert_eq!(manager.count(), 0);

    Lifecycle::close(&manager).expect("second lifecycle close");
    assert_eq!(manager.count(), 0);
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
    fn open_tun(&self, _options: hammer_adapter::TunOptions) -> Result<i32, HammerError> {
        Ok(42)
    }

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
    let connection = Arc::new(ConnectionManager::new());
    let tracked = Arc::new(FakeHandle::default());
    connection.track(Arc::clone(&tracked));
    let network = NetworkManager::with_platform(
        logger("network"),
        true,
        Arc::clone(&platform),
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

#[test]
fn network_manager_ignores_duplicate_default_interface_updates() {
    let platform = Arc::new(MockPlatform::default());
    let pause = Arc::new(PauseManager::new());
    let connection = Arc::new(ConnectionManager::new());
    let first = Arc::new(FakeHandle::default());
    connection.track(Arc::clone(&first));
    let network = NetworkManager::with_platform(
        logger("network"),
        true,
        Arc::clone(&platform),
        Arc::clone(&pause),
        Arc::clone(&connection),
    );

    network
        .start(StartStage::Initialize)
        .expect("start monitor");
    network.start(StartStage::Started).expect("mark started");

    platform.emit("en0", 7, true, false);
    assert!(first.closed.load(Ordering::SeqCst));

    let duplicate_guard = Arc::new(FakeHandle::default());
    connection.track(Arc::clone(&duplicate_guard));
    platform.emit("en0", 7, true, false);

    assert!(
        !duplicate_guard.closed.load(Ordering::SeqCst),
        "duplicate interface updates should not reset live connections"
    );
}
