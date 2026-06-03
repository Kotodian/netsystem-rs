#![cfg(feature = "hysteria2")]

use std::time::Duration;

#[cfg(feature = "wireguard")]
use hammer_core::config::EndpointKind;

use hammer_core::config::{
    self, CertificateSource, DnsServerKind, EchConfigSource, Hysteria2Network, Hysteria2ObfsType,
    InboundKind, OutboundKind, PrivateKeySource, RuleActionKind, RuleMatcher, TunStack,
    UtlsFingerprint,
};
use hammer_core::log::Level;
use hammer_core::protocol::congestion::BbrProfile;

const MINIMAL_CONFIG: &str = r#"
[log]
level = "info"

[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0", "::/0"]
sniff = true
hijack_dns = true
block_quic = true
sniff_override_destination = true
sniff_timeout = "300ms"
domain_strategy = "prefer_ipv4"
udp_disable_domain_unmapping = true

[hysteria2]
server = "example.com"
server_port = 443
password = "secret"
up_mbps = 50
down_mbps = 200
sni = "example.com"
insecure = false

[hysteria2.obfs]
type = "salamander"
password = "obfs-secret"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "hysteria2"
"#;

#[test]
fn parse_config_builds_hysteria_tun_options() {
    let options = config::parse_config(MINIMAL_CONFIG).expect("parse");
    assert_eq!(options.log.level, Level::Info);
    assert_eq!(options.inbounds.len(), 1);
    assert_eq!(options.inbounds[0].type_name(), "tun");
    matches::assert_inbound_tun(&options.inbounds[0].kind);
    let InboundKind::Tun(tun) = &options.inbounds[0].kind else {
        panic!("inbound[0] not tun");
    };
    assert_eq!(tun.stack, TunStack::System);
    assert!(!tun.tap);

    assert_eq!(options.route.rules.len(), 5, "expected 5 tun rules");
    assert_rule_action(&options.route.rules[0].default_options.action, "sniff");
    let sniff = match &options.route.rules[0].default_options.action {
        RuleActionKind::Sniff(o) => o,
        _ => panic!("rule[0] not Sniff"),
    };
    assert_eq!(sniff.timeout, Some(Duration::from_millis(300)));
    assert!(sniff.override_destination, "sniff override expected true");
    assert_eq!(
        options.route.rules[0].default_options.matcher,
        RuleMatcher::Inbound(vec!["tun".to_owned()])
    );

    assert_rule_action(&options.route.rules[1].default_options.action, "resolve");
    let resolve = match &options.route.rules[1].default_options.action {
        RuleActionKind::Resolve(o) => o,
        _ => panic!("rule[1] not Resolve"),
    };
    assert_eq!(resolve.strategy, config::DomainStrategy::PreferIpv4);

    assert_rule_action(
        &options.route.rules[2].default_options.action,
        "route-options",
    );
    let route_opts = match &options.route.rules[2].default_options.action {
        RuleActionKind::RouteOptions(o) => o,
        _ => panic!("rule[2] not RouteOptions"),
    };
    assert!(route_opts.udp_disable_domain_unmapping);

    assert_rule_action(&options.route.rules[3].default_options.action, "hijack-dns");
    assert_eq!(
        options.route.rules[3].default_options.matcher,
        RuleMatcher::Protocol(vec!["dns".to_owned()])
    );

    assert_rule_action(&options.route.rules[4].default_options.action, "reject");
    assert_eq!(
        options.route.rules[4].default_options.matcher,
        RuleMatcher::Protocol(vec!["quic".to_owned()])
    );
    let reject = match &options.route.rules[4].default_options.action {
        RuleActionKind::Reject(o) => o,
        _ => panic!("rule[4] not Reject"),
    };
    assert_eq!(reject.method, "default");

    assert_eq!(options.outbounds.len(), 2);
    assert_eq!(options.outbounds[0].type_name(), "hysteria2");
    let hysteria = match &options.outbounds[0].kind {
        OutboundKind::Hysteria2(o) => o,
        _ => panic!("outbound[0] not hysteria2"),
    };
    assert_eq!(
        hysteria.network,
        vec![Hysteria2Network::Tcp, Hysteria2Network::Udp]
    );
    assert_eq!(hysteria.bbr_profile, BbrProfile::Standard);
    let obfs = hysteria.obfs.as_ref().expect("obfs should be parsed");
    assert_eq!(obfs.type_, Hysteria2ObfsType::Salamander);
    assert_eq!(options.outbounds[1].type_name(), "direct");
    assert_eq!(options.outbounds[1].id, "direct");

    assert_eq!(options.route.final_, "hysteria2");

    assert_eq!(options.dns.servers.len(), 1);
    let https = match &options.dns.servers[0].kind {
        DnsServerKind::Https(o) => o,
        _ => panic!("dns server is not Https"),
    };
    assert_eq!(https.via, "direct");
}

#[test]
fn parse_config_defaults_tun_tap_to_false() {
    let options = config::parse_config(MINIMAL_CONFIG).expect("parse");
    let InboundKind::Tun(tun) = &options.inbounds[0].kind else {
        panic!("inbound[0] not tun");
    };

    assert!(!tun.tap);
}

#[test]
fn parse_config_passes_through_tun_tap_true() {
    let cfg = MINIMAL_CONFIG.replacen("[tun]\n", "[tun]\ntap = true\n", 1);
    let options = config::parse_config(&cfg).expect("parse");
    let InboundKind::Tun(tun) = &options.inbounds[0].kind else {
        panic!("inbound[0] not tun");
    };

    assert!(tun.tap);
}

#[test]
fn parse_config_builds_socks_http_mixed_without_tun_rules() {
    let cfg = format!(
        r#"
[[inbounds]]
type = "socks"
id = "socks-in"
listen = "127.0.0.1"
listen_port = 1080
udp_timeout = "20s"

[[inbounds.users]]
username = "alice"
password = "secret"

[[inbounds]]
type = "http"
id = "http-in"
listen = "127.0.0.1"
listen_port = 8080

[[inbounds.users]]
username = "bob"
password = "secret"

[[inbounds]]
type = "mixed"
id = "mixed-in"
listen = "::1"
listen_port = 2080

[[outbounds]]
type = "direct"
id = "direct"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "direct"
"#
    );
    let options = config::parse_config(&cfg).expect("parse proxy inbounds");

    assert_eq!(options.inbounds.len(), 3);
    assert_eq!(options.inbounds[0].type_name(), "socks");
    assert_eq!(options.inbounds[1].type_name(), "http");
    assert_eq!(options.inbounds[2].type_name(), "mixed");
    assert!(
        options.route.rules.is_empty(),
        "non-TUN inbounds must not derive TUN route rules"
    );

    let InboundKind::Socks(socks) = &options.inbounds[0].kind else {
        panic!("inbound[0] not socks");
    };
    assert_eq!(socks.listen.listen.to_string(), "127.0.0.1");
    assert_eq!(socks.listen.listen_port, 1080);
    assert_eq!(socks.listen.udp_timeout, Some(Duration::from_secs(20)));
    assert_eq!(socks.users[0].username, "alice");

    let InboundKind::Http(http) = &options.inbounds[1].kind else {
        panic!("inbound[1] not http");
    };
    assert_eq!(http.listen.listen_port, 8080);
    assert_eq!(http.users[0].username, "bob");

    let InboundKind::Mixed(mixed) = &options.inbounds[2].kind else {
        panic!("inbound[2] not mixed");
    };
    assert_eq!(mixed.listen.listen.to_string(), "::1");
    assert_eq!(mixed.listen.listen_port, 2080);
}

#[test]
fn check_config_rejects_unsupported_config_keys() {
    let err = config::check_config(&format!("{MINIMAL_CONFIG}\n[profile]\nenabled = true\n"))
        .expect_err("CheckConfig accepted an unsupported section");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported config key: profile"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_passes_through_default_domain_resolver() {
    // Explicit-only: route.default_domain_resolver is no longer
    // auto-populated from dns.final. Without it set, the typed value
    // stays None.
    let options = config::parse_config(MINIMAL_CONFIG).expect("parse");
    assert!(
        options.route.default_domain_resolver.is_none(),
        "default_domain_resolver must be None when not configured"
    );

    // When explicitly set to a known DNS server id it round-trips.
    let cfg = MINIMAL_CONFIG.replace(
        "[route]\nfinal = \"hysteria2\"\n",
        "[route]\nfinal = \"hysteria2\"\ndefault_domain_resolver = \"default\"\n",
    );
    let options = config::parse_config(&cfg).expect("parse with default resolver");
    let resolver = options
        .route
        .default_domain_resolver
        .as_ref()
        .expect("default_domain_resolver must round-trip when set");
    assert_eq!(resolver.server, "default");
}

#[test]
fn parse_config_honors_explicit_dns_via() {
    let cfg = MINIMAL_CONFIG.replacen("[dns]\n", "[dns]\nvia = \"hysteria2\"\n", 1);
    let options = config::parse_config(&cfg).expect("parse");
    let https = match &options.dns.servers[0].kind {
        DnsServerKind::Https(o) => o,
        _ => panic!("dns server is not Https"),
    };
    assert_eq!(https.via, "hysteria2");
}

#[test]
fn parse_config_rejects_hysteria2_port_hopping() {
    let cfg = MINIMAL_CONFIG.replacen(
        "server_port = 443\n",
        "server_port = 443\nserver_ports = [\"443\", \"8443\"]\n",
        1,
    );
    let err = config::parse_config(&cfg).expect_err("port hopping must be rejected");
    assert!(
        err.to_string().contains("port hopping is not supported"),
        "error = {err:?}"
    );

    let cfg = MINIMAL_CONFIG.replacen(
        "server_port = 443\n",
        "server_port = 443\nhop_interval = \"30s\"\n",
        1,
    );
    let err = config::parse_config(&cfg).expect_err("hop interval must be rejected");
    assert!(
        err.to_string().contains("port hopping is not supported"),
        "error = {err:?}"
    );
}

#[test]
fn parse_config_expands_inline_tls_material_to_der_and_keeps_paths_typed() {
    let cfg = MINIMAL_CONFIG.replace(
        "insecure = false\n\n[hysteria2.obfs]\n",
        r#"insecure = false

[hysteria2.tls]
server_name = "example.com"
insecure = false
alpn = ["h3"]
server_fingerprint = ["sha256/1111111111111111111111111111111111111111111111111111111111111111"]
client_certificate = ['''-----BEGIN CERTIFICATE-----
AQID
-----END CERTIFICATE-----''']
client_certificate_path = ["/tmp/client.pem"]
client_key = '''-----BEGIN PRIVATE KEY-----
BAUG
-----END PRIVATE KEY-----'''

[hysteria2.tls.utls]
enabled = true
fingerprint = "chrome"

[hysteria2.tls.ech]
enabled = true
config = "AQIDBA=="

[hysteria2.tls.reality]
enabled = true
public_key = "0000000000000000000000000000000000000000000000000000000000000000"
short_id = "0a0b"

[hysteria2.tls.fragment]
enabled = true
size = "tlshello"
sleep = "1ms"

[hysteria2.tls.record_fragment]
enabled = true

[hysteria2.obfs]
"#,
    );
    let options = config::parse_config(&cfg).expect("parse tls config");
    let hysteria = match &options.outbounds[0].kind {
        OutboundKind::Hysteria2(o) => o,
        _ => panic!("outbound[0] not hysteria2"),
    };
    let tls = &hysteria.tls;

    assert_eq!(tls.alpn, vec!["h3".to_owned()]);
    assert_eq!(tls.server_fingerprints[0].digest, vec![0x11; 32]);

    let auth = tls.client_auth.as_ref().expect("client auth");
    assert_eq!(auth.certificates.len(), 2);
    match &auth.certificates[0] {
        CertificateSource::Inline(cert) => assert_eq!(cert.0, vec![1, 2, 3]),
        other => panic!("expected inline cert, got {other:?}"),
    }
    match &auth.certificates[1] {
        CertificateSource::Path(path) => assert_eq!(path, std::path::Path::new("/tmp/client.pem")),
        other => panic!("expected cert path, got {other:?}"),
    }
    match &auth.key {
        PrivateKeySource::Inline(key) => assert_eq!(key.0, vec![4, 5, 6]),
        other => panic!("expected inline key, got {other:?}"),
    }

    assert_eq!(
        tls.utls.as_ref().expect("utls").fingerprint,
        UtlsFingerprint::Chrome
    );
    match &tls.ech.as_ref().expect("ech").config_source {
        Some(EchConfigSource::Inline(config)) => assert_eq!(config.0, vec![1, 2, 3, 4]),
        other => panic!("expected inline ECH config, got {other:?}"),
    }
    let reality = tls.reality.as_ref().expect("reality");
    assert_eq!(reality.public_key.0, [0u8; 32]);
    assert_eq!(reality.short_id.0, vec![0x0a, 0x0b]);
    assert_eq!(
        tls.fragment.as_ref().expect("fragment").sleep,
        Duration::from_millis(1)
    );
    assert!(tls.record_fragment);
}

#[test]
fn parse_config_rejects_conflicting_legacy_and_nested_tls_fields() {
    let cfg = MINIMAL_CONFIG.replace(
        "insecure = false\n\n[hysteria2.obfs]\n",
        "insecure = false\n\n[hysteria2.tls]\nserver_name = \"other.example.com\"\n\n[hysteria2.obfs]\n",
    );
    let err = config::parse_config(&cfg).expect_err("accepted conflicting tls server_name");
    let msg = err.to_string();
    assert!(
        msg.contains("hysteria2.sni conflicts with hysteria2.tls.server_name"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_defaults_dns_strategy_to_as_is() {
    let options = config::parse_config(MINIMAL_CONFIG).expect("parse");
    assert_eq!(options.dns.strategy, config::DomainStrategy::AsIs);
}

#[test]
fn parse_config_propagates_dns_strategy() {
    let cfg = MINIMAL_CONFIG.replacen("[dns]\n", "[dns]\nstrategy = \"prefer_ipv4\"\n", 1);
    let options = config::parse_config(&cfg).expect("parse");
    assert_eq!(options.dns.strategy, config::DomainStrategy::PreferIpv4);
}

#[test]
fn parse_config_rejects_unknown_dns_strategy() {
    let cfg = MINIMAL_CONFIG.replacen("[dns]\n", "[dns]\nstrategy = \"prefer_quantum\"\n", 1);
    let err = config::parse_config(&cfg).expect_err("accepted unknown dns.strategy");
    let msg = err.to_string();
    assert!(msg.contains("dns.strategy"), "error = {msg:?}");
}

#[test]
fn parse_config_rejects_empty_dns_strategy() {
    let cfg = MINIMAL_CONFIG.replacen("[dns]\n", "[dns]\nstrategy = \"\"\n", 1);
    let err = config::parse_config(&cfg).expect_err("accepted empty dns.strategy");
    let msg = err.to_string();
    assert!(msg.contains("dns.strategy"), "error = {msg:?}");
}

#[test]
fn parse_config_rejects_empty_duration() {
    let cfg = MINIMAL_CONFIG.replacen("sniff_timeout = \"300ms\"\n", "sniff_timeout = \"\"\n", 1);
    let err = config::parse_config(&cfg).expect_err("accepted empty duration");
    let msg = err.to_string();
    assert!(msg.contains("sniff_timeout"), "error = {msg:?}");
}

#[test]
fn parse_config_rejects_unknown_log_level() {
    let cfg = MINIMAL_CONFIG.replacen("level = \"info\"\n", "level = \"verbose\"\n", 1);
    let err = config::parse_config(&cfg).expect_err("accepted unknown log.level");
    let msg = err.to_string();
    assert!(msg.contains("log.level"), "error = {msg:?}");
}

#[test]
fn parse_config_rejects_unknown_hysteria2_bbr_profile() {
    let cfg = MINIMAL_CONFIG.replacen(
        "[hysteria2]\n",
        "[hysteria2]\nbbr_profile = \"reckless\"\n",
        1,
    );
    let err = config::parse_config(&cfg).expect_err("accepted unknown hysteria2.bbr_profile");
    let msg = err.to_string();
    assert!(msg.contains("hysteria2.bbr_profile"), "error = {msg:?}");
}

#[test]
fn parse_config_rejects_hijack_dns_without_sniff() {
    let cfg = MINIMAL_CONFIG
        .replacen("sniff = true\n", "sniff = false\n", 1)
        .replacen("block_quic = true\n", "", 1)
        .replacen("sniff_override_destination = true\n", "", 1);
    let err = config::parse_config(&cfg).expect_err("accepted hijack_dns without sniff");
    let msg = err.to_string();
    assert!(
        msg.contains("tun.hijack_dns requires tun.sniff=true"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_rejects_block_quic_without_sniff() {
    let cfg = MINIMAL_CONFIG
        .replacen("sniff = true\n", "sniff = false\n", 1)
        .replacen("hijack_dns = true\n", "", 1)
        .replacen("sniff_override_destination = true\n", "", 1);
    let err = config::parse_config(&cfg).expect_err("accepted block_quic without sniff");
    let msg = err.to_string();
    assert!(
        msg.contains("tun.block_quic requires tun.sniff=true"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_appends_user_route_rules_after_tun_rules() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[route.rules]]\ndomain_suffix = [\"google.com\"]\noutbound = \"hysteria2\"\n"
    );
    let options = config::parse_config(&cfg).expect("parse");
    assert_eq!(
        options.route.rules.len(),
        6,
        "expected 5 tun rules + 1 user rule"
    );
    let user = &options.route.rules[5].default_options;
    assert_eq!(
        user.matcher,
        RuleMatcher::DomainSuffix(vec!["google.com".to_owned()])
    );
    let route = match &user.action {
        RuleActionKind::Route(o) => o,
        _ => panic!("user rule action is not Route"),
    };
    assert_eq!(route.outbound, "hysteria2");
}

#[test]
fn parse_config_rejects_user_rule_with_multiple_matchers() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[route.rules]]\ndomain_suffix = [\"google.com\"]\nip_cidr = [\"8.8.8.8/32\"]\noutbound = \"hysteria2\"\n"
    );
    let err = config::parse_config(&cfg).expect_err("accepted rule with multiple matchers");
    let msg = err.to_string();
    assert!(
        msg.contains("exactly one matcher"),
        "error should explain single matcher rule: {msg:?}"
    );
}

#[test]
fn parse_config_rejects_invalid_ip_cidr() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[route.rules]]\nip_cidr = [\"not-a-cidr\"]\noutbound = \"hysteria2\"\n"
    );
    let err = config::parse_config(&cfg).expect_err("accepted invalid CIDR");
    let msg = err.to_string();
    assert!(
        msg.contains("route.rules") && msg.contains("ip_cidr"),
        "error should pin user-visible rule index: {msg:?}"
    );
}

#[test]
fn parse_config_rejects_bare_ip_cidr() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[route.rules]]\nip_cidr = [\"8.8.8.8\"]\noutbound = \"hysteria2\"\n"
    );
    let err = config::parse_config(&cfg).expect_err("accepted bare IP as CIDR");
    let msg = err.to_string();
    assert!(
        msg.contains("route.rules") && msg.contains("ip_cidr"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_rejects_user_rule_without_outbound() {
    let cfg = format!("{MINIMAL_CONFIG}\n[[route.rules]]\ndomain_suffix = [\"google.com\"]\n");
    let err = config::parse_config(&cfg).expect_err("accepted rule without outbound");
    let msg = err.to_string();
    assert!(msg.contains("outbound is required"), "error = {msg:?}");
}

#[test]
fn parse_config_rejects_user_rule_without_any_matcher() {
    let cfg = format!("{MINIMAL_CONFIG}\n[[route.rules]]\noutbound = \"hysteria2\"\n");
    let err = config::parse_config(&cfg).expect_err("accepted rule without matcher");
    let msg = err.to_string();
    assert!(
        msg.contains("requires exactly one matcher"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_accepts_declared_inbounds_outbounds_and_dns_servers() {
    let cfg = r#"
[log]
level = "info"

[[inbounds]]
type = "tun"
id = "tun-a"
mtu = 1400
stack = "system"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
sniff = true
hijack_dns = true

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "hysteria2"
id = "hy-a"
server = "example.com"
server_port = 443
password = "secret"
sni = "example.com"

[dns]
strategy = "prefer_ipv4"
final = "cf"

[[dns.servers]]
type = "udp"
id = "cf"
server = "1.1.1.1"
via = "hy-a"

[[dns.servers]]
type = "local"
id = "local"

[route]
auto_detect_interface = true
default_domain_resolver = "cf"
"#;
    let options = config::parse_config(cfg).expect("parse declared config");

    assert_eq!(options.inbounds.len(), 1);
    assert_eq!(options.inbounds[0].id, "tun-a");
    assert_eq!(options.route.rules.len(), 2, "sniff + hijack-dns");
    assert_eq!(
        options.route.rules[0].default_options.matcher,
        RuleMatcher::Inbound(vec!["tun-a".to_owned()])
    );

    assert_eq!(
        options
            .outbounds
            .iter()
            .filter(|outbound| outbound.id == "direct")
            .count(),
        1,
        "explicit direct outbound must not be duplicated"
    );
    assert_eq!(
        options.route.final_, "hy-a",
        "default final should pick the first non-direct outbound"
    );

    assert_eq!(options.dns.servers.len(), 2);
    assert_eq!(options.dns.final_, "cf");
    assert_eq!(options.dns.strategy, config::DomainStrategy::PreferIpv4);
    let resolver = options
        .route
        .default_domain_resolver
        .as_ref()
        .expect("resolver");
    assert_eq!(
        resolver.server, "cf",
        "route resolver must follow dns.final for [[dns.servers]]"
    );
    let udp = match &options.dns.servers[0].kind {
        DnsServerKind::Udp(o) => o,
        _ => panic!("dns server[0] is not UDP"),
    };
    assert_eq!(udp.via, "hy-a");
}

#[test]
fn parse_config_rejects_duplicate_declared_ids() {
    let duplicate_inbound = MINIMAL_CONFIG.replace(
        "[tun]\n",
        r#"[[inbounds]]
type = "tun"
id = "dup"
address = ["172.19.0.1/30"]

[[inbounds]]
type = "tun"
id = "dup"
address = ["172.19.0.2/30"]

[tun]
"#,
    );
    let err = config::parse_config(&duplicate_inbound).expect_err("duplicate inbound ids");
    assert!(err.to_string().contains("duplicate inbounds id: dup"));

    let duplicate_outbound = MINIMAL_CONFIG.replace(
        "[hysteria2]\n",
        r#"[[outbounds]]
type = "block"
id = "dup"

[[outbounds]]
type = "direct"
id = "dup"

[hysteria2]
"#,
    );
    let err = config::parse_config(&duplicate_outbound).expect_err("duplicate outbound ids");
    assert!(err.to_string().contains("duplicate outbounds id: dup"));

    let duplicate_dns = r#"
[log]
level = "info"

[[inbounds]]
type = "tun"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "hysteria2"
server = "example.com"
server_port = 443
password = "secret"
sni = "example.com"

[dns]
[[dns.servers]]
type = "local"
id = "dup"

[[dns.servers]]
type = "hosts"
id = "dup"
"#;
    let err = config::parse_config(duplicate_dns).expect_err("duplicate dns ids");
    assert!(err.to_string().contains("duplicate dns.servers id: dup"));
}

#[test]
fn parse_config_rejects_unknown_route_final() {
    let cfg = MINIMAL_CONFIG.replacen("final = \"hysteria2\"", "final = \"typo\"", 1);
    let err = config::parse_config(&cfg).expect_err("unknown route.final");
    assert!(
        err.to_string()
            .contains("route.final references unknown outbound id: typo"),
        "error = {err:?}"
    );
}

#[test]
fn parse_config_rejects_unknown_user_rule_outbound() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[route.rules]]\ndomain_suffix = [\"example.com\"]\noutbound = \"missing\"\n"
    );
    let err = config::parse_config(&cfg).expect_err("unknown route rule outbound");
    assert!(
        err.to_string()
            .contains("route.rules outbound references unknown outbound id: missing"),
        "error = {err:?}"
    );
}

#[test]
fn parse_config_rejects_unknown_dns_final() {
    let cfg = MINIMAL_CONFIG.replace(
        "[dns]\nserver = \"https://1.1.1.1/dns-query\"\n",
        r#"[dns]
final = "missing"

[[dns.servers]]
type = "udp"
id = "cf"
server = "1.1.1.1"
"#,
    );
    let err = config::parse_config(&cfg).expect_err("unknown dns final");
    assert!(
        err.to_string()
            .contains("dns.final references unknown server id: missing"),
        "error = {err:?}"
    );
}

#[test]
fn parse_config_rejects_unknown_dns_via() {
    let cfg = MINIMAL_CONFIG.replacen("[dns]\n", "[dns]\nvia = \"missing\"\n", 1);
    let err = config::parse_config(&cfg).expect_err("unknown dns via");
    assert!(
        err.to_string()
            .contains("dns.server via references unknown outbound id: missing"),
        "error = {err:?}"
    );
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_endpoint_id_that_duplicates_outbound_id() {
    let cfg = format!("{MINIMAL_CONFIG}\n{}", wg_endpoint_block(""));
    let cfg = cfg.replacen("id = \"wg-out\"", "id = \"hysteria2\"", 1);
    let err = config::parse_config(&cfg).expect_err("endpoint id collides with outbound id");
    assert!(
        err.to_string()
            .contains("duplicate outbound/endpoint id: hysteria2"),
        "error = {err:?}"
    );
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_urltest_child_that_is_endpoint_id() {
    let cfg = format!(
        r#"
[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "urltest"
id = "auto"
outbounds = ["wg-out", "direct"]

[dns]
server = "udp://1.1.1.1"

[route]
final = "auto"
{}
"#,
        wg_endpoint_block("")
    );
    let err = config::parse_config(&cfg).expect_err("urltest child must be an outbound id");
    assert!(
        err.to_string()
            .contains("urltest 'auto' references unknown outbound id: wg-out"),
        "error = {err:?}"
    );
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_accepts_endpoint_only_wireguard_without_legacy_hysteria() {
    let cfg = format!(
        r#"
[log]
level = "info"

[[inbounds]]
type = "tun"
id = "tun"
mtu = 1400
stack = "system"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
sniff = true
hijack_dns = true

[dns]
final = "default"
strategy = "ipv4_only"

[[dns.servers]]
type = "udp"
id = "default"
server = "1.1.1.1"
via = "direct"

{}

[route]
final = "wg-out"
auto_detect_interface = true
"#,
        wg_endpoint_block("")
    );

    let options = config::parse_config(&cfg).expect("endpoint-only wireguard config should parse");
    assert_eq!(options.endpoints.len(), 1);
    assert_eq!(options.endpoints[0].id, "wg-out");
    assert_eq!(options.route.final_, "wg-out");
    assert!(
        options
            .outbounds
            .iter()
            .any(|outbound| outbound.id == "direct"),
        "endpoint-only configs should still synthesize direct outbound"
    );
}

#[test]
fn format_config_round_trips_and_strips_unknown() {
    let formatted = config::format_config(MINIMAL_CONFIG).expect("format");
    assert!(!formatted.is_empty());
    assert!(
        !formatted.to_lowercase().contains("[profile]"),
        "formatted contains profile section: {formatted}"
    );
}

fn assert_rule_action(action: &RuleActionKind, want: &'static str) {
    assert_eq!(action.name(), want, "rule action mismatch");
}

// `BASE64(0x01 * 32)` — placeholder Curve25519 private key for parser tests.
#[cfg(feature = "wireguard")]
const TEST_WG_PRIVATE_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
// `BASE64(0x02 * 32)` — placeholder peer public key.
#[cfg(feature = "wireguard")]
const TEST_WG_PEER_PUBLIC_KEY: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=";
// `BASE64(0x03 * 32)` — placeholder pre-shared key.
#[cfg(feature = "wireguard")]
const TEST_WG_PSK: &str = "AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM=";

// `extra` is appended after the built-in peer so that callers can tack on
// further `[[endpoints.peers]]` blocks without violating TOML's "table headers
// terminate the previous table" rule.
#[cfg(feature = "wireguard")]
fn wg_endpoint_block(extra: &str) -> String {
    format!(
        r#"
[[endpoints]]
type = "wireguard"
id = "wg-out"
private_key = "{TEST_WG_PRIVATE_KEY}"
mtu = 1408
address = ["10.66.0.2/32"]

[[endpoints.peers]]
public_key = "{TEST_WG_PEER_PUBLIC_KEY}"
address = "1.2.3.4"
port = 51820
allowed_ips = ["0.0.0.0/0"]
persistent_keepalive_interval = 25
{extra}
"#
    )
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_accepts_wireguard_endpoint() {
    let cfg = format!("{MINIMAL_CONFIG}\n{}", wg_endpoint_block(""));
    let options = config::parse_config(&cfg).expect("parse");
    assert_eq!(options.endpoints.len(), 1);
    let endpoint = &options.endpoints[0];
    assert_eq!(endpoint.id, "wg-out");
    assert_eq!(endpoint.type_name(), "wireguard");
    let EndpointKind::Wireguard(wg) = &endpoint.kind;
    assert_eq!(wg.private_key, [1u8; 32]);
    assert_eq!(endpoint.interface.mtu, 1408);
    assert_eq!(wg.listen_port, 0);
    assert_eq!(endpoint.interface.address.len(), 1);
    assert_eq!(endpoint.interface.address[0].to_string(), "10.66.0.2/32");
    assert_eq!(wg.peers.len(), 1);
    let peer = &wg.peers[0];
    assert_eq!(peer.public_key, [2u8; 32]);
    assert!(peer.pre_shared_key.is_none());
    assert_eq!(peer.endpoint.to_string(), "1.2.3.4:51820");
    assert_eq!(peer.allowed_ips.len(), 1);
    assert_eq!(peer.allowed_ips[0].to_string(), "0.0.0.0/0");
    assert_eq!(peer.persistent_keepalive, Some(Duration::from_secs(25)));
    assert_eq!(peer.reserved, [0, 0, 0]);
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_supports_multi_peer_wireguard() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n{}",
        wg_endpoint_block(&format!(
            r#"
[[endpoints.peers]]
public_key = "{TEST_WG_PSK}"
address = "5.6.7.8"
port = 51821
allowed_ips = ["192.168.0.0/16", "fd00::/8"]
"#
        ))
    );
    let options = config::parse_config(&cfg).expect("parse");
    let EndpointKind::Wireguard(wg) = &options.endpoints[0].kind;
    assert_eq!(wg.peers.len(), 2);
    assert_eq!(wg.peers[1].endpoint.to_string(), "5.6.7.8:51821");
    assert_eq!(wg.peers[1].allowed_ips.len(), 2);
    assert!(wg.peers[1].persistent_keepalive.is_none());
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_round_trips_wireguard_pre_shared_key_and_reserved() {
    let cfg = format!(
        r#"{MINIMAL_CONFIG}

[[endpoints]]
type = "wireguard"
id = "wg-out"
private_key = "{TEST_WG_PRIVATE_KEY}"
address = ["10.66.0.2/32"]
listen_port = 12345

[[endpoints.peers]]
public_key = "{TEST_WG_PEER_PUBLIC_KEY}"
pre_shared_key = "{TEST_WG_PSK}"
address = "1.2.3.4"
port = 51820
allowed_ips = ["0.0.0.0/0"]
reserved = [255, 0, 128]
"#
    );
    let options = config::parse_config(&cfg).expect("parse");
    let EndpointKind::Wireguard(wg) = &options.endpoints[0].kind;
    assert_eq!(wg.listen_port, 12345);
    assert_eq!(
        options.endpoints[0].interface.mtu, 1408,
        "mtu defaults to sing-box's 1408 when omitted"
    );
    assert_eq!(wg.peers[0].pre_shared_key, Some([3u8; 32]));
    assert_eq!(wg.peers[0].reserved, [255, 0, 128]);
}

#[cfg(feature = "amneziawg")]
#[test]
fn parse_config_accepts_wireguard_amnezia2_endpoint() {
    let cfg = format!(
        r#"{MINIMAL_CONFIG}

[[endpoints]]
type = "wireguard"
id = "wg-out"
private_key = "{TEST_WG_PRIVATE_KEY}"
address = ["10.66.0.2/32"]

[endpoints.amnezia]
enabled = true
version = "2.0"
h1 = "11-21"
h2 = "22-32"
h3 = "33-43"
h4 = "44-54"
s1 = 21
s2 = 22
s3 = 23
s4 = 24
jc = 3
jmin = 64
jmax = 80
i1 = "5-20"
i2 = "10-25"
i3 = "15-30"
i4 = "20-35"
i5 = "25-40"

[[endpoints.peers]]
public_key = "{TEST_WG_PEER_PUBLIC_KEY}"
address = "1.2.3.4"
port = 51820
allowed_ips = ["0.0.0.0/0"]
"#
    );
    let options = config::parse_config(&cfg).expect("parse");
    let EndpointKind::Wireguard(wg) = &options.endpoints[0].kind;
    let amnezia = wg.amnezia.as_ref().expect("amnezia should be parsed");
    assert!(amnezia.enabled);
    assert_eq!(amnezia.h1.min, 11);
    assert_eq!(amnezia.h1.max, 21);
    assert_eq!(amnezia.s4, 24);
    assert_eq!(amnezia.jmax, 80);
    assert_eq!(amnezia.i5.as_deref(), Some("25-40"));
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_wireguard_without_id() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[endpoints]]\ntype = \"wireguard\"\nprivate_key = \"{TEST_WG_PRIVATE_KEY}\"\naddress = [\"10.66.0.2/32\"]\n[[endpoints.peers]]\npublic_key = \"{TEST_WG_PEER_PUBLIC_KEY}\"\naddress = \"1.2.3.4\"\nport = 51820\nallowed_ips = [\"0.0.0.0/0\"]\n"
    );
    let err = config::parse_config(&cfg).expect_err("must require id");
    assert!(err.to_string().contains("endpoints[0].id"), "got {err:?}");
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_wireguard_without_peers() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[endpoints]]\ntype = \"wireguard\"\nid = \"wg-out\"\nprivate_key = \"{TEST_WG_PRIVATE_KEY}\"\naddress = [\"10.66.0.2/32\"]\n"
    );
    let err = config::parse_config(&cfg).expect_err("must require peers");
    assert!(
        err.to_string().contains("must contain at least one peer"),
        "got {err:?}"
    );
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_wireguard_invalid_base64_key() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[endpoints]]\ntype = \"wireguard\"\nid = \"wg-out\"\nprivate_key = \"not-base64!!\"\naddress = [\"10.66.0.2/32\"]\n[[endpoints.peers]]\npublic_key = \"{TEST_WG_PEER_PUBLIC_KEY}\"\naddress = \"1.2.3.4\"\nport = 51820\nallowed_ips = [\"0.0.0.0/0\"]\n"
    );
    let err = config::parse_config(&cfg).expect_err("must reject bad base64");
    assert!(err.to_string().contains("endpoints[0]"), "got {err:?}");
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_wireguard_peer_with_hostname_endpoint() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[endpoints]]\ntype = \"wireguard\"\nid = \"wg-out\"\nprivate_key = \"{TEST_WG_PRIVATE_KEY}\"\naddress = [\"10.66.0.2/32\"]\n[[endpoints.peers]]\npublic_key = \"{TEST_WG_PEER_PUBLIC_KEY}\"\naddress = \"vpn.example.com\"\nport = 51820\nallowed_ips = [\"0.0.0.0/0\"]\n"
    );
    let err = config::parse_config(&cfg).expect_err("peer hostnames are not yet supported");
    assert!(err.to_string().contains("IP literal"), "got {err:?}");
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_rejects_wireguard_peer_with_zero_port() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[endpoints]]\ntype = \"wireguard\"\nid = \"wg-out\"\nprivate_key = \"{TEST_WG_PRIVATE_KEY}\"\naddress = [\"10.66.0.2/32\"]\n[[endpoints.peers]]\npublic_key = \"{TEST_WG_PEER_PUBLIC_KEY}\"\naddress = \"1.2.3.4\"\nport = 0\nallowed_ips = [\"0.0.0.0/0\"]\n"
    );
    let err = config::parse_config(&cfg).expect_err("zero port must be rejected");
    assert!(
        err.to_string().contains("port must be non-zero"),
        "got {err:?}"
    );
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_default_endpoints_is_empty() {
    let options = config::parse_config(MINIMAL_CONFIG).expect("parse");
    assert!(
        options.endpoints.is_empty(),
        "endpoints default must be empty"
    );
}

#[test]
fn parse_config_rejects_smoltcp_tun_stack() {
    let cfg = MINIMAL_CONFIG.replace("stack = \"system\"", "stack = \"smoltcp\"");
    let err = config::parse_config(&cfg).expect_err("smoltcp stack is no longer supported");
    assert!(err.to_string().contains("smoltcp"), "got {err:?}");
}

/// Two-server fixture used by the domain_resolver suite below. Bootstrap is
/// an IP-literal UDP server on `direct`; remote is a domain UDP server whose
/// `domain_resolver` (or the route-level fallback) decides who resolves it.
const DOMAIN_RESOLVER_FIXTURE: &str = r#"
[log]
level = "info"

[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]
sniff = true
hijack_dns = true

[[outbounds]]
type = "direct"
id = "direct"

[[dns.servers]]
type = "udp"
id = "bootstrap"
server = "1.1.1.1"

[[dns.servers]]
type = "udp"
id = "remote"
server = "resolver.example.com"
via = "direct"
domain_resolver = "bootstrap"

[dns]
final = "remote"

[route]
final = "direct"
"#;

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_accepts_dns_via_endpoint_id() {
    // Endpoint ids are valid `via` targets — the runtime registers an
    // `EndpointOutboundAdapter` per endpoint so the lookup resolves through
    // the OutboundManager just like any other outbound.
    let cfg = format!(
        "{}\n{}",
        DOMAIN_RESOLVER_FIXTURE.replace("via = \"direct\"", "via = \"wg-out\""),
        wg_endpoint_block("")
    );
    let options = config::parse_config(&cfg).expect("dns via endpoint must parse");
    let remote = options
        .dns
        .servers
        .iter()
        .find(|s| s.id == "remote")
        .expect("remote dns server");
    assert_eq!(remote.via(), "wg-out");
}

#[test]
fn parse_config_still_rejects_dns_via_unknown() {
    // Negative path: a `via` that matches neither an outbound nor an
    // endpoint must still fail at parse time.
    let cfg = DOMAIN_RESOLVER_FIXTURE.replace("via = \"direct\"", "via = \"ghost\"");
    let err = config::parse_config(&cfg).expect_err("via=ghost must reject");
    assert!(
        err.to_string()
            .contains("dns.server via references unknown outbound id: ghost"),
        "got {err:?}"
    );
}

#[test]
fn parse_config_round_trips_dns_server_domain_resolver() {
    let options = config::parse_config(DOMAIN_RESOLVER_FIXTURE).expect("parse");
    let remote = options
        .dns
        .servers
        .iter()
        .find(|s| s.id == "remote")
        .expect("remote dns server");
    let kind = match &remote.kind {
        DnsServerKind::Udp(o) => o,
        _ => panic!("remote is not UDP"),
    };
    assert_eq!(kind.domain_resolver, "bootstrap");
    assert_eq!(kind.via, "direct");
}

#[test]
fn parse_config_rejects_unknown_dns_server_domain_resolver_tag() {
    let cfg = DOMAIN_RESOLVER_FIXTURE.replace(
        "domain_resolver = \"bootstrap\"",
        "domain_resolver = \"nope\"",
    );
    let err = config::parse_config(&cfg).expect_err("unknown tag must error");
    let msg = err.to_string();
    assert!(
        msg.contains("domain_resolver references unknown dns.server id"),
        "unexpected: {msg}"
    );
}

#[test]
fn parse_config_requires_domain_resolver_for_domain_via_server() {
    // Strip per-server domain_resolver and route-level fallback. The
    // remote server is domain + via -> strict validation must reject.
    let cfg = DOMAIN_RESOLVER_FIXTURE.replace("domain_resolver = \"bootstrap\"\n", "");
    let err = config::parse_config(&cfg).expect_err("missing bootstrap must error");
    let msg = err.to_string();
    assert!(
        msg.contains("domain server with via requires"),
        "unexpected: {msg}"
    );
}

#[test]
fn parse_config_accepts_default_domain_resolver_as_fallback() {
    // Per-server unset, but route.default_domain_resolver provides
    // the fallback bootstrap.
    let cfg = DOMAIN_RESOLVER_FIXTURE
        .replace("domain_resolver = \"bootstrap\"\n", "")
        .replace(
            "[route]\nfinal = \"direct\"\n",
            "[route]\nfinal = \"direct\"\ndefault_domain_resolver = \"bootstrap\"\n",
        );
    let options = config::parse_config(&cfg).expect("default fallback must validate");
    let resolver = options
        .route
        .default_domain_resolver
        .as_ref()
        .expect("default_domain_resolver round-trips");
    assert_eq!(resolver.server, "bootstrap");
}

#[test]
fn parse_config_rejects_dns_bootstrap_cycle() {
    // Two domain-via servers pointing at each other -> cycle.
    let cfg = r#"
[log]
level = "info"

[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]
sniff = true
hijack_dns = true

[[outbounds]]
type = "direct"
id = "direct"

[[dns.servers]]
type = "udp"
id = "a"
server = "a.example.com"
via = "direct"
domain_resolver = "b"

[[dns.servers]]
type = "udp"
id = "b"
server = "b.example.com"
via = "direct"
domain_resolver = "a"

[dns]
final = "a"

[route]
final = "direct"
"#;
    let err = config::parse_config(cfg).expect_err("cycle must error");
    let msg = err.to_string();
    assert!(
        msg.contains("dns.server bootstrap cycle"),
        "unexpected: {msg}"
    );
}

#[test]
fn urltest_outbound_parses_with_defaults() {
    let cfg = r#"
[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "direct"
id = "direct-2"

[[outbounds]]
type = "urltest"
id = "auto"
outbounds = ["direct", "direct-2"]

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "auto"
"#;
    let opts = config::parse_config(cfg).expect("parse urltest");
    let urltest = opts
        .outbounds
        .iter()
        .find(|o| o.id == "auto")
        .expect("auto outbound");
    let OutboundKind::Urltest(opts) = &urltest.kind else {
        panic!("auto is not urltest");
    };
    assert_eq!(opts.outbounds, vec!["direct", "direct-2"]);
    assert_eq!(opts.url.as_str(), "https://www.gstatic.com/generate_204");
    assert_eq!(opts.tolerance, Duration::from_millis(50));
    assert_eq!(opts.timeout, Duration::from_secs(5));
}

#[test]
fn urltest_rejects_unknown_child_id() {
    let cfg = r#"
[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "urltest"
id = "auto"
outbounds = ["direct", "missing"]

[route]
final = "auto"
"#;
    let err = config::parse_config(cfg).expect_err("dangling child should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("references unknown outbound id: missing"),
        "unexpected: {msg}"
    );
}

#[test]
fn urltest_rejects_nested_urltest() {
    let cfg = r#"
[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "urltest"
id = "inner"
outbounds = ["direct"]

[[outbounds]]
type = "urltest"
id = "outer"
outbounds = ["inner"]

[route]
final = "outer"
"#;
    let err = config::parse_config(cfg).expect_err("nested urltest should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("cannot nest another urltest"),
        "unexpected: {msg}"
    );
}

#[test]
fn urltest_rejects_self_reference() {
    let cfg = r#"
[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "urltest"
id = "auto"
outbounds = ["direct", "auto"]

[route]
final = "auto"
"#;
    let err = config::parse_config(cfg).expect_err("self-reference should fail");
    let msg = err.to_string();
    assert!(msg.contains("cannot reference itself"), "unexpected: {msg}");
}

#[test]
fn urltest_rejects_duplicate_child_id() {
    let cfg = r#"
[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[[outbounds]]
type = "urltest"
id = "auto"
outbounds = ["direct", "direct"]

[route]
final = "auto"
"#;
    let err = config::parse_config(cfg).expect_err("duplicate child should fail");
    let msg = err.to_string();
    assert!(msg.contains("duplicate child id"), "unexpected: {msg}");
}

mod matches {
    use super::*;

    pub fn assert_inbound_tun(kind: &InboundKind) {
        match kind {
            InboundKind::Tun(_) => {}
            _ => panic!("inbound not tun"),
        }
    }

    #[allow(dead_code)]
    pub fn outbound_kind(kind: &OutboundKind) -> &'static str {
        match kind {
            OutboundKind::Hysteria2(_) => "hysteria2",
            #[cfg(feature = "vless")]
            OutboundKind::Vless(_) => "vless",
            OutboundKind::Direct(_) => "direct",
            OutboundKind::Block => "block",
            OutboundKind::Urltest(_) => "urltest",
        }
    }
}
