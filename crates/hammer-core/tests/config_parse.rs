use std::time::Duration;

use hammer_core::config::{self, DnsServerKind, InboundKind, OutboundKind, RuleActionKind};

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
    assert_eq!(options.inbounds.len(), 1);
    assert_eq!(options.inbounds[0].type_name(), "tun");
    matches::assert_inbound_tun(&options.inbounds[0].kind);

    assert_eq!(options.route.rules.len(), 5, "expected 5 tun rules");
    assert_rule_action(&options.route.rules[0].default_options.action, "sniff");
    let sniff = match &options.route.rules[0].default_options.action {
        RuleActionKind::Sniff(o) => o,
        _ => panic!("rule[0] not Sniff"),
    };
    assert_eq!(sniff.timeout, Some(Duration::from_millis(300)));
    assert!(sniff.override_destination, "sniff override expected true");
    assert_eq!(
        options.route.rules[0].default_options.inbound,
        vec!["tun".to_owned()]
    );

    assert_rule_action(&options.route.rules[1].default_options.action, "hijack-dns");
    assert_eq!(
        options.route.rules[1].default_options.protocol,
        vec!["dns".to_owned()]
    );

    assert_rule_action(&options.route.rules[2].default_options.action, "reject");
    assert_eq!(
        options.route.rules[2].default_options.protocol,
        vec!["quic".to_owned()]
    );
    let reject = match &options.route.rules[2].default_options.action {
        RuleActionKind::Reject(o) => o,
        _ => panic!("rule[2] not Reject"),
    };
    assert_eq!(reject.method, "default");

    assert_rule_action(&options.route.rules[3].default_options.action, "resolve");
    let resolve = match &options.route.rules[3].default_options.action {
        RuleActionKind::Resolve(o) => o,
        _ => panic!("rule[3] not Resolve"),
    };
    assert_eq!(resolve.strategy, config::DomainStrategy::PreferIpv4);

    assert_rule_action(
        &options.route.rules[4].default_options.action,
        "route-options",
    );
    let route_opts = match &options.route.rules[4].default_options.action {
        RuleActionKind::RouteOptions(o) => o,
        _ => panic!("rule[4] not RouteOptions"),
    };
    assert!(route_opts.udp_disable_domain_unmapping);

    assert_eq!(options.outbounds.len(), 2);
    assert_eq!(options.outbounds[0].type_name(), "hysteria2");
    assert_eq!(options.outbounds[1].type_name(), "direct");
    assert_eq!(options.outbounds[1].tag, "direct");

    assert_eq!(options.route.final_, "hysteria2");

    assert_eq!(options.dns.servers.len(), 1);
    let https = match &options.dns.servers[0].kind {
        DnsServerKind::Https(o) => o,
        _ => panic!("dns server is not Https"),
    };
    assert_eq!(https.via, "direct");
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
fn parse_config_uses_custom_dns_id_for_domain_resolver() {
    let cfg = MINIMAL_CONFIG.replacen("[dns]\n", "[dns]\nid = \"primary\"\n", 1);
    let options = config::parse_config(&cfg).expect("parse");
    let resolver = options
        .route
        .default_domain_resolver
        .as_ref()
        .expect("DefaultDomainResolver must be present");
    assert_eq!(resolver.server, "primary");
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
        "{MINIMAL_CONFIG}\n[[route.rules]]\ndomain_suffix = [\"google.com\"]\nip_cidr = [\"8.8.8.8/32\"]\noutbound = \"hysteria2\"\n"
    );
    let options = config::parse_config(&cfg).expect("parse");
    assert_eq!(
        options.route.rules.len(),
        6,
        "expected 5 tun rules + 1 user rule"
    );
    let user = &options.route.rules[5].default_options;
    assert_eq!(user.domain_suffix, vec!["google.com".to_owned()]);
    assert_eq!(
        user.ip_cidr,
        vec!["8.8.8.8/32".parse::<ipnet::IpNet>().unwrap()]
    );
    let route = match &user.action {
        RuleActionKind::Route(o) => o,
        _ => panic!("user rule action is not Route"),
    };
    assert_eq!(route.outbound, "hysteria2");
}

#[test]
fn parse_config_rejects_invalid_ip_cidr() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n[[route.rules]]\nip_cidr = [\"not-a-cidr\"]\noutbound = \"hysteria2\"\n"
    );
    let err = config::parse_config(&cfg).expect_err("accepted invalid CIDR");
    let msg = err.to_string();
    assert!(
        msg.contains("route.rules[0].ip_cidr"),
        "error should pin user-visible rule index: {msg:?}"
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
        msg.contains("requires at least one matcher"),
        "error = {msg:?}"
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

mod matches {
    use super::*;

    pub fn assert_inbound_tun(kind: &InboundKind) {
        match kind {
            InboundKind::Tun(_) => {}
        }
    }

    #[allow(dead_code)]
    pub fn outbound_kind(kind: &OutboundKind) -> &'static str {
        match kind {
            OutboundKind::Hysteria2(_) => "hysteria2",
            OutboundKind::Direct(_) => "direct",
            OutboundKind::Block => "block",
            OutboundKind::Dns => "dns",
        }
    }
}
