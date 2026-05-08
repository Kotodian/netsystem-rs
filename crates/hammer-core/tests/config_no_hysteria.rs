#![cfg(not(feature = "outbound-hysteria2"))]

use hammer_core::config::{self, OutboundKind};
use indoc::indoc;

#[test]
fn parse_config_defaults_to_direct_without_hysteria2_feature() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]
        route_address = ["0.0.0.0/0"]

        [dns]
        server = "local"
    "#};

    let options = config::parse_config(cfg).expect("parse direct-only config");

    assert_eq!(options.outbounds.len(), 1);
    assert_eq!(options.outbounds[0].id, "direct");
    assert!(matches!(options.outbounds[0].kind, OutboundKind::Direct(_)));
    assert_eq!(options.route.final_, "direct");
}

#[test]
fn parse_config_rejects_top_level_hysteria2_without_feature() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]
        route_address = ["0.0.0.0/0"]

        [hysteria2]
        server = "example.com"
        server_port = 443
        password = "secret"
    "#};

    let err = config::parse_config(cfg).expect_err("accepted disabled top-level hysteria2 config");
    let msg = err.to_string();

    assert!(
        msg.contains("unsupported config key: hysteria2"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_rejects_hysteria2_outbound_without_feature() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]
        route_address = ["0.0.0.0/0"]

        [[outbounds]]
        type = "hysteria2"
        id = "hy2"
        server = "example.com"
        server_port = 443
        password = "secret"
    "#};

    let err = config::parse_config(cfg).expect_err("accepted disabled hysteria2 outbound");
    let msg = err.to_string();

    assert!(msg.contains("hysteria2"), "error = {msg:?}");
}
