#![cfg(feature = "vless")]

use std::time::Duration;

use hammer_core::Network;
use hammer_core::config::{self, EchConfigSource, OutboundKind, RealityShortId, UtlsFingerprint};
use indoc::indoc;

#[test]
fn parse_config_accepts_vless_reality_vision_outbound() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]
        route_address = ["0.0.0.0/0"]

        [[outbounds]]
        type = "vless"
        id = "vl-reality"
        server = "edge.example.com"
        server_port = 443
        uuid = "00112233-4455-6677-8899-aabbccddeeff"
        flow = "xtls-rprx-vision"
        network = ["tcp"]

        [outbounds.tls]
        enabled = true
        server_name = "www.example.com"
        alpn = ["h2", "http/1.1"]

        [outbounds.tls.utls]
        enabled = true
        fingerprint = "chrome"

        [outbounds.tls.reality]
        enabled = true
        public_key = "0000000000000000000000000000000000000000000000000000000000000000"
        short_id = "0a0b"

        [dns]
        server = "local"
    "#};

    let options = config::parse_config(cfg).expect("parse vless config");

    assert_eq!(options.route.final_, "vl-reality");
    assert_eq!(options.outbounds.len(), 2, "direct is synthesized");
    assert_eq!(options.outbounds[0].type_name(), "vless");
    let vless = match &options.outbounds[0].kind {
        OutboundKind::Vless(options) => options,
        other => panic!("outbound[0] not vless: {other:?}"),
    };
    assert_eq!(vless.server, "edge.example.com");
    assert_eq!(vless.server_port, 443);
    assert_eq!(
        vless.uuid,
        [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]
    );
    assert_eq!(vless.flow.as_deref(), Some("xtls-rprx-vision"));
    assert_eq!(vless.network, vec![Network::Tcp]);
    assert!(vless.tls.enabled);
    assert_eq!(vless.tls.server_name, "www.example.com");
    assert_eq!(vless.tls.alpn, vec!["h2".to_owned(), "http/1.1".to_owned()]);
    assert_eq!(
        vless.tls.utls.as_ref().expect("utls").fingerprint,
        UtlsFingerprint::Chrome
    );
    let reality = vless.tls.reality.as_ref().expect("reality");
    assert_eq!(reality.public_key.0, [0u8; 32]);
    assert_eq!(reality.short_id, RealityShortId(vec![0x0a, 0x0b]));
}

#[test]
fn parse_config_rejects_vless_vision_when_udp_is_enabled() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]

        [[outbounds]]
        type = "vless"
        id = "vl"
        server = "example.com"
        server_port = 443
        uuid = "00112233-4455-6677-8899-aabbccddeeff"
        flow = "xtls-rprx-vision"

        [outbounds.tls]
        enabled = true
        server_name = "example.com"

        [dns]
        server = "local"
    "#};

    let err = config::parse_config(cfg).expect_err("vless vision accepted default udp network");
    assert!(
        err.to_string()
            .contains("flow xtls-rprx-vision supports only tcp network"),
        "error = {err:?}"
    );
}

#[test]
fn parse_config_rejects_invalid_vless_uuid_and_flow() {
    let invalid_uuid = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]

        [[outbounds]]
        type = "vless"
        id = "vl"
        server = "example.com"
        server_port = 443
        uuid = "not-a-uuid"

        [dns]
        server = "local"
    "#};
    let err = config::parse_config(invalid_uuid).expect_err("invalid uuid accepted");
    assert!(
        err.to_string().contains("outbounds[0].uuid"),
        "error = {err:?}"
    );

    let invalid_flow = invalid_uuid.replace(
        "uuid = \"not-a-uuid\"",
        "uuid = \"00112233-4455-6677-8899-aabbccddeeff\"\nflow = \"xtls-rprx-direct\"",
    );
    let err = config::parse_config(&invalid_flow).expect_err("invalid flow accepted");
    assert!(
        err.to_string()
            .contains("outbounds[0] (vless 'vl') unsupported flow"),
        "error = {err:?}"
    );
}

#[test]
fn parse_config_accepts_vless_ech_and_tls_fragment_options() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]

        [[outbounds]]
        type = "vless"
        id = "vl"
        server = "example.com"
        server_port = 443
        uuid = "00112233-4455-6677-8899-aabbccddeeff"

        [outbounds.tls]
        enabled = true
        server_name = "example.com"

        [outbounds.tls.ech]
        enabled = true
        config = "AQIDBA=="
        dynamic_record_sizing_disabled = true

        [outbounds.tls.fragment]
        enabled = true
        size = "1-3"
        sleep = "5ms"

        [outbounds.tls.record_fragment]
        enabled = true

        [dns]
        server = "local"
    "#};

    let options = config::parse_config(cfg).expect("parse vless tls config");
    let vless = match &options.outbounds[0].kind {
        OutboundKind::Vless(options) => options,
        other => panic!("outbound[0] not vless: {other:?}"),
    };
    match &vless.tls.ech.as_ref().expect("ech").config_source {
        Some(EchConfigSource::Inline(config)) => assert_eq!(config.0, vec![1, 2, 3, 4]),
        other => panic!("expected inline ECH config, got {other:?}"),
    }
    assert!(
        vless
            .tls
            .ech
            .as_ref()
            .unwrap()
            .dynamic_record_sizing_disabled
    );
    assert_eq!(
        vless.tls.fragment.as_ref().expect("fragment").sleep,
        Duration::from_millis(5)
    );
    assert!(vless.tls.record_fragment);
}

#[test]
fn parse_config_accepts_vless_network_selection() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]

        [[outbounds]]
        type = "vless"
        id = "vl"
        server = "example.com"
        server_port = 443
        uuid = "00112233-4455-6677-8899-aabbccddeeff"
        network = ["tcp"]

        [dns]
        server = "local"
    "#};

    let options = config::parse_config(cfg).expect("parse vless network config");
    let vless = match &options.outbounds[0].kind {
        OutboundKind::Vless(options) => options,
        other => panic!("outbound[0] not vless: {other:?}"),
    };

    assert_eq!(vless.network, vec![Network::Tcp]);
}

#[test]
fn parse_config_rejects_vless_icmp_network() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]

        [[outbounds]]
        type = "vless"
        id = "vl"
        server = "example.com"
        server_port = 443
        uuid = "00112233-4455-6677-8899-aabbccddeeff"
        network = ["icmp"]

        [dns]
        server = "local"
    "#};

    let err = config::parse_config(cfg).expect_err("icmp network accepted");
    assert!(
        err.to_string()
            .contains("outbounds[0] (vless 'vl').network supports only tcp and udp"),
        "error = {err:?}"
    );
}
