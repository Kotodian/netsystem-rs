use hammer_core::config::{self, InboundKind, OutboundKind};
use indoc::indoc;

#[test]
fn parse_config_accepts_tun_and_block_outbound() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]
        route_address = ["0.0.0.0/0"]

        [[outbounds]]
        type = "block"
        id = "block"

        [route]
        final = "block"
    "#};

    let options = config::parse_config(cfg).expect("parse block config");

    assert_eq!(options.outbounds.len(), 1);
    assert_eq!(options.outbounds[0].id, "block");
    assert!(matches!(options.outbounds[0].kind, OutboundKind::Block));
    let InboundKind::Tun(tun) = &options.inbounds[0].kind;
    assert!(!tun.tap);
    assert_eq!(options.route.final_, "block");
}

#[cfg(feature = "wireguard")]
#[test]
fn parse_config_accepts_endpoint_only_wireguard() {
    let cfg = indoc! {r#"
        [tun]
        address = ["172.19.0.1/30"]
        route_address = ["0.0.0.0/0"]

        [[endpoints]]
        type = "wireguard"
        id = "wg-out"
        private_key = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
        mtu = 1408
        address = ["10.66.0.2/32"]

        [[endpoints.peers]]
        public_key = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI="
        address = "1.2.3.4"
        port = 51820
        allowed_ips = ["0.0.0.0/0"]
        persistent_keepalive_interval = 25

        [route]
        final = "wg-out"
    "#};

    let options = config::parse_config(cfg).expect("parse endpoint-only config");

    assert!(options.outbounds.is_empty());
    assert_eq!(options.endpoints.len(), 1);
    assert_eq!(options.endpoints[0].id, "wg-out");
    assert_eq!(options.route.final_, "wg-out");
}
