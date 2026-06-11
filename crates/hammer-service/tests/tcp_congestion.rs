use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionId, TcpConnectionKey, TcpControlPlaneAction, TcpListenerId,
    TcpListenerKey, TcpNegotiatedOptions, TcpState as SharedTcpState,
};
use hammer_service::transport::tcp::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionState, TcpListenerConfig,
};

#[test]
fn tcp_connection_defaults_to_hammer_owned_bbr() {
    let registry = TcpCongestionRegistry::new(TcpCongestionAlgorithm::Bbr);

    assert_eq!(
        registry
            .selected_algorithm(None)
            .expect("registry default bbr selection"),
        registry.default_algorithm()
    );

    let connection = TcpConnectionState::new(&registry, None).expect("default bbr selection");

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Bbr
    );
}

#[test]
fn tcp_listener_config_builds_shared_install_listener_action_after_bbr_validation() {
    let registry = TcpCongestionRegistry::default();
    let listener_id = TcpListenerId::new(17);
    let listener = TcpListenerKey::v4(0, "192.0.2.10".parse().expect("listener addr"), 443);

    let action = TcpListenerConfig::new()
        .install_listener_action(&registry, listener_id, listener)
        .expect("default listener config should install with BBR");

    assert_eq!(
        action,
        TcpControlPlaneAction::InstallListener {
            listener_id,
            listener,
            capabilities: TcpCapabilities::default(),
        }
    );
}

#[test]
fn tcp_connection_override_to_reno_is_rejected_until_hammer_tcp_node_support_exists() {
    let registry = TcpCongestionRegistry::default();

    let err = TcpConnectionState::new(&registry, Some(TcpCongestionAlgorithm::Reno))
        .expect_err("reno must wait for Hammer TCP node support");

    assert!(
        err.to_string()
            .contains("Hammer-owned congestion controller"),
        "unexpected err={err}"
    );
}

#[test]
fn tcp_connection_override_to_cubic_is_rejected_until_hammer_tcp_node_support_exists() {
    let registry = TcpCongestionRegistry::default();

    let err = TcpConnectionState::new(&registry, Some(TcpCongestionAlgorithm::Cubic))
        .expect_err("cubic must wait for Hammer TCP node support");

    assert!(
        err.to_string()
            .contains("Hammer-owned congestion controller"),
        "unexpected err={err}"
    );
}

#[test]
fn tcp_listener_override_to_reno_is_rejected_before_install_listener_action() {
    let registry = TcpCongestionRegistry::default();
    let listener = TcpListenerConfig::new().with_congestion_algorithm(TcpCongestionAlgorithm::Reno);
    let listener_key = TcpListenerKey::v4(0, "192.0.2.20".parse().expect("listener addr"), 8443);

    let err = listener
        .install_listener_action(&registry, TcpListenerId::new(23), listener_key)
        .expect_err("reno must wait for Hammer TCP node support");

    assert!(
        err.to_string()
            .contains("Hammer-owned congestion controller"),
        "unexpected err={err}"
    );
}

#[test]
fn tcp_listener_override_to_cubic_is_rejected_before_install_listener_action() {
    let registry = TcpCongestionRegistry::default();
    let listener =
        TcpListenerConfig::new().with_congestion_algorithm(TcpCongestionAlgorithm::Cubic);
    let listener_key = TcpListenerKey::v4(0, "192.0.2.30".parse().expect("listener addr"), 9443);

    let err = listener
        .install_listener_action(&registry, TcpListenerId::new(29), listener_key)
        .expect_err("cubic must wait for Hammer TCP node support");

    assert!(
        err.to_string()
            .contains("Hammer-owned congestion controller"),
        "unexpected err={err}"
    );
}

#[test]
fn tcp_connection_rejects_registry_default_until_hammer_tcp_node_support_exists() {
    let reno = TcpConnectionState::new(
        &TcpCongestionRegistry::new(TcpCongestionAlgorithm::Reno),
        None,
    )
    .expect_err("reno default must wait for Hammer TCP node support");
    let cubic = TcpConnectionState::new(
        &TcpCongestionRegistry::new(TcpCongestionAlgorithm::Cubic),
        None,
    )
    .expect_err("cubic default must wait for Hammer TCP node support");

    assert!(
        reno.to_string()
            .contains("Hammer-owned congestion controller")
    );
    assert!(
        cubic
            .to_string()
            .contains("Hammer-owned congestion controller")
    );
}

#[test]
fn tcp_connection_state_builds_shared_install_connection_action() {
    let registry = TcpCongestionRegistry::default();
    let connection_id = TcpConnectionId::new(31);
    let key = TcpConnectionKey::v4(
        0,
        "192.0.2.40".parse().expect("local addr"),
        443,
        "198.51.100.40".parse().expect("remote addr"),
        54_000,
    );

    let action = TcpConnectionState::new(&registry, None)
        .expect("default BBR connection state")
        .install_connection_action(connection_id, key, SharedTcpState::Established);

    assert_eq!(
        action,
        TcpControlPlaneAction::InstallConnection {
            connection_id,
            key,
            state: SharedTcpState::Established,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        }
    );
}
