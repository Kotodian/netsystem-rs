use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionId, TcpConnectionKey, TcpControlPlaneAction, TcpListenerId,
    TcpListenerKey, TcpNegotiatedOptions, TcpState as SharedTcpState,
};
use hammer_service::transport::tcp::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionState, TcpListenerConfig,
};
use smoltcp::socket::tcp::CongestionControl as SmolTcpCongestionControl;

#[test]
fn tcp_connection_defaults_to_hammer_owned_bbr_without_smoltcp_fallback() {
    let registry = TcpCongestionRegistry::default();

    let connection = TcpConnectionState::new(&registry, None).expect("default bbr selection");

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Bbr
    );
    assert_eq!(connection.smoltcp_congestion_fallback(), None);
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
fn tcp_connection_override_to_reno_uses_smoltcp_reno_fallback() {
    let registry = TcpCongestionRegistry::default();
    let connection = TcpConnectionState::new(&registry, Some(TcpCongestionAlgorithm::Reno))
        .expect("reno selection should be accepted");

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Reno
    );
    assert_eq!(
        connection.smoltcp_congestion_fallback(),
        Some(SmolTcpCongestionControl::Reno)
    );
}

#[test]
fn tcp_connection_override_to_cubic_uses_smoltcp_cubic_fallback() {
    let registry = TcpCongestionRegistry::default();
    let connection = TcpConnectionState::new(&registry, Some(TcpCongestionAlgorithm::Cubic))
        .expect("cubic selection should be accepted");

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Cubic
    );
    assert_eq!(
        connection.smoltcp_congestion_fallback(),
        Some(SmolTcpCongestionControl::Cubic)
    );
}

#[test]
fn tcp_listener_override_to_reno_is_accepted_before_install_listener_action() {
    let registry = TcpCongestionRegistry::default();
    let listener = TcpListenerConfig::new().with_congestion_algorithm(TcpCongestionAlgorithm::Reno);
    let listener_id = TcpListenerId::new(23);
    let listener_key = TcpListenerKey::v4(0, "192.0.2.20".parse().expect("listener addr"), 8443);

    let action = listener
        .install_listener_action(&registry, listener_id, listener_key)
        .expect("reno must be accepted before installing listener");

    assert_eq!(
        action,
        TcpControlPlaneAction::InstallListener {
            listener_id,
            listener: listener_key,
            capabilities: TcpCapabilities::default(),
        }
    );
}

#[test]
fn tcp_listener_override_to_cubic_is_accepted_before_install_listener_action() {
    let registry = TcpCongestionRegistry::default();
    let listener =
        TcpListenerConfig::new().with_congestion_algorithm(TcpCongestionAlgorithm::Cubic);
    let listener_id = TcpListenerId::new(29);
    let listener_key = TcpListenerKey::v4(0, "192.0.2.30".parse().expect("listener addr"), 9443);

    let action = listener
        .install_listener_action(&registry, listener_id, listener_key)
        .expect("cubic must be accepted before installing listener");

    assert_eq!(
        action,
        TcpControlPlaneAction::InstallListener {
            listener_id,
            listener: listener_key,
            capabilities: TcpCapabilities::default(),
        }
    );
}

#[test]
fn tcp_connection_uses_registry_default_algorithm_for_smoltcp_backed_modes() {
    let reno = TcpConnectionState::new(
        &TcpCongestionRegistry::new(TcpCongestionAlgorithm::Reno),
        None,
    )
    .expect("reno default should be accepted");
    let cubic = TcpConnectionState::new(
        &TcpCongestionRegistry::new(TcpCongestionAlgorithm::Cubic),
        None,
    )
    .expect("cubic default should be accepted");

    assert_eq!(
        reno.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Reno
    );
    assert_eq!(
        reno.smoltcp_congestion_fallback(),
        Some(SmolTcpCongestionControl::Reno)
    );
    assert_eq!(
        cubic.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Cubic
    );
    assert_eq!(
        cubic.smoltcp_congestion_fallback(),
        Some(SmolTcpCongestionControl::Cubic)
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
