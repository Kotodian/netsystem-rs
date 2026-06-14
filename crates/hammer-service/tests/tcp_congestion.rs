use hammer_service::transport::tcp::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionConfigState,
};

#[test]
fn tcp_connection_defaults_to_hammer_owned_congestion_control() {
    let registry = TcpCongestionRegistry::new(TcpCongestionAlgorithm::Hammer);

    assert_eq!(
        registry
            .selected_algorithm(None)
            .expect("registry default congestion control selection"),
        registry.default_algorithm()
    );

    let connection = TcpConnectionConfigState::new(&registry, None)
        .expect("default congestion control selection");

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Hammer
    );
}

#[test]
fn tcp_connection_override_to_reno_is_rejected_until_hammer_tcp_node_support_exists() {
    let registry = TcpCongestionRegistry::default();

    let err = TcpConnectionConfigState::new(&registry, Some(TcpCongestionAlgorithm::Reno))
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

    let err = TcpConnectionConfigState::new(&registry, Some(TcpCongestionAlgorithm::Cubic))
        .expect_err("cubic must wait for Hammer TCP node support");

    assert!(
        err.to_string()
            .contains("Hammer-owned congestion controller"),
        "unexpected err={err}"
    );
}

#[test]
fn tcp_connection_rejects_registry_default_until_hammer_tcp_node_support_exists() {
    let reno = TcpConnectionConfigState::new(
        &TcpCongestionRegistry::new(TcpCongestionAlgorithm::Reno),
        None,
    )
    .expect_err("reno default must wait for Hammer TCP node support");
    let cubic = TcpConnectionConfigState::new(
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
