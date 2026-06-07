use hammer_service::transport::tcp::{
    TcpCongestionAlgorithm, TcpCongestionRegistry, TcpConnectionState, TcpListenerConfig,
};
use smoltcp::socket::tcp::CongestionControl as SmolTcpCongestionControl;

#[test]
fn tcp_connection_defaults_to_hammer_owned_bbr_without_smoltcp_fallback() {
    let registry = TcpCongestionRegistry::default();

    let connection = TcpConnectionState::new(&registry, None);

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Bbr
    );
    assert_eq!(connection.smoltcp_congestion_fallback(), None);
}

#[test]
fn tcp_listener_override_to_reno_exposes_smoltcp_fallback() {
    let registry = TcpCongestionRegistry::default();
    let listener = TcpListenerConfig::new().with_congestion_algorithm(TcpCongestionAlgorithm::Reno);

    let connection = TcpConnectionState::new(&registry, listener.congestion_algorithm());

    assert_eq!(
        connection.selected_congestion_algorithm(),
        TcpCongestionAlgorithm::Reno
    );
    assert_eq!(
        connection.smoltcp_congestion_fallback(),
        Some(SmolTcpCongestionControl::Reno)
    );
}
