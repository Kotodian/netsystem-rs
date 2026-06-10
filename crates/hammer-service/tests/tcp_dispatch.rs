use hammer_service::transport::tcp::{
    TcpDispatchTable, TcpInputError, TcpInputFlags, TcpInputNext, TcpState,
};

#[test]
fn tcp_dispatch_table_routes_listen_syn_to_listen_next() {
    let table = TcpDispatchTable::default();
    let entry = table.entry(TcpState::Listen, TcpInputFlags::SYN);

    assert_eq!(entry.next, TcpInputNext::Listen);
    assert_eq!(entry.error, None);
}

#[test]
fn tcp_dispatch_table_routes_listen_ack_to_reset_next() {
    let table = TcpDispatchTable::default();
    let entry = table.entry(TcpState::Listen, TcpInputFlags::ACK);

    assert_eq!(entry.next, TcpInputNext::Reset);
    assert_eq!(entry.error, Some(TcpInputError::AckInvalid));
}

#[test]
fn tcp_dispatch_table_matches_expected_state_flag_matrix() {
    let table = TcpDispatchTable::default();
    let cases = [
        (
            TcpState::Listen,
            TcpInputFlags::SYN,
            TcpInputNext::Listen,
            None,
        ),
        (
            TcpState::Listen,
            TcpInputFlags::ACK,
            TcpInputNext::Reset,
            Some(TcpInputError::AckInvalid),
        ),
        (
            TcpState::SynSent,
            TcpInputFlags::SYN | TcpInputFlags::ACK,
            TcpInputNext::SynSent,
            None,
        ),
        (
            TcpState::SynRcvd,
            TcpInputFlags::ACK,
            TcpInputNext::Listen,
            None,
        ),
        (
            TcpState::Established,
            TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::Established,
            TcpInputFlags::RST,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::Established,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::Closed,
            TcpInputFlags::RST,
            TcpInputNext::Drop,
            Some(TcpInputError::ConnectionClosed),
        ),
    ];

    for (state, flags, expected_next, expected_error) in cases {
        let entry = table.entry(state, flags);
        assert_eq!(
            entry.next, expected_next,
            "unexpected next for {state:?} + {flags:?}"
        );
        assert_eq!(
            entry.error, expected_error,
            "unexpected error for {state:?} + {flags:?}"
        );
    }
}
