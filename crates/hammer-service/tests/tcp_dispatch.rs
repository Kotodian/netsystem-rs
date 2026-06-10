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
        (
            TcpState::Closed,
            TcpInputFlags::ACK,
            TcpInputNext::Reset,
            Some(TcpInputError::ConnectionClosed),
        ),
        (
            TcpState::Closed,
            TcpInputFlags::SYN,
            TcpInputNext::Reset,
            Some(TcpInputError::ConnectionClosed),
        ),
        (
            TcpState::Closed,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::Reset,
            Some(TcpInputError::ConnectionClosed),
        ),
        (
            TcpState::Closed,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            TcpInputNext::Drop,
            Some(TcpInputError::ConnectionClosed),
        ),
        (
            TcpState::FinWait1,
            TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::FinWait1,
            TcpInputFlags::RST,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::FinWait1,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::FinWait1,
            TcpInputFlags::SYN,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::FinWait2,
            TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::FinWait2,
            TcpInputFlags::RST,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::FinWait2,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::CloseWait,
            TcpInputFlags::ACK,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::CloseWait,
            TcpInputFlags::RST,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::CloseWait,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::CloseWait,
            TcpInputFlags::SYN,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::Closing,
            TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::Closing,
            TcpInputFlags::RST,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::Closing,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::LastAck,
            TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::LastAck,
            TcpInputFlags::RST,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::LastAck,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::Established,
            None,
        ),
        (
            TcpState::TimeWait,
            TcpInputFlags::ACK,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::TimeWait,
            TcpInputFlags::RST,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::TimeWait,
            TcpInputFlags::FIN | TcpInputFlags::ACK,
            TcpInputNext::RcvProcess,
            None,
        ),
        (
            TcpState::TimeWait,
            TcpInputFlags::SYN,
            TcpInputNext::RcvProcess,
            None,
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

#[test]
fn tcp_dispatch_table_routes_closing_flag_supersets_to_established_when_state_updates_live_there() {
    let table = TcpDispatchTable::default();
    let cases = [
        (
            TcpState::FinWait1,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            TcpInputNext::Established,
        ),
        (
            TcpState::FinWait1,
            TcpInputFlags::ACK | TcpInputFlags::SYN,
            TcpInputNext::Established,
        ),
        (
            TcpState::FinWait2,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            TcpInputNext::Established,
        ),
        (
            TcpState::CloseWait,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            TcpInputNext::Established,
        ),
        (
            TcpState::Closing,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            TcpInputNext::Established,
        ),
        (
            TcpState::LastAck,
            TcpInputFlags::ACK | TcpInputFlags::RST,
            TcpInputNext::Established,
        ),
    ];

    for (state, flags, expected_next) in cases {
        let entry = table.entry(state, flags);
        assert_eq!(
            entry.next, expected_next,
            "unexpected next for {state:?} + {flags:?}"
        );
        assert_eq!(
            entry.error, None,
            "unexpected error for {state:?} + {flags:?}"
        );
    }
}
