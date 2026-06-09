use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use hammer_core::ds::FlatHashTable;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpConnectionKey, TcpControlPlaneAction,
    TcpListenerId, TcpListenerKey, TcpNegotiatedOptions, TcpSeq, TcpShutdownDirection, TcpState,
    TcpTimerId, TcpTimerKind, TcpV4ConnectionKey, TcpV6ConnectionKey, TcpV6ListenerKey,
    TcpWorkerEvent,
};

#[test]
fn tcp_seq_wraparound_order_and_advance_are_safe() {
    let before_wrap = TcpSeq::new(u32::MAX - 3);
    let after_wrap = before_wrap.advance(8);

    assert_eq!(after_wrap.raw(), 4);
    assert!(before_wrap.before(after_wrap));
    assert!(after_wrap.after(before_wrap));
    assert_eq!(before_wrap.distance_to(after_wrap), 8);
}

#[test]
fn tcp_connection_keys_reverse_direction_and_hash_for_lookup_tables() {
    let key = TcpV4ConnectionKey::new(
        9,
        Ipv4Addr::new(192, 0, 2, 10),
        443,
        Ipv4Addr::new(198, 51, 100, 20),
        54_321,
    );
    let reversed = key.reverse();
    let mut table = FlatHashTable::new();
    table.insert(key, 17u32);

    assert_eq!(table.lookup(&key), Some(17));
    assert_eq!(table.lookup(&reversed), None);

    let generic = TcpConnectionKey::V4(key);
    assert_eq!(generic.scope_id(), 9);
    assert_eq!(generic.local_port(), 443);
    assert_eq!(generic.remote_port(), 54_321);
    assert_eq!(generic.reverse(), TcpConnectionKey::V4(reversed));
}

#[test]
fn tcp_control_and_worker_messages_share_the_same_contract_types() {
    let listener = TcpListenerKey::V6(TcpV6ListenerKey::new(
        7,
        Ipv6Addr::new(0x2001, 0xdb8, 0, 7, 0, 0, 0, 10),
        443,
    ));
    let key = TcpConnectionKey::V6(TcpV6ConnectionKey::new(
        7,
        Ipv6Addr::new(0x2001, 0xdb8, 0, 7, 0, 0, 0, 10),
        443,
        Ipv6Addr::new(0x2001, 0xdb8, 0, 8, 0, 0, 0, 20),
        49_152,
    ));
    let capabilities = TcpCapabilities {
        max_segment_size: Some(1440),
        window_scale: Some(7),
        sack: true,
        timestamps: true,
        ecn: false,
    };
    let negotiated = TcpNegotiatedOptions {
        send_max_segment_size: Some(1440),
        receive_max_segment_size: Some(1380),
        send_window_scale: Some(7),
        receive_window_scale: Some(5),
        sack: true,
        timestamps: true,
        ecn: false,
    };

    let install = TcpControlPlaneAction::InstallConnection {
        connection_id: TcpConnectionId::new(42),
        key,
        state: TcpState::SynSent,
        capabilities,
        negotiated,
    };
    match install {
        TcpControlPlaneAction::InstallConnection {
            connection_id,
            key: action_key,
            state,
            capabilities: action_capabilities,
            negotiated: action_negotiated,
        } => {
            assert_eq!(connection_id.get(), 42);
            assert_eq!(action_key, key);
            assert_eq!(state, TcpState::SynSent);
            assert_eq!(action_capabilities, capabilities);
            assert_eq!(action_negotiated, negotiated);
        }
        other => panic!("unexpected action: {other:?}"),
    }

    let timer = TcpControlPlaneAction::ArmTimer {
        connection_id: TcpConnectionId::new(42),
        timer_id: TcpTimerId::new(9),
        kind: TcpTimerKind::Retransmit,
        timeout: Duration::from_millis(250),
    };
    match timer {
        TcpControlPlaneAction::ArmTimer {
            connection_id,
            timer_id,
            kind,
            timeout,
        } => {
            assert_eq!(connection_id.get(), 42);
            assert_eq!(timer_id.get(), 9);
            assert_eq!(kind, TcpTimerKind::Retransmit);
            assert_eq!(timeout, Duration::from_millis(250));
        }
        other => panic!("unexpected timer action: {other:?}"),
    }

    let shutdown = TcpControlPlaneAction::ShutdownConnection {
        connection_id: TcpConnectionId::new(42),
        direction: TcpShutdownDirection::Write,
        reason: TcpCloseReason::LocalShutdown,
    };
    assert!(matches!(
        shutdown,
        TcpControlPlaneAction::ShutdownConnection {
            direction: TcpShutdownDirection::Write,
            reason: TcpCloseReason::LocalShutdown,
            ..
        }
    ));

    let incoming = TcpWorkerEvent::IncomingConnection {
        listener_id: TcpListenerId::new(5),
        listener,
        key,
        capabilities,
    };
    match incoming {
        TcpWorkerEvent::IncomingConnection {
            listener_id,
            listener: incoming_listener,
            key: incoming_key,
            capabilities: incoming_capabilities,
        } => {
            assert_eq!(listener_id.get(), 5);
            assert_eq!(incoming_listener, listener);
            assert_eq!(incoming_key, key);
            assert_eq!(incoming_capabilities, capabilities);
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let state_changed = TcpWorkerEvent::StateChanged {
        connection_id: TcpConnectionId::new(42),
        key,
        state: TcpState::Established,
    };
    match state_changed {
        TcpWorkerEvent::StateChanged {
            connection_id,
            key: changed_key,
            state,
        } => {
            assert_eq!(connection_id.get(), 42);
            assert_eq!(changed_key, key);
            assert_eq!(state, TcpState::Established);
        }
        other => panic!("unexpected state event: {other:?}"),
    }

    let closed = TcpWorkerEvent::Closed {
        connection_id: TcpConnectionId::new(42),
        reason: TcpCloseReason::RemoteReset,
    };
    assert!(matches!(
        closed,
        TcpWorkerEvent::Closed {
            reason: TcpCloseReason::RemoteReset,
            ..
        }
    ));

    let expired = TcpWorkerEvent::TimerExpired {
        connection_id: TcpConnectionId::new(42),
        timer_id: TcpTimerId::new(9),
        kind: TcpTimerKind::Retransmit,
    };
    assert!(matches!(
        expired,
        TcpWorkerEvent::TimerExpired {
            timer_id,
            kind: TcpTimerKind::Retransmit,
            ..
        } if timer_id.get() == 9
    ));
}
