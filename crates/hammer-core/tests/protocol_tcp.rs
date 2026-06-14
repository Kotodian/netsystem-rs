use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use hammer_core::ds::FlatHashTable;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpControlPlaneAction, TcpListenerId, TcpListenerKey, TcpSeq,
    TcpV6ListenerKey, TcpWorkerEvent,
};
use hammer_core::protocol::transport::TransportConnectionKey;

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
    let key = TransportConnectionKey::new(
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

    let generic = TransportConnectionKey::new(
        9,
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
        443,
        IpAddr::V4(Ipv4Addr::new(198, 51, 100, 20)),
        54_321,
    );
    assert_eq!(generic.scope_id(), 9);
    assert_eq!(generic.local_port(), 443);
    assert_eq!(generic.remote_port(), 54_321);
    assert_eq!(
        generic.reverse(),
        TransportConnectionKey::new(
            9,
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 20)),
            54_321,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            443,
        )
    );
}

#[test]
fn tcp_control_and_worker_messages_share_the_same_contract_types() {
    let listener = TcpListenerKey::V6(TcpV6ListenerKey::new(
        7,
        Ipv6Addr::new(0x2001, 0xdb8, 0, 7, 0, 0, 0, 10),
        443,
    ));
    let key = TransportConnectionKey::new(
        7,
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 7, 0, 0, 0, 10)),
        443,
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 8, 0, 0, 0, 20)),
        49_152,
    );
    let capabilities = TcpCapabilities {
        max_segment_size: Some(1440),
        window_scale: Some(7),
        sack: true,
        timestamps: true,
        ecn: false,
    };
    let install = TcpControlPlaneAction::InstallListener {
        listener_id: TcpListenerId::new(42),
        listener,
        capabilities,
    };
    match install {
        TcpControlPlaneAction::InstallListener {
            listener_id,
            listener: action_listener,
            capabilities: action_capabilities,
        } => {
            assert_eq!(listener_id.get(), 42);
            assert_eq!(action_listener, listener);
            assert_eq!(action_capabilities, capabilities);
        }
        other => panic!("unexpected action: {other:?}"),
    }

    let remove = TcpControlPlaneAction::RemoveListener {
        listener_id: TcpListenerId::new(42),
        reason: TcpCloseReason::LocalRequest,
    };
    assert!(matches!(
        remove,
        TcpControlPlaneAction::RemoveListener {
            listener_id,
            reason: TcpCloseReason::LocalRequest,
        } if listener_id.get() == 42
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
    }

    let _ = key;
}
