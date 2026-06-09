use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_adapter::DataWorkerId;
use hammer_service::transport::tcp::{
    TcpLookupKind, TcpV4ConnectionKey, TcpV4ListenerKey, TcpV4PendingConnectionKey,
    TcpV6ConnectionKey, TcpV6ListenerKey, TcpWorkerOwnedState,
};

#[test]
fn tcp_lookup_prefers_established_connection_before_listener() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(3));
    let listener_key = TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 10), 443);
    let connection_key = TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(192, 0, 2, 10),
        443,
        Ipv4Addr::new(198, 51, 100, 20),
        54_000,
    );
    owner.insert_listener_v4(listener_key, 10);
    owner.insert_connection_v4(connection_key, 20);
    assert_eq!(owner.owner_worker(), DataWorkerId::new(3));

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_v4(
            connection_key,
            TcpV4PendingConnectionKey::new(0, 443, Ipv4Addr::new(198, 51, 100, 20), 54_001),
            listener_key,
        )
        .expect("lookup should match established connection");

    assert_eq!(lookup.kind, TcpLookupKind::EstablishedConnection);
    assert_eq!(lookup.id, 20);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(3));
}

#[test]
fn tcp_lookup_returns_owner_worker_for_connection() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(7));
    let connection_key = TcpV4ConnectionKey::new(
        4,
        Ipv4Addr::new(203, 0, 113, 9),
        8443,
        Ipv4Addr::new(198, 51, 100, 33),
        51_234,
    );
    owner.insert_connection_v4(connection_key, 77);

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_connection_v4(connection_key)
        .expect("connection lookup should exist");

    assert_eq!(lookup.kind, TcpLookupKind::EstablishedConnection);
    assert_eq!(lookup.id, 77);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(7));
}

#[test]
fn tcp_lookup_listener_snapshot_returns_owner_worker_and_kind_deterministically() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(11));
    let listener_key = TcpV6ListenerKey::new(9, "2001:db8::10".parse::<Ipv6Addr>().unwrap(), 443);
    let miss_connection_key = TcpV6ConnectionKey::new(
        9,
        "2001:db8::10".parse::<Ipv6Addr>().unwrap(),
        443,
        "2001:db8::20".parse::<Ipv6Addr>().unwrap(),
        60_000,
    );
    owner.insert_listener_v6(listener_key, 101);

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_v6(
            miss_connection_key,
            hammer_service::transport::tcp::TcpV6PendingConnectionKey::new(
                9,
                443,
                "2001:db8::20".parse::<Ipv6Addr>().unwrap(),
                60_001,
            ),
            listener_key,
        )
        .expect("lookup should fall back to listener");

    assert_eq!(lookup.kind, TcpLookupKind::Listener);
    assert_eq!(lookup.id, 101);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(11));
}

#[test]
fn tcp_lookup_returns_owner_worker_for_ipv6_connection_deterministically() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(12));
    let connection_key = TcpV6ConnectionKey::new(
        2,
        "2001:db8::100".parse::<Ipv6Addr>().unwrap(),
        9443,
        "2001:db8::200".parse::<Ipv6Addr>().unwrap(),
        40_123,
    );
    owner.insert_connection_v6(connection_key, 202);

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_connection_v6(connection_key)
        .expect("IPv6 connection lookup should exist");

    assert_eq!(lookup.kind, TcpLookupKind::EstablishedConnection);
    assert_eq!(lookup.id, 202);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(12));
}

#[test]
fn tcp_lookup_falls_back_to_syn_sent_connection_before_listener() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(13));
    let listener_key = TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 44), 443);
    let miss_connection_key = TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(192, 0, 2, 44),
        40_144,
        Ipv4Addr::new(198, 51, 100, 44),
        443,
    );
    let syn_sent_key =
        TcpV4PendingConnectionKey::new(0, 40_144, Ipv4Addr::new(198, 51, 100, 44), 443);
    owner.insert_listener_v4(listener_key, 104);
    owner.insert_syn_sent_connection_v4(syn_sent_key, 204);

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_v4(miss_connection_key, syn_sent_key, listener_key)
        .expect("lookup should fall back to syn-sent connection before listener");

    assert_eq!(lookup.kind, TcpLookupKind::SynSentConnection);
    assert_eq!(lookup.id, 204);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(13));
}
