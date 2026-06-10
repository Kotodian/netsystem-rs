use std::net::SocketAddr;

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::transport::tcp::{
    TcpConnectionSnapshot, TcpState, TcpWorkerOwnedConnectionState,
};

#[test]
fn tcp_connection_snapshot_pool_returns_entries_by_lookup_and_connection_id() {
    let mut owner = TcpWorkerOwnedConnectionState::new(DataWorkerId::new(5));
    let remote: SocketAddr = "198.51.100.55:443".parse().expect("remote");
    let local: SocketAddr = "192.0.2.55:49152".parse().expect("local");

    owner.insert(TcpConnectionSnapshot {
        lookup_id: 41,
        connection_id: Some(TcpConnectionId::new(99)),
        owner_worker: DataWorkerId::new(5),
        state: TcpState::Established,
        local_port: local.port(),
        local: Some(local),
        remote,
        iss: 1_000,
        irs: 2_000,
        snd_una: 1_001,
        snd_nxt: 1_120,
        snd_wnd: 8_192,
        rcv_nxt: 2_001,
        rcv_wnd: 16_384,
    });

    let snapshot = owner.publish_snapshot();
    let by_lookup = snapshot
        .lookup_by_lookup_id(41)
        .expect("lookup-id entry should exist");
    let by_connection = snapshot
        .lookup_by_connection_id(TcpConnectionId::new(99))
        .expect("connection-id entry should exist");

    assert_eq!(by_lookup, by_connection);
    assert_eq!(by_lookup.owner_worker, DataWorkerId::new(5));
    assert_eq!(by_lookup.state, TcpState::Established);
    assert_eq!(by_lookup.local, Some(local));
    assert_eq!(by_lookup.remote, remote);
    assert_eq!(by_lookup.iss, 1_000);
    assert_eq!(by_lookup.irs, 2_000);
    assert_eq!(by_lookup.snd_una, 1_001);
    assert_eq!(by_lookup.snd_nxt, 1_120);
    assert_eq!(by_lookup.snd_wnd, 8_192);
    assert_eq!(by_lookup.rcv_nxt, 2_001);
    assert_eq!(by_lookup.rcv_wnd, 16_384);
}

#[test]
fn tcp_connection_snapshot_pool_retains_syn_sent_scaffolding_without_local_address() {
    let mut owner = TcpWorkerOwnedConnectionState::new(DataWorkerId::new(7));
    let remote: SocketAddr = "[2001:db8::77]:443".parse().expect("remote");

    owner.insert(TcpConnectionSnapshot {
        lookup_id: 52,
        connection_id: Some(TcpConnectionId::new(107)),
        owner_worker: DataWorkerId::new(7),
        state: TcpState::SynSent,
        local_port: 49_200,
        local: None,
        remote,
        iss: 0,
        irs: 0,
        snd_una: 0,
        snd_nxt: 0,
        snd_wnd: 65_535,
        rcv_nxt: 0,
        rcv_wnd: 65_535,
    });

    let snapshot = owner.publish_snapshot();
    let published = snapshot
        .lookup_by_lookup_id(52)
        .expect("syn-sent snapshot should exist");

    assert_eq!(published.connection_id, Some(TcpConnectionId::new(107)));
    assert_eq!(published.owner_worker, DataWorkerId::new(7));
    assert_eq!(published.state, TcpState::SynSent);
    assert_eq!(published.local_port, 49_200);
    assert_eq!(published.local, None);
    assert_eq!(published.remote, remote);
    assert_eq!(published.snd_wnd, 65_535);
    assert_eq!(published.rcv_wnd, 65_535);
}
