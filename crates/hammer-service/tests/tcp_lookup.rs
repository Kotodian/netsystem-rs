use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_adapter::DataWorkerId;
use hammer_service::transport::tcp::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpV4ListenerKey, TcpV6ListenerKey,
    TcpWorkerOwnedState,
};

#[test]
fn tcp_lookup_returns_owner_worker_for_ipv4_listener() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(3));
    let listener_key = TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 10), 443);
    owner.insert_listener::<TcpIpv4ListenerAddress>(listener_key, 10);
    assert_eq!(owner.owner_worker(), DataWorkerId::new(3));

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_listener::<TcpIpv4ListenerAddress>(listener_key)
        .expect("listener lookup should exist");

    assert_eq!(lookup.id, 10);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(3));
}

#[test]
fn tcp_lookup_returns_owner_worker_for_ipv6_listener() {
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(11));
    let listener_key = TcpV6ListenerKey::new(9, "2001:db8::10".parse::<Ipv6Addr>().unwrap(), 443);
    owner.insert_listener::<TcpIpv6ListenerAddress>(listener_key, 101);

    let snapshot = owner.publish_snapshot();
    let lookup = snapshot
        .lookup_listener::<TcpIpv6ListenerAddress>(listener_key)
        .expect("listener lookup should exist");

    assert_eq!(lookup.id, 101);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(11));
}
