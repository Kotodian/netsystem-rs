use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::protocol::tcp::TcpCapabilities;
use hammer_plugin_tcp::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerLookupAccess, TcpLookupSnapshot,
    TcpLookupValue, TcpV4ListenerKey, TcpV6ListenerKey,
};
use hammer_runtime::DataWorkerId;

#[test]
fn tcp_lookup_returns_owner_worker_for_ipv4_listener() {
    let mut snapshot = TcpLookupSnapshot::default();
    let listener_key = TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 10), 443);
    <TcpLookupSnapshot as TcpListenerLookupAccess<TcpIpv4ListenerAddress>>::listener_table_mut(
        &mut snapshot,
    )
    .insert(
        listener_key,
        TcpLookupValue {
            id: 10,
            owner_worker: DataWorkerId::new(3),
            capabilities: TcpCapabilities::default(),
        },
    );
    let lookup = snapshot
        .lookup_listener::<TcpIpv4ListenerAddress>(listener_key)
        .expect("listener lookup should exist");

    assert_eq!(lookup.id, 10);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(3));
}

#[test]
fn tcp_lookup_returns_owner_worker_for_ipv6_listener() {
    let mut snapshot = TcpLookupSnapshot::default();
    let listener_key = TcpV6ListenerKey::new(9, "2001:db8::10".parse::<Ipv6Addr>().unwrap(), 443);
    <TcpLookupSnapshot as TcpListenerLookupAccess<TcpIpv6ListenerAddress>>::listener_table_mut(
        &mut snapshot,
    )
    .insert(
        listener_key,
        TcpLookupValue {
            id: 101,
            owner_worker: DataWorkerId::new(11),
            capabilities: TcpCapabilities::default(),
        },
    );
    let lookup = snapshot
        .lookup_listener::<TcpIpv6ListenerAddress>(listener_key)
        .expect("listener lookup should exist");

    assert_eq!(lookup.id, 101);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(11));
}

#[test]
fn tcp_lookup_listener_table_grows_beyond_initial_listener_count() {
    let mut snapshot = TcpLookupSnapshot::default();

    {
        let table =
            <TcpLookupSnapshot as TcpListenerLookupAccess<TcpIpv4ListenerAddress>>::listener_table_mut(
                &mut snapshot,
            );
        for id in 0..80u32 {
            table.insert(
                TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 10), 10_000 + id as u16),
                TcpLookupValue {
                    id,
                    owner_worker: DataWorkerId::new(id),
                    capabilities: TcpCapabilities::default(),
                },
            );
        }
    }

    let listener_key = TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 10), 10_079);
    let lookup = snapshot
        .lookup_listener::<TcpIpv4ListenerAddress>(listener_key)
        .expect("listener lookup should exist past the original pool size");

    assert_eq!(lookup.id, 79);
    assert_eq!(lookup.owner_worker, DataWorkerId::new(79));
}
