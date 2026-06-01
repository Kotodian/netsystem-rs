use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::forwarding::{
    DpoId, FibSnapshotBuilder, Ip4Mtrie, Ip4MtrieRoute, Ip6PrefixHashTable,
};
use hammer_core::protocol::ip::IpVersion;
use ipnet::{Ipv4Net, Ipv6Net};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextHop {
    Drop,
    Direct,
}

#[test]
fn ip4_mtrie_is_generic_and_uses_longest_prefix_match() {
    let trie = Ip4Mtrie::from_routes([
        Ip4MtrieRoute::new(
            Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
            NextHop::Drop,
        ),
        Ip4MtrieRoute::new(
            Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("subnet route"),
            NextHop::Direct,
        ),
    ]);

    assert_eq!(
        trie.lookup(Ipv4Addr::new(203, 0, 113, 7)),
        Some(NextHop::Drop)
    );
    assert_eq!(
        trie.lookup(Ipv4Addr::new(198, 51, 100, 42)),
        Some(NextHop::Direct)
    );
}

#[test]
fn ip6_prefix_hash_table_is_generic_and_uses_longest_prefix_match() {
    let table = Ip6PrefixHashTable::from_routes([
        (
            Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).expect("default route"),
            NextHop::Drop,
        ),
        (
            Ipv6Net::new("2001:db8:64::".parse().expect("subnet"), 64).expect("subnet route"),
            NextHop::Direct,
        ),
    ]);

    assert_eq!(
        table.lookup("2001:db8:ffff::1".parse().expect("default destination")),
        Some(NextHop::Drop)
    );
    assert_eq!(
        table.lookup("2001:db8:64::42".parse().expect("subnet destination")),
        Some(NextHop::Direct)
    );
    assert_eq!(table.prefix_lengths(), &[64, 0]);
}

#[test]
fn fib_snapshot_is_generic_over_next_target() {
    let mut builder = FibSnapshotBuilder::new(NextHop::Drop);
    let adjacency = builder.add_adjacency(IpVersion::V4, NextHop::Direct);
    let load_balance =
        builder.add_load_balance([DpoId::adjacency(IpVersion::V4, adjacency, NextHop::Direct)]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        load_balance,
    );
    let snapshot = builder.build();

    let result = snapshot
        .lookup_ip4(Ipv4Addr::new(198, 51, 100, 42), 0)
        .expect("lookup result");
    assert_eq!(result.dpo.next, NextHop::Direct);
    assert_eq!(snapshot.drop_next(), NextHop::Drop);
}
