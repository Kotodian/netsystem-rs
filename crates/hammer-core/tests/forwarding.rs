use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::forwarding::{
    AdjacencyIndex, CustomDpoIndex, CustomDpoRegistry, CustomDpoType, Dpo, DpoClass, DpoId,
    DpoStackRegistry, DpoType, FibSnapshotBuilder, Ip4Mtrie, Ip4MtrieRoute, Ip4MtrieValue,
    Ip6PrefixHashTable, Ip6PrefixKey,
};
use hammer_core::protocol::ip::{
    IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket,
};
use ipnet::{Ipv4Net, Ipv6Net};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextHop {
    Drop,
    Direct,
}

impl Ip4MtrieValue for NextHop {
    #[inline(always)]
    fn into_leaf_value(self) -> u32 {
        match self {
            Self::Drop => 0,
            Self::Direct => 1,
        }
    }

    #[inline(always)]
    fn from_leaf_value(value: u32) -> Self {
        match value {
            0 => Self::Drop,
            1 => Self::Direct,
            other => panic!("unexpected next hop value: {other}"),
        }
    }
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
fn ip6_prefix_hash_table_exposes_explicit_prefetch_for_flat_buckets() {
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
    let destination = "2001:db8:64::42".parse().expect("destination");

    table.prefetch_key(Ip6PrefixKey::new(destination, 64));
    table.prefetch_destination(destination);

    assert_eq!(table.lookup(destination), Some(NextHop::Direct));
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
    assert_eq!(result.dpo.next(), NextHop::Direct);
    assert_eq!(snapshot.drop_next(), NextHop::Drop);
}

#[test]
fn dpo_receive_has_no_adjacency_index() {
    let dpo = Dpo::receive(IpVersion::V4, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::Receive);
    assert_eq!(dpo.proto(), IpVersion::V4);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.adjacency_index(), None);
}

#[test]
fn dpo_adjacency_carries_typed_adjacency_index() {
    let adjacency = AdjacencyIndex::new(7);
    let dpo = Dpo::adjacency(IpVersion::V6, adjacency, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::Adjacency);
    assert_eq!(dpo.proto(), IpVersion::V6);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.adjacency_index(), Some(adjacency));
    assert_eq!(dpo.forwarding_index(), adjacency.get());
}

#[test]
fn dpo_keeps_compact_hot_path_layout_for_u32_next() {
    assert_eq!(std::mem::size_of::<Dpo<u32>>(), 12);
}

#[test]
fn dpo_custom_carries_custom_type_index_and_next() {
    let custom_type = CustomDpoType::new(42).expect("custom type");
    let custom_index = CustomDpoIndex::new(9001);
    let dpo = Dpo::custom(IpVersion::V6, custom_type, custom_index, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::Custom);
    assert_eq!(dpo.proto(), IpVersion::V6);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.custom_type(), Some(custom_type));
    assert_eq!(dpo.custom_index(), Some(custom_index));
    assert_eq!(dpo.forwarding_index(), custom_index.get());
}

#[test]
fn dpo_load_balance_carries_typed_load_balance_index() {
    let load_balance = hammer_core::forwarding::LoadBalanceIndex::new(5);
    let dpo = Dpo::load_balance(IpVersion::V4, load_balance, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::LoadBalance);
    assert_eq!(dpo.proto(), IpVersion::V4);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.load_balance_index(), Some(load_balance));
    assert_eq!(dpo.forwarding_index(), load_balance.get());
    assert_eq!(
        dpo.class(),
        DpoClass::builtin(DpoType::LoadBalance).expect("load-balance class")
    );
}

#[test]
fn custom_dpo_registry_allocates_unique_nonzero_types() {
    let mut registry = CustomDpoRegistry::new();

    let first = registry.register().expect("first custom dpo type");
    let second = registry.register().expect("second custom dpo type");

    assert_ne!(first, second);
    assert_ne!(first.get(), 0);
    assert_ne!(second.get(), 0);
}

#[test]
fn dpo_stack_preserves_parent_identity_and_updates_next() {
    let custom_type = CustomDpoType::new(9).expect("custom type");
    let custom_index = CustomDpoIndex::new(17);
    let parent = Dpo::custom(IpVersion::V4, custom_type, custom_index, NextHop::Drop);
    let stacked = Dpo::stack(parent, NextHop::Direct);

    assert_eq!(stacked.kind(), DpoType::Custom);
    assert_eq!(stacked.proto(), IpVersion::V4);
    assert_eq!(stacked.custom_type(), Some(custom_type));
    assert_eq!(stacked.custom_index(), Some(custom_index));
    assert_eq!(stacked.forwarding_index(), custom_index.get());
    assert_eq!(stacked.next(), NextHop::Direct);
}

#[test]
fn dpo_stack_registry_stacks_parent_from_child_parent_proto_edge() {
    let mut custom_registry = CustomDpoRegistry::new();
    let child_type = custom_registry.register().expect("child custom type");
    let parent_type = custom_registry.register().expect("parent custom type");
    let child = DpoClass::custom(child_type);
    let parent = Dpo::custom(
        IpVersion::V4,
        parent_type,
        CustomDpoIndex::new(23),
        NextHop::Drop,
    );
    let mut stack_registry = DpoStackRegistry::new();
    stack_registry.register(child, parent.class(), IpVersion::V4, NextHop::Direct);

    let stacked = stack_registry
        .stack(child, parent)
        .expect("registered stack edge");

    assert_eq!(stacked.class(), parent.class());
    assert_eq!(stacked.custom_index(), parent.custom_index());
    assert_eq!(stacked.next(), NextHop::Direct);
}

#[test]
fn dpo_stack_registry_rejects_missing_stack_edge() {
    let child = DpoClass::builtin(DpoType::Receive).expect("receive class");
    let parent = Dpo::receive(IpVersion::V6, NextHop::Drop);
    let stack_registry = DpoStackRegistry::<NextHop>::new();

    assert_eq!(stack_registry.stack(child, parent), None);
}

#[test]
fn fib_snapshot_can_route_to_receive_dpo() {
    let mut builder = FibSnapshotBuilder::new(NextHop::Drop);
    let load_balance = builder.add_load_balance([DpoId::receive(IpVersion::V4, NextHop::Direct)]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 10), 32).expect("receive host route"),
        load_balance,
    );
    let snapshot = builder.build();

    let result = snapshot
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 10), 0)
        .expect("lookup result");
    assert_eq!(result.dpo.next(), NextHop::Direct);
    assert_eq!(result.dpo.proto(), IpVersion::V4);
    assert_eq!(result.dpo.adjacency_index(), None);
}

#[test]
fn fib_snapshot_exposes_route_load_balance_dpo() {
    let mut builder = FibSnapshotBuilder::new(NextHop::Drop);
    let load_balance = builder.add_load_balance([DpoId::receive(IpVersion::V4, NextHop::Direct)]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 40), 32).expect("load-balance route"),
        load_balance,
    );
    let snapshot = builder.build();

    let result = snapshot
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 40), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::LoadBalance);
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.load_balance(), Some(load_balance));
    assert_eq!(result.bucket_index(), Some(0));
    assert_eq!(result.dpo.kind(), DpoType::Receive);
}

#[test]
fn fib_snapshot_can_route_directly_to_terminal_dpo() {
    let mut builder = FibSnapshotBuilder::new(NextHop::Drop);
    builder.add_ip4_route_dpo(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 50), 32).expect("receive route"),
        DpoId::receive(IpVersion::V4, NextHop::Direct),
    );
    let ip6_destination = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0050);
    builder.add_ip6_route_dpo(
        Ipv6Net::new(ip6_destination, 128).expect("receive route"),
        DpoId::receive(IpVersion::V6, NextHop::Direct),
    );
    let snapshot = builder.build();

    let result = snapshot
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 50), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::Receive);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::Receive);
    assert_eq!(result.dpo.next(), NextHop::Direct);

    let result = snapshot
        .lookup_ip6(ip6_destination, 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::Receive);
    assert_eq!(result.route_dpo.proto(), IpVersion::V6);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_snapshot_exposes_packet_prefetch_before_lookup() {
    let mut builder = FibSnapshotBuilder::new(NextHop::Drop);
    let adjacency = builder.add_adjacency(IpVersion::V4, NextHop::Direct);
    let load_balance =
        builder.add_load_balance([DpoId::adjacency(IpVersion::V4, adjacency, NextHop::Direct)]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        load_balance,
    );
    let snapshot = builder.build();
    let packet = ParsedIpPacket {
        version: IpVersion::V4,
        protocol: IpProtocol::Udp,
        input_target: IpInputTarget::Lookup,
        input_error: IpInputError::None,
        source: Ipv4Addr::new(10, 0, 0, 1).into(),
        destination: Ipv4Addr::new(198, 51, 100, 42).into(),
        packet_len: 28,
        network_header_offset: 0,
        network_header_len: 20,
        transport_header_offset: 20,
        transport_header_len: 8,
    };

    snapshot.prefetch_packet(&packet);

    let result = snapshot.lookup_packet(&packet).expect("lookup result");
    assert_eq!(result.dpo.next(), NextHop::Direct);
}
