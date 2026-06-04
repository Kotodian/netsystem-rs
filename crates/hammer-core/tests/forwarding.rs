use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::forwarding::{
    AdjacencyIndex, AdjacencyRewrite, AdjacencyRewriteError, Dpo, DpoId, DpoKind, DpoProto,
    DpoStackRegistry, DpoType, DpoTypeRegistry, FibEntry, FibRouteDpoError, FibTableBuilder,
    Ip4Mtrie, Ip4MtrieRoute, Ip4MtrieValue, Ip6PrefixHashTable, Ip6PrefixKey, LoadBalanceError,
    LoadBalanceIndex,
};
use hammer_core::protocol::ip::{
    IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket,
};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextHop {
    Drop,
    Direct,
    Rewrite,
}

#[derive(Debug, Clone, Copy)]
struct TestRegisteredDpoKind {
    dpo_type: DpoType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestRegisteredDpoIndex(u32);

impl DpoKind for TestRegisteredDpoKind {
    type Index = TestRegisteredDpoIndex;

    #[inline(always)]
    fn dpo_type(self) -> DpoType {
        self.dpo_type
    }

    #[inline(always)]
    fn encode_index(index: Self::Index) -> u32 {
        index.0
    }
}

impl Ip4MtrieValue for NextHop {
    #[inline(always)]
    fn into_leaf_value(self) -> u32 {
        match self {
            Self::Drop => 0,
            Self::Direct => 1,
            Self::Rewrite => 2,
        }
    }

    #[inline(always)]
    fn from_leaf_value(value: u32) -> Self {
        match value {
            0 => Self::Drop,
            1 => Self::Direct,
            2 => Self::Rewrite,
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
fn fib_table_is_generic_over_next_target() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let adjacency = builder.add_adjacency_dpo(DpoProto::IP4, NextHop::Direct);
    let load_balance = builder.add_load_balance(DpoProto::IP4, [adjacency]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        load_balance,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(198, 51, 100, 42), 0)
        .expect("lookup result");
    assert_eq!(result.dpo.next(), NextHop::Direct);
    assert_eq!(table.drop_next(), NextHop::Drop);
}

#[test]
fn fib_table_builder_adds_adjacency_dpo() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let dpo = builder.add_adjacency_dpo(DpoProto::IP4, NextHop::Direct);
    let adjacency = dpo.adjacency_index().expect("adjacency DPO index");

    assert_eq!(dpo.proto(), DpoProto::IP4);
    assert_eq!(dpo.next(), NextHop::Direct);

    let table = builder.build();
    let adjacency_entry = table.adjacency(adjacency).expect("adjacency entry");
    assert_eq!(adjacency_entry.proto, DpoProto::IP4);
    assert_eq!(adjacency_entry.next, NextHop::Direct);
    assert_eq!(adjacency_entry.egress_interface, None);
    assert!(adjacency_entry.rewrite.as_slice().is_empty());
}

#[test]
fn fib_table_builder_adds_interface_adjacency_with_rewrite() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let rewrite =
        AdjacencyRewrite::try_new(&[0xaa, 0xbb, 0x08, 0x00]).expect("rewrite fits inline storage");

    let adjacency = builder.add_interface_adjacency(DpoProto::IP4, 7, rewrite, NextHop::Direct);
    let table = builder.build();

    let adjacency_entry = table.adjacency(adjacency).expect("adjacency entry");
    assert_eq!(adjacency_entry.proto, DpoProto::IP4);
    assert_eq!(adjacency_entry.egress_interface, Some(7));
    assert_eq!(
        adjacency_entry.rewrite.as_slice(),
        &[0xaa, 0xbb, 0x08, 0x00]
    );
    assert_eq!(adjacency_entry.next, NextHop::Direct);
}

#[test]
fn fib_table_builder_interface_adjacency_dpo_keeps_rewrite_next_separate() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let rewrite =
        AdjacencyRewrite::try_new(&[0xaa, 0xbb, 0x08, 0x00]).expect("rewrite fits inline storage");

    let dpo = builder.add_interface_adjacency_dpo(
        DpoProto::IP4,
        9,
        rewrite,
        NextHop::Rewrite,
        NextHop::Direct,
    );
    let adjacency = dpo.adjacency_index().expect("adjacency DPO index");

    assert_eq!(dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(dpo.next(), NextHop::Rewrite);

    let table = builder.build();
    let adjacency_entry = table.adjacency(adjacency).expect("adjacency entry");
    assert_eq!(adjacency_entry.next, NextHop::Direct);
    assert_eq!(adjacency_entry.egress_interface, Some(9));
    assert_eq!(
        adjacency_entry.rewrite.as_slice(),
        &[0xaa, 0xbb, 0x08, 0x00]
    );
}

#[test]
fn adjacency_rewrite_rejects_oversized_bytes() {
    let bytes = [0u8; 65];
    let err = AdjacencyRewrite::try_new(&bytes).expect_err("rewrite must fit inline storage");

    assert_eq!(
        err,
        AdjacencyRewriteError::TooLarge {
            len: 65,
            max: AdjacencyRewrite::MAX_LEN,
        }
    );
}

#[test]
fn fib_table_selects_interface_adjacency_from_load_balance() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let rewrite =
        AdjacencyRewrite::try_new(&[0xde, 0xad, 0xbe, 0xef]).expect("rewrite fits inline storage");
    let adjacency = builder.add_interface_adjacency_dpo(
        DpoProto::IP4,
        11,
        rewrite,
        NextHop::Rewrite,
        NextHop::Direct,
    );
    let load_balance = builder.add_load_balance(DpoProto::IP4, [adjacency]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(203, 0, 113, 0), 24).expect("route"),
        load_balance,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(203, 0, 113, 7), 0)
        .expect("lookup result");
    let adjacency = result.dpo.adjacency_index().expect("adjacency index");
    let adjacency_entry = table.adjacency(adjacency).expect("adjacency entry");

    assert_eq!(result.dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(result.dpo.next(), NextHop::Rewrite);
    assert_eq!(adjacency_entry.egress_interface, Some(11));
    assert_eq!(adjacency_entry.next, NextHop::Direct);
    assert_eq!(
        adjacency_entry.rewrite.as_slice(),
        &[0xde, 0xad, 0xbe, 0xef]
    );
}

#[test]
fn fib_table_builder_adds_load_balance_dpo() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo = builder.add_load_balance_dpo(
        DpoProto::IP4,
        [DpoId::receive(DpoProto::IP4, NextHop::Direct)],
        NextHop::Drop,
    );
    let load_balance = route_dpo
        .load_balance_index()
        .expect("load-balance DPO index");

    assert_eq!(route_dpo.kind(), DpoType::LOAD_BALANCE);
    assert_eq!(route_dpo.proto(), DpoProto::IP4);
    assert_eq!(route_dpo.next(), NextHop::Drop);

    builder.add_ip4_route_dpo(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 12), 32).expect("route"),
        route_dpo,
    );
    let table = builder.build();

    assert_eq!(
        table
            .load_balance(load_balance)
            .expect("load-balance")
            .bucket_count(),
        1
    );
    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 12), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_single_path_load_balance() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance = builder.add_single_path_load_balance(DpoProto::IP4, NextHop::Direct);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 13), 32).expect("route"),
        load_balance,
    );
    let table = builder.build();

    let load_balance_entry = table.load_balance(load_balance).expect("load-balance");
    assert_eq!(load_balance_entry.bucket_count(), 1);
    let adjacency = load_balance_entry.buckets()[0]
        .adjacency_index()
        .expect("adjacency bucket");
    let adjacency_entry = table.adjacency(adjacency).expect("adjacency");
    assert_eq!(adjacency_entry.proto, DpoProto::IP4);
    assert_eq!(adjacency_entry.next, NextHop::Direct);

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 13), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(result.dpo.adjacency_index(), Some(adjacency));
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip4_single_path_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance = builder.add_ip4_single_path_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 14), 32).expect("route"),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 14), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.load_balance(), Some(load_balance));
    assert_eq!(result.dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip6_single_path_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let destination = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0014);
    let load_balance = builder.add_ip6_single_path_route(
        Ipv6Net::new(destination, 128).expect("route"),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table.lookup_ip6(destination, 0).expect("lookup result");
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.load_balance(), Some(load_balance));
    assert_eq!(result.dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_generic_single_path_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance = builder.add_single_path_route(
        IpNet::V4(Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 15), 32).expect("route")),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 15), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.load_balance(), Some(load_balance));
    assert_eq!(result.dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip4_receive_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo = builder.add_ip4_receive_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 16), 32).expect("route"),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 16), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip6_receive_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let destination = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0016);
    let route_dpo = builder.add_ip6_receive_route(
        Ipv6Net::new(destination, 128).expect("route"),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table.lookup_ip6(destination, 0).expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.dpo.proto(), DpoProto::IP6);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_generic_receive_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo = builder.add_receive_route(
        IpNet::V4(Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 17), 32).expect("route")),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 17), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip4_punt_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo = builder.add_ip4_punt_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 18), 32).expect("route"),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 18), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::PUNT);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip6_punt_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let destination = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0018);
    let route_dpo = builder.add_ip6_punt_route(
        Ipv6Net::new(destination, 128).expect("route"),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table.lookup_ip6(destination, 0).expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::PUNT);
    assert_eq!(result.dpo.proto(), DpoProto::IP6);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_generic_punt_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo = builder.add_punt_route(
        IpNet::V4(Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 19), 32).expect("route")),
        NextHop::Direct,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 19), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::PUNT);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_builder_adds_ip4_drop_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo =
        builder.add_ip4_drop_route(Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 20), 32).expect("route"));
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 20), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::DROP);
    assert_eq!(result.dpo.next(), NextHop::Drop);
}

#[test]
fn fib_table_builder_adds_ip6_drop_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let destination = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0020);
    let route_dpo = builder.add_ip6_drop_route(Ipv6Net::new(destination, 128).expect("route"));
    let table = builder.build();

    let result = table.lookup_ip6(destination, 0).expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::DROP);
    assert_eq!(result.dpo.proto(), DpoProto::IP6);
    assert_eq!(result.dpo.next(), NextHop::Drop);
}

#[test]
fn fib_table_builder_adds_generic_drop_route() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let route_dpo = builder.add_drop_route(IpNet::V4(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 21), 32).expect("route"),
    ));
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 21), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo, route_dpo);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::DROP);
    assert_eq!(result.dpo.next(), NextHop::Drop);
}

#[test]
fn dpo_receive_has_no_adjacency_index() {
    let dpo = Dpo::receive(DpoProto::IP4, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::RECEIVE);
    assert_eq!(dpo.proto(), DpoProto::IP4);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.adjacency_index(), None);
}

#[test]
fn dpo_adjacency_carries_typed_adjacency_index() {
    let adjacency = AdjacencyIndex::new(7);
    let dpo = Dpo::adjacency(DpoProto::IP6, adjacency, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(dpo.proto(), DpoProto::IP6);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.adjacency_index(), Some(adjacency));
    assert_eq!(dpo.forwarding_index(), adjacency.get());
}

#[test]
fn dpo_keeps_compact_hot_path_layout_for_u32_next() {
    assert_eq!(std::mem::size_of::<Dpo<u32>>(), 12);
}

#[test]
fn dpo_registered_type_carries_type_index_and_next() {
    let dpo_type = DpoType::new(42);
    let dpo = Dpo::new(DpoProto::IP6, dpo_type, 9001, NextHop::Direct);

    assert_eq!(dpo.kind(), dpo_type);
    assert_eq!(dpo.proto(), DpoProto::IP6);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.adjacency_index(), None);
    assert_eq!(dpo.load_balance_index(), None);
    assert_eq!(dpo.forwarding_index(), 9001);
}

#[test]
fn dpo_kind_trait_builds_registered_dpo_with_typed_index() {
    let mut registry = DpoTypeRegistry::new();
    let dpo_type = registry.register().expect("registered dpo type");
    let kind = TestRegisteredDpoKind { dpo_type };
    let index = TestRegisteredDpoIndex(33);
    let dpo = Dpo::typed(DpoProto::IP4, kind, index, NextHop::Direct);

    assert_eq!(dpo.kind(), dpo_type);
    assert_eq!(dpo.proto(), DpoProto::IP4);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.forwarding_index(), index.0);
}

#[test]
fn dpo_load_balance_carries_typed_load_balance_index() {
    let load_balance = hammer_core::forwarding::LoadBalanceIndex::new(5);
    let dpo = Dpo::load_balance(DpoProto::IP4, load_balance, NextHop::Direct);

    assert_eq!(dpo.kind(), DpoType::LOAD_BALANCE);
    assert_eq!(dpo.proto(), DpoProto::IP4);
    assert_eq!(dpo.next(), NextHop::Direct);
    assert_eq!(dpo.load_balance_index(), Some(load_balance));
    assert_eq!(dpo.forwarding_index(), load_balance.get());
    assert_eq!(dpo.class(), DpoType::LOAD_BALANCE);
}

#[test]
fn dpo_type_registry_allocates_unique_registered_types() {
    let mut registry = DpoTypeRegistry::new();

    let first = registry.register().expect("first registered dpo type");
    let second = registry.register().expect("second registered dpo type");

    assert_ne!(first, second);
    assert!(first.get() >= DpoType::FIRST_REGISTERED);
    assert!(second.get() >= DpoType::FIRST_REGISTERED);
    assert!(!first.is_builtin());
    assert!(!second.is_builtin());
}

#[test]
fn dpo_stack_preserves_parent_identity_and_updates_next() {
    let dpo_type = DpoType::new(9);
    let parent = Dpo::new(DpoProto::IP4, dpo_type, 17, NextHop::Drop);
    let stacked = Dpo::stack(parent, NextHop::Direct);

    assert_eq!(stacked.kind(), dpo_type);
    assert_eq!(stacked.proto(), DpoProto::IP4);
    assert_eq!(stacked.forwarding_index(), 17);
    assert_eq!(stacked.next(), NextHop::Direct);
}

#[test]
fn dpo_stack_registry_stacks_parent_from_child_parent_proto_edge() {
    let mut type_registry = DpoTypeRegistry::new();
    let child = type_registry.register().expect("child registered type");
    let child_proto = DpoProto::IP6;
    let parent_type = type_registry.register().expect("parent registered type");
    let parent = Dpo::new(DpoProto::IP4, parent_type, 23, NextHop::Drop);
    let mut stack_registry = DpoStackRegistry::new();
    stack_registry.register(
        child,
        child_proto,
        parent.class(),
        parent.proto(),
        NextHop::Direct,
    );

    let stacked = stack_registry
        .stack(child, child_proto, parent)
        .expect("registered stack edge");

    assert_eq!(stacked.class(), parent.class());
    assert_eq!(stacked.forwarding_index(), parent.forwarding_index());
    assert_eq!(stacked.next(), NextHop::Direct);

    assert_eq!(stack_registry.stack(child, DpoProto::IP4, parent), None);
    let wrong_parent_proto = Dpo::new(DpoProto::IP6, parent_type, 23, NextHop::Drop);
    assert_eq!(
        stack_registry.stack(child, child_proto, wrong_parent_proto),
        None
    );
}

#[test]
fn dpo_stack_registry_rejects_missing_stack_edge() {
    let child = DpoType::RECEIVE;
    let parent = Dpo::receive(DpoProto::IP6, NextHop::Drop);
    let stack_registry = DpoStackRegistry::<NextHop>::new();

    assert_eq!(stack_registry.stack(child, DpoProto::IP6, parent), None);
}

#[test]
fn fib_table_can_route_to_receive_dpo() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance = builder.add_load_balance(
        DpoProto::IP4,
        [DpoId::receive(DpoProto::IP4, NextHop::Direct)],
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 10), 32).expect("receive host route"),
        load_balance,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 10), 0)
        .expect("lookup result");
    assert_eq!(result.dpo.next(), NextHop::Direct);
    assert_eq!(result.dpo.proto(), DpoProto::IP4);
    assert_eq!(result.dpo.adjacency_index(), None);
}

#[test]
fn fib_table_exposes_route_load_balance_dpo() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance = builder.add_load_balance(
        DpoProto::IP4,
        [DpoId::receive(DpoProto::IP4, NextHop::Direct)],
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 40), 32).expect("load-balance route"),
        load_balance,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 40), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::LOAD_BALANCE);
    assert_eq!(result.route_dpo.load_balance_index(), Some(load_balance));
    assert_eq!(result.load_balance(), Some(load_balance));
    assert_eq!(result.bucket_index(), Some(0));
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
}

#[test]
fn fib_table_resolves_nested_load_balance_dpo_bucket() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let inner = builder.add_load_balance(
        DpoProto::IP4,
        [
            DpoId::drop(DpoProto::IP4, NextHop::Drop),
            DpoId::receive(DpoProto::IP4, NextHop::Direct),
        ],
    );
    let outer = builder.add_load_balance(
        DpoProto::IP4,
        [DpoId::load_balance(DpoProto::IP4, inner, NextHop::Drop)],
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 41), 32).expect("nested load-balance route"),
        outer,
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 41), 1)
        .expect("lookup result");
    assert_eq!(result.route_dpo.load_balance_index(), Some(outer));
    assert_eq!(result.load_balance(), Some(inner));
    assert_eq!(result.bucket_index(), Some(1));
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_can_route_directly_to_terminal_dpo() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    builder.add_ip4_route_dpo(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 50), 32).expect("receive route"),
        DpoId::receive(DpoProto::IP4, NextHop::Direct),
    );
    let ip6_destination = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0x0050);
    builder.add_ip6_route_dpo(
        Ipv6Net::new(ip6_destination, 128).expect("receive route"),
        DpoId::receive(DpoProto::IP6, NextHop::Direct),
    );
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 50), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.dpo.next(), NextHop::Direct);

    let result = table.lookup_ip6(ip6_destination, 0).expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::RECEIVE);
    assert_eq!(result.route_dpo.proto(), DpoProto::IP6);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_adds_dpo_backed_fib_entry() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    builder.add_entry(FibEntry::new(
        IpNet::V4(Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 60), 32).expect("punt route")),
        DpoId::punt(DpoProto::IP4, NextHop::Direct),
    ));
    let table = builder.build();

    let result = table
        .lookup_ip4(Ipv4Addr::new(192, 0, 2, 60), 0)
        .expect("lookup result");
    assert_eq!(result.route_dpo.kind(), DpoType::PUNT);
    assert_eq!(result.load_balance(), None);
    assert_eq!(result.bucket_index(), None);
    assert_eq!(result.dpo.kind(), DpoType::PUNT);
    assert_eq!(result.dpo.next(), NextHop::Direct);
}

#[test]
fn fib_table_rejects_route_dpo_with_wrong_ip_proto() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let err = builder
        .try_add_ip4_route_dpo(
            Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 70), 32).expect("route"),
            DpoId::receive(DpoProto::IP6, NextHop::Direct),
        )
        .expect_err("wrong-proto route DPO should be rejected");

    assert_eq!(
        err,
        FibRouteDpoError::ProtoMismatch {
            expected: DpoProto::IP4,
            actual: DpoProto::IP6,
        }
    );

    let err = builder
        .try_add_entry(FibEntry::new(
            IpNet::V6(Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).expect("route")),
            DpoId::punt(DpoProto::IP4, NextHop::Direct),
        ))
        .expect_err("wrong-proto FIB entry should be rejected");

    assert_eq!(
        err,
        FibRouteDpoError::ProtoMismatch {
            expected: DpoProto::IP6,
            actual: DpoProto::IP4,
        }
    );
}

#[test]
fn fib_table_rejects_route_dpo_with_missing_adjacency() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let missing = AdjacencyIndex::new(99);
    let err = builder
        .try_add_ip4_route_dpo(
            Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 75), 32).expect("route"),
            DpoId::adjacency(DpoProto::IP4, missing, NextHop::Direct),
        )
        .expect_err("route DPO with missing adjacency should be rejected");

    assert_eq!(err, FibRouteDpoError::AdjacencyMissing { index: missing });
}

#[test]
fn fib_table_rejects_route_dpo_with_wrong_adjacency_proto() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let adjacency = builder.add_adjacency(DpoProto::IP6, NextHop::Direct);
    let err = builder
        .try_add_ip4_route_dpo(
            Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 76), 32).expect("route"),
            DpoId::adjacency(DpoProto::IP4, adjacency, NextHop::Direct),
        )
        .expect_err("route DPO with wrong-proto adjacency should be rejected");

    assert_eq!(
        err,
        FibRouteDpoError::AdjacencyProtoMismatch {
            index: adjacency,
            expected: DpoProto::IP4,
            actual: DpoProto::IP6,
        }
    );
}

#[test]
fn fib_table_rejects_load_balance_route_with_wrong_proto_or_index() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance =
        builder.add_load_balance(DpoProto::IP6, [DpoId::drop(DpoProto::IP6, NextHop::Drop)]);

    let err = builder
        .try_add_ip4_route(
            Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 80), 32).expect("route"),
            load_balance,
        )
        .expect_err("wrong-proto load-balance route should be rejected");

    assert_eq!(
        err,
        FibRouteDpoError::LoadBalanceProtoMismatch {
            expected: DpoProto::IP4,
            actual: DpoProto::IP6,
        }
    );

    let missing = LoadBalanceIndex::new(99);
    let err = builder
        .try_add_ip6_route(
            Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).expect("route"),
            missing,
        )
        .expect_err("missing load-balance route should be rejected");

    assert_eq!(err, FibRouteDpoError::LoadBalanceMissing { index: missing });
}

#[test]
fn fib_table_rejects_load_balance_dpo_route_with_wrong_proto_or_index() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let load_balance =
        builder.add_load_balance(DpoProto::IP6, [DpoId::drop(DpoProto::IP6, NextHop::Drop)]);

    let err = builder
        .try_add_ip4_route_dpo(
            Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 85), 32).expect("route"),
            DpoId::load_balance(DpoProto::IP4, load_balance, NextHop::Drop),
        )
        .expect_err("wrong-proto load-balance DPO route should be rejected");

    assert_eq!(
        err,
        FibRouteDpoError::LoadBalanceProtoMismatch {
            expected: DpoProto::IP4,
            actual: DpoProto::IP6,
        }
    );

    let missing = LoadBalanceIndex::new(99);
    let err = builder
        .try_add_ip6_route_dpo(
            Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).expect("route"),
            DpoId::load_balance(DpoProto::IP6, missing, NextHop::Drop),
        )
        .expect_err("missing load-balance DPO route should be rejected");

    assert_eq!(err, FibRouteDpoError::LoadBalanceMissing { index: missing });
}

#[test]
fn fib_table_rejects_load_balance_bucket_with_missing_adjacency() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let missing = AdjacencyIndex::new(99);
    let err = builder
        .try_add_load_balance(
            DpoProto::IP4,
            [DpoId::adjacency(DpoProto::IP4, missing, NextHop::Direct)],
        )
        .expect_err("load-balance bucket with missing adjacency should be rejected");

    assert_eq!(
        err,
        LoadBalanceError::BucketAdjacencyMissing {
            index: missing,
            bucket_index: 0,
        }
    );
}

#[test]
fn fib_table_rejects_load_balance_bucket_with_wrong_adjacency_proto() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let adjacency = builder.add_adjacency(DpoProto::IP6, NextHop::Direct);
    let err = builder
        .try_add_load_balance(
            DpoProto::IP4,
            [DpoId::adjacency(DpoProto::IP4, adjacency, NextHop::Direct)],
        )
        .expect_err("load-balance bucket with wrong-proto adjacency should be rejected");

    assert_eq!(
        err,
        LoadBalanceError::BucketAdjacencyProtoMismatch {
            index: adjacency,
            expected: DpoProto::IP4,
            actual: DpoProto::IP6,
            bucket_index: 0,
        }
    );
}

#[test]
fn fib_table_rejects_load_balance_bucket_with_missing_load_balance() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let missing = LoadBalanceIndex::new(99);
    let err = builder
        .try_add_load_balance(
            DpoProto::IP4,
            [DpoId::load_balance(DpoProto::IP4, missing, NextHop::Direct)],
        )
        .expect_err("load-balance bucket with missing load-balance should be rejected");

    assert_eq!(
        err,
        LoadBalanceError::BucketLoadBalanceMissing {
            index: missing,
            bucket_index: 0,
        }
    );
}

#[test]
fn fib_table_rejects_load_balance_bucket_with_wrong_load_balance_proto() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let inner =
        builder.add_load_balance(DpoProto::IP6, [DpoId::drop(DpoProto::IP6, NextHop::Drop)]);
    let err = builder
        .try_add_load_balance(
            DpoProto::IP4,
            [DpoId::load_balance(DpoProto::IP4, inner, NextHop::Direct)],
        )
        .expect_err("load-balance bucket with wrong-proto load-balance should be rejected");

    assert_eq!(
        err,
        LoadBalanceError::BucketLoadBalanceProtoMismatch {
            index: inner,
            expected: DpoProto::IP4,
            actual: DpoProto::IP6,
            bucket_index: 0,
        }
    );
}

#[test]
fn fib_table_exposes_packet_prefetch_before_lookup() {
    let mut builder = FibTableBuilder::new(NextHop::Drop);
    let adjacency = builder.add_adjacency_dpo(DpoProto::IP4, NextHop::Direct);
    let load_balance = builder.add_load_balance(DpoProto::IP4, [adjacency]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        load_balance,
    );
    let table = builder.build();
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

    table.prefetch_packet(&packet);

    let result = table.lookup_packet(&packet).expect("lookup result");
    assert_eq!(result.dpo.next(), NextHop::Direct);
}
