use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_service::net::{DpoId, DpoType, FibEntry, FibSource, FibTable, FibTableBackend};
use ipnet::{Ipv4Net, Ipv6Net};

use crate::ip::IpPathFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IpFibError {
    #[error("IP prefix already exists")]
    PrefixExists,
    #[error("IP prefix is not present")]
    PrefixMissing,
    #[error("IP forwarding requires a load-balance DPO")]
    ForwardingDpoRequired,
}

#[derive(Debug, Clone, Copy)]
struct Ip4TrieNode {
    child: [Option<u32>; 2],
    forwarding: Option<DpoId>,
}

impl Default for Ip4TrieNode {
    fn default() -> Self {
        Self {
            child: [None, None],
            forwarding: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Ip4ForwardingTrie {
    nodes: Vec<Ip4TrieNode>,
}

impl Ip4ForwardingTrie {
    fn new() -> Self {
        Self {
            nodes: vec![Ip4TrieNode::default()],
        }
    }

    fn insert(&mut self, prefix: Ipv4Net, forwarding: DpoId) {
        if self.nodes.is_empty() {
            self.nodes.push(Ip4TrieNode::default());
        }
        let mut node = 0usize;
        let address = u32::from(prefix.network());
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (31 - bit)) & 1) as usize;
            let child = if let Some(child) = self.nodes[node].child[branch] {
                child as usize
            } else {
                let child = self.nodes.len() as u32;
                self.nodes[node].child[branch] = Some(child);
                self.nodes.push(Ip4TrieNode::default());
                child as usize
            };
            node = child;
        }
        self.nodes[node].forwarding = Some(forwarding);
    }

    fn remove(&mut self, prefix: Ipv4Net) {
        let address = u32::from(prefix.network());
        let mut node = 0usize;
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (31 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                return;
            };
            node = child as usize;
        }
        self.nodes[node].forwarding = None;
    }

    #[inline(always)]
    fn lookup(&self, address: Ipv4Addr) -> Option<DpoId> {
        let mut node = 0usize;
        let mut result = self.nodes.first()?.forwarding;
        let value = u32::from(address);
        for bit in 0..32 {
            let branch = ((value >> (31 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                break;
            };
            let child = child as usize;
            node = child;
            if self.nodes[node].forwarding.is_some() {
                result = self.nodes[node].forwarding;
            }
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct Ip4FibBackend {
    entries: BTreeMap<Ipv4Net, u32>,
    forwarding: Ip4ForwardingTrie,
}

impl Default for Ip4FibBackend {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            forwarding: Ip4ForwardingTrie::new(),
        }
    }
}

impl FibTableBackend for Ip4FibBackend {
    type Prefix = Ipv4Net;
    type PacketAddress = Ipv4Addr;
    type NextHop = Ipv4Addr;
    type PathFlags = IpPathFlags;
    type Error = IpFibError;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32> {
        self.entries.get(&prefix).copied()
    }

    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32> {
        self.lookup(prefix)
    }

    fn less_specific(&self, prefix: Self::Prefix) -> Option<(Self::Prefix, u32)> {
        self.entries
            .iter()
            .filter(|(candidate, _)| {
                candidate.prefix_len() < prefix.prefix_len()
                    && candidate.contains(&prefix.network())
            })
            .max_by_key(|(candidate, _)| candidate.prefix_len())
            .map(|(candidate, entry)| (*candidate, *entry))
    }

    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error> {
        if self.entries.insert(prefix, entry).is_some() {
            return Err(IpFibError::PrefixExists);
        }
        Ok(())
    }

    fn remove_entry(&mut self, prefix: Self::Prefix, _: u32) -> Result<(), Self::Error> {
        self.entries
            .remove(&prefix)
            .ok_or(IpFibError::PrefixMissing)?;
        self.forwarding.remove(prefix);
        Ok(())
    }

    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<DpoId> {
        self.forwarding.lookup(address)
    }

    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId) -> Result<(), Self::Error> {
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(IpFibError::ForwardingDpoRequired);
        }
        if !self.entries.contains_key(&prefix) {
            return Err(IpFibError::PrefixMissing);
        }
        self.forwarding.insert(prefix, dpo);
        Ok(())
    }

    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        _old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error> {
        self.forwarding.remove(prefix);
        if let Some((cover_prefix, cover_dpo)) = cover {
            if self.entries.contains_key(&cover_prefix) {
                self.forwarding.insert(cover_prefix, cover_dpo);
            }
        }
        Ok(())
    }

    fn project_forwarding(
        &mut self,
        entry: &FibEntry,
        _: FibSource,
    ) -> Result<Option<DpoId>, Self::Error> {
        if let Some(dpo) = entry.forwarding {
            hammer_service::net::NetMain::global()
                .expect("IP FIB projection requires the initialized net owner")
                .lock_dpo(dpo);
        }
        Ok(entry.forwarding)
    }
}

#[derive(Debug, Clone, Copy)]
struct Ip6TrieNode {
    child: [Option<u32>; 2],
    forwarding: Option<DpoId>,
}

impl Default for Ip6TrieNode {
    fn default() -> Self {
        Self {
            child: [None, None],
            forwarding: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Ip6ForwardingTrie {
    nodes: Vec<Ip6TrieNode>,
}

impl Ip6ForwardingTrie {
    fn new() -> Self {
        Self {
            nodes: vec![Ip6TrieNode::default()],
        }
    }

    fn insert(&mut self, prefix: Ipv6Net, forwarding: DpoId) {
        if self.nodes.is_empty() {
            self.nodes.push(Ip6TrieNode::default());
        }
        let mut node = 0usize;
        let address = u128::from_be_bytes(prefix.network().octets());
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (127 - bit)) & 1) as usize;
            let child = if let Some(child) = self.nodes[node].child[branch] {
                child as usize
            } else {
                let child = self.nodes.len() as u32;
                self.nodes[node].child[branch] = Some(child);
                self.nodes.push(Ip6TrieNode::default());
                child as usize
            };
            node = child;
        }
        self.nodes[node].forwarding = Some(forwarding);
    }

    fn remove(&mut self, prefix: Ipv6Net) {
        let address = u128::from_be_bytes(prefix.network().octets());
        let mut node = 0usize;
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (127 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                return;
            };
            node = child as usize;
        }
        self.nodes[node].forwarding = None;
    }

    #[inline(always)]
    fn lookup(&self, address: Ipv6Addr) -> Option<DpoId> {
        let mut node = 0usize;
        let mut result = self.nodes.first()?.forwarding;
        let value = u128::from_be_bytes(address.octets());
        for bit in 0..128 {
            let branch = ((value >> (127 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                break;
            };
            let child = child as usize;
            node = child;
            if self.nodes[node].forwarding.is_some() {
                result = self.nodes[node].forwarding;
            }
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct Ip6FibBackend {
    entries: BTreeMap<Ipv6Net, u32>,
    forwarding: Ip6ForwardingTrie,
}

impl Default for Ip6FibBackend {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            forwarding: Ip6ForwardingTrie::new(),
        }
    }
}

impl FibTableBackend for Ip6FibBackend {
    type Prefix = Ipv6Net;
    type PacketAddress = Ipv6Addr;
    type NextHop = Ipv6Addr;
    type PathFlags = IpPathFlags;
    type Error = IpFibError;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32> {
        self.entries.get(&prefix).copied()
    }
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32> {
        self.lookup(prefix)
    }
    fn less_specific(&self, prefix: Self::Prefix) -> Option<(Self::Prefix, u32)> {
        self.entries
            .iter()
            .filter(|(candidate, _)| {
                candidate.prefix_len() < prefix.prefix_len()
                    && candidate.contains(&prefix.network())
            })
            .max_by_key(|(candidate, _)| candidate.prefix_len())
            .map(|(candidate, entry)| (*candidate, *entry))
    }
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error> {
        if self.entries.insert(prefix, entry).is_some() {
            return Err(IpFibError::PrefixExists);
        }
        Ok(())
    }
    fn remove_entry(&mut self, prefix: Self::Prefix, _: u32) -> Result<(), Self::Error> {
        self.entries
            .remove(&prefix)
            .ok_or(IpFibError::PrefixMissing)?;
        self.forwarding.remove(prefix);
        Ok(())
    }
    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<DpoId> {
        self.forwarding.lookup(address)
    }
    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId) -> Result<(), Self::Error> {
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(IpFibError::ForwardingDpoRequired);
        }
        if !self.entries.contains_key(&prefix) {
            return Err(IpFibError::PrefixMissing);
        }
        self.forwarding.insert(prefix, dpo);
        Ok(())
    }
    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        _old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error> {
        self.forwarding.remove(prefix);
        if let Some((cover_prefix, cover_dpo)) = cover {
            if self.entries.contains_key(&cover_prefix) {
                self.forwarding.insert(cover_prefix, cover_dpo);
            }
        }
        Ok(())
    }
    fn project_forwarding(
        &mut self,
        entry: &FibEntry,
        _: FibSource,
    ) -> Result<Option<DpoId>, Self::Error> {
        if let Some(dpo) = entry.forwarding {
            hammer_service::net::NetMain::global()
                .expect("IP FIB projection requires the initialized net owner")
                .lock_dpo(dpo);
        }
        Ok(entry.forwarding)
    }
}

pub type Ip4FibTable = FibTable<Ipv4Net, Ip4FibBackend>;
pub type Ip6FibTable = FibTable<Ipv6Net, Ip6FibBackend>;

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_runtime::{DataPlaneBufferConfig, DataPlaneMain, GlobalMain, RuntimeRegistry};
    use hammer_service::interface::InterfaceMain;
    use hammer_service::net::{
        DpoError, DpoProto, DpoType, FibPath, FibPathList, FibPathListFlags, LoadBalanceDpo,
        LoadBalanceFlags, LoadBalancePath, NetMain,
    };
    use std::sync::Arc;

    #[test]
    fn route_sources_retain_forwarding_until_withdrawal() -> Result<(), DpoError> {
        hammer_runtime::config::Memory::default().ensure_main_heap()?;
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0, 1],
                2,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut main = GlobalMain::new(
            DataPlaneMain::new(DataPlaneBufferConfig::default()),
            RuntimeRegistry::new(),
        );
        main.install_current();
        let net = NetMain::init(Arc::new(InterfaceMain::new()))?;
        let runtime = main.data_plane_main_mut();
        let terminal = hammer_service::data_plane::register_drop(runtime)?;
        let punt_terminal = runtime
            .nodes()
            .try_register_internal(hammer_service::data_plane::PuntNode::new())?;
        // VPP punt_dpo.c binds each protocol to its punt arc entry, not the
        // terminal disposition node. Resolve the stacked edge through the graph.
        for (proto, entry, name) in [
            (
                DpoProto::IP4,
                &crate::punt::__IP_GRAPH_NODE_IP4_PUNT_NODE,
                "ip4-punt",
            ),
            (
                DpoProto::IP6,
                &crate::punt::__IP_GRAPH_NODE_IP6_PUNT_NODE,
                "ip6-punt",
            ),
        ] {
            let punt = (entry.init)(runtime)?;
            runtime.nodes().resolve_named_next_nodes()?;
            let forwarding =
                net.dpo_main_mut()
                    .stack_from_node(runtime, terminal, DpoId::punt(proto))?;
            assert_eq!(
                runtime
                    .nodes()
                    .node_next_slot(terminal, usize::from(forwarding.next()))?,
                punt,
            );
            assert_eq!(runtime.nodes().node_by_name(name), Some(punt));
            assert_ne!(punt, punt_terminal);
            assert_eq!(runtime.nodes().node_next_slot(punt, 0)?, punt_terminal);
        }
        net.register_dpo(
            Some(DpoType::LOAD_BALANCE),
            &[(DpoProto::IP4, &[terminal]), (DpoProto::IP6, &[terminal])],
            Some((LoadBalanceDpo::lock, LoadBalanceDpo::unlock)),
            None,
            Some(LoadBalanceDpo::mtu),
            None,
            None,
            Some(LoadBalanceDpo::format),
            Some(LoadBalanceDpo::memory),
        )?;
        let mut roots = Vec::new();
        for proto in [
            DpoProto::IP4,
            DpoProto::IP4,
            DpoProto::IP4,
            DpoProto::IP6,
            DpoProto::IP6,
        ] {
            roots.push(net.create_load_balance(
                runtime,
                proto,
                LoadBalanceDpo::new(proto, &[], LoadBalanceFlags::empty(), 0x9f)?,
            )?);
        }
        let mut table = Ip4FibTable::new(Ip4FibBackend::default());
        let cover = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 8).unwrap();
        let specific = Ipv4Net::new(Ipv4Addr::new(10, 1, 0, 0), 16).unwrap();
        table.add_route(cover, FibSource::API, roots[0]).unwrap();
        table.add_route(cover, FibSource::API, roots[0]).unwrap();
        table
            .add_route(specific, FibSource::INTERFACE, roots[2])
            .unwrap();
        table.add_route(specific, FibSource::API, roots[1]).unwrap();
        let default = Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap();
        let host = Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 7), 32).unwrap();
        table.add_route(default, FibSource::API, roots[0]).unwrap();
        table.add_route(host, FibSource::API, roots[2]).unwrap();
        assert_eq!(table.forwarding_lookup(host.addr()), Some(roots[2]));
        table.remove_route(host, FibSource::API).unwrap();
        assert_eq!(table.forwarding_lookup(host.addr()), Some(roots[0]));
        for &root in &roots[..3] {
            net.unlock_dpo(root);
        }
        assert_eq!(table.winner_source(specific), Some(FibSource::INTERFACE));
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(10, 1, 2, 3)),
            Some(roots[2])
        );
        let rejected = Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 7), 32).unwrap();
        let error = table
            .add_route(rejected, FibSource::API, DpoId::drop(DpoProto::IP4))
            .unwrap_err();
        assert!(matches!(
            error,
            hammer_service::net::fib::FibError::Backend(IpFibError::ForwardingDpoRequired)
        ));
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<IpFibError>()
        );
        assert_eq!(table.lookup_exact(rejected), None);
        assert_eq!(table.winner_source(rejected), None);
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(10, 1, 2, 3)),
            Some(roots[2])
        );

        table.remove_route(specific, FibSource::INTERFACE).unwrap();
        assert!(net.load_balance(roots[2].index()).is_none());
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(10, 1, 2, 3)),
            Some(roots[1])
        );
        table.remove_route(specific, FibSource::API).unwrap();
        assert!(net.load_balance(roots[1].index()).is_none());
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(10, 1, 2, 3)),
            Some(roots[0])
        );
        table.remove_route(cover, FibSource::API).unwrap();
        assert!(net.load_balance(roots[0].index()).is_some());
        drop(table);
        assert!(net.load_balance(roots[0].index()).is_none());

        let mut table6 = Ip6FibTable::new(Ip6FibBackend::default());
        let first = Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).unwrap();
        let second = Ipv6Net::new(Ipv6Addr::from([0, 0, 0, 0, 0, 0, 0, 1]), 64).unwrap();
        table6.add_route(second, FibSource::API, roots[3]).unwrap();
        table6.add_route(first, FibSource::API, roots[4]).unwrap();
        for &root in &roots[3..] {
            net.unlock_dpo(root);
        }
        assert_eq!(
            table6.forwarding_lookup(Ipv6Addr::LOCALHOST),
            Some(roots[4])
        );
        assert_eq!(table6.lookup_exact(second), Some(0));
        table6.remove_route(first, FibSource::API).unwrap();
        assert_eq!(
            table6.forwarding_lookup(Ipv6Addr::LOCALHOST),
            Some(roots[3])
        );
        drop(table6);
        for root in roots {
            assert!(net.load_balance(root.index()).is_none());
        }
        // VPP plugins/unittest/fib_test.c:9018-9054 and 9152-9170:
        // resolve an interface-RX bucket through a route, withdraw the route,
        // then release the independent reference and check pool reclamation.
        // The source test uses MPLS; only its protocol-neutral DPO ownership
        // sequence is reused here. No MPLS policy is added to Hammer.
        (crate::lookup::__IP_GRAPH_NODE_IP4_INTERFACE_RX_NODE.init)(runtime)?;
        (crate::lookup::__IP_GRAPH_NODE_IP6_INTERFACE_RX_NODE.init)(runtime)?;
        let interfaces = net.interface_main();
        let hardware = interfaces.register_hardware_interface(0, 1, 0, 0).unwrap();
        let software = interfaces.hardware_interface(hardware).unwrap().sw_if_index;
        let rx4 = interfaces
            .add_or_lock_rx_dpo(DpoProto::IP4, software)?
            .unwrap();
        let shared = interfaces
            .add_or_lock_rx_dpo(DpoProto::IP4, software)?
            .unwrap();
        assert_eq!(rx4, shared);
        let forwarding = net.create_load_balance(
            runtime,
            DpoProto::IP4,
            LoadBalanceDpo::new(
                DpoProto::IP4,
                &[LoadBalancePath {
                    dpo: rx4,
                    path_index: u32::MAX,
                    weight: 1,
                }],
                LoadBalanceFlags::empty(),
                0x9f,
            )?,
        )?;
        let mut table = Ip4FibTable::new(Ip4FibBackend::default());
        let destination = Ipv4Net::new(Ipv4Addr::new(1, 1, 1, 0), 24).unwrap();
        table
            .add_route(destination, FibSource::API, forwarding)
            .unwrap();
        net.unlock_dpo(forwarding);
        net.unlock_dpo(shared);
        let selected = table.forwarding_lookup(Ipv4Addr::new(1, 1, 1, 1)).unwrap();
        assert_eq!(net.select_load_balance(selected, |_, _| Some(0)), Some(rx4));
        table.remove_route(destination, FibSource::API).unwrap();
        assert!(net.load_balance(forwarding.index()).is_none());
        assert_eq!(interfaces.rx_dpo_interface(rx4), Some(software));
        net.unlock_dpo(rx4);
        assert_eq!(hammer_service::net::InterfaceRxDpo::memory().1, 0);
        drop(table);
        interfaces.delete_hardware_interface(hardware).unwrap();

        // VPP plugins/unittest/fib_test.c:774-834 checks multipath buckets
        // through a route and pool reclamation after withdrawal. Distinct RX
        // objects also expose bucket order across inline/overflow replacement.
        let load_balance_count = LoadBalanceDpo::memory().1;
        let rx_count = hammer_service::net::InterfaceRxDpo::memory().1;
        let mut hardware_interfaces = Vec::new();
        let mut rx_paths = Vec::new();
        for instance in 0..8 {
            let hardware = interfaces
                .register_hardware_interface(0, instance, 0, 0)
                .unwrap();
            let software = interfaces.hardware_interface(hardware).unwrap().sw_if_index;
            hardware_interfaces.push(hardware);
            rx_paths.push(LoadBalancePath {
                dpo: interfaces
                    .add_or_lock_rx_dpo(DpoProto::IP4, software)?
                    .unwrap(),
                path_index: instance,
                weight: 1,
            });
        }
        let forwarding = net.create_load_balance(
            runtime,
            DpoProto::IP4,
            LoadBalanceDpo::new(
                DpoProto::IP4,
                &rx_paths[..4],
                LoadBalanceFlags::STICKY,
                0x9f,
            )?,
        )?;
        let accepting_interfaces: Vec<_> = hardware_interfaces
            .iter()
            .map(|&hardware| interfaces.hardware_interface(hardware).unwrap().sw_if_index)
            .collect();
        let paths = accepting_interfaces
            .iter()
            .enumerate()
            .map(|(index, &sw_if_index)| FibPath {
                sw_if_index,
                table_id: 0,
                rpf_id: u32::MAX,
                weight: 1,
                preference: 0,
                flags: IpPathFlags::empty(),
                next_hop: Ipv4Addr::new(10, 0, 0, index as u8 + 1),
            })
            .collect();
        let mut path_list = FibPathList::new(paths, FibPathListFlags::SHARED);
        path_list.bake_urpf(accepting_interfaces.clone())?;
        let shared_urpf = path_list.urpf_index().unwrap();
        assert_eq!(
            net.set_load_balance_urpf(forwarding, shared_urpf)?,
            Some(())
        );
        let other_forwarding = net.create_load_balance(
            runtime,
            DpoProto::IP4,
            LoadBalanceDpo::new(DpoProto::IP4, &rx_paths, LoadBalanceFlags::empty(), 0x9f)?,
        )?;
        assert_eq!(
            net.set_load_balance_urpf(other_forwarding, shared_urpf)?,
            Some(())
        );
        // A path-state rebuild creates a new immutable list; another entry
        // keeps the previous accepting-interface set until its own withdrawal.
        path_list.bake_urpf(accepting_interfaces[..4].to_vec())?;
        let urpf = path_list.urpf_index().unwrap();
        assert_eq!(net.set_load_balance_urpf(forwarding, urpf)?, Some(()));
        drop(path_list);
        assert_eq!(
            net.urpf_check(shared_urpf, accepting_interfaces[7]),
            Some(true)
        );
        assert_eq!(net.urpf_check(urpf, accepting_interfaces[7]), Some(false));
        assert_eq!(net.urpf_check(urpf, accepting_interfaces[0]), Some(true));
        net.unlock_dpo(other_forwarding);
        assert!(net.urpf_list(shared_urpf).is_none());
        let mut table = Ip4FibTable::new(Ip4FibBackend::default());
        table
            .add_route(destination, FibSource::API, forwarding)
            .unwrap();
        net.unlock_dpo(forwarding);
        for count in [4, 8, 1, 8, 4] {
            assert_eq!(
                net.update_load_balance(runtime, forwarding, &rx_paths[..count])?,
                Some(())
            );
            let selected = table.forwarding_lookup(destination.addr()).unwrap();
            assert_eq!(selected, forwarding);
            for (bucket, &expected) in rx_paths[..count].iter().enumerate() {
                assert_eq!(
                    net.select_load_balance(selected, |bucket_count, _| {
                        assert_eq!(usize::from(bucket_count), count);
                        Some(bucket as u32)
                    }),
                    Some(expected.dpo)
                );
            }
            assert_eq!(LoadBalanceDpo::memory().1, load_balance_count + 1);
            assert_eq!(net.load_balance_urpf(selected), Some(urpf));
            assert_eq!(net.urpf_size(urpf), Some(4));
        }
        // VPP fib_test_sticky: three equal paths, one down, recovery, then
        // weights 3:1:1. The concrete adjacency/BFD producer is not exercised
        // here; its resolved drop projection is supplied to the LB owner.
        for (weights, down, expected) in [
            (
                [1, 1, 1],
                None,
                [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2],
            ),
            (
                [1, 1, 1],
                Some(1),
                [0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 0, 2, 2, 2, 2, 2],
            ),
            (
                [1, 1, 1],
                None,
                [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2],
            ),
            (
                [3, 1, 1],
                None,
                [1, 1, 1, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                [3, 1, 1],
                Some(1),
                [2, 0, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
        ] {
            let mut paths = [rx_paths[0], rx_paths[1], rx_paths[2]];
            for (index, path) in paths.iter_mut().enumerate() {
                path.weight = weights[index];
                if down == Some(index) {
                    path.dpo = DpoId::drop(DpoProto::IP4);
                }
            }
            assert_eq!(
                net.update_load_balance(runtime, forwarding, &paths)?,
                Some(())
            );
            let selected = table.forwarding_lookup(destination.addr()).unwrap();
            for (bucket, path) in expected.into_iter().enumerate() {
                assert_eq!(
                    net.select_load_balance(selected, |count, _| {
                        assert_eq!(count, 16);
                        Some(bucket as u32)
                    }),
                    Some(rx_paths[path].dpo)
                );
            }
        }
        assert_eq!(net.update_load_balance(runtime, forwarding, &[])?, Some(()));
        assert_eq!(
            net.select_load_balance(forwarding, |count, _| {
                assert_eq!(count, 1);
                Some(0)
            }),
            Some(DpoId::drop(DpoProto::IP4))
        );
        assert_eq!(
            net.update_load_balance(runtime, forwarding, &rx_paths[..4])?,
            Some(())
        );
        // Hammer rejects VPP's empty normalized-prefix corner instead of
        // publishing uninitialized buckets or inverting the requested weights.
        for weights in [[0, 1, 1], [1, 1, 65535]] {
            let mut paths = [rx_paths[0], rx_paths[1], rx_paths[2]];
            for (path, weight) in paths.iter_mut().zip(weights) {
                path.weight = weight;
            }
            assert!(matches!(
                net.update_load_balance(runtime, forwarding, &paths),
                Err(DpoError::InvalidBucketCount)
            ));
            assert_eq!(
                table.forwarding_lookup(destination.addr()),
                Some(forwarding)
            );
            for (bucket, path) in rx_paths[..4].iter().enumerate() {
                assert_eq!(
                    net.select_load_balance(forwarding, |count, _| {
                        assert_eq!(count, 4);
                        Some(bucket as u32)
                    }),
                    Some(path.dpo)
                );
            }
            assert_eq!(LoadBalanceDpo::memory().1, load_balance_count + 1);
            assert_eq!(
                hammer_service::net::InterfaceRxDpo::memory().1,
                rx_count + 8
            );
        }
        for &path in &rx_paths {
            net.unlock_dpo(path.dpo);
        }
        assert_eq!(
            hammer_service::net::InterfaceRxDpo::memory().1,
            rx_count + 4
        );
        table.remove_route(destination, FibSource::API).unwrap();
        assert_eq!(table.forwarding_lookup(destination.addr()), None);
        assert_eq!(LoadBalanceDpo::memory().1, load_balance_count);
        assert!(net.urpf_list(urpf).is_none());
        assert_eq!(hammer_service::net::InterfaceRxDpo::memory().1, rx_count);
        drop(table);
        for hardware in hardware_interfaces {
            interfaces.delete_hardware_interface(hardware).unwrap();
        }
        crate::local::tests::receive_interface_and_checksum(runtime).unwrap();
        crate::icmp_error::error_response_source_and_origin(runtime)?;
        main.close()?;
        GlobalMain::uninstall_current();
        Ok(())
    }
}
