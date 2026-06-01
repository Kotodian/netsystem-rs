use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ipnet::{IpNet, Ipv4Net, Ipv6Net};

use crate::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};

use super::dpo::{Adjacency, AdjacencyIndex, DpoId};
use super::ip4_mtrie::{Ip4Mtrie, Ip4MtrieRoute, Ip4MtrieValue};
use super::ip6_fib::Ip6Fib;
use super::load_balance::{LoadBalance, LoadBalanceIndex};
use crate::ds::prefetch::prefetch_read_l1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FibEntry {
    pub prefix: IpNet,
    pub load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FibRouteDpoIndex(u32);

impl FibRouteDpoIndex {
    #[inline(always)]
    const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    const fn slot(self) -> usize {
        self.0 as usize
    }
}

impl Ip4MtrieValue for FibRouteDpoIndex {
    #[inline(always)]
    fn into_leaf_value(self) -> u32 {
        self.0
    }

    #[inline(always)]
    fn from_leaf_value(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone)]
pub struct FibSnapshot<N: Copy> {
    lookup: FibLookupTables<N>,
    adjacencies: Box<[Adjacency<N>]>,
    drop_next: N,
    ip4_drop: DpoId<N>,
    ip6_drop: DpoId<N>,
}

#[derive(Debug, Clone)]
#[repr(C)]
struct FibLookupTables<N: Copy> {
    ip4: Ip4Mtrie<FibRouteDpoIndex>,
    ip6: Ip6Fib<FibRouteDpoIndex>,
    route_dpos: Box<[DpoId<N>]>,
    load_balances: Box<[LoadBalance<N>]>,
}

impl<N: Copy> FibSnapshot<N> {
    #[inline(always)]
    pub fn lookup_packet(&self, packet: &ParsedIpPacket) -> Option<FibLookupResult<N>> {
        let hash = flow_hash(packet);
        match packet.destination {
            IpAddr::V4(destination) => self.lookup_ip4(destination, hash),
            IpAddr::V6(destination) => self.lookup_ip6(destination, hash),
        }
    }

    #[inline(always)]
    pub fn prefetch_packet(&self, packet: &ParsedIpPacket) {
        match packet.destination {
            IpAddr::V4(destination) => self.prefetch_ip4(destination),
            IpAddr::V6(destination) => self.prefetch_ip6(destination),
        }
    }

    #[inline(always)]
    pub fn lookup_ip4(&self, destination: Ipv4Addr, hash: usize) -> Option<FibLookupResult<N>> {
        let route_dpo = self.lookup.ip4.lookup(destination)?;
        self.select_route_dpo(route_dpo, hash)
    }

    #[inline(always)]
    pub fn lookup_ip6(&self, destination: Ipv6Addr, hash: usize) -> Option<FibLookupResult<N>> {
        let route_dpo = self.lookup.ip6.lookup(destination)?;
        self.select_route_dpo(route_dpo, hash)
    }

    #[inline(always)]
    pub fn prefetch_ip4(&self, destination: Ipv4Addr) {
        self.lookup.ip4.prefetch(destination);
    }

    #[inline(always)]
    pub fn prefetch_ip6(&self, destination: Ipv6Addr) {
        self.lookup.ip6.prefetch_destination(destination);
    }

    #[inline(always)]
    pub fn load_balance(&self, index: LoadBalanceIndex) -> Option<&LoadBalance<N>> {
        self.lookup.load_balances.get(index.slot())
    }

    #[inline(always)]
    pub fn adjacency(&self, index: AdjacencyIndex) -> Option<Adjacency<N>> {
        self.adjacencies.get(index.slot()).copied()
    }

    #[inline(always)]
    pub fn drop_next(&self) -> N {
        self.drop_next
    }

    #[inline(always)]
    pub fn drop_dpo(&self, version: IpVersion) -> DpoId<N> {
        match version {
            IpVersion::V4 => self.ip4_drop,
            IpVersion::V6 => self.ip6_drop,
        }
    }

    #[inline(always)]
    fn select_route_dpo(
        &self,
        route_dpo_index: FibRouteDpoIndex,
        hash: usize,
    ) -> Option<FibLookupResult<N>> {
        let route_dpo = *self.lookup.route_dpos.get(route_dpo_index.slot())?;
        let Some(load_balance) = route_dpo.load_balance_index() else {
            return Some(FibLookupResult::terminal(route_dpo));
        };
        let load_balance_ref = self.load_balance(load_balance)?;
        prefetch_read_l1(load_balance_ref);
        load_balance_ref.prefetch_bucket(hash);
        let (bucket_index, dpo) = load_balance_ref.select_hash(hash)?;
        Some(FibLookupResult::from_load_balance(
            route_dpo,
            load_balance,
            bucket_index,
            dpo,
        ))
    }
}

const FIB_LOOKUP_NO_LOAD_BALANCE: u32 = u32::MAX;
const FIB_LOOKUP_NO_BUCKET: u16 = u16::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibLookupResult<N> {
    pub route_dpo: DpoId<N>,
    load_balance: u32,
    bucket_index: u16,
    pub dpo: DpoId<N>,
}

impl<N: Copy> FibLookupResult<N> {
    #[inline(always)]
    pub fn terminal(route_dpo: DpoId<N>) -> Self {
        Self {
            route_dpo,
            load_balance: FIB_LOOKUP_NO_LOAD_BALANCE,
            bucket_index: FIB_LOOKUP_NO_BUCKET,
            dpo: route_dpo,
        }
    }

    #[inline(always)]
    pub fn from_load_balance(
        route_dpo: DpoId<N>,
        load_balance: LoadBalanceIndex,
        bucket_index: u16,
        dpo: DpoId<N>,
    ) -> Self {
        Self {
            route_dpo,
            load_balance: load_balance.get(),
            bucket_index,
            dpo,
        }
    }

    #[inline(always)]
    pub fn load_balance(&self) -> Option<LoadBalanceIndex> {
        (self.load_balance != FIB_LOOKUP_NO_LOAD_BALANCE)
            .then_some(LoadBalanceIndex::new(self.load_balance))
    }

    #[inline(always)]
    pub fn bucket_index(&self) -> Option<u16> {
        (self.bucket_index != FIB_LOOKUP_NO_BUCKET).then_some(self.bucket_index)
    }

    #[inline(always)]
    pub fn forwarding_load_balance_index(&self) -> u32 {
        self.load_balance
    }

    #[inline(always)]
    pub fn forwarding_bucket_index(&self) -> u16 {
        self.bucket_index
    }
}

pub struct FibSnapshotBuilder<N: Copy> {
    drop_next: N,
    load_balances: Vec<LoadBalance<N>>,
    route_dpos: Vec<DpoId<N>>,
    adjacencies: Vec<Adjacency<N>>,
    ip4_routes: Vec<Ip4Route>,
    ip6_routes: Vec<Ip6Route>,
}

impl<N: Copy> FibSnapshotBuilder<N> {
    #[inline]
    pub fn new(drop_next: N) -> Self {
        Self {
            drop_next,
            load_balances: Vec::new(),
            route_dpos: Vec::new(),
            adjacencies: Vec::new(),
            ip4_routes: Vec::new(),
            ip6_routes: Vec::new(),
        }
    }

    #[inline]
    pub fn add_adjacency(&mut self, proto: IpVersion, next: N) -> AdjacencyIndex {
        let index = AdjacencyIndex::new(self.adjacencies.len() as u32);
        self.adjacencies.push(Adjacency { next, proto });
        index
    }

    #[inline]
    fn add_route_dpo_entry(&mut self, dpo: DpoId<N>) -> FibRouteDpoIndex {
        let index = FibRouteDpoIndex::new(self.route_dpos.len() as u32);
        self.route_dpos.push(dpo);
        index
    }

    #[inline]
    pub fn add_load_balance(&mut self, buckets: impl Into<Vec<DpoId<N>>>) -> LoadBalanceIndex {
        let index = LoadBalanceIndex::new(self.load_balances.len() as u32);
        self.load_balances.push(LoadBalance::new(buckets));
        index
    }

    #[inline]
    pub fn add_ip4_route(&mut self, prefix: Ipv4Net, load_balance: LoadBalanceIndex) {
        let route_dpo = self.add_route_dpo_entry(DpoId::load_balance(
            IpVersion::V4,
            load_balance,
            self.drop_next,
        ));
        self.ip4_routes.push(Ip4Route { prefix, route_dpo });
    }

    #[inline]
    pub fn add_ip6_route(&mut self, prefix: Ipv6Net, load_balance: LoadBalanceIndex) {
        let route_dpo = self.add_route_dpo_entry(DpoId::load_balance(
            IpVersion::V6,
            load_balance,
            self.drop_next,
        ));
        self.ip6_routes.push(Ip6Route { prefix, route_dpo });
    }

    #[inline]
    pub fn add_ip4_route_dpo(&mut self, prefix: Ipv4Net, dpo: DpoId<N>) {
        let route_dpo = self.add_route_dpo_entry(dpo);
        self.ip4_routes.push(Ip4Route { prefix, route_dpo });
    }

    #[inline]
    pub fn add_ip6_route_dpo(&mut self, prefix: Ipv6Net, dpo: DpoId<N>) {
        let route_dpo = self.add_route_dpo_entry(dpo);
        self.ip6_routes.push(Ip6Route { prefix, route_dpo });
    }

    #[inline]
    pub fn add_route(&mut self, prefix: IpNet, load_balance: LoadBalanceIndex) {
        match prefix {
            IpNet::V4(prefix) => self.add_ip4_route(prefix, load_balance),
            IpNet::V6(prefix) => self.add_ip6_route(prefix, load_balance),
        }
    }

    #[inline]
    pub fn build(mut self) -> FibSnapshot<N> {
        self.ip4_routes
            .sort_by_key(|route| route.prefix.prefix_len());
        FibSnapshot {
            lookup: FibLookupTables {
                ip4: Ip4Mtrie::from_routes(
                    self.ip4_routes
                        .iter()
                        .map(|route| Ip4MtrieRoute::new(route.prefix, route.route_dpo)),
                ),
                ip6: Ip6Fib::from_routes(
                    self.ip6_routes
                        .iter()
                        .map(|route| (route.prefix, route.route_dpo)),
                ),
                route_dpos: self.route_dpos.into_boxed_slice(),
                load_balances: self.load_balances.into_boxed_slice(),
            },
            adjacencies: self.adjacencies.into_boxed_slice(),
            drop_next: self.drop_next,
            ip4_drop: DpoId::drop(IpVersion::V4, self.drop_next),
            ip6_drop: DpoId::drop(IpVersion::V6, self.drop_next),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Ip4Route {
    prefix: Ipv4Net,
    route_dpo: FibRouteDpoIndex,
}

#[derive(Debug, Clone, Copy)]
struct Ip6Route {
    prefix: Ipv6Net,
    route_dpo: FibRouteDpoIndex,
}

#[inline(always)]
pub fn flow_hash(packet: &ParsedIpPacket) -> usize {
    let mut value = 0x9e37_79b9_7f4a_7c15u64 ^ u64::from(ip_protocol_number(packet.protocol));
    value = mix_ip(value, packet.source);
    value = mix_ip(value, packet.destination);
    value as usize
}

#[inline(always)]
fn mix_ip(mut value: u64, addr: IpAddr) -> u64 {
    match addr {
        IpAddr::V4(addr) => {
            value ^= u32::from(addr) as u64;
            value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            value ^= raw as u64;
            value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= (raw >> 64) as u64;
            value = value.rotate_left(31).wrapping_mul(0x2545_f491_4f6c_dd1d);
        }
    }
    value
}

#[inline(always)]
fn ip_protocol_number(protocol: IpProtocol) -> u8 {
    match protocol {
        IpProtocol::Icmpv4 => 1,
        IpProtocol::Tcp => 6,
        IpProtocol::Udp => 17,
        IpProtocol::Icmpv6 => 58,
        IpProtocol::Other(value) => value,
    }
}
