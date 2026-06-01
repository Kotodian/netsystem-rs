use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use rustc_hash::FxBuildHasher;

use crate::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};

const IP4_MTRIE_ROOT_BITS: u8 = 16;
const IP4_MTRIE_PLY_BITS: u8 = 8;
const IP4_MTRIE_ROOT_LEN: usize = 1 << IP4_MTRIE_ROOT_BITS;
const IP4_MTRIE_PLY_LEN: usize = 1 << IP4_MTRIE_PLY_BITS;
const MTRIE_LEAF_EMPTY: u32 = 0;
const MTRIE_LEAF_TERMINAL: u32 = 1 << 31;
const MTRIE_LEAF_INDEX_MASK: u32 = MTRIE_LEAF_TERMINAL - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LoadBalanceIndex(u32);

impl LoadBalanceIndex {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdjacencyIndex(u32);

impl AdjacencyIndex {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpoType {
    Drop,
    Punt,
    Adjacency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DpoId<N> {
    pub dpo_type: DpoType,
    pub proto: IpVersion,
    pub index: u32,
    pub next: N,
}

impl<N> DpoId<N> {
    #[inline(always)]
    pub const fn drop(proto: IpVersion, next: N) -> Self {
        Self {
            dpo_type: DpoType::Drop,
            proto,
            index: 0,
            next,
        }
    }

    #[inline(always)]
    pub const fn punt(proto: IpVersion, next: N) -> Self {
        Self {
            dpo_type: DpoType::Punt,
            proto,
            index: 0,
            next,
        }
    }

    #[inline(always)]
    pub const fn adjacency(proto: IpVersion, adjacency: AdjacencyIndex, next: N) -> Self {
        Self {
            dpo_type: DpoType::Adjacency,
            proto,
            index: adjacency.get(),
            next,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadBalance<N> {
    buckets: Box<[DpoId<N>]>,
}

impl<N> LoadBalance<N> {
    #[inline]
    pub fn new(buckets: impl Into<Vec<DpoId<N>>>) -> Self {
        Self {
            buckets: buckets.into().into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    #[inline(always)]
    pub fn buckets(&self) -> &[DpoId<N>] {
        &self.buckets
    }
}

impl<N: Copy> LoadBalance<N> {
    #[inline(always)]
    pub fn select_hash(&self, hash: usize) -> Option<(u16, DpoId<N>)> {
        if self.buckets.is_empty() {
            return None;
        }
        let bucket = hash % self.buckets.len();
        Some((bucket as u16, self.buckets[bucket]))
    }

    #[inline(always)]
    pub fn select_packet(&self, packet: &ParsedIpPacket) -> Option<(u16, DpoId<N>)> {
        self.select_hash(flow_hash(packet))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency<N> {
    pub proto: IpVersion,
    pub next: N,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FibEntry {
    pub prefix: IpNet,
    pub load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone)]
pub struct FibSnapshot<N> {
    ip4: Ip4Mtrie<LoadBalanceIndex>,
    ip6: Ip6PrefixHashTable<LoadBalanceIndex>,
    load_balances: Box<[LoadBalance<N>]>,
    adjacencies: Box<[Adjacency<N>]>,
    drop_next: N,
    ip4_drop: DpoId<N>,
    ip6_drop: DpoId<N>,
}

impl<N: Copy> FibSnapshot<N> {
    #[inline(always)]
    pub fn lookup_packet(&self, packet: &ParsedIpPacket) -> Option<FibLookupResult<N>> {
        match packet.destination {
            IpAddr::V4(destination) => self.lookup_ip4(destination, flow_hash(packet)),
            IpAddr::V6(destination) => self.lookup_ip6(destination, flow_hash(packet)),
        }
    }

    #[inline(always)]
    pub fn lookup_ip4(&self, destination: Ipv4Addr, hash: usize) -> Option<FibLookupResult<N>> {
        let load_balance = self.ip4.lookup(destination)?;
        self.select_load_balance(load_balance, hash)
    }

    #[inline(always)]
    pub fn lookup_ip6(&self, destination: Ipv6Addr, hash: usize) -> Option<FibLookupResult<N>> {
        let load_balance = self.ip6.lookup(destination)?;
        self.select_load_balance(load_balance, hash)
    }

    #[inline(always)]
    pub fn load_balance(&self, index: LoadBalanceIndex) -> Option<&LoadBalance<N>> {
        self.load_balances.get(index.slot())
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
    fn select_load_balance(
        &self,
        load_balance: LoadBalanceIndex,
        hash: usize,
    ) -> Option<FibLookupResult<N>> {
        let (bucket_index, dpo) = self.load_balance(load_balance)?.select_hash(hash)?;
        Some(FibLookupResult {
            load_balance,
            bucket_index,
            dpo,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibLookupResult<N> {
    pub load_balance: LoadBalanceIndex,
    pub bucket_index: u16,
    pub dpo: DpoId<N>,
}

pub struct FibSnapshotBuilder<N> {
    drop_next: N,
    load_balances: Vec<LoadBalance<N>>,
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
            adjacencies: Vec::new(),
            ip4_routes: Vec::new(),
            ip6_routes: Vec::new(),
        }
    }

    #[inline]
    pub fn add_adjacency(&mut self, proto: IpVersion, next: N) -> AdjacencyIndex {
        let index = AdjacencyIndex::new(self.adjacencies.len() as u32);
        self.adjacencies.push(Adjacency { proto, next });
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
        self.ip4_routes.push(Ip4Route {
            prefix,
            load_balance,
        });
    }

    #[inline]
    pub fn add_ip6_route(&mut self, prefix: Ipv6Net, load_balance: LoadBalanceIndex) {
        self.ip6_routes.push(Ip6Route {
            prefix,
            load_balance,
        });
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
            ip4: Ip4Mtrie::from_routes(
                self.ip4_routes
                    .iter()
                    .map(|route| Ip4MtrieRoute::new(route.prefix, route.load_balance)),
            ),
            ip6: Ip6PrefixHashTable::from_routes(
                self.ip6_routes
                    .iter()
                    .map(|route| (route.prefix, route.load_balance)),
            ),
            load_balances: self.load_balances.into_boxed_slice(),
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
    load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone, Copy)]
struct Ip6Route {
    prefix: Ipv6Net,
    load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ip4MtrieRoute<V> {
    pub prefix: Ipv4Net,
    pub value: V,
}

impl<V> Ip4MtrieRoute<V> {
    #[inline(always)]
    pub fn new(prefix: Ipv4Net, value: V) -> Self {
        Self { prefix, value }
    }
}

#[derive(Debug, Clone)]
pub struct Ip4Mtrie<V> {
    root: Box<[MtrieLeaf]>,
    plies: Vec<Ip4MtriePly>,
    values: Vec<V>,
}

impl<V: Copy> Ip4Mtrie<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            root: vec![MtrieLeaf::empty(); IP4_MTRIE_ROOT_LEN].into_boxed_slice(),
            plies: Vec::new(),
            values: Vec::new(),
        }
    }

    #[inline]
    pub fn from_routes(routes: impl IntoIterator<Item = Ip4MtrieRoute<V>>) -> Self {
        let mut routes = routes.into_iter().collect::<Vec<_>>();
        routes.sort_by_key(|route| route.prefix.prefix_len());
        let mut trie = Self::empty();
        for route in routes {
            trie.insert(route.prefix, route.value);
        }
        trie
    }

    #[inline]
    pub fn insert(&mut self, prefix: Ipv4Net, value: V) {
        let terminal = self.add_terminal(value);
        let prefix_len = prefix.prefix_len();
        let octets = prefix.addr().octets();
        let root_value = u16::from_be_bytes([octets[0], octets[1]]) as usize;
        if prefix_len <= IP4_MTRIE_ROOT_BITS {
            fill_stride(
                &mut self.root,
                root_value,
                prefix_len,
                IP4_MTRIE_ROOT_BITS,
                terminal,
            );
            return;
        }

        let first_ply = self.ensure_root_ply(root_value);
        if prefix_len <= IP4_MTRIE_ROOT_BITS + IP4_MTRIE_PLY_BITS {
            fill_stride(
                &mut self.plies[first_ply].leaves,
                octets[2] as usize,
                prefix_len - IP4_MTRIE_ROOT_BITS,
                IP4_MTRIE_PLY_BITS,
                terminal,
            );
            return;
        }

        let second_ply = self.ensure_child_ply(first_ply, octets[2] as usize);
        fill_stride(
            &mut self.plies[second_ply].leaves,
            octets[3] as usize,
            prefix_len - IP4_MTRIE_ROOT_BITS - IP4_MTRIE_PLY_BITS,
            IP4_MTRIE_PLY_BITS,
            terminal,
        );
    }

    #[inline(always)]
    pub fn lookup(&self, destination: Ipv4Addr) -> Option<V> {
        let octets = destination.octets();
        let root_value = u16::from_be_bytes([octets[0], octets[1]]) as usize;
        let mut leaf = self.root[root_value];
        if let Some(index) = leaf.value_index() {
            return self.values.get(index).copied();
        }
        let first_ply = leaf.ply_index()?;
        leaf = self.plies.get(first_ply)?.leaves[octets[2] as usize];
        if let Some(index) = leaf.value_index() {
            return self.values.get(index).copied();
        }
        let second_ply = leaf.ply_index()?;
        leaf = self.plies.get(second_ply)?.leaves[octets[3] as usize];
        let index = leaf.value_index()?;
        self.values.get(index).copied()
    }

    #[inline]
    fn add_terminal(&mut self, value: V) -> MtrieLeaf {
        let index = self.values.len();
        self.values.push(value);
        MtrieLeaf::terminal(index)
    }

    #[inline]
    fn ensure_root_ply(&mut self, root_value: usize) -> usize {
        let leaf = self.root[root_value];
        if let Some(index) = leaf.ply_index() {
            return index;
        }
        let index = self.alloc_ply(leaf);
        self.root[root_value] = MtrieLeaf::ply(index);
        index
    }

    #[inline]
    fn ensure_child_ply(&mut self, parent: usize, child: usize) -> usize {
        let leaf = self.plies[parent].leaves[child];
        if let Some(index) = leaf.ply_index() {
            return index;
        }
        let index = self.alloc_ply(leaf);
        self.plies[parent].leaves[child] = MtrieLeaf::ply(index);
        index
    }

    #[inline]
    fn alloc_ply(&mut self, inherited: MtrieLeaf) -> usize {
        let index = self.plies.len();
        self.plies.push(Ip4MtriePly::filled(inherited));
        index
    }
}

#[derive(Debug, Clone)]
struct Ip4MtriePly {
    leaves: Box<[MtrieLeaf]>,
}

impl Ip4MtriePly {
    #[inline]
    fn filled(leaf: MtrieLeaf) -> Self {
        Self {
            leaves: vec![leaf; IP4_MTRIE_PLY_LEN].into_boxed_slice(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MtrieLeaf(u32);

impl MtrieLeaf {
    #[inline(always)]
    const fn empty() -> Self {
        Self(MTRIE_LEAF_EMPTY)
    }

    #[inline(always)]
    fn terminal(value_index: usize) -> Self {
        assert!(
            value_index <= MTRIE_LEAF_INDEX_MASK as usize,
            "IPv4 mtrie value table is full"
        );
        Self(MTRIE_LEAF_TERMINAL | value_index as u32)
    }

    #[inline(always)]
    fn ply(index: usize) -> Self {
        Self((index as u32) + 1)
    }

    #[inline(always)]
    fn value_index(self) -> Option<usize> {
        ((self.0 & MTRIE_LEAF_TERMINAL) != 0).then_some((self.0 & MTRIE_LEAF_INDEX_MASK) as usize)
    }

    #[inline(always)]
    fn ply_index(self) -> Option<usize> {
        (self.0 != MTRIE_LEAF_EMPTY && (self.0 & MTRIE_LEAF_TERMINAL) == 0)
            .then(|| (self.0 - 1) as usize)
    }
}

#[inline(always)]
fn fill_stride(
    leaves: &mut [MtrieLeaf],
    value: usize,
    prefix_bits: u8,
    stride_bits: u8,
    leaf: MtrieLeaf,
) {
    let span = 1usize << (stride_bits - prefix_bits);
    let start = value & !(span - 1);
    let end = start + span;
    for slot in &mut leaves[start..end] {
        *slot = leaf;
    }
}

type Ip6Map<V> = HashMap<Ip6PrefixKey, V, FxBuildHasher>;

#[derive(Debug, Clone)]
pub struct Ip6PrefixHashTable<V> {
    routes: Ip6Map<V>,
    prefix_lengths: Box<[u8]>,
}

impl<V: Copy> Ip6PrefixHashTable<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            routes: HashMap::with_hasher(FxBuildHasher::default()),
            prefix_lengths: Box::new([]),
        }
    }

    #[inline]
    pub fn from_routes(routes: impl IntoIterator<Item = (Ipv6Net, V)>) -> Self {
        let mut table = Self::empty();
        for (prefix, value) in routes {
            table.insert(prefix, value);
        }
        table
    }

    #[inline]
    pub fn insert(&mut self, prefix: Ipv6Net, value: V) {
        let prefix_len = prefix.prefix_len();
        self.routes
            .insert(Ip6PrefixKey::new(prefix.addr(), prefix_len), value);
        if !self.prefix_lengths.contains(&prefix_len) {
            let mut lengths = self.prefix_lengths.to_vec();
            lengths.push(prefix_len);
            lengths.sort_by(|a, b| b.cmp(a));
            self.prefix_lengths = lengths.into_boxed_slice();
        }
    }

    #[inline(always)]
    pub fn lookup(&self, destination: Ipv6Addr) -> Option<V> {
        for prefix_len in self.prefix_lengths.iter().copied() {
            let key = Ip6PrefixKey::new(destination, prefix_len);
            if let Some(value) = self.routes.get(&key).copied() {
                return Some(value);
            }
        }
        None
    }

    #[inline(always)]
    pub fn prefix_lengths(&self) -> &[u8] {
        &self.prefix_lengths
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ip6PrefixKey {
    masked: u128,
    prefix_len: u8,
}

impl Ip6PrefixKey {
    #[inline(always)]
    pub fn new(addr: Ipv6Addr, prefix_len: u8) -> Self {
        Self {
            masked: mask_ipv6(addr, prefix_len),
            prefix_len,
        }
    }

    #[inline(always)]
    pub const fn masked(self) -> u128 {
        self.masked
    }

    #[inline(always)]
    pub const fn prefix_len(self) -> u8 {
        self.prefix_len
    }
}

#[inline(always)]
pub fn mask_ipv6(addr: Ipv6Addr, prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        return 0;
    }
    let value = u128::from(addr);
    value & (!0u128 << (128 - prefix_len))
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
            value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11ebu64);
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            value ^= raw as u64;
            value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11ebu64);
            value ^= (raw >> 64) as u64;
            value = value.rotate_left(31).wrapping_mul(0x2545_f491_4f6c_dd1du64);
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
