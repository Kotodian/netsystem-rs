use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, ForwardingDpoType, ForwardingMetadata,
    InternalNode, Node, NodeId, NodeNextFrames, NodeResult, for_each_buffer_frame_index,
};
use hammer_core::error::{CoreResult, HammerResult};
use hammer_core::protocol::ip::{
    IpProtocol, IpVersion, ParsedIpPacket, parse_ip_packet_with_chain_len,
};
use hammer_runtime::ControlThreadHandle;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use rustc_hash::FxBuildHasher;

const IP4_MTRIE_ROOT_BITS: u8 = 16;
const IP4_MTRIE_PLY_BITS: u8 = 8;
const IP4_MTRIE_ROOT_LEN: usize = 1 << IP4_MTRIE_ROOT_BITS;
const IP4_MTRIE_PLY_LEN: usize = 1 << IP4_MTRIE_PLY_BITS;
const MTRIE_LEAF_EMPTY: u32 = 0;
const MTRIE_LEAF_TERMINAL: u32 = 1 << 31;
const MTRIE_LEAF_INDEX_MASK: u32 = MTRIE_LEAF_TERMINAL - 1;
const FORWARDING_MISS_INDEX: u32 = u32::MAX;
const FORWARDING_MISS_BUCKET: u16 = u16::MAX;

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
    const fn slot(self) -> usize {
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
    const fn slot(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpoType {
    Drop,
    Punt,
    Adjacency,
}

impl From<DpoType> for ForwardingDpoType {
    #[inline(always)]
    fn from(value: DpoType) -> Self {
        match value {
            DpoType::Drop => Self::Drop,
            DpoType::Punt => Self::Punt,
            DpoType::Adjacency => Self::Adjacency,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DpoId {
    pub dpo_type: DpoType,
    pub proto: IpVersion,
    pub index: u32,
    pub next_node: NodeId,
}

impl DpoId {
    #[inline(always)]
    pub const fn drop(proto: IpVersion, next_node: NodeId) -> Self {
        Self {
            dpo_type: DpoType::Drop,
            proto,
            index: 0,
            next_node,
        }
    }

    #[inline(always)]
    pub const fn punt(proto: IpVersion, next_node: NodeId) -> Self {
        Self {
            dpo_type: DpoType::Punt,
            proto,
            index: 0,
            next_node,
        }
    }

    #[inline(always)]
    pub const fn adjacency(proto: IpVersion, adjacency: AdjacencyIndex, next_node: NodeId) -> Self {
        Self {
            dpo_type: DpoType::Adjacency,
            proto,
            index: adjacency.get(),
            next_node,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadBalance {
    buckets: Box<[DpoId]>,
}

impl LoadBalance {
    #[inline]
    pub fn new(buckets: impl Into<Vec<DpoId>>) -> Self {
        Self {
            buckets: buckets.into().into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    #[inline(always)]
    fn select(&self, packet: &ParsedIpPacket) -> Option<(u16, DpoId)> {
        if self.buckets.is_empty() {
            return None;
        }
        let bucket = flow_hash(packet) % self.buckets.len();
        Some((bucket as u16, self.buckets[bucket]))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency {
    pub proto: IpVersion,
    pub next_node: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FibEntry {
    pub prefix: IpNet,
    pub load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone)]
pub struct FibSnapshot {
    ip4: Ip4Mtrie,
    ip6: Ip6ForwardingTable,
    load_balances: Box<[LoadBalance]>,
    adjacencies: Box<[Adjacency]>,
    drop_node: NodeId,
    ip4_drop: DpoId,
    ip6_drop: DpoId,
}

impl FibSnapshot {
    #[inline(always)]
    pub fn lookup(&self, packet: &ParsedIpPacket) -> Option<FibLookupResult> {
        match packet.destination {
            IpAddr::V4(destination) => self.lookup_ip4(packet, destination),
            IpAddr::V6(destination) => self.lookup_ip6(packet, destination),
        }
    }

    #[inline(always)]
    pub fn load_balance(&self, index: LoadBalanceIndex) -> Option<&LoadBalance> {
        self.load_balances.get(index.slot())
    }

    #[inline(always)]
    pub fn adjacency(&self, index: AdjacencyIndex) -> Option<Adjacency> {
        self.adjacencies.get(index.slot()).copied()
    }

    #[inline(always)]
    pub fn drop_dpo(&self, version: IpVersion) -> DpoId {
        match version {
            IpVersion::V4 => self.ip4_drop,
            IpVersion::V6 => self.ip6_drop,
        }
    }

    #[inline(always)]
    fn lookup_ip4(
        &self,
        packet: &ParsedIpPacket,
        destination: Ipv4Addr,
    ) -> Option<FibLookupResult> {
        let load_balance = self.ip4.lookup(destination)?;
        self.select_load_balance(load_balance, packet)
    }

    #[inline(always)]
    fn lookup_ip6(
        &self,
        packet: &ParsedIpPacket,
        destination: Ipv6Addr,
    ) -> Option<FibLookupResult> {
        let load_balance = self.ip6.lookup(destination)?;
        self.select_load_balance(load_balance, packet)
    }

    #[inline(always)]
    fn select_load_balance(
        &self,
        load_balance: LoadBalanceIndex,
        packet: &ParsedIpPacket,
    ) -> Option<FibLookupResult> {
        let (bucket_index, dpo) = self.load_balance(load_balance)?.select(packet)?;
        Some(FibLookupResult {
            load_balance,
            bucket_index,
            dpo,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibLookupResult {
    pub load_balance: LoadBalanceIndex,
    pub bucket_index: u16,
    pub dpo: DpoId,
}

pub struct FibSnapshotBuilder {
    drop_node: NodeId,
    load_balances: Vec<LoadBalance>,
    adjacencies: Vec<Adjacency>,
    ip4_routes: Vec<Ip4Route>,
    ip6_routes: Vec<Ip6Route>,
}

impl FibSnapshotBuilder {
    #[inline]
    pub fn new(drop_node: NodeId) -> Self {
        Self {
            drop_node,
            load_balances: Vec::new(),
            adjacencies: Vec::new(),
            ip4_routes: Vec::new(),
            ip6_routes: Vec::new(),
        }
    }

    #[inline]
    pub fn add_adjacency(&mut self, proto: IpVersion, next_node: NodeId) -> AdjacencyIndex {
        let index = AdjacencyIndex::new(self.adjacencies.len() as u32);
        self.adjacencies.push(Adjacency { proto, next_node });
        index
    }

    #[inline]
    pub fn add_load_balance(&mut self, buckets: impl Into<Vec<DpoId>>) -> LoadBalanceIndex {
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
    pub fn build(mut self) -> FibSnapshot {
        self.ip4_routes
            .sort_by_key(|route| route.prefix.prefix_len());
        FibSnapshot {
            ip4: Ip4Mtrie::from_routes(&self.ip4_routes),
            ip6: Ip6ForwardingTable::from_routes(&self.ip6_routes),
            load_balances: self.load_balances.into_boxed_slice(),
            adjacencies: self.adjacencies.into_boxed_slice(),
            drop_node: self.drop_node,
            ip4_drop: DpoId::drop(IpVersion::V4, self.drop_node),
            ip6_drop: DpoId::drop(IpVersion::V6, self.drop_node),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FibSnapshotHandle {
    inner: Arc<ArcSwap<FibSnapshot>>,
}

impl FibSnapshotHandle {
    #[inline]
    pub fn new(snapshot: FibSnapshot) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(snapshot)),
        }
    }

    #[inline]
    pub fn load(&self) -> arc_swap::Guard<Arc<FibSnapshot>> {
        self.inner.load()
    }

    #[inline]
    fn store(&self, snapshot: FibSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }
}

pub struct IpLookupControlPlane {
    snapshot: FibSnapshotHandle,
    control_handle: Option<Arc<ControlThreadHandle>>,
}

impl IpLookupControlPlane {
    #[inline]
    pub fn new(snapshot: FibSnapshot) -> Self {
        Self {
            snapshot: FibSnapshotHandle::new(snapshot),
            control_handle: None,
        }
    }

    #[inline]
    pub fn from_handle(snapshot: FibSnapshotHandle) -> Self {
        Self {
            snapshot,
            control_handle: None,
        }
    }

    #[inline]
    pub fn with_control_handle(mut self, control_handle: Arc<ControlThreadHandle>) -> Self {
        self.control_handle = Some(control_handle);
        self
    }

    #[inline]
    pub fn snapshot_handle(&self) -> FibSnapshotHandle {
        self.snapshot.clone()
    }

    #[inline]
    pub fn node(&self) -> IpLookupNode {
        IpLookupNode::new(self.snapshot_handle())
    }

    #[inline]
    pub fn publish(&self, snapshot: FibSnapshot) -> HammerResult<()> {
        let snapshot_handle = self.snapshot.clone();
        if let Some(control_handle) = &self.control_handle {
            control_handle.call(move || snapshot_handle.store(snapshot))?;
        } else {
            snapshot_handle.store(snapshot);
        }
        Ok(())
    }
}

pub struct IpLookupNode {
    snapshot: FibSnapshotHandle,
}

impl IpLookupNode {
    #[inline]
    pub fn new(snapshot: FibSnapshotHandle) -> Self {
        Self { snapshot }
    }

    #[inline(always)]
    fn process_index<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        next_frames: &mut NodeNextFrames,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let snapshot = self.snapshot.load();
        let next_node = {
            let mut buffer = runtime.get_buffer_mut(index)?;
            let parsed = match parse_ip_packet_with_chain_len(
                buffer.current(),
                buffer.total_len_not_including_first(),
            ) {
                Ok(parsed) => parsed,
                Err(_) => {
                    buffer.metadata_mut().forwarding = None;
                    return next_frames.enqueue(runtime, snapshot.drop_node, index);
                }
            };
            let result = match snapshot.lookup(&parsed) {
                Some(result) => result,
                None => FibLookupResult {
                    load_balance: LoadBalanceIndex::new(FORWARDING_MISS_INDEX),
                    bucket_index: FORWARDING_MISS_BUCKET,
                    dpo: snapshot.drop_dpo(parsed.version),
                },
            };
            buffer.metadata_mut().forwarding = Some(ForwardingMetadata {
                fib_index: 0,
                load_balance_index: result.load_balance.get(),
                bucket_index: result.bucket_index,
                dpo_type: result.dpo.dpo_type.into(),
                dpo_index: result.dpo.index,
            });
            result.dpo.next_node
        };
        next_frames.enqueue(runtime, next_node, index)
    }
}

impl<G> Node<G> for IpLookupNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut next_frames = NodeNextFrames::default();
        for_each_buffer_frame_index!(runtime, frame, |index| {
            self.process_index(runtime, &mut next_frames, index)
        })?;
        frame.clear();
        next_frames.schedule(runtime)?;
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for IpLookupNode {}

#[derive(Debug, Clone, Copy)]
struct Ip4Route {
    prefix: Ipv4Net,
    load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone)]
struct Ip4Mtrie {
    root: Box<[MtrieLeaf]>,
    plies: Vec<Ip4MtriePly>,
}

impl Ip4Mtrie {
    #[inline]
    fn empty() -> Self {
        Self {
            root: vec![MtrieLeaf::empty(); IP4_MTRIE_ROOT_LEN].into_boxed_slice(),
            plies: Vec::new(),
        }
    }

    #[inline]
    fn from_routes(routes: &[Ip4Route]) -> Self {
        let mut trie = Self::empty();
        for route in routes.iter().copied() {
            trie.insert(route.prefix, route.load_balance);
        }
        trie
    }

    #[inline]
    fn insert(&mut self, prefix: Ipv4Net, load_balance: LoadBalanceIndex) {
        let terminal = MtrieLeaf::terminal(load_balance);
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
    fn lookup(&self, destination: Ipv4Addr) -> Option<LoadBalanceIndex> {
        let octets = destination.octets();
        let root_value = u16::from_be_bytes([octets[0], octets[1]]) as usize;
        let mut leaf = self.root[root_value];
        if let Some(load_balance) = leaf.load_balance() {
            return Some(load_balance);
        }
        let first_ply = leaf.ply_index()?;
        leaf = self.plies.get(first_ply)?.leaves[octets[2] as usize];
        if let Some(load_balance) = leaf.load_balance() {
            return Some(load_balance);
        }
        let second_ply = leaf.ply_index()?;
        leaf = self.plies.get(second_ply)?.leaves[octets[3] as usize];
        leaf.load_balance()
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
    const fn terminal(load_balance: LoadBalanceIndex) -> Self {
        Self(MTRIE_LEAF_TERMINAL | (load_balance.get() & MTRIE_LEAF_INDEX_MASK))
    }

    #[inline(always)]
    fn ply(index: usize) -> Self {
        Self((index as u32) + 1)
    }

    #[inline(always)]
    fn load_balance(self) -> Option<LoadBalanceIndex> {
        ((self.0 & MTRIE_LEAF_TERMINAL) != 0)
            .then(|| LoadBalanceIndex::new(self.0 & MTRIE_LEAF_INDEX_MASK))
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

type Ip6Map = HashMap<Ip6PrefixKey, LoadBalanceIndex, FxBuildHasher>;

#[derive(Debug, Clone, Copy)]
struct Ip6Route {
    prefix: Ipv6Net,
    load_balance: LoadBalanceIndex,
}

#[derive(Debug, Clone)]
struct Ip6ForwardingTable {
    routes: Ip6Map,
    prefix_lengths: Box<[u8]>,
}

impl Ip6ForwardingTable {
    #[inline]
    fn from_routes(routes: &[Ip6Route]) -> Self {
        let mut table = HashMap::with_hasher(FxBuildHasher::default());
        let mut prefix_lengths = [false; 129];
        for route in routes.iter().copied() {
            let prefix_len = route.prefix.prefix_len();
            prefix_lengths[prefix_len as usize] = true;
            table.insert(
                Ip6PrefixKey::new(route.prefix.addr(), prefix_len),
                route.load_balance,
            );
        }
        let prefix_lengths = (0u8..=128)
            .rev()
            .filter(|prefix_len| prefix_lengths[*prefix_len as usize])
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            routes: table,
            prefix_lengths,
        }
    }

    #[inline(always)]
    fn lookup(&self, destination: Ipv6Addr) -> Option<LoadBalanceIndex> {
        for prefix_len in self.prefix_lengths.iter().copied() {
            let key = Ip6PrefixKey::new(destination, prefix_len);
            if let Some(load_balance) = self.routes.get(&key).copied() {
                return Some(load_balance);
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Ip6PrefixKey {
    masked: u128,
    prefix_len: u8,
}

impl Ip6PrefixKey {
    #[inline(always)]
    fn new(addr: Ipv6Addr, prefix_len: u8) -> Self {
        Self {
            masked: mask_ipv6(addr, prefix_len),
            prefix_len,
        }
    }
}

#[inline(always)]
fn mask_ipv6(addr: Ipv6Addr, prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        return 0;
    }
    let value = u128::from(addr);
    value & (!0u128 << (128 - prefix_len))
}

#[inline(always)]
fn flow_hash(packet: &ParsedIpPacket) -> usize {
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
