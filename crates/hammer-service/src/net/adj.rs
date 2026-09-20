use std::hash::Hash;
use std::mem::{align_of, size_of};

use hammer_infra::pool::Pool;

use crate::interface::{InterfaceMain, InterfaceMtuKind};

use super::fib_node::{FibNode, FibNodeList, FibNodePtr, FibNodeType};
use super::rewrite::RewriteHeader;
use super::{DpoId, DpoMain, DpoProto, DpoType};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdjacencyIndex(pub u32);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibProtocolId(pub u8);

impl FibProtocolId {
    pub const IP4: Self = Self(0);
    pub const IP6: Self = Self(1);
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyLookupNext {
    Incomplete = 3,
    Glean = 4,
    Rewrite = 5,
    Multicast = 6,
    Broadcast = 9,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(transparent)]
    pub struct AdjacencyFlags: u8 {
        const SYNC_WALK_ACTIVE = 1;
    }
}

pub trait FibProtocol: Copy + 'static {
    type Address: Copy + Eq + Hash;
    type Prefix: Copy + Eq + Hash;
    type AdjacencySubtype;
    type Link: Copy + Eq + Hash;

    const ID: FibProtocolId;
    const DPO_PROTOCOL: DpoProto;

    fn prefix_address(prefix: Self::Prefix) -> Self::Address;
    fn zero_address() -> Self::Address;
    fn glean_subtype(connected: Self::Prefix) -> Self::AdjacencySubtype;
    fn neighbor_subtype(next_hop: Self::Address) -> Self::AdjacencySubtype;
    fn glean_prefix(subtype: &Self::AdjacencySubtype) -> Self::Prefix;
    fn neighbor_address(subtype: &Self::AdjacencySubtype) -> Self::Address;
    fn set_glean_source(subtype: &mut Self::AdjacencySubtype, source: Self::Address);
    fn dpo_protocol(link: Self::Link) -> DpoProto;
    fn mtu_kind(link: Self::Link) -> InterfaceMtuKind;
    fn build_rewrite(
        interfaces: &InterfaceMain,
        sw_if_index: u32,
        link: Self::Link,
        destination: Option<&[u8]>,
        output: &mut [u8],
    ) -> usize;
}

#[repr(C, align(64))]
pub struct Adjacency<P: FibProtocol> {
    pub node: FibNode,
    pub config_index: u32,
    pub subtype: P::AdjacencySubtype,
    pub rewrite_header: RewriteHeader,
    pub rewrite_data: [u8; 116],
    pub delegates: u64,
    pub node_index: u32,
    pub lookup_next: AdjacencyLookupNext,
    pub link: P::Link,
    pub next_hop_protocol: FibProtocolId,
    pub flags: AdjacencyFlags,
    pub padding: [u8; 48],
}

pub struct AdjacencyMain<P: FibProtocol> {
    objects: Pool<Adjacency<P>>,
    node_type: FibNodeType,
}

impl<P: FibProtocol> AdjacencyMain<P> {
    pub fn new(node_type: FibNodeType) -> Self {
        Self {
            objects: Pool::new(),
            node_type,
        }
    }

    pub fn contains(&self, index: AdjacencyIndex) -> bool {
        self.objects.contains_key(index.0)
    }

    pub fn node(&self, index: AdjacencyIndex) -> FibNodePtr {
        assert!(self.contains(index), "adjacency index must be live");
        FibNodePtr::new(self.node_type, index.0)
    }

    pub fn insert(
        &mut self,
        subtype: P::AdjacencySubtype,
        link: P::Link,
        node_index: u32,
        lookup_next: AdjacencyLookupNext,
    ) -> AdjacencyIndex {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("adjacency allocation requires publication ownership");
        AdjacencyIndex(self.objects.insert(Adjacency {
            node: FibNode::new(self.node_type),
            config_index: u32::MAX,
            subtype,
            rewrite_header: RewriteHeader::poisoned(),
            rewrite_data: [0xfe; 116],
            delegates: u64::MAX,
            node_index,
            lookup_next,
            link,
            next_hop_protocol: P::ID,
            flags: AdjacencyFlags::empty(),
            padding: [0; 48],
        }))
    }

    pub fn get(&self, index: AdjacencyIndex) -> &Adjacency<P> {
        self.objects
            .get(index.0)
            .expect("adjacency index must be live")
    }

    pub fn get_mut(&mut self, index: AdjacencyIndex) -> &mut Adjacency<P> {
        self.objects
            .get_mut(index.0)
            .expect("adjacency index must be live")
    }

    pub fn release(&mut self, index: AdjacencyIndex) -> Adjacency<P> {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("adjacency retirement requires publication ownership");
        let adjacency = self
            .objects
            .remove(index.0)
            .expect("adjacency index must be live");
        adjacency.node.assert_detached();
        adjacency
    }

    pub fn node_lock(&mut self, index: AdjacencyIndex) {
        self.get_mut(index).node.lock();
    }

    pub fn node_unlock(&mut self, index: AdjacencyIndex) -> bool {
        self.get_mut(index).node.unlock()
    }

    pub fn children(&self, index: AdjacencyIndex) -> FibNodeList {
        self.get(index).node.children()
    }

    pub fn set_children(&mut self, index: AdjacencyIndex, children: FibNodeList) {
        self.get_mut(index).node.set_children(children);
    }

    pub fn identity(&self, registry: &DpoMain, index: AdjacencyIndex) -> DpoId {
        let adjacency = self.get(index);
        registry
            .identity(DpoType::ADJACENCY, P::dpo_protocol(adjacency.link), index.0)
            .expect("adjacency DPO class must be registered")
    }

    pub fn dpo(&self, registry: &DpoMain, index: AdjacencyIndex) -> DpoId {
        let adjacency = self.get(index);
        let class = match adjacency.lookup_next {
            AdjacencyLookupNext::Glean => DpoType::ADJACENCY_GLEAN,
            AdjacencyLookupNext::Incomplete | AdjacencyLookupNext::Broadcast => {
                DpoType::ADJACENCY_INCOMPLETE
            }
            AdjacencyLookupNext::Rewrite => DpoType::ADJACENCY,
            AdjacencyLookupNext::Multicast => DpoType::ADJACENCY_MCAST,
        };
        registry
            .identity(class, P::dpo_protocol(adjacency.link), index.0)
            .expect("adjacency subtype DPO class must be registered")
    }

    pub fn mtu(&self, index: AdjacencyIndex) -> u16 {
        self.get(index).rewrite_header.max_l3_packet_bytes
    }

    pub fn urpf(&self, index: AdjacencyIndex) -> u32 {
        self.get(index).rewrite_header.sw_if_index
    }

    pub fn link(&self, index: AdjacencyIndex) -> P::Link {
        self.get(index).link
    }

    pub fn sw_if_index(&self, index: AdjacencyIndex) -> u32 {
        self.get(index).rewrite_header.sw_if_index
    }

    pub fn memory(&self) -> (usize, usize, usize) {
        (
            size_of::<Adjacency<P>>(),
            self.objects.len(),
            self.objects.capacity(),
        )
    }
}

const _: () = assert!(size_of::<usize>() == 8);
const _: () = assert!(align_of::<RewriteHeader>() <= 64);
