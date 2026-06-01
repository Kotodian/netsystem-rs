use std::marker::PhantomData;
use std::net::Ipv4Addr;

use ipnet::Ipv4Net;

use super::prefetch::prefetch_read_l1;

const IP4_MTRIE_ROOT_BITS: u8 = 16;
const IP4_MTRIE_PLY_BITS: u8 = 8;
const IP4_MTRIE_ROOT_LEN: usize = 1 << IP4_MTRIE_ROOT_BITS;
const IP4_MTRIE_PLY_LEN: usize = 1 << IP4_MTRIE_PLY_BITS;
const MTRIE_LEAF_EMPTY: u32 = 0;
const MTRIE_LEAF_TERMINAL: u32 = 1 << 31;
const MTRIE_LEAF_VALUE_MASK: u32 = MTRIE_LEAF_TERMINAL - 1;

pub trait Ip4MtrieValue: Copy {
    fn into_leaf_value(self) -> u32;
    fn from_leaf_value(value: u32) -> Self;
}

impl Ip4MtrieValue for u32 {
    #[inline(always)]
    fn into_leaf_value(self) -> u32 {
        self
    }

    #[inline(always)]
    fn from_leaf_value(value: u32) -> Self {
        value
    }
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
pub struct Ip4Mtrie<V: Ip4MtrieValue> {
    root: Box<[MtrieLeaf]>,
    plies: Vec<Ip4MtriePly>,
    _value: PhantomData<V>,
}

impl<V: Ip4MtrieValue> Ip4Mtrie<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            root: vec![MtrieLeaf::empty(); IP4_MTRIE_ROOT_LEN].into_boxed_slice(),
            plies: Vec::new(),
            _value: PhantomData,
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
        let terminal = MtrieLeaf::terminal(value.into_leaf_value());
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
        if let Some(value) = leaf.value() {
            return Some(V::from_leaf_value(value));
        }
        let first_ply = leaf.ply_index()?;
        leaf = self.plies.get(first_ply)?.leaves[octets[2] as usize];
        if let Some(value) = leaf.value() {
            return Some(V::from_leaf_value(value));
        }
        let second_ply = leaf.ply_index()?;
        leaf = self.plies.get(second_ply)?.leaves[octets[3] as usize];
        leaf.value().map(V::from_leaf_value)
    }

    #[inline(always)]
    pub fn prefetch(&self, destination: Ipv4Addr) {
        let octets = destination.octets();
        let root_value = u16::from_be_bytes([octets[0], octets[1]]) as usize;
        let root_leaf = &self.root[root_value];
        prefetch_read_l1(root_leaf);
        if let Some(first_ply) = root_leaf.ply_index()
            && let Some(ply) = self.plies.get(first_ply)
        {
            let first_leaf = &ply.leaves[octets[2] as usize];
            prefetch_read_l1(first_leaf);
            if let Some(second_ply) = first_leaf.ply_index()
                && let Some(ply) = self.plies.get(second_ply)
            {
                prefetch_read_l1(&ply.leaves[octets[3] as usize]);
            }
        }
    }

    #[inline(always)]
    pub fn ply_count(&self) -> usize {
        self.plies.len()
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
#[repr(C, align(64))]
struct Ip4MtriePly {
    leaves: [MtrieLeaf; IP4_MTRIE_PLY_LEN],
}

impl Ip4MtriePly {
    #[inline]
    fn filled(leaf: MtrieLeaf) -> Self {
        Self {
            leaves: [leaf; IP4_MTRIE_PLY_LEN],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct MtrieLeaf(u32);

impl MtrieLeaf {
    #[inline(always)]
    const fn empty() -> Self {
        Self(MTRIE_LEAF_EMPTY)
    }

    #[inline(always)]
    fn terminal(value: u32) -> Self {
        assert!(
            value <= MTRIE_LEAF_VALUE_MASK,
            "IPv4 mtrie terminal value exceeds leaf capacity"
        );
        Self(MTRIE_LEAF_TERMINAL | value)
    }

    #[inline(always)]
    fn ply(index: usize) -> Self {
        assert!(
            index < MTRIE_LEAF_VALUE_MASK as usize,
            "IPv4 mtrie ply index exceeds leaf capacity"
        );
        Self((index as u32) + 1)
    }

    #[inline(always)]
    fn value(self) -> Option<u32> {
        ((self.0 & MTRIE_LEAF_TERMINAL) != 0).then_some(self.0 & MTRIE_LEAF_VALUE_MASK)
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
