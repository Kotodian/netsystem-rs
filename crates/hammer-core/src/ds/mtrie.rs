use std::marker::PhantomData;

use super::prefetch::prefetch_read_l1;

const MTRIE_ROOT_BITS: u8 = 16;
const MTRIE_PLY_BITS: u8 = 8;
const MTRIE_ROOT_LEN: usize = 1 << MTRIE_ROOT_BITS;
const MTRIE_PLY_LEN: usize = 1 << MTRIE_PLY_BITS;
const MTRIE_LEAF_EMPTY: u32 = 0;
const MTRIE_LEAF_TERMINAL: u32 = 1 << 31;
const MTRIE_LEAF_VALUE_MASK: u32 = MTRIE_LEAF_TERMINAL - 1;

pub trait PackedMtrieValue: Copy {
    fn into_leaf_value(self) -> u32;
    fn from_leaf_value(value: u32) -> Self;
}

impl PackedMtrieValue for u32 {
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
pub struct MtrieEntry<V> {
    pub key: u32,
    pub prefix_len: u8,
    pub value: V,
}

impl<V> MtrieEntry<V> {
    #[inline(always)]
    pub fn new(key: u32, prefix_len: u8, value: V) -> Self {
        assert!(prefix_len <= 32, "invalid mtrie prefix length");
        Self {
            key,
            prefix_len,
            value,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Mtrie<V: Copy> {
    raw: RawMtrie,
    values: Vec<V>,
}

impl<V: Copy> Mtrie<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            raw: RawMtrie::empty(),
            values: Vec::new(),
        }
    }

    #[inline]
    pub fn from_entries(entries: impl IntoIterator<Item = MtrieEntry<V>>) -> Self {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.prefix_len);
        let mut trie = Self::empty();
        for entry in entries {
            trie.insert(entry.key, entry.prefix_len, entry.value);
        }
        trie
    }

    #[inline]
    pub fn insert(&mut self, key: u32, prefix_len: u8, value: V) {
        let index = self.values.len();
        assert!(
            index <= MTRIE_LEAF_VALUE_MASK as usize,
            "mtrie value table exceeds leaf capacity"
        );
        self.values.push(value);
        self.raw.insert(key, prefix_len, index as u32);
    }

    #[inline(always)]
    pub fn lookup(&self, key: u32) -> Option<V> {
        let index = self.raw.lookup(key)? as usize;
        self.values.get(index).copied()
    }

    #[inline(always)]
    pub fn prefetch(&self, key: u32) {
        self.raw.prefetch(key);
    }

    #[inline(always)]
    pub fn ply_count(&self) -> usize {
        self.raw.ply_count()
    }

    #[inline(always)]
    pub fn root_alignment(&self) -> usize {
        self.raw.root_alignment()
    }

    #[inline(always)]
    pub fn root_addr(&self) -> usize {
        self.raw.root_addr()
    }

    #[inline(always)]
    pub fn ply_addr(&self, index: usize) -> Option<usize> {
        self.raw.ply_addr(index)
    }
}

#[derive(Debug, Clone)]
pub struct PackedMtrie<V: PackedMtrieValue> {
    raw: RawMtrie,
    _value: PhantomData<V>,
}

impl<V: PackedMtrieValue> PackedMtrie<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            raw: RawMtrie::empty(),
            _value: PhantomData,
        }
    }

    #[inline]
    pub fn from_entries(entries: impl IntoIterator<Item = MtrieEntry<V>>) -> Self {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.prefix_len);
        let mut trie = Self::empty();
        for entry in entries {
            trie.insert(entry.key, entry.prefix_len, entry.value);
        }
        trie
    }

    #[inline]
    pub fn insert(&mut self, key: u32, prefix_len: u8, value: V) {
        self.raw.insert(key, prefix_len, value.into_leaf_value());
    }

    #[inline(always)]
    pub fn lookup(&self, key: u32) -> Option<V> {
        self.raw.lookup(key).map(V::from_leaf_value)
    }

    #[inline(always)]
    pub fn prefetch(&self, key: u32) {
        self.raw.prefetch(key);
    }

    #[inline(always)]
    pub fn ply_count(&self) -> usize {
        self.raw.ply_count()
    }

    #[inline(always)]
    pub fn root_alignment(&self) -> usize {
        self.raw.root_alignment()
    }

    #[inline(always)]
    pub fn root_addr(&self) -> usize {
        self.raw.root_addr()
    }

    #[inline(always)]
    pub fn ply_addr(&self, index: usize) -> Option<usize> {
        self.raw.ply_addr(index)
    }
}

#[derive(Debug, Clone)]
struct RawMtrie {
    root: Box<MtrieRoot>,
    plies: Vec<MtriePly>,
}

impl RawMtrie {
    #[inline]
    fn empty() -> Self {
        Self {
            root: Box::new(MtrieRoot::filled(MtrieLeaf::empty())),
            plies: Vec::new(),
        }
    }

    #[inline]
    fn insert(&mut self, key: u32, prefix_len: u8, value: u32) {
        assert!(prefix_len <= 32, "invalid mtrie prefix length");
        let terminal = MtrieLeaf::terminal(value);
        let root_value = root_stride(key);
        if prefix_len <= MTRIE_ROOT_BITS {
            fill_stride(
                &mut self.root.leaves,
                root_value,
                prefix_len,
                MTRIE_ROOT_BITS,
                terminal,
            );
            return;
        }

        let first_ply = self.ensure_root_ply(root_value);
        if prefix_len <= MTRIE_ROOT_BITS + MTRIE_PLY_BITS {
            fill_stride(
                &mut self.plies[first_ply].leaves,
                first_child_stride(key),
                prefix_len - MTRIE_ROOT_BITS,
                MTRIE_PLY_BITS,
                terminal,
            );
            return;
        }

        let second_ply = self.ensure_child_ply(first_ply, first_child_stride(key));
        fill_stride(
            &mut self.plies[second_ply].leaves,
            second_child_stride(key),
            prefix_len - MTRIE_ROOT_BITS - MTRIE_PLY_BITS,
            MTRIE_PLY_BITS,
            terminal,
        );
    }

    #[inline(always)]
    fn lookup(&self, key: u32) -> Option<u32> {
        let mut leaf = self.root.leaves[root_stride(key)];
        if let Some(value) = leaf.value() {
            return Some(value);
        }
        let first_ply = leaf.ply_index()?;
        leaf = self.plies.get(first_ply)?.leaves[first_child_stride(key)];
        if let Some(value) = leaf.value() {
            return Some(value);
        }
        let second_ply = leaf.ply_index()?;
        leaf = self.plies.get(second_ply)?.leaves[second_child_stride(key)];
        leaf.value()
    }

    #[inline(always)]
    fn prefetch(&self, key: u32) {
        let root_leaf = &self.root.leaves[root_stride(key)];
        prefetch_read_l1(root_leaf);
        if let Some(first_ply) = root_leaf.ply_index()
            && let Some(ply) = self.plies.get(first_ply)
        {
            let first_leaf = &ply.leaves[first_child_stride(key)];
            prefetch_read_l1(first_leaf);
            if let Some(second_ply) = first_leaf.ply_index()
                && let Some(ply) = self.plies.get(second_ply)
            {
                prefetch_read_l1(&ply.leaves[second_child_stride(key)]);
            }
        }
    }

    #[inline(always)]
    fn ply_count(&self) -> usize {
        self.plies.len()
    }

    #[inline(always)]
    fn root_alignment(&self) -> usize {
        std::mem::align_of_val(self.root.as_ref())
    }

    #[inline(always)]
    fn root_addr(&self) -> usize {
        std::ptr::from_ref(self.root.as_ref()).addr()
    }

    #[inline(always)]
    fn ply_addr(&self, index: usize) -> Option<usize> {
        self.plies
            .get(index)
            .map(|ply| std::ptr::from_ref(ply).addr())
    }

    #[inline]
    fn ensure_root_ply(&mut self, root_value: usize) -> usize {
        let leaf = self.root.leaves[root_value];
        if let Some(index) = leaf.ply_index() {
            return index;
        }
        let index = self.alloc_ply(leaf);
        self.root.leaves[root_value] = MtrieLeaf::ply(index);
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
        self.plies.push(MtriePly::filled(inherited));
        index
    }
}

#[derive(Debug, Clone)]
#[repr(C, align(64))]
struct MtrieRoot {
    leaves: [MtrieLeaf; MTRIE_ROOT_LEN],
}

impl MtrieRoot {
    #[inline]
    fn filled(leaf: MtrieLeaf) -> Self {
        Self {
            leaves: [leaf; MTRIE_ROOT_LEN],
        }
    }
}

#[derive(Debug, Clone)]
#[repr(C, align(64))]
struct MtriePly {
    leaves: [MtrieLeaf; MTRIE_PLY_LEN],
}

impl MtriePly {
    #[inline]
    fn filled(leaf: MtrieLeaf) -> Self {
        Self {
            leaves: [leaf; MTRIE_PLY_LEN],
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
            "mtrie terminal value exceeds leaf capacity"
        );
        Self(MTRIE_LEAF_TERMINAL | value)
    }

    #[inline(always)]
    fn ply(index: usize) -> Self {
        assert!(
            index < MTRIE_LEAF_VALUE_MASK as usize,
            "mtrie ply index exceeds leaf capacity"
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
fn root_stride(key: u32) -> usize {
    (key >> 16) as usize
}

#[inline(always)]
fn first_child_stride(key: u32) -> usize {
    ((key >> 8) & 0xff) as usize
}

#[inline(always)]
fn second_child_stride(key: u32) -> usize {
    (key & 0xff) as usize
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
