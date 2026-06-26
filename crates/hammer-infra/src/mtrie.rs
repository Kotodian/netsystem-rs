//! IPv4 multi-way trie (mtrie) for longest-prefix match, aligned with VPP's
//! `ip4_mtrie_16` (16-8-8 stride).
//!
//! # Layout (mirrors `src/vnet/ip/ip4_mtrie.h`)
//!
//! - `Mtrie16Ply`: the root ply, 65536 slots indexed by the top 16 address
//!   bits, each slot carrying a `MtrieLeaf` and a per-slot prefix length. The
//!   root is never pooled or reclaimed, so it omits the non-empty count.
//! - `Mtrie8Ply`: a child ply, 256 slots indexed by one address byte, each
//!   slot carrying a `MtrieLeaf` and a per-slot prefix length, plus
//!   `base_len` (the covering prefix length — a slot is "non-empty" when its
//!   `prefix_len > base_len`) and `n_non_empty` (count of non-empty slots,
//!   used to reclaim plies that drain to zero).
//!
//! # Leaf encoding
//!
//! `MtrieLeaf(u32)` uses the low bit as a terminal/non-terminal tag, exactly
//! like VPP: `leaf & 1 == 1` ⇒ terminal, `adj_index = leaf >> 1`;
//! `leaf & 1 == 0` ⇒ non-terminal, `ply_index = leaf >> 1`; `1` ⇒ empty
//! (`1 + 2 * 0`, i.e. terminal with adj_index 0 reserved as the miss value).
//!
//! # Inheritance
//!
//! A slot whose `prefix_len <= base_len` holds the covering prefix's leaf
//! (inherited from the parent); a slot whose `prefix_len > base_len` holds a
//! more specific route's leaf. `route_add` only writes the more-specific
//! slots, so a lookup reading `ply.leaves[byte]` always gets a usable leaf —
//! no runtime `prefix_len` comparison on the hot path, matching VPP.

use std::marker::PhantomData;

use crate::prefetch::prefetch_read_l1;
use crate::vec::Vec;

const MTRIE_16_BITS: u8 = 16;
const MTRIE_8_BITS: u8 = 8;
const MTRIE_16_LEN: usize = 1 << MTRIE_16_BITS;
const MTRIE_8_LEN: usize = 1 << MTRIE_8_BITS;

/// `MtrieLeaf` is the only place a value's `0` is interpreted: VPP reserves
/// adj_index 0 as a miss adjacency, but Hammer treats value `0` as a real
/// DPO, so `MtrieLeaf::MISS = 0` is the explicit miss sentinel instead.
/// See `MtrieLeaf` for the full encoding.
const _: () = ();

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
pub struct PackedMtrie<V: PackedMtrieValue> {
    mtrie: Mtrie,
    _value: PhantomData<V>,
}

impl<V: PackedMtrieValue> PackedMtrie<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            mtrie: Mtrie::empty(),
            _value: PhantomData,
        }
    }

    #[inline]
    pub fn from_entries(entries: impl IntoIterator<Item = MtrieEntry<V>>) -> Self {
        // Insert shortest prefixes first so more-specific routes overwrite
        // the covering slots and drive ply allocation as they descend.
        let mut entries = entries.into_iter().collect::<std::vec::Vec<_>>();
        entries.sort_by_key(|entry| entry.prefix_len);
        let mut trie = Self::empty();
        for entry in entries {
            trie.insert(entry.key, entry.prefix_len, entry.value);
        }
        trie
    }

    #[inline]
    pub fn insert(&mut self, key: u32, prefix_len: u8, value: V) {
        self.mtrie.insert(key, prefix_len, value.into_leaf_value());
    }

    #[inline(always)]
    pub fn lookup(&self, key: u32) -> Option<V> {
        self.mtrie.lookup(key).map(V::from_leaf_value)
    }

    #[inline(always)]
    pub fn prefetch(&self, key: u32) {
        self.mtrie.prefetch(key);
    }

    #[inline(always)]
    pub fn ply_count(&self) -> usize {
        self.mtrie.ply_count()
    }

    #[inline(always)]
    pub fn root_alignment(&self) -> usize {
        self.mtrie.root_alignment()
    }

    #[inline(always)]
    pub fn root_addr(&self) -> usize {
        self.mtrie.root_addr()
    }

    #[inline(always)]
    pub fn ply_addr(&self, index: usize) -> Option<usize> {
        self.mtrie.ply_addr(index)
    }
}

/// 16-8-8 multi-way trie. Root is an embedded 16-bit ply; 8-bit child plies
/// live in a pool (`plies`), referenced by `MtrieLeaf` non-terminal values.
#[derive(Debug, Clone)]
struct Mtrie {
    root: Box<Mtrie16Ply>,
    plies: Vec<Mtrie8Ply>,
}

impl Mtrie {
    #[inline]
    fn empty() -> Self {
        Self {
            root: Box::new(Mtrie16Ply::filled(MtrieLeaf::MISS, 0)),
            plies: Vec::new(),
        }
    }

    fn insert(&mut self, key: u32, prefix_len: u8, value: u32) {
        assert!(prefix_len <= 32, "invalid mtrie prefix length");
        let terminal = MtrieLeaf::terminal(value);

        if prefix_len <= MTRIE_16_BITS {
            self.root
                .fill_stride(root_stride(key), prefix_len, MTRIE_16_BITS, terminal);
            return;
        }

        // First 8-bit ply: covers bits 16..24.
        let first_ply = self.ensure_root_ply(root_stride(key), MTRIE_16_BITS);
        if prefix_len <= MTRIE_16_BITS + MTRIE_8_BITS {
            self.plies[first_ply].fill_stride(
                first_stride(key),
                prefix_len - MTRIE_16_BITS,
                MTRIE_8_BITS,
                terminal,
            );
            return;
        }

        // Second 8-bit ply: covers bits 24..32.
        let second_ply =
            self.ensure_child_ply(first_ply, first_stride(key), MTRIE_16_BITS + MTRIE_8_BITS);
        self.plies[second_ply].fill_stride(
            second_stride(key),
            prefix_len - MTRIE_16_BITS - MTRIE_8_BITS,
            MTRIE_8_BITS,
            terminal,
        );
    }

    /// VPP `ip4_mtrie_16_lookup_step` x3: one 16-bit root step + up to two
    /// 8-bit child steps. Each step is a single array index plus a low-bit
    /// terminal check; no enum match, no binary search, no prefix-length
    /// comparison on the hot path.
    #[inline(always)]
    fn lookup(&self, key: u32) -> Option<u32> {
        let mut leaf = self.root.leaves[root_stride(key)];
        if !leaf.is_terminal() {
            let ply = leaf.ply_index().and_then(|idx| self.plies.get(idx))?;
            leaf = ply.leaves[first_stride(key)];
            if !leaf.is_terminal() {
                let ply = leaf.ply_index().and_then(|idx| self.plies.get(idx))?;
                leaf = ply.leaves[second_stride(key)];
            }
        }
        leaf.adj_index()
    }

    #[inline(always)]
    fn prefetch(&self, key: u32) {
        let root_leaf = &self.root.leaves[root_stride(key)];
        prefetch_read_l1(root_leaf as *const _);
        if let Some(ply) = root_leaf.ply_index().and_then(|idx| self.plies.get(idx)) {
            prefetch_read_l1(ply as *const _);
            let first_leaf = &ply.leaves[first_stride(key)];
            prefetch_read_l1(first_leaf as *const _);
            if let Some(ply) = first_leaf.ply_index().and_then(|idx| self.plies.get(idx)) {
                prefetch_read_l1(ply as *const _);
                prefetch_read_l1(&ply.leaves[second_stride(key)] as *const _);
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

    /// Ensure a child 8-ply hangs off root slot `root_slot`, allocated with
    /// `cover_len` as its `base_len` and the root slot's current leaf as the
    /// inherited cover for every slot.
    #[inline]
    fn ensure_root_ply(&mut self, root_slot: usize, cover_len: u8) -> usize {
        let leaf = self.root.leaves[root_slot];
        if let Some(idx) = leaf.ply_index() {
            return idx;
        }
        let cover = self.root.prefix_lens[root_slot].max(cover_len);
        let idx = self.alloc_ply(leaf, cover);
        self.root.leaves[root_slot] = MtrieLeaf::non_terminal(idx);
        self.root.prefix_lens[root_slot] = cover_len;
        idx
    }

    /// Ensure a child 8-ply hangs off `parent_ply` slot `slot`, allocated
    /// with `cover_len` as its `base_len`.
    #[inline]
    fn ensure_child_ply(&mut self, parent_ply: usize, slot: usize, cover_len: u8) -> usize {
        let leaf = self.plies[parent_ply].leaves[slot];
        if let Some(idx) = leaf.ply_index() {
            return idx;
        }
        let cover = self.plies[parent_ply].prefix_lens[slot].max(cover_len);
        let idx = self.alloc_ply(leaf, cover);
        self.plies[parent_ply].leaves[slot] = MtrieLeaf::non_terminal(idx);
        self.plies[parent_ply].prefix_lens[slot] = cover_len;
        self.plies[parent_ply].n_non_empty += 1;
        idx
    }

    #[inline]
    fn alloc_ply(&mut self, inherited: MtrieLeaf, base_len: u8) -> usize {
        let idx = self.plies.len();
        self.plies.push(Mtrie8Ply::filled(inherited, base_len));
        idx
    }
}

/// The 16-bit root ply. 65536 slots, each with a leaf and the prefix length
/// of the route that last wrote it. Never pooled or reclaimed, so it carries
/// no non-empty count.
#[derive(Debug, Clone)]
#[repr(C, align(64))]
struct Mtrie16Ply {
    leaves: [MtrieLeaf; MTRIE_16_LEN],
    prefix_lens: [u8; MTRIE_16_LEN],
}

impl Mtrie16Ply {
    #[inline]
    fn filled(leaf: MtrieLeaf, prefix_len: u8) -> Self {
        Self {
            leaves: [leaf; MTRIE_16_LEN],
            prefix_lens: [prefix_len; MTRIE_16_LEN],
        }
    }

    /// Write `leaf` across the stride-aligned span covering `base`, recording
    /// `prefix_len` on every slot touched. Stride is 16 bits, so a /0 fills
    /// all 65536 slots, a /8 fills 256, a /16 fills 1.
    #[inline]
    fn fill_stride(&mut self, base: usize, prefix_bits: u8, stride_bits: u8, leaf: MtrieLeaf) {
        let span = 1usize << (stride_bits - prefix_bits);
        let start = base & !(span - 1);
        let end = start + span;
        for slot in start..end {
            self.leaves[slot] = leaf;
            self.prefix_lens[slot] = prefix_bits;
        }
    }
}

/// An 8-bit child ply. 256 slots, each with a leaf and prefix length, plus
/// `base_len` (the covering prefix length — a slot is non-empty when its
/// `prefix_len > base_len`) and `n_non_empty` (count of non-empty slots).
#[derive(Debug, Clone)]
#[repr(C, align(64))]
struct Mtrie8Ply {
    leaves: [MtrieLeaf; MTRIE_8_LEN],
    prefix_lens: [u8; MTRIE_8_LEN],
    n_non_empty: i32,
    base_len: u8,
}

impl Mtrie8Ply {
    #[inline]
    fn filled(inherited: MtrieLeaf, base_len: u8) -> Self {
        Self {
            leaves: [inherited; MTRIE_8_LEN],
            prefix_lens: [base_len; MTRIE_8_LEN],
            n_non_empty: 0,
            base_len,
        }
    }

    /// Write `leaf` across the stride-aligned span covering `base`, recording
    /// `prefix_len` on every slot touched. A slot transitions from empty to
    /// non-empty (and bumps `n_non_empty`) when its previous `prefix_len` was
    /// `<= base_len` and the new `prefix_len` is `> base_len`.
    #[inline]
    fn fill_stride(&mut self, base: usize, prefix_bits: u8, stride_bits: u8, leaf: MtrieLeaf) {
        let span = 1usize << (stride_bits - prefix_bits);
        let start = base & !(span - 1);
        let end = start + span;
        for slot in start..end {
            let was_empty = self.prefix_lens[slot] <= self.base_len;
            let now_non_empty = prefix_bits > self.base_len;
            self.leaves[slot] = leaf;
            self.prefix_lens[slot] = prefix_bits;
            match (was_empty, now_non_empty) {
                (true, true) => self.n_non_empty += 1,
                (false, false) => self.n_non_empty -= 1,
                _ => {}
            }
        }
    }
}

/// Low-bit-tagged leaf, matching VPP's `ip4_mtrie_leaf_t` encoding — with one
/// divergence: VPP reserves adj_index 0 as the "miss" adjacency, so its
/// `IP4_MTRIE_LEAF_EMPTY == 1`. Hammer lets value `0` be a real DPO (e.g. a
/// `Drop` adjacency), so `0` is used as the explicit miss sentinel instead.
///
/// Encoding:
/// - `0` ⇒ miss (no route). `is_terminal()` is false and both `adj_index()`
///   and `ply_index()` return `None`.
/// - bit0 set ⇒ terminal, `adj_index = leaf >> 1` (adj 0 is legal → leaf `1`).
/// - bit0 clear and `!= 0` ⇒ non-terminal, `ply_index = (leaf >> 1) - 1`
///   (ply 0 is legal → leaf `2`; the `-1` keeps `0` exclusive to miss).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct MtrieLeaf(u32);

impl MtrieLeaf {
    /// `0` — no route installed in this slot.
    const MISS: Self = Self(0);

    /// Terminal leaf: low bit set, `adj_index = leaf >> 1`.
    #[inline(always)]
    fn terminal(adj_index: u32) -> Self {
        Self((adj_index << 1) | 1)
    }

    /// Non-terminal leaf: low bit clear, `ply_index = (leaf >> 1) - 1`.
    #[inline(always)]
    fn non_terminal(ply_index: usize) -> Self {
        Self((ply_index as u32 + 1) << 1)
    }

    #[inline(always)]
    fn is_terminal(self) -> bool {
        self.0 & 1 == 1
    }

    /// Return the encoded adj_index for a terminal leaf, or `None` for
    /// non-terminal / miss.
    #[inline(always)]
    fn adj_index(self) -> Option<u32> {
        (self.0 & 1 == 1).then_some(self.0 >> 1)
    }

    /// Return the encoded ply index for a non-terminal leaf, or `None` for
    /// terminal / miss.
    #[inline(always)]
    fn ply_index(self) -> Option<usize> {
        (self.0 != 0 && self.0 & 1 == 0).then(|| (self.0 >> 1) as usize - 1)
    }
}

#[inline(always)]
fn root_stride(key: u32) -> usize {
    (key >> 16) as usize
}

#[inline(always)]
fn first_stride(key: u32) -> usize {
    ((key >> 8) & 0xff) as usize
}

#[inline(always)]
fn second_stride(key: u32) -> usize {
    (key & 0xff) as usize
}
