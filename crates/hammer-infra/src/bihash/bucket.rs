//! VPP-style bihash bucket: a 64-bit packed word with bitfield accessors.
//!
//! Bit layout (LSB → MSB):
//!
//! ```text
//! | offset (36) | lock (1) | linear_search (1) | log2_pages (8) | refcnt (13) | generation (5) |
//! ```
//!
//! The offset field occupies bits 28–63 and indexes into the page arena.
//! All remaining fields fit in the lower 28 bits so that extracting
//! `offset` is a single shift, and extracting the low fields is a single
//! `and`/`shr` sequence.

use core::sync::atomic::{AtomicU64, Ordering};

// ── bit-field constants ────────────────────────────────────────────────

const GEN_BITS: u32 = 5;
const REFCNT_BITS: u32 = 13;
const LP_BITS: u32 = 8;
const LIN_BITS: u32 = 1;
const LOCK_BITS: u32 = 1;
const OFFSET_BITS: u32 = 36;

const GEN_MASK: u64 = (1u64 << GEN_BITS) - 1;
const REFCNT_MASK: u64 = (1u64 << REFCNT_BITS) - 1;
const LP_MASK: u64 = (1u64 << LP_BITS) - 1;
const LIN_MASK: u64 = (1u64 << LIN_BITS) - 1;
const LOCK_MASK: u64 = (1u64 << LOCK_BITS) - 1;
const OFFSET_MASK: u64 = (1u64 << OFFSET_BITS) - 1;

const GEN_SHIFT: u32 = 0;
const REFCNT_SHIFT: u32 = GEN_SHIFT + GEN_BITS; // 5
const LP_SHIFT: u32 = REFCNT_SHIFT + REFCNT_BITS; // 18
const LIN_SHIFT: u32 = LP_SHIFT + LP_BITS; // 26
const LOCK_SHIFT: u32 = LIN_SHIFT + LIN_BITS; // 27
const OFFSET_SHIFT: u32 = LOCK_SHIFT + LOCK_BITS; // 28

// Total bits: 36 + 1 + 1 + 8 + 13 + 5 = 64.

// ── Bucket ─────────────────────────────────────────────────────────────

/// A 64-bit packed bihash bucket word.
///
/// Every field is read-only through const accessors. Mutation is performed
/// through CAS on an `AtomicU64` wrapper (see `AtomicBucket`).
#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct Bucket(u64);

impl Bucket {
    /// Returns the zero-valued bucket — the empty sentinel.
    #[inline(always)]
    pub const fn empty() -> Self {
        Bucket(0)
    }

    /// Wraps a raw `u64` into a bucket (no validation).
    #[inline(always)]
    pub const fn from_raw(raw: u64) -> Self {
        Bucket(raw)
    }

    /// Returns the underlying `u64` representation.
    #[inline(always)]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns `true` when the bucket is empty: offset is zero AND
    /// either `log2_pages` is zero or `refcnt` is zero.
    #[inline(always)]
    pub const fn is_empty(self) -> bool {
        self.offset() == 0 && (self.log2_pages() == 0 || self.refcnt() == 0)
    }

    /// Page-arena offset (36 bits).
    #[inline(always)]
    pub const fn offset(self) -> u64 {
        (self.0 >> OFFSET_SHIFT) & OFFSET_MASK
    }

    /// Lock flag.
    #[inline(always)]
    pub const fn is_locked(self) -> bool {
        ((self.0 >> LOCK_SHIFT) & LOCK_MASK) != 0
    }

    /// Linear-search flag (bucket is in linear-scan mode, not hash lookup).
    #[inline(always)]
    pub const fn is_linear_search(self) -> bool {
        ((self.0 >> LIN_SHIFT) & LIN_MASK) != 0
    }

    /// Log2 of the number of pages backing this bucket (8 bits).
    #[inline(always)]
    pub const fn log2_pages(self) -> u8 {
        ((self.0 >> LP_SHIFT) & LP_MASK) as u8
    }

    /// Reference count (13 bits, max 8191).
    #[inline(always)]
    pub const fn refcnt(self) -> u16 {
        ((self.0 >> REFCNT_SHIFT) & REFCNT_MASK) as u16
    }

    /// Generation counter (5 bits, wraps at 32).
    #[inline(always)]
    pub const fn generation(self) -> u8 {
        ((self.0 >> GEN_SHIFT) & GEN_MASK) as u8
    }

    /// Pack all fields into a single `Bucket`.
    #[inline(always)]
    pub const fn pack(
        offset: u64,
        log2_pages: u8,
        refcnt: u16,
        generation: u8,
        linear_search: bool,
        lock: bool,
    ) -> Self {
        let raw = (offset & OFFSET_MASK) << OFFSET_SHIFT
            | ((lock as u64) & LOCK_MASK) << LOCK_SHIFT
            | ((linear_search as u64) & LIN_MASK) << LIN_SHIFT
            | ((log2_pages as u64) & LP_MASK) << LP_SHIFT
            | ((refcnt as u64) & REFCNT_MASK) << REFCNT_SHIFT
            | ((generation as u64) & GEN_MASK) << GEN_SHIFT;
        Bucket(raw)
    }

    /// Increment the generation counter, wrapping at 32.
    #[inline(always)]
    pub const fn bump_generation(self) -> Self {
        let next = (self.generation() + 1) & 0x1F; // mod 32
        Bucket((self.0 & !(GEN_MASK << GEN_SHIFT)) | ((next as u64) << GEN_SHIFT))
    }

    /// Construct a linear-search sentinel bucket.
    ///
    /// Sets `lock = true`, `linear_search = true`, `log2_pages = 0`,
    /// and the given `refcnt`. Offset, generation are zeroed.
    #[inline(always)]
    pub const fn make_linear_search(refcnt: u16) -> Self {
        Bucket::pack(0, 0, refcnt, 0, true, true)
    }
}

// ── Debug ──────────────────────────────────────────────────────────────

impl core::fmt::Debug for Bucket {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Bucket")
            .field("offset", &self.offset())
            .field("lock", &self.is_locked())
            .field("linear_search", &self.is_linear_search())
            .field("log2_pages", &self.log2_pages())
            .field("refcnt", &self.refcnt())
            .field("generation", &self.generation())
            .finish()
    }
}

// ── AtomicBucket ───────────────────────────────────────────────────────

/// An atomic wrapper around `Bucket` backed by `AtomicU64`.
///
/// This is the type used in the shared bucket array so that readers and
/// writers can synchronise lock-free via CAS.
#[derive(Default)]
#[repr(transparent)]
pub struct AtomicBucket(AtomicU64);

impl AtomicBucket {
    /// Creates a new atomic bucket from a `Bucket`.
    #[inline(always)]
    pub const fn new(b: Bucket) -> Self {
        AtomicBucket(AtomicU64::new(b.0))
    }

    /// Loads a snapshot of the bucket.
    #[inline(always)]
    pub fn load(&self, order: Ordering) -> Bucket {
        Bucket(self.0.load(order))
    }

    /// Stores a bucket value.
    #[inline(always)]
    pub fn store(&self, b: Bucket, order: Ordering) {
        self.0.store(b.0, order);
    }

    /// Compares and swaps — returns `(previous, succeeded)`.
    #[inline(always)]
    pub fn compare_exchange(
        &self,
        current: Bucket,
        new: Bucket,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Bucket, Bucket> {
        self.0
            .compare_exchange(current.0, new.0, success, failure)
            .map(Bucket)
            .map_err(Bucket)
    }

    /// Fetches a `Bucket` value and replaces it with another.
    #[inline(always)]
    pub fn swap(&self, b: Bucket, order: Ordering) -> Bucket {
        Bucket(self.0.swap(b.0, order))
    }
}
