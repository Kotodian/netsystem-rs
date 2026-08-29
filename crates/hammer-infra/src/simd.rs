#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Simd<T, const LANES: usize>([T; LANES]);

impl<T, const LANES: usize> Simd<T, LANES> {
    #[inline(always)]
    pub const fn from_array(values: [T; LANES]) -> Self {
        Self(values)
    }

    #[inline(always)]
    pub fn to_array(self) -> [T; LANES] {
        self.0
    }

    #[inline]
    pub fn is_supported() -> bool {
        if LANES == 0 {
            return false;
        }
        let vector_bytes = core::mem::size_of::<Self>();
        if vector_bytes <= 1 {
            return true;
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            return match vector_bytes {
                16 => std::is_x86_feature_detected!("sse2"),
                32 => std::is_x86_feature_detected!("avx2"),
                64 => {
                    std::is_x86_feature_detected!("avx512f")
                        && std::is_x86_feature_detected!("avx512bw")
                }
                _ => false,
            };
        }
        #[cfg(target_arch = "aarch64")]
        {
            return vector_bytes == 16;
        }
        #[cfg(target_arch = "arm")]
        {
            return vector_bytes == 16 && cfg!(target_feature = "neon");
        }
        #[allow(unreachable_code)]
        false
    }
}

impl<T: Copy, const LANES: usize> Simd<T, LANES> {
    #[inline(always)]
    pub const fn splat(value: T) -> Self {
        Self([value; LANES])
    }
}

impl<const LANES: usize> Simd<u16, LANES> {
    #[inline(always)]
    pub fn swap_bytes(self) -> Self {
        Self(self.0.map(u16::swap_bytes))
    }
}

impl<const LANES: usize> From<Simd<u16, LANES>> for Simd<u32, LANES> {
    #[inline(always)]
    fn from(value: Simd<u16, LANES>) -> Self {
        Self(value.0.map(u32::from))
    }
}

impl<const LANES: usize> Simd<u32, LANES> {
    #[inline(always)]
    pub fn reduce_sum(self) -> u32 {
        self.0.into_iter().sum()
    }
}

// ── movemask_4 : 4 bools → 4-bit mask ──────────────────────────

/// Pack 4 booleans into a 4-bit mask (bit 0 = kept[0]).
///
/// For 4-wide, scalar bit-pack is optimal on all architectures.
/// Each arch gets the same implementation so callers can rely on
/// the function unconditionally while arch-detection code remains
/// in-tree for future SIMD acceleration.
#[cfg(target_arch = "x86_64")]
#[inline]
pub fn movemask_4(kept: [bool; 4]) -> u8 {
    (kept[0] as u8) | ((kept[1] as u8) << 1) | ((kept[2] as u8) << 2) | ((kept[3] as u8) << 3)
}

/// Pack 4 booleans into a 4-bit mask (bit 0 = kept[0]).
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn movemask_4(kept: [bool; 4]) -> u8 {
    (kept[0] as u8) | ((kept[1] as u8) << 1) | ((kept[2] as u8) << 2) | ((kept[3] as u8) << 3)
}

/// Pack 4 booleans into a 4-bit mask (bit 0 = kept[0]).
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
pub fn movemask_4(kept: [bool; 4]) -> u8 {
    (kept[0] as u8) | ((kept[1] as u8) << 1) | ((kept[2] as u8) << 2) | ((kept[3] as u8) << 3)
}

// ── compact_indices : 4 indices in → up to 4 kept out ──────────

/// Compact up to 4 `u32` indices based on a 4-bit keep mask.
/// Reads `indices[offset..offset+4]`, writes those with keep_mask bits
/// set to the run starting at `*write`, advances `*write`.
///
/// Scalar bit-scan loop (4 iterations max, compiler unrolls).
/// For 4-wide compaction, scalar is optimal on all architectures.
#[inline]
pub fn compact_indices(indices: &mut [u32], keep_mask: u8, offset: usize, write: &mut usize) {
    assert!(keep_mask < 16, "keep_mask must be 4-bit");

    let mut mask = keep_mask;
    while mask != 0 {
        let lsb = mask.trailing_zeros();
        let src = offset + lsb as usize;
        indices[*write] = indices[src];
        *write += 1;
        mask &= mask - 1;
    }
}

// ── copy_bytes_simd : SIMD-accelerated byte copy ─────────────────

/// Copy bytes from `src` to `dst` (length = min(dst.len(), src.len())).
///
/// x86_64: SSE2 128-bit vector copy for bulk throughput.
/// Fallback: `ptr::copy_nonoverlapping`.
#[cfg(target_arch = "x86_64")]
#[inline]
pub fn copy_bytes_simd(dst: &mut [u8], src: &[u8]) -> usize {
    use core::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_storeu_si128};

    let len = dst.len().min(src.len());
    let mut i = 0usize;
    unsafe {
        while i + 16 <= len {
            let v = _mm_loadu_si128(src.as_ptr().add(i).cast::<__m128i>());
            _mm_storeu_si128(dst.as_mut_ptr().add(i).cast::<__m128i>(), v);
            i += 16;
        }
        if i < len {
            core::ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), len - i);
        }
    }
    len
}

/// Copy bytes from `src` to `dst` (length = min(dst.len(), src.len())).
///
/// aarch64: NEON 128-bit vector copy for bulk throughput.
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn copy_bytes_simd(dst: &mut [u8], src: &[u8]) -> usize {
    let len = dst.len().min(src.len());
    let mut i = 0usize;
    unsafe {
        while i + 16 <= len {
            let v = core::arch::aarch64::vld1q_u8(src.as_ptr().add(i));
            core::arch::aarch64::vst1q_u8(dst.as_mut_ptr().add(i), v);
            i += 16;
        }
        if i < len {
            core::ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), len - i);
        }
    }
    len
}

/// Copy bytes from `src` to `dst` (length = min(dst.len(), src.len())).
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
pub fn copy_bytes_simd(dst: &mut [u8], src: &[u8]) -> usize {
    let len = dst.len().min(src.len());
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len);
    }
    len
}

// ── Tests ───────────────────────────────────────────────────────
