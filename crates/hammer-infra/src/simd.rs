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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn movemask_4_all_kept() {
        assert_eq!(movemask_4([true, true, true, true]), 0b1111);
    }

    #[test]
    fn movemask_4_none_kept() {
        assert_eq!(movemask_4([false, false, false, false]), 0b0000);
    }

    #[test]
    fn movemask_4_first_and_last() {
        assert_eq!(movemask_4([true, false, false, true]), 0b1001);
    }

    #[test]
    fn compact_indices_keeps_first_and_last() {
        let mut idx: Vec<u32> = vec![10, 20, 30, 40];
        let mut write = 0usize;
        compact_indices(&mut idx, 0b1001, 0, &mut write);
        assert_eq!(write, 2);
        assert_eq!(idx[0], 10);
        assert_eq!(idx[1], 40);
    }

    #[test]
    fn compact_indices_none_kept() {
        let mut idx: Vec<u32> = vec![10, 20, 30, 40];
        let mut write = 0usize;
        compact_indices(&mut idx, 0b0000, 0, &mut write);
        assert_eq!(write, 0);
    }

    #[test]
    fn compact_indices_all_kept() {
        let mut idx: Vec<u32> = vec![10, 20, 30, 40];
        let mut write = 0usize;
        compact_indices(&mut idx, 0b1111, 0, &mut write);
        assert_eq!(write, 4);
        assert_eq!(idx[0], 10);
        assert_eq!(idx[1], 20);
        assert_eq!(idx[2], 30);
        assert_eq!(idx[3], 40);
    }

    #[test]
    fn copy_bytes_simd_roundtrip() {
        let src = vec![1u8, 2, 3, 4, 5];
        let mut dst = vec![0u8; 5];
        let n = copy_bytes_simd(&mut dst, &src);
        assert_eq!(n, 5);
        assert_eq!(&dst, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn copy_bytes_simd_truncates_to_shorter() {
        let src = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let mut dst = vec![0u8; 3];
        let n = copy_bytes_simd(&mut dst, &src);
        assert_eq!(n, 3);
        assert_eq!(&dst[..3], &[1, 2, 3]);
    }

    #[test]
    fn copy_bytes_simd_exact_16() {
        let src: Vec<u8> = (0..16).collect();
        let mut dst = vec![0u8; 16];
        let n = copy_bytes_simd(&mut dst, &src);
        assert_eq!(n, 16);
        assert_eq!(&dst, &src);
    }

    #[test]
    fn copy_bytes_simd_over_16() {
        let src: Vec<u8> = (0..32).collect();
        let mut dst = vec![0u8; 32];
        let n = copy_bytes_simd(&mut dst, &src);
        assert_eq!(n, 32);
        assert_eq!(&dst, &src);
    }

    #[test]
    fn copy_bytes_simd_unaligned() {
        let src: Vec<u8> = (0..24).collect();
        let mut dst = vec![0u8; 24];
        let n = copy_bytes_simd(&mut dst[1..], &src[1..]);
        assert_eq!(n, 23);
        assert_eq!(&dst[1..], &src[1..]);
    }
}
