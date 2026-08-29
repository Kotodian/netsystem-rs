//! Compare a batch of `u16` values to one selected value and produce stable
//! match bitmasks, following VPP `clib_mask_compare_u16` semantics.
//!
//! Bit `i` of the output mask words corresponds to input element `i`. Partial
//! final words clear every bit beyond the logical input length. The operation
//! allocates nothing.

/// Number of `u64` mask words required for `n_elts` input elements.
#[inline]
pub const fn mask_compare_u16_words(n_elts: usize) -> usize {
    n_elts.div_ceil(64)
}

/// Compare each element of `values` to `selected`.
///
/// Writes one bit per element into `masks` (64 elements per `u64` word, bit 0
/// of word 0 = `values[0]`). Bits beyond `values.len()` in the final written
/// word are cleared. Returns the number of matching elements.
///
/// Dispatches to the selected scalar or architecture-specific path. Every path
/// must produce identical masks and counts.
///
/// `# Panics`
///
/// Panics if `masks` is shorter than [`mask_compare_u16_words`]`(values.len())`.
#[inline]
pub fn mask_compare_u16(selected: u16, values: &[u16], masks: &mut [u64]) -> u32 {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        mask_compare_u16_arch(selected, values, masks)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        mask_compare_u16_scalar(selected, values, masks)
    }
}

/// Architecture-selected path. Today matches the scalar reference on all
/// supported targets; SIMD acceleration may replace the body without changing
/// the public contract.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline]
pub fn mask_compare_u16_arch(selected: u16, values: &[u16], masks: &mut [u64]) -> u32 {
    mask_compare_u16_scalar(selected, values, masks)
}

/// Scalar reference implementation. Architecture-specific paths must match it.
#[inline]
pub fn mask_compare_u16_scalar(selected: u16, values: &[u16], masks: &mut [u64]) -> u32 {
    let words = mask_compare_u16_words(values.len());
    assert!(
        masks.len() >= words,
        "masks length {} < required {}",
        masks.len(),
        words
    );

    let mut count = 0u32;
    let mut word = 0usize;
    while word < words {
        let base = word * 64;
        let end = (base + 64).min(values.len());
        let mut bits = 0u64;
        for (offset, &value) in values[base..end].iter().enumerate() {
            if value == selected {
                bits |= 1u64 << offset;
                count += 1;
            }
        }
        masks[word] = bits;
        word += 1;
    }
    count
}
