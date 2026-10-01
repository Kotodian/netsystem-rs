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
#[inline(always)]
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

/// x86-64 baseline SSE2 comparison, including a scalar tail for valid slices.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn mask_compare_u16_arch(selected: u16, values: &[u16], masks: &mut [u64]) -> u32 {
    use core::arch::x86_64::{
        __m128i, _mm_cmpeq_epi16, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi16,
    };

    let words = mask_compare_u16_words(values.len());
    assert!(
        masks.len() >= words,
        "mask output is shorter than input bitmap"
    );
    let selected_vector = unsafe { _mm_set1_epi16(selected as i16) };
    let mut count = 0u32;
    for (word, mask) in masks[..words].iter_mut().enumerate() {
        let base = word * 64;
        let end = (base + 64).min(values.len());
        let mut bits = 0u64;
        let mut offset = base;
        while offset + 8 <= end {
            let vector = unsafe { _mm_loadu_si128(values.as_ptr().add(offset).cast::<__m128i>()) };
            let equal = unsafe { _mm_cmpeq_epi16(vector, selected_vector) };
            let mut paired = unsafe { _mm_movemask_epi8(equal) } as u32 & 0x5555;
            paired = (paired | (paired >> 1)) & 0x3333;
            paired = (paired | (paired >> 2)) & 0x0f0f;
            paired = (paired | (paired >> 4)) & 0x00ff;
            bits |= u64::from(paired) << (offset - base);
            offset += 8;
        }
        while offset < end {
            bits |= u64::from(values[offset] == selected) << (offset - base);
            offset += 1;
        }
        *mask = bits;
        count += bits.count_ones();
    }
    count
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
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

/// Stable compression of Buffer-sized indices under a valid-slice bitmap.
/// VPP `clib_compress_u32`, buffer_funcs.c:34-45.
#[inline(always)]
pub fn compress_u32(dst: &mut [u32], src: &[u32], mask: &[u64]) -> usize {
    assert!(mask.len() >= mask_compare_u16_words(src.len()));
    let mut written = 0;
    for (word_index, &word) in mask
        .iter()
        .take(mask_compare_u16_words(src.len()))
        .enumerate()
    {
        let mut bits = word;
        while bits != 0 {
            let index = word_index * 64 + bits.trailing_zeros() as usize;
            assert!(index < src.len(), "mask bit exceeds input slice");
            assert!(written < dst.len(), "compressed output exceeds destination");
            dst[written] = src[index];
            written += 1;
            bits &= bits - 1;
        }
    }
    written
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_and_compress_partial_words() {
        for length in [0, 1, 7, 8, 9, 63, 64, 65, 127, 256] {
            let values = (0..length)
                .map(|index| if index % 3 == 0 { 7 } else { 9 })
                .collect::<Vec<u16>>();
            let source = (0..length as u32).collect::<Vec<_>>();
            let mut mask = vec![0; mask_compare_u16_words(length)];
            let mut reference = mask.clone();
            let count = mask_compare_u16(7, &values, &mut mask);
            assert_eq!(count, mask_compare_u16_scalar(7, &values, &mut reference));
            assert_eq!(mask, reference);

            let mut destination = vec![0; count as usize];
            assert_eq!(
                compress_u32(&mut destination, &source, &mask),
                count as usize
            );
            let expected = source
                .iter()
                .enumerate()
                .filter_map(|(index, &value)| (index % 3 == 0).then_some(value))
                .collect::<Vec<_>>();
            assert_eq!(destination, expected);
        }
    }
}
