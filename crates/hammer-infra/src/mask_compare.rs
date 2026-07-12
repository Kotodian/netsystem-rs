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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_counts_zero_and_writes_no_words() {
        let mut masks = [0xFFFF_FFFF_FFFF_FFFFu64; 1];
        assert_eq!(mask_compare_u16(1, &[], &mut masks), 0);
        assert_eq!(mask_compare_u16_words(0), 0);
        // No words written; caller-supplied buffer is untouched when n_elts == 0.
        assert_eq!(masks[0], 0xFFFF_FFFF_FFFF_FFFF);
    }

    #[test]
    fn single_match_sets_bit_zero() {
        let mut masks = [0u64; 1];
        assert_eq!(mask_compare_u16(7, &[7u16], &mut masks), 1);
        assert_eq!(masks[0], 0b1);
    }

    #[test]
    fn single_mismatch_clears_word() {
        let mut masks = [!0u64; 1];
        assert_eq!(mask_compare_u16(7, &[8u16], &mut masks), 0);
        assert_eq!(masks[0], 0);
    }

    #[test]
    fn alternating_pattern_uses_literal_expectation() {
        let values = [1u16, 0, 1, 0, 1, 0, 1, 0];
        let mut masks = [0u64; 1];
        assert_eq!(mask_compare_u16(1, &values, &mut masks), 4);
        assert_eq!(masks[0], 0b0101_0101);
    }

    #[test]
    fn sparse_matches_use_literal_expectation() {
        let values = [9u16, 1, 9, 2, 3, 9, 4];
        let mut masks = [0u64; 1];
        assert_eq!(mask_compare_u16(9, &values, &mut masks), 3);
        assert_eq!(masks[0], (1 << 0) | (1 << 2) | (1 << 5));
    }

    #[test]
    fn all_match_and_no_match_use_literal_expectations() {
        let values = [4u16; 16];
        let mut masks = [0u64; 1];
        assert_eq!(mask_compare_u16(4, &values, &mut masks), 16);
        assert_eq!(masks[0], (1u64 << 16) - 1);

        let mut masks = [!0u64; 1];
        assert_eq!(mask_compare_u16(5, &values, &mut masks), 0);
        assert_eq!(masks[0], 0);
    }

    #[test]
    fn partial_final_word_clears_bits_beyond_length() {
        let values = [9u16, 9, 9];
        let mut masks = [!0u64; 1];
        assert_eq!(mask_compare_u16(9, &values, &mut masks), 3);
        assert_eq!(masks[0], 0b111);
    }

    #[test]
    fn lengths_from_zero_through_256_map_bits_to_elements() {
        for len in 0usize..=256 {
            let mut values = vec![0u16; len];
            for (i, slot) in values.iter_mut().enumerate() {
                if i % 3 == 0 {
                    *slot = 11;
                }
            }
            let words = mask_compare_u16_words(len);
            let mut masks = vec![!0u64; words.max(1)];
            let count = mask_compare_u16(11, &values, &mut masks[..words]);
            let expected = values.iter().filter(|&&v| v == 11).count() as u32;
            assert_eq!(count, expected, "len={len}");
            for i in 0..len {
                let bit = (masks[i / 64] >> (i % 64)) & 1;
                let want = u64::from(values[i] == 11);
                assert_eq!(bit, want, "len={len} i={i}");
            }
            if len % 64 != 0 {
                let last = masks[words - 1];
                let valid = (1u64 << (len % 64)) - 1;
                assert_eq!(last & !valid, 0, "len={len} tail bits must be clear");
            }
        }
    }

    #[test]
    fn length_256_uses_four_mask_words_with_literal_corners() {
        let mut values = [0u16; 256];
        values[0] = 5;
        values[63] = 5;
        values[64] = 5;
        values[255] = 5;
        let mut masks = [0u64; 4];
        assert_eq!(mask_compare_u16(5, &values, &mut masks), 4);
        assert_eq!(masks[0], 1 | (1u64 << 63));
        assert_eq!(masks[1], 1);
        assert_eq!(masks[2], 0);
        assert_eq!(masks[3], 1u64 << 63);
    }

    #[test]
    fn scalar_and_public_path_agree_on_fixture() {
        let values = [3u16, 1, 3, 2, 3, 3, 0, 3, 9, 3];
        let mut public_masks = [0u64; 1];
        let mut scalar_masks = [0u64; 1];
        let public_count = mask_compare_u16(3, &values, &mut public_masks);
        let scalar_count = mask_compare_u16_scalar(3, &values, &mut scalar_masks);
        assert_eq!(public_count, scalar_count);
        assert_eq!(public_masks, scalar_masks);
        assert_eq!(public_count, 6);
        assert_eq!(public_masks[0], 0b10_1011_0101);
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    #[test]
    fn arch_path_matches_scalar_on_fixture() {
        let values = [2u16, 7, 2, 2, 0, 2, 1, 2];
        let mut arch_masks = [0u64; 1];
        let mut scalar_masks = [0u64; 1];
        let arch_count = mask_compare_u16_arch(2, &values, &mut arch_masks);
        let scalar_count = mask_compare_u16_scalar(2, &values, &mut scalar_masks);
        assert_eq!(arch_count, scalar_count);
        assert_eq!(arch_masks, scalar_masks);
        assert_eq!(arch_count, 5);
        assert_eq!(arch_masks[0], 0b1010_1101);
    }

    #[test]
    fn public_surface_names_avoid_graph_vocabulary() {
        let src = include_str!("mask_compare.rs");
        let production = src
            .split("#[cfg(test)]")
            .next()
            .expect("production section before tests");
        for forbidden in [
            "Graph Node",
            "Next Arc",
            "NextArc",
            "BufferFrame",
            "NodeNext",
            "enqueue_to_next",
        ] {
            assert!(
                !production.contains(forbidden),
                "mask_compare production surface must not contain {forbidden:?}"
            );
        }
    }
}
