use core::{hash::Hasher, mem::size_of, ptr};
#[cfg(target_arch = "x86_64")]
use std::sync::OnceLock;

use crate::simd::Simd;

type ChecksumParts = fn(&[&[u8]]) -> u16;
const ACCUMULATE_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
pub struct InternetChecksum<const SIMD_BYTES: usize = 1> {
    sum: u64,
    trailing_high: Option<u8>,
}

impl<const SIMD_BYTES: usize> Default for InternetChecksum<SIMD_BYTES> {
    #[inline]
    fn default() -> Self {
        const {
            assert!(matches!(SIMD_BYTES, 1 | 16 | 32 | 64));
        }
        Self {
            sum: 0,
            trailing_high: None,
        }
    }
}

impl<const SIMD_BYTES: usize> Hasher for InternetChecksum<SIMD_BYTES> {
    #[inline]
    fn finish(&self) -> u64 {
        let mut sum = self.sum;
        if let Some(high) = self.trailing_high {
            sum = sum.wrapping_add(u64::from(high) << 8);
        }
        u64::from(finish_checksum(sum))
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut start = 0usize;
        if let Some(high) = self.trailing_high.take() {
            let Some(&low) = bytes.first() else {
                self.trailing_high = Some(high);
                return;
            };
            self.sum = self
                .sum
                .wrapping_add(u64::from(u16::from_be_bytes([high, low])));
            start = 1;
        }

        let remainder = &bytes[start..];
        let even_len = remainder.len() & !1;
        for chunk in remainder[..even_len].chunks(ACCUMULATE_CHUNK_BYTES) {
            self.sum = fold_checksum_sum(
                self.sum
                    .wrapping_add(accumulate_for_simd::<SIMD_BYTES>(chunk)),
            );
        }
        self.trailing_high = remainder.get(even_len).copied();
    }
}

#[inline]
pub fn internet_checksum(bytes: &[u8]) -> u16 {
    internet_checksum_parts(&[bytes])
}

#[inline]
pub fn internet_checksum_parts(parts: &[&[u8]]) -> u16 {
    native_checksum_parts()(parts)
}

#[inline(always)]
fn checksum_parts_with_simd<const SIMD_BYTES: usize>(parts: &[&[u8]]) -> u16 {
    let mut checksum = InternetChecksum::<SIMD_BYTES>::default();
    for part in parts {
        checksum.write(part);
    }
    checksum.finish() as u16
}

#[inline]
fn fold_checksum_sum(mut sum: u64) -> u64 {
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    sum
}

#[inline]
fn finish_checksum(sum: u64) -> u16 {
    !(fold_checksum_sum(sum) as u16)
}

#[inline(always)]
fn accumulate_simd<const LANES: usize>(bytes: &[u8]) -> u64 {
    const {
        assert!(LANES > 0);
    }
    let vector_bytes = LANES * size_of::<u16>();
    let mut total = 0u64;
    let mut index = 0usize;
    while index + vector_bytes <= bytes.len() {
        let values =
            unsafe { ptr::read_unaligned(bytes.as_ptr().add(index).cast::<[u16; LANES]>()) };
        let words = Simd::from_array(values);
        #[cfg(target_endian = "little")]
        let words = words.swap_bytes();
        let widened: Simd<u32, LANES> = words.into();
        total = total.wrapping_add(u64::from(widened.reduce_sum()));
        index += vector_bytes;
    }
    total.wrapping_add(accumulate_u64_words(&bytes[index..]))
}

#[inline(always)]
fn accumulate_for_simd<const SIMD_BYTES: usize>(bytes: &[u8]) -> u64 {
    match SIMD_BYTES {
        64 => accumulate_simd::<32>(bytes),
        32 => accumulate_simd::<16>(bytes),
        16 => accumulate_simd::<8>(bytes),
        1 => accumulate_u64_words(bytes),
        _ => unreachable!("unsupported SIMD width"),
    }
}

#[cfg(target_arch = "x86_64")]
fn native_checksum_parts() -> ChecksumParts {
    static CHECKSUM: OnceLock<ChecksumParts> = OnceLock::new();
    *CHECKSUM.get_or_init(|| {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
            checksum_avx512
        } else if is_x86_feature_detected!("avx2") {
            checksum_avx2
        } else {
            checksum_sse2
        }
    })
}

#[cfg(target_arch = "x86_64")]
fn checksum_avx512(parts: &[&[u8]]) -> u16 {
    unsafe { checksum_avx512_inner(parts) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn checksum_avx512_inner(parts: &[&[u8]]) -> u16 {
    checksum_parts_with_simd::<64>(parts)
}

#[cfg(target_arch = "x86_64")]
fn checksum_avx2(parts: &[&[u8]]) -> u16 {
    unsafe { checksum_avx2_inner(parts) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn checksum_avx2_inner(parts: &[&[u8]]) -> u16 {
    checksum_parts_with_simd::<32>(parts)
}

#[cfg(target_arch = "x86_64")]
fn checksum_sse2(parts: &[&[u8]]) -> u16 {
    checksum_parts_with_simd::<16>(parts)
}

#[cfg(target_arch = "aarch64")]
fn native_checksum_parts() -> ChecksumParts {
    checksum_parts_with_simd::<16>
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn native_checksum_parts() -> ChecksumParts {
    checksum_parts_with_simd::<1>
}

#[inline]
fn accumulate_u64_words(bytes: &[u8]) -> u64 {
    let mut sum = 0u64;
    let mut index = 0usize;
    while index + 8 <= bytes.len() {
        let word = unsafe { ptr::read_unaligned(bytes.as_ptr().add(index).cast::<u64>()) };
        let swapped = swap_bytes_in_each_u16(word);
        sum = sum
            .wrapping_add(swapped & 0xffff)
            .wrapping_add((swapped >> 16) & 0xffff)
            .wrapping_add((swapped >> 32) & 0xffff)
            .wrapping_add(swapped >> 48);
        index += 8;
    }
    while index + 2 <= bytes.len() {
        sum = sum.wrapping_add(u64::from(u16::from_be_bytes([
            bytes[index],
            bytes[index + 1],
        ])));
        index += 2;
    }
    sum
}

#[inline]
const fn swap_bytes_in_each_u16(value: u64) -> u64 {
    ((value & 0x00ff_00ff_00ff_00ff) << 8) | ((value & 0xff00_ff00_ff00_ff00) >> 8)
}

#[cfg(test)]
mod tests {
    use core::hash::Hasher;

    use super::{
        InternetChecksum, accumulate_u64_words, internet_checksum, internet_checksum_parts,
    };

    fn scalar_internet_checksum(bytes: &[u8]) -> u16 {
        let mut sum = 0u32;
        for chunk in bytes.chunks(2) {
            let word = match chunk {
                [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
                [hi] => u16::from_be_bytes([*hi, 0]) as u32,
                _ => unreachable!(),
            };
            sum += word;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }

    #[test]
    fn internet_checksum_matches_scalar_reference() {
        let payload: Vec<u8> = (0..197).map(|value| value as u8).collect();
        assert_eq!(
            internet_checksum(&payload),
            scalar_internet_checksum(&payload)
        );
    }

    #[test]
    fn internet_checksum_parts_matches_concatenated_reference() {
        let first: Vec<u8> = (0..31).map(|value| value as u8).collect();
        let second: Vec<u8> = (31..95).map(|value| value as u8).collect();
        let third: Vec<u8> = (95..160).map(|value| value as u8).collect();
        let mut combined = Vec::new();
        combined.extend_from_slice(&first);
        combined.extend_from_slice(&second);
        combined.extend_from_slice(&third);
        assert_eq!(
            internet_checksum_parts(&[&first, &second, &third]),
            scalar_internet_checksum(&combined)
        );
    }

    #[test]
    fn internet_checksum_hasher_preserves_words_across_odd_parts() {
        let parts: [&[u8]; 5] = [&[0x01], &[], &[0x02, 0x03], &[0x04], &[0x05]];
        let mut checksum = InternetChecksum::<1>::default();
        for part in parts {
            checksum.write(part);
        }
        assert_eq!(
            checksum.finish() as u16,
            internet_checksum(&[1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn internet_checksum_hasher_folds_long_incremental_input() {
        let payload: Vec<u8> = (0..200_003).map(|value| value as u8).collect();
        let mut checksum = InternetChecksum::<1>::default();
        for part in payload.chunks(997) {
            checksum.write(part);
        }
        assert_eq!(checksum.finish() as u16, scalar_internet_checksum(&payload));
    }

    #[test]
    fn accumulate_u64_words_matches_scalar_even_words() {
        let payload: Vec<u8> = (0..128).map(|value| value as u8).collect();
        let scalar = payload
            .chunks_exact(2)
            .map(|chunk| u64::from(u16::from_be_bytes([chunk[0], chunk[1]])))
            .sum::<u64>();
        assert_eq!(accumulate_u64_words(&payload), scalar);
    }
}
