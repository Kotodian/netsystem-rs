use core::ptr;

#[inline]
pub fn internet_checksum(bytes: &[u8]) -> u16 {
    let even_len = bytes.len() & !1;
    let mut sum = accumulate_even_words(&bytes[..even_len]);
    if let Some(&high) = bytes.get(even_len) {
        sum = sum.wrapping_add(u64::from(high) << 8);
    }
    finish_checksum(sum)
}

#[inline]
pub fn internet_checksum_parts(parts: &[&[u8]]) -> u16 {
    let mut sum = 0u64;
    let mut high = None;
    for part in parts {
        let mut start = 0usize;
        if let Some(hi) = high.take() {
            if let Some(&lo) = part.first() {
                sum = sum.wrapping_add(u64::from(u16::from_be_bytes([hi, lo])));
                start = 1;
            } else {
                high = Some(hi);
                continue;
            }
        }
        let remainder = &part[start..];
        let even_len = remainder.len() & !1;
        sum = sum.wrapping_add(accumulate_even_words(&remainder[..even_len]));
        if let Some(&trailing_high) = remainder.get(even_len) {
            high = Some(trailing_high);
        }
    }
    if let Some(hi) = high {
        sum = sum.wrapping_add(u64::from(hi) << 8);
    }
    finish_checksum(sum)
}

#[inline]
fn finish_checksum(mut sum: u64) -> u16 {
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn accumulate_avx512(bytes: &[u8]) -> u64 {
    use core::arch::x86_64::{
        __m512i, _mm512_add_epi32, _mm512_loadu_si512, _mm512_or_si512, _mm512_setzero_si512,
        _mm512_slli_epi16, _mm512_srli_epi16, _mm512_storeu_si512, _mm512_unpackhi_epi16,
        _mm512_unpacklo_epi16,
    };

    let mut sum = _mm512_setzero_si512();
    let mut index = 0usize;
    while index + 64 <= bytes.len() {
        let vector = _mm512_loadu_si512(bytes.as_ptr().add(index).cast::<__m512i>());
        let swapped = _mm512_or_si512(_mm512_slli_epi16(vector, 8), _mm512_srli_epi16(vector, 8));
        let zero = _mm512_setzero_si512();
        sum = _mm512_add_epi32(sum, _mm512_unpacklo_epi16(swapped, zero));
        sum = _mm512_add_epi32(sum, _mm512_unpackhi_epi16(swapped, zero));
        index += 64;
    }
    let mut lanes = [0u32; 16];
    _mm512_storeu_si512(lanes.as_mut_ptr().cast::<__m512i>(), sum);
    let mut total = lanes.into_iter().map(u64::from).sum::<u64>();
    total = total.wrapping_add(accumulate_u64_words(&bytes[index..]));
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn accumulate_avx2(bytes: &[u8]) -> u64 {
    use core::arch::x86_64::{
        __m256i, _mm256_add_epi32, _mm256_loadu_si256, _mm256_or_si256, _mm256_setzero_si256,
        _mm256_slli_epi16, _mm256_srli_epi16, _mm256_storeu_si256, _mm256_unpackhi_epi16,
        _mm256_unpacklo_epi16,
    };

    let mut sum = _mm256_setzero_si256();
    let mut index = 0usize;
    while index + 32 <= bytes.len() {
        let vector = _mm256_loadu_si256(bytes.as_ptr().add(index).cast::<__m256i>());
        let swapped = _mm256_or_si256(_mm256_slli_epi16(vector, 8), _mm256_srli_epi16(vector, 8));
        let zero = _mm256_setzero_si256();
        sum = _mm256_add_epi32(sum, _mm256_unpacklo_epi16(swapped, zero));
        sum = _mm256_add_epi32(sum, _mm256_unpackhi_epi16(swapped, zero));
        index += 32;
    }
    let mut lanes = [0u32; 8];
    _mm256_storeu_si256(lanes.as_mut_ptr().cast::<__m256i>(), sum);
    let mut total = lanes.into_iter().map(u64::from).sum::<u64>();
    total = total.wrapping_add(accumulate_u64_words(&bytes[index..]));
    total
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn accumulate_even_words(bytes: &[u8]) -> u64 {
    use core::arch::x86_64::{
        __m128i, _mm_add_epi32, _mm_loadu_si128, _mm_or_si128, _mm_setzero_si128, _mm_slli_epi16,
        _mm_srli_epi16, _mm_storeu_si128, _mm_unpackhi_epi16, _mm_unpacklo_epi16,
    };

    if is_x86_feature_detected!("avx512f") {
        unsafe { accumulate_avx512(bytes) }
    } else if is_x86_feature_detected!("avx2") {
        unsafe { accumulate_avx2(bytes) }
    } else {
        unsafe {
            let mut sum = _mm_setzero_si128();
            let mut index = 0usize;
            while index + 16 <= bytes.len() {
                let vector = _mm_loadu_si128(bytes.as_ptr().add(index).cast::<__m128i>());
                let swapped = _mm_or_si128(_mm_slli_epi16(vector, 8), _mm_srli_epi16(vector, 8));
                let zero = _mm_setzero_si128();
                sum = _mm_add_epi32(sum, _mm_unpacklo_epi16(swapped, zero));
                sum = _mm_add_epi32(sum, _mm_unpackhi_epi16(swapped, zero));
                index += 16;
            }
            let mut lanes = [0u32; 4];
            _mm_storeu_si128(lanes.as_mut_ptr().cast::<__m128i>(), sum);
            let mut total = lanes.into_iter().map(u64::from).sum::<u64>();
            total = total.wrapping_add(accumulate_u64_words(&bytes[index..]));
            total
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn accumulate_even_words(bytes: &[u8]) -> u64 {
    use core::arch::aarch64::{
        uint16x8_t, uint32x4_t, vaddlvq_u32, vaddq_u32, vget_high_u16, vget_low_u16, vld1q_u8,
        vmovl_u16, vreinterpretq_u16_u8, vrev16q_u8,
    };

    unsafe {
        let mut sum: uint32x4_t = core::mem::zeroed();
        let mut index = 0usize;
        while index + 16 <= bytes.len() {
            let vector = vld1q_u8(bytes.as_ptr().add(index));
            let swapped: uint16x8_t = vreinterpretq_u16_u8(vrev16q_u8(vector));
            let lo = vmovl_u16(vget_low_u16(swapped));
            let hi = vmovl_u16(vget_high_u16(swapped));
            sum = vaddq_u32(sum, lo);
            sum = vaddq_u32(sum, hi);
            index += 16;
        }
        let mut total = u64::from(vaddlvq_u32(sum));
        total = total.wrapping_add(accumulate_u64_words(&bytes[index..]));
        total
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn accumulate_even_words(bytes: &[u8]) -> u64 {
    accumulate_u64_words(bytes)
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
    use super::{accumulate_u64_words, internet_checksum, internet_checksum_parts};

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
    fn accumulate_u64_words_matches_scalar_even_words() {
        let payload: Vec<u8> = (0..128).map(|value| value as u8).collect();
        let scalar = payload
            .chunks_exact(2)
            .map(|chunk| u64::from(u16::from_be_bytes([chunk[0], chunk[1]])))
            .sum::<u64>();
        assert_eq!(accumulate_u64_words(&payload), scalar);
    }
}
