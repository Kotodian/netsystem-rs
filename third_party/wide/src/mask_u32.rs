use crate::{u32x4, u32x8, u32x16};

impl u32x4 {
  /// Return one equality bit per lane using SSE2, with lane zero in bit zero.
  ///
  /// # Safety
  /// The caller must ensure that SSE2 is available.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "sse2")]
  pub unsafe fn is_equal_mask_sse2(self, other: Self) -> u8 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m128i, _mm_castsi128_ps, _mm_cmpeq_epi32, _mm_movemask_ps};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m128i, _mm_castsi128_ps, _mm_cmpeq_epi32, _mm_movemask_ps};

    // SAFETY: u32x4 and __m128i have the same 128-bit representation.
    unsafe {
      let left: __m128i = core::mem::transmute(self);
      let right: __m128i = core::mem::transmute(other);
      _mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(left, right))) as u8
    }
  }

  /// Return one equality bit per lane using AVX-512VL.
  ///
  /// # Safety
  /// The caller must ensure that AVX-512F and AVX-512VL are available.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f,avx512vl")]
  pub unsafe fn is_equal_mask_avx512vl(self, other: Self) -> u8 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m128i, _mm_cmpeq_epu32_mask};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m128i, _mm_cmpeq_epu32_mask};

    // SAFETY: u32x4 and __m128i have the same 128-bit representation.
    unsafe {
      let left: __m128i = core::mem::transmute(self);
      let right: __m128i = core::mem::transmute(other);
      _mm_cmpeq_epu32_mask(left, right)
    }
  }
}

impl u32x8 {
  /// Return one equality bit per lane using AVX2 comparison and movemask.
  ///
  /// # Safety
  /// The caller must ensure that AVX2 is available.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx2")]
  pub unsafe fn is_equal_mask_avx2(self, other: Self) -> u8 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m256i, _mm256_castsi256_ps, _mm256_cmpeq_epi32, _mm256_movemask_ps};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m256i, _mm256_castsi256_ps, _mm256_cmpeq_epi32, _mm256_movemask_ps};

    // SAFETY: u32x8 and __m256i have the same 256-bit representation.
    unsafe {
      let left: __m256i = core::mem::transmute(self);
      let right: __m256i = core::mem::transmute(other);
      _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(left, right))) as u8
    }
  }

  /// Return one equality bit per lane using AVX-512VL.
  ///
  /// # Safety
  /// The caller must ensure that AVX-512F and AVX-512VL are available.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f,avx512vl")]
  pub unsafe fn is_equal_mask_avx512vl(self, other: Self) -> u8 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m256i, _mm256_cmpeq_epu32_mask};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m256i, _mm256_cmpeq_epu32_mask};

    // SAFETY: u32x8 and __m256i have the same 256-bit representation.
    unsafe {
      let left: __m256i = core::mem::transmute(self);
      let right: __m256i = core::mem::transmute(other);
      _mm256_cmpeq_epu32_mask(left, right)
    }
  }

  /// Store selected lanes with AVX-512VL. The caller must have checked support.
  ///
  /// # Safety
  /// Executing this method without AVX-512F and AVX-512VL is undefined behavior.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f,avx512vl")]
  pub unsafe fn mask_store_avx512vl(self, destination: &mut [u32], mask: u8) {
    assert!(destination.len() >= (u8::BITS - mask.leading_zeros()) as usize);
    if mask == 0 {
      return;
    }
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m256i, _mm256_mask_storeu_epi32};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m256i, _mm256_mask_storeu_epi32};

    // SAFETY: both vector representations are 256 bits. The mask suppresses
    // every access outside the checked slice, including the short final slot.
    unsafe {
      let values: __m256i = core::mem::transmute(self);
      _mm256_mask_storeu_epi32(destination.as_mut_ptr().cast(), mask, values);
    }
  }

  /// Load selected lanes with AVX-512VL, zeroing the others.
  ///
  /// # Safety
  /// Executing this method without AVX-512F and AVX-512VL is undefined behavior.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f,avx512vl")]
  pub unsafe fn mask_load_zero_avx512vl(source: &[u32], mask: u8) -> Self {
    assert!(source.len() >= (u8::BITS - mask.leading_zeros()) as usize);
    if mask == 0 {
      return Self::splat(0);
    }
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m256i, _mm256_maskz_loadu_epi32};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m256i, _mm256_maskz_loadu_epi32};

    // SAFETY: the mask suppresses every read outside the checked slice.
    unsafe {
      let values: __m256i = _mm256_maskz_loadu_epi32(mask, source.as_ptr().cast());
      core::mem::transmute(values)
    }
  }
}

impl u32x16 {
  /// Return one equality bit per lane using AVX-512F.
  ///
  /// # Safety
  /// The caller must ensure that AVX-512F is available.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f")]
  pub unsafe fn is_equal_mask_avx512(self, other: Self) -> u16 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m512i, _mm512_cmpeq_epu32_mask};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m512i, _mm512_cmpeq_epu32_mask};

    // SAFETY: u32x16 and __m512i have the same 512-bit representation.
    unsafe {
      let left: __m512i = core::mem::transmute(self);
      let right: __m512i = core::mem::transmute(other);
      _mm512_cmpeq_epu32_mask(left, right)
    }
  }

  /// Store selected lanes with AVX-512F. The caller must have checked support.
  ///
  /// # Safety
  /// Executing this method without AVX-512F support is undefined behavior.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f")]
  pub unsafe fn mask_store_avx512(self, destination: &mut [u32], mask: u16) {
    assert!(destination.len() >= (u16::BITS - mask.leading_zeros()) as usize);
    if mask == 0 {
      return;
    }
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m512i, _mm512_mask_storeu_epi32};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m512i, _mm512_mask_storeu_epi32};

    // SAFETY: both vector representations are 512 bits. The hardware mask
    // suppresses every access outside the checked slice.
    unsafe {
      let values: __m512i = core::mem::transmute(self);
      _mm512_mask_storeu_epi32(destination.as_mut_ptr().cast(), mask, values);
    }
  }

  /// Load selected lanes with AVX-512F, zeroing the others.
  ///
  /// # Safety
  /// Executing this method without AVX-512F support is undefined behavior.
  #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
  #[target_feature(enable = "avx512f")]
  pub unsafe fn mask_load_zero_avx512(source: &[u32], mask: u16) -> Self {
    assert!(source.len() >= (u16::BITS - mask.leading_zeros()) as usize);
    if mask == 0 {
      return Self::splat(0);
    }
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m512i, _mm512_maskz_loadu_epi32};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m512i, _mm512_maskz_loadu_epi32};

    // SAFETY: the hardware mask suppresses every read outside the checked slice.
    unsafe {
      let values: __m512i = _mm512_maskz_loadu_epi32(mask, source.as_ptr().cast());
      core::mem::transmute(values)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  extern crate std;

  #[test]
  fn equality_masks_select_matching_lanes() {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
      let four = u32x4::new([0, 1, 0, 1]);
      if std::is_x86_feature_detected!("sse2") {
        assert_eq!(unsafe { four.is_equal_mask_sse2(u32x4::splat(0)) }, 0b0101);
      }
      if std::is_x86_feature_detected!("avx2") {
        let eight = u32x8::new([0, 1, 0, 1, 1, 0, 1, 0]);
        assert_eq!(unsafe { eight.is_equal_mask_avx2(u32x8::splat(0)) }, 0b1010_0101);
      }
      if std::is_x86_feature_detected!("avx512f") {
        let sixteen = u32x16::new([0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0]);
        assert_eq!(unsafe { sixteen.is_equal_mask_avx512(u32x16::splat(0)) }, 0b1010_0101_1010_0101);
        if std::is_x86_feature_detected!("avx512vl") {
          assert_eq!(unsafe { four.is_equal_mask_avx512vl(u32x4::splat(0)) }, 0b0101);
          let eight = u32x8::new([0, 1, 0, 1, 1, 0, 1, 0]);
          assert_eq!(unsafe { eight.is_equal_mask_avx512vl(u32x8::splat(0)) }, 0b1010_0101);
        }
      }
    }
  }

  #[test]
  fn masked_u32x8_does_not_touch_unselected_lanes() {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("avx512vl") {
      let values = u32x8::new([1, 2, 3, 4, 5, 6, 7, 8]);
      let mut destination = [99; 7];
      unsafe { values.mask_store_avx512vl(&mut destination, 0b0101_0011) };
      assert_eq!(destination, [1, 2, 99, 99, 5, 99, 7]);
      assert_eq!(
        unsafe { u32x8::mask_load_zero_avx512vl(&destination, 0b0101_0011) }.to_array(),
        [1, 2, 0, 0, 5, 0, 7, 0]
      );
      let mut empty: [u32; 0] = [];
      unsafe { values.mask_store_avx512vl(&mut empty, 0) };
      assert_eq!(unsafe { u32x8::mask_load_zero_avx512vl(&empty, 0) }, u32x8::splat(0));
    }
  }

  #[test]
  fn masked_u32x16_accepts_a_short_destination() {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if std::is_x86_feature_detected!("avx512f") {
      let values = u32x16::new([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
      let mut destination = [99; 5];
      unsafe { values.mask_store_avx512(&mut destination, 0b0000_0000_0001_0101) };
      assert_eq!(destination, [1, 99, 3, 99, 5]);
      assert_eq!(
        unsafe { u32x16::mask_load_zero_avx512(&destination, 0b0000_0000_0001_0101) }.to_array()[..5],
        [1, 0, 3, 0, 5]
      );
      let mut empty: [u32; 0] = [];
      unsafe { values.mask_store_avx512(&mut empty, 0) };
      assert_eq!(unsafe { u32x16::mask_load_zero_avx512(&empty, 0) }, u32x16::splat(0));
    }
  }
}
