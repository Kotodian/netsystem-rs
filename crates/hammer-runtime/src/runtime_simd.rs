use crate::Simd;
use hammer_core::data_plane::FrameBatchWidth;

pub(crate) const SCALAR_SIMD_BYTES: usize = 1;

pub(crate) fn native_simd_bytes() -> usize {
    [64, 32, 16]
        .into_iter()
        .find(|&bytes| match bytes {
            64 => Simd::<u8, 64>::is_supported(),
            32 => Simd::<u8, 32>::is_supported(),
            16 => Simd::<u8, 16>::is_supported(),
            _ => false,
        })
        .unwrap_or(SCALAR_SIMD_BYTES)
}

pub(crate) const fn preferred_frame_batch_width(simd_bytes: usize) -> FrameBatchWidth {
    #[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
    if simd_bytes >= 16 {
        return FrameBatchWidth::Quad;
    }

    if simd_bytes >= 64 {
        FrameBatchWidth::Octo
    } else if simd_bytes >= 32 {
        FrameBatchWidth::Quad
    } else {
        FrameBatchWidth::Pair
    }
}
