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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wider_simd_selects_wider_frame_batches() {
        assert_eq!(preferred_frame_batch_width(1), FrameBatchWidth::Pair);
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            assert_eq!(preferred_frame_batch_width(16), FrameBatchWidth::Pair);
            assert_eq!(preferred_frame_batch_width(32), FrameBatchWidth::Quad);
            assert_eq!(preferred_frame_batch_width(64), FrameBatchWidth::Octo);
        }
        #[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
        assert_eq!(preferred_frame_batch_width(16), FrameBatchWidth::Quad);
    }
}
