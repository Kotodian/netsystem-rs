#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBatchWidth {
    Pair,
    Quad,
    Octo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataPlaneInstructionSet {
    Scalar,
    Sse2,
    Avx2,
    Avx512,
    Neon,
}

impl DataPlaneInstructionSet {
    pub fn native() -> Self {
        native_instruction_set()
    }

    pub fn preferred_frame_batch_width(self) -> FrameBatchWidth {
        match self {
            Self::Scalar | Self::Sse2 => FrameBatchWidth::Pair,
            Self::Avx2 | Self::Neon => FrameBatchWidth::Quad,
            Self::Avx512 => FrameBatchWidth::Octo,
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn native_instruction_set() -> DataPlaneInstructionSet {
    // AVX-512 priority: avx512f → avx2 → sse2 → scalar
    if std::is_x86_feature_detected!("avx512f") {
        DataPlaneInstructionSet::Avx512
    } else if std::is_x86_feature_detected!("avx2") {
        DataPlaneInstructionSet::Avx2
    } else if std::is_x86_feature_detected!("sse2") {
        DataPlaneInstructionSet::Sse2
    } else {
        DataPlaneInstructionSet::Scalar
    }
}

#[cfg(target_arch = "aarch64")]
fn native_instruction_set() -> DataPlaneInstructionSet {
    DataPlaneInstructionSet::Neon
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
fn native_instruction_set() -> DataPlaneInstructionSet {
    DataPlaneInstructionSet::Neon
}

#[cfg(not(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "aarch64",
    all(target_arch = "arm", target_feature = "neon")
)))]
fn native_instruction_set() -> DataPlaneInstructionSet {
    DataPlaneInstructionSet::Scalar
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avx512_preferred_batch_width_is_octo() {
        assert_eq!(
            DataPlaneInstructionSet::Avx512.preferred_frame_batch_width(),
            FrameBatchWidth::Octo
        );
    }
}
