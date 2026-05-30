#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBatchWidth {
    Pair,
    Quad,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataPlaneInstructionSet {
    Scalar,
    Sse2,
    Avx2,
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
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn native_instruction_set() -> DataPlaneInstructionSet {
    if std::is_x86_feature_detected!("avx2") {
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
