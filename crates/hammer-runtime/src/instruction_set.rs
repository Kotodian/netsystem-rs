#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBatchWidth {
    Pair,
    Quad,
    Octo,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataPlaneInstructionSet {
    Scalar,
    Sse2,
    Avx2,
    Avx512,
    Neon,
}

impl DataPlaneInstructionSet {
    pub(crate) const VARIANT_COUNT: usize = Self::Neon as usize + 1;

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

    pub(crate) fn is_supported(self) -> bool {
        match self {
            Self::Scalar => true,
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            Self::Sse2 => std::is_x86_feature_detected!("sse2"),
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            Self::Avx2 => std::is_x86_feature_detected!("avx2"),
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            Self::Avx512 => std::is_x86_feature_detected!("avx512f"),
            #[cfg(target_arch = "aarch64")]
            Self::Neon => true,
            #[cfg(target_arch = "arm")]
            Self::Neon => cfg!(target_feature = "neon"),
            _ => false,
        }
    }

    pub(crate) const fn candidate_priority(self, candidate: Self) -> Option<u8> {
        if matches!(candidate, Self::Scalar) {
            return Some(0);
        }
        if self.same_architecture_family(candidate) && candidate.rank() <= self.rank() {
            Some(candidate.rank())
        } else {
            None
        }
    }

    const fn same_architecture_family(self, candidate: Self) -> bool {
        match self {
            Self::Sse2 | Self::Avx2 | Self::Avx512 => {
                matches!(candidate, Self::Sse2 | Self::Avx2 | Self::Avx512)
            }
            Self::Neon => matches!(candidate, Self::Neon),
            Self::Scalar => false,
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Scalar => 0,
            Self::Sse2 | Self::Neon => 1,
            Self::Avx2 => 2,
            Self::Avx512 => 3,
        }
    }

    pub(crate) const fn slot(self) -> usize {
        self as usize
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
    fn candidate_priority_is_ranked_only_within_an_architecture_family() {
        let cases = [
            (
                DataPlaneInstructionSet::Avx512,
                DataPlaneInstructionSet::Scalar,
                Some(0),
            ),
            (
                DataPlaneInstructionSet::Avx512,
                DataPlaneInstructionSet::Sse2,
                Some(1),
            ),
            (
                DataPlaneInstructionSet::Avx512,
                DataPlaneInstructionSet::Avx2,
                Some(2),
            ),
            (
                DataPlaneInstructionSet::Avx2,
                DataPlaneInstructionSet::Avx512,
                None,
            ),
            (
                DataPlaneInstructionSet::Neon,
                DataPlaneInstructionSet::Scalar,
                Some(0),
            ),
            (
                DataPlaneInstructionSet::Neon,
                DataPlaneInstructionSet::Neon,
                Some(1),
            ),
            (
                DataPlaneInstructionSet::Neon,
                DataPlaneInstructionSet::Sse2,
                None,
            ),
            (
                DataPlaneInstructionSet::Avx2,
                DataPlaneInstructionSet::Neon,
                None,
            ),
        ];

        for (configured, candidate, expected) in cases {
            assert_eq!(configured.candidate_priority(candidate), expected);
        }
    }

    #[test]
    fn avx512_preferred_batch_width_is_octo() {
        assert_eq!(
            DataPlaneInstructionSet::Avx512.preferred_frame_batch_width(),
            FrameBatchWidth::Octo
        );
    }
}
