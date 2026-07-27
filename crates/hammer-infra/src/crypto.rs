//! Protocol-neutral portable cryptographic algorithms.
//!
//! This layer accepts raw caller-owned memory and implements algorithm
//! semantics only. Key policy, implementation selection, prepared contexts,
//! and operation lifecycle belong to `hammer-service`.

use std::ops::{BitOr, BitOrAssign};

/// CPU instruction capabilities relevant to cryptographic implementations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InstructionSet(u16);

impl InstructionSet {
    /// No optional cryptographic instructions.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// AES round instructions.
    pub const AES: Self = Self(1 << 0);
    /// Carry-less polynomial multiplication instructions used by GCM.
    pub const POLYNOMIAL_MULTIPLY: Self = Self(1 << 1);
    /// SHA-256 compression instructions.
    pub const SHA2: Self = Self(1 << 2);
    /// AVX2 vector instructions.
    pub const AVX2: Self = Self(1 << 3);
    /// Arm Advanced SIMD instructions.
    pub const NEON: Self = Self(1 << 4);

    /// Detects instruction capabilities on the current CPU.
    pub fn detect() -> Self {
        let mut instructions = Self::empty();

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if std::arch::is_x86_feature_detected!("aes") {
                instructions |= Self::AES;
            }
            if std::arch::is_x86_feature_detected!("pclmulqdq") {
                instructions |= Self::POLYNOMIAL_MULTIPLY;
            }
            if std::arch::is_x86_feature_detected!("sha") {
                instructions |= Self::SHA2;
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                instructions |= Self::AVX2;
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("aes") {
                instructions |= Self::AES;
            }
            if std::arch::is_aarch64_feature_detected!("pmull") {
                instructions |= Self::POLYNOMIAL_MULTIPLY;
            }
            if std::arch::is_aarch64_feature_detected!("sha2") {
                instructions |= Self::SHA2;
            }
            if std::arch::is_aarch64_feature_detected!("neon") {
                instructions |= Self::NEON;
            }
        }

        instructions
    }

    /// Returns whether every required instruction is present.
    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

impl BitOr for InstructionSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for InstructionSet {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

pub mod aead;
pub mod hash;
pub mod kdf;
pub mod key_establishment;
pub mod mac;
pub mod signature;
