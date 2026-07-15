//! `[memory]` config for the VPP-shaped fixed-capacity main heap.

use serde::{Deserialize, de::Error as _};

use crate::error::{HammerError, HammerResult};

pub const DEFAULT_MAIN_HEAP_SIZE: usize = hammer_infra::main_heap::DEFAULT_MAIN_HEAP_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Memory {
    #[serde(deserialize_with = "deserialize_memory_size")]
    pub main_heap_size: usize,
}

impl Default for Memory {
    fn default() -> Self {
        Self {
            main_heap_size: DEFAULT_MAIN_HEAP_SIZE,
        }
    }
}

impl Memory {
    pub fn validate(&self) -> HammerResult<()> {
        let minimum = hammer_infra::main_heap::minimum_capacity();
        if self.main_heap_size < minimum {
            return Err(HammerError::config_validation(format!(
                "memory.main_heap_size must be at least {} bytes",
                minimum
            )));
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum MemorySize {
    Bytes(usize),
    Text(String),
}

fn deserialize_memory_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match MemorySize::deserialize(deserializer)? {
        MemorySize::Bytes(bytes) => Ok(bytes),
        MemorySize::Text(text) => parse_memory_size(&text).map_err(D::Error::custom),
    }
}

fn parse_memory_size(text: &str) -> Result<usize, String> {
    let trimmed = text.trim();
    let suffix_index = trimmed
        .find(|character: char| !character.is_ascii_digit() && character != '_')
        .unwrap_or(trimmed.len());
    let number = trimmed[..suffix_index]
        .replace('_', "")
        .parse::<usize>()
        .map_err(|error| format!("invalid memory size `{text}`: {error}"))?;
    let suffix = trimmed[suffix_index..].trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1usize << 10,
        "m" | "mb" | "mib" => 1usize << 20,
        "g" | "gb" | "gib" => 1usize << 30,
        "t" | "tb" | "tib" => 1usize << 40,
        _ => return Err(format!("unsupported memory size suffix in `{text}`")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("memory size `{text}` overflows usize"))
}
