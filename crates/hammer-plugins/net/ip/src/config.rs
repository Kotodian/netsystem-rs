//! IP-plugin configuration read from `[network]`.

use std::time::Duration;

use hammer_runtime::{RuntimeError, RuntimeResult};

const REASSEMBLY_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_REASSEMBLIES: usize = 1_024;
const MAX_FRAGMENTS_PER_REASSEMBLY: usize = 64;
/// The IP owner reads its IP and route fields from `[network]`. Other fields
/// in that table belong to service owners and are intentionally ignored here.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct NetworkIpConfig {
    pub ip: IpConfig,
}

impl NetworkIpConfig {
    pub fn validate(&self) -> RuntimeResult<()> {
        self.ip.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct IpConfig {
    #[serde(default)]
    pub reassembly: ReassemblyConfig,
}

impl Default for IpConfig {
    fn default() -> Self {
        Self {
            reassembly: ReassemblyConfig::default(),
        }
    }
}

impl IpConfig {
    fn validate(&self) -> RuntimeResult<()> {
        self.reassembly.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReassemblyConfig {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub max_reassemblies: usize,
    pub max_fragments_per_reassembly: usize,
}

impl Default for ReassemblyConfig {
    fn default() -> Self {
        Self {
            timeout: REASSEMBLY_TIMEOUT,
            max_reassemblies: MAX_REASSEMBLIES,
            max_fragments_per_reassembly: MAX_FRAGMENTS_PER_REASSEMBLY,
        }
    }
}

impl ReassemblyConfig {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.timeout.is_zero() {
            return Err(RuntimeError::config_validation(
                "network.ip.reassembly.timeout must be non-zero",
            ));
        }
        if self.max_reassemblies == 0 {
            return Err(RuntimeError::config_validation(
                "network.ip.reassembly.max_reassemblies must be non-zero",
            ));
        }
        if self.max_fragments_per_reassembly == 0 {
            return Err(RuntimeError::config_validation(
                "network.ip.reassembly.max_fragments_per_reassembly must be non-zero",
            ));
        }
        Ok(())
    }
}
