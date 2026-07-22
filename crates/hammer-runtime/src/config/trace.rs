//! `[trace]` config section: declarative packet trace policy. Single-layer schema.

use crate::error::{RuntimeError, RuntimeResult};

pub const DEFAULT_TRACE_RECORD_CAPACITY: usize = 1024;
pub const DEFAULT_TRACE_PACKET_CAPACITY: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Trace {
    /// Whether packet tracing is enabled.
    pub enabled: bool,
    /// Maximum completed records kept by the control plane.
    pub record_capacity: usize,
    /// Maximum in-flight traced packets kept by trace control.
    pub packet_capacity: usize,
    /// Input node quotas. Empty means no packets are sampled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<TraceInput>,
}

impl Default for Trace {
    fn default() -> Self {
        Self {
            enabled: false,
            record_capacity: DEFAULT_TRACE_RECORD_CAPACITY,
            packet_capacity: DEFAULT_TRACE_PACKET_CAPACITY,
            inputs: Vec::new(),
        }
    }
}

impl Trace {
    pub fn is_default(&self) -> bool {
        *self == Trace::default()
    }

    pub fn validate(&self) -> RuntimeResult<()> {
        if self.enabled && self.record_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "trace.record_capacity must be non-zero when trace is enabled",
            ));
        }
        if self.enabled && self.packet_capacity == 0 {
            return Err(RuntimeError::config_validation(
                "trace.packet_capacity must be non-zero when trace is enabled",
            ));
        }
        for input in &self.inputs {
            if input.node.is_empty() {
                return Err(RuntimeError::config_validation(
                    "trace.inputs node must not be empty",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraceInput {
    pub node: String,
    #[serde(default)]
    pub count: u32,
}
