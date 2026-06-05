//! `[trace]` config section: declarative packet trace policy.

use crate::error::{HammerError, HammerResult};

use super::raw_struct_with_default_check;

pub const DEFAULT_TRACE_RECORD_CAPACITY: usize = 1024;
pub const DEFAULT_TRACE_PACKET_CAPACITY: usize = 256;

raw_struct_with_default_check! {
    pub struct RawTraceConfig {
        /// Whether packet tracing is enabled.
        pub enabled: Option<bool> => "Option::is_none",
        /// Maximum completed records kept by the control plane.
        pub record_capacity: Option<usize> => "Option::is_none",
        /// Maximum in-flight traced packets kept by trace control.
        pub packet_capacity: Option<usize> => "Option::is_none",
        /// Input node quotas. Empty means no packets are sampled.
        pub inputs: Vec<RawTraceInputConfig> => "Vec::is_empty",
    }
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawTraceInputConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node: String,
    #[serde(default)]
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceOptions {
    pub enabled: bool,
    pub record_capacity: usize,
    pub packet_capacity: usize,
    pub inputs: Vec<TraceInputOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceInputOptions {
    pub node: String,
    pub count: u32,
}

impl Default for TraceOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            record_capacity: DEFAULT_TRACE_RECORD_CAPACITY,
            packet_capacity: DEFAULT_TRACE_PACKET_CAPACITY,
            inputs: Vec::new(),
        }
    }
}

pub(super) fn build_trace_options(raw: RawTraceConfig) -> HammerResult<TraceOptions> {
    let enabled = raw.enabled.unwrap_or(false);
    let record_capacity = raw.record_capacity.unwrap_or(DEFAULT_TRACE_RECORD_CAPACITY);
    let packet_capacity = raw.packet_capacity.unwrap_or(DEFAULT_TRACE_PACKET_CAPACITY);
    if enabled && record_capacity == 0 {
        return Err(HammerError::config_validation(
            "trace.record_capacity must be non-zero when trace is enabled",
        ));
    }
    if enabled && packet_capacity == 0 {
        return Err(HammerError::config_validation(
            "trace.packet_capacity must be non-zero when trace is enabled",
        ));
    }

    let mut inputs = Vec::with_capacity(raw.inputs.len());
    for input in raw.inputs {
        if input.node.is_empty() {
            return Err(HammerError::config_validation(
                "trace.inputs node must not be empty",
            ));
        }
        inputs.push(TraceInputOptions {
            node: input.node,
            count: input.count,
        });
    }

    Ok(TraceOptions {
        enabled,
        record_capacity,
        packet_capacity,
        inputs,
    })
}
