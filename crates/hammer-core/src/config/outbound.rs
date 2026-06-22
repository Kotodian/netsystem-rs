//! `[[outbounds]]` config sections.
//!
//! The current VPP-aligned surface keeps only runtime outbounds that are
//! still part of the live dataplane architecture.

use serde::{Deserialize, Serialize};

use crate::error::HammerError;

use super::constants as C;
use super::raw_struct;

raw_struct! {
    pub struct RawBlockOutboundConfig {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawOutbound {
    Block(RawBlockOutboundConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    pub id: String,
    pub kind: OutboundKind,
}

impl Outbound {
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            OutboundKind::Block => C::TYPE_BLOCK,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundKind {
    Block,
}

pub(super) fn build_declared_outbounds(
    raw: Vec<RawOutbound>,
) -> Result<Vec<Outbound>, HammerError> {
    let mut outbounds = Vec::with_capacity(raw.len());
    for (idx, raw) in raw.into_iter().enumerate() {
        let outbound = match raw {
            RawOutbound::Block(raw) => {
                if raw.id.is_empty() {
                    return Err(HammerError::config_validation(format!(
                        "outbounds[{idx}].id is required"
                    )));
                }
                Outbound {
                    id: raw.id,
                    kind: OutboundKind::Block,
                }
            }
        };
        outbounds.push(outbound);
    }
    Ok(outbounds)
}
