//! IP-plugin configuration read from `[network]`.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use hammer_runtime::{RuntimeError, RuntimeResult};
use ipnet::IpNet;

const REASSEMBLY_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_REASSEMBLIES: usize = 1_024;
const MAX_FRAGMENTS_PER_REASSEMBLY: usize = 64;
/// The IP owner reads its IP and route fields from `[network]`. Other fields
/// in that table belong to service owners and are intentionally ignored here.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct NetworkIpConfig {
    pub ip: IpConfig,
    pub route: Vec<Route>,
}

impl NetworkIpConfig {
    pub fn validate(&self) -> RuntimeResult<()> {
        self.ip.validate()?;
        validate_routes(&self.route)
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub prefix: IpNet,
    #[serde(default)]
    pub drop: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<Via>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub interface: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Via {
    One(IpAddr),
    Many(Vec<IpAddr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteAction {
    Drop,
    Adjacency {
        via: Option<IpAddr>,
        interface: String,
    },
    LoadBalance {
        via: Vec<IpAddr>,
        interface: String,
    },
}

impl Route {
    pub fn action(&self) -> RuntimeResult<RouteAction> {
        if self.drop {
            if self.via.is_some() || !self.interface.is_empty() {
                return Err(RuntimeError::config_validation(format!(
                    "network.route[{}] is `drop` and must not specify `via` or `interface`",
                    self.prefix
                )));
            }
            return Ok(RouteAction::Drop);
        }
        if self.interface.is_empty() {
            return Err(RuntimeError::config_validation(format!(
                "network.route[{}] requires `interface` (or `drop = true`)",
                self.prefix
            )));
        }
        match &self.via {
            None => Ok(RouteAction::Adjacency {
                via: None,
                interface: self.interface.clone(),
            }),
            Some(Via::One(address)) => Ok(RouteAction::Adjacency {
                via: Some(*address),
                interface: self.interface.clone(),
            }),
            Some(Via::Many(addresses)) if addresses.is_empty() => {
                Err(RuntimeError::config_validation(format!(
                    "network.route[{}] `via` list must not be empty",
                    self.prefix
                )))
            }
            Some(Via::Many(addresses)) if addresses.len() == 1 => Ok(RouteAction::Adjacency {
                via: Some(addresses[0]),
                interface: self.interface.clone(),
            }),
            Some(Via::Many(addresses)) => Ok(RouteAction::LoadBalance {
                via: addresses.clone(),
                interface: self.interface.clone(),
            }),
        }
    }
}

pub fn validate_routes(routes: &[Route]) -> RuntimeResult<()> {
    let mut prefixes = std::collections::HashSet::new();
    for route in routes {
        if !prefixes.insert(route.prefix) {
            return Err(RuntimeError::config_validation(format!(
                "duplicate network.route prefix: {}",
                route.prefix
            )));
        }
        route.action()?;
    }
    Ok(())
}
