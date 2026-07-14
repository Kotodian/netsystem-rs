//! `[network.route]` config: FIB/DPO route declarations. Single-layer schema.
//!
//! Routes describe a prefix and the DPO (data-path object) to apply to matching
//! packets. This is the declarative config layer; the runtime resolves each
//! route into a concrete `Dpo` (drop / adjacency / load-balance) in the FIB.
//!
//! Schema mirrors VPP `ip route add <prefix> via <nh> <iface> [drop]`:
//!
//! ```toml
//! [[network.route]]
//! prefix = "0.0.0.0/0"
//! via = "10.0.0.2"          # single next-hop → adjacency DPO
//! interface = "tun0"
//!
//! [[network.route]]
//! prefix = "192.168.0.0/16"
//! drop = true               # → drop DPO
//!
//! [[network.route]]
//! prefix = "172.16.0.0/12"
//! via = ["10.0.0.2", "10.0.0.3"]  # multiple next-hops → load-balance DPO
//! interface = "tun0"
//! ```

use std::net::IpAddr;

use ipnet::IpNet;

use crate::error::{HammerError, HammerResult};
use hammer_infra::vec::Vec;

/// A single FIB entry: a prefix and the DPO action for matching packets.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// Destination prefix, e.g. `0.0.0.0/0` or `2001:db8::/32`.
    pub prefix: IpNet,
    /// Explicitly drop matching packets. Mutually exclusive with `via`.
    #[serde(default)]
    pub drop: bool,
    /// Next-hop address(es). A single address resolves to an adjacency DPO;
    /// multiple addresses resolve to a load-balance (ECMP) DPO. Omit (with
    /// `interface` only) for a glean adjacency on a directly-connected prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<Via>,
    /// Egress interface name. Required when `via` is present; optional for
    /// `drop` routes.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub interface: String,
}

/// One or more next-hop addresses for a route's `via` field.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Via {
    /// Single next-hop → adjacency DPO.
    One(IpAddr),
    /// Multiple next-hops → load-balance (ECMP) DPO.
    Many(Vec<IpAddr>),
}

/// Resolved DPO action for a route, produced by [`Route::action`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteAction {
    Drop,
    /// Forward out `interface` to a single next-hop (or glean if `via` is None).
    Adjacency {
        via: Option<IpAddr>,
        interface: String,
    },
    /// Load-balance across multiple next-hops out of `interface`.
    LoadBalance {
        via: Vec<IpAddr>,
        interface: String,
    },
}

impl Route {
    /// Resolve this route's declared fields into a DPO action, enforcing the
    /// mutual-exclusion invariants serde cannot express.
    pub fn action(&self) -> HammerResult<RouteAction> {
        if self.drop {
            if self.via.is_some() || !self.interface.is_empty() {
                return Err(HammerError::config_validation(format!(
                    "network.route[{}] is `drop` and must not specify `via` or `interface`",
                    self.prefix
                )));
            }
            return Ok(RouteAction::Drop);
        }
        if self.interface.is_empty() {
            return Err(HammerError::config_validation(format!(
                "network.route[{}] requires `interface` (or `drop = true`)",
                self.prefix
            )));
        }
        match &self.via {
            None => Ok(RouteAction::Adjacency {
                via: None,
                interface: self.interface.clone(),
            }),
            Some(Via::One(addr)) => Ok(RouteAction::Adjacency {
                via: Some(*addr),
                interface: self.interface.clone(),
            }),
            Some(Via::Many(addrs)) => {
                if addrs.is_empty() {
                    return Err(HammerError::config_validation(format!(
                        "network.route[{}] `via` list must not be empty",
                        self.prefix
                    )));
                }
                if addrs.len() == 1 {
                    return Ok(RouteAction::Adjacency {
                        via: Some(addrs[0]),
                        interface: self.interface.clone(),
                    });
                }
                Ok(RouteAction::LoadBalance {
                    via: addrs.clone(),
                    interface: self.interface.clone(),
                })
            }
        }
    }
}

/// Validate a set of routes: each route's fields must resolve to a DPO action,
/// and prefixes must be unique (the FIB holds one entry per prefix).
pub fn validate_routes(routes: &[Route]) -> HammerResult<()> {
    let mut seen = std::collections::HashSet::new();
    for route in routes {
        if !seen.insert(route.prefix) {
            return Err(HammerError::config_validation(format!(
                "duplicate network.route prefix: {}",
                route.prefix
            )));
        }
        route.action()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn drop_route_resolves_to_drop() {
        let route: Route = toml::from_str(
            r#"
            prefix = "192.168.0.0/16"
            drop = true
            "#,
        )
        .expect("parse");
        assert_eq!(route.action().unwrap(), RouteAction::Drop);
    }

    #[test]
    fn drop_route_rejects_via() {
        let route: Route = toml::from_str(
            r#"
            prefix = "192.168.0.0/16"
            drop = true
            via = "10.0.0.2"
            "#,
        )
        .expect("parse");
        let err = route.action().expect_err("reject");
        assert!(err.to_string().contains("is `drop`"));
    }

    #[test]
    fn single_via_resolves_to_adjacency() {
        let route: Route = toml::from_str(
            r#"
            prefix = "0.0.0.0/0"
            via = "10.0.0.2"
            interface = "tun0"
            "#,
        )
        .expect("parse");
        assert_eq!(
            route.action().unwrap(),
            RouteAction::Adjacency {
                via: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
                interface: "tun0".to_owned()
            }
        );
    }

    #[test]
    fn many_via_resolves_to_load_balance() {
        let route: Route = toml::from_str(
            r#"
            prefix = "172.16.0.0/12"
            via = ["10.0.0.2", "10.0.0.3"]
            interface = "tun0"
            "#,
        )
        .expect("parse");
        assert_eq!(
            route.action().unwrap(),
            RouteAction::LoadBalance {
                via: hammer_infra::vec![
                    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3))
                ],
                interface: "tun0".to_owned()
            }
        );
    }

    #[test]
    fn single_element_via_list_collapses_to_adjacency() {
        let route: Route = toml::from_str(
            r#"
            prefix = "10.0.0.0/24"
            via = ["10.0.0.2"]
            interface = "tun0"
            "#,
        )
        .expect("parse");
        assert!(matches!(
            route.action().unwrap(),
            RouteAction::Adjacency { .. }
        ));
    }

    #[test]
    fn glean_adjacency_without_via() {
        let route: Route = toml::from_str(
            r#"
            prefix = "10.0.0.0/24"
            interface = "tun0"
            "#,
        )
        .expect("parse");
        assert_eq!(
            route.action().unwrap(),
            RouteAction::Adjacency {
                via: None,
                interface: "tun0".to_owned()
            }
        );
    }

    #[test]
    fn route_without_interface_or_drop_rejected() {
        let route: Route = toml::from_str(
            r#"
            prefix = "10.0.0.0/24"
            "#,
        )
        .expect("parse");
        let err = route.action().expect_err("reject");
        assert!(err.to_string().contains("requires `interface`"));
    }

    #[test]
    fn validate_routes_rejects_duplicate_prefix() {
        let routes: Vec<Route> = toml::from_str(
            r#"
            [[route]]
            prefix = "10.0.0.0/24"
            interface = "tun0"
            [[route]]
            prefix = "10.0.0.0/24"
            interface = "tun0"
            "#,
        )
        .map(|r: Routes| r.route)
        .expect("parse");
        let err = validate_routes(&routes).expect_err("reject");
        assert!(err.to_string().contains("duplicate network.route prefix"));
    }

    #[derive(serde::Deserialize)]
    struct Routes {
        route: Vec<Route>,
    }
}
