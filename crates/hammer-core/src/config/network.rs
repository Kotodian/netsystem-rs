//! `[network]` config section: shared IP, session, and interface parameters.
//!
//! Single-layer schema: each struct directly deserializes from TOML. Container
//! `#[serde(default)]` fills missing fields from each struct's `Default` impl
//! (which carries the production constants below); `#[serde(with =
//! "humantime_serde")]` handles `Duration` parsing. A `validate()` pass
//! enforces non-zero / ordering invariants after parse.

// Default impls below carry production constants (not zero values), so they
// cannot be replaced by `#[derive(Default)]`.
#![allow(clippy::derivable_impls)]
//!
//! Defaults are derived from the production constants in `hammer-service`:
//! - IP reassembly: `net/ip/reassembly.rs::{DEFAULT_REASSEMBLY_TIMEOUT,
//!   DEFAULT_MAX_REASSEMBLIES,DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY}`.
//! - Session: `session/runtime.rs::{DEFAULT_SESSION_TIMER_TICK,
//!   DEFAULT_SESSION_POOL_CAPACITY,DEFAULT_OOO_CAPACITY}`,
//!   `session/ready.rs::DEFAULT_READY_QUEUE_CAPACITY`,
//!   `session/app.rs::{DEFAULT_APP_SESSION_CAPACITY,DataPlaneBufferConfig defaults}`.
//! - Interface: `interface.rs::InterfaceMtu`, `constants::DEFAULT_TUN_MTU`.

use std::time::Duration;

use ipnet::IpNet;

use crate::error::{HammerError, HammerResult};

use super::constants as C;
use super::route::{Route, validate_routes};
use hammer_infra::vec::Vec;

// IP reassembly defaults (net/ip/reassembly.rs).
const REASSEMBLY_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_REASSEMBLIES: usize = 1_024;
const MAX_FRAGMENTS_PER_REASSEMBLY: usize = 64;

// Session defaults (session/runtime.rs / ready.rs / app.rs).
const SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const SESSION_POOL_CAPACITY: usize = 1_024;
const READY_QUEUE_CAPACITY: usize = 1_024;
const APP_SESSION_CAPACITY: usize = 1_024;
const OOO_CAPACITY: usize = 8;
const SESSION_BUFFER_SLOT_BYTES: usize = 2_048;
const SESSION_BUFFER_SLOTS: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Network {
    pub ip: Ip,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<Session>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interface: Vec<Interface>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route: Vec<Route>,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            ip: Ip::default(),
            session: None,
            interface: Vec::new(),
            route: Vec::new(),
        }
    }
}

impl Network {
    pub fn is_default(&self) -> bool {
        *self == Network::default()
    }

    pub fn validate(&self) -> HammerResult<()> {
        self.ip.validate()?;
        if let Some(session) = &self.session {
            session.validate()?;
        }
        self.validate_interfaces()?;
        validate_routes(&self.route)?;
        Ok(())
    }

    fn validate_interfaces(&self) -> HammerResult<()> {
        let mut seen = std::collections::HashSet::new();
        for iface in &self.interface {
            if iface.name.is_empty() {
                return Err(HammerError::config_validation(
                    "network.interface.name must be non-empty",
                ));
            }
            if !seen.insert(iface.name.as_str()) {
                return Err(HammerError::config_validation(format!(
                    "duplicate network.interface name: {}",
                    iface.name
                )));
            }
            if iface.mtu.l3 == 0 || iface.mtu.ip4 == 0 || iface.mtu.ip6 == 0 || iface.mtu.mpls == 0
            {
                return Err(HammerError::config_validation(
                    "network.interface.mtu values must be non-zero",
                ));
            }
        }
        Ok(())
    }

    /// Auto-derived FIB entries from interface addresses (VPP
    /// adjacency-source semantics). For each `address = "A.B.C.D/N"` on an
    /// interface, derive:
    /// - the containing subnet `A.B.C.0/N` → glean adjacency out of the
    ///   interface (directly-connected network),
    /// - the host address `A.B.C.D/32` (or `/128`) → local receive.
    ///
    /// Explicit `[[network.route]]` entries take precedence over these: the
    /// runtime installs user routes with a higher source priority, so a
    /// derived entry is only the fallback when no user route claims the same
    /// prefix. This method yields the derived routes; precedence is applied
    /// at FIB install time, not here.
    pub fn derived_routes(&self) -> Vec<super::route::Route> {
        let mut out = Vec::new();
        for iface in &self.interface {
            for cidr in &iface.address {
                let network = network_prefix(*cidr);
                let host = host_prefix(*cidr);
                out.push(super::route::Route {
                    prefix: network,
                    drop: false,
                    via: None,
                    interface: iface.name.clone(),
                });
                out.push(super::route::Route {
                    prefix: host,
                    drop: false,
                    via: None,
                    interface: iface.name.clone(),
                });
            }
        }
        out
    }
}

/// The network (subnet) prefix of a CIDR, e.g. `10.0.0.1/24` → `10.0.0.0/24`.
fn network_prefix(cidr: IpNet) -> IpNet {
    cidr.trunc()
}

/// The host address as a `/32` (IPv4) or `/128` (IPv6) prefix.
fn host_prefix(cidr: IpNet) -> IpNet {
    match cidr {
        IpNet::V4(v4) => IpNet::V4(ipnet::Ipv4Net::new(v4.addr(), 32).expect("/32 is valid")),
        IpNet::V6(v6) => IpNet::V6(ipnet::Ipv6Net::new(v6.addr(), 128).expect("/128 is valid")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionBackend {
    Local,
    #[serde(rename = "svm")]
    Svm,
}

impl Default for SessionBackend {
    fn default() -> Self {
        Self::Local
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Ip {
    #[serde(default)]
    pub reassembly: Reassembly,
}

impl Default for Ip {
    fn default() -> Self {
        Self {
            reassembly: Reassembly::default(),
        }
    }
}

impl Ip {
    fn validate(&self) -> HammerResult<()> {
        self.reassembly.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Reassembly {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub max_reassemblies: usize,
    pub max_fragments_per_reassembly: usize,
}

impl Default for Reassembly {
    fn default() -> Self {
        Self {
            timeout: REASSEMBLY_TIMEOUT,
            max_reassemblies: MAX_REASSEMBLIES,
            max_fragments_per_reassembly: MAX_FRAGMENTS_PER_REASSEMBLY,
        }
    }
}

impl Reassembly {
    fn validate(&self) -> HammerResult<()> {
        if self.timeout.is_zero() {
            return Err(HammerError::config_validation(
                "network.ip.reassembly.timeout must be non-zero",
            ));
        }
        if self.max_reassemblies == 0 {
            return Err(HammerError::config_validation(
                "network.ip.reassembly.max_reassemblies must be non-zero",
            ));
        }
        if self.max_fragments_per_reassembly == 0 {
            return Err(HammerError::config_validation(
                "network.ip.reassembly.max_fragments_per_reassembly must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Session {
    #[serde(default)]
    pub backend: SessionBackend,
    #[serde(default)]
    pub attach_socket_path: Option<String>,
    #[serde(with = "humantime_serde")]
    pub timer_tick: Duration,
    /// VPP `session { preallocated-sessions }` alias: `preallocated_sessions`.
    #[serde(alias = "preallocated_sessions")]
    pub pool_capacity: usize,
    /// VPP `session { event-queue-length }` alias: `event_queue_length`.
    #[serde(alias = "event_queue_length")]
    pub ready_queue_capacity: usize,
    pub app_session_capacity: usize,
    pub ooo_capacity: usize,
    pub buffer: SessionBuffer,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            backend: SessionBackend::default(),
            attach_socket_path: None,
            timer_tick: SESSION_TIMER_TICK,
            pool_capacity: SESSION_POOL_CAPACITY,
            ready_queue_capacity: READY_QUEUE_CAPACITY,
            app_session_capacity: APP_SESSION_CAPACITY,
            ooo_capacity: OOO_CAPACITY,
            buffer: SessionBuffer::default(),
        }
    }
}

impl Session {
    fn validate(&self) -> HammerResult<()> {
        if self.timer_tick.is_zero() {
            return Err(HammerError::config_validation(
                "network.session.timer_tick must be non-zero",
            ));
        }
        if self.pool_capacity == 0 {
            return Err(HammerError::config_validation(
                "network.session.pool_capacity must be non-zero",
            ));
        }
        if self.ready_queue_capacity == 0 {
            return Err(HammerError::config_validation(
                "network.session.ready_queue_capacity must be non-zero",
            ));
        }
        if self.app_session_capacity == 0 {
            return Err(HammerError::config_validation(
                "network.session.app_session_capacity must be non-zero",
            ));
        }
        self.buffer.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SessionBuffer {
    pub slot_bytes: usize,
    pub slots: usize,
}

impl Default for SessionBuffer {
    fn default() -> Self {
        Self {
            slot_bytes: SESSION_BUFFER_SLOT_BYTES,
            slots: SESSION_BUFFER_SLOTS,
        }
    }
}

impl SessionBuffer {
    fn validate(&self) -> HammerResult<()> {
        if self.slot_bytes == 0 {
            return Err(HammerError::config_validation(
                "network.session.buffer.slot_bytes must be non-zero",
            ));
        }
        if self.slots == 0 {
            return Err(HammerError::config_validation(
                "network.session.buffer.slots must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Interface {
    pub name: String,
    /// Local interface addresses in CIDR form, e.g. `10.0.0.1/24`.
    /// Each address auto-derives two FIB entries (VPP adjacency-source
    /// semantics): the containing subnet → glean adjacency out of this
    /// interface, and the host address → local receive. Explicit
    /// `[[network.route]]` entries override the auto-derived ones.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<IpNet>,
    #[serde(default = "InterfaceMtu::default")]
    pub mtu: InterfaceMtu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct InterfaceMtu {
    pub l3: u32,
    pub ip4: u32,
    pub ip6: u32,
    pub mpls: u32,
}

impl Default for InterfaceMtu {
    fn default() -> Self {
        Self {
            l3: C::DEFAULT_TUN_MTU,
            ip4: C::DEFAULT_TUN_MTU,
            ip6: C::DEFAULT_TUN_MTU,
            mpls: C::DEFAULT_TUN_MTU,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_defaults_match_production_constants() {
        let network = Network::default();
        assert_eq!(network.ip.reassembly.timeout, REASSEMBLY_TIMEOUT);
        assert_eq!(network.ip.reassembly.max_reassemblies, MAX_REASSEMBLIES);
        assert_eq!(
            network.ip.reassembly.max_fragments_per_reassembly,
            MAX_FRAGMENTS_PER_REASSEMBLY
        );
        assert!(network.session.is_none());
        let session = Session::default();
        assert_eq!(session.timer_tick, SESSION_TIMER_TICK);
        assert_eq!(session.pool_capacity, SESSION_POOL_CAPACITY);
        assert_eq!(session.ready_queue_capacity, READY_QUEUE_CAPACITY);
        assert_eq!(session.app_session_capacity, APP_SESSION_CAPACITY);
        assert_eq!(session.ooo_capacity, OOO_CAPACITY);
        assert_eq!(session.buffer.slot_bytes, SESSION_BUFFER_SLOT_BYTES);
        assert_eq!(session.buffer.slots, SESSION_BUFFER_SLOTS);
        assert_eq!(network.interface, Vec::new());
    }

    /// `Network` mirrors the `[network]` table shape, so tests feed the inner
    /// sections directly (no `[network]` wrapper) to `toml::from_str`.
    fn parse_inner(input: &str) -> Network {
        toml::from_str(input).expect("parse Network")
    }

    #[test]
    fn parse_network_full() {
        let input = r#"
            [ip.reassembly]
            timeout = "200ms"
            max_reassemblies = 512
            max_fragments_per_reassembly = 32

            [session]
            timer_tick = "5ms"
            pool_capacity = 2048
            ready_queue_capacity = 2048
            app_session_capacity = 2048
            ooo_capacity = 16
            [session.buffer]
            slot_bytes = 4096
            slots = 4

            [[interface]]
            name = "tun0"
            [interface.mtu]
            l3 = 1500
            ip4 = 1500
            ip6 = 1500
            mpls = 1500
        "#;
        let network = parse_inner(input);
        network.validate().expect("build");
        assert_eq!(network.ip.reassembly.max_reassemblies, 512);
        let session = network.session.as_ref().expect("configured session");
        assert_eq!(session.timer_tick, Duration::from_millis(5));
        assert_eq!(session.buffer.slot_bytes, 4096);
        assert_eq!(session.buffer.slots, 4);
        assert_eq!(network.interface.len(), 1);
        assert_eq!(network.interface[0].name, "tun0");
        assert_eq!(network.interface[0].mtu.l3, 1500);
    }

    #[test]
    fn parse_network_partial_fill_uses_defaults() {
        let network = parse_inner("[ip.reassembly]\nmax_reassemblies = 512\n");
        network.validate().expect("build");
        assert_eq!(network.ip.reassembly.max_reassemblies, 512);
        assert_eq!(
            network.ip.reassembly.max_fragments_per_reassembly,
            MAX_FRAGMENTS_PER_REASSEMBLY
        );
        assert!(network.session.is_none());
    }

    #[test]
    fn parse_network_uses_defaults_when_empty() {
        let network = parse_inner("");
        network.validate().expect("build");
        assert_eq!(network, Network::default());
    }

    #[test]
    fn parse_network_rejects_unknown_interface_field() {
        let err = toml::from_str::<Network>(
            r#"
            [[interface]]
            name = "tun0"
            driver = "wireguard"
            "#,
        )
        .expect_err("parse should reject unknown interface fields");
        assert!(err.to_string().contains("driver") || err.to_string().contains("unknown"));
    }

    #[test]
    fn parse_network_rejects_duplicate_interface_name() {
        let input = r#"
            [[interface]]
            name = "tun0"
            [[interface]]
            name = "tun0"
        "#;
        let err = parse_inner(input).validate().expect_err("reject");
        assert!(err.to_string().contains("duplicate network.interface name"));
    }

    #[test]
    fn session_config_parses_attach_socket_path() {
        let toml = r#"
            backend = "svm"
            attach_socket_path = "/tmp/hammer-attach.sock"
        "#;
        let config: super::Session = toml::from_str(toml).unwrap();
        assert_eq!(config.backend, super::SessionBackend::Svm);
        assert_eq!(
            config.attach_socket_path.as_deref(),
            Some("/tmp/hammer-attach.sock")
        );
    }

    #[test]
    fn parse_network_rejects_empty_interface_name() {
        let input = r#"
            [[interface]]
            name = ""
        "#;
        let err = parse_inner(input).validate().expect_err("reject");
        assert!(
            err.to_string()
                .contains("network.interface.name must be non-empty")
        );
    }
}
