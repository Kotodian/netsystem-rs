//! `[[endpoints]]` config sections.
//!
//! Mirrors sing-box 1.11+'s endpoint concept: an outbound that also has
//! Lifecycle state. Adding a new endpoint protocol drops in another
//! `RawEndpoint::*` variant behind its own sub-feature without breaking
//! existing TOML files. The generic endpoint domain is gated on
//! `feature = "endpoint"` from mod.rs; concrete endpoint protocols add their
//! own item-level `#[cfg]` annotations.

#[cfg(feature = "wireguard")]
use std::net::{IpAddr, SocketAddr};
#[cfg(feature = "wireguard")]
use std::time::Duration;

#[cfg(feature = "wireguard")]
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
#[cfg(feature = "wireguard")]
use serde_with::{As, base64::Base64};

use crate::error::HammerError;

#[cfg(feature = "wireguard")]
use super::constants as C;
#[cfg(feature = "wireguard")]
use super::raw_struct;

/// Outer endpoint variant — sing-box style `[[endpoints]]` entries with a
/// `type` discriminator. Adding a new endpoint protocol (e.g. tailscale)
/// means adding a new variant here without breaking existing TOML files.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawEndpoint {
    /// WireGuard endpoint entry.
    #[cfg(feature = "wireguard")]
    Wireguard(RawWireguardEndpoint),
}

#[cfg(feature = "wireguard")]
raw_struct! {
    pub struct RawWireguardEndpoint {
        /// Endpoint id used by route rules and lifecycle managers.
        pub id: String => "String::is_empty",
        /// Base64-encoded WireGuard private key.
        pub private_key: Option<RawWireguardKey> => "Option::is_none",
        /// Optional UDP listen port.
        pub listen_port: Option<u16> => "Option::is_none",
        /// Optional WireGuard interface MTU.
        pub mtu: Option<u32> => "Option::is_none",
        /// Local WireGuard interface addresses in CIDR form.
        pub address: Vec<IpNet> => "Vec::is_empty",
        /// WireGuard peer list.
        pub peers: Vec<RawWireguardPeer> => "Vec::is_empty",
    }
}

#[cfg(feature = "wireguard")]
raw_struct! {
    pub struct RawWireguardPeer {
        /// Base64-encoded peer public key.
        pub public_key: Option<RawWireguardKey> => "Option::is_none",
        /// Optional base64-encoded pre-shared key.
        pub pre_shared_key: Option<RawWireguardKey> => "Option::is_none",
        /// Peer endpoint address; currently must be an IP literal.
        pub address: String => "String::is_empty",
        /// Peer endpoint UDP port.
        pub port: Option<u16> => "Option::is_none",
        /// Allowed IP prefixes routed to this peer.
        pub allowed_ips: Vec<IpNet> => "Vec::is_empty",
        /// Optional persistent keepalive interval in seconds.
        pub persistent_keepalive_interval: Option<u32> => "Option::is_none",
        /// Optional reserved WARP-style header bytes.
        pub reserved: Option<[u8; 3]> => "Option::is_none",
    }
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct RawWireguardKey(#[serde(with = "As::<Base64>")] [u8; 32]);

#[cfg(feature = "wireguard")]
impl RawWireguardKey {
    fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// `[[endpoints]]` element — protocols that maintain long-lived state and
/// participate in the lifecycle alongside outbounds. Mirrors the sing-box
/// 1.11+ endpoint concept: `Endpoint = Outbound + Lifecycle` (see
/// `crates/hammer-adapter/src/endpoint.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub id: String,
    pub kind: EndpointKind,
}

impl Endpoint {
    pub fn type_name(&self) -> &'static str {
        #[cfg(feature = "wireguard")]
        match &self.kind {
            EndpointKind::Wireguard(_) => C::TYPE_WIREGUARD,
        }
        #[cfg(not(feature = "wireguard"))]
        match &self.kind {
            _ => unreachable!("endpoint feature has no enabled endpoint protocols"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    #[cfg(feature = "wireguard")]
    Wireguard(WireguardEndpointOptions),
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireguardEndpointOptions {
    /// Static private key (Curve25519, 32 bytes).
    pub private_key: [u8; 32],
    /// Local UDP listen port; `0` lets the OS pick.
    pub listen_port: u16,
    /// Tunnel MTU advertised to the inner stack. sing-box default is 1408.
    pub mtu: u32,
    /// Local addresses inside the tunnel (CIDR form).
    pub address: Vec<IpNet>,
    pub peers: Vec<WireguardPeerOptions>,
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireguardPeerOptions {
    /// Peer static public key (Curve25519, 32 bytes).
    pub public_key: [u8; 32],
    /// Optional pre-shared key (32 bytes) for additional symmetric mixing.
    pub pre_shared_key: Option<[u8; 32]>,
    /// Resolved peer endpoint. Hostname-only entries are resolved during
    /// endpoint lifecycle Start, not at config parse time.
    pub endpoint: SocketAddr,
    pub allowed_ips: Vec<IpNet>,
    /// `None` disables persistent keepalive.
    pub persistent_keepalive: Option<Duration>,
    /// First three reserved bytes of every WireGuard packet — non-zero values
    /// are how Cloudflare WARP demuxes traffic per-connection.
    pub reserved: [u8; 3],
}

pub(super) fn build_endpoints(raw: Vec<RawEndpoint>) -> Result<Vec<Endpoint>, HammerError> {
    raw.into_iter()
        .enumerate()
        .map(|(idx, item)| {
            #[cfg(feature = "wireguard")]
            {
                match item {
                    RawEndpoint::Wireguard(wg) => build_wireguard_endpoint(idx, wg),
                }
            }
            #[cfg(not(feature = "wireguard"))]
            {
                let _ = idx;
                match item {}
            }
        })
        .collect()
}

#[cfg(feature = "wireguard")]
fn build_wireguard_endpoint(
    idx: usize,
    mut raw: RawWireguardEndpoint,
) -> Result<Endpoint, HammerError> {
    let id = if raw.id.is_empty() {
        return Err(HammerError::config_validation(format!(
            "endpoints[{idx}].id is required"
        )));
    } else {
        std::mem::take(&mut raw.id)
    };
    let private_key = raw.private_key.ok_or_else(|| {
        HammerError::config_validation(format!("endpoints[{idx}].private_key is required"))
    })?;
    let listen_port = raw.listen_port.unwrap_or(0);
    let mtu = raw.mtu.unwrap_or(C::DEFAULT_WIREGUARD_MTU);
    let address = raw.address;
    if address.is_empty() {
        return Err(HammerError::config_validation(format!(
            "endpoints[{idx}].address is required"
        )));
    }
    if raw.peers.is_empty() {
        return Err(HammerError::config_validation(format!(
            "endpoints[{idx}].peers must contain at least one peer"
        )));
    }
    let peers = raw
        .peers
        .into_iter()
        .enumerate()
        .map(|(peer_idx, peer)| build_wireguard_peer(idx, peer_idx, peer))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Endpoint {
        id,
        kind: EndpointKind::Wireguard(WireguardEndpointOptions {
            private_key: private_key.into_bytes(),
            listen_port,
            mtu,
            address,
            peers,
        }),
    })
}

#[cfg(feature = "wireguard")]
fn build_wireguard_peer(
    endpoint_idx: usize,
    peer_idx: usize,
    raw: RawWireguardPeer,
) -> Result<WireguardPeerOptions, HammerError> {
    let prefix = format!("endpoints[{endpoint_idx}].peers[{peer_idx}]");
    let public_key = raw.public_key.ok_or_else(|| {
        HammerError::config_validation(format!("{prefix}.public_key is required"))
    })?;
    let pre_shared_key = raw.pre_shared_key.map(RawWireguardKey::into_bytes);
    if raw.address.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{prefix}.address is required"
        )));
    }
    let endpoint = parse_socket_addr(
        &format!("{prefix}.address"),
        &raw.address,
        raw.port.unwrap_or(0),
    )?;
    let allowed_ips = raw.allowed_ips;
    if allowed_ips.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{prefix}.allowed_ips is required"
        )));
    }
    let persistent_keepalive = raw
        .persistent_keepalive_interval
        .filter(|secs| *secs > 0)
        .map(|secs| Duration::from_secs(u64::from(secs)));
    let reserved = raw.reserved.unwrap_or([0u8; 3]);
    Ok(WireguardPeerOptions {
        public_key: public_key.into_bytes(),
        pre_shared_key,
        endpoint,
        allowed_ips,
        persistent_keepalive,
        reserved,
    })
}

/// Parse a `host:port`-style endpoint. WireGuard currently requires an IP
/// literal; hostname endpoints would need lifecycle-time DNS resolution
/// which is not wired up yet.
#[cfg(feature = "wireguard")]
fn parse_socket_addr(field: &str, host: &str, port: u16) -> Result<SocketAddr, HammerError> {
    if port == 0 {
        return Err(HammerError::config_validation(format!(
            "{field}: port must be non-zero"
        )));
    }
    let ip: IpAddr = host.parse().map_err(|_| {
        HammerError::config_validation(format!(
            "{field}: peer address must be an IP literal (got {host:?}); hostnames are not supported yet"
        ))
    })?;
    Ok(SocketAddr::new(ip, port))
}

#[cfg(all(test, feature = "wireguard"))]
mod tests {
    use super::*;

    #[test]
    fn socket_addr_requires_ip_literal_and_nonzero_port() {
        let addr = parse_socket_addr("wg.peer", "1.2.3.4", 51820).unwrap();
        assert_eq!(addr.to_string(), "1.2.3.4:51820");
        assert!(parse_socket_addr("wg.peer", "example.com", 51820).is_err());
        assert!(parse_socket_addr("wg.peer", "1.2.3.4", 0).is_err());
    }
}
