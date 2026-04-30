//! `[[endpoints]]` config sections (currently only WireGuard).
//!
//! Mirrors sing-box 1.11+'s endpoint concept: an outbound that also has
//! Lifecycle state. Adding a new endpoint protocol drops in another
//! `RawEndpoint::*` variant without breaking existing TOML files. The
//! whole module is gated on `feature = "wireguard"` from mod.rs, so this
//! file does not need per-item `#[cfg]` annotations.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use ipnet::IpNet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::HammerError;

use super::constants as C;
use super::parse::{self, deserialize_ipnet_vec};
use super::raw_struct;

/// Outer endpoint variant — sing-box style `[[endpoints]]` entries with a
/// `type` discriminator. Adding a new endpoint protocol (e.g. tailscale)
/// means adding a new variant here without breaking existing TOML files.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawEndpoint {
    /// WireGuard endpoint entry.
    Wireguard(RawWireguardEndpoint),
}

raw_struct! {
    pub struct RawWireguardEndpoint {
        /// Endpoint id used by route rules and lifecycle managers.
        pub id: String => "String::is_empty",
        /// Base64-encoded WireGuard private key.
        pub private_key: RawBase64Key => "RawBase64Key::is_empty",
        /// Optional UDP listen port.
        pub listen_port: Option<u16> => "Option::is_none",
        /// Optional WireGuard interface MTU.
        pub mtu: Option<u32> => "Option::is_none",
        /// Local WireGuard interface addresses in CIDR form.
        #[serde(
            deserialize_with = "deserialize_wireguard_addresses",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub address: Vec<IpNet> => "Vec::is_empty",
        /// WireGuard peer list.
        pub peers: Vec<RawWireguardPeer> => "Vec::is_empty",
    }
}

raw_struct! {
    pub struct RawWireguardPeer {
        /// Base64-encoded peer public key.
        pub public_key: RawBase64Key => "RawBase64Key::is_empty",
        /// Optional base64-encoded pre-shared key.
        pub pre_shared_key: Option<RawBase64Key> => "Option::is_none",
        /// Peer endpoint address; currently must be an IP literal.
        pub address: String => "String::is_empty",
        /// Peer endpoint UDP port.
        pub port: u16 => "is_zero_u16",
        /// Allowed IP prefixes routed to this peer.
        #[serde(
            deserialize_with = "deserialize_wireguard_allowed_ips",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub allowed_ips: Vec<IpNet> => "Vec::is_empty",
        /// Optional persistent keepalive interval in seconds.
        pub persistent_keepalive_interval: Option<u32> => "Option::is_none",
        /// Optional reserved WARP-style header bytes.
        pub reserved: Option<[u8; 3]> => "Option::is_none",
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct RawBase64Key(String);

impl RawBase64Key {
    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }

    pub fn decode_32(&self, field: &str) -> Result<[u8; 32], HammerError> {
        parse_base64_key(field, self.0.trim())
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
        match &self.kind {
            EndpointKind::Wireguard(_) => C::TYPE_WIREGUARD,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Wireguard(WireguardEndpointOptions),
}

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
        .map(|(idx, item)| match item {
            RawEndpoint::Wireguard(wg) => build_wireguard_endpoint(idx, wg),
        })
        .collect()
}

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
    let private_key = raw
        .private_key
        .decode_32(&format!("endpoints[{idx}].private_key"))?;
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
            private_key,
            listen_port,
            mtu,
            address,
            peers,
        }),
    })
}

fn build_wireguard_peer(
    endpoint_idx: usize,
    peer_idx: usize,
    raw: RawWireguardPeer,
) -> Result<WireguardPeerOptions, HammerError> {
    let prefix = format!("endpoints[{endpoint_idx}].peers[{peer_idx}]");
    let public_key = raw.public_key.decode_32(&format!("{prefix}.public_key"))?;
    let pre_shared_key = match raw.pre_shared_key.as_ref() {
        Some(value) if !value.is_empty() => {
            Some(value.decode_32(&format!("{prefix}.pre_shared_key"))?)
        }
        _ => None,
    };
    if raw.address.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{prefix}.address is required"
        )));
    }
    let endpoint = parse_socket_addr(&format!("{prefix}.address"), &raw.address, raw.port)?;
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
        public_key,
        pre_shared_key,
        endpoint,
        allowed_ips,
        persistent_keepalive,
        reserved,
    })
}

/// Decode a base64-encoded 32-byte key (Curve25519 public/private/PSK).
fn parse_base64_key(field: &str, value: &str) -> Result<[u8; 32], HammerError> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|err| HammerError::config_validation(format!("{field}: invalid base64: {err}")))?;
    if bytes.len() != 32 {
        return Err(HammerError::config_validation(format!(
            "{field}: expected 32 decoded bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Parse a `host:port`-style endpoint. WireGuard currently requires an IP
/// literal; hostname endpoints would need lifecycle-time DNS resolution
/// which is not wired up yet.
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

fn deserialize_wireguard_addresses<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("endpoints.address", deserializer)
}

fn deserialize_wireguard_allowed_ips<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("endpoints.peers.allowed_ips", deserializer)
}

fn serialize_ipnet_vec<S>(value: &[IpNet], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    parse::serialize_ipnet_vec(value, serializer)
}

// Re-export `is_zero_u16` so the `raw_struct!` macro for `RawWireguardPeer`
// can resolve the literal `"is_zero_u16"` skip path within this module's
// scope.
use super::parse::is_zero_u16;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_key_round_trips_32_bytes() {
        let raw = [7u8; 32];
        let encoded = BASE64_STANDARD.encode(raw);
        let decoded = parse_base64_key("wg.private_key", &encoded).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn base64_key_rejects_wrong_length() {
        let encoded = BASE64_STANDARD.encode([1u8; 16]);
        let err = parse_base64_key("wg.private_key", &encoded).unwrap_err();
        assert!(err.to_string().contains("32 decoded bytes"));
    }

    #[test]
    fn base64_key_rejects_invalid_base64() {
        let err = parse_base64_key("wg.private_key", "not!base64!!").unwrap_err();
        assert!(err.to_string().contains("invalid base64"));
    }

    #[test]
    fn socket_addr_requires_ip_literal_and_nonzero_port() {
        let addr = parse_socket_addr("wg.peer", "1.2.3.4", 51820).unwrap();
        assert_eq!(addr.to_string(), "1.2.3.4:51820");
        assert!(parse_socket_addr("wg.peer", "example.com", 51820).is_err());
        assert!(parse_socket_addr("wg.peer", "1.2.3.4", 0).is_err());
    }
}
