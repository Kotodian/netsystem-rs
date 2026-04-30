use std::net::IpAddr;
#[cfg(feature = "wireguard")]
use std::net::SocketAddr;
use std::time::Duration;

#[cfg(feature = "wireguard")]
use base64::Engine as _;
#[cfg(feature = "wireguard")]
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use ipnet::IpNet;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::error::HammerError;

use super::options::DomainStrategy;

/// Serialize a list of CIDRs back to TOML strings — shared by every section
/// that has a `Vec<IpNet>` field (`tun.address`, `route.rules.ip_cidr`,
/// `endpoints.address`, ...). Each section keeps its own `deserialize_with`
/// so error messages can point at the offending key, but on the way out a
/// generic `to_string()` round-trip is enough.
pub fn serialize_ipnet_vec<S>(value: &[IpNet], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .serialize(serializer)
}

/// Serialize an optional Go-style `Duration` (e.g. "300ms", "1h2m3s") so
/// `format_config` produces output that round-trips back through
/// `parse_optional_duration`.
pub fn serialize_duration_option<S>(
    value: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_str(&format_duration_go_style(*value)),
        None => serializer.serialize_none(),
    }
}

/// Format a `Duration` using the largest single unit that divides it evenly
/// (e.g. `60s` → `"1m"`, `1500ms` → `"1500ms"` since 1.5s isn't a whole
/// second). Falls back to nanoseconds for sub-microsecond residues.
pub fn format_duration_go_style(value: Duration) -> String {
    const NS: u128 = 1;
    const US: u128 = 1_000 * NS;
    const MS: u128 = 1_000 * US;
    const S: u128 = 1_000 * MS;
    const M: u128 = 60 * S;
    const H: u128 = 60 * M;

    let nanos = value.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    for (unit, scale) in [
        ("h", H),
        ("m", M),
        ("s", S),
        ("ms", MS),
        ("us", US),
        ("ns", NS),
    ] {
        if nanos.is_multiple_of(scale) {
            return format!("{}{}", nanos / scale, unit);
        }
    }
    format!("{nanos}ns")
}

/// Deserialize a `Vec<IpNet>` from a list of CIDR / IP strings, attaching
/// `field` to any error so downstream messages name the bad key. Each
/// section wraps this with its own field-tagged `deserialize_with` because
/// serde's attribute strings reference a single function name.
pub fn deserialize_ipnet_vec<'de, D>(field: &str, deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|value| parse_ipnet(field, &value).map_err(de::Error::custom))
        .collect()
}

/// Deserialize an optional Go-style duration string. Empty string yields
/// `Ok(None)`; an unparseable value bubbles up tagged with `field`.
pub fn deserialize_duration_option<'de, D>(
    field: &str,
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_optional_duration(field, &value).map_err(de::Error::custom)
}

/// Parse Go-style duration strings like "300ms", "1.5s", "2m30s", "1h".
/// Empty input yields `None`.
pub fn parse_optional_duration(field: &str, value: &str) -> Result<Option<Duration>, HammerError> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_duration_go_style(value)
        .map(Some)
        .map_err(|err| HammerError::config_validation(format!("{field}: {err}")))
}

pub fn parse_optional_port(field: &str, value: &str) -> Result<u16, HammerError> {
    if value.is_empty() {
        return Ok(0);
    }
    value
        .parse::<u16>()
        .map_err(|err| HammerError::config_validation(format!("{field} port: {err}")))
}

pub fn parse_domain_strategy(field: &str, value: &str) -> Result<DomainStrategy, HammerError> {
    match value {
        "" | "as_is" => Ok(DomainStrategy::AsIs),
        "prefer_ipv4" => Ok(DomainStrategy::PreferIpv4),
        "prefer_ipv6" => Ok(DomainStrategy::PreferIpv6),
        "ipv4_only" => Ok(DomainStrategy::Ipv4Only),
        "ipv6_only" => Ok(DomainStrategy::Ipv6Only),
        other => Err(HammerError::config_validation(format!(
            "{field}: unknown domain strategy {other:?}"
        ))),
    }
}

/// Mimic of `time.ParseDuration` covering ms / s / m / h with decimal fractions.
/// Accepts e.g. `"300ms"`, `"1.5s"`, `"2m30s"`, `"1h2m3s"`.
fn parse_duration_go_style(input: &str) -> Result<Duration, String> {
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut total = Duration::ZERO;
    let mut consumed_any = false;

    while i < bytes.len() {
        // 1) read decimal number (possibly fractional, leading sign disallowed).
        let num_start = i;
        let mut saw_digit = false;
        let mut saw_dot = false;
        while i < bytes.len() {
            let c = bytes[i];
            if c.is_ascii_digit() {
                saw_digit = true;
                i += 1;
            } else if c == b'.' && !saw_dot {
                saw_dot = true;
                i += 1;
            } else {
                break;
            }
        }
        if !saw_digit {
            return Err(format!("invalid duration {input:?}"));
        }
        let value: f64 = input[num_start..i]
            .parse()
            .map_err(|_| format!("invalid duration {input:?}"))?;

        // 2) read unit (ns, us, µs, ms, s, m, h)
        let unit_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() && bytes[i] != b'.' {
            i += 1;
        }
        if i == unit_start {
            return Err(format!("missing unit in duration {input:?}"));
        }
        let unit = &input[unit_start..i];
        let nanos_per_unit: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3_600.0 * 1_000_000_000.0,
            other => return Err(format!("unknown unit {other:?} in duration {input:?}")),
        };
        let nanos = value * nanos_per_unit;
        if !nanos.is_finite() || nanos < 0.0 {
            return Err(format!("invalid duration {input:?}"));
        }
        total += Duration::from_nanos(nanos as u64);
        consumed_any = true;
    }

    if !consumed_any {
        return Err(format!("empty duration {input:?}"));
    }
    Ok(total)
}

/// Decode a base64-encoded 32-byte key (Curve25519 public/private/PSK).
#[cfg(feature = "wireguard")]
pub fn parse_base64_key(field: &str, value: &str) -> Result<[u8; 32], HammerError> {
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

/// Parse a CIDR-form prefix into `ipnet::IpNet`. Bare IPs without a prefix are
/// treated as host routes (`/32` or `/128`).
pub fn parse_ipnet(field: &str, value: &str) -> Result<IpNet, HammerError> {
    if let Ok(net) = value.parse::<IpNet>() {
        return Ok(net);
    }
    let ip: IpAddr = value.parse().map_err(|_| {
        HammerError::config_validation(format!("{field}: invalid CIDR or IP {value:?}"))
    })?;
    let host_prefix = if ip.is_ipv4() { 32 } else { 128 };
    IpNet::new(ip, host_prefix)
        .map_err(|err| HammerError::config_validation(format!("{field}: {err}")))
}

/// Parse a `host:port`-style endpoint. WireGuard currently requires an IP
/// literal; hostname endpoints need lifecycle DNS resolution before enabling.
#[cfg(feature = "wireguard")]
pub fn parse_socket_addr(field: &str, host: &str, port: u16) -> Result<SocketAddr, HammerError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parses_go_style() {
        assert_eq!(
            parse_optional_duration("x", "300ms").unwrap().unwrap(),
            Duration::from_millis(300)
        );
        assert_eq!(
            parse_optional_duration("x", "1.5s").unwrap().unwrap(),
            Duration::from_millis(1_500)
        );
        assert_eq!(
            parse_optional_duration("x", "2m30s").unwrap().unwrap(),
            Duration::from_secs(150)
        );
        assert_eq!(
            parse_optional_duration("x", "1h").unwrap().unwrap(),
            Duration::from_secs(3_600)
        );
        assert!(parse_optional_duration("x", "").unwrap().is_none());
        assert!(parse_optional_duration("x", "what").is_err());
    }

    #[test]
    fn domain_strategy_table() {
        assert_eq!(
            parse_domain_strategy("dns.strategy", "").unwrap(),
            DomainStrategy::AsIs
        );
        assert_eq!(
            parse_domain_strategy("dns.strategy", "prefer_ipv4").unwrap(),
            DomainStrategy::PreferIpv4
        );
        let err = parse_domain_strategy("dns.strategy", "prefer_quantum").unwrap_err();
        assert!(err.to_string().contains("dns.strategy"));
    }

    #[cfg(feature = "wireguard")]
    #[test]
    fn base64_key_round_trips_32_bytes() {
        let raw = [7u8; 32];
        let encoded = BASE64_STANDARD.encode(raw);
        let decoded = parse_base64_key("wg.private_key", &encoded).unwrap();
        assert_eq!(decoded, raw);
    }

    #[cfg(feature = "wireguard")]
    #[test]
    fn base64_key_rejects_wrong_length() {
        let encoded = BASE64_STANDARD.encode([1u8; 16]);
        let err = parse_base64_key("wg.private_key", &encoded).unwrap_err();
        assert!(err.to_string().contains("32 decoded bytes"));
    }

    #[cfg(feature = "wireguard")]
    #[test]
    fn base64_key_rejects_invalid_base64() {
        let err = parse_base64_key("wg.private_key", "not!base64!!").unwrap_err();
        assert!(err.to_string().contains("invalid base64"));
    }

    #[test]
    fn ipnet_accepts_cidr_and_promotes_bare_ip_to_host_prefix() {
        let v4 = parse_ipnet("wg.address", "10.0.0.1").unwrap();
        assert_eq!(v4.prefix_len(), 32);
        let v6 = parse_ipnet("wg.address", "fd00::1").unwrap();
        assert_eq!(v6.prefix_len(), 128);
        let cidr = parse_ipnet("wg.address", "10.0.0.0/24").unwrap();
        assert_eq!(cidr.prefix_len(), 24);
    }

    #[test]
    fn ipnet_rejects_garbage() {
        assert!(parse_ipnet("wg.address", "not-an-ip").is_err());
    }

    #[cfg(feature = "wireguard")]
    #[test]
    fn socket_addr_requires_ip_literal_and_nonzero_port() {
        let addr = parse_socket_addr("wg.peer", "1.2.3.4", 51820).unwrap();
        assert_eq!(addr.to_string(), "1.2.3.4:51820");
        assert!(parse_socket_addr("wg.peer", "example.com", 51820).is_err());
        assert!(parse_socket_addr("wg.peer", "1.2.3.4", 0).is_err());
    }
}
