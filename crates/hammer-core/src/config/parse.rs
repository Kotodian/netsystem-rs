#[cfg(feature = "wireguard")]
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

#[cfg(feature = "wireguard")]
use base64::Engine as _;
#[cfg(feature = "wireguard")]
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
#[cfg(feature = "wireguard")]
use ipnet::IpNet;

use crate::error::HammerError;

use super::options::{DomainStrategy, Prefix};

/// Validate that `value` parses as either an IP address (with optional `/prefix`)
/// or returns the raw text wrapped in `Prefix`. We keep the original textual form
/// because M1 does not consume prefixes operationally — they are persisted
/// verbatim and re-validated at use-time in M5.
pub fn parse_prefix(field: &str, value: &str) -> Result<Prefix, HammerError> {
    let (host, prefix_part) = match value.split_once('/') {
        Some((h, p)) => (h, Some(p)),
        None => (value, None),
    };
    let ip: std::net::IpAddr = host.parse().map_err(|_| {
        HammerError::config_validation(format!("{field}: invalid IP address {host:?}"))
    })?;
    if let Some(p) = prefix_part {
        let bits: u8 = p.parse().map_err(|_| {
            HammerError::config_validation(format!("{field}: invalid prefix length {p:?}"))
        })?;
        let max = if ip.is_ipv4() { 32 } else { 128 };
        if bits > max {
            return Err(HammerError::config_validation(format!(
                "{field}: prefix length {bits} exceeds {max}"
            )));
        }
    }
    Ok(Prefix(value.to_owned()))
}

pub fn parse_prefix_list(field: &str, values: &[String]) -> Result<Vec<Prefix>, HammerError> {
    values.iter().map(|v| parse_prefix(field, v)).collect()
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
#[cfg(feature = "wireguard")]
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

#[cfg(feature = "wireguard")]
pub fn parse_ipnet_list(field: &str, values: &[String]) -> Result<Vec<IpNet>, HammerError> {
    values.iter().map(|v| parse_ipnet(field, v)).collect()
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
    fn parse_prefix_accepts_v4_v6_and_bare_ip() {
        assert!(parse_prefix("tun.address", "172.19.0.1/30").is_ok());
        assert!(parse_prefix("tun.address", "::1/128").is_ok());
        assert!(parse_prefix("tun.address", "10.0.0.1").is_ok());
    }

    #[test]
    fn parse_prefix_rejects_invalid() {
        assert!(parse_prefix("tun.address", "not-an-ip").is_err());
        assert!(parse_prefix("tun.address", "10.0.0.1/40").is_err());
        assert!(parse_prefix("tun.address", "::1/200").is_err());
    }

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

    #[cfg(feature = "wireguard")]
    #[test]
    fn ipnet_accepts_cidr_and_promotes_bare_ip_to_host_prefix() {
        let v4 = parse_ipnet("wg.address", "10.0.0.1").unwrap();
        assert_eq!(v4.prefix_len(), 32);
        let v6 = parse_ipnet("wg.address", "fd00::1").unwrap();
        assert_eq!(v6.prefix_len(), 128);
        let cidr = parse_ipnet("wg.address", "10.0.0.0/24").unwrap();
        assert_eq!(cidr.prefix_len(), 24);
    }

    #[cfg(feature = "wireguard")]
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
