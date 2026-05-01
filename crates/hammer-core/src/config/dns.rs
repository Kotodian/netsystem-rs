//! `[dns]` config section.
//!
//! `RawDnsConfig` covers the single user-facing TOML block; `DnsOptions`
//! and friends are the runtime view (one or more `DnsServer`s plus a
//! finalization pointer). `DomainStrategy` lives here because the dns
//! section is its primary configurer; `tun.domain_strategy` and
//! `route.rules.*` reach into this module to reuse the same enum.

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::HammerError;

use super::constants as C;
use super::{raw_struct, raw_struct_with_default_check};

raw_struct_with_default_check! {
    pub struct RawDnsConfig {
        /// DNS transport id.
        pub id: String => "String::is_empty",
        /// Upstream DNS server URL or address.
        pub server: String => "String::is_empty",
        /// Default DNS transport id when using `[[dns.servers]]`.
        #[serde(rename = "final")]
        pub final_: String => "String::is_empty",
        /// DNS answer selection strategy.
        pub strategy: DomainStrategy => "DomainStrategy::is_default",
        /// Outbound id used to reach the upstream DNS server.
        pub via: String => "String::is_empty",
        /// Single-server form: bootstrap DNS server tag for resolving
        /// `server` when it is a domain name. Equivalent of
        /// `[[dns.servers]].domain_resolver`.
        pub domain_resolver: String => "String::is_empty",
        /// Explicit DNS transport list.
        pub servers: Vec<RawDnsServer> => "Vec::is_empty",
    }
}

raw_struct! {
    pub struct RawRemoteDnsServer {
        /// DNS transport id.
        pub id: String => "String::is_empty",
        /// Upstream DNS host or IP.
        pub server: String => "String::is_empty",
        /// Upstream DNS port.
        pub server_port: Option<u16> => "Option::is_none",
        /// Outbound id used to reach the upstream DNS server.
        pub via: String => "String::is_empty",
        /// DNS server tag used to resolve `server` when it is a domain.
        /// Required when `via` is non-empty and `server` is a domain,
        /// unless `route.default_domain_resolver` provides a fallback.
        pub domain_resolver: String => "String::is_empty",
    }
}

raw_struct! {
    pub struct RawRemoteHttpsDnsServer {
        /// DNS transport id.
        pub id: String => "String::is_empty",
        /// Upstream DNS host or IP.
        pub server: String => "String::is_empty",
        /// Upstream HTTPS DNS port.
        pub server_port: Option<u16> => "Option::is_none",
        /// Outbound id used to reach the upstream DNS server.
        pub via: String => "String::is_empty",
        /// HTTP path for DNS-over-HTTPS.
        pub path: String => "String::is_empty",
        /// DNS server tag used to resolve `server` when it is a domain.
        pub domain_resolver: String => "String::is_empty",
    }
}

raw_struct! {
    pub struct RawNamedDnsServer {
        /// DNS transport id.
        pub id: String => "String::is_empty",
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawDnsServer {
    Udp(RawRemoteDnsServer),
    Tcp(RawRemoteDnsServer),
    Https(RawRemoteHttpsDnsServer),
    Hosts(RawNamedDnsServer),
    Local(RawNamedDnsServer),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsOptions {
    pub servers: Vec<DnsServer>,
    pub final_: String,
    pub strategy: DomainStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsServer {
    pub id: String,
    pub kind: DnsServerKind,
}

impl DnsServer {
    pub fn type_name(&self) -> &'static str {
        match &self.kind {
            DnsServerKind::Udp(_) => "udp",
            DnsServerKind::Tcp(_) => "tcp",
            DnsServerKind::Https(_) => "https",
            DnsServerKind::Hosts => C::DNS_TYPE_HOSTS,
            DnsServerKind::Local => C::DNS_TYPE_LOCAL,
        }
    }

    pub fn via(&self) -> &str {
        match &self.kind {
            DnsServerKind::Udp(o) => &o.via,
            DnsServerKind::Tcp(o) => &o.via,
            DnsServerKind::Https(o) => &o.via,
            DnsServerKind::Hosts | DnsServerKind::Local => "",
        }
    }

    /// Bootstrap DNS server tag for resolving this server's `server`
    /// when it is a domain. Empty means "unset; rely on
    /// `route.default_domain_resolver`".
    pub fn domain_resolver(&self) -> &str {
        match &self.kind {
            DnsServerKind::Udp(o) => &o.domain_resolver,
            DnsServerKind::Tcp(o) => &o.domain_resolver,
            DnsServerKind::Https(o) => &o.domain_resolver,
            DnsServerKind::Hosts | DnsServerKind::Local => "",
        }
    }

    /// Upstream server string (host or IP) for remote kinds, `None`
    /// for hosts/local which have no remote endpoint.
    pub fn server_string(&self) -> Option<&str> {
        match &self.kind {
            DnsServerKind::Udp(o) => Some(o.server.as_str()),
            DnsServerKind::Tcp(o) => Some(o.server.as_str()),
            DnsServerKind::Https(o) => Some(o.server.as_str()),
            DnsServerKind::Hosts | DnsServerKind::Local => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsServerKind {
    Udp(RemoteDnsServer),
    Tcp(RemoteDnsServer),
    Https(RemoteHttpsDnsServer),
    Hosts,
    Local,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteDnsServer {
    pub server: String,
    pub server_port: u16,
    pub via: String,
    /// DNS server tag used to bootstrap-resolve `server` when it is a
    /// domain name. Empty means "unset; fall back to
    /// `route.default_domain_resolver`".
    pub domain_resolver: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteHttpsDnsServer {
    pub server: String,
    pub server_port: u16,
    pub via: String,
    pub path: String,
    pub domain_resolver: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DomainResolveOptions {
    pub server: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainStrategy {
    #[default]
    AsIs,
    PreferIpv4,
    PreferIpv6,
    Ipv4Only,
    Ipv6Only,
}

impl DomainStrategy {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn name(self) -> &'static str {
        match self {
            DomainStrategy::AsIs => "as_is",
            DomainStrategy::PreferIpv4 => "prefer_ipv4",
            DomainStrategy::PreferIpv6 => "prefer_ipv6",
            DomainStrategy::Ipv4Only => "ipv4_only",
            DomainStrategy::Ipv6Only => "ipv6_only",
        }
    }
}

/// Resolve the DNS server id, defaulting to `default` when the user didn't
/// pick one. Both `build_dns_options` (this module) and `build_options`
/// (mod.rs's orchestrator, for `RouteOptions::default_domain_resolver`)
/// need this so it has to be reachable across submodules.
pub(super) fn dns_id(raw: &RawDnsConfig) -> &str {
    if raw.id.is_empty() {
        C::DEFAULT_DNS_ID
    } else {
        &raw.id
    }
}

pub(super) fn build_dns_options(
    raw: &RawDnsConfig,
    default_via: &str,
) -> Result<DnsOptions, HammerError> {
    if !raw.servers.is_empty() {
        return build_declared_dns_options(raw, default_via);
    }
    if raw.server.is_empty() {
        return Err(HammerError::config_validation("dns.server is required"));
    }
    let via = if raw.via.is_empty() {
        default_via.to_owned()
    } else {
        raw.via.clone()
    };
    let server = build_dns_server(raw, &via, &raw.domain_resolver)?;
    let final_id = server.id.clone();
    Ok(DnsOptions {
        servers: vec![server],
        final_: final_id,
        strategy: raw.strategy,
    })
}

fn build_declared_dns_options(
    raw: &RawDnsConfig,
    default_via: &str,
) -> Result<DnsOptions, HammerError> {
    let mut servers = Vec::new();
    for (idx, server) in raw.servers.iter().enumerate() {
        servers.push(build_declared_dns_server(idx, server, default_via)?);
    }
    let final_ = if raw.final_.is_empty() {
        servers
            .first()
            .map(|server| server.id.clone())
            .unwrap_or_else(|| C::DEFAULT_DNS_ID.to_owned())
    } else {
        raw.final_.clone()
    };
    Ok(DnsOptions {
        servers,
        final_,
        strategy: raw.strategy,
    })
}

fn build_declared_dns_server(
    idx: usize,
    raw: &RawDnsServer,
    default_via: &str,
) -> Result<DnsServer, HammerError> {
    let id = match raw {
        RawDnsServer::Udp(raw) => raw.id.as_str(),
        RawDnsServer::Tcp(raw) => raw.id.as_str(),
        RawDnsServer::Https(raw) => raw.id.as_str(),
        RawDnsServer::Hosts(raw) => raw.id.as_str(),
        RawDnsServer::Local(raw) => raw.id.as_str(),
    };
    let id = if id.is_empty() {
        format!("dns-{idx}")
    } else {
        id.to_owned()
    };
    match raw {
        RawDnsServer::Udp(raw) => {
            let server = remote_dns_options("dns.servers", raw, default_via, 53)?;
            Ok(DnsServer {
                id,
                kind: DnsServerKind::Udp(server),
            })
        }
        RawDnsServer::Tcp(raw) => {
            let server = remote_dns_options("dns.servers", raw, default_via, 53)?;
            Ok(DnsServer {
                id,
                kind: DnsServerKind::Tcp(server),
            })
        }
        RawDnsServer::Https(raw) => {
            if raw.server.is_empty() {
                return Err(HammerError::config_validation(
                    "dns.servers.server is required",
                ));
            }
            let via = if raw.via.is_empty() {
                default_via.to_owned()
            } else {
                raw.via.clone()
            };
            Ok(DnsServer {
                id,
                kind: DnsServerKind::Https(RemoteHttpsDnsServer {
                    server: raw.server.clone(),
                    server_port: raw.server_port.filter(|port| *port != 0).unwrap_or(443),
                    via,
                    path: if raw.path.is_empty() {
                        C::DEFAULT_DNS_PATH.to_owned()
                    } else {
                        raw.path.clone()
                    },
                    domain_resolver: raw.domain_resolver.clone(),
                }),
            })
        }
        RawDnsServer::Hosts(_) => Ok(DnsServer {
            id,
            kind: DnsServerKind::Hosts,
        }),
        RawDnsServer::Local(_) => Ok(DnsServer {
            id,
            kind: DnsServerKind::Local,
        }),
    }
}

fn remote_dns_options(
    field: &str,
    raw: &RawRemoteDnsServer,
    default_via: &str,
    default_port: u16,
) -> Result<RemoteDnsServer, HammerError> {
    if raw.server.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{field}.server is required"
        )));
    }
    let via = if raw.via.is_empty() {
        default_via.to_owned()
    } else {
        raw.via.clone()
    };
    Ok(RemoteDnsServer {
        server: raw.server.clone(),
        server_port: raw
            .server_port
            .filter(|port| *port != 0)
            .unwrap_or(default_port),
        via,
        domain_resolver: raw.domain_resolver.clone(),
    })
}

fn build_dns_server(
    raw: &RawDnsConfig,
    via: &str,
    domain_resolver: &str,
) -> Result<DnsServer, HammerError> {
    let id = dns_id(raw).to_owned();

    match raw.server.as_str() {
        C::DNS_TYPE_HOSTS => {
            return Ok(DnsServer {
                id,
                kind: DnsServerKind::Hosts,
            });
        }
        C::DNS_TYPE_LOCAL => {
            return Ok(DnsServer {
                id,
                kind: DnsServerKind::Local,
            });
        }
        _ => {}
    }

    if let Ok(parsed) = Url::parse(&raw.server)
        && !parsed.scheme().is_empty()
        && parsed.has_host()
    {
        return build_dns_server_from_url(parsed, id, via, domain_resolver);
    }

    let (host, port_str) = match raw.server.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            (h.to_owned(), p.to_owned())
        }
        _ => (raw.server.clone(), String::new()),
    };
    let port = if port_str.is_empty() {
        53
    } else {
        port_str
            .parse::<u16>()
            .map_err(|err| HammerError::config_validation(format!("dns.server port: {err}")))?
    };
    Ok(DnsServer {
        id,
        kind: DnsServerKind::Udp(RemoteDnsServer {
            server: host,
            server_port: port,
            via: via.to_owned(),
            domain_resolver: domain_resolver.to_owned(),
        }),
    })
}

fn build_dns_server_from_url(
    parsed: Url,
    id: String,
    via: &str,
    domain_resolver: &str,
) -> Result<DnsServer, HammerError> {
    let host = parsed
        .host_str()
        .ok_or_else(|| HammerError::config_validation("dns.server host is required"))?
        .to_owned();
    match parsed.scheme() {
        "https" => {
            let port = parsed.port().unwrap_or(443);
            let path = if parsed.path().is_empty() || parsed.path() == "/" {
                C::DEFAULT_DNS_PATH.to_owned()
            } else {
                parsed.path().to_owned()
            };
            Ok(DnsServer {
                id,
                kind: DnsServerKind::Https(RemoteHttpsDnsServer {
                    server: host,
                    server_port: port,
                    via: via.to_owned(),
                    path,
                    domain_resolver: domain_resolver.to_owned(),
                }),
            })
        }
        "udp" => Ok(DnsServer {
            id,
            kind: DnsServerKind::Udp(RemoteDnsServer {
                server: host,
                server_port: parsed.port().unwrap_or(53),
                via: via.to_owned(),
                domain_resolver: domain_resolver.to_owned(),
            }),
        }),
        "tcp" => Ok(DnsServer {
            id,
            kind: DnsServerKind::Tcp(RemoteDnsServer {
                server: host,
                server_port: parsed.port().unwrap_or(53),
                via: via.to_owned(),
                domain_resolver: domain_resolver.to_owned(),
            }),
        }),
        other => Err(HammerError::config_validation(format!(
            "unsupported dns.server scheme: {other}"
        ))),
    }
}
