use url::Url;

use crate::error::HammerError;

use super::constants as C;
#[cfg(feature = "wireguard")]
use super::endpoint::build_endpoints;
use super::inbound::{RawTunConfig, build_tun_inbound};
use super::log::build_log_options;
use super::options::*;
use super::outbound::{build_hysteria_options, build_outbounds};
use super::parse::parse_optional_port;
use super::raw::*;

pub(crate) fn build_options(raw: RawConfig) -> Result<Options, HammerError> {
    #[cfg(feature = "wireguard")]
    let RawConfig {
        log: raw_log,
        tun: raw_tun,
        hysteria2: raw_hysteria,
        endpoints: raw_endpoints,
        dns: raw_dns,
        route: raw_route,
    } = raw;
    #[cfg(not(feature = "wireguard"))]
    let RawConfig {
        log: raw_log,
        tun: raw_tun,
        hysteria2: raw_hysteria,
        dns: raw_dns,
        route: raw_route,
    } = raw;

    let (tun_inbound, tun_id) = build_tun_inbound(&raw_tun)?;
    let mut rules = derive_tun_route_rules(&raw_tun, &tun_id)?;
    rules.extend(build_user_rules(&raw_route.rules)?);

    let (hysteria_options, hysteria_id) = build_hysteria_options(raw_hysteria)?;

    #[cfg(feature = "wireguard")]
    let endpoints = build_endpoints(raw_endpoints)?;

    let route_final = if raw_route.final_.is_empty() {
        hysteria_id.clone()
    } else {
        raw_route.final_.clone()
    };
    let auto_detect = raw_route.auto_detect_interface.unwrap_or(true);

    let dns_options = build_dns_options(&raw_dns, C::DEFAULT_DIRECT_ID)?;
    let route_options = RouteOptions {
        final_: route_final,
        auto_detect_interface: auto_detect,
        rules,
        default_domain_resolver: Some(DomainResolveOptions {
            server: dns_id(&raw_dns).to_owned(),
        }),
    };

    Ok(Options {
        log: build_log_options(raw_log),
        dns: dns_options,
        inbounds: vec![tun_inbound],
        outbounds: build_outbounds(hysteria_options, hysteria_id),
        #[cfg(feature = "wireguard")]
        endpoints,
        route: route_options,
    })
}

fn build_dns_options(raw: &RawDnsConfig, default_via: &str) -> Result<DnsOptions, HammerError> {
    if raw.server.is_empty() {
        return Err(HammerError::config_validation("dns.server is required"));
    }
    let via = if raw.via.is_empty() {
        default_via.to_owned()
    } else {
        raw.via.clone()
    };
    let server = build_dns_server(raw, &via)?;
    let final_id = server.id.clone();
    Ok(DnsOptions {
        servers: vec![server],
        final_: final_id,
        strategy: raw.strategy,
    })
}

pub(crate) fn dns_id(raw: &RawDnsConfig) -> &str {
    if raw.id.is_empty() {
        C::DEFAULT_DNS_ID
    } else {
        &raw.id
    }
}

fn build_dns_server(raw: &RawDnsConfig, via: &str) -> Result<DnsServer, HammerError> {
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
        return build_dns_server_from_url(parsed, id, via);
    }

    let (host, port_str) = match raw.server.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            (h.to_owned(), p.to_owned())
        }
        _ => (raw.server.clone(), String::new()),
    };
    let port = parse_optional_port("dns.server", &port_str)?;
    Ok(DnsServer {
        id,
        kind: DnsServerKind::Udp(RemoteDnsServer {
            server: host,
            server_port: if port == 0 { 53 } else { port },
            via: via.to_owned(),
        }),
    })
}

fn build_dns_server_from_url(parsed: Url, id: String, via: &str) -> Result<DnsServer, HammerError> {
    let host = parsed
        .host_str()
        .ok_or_else(|| HammerError::config_validation("dns.server host is required"))?
        .to_owned();
    let port_str = parsed.port().map(|p| p.to_string()).unwrap_or_default();
    let port = parse_optional_port("dns.server", &port_str)?;
    match parsed.scheme() {
        "https" => {
            let port = if port == 0 { 443 } else { port };
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
                }),
            })
        }
        "udp" => Ok(DnsServer {
            id,
            kind: DnsServerKind::Udp(RemoteDnsServer {
                server: host,
                server_port: if port == 0 { 53 } else { port },
                via: via.to_owned(),
            }),
        }),
        "tcp" => Ok(DnsServer {
            id,
            kind: DnsServerKind::Tcp(RemoteDnsServer {
                server: host,
                server_port: if port == 0 { 53 } else { port },
                via: via.to_owned(),
            }),
        }),
        other => Err(HammerError::config_validation(format!(
            "unsupported dns.server scheme: {other}"
        ))),
    }
}

pub(crate) fn derive_tun_route_rules(
    raw_tun: &RawTunConfig,
    tun_id: &str,
) -> Result<Vec<Rule>, HammerError> {
    let sniff_timeout = raw_tun.sniff_timeout;
    let domain_strategy = raw_tun.domain_strategy;
    if raw_tun.sniff_override_destination && !raw_tun.sniff {
        return Err(HammerError::config_validation(
            "tun.sniff_override_destination requires tun.sniff=true",
        ));
    }
    if raw_tun.hijack_dns && !raw_tun.sniff {
        return Err(HammerError::config_validation(
            "tun.hijack_dns requires tun.sniff=true",
        ));
    }
    if raw_tun.block_quic && !raw_tun.sniff {
        return Err(HammerError::config_validation(
            "tun.block_quic requires tun.sniff=true",
        ));
    }
    let mut rules = Vec::new();
    if raw_tun.sniff {
        rules.push(tun_rule(
            tun_id,
            RuleActionKind::Sniff(SniffActionOptions {
                timeout: sniff_timeout,
                override_destination: raw_tun.sniff_override_destination,
            }),
        ));
    }
    if raw_tun.hijack_dns {
        rules.push(protocol_rule(C::PROTOCOL_DNS, RuleActionKind::HijackDns));
    }
    if raw_tun.block_quic {
        rules.push(protocol_rule(
            C::PROTOCOL_QUIC,
            RuleActionKind::Reject(RejectActionOptions {
                method: C::REJECT_METHOD_DEFAULT.to_owned(),
            }),
        ));
    }
    if domain_strategy != DomainStrategy::AsIs {
        rules.push(tun_rule(
            tun_id,
            RuleActionKind::Resolve(ResolveActionOptions {
                strategy: domain_strategy,
            }),
        ));
    }
    if raw_tun.udp_disable_domain_unmapping {
        rules.push(tun_rule(
            tun_id,
            RuleActionKind::RouteOptions(RouteOptionsActionOptions {
                udp_disable_domain_unmapping: true,
            }),
        ));
    }
    Ok(rules)
}

fn tun_rule(tun_id: &str, action: RuleActionKind) -> Rule {
    Rule {
        default_options: DefaultRule {
            matcher: RuleMatcher::Inbound(vec![tun_id.to_owned()]),
            action,
        },
    }
}

fn protocol_rule(protocol: &str, action: RuleActionKind) -> Rule {
    Rule {
        default_options: DefaultRule {
            matcher: RuleMatcher::Protocol(vec![protocol.to_owned()]),
            action,
        },
    }
}

fn build_user_rules(raw: &[RawRouteRule]) -> Result<Vec<Rule>, HammerError> {
    raw.iter()
        .enumerate()
        .map(|(idx, raw)| build_user_rule(idx, raw))
        .collect()
}

fn build_user_rule(idx: usize, raw: &RawRouteRule) -> Result<Rule, HammerError> {
    if raw.outbound.is_empty() {
        return Err(HammerError::config_validation(format!(
            "route.rules[{idx}].outbound is required",
        )));
    }
    Ok(Rule {
        default_options: DefaultRule {
            matcher: raw.matcher.clone(),
            action: RuleActionKind::Route(RouteActionOptions {
                outbound: raw.outbound.clone(),
            }),
        },
    })
}
