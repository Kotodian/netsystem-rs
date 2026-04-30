#[cfg(feature = "wireguard")]
use std::time::Duration;

use ipnet::IpNet;
use url::Url;

use crate::error::HammerError;

use super::options::constants as C;
use super::options::*;
#[cfg(feature = "wireguard")]
use super::parse::{parse_base64_key, parse_ipnet_list, parse_socket_addr};
use super::parse::{parse_optional_duration, parse_optional_port, parse_prefix_list};
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

    let tun_id = if raw_tun.id.is_empty() {
        C::DEFAULT_TUN_ID.to_owned()
    } else {
        raw_tun.id.clone()
    };
    let tun_options = build_tun_options(&raw_tun)?;
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
        log: LogOptions {
            disabled: raw_log.disabled,
            level: raw_log.level,
            output: raw_log.output,
            timestamp: raw_log.timestamp,
        },
        dns: dns_options,
        inbounds: vec![Inbound {
            tag: tun_id,
            kind: InboundKind::Tun(tun_options),
        }],
        outbounds: build_outbounds(hysteria_options, hysteria_id),
        #[cfg(feature = "wireguard")]
        endpoints,
        route: route_options,
    })
}

fn build_outbounds(hysteria: Hysteria2OutboundOptions, hysteria_id: String) -> Vec<Outbound> {
    vec![
        Outbound {
            tag: hysteria_id,
            kind: OutboundKind::Hysteria2(hysteria),
        },
        Outbound {
            tag: C::DEFAULT_DIRECT_ID.to_owned(),
            kind: OutboundKind::Direct(DirectOutboundOptions {
                network_strategy: C::NETWORK_STRATEGY_DEFAULT.to_owned(),
            }),
        },
    ]
}

fn build_tun_options(raw: &RawTunConfig) -> Result<TunInboundOptions, HammerError> {
    let mtu = if raw.mtu == 0 {
        C::DEFAULT_TUN_MTU
    } else {
        raw.mtu
    };
    let stack = raw.stack.unwrap_or_default();
    let address = parse_prefix_list("tun.address", &raw.address)?;
    if address.is_empty() {
        return Err(HammerError::config_validation("tun.address is required"));
    }
    let route_address = parse_prefix_list("tun.route_address", &raw.route_address)?;
    let route_exclude_address =
        parse_prefix_list("tun.route_exclude_address", &raw.route_exclude_address)?;
    let auto_route = raw.auto_route.unwrap_or(true);
    let udp_timeout = parse_optional_duration("tun.udp_timeout", &raw.udp_timeout)?;
    Ok(TunInboundOptions {
        interface_name: raw.interface_name.clone(),
        mtu,
        address,
        route_address,
        route_exclude_address,
        auto_route,
        strict_route: raw.strict_route,
        stack,
        udp_timeout,
    })
}

fn build_hysteria_options(
    mut raw: RawHysteria2Config,
) -> Result<(Hysteria2OutboundOptions, String), HammerError> {
    let id = if raw.id.is_empty() {
        C::DEFAULT_HYSTERIA_ID.to_owned()
    } else {
        std::mem::take(&mut raw.id)
    };
    if raw.server.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.server is required",
        ));
    }
    let mut server_port = raw.server_port;
    if server_port == 0 && raw.server_ports.is_empty() {
        server_port = C::DEFAULT_HYSTERIA_PORT;
    }
    if raw.password.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.password is required",
        ));
    }
    if !raw.server_ports.is_empty()
        || !raw.hop_interval.is_empty()
        || !raw.hop_interval_max.is_empty()
    {
        return Err(HammerError::config_validation(
            "hysteria2 port hopping is not supported yet",
        ));
    }
    if raw.up_mbps < 0 || raw.down_mbps < 0 {
        return Err(HammerError::config_validation(
            "hysteria2.up_mbps and hysteria2.down_mbps must be non-negative",
        ));
    }
    if !raw.insecure && raw.sni.is_empty() && raw.server.parse::<std::net::IpAddr>().is_err() {
        raw.sni = raw.server.clone();
    }
    if !raw.insecure && raw.sni.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.sni is required unless insecure=true",
        ));
    }
    let network = if raw.network.is_empty() {
        vec![Hysteria2Network::Tcp, Hysteria2Network::Udp]
    } else {
        raw.network
    };
    let hop_interval = parse_optional_duration("hysteria2.hop_interval", &raw.hop_interval)?;
    let hop_interval_max =
        parse_optional_duration("hysteria2.hop_interval_max", &raw.hop_interval_max)?;
    let idle_timeout = parse_optional_duration("hysteria2.idle_timeout", &raw.idle_timeout)?;
    let keep_alive_period =
        parse_optional_duration("hysteria2.keep_alive_period", &raw.keep_alive_period)?;
    let bbr_profile = raw.bbr_profile.unwrap_or_default();
    let obfs = build_obfs(raw.obfs)?;
    Ok((
        Hysteria2OutboundOptions {
            server: raw.server,
            server_port,
            server_ports: raw.server_ports,
            password: raw.password,
            up_mbps: raw.up_mbps,
            down_mbps: raw.down_mbps,
            network,
            hop_interval,
            hop_interval_max,
            idle_timeout,
            keep_alive_period,
            bbr_profile,
            brutal_debug: raw.brutal_debug,
            disable_path_mtu_discovery: raw.disable_path_mtu,
            initial_packet_size: raw.initial_packet_size,
            tls: OutboundTlsOptions {
                enabled: true,
                server_name: raw.sni,
                insecure: raw.insecure,
            },
            obfs,
        },
        id,
    ))
}

fn build_obfs(raw: RawHysteria2Obfs) -> Result<Option<Hysteria2Obfs>, HammerError> {
    if raw.type_.is_none() && raw.password.is_empty() {
        return Ok(None);
    }
    let Some(type_) = raw.type_ else {
        return Err(HammerError::config_validation(
            "hysteria2.obfs.type and hysteria2.obfs.password must be set together",
        ));
    };
    if raw.password.is_empty() {
        return Err(HammerError::config_validation(
            "hysteria2.obfs.type and hysteria2.obfs.password must be set together",
        ));
    }
    Ok(Some(Hysteria2Obfs {
        type_,
        password: raw.password,
    }))
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
    let final_tag = server.tag.clone();
    Ok(DnsOptions {
        servers: vec![server],
        final_: final_tag,
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
                tag: id,
                kind: DnsServerKind::Hosts,
            });
        }
        C::DNS_TYPE_LOCAL => {
            return Ok(DnsServer {
                tag: id,
                kind: DnsServerKind::Local,
            });
        }
        _ => {}
    }

    if let Ok(parsed) = Url::parse(&raw.server) {
        if !parsed.scheme().is_empty() && parsed.has_host() {
            return build_dns_server_from_url(parsed, id, via);
        }
    }

    let (host, port_str) = match raw.server.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            (h.to_owned(), p.to_owned())
        }
        _ => (raw.server.clone(), String::new()),
    };
    let port = parse_optional_port("dns.server", &port_str)?;
    Ok(DnsServer {
        tag: id,
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
                tag: id,
                kind: DnsServerKind::Https(RemoteHttpsDnsServer {
                    server: host,
                    server_port: port,
                    via: via.to_owned(),
                    path,
                }),
            })
        }
        "udp" => Ok(DnsServer {
            tag: id,
            kind: DnsServerKind::Udp(RemoteDnsServer {
                server: host,
                server_port: if port == 0 { 53 } else { port },
                via: via.to_owned(),
            }),
        }),
        "tcp" => Ok(DnsServer {
            tag: id,
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
    tun_tag: &str,
) -> Result<Vec<Rule>, HammerError> {
    let sniff_timeout = parse_optional_duration("tun.sniff_timeout", &raw_tun.sniff_timeout)?;
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
            tun_tag,
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
            tun_tag,
            RuleActionKind::Resolve(ResolveActionOptions {
                strategy: domain_strategy,
            }),
        ));
    }
    if raw_tun.udp_disable_domain_unmapping {
        rules.push(tun_rule(
            tun_tag,
            RuleActionKind::RouteOptions(RouteOptionsActionOptions {
                udp_disable_domain_unmapping: true,
            }),
        ));
    }
    Ok(rules)
}

fn tun_rule(tun_tag: &str, action: RuleActionKind) -> Rule {
    Rule {
        default_options: DefaultRule {
            inbound: vec![tun_tag.to_owned()],
            action,
            ..Default::default()
        },
    }
}

fn protocol_rule(protocol: &str, action: RuleActionKind) -> Rule {
    Rule {
        default_options: DefaultRule {
            protocol: vec![protocol.to_owned()],
            action,
            ..Default::default()
        },
    }
}

fn build_user_rules(raw: &[RawRouteRule]) -> Result<Vec<Rule>, HammerError> {
    raw.iter()
        .enumerate()
        .map(|(idx, raw)| build_user_rule(idx, raw))
        .collect()
}

#[cfg(feature = "wireguard")]
fn build_endpoints(raw: Vec<RawEndpoint>) -> Result<Vec<Endpoint>, HammerError> {
    raw.into_iter()
        .enumerate()
        .map(|(idx, item)| match item {
            RawEndpoint::Wireguard(wg) => build_wireguard_endpoint(idx, wg),
        })
        .collect()
}

#[cfg(feature = "wireguard")]
fn build_wireguard_endpoint(
    idx: usize,
    mut raw: RawWireguardEndpoint,
) -> Result<Endpoint, HammerError> {
    let tag = if raw.id.is_empty() {
        return Err(HammerError::config_validation(format!(
            "endpoints[{idx}].id is required"
        )));
    } else {
        std::mem::take(&mut raw.id)
    };
    let private_key = parse_base64_key(
        &format!("endpoints[{idx}].private_key"),
        raw.private_key.trim(),
    )?;
    let listen_port = raw.listen_port.unwrap_or(0);
    let mtu = raw.mtu.unwrap_or(C::DEFAULT_WIREGUARD_MTU);
    let address = parse_ipnet_list(&format!("endpoints[{idx}].address"), &raw.address)?;
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
        tag,
        kind: EndpointKind::Wireguard(WireguardEndpointOptions {
            private_key,
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
    let public_key = parse_base64_key(&format!("{prefix}.public_key"), raw.public_key.trim())?;
    let pre_shared_key = match raw.pre_shared_key.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => Some(parse_base64_key(
            &format!("{prefix}.pre_shared_key"),
            value,
        )?),
        _ => None,
    };
    if raw.address.is_empty() {
        return Err(HammerError::config_validation(format!(
            "{prefix}.address is required"
        )));
    }
    let endpoint = parse_socket_addr(&format!("{prefix}.address"), &raw.address, raw.port)?;
    let allowed_ips = parse_ipnet_list(&format!("{prefix}.allowed_ips"), &raw.allowed_ips)?;
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

fn build_user_rule(idx: usize, raw: &RawRouteRule) -> Result<Rule, HammerError> {
    if raw.outbound.is_empty() {
        return Err(HammerError::config_validation(format!(
            "route.rules[{idx}].outbound is required",
        )));
    }
    let has_filter = !raw.inbound.is_empty()
        || !raw.protocol.is_empty()
        || !raw.domain.is_empty()
        || !raw.domain_suffix.is_empty()
        || !raw.domain_keyword.is_empty()
        || !raw.ip_cidr.is_empty();
    if !has_filter {
        return Err(HammerError::config_validation(format!(
            "route.rules[{idx}] requires at least one matcher (inbound/protocol/domain/domain_suffix/domain_keyword/ip_cidr)",
        )));
    }
    let ip_cidr = raw
        .ip_cidr
        .iter()
        .map(|raw_cidr| {
            raw_cidr.parse::<IpNet>().map_err(|err| {
                HammerError::config_validation(format!(
                    "route.rules[{idx}].ip_cidr {raw_cidr:?}: {err}",
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Rule {
        default_options: DefaultRule {
            inbound: raw.inbound.clone(),
            protocol: raw.protocol.clone(),
            domain: raw.domain.clone(),
            domain_suffix: raw.domain_suffix.clone(),
            domain_keyword: raw.domain_keyword.clone(),
            ip_cidr,
            action: RuleActionKind::Route(RouteActionOptions {
                outbound: raw.outbound.clone(),
            }),
        },
    })
}
