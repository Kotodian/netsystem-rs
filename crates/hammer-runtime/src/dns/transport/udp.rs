use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use hammer_adapter::{
    DnsTransport, DnsTransportComponent, Lifecycle, OutboundManager as _, StartStage,
};
use hammer_core::config::{DnsServerKind, RemoteDnsServer};
use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use hammer_core::protocol::dns::MessageExt;
use hickory_proto::op::Message;
use tokio::net::UdpSocket;
use tracing::{debug, info};

use crate::OutboundManager;
use crate::socket_protector::SocketProtector;

use super::tcp::{tcp_exchange_direct, tcp_exchange_via_or_direct};
use super::{
    dependency_with_bootstrap, destination_via_bootstrap, resolve_first, udp_exchange_via,
};

#[hammer_component_macros::hammer_component(
    dns_transport,
    name = "udp",
    builder = build_transport,
    metrics = ("dns", "transport")
)]
pub struct UdpDnsTransport {
    id: String,
    server: String,
    port: u16,
    via: String,
    dependencies: Vec<String>,
    outbound: Option<Arc<OutboundManager>>,
    bootstrap: Option<DnsTransportComponent>,
    protector: SocketProtector,
}

impl UdpDnsTransport {
    pub fn new(id: impl Into<String>, server: String, port: u16, _logger: Logger) -> Self {
        Self {
            id: id.into(),
            server,
            port,
            via: String::new(),
            dependencies: Vec::new(),
            outbound: None,
            bootstrap: None,
            protector: SocketProtector::default(),
        }
    }

    pub(in crate::dns) fn new_with_runtime(
        id: impl Into<String>,
        options: &RemoteDnsServer,
        _logger: Logger,
        outbound: Option<Arc<OutboundManager>>,
        bootstrap: Option<DnsTransportComponent>,
        protector: SocketProtector,
    ) -> Self {
        Self {
            id: id.into(),
            server: options.server.clone(),
            port: options.server_port,
            via: options.via.clone(),
            dependencies: dependency_with_bootstrap(&options.via, &options.domain_resolver),
            outbound,
            bootstrap,
            protector,
        }
    }
}

pub(crate) fn build_transport(
    id: String,
    kind: &DnsServerKind,
    logger: Logger,
    outbound: Option<Arc<OutboundManager>>,
    bootstrap: Option<DnsTransportComponent>,
    protector: SocketProtector,
) -> HammerResult<Arc<UdpDnsTransport>> {
    match kind {
        DnsServerKind::Udp(options) => Ok(Arc::new(UdpDnsTransport::new_with_runtime(
            id, options, logger, outbound, bootstrap, protector,
        ))),
        _ => Err(HammerError::internal(
            "udp DNS factory received wrong options",
        )),
    }
}

impl Lifecycle for UdpDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/udp"
    }
    fn start(&self, stage: StartStage) -> HammerResult<()> {
        if stage != StartStage::Start || self.via.is_empty() {
            return Ok(());
        }
        let Some(outbound) = self.outbound.clone() else {
            return Ok(());
        };
        let id = self.id.clone();
        let via = self.via.clone();
        crate::spawn::spawn(async move {
            let Some(outbound) = outbound.get(&via) else {
                debug!("dns udp warm-up skipped server={id} via={via}: outbound not found");
                return;
            };
            match outbound.runtime().listen_packet().await {
                Ok(_conn) => debug!("dns udp warm-up ready server={id} via={via}"),
                Err(err) => debug!("dns udp warm-up failed server={id} via={via}: {err}"),
            }
        });
        Ok(())
    }
    fn close(&self) -> HammerResult<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
impl DnsTransport for UdpDnsTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> HammerResult<Message> {
        if !self.via.is_empty() {
            let server =
                destination_via_bootstrap(self.bootstrap.as_ref(), &self.server, self.port).await?;
            let response = udp_exchange_via(
                self.outbound.as_ref(),
                &self.via,
                server.clone(),
                &MessageExt::to_bytes(&message)?,
            )
            .await?;
            let response = <Message as MessageExt>::from_bytes(&response)?;
            if response.metadata.truncation {
                info!("response truncated, retrying with TCP");
                return tcp_exchange_via_or_direct(
                    self.outbound.as_ref(),
                    &self.via,
                    server,
                    message,
                    &self.protector,
                )
                .await;
            }
            return Ok(response);
        }
        let server = resolve_first(&self.server, self.port).await?;
        let bind = if server.is_ipv6() {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
        } else {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        };
        let socket = UdpSocket::bind(bind)
            .await
            .with_context(|| "bind UDP DNS socket")?;
        self.protector.protect(&socket)?;
        socket
            .connect(server)
            .await
            .with_context(|| "connect UDP DNS socket")?;
        socket
            .send(&MessageExt::to_bytes(&message)?)
            .await
            .with_context(|| "write UDP DNS request")?;
        let mut buf = vec![0_u8; 4096];
        let len = socket
            .recv(&mut buf)
            .await
            .with_context(|| "read UDP DNS response")?;
        let response = <Message as MessageExt>::from_bytes(&buf[..len])?;
        if response.metadata.truncation {
            info!("response truncated, retrying with TCP");
            return tcp_exchange_direct(&server, message, &self.protector).await;
        }
        Ok(response)
    }
}
