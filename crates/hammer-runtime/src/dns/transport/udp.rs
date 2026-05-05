use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use hammer_adapter::{DnsTransport, Lifecycle, OutboundManager as _, StartStage};
use hammer_core::config::{DnsServerKind, RemoteDnsServer};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::Message;
use tokio::net::UdpSocket;
use tracing::{debug, info};

use crate::OutboundManager;
use crate::dns::MessageExt;
use crate::socket_protector::SocketProtector;

use super::tcp::{tcp_exchange_direct, tcp_exchange_via_or_direct};
use super::{
    dependency_with_bootstrap, destination_via_bootstrap, resolve_first, udp_exchange_via,
};

pub struct UdpDnsTransport {
    id: String,
    server: String,
    port: u16,
    via: String,
    dependencies: Vec<String>,
    outbound: Option<Arc<OutboundManager>>,
    bootstrap: Option<Arc<dyn DnsTransport>>,
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
        bootstrap: Option<Arc<dyn DnsTransport>>,
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
    bootstrap: Option<Arc<dyn DnsTransport>>,
    protector: SocketProtector,
) -> Result<Arc<dyn DnsTransport>, HammerError> {
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
    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
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
            match outbound.listen_packet().await {
                Ok(_conn) => debug!("dns udp warm-up ready server={id} via={via}"),
                Err(err) => debug!("dns udp warm-up failed server={id} via={via}: {err}"),
            }
        });
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for UdpDnsTransport {
    fn type_name(&self) -> &str {
        "udp"
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
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
            .map_err(|e| HammerError::internal(format!("bind UDP DNS socket: {e}")))?;
        self.protector.protect(&socket)?;
        socket
            .connect(server)
            .await
            .map_err(|e| HammerError::internal(format!("connect UDP DNS socket: {e}")))?;
        socket
            .send(&MessageExt::to_bytes(&message)?)
            .await
            .map_err(|e| HammerError::internal(format!("write UDP DNS request: {e}")))?;
        let mut buf = vec![0_u8; 4096];
        let len = socket
            .recv(&mut buf)
            .await
            .map_err(|e| HammerError::internal(format!("read UDP DNS response: {e}")))?;
        let response = <Message as MessageExt>::from_bytes(&buf[..len])?;
        if response.metadata.truncation {
            info!("response truncated, retrying with TCP");
            return tcp_exchange_direct(&server, message, &self.protector).await;
        }
        Ok(response)
    }
}
