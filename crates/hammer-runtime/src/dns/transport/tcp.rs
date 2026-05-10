use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use hammer_adapter::{DnsTransport, DnsTransportComponent, Lifecycle, Network, StartStage};
use hammer_core::config::{DnsServerKind, RemoteDnsServer};
use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use hammer_core::protocol::dns::{
    encode_tcp_dns_query, read_tcp_dns_response, tcp_exchange_over_stream,
};
use hickory_proto::op::Message;

use crate::OutboundManager;
use crate::socket_protector::SocketProtector;

use super::{
    dependency_with_bootstrap, destination_via_bootstrap, direct_tcp_connect, outbound_by_id,
    resolve_first, socks_to_socket_addr,
};

#[hammer_component_macros::hammer_component(
    dns_transport,
    name = "tcp",
    builder = build_transport,
    metrics = ("dns", "transport")
)]
pub struct TcpDnsTransport {
    id: String,
    server: String,
    port: u16,
    via: String,
    dependencies: Vec<String>,
    outbound: Option<Arc<OutboundManager>>,
    bootstrap: Option<DnsTransportComponent>,
    protector: SocketProtector,
}

impl TcpDnsTransport {
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
    _logger: hammer_core::log::Logger,
    outbound: Option<Arc<OutboundManager>>,
    bootstrap: Option<DnsTransportComponent>,
    protector: SocketProtector,
) -> HammerResult<Arc<TcpDnsTransport>> {
    match kind {
        DnsServerKind::Tcp(options) => Ok(Arc::new(TcpDnsTransport::new_with_runtime(
            id, options, outbound, bootstrap, protector,
        ))),
        _ => Err(HammerError::internal(
            "tcp DNS factory received wrong options",
        )),
    }
}

impl Lifecycle for TcpDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/tcp"
    }
    fn start(&self, _stage: StartStage) -> HammerResult<()> {
        Ok(())
    }
    fn close(&self) -> HammerResult<()> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for TcpDnsTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> HammerResult<Message> {
        let server =
            destination_via_bootstrap(self.bootstrap.as_ref(), &self.server, self.port).await?;
        tcp_exchange_via_or_direct(
            self.outbound.as_ref(),
            &self.via,
            server,
            message,
            &self.protector,
        )
        .await
    }
}

pub(super) async fn tcp_exchange_direct(
    server: &SocketAddr,
    message: Message,
    protector: &SocketProtector,
) -> HammerResult<Message> {
    let mut stream = direct_tcp_connect(*server, protector)
        .await
        .with_context(|| "connect TCP DNS socket")?;
    tcp_exchange_over_stream(&mut stream, message).await
}

pub(super) async fn tcp_exchange_via_or_direct(
    outbound: Option<&Arc<OutboundManager>>,
    via: &str,
    server: hammer_adapter::SocksAddr,
    message: Message,
    protector: &SocketProtector,
) -> HammerResult<Message> {
    if via.is_empty() {
        let server = if let Some(server) = socks_to_socket_addr(&server) {
            server
        } else {
            resolve_first(&server.destination_host(), server.port).await?
        };
        return tcp_exchange_direct(&server, message, protector).await;
    }
    let frame = encode_tcp_dns_query(&message)?;
    let mut stream = outbound_by_id(outbound, via)?
        .runtime()
        .dial(Network::Tcp, server, &frame)
        .await?;
    read_tcp_dns_response(&mut stream).await
}
