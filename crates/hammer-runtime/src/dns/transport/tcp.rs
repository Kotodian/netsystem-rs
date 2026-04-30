use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use hammer_adapter::{DnsTransport, Lifecycle, Network, StartStage};
use hammer_core::config::{DnsServerKind, RemoteDnsServer};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::OutboundManager;
use crate::dns::MessageExt;
use crate::socket_protector::SocketProtector;

use super::{
    dependency, direct_tcp_connect, encode_tcp_dns_query, outbound_by_id, read_tcp_dns_response,
    resolve_first, socket_addr_to_socks,
};

pub struct TcpDnsTransport {
    id: String,
    server: String,
    port: u16,
    via: String,
    dependencies: Vec<String>,
    outbound: Option<Arc<OutboundManager>>,
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
            protector: SocketProtector::default(),
        }
    }

    pub(in crate::dns) fn new_with_runtime(
        id: impl Into<String>,
        options: &RemoteDnsServer,
        outbound: Option<Arc<OutboundManager>>,
        protector: SocketProtector,
    ) -> Self {
        Self {
            id: id.into(),
            server: options.server.clone(),
            port: options.server_port,
            via: options.via.clone(),
            dependencies: dependency(&options.via),
            outbound,
            protector,
        }
    }
}

pub(crate) fn build_transport(
    id: String,
    kind: &DnsServerKind,
    _logger: hammer_core::log::Logger,
    outbound: Option<Arc<OutboundManager>>,
    protector: SocketProtector,
) -> Result<Arc<dyn DnsTransport>, HammerError> {
    match kind {
        DnsServerKind::Tcp(options) => Ok(Arc::new(TcpDnsTransport::new_with_runtime(
            id, options, outbound, protector,
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
    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for TcpDnsTransport {
    fn type_name(&self) -> &str {
        "tcp"
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        let server = resolve_first(&self.server, self.port).await?;
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
) -> Result<Message, HammerError> {
    let mut stream = direct_tcp_connect(*server, protector)
        .await
        .map_err(|e| HammerError::internal(format!("connect TCP DNS socket: {e}")))?;
    tcp_exchange_over_stream(&mut stream, message).await
}

pub(super) async fn tcp_exchange_via_or_direct(
    outbound: Option<&Arc<OutboundManager>>,
    via: &str,
    server: SocketAddr,
    message: Message,
    protector: &SocketProtector,
) -> Result<Message, HammerError> {
    if via.is_empty() {
        return tcp_exchange_direct(&server, message, protector).await;
    }
    let frame = encode_tcp_dns_query(&message)?;
    let mut stream = outbound_by_id(outbound, via)?
        .dial(Network::Tcp, socket_addr_to_socks(server), &frame)
        .await?;
    read_tcp_dns_response(&mut stream).await
}

async fn tcp_exchange_over_stream<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    message: Message,
) -> Result<Message, HammerError> {
    let bytes = MessageExt::to_bytes(&message)?;
    let len = u16::try_from(bytes.len())
        .map_err(|_| HammerError::internal("DNS message exceeds TCP frame limit"))?;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| HammerError::internal(format!("write TCP DNS length: {e}")))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| HammerError::internal(format!("write TCP DNS request: {e}")))?;
    let mut len_buf = [0_u8; 2];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS length: {e}")))?;
    let len = usize::from(u16::from_be_bytes(len_buf));
    let mut payload = vec![0_u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS response: {e}")))?;
    <Message as MessageExt>::from_bytes(&payload)
}
