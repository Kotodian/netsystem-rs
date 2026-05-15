use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{
    DnsTransport, DnsTransportComponent, Lifecycle, Network, PlatformInterface, ProxyStream,
    StartStage,
};
use hammer_core::config::{DnsServerKind, RemoteHttpsDnsServer};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::protocol::dns::MessageExt;
use hickory_proto::op::Message;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_rustls::TlsConnector;

use crate::OutboundManager;
use crate::socket_protector::SocketProtector;
use crate::tls::{BasicClientTlsConfig, tls13_client_config};

use super::{
    dependency_with_bootstrap, destination_via_bootstrap, direct_tcp_connect, host_header,
    outbound_by_id, resolve_first, socks_to_socket_addr,
};

const DOH_MIME_TYPE: &str = "application/dns-message";

#[hammer_component_macros::hammer_component(
    dns_transport,
    name = "https",
    builder = build_transport,
    metrics = ("dns", "transport")
)]
pub struct HttpsDnsTransport {
    id: String,
    server: String,
    port: u16,
    path: String,
    via: String,
    url: String,
    dependencies: Vec<String>,
    outbound: Option<Arc<OutboundManager>>,
    bootstrap: Option<DnsTransportComponent>,
    protector: SocketProtector,
    platform: Option<Arc<dyn PlatformInterface>>,
}

impl HttpsDnsTransport {
    pub fn new(id: impl Into<String>, options: &RemoteHttpsDnsServer) -> Self {
        Self::new_with_runtime(id, options, None, None, SocketProtector::default())
    }

    pub(in crate::dns) fn new_with_runtime(
        id: impl Into<String>,
        options: &RemoteHttpsDnsServer,
        outbound: Option<Arc<OutboundManager>>,
        bootstrap: Option<DnsTransportComponent>,
        protector: SocketProtector,
    ) -> Self {
        let host = if options.server_port == 443 {
            options.server.clone()
        } else {
            format!("{}:{}", options.server, options.server_port)
        };
        let path = if options.path.is_empty() {
            "/dns-query"
        } else {
            &options.path
        };
        Self {
            id: id.into(),
            server: options.server.clone(),
            port: options.server_port,
            path: path.to_owned(),
            via: options.via.clone(),
            url: format!("https://{host}{path}"),
            dependencies: dependency_with_bootstrap(&options.via, &options.domain_resolver),
            platform: protector.platform(),
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
) -> HammerResult<Arc<HttpsDnsTransport>> {
    match kind {
        DnsServerKind::Https(options) => Ok(Arc::new(HttpsDnsTransport::new_with_runtime(
            id, options, outbound, bootstrap, protector,
        ))),
        _ => Err(HammerError::internal(
            "https DNS factory received wrong options",
        )),
    }
}

impl Lifecycle for HttpsDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/https"
    }
    fn start(&self, _stage: StartStage) -> HammerResult<()> {
        Ok(())
    }
    fn close(&self) -> HammerResult<()> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for HttpsDnsTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> HammerResult<Message> {
        let server =
            destination_via_bootstrap(self.bootstrap.as_ref(), &self.server, self.port).await?;
        let payload = MessageExt::to_bytes(&message)?;
        let bytes = doh_exchange_http2(
            self.outbound.as_ref(),
            &self.via,
            server,
            &self.server,
            &self.path,
            &self.url,
            payload,
            &self.protector,
            self.platform.clone(),
        )
        .await?;
        <Message as MessageExt>::from_bytes(&bytes)
    }
}

#[allow(clippy::too_many_arguments)]
async fn doh_exchange_http2(
    outbound: Option<&Arc<OutboundManager>>,
    via: &str,
    server: hammer_adapter::SocksAddr,
    server_name: &str,
    path: &str,
    url: &str,
    payload: Vec<u8>,
    protector: &SocketProtector,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> HammerResult<Bytes> {
    let host_name = server_name.to_owned();
    let stream: Box<dyn ProxyStream> = if via.is_empty() {
        let server = if let Some(server) = socks_to_socket_addr(&server) {
            server
        } else {
            resolve_first(&server.destination_host(), server.port).await?
        };
        Box::new(direct_tcp_connect(server, protector).await?)
    } else {
        outbound_by_id(outbound, via)?
            .runtime()
            .dial(Network::Tcp, server.clone(), &[])
            .await?
    };
    let tls_config = tls13_client_config(BasicClientTlsConfig {
        platform,
        alpn_protocols: Vec::new(),
    })?;
    let connector = TlsConnector::from(Arc::new(tls_config));
    let server_name = rustls::pki_types::ServerName::try_from(host_name.clone())
        .map_err(|err| HammerError::internal(format!("invalid DNS TLS server name: {err}")))?;
    let tls = connector
        .connect(server_name, stream)
        .await
        .map_err(|err| HammerError::internal(format!("connect HTTPS DNS TLS: {err}")))?;
    let io = TokioIo::new(tls);
    let (mut sender, connection) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .map_err(|err| HammerError::internal(format!("start HTTPS DNS h2: {err}")))?;
    crate::spawn::spawn(async move {
        let _ = connection.await;
    });
    let request = Request::post(path)
        .header(http::header::HOST, host_header(&host_name, server.port))
        .header(http::header::CONTENT_TYPE, DOH_MIME_TYPE)
        .header(http::header::ACCEPT, DOH_MIME_TYPE)
        .body(Full::new(Bytes::from(payload)))
        .map_err(|err| HammerError::internal(format!("build HTTPS DNS request {url}: {err}")))?;
    let response = sender
        .send_request(request)
        .await
        .map_err(|err| HammerError::internal(format!("send HTTPS DNS request {url}: {err}")))?;
    if !response.status().is_success() {
        return Err(HammerError::internal(format!(
            "unexpected DNS HTTPS status: {}",
            response.status()
        )));
    }
    response
        .into_body()
        .collect()
        .await
        .map(|body| body.to_bytes())
        .map_err(|e| HammerError::internal(format!("read HTTPS DNS response: {e}")))
}
