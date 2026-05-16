use std::sync::Arc;
use std::task::{Context, Poll};
use std::{io, pin::Pin};

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{Network, Outbound, ProxyDatagram, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::{OutboundKind, VlessOutboundOptions};
use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use hammer_core::protocol::vless::{
    VlessCommand, VlessStream, encode_request, encode_udp_packet, read_udp_packet,
};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::protocol::server_tcp::ServerTcpConnector;
use crate::socket_protector::SocketProtector;
use crate::tls::{OutboundClientTlsConfig, outbound_client_config};

#[hammer_component_macros::hammer_component(
    outbound,
    name = "vless",
    builder = build_outbound,
    metrics = ("outbound", "outbound")
)]
pub struct VlessOutbound {
    id: String,
    options: VlessOutboundOptions,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    connector: ServerTcpConnector,
}

impl VlessOutbound {
    pub fn new(logger: Logger, id: String, options: VlessOutboundOptions) -> HammerResult<Self> {
        Self::new_with_protector(logger, id, options, SocketProtector::default())
    }

    pub(crate) fn new_with_protector(
        _logger: Logger,
        id: String,
        options: VlessOutboundOptions,
        protector: SocketProtector,
    ) -> HammerResult<Self> {
        let connector = ServerTcpConnector::builder()
            .server(options.server.clone())
            .server_port(options.server_port)
            .protector(protector)
            .build()?;
        Ok(Self {
            id,
            options,
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            connector,
        })
    }

    fn validate_runtime_options(&self) -> HammerResult<()> {
        if self.options.flow.is_some() {
            return Err(HammerError::config_validation(
                "vless flow xtls-rprx-vision is parsed but not supported by the runtime yet",
            ));
        }
        let tls = &self.options.tls;
        if tls.reality.is_some() {
            return Err(HammerError::config_validation(
                "vless tls.reality is parsed but not supported by the runtime yet",
            ));
        }
        if tls.fragment.is_some() || tls.record_fragment {
            return Err(HammerError::config_validation(
                "vless tls fragmentation is parsed but not supported by the runtime yet",
            ));
        }
        Ok(())
    }
}

pub(crate) fn build_outbound(
    logger: Logger,
    id: String,
    kind: &OutboundKind,
    protector: SocketProtector,
) -> HammerResult<Arc<VlessOutbound>> {
    match kind {
        OutboundKind::Vless(options) => Ok(Arc::new(VlessOutbound::new_with_protector(
            logger,
            id,
            options.clone(),
            protector,
        )?)),
        _ => Err(HammerError::internal(
            "vless factory received wrong options",
        )),
    }
}

#[async_trait]
impl Outbound for VlessOutbound {
    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> HammerResult<Box<dyn ProxyStream>> {
        if network != Network::Tcp {
            return Err(HammerError::internal("vless dial only supports tcp"));
        }
        self.validate_runtime_options()?;
        let request = encode_request(
            &self.options.uuid,
            VlessCommand::Tcp,
            &destination,
            initial_payload,
        )?;
        let stream = self.connector.connect("vless").await?;
        if self.options.tls.enabled {
            let stream = connect_tls(&self.connector, &self.options, stream).await?;
            let stream = write_request_and_wrap(stream, &request).await?;
            return Ok(Box::new(stream));
        }
        let stream = write_request_and_wrap(stream, &request).await?;
        Ok(Box::new(stream))
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        self.validate_runtime_options()?;
        Ok(Box::new(VlessPacketConn {
            options: self.options.clone(),
            connector: self.connector.clone(),
            destination: None,
            stream: None,
        }))
    }
}

enum VlessServerStream {
    Plain(VlessStream<TcpStream>),
    Tls(VlessStream<TlsStream<TcpStream>>),
}

impl AsyncRead for VlessServerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for VlessServerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

struct VlessPacketConn {
    options: VlessOutboundOptions,
    connector: ServerTcpConnector,
    destination: Option<SocksAddr>,
    stream: Option<VlessServerStream>,
}

impl VlessPacketConn {
    async fn ensure_stream(&mut self, destination: &SocksAddr) -> HammerResult<()> {
        if self.destination.as_ref() == Some(destination) && self.stream.is_some() {
            return Ok(());
        }

        let request = encode_request(&self.options.uuid, VlessCommand::Udp, destination, &[])?;
        let stream = self.connector.connect("vless udp").await?;
        let stream = if self.options.tls.enabled {
            let stream = connect_tls(&self.connector, &self.options, stream).await?;
            let stream = write_request_and_wrap(stream, &request).await?;
            VlessServerStream::Tls(stream)
        } else {
            let stream = write_request_and_wrap(stream, &request).await?;
            VlessServerStream::Plain(stream)
        };
        self.destination = Some(destination.clone());
        self.stream = Some(stream);
        Ok(())
    }
}

#[async_trait]
impl ProxyPacketConn for VlessPacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: Bytes) -> HammerResult<()> {
        self.ensure_stream(&destination).await?;
        let packet = encode_udp_packet(&payload)?;
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| HammerError::internal("vless udp stream not initialized"))?;
        stream
            .write_all(&packet)
            .await
            .with_context(|| "vless udp write packet")
    }

    async fn recv_from(&mut self) -> HammerResult<ProxyDatagram> {
        let destination = self
            .destination
            .clone()
            .ok_or_else(|| HammerError::internal("vless udp recv before send"))?;
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| HammerError::internal("vless udp stream not initialized"))?;
        let payload = read_udp_packet(stream).await?;
        Ok(ProxyDatagram {
            destination,
            payload: Bytes::from(payload),
        })
    }
}

async fn write_request_and_wrap<S>(mut stream: S, request: &[u8]) -> HammerResult<VlessStream<S>>
where
    S: ProxyStream,
{
    stream
        .write_all(request)
        .await
        .with_context(|| "vless tcp write request")?;
    Ok(VlessStream::new(stream))
}

async fn connect_tls(
    connector: &ServerTcpConnector,
    options: &VlessOutboundOptions,
    stream: TcpStream,
) -> HammerResult<TlsStream<TcpStream>> {
    let tls = &options.tls;
    let config = outbound_client_config(OutboundClientTlsConfig {
        platform: connector.platform(),
        insecure: tls.insecure,
        alpn_protocols: tls
            .alpn
            .iter()
            .map(|item| item.as_bytes().to_vec())
            .collect(),
        server_fingerprints: tls.server_fingerprints.clone(),
        client_auth: tls.client_auth.clone(),
        ech: tls.ech.clone(),
        #[cfg(feature = "tls-utls")]
        ech_retry_configs: None,
        utls: tls.utls.clone(),
    })?;
    let server_name = ServerName::try_from(tls.server_name.clone()).map_err(|_| {
        HammerError::config_validation(
            "vless.tls.server_name must be a valid DNS name or IP address",
        )
    })?;
    TlsConnector::from(Arc::new(config))
        .connect(server_name, stream)
        .await
        .with_context(|| "vless tls connect")
}
