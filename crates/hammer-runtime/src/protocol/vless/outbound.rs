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
    FLOW_XTLS_RPRX_VISION, VlessCommand, VlessRequestBuilder, VlessStream, VlessStreamBuilder,
    encode_udp_packet, read_udp_packet,
};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::protocol::server_tcp::ServerTcpConnector;
use crate::socket_protector::SocketProtector;
use crate::tls::{OutboundClientTlsConfig, TlsClientStream, outbound_client_stream};

const TLS_RECORD_FRAGMENT_SIZE: usize = 32;

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
            networks: options.network.clone(),
            options,
            dependencies: Vec::new(),
            connector,
        })
    }

    fn validate_runtime_options(&self) -> HammerResult<()> {
        if let Some(flow) = self.options.flow.as_deref()
            && flow != FLOW_XTLS_RPRX_VISION
        {
            return Err(HammerError::config_validation(format!(
                "unsupported vless flow: {flow}"
            )));
        }
        let tls = &self.options.tls;
        if tls.reality.is_some() {
            if !tls.enabled {
                return Err(HammerError::config_validation(
                    "vless tls.reality requires tls.enabled",
                ));
            }
            if tls.utls.is_none() {
                return Err(HammerError::config_validation(
                    "vless tls.reality requires tls.utls",
                ));
            }
            if tls.ech.is_some() {
                return Err(HammerError::config_validation(
                    "vless tls.reality cannot be combined with tls.ech",
                ));
            }
            #[cfg(not(feature = "tls-utls"))]
            {
                return Err(HammerError::config_validation(
                    "vless tls.reality requires the hammer-runtime tls-utls feature",
                ));
            }
        }
        if tls.fragment.is_some() {
            return Err(HammerError::config_validation(
                "vless tls.fragment is parsed but not supported by the runtime yet",
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
        if !self.options.network.contains(&Network::Tcp) {
            return Err(HammerError::config_validation(
                "vless tcp is disabled by network",
            ));
        }
        self.validate_runtime_options()?;
        let initial_payload_in_header = if is_vision_flow(&self.options) {
            &[][..]
        } else {
            initial_payload
        };
        let request = VlessRequestBuilder::new(&self.options.uuid, VlessCommand::Tcp, &destination)
            .optional_flow(self.options.flow.as_deref())
            .initial_payload(initial_payload_in_header)
            .encode()?;
        let stream = self.connector.connect("vless").await?;
        if self.options.tls.enabled {
            let stream = connect_tls(&self.connector, &self.options, stream).await?;
            let stream =
                write_request_and_wrap(stream, &self.options, &request, initial_payload).await?;
            return Ok(Box::new(stream));
        }
        let stream =
            write_request_and_wrap(stream, &self.options, &request, initial_payload).await?;
        Ok(Box::new(stream))
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        if !self.options.network.contains(&Network::Udp) {
            return Err(HammerError::config_validation(
                "vless udp is disabled by network",
            ));
        }
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
    Tls(VlessStream<TlsClientStream>),
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

        let request = VlessRequestBuilder::new(&self.options.uuid, VlessCommand::Udp, destination)
            .optional_flow(self.options.flow.as_deref())
            .encode()?;
        let stream = self.connector.connect("vless udp").await?;
        let stream = if self.options.tls.enabled {
            let stream = connect_tls(&self.connector, &self.options, stream).await?;
            let stream = write_request_and_wrap(stream, &self.options, &request, &[]).await?;
            VlessServerStream::Tls(stream)
        } else {
            let stream = write_request_and_wrap(stream, &self.options, &request, &[]).await?;
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

async fn write_request_and_wrap<S>(
    mut stream: S,
    options: &VlessOutboundOptions,
    request: &[u8],
    initial_payload: &[u8],
) -> HammerResult<VlessStream<S>>
where
    S: ProxyStream,
{
    stream
        .write_all(request)
        .await
        .with_context(|| "vless tcp write request")?;
    let mut stream = if is_vision_flow(options) {
        VlessStreamBuilder::new(stream)
            .vision(&options.uuid)
            .build()
    } else {
        VlessStream::new(stream)
    };
    if is_vision_flow(options) && !initial_payload.is_empty() {
        stream
            .write_all(initial_payload)
            .await
            .with_context(|| "vless vision write initial payload")?;
    }
    Ok(stream)
}

fn is_vision_flow(options: &VlessOutboundOptions) -> bool {
    options.flow.as_deref() == Some(FLOW_XTLS_RPRX_VISION)
}

async fn connect_tls(
    connector: &ServerTcpConnector,
    options: &VlessOutboundOptions,
    stream: TcpStream,
) -> HammerResult<TlsClientStream> {
    let tls = &options.tls;
    let server_name = ServerName::try_from(tls.server_name.clone()).map_err(|_| {
        HammerError::config_validation(
            "vless.tls.server_name must be a valid DNS name or IP address",
        )
    })?;
    outbound_client_stream(
        OutboundClientTlsConfig {
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
            max_fragment_size: tls.record_fragment.then_some(TLS_RECORD_FRAGMENT_SIZE),
            #[cfg(feature = "tls-utls")]
            ech_retry_configs: None,
            #[cfg(feature = "tls-utls")]
            reality: tls.reality.clone(),
            utls: tls.utls.clone(),
        },
        server_name,
        stream,
    )
    .await
    .with_context(|| "vless tls connect")
}
