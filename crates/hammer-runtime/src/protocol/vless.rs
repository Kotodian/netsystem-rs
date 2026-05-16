use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use hammer_adapter::{Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::{OutboundKind, VlessOutboundOptions};
use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::protocol::server_tcp::ServerTcpConnector;
use crate::socket_protector::SocketProtector;

const VLESS_VERSION: u8 = 0;
const VLESS_COMMAND_TCP: u8 = 1;
const VLESS_ADDRESS_IPV4: u8 = 1;
const VLESS_ADDRESS_DOMAIN: u8 = 2;
const VLESS_ADDRESS_IPV6: u8 = 3;

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
            networks: vec![Network::Tcp],
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
        if tls.enabled {
            return Err(HammerError::config_validation(
                "vless tls is parsed but not supported by the runtime yet",
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
        let mut stream = self.connector.connect("vless").await?;
        let request = encode_tcp_request(&self.options, &destination, initial_payload)?;
        stream
            .write_all(&request)
            .await
            .with_context(|| "vless tcp write request")?;
        Ok(Box::new(VlessTcpStream::new(stream)))
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        Err(HammerError::internal(
            "vless udp packet connections are not supported yet",
        ))
    }
}

fn encode_tcp_request(
    options: &VlessOutboundOptions,
    destination: &SocksAddr,
    initial_payload: &[u8],
) -> HammerResult<Vec<u8>> {
    let mut out = Vec::with_capacity(24 + initial_payload.len());
    out.push(VLESS_VERSION);
    out.extend_from_slice(&options.uuid);
    out.push(0);
    out.push(VLESS_COMMAND_TCP);
    out.extend_from_slice(&destination.port.to_be_bytes());
    encode_address(destination, &mut out)?;
    out.extend_from_slice(initial_payload);
    Ok(out)
}

fn encode_address(destination: &SocksAddr, out: &mut Vec<u8>) -> HammerResult<()> {
    if let Some(domain) = destination.domain.as_deref() {
        let len = u8::try_from(domain.len()).map_err(|_| {
            HammerError::config_validation("vless destination domain must fit in 255 bytes")
        })?;
        out.push(VLESS_ADDRESS_DOMAIN);
        out.push(len);
        out.extend_from_slice(domain.as_bytes());
        return Ok(());
    }

    match destination.host {
        IpAddr::V4(ip) => {
            out.push(VLESS_ADDRESS_IPV4);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.push(VLESS_ADDRESS_IPV6);
            out.extend_from_slice(&ip.octets());
        }
    }
    Ok(())
}

struct VlessTcpStream {
    inner: TcpStream,
    response: ResponseHeaderState,
}

impl VlessTcpStream {
    fn new(inner: TcpStream) -> Self {
        Self {
            inner,
            response: ResponseHeaderState::new(),
        }
    }
}

impl AsyncRead for VlessTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while !this.response.is_done() {
            match this.response.poll_read(&mut this.inner, cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for VlessTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

struct ResponseHeaderState {
    bytes: Vec<u8>,
    read: usize,
    done: bool,
}

impl ResponseHeaderState {
    fn new() -> Self {
        Self {
            bytes: vec![0; 2],
            read: 0,
            done: false,
        }
    }

    fn is_done(&self) -> bool {
        self.done
    }

    fn poll_read(&mut self, stream: &mut TcpStream, cx: &mut Context<'_>) -> io::Result<Poll<()>> {
        let target = self.bytes.len();
        let before = self.read;
        let mut buf = ReadBuf::new(&mut self.bytes[self.read..target]);
        match Pin::new(stream).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Err(err),
            Poll::Pending => return Ok(Poll::Pending),
        }
        let read_now = buf.filled().len();
        if read_now == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "vless response header eof",
            ));
        }
        self.read = before + read_now;
        if self.read == 2 && self.bytes.len() == 2 {
            if self.bytes[0] != VLESS_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid vless response version",
                ));
            }
            let addon_len = self.bytes[1] as usize;
            self.bytes.resize(2 + addon_len, 0);
        }
        if self.read == self.bytes.len() {
            self.done = true;
        }
        Ok(Poll::Ready(()))
    }
}
