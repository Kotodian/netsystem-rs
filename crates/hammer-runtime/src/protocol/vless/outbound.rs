use std::sync::Arc;

use async_trait::async_trait;
use hammer_adapter::{Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::{OutboundKind, VlessOutboundOptions};
use hammer_core::error::{HammerError, HammerResult, WithContext};
use hammer_core::log::Logger;
use hammer_core::protocol::vless::{VlessCommand, VlessStream, encode_request};
use rustls::pki_types::ServerName;
use tokio::io::AsyncWriteExt;
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
            let stream = self.connect_tls(stream).await?;
            return write_request_and_wrap(stream, &request).await;
        }
        write_request_and_wrap(stream, &request).await
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        Err(HammerError::internal(
            "vless udp packet connections are not supported yet",
        ))
    }
}

impl VlessOutbound {
    async fn connect_tls(&self, stream: TcpStream) -> HammerResult<TlsStream<TcpStream>> {
        let tls = &self.options.tls;
        let config = outbound_client_config(OutboundClientTlsConfig {
            platform: self.connector.platform(),
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
}

async fn write_request_and_wrap<S>(
    mut stream: S,
    request: &[u8],
) -> HammerResult<Box<dyn ProxyStream>>
where
    S: ProxyStream,
{
    stream
        .write_all(request)
        .await
        .with_context(|| "vless tcp write request")?;
    Ok(Box::new(VlessStream::new(stream)))
}
