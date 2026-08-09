use std::cell::UnsafeCell;
use std::sync::Arc;
use std::thread::{self, ThreadId};

use hammer_infra::pool::{Index, Pool};
use hammer_runtime::Engine;
use hammer_runtime::app::{AppSessionConfig, ApplicationId};
use prost::Message;
use quinn_proto::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use quinn_proto::rustls::server::WebPkiClientVerifier;
use quinn_proto::rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use thiserror::Error;

use crate::listener::{QUIC_MAIN, QuicMain};

pub(crate) const QUIC_CONFIG_CAPACITY: usize = 1_024;
const DEFAULT_CONNECTION_TIMEOUT: u32 = 30_000;
const DEFAULT_MAX_STREAMS_BIDI: u32 = 100;
const DEFAULT_MAX_STREAMS_UNI: u32 = 100;

/// Generation-checked identity for one QUIC client or server configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ConfigId(u64);

impl ConfigId {
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    #[inline]
    const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

/// Application-selected QUIC transport limits.
///
/// The fields correspond to VPP's `transport_endpt_cfg_quic_t`. Other Quinn
/// engine knobs remain plugin policy and are not exposed through the
/// Application configuration surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub connection_timeout: u32,
    pub max_concurrent_bidi_streams: u32,
    pub max_concurrent_uni_streams: u32,
}

impl TransportConfig {
    pub const fn new(
        connection_timeout: u32,
        max_concurrent_bidi_streams: u32,
        max_concurrent_uni_streams: u32,
    ) -> Self {
        Self {
            connection_timeout,
            max_concurrent_bidi_streams,
            max_concurrent_uni_streams,
        }
    }
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_MAX_STREAMS_BIDI,
            DEFAULT_MAX_STREAMS_UNI,
        )
    }
}

/// Input for an Application-owned QUIC server configuration.
pub struct ServerConfig {
    certificate_der: Vec<Vec<u8>>,
    private_key_der: Vec<u8>,
    client_trust_anchor_der: Vec<Vec<u8>>,
    alpn_protocols: Vec<Vec<u8>>,
    transport: TransportConfig,
}

impl ServerConfig {
    pub fn new(certificate_der: Vec<Vec<u8>>, private_key_der: Vec<u8>) -> Self {
        Self {
            certificate_der,
            private_key_der,
            client_trust_anchor_der: Vec::new(),
            alpn_protocols: Vec::new(),
            transport: TransportConfig::default(),
        }
    }

    pub fn with_client_authentication(mut self, trust_anchor_der: Vec<Vec<u8>>) -> Self {
        self.client_trust_anchor_der = trust_anchor_der;
        self
    }

    pub fn with_alpn_protocols(mut self, alpn_protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = alpn_protocols;
        self
    }

    pub fn with_transport_config(mut self, transport: TransportConfig) -> Self {
        self.transport = transport;
        self
    }
}

/// Input for an Application-owned QUIC client configuration.
pub struct ClientConfig {
    trust_anchor_der: Vec<Vec<u8>>,
    certificate_der: Vec<Vec<u8>>,
    private_key_der: Vec<u8>,
    alpn_protocols: Vec<Vec<u8>>,
    transport: TransportConfig,
}

impl ClientConfig {
    pub fn new(trust_anchor_der: Vec<Vec<u8>>) -> Self {
        Self {
            trust_anchor_der,
            certificate_der: Vec::new(),
            private_key_der: Vec::new(),
            alpn_protocols: Vec::new(),
            transport: TransportConfig::default(),
        }
    }

    pub fn with_identity(
        mut self,
        certificate_der: Vec<Vec<u8>>,
        private_key_der: Vec<u8>,
    ) -> Self {
        self.certificate_der = certificate_der;
        self.private_key_der = private_key_der;
        self
    }

    pub fn with_alpn_protocols(mut self, alpn_protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = alpn_protocols;
        self
    }

    pub fn with_transport_config(mut self, transport: TransportConfig) -> Self {
        self.transport = transport;
        self
    }
}

enum ConnectionConfig {
    Client {
        config: Arc<quinn_proto::ClientConfig>,
        transport: TransportConfig,
    },
    Server {
        config: Arc<quinn_proto::ServerConfig>,
        transport: TransportConfig,
    },
}

struct ConfigEntry {
    application: ApplicationId,
    config: ConnectionConfig,
}

pub(crate) struct QuicConfigRegistry {
    owner: ThreadId,
    state: UnsafeCell<Pool<ConfigEntry>>,
}

// SAFETY: every state access first verifies the owning Main Thread. When Data
// Workers exist, the Main Thread performs the access while WorkerBarrier holds
// them stopped; worker protocol state retains only immutable Arc configurations
// after this registry lookup.
unsafe impl Sync for QuicConfigRegistry {}

impl QuicConfigRegistry {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            owner: thread::current().id(),
            state: UnsafeCell::new(Pool::with_capacity(capacity)),
        }
    }

    fn register_server_config(
        &self,
        application: ApplicationId,
        config: ServerConfig,
        fifo_capacity: usize,
    ) -> Result<ConfigId, ConfigError> {
        let transport = config.transport;
        let config = Arc::new(build_server_config(config, fifo_capacity)?);
        self.insert(application, ConnectionConfig::Server { config, transport })
    }

    fn register_client_config(
        &self,
        application: ApplicationId,
        config: ClientConfig,
        fifo_capacity: usize,
    ) -> Result<ConfigId, ConfigError> {
        let transport = config.transport;
        let config = Arc::new(build_client_config(config, fifo_capacity)?);
        self.insert(application, ConnectionConfig::Client { config, transport })
    }

    pub(crate) fn server_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<Arc<quinn_proto::ServerConfig>, ConfigError> {
        self.with_entry(application, config, |entry| match &entry.config {
            ConnectionConfig::Server { config, .. } => Ok(Arc::clone(config)),
            ConnectionConfig::Client { .. } => Err(ConfigError::RoleMismatch { config }),
        })
    }

    pub(crate) fn client_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<Arc<quinn_proto::ClientConfig>, ConfigError> {
        self.with_entry(application, config, |entry| match &entry.config {
            ConnectionConfig::Client { config, .. } => Ok(Arc::clone(config)),
            ConnectionConfig::Server { .. } => Err(ConfigError::RoleMismatch { config }),
        })
    }

    pub(crate) fn transport_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<TransportConfig, ConfigError> {
        self.with_entry(application, config, |entry| match &entry.config {
            ConnectionConfig::Client { transport, .. }
            | ConnectionConfig::Server { transport, .. } => Ok(*transport),
        })
    }

    pub(crate) fn remove_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<(), ConfigError> {
        self.with_state_mut(|state| {
            config_entry(state, application, config)?;
            state
                .remove(config_index(config))
                .expect("validated QUIC configuration remains present until removal");
            Ok(())
        })?
    }

    fn insert(
        &self,
        application: ApplicationId,
        config: ConnectionConfig,
    ) -> Result<ConfigId, ConfigError> {
        self.with_state_mut(|state| {
            state
                .insert(ConfigEntry {
                    application,
                    config,
                })
                .map(config_id)
                .ok_or(ConfigError::CapacityExhausted {
                    capacity: state.capacity(),
                })
        })?
    }

    fn with_entry<R>(
        &self,
        application: ApplicationId,
        config: ConfigId,
        operation: impl FnOnce(&ConfigEntry) -> Result<R, ConfigError>,
    ) -> Result<R, ConfigError> {
        self.with_state(|state| operation(config_entry(state, application, config)?))
    }

    fn with_state<R>(
        &self,
        operation: impl FnOnce(&Pool<ConfigEntry>) -> Result<R, ConfigError>,
    ) -> Result<R, ConfigError> {
        self.with_control_barrier(|| {
            // SAFETY: `with_control_barrier` confines access to the owning
            // Main Thread and either holds WorkerBarrier or runs before Data
            // Workers exist.
            unsafe { operation(&*self.state.get()) }
        })?
    }

    fn with_state_mut<R>(
        &self,
        operation: impl FnOnce(&mut Pool<ConfigEntry>) -> R,
    ) -> Result<R, ConfigError> {
        self.with_control_barrier(|| {
            // SAFETY: `with_control_barrier` confines access to the owning
            // Main Thread and either holds WorkerBarrier or runs before Data
            // Workers exist.
            unsafe { operation(&mut *self.state.get()) }
        })
    }

    fn with_control_barrier<R>(&self, operation: impl FnOnce() -> R) -> Result<R, ConfigError> {
        if thread::current().id() != self.owner {
            return Err(ConfigError::WrongThread);
        }
        let barrier = Engine::with_current(|engine| engine.worker_barrier());
        Ok(match barrier {
            Some(barrier) if barrier.is_pending() => operation(),
            Some(barrier) => barrier.sync(operation),
            None => operation(),
        })
    }
}

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("QUIC Main is not initialized")]
    MainNotInitialized,
    #[error("QUIC configuration state is owned by another thread")]
    WrongThread,
    #[error("QUIC configuration capacity {capacity} is exhausted")]
    CapacityExhausted { capacity: usize },
    #[error("QUIC configuration {config:?} is not registered")]
    Missing { config: ConfigId },
    #[error("QUIC configuration {config:?} is not owned by Application {application:?}")]
    NotOwned {
        application: ApplicationId,
        config: ConfigId,
    },
    #[error("QUIC configuration {config:?} has the wrong connection role")]
    RoleMismatch { config: ConfigId },
    #[error("QUIC server configuration requires a certificate chain")]
    CertificateChainEmpty,
    #[error("QUIC client configuration requires a trust anchor")]
    TrustAnchorsEmpty,
    #[error("QUIC private key is invalid")]
    PrivateKeyInvalid,
    #[error("QUIC client identity certificate and private key must be supplied together")]
    ClientIdentityIncomplete,
    #[error("QUIC trust anchor at index {index} is invalid")]
    TrustAnchorInvalid {
        index: usize,
        #[source]
        source: quinn_proto::rustls::Error,
    },
    #[error("QUIC ALPN protocol at index {index} has invalid length {bytes}")]
    AlpnInvalid { index: usize, bytes: usize },
    #[error("QUIC server configuration is invalid")]
    ServerInvalid {
        #[source]
        source: quinn_proto::rustls::Error,
    },
    #[error("QUIC client configuration is invalid")]
    ClientInvalid {
        #[source]
        source: quinn_proto::rustls::Error,
    },
    #[error("QUIC client verifier configuration is invalid")]
    ClientVerifierInvalid {
        #[source]
        source: quinn_proto::rustls::client::VerifierBuilderError,
    },
    #[error("QUIC server crypto configuration has no supported initial cipher suite")]
    ServerCryptoInvalid {
        #[source]
        source: quinn_proto::crypto::rustls::NoInitialCipherSuite,
    },
    #[error("QUIC client crypto configuration has no supported initial cipher suite")]
    ClientCryptoInvalid {
        #[source]
        source: quinn_proto::crypto::rustls::NoInitialCipherSuite,
    },
    #[error("QUIC connection timeout must be non-zero")]
    ConnectionTimeoutInvalid,
    #[error("QUIC configuration requires an attached Application")]
    ApplicationRequired,
    #[error("QUIC listener or connection requires a configuration")]
    ConfigurationRequired,
}

impl QuicMain {
    pub fn register_server_config(
        &self,
        application: ApplicationId,
        config: ServerConfig,
    ) -> Result<ConfigId, ConfigError> {
        self.ensure_application(application)?;
        self.configs.register_server_config(
            application,
            config,
            AppSessionConfig::DEFAULT.fifo_capacity,
        )
    }

    pub fn register_client_config(
        &self,
        application: ApplicationId,
        config: ClientConfig,
    ) -> Result<ConfigId, ConfigError> {
        self.ensure_application(application)?;
        self.configs.register_client_config(
            application,
            config,
            AppSessionConfig::DEFAULT.fifo_capacity,
        )
    }

    pub fn server_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<Arc<quinn_proto::ServerConfig>, ConfigError> {
        self.ensure_application(application)?;
        self.configs.server_config(application, config)
    }

    pub fn client_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<Arc<quinn_proto::ClientConfig>, ConfigError> {
        self.ensure_application(application)?;
        self.configs.client_config(application, config)
    }

    pub fn remove_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<(), ConfigError> {
        self.ensure_application(application)?;
        self.configs.remove_config(application, config)
    }

    fn ensure_application(&self, application: ApplicationId) -> Result<(), ConfigError> {
        match self.application_is_attached(application) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ConfigError::ApplicationRequired),
            Err(hammer_service::session::ApplicationError::WrongThread) => {
                Err(ConfigError::WrongThread)
            }
            Err(_) => Err(ConfigError::ApplicationRequired),
        }
    }
}

pub fn register_server_config(
    application: ApplicationId,
    config: ServerConfig,
) -> Result<ConfigId, ConfigError> {
    main()?.register_server_config(application, config)
}

pub fn register_client_config(
    application: ApplicationId,
    config: ClientConfig,
) -> Result<ConfigId, ConfigError> {
    main()?.register_client_config(application, config)
}

pub fn remove_config(application: ApplicationId, config: ConfigId) -> Result<(), ConfigError> {
    main()?.remove_config(application, config)
}

pub(crate) fn main() -> Result<&'static Arc<QuicMain>, ConfigError> {
    QUIC_MAIN.get().ok_or(ConfigError::MainNotInitialized)
}

fn build_server_config(
    config: ServerConfig,
    fifo_capacity: usize,
) -> Result<quinn_proto::ServerConfig, ConfigError> {
    if config.certificate_der.is_empty() {
        return Err(ConfigError::CertificateChainEmpty);
    }
    validate_alpn(&config.alpn_protocols)?;
    let private_key = PrivateKeyDer::try_from(config.private_key_der)
        .map_err(|_| ConfigError::PrivateKeyInvalid)?;
    let certificates = config
        .certificate_der
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    let builder = quinn_proto::rustls::ServerConfig::builder();
    let mut rustls = if config.client_trust_anchor_der.is_empty() {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|source| ConfigError::ServerInvalid { source })?
    } else {
        let roots = trust_roots(config.client_trust_anchor_der)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|source| ConfigError::ClientVerifierInvalid { source })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .map_err(|source| ConfigError::ServerInvalid { source })?
    };
    rustls.alpn_protocols = config.alpn_protocols;
    rustls.max_early_data_size = 0;
    let crypto = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls))
        .map_err(|source| ConfigError::ServerCryptoInvalid { source })?;
    let mut quic = quinn_proto::ServerConfig::with_crypto(Arc::new(crypto));
    quic.migration(false);
    quic.transport_config(build_transport_config(config.transport, fifo_capacity)?);
    Ok(quic)
}

fn build_client_config(
    config: ClientConfig,
    fifo_capacity: usize,
) -> Result<quinn_proto::ClientConfig, ConfigError> {
    if config.trust_anchor_der.is_empty() {
        return Err(ConfigError::TrustAnchorsEmpty);
    }
    validate_alpn(&config.alpn_protocols)?;
    let roots = trust_roots(config.trust_anchor_der)?;
    let builder = RustlsClientConfig::builder().with_root_certificates(roots);
    let has_certificates = !config.certificate_der.is_empty();
    let has_private_key = !config.private_key_der.is_empty();
    let mut rustls = match (has_certificates, has_private_key) {
        (false, false) => builder.with_no_client_auth(),
        (true, true) => {
            let private_key = PrivateKeyDer::try_from(config.private_key_der)
                .map_err(|_| ConfigError::PrivateKeyInvalid)?;
            let certificates = config
                .certificate_der
                .into_iter()
                .map(CertificateDer::from)
                .collect();
            builder
                .with_client_auth_cert(certificates, private_key)
                .map_err(|source| ConfigError::ClientInvalid { source })?
        }
        _ => return Err(ConfigError::ClientIdentityIncomplete),
    };
    rustls.alpn_protocols = config.alpn_protocols;
    rustls.enable_early_data = false;
    let crypto = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls))
        .map_err(|source| ConfigError::ClientCryptoInvalid { source })?;
    let mut quic = quinn_proto::ClientConfig::new(Arc::new(crypto));
    quic.transport_config(build_transport_config(config.transport, fifo_capacity)?);
    Ok(quic)
}

fn build_transport_config(
    config: TransportConfig,
    fifo_capacity: usize,
) -> Result<Arc<quinn_proto::TransportConfig>, ConfigError> {
    if config.connection_timeout == 0 {
        return Err(ConfigError::ConnectionTimeoutInvalid);
    }
    let timeout = std::time::Duration::from_millis(u64::from(config.connection_timeout))
        .try_into()
        .map_err(|_| ConfigError::ConnectionTimeoutInvalid)?;
    let mut transport = quinn_proto::TransportConfig::default();
    let stream_receive_window = quinn_proto::VarInt::try_from(fifo_capacity as u64)
        .expect("Session FIFO capacity fits QUIC VarInt");
    transport
        .max_idle_timeout(Some(timeout))
        .max_concurrent_bidi_streams(quinn_proto::VarInt::from_u32(
            config.max_concurrent_bidi_streams,
        ))
        .max_concurrent_uni_streams(quinn_proto::VarInt::from_u32(
            config.max_concurrent_uni_streams,
        ))
        .stream_receive_window(stream_receive_window)
        .datagram_receive_buffer_size(None)
        .datagram_send_buffer_size(0);
    Ok(Arc::new(transport))
}

fn trust_roots(certificates: Vec<Vec<u8>>) -> Result<RootCertStore, ConfigError> {
    let mut roots = RootCertStore::empty();
    for (index, certificate) in certificates.into_iter().enumerate() {
        roots
            .add(CertificateDer::from(certificate))
            .map_err(|source| ConfigError::TrustAnchorInvalid { index, source })?;
    }
    Ok(roots)
}

fn validate_alpn(protocols: &[Vec<u8>]) -> Result<(), ConfigError> {
    for (index, protocol) in protocols.iter().enumerate() {
        if protocol.is_empty() || protocol.len() > u8::MAX as usize {
            return Err(ConfigError::AlpnInvalid {
                index,
                bytes: protocol.len(),
            });
        }
    }
    Ok(())
}

#[inline]
fn config_id(index: Index) -> ConfigId {
    ConfigId((index.slot() as u64) | ((index.generation() as u64) << 32))
}

#[inline]
fn config_index(config: ConfigId) -> Index {
    Index::new(config.slot(), config.generation())
}

fn config_entry(
    configs: &Pool<ConfigEntry>,
    application: ApplicationId,
    config: ConfigId,
) -> Result<&ConfigEntry, ConfigError> {
    let entry = configs
        .get(config_index(config))
        .ok_or(ConfigError::Missing { config })?;
    if entry.application != application {
        return Err(ConfigError::NotOwned {
            application,
            config,
        });
    }
    Ok(entry)
}

#[derive(Clone, PartialEq, Message)]
pub struct RegisterServerConfigRequest {
    #[prost(uint64, tag = "1")]
    pub application_id: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub certificate_der: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "3")]
    pub private_key_der: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub client_trust_anchor_der: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "5")]
    pub alpn_protocols: Vec<Vec<u8>>,
    #[prost(uint32, tag = "6")]
    pub connection_timeout: u32,
    #[prost(uint32, tag = "7")]
    pub max_concurrent_bidi_streams: u32,
    #[prost(uint32, tag = "8")]
    pub max_concurrent_uni_streams: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct RegisterClientConfigRequest {
    #[prost(uint64, tag = "1")]
    pub application_id: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub trust_anchor_der: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub certificate_der: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "4")]
    pub private_key_der: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "5")]
    pub alpn_protocols: Vec<Vec<u8>>,
    #[prost(uint32, tag = "6")]
    pub connection_timeout: u32,
    #[prost(uint32, tag = "7")]
    pub max_concurrent_bidi_streams: u32,
    #[prost(uint32, tag = "8")]
    pub max_concurrent_uni_streams: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct RegisterServerConfigReply {
    #[prost(enumeration = "QuicApiStatus", tag = "1")]
    pub status: i32,
    #[prost(uint64, tag = "2")]
    pub config_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RegisterClientConfigReply {
    #[prost(enumeration = "QuicApiStatus", tag = "1")]
    pub status: i32,
    #[prost(uint64, tag = "2")]
    pub config_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RemoveConfigRequest {
    #[prost(uint64, tag = "1")]
    pub application_id: u64,
    #[prost(uint64, tag = "2")]
    pub config_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RemoveConfigReply {
    #[prost(enumeration = "QuicApiStatus", tag = "1")]
    pub status: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum QuicApiStatus {
    Ok = 0,
    ApplicationMissing = 1,
    MainThreadUnavailable = 2,
    WrongThread = 3,
    CapacityExhausted = 4,
    ConfigMissing = 5,
    ConfigNotOwned = 6,
    ConfigRoleMismatch = 7,
    CertificateChainEmpty = 8,
    TrustAnchorsEmpty = 9,
    PrivateKeyInvalid = 10,
    ClientIdentityIncomplete = 11,
    TrustAnchorInvalid = 12,
    AlpnInvalid = 13,
    ConfigurationInvalid = 14,
}

#[hammer_component_macros::binary_api(name = "quic.server-config.register")]
fn register_server_config_api(request: RegisterServerConfigRequest) -> RegisterServerConfigReply {
    let application = match binary_application(request.application_id) {
        Ok(application) => application,
        Err(status) => return server_reply(status, 0),
    };
    let config = ServerConfig::new(request.certificate_der, request.private_key_der)
        .with_client_authentication(request.client_trust_anchor_der)
        .with_alpn_protocols(request.alpn_protocols)
        .with_transport_config(api_transport(
            request.connection_timeout,
            request.max_concurrent_bidi_streams,
            request.max_concurrent_uni_streams,
        ));
    match main().and_then(|main| main.register_server_config(application, config)) {
        Ok(config) => server_reply(QuicApiStatus::Ok, config.raw()),
        Err(error) => server_reply(quic_api_status(&error), 0),
    }
}

#[hammer_component_macros::binary_api(name = "quic.client-config.register")]
fn register_client_config_api(request: RegisterClientConfigRequest) -> RegisterClientConfigReply {
    let application = match binary_application(request.application_id) {
        Ok(application) => application,
        Err(status) => return client_reply(status, 0),
    };
    let mut config = ClientConfig::new(request.trust_anchor_der)
        .with_alpn_protocols(request.alpn_protocols)
        .with_transport_config(api_transport(
            request.connection_timeout,
            request.max_concurrent_bidi_streams,
            request.max_concurrent_uni_streams,
        ));
    if !request.certificate_der.is_empty() || !request.private_key_der.is_empty() {
        config = config.with_identity(request.certificate_der, request.private_key_der);
    }
    match main().and_then(|main| main.register_client_config(application, config)) {
        Ok(config) => client_reply(QuicApiStatus::Ok, config.raw()),
        Err(error) => client_reply(quic_api_status(&error), 0),
    }
}

#[hammer_component_macros::binary_api(name = "quic.config.remove")]
fn remove_config_api(request: RemoveConfigRequest) -> RemoveConfigReply {
    let application = match binary_application(request.application_id) {
        Ok(application) => application,
        Err(status) => return remove_reply(status),
    };
    match main()
        .and_then(|main| main.remove_config(application, ConfigId::from_raw(request.config_id)))
    {
        Ok(()) => remove_reply(QuicApiStatus::Ok),
        Err(error) => remove_reply(quic_api_status(&error)),
    }
}

fn api_transport(
    connection_timeout: u32,
    max_concurrent_bidi_streams: u32,
    max_concurrent_uni_streams: u32,
) -> TransportConfig {
    let defaults = TransportConfig::default();
    TransportConfig::new(
        if connection_timeout == 0 {
            defaults.connection_timeout
        } else {
            connection_timeout
        },
        if max_concurrent_bidi_streams == 0 {
            defaults.max_concurrent_bidi_streams
        } else {
            max_concurrent_bidi_streams
        },
        if max_concurrent_uni_streams == 0 {
            defaults.max_concurrent_uni_streams
        } else {
            max_concurrent_uni_streams
        },
    )
}

fn binary_application(application: u64) -> Result<ApplicationId, QuicApiStatus> {
    let application = ApplicationId::from_raw(application);
    let Some(attached) = Engine::with_current(|engine| {
        let applications = engine
            .registry
            .require::<hammer_service::session::ApplicationMain>()
            .map_err(|_| QuicApiStatus::MainThreadUnavailable)?;
        applications
            .contains(application)
            .map_err(|error| match error {
                hammer_service::session::ApplicationError::WrongThread => {
                    QuicApiStatus::WrongThread
                }
                _ => QuicApiStatus::MainThreadUnavailable,
            })
    }) else {
        return Err(QuicApiStatus::MainThreadUnavailable);
    };
    match attached {
        Ok(true) => Ok(application),
        Ok(false) => Err(QuicApiStatus::ApplicationMissing),
        Err(status) => Err(status),
    }
}

fn quic_api_status(error: &ConfigError) -> QuicApiStatus {
    match error {
        ConfigError::MainNotInitialized => QuicApiStatus::MainThreadUnavailable,
        ConfigError::WrongThread => QuicApiStatus::WrongThread,
        ConfigError::CapacityExhausted { .. } => QuicApiStatus::CapacityExhausted,
        ConfigError::Missing { .. } => QuicApiStatus::ConfigMissing,
        ConfigError::NotOwned { .. } => QuicApiStatus::ConfigNotOwned,
        ConfigError::RoleMismatch { .. } => QuicApiStatus::ConfigRoleMismatch,
        ConfigError::CertificateChainEmpty => QuicApiStatus::CertificateChainEmpty,
        ConfigError::TrustAnchorsEmpty => QuicApiStatus::TrustAnchorsEmpty,
        ConfigError::PrivateKeyInvalid => QuicApiStatus::PrivateKeyInvalid,
        ConfigError::ClientIdentityIncomplete => QuicApiStatus::ClientIdentityIncomplete,
        ConfigError::TrustAnchorInvalid { .. } => QuicApiStatus::TrustAnchorInvalid,
        ConfigError::AlpnInvalid { .. } => QuicApiStatus::AlpnInvalid,
        ConfigError::ServerInvalid { .. }
        | ConfigError::ClientInvalid { .. }
        | ConfigError::ClientVerifierInvalid { .. }
        | ConfigError::ServerCryptoInvalid { .. }
        | ConfigError::ClientCryptoInvalid { .. }
        | ConfigError::ConnectionTimeoutInvalid
        | ConfigError::ApplicationRequired
        | ConfigError::ConfigurationRequired => QuicApiStatus::ConfigurationInvalid,
    }
}

fn server_reply(status: QuicApiStatus, config_id: u64) -> RegisterServerConfigReply {
    RegisterServerConfigReply {
        status: status as i32,
        config_id,
    }
}

fn client_reply(status: QuicApiStatus, config_id: u64) -> RegisterClientConfigReply {
    RegisterClientConfigReply {
        status: status as i32,
        config_id,
    }
}

fn remove_reply(status: QuicApiStatus) -> RemoveConfigReply {
    RemoveConfigReply {
        status: status as i32,
    }
}

#[cfg(test)]
mod tests {
    use hammer_runtime::{
        DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, RuntimeRegistry,
        SessionTransportRegistration,
    };
    use rcgen::generate_simple_self_signed;

    use super::*;

    fn identity() -> (Vec<Vec<u8>>, Vec<u8>) {
        let certified = generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        (
            vec![certified.cert.der().to_vec()],
            certified.signing_key.serialize_der(),
        )
    }

    fn test_main() -> (Arc<QuicMain>, ApplicationId, ApplicationId) {
        let applications = hammer_service::session::ApplicationMain::new(4);
        let first = applications.attach().expect("attach first Application");
        let second = applications.attach().expect("attach second Application");
        let inner_application = applications.attach().expect("attach QUIC Application");
        let sessions = Arc::new(hammer_service::session::runtime::SessionMain::new(
            1,
            Arc::clone(&applications),
        ));
        let main = Arc::new(QuicMain::new(
            sessions,
            inner_application,
            hammer_runtime::app::SessionAppId::new(0),
            SessionTransportRegistration::new("udp", None, None, None),
            1,
        ));
        (main, first, second)
    }

    #[test]
    fn generation_and_application_ownership_survive_config_removal() {
        let (main, first, second) = test_main();
        let (certificate_der, private_key_der) = identity();
        let server = main
            .register_server_config(
                first,
                ServerConfig::new(certificate_der.clone(), private_key_der),
            )
            .expect("register QUIC server configuration");
        assert!(matches!(
            main.server_config(second, server),
            Err(ConfigError::NotOwned { application, config })
                if application == second && config == server
        ));
        let retained = main
            .server_config(first, server)
            .expect("resolve QUIC server configuration");
        let replacement = {
            let (certificate_der, private_key_der) = identity();
            main.remove_config(first, server)
                .expect("remove QUIC server configuration");
            main.register_server_config(first, ServerConfig::new(certificate_der, private_key_der))
                .expect("register replacement QUIC server configuration")
        };
        assert_ne!(server, replacement);
        assert!(matches!(
            main.server_config(first, server),
            Err(ConfigError::Missing { config }) if config == server
        ));
        drop(retained);
        main.remove_config(first, replacement)
            .expect("remove replacement QUIC server configuration");
    }

    #[test]
    fn client_configuration_accepts_trust_anchor() {
        let (main, application, _) = test_main();
        let (certificate_der, _) = identity();
        main.register_client_config(application, ClientConfig::new(certificate_der))
            .expect("register QUIC client configuration");
    }

    #[test]
    fn configuration_access_from_foreign_thread_reports_wrong_thread() {
        let (main, application, _) = test_main();
        let (certificate_der, private_key_der) = identity();
        let result = std::thread::spawn(move || {
            main.register_server_config(
                application,
                ServerConfig::new(certificate_der, private_key_der),
            )
        })
        .join()
        .expect("configuration operation thread");

        assert!(matches!(result, Err(ConfigError::WrongThread)));
    }

    #[test]
    fn config_mutation_observes_the_worker_barrier() {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        engine.install_current();
        let registry = QuicConfigRegistry::new(1);
        let barrier_seen = registry
            .with_state_mut(|_| {
                Engine::with_current(|engine| engine.worker_barrier().is_pending())
                    .expect("current Main Thread Engine")
            })
            .expect("barriered QUIC config mutation");
        assert!(barrier_seen);
        Engine::uninstall_current();
    }
}
