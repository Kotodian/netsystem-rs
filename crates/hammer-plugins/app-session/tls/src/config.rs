use std::cell::UnsafeCell;
use std::sync::{Arc, OnceLock};
use std::thread::{self, ThreadId};

use hammer_infra::pool::{Index, Pool};
use hammer_runtime::app::ApplicationId;
use hammer_runtime::{Engine, RuntimeResult};
use hammer_service::session::{ApplicationMain, ApplicationRegistration};
use prost::Message;
use rustls::ServerConfig as RustlsServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use thiserror::Error;

const TLS_CONFIG_CAPACITY: usize = 1_024;
pub(crate) const TLS_BUFFER_LIMIT: usize = 64 * 1_024;

static TLS_MAIN: OnceLock<Arc<TlsMain>> = OnceLock::new();

pub struct ServerConfig {
    certificate_der: Vec<Vec<u8>>,
    private_key_der: Vec<u8>,
    client_trust_anchor_der: Vec<Vec<u8>>,
    alpn_protocols: Vec<Vec<u8>>,
}

impl ServerConfig {
    pub fn new(certificate_der: Vec<Vec<u8>>, private_key_der: Vec<u8>) -> Self {
        Self {
            certificate_der,
            private_key_der,
            client_trust_anchor_der: Vec::new(),
            alpn_protocols: Vec::new(),
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
}

pub struct ClientConfig {
    trust_anchor_der: Vec<Vec<u8>>,
    certificate_der: Vec<Vec<u8>>,
    private_key_der: Vec<u8>,
    alpn_protocols: Vec<Vec<u8>>,
}

impl ClientConfig {
    pub fn new(trust_anchor_der: Vec<Vec<u8>>) -> Self {
        Self {
            trust_anchor_der,
            certificate_der: Vec::new(),
            private_key_der: Vec::new(),
            alpn_protocols: Vec::new(),
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
}

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

enum ConnectionConfig {
    Client(Arc<RustlsClientConfig>),
    Server(Arc<RustlsServerConfig>),
}

struct ConfigEntry {
    application: ApplicationId,
    config: ConnectionConfig,
}

struct TlsState {
    configs: Pool<ConfigEntry>,
}

/// TLS plugin Main Thread state.
pub struct TlsMain {
    owner: ThreadId,
    state: UnsafeCell<TlsState>,
}

// SAFETY: all accesses verify the creating Main Thread before dereferencing
// state. Immutable rustls configurations may subsequently be cloned to workers.
unsafe impl Send for TlsMain {}
// SAFETY: shared references can cross threads, but mutable state is reachable
// only through Main Thread-checked operations.
unsafe impl Sync for TlsMain {}

impl TlsMain {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            owner: thread::current().id(),
            state: UnsafeCell::new(TlsState {
                configs: Pool::with_capacity(capacity),
            }),
        })
    }

    pub fn register_server_config(
        &self,
        application: ApplicationId,
        config: ServerConfig,
    ) -> Result<ConfigId, ConfigError> {
        let config = build_server_config(config)?;
        self.insert(application, ConnectionConfig::Server(Arc::new(config)))
    }

    pub fn register_client_config(
        &self,
        application: ApplicationId,
        config: ClientConfig,
    ) -> Result<ConfigId, ConfigError> {
        let config = build_client_config(config)?;
        self.insert(application, ConnectionConfig::Client(Arc::new(config)))
    }

    pub fn server_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<Arc<RustlsServerConfig>, ConfigError> {
        match &self.entry(application, config)?.config {
            ConnectionConfig::Server(config) => Ok(Arc::clone(config)),
            ConnectionConfig::Client(_) => Err(ConfigError::RoleMismatch { config }),
        }
    }

    pub fn client_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<Arc<RustlsClientConfig>, ConfigError> {
        match &self.entry(application, config)?.config {
            ConnectionConfig::Client(config) => Ok(Arc::clone(config)),
            ConnectionConfig::Server(_) => Err(ConfigError::RoleMismatch { config }),
        }
    }

    pub fn remove_config(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<(), ConfigError> {
        self.with_state_mut(|state| {
            config_entry(&state.configs, application, config)?;
            state
                .configs
                .remove(config_index(config))
                .expect("validated TLS configuration remains present until removal");
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
                .configs
                .insert(ConfigEntry {
                    application,
                    config,
                })
                .map(config_id)
                .ok_or(ConfigError::CapacityExhausted {
                    capacity: state.configs.capacity(),
                })
        })?
    }

    fn entry(
        &self,
        application: ApplicationId,
        config: ConfigId,
    ) -> Result<&ConfigEntry, ConfigError> {
        config_entry(&self.state().configs, application, config)
    }

    fn state(&self) -> &TlsState {
        // SAFETY: Main Thread mutations hold `barrier`, so Data Workers cannot
        // overlap a write. Readers only clone immutable Arc configurations.
        unsafe { &*self.state.get() }
    }

    fn with_state_mut<R>(
        &self,
        operation: impl FnOnce(&mut TlsState) -> R,
    ) -> Result<R, ConfigError> {
        self.ensure_main_thread()?;
        // SAFETY: the owner check confines writers to Main Thread. The worker
        // barrier stops every reader before mutable access begins.
        let state = unsafe { &mut *self.state.get() };
        let barrier = Engine::with_current(|engine| engine.worker_barrier());
        Ok(match barrier {
            Some(barrier) => barrier.sync(state, operation),
            None => operation(state),
        })
    }

    fn ensure_main_thread(&self) -> Result<(), ConfigError> {
        if thread::current().id() == self.owner {
            Ok(())
        } else {
            Err(ConfigError::WrongThread)
        }
    }
}

#[hammer_component_macros::runtime_error(subsystem = "tls")]
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("TLS Main is not initialized")]
    MainNotInitialized,
    #[error("TLS configuration state is owned by another thread")]
    WrongThread,
    #[error("TLS configuration capacity {capacity} is exhausted")]
    CapacityExhausted { capacity: usize },
    #[error("TLS configuration {config:?} is not registered")]
    Missing { config: ConfigId },
    #[error("TLS configuration {config:?} is not owned by Application {application:?}")]
    NotOwned {
        application: ApplicationId,
        config: ConfigId,
    },
    #[error("TLS configuration {config:?} has the wrong connection role")]
    RoleMismatch { config: ConfigId },
    #[error("TLS connection construction requires an attached Application")]
    ApplicationRequired,
    #[error("TLS connection construction requires a configuration")]
    ConfigurationRequired,
    #[error("TLS client connection construction requires a server name")]
    ServerNameRequired,
    #[error("TLS server name is invalid")]
    ServerNameInvalid,
    #[error("TLS certificate chain is empty")]
    CertificateChainEmpty,
    #[error("TLS trust anchor set is empty")]
    TrustAnchorsEmpty,
    #[error("TLS private key is invalid")]
    PrivateKeyInvalid,
    #[error("TLS client identity certificate and private key must be supplied together")]
    ClientIdentityIncomplete,
    #[error("TLS trust anchor at index {index} is invalid")]
    TrustAnchorInvalid {
        index: usize,
        #[source]
        source: rustls::Error,
    },
    #[error("TLS ALPN protocol at index {index} has invalid length {bytes}")]
    AlpnInvalid { index: usize, bytes: usize },
    #[error("TLS server configuration is invalid")]
    ServerInvalid {
        #[source]
        source: rustls::Error,
    },
    #[error("TLS client configuration is invalid")]
    ClientInvalid {
        #[source]
        source: rustls::Error,
    },
    #[error("TLS client authentication policy is invalid")]
    ClientAuthenticationInvalid {
        #[source]
        source: rustls::server::VerifierBuilderError,
    },
}

pub fn register_server_config(
    application: &ApplicationRegistration,
    config: ServerConfig,
) -> Result<ConfigId, ConfigError> {
    main()?.register_server_config(application.application(), config)
}

pub fn register_client_config(
    application: &ApplicationRegistration,
    config: ClientConfig,
) -> Result<ConfigId, ConfigError> {
    main()?.register_client_config(application.application(), config)
}

pub fn remove_config(
    application: &ApplicationRegistration,
    config: ConfigId,
) -> Result<(), ConfigError> {
    main()?.remove_config(application.application(), config)
}

pub(crate) fn init() -> RuntimeResult<Arc<TlsMain>> {
    let main = TlsMain::new(TLS_CONFIG_CAPACITY);
    assert!(
        TLS_MAIN.set(Arc::clone(&main)).is_ok(),
        "TLS Main initialized more than once"
    );
    Ok(main)
}

pub(crate) fn main() -> Result<&'static Arc<TlsMain>, ConfigError> {
    TLS_MAIN.get().ok_or(ConfigError::MainNotInitialized)
}

fn build_server_config(config: ServerConfig) -> Result<RustlsServerConfig, ConfigError> {
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
    let builder = RustlsServerConfig::builder();
    let mut rustls = if config.client_trust_anchor_der.is_empty() {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|source| ConfigError::ServerInvalid { source })?
    } else {
        let roots = trust_roots(config.client_trust_anchor_der)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|source| ConfigError::ClientAuthenticationInvalid { source })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .map_err(|source| ConfigError::ServerInvalid { source })?
    };
    rustls.alpn_protocols = config.alpn_protocols;
    Ok(rustls)
}

fn build_client_config(config: ClientConfig) -> Result<RustlsClientConfig, ConfigError> {
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
    Ok(rustls)
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
}

#[derive(Clone, PartialEq, Message)]
pub struct RegisterServerConfigReply {
    #[prost(enumeration = "TlsApiStatus", tag = "1")]
    pub status: i32,
    #[prost(uint64, tag = "2")]
    pub config_id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RegisterClientConfigReply {
    #[prost(enumeration = "TlsApiStatus", tag = "1")]
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
    #[prost(enumeration = "TlsApiStatus", tag = "1")]
    pub status: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum TlsApiStatus {
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

#[hammer_component_macros::binary_api(name = "tls.server-config.register")]
fn register_server_config_api(request: RegisterServerConfigRequest) -> RegisterServerConfigReply {
    let application = match binary_application(request.application_id) {
        Ok(application) => application,
        Err(status) => return server_reply(status, 0),
    };
    let config = ServerConfig::new(request.certificate_der, request.private_key_der)
        .with_client_authentication(request.client_trust_anchor_der)
        .with_alpn_protocols(request.alpn_protocols);
    match main().and_then(|main| main.register_server_config(application, config)) {
        Ok(config) => server_reply(TlsApiStatus::Ok, config.raw()),
        Err(error) => server_reply(tls_api_status(&error), 0),
    }
}

#[hammer_component_macros::binary_api(name = "tls.client-config.register")]
fn register_client_config_api(request: RegisterClientConfigRequest) -> RegisterClientConfigReply {
    let application = match binary_application(request.application_id) {
        Ok(application) => application,
        Err(status) => return client_reply(status, 0),
    };
    let mut config =
        ClientConfig::new(request.trust_anchor_der).with_alpn_protocols(request.alpn_protocols);
    if !request.certificate_der.is_empty() || !request.private_key_der.is_empty() {
        config = config.with_identity(request.certificate_der, request.private_key_der);
    }
    match main().and_then(|main| main.register_client_config(application, config)) {
        Ok(config) => client_reply(TlsApiStatus::Ok, config.raw()),
        Err(error) => client_reply(tls_api_status(&error), 0),
    }
}

#[hammer_component_macros::binary_api(name = "tls.config.remove")]
fn remove_config_api(request: RemoveConfigRequest) -> RemoveConfigReply {
    let application = match binary_application(request.application_id) {
        Ok(application) => application,
        Err(status) => return remove_reply(status),
    };
    match main()
        .and_then(|main| main.remove_config(application, ConfigId::from_raw(request.config_id)))
    {
        Ok(()) => remove_reply(TlsApiStatus::Ok),
        Err(error) => remove_reply(tls_api_status(&error)),
    }
}

fn binary_application(application: u64) -> Result<ApplicationId, TlsApiStatus> {
    let application = ApplicationId::from_raw(application);
    let Some(attached) = Engine::with_current(|engine| {
        engine
            .registry
            .require::<ApplicationMain>()
            .ok()
            .and_then(|applications| applications.contains(application).ok())
    }) else {
        return Err(TlsApiStatus::MainThreadUnavailable);
    };
    match attached {
        Some(true) => Ok(application),
        Some(false) => Err(TlsApiStatus::ApplicationMissing),
        None => Err(TlsApiStatus::MainThreadUnavailable),
    }
}

fn tls_api_status(error: &ConfigError) -> TlsApiStatus {
    match error {
        ConfigError::MainNotInitialized => TlsApiStatus::MainThreadUnavailable,
        ConfigError::WrongThread => TlsApiStatus::WrongThread,
        ConfigError::CapacityExhausted { .. } => TlsApiStatus::CapacityExhausted,
        ConfigError::Missing { .. } => TlsApiStatus::ConfigMissing,
        ConfigError::NotOwned { .. } => TlsApiStatus::ConfigNotOwned,
        ConfigError::RoleMismatch { .. } => TlsApiStatus::ConfigRoleMismatch,
        ConfigError::ApplicationRequired
        | ConfigError::ConfigurationRequired
        | ConfigError::ServerNameRequired
        | ConfigError::ServerNameInvalid => TlsApiStatus::ConfigurationInvalid,
        ConfigError::CertificateChainEmpty => TlsApiStatus::CertificateChainEmpty,
        ConfigError::TrustAnchorsEmpty => TlsApiStatus::TrustAnchorsEmpty,
        ConfigError::PrivateKeyInvalid => TlsApiStatus::PrivateKeyInvalid,
        ConfigError::ClientIdentityIncomplete => TlsApiStatus::ClientIdentityIncomplete,
        ConfigError::TrustAnchorInvalid { .. } => TlsApiStatus::TrustAnchorInvalid,
        ConfigError::AlpnInvalid { .. } => TlsApiStatus::AlpnInvalid,
        ConfigError::ServerInvalid { .. }
        | ConfigError::ClientInvalid { .. }
        | ConfigError::ClientAuthenticationInvalid { .. } => TlsApiStatus::ConfigurationInvalid,
    }
}

fn server_reply(status: TlsApiStatus, config_id: u64) -> RegisterServerConfigReply {
    RegisterServerConfigReply {
        status: status as i32,
        config_id,
    }
}

fn client_reply(status: TlsApiStatus, config_id: u64) -> RegisterClientConfigReply {
    RegisterClientConfigReply {
        status: status as i32,
        config_id,
    }
}

fn remove_reply(status: TlsApiStatus) -> RemoveConfigReply {
    RemoveConfigReply {
        status: status as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::generate_simple_self_signed;

    fn identity() -> (Vec<Vec<u8>>, Vec<u8>) {
        let certified = generate_simple_self_signed(["localhost".to_owned()])
            .expect("generate test certificate");
        (
            vec![certified.cert.der().to_vec()],
            certified.signing_key.serialize_der(),
        )
    }

    #[test]
    fn application_explicitly_removes_generation_checked_tls_configurations() {
        let tls = init().expect("initialize TLS Main");
        let applications =
            ApplicationMain::with_protocols(2, [crate::__APP_SESSION_PROTOCOL_CONNECTION]);
        let first = applications
            .register_local()
            .expect("register first Application");
        let first_application = first.application();
        let second = applications
            .register_local()
            .expect("register second Application");

        let (certificate_der, private_key_der) = identity();
        let server_id = register_server_config(
            &first,
            ServerConfig::new(certificate_der.clone(), private_key_der),
        )
        .expect("register server configuration");
        let client_id = register_client_config(&first, ClientConfig::new(certificate_der))
            .expect("register client configuration");

        assert!(matches!(
            tls.server_config(second.application(), server_id),
            Err(ConfigError::NotOwned { application, config })
                if application == second.application() && config == server_id
        ));
        assert!(matches!(
            tls.server_config(first.application(), client_id),
            Err(ConfigError::RoleMismatch { config }) if config == client_id
        ));

        let retained = tls
            .server_config(first.application(), server_id)
            .expect("resolve server configuration");
        let worker_tls = Arc::clone(&tls);
        std::thread::spawn(move || {
            worker_tls
                .server_config(first_application, server_id)
                .expect("Data Worker resolves published immutable TLS configuration");
        })
        .join()
        .expect("join TLS configuration reader");

        remove_config(&first, server_id).expect("remove server configuration");
        assert!(matches!(
            tls.server_config(first.application(), server_id),
            Err(ConfigError::Missing { config }) if config == server_id
        ));
        rustls::ServerConnection::new(Arc::clone(&retained))
            .expect("removed registry entry does not invalidate retained configuration");

        let (replacement_certificate_der, replacement_private_key_der) = identity();
        let replacement = register_server_config(
            &first,
            ServerConfig::new(replacement_certificate_der, replacement_private_key_der),
        )
        .expect("register replacement server configuration");
        assert_ne!(server_id, replacement);
        assert!(matches!(
            tls.server_config(first.application(), server_id),
            Err(ConfigError::Missing { config }) if config == server_id
        ));

        remove_config(&first, client_id).expect("remove client configuration");
        remove_config(&first, replacement).expect("remove replacement server configuration");
        drop(first);
        assert!(
            applications
                .contains(second.application())
                .expect("second Application remains attached")
        );
    }
}
