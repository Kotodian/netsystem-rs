//! External attached TLS echo app for the CI-only TUN lab.
//!
//! The app attaches through the Session Socket API, registers its rustls
//! server configuration through the TLS plugin Binary API, and selects that
//! configuration in its App Session protocol policy. TCP receives only the
//! Session listener identity and endpoint.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Parser;
use hammer_app::attach::{AppClient, AppClientError};
use hammer_app::echo::run_echo_loop;
use hammer_app::{
    APP_SESSION_POLICY_VERSION, AppSession, AppSessionError, AppSessionPolicy,
    AppSessionProtocolSelection, DataWorkerId, SessionListenEndpoint,
};
use hammer_runtime::app::SessionEvtType;
use hammer_service::binary_api::{BinaryApiClient, BinaryApiError};
use prost::Message;

const DEFAULT_ATTACH_SOCKET: &str = "/tmp/hammer-tcp-integration.attach.sock";
const DEFAULT_BINARY_API_SOCKET: &str = "/tmp/hammer-tcp-integration.binary-api.sock";
const DEFAULT_LISTEN_ADDRESS: &str = "10.66.77.1:7300";
const ECHO_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, Message)]
struct RegisterServerConfigRequest {
    #[prost(uint64, tag = "1")]
    application_id: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    certificate_der: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "3")]
    private_key_der: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    client_trust_anchor_der: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "5")]
    alpn_protocols: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct RegisterServerConfigReply {
    #[prost(enumeration = "TlsApiStatus", tag = "1")]
    status: i32,
    #[prost(uint64, tag = "2")]
    config_id: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RemoveConfigRequest {
    #[prost(uint64, tag = "1")]
    application_id: u64,
    #[prost(uint64, tag = "2")]
    config_id: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RemoveConfigReply {
    #[prost(enumeration = "TlsApiStatus", tag = "1")]
    status: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum TlsApiStatus {
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

#[derive(Debug, Parser)]
struct Arguments {
    #[arg(long, default_value = DEFAULT_ATTACH_SOCKET)]
    attach_socket: PathBuf,
    #[arg(long, default_value = DEFAULT_BINARY_API_SOCKET)]
    binary_api_socket: PathBuf,
    #[arg(long, default_value = DEFAULT_LISTEN_ADDRESS)]
    listen: SocketAddr,
    #[arg(long)]
    certificate_der: PathBuf,
    #[arg(long)]
    private_key_der: PathBuf,
    #[arg(long, default_value = "1")]
    connections: NonZeroUsize,
}

#[derive(Debug, thiserror::Error)]
enum EchoError {
    #[error(transparent)]
    Attach(#[from] AppClientError),
    #[error(transparent)]
    Session(#[from] AppSessionError),
    #[error("read TLS certificate DER from `{path}`")]
    CertificateRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read TLS private key DER from `{path}`")]
    PrivateKeyRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    BinaryApi(#[from] BinaryApiError),
    #[error("decode TLS configuration reply for `{method}`")]
    TlsReplyDecode {
        method: &'static str,
        #[source]
        source: prost::DecodeError,
    },
    #[error("TLS configuration operation `{method}` returned unknown status {status}")]
    TlsStatusInvalid { method: &'static str, status: i32 },
    #[error("TLS configuration operation `{method}` was rejected with {status:?}")]
    TlsRejected {
        method: &'static str,
        status: TlsApiStatus,
    },
    #[error("failed to build the echo Tokio runtime")]
    TokioRuntime {
        #[source]
        source: std::io::Error,
    },
}

fn main() -> Result<(), EchoError> {
    let arguments = Arguments::parse();
    let certificate_der =
        std::fs::read(&arguments.certificate_der).map_err(|source| EchoError::CertificateRead {
            path: arguments.certificate_der.clone(),
            source,
        })?;
    let private_key_der =
        std::fs::read(&arguments.private_key_der).map_err(|source| EchoError::PrivateKeyRead {
            path: arguments.private_key_der.clone(),
            source,
        })?;
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|source| EchoError::TokioRuntime { source })?;

    let mut application = AppClient::attach(
        arguments
            .attach_socket
            .to_str()
            .expect("attach socket path is valid UTF-8"),
    )?;
    let mut binary_api = BinaryApiClient::connect(&arguments.binary_api_socket)?;
    let config = register_server_config(
        &mut binary_api,
        application.application().raw(),
        certificate_der,
        private_key_der,
    )?;
    eprintln!("registered TLS server configuration {config}");

    let policy = AppSessionPolicy::new(
        APP_SESSION_POLICY_VERSION,
        [AppSessionProtocolSelection::with_id("tls", config)],
    )
    .expect("TLS App Session policy uses the supported version and a non-empty protocol name");
    let listener = application.listen(
        "tcp",
        SessionListenEndpoint::new(arguments.listen, DataWorkerId::new(0)),
        policy,
    )?;
    eprintln!(
        "attached Application {:?}; listening for TLS on {} as {listener:?}",
        application.application(),
        arguments.listen,
    );

    for _ in 0..arguments.connections.get() {
        let session = application.accept()?;
        eprintln!(
            "accepted plaintext App Session {:?}",
            session.session_handle()
        );
        tokio_runtime.block_on(run_echo(&session))?;
        eprintln!("closed App Session {:?}", session.session_handle());
    }

    application.unlisten(listener)?;
    remove_config(&mut binary_api, application.application().raw(), config)?;
    eprintln!("removed TLS server configuration {config}");
    Ok(())
}

fn register_server_config(
    binary_api: &mut BinaryApiClient,
    application_id: u64,
    certificate_der: Vec<u8>,
    private_key_der: Vec<u8>,
) -> Result<u64, EchoError> {
    const METHOD: &str = "tls.server-config.register";
    let payload = binary_api.call(
        METHOD,
        &RegisterServerConfigRequest {
            application_id,
            certificate_der: vec![certificate_der],
            private_key_der,
            client_trust_anchor_der: Vec::new(),
            alpn_protocols: Vec::new(),
        }
        .encode_to_vec(),
    )?;
    let reply = RegisterServerConfigReply::decode(payload.as_slice()).map_err(|source| {
        EchoError::TlsReplyDecode {
            method: METHOD,
            source,
        }
    })?;
    tls_status(METHOD, reply.status)?;
    Ok(reply.config_id)
}

fn remove_config(
    binary_api: &mut BinaryApiClient,
    application_id: u64,
    config: u64,
) -> Result<(), EchoError> {
    const METHOD: &str = "tls.config.remove";
    let payload = binary_api.call(
        METHOD,
        &RemoveConfigRequest {
            application_id,
            config_id: config,
        }
        .encode_to_vec(),
    )?;
    let reply = RemoveConfigReply::decode(payload.as_slice()).map_err(|source| {
        EchoError::TlsReplyDecode {
            method: METHOD,
            source,
        }
    })?;
    tls_status(METHOD, reply.status)
}

fn tls_status(method: &'static str, raw: i32) -> Result<(), EchoError> {
    let status = TlsApiStatus::try_from(raw).map_err(|_| EchoError::TlsStatusInvalid {
        method,
        status: raw,
    })?;
    if status == TlsApiStatus::Ok {
        Ok(())
    } else {
        Err(EchoError::TlsRejected { method, status })
    }
}

async fn run_echo(session: &AppSession) -> Result<(), EchoError> {
    let mut buffer = vec![0; ECHO_BUFFER_BYTES];
    loop {
        let event = session.next_event().await?;
        if event.session_index() != session.session_index() {
            continue;
        }
        match event.evt_type {
            SessionEvtType::Connect
            | SessionEvtType::RxDeq
            | SessionEvtType::TxEnq
            | SessionEvtType::ProtocolOutput => {}
            SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
                run_echo_loop(session, &mut buffer, ECHO_BUFFER_BYTES)?;
            }
            SessionEvtType::Close
            | SessionEvtType::HalfClose
            | SessionEvtType::Reset
            | SessionEvtType::Disconnected
            | SessionEvtType::TransportClosed => return Ok(()),
        }
    }
}
