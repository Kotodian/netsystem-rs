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
use hammer_app::{
    APP_SESSION_POLICY_VERSION, AppSession, AppSessionError, AppSessionPolicy,
    AppSessionProtocolSelection, DataWorkerId, SessionListenEndpoint,
};
use hammer_plugin_tls::{
    ConfigId, RegisterServerConfigReply, RegisterServerConfigRequest, RemoveConfigReply,
    RemoveConfigRequest, TlsApiStatus,
};
use hammer_runtime::app::SessionEvtType;
use hammer_service::binary_api::{BinaryApiClient, BinaryApiError};
use prost::Message;

const DEFAULT_ATTACH_SOCKET: &str = "/tmp/hammer-tcp-integration.attach.sock";
const DEFAULT_BINARY_API_SOCKET: &str = "/tmp/hammer-tcp-integration.binary-api.sock";
const DEFAULT_LISTEN_ADDRESS: &str = "10.66.77.1:7300";
const ECHO_BUFFER_BYTES: usize = 64 * 1024;

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
    eprintln!("registered TLS server configuration {config:?}");

    let policy = AppSessionPolicy::new(
        APP_SESSION_POLICY_VERSION,
        [AppSessionProtocolSelection::with_id("tls", config.raw())],
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
    eprintln!("removed TLS server configuration {config:?}");
    Ok(())
}

fn register_server_config(
    binary_api: &mut BinaryApiClient,
    application_id: u64,
    certificate_der: Vec<u8>,
    private_key_der: Vec<u8>,
) -> Result<ConfigId, EchoError> {
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
    Ok(ConfigId::from_raw(reply.config_id))
}

fn remove_config(
    binary_api: &mut BinaryApiClient,
    application_id: u64,
    config: ConfigId,
) -> Result<(), EchoError> {
    const METHOD: &str = "tls.config.remove";
    let payload = binary_api.call(
        METHOD,
        &RemoveConfigRequest {
            application_id,
            config_id: config.raw(),
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
            | SessionEvtType::TxDeq
            | SessionEvtType::RxDeq
            | SessionEvtType::TxEnq
            | SessionEvtType::ProtocolOutput => {}
            SessionEvtType::RxEnq => loop {
                let read = session.recv_bytes(&mut buffer);
                if read == 0 {
                    break;
                }
                session.send_all(&buffer[..read]).await?;
                session.consume_rx(read);
            },
            SessionEvtType::Close => return Ok(()),
        }
    }
}
