//! External attached TCP echo app.
//!
//! Connects to a running Hammer daemon over the attach socket. Each accepted
//! session is echoed until it closes, then the same Application waits for the
//! next session.
//!
//! ```text
//! cargo run -p hammer --example tun_tcp_echo -- /tmp/hammer-tcp-integration.attach.sock
//! ```

use hammer_app::attach::{AppClient, AppClientError};
use hammer_app::echo::run_echo_loop;
use hammer_app::{
    APP_SESSION_POLICY_VERSION, AppSession, AppSessionError, AppSessionPolicy, DataWorkerId,
    SessionListenEndpoint,
};
use hammer_runtime::app::SessionEvtType;

const DEFAULT_ATTACH_SOCKET: &str = "/tmp/hammer-tcp-integration.attach.sock";
const DEFAULT_LISTEN_ADDRESS: &str = "10.66.77.1:7300";
const ECHO_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
enum EchoError {
    #[error(transparent)]
    Attach(#[from] AppClientError),
    #[error(transparent)]
    Session(#[from] AppSessionError),
    #[error("invalid TCP echo listen address `{address}`")]
    ListenAddress {
        address: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("failed to build the echo Tokio runtime")]
    TokioRuntime {
        #[source]
        source: std::io::Error,
    },
}

fn main() -> Result<(), EchoError> {
    let mut arguments = std::env::args().skip(1);
    let socket_path = arguments
        .next()
        .unwrap_or_else(|| DEFAULT_ATTACH_SOCKET.to_owned());
    let listen_address = arguments
        .next()
        .unwrap_or_else(|| DEFAULT_LISTEN_ADDRESS.to_owned());
    let listen_address = listen_address
        .parse()
        .map_err(|source| EchoError::ListenAddress {
            address: listen_address,
            source,
        })?;
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|source| EchoError::TokioRuntime { source })?;
    let mut client = AppClient::attach(&socket_path)?;
    let policy = AppSessionPolicy::new(APP_SESSION_POLICY_VERSION, [])
        .expect("the built-in plaintext App Session policy is valid");
    let listener = client.listen(
        "tcp",
        SessionListenEndpoint::new(listen_address, DataWorkerId::new(0)),
        policy,
    )?;
    eprintln!(
        "attached Application {:?} via {socket_path}; listening on {listen_address} as {listener:?}",
        client.application(),
    );
    loop {
        let session = client.accept()?;
        eprintln!(
            "accepted session handle {:?} via {socket_path}",
            session.session_handle()
        );
        tokio_runtime.block_on(run_echo(&session))?;
        eprintln!("closed session handle {:?}", session.session_handle());
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
