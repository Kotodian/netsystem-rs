//! External attached TCP echo app.
//!
//! Connects to a running Hammer daemon over the attach socket. The first
//! publication delivers the listener session; every following connect blocks
//! until the daemon accepts a TCP session and publishes its FIFO/event-queue
//! descriptors. Each accepted session is echoed until it closes.
//!
//! ```text
//! cargo run -p hammer --example tun_tcp_echo -- /tmp/hammer-tcp-integration.attach.sock
//! ```

use hammer_app::attach::{AppClient, AppClientError};
use hammer_app::{AppSession, AppSessionError};
use hammer_runtime::app::SessionEvtType;

const DEFAULT_ATTACH_SOCKET: &str = "/tmp/hammer-tcp-integration.attach.sock";
const ECHO_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
enum EchoError {
    #[error(transparent)]
    Attach(#[from] AppClientError),
    #[error(transparent)]
    Session(#[from] AppSessionError),
    #[error("failed to build the echo Tokio runtime")]
    TokioRuntime {
        #[source]
        source: std::io::Error,
    },
}

fn main() -> Result<(), EchoError> {
    let socket_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ATTACH_SOCKET.to_owned());
    let listener = AppClient::connect(&socket_path)?;
    eprintln!(
        "attached session handle {:?} via {socket_path}",
        listener.session_handle()
    );
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|source| EchoError::TokioRuntime { source })?;
    loop {
        let session = AppClient::connect(&socket_path)?;
        eprintln!("accepted session handle {:?}", session.session_handle());
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
            SessionEvtType::Connect | SessionEvtType::TxDeq => {}
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
