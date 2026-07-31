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
    let mut arguments = std::env::args().skip(1);
    let socket_path = arguments
        .next()
        .unwrap_or_else(|| DEFAULT_ATTACH_SOCKET.to_owned());
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|source| EchoError::TokioRuntime { source })?;
    let client = AppClient::connect(&socket_path)?;
    eprintln!(
        "attached Application {:?} via {socket_path}",
        client.application()
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
            SessionEvtType::Connect | SessionEvtType::TxDeq | SessionEvtType::RxDeq => {}
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
