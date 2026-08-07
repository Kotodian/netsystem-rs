//! Session-side TLS over adjacent App Session FIFOs.
//!
//! rustls owns the TLS state machine, transcript, key schedule, certificate
//! verification, and record protection. This plugin owns one worker-local
//! Session App context and may call only rustls plus Session-owned FIFOs.
//! It does not access a transport, Data-Plane Buffers, or another protocol
//! layer.

use std::io::{self, BufRead, Write};
use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_runtime::app::{ApplicationId, SessionAppContext, SessionAppId};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::SessionId;
use hammer_service::session::protocol::SessionApp;
use hammer_service::session::runtime::SessionWorker;
use rustls::pki_types::ServerName;

mod config;

pub use config::{
    ClientConfig, ConfigError, ConfigId, RegisterClientConfigReply, RegisterClientConfigRequest,
    RegisterServerConfigReply, RegisterServerConfigRequest, RemoveConfigReply, RemoveConfigRequest,
    ServerConfig, TlsApiStatus, TlsMain, register_client_config, register_server_config,
    remove_config,
};

#[hammer_component_macros::runtime_error(subsystem = "tls")]
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("create TLS client connection")]
    ClientConnection {
        #[source]
        source: rustls::Error,
    },
    #[error("create TLS server connection")]
    ServerConnection {
        #[source]
        source: rustls::Error,
    },
    #[error("read TLS records from the lower App Session FIFO")]
    ReadRecords {
        #[source]
        source: io::Error,
    },
    #[error("process received TLS records")]
    ProcessRecords {
        #[source]
        source: rustls::Error,
    },
    #[error("read plaintext from rustls")]
    ReadPlaintext {
        #[source]
        source: io::Error,
    },
    #[error("write plaintext to rustls")]
    WritePlaintext {
        #[source]
        source: io::Error,
    },
    #[error("write TLS records to the lower App Session FIFO")]
    WriteRecords {
        #[source]
        source: io::Error,
    },
    #[error("TLS Session App upper Session is not published yet")]
    UpperSessionMissing,
}

/// A TLS connection owned and advanced by one Data Worker.
#[hammer_component_macros::session_app(name = "tls")]
#[derive(Debug)]
pub struct Connection {
    connection: rustls::Connection,
    peer_closed: bool,
    lower_session: Option<SessionId>,
    upper_session: Option<SessionId>,
}

impl Connection {
    /// Creates a client connection from Main Thread-owned configuration.
    pub fn client(
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Result<Self, rustls::Error> {
        let mut connection =
            rustls::Connection::Client(rustls::ClientConnection::new(config, server_name)?);
        connection.set_buffer_limit(Some(config::TLS_BUFFER_LIMIT));
        Ok(Self {
            connection,
            peer_closed: false,
            lower_session: None,
            upper_session: None,
        })
    }

    /// Creates a server connection from Main Thread-owned configuration.
    pub fn server(config: Arc<rustls::ServerConfig>) -> Result<Self, rustls::Error> {
        let mut connection = rustls::Connection::Server(rustls::ServerConnection::new(config)?);
        connection.set_buffer_limit(Some(config::TLS_BUFFER_LIMIT));
        Ok(Self {
            connection,
            peer_closed: false,
            lower_session: None,
            upper_session: None,
        })
    }

    #[inline]
    pub fn send_close_notify(&mut self) {
        self.connection.send_close_notify();
    }

    #[inline]
    pub const fn peer_has_closed(&self) -> bool {
        self.peer_closed
    }

    fn advance_ingress(
        &mut self,
        mut lower_rx_fifo: &Fifo,
        mut upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        let state = self
            .connection
            .process_new_packets()
            .map_err(|source| Error::ProcessRecords { source })?;
        self.peer_closed |= state.peer_has_closed();

        if upper_rx_fifo.max_enqueue() != 0 {
            let mut plaintext = self.connection.reader();
            match plaintext.fill_buf() {
                Ok(bytes) if !bytes.is_empty() => {
                    let produced = upper_rx_fifo
                        .write(bytes)
                        .map_err(|source| Error::ReadPlaintext { source })?;
                    plaintext.consume(produced);
                    return Ok((0, produced));
                }
                Ok(_) => {}
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
                Err(source) => {
                    return Err(Error::ReadPlaintext { source }.into());
                }
            }
        }

        if lower_rx_fifo.max_dequeue() == 0 {
            return Ok((0, 0));
        }

        let consumed = self
            .connection
            .read_tls(&mut lower_rx_fifo)
            .map_err(|source| Error::ReadRecords { source })?;
        Ok((consumed, 0))
    }

    fn advance_egress(
        &mut self,
        mut upper_tx_fifo: &Fifo,
        mut lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        if self.connection.wants_write() {
            if lower_tx_fifo.max_enqueue() == 0 {
                return Ok((0, 0));
            }
            let produced = self
                .connection
                .write_tls(&mut lower_tx_fifo)
                .map_err(|source| Error::WriteRecords { source })?;
            return Ok((0, produced));
        }

        if self.connection.is_handshaking() || upper_tx_fifo.max_dequeue() == 0 {
            return Ok((0, 0));
        }

        let plaintext = upper_tx_fifo
            .fill_buf()
            .map_err(|source| Error::WritePlaintext { source })?;
        let consumed = self
            .connection
            .writer()
            .write(plaintext)
            .map_err(|source| Error::WritePlaintext { source })?;
        upper_tx_fifo.consume(consumed);
        Ok((consumed, 0))
    }

    /// Direct adjacent-FIFO test and library entry for one ingress step.
    pub fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        self.advance_ingress(lower_rx_fifo, upper_rx_fifo)
    }

    /// Direct adjacent-FIFO test and library entry for one egress step.
    pub fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        self.advance_egress(upper_tx_fifo, lower_tx_fifo)
    }
}

impl SessionApp for Connection {
    fn create(
        application: Option<ApplicationId>,
        _: Option<SessionAppId>,
        opaque: Option<u64>,
        server_name: Option<&str>,
    ) -> RuntimeResult<Self> {
        let application = application.ok_or(config::ConfigError::ApplicationRequired)?;
        let config_id = opaque
            .map(config::ConfigId::from_raw)
            .ok_or(config::ConfigError::ConfigurationRequired)?;
        if let Some(server_name) = server_name {
            let server_name = ServerName::try_from(server_name.to_owned())
                .map_err(|_| config::ConfigError::ServerNameInvalid)?;
            Self::client(
                config::main()?.client_config(application, config_id)?,
                server_name,
            )
            .map_err(|source| Error::ClientConnection { source })
            .map_err(RuntimeError::from)
        } else {
            Self::server(config::main()?.server_config(application, config_id)?)
                .map_err(|source| Error::ServerConnection { source })
                .map_err(RuntimeError::from)
        }
    }

    fn accept(
        &mut self,
        worker: &mut SessionWorker<hammer_infra::pool::Index>,
        session: SessionId,
        context: SessionAppContext,
    ) -> RuntimeResult<()> {
        let upper = worker.create_upper_session(session, context)?;
        self.lower_session = Some(session);
        self.upper_session = Some(upper);
        Ok(())
    }

    fn connected(
        &mut self,
        worker: &mut SessionWorker<hammer_infra::pool::Index>,
        session: SessionId,
        context: SessionAppContext,
    ) -> RuntimeResult<()> {
        let upper = worker.create_upper_session(session, context)?;
        self.lower_session = Some(session);
        self.upper_session = Some(upper);
        Ok(())
    }

    fn builtin_rx(
        &mut self,
        worker: &mut SessionWorker<hammer_infra::pool::Index>,
        _: SessionId,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        let lower = self.lower_session.ok_or(Error::UpperSessionMissing)?;
        let upper = self.upper_session.ok_or(Error::UpperSessionMissing)?;
        let (lower_rx, _) = worker.fifo_pair(lower).ok_or(Error::UpperSessionMissing)?;
        let (upper_rx, _) = worker.fifo_pair(upper).ok_or(Error::UpperSessionMissing)?;
        let (consumed, produced) = self.advance_ingress(lower_rx, upper_rx)?;
        worker.publish_rx_dequeue(lower, consumed)?;
        worker.publish_rx_enqueue(upper, produced)?;
        Ok(())
    }

    fn builtin_tx(
        &mut self,
        worker: &mut SessionWorker<hammer_infra::pool::Index>,
        _: SessionId,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        let lower = self.lower_session.ok_or(Error::UpperSessionMissing)?;
        let upper = self.upper_session.ok_or(Error::UpperSessionMissing)?;
        let (_, lower_tx) = worker.fifo_pair(lower).ok_or(Error::UpperSessionMissing)?;
        let (_, upper_tx) = worker.fifo_pair(upper).ok_or(Error::UpperSessionMissing)?;
        let (consumed, produced) = self.advance_egress(upper_tx, lower_tx)?;
        worker.publish_tx_dequeue(upper, consumed)?;
        worker.publish_tx_enqueue(lower, produced)?;
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<hammer_infra::pool::Index>,
        _: SessionId,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        self.send_close_notify();
        Ok(())
    }

    fn transport_closed(
        &mut self,
        _: &mut SessionWorker<hammer_infra::pool::Index>,
        _: SessionId,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        self.peer_closed = true;
        Ok(())
    }
}

#[hammer_component_macros::init_function(name = "tls_init")]
fn init_tls() -> RuntimeResult<Arc<TlsMain>> {
    config::init()
}

hammer_component_macros::declare_plugin!(
    name = "tls",
    load_after = [],
    init_functions = [__INIT_FN_TLS_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
    session_apps = [__SESSION_APP_CONNECTION],
    binary_api_methods = [
        config::__BINARY_API_REGISTER_SERVER_CONFIG_API,
        config::__BINARY_API_REGISTER_CLIENT_CONFIG_API,
        config::__BINARY_API_REMOVE_CONFIG_API,
    ],
);

#[cfg(test)]
mod tests {
    #[test]
    fn registered_callbacks_are_a_concrete_static_table() {
        let callbacks = crate::__SESSION_APP_CONNECTION_CALLBACKS;
        assert!(callbacks.accept.is_some());
        assert!(callbacks.connected.is_some());
        assert!(callbacks.builtin_rx.is_some());
        assert!(callbacks.builtin_tx.is_some());
        assert!(callbacks.disconnect.is_some());
        assert!(callbacks.transport_closed.is_some());
        assert!(callbacks.cleanup.is_some());
    }
}
