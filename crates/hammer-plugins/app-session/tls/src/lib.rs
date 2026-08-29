//! Session-side TLS over adjacent App Session FIFOs.
//!
//! rustls owns the TLS state machine, transcript, key schedule, certificate
//! verification, and record protection. This plugin owns one worker-local
//! Session App context and may call only rustls plus Session-owned FIFOs.
//! It does not access a transport, Data-Plane Buffers, or another protocol
//! layer.

use std::io::{self, BufRead, Write};
use std::sync::{Arc, OnceLock};

use hammer_infra::fifo::Fifo;
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult};
use hammer_service::session::protocol::SessionAppVft;
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
    #[error("TLS worker {worker} is not installed")]
    WorkerMissing { worker: usize },
    #[error("TLS worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("TLS worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: hammer_infra::thread_owned::ThreadOwnedError,
    },
    #[error("TLS connection context {context:#x} is missing")]
    ConnectionMissing { context: u64 },
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
#[derive(Debug)]
pub struct Connection {
    connection: rustls::Connection,
    peer_closed: bool,
    lower_session: Option<u32>,
    upper_session: Option<u32>,
}

const TLS_CONNECTION_CAPACITY: usize = 1_024;

struct TlsWorkers {
    workers: Box<[ThreadOwned<Pool<Connection>>]>,
}

static TLS_WORKERS: OnceLock<TlsWorkers> = OnceLock::new();

impl TlsWorkers {
    fn new(worker_count: usize) -> Self {
        Self {
            workers: (0..worker_count)
                .map(|_| ThreadOwned::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn install(&self, worker: DataWorkerId) -> RuntimeResult<()> {
        let slot = self
            .workers
            .get(worker.slot())
            .ok_or(Error::WorkerMissing {
                worker: worker.slot(),
            })?;
        slot.install(Pool::with_capacity(TLS_CONNECTION_CAPACITY))
            .map_err(|_| {
                Error::WorkerAlreadyInstalled {
                    worker: worker.slot(),
                }
                .into()
            })
    }

    fn insert(&self, worker: DataWorkerId, connection: Connection) -> RuntimeResult<u64> {
        let slot = self
            .workers
            .get(worker.slot())
            .ok_or(Error::WorkerMissing {
                worker: worker.slot(),
            })?;
        let context = slot
            .with_mut(|pool| pool.insert(connection))
            .map_err(|source| Error::WorkerAccess {
                worker: worker.slot(),
                source,
            })?;
        Ok(context.into())
    }

    fn with_mut<R>(
        &self,
        worker: DataWorkerId,
        context: u64,
        operation: impl FnOnce(&mut Connection) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let index = u32::try_from(context).map_err(|_| Error::ConnectionMissing { context })?;
        let slot = self
            .workers
            .get(worker.slot())
            .ok_or(Error::WorkerMissing {
                worker: worker.slot(),
            })?;
        slot.with_mut(|pool| {
            let connection = pool
                .get_mut(index)
                .ok_or(Error::ConnectionMissing { context })?;
            operation(connection)
        })
        .map_err(|source| Error::WorkerAccess {
            worker: worker.slot(),
            source,
        })?
    }

    fn remove(&self, worker: DataWorkerId, context: u64) -> RuntimeResult<()> {
        let index = u32::try_from(context).map_err(|_| Error::ConnectionMissing { context })?;
        let slot = self
            .workers
            .get(worker.slot())
            .ok_or(Error::WorkerMissing {
                worker: worker.slot(),
            })?;
        slot.with_mut(|pool| {
            pool.remove(index);
        })
        .map_err(|source| Error::WorkerAccess {
            worker: worker.slot(),
            source,
        })?;
        Ok(())
    }
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

fn tls_workers() -> RuntimeResult<&'static TlsWorkers> {
    TLS_WORKERS
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tls" })
}

fn ensure_connection(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<u64> {
    if context != 0 {
        return Ok(context);
    }
    let (application, _, opaque, server_name) = worker
        .session_app_endpoint(session)
        .ok_or(Error::ConnectionMissing { context })?;
    let connection = Connection::create(Some(application), None, opaque, server_name)?;
    let context = tls_workers()?.insert(worker.worker(), connection)?;
    if let Err(error) = worker.set_app_session(session, context) {
        let _ = tls_workers()?.remove(worker.worker(), context);
        return Err(error);
    }
    Ok(context)
}

fn with_connection<R>(
    worker: &mut SessionWorker,
    context: u64,
    operation: impl FnOnce(&mut Connection, &mut SessionWorker) -> RuntimeResult<R>,
) -> RuntimeResult<R> {
    let workers = tls_workers()?;
    let worker_id = worker.worker();
    workers.with_mut(worker_id, context, |connection| {
        operation(connection, worker)
    })
}

fn accept(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    let context = ensure_connection(worker, session, context)?;
    with_connection(worker, context, |connection, worker| {
        connection.accept(worker, session, context)
    })
}

fn connected(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    let context = ensure_connection(worker, session, context)?;
    with_connection(worker, context, |connection, worker| {
        connection.connected(worker, session, context)
    })
}

fn builtin_rx(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    with_connection(worker, context, |connection, worker| {
        connection.builtin_rx(worker, session, context)
    })
}

fn builtin_tx(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    with_connection(worker, context, |connection, worker| {
        connection.builtin_tx(worker, session, context)
    })
}

fn disconnect(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    with_connection(worker, context, |connection, worker| {
        connection.disconnect(worker, session, context)
    })
}

fn transport_closed(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    with_connection(worker, context, |connection, worker| {
        connection.transport_closed(worker, session, context)
    })
}

fn cleanup(worker: &mut SessionWorker, session: u32, context: u64) -> RuntimeResult<()> {
    if context != 0 {
        tls_workers()?.remove(worker.worker(), context)?;
        worker.set_app_session(session, 0)?;
    }
    Ok(())
}

impl Connection {
    fn create(
        application: Option<u32>,
        _: Option<u32>,
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
        worker: &mut SessionWorker,
        session: u32,
        context: u64,
    ) -> RuntimeResult<()> {
        let upper = worker.create_upper_session(session, context)?;
        self.lower_session = Some(session);
        self.upper_session = Some(upper);
        Ok(())
    }

    fn connected(
        &mut self,
        worker: &mut SessionWorker,
        session: u32,
        context: u64,
    ) -> RuntimeResult<()> {
        let upper = worker.create_upper_session(session, context)?;
        self.lower_session = Some(session);
        self.upper_session = Some(upper);
        Ok(())
    }

    fn builtin_rx(&mut self, worker: &mut SessionWorker, _: u32, _: u64) -> RuntimeResult<()> {
        let lower = self.lower_session.ok_or(Error::UpperSessionMissing)?;
        let upper = self.upper_session.ok_or(Error::UpperSessionMissing)?;
        let (lower_rx, _) = worker.fifo_pair(lower).ok_or(Error::UpperSessionMissing)?;
        let (upper_rx, _) = worker.fifo_pair(upper).ok_or(Error::UpperSessionMissing)?;
        let (consumed, produced) = self.advance_ingress(lower_rx, upper_rx)?;
        worker.publish_rx_dequeue(lower, consumed)?;
        worker.publish_rx_enqueue(upper, produced)?;
        Ok(())
    }

    fn builtin_tx(&mut self, worker: &mut SessionWorker, _: u32, _: u64) -> RuntimeResult<()> {
        let lower = self.lower_session.ok_or(Error::UpperSessionMissing)?;
        let upper = self.upper_session.ok_or(Error::UpperSessionMissing)?;
        let (_, lower_tx) = worker.fifo_pair(lower).ok_or(Error::UpperSessionMissing)?;
        let (_, upper_tx) = worker.fifo_pair(upper).ok_or(Error::UpperSessionMissing)?;
        let (consumed, produced) = self.advance_egress(upper_tx, lower_tx)?;
        worker.publish_tx_dequeue(upper, consumed)?;
        worker.publish_tx_enqueue(lower, produced)?;
        Ok(())
    }

    fn disconnect(&mut self, _: &mut SessionWorker, _: u32, _: u64) -> RuntimeResult<()> {
        self.send_close_notify();
        Ok(())
    }

    fn transport_closed(&mut self, _: &mut SessionWorker, _: u32, _: u64) -> RuntimeResult<()> {
        self.peer_closed = true;
        Ok(())
    }
}

pub(crate) const VFT: SessionAppVft = SessionAppVft {
    name: "tls",
    accept: Some(accept),
    connected: Some(connected),
    disconnect: Some(disconnect),
    transport_closed: Some(transport_closed),
    cleanup: Some(cleanup),
    builtin_rx: Some(builtin_rx),
    builtin_tx: Some(builtin_tx),
    ..SessionAppVft::all_none("tls")
};

#[hammer_component_macros::init_function(name = "tls_init")]
fn init_tls(engine: &mut Engine) -> RuntimeResult<()> {
    config::init()?;
    TLS_WORKERS
        .set(TlsWorkers::new(engine.configured_worker_count()))
        .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "tls" })?;
    Ok(())
}

#[hammer_component_macros::worker_init_function(
    name = "tls_worker_init",
    runs_after = ["session_worker_init"]
)]
fn init_tls_worker(engine: &mut Engine) -> RuntimeResult<()> {
    TLS_WORKERS
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tls" })?
        .install(engine.data_worker_id()?)
}

hammer_component_macros::declare_plugin!(
    name = "tls",
    load_after = [],
    init_functions = [__INIT_FN_TLS_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [__INIT_FN_TLS_WORKER_INIT],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
    binary_api_methods = [
        config::__BINARY_API_REGISTER_SERVER_CONFIG_API,
        config::__BINARY_API_REGISTER_CLIENT_CONFIG_API,
        config::__BINARY_API_REMOVE_CONFIG_API,
    ],
);
