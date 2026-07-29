//! Session-side TLS over adjacent App Session FIFOs.
//!
//! rustls owns the TLS state machine, transcript, key schedule, certificate
//! verification, and record protection. This plugin owns one worker-local
//! connection and may call only rustls plus the source and destination FIFOs
//! supplied by [`AppSessionProtocol`]. It does not access an `AppSession`, a
//! transport, Data-Plane Buffers, or another protocol layer.

use std::io::{self, BufRead, Write};
use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::AppSessionProtocol;
use rustls::pki_types::ServerName;

#[derive(Debug, thiserror::Error)]
enum Error {
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
}

/// A TLS connection owned and advanced by one Data Worker.
#[derive(Debug)]
pub struct Connection {
    connection: rustls::Connection,
    peer_closed: bool,
}

impl Connection {
    /// Creates a client connection from Main Thread-owned configuration.
    pub fn client(
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
        buffer_limit: usize,
    ) -> Result<Self, rustls::Error> {
        let mut connection =
            rustls::Connection::Client(rustls::ClientConnection::new(config, server_name)?);
        connection.set_buffer_limit(Some(buffer_limit));
        Ok(Self {
            connection,
            peer_closed: false,
        })
    }

    /// Creates a server connection from Main Thread-owned configuration.
    pub fn server(
        config: Arc<rustls::ServerConfig>,
        buffer_limit: usize,
    ) -> Result<Self, rustls::Error> {
        let mut connection = rustls::Connection::Server(rustls::ServerConnection::new(config)?);
        connection.set_buffer_limit(Some(buffer_limit));
        Ok(Self {
            connection,
            peer_closed: false,
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
}

impl AppSessionProtocol for Connection {
    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        let state = self
            .connection
            .process_new_packets()
            .map_err(|source| RuntimeError::subsystem("tls", Error::ProcessRecords { source }))?;
        self.peer_closed |= state.peer_has_closed();

        if upper_rx_fifo.max_enqueue() != 0 {
            let mut plaintext = self.connection.reader();
            match plaintext.fill_buf() {
                Ok(bytes) if !bytes.is_empty() => {
                    let mut destination = upper_rx_fifo;
                    let produced = destination.write(bytes).map_err(|source| {
                        RuntimeError::subsystem("tls", Error::ReadPlaintext { source })
                    })?;
                    plaintext.consume(produced);
                    return Ok((0, produced));
                }
                Ok(_) => {}
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
                Err(source) => {
                    return Err(RuntimeError::subsystem(
                        "tls",
                        Error::ReadPlaintext { source },
                    ));
                }
            }
        }

        if lower_rx_fifo.max_dequeue() == 0 {
            return Ok((0, 0));
        }

        let mut source = lower_rx_fifo;
        let consumed = self
            .connection
            .read_tls(&mut source)
            .map_err(|source| RuntimeError::subsystem("tls", Error::ReadRecords { source }))?;
        Ok((consumed, 0))
    }

    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        if self.connection.wants_write() {
            if lower_tx_fifo.max_enqueue() == 0 {
                return Ok((0, 0));
            }
            let mut destination = lower_tx_fifo;
            let produced = self
                .connection
                .write_tls(&mut destination)
                .map_err(|source| RuntimeError::subsystem("tls", Error::WriteRecords { source }))?;
            return Ok((0, produced));
        }

        if self.connection.is_handshaking() || upper_tx_fifo.max_dequeue() == 0 {
            return Ok((0, 0));
        }

        let mut source = upper_tx_fifo;
        let plaintext = source
            .fill_buf()
            .map_err(|source| RuntimeError::subsystem("tls", Error::WritePlaintext { source }))?;
        let consumed = self
            .connection
            .writer()
            .write(plaintext)
            .map_err(|source| RuntimeError::subsystem("tls", Error::WritePlaintext { source }))?;
        source.consume(consumed);
        Ok((consumed, 0))
    }
}

hammer_component_macros::declare_plugin!(
    name = "tls",
    load_after = [],
    init_functions = [],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
);
