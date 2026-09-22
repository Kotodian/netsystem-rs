use std::cell::UnsafeCell;
use std::sync::OnceLock;

use hammer_runtime::app::SessionHandle;
use hammer_runtime::session::{
    SessionConnectEndpoint, SessionListenEndpoint, SessionStreamDirection,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use thiserror::Error;

pub mod congestion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportTxMode {
    Peek,
    Dequeue,
    Internal,
    Datagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportServiceType {
    VirtualCircuit,
    Connectionless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportOptions {
    pub tx_mode: TransportTxMode,
    pub service_type: TransportServiceType,
}

impl TransportOptions {
    #[inline(always)]
    pub const fn new(tx_mode: TransportTxMode, service_type: TransportServiceType) -> Self {
        Self {
            tx_mode,
            service_type,
        }
    }

    #[inline(always)]
    pub const fn tx_mode(self) -> TransportTxMode {
        self.tx_mode
    }

    #[inline(always)]
    pub const fn service_type(self) -> TransportServiceType {
        self.service_type
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportConnectionFlags {
    pub tx_paced: bool,
    pub no_lookup: bool,
    pub descheduled: bool,
    pub connectionless: bool,
    pub error: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSendFlags {
    pub deschedule: bool,
    pub postpone: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSendParams {
    pub send_space: u32,
    pub tx_offset: u32,
    pub send_mss: u16,
    pub max_burst_size: u32,
    pub bytes_dequeued: u32,
    pub flags: TransportSendFlags,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pacer {
    pub bytes_per_second: u64,
    pub bucket: i64,
    pub last_update: u64,
    pub tokens_per_period: f32,
    pub min_burst: u32,
    pub max_burst: u32,
}

impl Pacer {
    pub const MIN_MSS: u32 = 1_460;
    pub const MIN_BURST: u32 = Self::MIN_MSS;
    pub const MAX_BURST_PACKETS: u32 = 43;
    pub const MAX_BURST: u32 = Self::MAX_BURST_PACKETS * Self::MIN_MSS;

    pub const fn new() -> Self {
        Self {
            bytes_per_second: 0,
            bucket: 0,
            last_update: 0,
            tokens_per_period: 0.0,
            min_burst: Self::MIN_BURST,
            max_burst: Self::MAX_BURST,
        }
    }

    #[inline(always)]
    pub const fn is_enabled(&self) -> bool {
        self.bytes_per_second != 0
    }

    #[inline(always)]
    pub fn update(&mut self, now: u64) -> u32 {
        let periods = now.saturating_sub(self.last_update);
        let increment = periods as f32 * self.tokens_per_period;
        if increment > 10.0 {
            self.last_update = now;
            self.bucket = (self.bucket + increment as i64).min(i64::from(self.max_burst));
        }
        if self.bucket >= 0 { self.max_burst } else { 0 }
    }

    #[inline(always)]
    pub fn update_bytes(&mut self, bytes: u32) {
        self.bucket = self.bucket.saturating_sub(i64::from(bytes));
    }
}

impl Default for Pacer {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TransportConnection<E, O = u32> {
    pub session: crate::session::SessionHandle,
    pub connection_index: u32,
    pub worker_index: u32,
    pub flags: TransportConnectionFlags,
    pub endpoint: E,
    pub pacer: Pacer,
    pub opaque: O,
}

impl<E, O> TransportConnection<E, O> {
    #[inline]
    pub fn is_descheduled(&self) -> bool {
        self.flags.descheduled
    }

    #[inline]
    pub fn is_connectionless(&self) -> bool {
        self.flags.connectionless
    }

    #[inline(always)]
    pub fn is_tx_paced(&self) -> bool {
        self.flags.tx_paced
    }

    #[inline(always)]
    pub fn clear_descheduled(&mut self, now: u64) {
        self.flags.descheduled = false;
        if self.is_tx_paced() {
            self.pacer.last_update = now;
            self.pacer.bucket = 0;
        }
    }
}

pub trait Transport<T> {
    type Connection;
    type Attribute;

    fn options(&self) -> TransportOptions;
    fn connect(&self, endpoint: &T, session: crate::session::SessionHandle) -> i32;
    fn connect_stream(&self, endpoint: &T, session: crate::session::SessionHandle) -> i32;
    fn start_listen(&self, endpoint: &T, session: crate::session::SessionHandle) -> u32;
    fn stop_listen(&self, connection_index: u32) -> u32;
    fn half_close(&self, connection_index: u32, worker_index: u32);
    fn close(&self, connection_index: u32, worker_index: u32);
    fn reset(&self, connection_index: u32, worker_index: u32);
    fn cleanup(&self, connection_index: u32, worker_index: u32);
    fn cleanup_half_open(&self, connection_index: u32);
    fn push_header(
        &self,
        connection_index: u32,
        worker_index: u32,
        buffers: &mut [u32],
        available_bytes: u32,
    ) -> u32;
    fn send_params(&self, connection_index: u32, worker_index: u32) -> TransportSendParams;
    fn update_time(&self, now: f64, worker_index: u32);
    fn flush_data(&self, connection_index: u32, worker_index: u32);
    fn custom_tx(
        &self,
        session: crate::session::SessionHandle,
        params: &mut TransportSendParams,
    ) -> i32;
    fn app_rx_event(&self, connection_index: u32, worker_index: u32) -> i32;
    fn connection(&self, connection_index: u32, worker_index: u32) -> Option<&Self::Connection>;
    fn listener(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn half_open(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn endpoint(&self, connection_index: u32, worker_index: u32) -> (T, T);
    fn listener_endpoint(&self, connection_index: u32) -> (T, T);
    fn attribute(
        &self,
        connection_index: u32,
        worker_index: u32,
        attribute: &mut Self::Attribute,
    ) -> i32;
}

pub type TransportStartListen =
    fn(SessionHandle, u32, Option<u64>, SessionListenEndpoint) -> RuntimeResult<u32>;
pub type TransportStopListen = fn(u32) -> RuntimeResult<()>;
pub type TransportConnect =
    fn(&mut crate::session::runtime::SessionWorker, SessionConnectEndpoint) -> RuntimeResult<()>;
pub type TransportConnectStream =
    fn(&mut crate::session::runtime::SessionWorker, SessionConnectEndpoint) -> RuntimeResult<()>;
pub type TransportOpenStream = fn(
    &mut crate::session::runtime::SessionWorker,
    u32,
    SessionStreamDirection,
    u64,
) -> RuntimeResult<u32>;
pub type TransportResetStream =
    fn(&mut crate::session::runtime::SessionWorker, u32, u64) -> RuntimeResult<()>;
pub type TransportStopSending =
    fn(&mut crate::session::runtime::SessionWorker, u32, u64) -> RuntimeResult<()>;
pub type TransportCloseConnection =
    fn(&mut crate::session::runtime::SessionWorker, u32, u64, &[u8]) -> RuntimeResult<()>;

pub trait TransportMain: Sized {
    type Config;
    type Endpoint;
    type LocalEndpoint;

    fn init(config: Self::Config) -> Result<Self, i32>;
    fn global() -> Result<&'static Self, i32>;
    fn mark_used(&self, endpoint: &Self::Endpoint) -> i32;
    fn share(&self, endpoint: &Self::Endpoint);
    fn release(&self, endpoint: &Self::Endpoint) -> i32;
    fn allocate_local(&self, endpoint: Self::Endpoint) -> Result<Self::Endpoint, i32>;
}

/// A concrete transport operation table published in one numeric protocol
/// slot. The slot is assigned by the process-global transport authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportVft {
    pub(crate) start_listen: Option<TransportStartListen>,
    pub(crate) stop_listen: Option<TransportStopListen>,
    pub(crate) connect: Option<TransportConnect>,
    pub(crate) connect_stream: Option<TransportConnectStream>,
    pub(crate) open_stream: Option<TransportOpenStream>,
    pub(crate) reset_stream: Option<TransportResetStream>,
    pub(crate) stop_sending: Option<TransportStopSending>,
    pub(crate) close_connection: Option<TransportCloseConnection>,
}

impl TransportVft {
    #[inline]
    pub const fn new(
        start_listen: Option<TransportStartListen>,
        stop_listen: Option<TransportStopListen>,
        connect: Option<TransportConnect>,
        connect_stream: Option<TransportConnectStream>,
        open_stream: Option<TransportOpenStream>,
        reset_stream: Option<TransportResetStream>,
        stop_sending: Option<TransportStopSending>,
        close_connection: Option<TransportCloseConnection>,
    ) -> Self {
        Self {
            start_listen,
            stop_listen,
            connect,
            connect_stream,
            open_stream,
            reset_stream,
            stop_sending,
            close_connection,
        }
    }
}

/// Independent process-global protocol dispatch table, matching VPP's
static TRANSPORT_VFTS: OnceLock<TransportVftTable> = OnceLock::new();

struct TransportVftTable {
    entries: UnsafeCell<Vec<TransportVft>>,
}

impl TransportVftTable {
    const fn new() -> Self {
        Self {
            entries: UnsafeCell::new(Vec::new()),
        }
    }

    fn register(&self, vft: TransportVft) -> Result<u8, TransportError> {
        // SAFETY: registration is restricted to the Main Thread and, after
        // workers start, the WorkerBarrier. No reader can access the table
        // while this append may reallocate its storage.
        let entries = unsafe { &mut *self.entries.get() };
        let protocol = entries
            .len()
            .checked_add(1)
            .and_then(|protocol| u8::try_from(protocol).ok())
            .ok_or(TransportError::ProtocolSlotsExhausted)?;
        entries.push(vft);
        Ok(protocol)
    }

    fn get(&self, protocol: u8) -> Option<TransportVft> {
        let index = protocol.checked_sub(1)? as usize;
        // SAFETY: workers only read the table outside registration. Dynamic
        // plugin registration stops them at WorkerBarrier before appending.
        unsafe { (&*self.entries.get()).get(index).copied() }
    }
}

// SAFETY: Transport VFT mutation is confined to the Main Thread and is
// barrier-protected after Data Workers start. Reads copy one immutable VFT.
unsafe impl Sync for TransportVftTable {}

#[hammer_component_macros::runtime_error(subsystem = "transport")]
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("global TRANSPORT_VFTS is not initialized")]
    RegistryUnavailable,
    #[error("transport protocol slots are exhausted")]
    ProtocolSlotsExhausted,
}

/// Publishes one concrete transport VFT in the next available protocol slot.
///
/// Slot zero is reserved for the invalid transport value, matching the
/// Session transport index convention. The returned slot is the only protocol
/// identity a plugin needs to retain for its own Session records.
#[inline]
pub fn register_transport(vft: TransportVft) -> Result<u8, TransportError> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("Transport registration runs only from Main Thread init under WorkerBarrier");
    TRANSPORT_VFTS
        .get()
        .ok_or(TransportError::RegistryUnavailable)?
        .register(vft)
}

/// Returns one published transport VFT by its protocol index.
#[inline]
pub fn transport_vft(protocol: u8) -> Option<TransportVft> {
    TRANSPORT_VFTS.get()?.get(protocol)
}

#[hammer_component_macros::init_function(name = "transport_main_init")]
fn init_transport_main() -> RuntimeResult<()> {
    assert!(
        TRANSPORT_VFTS.set(TransportVftTable::new()).is_ok(),
        "Transport dispatch authority initializes once"
    );
    Ok(())
}
