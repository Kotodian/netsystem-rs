use std::cell::UnsafeCell;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;

use hammer_infra::bihash::{Bihash, BihashKey};
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;
use hammer_runtime::app::SessionHandle;
use crate::session::SessionEndpoint;
use hammer_runtime::session::{
    SessionConnectEndpoint, SessionListenEndpoint, SessionStreamDirection,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use thiserror::Error;

pub mod congestion;

const LOCAL_ENDPOINT_CLEANUP_THRESHOLD: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxMode {
    Peek,
    Dequeue,
    Internal,
    Datagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    VirtualCircuit,
    Connectionless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    name: &'static str,
    short_name: &'static str,
    tx_mode: TxMode,
    service: Service,
}

impl Options {
    #[inline(always)]
    pub const fn new(
        name: &'static str,
        short_name: &'static str,
        tx_mode: TxMode,
        service: Service,
    ) -> Self {
        Self {
            name,
            short_name,
            tx_mode,
            service,
        }
    }

    #[inline(always)]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[inline(always)]
    pub const fn short_name(self) -> &'static str {
        self.short_name
    }

    #[inline(always)]
    pub const fn tx_mode(self) -> TxMode {
        self.tx_mode
    }

    #[inline(always)]
    pub const fn service(self) -> Service {
        self.service
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct ConnectionFlags: u8 {
        const TX_PACED = 1 << 0;
        const NO_LOOKUP = 1 << 1;
        const DESCHEDULED = 1 << 2;
        const CONNECTIONLESS = 1 << 3;
        const ERROR = 1 << 4;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct SendFlags: u8 {
        const DESCHEDULED = 1 << 0;
        const POSTPONE = 1 << 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendParams {
    Packetized {
        send_space: u32,
        tx_offset: u32,
        mss: u16,
        flags: SendFlags,
    },
    Internal {
        max_burst_size: u32,
        bytes_dequeued: u32,
        flags: SendFlags,
    },
}

impl SendParams {
    #[inline(always)]
    pub const fn flags(self) -> SendFlags {
        match self {
            Self::Packetized { flags, .. } | Self::Internal { flags, .. } => flags,
        }
    }
}

#[derive(Debug)]
pub struct Pacer {
    bytes_per_second: u64,
    bucket: i64,
    last_update_micros: u64,
    tokens_per_microsecond: f32,
    min_burst: u32,
    max_burst: u32,
}

impl Pacer {
    pub const MIN_MSS: u32 = 1460;
    pub const MIN_BURST: u32 = Self::MIN_MSS;
    pub const MAX_BURST_PACKETS: u32 = 43;
    pub const MAX_BURST: u32 = Self::MAX_BURST_PACKETS * Self::MIN_MSS;
    pub const BURSTS_PER_RTT: u64 = 20;

    pub const fn new() -> Self {
        Self {
            bytes_per_second: 0,
            bucket: 0,
            last_update_micros: 0,
            tokens_per_microsecond: 0.0,
            min_burst: Self::MIN_BURST,
            max_burst: Self::MAX_BURST,
        }
    }

    #[inline(always)]
    pub const fn rate(&self) -> u64 {
        self.bytes_per_second
    }

    #[inline(always)]
    pub fn update_bytes(&mut self, bytes: u32) {
        self.bucket = self.bucket.saturating_sub(i64::from(bytes));
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Connection<I> {
    identity: I,
    connection_index: u32,
    thread_index: u32,
    flags: ConnectionFlags,
    pacer: Pacer,
}

impl<I> Connection<I> {
    pub const fn new(identity: I, thread_index: u32) -> Self {
        Self {
            identity,
            connection_index: u32::MAX,
            thread_index,
            flags: ConnectionFlags::empty(),
            pacer: Pacer::new(),
        }
    }

    #[inline(always)]
    pub const fn identity(&self) -> &I {
        &self.identity
    }

    #[inline(always)]
    pub fn identity_mut(&mut self) -> &mut I {
        &mut self.identity
    }

    #[inline(always)]
    pub const fn index(&self) -> u32 {
        self.connection_index
    }

    #[inline(always)]
    pub fn set_index(&mut self, connection_index: u32) {
        assert_eq!(self.connection_index, u32::MAX);
        self.connection_index = connection_index;
    }

    #[inline(always)]
    pub const fn thread_index(&self) -> u32 {
        self.thread_index
    }

    #[inline(always)]
    pub const fn flags(&self) -> ConnectionFlags {
        self.flags
    }

    #[inline(always)]
    pub fn insert_flags(&mut self, flags: ConnectionFlags) {
        self.flags.insert(flags);
    }

    #[inline(always)]
    pub fn remove_flags(&mut self, flags: ConnectionFlags) {
        self.flags.remove(flags);
    }

    #[inline(always)]
    pub const fn pacer(&self) -> &Pacer {
        &self.pacer
    }

    #[inline(always)]
    pub fn pacer_mut(&mut self) -> &mut Pacer {
        &mut self.pacer
    }

    #[inline(always)]
    pub fn update_tx_bytes(&mut self, bytes: u32) {
        self.pacer.update_bytes(bytes);
    }
}

pub trait Transport<T> {
    type Error;

    const OPTIONS: Options;

    fn start_listen(&self, endpoint: SessionEndpoint<T>) -> Result<u32, Self::Error>;

    fn stop_listen(&self, connection_index: u32) -> Result<(), Self::Error>;

    fn connect(&self, endpoint: SessionEndpoint<T>) -> Result<u32, Self::Error>;

    fn half_close(&self, _: u32, _: u32) {}

    fn close(&self, connection_index: u32, thread_index: u32);

    fn reset(&self, connection_index: u32, thread_index: u32) {
        self.close(connection_index, thread_index);
    }

    fn cleanup(&self, connection_index: u32, thread_index: u32);

    fn cleanup_half_open(&self, _: u32) {}

    fn send_params(
        &self,
        connection_index: u32,
        thread_index: u32,
    ) -> Result<SendParams, Self::Error>;

    fn update_time(&self, _: f64, _: u32) {}

    fn enable(&self, _: bool) -> Result<(), Self::Error> {
        Ok(())
    }
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

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Config {
    pub local_endpoints_table_buckets: u32,
    pub local_endpoints_table_memory: u32,
    pub min_src_port: u16,
    pub max_src_port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_endpoints_table_buckets: 0,
            local_endpoints_table_memory: 0,
            min_src_port: 1024,
            max_src_port: u16::MAX,
        }
    }
}

struct LocalEndpoint<E> {
    endpoint: SessionEndpoint<E>,
    references: AtomicU32,
}

struct EndpointCleanup {
    free: Vec<u32>,
    pending: bool,
}

struct PortAllocator {
    seed: u32,
    max_tries: u16,
    min_src_port: u16,
    max_src_port: u16,
}

pub struct TransportMain<E, K: BihashKey, A> {
    local_endpoints_table: Bihash<K, 4>,
    local_endpoints: UnsafeCell<Pool<LocalEndpoint<E>>>,
    endpoint_cleanup: SpinLock<EndpointCleanup>,
    port_allocator: UnsafeCell<PortAllocator>,
    alpn_protocol_by_name: A,
}

unsafe impl<E, K, A> Send for TransportMain<E, K, A>
where
    E: Send,
    K: BihashKey + Send,
    A: Send,
{
}

unsafe impl<E, K, A> Sync for TransportMain<E, K, A>
where
    E: Send,
    K: BihashKey + Send,
    A: Send + Sync,
{
}

impl<E, K, A> TransportMain<E, K, A>
where
    E: Copy,
    K: BihashKey + Copy + Default,
    for<'a> K: From<&'a SessionEndpoint<E>>,
{
    pub fn init(
        storage: &'static OnceLock<Self>,
        config: Config,
        alpn_protocol_by_name: A,
    ) -> RuntimeResult<()> {
        assert!(
            storage
                .set(Self::new(config, alpn_protocol_by_name))
                .is_ok(),
            "TransportMain initialization callback executes once"
        );
        Ok(())
    }

    pub fn global(storage: &'static OnceLock<Self>) -> RuntimeResult<&'static Self> {
        storage
            .get()
            .ok_or(RuntimeError::PluginStateNotInitialized {
                plugin: "transport",
            })
    }

    pub fn new(config: Config, alpn_protocol_by_name: A) -> Self {
        let buckets = if config.local_endpoints_table_buckets == 0 {
            250_000
        } else {
            config.local_endpoints_table_buckets
        };
        let memory = if config.local_endpoints_table_memory == 0 {
            512 << 20
        } else {
            config.local_endpoints_table_memory
        };
        Self {
            local_endpoints_table: Bihash::with_memory_size(buckets, memory),
            local_endpoints: UnsafeCell::new(Pool::new()),
            endpoint_cleanup: SpinLock::new(EndpointCleanup {
                free: Vec::new(),
                pending: false,
            }),
            port_allocator: UnsafeCell::new(PortAllocator {
                seed: 0,
                max_tries: 0,
                min_src_port: config.min_src_port,
                max_src_port: config.max_src_port,
            }),
            alpn_protocol_by_name,
        }
    }

    pub fn mark_used(&self, endpoint: SessionEndpoint<E>) -> bool {
        let key = K::from(&endpoint);
        if self.local_endpoints_table.lookup(&key).is_some() {
            return false;
        }
        let local_endpoints = unsafe { &mut *self.local_endpoints.get() };
        let index = local_endpoints.insert(LocalEndpoint {
            endpoint,
            references: AtomicU32::new(1),
        });
        if self
            .local_endpoints_table
            .insert_if_absent(key, index as u64)
            .is_ok()
        {
            true
        } else {
            let removed = local_endpoints.remove(index);
            debug_assert!(removed.is_some());
            false
        }
    }

    pub fn share(&self, endpoint: &SessionEndpoint<E>) {
        let key = K::from(endpoint);
        let Some(index) = self
            .local_endpoints_table
            .lookup(&key)
            .and_then(|index| u32::try_from(index).ok())
        else {
            return;
        };
        let local_endpoint = self
            .local_endpoints()
            .get(index)
            .expect("local endpoint table entry remains in the endpoint pool");
        let previous = local_endpoint
            .references
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        assert_ne!(previous, u32::MAX, "local endpoint reference count overflowed");
    }

    pub fn release(&self, endpoint: &SessionEndpoint<E>) -> bool {
        let key = K::from(endpoint);
        let Some(index) = self
            .local_endpoints_table
            .lookup(&key)
            .and_then(|index| u32::try_from(index).ok())
        else {
            return false;
        };
        let local_endpoint = self
            .local_endpoints()
            .get(index)
            .expect("local endpoint table entry remains in the endpoint pool");
        let previous = local_endpoint
            .references
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        assert_ne!(previous, 0, "local endpoint reference count underflowed");
        if previous != 1 {
            return false;
        }
        self.local_endpoints_table.remove_if_current(&key, index as u64);
        let mut cleanup = self.endpoint_cleanup.lock();
        cleanup.free.push(index);
        if cleanup.free.len() > LOCAL_ENDPOINT_CLEANUP_THRESHOLD {
            cleanup.pending = true;
        }
        true
    }

    pub fn reclaim(&self) {
        let free = {
            let mut cleanup = self.endpoint_cleanup.lock();
            cleanup.pending = false;
            std::mem::take(&mut cleanup.free)
        };
        let local_endpoints = unsafe { &mut *self.local_endpoints.get() };
        for index in free {
            let reclaim = self
                .local_endpoints()
                .get(index)
                .is_some_and(|endpoint| {
                    endpoint
                        .references
                        .load(std::sync::atomic::Ordering::Acquire)
                        == 0
                });
            if reclaim {
                let removed = local_endpoints.remove(index);
                debug_assert!(removed.is_some());
            }
        }
    }

    pub fn next_source_port(&self) -> u16 {
        let allocator = unsafe { &mut *self.port_allocator.get() };
        let range = allocator.max_src_port.saturating_sub(allocator.min_src_port);
        if range == 0 {
            return allocator.min_src_port;
        }
        allocator.seed = allocator
            .seed
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        allocator.min_src_port + (allocator.seed as u16 % range)
    }

    pub fn record_port_allocation_tries(&self, tries: u16) {
        let allocator = unsafe { &mut *self.port_allocator.get() };
        allocator.max_tries = allocator.max_tries.max(tries);
    }

    #[inline(always)]
    pub fn max_port_allocation_tries(&self) -> u16 {
        unsafe { (&*self.port_allocator.get()).max_tries }
    }

    pub fn clear_port_allocation_stats(&self) {
        unsafe { (&mut *self.port_allocator.get()).max_tries = 0 };
    }

    #[inline(always)]
    pub const fn alpn_protocols(&self) -> &A {
        &self.alpn_protocol_by_name
    }

    pub fn local_endpoints_in_use(&self) -> u32 {
        let local_endpoints = self.local_endpoints();
        local_endpoints
            .iter()
            .filter(|(_, endpoint)| {
                endpoint
                    .references
                    .load(std::sync::atomic::Ordering::Acquire)
                    != 0
            })
            .count()
            .try_into()
            .expect("local endpoint count fits u32")
    }

    pub fn cleanup_pending(&self) -> bool {
        self.endpoint_cleanup.lock().pending
    }

    #[inline]
    fn local_endpoints(&self) -> &Pool<LocalEndpoint<E>> {
        unsafe { &*self.local_endpoints.get() }
    }
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
