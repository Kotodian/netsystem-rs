use std::cell::UnsafeCell;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;

use hammer_infra::bihash::Bihash;
use hammer_infra::pool::Pool;
use hammer_runtime::app::SessionHandle;
use hammer_runtime::session::{
    SessionConnectEndpoint, SessionListenEndpoint, SessionStreamDirection,
};
use hammer_runtime::sync::SpinLock;
use hammer_runtime::{GlobalMain, RuntimeError, RuntimeResult};
use thiserror::Error;

pub mod congestion;

const LOCAL_ENDPOINT_CAPACITY: usize = 1024;
const LOCAL_ENDPOINT_CLEANUP_THRESHOLD: usize = 32;
const ALPN_PROTOCOL_CAPACITY: u32 = 64;

pub type TransportStartListen =
    fn(SessionHandle, u32, Option<u64>, SessionListenEndpoint) -> RuntimeResult<u32>;
pub type TransportStopListen = fn(u32) -> RuntimeResult<()>;
pub type TransportConnect = fn(SessionConnectEndpoint) -> RuntimeResult<()>;
pub type TransportConnectStream = fn(SessionConnectEndpoint) -> RuntimeResult<()>;
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

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct TransportEndpointKey([u64; 3]);

impl TransportEndpointKey {
    #[inline]
    fn new(endpoint: TransportEndpoint, protocol: u8) -> Self {
        let (ip_high, ip_low) = match endpoint.address {
            SocketAddr::V4(address) => (u64::from(u32::from(*address.ip())), 0),
            SocketAddr::V6(address) => {
                let octets = address.ip().octets();
                let mut high = [0; 8];
                let mut low = [0; 8];
                high.copy_from_slice(&octets[..8]);
                low.copy_from_slice(&octets[8..]);
                (u64::from_be_bytes(high), u64::from_be_bytes(low))
            }
        };
        Self([
            ip_high,
            ip_low,
            (u64::from(endpoint.fib_index) << 32)
                | (u64::from(endpoint.address.port()) << 8)
                | u64::from(protocol),
        ])
    }
}

impl hammer_infra::bihash::BihashKey for TransportEndpointKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        hammer_infra::bihash::hash_words(&self.0)
    }
}

/// Local endpoint facts used as the key for the Transport endpoint table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransportEndpoint {
    address: SocketAddr,
    fib_index: u32,
}

/// VPP-shaped lookup table from a local endpoint tuple to a Pool index.
type TransportEndpointTable = Bihash<TransportEndpointKey, 4>;

struct LocalEndpoint {
    endpoint: TransportEndpoint,
    protocol: u8,
    ref_count: AtomicU32,
}

struct LocalEndpointCleanup {
    pending: Vec<u32>,
    scheduled: bool,
}

/// Process-wide Transport policy and lifecycle authority.
///
/// Concrete protocol operation tables are deliberately kept in
/// [`TRANSPORT_VFTS`]. This Main owns Transport-wide state; it does not own a
/// concrete protocol Main or a Session/Application Main.
pub struct TransportMain {
    local_endpoints_table: TransportEndpointTable,
    local_endpoints: UnsafeCell<Pool<LocalEndpoint>>,
    cleanup: SpinLock<LocalEndpointCleanup>,
    port_allocator_seed: u32,
    port_alloc_max_tries: u16,
    port_allocator_min_src_port: u16,
    port_allocator_max_src_port: u16,
    alpn_protocol_by_name: Bihash<u64, 7>,
}

impl TransportMain {
    /// Initializes and publishes the process-global Transport authority.
    pub fn init() -> RuntimeResult<()> {
        if TRANSPORT_MAIN.get().is_some() || TRANSPORT_VFTS.get().is_some() {
            return Err(TransportError::MainAlreadyInitialized.into());
        }

        TRANSPORT_MAIN
            .set(Self::new())
            .map_err(|_| TransportError::MainAlreadyInitialized)?;
        assert!(
            TRANSPORT_VFTS.set(TransportVftTable::new()).is_ok(),
            "Transport VFT authority changed after the initialization preflight"
        );
        Ok(())
    }

    /// Returns the published process-global Transport authority.
    pub fn global() -> RuntimeResult<&'static Self> {
        TRANSPORT_MAIN
            .get()
            .ok_or(RuntimeError::PluginStateNotInitialized {
                plugin: "transport",
            })
    }
}

// SAFETY: control-path callers mutate the Pool through the `UnsafeCell` while
// Data Workers only read stable entries and update atomic reference counts.
// Pool reclamation is performed at the control/barrier boundary after those
// readers have stopped accessing the reclaimed entry.
unsafe impl Send for TransportMain {}
unsafe impl Sync for TransportMain {}

impl TransportMain {
    #[inline]
    pub fn new() -> Self {
        Self {
            local_endpoints_table: TransportEndpointTable::new(LOCAL_ENDPOINT_CAPACITY as u32),
            local_endpoints: UnsafeCell::new(Pool::new()),
            cleanup: SpinLock::new(LocalEndpointCleanup {
                pending: Vec::new(),
                scheduled: false,
            }),
            port_allocator_seed: 0,
            port_alloc_max_tries: 0,
            port_allocator_min_src_port: 0,
            port_allocator_max_src_port: u16::MAX,
            alpn_protocol_by_name: Bihash::new(ALPN_PROTOCOL_CAPACITY),
        }
    }

    /// Claims one local endpoint for a transport and returns its Pool index.
    ///
    /// Pool insertion is rolled back when the endpoint table reports a
    /// concurrent owner, so a failed claim does not publish partial state.
    pub fn mark_used(
        &self,
        protocol: u8,
        address: SocketAddr,
        fib_index: u32,
    ) -> Result<u32, TransportError> {
        let endpoint = TransportEndpoint { address, fib_index };
        let key = TransportEndpointKey::new(endpoint, protocol);
        if self.local_endpoints_table.lookup(&key).is_some() {
            return Err(TransportError::LocalEndpointInUse {
                address,
                fib_index,
                protocol,
            });
        }
        // SAFETY: endpoint allocation and the table publication are serialized
        // on the transport control path.
        let local_endpoints = unsafe { &mut *self.local_endpoints.get() };
        let index = local_endpoints.insert(LocalEndpoint {
            endpoint,
            protocol,
            ref_count: AtomicU32::new(1),
        });
        match self
            .local_endpoints_table
            .insert_if_absent(key, index as u64)
        {
            Ok(()) => Ok(index),
            Err(_) => {
                let removed = local_endpoints.remove(index);
                debug_assert!(removed.is_some());
                Err(TransportError::LocalEndpointInUse {
                    address,
                    fib_index,
                    protocol,
                })
            }
        }
    }

    /// Adds one reference to an existing local endpoint.
    pub fn share(
        &self,
        protocol: u8,
        address: SocketAddr,
        fib_index: u32,
    ) -> Result<(), TransportError> {
        let key = TransportEndpointKey::new(TransportEndpoint { address, fib_index }, protocol);
        let Some(index) = self
            .local_endpoints_table
            .lookup(&key)
            .and_then(|index| u32::try_from(index).ok())
        else {
            return Err(TransportError::LocalEndpointMissing {
                address,
                fib_index,
                protocol,
            });
        };
        let Some(endpoint) = self.local_endpoints().get(index) else {
            return Err(TransportError::LocalEndpointMissing {
                address,
                fib_index,
                protocol,
            });
        };
        let mut references = endpoint
            .ref_count
            .load(std::sync::atomic::Ordering::Acquire);
        loop {
            if references == 0 {
                return Err(TransportError::LocalEndpointMissing {
                    address,
                    fib_index,
                    protocol,
                });
            }
            let next = references.checked_add(1).ok_or(
                TransportError::LocalEndpointReferenceOverflow {
                    address,
                    fib_index,
                    protocol,
                },
            )?;
            match endpoint.ref_count.compare_exchange_weak(
                references,
                next,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => references = current,
            }
        }
    }

    /// Releases one local endpoint reference and queues zero-transition
    /// entries for control-worker reclamation.
    pub fn release(
        &self,
        protocol: u8,
        address: SocketAddr,
        fib_index: u32,
    ) -> Result<(), TransportError> {
        let key = TransportEndpointKey::new(TransportEndpoint { address, fib_index }, protocol);
        let Some(index) = self
            .local_endpoints_table
            .lookup(&key)
            .and_then(|index| u32::try_from(index).ok())
        else {
            return Err(TransportError::LocalEndpointMissing {
                address,
                fib_index,
                protocol,
            });
        };
        let Some(endpoint) = self.local_endpoints().get(index) else {
            return Err(TransportError::LocalEndpointMissing {
                address,
                fib_index,
                protocol,
            });
        };

        let mut references = endpoint
            .ref_count
            .load(std::sync::atomic::Ordering::Acquire);
        loop {
            if references == 0 {
                return Err(TransportError::LocalEndpointMissing {
                    address,
                    fib_index,
                    protocol,
                });
            }
            let next = references - 1;
            match endpoint.ref_count.compare_exchange_weak(
                references,
                next,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) if next > 0 => {
                    return Ok(());
                }
                Ok(_) => {
                    self.local_endpoints_table
                        .remove_if_current(&key, index as u64);
                    let mut cleanup = self.cleanup.lock();
                    cleanup.pending.push(index);
                    if !cleanup.scheduled
                        && cleanup.pending.len() > LOCAL_ENDPOINT_CLEANUP_THRESHOLD
                    {
                        cleanup.scheduled = true;
                    }
                    return Ok(());
                }
                Err(current) => references = current,
            }
        }
    }

    /// Reclaims zero-reference local endpoints on the transport control
    /// worker. Workers only enqueue indexes; Pool mutation stays here.
    pub fn reclaim(&self) -> Result<(), TransportError> {
        let pending = {
            let mut cleanup = self.cleanup.lock();
            cleanup.scheduled = false;
            std::mem::take(&mut cleanup.pending)
        };
        let mut retained = Vec::new();
        for index in pending {
            let reclaim = self.local_endpoints().get(index).is_some_and(|endpoint| {
                endpoint
                    .ref_count
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0
            });
            if reclaim {
                // SAFETY: reclamation runs on the transport control path after
                // workers have released their endpoint references.
                let removed = unsafe { &mut *self.local_endpoints.get() }.remove(index);
                debug_assert!(removed.is_some());
            } else {
                retained.push(index);
            }
        }
        let mut cleanup = self.cleanup.lock();
        cleanup.pending = retained;
        if cleanup.pending.len() > LOCAL_ENDPOINT_CLEANUP_THRESHOLD {
            cleanup.scheduled = true;
        }
        Ok(())
    }

    #[inline]
    fn local_endpoints(&self) -> &Pool<LocalEndpoint> {
        // SAFETY: the control worker does not reclaim Pool entries while Data
        // Workers may read them; each worker-visible field is immutable or an
        // atomic refcount, and reclamation is serialized at the control/barrier
        // boundary.
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

/// The process-global Transport Main, published during ordered init.
pub static TRANSPORT_MAIN: OnceLock<TransportMain> = OnceLock::new();

/// Independent process-global protocol dispatch table, matching VPP's
/// append-only `tp_vfts` authority rather than embedding dispatch in
/// `TransportMain`.
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
    #[error("global TransportMain or TRANSPORT_VFTS is already initialized")]
    MainAlreadyInitialized,
    #[error("global TRANSPORT_VFTS is not initialized")]
    RegistryUnavailable,
    #[error("transport protocol slots are exhausted")]
    ProtocolSlotsExhausted,
    #[error("transport operation `{operation}` is not registered")]
    OperationUnsupported { operation: &'static str },
    #[error(
        "local endpoint {address} on FIB {fib_index} for transport {protocol} is already in use"
    )]
    LocalEndpointInUse {
        address: SocketAddr,
        fib_index: u32,
        protocol: u8,
    },
    #[error("local endpoint {address} on FIB {fib_index} for transport {protocol} is missing")]
    LocalEndpointMissing {
        address: SocketAddr,
        fib_index: u32,
        protocol: u8,
    },
    #[error(
        "local endpoint {address} on FIB {fib_index} for transport {protocol} reference count overflowed"
    )]
    LocalEndpointReferenceOverflow {
        address: SocketAddr,
        fib_index: u32,
        protocol: u8,
    },
}

/// Returns the published process-global Transport authority.
#[inline]
pub fn transport_main() -> &'static TransportMain {
    TransportMain::global().expect("TransportMain is initialized before Transport use")
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
fn init_transport_main(_: &mut GlobalMain) -> RuntimeResult<()> {
    TransportMain::init()
}
