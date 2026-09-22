use std::cell::UnsafeCell;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};

use hammer_infra::bihash::{Bihash, BihashKey, hash_words};
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;
use hammer_service::session::{SessionEndpoint, SessionError};
use hammer_service::transport::{TransportConnection, TransportMain};

use crate::endpoint::{IpSessionEndpoint, IpTransportConnectionId, IpTransportEndpoint};

const DEFAULT_LOCAL_ENDPOINT_BUCKETS: u32 = 250_000;
const DEFAULT_LOCAL_ENDPOINT_MEMORY: usize = 512 << 20;
const LOCAL_ENDPOINT_CLEANUP_THRESHOLD: usize = 32;

type IpLocalEndpoint = SessionEndpoint<IpTransportEndpoint>;

pub type IpTransportConnection = TransportConnection<IpTransportConnectionId>;

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct IpTransportConfig {
    #[serde(alias = "local_endpoints_table_buckets")]
    pub local_endpoint_buckets: u32,
    #[serde(alias = "local_endpoints_table_memory")]
    pub local_endpoint_memory: usize,
    #[serde(alias = "min_src_port")]
    pub min_source_port: u16,
    #[serde(alias = "max_src_port")]
    pub max_source_port: u16,
}

impl Default for IpTransportConfig {
    fn default() -> Self {
        Self {
            local_endpoint_buckets: DEFAULT_LOCAL_ENDPOINT_BUCKETS,
            local_endpoint_memory: DEFAULT_LOCAL_ENDPOINT_MEMORY,
            min_source_port: 1_024,
            max_source_port: u16::MAX,
        }
    }
}

pub struct LocalEndpointCleanupState {
    pub freelist: Vec<u32>,
    pub cleanup_pending: bool,
}

struct IpLocalEndpointState {
    endpoint: IpLocalEndpoint,
    references: AtomicU32,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct IpLocalEndpointKey([u64; 3]);

impl BihashKey for IpLocalEndpointKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&self.0)
    }
}

impl From<&IpLocalEndpoint> for IpLocalEndpointKey {
    #[inline(always)]
    fn from(endpoint: &IpLocalEndpoint) -> Self {
        let (first, second) = match endpoint.transport().address {
            IpAddr::V4(address) => (u64::from(u32::from(address)), 0),
            IpAddr::V6(address) => {
                let octets = address.octets();
                let mut first = [0; 8];
                let mut second = [0; 8];
                first.copy_from_slice(&octets[..8]);
                second.copy_from_slice(&octets[8..]);
                (u64::from_be_bytes(first), u64::from_be_bytes(second))
            }
        };
        Self([
            first,
            second,
            (u64::from(endpoint.transport().fib_index) << 32)
                | (u64::from(endpoint.transport().port) << 8)
                | u64::from(endpoint.transport_protocol()),
        ])
    }
}

struct AlpnProtocolTable {
    by_name: Bihash<u64, 7>,
}

impl AlpnProtocolTable {
    fn new() -> Self {
        Self {
            by_name: Bihash::new(64),
        }
    }
}

pub struct IpTransportMain {
    local_endpoints_table: Bihash<IpLocalEndpointKey, 7>,
    local_endpoints: UnsafeCell<Pool<IpLocalEndpointState>>,
    port_allocator_seed: AtomicU32,
    port_allocator_min_src_port: u16,
    port_allocator_max_src_port: u16,
    local_endpoint_cleanup: SpinLock<LocalEndpointCleanupState>,
    alpn_protocols: AlpnProtocolTable,
}

// SAFETY: the Main Thread is the only pool writer. Data Workers update only
// endpoint reference counts and the typed cleanup queue; Bihash publishes its
// own concurrent updates.
unsafe impl Sync for IpTransportMain {}

impl IpTransportMain {
    #[inline]
    fn local_endpoints(&self) -> &Pool<IpLocalEndpointState> {
        unsafe { &*self.local_endpoints.get() }
    }

    fn reclaim_local_endpoints(&self) {
        let freelist = {
            let mut cleanup = self.local_endpoint_cleanup.lock();
            cleanup.cleanup_pending = false;
            std::mem::take(&mut cleanup.freelist)
        };
        let local_endpoints = unsafe { &mut *self.local_endpoints.get() };
        for index in freelist {
            let reclaim = local_endpoints
                .get(index)
                .is_some_and(|endpoint| endpoint.references.load(Ordering::Acquire) == 0);
            if reclaim {
                let removed = local_endpoints.remove(index);
                debug_assert!(removed.is_some());
            }
        }
    }

    fn next_source_port(&self) -> u16 {
        let range = self
            .port_allocator_max_src_port
            .saturating_sub(self.port_allocator_min_src_port);
        if range == 0 {
            return self.port_allocator_min_src_port;
        }
        let mut current = self.port_allocator_seed.load(Ordering::Relaxed);
        loop {
            let next = current.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            match self.port_allocator_seed.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return self.port_allocator_min_src_port + (next as u16 % range);
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl TransportMain for IpTransportMain {
    type Config = IpTransportConfig;
    type Endpoint = IpSessionEndpoint;
    type LocalEndpoint = IpLocalEndpoint;

    fn init(config: Self::Config) -> Result<Self, SessionError> {
        if config.min_source_port >= config.max_source_port {
            return Err(SessionError::Invalid);
        }
        let buckets = if config.local_endpoint_buckets == 0 {
            DEFAULT_LOCAL_ENDPOINT_BUCKETS
        } else {
            config.local_endpoint_buckets
        };
        let memory = if config.local_endpoint_memory == 0 {
            DEFAULT_LOCAL_ENDPOINT_MEMORY
        } else {
            config.local_endpoint_memory
        };
        Ok(Self {
            local_endpoints_table: Bihash::with_memory_size(buckets, memory),
            local_endpoints: UnsafeCell::new(Pool::new()),
            port_allocator_seed: AtomicU32::new(0),
            port_allocator_min_src_port: config.min_source_port,
            port_allocator_max_src_port: config.max_source_port,
            local_endpoint_cleanup: SpinLock::new(LocalEndpointCleanupState {
                freelist: Vec::new(),
                cleanup_pending: false,
            }),
            alpn_protocols: AlpnProtocolTable::new(),
        })
    }

    fn global() -> Result<&'static Self, SessionError> {
        crate::IpSessionMain::global().map(crate::IpSessionMain::transport)
    }

    fn mark_used(&self, endpoint: &Self::Endpoint) -> Result<(), SessionError> {
        let local = local_endpoint(endpoint);
        let key = IpLocalEndpointKey::from(&local);
        if self.local_endpoints_table.lookup(&key).is_some() {
            return Err(SessionError::PortInUse);
        }
        let local_endpoints = unsafe { &mut *self.local_endpoints.get() };
        let index = local_endpoints.insert(IpLocalEndpointState {
            endpoint: local,
            references: AtomicU32::new(1),
        });
        if self
            .local_endpoints_table
            .insert_if_absent(key, u64::from(index))
            .is_err()
        {
            let removed = local_endpoints.remove(index);
            debug_assert!(removed.is_some());
            return Err(SessionError::PortInUse);
        }
        Ok(())
    }

    fn share(&self, endpoint: &Self::Endpoint) {
        let local = local_endpoint(endpoint);
        let key = IpLocalEndpointKey::from(&local);
        let Some(index) = self
            .local_endpoints_table
            .lookup(&key)
            .and_then(|index| u32::try_from(index).ok())
        else {
            return;
        };
        let endpoint = self
            .local_endpoints()
            .get(index)
            .expect("local endpoint table entry remains in its pool");
        let previous = endpoint.references.fetch_add(1, Ordering::AcqRel);
        assert_ne!(
            previous,
            u32::MAX,
            "local endpoint reference count overflowed"
        );
    }

    fn release(&self, endpoint: &Self::Endpoint) -> Result<(), SessionError> {
        let local = local_endpoint(endpoint);
        let key = IpLocalEndpointKey::from(&local);
        let Some(index) = self
            .local_endpoints_table
            .lookup(&key)
            .and_then(|index| u32::try_from(index).ok())
        else {
            return Err(SessionError::AddressNotInUse);
        };
        let endpoint = self
            .local_endpoints()
            .get(index)
            .expect("local endpoint table entry remains in its pool");
        let previous = endpoint.references.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(previous, 0, "local endpoint reference count underflowed");
        if previous != 1 {
            return Err(SessionError::AddressNotInUse);
        }
        let removed = self
            .local_endpoints_table
            .remove_if_current(&key, u64::from(index));
        assert!(
            removed,
            "released local endpoint remains in its lookup table"
        );
        let mut cleanup = self.local_endpoint_cleanup.lock();
        cleanup.freelist.push(index);
        if cleanup.freelist.len() > LOCAL_ENDPOINT_CLEANUP_THRESHOLD {
            cleanup.cleanup_pending = true;
        }
        Ok(())
    }

    fn allocate_local(
        &self,
        mut endpoint: Self::Endpoint,
    ) -> Result<Self::Endpoint, SessionError> {
        if self.local_endpoint_cleanup.lock().cleanup_pending {
            self.reclaim_local_endpoints();
        }
        let mut config = *endpoint.transport();
        if config.local.address.is_unspecified() {
            return Err(SessionError::NoIp);
        }
        if config.local.port != 0 {
            self.mark_used(&endpoint)?;
            return Ok(endpoint);
        }
        let limit = self
            .port_allocator_max_src_port
            .saturating_sub(self.port_allocator_min_src_port);
        for _ in 0..limit {
            config.local.port = self.next_source_port();
            endpoint = SessionEndpoint::new(config, endpoint.transport_protocol());
            match self.mark_used(&endpoint) {
                Ok(()) => return Ok(endpoint),
                Err(SessionError::PortInUse) => {}
                Err(error) => return Err(error),
            }
        }
        Err(SessionError::NoPort)
    }
}

#[inline(always)]
fn local_endpoint(endpoint: &IpSessionEndpoint) -> IpLocalEndpoint {
    SessionEndpoint::new(endpoint.transport().local, endpoint.transport_protocol())
}

impl From<IpSessionEndpoint> for IpTransportConnectionId {
    #[inline(always)]
    fn from(endpoint: IpSessionEndpoint) -> Self {
        let config = *endpoint.transport();
        match (config.local.address, config.peer.address) {
            (IpAddr::V4(local_address), IpAddr::V4(remote_address)) => Self::Ip4 {
                remote_address,
                local_address,
                fib_index: config.local.fib_index,
                remote_port: config.peer.port,
                local_port: config.local.port,
                dscp: config.dscp,
                transport_protocol: endpoint.transport_protocol(),
            },
            (IpAddr::V6(local_address), IpAddr::V6(remote_address)) => Self::Ip6 {
                remote_address,
                local_address,
                fib_index: config.local.fib_index,
                remote_port: config.peer.port,
                local_port: config.local.port,
                dscp: config.dscp,
                transport_protocol: endpoint.transport_protocol(),
            },
            _ => panic!("IP transport endpoint family mismatch"),
        }
    }
}
