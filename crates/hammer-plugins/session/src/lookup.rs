use std::cell::UnsafeCell;
use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_infra::pool::Pool;
use hammer_runtime::app::SessionHandle;
use hammer_service::session::{SessionLookup, SessionLookupResult, SessionTableIndex};

use crate::config::IpSessionTableConfig;
use crate::endpoint::{IpHalfOpenHandle, IpSessionEndpoint, IpTransportConnectionId};
use crate::table::IpSessionTable;

type Ip4SessionKey = u128;
type Ip6SessionKey = [u64; 6];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpSessionFamily {
    Ip4,
    Ip6,
}

struct IpSessionLookupState {
    tables: Pool<IpSessionTable>,
    fib_index_to_table_index: [Vec<u32>; 2],
}

pub struct IpSessionLookup {
    state: UnsafeCell<IpSessionLookupState>,
    config: IpSessionTableConfig,
}

// SAFETY: table-pool and FIB-map mutation is confined to startup or a
// WorkerBarrier scope. Published tables are process-lifetime values and Bihash
// owns its writer serialization and lookup publication.
unsafe impl Sync for IpSessionLookup {}

impl IpSessionLookup {
    pub fn new(config: IpSessionTableConfig) -> Self {
        let mut tables = Pool::new();
        let ip4 = tables.insert(IpSessionTable::ip4(config));
        let ip6 = tables.insert(IpSessionTable::ip6(config));
        Self {
            state: UnsafeCell::new(IpSessionLookupState {
                tables,
                fib_index_to_table_index: [vec![ip4], vec![ip6]],
            }),
            config,
        }
    }

    #[inline]
    pub fn table_index(&self, family: IpSessionFamily, fib_index: u32) -> SessionTableIndex {
        let state = unsafe { &*self.state.get() };
        state.fib_index_to_table_index[family_index(family)]
            .get(fib_index as usize)
            .copied()
            .map(SessionTableIndex::new)
            .unwrap_or(SessionTableIndex::INVALID)
    }

    pub fn get_or_alloc_table_index(
        &self,
        family: IpSessionFamily,
        fib_index: u32,
    ) -> SessionTableIndex {
        assert_ne!(
            fib_index,
            u32::MAX,
            "Session table allocation requires a valid FIB index"
        );
        let table_index = self.table_index(family, fib_index);
        if table_index.is_valid() {
            return table_index;
        }

        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("Session table allocation requires the Main Thread and WorkerBarrier");
        // SAFETY: callers may allocate a FIB table only during startup or
        // while the WorkerBarrier excludes all Data Worker readers.
        let state = unsafe { &mut *self.state.get() };
        let table = match family {
            IpSessionFamily::Ip4 => IpSessionTable::ip4(self.config),
            IpSessionFamily::Ip6 => IpSessionTable::ip6(self.config),
        };
        let table_index = state.tables.insert(table);
        let mapping = &mut state.fib_index_to_table_index[family_index(family)];
        mapping.resize(fib_index as usize + 1, SessionTableIndex::INVALID.value());
        mapping[fib_index as usize] = table_index;
        SessionTableIndex::new(table_index)
    }

    pub fn table_memory_size(&self, table_index: SessionTableIndex) -> u64 {
        self.table(table_index)
            .map(IpSessionTable::memory_size)
            .unwrap_or(0)
    }

    pub fn replace_connection_if_current(
        &self,
        connection: &IpTransportConnectionId,
        expected: SessionHandle,
        replacement: SessionHandle,
    ) -> bool {
        let Some(table) = self.table_for_connection(connection) else {
            return false;
        };
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table.sessions().replace_if_current(
                    &ip4_connection_key(connection),
                    expected.into(),
                    replacement.into(),
                )
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table.sessions().replace_if_current(
                    &ip6_connection_key(connection),
                    expected.into(),
                    replacement.into(),
                )
            }
            _ => panic!("Session table family matches the connection family"),
        }
    }

    pub fn remove_connection_if_current(
        &self,
        connection: &IpTransportConnectionId,
        expected: SessionHandle,
    ) -> bool {
        let Some(table) = self.table_for_connection(connection) else {
            return false;
        };
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => table
                .sessions()
                .remove_if_current(&ip4_connection_key(connection), expected.into()),
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => table
                .sessions()
                .remove_if_current(&ip6_connection_key(connection), expected.into()),
            _ => panic!("Session table family matches the connection family"),
        }
    }

    #[inline]
    fn table(&self, table_index: SessionTableIndex) -> Option<&IpSessionTable> {
        if !table_index.is_valid() {
            return None;
        }
        let state = unsafe { &*self.state.get() };
        state.tables.get(table_index.value())
    }

    #[inline]
    fn table_for_connection(
        &self,
        connection: &IpTransportConnectionId,
    ) -> Option<&IpSessionTable> {
        self.table(self.table_index(connection_family(connection), connection.fib_index()))
    }

    fn listener_for_connection(
        &self,
        table: &IpSessionTable,
        connection: &IpTransportConnectionId,
        use_wildcard: bool,
    ) -> Option<SessionHandle> {
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                lookup_ip4_listener(
                    table.sessions(),
                    ip4_listener_key_from_connection(connection),
                    ip4_proxy_key_from_connection(connection),
                    use_wildcard,
                )
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                lookup_ip6_listener(
                    table.sessions(),
                    ip6_listener_key_from_connection(connection),
                    ip6_proxy_key_from_connection(connection),
                    use_wildcard,
                )
            }
            _ => panic!("Session table family matches the connection family"),
        }
    }
}

impl SessionLookup for IpSessionLookup {
    type Endpoint = IpSessionEndpoint;
    type ConnectionId = IpTransportConnectionId;
    type HalfOpenHandle = IpHalfOpenHandle;

    fn add_connection(&self, connection: &Self::ConnectionId, handle: SessionHandle) {
        let table_index =
            self.get_or_alloc_table_index(connection_family(connection), connection.fib_index());
        let table = self
            .table(table_index)
            .expect("allocated Session table remains live");
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table
                    .sessions()
                    .insert(ip4_connection_key(connection), handle.into());
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table
                    .sessions()
                    .insert(ip6_connection_key(connection), handle.into());
            }
            _ => panic!("Session table family matches the connection family"),
        }
    }

    fn remove_connection(&self, connection: &Self::ConnectionId) -> bool {
        let Some(table) = self.table_for_connection(connection) else {
            return false;
        };
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table.sessions().remove(&ip4_connection_key(connection))
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table.sessions().remove(&ip6_connection_key(connection))
            }
            _ => panic!("Session table family matches the connection family"),
        }
    }

    fn add_session_endpoint(
        &self,
        table_index: SessionTableIndex,
        endpoint: &Self::Endpoint,
        handle: SessionHandle,
    ) -> bool {
        let Some(table) = self.table(table_index) else {
            return false;
        };
        match (endpoint.transport().local.address, table) {
            (std::net::IpAddr::V4(_), IpSessionTable::Ip4(table)) => {
                table
                    .sessions()
                    .insert(ip4_listener_key(endpoint), handle.into());
                true
            }
            (std::net::IpAddr::V6(_), IpSessionTable::Ip6(table)) => {
                table
                    .sessions()
                    .insert(ip6_listener_key(endpoint), handle.into());
                true
            }
            _ => false,
        }
    }

    fn remove_session_endpoint(
        &self,
        table_index: SessionTableIndex,
        endpoint: &Self::Endpoint,
    ) -> bool {
        let Some(table) = self.table(table_index) else {
            return false;
        };
        match (endpoint.transport().local.address, table) {
            (std::net::IpAddr::V4(_), IpSessionTable::Ip4(table)) => {
                table.sessions().remove(&ip4_listener_key(endpoint))
            }
            (std::net::IpAddr::V6(_), IpSessionTable::Ip6(table)) => {
                table.sessions().remove(&ip6_listener_key(endpoint))
            }
            _ => false,
        }
    }

    fn add_half_open(&self, connection: &Self::ConnectionId, handle: Self::HalfOpenHandle) {
        let table_index =
            self.get_or_alloc_table_index(connection_family(connection), connection.fib_index());
        let table = self
            .table(table_index)
            .expect("allocated Session table remains live");
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table
                    .half_open()
                    .insert(ip4_connection_key(connection), handle.value());
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table
                    .half_open()
                    .insert(ip6_connection_key(connection), handle.value());
            }
            _ => panic!("Session table family matches the connection family"),
        }
    }

    fn remove_half_open(&self, connection: &Self::ConnectionId) -> bool {
        let Some(table) = self.table_for_connection(connection) else {
            return false;
        };
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table.half_open().remove(&ip4_connection_key(connection))
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table.half_open().remove(&ip6_connection_key(connection))
            }
            _ => panic!("Session table family matches the connection family"),
        }
    }

    fn half_open_handle(&self, connection: &Self::ConnectionId) -> Option<Self::HalfOpenHandle> {
        let table = self.table_for_connection(connection)?;
        match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => table
                .half_open()
                .lookup(&ip4_connection_key(connection))
                .map(IpHalfOpenHandle::new),
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => table
                .half_open()
                .lookup(&ip6_connection_key(connection))
                .map(IpHalfOpenHandle::new),
            _ => panic!("Session table family matches the connection family"),
        }
    }

    fn lookup_connection(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle> {
        let Some(table) = self.table_for_connection(connection) else {
            return SessionLookupResult::NotFound;
        };
        let (session, half_open) = match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => (
                table.sessions().lookup(&ip4_connection_key(connection)),
                table.half_open().lookup(&ip4_connection_key(connection)),
            ),
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => (
                table.sessions().lookup(&ip6_connection_key(connection)),
                table.half_open().lookup(&ip6_connection_key(connection)),
            ),
            _ => panic!("Session table family matches the connection family"),
        };
        if let Some(handle) = session {
            return SessionLookupResult::Session(handle.into());
        }
        if let Some(handle) = half_open {
            return SessionLookupResult::HalfOpen(IpHalfOpenHandle::new(handle));
        }
        self.listener_for_connection(table, connection, true)
            .map(SessionLookupResult::Session)
            .unwrap_or(SessionLookupResult::NotFound)
    }

    fn lookup_connection_on_thread(
        &self,
        connection: &Self::ConnectionId,
        thread_index: u32,
    ) -> SessionLookupResult<Self::HalfOpenHandle> {
        let Some(table) = self.table_for_connection(connection) else {
            return SessionLookupResult::NotFound;
        };
        let session = match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table.sessions().lookup(&ip4_connection_key(connection))
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table.sessions().lookup(&ip6_connection_key(connection))
            }
            _ => panic!("Session table family matches the connection family"),
        };
        if let Some(value) = session {
            let handle = SessionHandle::from(value);
            return if handle.thread_index == thread_index {
                SessionLookupResult::Session(handle)
            } else {
                SessionLookupResult::WrongThread
            };
        }
        let half_open = self.half_open_handle(connection);
        if let Some(handle) = half_open {
            return SessionLookupResult::HalfOpen(handle);
        }
        self.listener_for_connection(table, connection, true)
            .map(SessionLookupResult::Session)
            .unwrap_or(SessionLookupResult::NotFound)
    }

    fn lookup_session(&self, connection: &Self::ConnectionId) -> Option<SessionHandle> {
        let table = self.table_for_connection(connection)?;
        let established = match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => {
                table.sessions().lookup(&ip4_connection_key(connection))
            }
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => {
                table.sessions().lookup(&ip6_connection_key(connection))
            }
            _ => panic!("Session table family matches the connection family"),
        };
        established
            .map(SessionHandle::from)
            .or_else(|| self.listener_for_connection(table, connection, true))
    }

    fn lookup_exact(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle> {
        let Some(table) = self.table_for_connection(connection) else {
            return SessionLookupResult::NotFound;
        };
        let (session, half_open) = match (connection, table) {
            (IpTransportConnectionId::Ip4 { .. }, IpSessionTable::Ip4(table)) => (
                table.sessions().lookup(&ip4_connection_key(connection)),
                table.half_open().lookup(&ip4_connection_key(connection)),
            ),
            (IpTransportConnectionId::Ip6 { .. }, IpSessionTable::Ip6(table)) => (
                table.sessions().lookup(&ip6_connection_key(connection)),
                table.half_open().lookup(&ip6_connection_key(connection)),
            ),
            _ => panic!("Session table family matches the connection family"),
        };
        session
            .map(|value| SessionLookupResult::Session(value.into()))
            .or_else(|| {
                half_open.map(|value| SessionLookupResult::HalfOpen(IpHalfOpenHandle::new(value)))
            })
            .unwrap_or(SessionLookupResult::NotFound)
    }

    fn lookup_listener(
        &self,
        table_index: SessionTableIndex,
        endpoint: &Self::Endpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle> {
        let table = self.table(table_index)?;
        match (endpoint.transport().local.address, table) {
            (std::net::IpAddr::V4(_), IpSessionTable::Ip4(table)) => lookup_ip4_listener(
                table.sessions(),
                ip4_listener_key(endpoint),
                ip4_proxy_key(endpoint),
                use_wildcard,
            ),
            (std::net::IpAddr::V6(_), IpSessionTable::Ip6(table)) => lookup_ip6_listener(
                table.sessions(),
                ip6_listener_key(endpoint),
                ip6_proxy_key(endpoint),
                use_wildcard,
            ),
            _ => None,
        }
    }
}

#[inline]
const fn family_index(family: IpSessionFamily) -> usize {
    match family {
        IpSessionFamily::Ip4 => 0,
        IpSessionFamily::Ip6 => 1,
    }
}

#[inline]
const fn connection_family(connection: &IpTransportConnectionId) -> IpSessionFamily {
    match connection {
        IpTransportConnectionId::Ip4 { .. } => IpSessionFamily::Ip4,
        IpTransportConnectionId::Ip6 { .. } => IpSessionFamily::Ip6,
    }
}

#[inline]
fn ip4_connection_key(connection: &IpTransportConnectionId) -> Ip4SessionKey {
    let IpTransportConnectionId::Ip4 {
        remote_address,
        local_address,
        remote_port,
        local_port,
        transport_protocol,
        ..
    } = connection
    else {
        panic!("IP4 key requires an IP4 connection")
    };
    let word0 = (u64::from(ip4_word(*remote_address)) << 32) | u64::from(ip4_word(*local_address));
    let word1 = (u64::from(*transport_protocol) << 32)
        | (u64::from(*remote_port) << 16)
        | u64::from(*local_port);
    u128::from(word0) | (u128::from(word1) << 64)
}

#[inline]
fn ip6_connection_key(connection: &IpTransportConnectionId) -> Ip6SessionKey {
    let IpTransportConnectionId::Ip6 {
        remote_address,
        local_address,
        remote_port,
        local_port,
        transport_protocol,
        ..
    } = connection
    else {
        panic!("IP6 key requires an IP6 connection")
    };
    let local = ip6_words(*local_address);
    let remote = ip6_words(*remote_address);
    [
        local[0],
        local[1],
        remote[0],
        remote[1],
        (u64::from(*transport_protocol) << 32)
            | (u64::from(*remote_port) << 16)
            | u64::from(*local_port),
        0,
    ]
}

fn ip4_listener_key(endpoint: &IpSessionEndpoint) -> Ip4SessionKey {
    let std::net::IpAddr::V4(address) = endpoint.transport().local.address else {
        panic!("IP4 listener key requires an IP4 endpoint")
    };
    ip4_listener_words(
        address,
        endpoint.transport().local.port,
        endpoint.transport_protocol(),
    )
}

fn ip4_proxy_key(endpoint: &IpSessionEndpoint) -> Ip4SessionKey {
    let std::net::IpAddr::V4(address) = endpoint.transport().local.address else {
        panic!("IP4 proxy key requires an IP4 endpoint")
    };
    ip4_listener_words(address, 0, endpoint.transport_protocol())
}

fn ip6_listener_key(endpoint: &IpSessionEndpoint) -> Ip6SessionKey {
    let std::net::IpAddr::V6(address) = endpoint.transport().local.address else {
        panic!("IP6 listener key requires an IP6 endpoint")
    };
    ip6_listener_words(
        address,
        endpoint.transport().local.port,
        endpoint.transport_protocol(),
    )
}

fn ip6_proxy_key(endpoint: &IpSessionEndpoint) -> Ip6SessionKey {
    let std::net::IpAddr::V6(address) = endpoint.transport().local.address else {
        panic!("IP6 proxy key requires an IP6 endpoint")
    };
    ip6_listener_words(address, 0, endpoint.transport_protocol())
}

fn ip4_listener_key_from_connection(connection: &IpTransportConnectionId) -> Ip4SessionKey {
    let IpTransportConnectionId::Ip4 {
        local_address,
        local_port,
        transport_protocol,
        ..
    } = connection
    else {
        panic!("IP4 listener key requires an IP4 connection")
    };
    ip4_listener_words(*local_address, *local_port, *transport_protocol)
}

fn ip4_proxy_key_from_connection(connection: &IpTransportConnectionId) -> Ip4SessionKey {
    let IpTransportConnectionId::Ip4 {
        local_address,
        transport_protocol,
        ..
    } = connection
    else {
        panic!("IP4 proxy key requires an IP4 connection")
    };
    ip4_listener_words(*local_address, 0, *transport_protocol)
}

fn ip6_listener_key_from_connection(connection: &IpTransportConnectionId) -> Ip6SessionKey {
    let IpTransportConnectionId::Ip6 {
        local_address,
        local_port,
        transport_protocol,
        ..
    } = connection
    else {
        panic!("IP6 listener key requires an IP6 connection")
    };
    ip6_listener_words(*local_address, *local_port, *transport_protocol)
}

fn ip6_proxy_key_from_connection(connection: &IpTransportConnectionId) -> Ip6SessionKey {
    let IpTransportConnectionId::Ip6 {
        local_address,
        transport_protocol,
        ..
    } = connection
    else {
        panic!("IP6 proxy key requires an IP6 connection")
    };
    ip6_listener_words(*local_address, 0, *transport_protocol)
}

#[inline]
fn ip4_listener_words(address: Ipv4Addr, port: u16, protocol: u8) -> Ip4SessionKey {
    let word0 = u64::from(ip4_word(address));
    let word1 = (u64::from(protocol) << 32) | u64::from(port);
    u128::from(word0) | (u128::from(word1) << 64)
}

#[inline]
fn ip6_listener_words(address: Ipv6Addr, port: u16, protocol: u8) -> Ip6SessionKey {
    let address = ip6_words(address);
    [
        address[0],
        address[1],
        0,
        0,
        (u64::from(protocol) << 32) | u64::from(port),
        0,
    ]
}

fn lookup_ip4_listener(
    sessions: &hammer_infra::bihash::Bihash16x8,
    exact: Ip4SessionKey,
    proxy: Ip4SessionKey,
    use_wildcard: bool,
) -> Option<SessionHandle> {
    if let Some(value) = sessions.lookup(&exact) {
        return Some(value.into());
    }
    if use_wildcard {
        let wildcard = exact & !u128::from(u64::MAX);
        if let Some(value) = sessions.lookup(&wildcard) {
            return Some(value.into());
        }
    }
    sessions.lookup(&proxy).map(SessionHandle::from)
}

fn lookup_ip6_listener(
    sessions: &hammer_infra::bihash::Bihash48x8,
    exact: Ip6SessionKey,
    proxy: Ip6SessionKey,
    use_wildcard: bool,
) -> Option<SessionHandle> {
    if let Some(value) = sessions.lookup(&exact) {
        return Some(value.into());
    }
    if use_wildcard {
        let mut wildcard = exact;
        wildcard[0] = 0;
        wildcard[1] = 0;
        if let Some(value) = sessions.lookup(&wildcard) {
            return Some(value.into());
        }
    }
    sessions.lookup(&proxy).map(SessionHandle::from)
}

#[inline]
fn ip6_words(address: Ipv6Addr) -> [u64; 2] {
    let octets = address.octets();
    let mut first = [0; 8];
    let mut second = [0; 8];
    first.copy_from_slice(&octets[..8]);
    second.copy_from_slice(&octets[8..]);
    [u64::from_ne_bytes(first), u64::from_ne_bytes(second)]
}

#[inline]
fn ip4_word(address: Ipv4Addr) -> u32 {
    u32::from_ne_bytes(address.octets())
}
