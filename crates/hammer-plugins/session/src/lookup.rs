use std::cell::UnsafeCell;
use hammer_infra::pool::Pool;
use hammer_plugin_ip::{IpVersion, fib_table_lock, fib_table_unlock};
use hammer_runtime::app::SessionHandle;
use hammer_service::net::FibSource;
use hammer_service::session::{SessionLookup, SessionLookupResult};

use crate::config::IpSessionTableConfig;
use crate::endpoint::{IpHalfOpenHandle, IpSessionEndpoint, IpTransportConnectionId};
use crate::table::IpSessionTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpSessionFamily {
    Ip4,
    Ip6,
}

impl From<IpSessionFamily> for usize {
    #[inline(always)]
    fn from(family: IpSessionFamily) -> Self {
        match family {
            IpSessionFamily::Ip4 => 0,
            IpSessionFamily::Ip6 => 1,
        }
    }
}

impl From<IpSessionFamily> for IpVersion {
    #[inline(always)]
    fn from(family: IpSessionFamily) -> Self {
        match family {
            IpSessionFamily::Ip4 => IpVersion::V4,
            IpSessionFamily::Ip6 => IpVersion::V6,
        }
    }
}

impl From<&IpTransportConnectionId> for IpSessionFamily {
    #[inline(always)]
    fn from(connection: &IpTransportConnectionId) -> Self {
        match connection {
            IpTransportConnectionId::Ip4 { .. } => Self::Ip4,
            IpTransportConnectionId::Ip6 { .. } => Self::Ip6,
        }
    }
}

#[derive(Clone, Copy)]
enum Key {
    Ip4(u128),
    Ip6([u64; 6]),
}

impl From<&IpTransportConnectionId> for Key {
    #[inline(always)]
    fn from(connection: &IpTransportConnectionId) -> Self {
        match connection {
            IpTransportConnectionId::Ip4 {
                remote_address,
                local_address,
                remote_port,
                local_port,
                transport_protocol,
                ..
            } => {
                let remote = u64::from(u32::from(*remote_address));
                let local = u64::from(u32::from(*local_address));
                let addresses = (remote << 32) | local;
                let ports = (u64::from(*transport_protocol) << 32)
                    | (u64::from(*remote_port) << 16)
                    | u64::from(*local_port);
                Self::Ip4(u128::from(addresses) | (u128::from(ports) << 64))
            }
            IpTransportConnectionId::Ip6 {
                remote_address,
                local_address,
                remote_port,
                local_port,
                transport_protocol,
                ..
            } => {
                let local = local_address.octets();
                let remote = remote_address.octets();
                let mut local_first = [0; 8];
                let mut local_second = [0; 8];
                let mut remote_first = [0; 8];
                let mut remote_second = [0; 8];
                local_first.copy_from_slice(&local[..8]);
                local_second.copy_from_slice(&local[8..]);
                remote_first.copy_from_slice(&remote[..8]);
                remote_second.copy_from_slice(&remote[8..]);
                Self::Ip6([
                    u64::from_ne_bytes(local_first),
                    u64::from_ne_bytes(local_second),
                    u64::from_ne_bytes(remote_first),
                    u64::from_ne_bytes(remote_second),
                    (u64::from(*transport_protocol) << 32)
                        | (u64::from(*remote_port) << 16)
                        | u64::from(*local_port),
                    0,
                ])
            }
        }
    }
}

impl From<&IpSessionEndpoint> for Key {
    #[inline(always)]
    fn from(endpoint: &IpSessionEndpoint) -> Self {
        match endpoint.transport().local.address {
            std::net::IpAddr::V4(address) => {
                let word = u64::from(u32::from(address));
                Self::Ip4(
                    u128::from(word)
                        | ((u128::from(endpoint.transport_protocol()) << 32
                            | u128::from(endpoint.transport().local.port))
                            << 64),
                )
            }
            std::net::IpAddr::V6(address) => {
                let octets = address.octets();
                let mut first = [0; 8];
                let mut second = [0; 8];
                first.copy_from_slice(&octets[..8]);
                second.copy_from_slice(&octets[8..]);
                Self::Ip6([
                    u64::from_ne_bytes(first),
                    u64::from_ne_bytes(second),
                    0,
                    0,
                    (u64::from(endpoint.transport_protocol()) << 32)
                        | u64::from(endpoint.transport().local.port),
                    0,
                ])
            }
        }
    }
}

impl From<Key> for u128 {
    #[inline(always)]
    fn from(key: Key) -> Self {
        match key {
            Key::Ip4(value) => value,
            Key::Ip6(_) => panic!("IP4 Session key requires an IP4 endpoint"),
        }
    }
}

impl From<Key> for [u64; 6] {
    #[inline(always)]
    fn from(key: Key) -> Self {
        match key {
            Key::Ip4(_) => panic!("IP6 Session key requires an IP6 endpoint"),
            Key::Ip6(value) => value,
        }
    }
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
    pub fn table_index(&self, family: IpSessionFamily, fib_index: u32) -> u32 {
        let state = unsafe { &*self.state.get() };
        state.fib_index_to_table_index[usize::from(family)]
            .get(fib_index as usize)
            .copied()
            .unwrap_or(u32::MAX)
    }

    pub fn get_or_alloc_table_index(&self, family: IpSessionFamily, fib_index: u32) -> u32 {
        assert_ne!(
            fib_index,
            u32::MAX,
            "Session table allocation requires a valid FIB index"
        );
        let table_index = self.table_index(family, fib_index);
        if table_index != u32::MAX {
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
        let mapping = &mut state.fib_index_to_table_index[usize::from(family)];
        mapping.resize(fib_index as usize + 1, u32::MAX);
        mapping[fib_index as usize] = table_index;
        table_index
    }

    pub fn table_memory_size(&self, table_index: u32) -> u64 {
        self.table(table_index)
            .map(IpSessionTable::memory_size)
            .unwrap_or(0)
    }

    pub fn alloc_local(&self) -> u32 {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("local Session table allocation requires the Main Thread and WorkerBarrier");
        let state = unsafe { &mut *self.state.get() };
        state.tables.insert(IpSessionTable::local(self.config))
    }

    pub fn bind_local(&self, appns_index: u32, table_index: u32) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("local Session table binding requires the Main Thread and WorkerBarrier");
        let table = self
            .table_mut(table_index)
            .expect("local Session table binding names an installed table");
        assert!(
            table.is_local(),
            "local binding requires a local Session table"
        );
        assert!(
            table.appns_indices().is_empty(),
            "local Session table binds once"
        );
        table.appns_indices_mut().push(appns_index);
    }

    pub fn bind_global(
        &self,
        appns_index: u32,
        family: IpSessionFamily,
        fib_index: u32,
        source: FibSource,
    ) {
        let table_index = self.get_or_alloc_table_index(family, fib_index);
        let table = self
            .table_mut(table_index)
            .expect("global Session table binding names an installed table");
        assert!(
            table.family_matches(matches!(family, IpSessionFamily::Ip4)),
            "global Session table family matches its FIB"
        );
        assert!(
            !table.appns_indices().contains(&appns_index),
            "namespace binds to a global Session table once"
        );
        table.appns_indices_mut().push(appns_index);
        fib_table_lock(IpVersion::from(family), fib_index, source);
    }

    pub fn unbind_global(
        &self,
        appns_index: u32,
        family: IpSessionFamily,
        fib_index: u32,
        source: FibSource,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("global Session table unbinding requires the Main Thread and WorkerBarrier");
        let table_index = self.table_index(family, fib_index);
        assert!(
            table_index != u32::MAX,
            "global Session table binding must exist"
        );
        let state = unsafe { &mut *self.state.get() };
        let table = state
            .tables
            .get_mut(table_index)
            .expect("global Session table binding remains installed");
        let position = table
            .appns_indices()
            .iter()
            .position(|candidate| *candidate == appns_index)
            .expect("namespace must be associated with the global Session table");
        table.appns_indices_mut().remove(position);
        fib_table_unlock(IpVersion::from(family), fib_index, source);
        if table.appns_indices().is_empty() {
            state.fib_index_to_table_index[usize::from(family)][fib_index as usize] = u32::MAX;
            state
                .tables
                .remove(table_index)
                .expect("unbound global Session table remains installed");
        }
    }

    pub fn free_local(&self, appns_index: u32, table_index: u32) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("local Session table release requires the Main Thread and WorkerBarrier");
        let state = unsafe { &mut *self.state.get() };
        let table = state
            .tables
            .get(table_index)
            .expect("local Session table release names an installed table");
        assert!(
            table.is_local(),
            "local Session table release requires a local table"
        );
        assert_eq!(table.appns_indices(), &[appns_index]);
        state
            .tables
            .remove(table_index)
            .expect("local Session table remains installed until release");
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
        match connection {
            IpTransportConnectionId::Ip4 { .. } => {
                let table = table
                    .ip4_hashes()
                    .expect("IP4 connection uses an IP4 Session table");
                table.sessions().replace_if_current(
                    &u128::from(Key::from(connection)),
                    expected.into(),
                    replacement.into(),
                )
            }
            IpTransportConnectionId::Ip6 { .. } => {
                let table = table
                    .ip6_hashes()
                    .expect("IP6 connection uses an IP6 Session table");
                table.sessions().replace_if_current(
                    &<[u64; 6]>::from(Key::from(connection)),
                    expected.into(),
                    replacement.into(),
                )
            }
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
        match connection {
            IpTransportConnectionId::Ip4 { .. } => table
                .ip4_hashes()
                .expect("IP4 connection uses an IP4 Session table")
                .sessions()
                .remove_if_current(&u128::from(Key::from(connection)), expected.into()),
            IpTransportConnectionId::Ip6 { .. } => table
                .ip6_hashes()
                .expect("IP6 connection uses an IP6 Session table")
                .sessions()
                .remove_if_current(&<[u64; 6]>::from(Key::from(connection)), expected.into()),
        }
    }

    #[inline]
    fn table(&self, table_index: u32) -> Option<&IpSessionTable> {
        if table_index == u32::MAX {
            return None;
        }
        let state = unsafe { &*self.state.get() };
        state.tables.get(table_index)
    }

    #[allow(clippy::mut_from_ref)]
    #[inline]
    fn table_mut(&self, table_index: u32) -> Option<&mut IpSessionTable> {
        if table_index == u32::MAX {
            return None;
        }
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("Session table mutation requires the Main Thread and WorkerBarrier");
        let state = unsafe { &mut *self.state.get() };
        state.tables.get_mut(table_index)
    }

    #[inline]
    fn table_for_connection(
        &self,
        connection: &IpTransportConnectionId,
    ) -> Option<&IpSessionTable> {
        self.table(
            self.table_index(IpSessionFamily::from(connection), connection.fib_index()),
        )
    }

    fn listener_for_connection(
        &self,
        table: &IpSessionTable,
        connection: &IpTransportConnectionId,
        use_wildcard: bool,
    ) -> Option<SessionHandle> {
        table.lookup_listener(Key::from(connection), use_wildcard)
    }
}

impl IpSessionTable {
    #[inline]
    fn lookup_listener(&self, key: Key, use_wildcard: bool) -> Option<SessionHandle> {
        match key {
            Key::Ip4(exact) => {
                let table = self
                    .ip4_hashes()
                    .expect("IP4 listener lookup uses an IP4 Session table");
                if let Some(value) = table.sessions().lookup(&exact) {
                    return Some(value.into());
                }
                if use_wildcard {
                    let wildcard = exact & !u128::from(u64::MAX);
                    if let Some(value) = table.sessions().lookup(&wildcard) {
                        return Some(value.into());
                    }
                }
                let proxy = exact & !(u128::from(u16::MAX) << 64);
                table.sessions().lookup(&proxy).map(SessionHandle::from)
            }
            Key::Ip6(exact) => {
                let table = self
                    .ip6_hashes()
                    .expect("IP6 listener lookup uses an IP6 Session table");
                if let Some(value) = table.sessions().lookup(&exact) {
                    return Some(value.into());
                }
                if use_wildcard {
                    let mut wildcard = exact;
                    wildcard[0] = 0;
                    wildcard[1] = 0;
                    if let Some(value) = table.sessions().lookup(&wildcard) {
                        return Some(value.into());
                    }
                }
                let mut proxy = exact;
                proxy[2] = 0;
                proxy[3] = 0;
                proxy[4] &= !u64::from(u16::MAX);
                table.sessions().lookup(&proxy).map(SessionHandle::from)
            }
        }
    }
}

impl SessionLookup for IpSessionLookup {
    type Endpoint = IpSessionEndpoint;
    type ConnectionId = IpTransportConnectionId;
    type HalfOpenHandle = IpHalfOpenHandle;

    fn add_connection(&self, connection: &Self::ConnectionId, handle: SessionHandle) {
        let table_index =
            self.get_or_alloc_table_index(
                IpSessionFamily::from(connection),
                connection.fib_index(),
            );
        let table = self
            .table(table_index)
            .expect("allocated Session table remains live");
        match connection {
            IpTransportConnectionId::Ip4 { .. } => {
                let table = table
                    .ip4_hashes()
                    .expect("IP4 connection uses an IP4 Session table");
                table
                    .sessions()
                    .insert(u128::from(Key::from(connection)), handle.into());
            }
            IpTransportConnectionId::Ip6 { .. } => {
                let table = table
                    .ip6_hashes()
                    .expect("IP6 connection uses an IP6 Session table");
                table
                    .sessions()
                    .insert(<[u64; 6]>::from(Key::from(connection)), handle.into());
            }
        }
    }

    fn remove_connection(&self, connection: &Self::ConnectionId) -> bool {
        let Some(table) = self.table_for_connection(connection) else {
            return false;
        };
        match connection {
            IpTransportConnectionId::Ip4 { .. } => table
                .ip4_hashes()
                .expect("IP4 connection uses an IP4 Session table")
                .sessions()
                .remove(&u128::from(Key::from(connection))),
            IpTransportConnectionId::Ip6 { .. } => table
                .ip6_hashes()
                .expect("IP6 connection uses an IP6 Session table")
                .sessions()
                .remove(&<[u64; 6]>::from(Key::from(connection))),
        }
    }

    fn add_session_endpoint(
        &self,
        table_index: u32,
        endpoint: &Self::Endpoint,
        handle: SessionHandle,
    ) -> bool {
        let Some(table) = self.table(table_index) else {
            return false;
        };
        match endpoint.transport().local.address {
            std::net::IpAddr::V4(_) => {
                let Some(table) = table.ip4_hashes() else {
                    return false;
                };
                table
                    .sessions()
                    .insert(u128::from(Key::from(endpoint)), handle.into());
                true
            }
            std::net::IpAddr::V6(_) => {
                let Some(table) = table.ip6_hashes() else {
                    return false;
                };
                table
                    .sessions()
                    .insert(<[u64; 6]>::from(Key::from(endpoint)), handle.into());
                true
            }
        }
    }

    fn remove_session_endpoint(&self, table_index: u32, endpoint: &Self::Endpoint) -> bool {
        let Some(table) = self.table(table_index) else {
            return false;
        };
        match endpoint.transport().local.address {
            std::net::IpAddr::V4(_) => table
                .ip4_hashes()
                .is_some_and(|table| {
                    table
                        .sessions()
                        .remove(&u128::from(Key::from(endpoint)))
                }),
            std::net::IpAddr::V6(_) => table
                .ip6_hashes()
                .is_some_and(|table| {
                    table
                        .sessions()
                        .remove(&<[u64; 6]>::from(Key::from(endpoint)))
                }),
        }
    }

    fn add_half_open(&self, connection: &Self::ConnectionId, handle: Self::HalfOpenHandle) {
        let table_index =
            self.get_or_alloc_table_index(
                IpSessionFamily::from(connection),
                connection.fib_index(),
            );
        let table = self
            .table(table_index)
            .expect("allocated Session table remains live");
        match connection {
            IpTransportConnectionId::Ip4 { .. } => {
                let table = table
                    .ip4_hashes()
                    .expect("IP4 connection uses an IP4 Session table");
                table
                    .half_open()
                    .insert(u128::from(Key::from(connection)), handle.value());
            }
            IpTransportConnectionId::Ip6 { .. } => {
                let table = table
                    .ip6_hashes()
                    .expect("IP6 connection uses an IP6 Session table");
                table
                    .half_open()
                    .insert(<[u64; 6]>::from(Key::from(connection)), handle.value());
            }
        }
    }

    fn remove_half_open(&self, connection: &Self::ConnectionId) -> bool {
        let Some(table) = self.table_for_connection(connection) else {
            return false;
        };
        match connection {
            IpTransportConnectionId::Ip4 { .. } => table
                .ip4_hashes()
                .expect("IP4 connection uses an IP4 Session table")
                .half_open()
                .remove(&u128::from(Key::from(connection))),
            IpTransportConnectionId::Ip6 { .. } => table
                .ip6_hashes()
                .expect("IP6 connection uses an IP6 Session table")
                .half_open()
                .remove(&<[u64; 6]>::from(Key::from(connection))),
        }
    }

    fn half_open_handle(&self, connection: &Self::ConnectionId) -> Option<Self::HalfOpenHandle> {
        let table = self.table_for_connection(connection)?;
        match connection {
            IpTransportConnectionId::Ip4 { .. } => table
                .ip4_hashes()
                .expect("IP4 connection uses an IP4 Session table")
                .half_open()
                .lookup(&u128::from(Key::from(connection)))
                .map(IpHalfOpenHandle::new),
            IpTransportConnectionId::Ip6 { .. } => table
                .ip6_hashes()
                .expect("IP6 connection uses an IP6 Session table")
                .half_open()
                .lookup(&<[u64; 6]>::from(Key::from(connection)))
                .map(IpHalfOpenHandle::new),
        }
    }

    fn lookup_connection(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle> {
        let Some(table) = self.table_for_connection(connection) else {
            return SessionLookupResult::NotFound;
        };
        let (session, half_open) = match connection {
            IpTransportConnectionId::Ip4 { .. } => {
                let table = table
                    .ip4_hashes()
                    .expect("IP4 connection uses an IP4 Session table");
                (
                    table.sessions().lookup(&u128::from(Key::from(connection))),
                    table.half_open().lookup(&u128::from(Key::from(connection))),
                )
            }
            IpTransportConnectionId::Ip6 { .. } => {
                let table = table
                    .ip6_hashes()
                    .expect("IP6 connection uses an IP6 Session table");
                (
                    table
                        .sessions()
                        .lookup(&<[u64; 6]>::from(Key::from(connection))),
                    table
                        .half_open()
                        .lookup(&<[u64; 6]>::from(Key::from(connection))),
                )
            }
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
        let session = match connection {
            IpTransportConnectionId::Ip4 { .. } => table
                .ip4_hashes()
                .expect("IP4 connection uses an IP4 Session table")
                .sessions()
                .lookup(&u128::from(Key::from(connection))),
            IpTransportConnectionId::Ip6 { .. } => table
                .ip6_hashes()
                .expect("IP6 connection uses an IP6 Session table")
                .sessions()
                .lookup(&<[u64; 6]>::from(Key::from(connection))),
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
        let established = match connection {
            IpTransportConnectionId::Ip4 { .. } => table
                .ip4_hashes()
                .expect("IP4 connection uses an IP4 Session table")
                .sessions()
                .lookup(&u128::from(Key::from(connection))),
            IpTransportConnectionId::Ip6 { .. } => table
                .ip6_hashes()
                .expect("IP6 connection uses an IP6 Session table")
                .sessions()
                .lookup(&<[u64; 6]>::from(Key::from(connection))),
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
        let (session, half_open) = match connection {
            IpTransportConnectionId::Ip4 { .. } => {
                let table = table
                    .ip4_hashes()
                    .expect("IP4 connection uses an IP4 Session table");
                (
                    table.sessions().lookup(&u128::from(Key::from(connection))),
                    table.half_open().lookup(&u128::from(Key::from(connection))),
                )
            }
            IpTransportConnectionId::Ip6 { .. } => {
                let table = table
                    .ip6_hashes()
                    .expect("IP6 connection uses an IP6 Session table");
                (
                    table
                        .sessions()
                        .lookup(&<[u64; 6]>::from(Key::from(connection))),
                    table
                        .half_open()
                        .lookup(&<[u64; 6]>::from(Key::from(connection))),
                )
            }
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
        table_index: u32,
        endpoint: &Self::Endpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle> {
        let table = self.table(table_index)?;
        table.lookup_listener(Key::from(endpoint), use_wildcard)
    }
}
