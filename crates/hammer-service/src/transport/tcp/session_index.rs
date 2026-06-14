use std::net::SocketAddr;

use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_core::protocol::transport::TransportConnectionKey;
use hammer_infra::map::FlatHashTable;

use crate::session::SessionId;
use crate::transport::tcp::TcpConnectionState;

#[derive(Debug, Clone)]
pub struct TcpSessionConnectionIndex {
    entries: hammer_infra::vec::Vec<TcpConnectionIndexEntry>,
    connection_slots: FlatHashTable<u64, SessionId>,
    tuple_slots: FlatHashTable<TransportConnectionKey, SessionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpConnectionIndexEntry {
    session_id: SessionId,
    connection_id: Option<TcpConnectionId>,
    local: Option<SocketAddr>,
    remote: SocketAddr,
}

impl TcpConnectionIndexEntry {
    #[inline]
    fn new(session_id: SessionId, connection: &TcpConnectionState) -> Self {
        Self {
            session_id,
            connection_id: connection.connection_id(),
            local: connection.local(),
            remote: connection.remote(),
        }
    }

    #[inline]
    fn tuple_key(self) -> Option<TransportConnectionKey> {
        self.local
            .and_then(|local| TransportConnectionKey::from_socket_addrs(0, local, self.remote))
    }
}

impl TcpSessionConnectionIndex {
    #[inline]
    pub fn empty() -> Self {
        Self {
            entries: hammer_infra::vec::Vec::new(),
            connection_slots: FlatHashTable::new(),
            tuple_slots: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, session_id: SessionId, connection: &TcpConnectionState) {
        let entry = TcpConnectionIndexEntry::new(session_id, connection);
        self.entries.push(entry);
        self.index_entry(entry);
    }

    #[inline]
    fn index_entry(&mut self, entry: TcpConnectionIndexEntry) {
        if let Some(connection_id) = entry.connection_id {
            self.connection_slots
                .insert(connection_id.get(), entry.session_id);
        }
        if let Some(key) = entry.tuple_key() {
            self.tuple_slots.insert(key, entry.session_id);
        }
    }

    #[inline]
    pub fn upsert(&mut self, session_id: SessionId, connection: &TcpConnectionState) {
        let entry = TcpConnectionIndexEntry::new(session_id, connection);
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|existing| existing.session_id == session_id)
        {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
        self.rebuild_indexes();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn iter_session_ids(&self) -> impl Iterator<Item = SessionId> + '_ {
        self.entries.iter().map(|entry| entry.session_id)
    }

    #[inline]
    pub fn lookup_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        self.connection_slots.lookup(&connection_id.get())
    }

    #[inline]
    pub fn lookup_by_tuple(&self, local: SocketAddr, remote: SocketAddr) -> Option<SessionId> {
        self.tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)
    }

    pub fn remove_session(&mut self, session_id: SessionId) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.session_id == session_id)
        {
            let last = self
                .entries
                .pop()
                .expect("connection index entry exists at computed index");
            if index != self.entries.len() {
                self.entries[index] = last;
            }
        }
        self.rebuild_indexes();
    }

    fn rebuild_indexes(&mut self) {
        self.connection_slots = FlatHashTable::with_capacity(self.entries.len().max(1) * 2);
        self.tuple_slots = FlatHashTable::with_capacity(self.entries.len().max(1) * 2);
        for index in 0..self.entries.len() {
            let entry = self.entries[index];
            self.index_entry(entry);
        }
    }
}

impl Default for TcpSessionConnectionIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}
