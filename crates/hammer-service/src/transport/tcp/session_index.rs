use std::net::SocketAddr;

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_core::protocol::transport::TransportConnectionKey;
use hammer_infra::map::FlatHashTable;

use crate::session::SessionId;
use crate::transport::tcp::TcpInputNext;

#[derive(Debug, Clone)]
pub struct TcpSessionConnectionIndex {
    entries: hammer_infra::vec::Vec<TcpConnectionIndexEntry>,
    connection_slots: FlatHashTable<u64, SessionId>,
    tuple_slots: FlatHashTable<TransportConnectionKey, SessionId>,
}

#[derive(Debug, Clone)]
pub struct TcpPendingIndex {
    entries: hammer_infra::vec::Vec<TcpPendingIndexEntry>,
    tuple_slots: FlatHashTable<TransportConnectionKey, SessionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpPendingIndexEntry {
    id: SessionId,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpConnectionIndexEntry {
    session_id: SessionId,
    connection_id: Option<TcpConnectionId>,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    owner: DataWorkerId,
    next: TcpInputNext,
}

impl TcpConnectionIndexEntry {
    #[inline]
    fn new(
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            local,
            remote,
            owner,
            next,
        }
    }

    #[inline]
    fn tuple_key(self) -> Option<TransportConnectionKey> {
        self.local
            .and_then(|local| TransportConnectionKey::from_socket_addrs(0, local, self.remote))
    }
}

impl TcpPendingIndexEntry {
    #[inline]
    fn new(
        id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) -> Self {
        Self {
            id,
            local,
            remote,
            owner,
            next,
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
    pub fn remember_session(
        &mut self,
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        let entry =
            TcpConnectionIndexEntry::new(session_id, connection_id, local, remote, owner, next);
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
    fn unindex_entry(&mut self, entry: TcpConnectionIndexEntry) {
        if let Some(connection_id) = entry.connection_id {
            let key = connection_id.get();
            if self.connection_slots.lookup(&key) == Some(entry.session_id) {
                self.connection_slots.remove(&key);
            }
        }
        if let Some(key) = entry.tuple_key()
            && self.tuple_slots.lookup(&key) == Some(entry.session_id)
        {
            self.tuple_slots.remove(&key);
        }
    }

    #[inline]
    pub fn upsert(
        &mut self,
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        let entry =
            TcpConnectionIndexEntry::new(session_id, connection_id, local, remote, owner, next);
        if let Some(position) = self
            .entries
            .iter()
            .position(|existing| existing.session_id == session_id)
        {
            let old_entry = self.entries[position];
            self.unindex_entry(old_entry);
            self.entries[position] = entry;
        } else {
            self.entries.push(entry);
        }
        self.index_entry(entry);
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
    pub fn lookup_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        let session_id = self
            .tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)?;
        self.entries
            .iter()
            .find(|entry| entry.session_id == session_id)
            .map(|entry| (entry.session_id, entry.owner, entry.next))
    }

    pub fn forget_session(&mut self, session_id: SessionId) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.session_id == session_id)
        {
            let removed = self.entries[index];
            self.unindex_entry(removed);
            let last = self
                .entries
                .pop()
                .expect("connection index entry exists at computed index");
            if index != self.entries.len() {
                self.entries[index] = last;
            }
        }
    }
}

impl Default for TcpSessionConnectionIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl TcpPendingIndex {
    #[inline]
    pub fn empty() -> Self {
        Self {
            entries: hammer_infra::vec::Vec::new(),
            tuple_slots: FlatHashTable::new(),
        }
    }

    #[inline]
    fn index_entry(&mut self, entry: TcpPendingIndexEntry) {
        if let Some(key) = entry.tuple_key() {
            self.tuple_slots.insert(key, entry.id);
        }
    }

    #[inline]
    fn unindex_entry(&mut self, entry: TcpPendingIndexEntry) {
        if let Some(key) = entry.tuple_key()
            && self.tuple_slots.lookup(&key) == Some(entry.id)
        {
            self.tuple_slots.remove(&key);
        }
    }

    pub fn remember_pending_open(
        &mut self,
        id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        let entry = TcpPendingIndexEntry::new(id, local, remote, owner, next);
        if let Some(position) = self.entries.iter().position(|existing| existing.id == id) {
            let old_entry = self.entries[position];
            self.unindex_entry(old_entry);
            self.entries[position] = entry;
        } else {
            self.entries.push(entry);
        }
        self.index_entry(entry);
    }

    pub fn lookup_pending_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        let id = self
            .tuple_slots
            .lookup(&TransportConnectionKey::from_socket_addrs(
                0, local, remote,
            )?)?;
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| (entry.id, entry.owner, entry.next))
    }

    pub fn forget_pending_open(&mut self, id: SessionId) {
        if let Some(index) = self.entries.iter().position(|entry| entry.id == id) {
            let removed = self.entries[index];
            self.unindex_entry(removed);
            let last = self
                .entries
                .pop()
                .expect("pending index entry exists at computed index");
            if index != self.entries.len() {
                self.entries[index] = last;
            }
        }
    }
}

impl Default for TcpPendingIndex {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hammer_adapter::DataWorkerId;
    use hammer_core::protocol::tcp::TcpConnectionId;

    use super::*;

    fn addr(value: &str) -> SocketAddr {
        value.parse().expect("socket address")
    }

    fn remember(
        index: &mut TcpSessionConnectionIndex,
        session_id: SessionId,
        connection_id: Option<u64>,
        local: SocketAddr,
        remote: SocketAddr,
    ) {
        index.remember_session(
            session_id,
            connection_id.map(TcpConnectionId::new),
            Some(local),
            remote,
            DataWorkerId::new(0),
            TcpInputNext::Established,
        );
    }

    #[test]
    fn upsert_removes_stale_tuple_and_connection_id_keys() {
        let mut index = TcpSessionConnectionIndex::empty();
        let session_id = SessionId::new(41);
        let remote = addr("198.51.100.10:443");
        let old_local = addr("192.0.2.10:50010");
        let new_local = addr("192.0.2.11:50011");

        remember(&mut index, session_id, Some(7_001), old_local, remote);
        assert_eq!(
            index.lookup_by_tuple(old_local, remote),
            Some((session_id, DataWorkerId::new(0), TcpInputNext::Established))
        );
        assert_eq!(
            index.lookup_by_connection_id(TcpConnectionId::new(7_001)),
            Some(session_id)
        );

        index.upsert(
            session_id,
            None,
            Some(new_local),
            remote,
            DataWorkerId::new(0),
            TcpInputNext::Established,
        );

        assert_eq!(index.len(), 1);
        assert_eq!(index.lookup_by_tuple(old_local, remote), None);
        assert_eq!(
            index.lookup_by_connection_id(TcpConnectionId::new(7_001)),
            None
        );
        assert_eq!(
            index.lookup_by_tuple(new_local, remote),
            Some((session_id, DataWorkerId::new(0), TcpInputNext::Established))
        );
    }

    #[test]
    fn remove_session_unindexes_removed_entry_and_preserves_swapped_entry() {
        let mut index = TcpSessionConnectionIndex::empty();
        let removed_session = SessionId::new(51);
        let kept_session = SessionId::new(52);
        let removed_local = addr("192.0.2.20:50020");
        let kept_local = addr("192.0.2.21:50021");
        let removed_remote = addr("198.51.100.20:443");
        let kept_remote = addr("198.51.100.21:443");

        remember(
            &mut index,
            removed_session,
            Some(8_001),
            removed_local,
            removed_remote,
        );
        remember(
            &mut index,
            kept_session,
            Some(8_002),
            kept_local,
            kept_remote,
        );

        index.forget_session(removed_session);

        assert_eq!(index.len(), 1);
        assert_eq!(index.lookup_by_tuple(removed_local, removed_remote), None);
        assert_eq!(
            index.lookup_by_connection_id(TcpConnectionId::new(8_001)),
            None
        );
        assert_eq!(
            index.lookup_by_tuple(kept_local, kept_remote),
            Some((
                kept_session,
                DataWorkerId::new(0),
                TcpInputNext::Established
            ))
        );
        assert_eq!(
            index.lookup_by_connection_id(TcpConnectionId::new(8_002)),
            Some(kept_session)
        );
        assert_eq!(
            index.iter_session_ids().collect::<std::vec::Vec<_>>(),
            vec![kept_session]
        );
    }
}
