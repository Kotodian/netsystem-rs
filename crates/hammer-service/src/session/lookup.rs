use hammer_runtime::app::SessionHandle;

use super::SessionTableIndex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLookupResult<H> {
    Session(SessionHandle),
    HalfOpen(H),
    WrongThread,
    NotFound,
}

pub trait SessionLookup {
    type Endpoint;
    type ConnectionId;
    type HalfOpenHandle;

    fn add_connection(&self, connection: &Self::ConnectionId, handle: SessionHandle);

    fn remove_connection(&self, connection: &Self::ConnectionId) -> bool;

    fn add_session_endpoint(
        &self,
        table_index: SessionTableIndex,
        endpoint: &Self::Endpoint,
        handle: SessionHandle,
    ) -> bool;

    fn remove_session_endpoint(
        &self,
        table_index: SessionTableIndex,
        endpoint: &Self::Endpoint,
    ) -> bool;

    fn add_half_open(&self, connection: &Self::ConnectionId, handle: Self::HalfOpenHandle);

    fn remove_half_open(&self, connection: &Self::ConnectionId) -> bool;

    fn half_open_handle(&self, connection: &Self::ConnectionId) -> Option<Self::HalfOpenHandle>;

    fn lookup_connection(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle>;

    fn lookup_connection_on_thread(
        &self,
        connection: &Self::ConnectionId,
        thread_index: u32,
    ) -> SessionLookupResult<Self::HalfOpenHandle>;

    fn lookup_session(&self, connection: &Self::ConnectionId) -> Option<SessionHandle>;

    fn lookup_exact(
        &self,
        connection: &Self::ConnectionId,
    ) -> SessionLookupResult<Self::HalfOpenHandle>;

    fn lookup_listener(
        &self,
        table_index: SessionTableIndex,
        endpoint: &Self::Endpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle>;
}
