use std::sync::OnceLock;

use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_service::session::{
    SessionConfig, SessionError, SessionHandle, SessionLookup, SessionMain, SessionState,
};
use hammer_service::transport::{TransportMain, TransportTxMode};

use crate::config::IpSessionTableConfig;
use crate::endpoint::{IpSessionEndpoint, IpTransportConnectionId};
use crate::lookup::IpSessionLookup;
use crate::transport::{IpTransportConfig, IpTransportMain};

static IP_SESSION_MAIN: OnceLock<IpSessionMain> = OnceLock::new();

pub struct IpSessionMain {
    session: SessionMain<u32>,
    lookup: IpSessionLookup,
    transport: IpTransportMain,
}

impl IpSessionMain {
    pub fn init(
        session_config: SessionConfig,
        table_config: IpSessionTableConfig,
        transport_config: IpTransportConfig,
        worker_mq_segment: SvmFifoSegment,
    ) -> Result<(), SessionError> {
        let main = Self {
            session: SessionMain::init(session_config, worker_mq_segment)?,
            lookup: IpSessionLookup::new(table_config),
            transport: IpTransportMain::init(transport_config)?,
        };
        assert!(
            IP_SESSION_MAIN.set(main).is_ok(),
            "IP Session Main initialization callback executes once"
        );
        Ok(())
    }

    pub fn global() -> Result<&'static Self, SessionError> {
        IP_SESSION_MAIN.get().ok_or(SessionError::Unknown)
    }

    #[inline(always)]
    pub fn session(&self) -> &SessionMain<u32> {
        &self.session
    }

    #[inline(always)]
    pub fn lookup_main(&self) -> &IpSessionLookup {
        &self.lookup
    }

    #[inline(always)]
    pub fn transport(&self) -> &IpTransportMain {
        &self.transport
    }

    #[inline(always)]
    pub fn lookup(&self, key: &IpTransportConnectionId) -> Option<SessionHandle> {
        self.lookup.lookup_session(key).map(SessionHandle::from)
    }

    pub fn publish(
        &self,
        key: IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        self.lookup.add_connection(&key, session.into());
        Ok(())
    }

    pub fn remove(&self, key: &IpTransportConnectionId) -> Result<(), SessionError> {
        if self.lookup.remove_connection(key) {
            Ok(())
        } else {
            Err(SessionError::NoSession)
        }
    }

    pub fn allocate_for_connection(
        &mut self,
        worker_index: u32,
        endpoint: &IpSessionEndpoint,
        connection_index: u32,
    ) -> Result<SessionHandle, SessionError> {
        let session = self.session.allocate(
            worker_index,
            SessionState::Connecting,
            endpoint.transport_protocol(),
        )?;
        self.session
            .attach_transport(session, endpoint.transport_protocol(), connection_index)?;
        Ok(session)
    }

    pub fn prepare_endpoint(
        &self,
        endpoint: IpSessionEndpoint,
    ) -> Result<IpSessionEndpoint, SessionError> {
        self.transport.allocate_local(endpoint)
    }

    pub fn release_endpoint(&self, endpoint: &IpSessionEndpoint) -> Result<(), SessionError> {
        self.transport.release(endpoint)
    }

    pub fn register_transport_type(
        &self,
        protocol: u8,
        tx_mode: TransportTxMode,
        output_next: u32,
    ) -> Result<u8, SessionError> {
        self.session
            .register_transport_type(protocol, tx_mode, output_next)
    }

    pub fn rx_fifo(&self, session: SessionHandle) -> Result<&SvmFifo, SessionError> {
        self.session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
            .ok_or(SessionError::NoSession)?
            .rx_fifo()
            .ok_or(SessionError::SegmentNoSpace)
    }

    pub fn tx_fifo(&self, session: SessionHandle) -> Result<&SvmFifo, SessionError> {
        self.session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
            .ok_or(SessionError::NoSession)?
            .tx_fifo()
            .ok_or(SessionError::SegmentNoSpace)
    }

    pub fn notify_closed(
        &self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let Some(entry) = self
            .session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
        else {
            return Err(SessionError::NoSession);
        };
        if entry.connection_index() != connection_index {
            return Err(SessionError::NoSession);
        }
        entry.store_state(SessionState::TransportClosed);
        Ok(())
    }

    pub fn notify_reset(&self, session: SessionHandle, connection_index: u32) -> Result<(), SessionError> {
        self.notify_closed(session, connection_index)
    }

    pub fn notify_deleted(
        &mut self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let Some(entry) = self
            .session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
        else {
            return Err(SessionError::NoSession);
        };
        if entry.connection_index() != connection_index {
            return Err(SessionError::NoSession);
        }
        self.session.detach_transport(session)
    }
}
