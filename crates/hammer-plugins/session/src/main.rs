use std::sync::OnceLock;

use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_service::session::{
    SESSION_E_NONE, SESSION_E_NOSESSION, SESSION_E_SEG_NO_SPACE, SESSION_E_UNKNOWN, SessionConfig,
    SessionHandle, SessionLookup, SessionMain, SessionState,
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
    ) -> Result<Self, i32> {
        Ok(Self {
            session: SessionMain::init(session_config, worker_mq_segment)?,
            lookup: IpSessionLookup::new(table_config),
            transport: IpTransportMain::init(transport_config)?,
        })
    }

    pub(crate) fn publish_global(main: Self) {
        assert!(
            IP_SESSION_MAIN.set(main).is_ok(),
            "IP Session Main initialization callback executes once"
        );
    }

    pub fn global() -> Result<&'static Self, i32> {
        IP_SESSION_MAIN.get().ok_or(SESSION_E_UNKNOWN)
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

    pub fn publish(&self, key: IpTransportConnectionId, session: SessionHandle) -> i32 {
        self.lookup.add_connection(&key, session.into());
        SESSION_E_NONE
    }

    pub fn remove(&self, key: &IpTransportConnectionId) -> i32 {
        if self.lookup.remove_connection(key) {
            SESSION_E_NONE
        } else {
            SESSION_E_NOSESSION
        }
    }

    pub fn allocate_for_connection(
        &mut self,
        worker_index: u32,
        endpoint: &IpSessionEndpoint,
        connection_index: u32,
    ) -> Result<SessionHandle, i32> {
        let session = self.session.allocate(
            worker_index,
            SessionState::Connecting,
            endpoint.transport_protocol(),
        )?;
        let retval =
            self.session
                .attach_transport(session, endpoint.transport_protocol(), connection_index);
        if retval != SESSION_E_NONE {
            return Err(retval);
        }
        Ok(session)
    }

    pub fn prepare_endpoint(&self, endpoint: IpSessionEndpoint) -> Result<IpSessionEndpoint, i32> {
        self.transport.allocate_local(endpoint)
    }

    pub fn release_endpoint(&self, endpoint: &IpSessionEndpoint) -> i32 {
        self.transport.release(endpoint)
    }

    pub fn register_transport_type(
        &self,
        protocol: u8,
        tx_mode: TransportTxMode,
        output_next: u32,
    ) -> Result<u8, i32> {
        self.session
            .register_transport_type(protocol, tx_mode, output_next)
    }

    pub fn rx_fifo(&self, session: SessionHandle) -> Result<&SvmFifo, i32> {
        self.session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
            .ok_or(SESSION_E_NOSESSION)?
            .rx_fifo()
            .ok_or(SESSION_E_SEG_NO_SPACE)
    }

    pub fn tx_fifo(&self, session: SessionHandle) -> Result<&SvmFifo, i32> {
        self.session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
            .ok_or(SESSION_E_NOSESSION)?
            .tx_fifo()
            .ok_or(SESSION_E_SEG_NO_SPACE)
    }

    pub fn notify_closed(&mut self, session: SessionHandle, connection_index: u32) -> i32 {
        let Some(entry) = self
            .session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
        else {
            return SESSION_E_NOSESSION;
        };
        if entry.connection_index() != connection_index {
            return SESSION_E_NOSESSION;
        }
        entry.store_state(SessionState::TransportClosed);
        SESSION_E_NONE
    }

    pub fn notify_reset(&mut self, session: SessionHandle, connection_index: u32) -> i32 {
        self.notify_closed(session, connection_index)
    }

    pub fn notify_deleted(&mut self, session: SessionHandle, connection_index: u32) -> i32 {
        let Some(entry) = self
            .session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
        else {
            return SESSION_E_NOSESSION;
        };
        if entry.connection_index() != connection_index {
            return SESSION_E_NOSESSION;
        }
        self.session.detach_transport(session)
    }
}
