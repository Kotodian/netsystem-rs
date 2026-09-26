use std::net::IpAddr;
use std::sync::OnceLock;

use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_service::session::{
    ApplicationConfig, ApplicationError, SessionConfig, SessionError, SessionHandle, SessionLookup,
    SessionLookupResult, SessionMain, SessionState, application_main,
};
use hammer_service::transport::{Transport, TransportMain, TransportTxMode};

use crate::config::IpSessionTableConfig;
use crate::endpoint::{IpSessionEndpoint, IpTransportConnectionId};
use crate::lookup::{IpSessionFamily, IpSessionLookup};
use crate::namespace::namespaces;
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

    // VPP: vnet_listen, application.c:1277-1304; session_endpoint_in_ns,
    // application.c:1220-1275. Service stores only the raw namespace index.
    pub fn validate_application_namespace(
        &self,
        application: u32,
        namespace: u32,
    ) -> Result<(), SessionError> {
        let attached = application_main()
            .namespace(application)
            .ok_or(SessionError::NoApplication)?;
        if attached != namespace || namespaces().get(namespace).is_none() {
            return Err(SessionError::InvalidNamespace);
        }
        Ok(())
    }

    // VPP: vnet_application_attach, application.c:1112-1199. The IP binding
    // belongs to this plugin; ApplicationMain owns the generic app record.
    pub fn attach(&self, config: ApplicationConfig) -> Result<u32, SessionError> {
        if namespaces().get(config.namespace).is_none() {
            return Err(SessionError::InvalidNamespace);
        }
        application_main()
            .attach(config)
            .map_err(|source| match source {
                ApplicationError::AlreadyAttached => SessionError::ApplicationAttached,
                source => SessionError::ApplicationAttach { source },
            })
    }

    // VPP: vnet_application_detach, application.c:1196-1219.
    pub fn detach(&self, application: u32) -> Result<(), SessionError> {
        application_main()
            .detach(application)
            .map_err(|source| match source {
                ApplicationError::Missing { .. } => SessionError::NoApplication,
                source => SessionError::ApplicationDetach { source },
            })
    }

    #[inline(always)]
    pub fn lookup(&self, key: &IpTransportConnectionId) -> Option<SessionHandle> {
        self.lookup.lookup_session(key).map(SessionHandle::from)
    }

    // VPP: session_lookup_endpoint_listener, session_lookup.c:508-544;
    // app_listener_lookup, application.c:87-143.
    pub fn lookup_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle> {
        let local = endpoint.transport().local;
        let family = match local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let table = self.lookup.table_index(family, local.fib_index);
        self.lookup
            .lookup_listener(table, endpoint, use_wildcard)
            .map(SessionHandle::from)
    }

    // VPP: session_lookup_add_connection, session_lookup.c:258-294.
    pub fn add_connection(
        &self,
        connection: IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        self.lookup.add_connection(&connection, session.into());
        Ok(())
    }

    // VPP: session_lookup_del_connection, session_lookup.c:373-405.
    pub fn remove_connection(
        &self,
        connection: &IpTransportConnectionId,
        expected: SessionHandle,
    ) -> Result<(), SessionError> {
        match self.lookup.lookup_exact(connection) {
            SessionLookupResult::Session(current) => {
                assert_eq!(
                    SessionHandle::from(current),
                    expected,
                    "IP connection backlink must match its Session"
                );
            }
            _ => return Err(SessionError::NoSession),
        }
        assert!(
            self.lookup
                .remove_connection_if_current(connection, expected.into()),
            "validated IP connection remains installed until removal"
        );
        Ok(())
    }

    // VPP: session_lookup_add_session_endpoint, session_lookup.c:297-333;
    // app_worker_listen_sep, application_worker.c:230-322. Listener lookup
    // identity is owned by the IP plugin; the service only owns the Session
    // and ApplicationListener records.
    pub fn add_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        hammer_runtime::ensure_main_thread_with_barrier().map_err(|_| SessionError::Invalid)?;
        if application_main().listener_for_session(session) == Some(listener) {
            return Err(SessionError::AlreadyListening);
        }
        let local = endpoint.transport().local;
        let family = match local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let table = self.lookup.table_index(family, local.fib_index);
        if table == u32::MAX
            || !self
                .lookup
                .add_session_endpoint(table, endpoint, session.into())
        {
            return Err(SessionError::NoRoute);
        }
        Ok(())
    }

    // VPP: session_lookup_del_session_endpoint, session_lookup.c:335-371;
    // app_worker_stop_listen, application_worker.c:408-440.
    pub fn remove_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        hammer_runtime::ensure_main_thread_with_barrier().map_err(|_| SessionError::Invalid)?;
        if let Some(current) = application_main().listener_for_session(session) {
            if current != listener {
                return Err(SessionError::NoSession);
            }
        }
        let local = endpoint.transport().local;
        let family = match local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let table = self.lookup.table_index(family, local.fib_index);
        if table == u32::MAX || !self.lookup.remove_session_endpoint(table, endpoint) {
            return Err(SessionError::NoSession);
        }
        Ok(())
    }

    // VPP: vnet_listen, application.c:1277-1320; app_worker_listen_sep,
    // application_worker.c:230-322. The transport remains a concrete owner;
    // this method only composes it with the service Session and IP lookup.
    pub fn listen<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        endpoint: &IpSessionEndpoint,
    ) -> Result<SessionHandle, SessionError> {
        let namespace = application_main()
            .namespace(application)
            .ok_or(SessionError::NoApplication)?;
        self.validate_application_namespace(application, namespace)?;
        let family = match endpoint.transport().local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let namespace_fib = namespaces()
            .get(namespace)
            .expect("validated Application Namespace remains live")
            .binding()
            .fib_index(family);
        if namespace_fib != endpoint.transport().local.fib_index {
            return Err(SessionError::NoRoute);
        }

        if let Some(existing) = self.lookup_listener(endpoint, true) {
            let listener = application_main()
                .listener_for_session(existing)
                .ok_or(SessionError::NoSession)?;
            let owner = application_main()
                .listener(listener)
                .map_err(|source| SessionError::ApplicationAttach { source })?
                .application();
            return if owner == application {
                Ok(existing)
            } else {
                Err(SessionError::AlreadyListening)
            };
        }

        let listener = application_main()
            .allocate_listener(application, 0)
            .map_err(|source| SessionError::ApplicationAttach { source })?;
        let session =
            match self
                .session
                .allocate_listening_session(0, endpoint.transport_protocol(), 0)
            {
                Ok(session) => session,
                Err(primary) => {
                    if let Err(cleanup) = application_main().remove_listener(listener) {
                        tracing::error!(%cleanup, listener, "listener allocation rollback failed");
                    }
                    return Err(primary);
                }
            };
        let connection = match transport.start_listen(endpoint.transport(), session) {
            Ok(connection) => connection,
            Err(primary) => {
                if let Err(cleanup) = self.session.remove(session) {
                    tracing::error!(%cleanup, ?session, "listening Session rollback failed");
                }
                if let Err(cleanup) = application_main().remove_listener(listener) {
                    tracing::error!(%cleanup, listener, "listener allocation rollback failed");
                }
                return Err(primary);
            }
        };
        if let Err(primary) =
            self.session
                .attach_transport(session, endpoint.transport_protocol(), connection)
        {
            if let Err(cleanup) = transport.stop_listen(connection) {
                tracing::error!(%cleanup, connection, "transport listener rollback failed");
            }
            if let Err(cleanup) = self.session.remove(session) {
                tracing::error!(%cleanup, ?session, "listening Session rollback failed");
            }
            if let Err(cleanup) = application_main().remove_listener(listener) {
                tracing::error!(%cleanup, listener, "listener allocation rollback failed");
            }
            return Err(primary);
        }
        if let Err(primary) = self.add_listener(endpoint, listener, session) {
            if let Err(cleanup) = transport.stop_listen(connection) {
                tracing::error!(%cleanup, connection, "transport listener rollback failed");
            }
            if let Err(cleanup) = self.session.remove(session) {
                tracing::error!(%cleanup, ?session, "listening Session rollback failed");
            }
            if let Err(cleanup) = application_main().remove_listener(listener) {
                tracing::error!(%cleanup, listener, "listener allocation rollback failed");
            }
            return Err(primary);
        }
        if let Err(primary) =
            application_main().attach_listener_session(listener, Some(session), None)
        {
            if let Err(cleanup) = self.remove_listener(endpoint, listener, session) {
                tracing::error!(%cleanup, listener, "listener lookup rollback failed");
            }
            if let Err(cleanup) = transport.stop_listen(connection) {
                tracing::error!(%cleanup, connection, "transport listener rollback failed");
            }
            if let Err(cleanup) = self.session.remove(session) {
                tracing::error!(%cleanup, ?session, "listening Session rollback failed");
            }
            if let Err(cleanup) = application_main().remove_listener(listener) {
                tracing::error!(%cleanup, listener, "listener allocation rollback failed");
            }
            return Err(SessionError::ApplicationAttach { source: primary });
        }
        if let Err(primary) = application_main().attach_listener_worker(listener, 0) {
            if let Err(cleanup) = self.remove_listener(endpoint, listener, session) {
                tracing::error!(%cleanup, listener, "listener lookup rollback failed");
            }
            if let Err(cleanup) = transport.stop_listen(connection) {
                tracing::error!(%cleanup, connection, "transport listener rollback failed");
            }
            if let Err(cleanup) = self.session.remove(session) {
                tracing::error!(%cleanup, ?session, "listening Session rollback failed");
            }
            if let Err(cleanup) = application_main().remove_listener(listener) {
                tracing::error!(%cleanup, listener, "listener worker rollback failed");
            }
            return Err(SessionError::ApplicationAttach { source: primary });
        }
        Ok(session)
    }

    // VPP: vnet_disconnect_session, application.c:1518-1548. Transport owns
    // protocol close; Session retains the transport backlink until its normal
    // deleted notification arrives.
    pub fn disconnect<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        if application_main().application(application).is_none() {
            return Err(SessionError::NoApplication);
        }
        let entry = self
            .session
            .worker(session.worker_index)
            .and_then(|worker| worker.session(session.session_index))
            .ok_or(SessionError::NoSession)?;
        let connection = entry.connection_index();
        if connection == u32::MAX {
            return Err(SessionError::NoSession);
        }
        transport.close(connection, session.worker_index);
        Ok(())
    }

    #[deprecated(note = "ADR-0040: use add_connection with explicit IP identity")]
    pub fn publish(
        &self,
        key: IpTransportConnectionId,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        self.lookup.add_connection(&key, session.into());
        Ok(())
    }

    #[deprecated(note = "ADR-0040: use remove_connection with the expected Session")]
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

    pub fn notify_reset(
        &self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
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
