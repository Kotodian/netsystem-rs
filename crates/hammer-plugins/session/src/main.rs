use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;

use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_runtime::DataPlaneMain;
use hammer_service::session::app::{ApplicationError, ApplicationEventResult, ApplicationMain};
use hammer_service::session::{
    SessionEndpoint, SessionError, SessionHandle, SessionLookup, SessionMain, SessionQueueNext,
    SessionState, SessionTxDispatch, SessionWorker,
};
use hammer_service::transport::{Transport, TransportMain};

use crate::config::IpSessionConfig;
use crate::endpoint::{
    ENDPOINT_INVALID_INDEX, IpSessionEndpoint, IpSessionEndpointConfig, IpTransportConnectionId,
    IpTransportEndpoint, IpTransportEndpointConfig,
};
use crate::lookup::{IpSessionFamily, IpSessionLookup};
use crate::transport::{IpTransportConfig, IpTransportMain};

static IP_SESSION_MAIN: OnceLock<IpSessionMain> = OnceLock::new();

#[inline(always)]
fn session_type(protocol: u8, family: IpSessionFamily) -> u8 {
    let base = protocol
        .checked_mul(2)
        .expect("IP Session transport protocol fits the Session type field");
    base | u8::from(matches!(family, IpSessionFamily::Ip4))
}

pub struct IpSessionMain {
    session: &'static SessionMain,
    lookup: IpSessionLookup,
    transport: IpTransportMain,
}

impl IpSessionMain {
    pub fn init(
        table_config: IpSessionConfig,
        transport_config: IpTransportConfig,
    ) -> Result<(), SessionError> {
        let main = Self {
            session: SessionMain::global()?,
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
    pub fn session(&self) -> &SessionMain {
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

    /// VPP: `app_listener_lookup`, application.c:87-143;
    /// `session_lookup_endpoint_listener`, session_lookup.c.
    #[inline]
    pub fn lookup_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        use_wildcard: bool,
    ) -> Option<SessionHandle> {
        let family = match endpoint.transport().local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let table = self
            .lookup
            .table_index(family, endpoint.transport().local.fib_index);
        self.lookup
            .lookup_listener(table, endpoint, use_wildcard)
            .map(SessionHandle::from)
    }

    /// VPP: `app_worker_listen_sep`, application_worker.c:297-315. The
    /// listening Session must already be attached to its ApplicationListener.
    pub fn add_listener(
        &self,
        endpoint: &IpSessionEndpoint,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        let application_main = ApplicationMain::global()
            .expect("Application Main initializes before IP listener publication");
        if application_main.listener_for_session(self.session, session) != Some(listener) {
            return Err(SessionError::Owner);
        }
        let family = match endpoint.transport().local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let table = self
            .lookup
            .table_index(family, endpoint.transport().local.fib_index);
        if table == ENDPOINT_INVALID_INDEX {
            return Err(SessionError::NoRoute);
        }
        if self.lookup.lookup_listener(table, endpoint, true).is_some() {
            return Err(SessionError::AlreadyListening);
        }
        assert!(
            self.lookup
                .add_session_endpoint(table, endpoint, session.into()),
            "validated IP Session listener table remains allocated"
        );
        Ok(())
    }

    /// VPP: `vnet_listen`, application.c:1277-1321;
    /// `app_worker_start_listen`, application_worker.c:338-384;
    /// `session_listen`, session.c:1467-1491.
    pub fn listen<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        application: u32,
        worker_map: u32,
        request: &IpSessionEndpointConfig,
    ) -> Result<Option<(u32, SessionHandle)>, SessionError> {
        let Some(()) = self.validate_application_namespace(application, request.namespace)? else {
            return Ok(None);
        };
        let application_main = ApplicationMain::global()
            .expect("Application Main initializes before IP listener setup");
        // SAFETY: listen runs on Main Thread under WorkerBarrier. The
        // Application cannot be detached while this listener is installed.
        let app_worker = unsafe { application_main.application(application) }
            .and_then(|record| record.worker(worker_map))
            .ok_or(SessionError::InvalidApplicationWorker)?;
        let family = match request.endpoint.transport().local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let table = self
            .lookup
            .table_index(family, request.endpoint.transport().local.fib_index);
        if table == ENDPOINT_INVALID_INDEX {
            return Err(SessionError::NoRoute);
        }
        if self
            .lookup
            .lookup_listener(table, &request.endpoint, true)
            .is_some()
        {
            return Err(SessionError::AlreadyListening);
        }
        let opaque = request.opaque.unwrap_or_default();
        let listener = application_main
            .allocate_listener(application, worker_map, u64::from(opaque))
            .map_err(|source| match source {
                ApplicationError::WorkerMissing { .. } => SessionError::InvalidApplicationWorker,
                source => panic!("validated Application listener allocation failed: {source}"),
            })?;
        let session = match self.session.allocate_listening_session(
            session_type(request.endpoint.transport_protocol(), family),
            request.endpoint.transport_protocol(),
            app_worker,
            opaque,
        ) {
            Ok(session) => session,
            Err(source) => {
                application_main
                    .remove_listener(self.session, listener)
                    .expect("unattached listener has no resources to release");
                return Err(source);
            }
        };
        let connection = match transport.start_listen(request.endpoint.transport(), session) {
            Ok(connection) => connection,
            Err(source) => {
                self.session
                    .cleanup_listening_session(session)
                    .expect("unattached listening Session remains allocated");
                application_main
                    .remove_listener(self.session, listener)
                    .expect("unattached listener has no resources to release");
                return Err(source);
            }
        };
        self.session
            .attach_connection(session, connection)
            .expect("new listening Session retains its handle");
        application_main
            .attach_listener_session(self.session, listener, Some(session), None)
            .expect("new Application listener accepts its listening Session");
        if let Err(source) = self.add_listener(&request.endpoint, listener, session) {
            transport
                .stop_listen(connection)
                .expect("a newly started transport listener can stop");
            application_main
                .remove_listener(self.session, listener)
                .expect("unpublished listener remains allocated until cleanup");
            return Err(source);
        }
        if let Err(source) = application_main.attach_listener_worker(listener, worker_map) {
            self.unlisten(transport, &request.endpoint, listener, session)
                .expect("a newly published listener can stop");
            return Err(match source {
                ApplicationError::SegmentCreate { source } => {
                    SessionError::SegmentCreate { source }
                }
                ApplicationError::SegmentNoSpace => SessionError::SegmentNoSpace,
                ApplicationError::ListenerWorkerAttached { .. } => SessionError::AlreadyListening,
                source => panic!("validated Application listener worker setup failed: {source}"),
            });
        }
        Ok(Some((listener, session)))
    }

    /// VPP: `vnet_unlisten`, application.c:1466-1490;
    /// `session_stop_listen`, session.c:1497-1515. IP lookup belongs here,
    /// while the concrete transport and generic Session retain their owners.
    pub fn unlisten<T: Transport<IpTransportEndpointConfig>>(
        &self,
        transport: &T,
        endpoint: &IpSessionEndpoint,
        listener: u32,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        let application_main = ApplicationMain::global()
            .expect("Application Main remains initialized until listener removal");
        if application_main.listener_for_session(self.session, session) != Some(listener) {
            return Err(SessionError::Owner);
        }
        let connection = self
            .session
            .listening_connection_index(session)
            .ok_or(SessionError::NoSession)?;
        assert_ne!(
            connection,
            u32::MAX,
            "listening Session retains its transport"
        );
        match self.lookup_listener(endpoint, false) {
            Some(current) if current == session => {
                let family = match endpoint.transport().local.address {
                    IpAddr::V4(_) => IpSessionFamily::Ip4,
                    IpAddr::V6(_) => IpSessionFamily::Ip6,
                };
                let table = self
                    .lookup
                    .table_index(family, endpoint.transport().local.fib_index);
                assert!(
                    self.lookup.remove_session_endpoint(table, endpoint),
                    "validated IP listener lookup remains published until unlisten"
                );
            }
            Some(_) => return Err(SessionError::Owner),
            None => {}
        }
        transport.stop_listen(connection)?;
        application_main
            .remove_listener(self.session, listener)
            .expect("validated listener remains allocated through unlisten");
        Ok(())
    }

    /// VPP: `vnet_listen`, application.c:1277-1302; namespace lookup and
    /// `session_endpoint_in_ns`, application.c:1232-1269. A missing
    /// Application remains an ordinary lookup absence for Rust callers.
    pub fn validate_application_namespace(
        &self,
        application: u32,
        namespace: u32,
    ) -> Result<Option<()>, SessionError> {
        let application_main = ApplicationMain::global()
            .expect("Application Main initializes before IP namespace validation");
        let Some(attached_namespace) = application_main.namespace(application) else {
            return Ok(None);
        };
        if attached_namespace != namespace
            || crate::namespace::namespaces().get(namespace).is_none()
        {
            return Err(SessionError::InvalidNamespace);
        }
        Ok(Some(()))
    }

    /// # Safety
    /// The runtime must exclusively own the selected Session worker pool.
    pub unsafe fn allocate_for_connection(
        &self,
        runtime: &mut DataPlaneMain,
        endpoint: &IpSessionEndpoint,
        connection_index: u32,
    ) -> Result<SessionHandle, SessionError> {
        // SAFETY: the TCP node calls this for the exclusively executing
        // Data Worker; no other runtime may borrow this worker's Session pool.
        let worker = unsafe { self.session.worker_mut(runtime)? };
        let family = match endpoint.transport().local.address {
            IpAddr::V4(_) => IpSessionFamily::Ip4,
            IpAddr::V6(_) => IpSessionFamily::Ip6,
        };
        let session = worker.allocate(
            SessionState::Connecting,
            session_type(endpoint.transport_protocol(), family),
            endpoint.transport_protocol(),
            0,
        );
        worker.attach_transport(session, connection_index)?;
        Ok(session)
    }

    /// VPP: `session_stream_accept`, session.c:1213-1244. Session owns the
    /// accepted Session/FIFO pair; the caller retains its transport child.
    pub unsafe fn accept(
        &self,
        runtime: &mut DataPlaneMain,
        listener: SessionHandle,
        connection_index: u32,
        protocol: u8,
        family: IpSessionFamily,
    ) -> Result<Option<SessionHandle>, SessionError> {
        let application = ApplicationMain::global()
            .expect("Application Main initializes before TCP accepts Sessions");
        let app_listener = application
            .listener_for_session(self.session, listener)
            .ok_or(SessionError::NotListening)?;
        // SAFETY: the caller executes on the Session's owning Data Worker.
        let worker = unsafe { self.session.worker_mut(runtime)? };
        let session = worker.allocate_accepted(
            listener,
            connection_index,
            session_type(protocol, family),
            protocol,
            0,
        );
        match application.init_accepted(runtime, worker, app_listener, session) {
            Ok(ApplicationEventResult::Queued) => {
                worker.store_state(session, SessionState::Accepting)?;
                Ok(Some(session))
            }
            Ok(
                ApplicationEventResult::Deferred
                | ApplicationEventResult::QueueFull
                | ApplicationEventResult::LockUnavailable,
            ) => {
                worker
                    .cleanup(session)
                    .expect("unpublished accepted Session remains allocated");
                Ok(None)
            }
            Err(source) => {
                worker
                    .cleanup(session)
                    .expect("failed accepted Session remains allocated");
                Err(match source {
                    ApplicationError::SegmentCreate { source } => {
                        SessionError::SegmentCreate { source }
                    }
                    ApplicationError::SegmentNoSpace => SessionError::SegmentNoSpace,
                    ApplicationError::NoAcceptingWorker
                    | ApplicationError::InvalidApplicationWorker { .. }
                    | ApplicationError::WorkerMissing { .. } => {
                        SessionError::InvalidApplicationWorker
                    }
                    ApplicationError::NoListener { .. } => SessionError::NotListening,
                    ApplicationError::MessageAllocation { source } => {
                        SessionError::MessageQueueAllocation { source }
                    }
                    source => {
                        panic!("validated accepted Session rejected by Application: {source}")
                    }
                })
            }
        }
    }

    /// Builds a listening request using the Application Namespace's IP binding.
    /// VPP: application.c:1232-1269, `session_endpoint_update_for_app`.
    pub fn listen_endpoint_config(
        &self,
        address: SocketAddr,
        namespace: u32,
        transport_protocol: u8,
    ) -> Result<IpSessionEndpointConfig, SessionError> {
        let family = if address.is_ipv4() {
            IpSessionFamily::Ip4
        } else {
            IpSessionFamily::Ip6
        };
        let binding = crate::namespace::namespaces()
            .get(namespace)
            .ok_or(SessionError::InvalidNamespace)?;
        let binding = binding.binding();
        let fib_index = binding.fib_index(family);
        if fib_index == ENDPOINT_INVALID_INDEX {
            return Err(SessionError::NoRoute);
        }
        let local = IpTransportEndpoint {
            address: address.ip(),
            port: address.port(),
            sw_if_index: binding.sw_if_index(),
            fib_index,
        };
        let peer = IpTransportEndpoint {
            address: match family {
                IpSessionFamily::Ip4 => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpSessionFamily::Ip6 => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            },
            port: 0,
            sw_if_index: ENDPOINT_INVALID_INDEX,
            fib_index,
        };
        let mut config = IpSessionEndpointConfig::new(SessionEndpoint::new(
            IpTransportEndpointConfig {
                local,
                peer,
                next_node_index: 0,
                next_node_opaque: 0,
                mss: 0,
                dscp: 0,
                transport_flags: 0,
            },
            transport_protocol,
        ));
        config.namespace = namespace;
        Ok(config)
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

    pub fn register_transport(
        &self,
        protocol: u8,
        family: IpSessionFamily,
        output_next: SessionQueueNext,
        tx: SessionTxDispatch,
    ) {
        self.session
            .register_transport(session_type(protocol, family), output_next, tx);
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub unsafe fn rx_fifo<'worker>(
        &'worker self,
        runtime: &'worker mut DataPlaneMain,
        session: SessionHandle,
    ) -> Result<&'worker SvmFifo, SessionError> {
        (unsafe { self.session.worker_mut(runtime)? })
            .session(session.session_index)
            .filter(|entry| entry.handle() == session)
            .ok_or(SessionError::NoSession)?
            .rx_fifo()
            .ok_or(SessionError::SegmentNoSpace)
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub unsafe fn tx_fifo<'worker>(
        &'worker self,
        runtime: &'worker mut DataPlaneMain,
        session: SessionHandle,
    ) -> Result<&'worker SvmFifo, SessionError> {
        (unsafe { self.session.worker_mut(runtime)? })
            .session(session.session_index)
            .filter(|entry| entry.handle() == session)
            .ok_or(SessionError::NoSession)?
            .tx_fifo()
            .ok_or(SessionError::SegmentNoSpace)
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub unsafe fn notify_closing(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        (unsafe { self.session.worker_mut(runtime)? }).transport_closing(
            runtime,
            session,
            connection_index,
        )
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub unsafe fn notify_closed(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        (unsafe { self.session.worker_mut(runtime)? }).transport_closed(
            runtime,
            session,
            connection_index,
        )
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub unsafe fn notify_reset(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        (unsafe { self.session.worker_mut(runtime)? }).transport_reset(
            runtime,
            session,
            connection_index,
        )
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub fn notify_deleted(
        &self,
        runtime: &DataPlaneMain,
        sessions: &mut SessionWorker,
        session: SessionHandle,
        connection_index: u32,
        connection: &IpTransportConnectionId,
    ) -> Result<bool, SessionError> {
        let Some(entry) = sessions.session_from_handle(session) else {
            return Err(SessionError::NoSession);
        };
        if entry.handle() != session || entry.connection_index() != connection_index {
            return Err(SessionError::NoSession);
        }
        // VPP session.c:1064-1121 removes the Session lookup before posting
        // application transport cleanup; a reused tuple must not erase a new
        // Session's handle.
        self.lookup
            .remove_connection_if_current(connection, session.into());
        sessions.transport_delete_request(runtime, session, connection_index)
    }
}
