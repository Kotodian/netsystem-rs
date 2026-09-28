use std::cell::UnsafeCell;
use std::sync::OnceLock;

use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_plugin_session::{AppSessionTransport, IpSessionMain};
use hammer_plugin_tcp::TCP_MAIN;
use hammer_runtime::{DataPlaneMain, DataWorkerId, RuntimeError, RuntimeResult};
use hammer_service::session::app::{
    ApplicationConfig, ApplicationError, ApplicationFlags, ApplicationMain, SessionCleanup,
};
use hammer_service::session::{
    AppSession, SegmentManagerProperties, Session, SessionError, SessionEventEnqueue,
    SessionEventType, SessionHandle, SessionMain, SessionState,
};
use thiserror::Error;

use crate::config::{Iperf3Config, Iperf3ConfigError};
use crate::protocol::{
    ControlAction, ControlParameters, ControlParser, ControlState, Iperf3ProtocolError,
};

const CONTROL_CONTEXT: u64 = 0;
const DATA_CONTEXT: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ListenerRole {
    Control = 0,
    Data = 1,
}

impl From<ListenerRole> for u64 {
    #[inline(always)]
    fn from(role: ListenerRole) -> Self {
        match role {
            ListenerRole::Control => CONTROL_CONTEXT,
            ListenerRole::Data => DATA_CONTEXT,
        }
    }
}

impl From<ListenerRole> for u32 {
    #[inline(always)]
    fn from(role: ListenerRole) -> Self {
        role as Self
    }
}

impl TryFrom<u64> for ListenerRole {
    type Error = Iperf3Error;

    #[inline(always)]
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            CONTROL_CONTEXT => Ok(Self::Control),
            DATA_CONTEXT => Ok(Self::Data),
            context => Err(Iperf3Error::ListenerContext { context }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    Accepted,
    Running,
    Closing,
}

#[repr(C)]
pub struct Iperf3Session {
    cacheline0: CacheLineAlignMark,
    app: AppSession<AppSessionTransport>,
    role: ListenerRole,
    phase: SessionPhase,
    parser: ControlParser,
    parameters: Option<ControlParameters>,
    received: u64,
    sent: u64,
}

impl Iperf3Session {
    fn new(app: AppSession<AppSessionTransport>, role: ListenerRole) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            app,
            role,
            phase: SessionPhase::Accepted,
            parser: ControlParser::new(),
            parameters: None,
            received: 0,
            sent: 0,
        }
    }

    /// VPP: vperf_protos.c:142-155, vp_proto_server_stream_rx_no_echo.
    #[inline]
    fn server_stream_rx_no_echo(&mut self, session: &mut Session) {
        let rx = session
            .rx_fifo()
            .expect("accepted iperf3 Session owns an RX FIFO");
        let available = rx.max_dequeue();
        if available == 0 {
            return;
        }
        let consumed = rx.drop_dequeue(available);
        self.received = self.received.saturating_add(consumed as u64);
        if rx.needs_deq_notification(consumed) {
            match hammer_service::session::program_transport_io_event(
                session.handle(),
                SessionEventType::Rx,
            )
            .expect("accepted iperf3 Session retains its worker event queue")
            {
                SessionEventEnqueue::Enqueued
                | SessionEventEnqueue::Busy
                | SessionEventEnqueue::Full => {}
            }
        }
    }
}

#[repr(C)]
struct Iperf3Worker {
    cacheline0: CacheLineAlignMark,
    sessions: Pool<Iperf3Session>,
    cacheline1: CacheLineAlignMark,
}

impl Iperf3Worker {
    fn new() -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            sessions: Pool::new(),
            cacheline1: CacheLineAlignMark,
        }
    }
}

#[repr(C)]
pub struct Iperf3Main {
    cacheline0: CacheLineAlignMark,
    config: Iperf3Config,
    application: u32,
    control_application_listener: u32,
    data_application_listener: u32,
    control_listener: SessionHandle,
    data_listener: SessionHandle,
    transport_protocol: u8,
    cacheline1: CacheLineAlignMark,
    workers: Vec<UnsafeCell<Iperf3Worker>>,
}

// SAFETY: each worker slot is permanently owned by one Data Worker. Main
// Thread fields are published before the OnceLock becomes visible.
unsafe impl Send for Iperf3Main {}
unsafe impl Sync for Iperf3Main {}

static IPERF3_MAIN: OnceLock<Iperf3Main> = OnceLock::new();
static IPERF3_CONFIG: OnceLock<Iperf3Config> = OnceLock::new();

impl Iperf3Main {
    fn worker(&self, worker: DataWorkerId) -> Result<&mut Iperf3Worker, Iperf3Error> {
        let slot = self
            .workers
            .get(worker.slot())
            .ok_or(Iperf3Error::WorkerMissing {
                worker: worker.slot(),
            })?;
        // SAFETY: the runtime invokes callbacks for a worker only on its
        // assigned thread, so no two callbacks mutate one worker slot.
        Ok(unsafe { &mut *slot.get() })
    }

    fn start(config: Iperf3Config) -> Result<Self, Iperf3Error> {
        let app_main =
            ApplicationMain::global().expect("Application Main initializes before builtin iperf3");
        let ip_session =
            IpSessionMain::global().map_err(|source| Iperf3Error::ControlListen { source })?;
        let tcp = TCP_MAIN.get().ok_or(Iperf3Error::TransportProtocol {
            source: RuntimeError::PluginStateNotInitialized { plugin: "tcp" },
        })?;
        let protocol = tcp.protocol();
        let mut control_request = ip_session
            .listen_endpoint_config(config.control_endpoint, config.namespace, protocol)
            .map_err(|source| Iperf3Error::ControlListen { source })?;
        let mut data_request = ip_session
            .listen_endpoint_config(config.data_endpoint, config.namespace, protocol)
            .map_err(|source| Iperf3Error::DataListen { source })?;
        // VPP vp_server_create creates worker slots before attach; its test
        // session pool is reserved only after test parameters are known.
        let worker_count = hammer_runtime::config::worker::worker_count();
        let workers = (0..worker_count)
            .map(|_| UnsafeCell::new(Iperf3Worker::new()))
            .collect();
        let application = app_main
            .attach(ApplicationConfig {
                namespace: config.namespace,
                flags: ApplicationFlags::BUILTIN,
                name: "iperf3".to_owned(),
                segment: SegmentManagerProperties::default(),
                add_segment: |_, _| Ok(()),
                del_segment: |_, _| Ok(()),
                accepted: on_accept,
                connected: |_, _, _, _| Err(ApplicationError::CallbackRejected),
                disconnected: on_disconnect,
                reset: on_reset,
                transport_closed: Some(on_transport_closed),
                cleanup: Some(on_cleanup),
                half_open_cleanup: None,
                migrated: None,
                listened: None,
                unlistened: None,
                builtin_rx: Some(on_rx),
                builtin_tx: None,
            })
            .map_err(|source| Iperf3Error::ApplicationAttach { source })?;
        control_request.opaque = Some(u32::from(ListenerRole::Control));
        data_request.opaque = Some(u32::from(ListenerRole::Data));
        let (control_application_listener, control_listener) = match ip_session.listen(
            tcp,
            application,
            0,
            &control_request,
        ) {
            Ok(Some(listener)) => listener,
            Ok(None) => panic!("newly attached iperf3 Application remains allocated"),
            Err(source) => {
                app_main.detach(application).unwrap_or_else(|cleanup_error| {
                        panic!("iperf3 control listen failed: {source}; Application detach failed: {cleanup_error}")
                    });
                return Err(Iperf3Error::ControlListen { source });
            }
        };
        let (data_application_listener, data_listener) = match ip_session.listen(
            tcp,
            application,
            0,
            &data_request,
        ) {
            Ok(Some(listener)) => listener,
            Ok(None) => panic!("iperf3 Application remains attached during data listen"),
            Err(source) => {
                let unlisten_error = ip_session
                    .unlisten(
                        tcp,
                        &control_request.endpoint,
                        control_application_listener,
                        control_listener,
                    )
                    .err();
                let detach_error = app_main.detach(application).err();
                assert!(
                    unlisten_error.is_none() && detach_error.is_none(),
                    "iperf3 data listen failed: {source}; control unlisten: {unlisten_error:?}; Application detach: {detach_error:?}"
                );
                return Err(Iperf3Error::DataListen { source });
            }
        };
        Ok(Self {
            cacheline0: CacheLineAlignMark,
            config,
            application,
            control_application_listener,
            data_application_listener,
            control_listener,
            data_listener,
            transport_protocol: protocol,
            cacheline1: CacheLineAlignMark,
            workers,
        })
    }
}

#[hammer_component_macros::runtime_error(subsystem = "iperf3")]
#[derive(Debug, Error)]
pub enum Iperf3Error {
    #[error("iperf3 configuration is invalid: {source}")]
    Configuration {
        #[source]
        source: Iperf3ConfigError,
    },
    #[error("iperf3 Application attach failed")]
    ApplicationAttach {
        #[source]
        source: ApplicationError,
    },
    #[error("iperf3 transport protocol is unavailable")]
    TransportProtocol {
        #[source]
        source: RuntimeError,
    },
    #[error("iperf3 control listener failed")]
    ControlListen {
        #[source]
        source: SessionError,
    },
    #[error("iperf3 data listener failed")]
    DataListen {
        #[source]
        source: SessionError,
    },
    #[error("iperf3 worker {worker} is not present")]
    WorkerMissing { worker: usize },
    #[error("iperf3 listener context {context} is invalid")]
    ListenerContext { context: u64 },
    #[error("iperf3 Session {session_id} is not present")]
    SessionMissing { session_id: u32 },
    #[error("iperf3 control protocol failed")]
    Protocol {
        #[source]
        source: Iperf3ProtocolError,
    },
}

fn on_accept(session: &mut Session) -> Result<(), ApplicationError> {
    let main = IPERF3_MAIN
        .get()
        .expect("iperf3 Main publishes before Session callbacks execute");
    let handle = session.handle();
    let listener = session.listener_handle();
    let role = if listener == main.control_listener {
        ListenerRole::Control
    } else if listener == main.data_listener {
        ListenerRole::Data
    } else {
        return Err(ApplicationError::CallbackRejected);
    };
    let slot = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker");
    let app = AppSession::new(session);
    let index = slot.sessions.insert(Iperf3Session::new(app, role));
    *session.opaque_mut() = index;
    session.store_state(SessionState::Ready);
    Ok(())
}

fn on_rx(app_session: &mut Session) {
    let main = IPERF3_MAIN
        .get()
        .expect("iperf3 Main publishes before Session callbacks execute");
    let handle = app_session.handle();
    let index = *app_session.opaque();
    let slot = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker");
    let session = slot
        .sessions
        .get_mut(index)
        .expect("iperf3 Session record remains live until Session cleanup");
    match session.role {
        ListenerRole::Control => {
            let rx = app_session
                .rx_fifo()
                .expect("accepted iperf3 Session owns an RX FIFO");
            let tx = app_session
                .tx_fifo()
                .expect("accepted iperf3 Session owns a TX FIFO");
            rx.unset_event();
            loop {
                let inspected = match ControlParser::inspect(session.parser.phase(), rx) {
                    Ok(inspected) => inspected,
                    Err(_) => {
                        session.phase = SessionPhase::Closing;
                        app_session.store_state(SessionState::AppClosed);
                        return;
                    }
                };
                let Some((action, inspected_len)) = inspected else {
                    break;
                };

                let reply = match action {
                    ControlAction::Parameters(_) => {
                        Some(ControlParser::state_bytes(ControlState::CreateStreams))
                    }
                    ControlAction::SendState(state) => Some(ControlParser::state_bytes(state)),
                    ControlAction::Close => None,
                };
                if reply
                    .as_ref()
                    .is_some_and(|bytes| tx.max_enqueue() < bytes.len())
                {
                    // VPP: vperf_protos.c:80-99. The current session-input
                    // dispatch retains this AppWorker's pending bit until the
                    // callback returns and observes the appended RX event.
                    if rx.set_event() {
                        hammer_service::session::enqueue_notify(app_session)
                            .expect("accepted iperf3 Session retains its AppWorker");
                    }
                    return;
                }

                // VPP: app_recv_stream_raw, application_interface.h:795-810.
                let consumed = rx.drop_dequeue(inspected_len);
                assert_eq!(
                    consumed, inspected_len,
                    "inspected command is still in the RX FIFO"
                );
                session.received = session.received.saturating_add(consumed as u64);
                if rx.needs_deq_notification(consumed) {
                    match hammer_service::session::program_transport_io_event(
                        handle,
                        SessionEventType::Rx,
                    )
                    .expect("accepted iperf3 Session retains its worker event queue")
                    {
                        SessionEventEnqueue::Enqueued
                        | SessionEventEnqueue::Busy
                        | SessionEventEnqueue::Full => {}
                    }
                }
                session.parser.commit(&action);

                match action {
                    ControlAction::Parameters(parameters) => {
                        session.parameters = Some(parameters);
                    }
                    ControlAction::SendState(state) => {
                        session.phase = if state == ControlState::TestStart {
                            SessionPhase::Running
                        } else {
                            SessionPhase::Accepted
                        };
                    }
                    ControlAction::Close => {
                        session.phase = SessionPhase::Closing;
                        app_session.store_state(SessionState::AppClosed);
                        return;
                    }
                }

                if let Some(bytes) = reply {
                    let sent = session
                        .app
                        .send_stream(&bytes, false)
                        .expect("blocking iperf3 stream send publishes its TX event");
                    assert_eq!(sent, bytes.len(), "reserved control response fits TX FIFO");
                    session.sent = session.sent.saturating_add(sent as u64);
                }
            }
        }
        ListenerRole::Data => {
            session.server_stream_rx_no_echo(app_session);
        }
    }
}

fn on_disconnect(app_session: &mut Session) {
    let main = IPERF3_MAIN
        .get()
        .expect("iperf3 Main publishes before Session callbacks execute");
    let handle = app_session.handle();
    let index = *app_session.opaque();
    let session = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker")
        .sessions
        .get_mut(index)
        .expect("iperf3 Session record remains live until Session cleanup");
    session.phase = SessionPhase::Closing;
    app_session.store_state(SessionState::AppClosed);
}

fn on_reset(session: &mut Session) {
    on_disconnect(session);
}

fn on_transport_closed(app_session: &mut Session) {
    let main = IPERF3_MAIN
        .get()
        .expect("iperf3 Main publishes before Session callbacks execute");
    let handle = app_session.handle();
    let index = *app_session.opaque();
    let session = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker")
        .sessions
        .get_mut(index)
        .expect("iperf3 Session record remains live until Session cleanup");
    session.phase = SessionPhase::Closing;
}

fn on_cleanup(app_session: &mut Session, cleanup: SessionCleanup) {
    if cleanup != SessionCleanup::Session {
        return;
    }
    let main = IPERF3_MAIN
        .get()
        .expect("iperf3 Main publishes before Session callbacks execute");
    let handle = app_session.handle();
    let index = *app_session.opaque();
    main.worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker")
        .sessions
        .remove(index)
        .expect("iperf3 Session record remains live until Session cleanup");
}

#[hammer_component_macros::config_function(
    name = "iperf3_config",
    section = "plugin.iperf3",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_iperf3(config: Iperf3Config) -> RuntimeResult<()> {
    config
        .validate()
        .map_err(|source| Iperf3Error::Configuration { source })?;
    assert!(
        IPERF3_CONFIG.set(config).is_ok(),
        "iperf3 configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "iperf3_init",
    runs_after = ["tcp_init", "session_lookup_init"]
)]
fn init_iperf3(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let config = IPERF3_CONFIG.get().cloned().unwrap_or_default();
    if !config.enable {
        return Ok(());
    }
    let session_main = SessionMain::global()?;
    session_main.enable()?;
    let main = Iperf3Main::start(config)?;
    assert!(
        IPERF3_MAIN.set(main).is_ok(),
        "iperf3 Main initialization callback executes once"
    );
    Ok(())
}

pub fn main() -> &'static Iperf3Main {
    IPERF3_MAIN
        .get()
        .expect("iperf3 Main is initialized before use")
}
