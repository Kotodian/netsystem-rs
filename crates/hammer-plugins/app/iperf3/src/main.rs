use std::cell::UnsafeCell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_infra::sync::SpinLock;
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
    COOKIE_SIZE, ControlAction, ControlParameters, ControlParser, ControlState,
    Iperf3ProtocolError, MAX_STREAMS,
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
    role: Option<ListenerRole>,
    phase: SessionPhase,
    parser: ControlParser,
    parameters: Option<ControlParameters>,
    stream_index: Option<u32>,
    received: u64,
    sent: u64,
}

impl Iperf3Session {
    fn new(app: AppSession<AppSessionTransport>, role: Option<ListenerRole>) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            app,
            role,
            phase: SessionPhase::Accepted,
            parser: ControlParser::new(),
            parameters: None,
            stream_index: None,
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
    control_session: Option<u32>,
    cacheline1: CacheLineAlignMark,
}

impl Iperf3Worker {
    fn new() -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            sessions: Pool::new(),
            control_session: None,
            cacheline1: CacheLineAlignMark,
        }
    }
}

// VPP: vperf_server.c:139-149,169-180,235-255. Control facts are process
// scoped; Session records and RX work remain owned by their worker.
struct ControlTest {
    handle: SessionHandle,
    cookie: Option<[u8; COOKIE_SIZE]>,
    expected_streams: u32,
    accepted_streams: u32,
    data_sessions: u32,
    start_time: Option<Instant>,
    closed: bool,
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
    control: SpinLock<Option<ControlTest>>,
    // Each column has one Data Worker writer; control takes Acquire snapshots.
    stream_bytes: Vec<Vec<AtomicU64>>,
    workers: Vec<UnsafeCell<Iperf3Worker>>,
}

// SAFETY: each worker slot is permanently owned by one Data Worker. Main
// Thread fields are published before the OnceLock becomes visible.
unsafe impl Send for Iperf3Main {}
unsafe impl Sync for Iperf3Main {}

static IPERF3_MAIN: OnceLock<Iperf3Main> = OnceLock::new();
static IPERF3_CONFIG: OnceLock<Iperf3Config> = OnceLock::new();

impl Iperf3Main {
    #[expect(
        clippy::mut_from_ref,
        reason = "each Data Worker exclusively owns its slot"
    )]
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
        let stream_bytes = (0..worker_count)
            .map(|_| (0..MAX_STREAMS).map(|_| AtomicU64::new(0)).collect())
            .collect();
        let application = app_main
            .attach(ApplicationConfig {
                namespace: config.namespace,
                flags: ApplicationFlags::BUILTIN,
                name: "iperf3".to_owned(),
                // VPP vperf_server.c:436-443,569-573: the builtin server
                // provisions 4 MiB FIFOs in a 512 MiB private segment.
                segment: SegmentManagerProperties {
                    rx_fifo_size: 4 << 20,
                    tx_fifo_size: 4 << 20,
                    segment_size: 512 << 20,
                    add_segment_size: 512 << 20,
                    preallocated_fifos: 1,
                    ..SegmentManagerProperties::default()
                },
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
            control: SpinLock::new(None),
            stream_bytes,
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
    let slot = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker");
    let role = if listener == main.control_listener {
        let mut control = main.control.lock();
        if control.is_none() {
            *control = Some(ControlTest {
                handle,
                cookie: None,
                expected_streams: 0,
                accepted_streams: 0,
                data_sessions: 0,
                start_time: None,
                closed: false,
            });
            Some(ListenerRole::Control)
        } else {
            None
        }
    } else if listener == main.data_listener {
        Some(ListenerRole::Data)
    } else {
        return Err(ApplicationError::CallbackRejected);
    };
    let app = AppSession::new(session);
    let index = slot.sessions.insert(Iperf3Session::new(app, role));
    if role == Some(ListenerRole::Control) {
        slot.control_session = Some(index);
    }
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
    let role = slot
        .sessions
        .get(index)
        .expect("iperf3 Session record remains live until Session cleanup")
        .role;
    match role {
        Some(ListenerRole::Control) => {
            let session = slot
                .sessions
                .get_mut(index)
                .expect("iperf3 control Session remains live");
            let rx = app_session
                .rx_fifo()
                .expect("accepted iperf3 Session owns an RX FIFO");
            let tx = app_session
                .tx_fifo()
                .expect("accepted iperf3 Session owns a TX FIFO");
            rx.unset_event();
            loop {
                let cookie = if session.parser.phase() == crate::protocol::ControlPhase::Cookie
                    && rx.max_dequeue() >= COOKIE_SIZE
                {
                    let mut cookie = [0; COOKIE_SIZE];
                    assert_eq!(rx.peek(0, COOKIE_SIZE, &mut cookie), COOKIE_SIZE);
                    Some(cookie)
                } else {
                    None
                };
                let inspected = match ControlParser::inspect(session.parser.phase(), rx) {
                    Ok(inspected) => inspected,
                    Err(_) => {
                        session.phase = SessionPhase::Closing;
                        app_session.close();
                        return;
                    }
                };
                let Some((action, inspected_len)) = inspected else {
                    break;
                };

                let reply = match &action {
                    ControlAction::Parameters(_) => {
                        vec![ControlState::CreateStreams as u8]
                    }
                    ControlAction::SendState(state) => ControlParser::state_bytes(*state).to_vec(),
                    ControlAction::Results(stream_ids) => {
                        let control = main.control.lock();
                        let test = control
                            .as_ref()
                            .expect("control Session retains its test record");
                        if stream_ids.len() != test.expected_streams as usize {
                            app_session.close();
                            return;
                        }
                        let duration = test
                            .start_time
                            .map(|start| start.elapsed().as_secs_f64())
                            .unwrap_or(0.0);
                        drop(control);
                        // ESnet iperf_api.c:2875-2965,3010-3095. Result IDs
                        // come from the client; RX byte counts are published
                        // by the owning Data Worker into its own column.
                        let streams: Vec<_> = stream_ids
                            .iter()
                            .enumerate()
                            .map(|(stream_index, id)| {
                                let bytes = main
                                    .stream_bytes
                                    .iter()
                                    .map(|worker| worker[stream_index].load(Ordering::Acquire))
                                    .sum::<u64>();
                                serde_json::json!({
                                    "id": id,
                                    "bytes": bytes,
                                    "retransmits": -1,
                                    "jitter": 0,
                                    "errors": 0,
                                    "omitted_errors": 0,
                                    "packets": 0,
                                    "omitted_packets": 0,
                                    "start_time": 0.0,
                                    "end_time": duration,
                                })
                            })
                            .collect();
                        let results = serde_json::json!({
                            "cpu_util_total": 0.0,
                            "cpu_util_user": 0.0,
                            "cpu_util_system": 0.0,
                            "sender_has_retransmits": -1,
                            "streams": streams,
                        });
                        let encoded = serde_json::to_vec(&results)
                            .expect("finite iperf3 result fields serialize");
                        let length = u32::try_from(encoded.len())
                            .expect("bounded iperf3 result fits its length field");
                        let mut reply = Vec::with_capacity(4 + encoded.len() + 1);
                        reply.extend_from_slice(&length.to_be_bytes());
                        reply.extend_from_slice(&encoded);
                        reply.push(ControlState::DisplayResults as u8);
                        reply
                    }
                    ControlAction::Close => Vec::new(),
                };
                if tx.max_enqueue() < reply.len() {
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
                if let Some(cookie) = cookie {
                    let mut control = main.control.lock();
                    let test = control
                        .as_mut()
                        .expect("control Session retains its test record");
                    assert_eq!(test.handle, handle);
                    test.cookie = Some(cookie);
                }

                match action {
                    ControlAction::Parameters(parameters) => {
                        for worker in &main.stream_bytes {
                            for bytes in worker {
                                bytes.store(0, Ordering::Relaxed);
                            }
                        }
                        let mut control = main.control.lock();
                        let test = control
                            .as_mut()
                            .expect("control Session retains its test record");
                        test.expected_streams = parameters.parallel.unwrap_or(1);
                        test.accepted_streams = 0;
                        session.parameters = Some(parameters);
                    }
                    ControlAction::SendState(state) => {
                        if state == ControlState::ExchangeResults {
                            session.phase = SessionPhase::Running;
                        }
                    }
                    ControlAction::Close => {
                        session.phase = SessionPhase::Closing;
                        app_session.close();
                        return;
                    }
                    ControlAction::Results(_) => {}
                }

                if !reply.is_empty() {
                    let sent = session
                        .app
                        .send_stream(&reply, false)
                        .expect("blocking iperf3 stream send publishes its TX event");
                    assert_eq!(sent, reply.len(), "reserved control response fits TX FIFO");
                    session.sent = session.sent.saturating_add(sent as u64);
                }
            }
        }
        Some(ListenerRole::Data) => {
            let session = slot
                .sessions
                .get_mut(index)
                .expect("iperf3 data Session remains live");
            session.server_stream_rx_no_echo(app_session);
            if let Some(stream_index) = session.stream_index {
                main.stream_bytes[handle.worker_index as usize][stream_index as usize]
                    .store(session.received, Ordering::Release);
            }
        }
        None => {
            let rx = app_session
                .rx_fifo()
                .expect("accepted iperf3 Session owns an RX FIFO");
            rx.unset_event();
            if rx.max_dequeue() < COOKIE_SIZE {
                return;
            }
            let mut cookie = [0; COOKIE_SIZE];
            assert_eq!(rx.peek(0, COOKIE_SIZE, &mut cookie), COOKIE_SIZE);
            let assigned = {
                let mut control = main.control.lock();
                control.as_mut().and_then(|test| {
                    if test.closed
                        || test.cookie != Some(cookie)
                        || test.expected_streams == 0
                        || test.accepted_streams >= test.expected_streams
                    {
                        return None;
                    }
                    let stream_index = test.accepted_streams;
                    test.accepted_streams += 1;
                    test.data_sessions += 1;
                    let last = test.accepted_streams == test.expected_streams;
                    if last {
                        test.start_time = Some(Instant::now());
                    }
                    Some((stream_index, last, test.handle))
                })
            };
            let Some((stream_index, last, control_handle)) = assigned else {
                app_session.close();
                return;
            };
            assert_eq!(rx.drop_dequeue(COOKIE_SIZE), COOKIE_SIZE);
            let session = slot
                .sessions
                .get_mut(index)
                .expect("iperf3 data Session remains live");
            session.role = Some(ListenerRole::Data);
            session.phase = SessionPhase::Running;
            session.stream_index = Some(stream_index);
            session.server_stream_rx_no_echo(app_session);
            main.stream_bytes[handle.worker_index as usize][stream_index as usize]
                .store(session.received, Ordering::Release);
            if last {
                // Force the queue path even when control and data share a
                // worker: the RPC must run after this callback releases its
                // mutable worker Session borrow.
                let status = hammer_service::session::send_rpc_event_force(
                    control_handle.worker_index,
                    start_control_test,
                    u64::from(control_handle.session_index),
                );
                if !matches!(status, Ok(SessionEventEnqueue::Enqueued)) {
                    app_session.close();
                    hammer_service::session::send_control_event(
                        control_handle,
                        SessionEventType::Close,
                    )
                    .expect("control Session retains its worker event queue");
                }
            }
        }
    }
}

// VPP: vperf_server.c:139-149 sends worker operations through Session RPC.
// This callback runs on the control Session's worker, never on the data worker.
fn start_control_test(session_index: u64) {
    let main = IPERF3_MAIN
        .get()
        .expect("iperf3 Main publishes before Session RPC executes");
    let handle = {
        let control = main.control.lock();
        let Some(test) = control.as_ref() else {
            return;
        };
        if test.closed
            || u64::from(test.handle.session_index) != session_index
            || test.expected_streams != test.accepted_streams
        {
            return;
        }
        test.handle
    };
    let slot = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("control RPC executes on its Session worker");
    let Some(index) = slot.control_session else {
        return;
    };
    let control = slot
        .sessions
        .get_mut(index)
        .expect("control Session remains live until its callback finishes");
    let states = [
        ControlState::TestStart as u8,
        ControlState::TestRunning as u8,
    ];
    let sent = control
        .app
        .send_stream(&states, false)
        .expect("control Session TX event remains available");
    assert_eq!(sent, states.len(), "control states fit the TX FIFO");
    control.sent += sent as u64;
    control.phase = SessionPhase::Running;
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
    app_session.close();
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
    let slot = main
        .worker(DataWorkerId::new(handle.worker_index))
        .expect("iperf3 worker exists for each Session worker");
    let role = slot
        .sessions
        .get(index)
        .expect("iperf3 Session record remains live until Session cleanup")
        .role;
    let stream_index = slot
        .sessions
        .get(index)
        .expect("iperf3 Session record remains live until Session cleanup")
        .stream_index;
    slot.sessions
        .remove(index)
        .expect("iperf3 Session record remains live until Session cleanup");
    if role == Some(ListenerRole::Control) {
        assert_eq!(slot.control_session, Some(index));
        slot.control_session = None;
        let mut control = main.control.lock();
        let test = control
            .as_mut()
            .expect("control Session retains its test record until cleanup");
        assert_eq!(test.handle, handle);
        test.closed = true;
        if test.data_sessions == 0 {
            *control = None;
        }
    } else if stream_index.is_some() {
        let mut control = main.control.lock();
        let test = control
            .as_mut()
            .expect("data Session retains its control test until cleanup");
        assert!(test.data_sessions > 0);
        test.data_sessions -= 1;
        if test.closed && test.data_sessions == 0 {
            *control = None;
        }
    }
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
    runs_after = ["tcp_init", "session_lookup_init", "application_init"]
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
