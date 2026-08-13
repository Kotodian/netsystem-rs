//! One real VCL attach/bootstrap integration test over the production attach
//! path, independent of QUIC.
//!
//! The daemon side is a real `AppServer::serve` on a temp Unix socket: it
//! allocates the real `ApplicationId`, builds the real
//! `ApplicationMqPublication` (per-worker Application Rx MQ set and
//! ext-config store in a shared segment), and delivers them over the real
//! attach descriptors (metadata words + SCM_RIGHTS). Established Sessions
//! arrive through the real `AppSessionPublisher::try_publish` as
//! `AppSessionPublication`s carrying descriptors and concrete ACCEPTED /
//! CONNECTED control messages; the client attaches through the public
//! `VclWorker::attach`.
//!
//! `application_session_control` is the only scripted request -> reply seam:
//! it consumes the client's real control requests (LISTEN, CONNECT_STREAM,
//! ACCEPTED_REPLY) on the real request queue and forwards them to the test
//! for assertion. No `AppClient::with_queues`, no hand-scripted descriptor
//! frames, no environment or interposition shim.

use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, ApplicationId, SessionAcceptedMsg, SessionAcceptedReplyMsg, SessionBoundMsg,
    SessionConnectMsg, SessionConnectedMsg, SessionEvtType, SessionFlags, SessionHandle,
    SessionListenMsg, SessionMsgQueue, SessionOffsets, TransportProtocol,
};
use hammer_runtime::attach::{
    AppServer, AppSessionPublication, ApplicationMqPublication, ExtConfigStore,
};
use hammer_runtime::{DataWorkerId, SessionListenEndpoint};
use hammer_vcl::{VclEvent, VclInitiator, VclSessionState, VclWorker};

const FIFO_CAPACITY: usize = 4096;
const EVT_Q_CAPACITY: usize = 16;
const WORKER_COUNT: usize = 3;
const EVENT_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(2);

const LOCAL: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4433);
const REMOTE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 4433);
const LISTENER_WIRE: SessionHandle = SessionHandle::new(101, 0);
const PEER_WIRE: SessionHandle = SessionHandle::new(5, 0);
const CHILD_WIRE: SessionHandle = SessionHandle::new(9, 0);

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn socket_path() -> PathBuf {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("hm-vcl-{:x}-{counter:x}.sock", std::process::id()))
}

fn queue_capacity_words() -> (u32, u32) {
    let ring_nitems = EVT_Q_CAPACITY as u32;
    let q_nitems = (EVT_Q_CAPACITY + 1).next_power_of_two() as u32;
    (q_nitems, ring_nitems)
}

/// The real Application resources the daemon publishes to the attaching
/// client: a shared segment holding the per-worker Application Rx MQ set and
/// the ext-config store, plus the `ApplicationMqPublication` carrying them.
#[derive(Clone)]
struct ApplicationMqs {
    publication: ApplicationMqPublication,
    queues: Box<[Arc<SessionMsgQueue>]>,
}

fn build_application_mqs() -> ApplicationMqs {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let segment = Segment::shared(
        &format!("hamr{}-{counter}", std::process::id()),
        1024 * 1024,
    )
    .expect("Application Rx MQ segment");
    let (q_nitems, ring_nitems) = queue_capacity_words();
    let queue_bytes = SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).expect("queue layout");
    let mut queues = Vec::with_capacity(WORKER_COUNT);
    let mut offsets = Vec::with_capacity(WORKER_COUNT);
    for _ in 0..WORKER_COUNT {
        let offset = segment.alloc(queue_bytes, 64).expect("queue offset");
        let queue = unsafe {
            SessionMsgQueue::init_at_with_signal(segment.clone(), offset, q_nitems, ring_nitems)
        }
        .expect("Application Rx MQ");
        queues.push(Arc::new(queue));
        offsets.push(offset);
    }
    let queues = queues.into_boxed_slice();
    let offsets = offsets.into_boxed_slice();
    let ext_config_offset = segment
        .alloc(ExtConfigStore::layout_bytes(), 64)
        .expect("ext-config store offset");
    // SAFETY: the store layout was just allocated in an exclusively owned
    // segment; the free list is initialized exactly once before any client
    // attaches and reads or allocates from it.
    let ext_config =
        unsafe { ExtConfigStore::init_at(segment.clone(), ext_config_offset as usize) };
    let publication = ApplicationMqPublication::new(
        segment.clone(),
        queues.clone(),
        offsets.clone(),
        ext_config.offset() as u64,
    )
    .expect("Application MQ publication");
    ApplicationMqs {
        publication,
        queues,
    }
}

/// One daemon-side established Session: FIFOs and event queue in a shared
/// segment, the `AppSession` the daemon writes into, and the
/// `AppSessionPublication` carrying descriptors plus a concrete control
/// message to `AppSessionPublisher::try_publish`.
struct PublishedSession {
    session: Arc<AppSession>,
    publication: AppSessionPublication,
}

fn build_publication(
    application: ApplicationId,
    handle: SessionHandle,
    application_mqs: &ApplicationMqs,
) -> PublishedSession {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let session_segment =
        Segment::shared(&format!("hs{}-{counter}", std::process::id()), 1024 * 1024)
            .expect("session segment");
    let (q_nitems, ring_nitems) = queue_capacity_words();
    let queue_bytes = SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).expect("queue layout");

    let fifo_bytes = Fifo::layout_bytes(FIFO_CAPACITY).expect("fifo layout");
    let rx_fifo_off = session_segment.alloc(fifo_bytes, 64).expect("rx offset");
    let tx_fifo_off = session_segment.alloc(fifo_bytes, 64).expect("tx offset");
    let evt_q_off = session_segment
        .alloc(queue_bytes, 64)
        .expect("event queue offset");
    // SAFETY: each offset was allocated with the matching layout size and is
    // used by exactly one queue.
    let rx_fifo = unsafe { Fifo::init_at(session_segment.clone(), rx_fifo_off, FIFO_CAPACITY) }
        .expect("rx fifo");
    // SAFETY: as above.
    let tx_fifo = unsafe { Fifo::init_at(session_segment.clone(), tx_fifo_off, FIFO_CAPACITY) }
        .expect("tx fifo");
    // SAFETY: as above.
    let evt_q = Arc::new(
        unsafe {
            SessionMsgQueue::init_at_with_signal(
                session_segment.clone(),
                evt_q_off,
                q_nitems,
                ring_nitems,
            )
        }
        .expect("event queue"),
    );

    let worker_queue = application_mqs.queues[handle.worker_index() as usize].clone();
    let session = Arc::new(AppSession::from_parts(
        Arc::new(rx_fifo),
        Arc::new(tx_fifo),
        evt_q,
        worker_queue,
        handle,
    ));
    let offsets = SessionOffsets {
        rx_fifo_off,
        tx_fifo_off,
        evt_q_off,
    };
    let publication = AppSessionPublication::new(
        Arc::clone(&session),
        application,
        session_segment.clone(),
        offsets,
    )
    .expect("session publication");
    PublishedSession {
        session,
        publication,
    }
}

/// One scripted observation forwarded by the `application_session_control`
/// seam; the test asserts on the real requests and replies the daemon side
/// exchanged with the real client.
#[derive(Debug)]
enum SeamEvent {
    Listen(SessionListenMsg, SessionBoundMsg),
    Connect(SessionConnectMsg),
    AcceptedReply(SessionAcceptedReplyMsg),
    Unexpected,
}

fn spawn_serve(
    server: Arc<AppServer>,
    application_mqs: ApplicationMqs,
    allocated: Arc<AtomicU64>,
    detached: Arc<AtomicU64>,
    seam: mpsc::Sender<SeamEvent>,
) {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("serve runtime");
        let next_application = Arc::clone(&allocated);
        let detach_count = Arc::clone(&detached);
        let _ = runtime.block_on(server.serve(
            move || {
                let raw = next_application.fetch_add(1, Ordering::Relaxed);
                allocated.store(raw, Ordering::Relaxed);
                Ok::<ApplicationId, Infallible>(ApplicationId::from_raw(raw))
            },
            move |_| {
                Ok::<ApplicationMqPublication, Infallible>(application_mqs.publication.clone())
            },
            move |_application, requests, replies| {
                while let Some(item) = requests.dequeue_control()? {
                    match item.event_type() {
                        SessionEvtType::Listen => {
                            let request = item
                                .decode::<SessionListenMsg>()
                                .expect("decode Listen request")
                                .expect("Listen payload");
                            let bound = SessionBoundMsg {
                                context: request.context,
                                result: Ok(LISTENER_WIRE),
                                local: Some(LOCAL),
                                opaque: None,
                            };
                            replies.enqueue_control(&bound)?;
                            seam.send(SeamEvent::Listen(request, bound))
                                .expect("forward Listen");
                        }
                        SessionEvtType::Connect | SessionEvtType::ConnectStream => {
                            let request = item
                                .decode::<SessionConnectMsg>()
                                .expect("decode Connect request")
                                .expect("Connect payload");
                            seam.send(SeamEvent::Connect(request))
                                .expect("forward Connect");
                        }
                        SessionEvtType::AcceptedReply => {
                            let request = item
                                .decode::<SessionAcceptedReplyMsg>()
                                .expect("decode AcceptedReply request")
                                .expect("AcceptedReply payload");
                            seam.send(SeamEvent::AcceptedReply(request))
                                .expect("forward AcceptedReply");
                        }
                        _ => {
                            let _ = seam.send(SeamEvent::Unexpected);
                        }
                    }
                }
                Ok(())
            },
            move |_| {
                detach_count.fetch_add(1, Ordering::Relaxed);
            },
        ));
    });
}

/// VCL's `session_poll` is a nonblocking pump (the real worker wakes on the
/// event-queue signal): the daemon delivers publications asynchronously
/// through the serve loop, so the test pumps until the expected events land
/// or the deadline expires.
fn pump_events(worker: &mut VclWorker) -> Vec<VclEvent> {
    let deadline = Instant::now() + EVENT_TIMEOUT;
    let mut events = Vec::new();
    loop {
        events.extend(worker.session_poll().expect("session poll"));
        if !events.is_empty() || Instant::now() >= deadline {
            return events;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Blocks until the scripted seam observes the next daemon/client exchange.
fn next_seam(seam: &mpsc::Receiver<SeamEvent>) -> SeamEvent {
    seam.recv_timeout(EVENT_TIMEOUT).expect("seam event")
}

/// One full real attach/bootstrap flow: `VclWorker::attach` over a real
/// `AppServer::serve`; listen resolved by the scripted control seam; a
/// peer-open parent delivered as a real ACCEPTED `AppSessionPublication`;
/// and a child stream-connect completed by a real CONNECTED
/// `AppSessionPublication` with descriptors, asserted Ready through the
/// public VCL API and functional over the shared FIFOs.
#[test]
fn attach_bootstrap_connects_through_real_server_and_publication() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 8).expect("bind app server"));
    let publisher = server.publisher();
    let application_mqs = build_application_mqs();
    // The daemon's attach closure fetch_adds: the first allocation is
    // ApplicationId(1), not 0 (ApplicationId(0) is no real identity).
    let allocated = Arc::new(AtomicU64::new(1));
    let detached = Arc::new(AtomicU64::new(0));
    let (seam_tx, seam_rx) = mpsc::channel();
    spawn_serve(
        Arc::clone(&server),
        application_mqs.clone(),
        Arc::clone(&allocated),
        Arc::clone(&detached),
        seam_tx,
    );

    // Public attach: the daemon allocated a real ApplicationId and delivered
    // the real ApplicationMqPublication resources over the attach protocol.
    let mut worker = VclWorker::attach(&path_text, 8).expect("VCL attach");
    let application = ApplicationId::from_raw(allocated.load(Ordering::Relaxed));
    assert_ne!(
        application,
        ApplicationId::from_raw(0),
        "real ApplicationId"
    );

    // Listen: the scripted control seam consumes the real LISTEN request and
    // replies BOUND on the real reply queue; the listener itself needs no
    // Session publication.
    let listener = worker
        .session_listen(
            TransportProtocol::Quic,
            SessionListenEndpoint::new(LOCAL, DataWorkerId::new(0)),
            None,
        )
        .expect("listen");
    let SeamEvent::Listen(request, bound) = next_seam(&seam_rx) else {
        panic!("expected Listen seam event")
    };
    assert_eq!(request.transport, TransportProtocol::Quic);
    assert_eq!(
        request.application, application,
        "real ApplicationId on the wire"
    );
    assert_eq!(bound.result, Ok(LISTENER_WIRE));
    assert_eq!(
        worker.session_state(listener).expect("listener state"),
        VclSessionState::Listen
    );

    // Peer-open parent: one real ACCEPTED publication with descriptors.
    let parent_published = build_publication(application, PEER_WIRE, &application_mqs);
    let mut parent_publication = parent_published.publication;
    parent_publication
        .set_accepted(SessionAcceptedMsg::new(
            application.raw(),
            LISTENER_WIRE,
            PEER_WIRE,
            SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL,
        ))
        .expect("set ACCEPTED");
    publisher
        .try_publish(&parent_publication)
        .expect("publish parent");
    let events = pump_events(&mut worker);
    assert_eq!(events.len(), 1, "one Accepted event, got {events:?}");
    let VclEvent::Accepted {
        session: parent,
        parent: accepted_parent,
    } = events[0]
    else {
        panic!("expected Accepted event, got {events:?}")
    };
    assert_eq!(accepted_parent, listener);
    assert_ne!(parent, listener);
    assert_eq!(
        worker.session_state(parent).expect("parent state"),
        VclSessionState::Ready
    );
    let parent_attributes = worker
        .session_attributes(parent)
        .expect("parent attributes");
    assert!(parent_attributes.stream);
    assert!(parent_attributes.unidirectional);
    assert_eq!(parent_attributes.initiator, VclInitiator::Peer);
    let SeamEvent::AcceptedReply(accepted_reply) = next_seam(&seam_rx) else {
        panic!("expected AcceptedReply seam event")
    };
    assert_eq!(accepted_reply.context, application.raw());
    assert_eq!(accepted_reply.session, PEER_WIRE);
    assert!(accepted_reply.result.is_ok());

    // Child stream-connect: the real CONNECT_STREAM request is consumed by
    // the scripted control seam; the child leaves the call in Connecting.
    let child = worker
        .session_create(TransportProtocol::Quic, true)
        .expect("create");
    worker
        .session_stream_connect(child, parent, REMOTE, None, SessionFlags::empty())
        .expect("nonblocking connect returns immediately");
    assert_eq!(
        worker.session_state(child).expect("child state"),
        VclSessionState::Connecting
    );
    let SeamEvent::Connect(request) = next_seam(&seam_rx) else {
        panic!("expected Connect seam event")
    };
    assert_eq!(request.context, child.raw());
    assert_eq!(request.transport, TransportProtocol::Quic);
    assert_eq!(request.remote, REMOTE);
    assert_eq!(request.parent_handle, Some(PEER_WIRE));
    assert!(request.flags.contains(SessionFlags::STREAM));
    assert_eq!(request.application, application);

    // Active-open child: one real CONNECTED publication with descriptors and
    // a concrete CONNECTED; VCL makes the child Ready through its public API.
    let child_published = build_publication(application, CHILD_WIRE, &application_mqs);
    let mut child_publication = child_published.publication;
    child_publication.set_connected(SessionConnectedMsg {
        context: child.raw(),
        result: Ok(CHILD_WIRE),
        local: Some(LOCAL),
        remote: Some(REMOTE),
        flags: SessionFlags::empty(),
        opaque: None,
    });
    publisher
        .try_publish(&child_publication)
        .expect("publish child");
    let events = pump_events(&mut worker);
    assert_eq!(events, vec![VclEvent::Connected { session: child }]);
    assert_eq!(
        worker.session_state(child).expect("child state"),
        VclSessionState::Ready
    );
    let attributes = worker.session_attributes(child).expect("child attributes");
    assert!(attributes.stream);
    assert!(!attributes.unidirectional);
    assert_eq!(attributes.initiator, VclInitiator::Local);
    assert!(attributes.readable());
    assert!(attributes.writable());

    // The delivered descriptors are functional: the daemon writes into the
    // published rx FIFO and reads the client's tx FIFO.
    child_published
        .session
        .enqueue_rx(b"ping")
        .expect("enqueue daemon rx");
    let mut buffer = [0_u8; 16];
    let read = worker.session_recv(child, &mut buffer).expect("VCL recv");
    assert_eq!(&buffer[..read], b"ping");
    assert_eq!(worker.session_send(child, b"pong").expect("VCL send"), 4);
    let mut echoed = [0_u8; 16];
    let read = child_published
        .session
        .tx_fifo()
        .peek(0, echoed.len(), &mut echoed);
    assert_eq!(&echoed[..read], b"pong");

    assert_eq!(detached.load(Ordering::Relaxed), 0, "no detach observed");
    drop(worker);
    let _ = std::fs::remove_file(path);
}

/// Generic active open through the real attach path: `session_connect`
/// drives the real ordinary CONNECT control message carrying the
/// create-time transport, local/remote endpoints, opaque, and the server
/// name stored in the real ext-config chunk (VPP `vcl_send_session_connect`,
/// vppcom.c:76); the daemon's real CONNECTED publication completes the
/// Session.
#[test]
fn attach_bootstrap_generic_connect_forwards_server_name_and_opaque() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 8).expect("bind app server"));
    let publisher = server.publisher();
    let application_mqs = build_application_mqs();
    let allocated = Arc::new(AtomicU64::new(1));
    let detached = Arc::new(AtomicU64::new(0));
    let (seam_tx, seam_rx) = mpsc::channel();
    spawn_serve(
        Arc::clone(&server),
        application_mqs.clone(),
        Arc::clone(&allocated),
        Arc::clone(&detached),
        seam_tx,
    );
    let mut worker = VclWorker::attach(&path_text, 8).expect("VCL attach");

    let session = worker
        .session_create(TransportProtocol::Http, true)
        .expect("create");
    worker
        .session_connect(
            session,
            REMOTE,
            Some(LOCAL),
            Some("example.com"),
            Some(0xCAFE),
        )
        .expect("nonblocking connect returns immediately");
    assert_eq!(
        worker.session_state(session).expect("session state"),
        VclSessionState::Connecting
    );

    // The scripted control seam consumed the real CONNECT: create-time
    // transport, endpoints and opaque are forwarded, no parent and no
    // stream flag; the server name was stored in the real ext-config chunk
    // and carried as the opaque bounded offset.
    let SeamEvent::Connect(request) = next_seam(&seam_rx) else {
        panic!("expected Connect seam event")
    };
    assert_eq!(request.transport, TransportProtocol::Http);
    assert_eq!(request.remote, REMOTE);
    assert_eq!(request.local, Some(LOCAL));
    assert_eq!(request.opaque, Some(0xCAFE));
    assert_eq!(request.parent_handle, None);
    assert!(!request.flags.contains(SessionFlags::STREAM));
    let chunk = request.ext_config.expect("server name ext-config chunk");
    let store = application_mqs
        .publication
        .ext_config_store()
        .expect("ext-config store")
        .expect("daemon published one");
    assert_eq!(store.read(chunk).expect("read chunk"), b"example.com");

    // The real CONNECTED publication with descriptors completes the Session
    // through `session_poll`.
    let application = ApplicationId::from_raw(allocated.load(Ordering::Relaxed));
    let child_published = build_publication(application, CHILD_WIRE, &application_mqs);
    let mut child_publication = child_published.publication;
    child_publication.set_connected(SessionConnectedMsg {
        context: request.context,
        result: Ok(CHILD_WIRE),
        local: Some(LOCAL),
        remote: Some(REMOTE),
        flags: SessionFlags::STREAM,
        opaque: None,
    });
    publisher
        .try_publish(&child_publication)
        .expect("publish child");
    let events = pump_events(&mut worker);
    assert_eq!(events, vec![VclEvent::Connected { session }]);
    assert_eq!(
        worker.session_state(session).expect("session state"),
        VclSessionState::Ready
    );
    let attributes = worker.session_attributes(session).expect("attributes");
    assert!(attributes.stream);
    assert_eq!(attributes.initiator, VclInitiator::Local);
    assert_eq!(
        worker.session_proto(session).expect("session proto"),
        TransportProtocol::Http
    );

    assert_eq!(detached.load(Ordering::Relaxed), 0, "no detach observed");
    drop(worker);
    let _ = std::fs::remove_file(path);
}
