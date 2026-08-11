//! Attach-protocol v3 integration tests: per-Application Rx MQ publication,
//! AppClient worker mapping, session publication, and descriptor lifetime on
//! every failure path.

use std::convert::Infallible;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hammer_app::attach::{AppClient, AppClientError, ControlReply};
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, ApplicationConnectionId, ApplicationId, SessionAcceptedMsg,
    SessionAcceptedReplyMsg, SessionBoundMsg, SessionConnectError, SessionConnectMsg,
    SessionConnectedMsg, SessionEvtType, SessionFlags, SessionHandle, SessionListenMsg,
    SessionMsgQueue, SessionMsgQueueError, SessionOffsets, SessionProducer, SingleProducer,
    TransportProtocol,
};
use hammer_runtime::attach::{
    APPLICATION_MQ_BASE_DESCRIPTOR_COUNT, APPLICATION_MQ_METADATA_WORDS, ATTACH_DESCRIPTOR_COUNT,
    ATTACH_PROTOCOL_VERSION, ATTACH_REPLY_BYTES, ATTACH_REQUEST_BYTES, ATTACH_STATUS_ACCEPTED,
    AppServer, AppSessionPublication, ApplicationMqPublication, EXT_CONFIG_CHUNK_BYTES,
    EXT_CONFIG_CHUNK_COUNT, ExtConfigStore,
};
use hammer_runtime::{AttachError, RuntimeError};

const FIFO_CAPACITY: usize = 4096;
const EVT_Q_CAPACITY: usize = 16;
const WORKER_COUNT: usize = 3;

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn socket_path() -> PathBuf {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("hm-{:x}-{counter:x}.sock", std::process::id()))
}

fn descriptor_identity(fd: RawFd) -> std::io::Result<(libc::dev_t, libc::ino_t)> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: status is writable for one stat and fstat initializes it on success.
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } == 0 {
        // SAFETY: successful fstat initialized the complete stat value.
        let status = unsafe { status.assume_init() };
        Ok((status.st_dev, status.st_ino))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn count_open_identity(identity: (libc::dev_t, libc::ino_t)) -> usize {
    std::fs::read_dir("/dev/fd")
        .expect("read /dev/fd")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<RawFd>().ok())
        .filter(|fd| descriptor_identity(*fd).ok() == Some(identity))
        .count()
}

/// Exact descriptor counting needs distinct fstat identities per object;
/// macOS reports degenerate shared identities for shm and socket
/// descriptors, so only Linux asserts the count.
fn assert_identity_count(identity: (libc::dev_t, libc::ino_t), expected: usize) {
    if cfg!(target_os = "linux") {
        assert_eq!(count_open_identity(identity), expected);
    }
}

struct PublishedSession {
    session: Arc<AppSession>,
    publication: AppSessionPublication,
    session_segment: Segment,
    application_mqs: ApplicationMqs,
    worker_queues: Vec<Arc<SessionMsgQueue>>,
}

#[derive(Clone)]
struct ApplicationMqs {
    publication: ApplicationMqPublication,
    segment: Segment,
    queues: Box<[Arc<SessionMsgQueue>]>,
    offsets: Box<[u64]>,
    ext_config_offset: u64,
}

impl ApplicationMqs {
    fn ext_config_store(&self) -> ExtConfigStore {
        ExtConfigStore::from_shared(self.segment.clone(), self.ext_config_offset as usize)
            .expect("attached ext-config store")
    }
}

fn queue_capacity_words() -> (u32, u32) {
    let ring_nitems = EVT_Q_CAPACITY as u32;
    let q_nitems = (EVT_Q_CAPACITY + 1).next_power_of_two() as u32;
    (q_nitems, ring_nitems)
}

fn build_application_mqs() -> ApplicationMqs {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let segment = Segment::shared(
        &format!("hamr{}-{counter}", std::process::id()),
        1024 * 1024,
    )
    .expect("rx MQ segment");
    let (q_nitems, ring_nitems) = queue_capacity_words();
    let queue_bytes = SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).expect("queue layout");
    let mut queues = Vec::with_capacity(WORKER_COUNT);
    let mut offsets = Vec::with_capacity(WORKER_COUNT);
    for _ in 0..WORKER_COUNT {
        let offset = segment.alloc(queue_bytes, 64).expect("queue offset");
        let queue = unsafe {
            SessionMsgQueue::init_at_with_signal(segment.clone(), offset, q_nitems, ring_nitems)
        }
        .expect("Application MQ");
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
        segment,
        queues,
        offsets,
        ext_config_offset,
    }
}

fn build_publication(application: ApplicationId, handle: SessionHandle) -> PublishedSession {
    let application_mqs = build_application_mqs();
    build_publication_with_mqs(application, handle, application_mqs)
}

fn build_publication_with_mqs(
    application: ApplicationId,
    handle: SessionHandle,
    application_mqs: ApplicationMqs,
) -> PublishedSession {
    build_publication_with_worker_queue(
        application,
        handle,
        application_mqs,
        handle.worker_index() as usize,
    )
}

fn build_publication_with_worker_queue(
    application: ApplicationId,
    handle: SessionHandle,
    application_mqs: ApplicationMqs,
    queue_worker: usize,
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

    let worker_queue = application_mqs.queues[queue_worker].clone();
    let worker_queues = application_mqs.queues.to_vec();
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
        session_segment,
        application_mqs,
        worker_queues,
    }
}

fn spawn_serve(
    server: Arc<AppServer>,
    first_application: ApplicationId,
    application_mqs: ApplicationMqs,
) {
    let next_application = Arc::new(AtomicU64::new(first_application.raw()));
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("serve runtime");
        let attach_sequence = Arc::clone(&next_application);
        let _ = runtime.block_on(server.serve(
            move || {
                Ok::<ApplicationId, Infallible>(ApplicationId::from_raw(
                    attach_sequence.fetch_add(1, Ordering::Relaxed),
                ))
            },
            move |_| {
                Ok::<ApplicationMqPublication, Infallible>(application_mqs.publication.clone())
            },
            |_, _, _| Ok(()),
            |_| {},
        ));
    });
}

fn spawn_serve_with_control(
    server: Arc<AppServer>,
    first_application: ApplicationId,
    application_mqs: ApplicationMqs,
    control: impl Fn(
        ApplicationId,
        &mut SessionMsgQueue<SingleProducer>,
        &mut SessionProducer,
    ) -> hammer_runtime::RuntimeResult<()>
    + Send
    + 'static,
) {
    let next_application = Arc::new(AtomicU64::new(first_application.raw()));
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("serve runtime");
        let attach_sequence = Arc::clone(&next_application);
        let _ = runtime.block_on(server.serve(
            move || {
                Ok::<ApplicationId, Infallible>(ApplicationId::from_raw(
                    attach_sequence.fetch_add(1, Ordering::Relaxed),
                ))
            },
            move |_| {
                Ok::<ApplicationMqPublication, Infallible>(application_mqs.publication.clone())
            },
            control,
            |_| {},
        ));
    });
}

fn send_fds(stream: &UnixStream, fds: &[RawFd], metadata: &[u64]) {
    let mut bytes = vec![0_u8; metadata.len() * size_of::<u64>()];
    for (chunk, word) in bytes
        .chunks_exact_mut(size_of::<u64>())
        .zip(metadata.iter().copied())
    {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    let iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast::<libc::c_void>(),
        iov_len: bytes.len(),
    };
    let control_bytes = unsafe { libc::CMSG_SPACE(std::mem::size_of_val(fds) as u32) as usize };
    let control_elements = control_bytes.div_ceil(size_of::<libc::cmsghdr>());
    let mut control = Vec::<libc::cmsghdr>::with_capacity(control_elements);
    control.resize_with(control_elements, || {
        // SAFETY: a cmsghdr contains only integer fields, for which zero is a
        // valid initialized value.
        unsafe { std::mem::zeroed() }
    });
    // SAFETY: zero is a valid initial value for every msghdr field.
    let mut message = unsafe { std::mem::zeroed::<libc::msghdr>() };
    message.msg_iov = std::ptr::from_ref(&iov).cast_mut();
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
    message.msg_controllen = control_bytes as _;

    // SAFETY: message owns a sufficiently large control buffer and the first
    // header is initialized before sendmsg reads it.
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        assert!(!header.is_null());
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN((std::mem::size_of_val(fds)) as u32) as _;
        std::ptr::copy_nonoverlapping(
            fds.as_ptr(),
            libc::CMSG_DATA(header).cast::<RawFd>(),
            fds.len(),
        );
        assert_eq!(
            libc::sendmsg(stream.as_raw_fd(), &message, 0),
            bytes.len() as isize
        );
    }
}

fn accept_application(
    stream: &mut UnixStream,
    application: ApplicationId,
    application_mqs: &ApplicationMqs,
) {
    let _ = accept_application_with(
        stream,
        application,
        application_mqs,
        |stream, fds, words| {
            send_fds(stream, fds, words);
        },
    );
}

fn accept_application_with(
    stream: &mut UnixStream,
    application: ApplicationId,
    application_mqs: &ApplicationMqs,
    send: impl FnOnce(&mut UnixStream, &[RawFd], &[u64]),
) -> (
    SessionMsgQueue<SingleProducer>,
    SessionMsgQueue<SingleProducer>,
) {
    accept_registration(stream, application);

    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let segment = Segment::shared(&format!("hc{}-{counter}", std::process::id()), 1024 * 1024)
        .expect("Application control segment");
    let queue_bytes =
        SessionMsgQueue::layout_bytes_with_control(16, 8).expect("control queue layout");
    let request_offset = segment
        .alloc(queue_bytes, 64)
        .expect("request queue offset");
    let reply_offset = segment.alloc(queue_bytes, 64).expect("reply queue offset");
    let requests = unsafe {
        SessionMsgQueue::init_at_with_signal_and_control(segment.clone(), request_offset, 16, 8)
            .expect("request queue")
    };
    let replies = unsafe {
        SessionMsgQueue::init_at_with_signal_and_control(segment.clone(), reply_offset, 16, 8)
            .expect("reply queue")
    };
    let mut descriptors = vec![
        segment.shared_fd().expect("control segment descriptor"),
        requests.write_fd().expect("request signal"),
        replies.read_fd().expect("reply signal"),
        application_mqs
            .segment
            .shared_fd()
            .expect("rx MQ segment descriptor"),
    ];
    for queue in &application_mqs.queues {
        descriptors.push(queue.write_fd().expect("worker MQ write signal"));
    }
    let mut words = vec![
        ATTACH_PROTOCOL_VERSION,
        segment.size() as u64,
        request_offset,
        reply_offset,
        application_mqs.segment.size() as u64,
        application_mqs.queues.len() as u64,
        application_mqs.ext_config_offset,
    ];
    for offset in &application_mqs.offsets {
        words.push(*offset);
    }
    send(stream, &descriptors, &words);
    // Hand the control queues back so callers can keep their signal read ends
    // open for the client's lifetime (the real daemon stores them in
    // AttachedApplication); dropping them here would EPIPE the client's next
    // enqueue signal.
    (requests, replies)
}

fn accept_registration(stream: &mut UnixStream, application: ApplicationId) {
    let mut registration = [0_u8; ATTACH_REQUEST_BYTES];
    stream
        .read_exact(&mut registration)
        .expect("read Application attach registration");
    assert_eq!(u64::from_le_bytes(registration), ATTACH_PROTOCOL_VERSION);
    let mut reply = [0_u8; ATTACH_REPLY_BYTES];
    for (chunk, word) in reply.chunks_exact_mut(size_of::<u64>()).zip([
        ATTACH_PROTOCOL_VERSION,
        ATTACH_STATUS_ACCEPTED,
        application.raw(),
    ]) {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    stream.write_all(&reply).expect("accept Application attach");
}

fn assert_publish_then_connect_round_trips_handle_and_descriptors() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(1);
    let handle = SessionHandle::new(5, 2);
    let published = build_publication(application, handle);
    publisher
        .try_publish(&published.publication)
        .expect("publish before client");
    spawn_serve(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
    );

    let segment_fd = published
        .session_segment
        .shared_fd()
        .expect("session segment descriptor");
    let identity = descriptor_identity(segment_fd).expect("segment identity");
    let baseline = count_open_identity(identity);

    let client = AppClient::attach(&path_text).expect("attach client");
    assert_eq!(client.application(), application);
    let client_session = client.accept().expect("accept App Session");
    assert_eq!(client_session.session_handle(), handle);
    assert_identity_count(identity, baseline + 1);

    // Dataplane -> app across the shared session segment and event queue.
    published
        .session
        .enqueue_rx(b"ping")
        .expect("enqueue server rx");
    let mut buffer = [0_u8; 16];
    let read = client_session.recv_bytes(&mut buffer);
    assert_eq!(&buffer[..read], b"ping");
    assert_eq!(client_session.consume_rx(read), read);

    // App -> dataplane through the per-worker Application Rx MQ selected by
    // the session handle, not through a per-session TX event MQ.
    assert_eq!(client_session.send_bytes(b"pong").expect("client send"), 4);
    let mut echoed = [0_u8; 16];
    let read = published
        .session
        .tx_fifo()
        .peek(0, echoed.len(), &mut echoed);
    assert_eq!(&echoed[..read], b"pong");
    let event = published.worker_queues[handle.worker_index() as usize]
        .dequeue()
        .expect("per-worker MQ dequeue")
        .expect("per-worker MQ event");
    assert_eq!(event.session_index(), handle.session_index());
    assert_eq!(event.evt_type, SessionEvtType::TxEnq);

    drop(client_session);
    drop(client);
    assert_identity_count(identity, baseline);
    let _ = std::fs::remove_file(path);
}

fn assert_connect_before_publish_completes_after_publication() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(2);
    let published = build_publication(application, SessionHandle::new(9, 0));
    spawn_serve(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
    );

    let client_path = path_text.clone();
    let client_thread = std::thread::spawn(move || {
        let client = AppClient::attach(&client_path)?;
        assert_eq!(client.application(), application);
        client.accept()
    });
    std::thread::sleep(Duration::from_millis(100));

    publisher
        .try_publish(&published.publication)
        .expect("publish after client");

    let client = client_thread
        .join()
        .expect("join client thread")
        .expect("attach client");
    assert_eq!(client.session_handle(), SessionHandle::new(9, 0));
    let _ = std::fs::remove_file(path);
}

fn assert_failed_attach_requeues_publication_for_next_client() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(3);
    let published = build_publication(application, SessionHandle::new(3, 1));
    spawn_serve(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
    );

    let dead = UnixStream::connect(&path).expect("connect doomed client");
    dead.shutdown(std::net::Shutdown::Both)
        .expect("shutdown doomed client");
    drop(dead);
    std::thread::sleep(Duration::from_millis(100));

    publisher
        .try_publish(&published.publication)
        .expect("publish after doomed client");

    let client = AppClient::attach(&path_text).expect("attach surviving client");
    assert_eq!(client.application(), application);
    let client_session = client.accept().expect("accept surviving App Session");
    assert_eq!(client_session.session_handle(), SessionHandle::new(3, 1));
    let _ = std::fs::remove_file(path);
}

fn assert_publication_queue_reports_full_and_closed() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = AppServer::bind(&path_text, 1).expect("bind app server");
    let publisher = server.publisher();
    let published = build_publication(ApplicationId::from_raw(4), SessionHandle::new(1, 0));

    publisher
        .try_publish(&published.publication)
        .expect("first publish fills the queue");
    let full = publisher
        .try_publish(&published.publication)
        .expect_err("second publish overflows");
    assert!(matches!(
        full,
        RuntimeError::Attach(AttachError::PublicationQueueFull)
    ));

    drop(server);
    let closed = publisher
        .try_publish(&published.publication)
        .expect_err("publish after server drop");
    assert!(matches!(
        closed,
        RuntimeError::Attach(AttachError::PublicationQueueClosed)
    ));
    let _ = std::fs::remove_file(path);
}

#[test]
fn app_client_buffers_interleaved_connected_messages() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let application = ApplicationId::from_raw(40);
    let application_mqs = build_application_mqs();
    let connection = ApplicationConnectionId::from_raw(0x4000_0001);
    spawn_serve_with_control(
        Arc::clone(&server),
        application,
        application_mqs,
        move |_, requests, replies| {
            let item = requests
                .dequeue_control()
                .expect("dequeue Session control request")
                .expect("listen request");
            let request = item
                .decode::<SessionListenMsg>()
                .expect("decode listen request")
                .expect("decode listen payload");
            let failure =
                SessionConnectedMsg::new(connection.raw(), Err(SessionConnectError::TimedOut));
            replies
                .enqueue_control(&failure)
                .expect("enqueue asynchronous CONNECTED message");
            let bound = SessionBoundMsg {
                context: request.context,
                result: Ok(SessionHandle::from(0x4000_0002)),
                local: None,
                opaque: None,
            };
            replies
                .enqueue_control(&bound)
                .expect("enqueue listen response");
            Ok(())
        },
    );

    let mut client = AppClient::attach(&path_text).expect("attach client");
    let listener = client
        .listen(
            TransportProtocol::Tcp,
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("listen endpoint"),
                hammer_runtime::DataWorkerId::new(0),
            ),
            None,
            None,
        )
        .expect("listen response after asynchronous CONNECTED message");
    assert_eq!(listener.raw(), 0x4000_0002);

    let error = client
        .wait_connection(connection)
        .expect_err("buffered asynchronous connection failure");
    assert!(matches!(
        error,
        AppClientError::SessionConnectFailed {
            connection: actual,
            error: SessionConnectError::TimedOut,
        } if actual == connection
    ));
    drop(client);
    let _ = std::fs::remove_file(path);
}

#[test]
fn app_client_rejects_connected_message_with_mismatched_session_handle() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(41);
    let actual_handle = SessionHandle::new(41, 1);
    let wrong_handle = SessionHandle::new(42, 1);
    let published = build_publication(application, actual_handle);
    let mut publication = published.publication;
    let connection = ApplicationConnectionId::from_raw(0x4100_0001);
    publication.set_connected(SessionConnectedMsg::new(connection.raw(), Ok(wrong_handle)));
    spawn_serve(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
    );

    let client = AppClient::attach(&path_text).expect("attach client");
    publisher
        .try_publish(&publication)
        .expect("publish CONNECTED message");
    let error = client
        .wait_connection(connection)
        .expect_err("mismatched connected Session handle");
    assert!(matches!(
        error,
        AppClientError::SessionHandleMismatch {
            expected,
            actual,
        } if expected == wrong_handle && actual == actual_handle
    ));
    drop(client);
    let _ = std::fs::remove_file(path);
}

#[test]
fn app_client_accepts_accepted_session_and_replies_accepted_reply() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(42);
    let handle = SessionHandle::new(7, 1);
    let listener = SessionHandle::from(0x4000_0001);
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let published = build_publication(application, handle);
    let mut publication = published.publication;
    publication
        .set_accepted(SessionAcceptedMsg::new(
            application.raw(),
            listener,
            handle,
            SessionFlags::empty(),
        ))
        .expect("set ACCEPTED message");
    spawn_serve_with_control(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
        move |_, requests, _| {
            let item = requests
                .dequeue_control()
                .expect("dequeue Session control request")
                .expect("accepted reply request");
            let reply = item
                .decode::<SessionAcceptedReplyMsg>()
                .expect("decode accepted reply")
                .expect("decode accepted reply payload");
            accepted_tx.send(reply).expect("send accepted reply");
            Ok(())
        },
    );

    let client = AppClient::attach(&path_text).expect("attach client");
    publisher
        .try_publish(&publication)
        .expect("publish accepted session");
    let session = client
        .accept_accepted()
        .expect("accept accepted App Session");
    assert_eq!(session.session_handle(), handle);

    let reply = accepted_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("ACCEPTED_REPLY received by the service");
    assert_eq!(reply.context, application.raw());
    assert_eq!(reply.session, handle);
    assert!(reply.result.is_ok());
    drop(client);
    let _ = std::fs::remove_file(path);
}

fn assert_missing_attach_server_returns_client_error() {
    let path = socket_path();
    let result = AppClient::attach(path.to_str().expect("socket path"));
    assert!(matches!(result, Err(AppClientError::Attach { .. })));
}

#[test]
fn attach_rejects_old_protocol_version() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let application_mqs = build_application_mqs();
    spawn_serve(
        Arc::clone(&server),
        ApplicationId::from_raw(99),
        application_mqs,
    );

    let mut client = UnixStream::connect(path_text).expect("connect old attach client");
    client
        .write_all(&1_u64.to_le_bytes())
        .expect("write old protocol version");
    let mut reply = [0_u8; ATTACH_REPLY_BYTES];
    let result = client.read_exact(&mut reply);
    assert!(result.is_err(), "old protocol version must not be accepted");

    let _ = std::fs::remove_file(path);
}

#[test]
fn attach_reads_fragmented_application_mq_metadata() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind fragmented attach server");
    let application = ApplicationId::from_raw(100);
    let application_mqs = build_application_mqs();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept fragmented attach client");
        let _ = accept_application_with(
            &mut stream,
            application,
            &application_mqs,
            |stream, descriptors, words| {
                send_fds(stream, descriptors, &words[..APPLICATION_MQ_METADATA_WORDS]);
                std::thread::sleep(Duration::from_millis(100));
                for offset in &words[APPLICATION_MQ_METADATA_WORDS..] {
                    stream
                        .write_all(&offset.to_le_bytes())
                        .expect("write fragmented worker MQ offset");
                }
            },
        );
    });

    let client = AppClient::attach(path.to_str().expect("socket path"))
        .expect("attach with fragmented Application MQ metadata");
    assert_eq!(client.application(), application);

    server_thread.join().expect("join fragmented attach server");
    let _ = std::fs::remove_file(path);
}

#[test]
fn attach_rejects_application_mq_descriptor_mismatch_and_closes_received_fds() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind malformed attach server");
    let application = ApplicationId::from_raw(101);
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept malformed attach client");
        accept_registration(&mut stream, application);
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let expected = APPLICATION_MQ_BASE_DESCRIPTOR_COUNT + WORKER_COUNT;
        let descriptors = vec![sent.as_raw_fd(); expected - 1];
        send_fds(
            &stream,
            &descriptors,
            &[
                ATTACH_PROTOCOL_VERSION,
                4096,
                64,
                128,
                4096,
                WORKER_COUNT as u64,
                64,
                128,
                192,
            ],
        );
        (sent, peer, identity, baseline)
    });

    let error = match AppClient::attach(path.to_str().expect("socket path")) {
        Ok(_) => panic!("descriptor mismatch must reject Application attach"),
        Err(error) => error,
    };
    assert!(
        matches!(
            &error,
            AppClientError::DescriptorCount { expected, actual }
                if *expected == APPLICATION_MQ_BASE_DESCRIPTOR_COUNT + WORKER_COUNT
                    && *actual == *expected - 1
        ),
        "unexpected descriptor mismatch error: {error:?}"
    );
    let (sent, peer, identity, baseline) = server_thread.join().expect("join malformed server");
    assert_identity_count(identity, baseline);
    drop(sent);
    drop(peer);
    let _ = std::fs::remove_file(path);
}

#[test]
fn accepted_session_rejects_excess_descriptors_and_closes_received_fds() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind malformed attach server");
    let application = ApplicationId::from_raw(103);
    let application_mqs = build_application_mqs();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept truncated attach client");
        accept_application(&mut stream, application, &application_mqs);

        let pairs = (0..ATTACH_DESCRIPTOR_COUNT + 1)
            .map(|_| UnixStream::pair().expect("descriptor pair"))
            .collect::<Vec<_>>();
        let descriptors = pairs
            .iter()
            .map(|(sent, _)| sent.as_raw_fd())
            .collect::<Vec<_>>();
        send_fds(
            &stream,
            &descriptors,
            &[ATTACH_PROTOCOL_VERSION, 1, 4096, 64, 128, 192],
        );
        pairs.into_iter().map(|(_, peer)| peer).collect::<Vec<_>>()
    });

    let client = AppClient::attach(path.to_str().expect("socket path")).expect("attach client");
    let result = client.accept();
    assert!(
        matches!(
            result,
            Err(AppClientError::DescriptorCount {
                expected: ATTACH_DESCRIPTOR_COUNT,
                actual
            }) if actual == ATTACH_DESCRIPTOR_COUNT + 1
        ),
        "unexpected excess descriptor result: {result:?}"
    );

    for mut peer in server_thread.join().expect("join malformed server") {
        peer.set_nonblocking(true).expect("set peer nonblocking");
        let mut byte = [0_u8; 1];
        assert_eq!(peer.read(&mut byte).expect("received descriptor closed"), 0);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn accepted_session_rejects_worker_outside_exact_data_worker_count() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(102);
    let application_mqs = build_application_mqs();
    let published = build_publication_with_worker_queue(
        application,
        SessionHandle::new(33, WORKER_COUNT as u32),
        application_mqs.clone(),
        0,
    );
    publisher
        .try_publish(&published.publication)
        .expect("publish out-of-range worker session");
    spawn_serve(Arc::clone(&server), application, application_mqs);

    let segment_fd = published
        .session_segment
        .shared_fd()
        .expect("session segment descriptor");
    let identity = descriptor_identity(segment_fd).expect("segment identity");
    let baseline = count_open_identity(identity);
    let client = AppClient::attach(&path_text).expect("attach client");
    let result = client.accept();
    assert!(
        matches!(
            result,
            Err(AppClientError::WorkerQueueMissing {
                worker,
                worker_count
            }) if worker == WORKER_COUNT && worker_count == WORKER_COUNT
        ),
        "unexpected worker selection result: {result:?}"
    );
    assert_identity_count(identity, baseline);

    drop(client);
    let _ = std::fs::remove_file(path);
}

fn assert_malformed_attach_closes_received_descriptor_before_returning_error() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind malformed attach server");
    let application_mqs = build_application_mqs();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept malformed client");
        accept_application(&mut stream, ApplicationId::from_raw(6), &application_mqs);
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        send_fds(
            &stream,
            &[sent.as_raw_fd()],
            &[ATTACH_PROTOCOL_VERSION, 0, 0, 0, 0, 0],
        );
        drop(sent);
        peer
    });

    let result =
        AppClient::attach(path.to_str().expect("socket path")).and_then(|client| client.accept());
    assert!(
        matches!(
            result,
            Err(AppClientError::DescriptorCount {
                expected: ATTACH_DESCRIPTOR_COUNT,
                actual: 1
            })
        ),
        "unexpected malformed attach result: {result:?}"
    );
    let mut peer = server_thread.join().expect("join malformed server");
    peer.set_nonblocking(true).expect("set peer nonblocking");
    let mut byte = [0_u8; 1];
    assert_eq!(peer.read(&mut byte).expect("received descriptor closed"), 0);
    let _ = std::fs::remove_file(path);
}

fn assert_offset_overflow_closes_every_received_descriptor() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind offset overflow attach server");
    let application_mqs = build_application_mqs();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept offset overflow client");
        accept_application(&mut stream, ApplicationId::from_raw(7), &application_mqs);
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let fds = [sent.as_raw_fd(); ATTACH_DESCRIPTOR_COUNT];
        send_fds(
            &stream,
            &fds,
            &[ATTACH_PROTOCOL_VERSION, 0, 4096, 4096, 0, 0],
        );
        (sent, peer, identity, baseline)
    });

    let result =
        AppClient::attach(path.to_str().expect("socket path")).and_then(|client| client.accept());
    assert!(
        matches!(result, Err(AppClientError::OffsetOverflow)),
        "unexpected offset overflow result: {result:?}"
    );
    let (sent, peer, identity, baseline) = server_thread.join().expect("join offset server");
    assert_identity_count(identity, baseline);
    drop(sent);
    drop(peer);
    let _ = std::fs::remove_file(path);
}

fn assert_mapping_failure_closes_every_received_descriptor() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind mapping failure attach server");
    let application_mqs = build_application_mqs();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept mapping failure client");
        accept_application(&mut stream, ApplicationId::from_raw(8), &application_mqs);
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let fds = [sent.as_raw_fd(); ATTACH_DESCRIPTOR_COUNT];
        send_fds(&stream, &fds, &[ATTACH_PROTOCOL_VERSION, 0, 4096, 0, 0, 0]);
        (sent, peer, identity, baseline)
    });

    let result =
        AppClient::attach(path.to_str().expect("socket path")).and_then(|client| client.accept());
    assert!(
        matches!(result, Err(AppClientError::SessionSegmentMap { .. })),
        "unexpected mapping failure result: {result:?}"
    );
    let (sent, peer, identity, baseline) = server_thread.join().expect("join mapping server");
    assert_identity_count(identity, baseline);
    drop(sent);
    drop(peer);
    let _ = std::fs::remove_file(path);
}

#[test]
fn attach_protocol_round_trips_and_releases_descriptors() {
    assert_publish_then_connect_round_trips_handle_and_descriptors();
    assert_connect_before_publish_completes_after_publication();
    assert_failed_attach_requeues_publication_for_next_client();
    assert_publication_queue_reports_full_and_closed();
    assert_missing_attach_server_returns_client_error();
    assert_malformed_attach_closes_received_descriptor_before_returning_error();
    assert_offset_overflow_closes_every_received_descriptor();
    assert_mapping_failure_closes_every_received_descriptor();
}

#[test]
fn attach_maps_each_worker_to_its_application_rx_mq() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(50);
    let published = build_publication(application, SessionHandle::new(1, 0));
    spawn_serve(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
    );

    let client = AppClient::attach(&path_text).expect("attach client");
    let first = build_publication_with_mqs(
        application,
        SessionHandle::new(11, 0),
        published.application_mqs.clone(),
    );
    let second = build_publication_with_mqs(
        application,
        SessionHandle::new(22, 2),
        published.application_mqs.clone(),
    );
    publisher
        .try_publish(&first.publication)
        .expect("publish worker 0 session");
    publisher
        .try_publish(&second.publication)
        .expect("publish worker 2 session");

    let first_session = client.accept().expect("accept worker 0 session");
    assert_eq!(first_session.send_bytes(b"a").expect("send"), 1);
    let first_event = published.worker_queues[0]
        .dequeue()
        .expect("worker 0 dequeue")
        .expect("worker 0 event");
    assert_eq!(first_event.session_index(), 11);

    let second_session = client.accept().expect("accept worker 2 session");
    assert_eq!(second_session.send_bytes(b"b").expect("send"), 1);
    let second_event = published.worker_queues[2]
        .dequeue()
        .expect("worker 2 dequeue")
        .expect("worker 2 event");
    assert_eq!(second_event.session_index(), 22);
    assert!(
        published.worker_queues[1]
            .dequeue()
            .expect("worker 1 dequeue")
            .is_none()
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn attach_connection_close_detaches_only_its_application_once() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let (detached_tx, detached_rx) = std::sync::mpsc::channel();
    let application_mqs = build_application_mqs();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("serve runtime");
        let next_application = Arc::new(AtomicU64::new(41));
        let attach_sequence = Arc::clone(&next_application);
        let _ = runtime.block_on(server.serve(
            move || {
                Ok::<ApplicationId, Infallible>(ApplicationId::from_raw(
                    attach_sequence.fetch_add(1, Ordering::Relaxed),
                ))
            },
            move |_| {
                Ok::<ApplicationMqPublication, Infallible>(application_mqs.publication.clone())
            },
            |_, _, _| Ok(()),
            move |application| {
                let _ = detached_tx.send(application);
            },
        ));
    });

    let first = ApplicationId::from_raw(41);
    let second = ApplicationId::from_raw(42);
    let first_client = AppClient::attach(&path_text).expect("attach first Application");
    let second_client = AppClient::attach(&path_text).expect("attach second Application");
    assert_eq!(first_client.application(), first);
    assert_eq!(second_client.application(), second);

    drop(first_client);
    assert_eq!(
        detached_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first Application detach"),
        first
    );
    assert!(detached_rx.try_recv().is_err());

    drop(second_client);
    assert_eq!(
        detached_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second Application detach"),
        second
    );
    assert!(detached_rx.try_recv().is_err());
    let _ = std::fs::remove_file(path);
}

#[test]
fn connect_carries_server_name_in_bounded_ext_config_chunk() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(50);
    let handle = SessionHandle::new(7, 1);
    let published = build_publication(application, handle);
    let mut publication = published.publication;
    let connection = ApplicationConnectionId::from_raw(0x5000_0001);
    publication.set_connected(SessionConnectedMsg::new(connection.raw(), Ok(handle)));
    let daemon_store = published.application_mqs.ext_config_store();
    let (observed_tx, observed_rx) = std::sync::mpsc::channel::<()>();
    spawn_serve_with_control(
        Arc::clone(&server),
        application,
        published.application_mqs.clone(),
        move |_, requests, _| {
            let item = requests
                .dequeue_control()
                .expect("dequeue Session control request")
                .expect("connect request");
            let request = item
                .decode::<SessionConnectMsg>()
                .expect("decode connect request")
                .expect("decode connect payload");
            let offset = request.ext_config.expect("bounded ext-config reference");
            assert_eq!(
                daemon_store.read(offset).expect("read ext-config chunk"),
                b"example.com"
            );
            daemon_store
                .free(offset)
                .expect("free ext-config chunk exactly once");
            assert!(
                daemon_store.read(offset).is_err(),
                "ext-config chunk must be freed after the daemon read it"
            );
            let _ = observed_tx.send(());
            Ok(())
        },
    );

    let mut client = AppClient::attach(&path_text).expect("attach client");
    publisher
        .try_publish(&publication)
        .expect("publish CONNECTED message");
    let _ = client
        .connect(
            TransportProtocol::Tcp,
            "127.0.0.1:0".parse().expect("remote endpoint"),
            None,
            None,
            None,
            Some("example.com"),
        )
        .expect("connect with server name");
    // The published CONNECTED message carries the test's connection identity,
    // not the client-local context.
    let session = client
        .wait_connection(connection)
        .expect("connected session");
    assert_eq!(session.session_handle(), handle);
    observed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("serve callback must report before the client drops");
    drop(client);
    let _ = std::fs::remove_file(path);
}

#[test]
fn connect_rejects_oversized_server_name_before_enqueue() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let application = ApplicationId::from_raw(51);
    let application_mqs = build_application_mqs();
    let daemon_store = application_mqs.ext_config_store();
    let (observed_tx, observed_rx) = std::sync::mpsc::channel::<()>();
    spawn_serve_with_control(
        Arc::clone(&server),
        application,
        application_mqs,
        move |_, requests, _| {
            let item = requests
                .dequeue_control()
                .expect("dequeue Session control request")
                .expect("connect request");
            let request = item
                .decode::<SessionConnectMsg>()
                .expect("decode connect request")
                .expect("decode connect payload");
            let offset = request.ext_config.expect("bounded ext-config reference");
            assert_eq!(
                daemon_store.read(offset).expect("read ext-config chunk"),
                b"ok.example"
            );
            daemon_store.free(offset).expect("free ext-config chunk");
            drop(item);
            assert!(
                requests.dequeue_control().expect("dequeue").is_none(),
                "oversized server_name must not be enqueued"
            );
            let _ = observed_tx.send(());
            Ok(())
        },
    );

    let mut client = AppClient::attach(&path_text).expect("attach client");
    let error = client
        .connect(
            TransportProtocol::Tcp,
            "127.0.0.1:0".parse().expect("remote endpoint"),
            None,
            None,
            None,
            Some(&"x".repeat(EXT_CONFIG_CHUNK_BYTES + 1)),
        )
        .expect_err("oversized server_name must be rejected before enqueue");
    assert!(
        matches!(error, AppClientError::ExtConfig { .. }),
        "unexpected oversized server_name error: {error:?}"
    );
    // The client stays usable and the next valid name is delivered intact.
    client
        .connect(
            TransportProtocol::Tcp,
            "127.0.0.1:0".parse().expect("remote endpoint"),
            None,
            None,
            None,
            Some("ok.example"),
        )
        .expect("valid connect after oversized rejection");
    observed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("serve callback must report before the client drops");
    drop(client);
    let _ = std::fs::remove_file(path);
}

#[test]
fn connect_frees_ext_config_chunk_when_enqueue_fails() {
    let path = socket_path();
    let listener = UnixListener::bind(&path).expect("bind queue-full attach server");
    let application = ApplicationId::from_raw(52);
    let application_mqs = build_application_mqs();
    let daemon_store = application_mqs.ext_config_store();
    let (hold_tx, hold_rx) = std::sync::mpsc::channel::<()>();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept queue-full attach client");
        let (_requests, _replies) = accept_application_with(
            &mut stream,
            application,
            &application_mqs,
            |stream, fds, words| {
                send_fds(stream, fds, words);
            },
        );
        // Hold the attach stream and the control queues (keeping their signal
        // read ends open); the fake daemon never drains the request queue, so
        // it fills and the next enqueue must fail.
        let _ = hold_rx.recv();
    });

    let mut client = AppClient::attach(path.to_str().expect("socket path")).expect("attach client");
    // The fake daemon's Ctrl ring holds 8 fixed control slots, so connects
    // 1..=8 enqueue and the 9th hits ControlFull.
    for name in 0..8 {
        client
            .connect(
                TransportProtocol::Tcp,
                "127.0.0.1:0".parse().expect("remote endpoint"),
                None,
                None,
                None,
                Some(&format!("name-{name}")),
            )
            .expect("connect while the request queue has capacity");
    }
    let error = client
        .connect(
            TransportProtocol::Tcp,
            "127.0.0.1:0".parse().expect("remote endpoint"),
            None,
            None,
            None,
            Some("overflow"),
        )
        .expect_err("connect past the request queue capacity");
    assert!(
        matches!(
            &error,
            AppClientError::SessionControl {
                source: SessionMsgQueueError::ControlFull
            }
        ),
        "unexpected queue-full connect error: {error:?}"
    );
    // The failed enqueue's chunk must have been returned to the free list:
    // the client holds chunks 0..=7 (still queued), chunk 8 was allocated for
    // the failed request and freed again, so all remaining 24 chunks are
    // allocatable by the daemon. A leaked chunk would make the last probe
    // fail with ExtConfigExhausted.
    for probe in 0..EXT_CONFIG_CHUNK_COUNT - 8 {
        daemon_store
            .alloc(format!("probe-{probe}").as_bytes())
            .expect("freed ext-config chunk must be reusable");
    }
    drop(hold_tx);
    server_thread.join().expect("join queue-full server");
    let _ = std::fs::remove_file(path);
}

#[test]
fn app_client_stream_connect_publishes_parent_and_flags_then_polls_nonblocking() {
    let path = socket_path();
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let application = ApplicationId::from_raw(50);
    let parent = SessionHandle::new(9, 1);
    let child = SessionHandle::new(10, 1);
    let flags = SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL;
    let (observed_tx, observed_rx) =
        std::sync::mpsc::channel::<(Option<SessionHandle>, SessionFlags)>();
    spawn_serve_with_control(
        Arc::clone(&server),
        application,
        build_application_mqs(),
        move |_, requests, replies| {
            while let Some(item) = requests
                .dequeue_control()
                .expect("dequeue Session control request")
            {
                match item.event_type() {
                    SessionEvtType::ConnectStream => {
                        let request = item
                            .decode::<SessionConnectMsg>()
                            .expect("decode CONNECT_STREAM")
                            .expect("decode CONNECT_STREAM payload");
                        assert_eq!(request.parent_handle, Some(parent));
                        assert_eq!(request.flags, flags);
                        let _ = observed_tx.send((request.parent_handle, request.flags));
                        replies
                            .enqueue_control(&SessionConnectedMsg {
                                context: request.context,
                                result: Ok(child),
                                local: None,
                                remote: None,
                                flags,
                                opaque: None,
                            })
                            .expect("enqueue CONNECTED message");
                    }
                    SessionEvtType::Listen => {
                        let request = item
                            .decode::<SessionListenMsg>()
                            .expect("decode LISTEN")
                            .expect("decode LISTEN payload");
                        replies
                            .enqueue_control(&SessionBoundMsg {
                                context: request.context,
                                result: Ok(SessionHandle::from(0x5000_0002)),
                                local: None,
                                opaque: None,
                            })
                            .expect("enqueue BOUND message");
                    }
                    event => panic!("unexpected Session control request {event:?}"),
                }
            }
            Ok(())
        },
    );

    let mut client = AppClient::attach(&path_text).expect("attach client");
    let connection = client
        .connect_stream(
            0x5000_0001,
            TransportProtocol::Tcp,
            "127.0.0.1:9000".parse().expect("remote endpoint"),
            None,
            None,
            parent,
            flags,
        )
        .expect("enqueue CONNECT_STREAM");
    assert_eq!(connection.raw(), 0x5000_0001);

    let poll_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let reply = loop {
        if let Some(reply) = client.poll_control().expect("poll Session control") {
            break reply;
        }
        assert!(
            std::time::Instant::now() < poll_deadline,
            "timed out waiting for the CONNECTED control reply"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    let ControlReply::Connected(connected) = reply else {
        panic!("expected CONNECTED control reply");
    };
    assert_eq!(connected.context, connection.raw());
    assert_eq!(connected.result, Ok(child));
    assert_eq!(connected.flags, flags);
    let (observed_parent, observed_flags) = observed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("serve callback must report CONNECT_STREAM observations");
    assert_eq!(observed_parent, Some(parent));
    assert_eq!(observed_flags, flags);

    // The nonblocking poll drained the client's single inbox: a second poll
    // returns immediately and finds nothing (it never blocks or spins).
    assert!(client.poll_control().expect("poll empty inbox").is_none());

    // Blocking paths reuse the same inbox and wait: listen() still completes
    // after the nonblocking poll consumed the CONNECTED message.
    let listener = client
        .listen(
            TransportProtocol::Tcp,
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("listen endpoint"),
                hammer_runtime::DataWorkerId::new(0),
            ),
            None,
            None,
        )
        .expect("blocking listen after nonblocking poll");
    assert_eq!(listener.raw(), 0x5000_0002);
    drop(client);
    let _ = std::fs::remove_file(path);
}
