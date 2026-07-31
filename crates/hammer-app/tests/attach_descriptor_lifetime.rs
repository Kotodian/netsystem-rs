//! Attach-protocol integration tests: server publication pairing, client
//! reconstruction from the four transferred descriptors, and descriptor
//! lifetime on every failure path.

use std::convert::Infallible;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hammer_app::attach::{AppClient, AppClientError};
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, ApplicationId, SessionEventQueue, SessionEvtType, SessionHandle, SessionMsgQueue,
    SessionOffsets,
};
use hammer_runtime::attach::{
    ATTACH_PROTOCOL_VERSION, ATTACH_REPLY_BYTES, ATTACH_REQUEST_BYTES, ATTACH_STATUS_ACCEPTED,
};
use hammer_runtime::attach::{AppServer, AppSessionPublication};
use hammer_runtime::{AttachError, RuntimeError};

const FIFO_CAPACITY: usize = 4096;
const EVT_Q_CAPACITY: usize = 16;

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn socket_path(name: &str) -> PathBuf {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hammer-{name}-{}-{counter}.sock",
        std::process::id()
    ))
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
}

/// Build a server-side session whose FIFOs and event queues live at known
/// offsets in shared segments, matching the daemon's listener layout.
fn build_publication(application: ApplicationId, handle: SessionHandle) -> PublishedSession {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let session_segment =
        Segment::shared(&format!("hs{}-{counter}", std::process::id()), 1024 * 1024)
            .expect("session segment");
    let tx_event_segment =
        Segment::shared(&format!("ht{}-{counter}", std::process::id()), 1024 * 1024)
            .expect("tx event segment");

    let ring_nitems = EVT_Q_CAPACITY as u32;
    let q_nitems = (EVT_Q_CAPACITY + 1).next_power_of_two() as u32;
    let queue_bytes = SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).expect("queue layout");
    let tx_evt_q_off = tx_event_segment
        .alloc(queue_bytes, 64)
        .expect("tx queue offset");
    // SAFETY: the offset was just allocated with the queue layout size.
    let tx_evt_q = Arc::new(
        unsafe {
            SessionMsgQueue::init_at_with_signal(
                tx_event_segment.clone(),
                tx_evt_q_off,
                q_nitems,
                ring_nitems,
            )
        }
        .expect("tx event queue"),
    );

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

    let session = Arc::new(AppSession::from_parts(
        Arc::new(rx_fifo),
        Arc::new(tx_fifo),
        evt_q,
        tx_evt_q,
        handle,
    ));
    let offsets = SessionOffsets {
        rx_fifo_off,
        tx_fifo_off,
        evt_q_off,
        tx_evt_q_off,
    };
    let publication = AppSessionPublication::new(
        Arc::clone(&session),
        application,
        session_segment.clone(),
        tx_event_segment,
        offsets,
    )
    .expect("session publication");
    PublishedSession {
        session,
        publication,
        session_segment,
    }
}

fn spawn_serve(server: Arc<AppServer>, first_application: ApplicationId) {
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
            |_, _, _| Ok(()),
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
    let mut control = [0_u8; 128];
    // SAFETY: zero is a valid initial value for every msghdr field.
    let mut message = unsafe { std::mem::zeroed::<libc::msghdr>() };
    message.msg_iov = std::ptr::from_ref(&iov).cast_mut();
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
    message.msg_controllen = control.len() as _;

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
        message.msg_controllen = (*header).cmsg_len;
        assert_eq!(libc::sendmsg(stream.as_raw_fd(), &message, 0), 64);
    }
}

fn accept_application(stream: &mut UnixStream, application: ApplicationId) {
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

    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let segment = Segment::shared(&format!("hc{}-{counter}", std::process::id()), 1024 * 1024)
        .expect("Application control segment");
    let queue_bytes = SessionMsgQueue::layout_bytes(16, 8).expect("control queue layout");
    let request_offset = segment
        .alloc(queue_bytes, 64)
        .expect("request queue offset");
    let reply_offset = segment.alloc(queue_bytes, 64).expect("reply queue offset");
    let requests = unsafe {
        SessionMsgQueue::init_at_with_signal(segment.clone(), request_offset, 16, 8)
            .expect("request queue")
    };
    let replies = unsafe {
        SessionMsgQueue::init_at_with_signal(segment.clone(), reply_offset, 16, 8)
            .expect("reply queue")
    };
    send_fds(
        stream,
        &[
            segment.shared_fd().expect("control segment descriptor"),
            requests.write_fd().expect("request signal"),
            replies.read_fd().expect("reply signal"),
        ],
        &[
            ATTACH_PROTOCOL_VERSION,
            segment.size() as u64,
            request_offset,
            reply_offset,
        ],
    );
}

fn assert_publish_then_connect_round_trips_handle_and_descriptors() {
    let path = socket_path("publish-first");
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(1);
    let handle = SessionHandle::new(5, 2);
    let published = build_publication(application, handle);
    publisher
        .try_publish(&published.publication)
        .expect("publish before client");
    spawn_serve(Arc::clone(&server), application);

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

    // App -> dataplane across the tx fifo and worker tx event queue.
    assert_eq!(client_session.send_bytes(b"pong").expect("client send"), 4);
    let mut echoed = [0_u8; 16];
    let read = published
        .session
        .tx_fifo()
        .peek(0, echoed.len(), &mut echoed);
    assert_eq!(&echoed[..read], b"pong");
    let event = published.session.tx_evt_q().dequeue().expect("tx event");
    assert_eq!(event.session_index(), handle.session_index());
    assert_eq!(event.evt_type, SessionEvtType::TxEnq);

    drop(client_session);
    drop(client);
    assert_identity_count(identity, baseline);
    let _ = std::fs::remove_file(path);
}

fn assert_connect_before_publish_completes_after_publication() {
    let path = socket_path("client-first");
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(2);
    spawn_serve(Arc::clone(&server), application);

    let client_path = path_text.clone();
    let client_thread = std::thread::spawn(move || {
        let client = AppClient::attach(&client_path)?;
        assert_eq!(client.application(), application);
        client.accept()
    });
    std::thread::sleep(Duration::from_millis(100));

    let handle = SessionHandle::new(9, 0);
    let published = build_publication(application, handle);
    publisher
        .try_publish(&published.publication)
        .expect("publish after client");

    let client = client_thread
        .join()
        .expect("join client thread")
        .expect("attach client");
    assert_eq!(client.session_handle(), handle);
    let _ = std::fs::remove_file(path);
}

fn assert_failed_attach_requeues_publication_for_next_client() {
    let path = socket_path("requeue");
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let publisher = server.publisher();
    let application = ApplicationId::from_raw(3);
    spawn_serve(Arc::clone(&server), application);

    let dead = UnixStream::connect(&path).expect("connect doomed client");
    dead.shutdown(std::net::Shutdown::Both)
        .expect("shutdown doomed client");
    drop(dead);
    std::thread::sleep(Duration::from_millis(100));

    let handle = SessionHandle::new(3, 1);
    let published = build_publication(application, handle);
    publisher
        .try_publish(&published.publication)
        .expect("publish after doomed client");

    let client = AppClient::attach(&path_text).expect("attach surviving client");
    assert_eq!(client.application(), application);
    let client_session = client.accept().expect("accept surviving App Session");
    assert_eq!(client_session.session_handle(), handle);
    let _ = std::fs::remove_file(path);
}

fn assert_publication_queue_reports_full_and_closed() {
    let path = socket_path("queue-limits");
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

fn assert_missing_attach_server_returns_client_error() {
    let path = socket_path("missing-server");
    let result = AppClient::attach(path.to_str().expect("socket path"));
    assert!(matches!(result, Err(AppClientError::Attach { .. })));
}

fn assert_malformed_attach_closes_received_descriptor_before_returning_error() {
    let path = socket_path("malformed");
    let listener = UnixListener::bind(&path).expect("bind malformed attach server");
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept malformed client");
        accept_application(&mut stream, ApplicationId::from_raw(6));
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        send_fds(&stream, &[sent.as_raw_fd()], &[0; 8]);
        drop(sent);
        peer
    });

    let result =
        AppClient::attach(path.to_str().expect("socket path")).and_then(|client| client.accept());
    assert!(matches!(
        result,
        Err(AppClientError::DescriptorCount {
            expected: 4,
            actual: 1
        })
    ));
    let mut peer = server_thread.join().expect("join malformed server");
    peer.set_nonblocking(true).expect("set peer nonblocking");
    let mut byte = [0_u8; 1];
    assert_eq!(peer.read(&mut byte).expect("received descriptor closed"), 0);
    let _ = std::fs::remove_file(path);
}

fn assert_offset_overflow_closes_every_received_descriptor() {
    let path = socket_path("offset-overflow");
    let listener = UnixListener::bind(&path).expect("bind offset overflow attach server");
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept offset overflow client");
        accept_application(&mut stream, ApplicationId::from_raw(7));
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let fds = [sent.as_raw_fd(); 4];
        send_fds(&stream, &fds, &[1, 0, 4096, 4096, 4096, 0, 0, 0]);
        (sent, peer, identity, baseline)
    });

    let result =
        AppClient::attach(path.to_str().expect("socket path")).and_then(|client| client.accept());
    assert!(matches!(result, Err(AppClientError::OffsetOverflow)));
    let (sent, peer, identity, baseline) = server_thread.join().expect("join offset server");
    assert_identity_count(identity, baseline);
    drop(sent);
    drop(peer);
    let _ = std::fs::remove_file(path);
}

fn assert_mapping_failure_closes_every_received_descriptor() {
    let path = socket_path("mapping-failure");
    let listener = UnixListener::bind(&path).expect("bind mapping failure attach server");
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept mapping failure client");
        accept_application(&mut stream, ApplicationId::from_raw(8));
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let fds = [sent.as_raw_fd(); 4];
        send_fds(&stream, &fds, &[1, 0, 4096, 4096, 0, 0, 0, 0]);
        (sent, peer, identity, baseline)
    });

    let result =
        AppClient::attach(path.to_str().expect("socket path")).and_then(|client| client.accept());
    assert!(matches!(
        result,
        Err(AppClientError::SessionSegmentMap { .. })
    ));
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
fn attach_connection_close_detaches_only_its_application_once() {
    let path = socket_path("application-lifetime");
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = Arc::new(AppServer::bind(&path_text, 4).expect("bind app server"));
    let (detached_tx, detached_rx) = std::sync::mpsc::channel();
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
