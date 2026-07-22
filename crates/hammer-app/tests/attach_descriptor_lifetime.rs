use std::io::Read;
use std::net::Shutdown;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_app::attach::{AttachClient, AttachClientError};
use hammer_app::remote_session::{RemoteAppSession, RemoteAppSessionError};
use hammer_infra::segment::{Segment, Svm};
use hammer_runtime::app::{AppSessionConfig, SessionEventQueue, SessionHandle};
use hammer_runtime::attach::{AttachServer, AttachedApp};
use hammer_runtime::{AttachError, RuntimeError};

static SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn socket_path(name: &str) -> PathBuf {
    let counter = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
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

fn count_open_descriptors() -> usize {
    std::fs::read_dir("/dev/fd")
        .expect("read /dev/fd")
        .filter_map(Result::ok)
        .count()
}

struct DescriptorBaseline {
    identity: (libc::dev_t, libc::ino_t),
    count: usize,
}

fn attach_pair(name: &str) -> (AttachClient, AttachedApp<Svm>, PathBuf, DescriptorBaseline) {
    let path = socket_path(name);
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = AttachServer::bind(&path_text).expect("bind attach server");
    let segment_id = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let segment = Svm::create(
        &format!("ha{}-{segment_id}", std::process::id()),
        16 * 1024 * 1024,
    )
    .expect("create SVM segment");
    let segment_fd = segment.fd().expect("SVM backing descriptor");
    let identity = descriptor_identity(segment_fd).expect("SVM descriptor identity");
    let baseline = DescriptorBaseline {
        identity,
        count: count_open_identity(identity),
    };
    let handle = SessionHandle::new(1, 0);
    let server_thread =
        std::thread::spawn(move || server.accept(AppSessionConfig::new(256, 16), &segment, handle));

    let client = AttachClient::connect(&path_text, handle).expect("attach client");
    let attached = server_thread
        .join()
        .expect("join attach server")
        .expect("accept attach client");
    (client, attached, path, baseline)
}

fn send_fds(stream: &UnixStream, fds: &[RawFd], offsets: [u64; 4]) {
    let iov = libc::iovec {
        iov_base: offsets.as_ptr().cast_mut().cast::<libc::c_void>(),
        iov_len: std::mem::size_of_val(&offsets),
    };
    let mut control = [0_u8; 64];
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
        assert_eq!(libc::sendmsg(stream.as_raw_fd(), &message, 0), 32);
    }
}

fn assert_attach_server_releases_sender_side_signal_endpoint_after_transfer() {
    let (client, attached, path, _) = attach_pair("sender-signal-owner");
    drop(client);

    let signal_read = attached
        .session
        .tx_evt_q()
        .read_fd()
        .expect("dataplane queue signal-read endpoint");
    let mut byte = 0_u8;
    // SAFETY: byte is writable for one byte and the attached session owns the
    // live nonblocking signal-read endpoint.
    let read = unsafe {
        libc::read(
            signal_read,
            std::ptr::from_mut(&mut byte).cast::<libc::c_void>(),
            1,
        )
    };

    assert_eq!(read, 0, "sender retained an extra signal-write endpoint");
    drop(attached);
    let _ = std::fs::remove_file(path);
}

fn assert_attach_server_failure_releases_created_descriptors() {
    let baseline = count_open_descriptors();
    let path = socket_path("sender-failure-owner");
    let path_text = path.to_str().expect("socket path").to_owned();
    let server = AttachServer::bind(&path_text).expect("bind attach server");
    let segment = Svm::create(&format!("hf{}", std::process::id()), 16 * 1024 * 1024)
        .expect("create SVM segment");
    let client = UnixStream::connect(&path).expect("connect attach client");
    client
        .shutdown(Shutdown::Both)
        .expect("shutdown attach client");
    drop(client);

    let result = server.accept(
        AppSessionConfig::new(256, 16),
        &segment,
        SessionHandle::new(1, 0),
    );
    assert!(matches!(
        result,
        Err(RuntimeError::Attach(AttachError::Send { .. }))
    ));

    drop(server);
    drop(segment);
    let _ = std::fs::remove_file(path);
    assert_eq!(count_open_descriptors(), baseline);
}

fn assert_attach_client_drop_releases_received_svm_backing_descriptor() {
    let (client, attached, path, baseline) = attach_pair("client-svm-owner");
    assert_eq!(count_open_identity(baseline.identity), baseline.count + 1);

    drop(client);

    assert_eq!(count_open_identity(baseline.identity), baseline.count);
    drop(attached);
    let _ = std::fs::remove_file(path);
}

fn assert_missing_attach_server_returns_client_error() {
    let path = socket_path("missing-server");
    let result = AttachClient::connect(
        path.to_str().expect("socket path"),
        SessionHandle::new(1, 0),
    );
    assert!(matches!(result, Err(AttachClientError::Connect { .. })));
}

fn assert_malformed_attach_closes_received_descriptor_before_returning_error() {
    let path = socket_path("malformed-owner");
    let listener = UnixListener::bind(&path).expect("bind malformed attach server");
    let server_thread = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept malformed client");
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        send_fds(&stream, &[sent.as_raw_fd()], [0; 4]);
        drop(sent);
        peer
    });

    let result = AttachClient::connect(
        path.to_str().expect("socket path"),
        SessionHandle::new(1, 0),
    );
    assert!(matches!(
        result,
        Err(AttachClientError::DescriptorCount {
            expected: 3,
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
    let path = socket_path("offset-overflow-owner");
    let listener = UnixListener::bind(&path).expect("bind offset overflow attach server");
    let server_thread = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept offset overflow client");
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let fds = [sent.as_raw_fd(); 3];
        send_fds(&stream, &fds, [0, 0, 0, u64::MAX]);
        (sent, peer, identity, baseline)
    });

    let result = AttachClient::connect(
        path.to_str().expect("socket path"),
        SessionHandle::new(1, 0),
    );
    assert!(matches!(result, Err(AttachClientError::OffsetOverflow)));
    let (sent, peer, identity, baseline) = server_thread.join().expect("join offset server");
    assert_eq!(count_open_identity(identity), baseline);
    drop(sent);
    drop(peer);
    let _ = std::fs::remove_file(path);
}

fn assert_mapping_failure_closes_every_received_descriptor() {
    let path = socket_path("mapping-failure-owner");
    let listener = UnixListener::bind(&path).expect("bind mapping failure attach server");
    let server_thread = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept mapping failure client");
        let (sent, peer) = UnixStream::pair().expect("descriptor pair");
        let identity = descriptor_identity(sent.as_raw_fd()).expect("descriptor identity");
        let baseline = count_open_identity(identity);
        let fds = [sent.as_raw_fd(); 3];
        send_fds(&stream, &fds, [0; 4]);
        (sent, peer, identity, baseline)
    });

    let result = AttachClient::connect(
        path.to_str().expect("socket path"),
        SessionHandle::new(1, 0),
    );
    assert!(matches!(result, Err(AttachClientError::SegmentMap { .. })));
    let (sent, peer, identity, baseline) = server_thread.join().expect("join mapping server");
    assert_eq!(count_open_identity(identity), baseline);
    drop(sent);
    drop(peer);
    let _ = std::fs::remove_file(path);
}

fn assert_remote_session_requires_read_signal() {
    let (client, attached, path, _) = attach_pair("remote-session-missing-signal");
    let session = Arc::new(attached.session);
    let result = RemoteAppSession::new(Arc::clone(&session));
    assert!(matches!(
        result,
        Err(RemoteAppSessionError::SessionSignalMissing)
    ));

    drop(session);
    drop(client);
    let _ = std::fs::remove_file(path);
}

fn assert_remote_session_duplicate_is_cloexec_and_drops_independently() {
    let (client, attached, path, _) = attach_pair("remote-session-owner");
    let session = Arc::new(hammer_app::AppSession::from_parts(
        Arc::clone(client.session.rx_fifo()),
        Arc::clone(client.session.tx_fifo()),
        Arc::clone(client.session.evt_q()),
        Arc::clone(client.session.tx_evt_q()),
        client.session.session_handle(),
    ));
    let queue_fd = session
        .evt_q()
        .read_fd()
        .expect("app queue signal-read endpoint");
    let queue_identity = descriptor_identity(queue_fd).expect("queue endpoint identity");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime");
    let runtime_guard = runtime.enter();
    let remote = RemoteAppSession::new(Arc::clone(&session)).expect("remote session");
    drop(runtime_guard);

    // SAFETY: fcntl only queries the live descriptor owned by RemoteAppSession.
    let status_flags = unsafe { libc::fcntl(remote.as_raw_fd(), libc::F_GETFL) };
    // SAFETY: as above, F_GETFD only queries descriptor flags.
    let descriptor_flags = unsafe { libc::fcntl(remote.as_raw_fd(), libc::F_GETFD) };
    assert_ne!(status_flags & libc::O_NONBLOCK, 0);
    assert_ne!(descriptor_flags & libc::FD_CLOEXEC, 0);

    drop(remote);
    assert_eq!(
        descriptor_identity(queue_fd).expect("queue endpoint remains open"),
        queue_identity
    );
    drop(session);
    drop(client);
    drop(attached);
    let _ = std::fs::remove_file(path);
}

#[test]
fn attach_descriptor_lifetimes_follow_raii_ownership() {
    assert_attach_client_drop_releases_received_svm_backing_descriptor();
    assert_attach_server_releases_sender_side_signal_endpoint_after_transfer();
    assert_attach_server_failure_releases_created_descriptors();
    assert_missing_attach_server_returns_client_error();
    assert_malformed_attach_closes_received_descriptor_before_returning_error();
    assert_offset_overflow_closes_every_received_descriptor();
    assert_mapping_failure_closes_every_received_descriptor();
    assert_remote_session_requires_read_signal();
    assert_remote_session_duplicate_is_cloexec_and_drops_independently();
}
