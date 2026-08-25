use std::fs::OpenOptions;
use std::mem::size_of;
use std::os::fd::AsRawFd;
#[cfg(target_os = "macos")]
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::ptr;
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use hammer_ipc::StatsClient;
use hammer_stats::{MetricValue, StatsError};
use socket2::{Domain, SockAddr, Socket, Type};

#[test]
fn connect_reports_a_typed_error_for_a_missing_socket() {
    let path = PathBuf::from(format!(
        "/tmp/hammer-stats-client-missing-{}",
        std::process::id()
    ));

    let result = StatsClient::connect(path);

    assert!(result.is_err());
}

#[test]
fn connect_rejects_multiple_received_fds_and_closes_them() {
    let sequence = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let socket_path =
        std::env::temp_dir().join(format!("hsc-mf-{}-{sequence}.sock", std::process::id()));
    let (ready_sender, ready_receiver) = mpsc::channel();
    let thread_socket_path = socket_path.clone();
    let server = thread::spawn(move || {
        #[cfg(target_os = "linux")]
        let listener = Socket::new(Domain::UNIX, Type::SEQPACKET, None);
        #[cfg(not(target_os = "linux"))]
        let listener = Socket::new(Domain::UNIX, Type::STREAM, None);
        let listener = listener.expect("create malformed ancillary listener");
        let address = SockAddr::unix(&thread_socket_path).expect("malformed ancillary address");
        listener
            .bind(&address)
            .expect("bind malformed ancillary listener");
        listener
            .listen(1)
            .expect("listen malformed ancillary listener");
        ready_sender
            .send(())
            .expect("signal malformed ancillary readiness");

        let (client, _) = listener
            .accept()
            .expect("accept malformed ancillary client");
        let first = OpenOptions::new()
            .read(true)
            .open("/dev/null")
            .expect("open first ancillary fd");
        let second = OpenOptions::new()
            .read(true)
            .open("/dev/null")
            .expect("open second ancillary fd");
        let control_size = unsafe { libc::CMSG_SPACE((size_of::<i32>() * 2) as u32) as usize };
        let mut control = vec![0_u8; control_size];
        #[cfg(not(target_os = "linux"))]
        let handoff = [1_u8];
        #[cfg(not(target_os = "linux"))]
        let mut iovec = libc::iovec {
            iov_base: handoff.as_ptr().cast_mut().cast(),
            iov_len: handoff.len(),
        };
        #[cfg(target_os = "linux")]
        let (iov_base, iov_len) = (ptr::null_mut(), 0);
        #[cfg(not(target_os = "linux"))]
        let (iov_base, iov_len) = (&mut iovec as *mut libc::iovec, 1);
        let message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iov_base,
            msg_iovlen: iov_len,
            msg_control: control.as_mut_ptr().cast(),
            msg_controllen: control.len().try_into().expect("ancillary length"),
            msg_flags: 0,
        };
        // SAFETY: `message` owns the live control buffer and both source fds
        // remain open until `sendmsg` returns.
        unsafe {
            let control_message = libc::CMSG_FIRSTHDR(&message);
            assert!(!control_message.is_null());
            (*control_message).cmsg_level = libc::SOL_SOCKET;
            (*control_message).cmsg_type = libc::SCM_RIGHTS;
            (*control_message).cmsg_len = libc::CMSG_LEN((size_of::<i32>() * 2) as u32) as _;
            let data = libc::CMSG_DATA(control_message).cast::<i32>();
            ptr::write_unaligned(data, first.as_raw_fd());
            ptr::write_unaligned(data.add(1), second.as_raw_fd());
            assert!(libc::sendmsg(client.as_raw_fd(), &message, 0) >= 0);
        }
        drop(client);
        let _ = std::fs::remove_file(&thread_socket_path);
    });
    ready_receiver
        .recv()
        .expect("malformed ancillary listener ready");

    let error = match StatsClient::connect(&socket_path) {
        Ok(_) => panic!("multiple ancillary fds must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        StatsError::ClientAncillaryData {
            received_fds: 2,
            malformed: false
        }
    ));
    server.join().expect("malformed ancillary server");
}

#[cfg(target_os = "macos")]
#[test]
fn connect_closes_rights_before_rejecting_malformed_stream_frame() {
    let sequence = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let socket_path = std::env::temp_dir().join(format!(
        "hsc-mf-frame-{}-{sequence}.sock",
        std::process::id()
    ));
    let (ready_sender, ready_receiver) = mpsc::channel();
    let thread_socket_path = socket_path.clone();
    let server = thread::spawn(move || {
        let listener =
            Socket::new(Domain::UNIX, Type::STREAM, None).expect("create malformed frame listener");
        let address = SockAddr::unix(&thread_socket_path).expect("malformed frame address");
        listener
            .bind(&address)
            .expect("bind malformed frame listener");
        listener.listen(1).expect("listen malformed frame listener");
        ready_sender
            .send(())
            .expect("signal malformed frame readiness");

        let (client, _) = listener.accept().expect("accept malformed frame client");
        let mut pipe_fds = [-1; 2];
        // SAFETY: `pipe_fds` points to storage for both descriptors.
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        // SAFETY: `pipe` initialized both descriptors on success.
        let pipe_read = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        // SAFETY: `pipe` initialized both descriptors on success.
        let pipe_write = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
        let control_size = unsafe { libc::CMSG_SPACE(size_of::<i32>() as u32) as usize };
        let mut control = vec![0_u8; control_size];
        let mut handoff = 0_u8;
        let mut iovec = libc::iovec {
            iov_base: (&mut handoff as *mut u8).cast(),
            iov_len: 1,
        };
        let message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut iovec,
            msg_iovlen: 1,
            msg_control: control.as_mut_ptr().cast(),
            msg_controllen: control.len().try_into().expect("ancillary length"),
            msg_flags: 0,
        };
        // SAFETY: `message` owns the live buffers and `pipe_write` remains
        // open until `sendmsg` returns.
        unsafe {
            let control_message = libc::CMSG_FIRSTHDR(&message);
            assert!(!control_message.is_null());
            (*control_message).cmsg_level = libc::SOL_SOCKET;
            (*control_message).cmsg_type = libc::SCM_RIGHTS;
            (*control_message).cmsg_len = libc::CMSG_LEN(size_of::<i32>() as u32) as _;
            ptr::write_unaligned(
                libc::CMSG_DATA(control_message).cast::<i32>(),
                pipe_write.as_raw_fd(),
            );
            assert!(libc::sendmsg(client.as_raw_fd(), &message, 0) >= 0);
        }
        drop(pipe_write);
        drop(client);

        let mut poll_fd = libc::pollfd {
            fd: pipe_read.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: `poll_fd` points to one live poll descriptor.
        let ready = unsafe { libc::poll(&mut poll_fd, 1, 5000) };
        assert_eq!(ready, 1, "received fd was not closed after rejection");
        let mut byte = [0_u8; 1];
        // SAFETY: `byte` is writable for one byte and `pipe_read` is live.
        let read = unsafe { libc::read(pipe_read.as_raw_fd(), byte.as_mut_ptr().cast(), 1) };
        assert_eq!(read, 0, "received pipe write fd remained open");
        let _ = std::fs::remove_file(&thread_socket_path);
    });
    ready_receiver
        .recv()
        .expect("malformed frame listener ready");

    let error = match StatsClient::connect(&socket_path) {
        Ok(_) => panic!("malformed frame must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, StatsError::ClientReceive { .. }));
    server.join().expect("malformed frame server");
}

#[test]
fn list_returns_published_metric_names() {
    let sequence = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let socket_path = std::env::temp_dir().join(format!(
        "hammer-stats-{}-{sequence}.sock",
        std::process::id()
    ));
    let segment_path = std::env::temp_dir().join(format!(
        "hammer-stats-{}-{sequence}.segment",
        std::process::id()
    ));
    let (ready_sender, ready_receiver) = mpsc::channel();
    let thread_socket_path = socket_path.clone();
    let thread_segment_path = segment_path.clone();
    let server = thread::spawn(move || {
        let segment = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&thread_segment_path)
            .expect("create stats fixture segment");
        segment.set_len(4096).expect("size stats fixture segment");
        // SAFETY: the file is four KiB long and remains open while the fixture
        // writes the exact VPP shared header and directory records.
        let mapping = unsafe {
            libc::mmap(
                ptr::null_mut(),
                4096,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                segment.as_raw_fd(),
                0,
            )
        };
        assert_ne!(mapping, libc::MAP_FAILED);
        let base = mapping as usize;
        let bytes = mapping.cast::<u8>();
        // SAFETY: every fixture offset is inside the four KiB mapped segment.
        unsafe {
            ptr::write_unaligned(bytes.cast::<u64>(), 2);
            ptr::write_unaligned(bytes.add(8).cast::<u64>(), base as u64);
            ptr::write_unaligned(bytes.add(16).cast::<u64>(), 1);
            ptr::write_unaligned(bytes.add(24).cast::<u64>(), 0);
            ptr::write_unaligned(bytes.add(32).cast::<u64>(), (base + 136) as u64);
            ptr::write_unaligned(bytes.add(128).cast::<u32>(), 4);
            *bytes.add(132) = 1;
            *bytes.add(133) = 3;
            ptr::write_unaligned(bytes.add(136).cast::<u32>(), 9);
            ptr::write_unaligned(bytes.add(144).cast::<u64>(), 42_f64.to_bits());
            ptr::copy_nonoverlapping(
                b"/sys/value\0".as_ptr(),
                bytes.add(152),
                b"/sys/value\0".len(),
            );
            ptr::write_unaligned(bytes.add(280).cast::<u32>(), 2);
            ptr::write_unaligned(bytes.add(288).cast::<u64>(), 0);
            ptr::copy_nonoverlapping(
                b"/sys/simple\0".as_ptr(),
                bytes.add(296),
                b"/sys/simple\0".len(),
            );
            ptr::write_unaligned(bytes.add(424).cast::<u32>(), 3);
            ptr::write_unaligned(bytes.add(432).cast::<u64>(), 0);
            ptr::copy_nonoverlapping(
                b"/sys/combined\0".as_ptr(),
                bytes.add(440),
                b"/sys/combined\0".len(),
            );
            ptr::write_unaligned(bytes.add(568).cast::<u32>(), 7);
            ptr::write_unaligned(bytes.add(576).cast::<u64>(), 0);
            ptr::copy_nonoverlapping(
                b"/sys/histogram\0".as_ptr(),
                bytes.add(584),
                b"/sys/histogram\0".len(),
            );
        }
        // SAFETY: the fixture mapping is no longer written after construction
        // and stays valid until this function's returned file is dropped.
        unsafe {
            libc::msync(mapping, 4096, libc::MS_SYNC);
            libc::munmap(mapping, 4096);
        }

        #[cfg(target_os = "linux")]
        let listener = Socket::new(Domain::UNIX, Type::SEQPACKET, None);
        #[cfg(not(target_os = "linux"))]
        let listener = Socket::new(Domain::UNIX, Type::STREAM, None);
        let listener = listener.expect("create stats fixture listener");
        listener
            .set_cloexec(true)
            .expect("set stats fixture listener cloexec");
        let address = SockAddr::unix(&thread_socket_path).expect("stats fixture address");
        listener
            .bind(&address)
            .expect("bind stats fixture listener");
        listener.listen(1).expect("listen stats fixture listener");
        ready_sender
            .send(())
            .expect("signal stats fixture readiness");

        let (client, _) = listener.accept().expect("accept stats fixture client");
        let mut control = vec![0_u8; unsafe { libc::CMSG_SPACE(size_of::<i32>() as u32) as usize }];
        #[cfg(not(target_os = "linux"))]
        let handoff = [1_u8];
        #[cfg(not(target_os = "linux"))]
        let mut iovec = libc::iovec {
            iov_base: handoff.as_ptr().cast_mut().cast(),
            iov_len: handoff.len(),
        };
        #[cfg(target_os = "linux")]
        let (iov_base, iov_len) = (ptr::null_mut(), 0);
        #[cfg(not(target_os = "linux"))]
        let (iov_base, iov_len) = (&mut iovec as *mut libc::iovec, 1);
        let header = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iov_base,
            msg_iovlen: iov_len,
            msg_control: control.as_mut_ptr().cast(),
            msg_controllen: control.len().try_into().expect("ancillary length"),
            msg_flags: 0,
        };
        // SAFETY: `header` owns the writable control buffer for this synchronous
        // sendmsg call, and `segment` remains open in the server.
        unsafe {
            let message = libc::CMSG_FIRSTHDR(&header);
            assert!(!message.is_null());
            (*message).cmsg_level = libc::SOL_SOCKET;
            (*message).cmsg_type = libc::SCM_RIGHTS;
            (*message).cmsg_len = libc::CMSG_LEN(size_of::<i32>() as u32) as _;
            ptr::write_unaligned(libc::CMSG_DATA(message).cast::<i32>(), segment.as_raw_fd());
            assert!(libc::sendmsg(client.as_raw_fd(), &header, 0) >= 0);
        }
        drop(client);
        drop(segment);
        let _ = std::fs::remove_file(&thread_socket_path);
        let _ = std::fs::remove_file(&thread_segment_path);
    });
    ready_receiver.recv().expect("stats fixture listener ready");

    let client = StatsClient::connect(&socket_path).expect("connect stats fixture");
    let names = client.list().expect("list stats fixture");
    let value = client.read("/sys/value").expect("read stats fixture");

    assert_eq!(
        names,
        vec![
            "/sys/value",
            "/sys/simple",
            "/sys/combined",
            "/sys/histogram",
        ]
    );
    assert!(matches!(value, MetricValue::Gauge(_)));
    assert_eq!(
        client.read("/sys/simple").expect("read simple fixture"),
        MetricValue::Simple(Vec::new())
    );
    assert_eq!(
        client.read("/sys/combined").expect("read combined fixture"),
        MetricValue::Combined(Vec::new())
    );
    assert_eq!(
        client
            .read("/sys/histogram")
            .expect("read histogram fixture"),
        MetricValue::Histogram(Vec::new())
    );
    server.join().expect("stats fixture server");
}
