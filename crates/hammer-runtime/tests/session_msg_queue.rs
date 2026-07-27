//! Behavioral tests for Session Message Queue (IO / CTRL rings).
//! Observable enqueue/dequeue only — no source greps.

use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use hammer_runtime::app::session_msg_queue::{
    SessionEvt, SessionEvtType, SessionMsgQueue, SessionMsgQueueError,
};
use hammer_runtime::{File, FileFunctions, FileMain};

#[test]
fn existing_session_event_discriminants_remain_stable() {
    assert_eq!(SessionEvtType::RxEnq as u8, 0);
    assert_eq!(SessionEvtType::TxDeq as u8, 1);
    assert_eq!(SessionEvtType::Connect as u8, 2);
    assert_eq!(SessionEvtType::Close as u8, 3);
    assert_eq!(SessionEvtType::RxDeq as u8, 4);
}

fn descriptor_identity(fd: RawFd) -> std::io::Result<(libc::dev_t, libc::ino_t)> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `status` is writable storage for one `stat`; a successful
    // `fstat` initializes the complete value before it is read.
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } == 0 {
        // SAFETY: the successful `fstat` initialized `status` above.
        let status = unsafe { status.assume_init() };
        Ok((status.st_dev, status.st_ino))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[test]
fn enqueue_io_roundtrips_on_io_ring() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let evt = SessionEvt::io(7, SessionEvtType::TxDeq);
    q.enqueue_io(evt).expect("enqueue_io");

    let got = q.dequeue().expect("dequeue");
    assert_eq!(got, evt);
    assert_eq!(got.session_index(), 7);
    assert_eq!(got.worker_index(), 0);
    assert!(got.flags().is_empty());
    assert!(q.dequeue().is_none());
}

#[test]
fn session_evt_io_preserves_urgent_flag() {
    use hammer_runtime::app::SessionEvtFlags;

    let q = SessionMsgQueue::with_defaults().expect("queue");
    let evt = SessionEvt::io_with_flags(11, SessionEvtType::RxEnq, SessionEvtFlags::URGENT);
    q.enqueue_io(evt).expect("enqueue_io");

    let got = q.dequeue().expect("dequeue");
    assert_eq!(got.evt_type, SessionEvtType::RxEnq);
    assert_eq!(got.session_index(), 11);
    assert!(got.flags().contains(SessionEvtFlags::URGENT));
}

#[test]
fn enqueue_ctrl_roundtrips_on_ctrl_ring() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let evt = SessionEvt::ctrl(3, 1, SessionEvtType::Close);
    q.enqueue_ctrl(evt).expect("enqueue_ctrl");

    let got = q.dequeue().expect("dequeue");
    assert_eq!(got, evt);
    assert_eq!(got.session_index(), 3);
    assert_eq!(got.worker_index(), 1);
    assert!(q.dequeue().is_none());
}

#[test]
fn io_then_ctrl_preserve_fifo_order_across_rings() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let io = SessionEvt::io(1, SessionEvtType::RxEnq);
    let ctrl = SessionEvt::ctrl(2, 0, SessionEvtType::Connect);
    q.enqueue_io(io).expect("io");
    q.enqueue_ctrl(ctrl).expect("ctrl");

    assert_eq!(q.dequeue(), Some(io));
    assert_eq!(q.dequeue(), Some(ctrl));
    assert!(q.dequeue().is_none());
}

#[test]
fn full_queue_returns_error_without_dropping_identity() {
    let q = SessionMsgQueue::with_cfg(2, 16).expect("tiny descriptor queue");
    q.enqueue_io(SessionEvt::io(1, SessionEvtType::TxDeq))
        .expect("first");
    // Fill until full.
    let mut last = Ok(());
    for i in 2..32 {
        last = q.enqueue_io(SessionEvt::io(i, SessionEvtType::TxDeq));
        if last.is_err() {
            break;
        }
    }
    match last {
        Err(SessionMsgQueueError::Full(evt)) => {
            assert_eq!(evt.evt_type, SessionEvtType::TxDeq);
        }
        other => panic!("expected Full, got {other:?}"),
    }
}

#[test]
fn adr0010_io_index_only_ctrl_handle_packing() {
    let io = SessionEvt::io(0xAABB_CCDD, SessionEvtType::RxEnq);
    assert_eq!(io.session_handle_raw(), 0xAABB_CCDDu64);

    let ctrl = SessionEvt::ctrl(0x1111_2222, 0x3333_4444, SessionEvtType::Close);
    assert_eq!(
        ctrl.session_handle_raw(),
        (0x1111_2222u64) | ((0x3333_4444u64) << 32)
    );
}

#[test]
fn svm_session_msg_queue_pipe_signal_wakes_consumer() {
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::SessionEventQueue;

    fn pipe_nonblock() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        for fd in &fds {
            let flags = unsafe { libc::fcntl(*fd, libc::F_GETFL) };
            unsafe {
                libc::fcntl(*fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        }
        (fds[0], fds[1])
    }

    let (read_fd, write_fd) = pipe_nonblock();
    let seg = Segment::shared_default();
    let bytes = SessionMsgQueue::layout_bytes(8, 4).expect("layout");
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    drop(unsafe { SessionMsgQueue::init_at(seg.clone(), off, 8, 4) }.expect("init"));

    let producer = unsafe { SessionMsgQueue::from_shared(seg.clone(), off, None, Some(write_fd)) };
    let consumer = unsafe { SessionMsgQueue::from_shared(seg, off, Some(read_fd), None) };

    assert!(!consumer.read_signal());
    producer
        .enqueue_io(SessionEvt::io(9, SessionEvtType::TxDeq))
        .expect("enqueue");
    assert!(consumer.read_signal());
    assert_eq!(consumer.dequeue().map(|e| e.session_index()), Some(9));
}

#[test]
fn svm_session_msg_queue_owns_attached_signal_descriptors() {
    use hammer_infra::segment::Segment;

    let mut fds = [0i32; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    let identities = [
        descriptor_identity(fds[0]).expect("pipe read descriptor identity"),
        descriptor_identity(fds[1]).expect("pipe write descriptor identity"),
    ];

    let seg = Segment::shared_default();
    let bytes = SessionMsgQueue::layout_bytes(8, 4).expect("layout");
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    drop(unsafe { SessionMsgQueue::init_at(seg.clone(), off, 8, 4) }.expect("init"));

    drop(unsafe { SessionMsgQueue::from_shared(seg, off, Some(fds[0]), Some(fds[1])) });

    for (fd, identity) in fds.into_iter().zip(identities) {
        match descriptor_identity(fd) {
            Err(error) => assert_eq!(error.raw_os_error(), Some(libc::EBADF)),
            Ok(reused) => assert_ne!(reused, identity, "signal descriptor remains open"),
        }
    }
}

#[test]
fn file_readiness_duplicate_closes_independently_of_queue_signal_owner() {
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::SessionEventQueue;

    let seg = Segment::shared_default();
    let bytes = SessionMsgQueue::layout_bytes(8, 4).expect("layout");
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    let queue =
        unsafe { SessionMsgQueue::init_at_with_signal(seg, off, 8, 4) }.expect("queue with signal");
    let queue_fd = queue.read_fd().expect("queue signal-read descriptor");
    let queue_identity = descriptor_identity(queue_fd).expect("queue descriptor identity");

    // SAFETY: F_DUPFD_CLOEXEC returns a fresh descriptor whose ownership moves
    // into File below. The queue retains its original endpoint.
    let duplicated = unsafe { libc::fcntl(queue_fd, libc::F_DUPFD_CLOEXEC, 0) };
    assert!(duplicated >= 0, "duplicate queue signal-read descriptor");
    let duplicate_identity =
        descriptor_identity(duplicated).expect("File duplicate descriptor identity");
    // SAFETY: fcntl returned a fresh descriptor and File becomes its sole owner.
    let file_fd = unsafe { OwnedFd::from_raw_fd(duplicated) };
    let mut files = FileMain::new().expect("FileMain");
    let file_index = files
        .add(File::new(
            file_fd,
            "SVM queue readiness duplicate".to_owned(),
            0,
            FileFunctions {
                read: Some(|_, _| Ok(())),
                ..FileFunctions::default()
            },
        ))
        .expect("register File duplicate");

    assert!(files.delete(file_index).expect("delete File duplicate"));
    match descriptor_identity(duplicated) {
        Err(error) => assert_eq!(error.raw_os_error(), Some(libc::EBADF)),
        Ok(reused) => assert_ne!(reused, duplicate_identity, "File duplicate remains open"),
    }
    assert_eq!(
        descriptor_identity(queue_fd).expect("queue endpoint remains open"),
        queue_identity
    );

    queue
        .enqueue_ctrl(SessionEvt::ctrl(1, 0, SessionEvtType::Close))
        .expect("signal through queue-owned endpoints");
    assert!(queue.read_signal());
}
