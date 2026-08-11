//! Behavioral tests for Session Message Queue (IO / CTRL rings).
//! Observable enqueue/dequeue only — no source greps.

use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use hammer_runtime::app::session_msg_queue::{
    SessionEvt, SessionEvtType, SessionMqRing, SessionMsgQueue, SessionMsgQueueError,
};
use hammer_runtime::{File, FileFunctions, FileMain};

#[test]
fn existing_session_event_discriminants_remain_stable() {
    assert_eq!(SessionEvtType::RxEnq as u8, 0);
    assert_eq!(SessionEvtType::TxDeq as u8, 1);
    assert_eq!(SessionEvtType::Connect as u8, 2);
    assert_eq!(SessionEvtType::Close as u8, 3);
    assert_eq!(SessionEvtType::RxDeq as u8, 4);
    assert_eq!(SessionEvtType::TxEnq as u8, 5);
    assert_eq!(SessionEvtType::ProtocolOutput as u8, 6);
    assert_eq!(SessionEvtType::HalfClose as u8, 7);
    assert_eq!(SessionEvtType::Reset as u8, 8);
    assert_eq!(SessionEvtType::Disconnected as u8, 9);
    assert_eq!(SessionEvtType::TransportClosed as u8, 10);
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

    let got = q.dequeue().expect("dequeue").expect("event");
    assert_eq!(got, evt);
    assert_eq!(got.session_index(), 7);
    assert_eq!(got.worker_index(), 0);
    assert!(got.flags().is_empty());
    assert!(q.dequeue().expect("dequeue").is_none());
}

#[test]
fn session_evt_io_preserves_urgent_flag() {
    use hammer_runtime::app::SessionEvtFlags;

    let q = SessionMsgQueue::with_defaults().expect("queue");
    let evt = SessionEvt::io_with_flags(11, SessionEvtType::RxEnq, SessionEvtFlags::URGENT);
    q.enqueue_io(evt).expect("enqueue_io");

    let got = q.dequeue().expect("dequeue").expect("event");
    assert_eq!(got.evt_type, SessionEvtType::RxEnq);
    assert_eq!(got.session_index(), 11);
    assert!(got.flags().contains(SessionEvtFlags::URGENT));
}

#[test]
fn enqueue_ctrl_roundtrips_on_ctrl_ring() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let evt = SessionEvt::ctrl(3, 1, SessionEvtType::Close);
    q.enqueue_ctrl(evt).expect("enqueue_ctrl");

    let got = q.dequeue().expect("dequeue").expect("event");
    assert_eq!(got, evt);
    assert_eq!(got.session_index(), 3);
    assert_eq!(got.worker_index(), 1);
    assert!(q.dequeue().expect("dequeue").is_none());
}

#[test]
fn io_then_ctrl_preserve_fifo_order_across_rings() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let io = SessionEvt::io(1, SessionEvtType::RxEnq);
    let ctrl = SessionEvt::ctrl(2, 0, SessionEvtType::Connect);
    q.enqueue_io(io).expect("io");
    q.enqueue_ctrl(ctrl).expect("ctrl");

    // SessionEvt-sized Ctrl elements (worker control events) are valid on the
    // generic path; FIFO order across rings is preserved.
    assert_eq!(q.dequeue().expect("dequeue").expect("event"), io);
    assert_eq!(q.dequeue().expect("dequeue").expect("event"), ctrl);
    assert!(q.dequeue().expect("dequeue").is_none());
}

#[test]
fn dequeue_with_ring_preserves_queue_classification() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let ctrl = SessionEvt::ctrl(2, 0, SessionEvtType::Connect);
    let io = SessionEvt::io(1, SessionEvtType::TxEnq);
    q.enqueue_ctrl(ctrl).expect("ctrl");
    q.enqueue_io(io).expect("io");

    assert_eq!(
        q.dequeue_with_ring().expect("dequeue"),
        Some((SessionMqRing::Ctrl, ctrl))
    );
    assert_eq!(
        q.dequeue_with_ring().expect("dequeue"),
        Some((SessionMqRing::Io, io))
    );
    assert!(q.dequeue_with_ring().expect("dequeue").is_none());
}

#[test]
fn single_producer_claim_is_sticky_and_typed() {
    use hammer_runtime::app::SingleProducer;

    let queue = SessionMsgQueue::<SingleProducer>::with_control_defaults().expect("control queue");
    queue.claim_producer().expect("first claim");
    // The claim is taken once; a second claim on the same mapping is a typed
    // error, never a panic.
    assert!(matches!(
        queue.claim_producer(),
        Err(SessionMsgQueueError::ProducerClaimed)
    ));
}

#[test]
fn single_producer_control_signals_only_on_empty_to_nonempty_transition() {
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::{SessionConnectedMsg, SessionHandle, SingleProducer};

    let bytes = SessionMsgQueue::<SingleProducer>::layout_bytes_with_control(8, 4)
        .expect("control queue layout");
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    let mut queue = unsafe {
        SessionMsgQueue::<SingleProducer>::init_at_with_signal_and_control(seg, off, 8, 4)
    }
    .expect("control queue");
    let mut producer = queue.claim_producer().expect("claim producer");
    let read_fd = queue.read_fd().expect("signal read descriptor");

    let message = SessionConnectedMsg::new(41, Ok(SessionHandle::new(17, 3)));
    producer.enqueue_control(&message).expect("first enqueue");
    producer.enqueue_control(&message).expect("second enqueue");

    // Only the first publish crossed empty → nonempty, so the pipe carries
    // exactly one signal byte for the two-message burst.
    let mut buf = [0_u8; 64];
    let read = unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    assert_eq!(read, 1, "signal must fire once per empty → nonempty burst");
    assert_eq!(
        queue
            .dequeue_control()
            .expect("dequeue")
            .map(|item| item.event_type()),
        Some(SessionEvtType::Connected)
    );
    assert_eq!(
        queue
            .dequeue_control()
            .expect("dequeue")
            .map(|item| item.event_type()),
        Some(SessionEvtType::Connected)
    );
}

#[test]
fn sp_dequeue_control_rejects_session_evt_sized_ctrl_ring() {
    use hammer_infra::multi_ring_msg_queue::{
        MultiProducer, MultiRingMsgQueue, MultiRingMsgQueueCfg, RingCfg,
        SingleProducer as InfraSingleProducer,
    };
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::SingleProducer;

    let cfg = MultiRingMsgQueueCfg {
        q_nitems: 8,
        rings: &[
            RingCfg {
                nitems: 4,
                elsize: 16,
            },
            RingCfg {
                nitems: 4,
                elsize: 16,
            },
        ],
    };
    let layout = MultiRingMsgQueue::<MultiProducer>::layout_bytes(&cfg);
    let seg = Segment::shared_default();
    let off = seg.alloc(layout, 64).expect("queue allocation");
    unsafe { MultiRingMsgQueue::<InfraSingleProducer>::init_at(seg.clone(), off, &cfg) }
        .expect("SP init");

    // A SessionEvt-sized CTRL ring is not a control queue: requesting a fixed
    // control slot from it is a typed queue error, and the event is not
    // consumed (a plain consumer mapping still reads it).
    let mut queue =
        unsafe { SessionMsgQueue::<SingleProducer>::from_shared(seg.clone(), off, None, None) }
            .expect("SP mapping");
    let mut producer = unsafe {
        MultiRingMsgQueue::<InfraSingleProducer>::from_shared(seg.clone(), off)
            .expect("infra producer mapping")
            .claim_producer()
            .expect("infra claim")
    };
    let mut reservation = producer
        .reserve(SessionMqRing::Ctrl as u32)
        .expect("reserve");
    let mut payload = [0_u8; 16];
    payload[0] = SessionEvtType::Connect as u8;
    payload[8..].copy_from_slice(&2u64.to_le_bytes());
    reservation.payload_mut().copy_from_slice(&payload);
    reservation.publish();

    assert!(matches!(
        queue.dequeue_control(),
        Err(SessionMsgQueueError::InvalidConfig)
    ));
    let mut consumer = unsafe { MultiRingMsgQueue::<InfraSingleProducer>::from_shared(seg, off) }
        .expect("infra consumer mapping");
    let message = consumer.sub().expect("event preserved");
    assert_eq!(message.ring_index(), SessionMqRing::Ctrl as u32);
    drop(message);
}

#[test]
fn full_queue_returns_error_without_dropping_identity() {
    let q = SessionMsgQueue::with_cfg(2, 16).expect("tiny descriptor queue");
    q.enqueue_io(SessionEvt::io(1, SessionEvtType::TxDeq))
        .expect("first");
    // Fill until full.
    let last = (2..32)
        .map(|i| q.enqueue_io(SessionEvt::io(i, SessionEvtType::TxDeq)))
        .find(Result::is_err)
        .unwrap_or(Ok(()));
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
        fds.iter().for_each(|fd| {
            let flags = unsafe { libc::fcntl(*fd, libc::F_GETFL) };
            unsafe {
                libc::fcntl(*fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        });
        (fds[0], fds[1])
    }

    let (read_fd, write_fd) = pipe_nonblock();
    let seg = Segment::shared_default();
    let bytes = SessionMsgQueue::layout_bytes(8, 4).expect("layout");
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    drop(unsafe { SessionMsgQueue::init_at(seg.clone(), off, 8, 4) }.expect("init"));

    let producer = unsafe { SessionMsgQueue::from_shared(seg.clone(), off, None, Some(write_fd)) }
        .expect("producer mapping");
    let consumer = unsafe { SessionMsgQueue::from_shared(seg, off, Some(read_fd), None) }
        .expect("consumer mapping");

    assert!(!consumer.read_signal());
    producer
        .enqueue_io(SessionEvt::io(9, SessionEvtType::TxDeq))
        .expect("enqueue");
    assert!(consumer.read_signal());
    assert_eq!(
        consumer
            .dequeue()
            .expect("dequeue")
            .map(|e| e.session_index()),
        Some(9)
    );
}

#[test]
fn svm_session_msg_queue_signals_only_on_empty_to_non_empty_transition()
-> Result<(), SessionMsgQueueError> {
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::SessionEventQueue;

    let mut fds = [0i32; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    fds.iter().for_each(|fd| {
        let flags = unsafe { libc::fcntl(*fd, libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(*fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
    });

    let seg = Segment::shared_default();
    let bytes = SessionMsgQueue::layout_bytes(8, 4)?;
    let off = seg
        .alloc(bytes, 64)
        .ok_or(SessionMsgQueueError::InvalidConfig)?;
    unsafe { SessionMsgQueue::init_at(seg.clone(), off, 8, 4) }?;

    let producer = unsafe { SessionMsgQueue::from_shared(seg.clone(), off, None, Some(fds[1])) }
        .expect("producer mapping");
    let consumer = unsafe { SessionMsgQueue::from_shared(seg, off, Some(fds[0]), None) }
        .expect("consumer mapping");

    producer.enqueue_io(SessionEvt::io(1, SessionEvtType::TxEnq))?;
    assert!(consumer.read_signal());
    producer.enqueue_io(SessionEvt::io(2, SessionEvtType::TxEnq))?;

    assert!(!consumer.read_signal());
    assert_eq!(
        consumer
            .dequeue()
            .expect("dequeue")
            .map(|event| event.session_index()),
        Some(1)
    );
    assert_eq!(
        consumer
            .dequeue()
            .expect("dequeue")
            .map(|event| event.session_index()),
        Some(2)
    );
    Ok(())
}

#[test]
fn svm_session_msg_queue_owns_attached_signal_descriptors() {
    use hammer_infra::multi_ring_msg_queue::MultiProducer;
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

    drop(
        unsafe {
            SessionMsgQueue::<MultiProducer>::from_shared(seg, off, Some(fds[0]), Some(fds[1]))
        }
        .expect("from_shared"),
    );

    fds.into_iter()
        .zip(identities)
        .for_each(|(fd, identity)| match descriptor_identity(fd) {
            Err(error) => assert_eq!(error.raw_os_error(), Some(libc::EBADF)),
            Ok(reused) => assert_ne!(reused, identity, "signal descriptor remains open"),
        });
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

#[test]
fn sp_dequeue_control_unexpected_ring_preserves_message() {
    use hammer_infra::multi_ring_msg_queue::{
        MultiRingMsgQueue, SingleProducer as InfraSingleProducer,
    };
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::SingleProducer;

    let bytes = SessionMsgQueue::<SingleProducer>::layout_bytes_with_control(8, 4)
        .expect("control queue layout");
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    let mut queue = unsafe {
        SessionMsgQueue::<SingleProducer>::init_at_with_signal_and_control(seg.clone(), off, 8, 4)
    }
    .expect("control queue");
    let mut producer = unsafe {
        MultiRingMsgQueue::<InfraSingleProducer>::from_shared(seg.clone(), off)
            .expect("infra producer mapping")
            .claim_producer()
            .expect("infra claim")
    };

    // An IO-ring event (SessionEvt-sized slot) sits ahead of the CTRL slot:
    // the control consumer must report it without consuming it.
    let mut reservation = producer.reserve(SessionMqRing::Io as u32).expect("reserve");
    let mut payload = [0_u8; 16];
    payload[0] = SessionEvtType::RxEnq as u8;
    payload[8..].copy_from_slice(&7u64.to_le_bytes());
    reservation.payload_mut().copy_from_slice(&payload);
    reservation.publish();
    drop(reservation);

    assert!(
        matches!(
            queue.dequeue_control(),
            Err(SessionMsgQueueError::UnexpectedRing { ring })
                if ring == SessionMqRing::Io as u32
        ),
        "IO-ring head must be a typed UnexpectedRing error"
    );
    assert!(
        matches!(
            queue.dequeue_control(),
            Err(SessionMsgQueueError::UnexpectedRing { ring })
                if ring == SessionMqRing::Io as u32
        ),
        "the message must still be queued after the first rejection"
    );
    // The IO event was not consumed: it stays available to the consumer
    // path that owns the IO ring, with its payload intact.
    let mut consumer = unsafe { MultiRingMsgQueue::<InfraSingleProducer>::from_shared(seg, off) }
        .expect("infra consumer mapping");
    let message = consumer.sub().expect("message preserved");
    assert_eq!(message.ring_index(), SessionMqRing::Io as u32);
    assert_eq!(message.as_slice(), &payload);
    drop(message);

    // The control consumer still serves the following CTRL slot.
    let mut reservation = producer
        .reserve(SessionMqRing::Ctrl as u32)
        .expect("reserve ctrl");
    let mut ctrl_payload =
        [0_u8; hammer_runtime::app::session_msg_queue::SESSION_CTRL_MSG_MAX_SIZE];
    ctrl_payload[0] = SessionEvtType::Connect as u8;
    reservation.payload_mut().copy_from_slice(&ctrl_payload);
    reservation.publish();
    assert_eq!(
        queue
            .dequeue_control()
            .expect("dequeue")
            .map(|item| item.event_type()),
        Some(SessionEvtType::Connect)
    );
}

#[test]
fn from_shared_rejects_out_of_range_on_segment_mode() {
    use hammer_infra::segment::Segment;
    use hammer_runtime::app::SingleProducer;

    let bytes = SessionMsgQueue::<SingleProducer>::layout_bytes_with_control(8, 4)
        .expect("control queue layout");
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    drop(
        unsafe {
            SessionMsgQueue::<SingleProducer>::init_at_with_signal_and_control(
                seg.clone(),
                off,
                8,
                4,
            )
        }
        .expect("control queue"),
    );
    // Corrupt the on-segment producer-mode tag to an out-of-range value
    // (QueueHeader is repr(C): mode sits at byte offset 24 after four u32
    // atomics, q_nitems, q_mask, and n_rings).
    unsafe {
        *(seg.base().add(off as usize + 24) as *mut u32) = 5;
    }
    assert!(matches!(
        unsafe { SessionMsgQueue::<SingleProducer>::from_shared(seg, off, None, None) },
        Err(SessionMsgQueueError::InvalidConfig)
    ));
}
