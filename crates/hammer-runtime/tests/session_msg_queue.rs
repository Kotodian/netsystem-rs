//! Behavioral tests for Session Message Queue (IO / CTRL rings).
//! Observable enqueue/dequeue only — no source greps.

use hammer_runtime::app::session_msg_queue::{
    SessionEvt, SessionEvtType, SessionMsgQueue, SessionMsgQueueError,
};

#[test]
fn enqueue_io_roundtrips_on_io_ring() {
    let q = SessionMsgQueue::with_defaults().expect("queue");
    let evt = SessionEvt::io(7, SessionEvtType::TxDeq);
    q.enqueue_io(evt).expect("enqueue_io");

    let got = q.dequeue().expect("dequeue");
    assert_eq!(got, evt);
    assert_eq!(got.session_index(), 7);
    assert_eq!(got.worker_index(), 0);
    assert!(q.dequeue().is_none());
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
    use std::os::fd::RawFd;

    use hammer_infra::segment::{Segment, Svm};
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
    let seg = Svm::default();
    let bytes = SessionMsgQueue::<Svm>::layout_bytes(8, 4).expect("layout");
    let off = seg.alloc(bytes, 64);
    drop(unsafe { SessionMsgQueue::<Svm>::init_at(seg.clone(), off, 8, 4) }.expect("init"));

    let producer =
        unsafe { SessionMsgQueue::<Svm>::from_shared(seg.clone(), off, None, Some(write_fd)) };
    let consumer = unsafe { SessionMsgQueue::<Svm>::from_shared(seg, off, Some(read_fd), None) };

    assert!(!consumer.read_signal());
    producer
        .enqueue_io(SessionEvt::io(9, SessionEvtType::TxDeq))
        .expect("enqueue");
    assert!(consumer.read_signal());
    assert_eq!(consumer.dequeue().map(|e| e.session_index()), Some(9));
}
