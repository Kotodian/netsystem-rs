use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Svm;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, SessionEvt, SessionEvtType, SessionHandle, SessionMsgQueue,
    SessionOffsets,
};

fn session_queue_sizes(config: AppSessionConfig) -> (u32, u32) {
    let ring_nitems = config.evt_q_capacity.max(1) as u32;
    let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
    (q_nitems, ring_nitems)
}

unsafe fn init_svm_session_queues(seg: &Svm, offsets: &SessionOffsets, config: AppSessionConfig) {
    Fifo::<Svm>::init_at(seg.clone(), offsets.rx_fifo_off, config.fifo_capacity)
        .expect("init rx fifo");
    Fifo::<Svm>::init_at(seg.clone(), offsets.tx_fifo_off, config.fifo_capacity)
        .expect("init tx fifo");
    let (q_nitems, ring_nitems) = session_queue_sizes(config);
    SessionMsgQueue::<Svm>::init_at(seg.clone(), offsets.evt_q_off, q_nitems, ring_nitems)
        .expect("init evt_q");
    SessionMsgQueue::<Svm>::init_at(seg.clone(), offsets.tx_evt_q_off, 64, 64)
        .expect("init tx_evt_q");
}

#[test]
fn svm_session_create_and_fifo_round_trip() {
    let seg = Svm::default();
    let config = AppSessionConfig::new(128, 16);
    let handle = SessionHandle::new(0, 0);

    let offsets =
        SessionOffsets::allocate(&seg, config.fifo_capacity as u32, config.evt_q_capacity);
    unsafe {
        init_svm_session_queues(&seg, &offsets, config);
    }

    let session =
        unsafe { AppSession::<Svm>::from_segment(handle, &seg, &offsets, None, None, None, None) };

    let written = session.rx_fifo().enqueue(b"hello");
    assert_eq!(written, 5);

    let mut out = [0u8; 16];
    let peeked = session.rx_fifo().peek(0, out.len(), &mut out);
    assert_eq!(peeked, 5);
    assert_eq!(&out[..5], b"hello");
}

#[test]
fn svm_session_multi_ring_evt_q_io_and_ctrl_round_trip() {
    let seg = Svm::default();
    let config = AppSessionConfig::new(128, 16);
    let handle = SessionHandle::new(3, 2);

    let offsets =
        SessionOffsets::allocate(&seg, config.fifo_capacity as u32, config.evt_q_capacity);
    unsafe {
        init_svm_session_queues(&seg, &offsets, config);
    }

    let session =
        unsafe { AppSession::<Svm>::from_segment(handle, &seg, &offsets, None, None, None, None) };

    session
        .push_event(SessionEvtType::Connect)
        .expect("push connect");
    session.want_rx_notification();
    assert_eq!(session.enqueue_rx(b"hi").expect("enqueue rx"), 2);

    let mut out = [SessionEvt::io(0, SessionEvtType::Close); 4];
    assert_eq!(session.poll_events(&mut out), 2);
    assert_eq!(out[0].evt_type, SessionEvtType::Connect);
    assert_eq!(out[0].session_index(), 3);
    assert_eq!(out[0].worker_index(), 2);
    assert_eq!(out[1].evt_type, SessionEvtType::RxEnq);
    assert_eq!(out[1].session_index(), 3);
    assert_eq!(out[1].worker_index(), 0);

    assert_eq!(session.send_bytes(b"x").expect("send"), 1);
    let tx = session.tx_evt_q().dequeue().expect("tx deq");
    assert_eq!(tx.evt_type, SessionEvtType::TxDeq);
    assert_eq!(tx.session_index(), 3);
}

#[test]
fn svm_segment_supports_multiple_sessions() {
    let seg = Svm::default();
    let config = AppSessionConfig::new(64, 8);

    let offsets1 =
        SessionOffsets::allocate(&seg, config.fifo_capacity as u32, config.evt_q_capacity);
    let offsets2 =
        SessionOffsets::allocate(&seg, config.fifo_capacity as u32, config.evt_q_capacity);

    assert_ne!(offsets1.rx_fifo_off, offsets2.rx_fifo_off);
    assert!(offsets2.rx_fifo_off > offsets1.tx_evt_q_off);
}
