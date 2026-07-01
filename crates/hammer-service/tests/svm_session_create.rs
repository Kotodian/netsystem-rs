use hammer_infra::fifo::Fifo;
use hammer_infra::msg_queue::MsgQueue;
use hammer_infra::segment::Svm;
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle, SessionOffsets};

#[test]
fn svm_session_create_and_fifo_round_trip() {
    let seg = Svm::default();
    let config = AppSessionConfig::new(128, 16);
    let handle = SessionHandle::new(0, 0);

    let offsets =
        SessionOffsets::allocate(&seg, config.fifo_capacity as u32, config.evt_q_capacity);

    unsafe {
        Fifo::<Svm>::init_at(seg.clone(), offsets.rx_fifo_off, config.fifo_capacity)
            .expect("init rx fifo");
        Fifo::<Svm>::init_at(seg.clone(), offsets.tx_fifo_off, config.fifo_capacity)
            .expect("init tx fifo");
    }
    let evt_q_ring = config
        .evt_q_capacity
        .saturating_add(1)
        .next_power_of_two()
        .max(2);
    unsafe {
        MsgQueue::<Svm>::init_at(seg.clone(), offsets.evt_q_off, evt_q_ring).expect("init evt_q");
        MsgQueue::<Svm>::init_at(seg.clone(), offsets.tx_evt_q_off, 64).expect("init tx_evt_q");
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
