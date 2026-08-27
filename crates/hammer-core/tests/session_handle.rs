use hammer_core::session::{SessionEvt, SessionEvtType, SessionHandle};

#[test]
fn session_handle_exposes_vpp_index_and_thread_fields() {
    let handle = SessionHandle::new(17, 3);

    assert_eq!(handle.session_index, 17);
    assert_eq!(handle.thread_index, 3);
}

#[test]
fn session_event_carries_explicit_target_fields() {
    let event = SessionEvt::io(17, SessionEvtType::RxEnq);
    assert_eq!(event.session_index, 17);
    assert_eq!(event.thread_index, 0);
}

#[test]
fn session_handle_conversion_preserves_vpp_field_order() {
    let handle = SessionHandle::new(0x11, 0x03);
    let value: u64 = handle.into();
    assert_eq!(value, 0x0000_0003_0000_0011);
    let restored: SessionHandle = value.into();
    assert_eq!(restored, handle);
}
