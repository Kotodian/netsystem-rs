#[test]
fn remote_wait_for_event_does_not_dequeue_session_events() {
    let source = include_str!("../src/remote_session.rs");

    assert!(
        !source.contains("if self.session.evt_q().dequeue().is_some()"),
        "RemoteAppSession::wait_for_event must not consume the event that woke the app"
    );
}
