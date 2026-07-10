#[test]
fn session_runtime_uses_static_transport_dispatch() {
    let source = include_str!("../src/session/runtime.rs");

    assert!(source.contains("trait SessionTransport<"));
    assert!(source.contains("trait SessionTransports<"));
    assert!(source.contains("struct SessionPacketizedTx"));
    assert!(source.contains("struct TransportInternalTx"));
    assert!(!source.contains("dyn SessionTransport"));
    assert!(!source.contains("enum SessionProtocol"));
    assert!(!source.contains("trait SessionQueueProtocol"));
}

#[test]
fn session_entry_stores_only_transport_lifecycle_and_schedule_state() {
    let source = include_str!("../src/session/runtime.rs");
    let start = source.find("struct SessionEntry<").expect("SessionEntry");
    let body = &source[start..source[start..].find("}\n").expect("entry end") + start];

    assert!(body.contains("transport: SessionTransportId"));
    assert!(body.contains("state: SessionState<Index>"));
    assert!(body.contains("schedule_pending: bool"));
    assert!(!body.contains("TcpConnection"));
}
