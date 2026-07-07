fn read_source(path: &str) -> String {
    std::fs::read_to_string(format!("{}/{}", env!("CARGO_MANIFEST_DIR"), path))
        .unwrap_or_else(|err| panic!("read {path}: {err}"))
}

#[test]
fn session_tx_external_seam_does_not_expose_prepare_cancel_commit() {
    let source = read_source("src/session/runtime.rs");

    assert!(!source.contains("fn prepare_tx("));
    assert!(!source.contains("fn cancel_tx("));
    assert!(!source.contains("fn commit_tx("));
}

#[test]
fn session_runtime_does_not_scan_tcp_timer_masks() {
    let source = read_source("src/session/protocol.rs");

    assert!(!source.contains("TCP_TIMER_COUNT"));
    assert!(!source.contains("active_timer_mask"));
    assert!(!source.contains("timer_mask"));
}

#[test]
fn session_runtime_does_not_refresh_tcp_timers_or_construct_tcp_output_intent() {
    let runtime = read_source("src/session/runtime.rs");
    let protocol = read_source("src/session/protocol.rs");

    assert!(!runtime.contains("refresh_tcp_timers"));
    assert!(!protocol.contains("refresh_tcp_timers"));
    assert!(!runtime.contains("TcpSegment::new("));
    assert!(!protocol.contains("TcpSegment::new("));
    assert!(!runtime.contains("tcp_option_len"));
    assert!(!protocol.contains("tcp_option_len"));
}

#[test]
fn session_runtime_only_consumes_send_goal_size_for_gso_shaping() {
    let source = read_source("src/session/runtime.rs");

    assert!(source.contains("send_goal_size"));
    assert!(!source.contains("gso_size"));
    assert!(!source.contains("gso_type"));
    assert!(!source.contains("VNET_BUFFER_F_GSO"));
    assert!(!source.contains("offload_metadata"));
}
