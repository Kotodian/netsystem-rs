use std::fs;
use std::path::Path;

fn read_tcp_source(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).expect("read tcp source")
}

#[test]
fn tcp_public_api_has_no_forbidden_middle_types() {
    let sources = [
        read_tcp_source("src/transport/tcp/connection.rs"),
        read_tcp_source("src/transport/tcp/listen.rs"),
        read_tcp_source("src/transport/tcp/syn_sent.rs"),
        read_tcp_source("src/transport/tcp/rcv_process.rs"),
        read_tcp_source("src/transport/tcp/established.rs"),
        read_tcp_source("src/transport/tcp/mod.rs"),
    ]
    .join("\n");

    let forbidden = [
        concat!("Tcp", "State", "Machine"),
        concat!("Tcp", "Connection", "View"),
        concat!("Tcp", "Output", "Send", "View"),
        concat!("Tcp", "State", "Segment"),
        concat!("Tcp", "State", "Transition"),
        concat!("Tcp", "State", "Machine", "Output"),
        concat!("Tcp", "Active", "Open"),
        concat!("Tcp", "Connection", "Route"),
        concat!("Tcp", "Connection", "Index", "Key"),
        concat!("Tcp", "Connection", "Queue", "Commit"),
        concat!("Tcp", "Connection", "Store"),
        concat!("Tcp", "Established", "Tx", "Capacity"),
        concat!("Tcp", "Established", "Tx", "Update"),
        concat!("write", "_established", "_payload", "_segment", "_header"),
        concat!("commit", "_established", "_payload", "_segment"),
        concat!("Disposition"),
        concat!("Effect"),
    ];

    for pattern in forbidden {
        assert!(
            !sources.contains(pattern),
            "forbidden TCP helper remains: {pattern}"
        );
    }
}

#[test]
fn packet_nodes_do_not_drive_tcp_queue_state() {
    let sources = [
        read_tcp_source("src/transport/tcp/listen.rs"),
        read_tcp_source("src/transport/tcp/syn_sent.rs"),
        read_tcp_source("src/transport/tcp/rcv_process.rs"),
        read_tcp_source("src/transport/tcp/established.rs"),
    ]
    .join("\n");

    let forbidden = [
        concat!("put", "_connection"),
        concat!("indexed", ".state()"),
        concat!("take_connection", "::"),
        concat!("TcpConnectionState::"),
        ".try_into()",
    ];

    for pattern in forbidden {
        assert!(
            !sources.contains(pattern),
            "packet node still drives TCP state or queue policy: {pattern}"
        );
    }
}

#[test]
fn tcp_timer_dispatch_is_owned_by_connection() {
    let source = read_tcp_source("src/transport/tcp/connection.rs");
    assert!(!source.contains("on_retransmit_timeout"));
    assert!(!source.contains("retransmit_syn_header_if_ready"));
    assert!(source.contains("pub(crate) fn on_tcp_timer_expiry("));
    assert!(source.contains("timer_dispatch_pending(timer_id)"));
    assert!(source.contains("match (self.state, timer_id)"));
    assert!(source.contains("match timer_id"));
    assert!(!source.contains("TcpConnectionTimerKind::all"));
}

#[test]
fn tcp_input_routes_close_side_receive_states_through_rcv_process() {
    let source = read_tcp_source("src/transport/tcp/mod.rs");

    assert!(source.contains("RcvProcess"));
    for next in [
        "SynRcvd",
        "CloseWait",
        "FinWait1",
        "FinWait2",
        "Closing",
        "LastAck",
        "TimeWait",
    ] {
        assert!(
            !source.contains(next),
            "TcpInputNext should not keep dedicated receive node {next}"
        );
    }
}

#[test]
fn tcp_close_path_updates_connection_state_in_connection() {
    let source = read_tcp_source("src/transport/tcp/connection.rs");
    assert!(source.contains("pub(crate) fn on_session_close(&mut self)"));
    assert!(source.contains("TcpState::Established"));
    assert!(source.contains("self.state = TcpState::FinWait1;"));
    assert!(source.contains("self.state = TcpState::LastAck;"));
}

#[test]
fn tcp_syn_sent_timer_expiry_updates_connection_state_in_connection() {
    let source = read_tcp_source("src/transport/tcp/connection.rs");
    assert!(source.contains("self.state == TcpState::SynSent"));
    assert!(source.contains("pub(crate) fn on_tcp_timer_expiry("));
    assert!(source.contains("self.timer_set(TCP_TIMER_RETRANSMIT);"));
}

#[test]
fn established_node_delegates_timer_refresh_to_shared_helper() {
    // Source-level guard: after the timer-refresh dedup, the established node
    // must not iterate a raw `0..TCP_TIMER_COUNT` literal inline. The literal
    // legitimately remains in connection.rs (the const def + the shared
    // helper). Here we assert established.rs delegates rather than reopening
    // the cancel-or-update loop per-site.
    let source = read_tcp_source("src/transport/tcp/established.rs");
    assert!(
        !source.contains("0..crate::transport::tcp::connection::TCP_TIMER_COUNT")
            && !source.contains("0..TCP_TIMER_COUNT"),
        "established.rs must delegate timer refresh to the shared helper, \
         not iterate 0..TCP_TIMER_COUNT inline"
    );
}

#[test]
fn timer_refresh_loops_consolidated_into_shared_helper() {
    // The cancel-or-update body must live in one shared helper, not be
    // re-opened at each call site. Assert that rcv_process.rs and the TCP
    // session-protocol impl in mod.rs no longer carry the raw literal; only
    // connection.rs (const def + helper) keeps it.
    for path in [
        "src/transport/tcp/rcv_process.rs",
        "src/transport/tcp/mod.rs",
    ] {
        let source = read_tcp_source(path);
        assert!(
            !source.contains("0..crate::transport::tcp::connection::TCP_TIMER_COUNT")
                && !source.contains("0..TCP_TIMER_COUNT"),
            "{path} must delegate timer refresh to the shared helper, \
             not iterate 0..TCP_TIMER_COUNT inline"
        );
    }
    let connection = read_tcp_source("src/transport/tcp/connection.rs");
    assert!(
        connection.contains("pub const TCP_TIMER_COUNT"),
        "TCP_TIMER_COUNT const definition must remain in connection.rs"
    );
}

#[test]
fn input_path_session_slot_prefetch_is_wired() {
    // Source-level smoke guard: the established and rcv_process input nodes
    // must call `queue.prefetch_session(session_id)` after resolving the
    // session id and before the `session_mut` borrow, warming the
    // cache-cold session pool slot via the T3 `Pool::prefetch_slot`
    // pass-through. We assert the call is present; the underlying
    // `Pool::prefetch_slot` no-panic behavior is covered by hammer-infra's
    // own tests.
    for path in [
        "src/transport/tcp/established.rs",
        "src/transport/tcp/rcv_process.rs",
    ] {
        let source = read_tcp_source(path);
        assert!(
            source.contains("queue.prefetch_session(session_id)"),
            "{path} must warm the session slot via queue.prefetch_session \
             before the session_mut borrow"
        );
    }
    let runtime = read_tcp_source("src/session/runtime.rs");
    assert!(
        runtime.contains("pub(crate) fn prefetch_session"),
        "SessionDriverRuntime must expose the prefetch_session pass-through"
    );
}

fn assert_tcp_state_node_uses_frame_discipline(path: &str, node: &str) {
    let source = read_tcp_source(path);
    assert!(
        !source.contains(concat!("node_rewrite", "_frame!")),
        "{node} still uses scalar node_rewrite_frame! macro"
    );
    assert!(
        source.contains("frame.pending_indices()"),
        "{node} does not read BufferIndex values from frame.pending_indices()"
    );
    assert!(
        source.contains("runtime.prefetch_header"),
        "{node} does not prefetch buffer headers across frame chunks"
    );
    assert!(
        source.contains("NodeNextFrames"),
        "{node} does not write next frames through NodeNextFrames"
    );
    assert!(
        source.contains("NodeResult::drop"),
        "{node} does not consume the input frame via NodeResult::drop()"
    );
}

#[test]
fn tcp_established_uses_frame_discipline_not_rewrite_macro() {
    assert_tcp_state_node_uses_frame_discipline(
        "src/transport/tcp/established.rs",
        "tcp-established",
    );
}

#[test]
fn tcp_listen_uses_frame_discipline_not_rewrite_macro() {
    let source = read_tcp_source("src/transport/tcp/listen.rs");
    assert_tcp_state_node_uses_frame_discipline("src/transport/tcp/listen.rs", "tcp-listen");
    assert!(
        source.contains("tcp_established"),
        "tcp-listen does not forward accepted payload to tcp-established next frame"
    );
}

#[test]
fn tcp_syn_sent_uses_frame_discipline_not_rewrite_macro() {
    assert_tcp_state_node_uses_frame_discipline("src/transport/tcp/syn_sent.rs", "tcp-syn-sent");
}

#[test]
fn tcp_rcv_process_uses_frame_discipline_not_rewrite_macro() {
    assert_tcp_state_node_uses_frame_discipline(
        "src/transport/tcp/rcv_process.rs",
        "tcp-rcv-process",
    );
}
