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
    assert!(source.contains("pub(super) fn on_session_close("));
    assert!(source.contains("TcpState::Established"));
    assert!(source.contains("self.state = TcpState::FinWait1;"));
    assert!(source.contains("self.state = TcpState::LastAck;"));
}

#[test]
fn tcp_syn_sent_timer_expiry_updates_connection_state_in_connection() {
    let source = read_tcp_source("src/transport/tcp/connection.rs");
    assert!(source.contains("self.state == TcpState::SynSent"));
    assert!(source.contains("fn on_typed_timer_expiry("));
    assert!(source.contains("TcpTimerKind::Retransmit"));
}

#[test]
fn tcp_receive_hot_paths_do_not_clone_connections_for_timer_or_lookup_sync() {
    for path in [
        "src/transport/tcp/established.rs",
        "src/transport/tcp/rcv_process.rs",
    ] {
        let source = read_tcp_source(path);
        assert!(
            !source.contains(".clone();"),
            "{path} must not deep-clone TcpConnection on the receive hot path"
        );
    }

    let source = read_tcp_source("src/transport/tcp/mod.rs");
    let start = source
        .find("fn publish_tcp_connection")
        .expect("publish_tcp_connection");
    let end = source[start..]
        .find("\n}\n")
        .expect("publish_tcp_connection end");
    assert!(
        !source[start..start + end].contains(".clone()"),
        "lookup publication must borrow the worker-owned connection"
    );
    assert!(
        !source.contains("*const TcpConnection"),
        "timer synchronization must not bypass driver borrowing with a raw connection pointer"
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
