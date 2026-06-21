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
    assert!(source.contains("self.on_tcp_timer(kind)"));
    assert!(source.contains("pub(crate) fn on_tcp_timer("));
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
    assert!(source.contains("pub(crate) fn on_tcp_timer("));
    assert!(source.contains("self.state == TcpState::SynSent"));
    assert!(source.contains("pub(crate) fn on_tcp_timer_expiry("));
    assert!(source.contains("self.tcp_timer_set(timer);"));
}
