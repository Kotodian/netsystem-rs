use std::fs;
use std::path::Path;

fn read_tcp_source(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).expect("read tcp source")
}

#[test]
fn tcp_state_machine_public_api_has_no_forbidden_middle_types() {
    let sources = [
        read_tcp_source("src/transport/tcp/connection.rs"),
        read_tcp_source("src/transport/tcp/state_machine.rs"),
        read_tcp_source("src/transport/tcp/session.rs"),
        read_tcp_source("src/transport/tcp/listen.rs"),
        read_tcp_source("src/transport/tcp/syn_sent.rs"),
        read_tcp_source("src/transport/tcp/established.rs"),
        read_tcp_source("src/transport/tcp/syn_rcvd.rs"),
        read_tcp_source("src/transport/tcp/close_wait.rs"),
        read_tcp_source("src/transport/tcp/fin_wait1.rs"),
        read_tcp_source("src/transport/tcp/fin_wait2.rs"),
        read_tcp_source("src/transport/tcp/closing.rs"),
        read_tcp_source("src/transport/tcp/last_ack.rs"),
        read_tcp_source("src/transport/tcp/time_wait.rs"),
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
        concat!("Disposition"),
        concat!("Effect"),
    ];

    for pattern in forbidden {
        assert!(
            !sources.contains(pattern),
            "forbidden TCP state-machine helper remains: {pattern}"
        );
    }
}

#[test]
fn packet_nodes_do_not_drive_tcp_queue_state() {
    let sources = [
        read_tcp_source("src/transport/tcp/listen.rs"),
        read_tcp_source("src/transport/tcp/syn_sent.rs"),
        read_tcp_source("src/transport/tcp/syn_rcvd.rs"),
        read_tcp_source("src/transport/tcp/established.rs"),
        read_tcp_source("src/transport/tcp/close_wait.rs"),
        read_tcp_source("src/transport/tcp/fin_wait1.rs"),
        read_tcp_source("src/transport/tcp/fin_wait2.rs"),
        read_tcp_source("src/transport/tcp/closing.rs"),
        read_tcp_source("src/transport/tcp/last_ack.rs"),
        read_tcp_source("src/transport/tcp/time_wait.rs"),
    ]
    .join("\n");

    let forbidden = [
        concat!("put", "_connection"),
        concat!("next", ".state()"),
        concat!("indexed", ".state()"),
        concat!("take_connection", "::"),
        concat!("TcpState::Closed"),
        concat!("TcpState::Established"),
        concat!("match next"),
        concat!("match connection"),
        concat!("TcpConnectionState::"),
    ];

    for pattern in forbidden {
        assert!(
            !sources.contains(pattern),
            "packet node still drives TCP state or queue policy: {pattern}"
        );
    }
}

#[test]
fn tcp_timer_dispatch_is_owned_by_tcp_state() {
    let source = read_tcp_source("src/transport/tcp/session.rs");
    assert!(!source.contains("match state"));
    assert!(!source.contains("TcpConnectionState::SynSent"));
    assert!(!source.contains("on_retransmit_timeout"));
    assert!(!source.contains("retransmit_syn_header_if_ready"));
    assert!(source.contains("on_tcp_timer_expiry"));
}

#[test]
fn tcp_input_has_dedicated_receive_nodes() {
    let source = read_tcp_source("src/transport/tcp/mod.rs");

    assert!(!source.contains("RcvProcess"));
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
            source.contains(next),
            "TcpInputNext is missing dedicated state node {next}"
        );
    }
}
