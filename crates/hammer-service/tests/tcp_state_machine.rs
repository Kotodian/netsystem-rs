#[test]
fn tcp_sources_do_not_expose_connection_mutation_hooks() {
    let sources = [
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/state_machine.rs"),
    ];
    let forbidden = [
        concat!("machine", "_"),
        concat!("set", "_state"),
        concat!("set", "_sequence", "_state"),
        concat!("set", "_send", "_state"),
        concat!("set", "_receive", "_state"),
        concat!("connection", "_mut"),
        concat!("option", "_state", "_mut"),
        concat!("congestion", "_mut"),
        concat!("retransmit", "_timeout", "_mut"),
        concat!("accept", "_in", "_order", "_payload"),
    ];

    for source in sources {
        for symbol in forbidden {
            assert!(!source.contains(symbol), "{symbol}");
        }
    }
}

#[test]
fn tcp_sources_do_not_expose_extra_transition_shapes() {
    let sources = [
        include_str!("../src/transport/tcp/state_machine.rs"),
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/listen.rs"),
        include_str!("../src/transport/tcp/syn_sent.rs"),
        include_str!("../src/transport/tcp/rcv_process.rs"),
        include_str!("../src/transport/tcp/established.rs"),
    ];
    let forbidden = [
        concat!("Tcp", "Active", "Open"),
        concat!("Tcp", "Output", "Send", "View"),
        concat!("Tcp", "State", "Segment"),
        concat!("Tcp", "State", "Transition"),
        concat!("Tcp", "State", "Machine", "Output"),
        concat!("Dis", "position"),
        concat!("enter", "_"),
    ];

    for source in sources {
        for symbol in forbidden {
            assert!(!source.contains(symbol), "{symbol}");
        }
    }
}

#[test]
fn tcp_state_machine_structs_do_not_expose_session_or_app_types() {
    let source = include_str!("../src/transport/tcp/state_machine.rs");
    let forbidden = [
        "AppOpId",
        "AppRingHandle",
        "SessionId",
        "SessionQueue",
        "BufferIndex",
        "BufferFrame",
        "alloc_tcp_segment",
    ];

    for symbol in forbidden {
        assert!(!source.contains(symbol), "{symbol}");
    }
}
