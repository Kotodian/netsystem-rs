#[test]
fn session_modules_do_not_own_or_dispatch_transport_timers() {
    let source = [
        include_str!("../src/session/protocol.rs"),
        include_str!("../src/session/runtime.rs"),
    ]
    .join("\n");
    for forbidden in [
        "TimerWheel",
        "timer_wheel",
        "ExpiredTimer",
        "pending_timers",
        "handle_expired_timer",
        "handle_legacy_timer",
        "poll_once_for_ticks",
        "TcpConnection",
        "TcpTimer",
    ] {
        assert!(
            !source.contains(forbidden),
            "session owns forbidden {forbidden}"
        );
    }
}

#[test]
fn legacy_tcp_queue_and_timer_reconciliation_surfaces_are_removed() {
    let source = [
        include_str!("../src/transport/tcp/mod.rs"),
        include_str!("../src/transport/tcp/connection.rs"),
        include_str!("../src/transport/tcp/established.rs"),
        include_str!("../src/transport/tcp/rcv_process.rs"),
    ]
    .join("\n");
    for forbidden in [
        "type TcpQueue",
        "SessionQueueProtocol",
        "sync_all_tcp_timers",
        "sync_tcp_timer",
        "TCP_TIMER_COUNT",
        "pub const TCP_TIMER_",
        "active_timer_mask",
        "fn custom_tx",
    ] {
        assert!(
            !source.contains(forbidden),
            "TCP retains forbidden {forbidden}"
        );
    }
}
