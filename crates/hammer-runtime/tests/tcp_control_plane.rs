use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hammer_core::log::{DiscardWriter, Level};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpConnectionKey, TcpControlPlaneAction,
    TcpNegotiatedOptions, TcpState, TcpTimerId, TcpTimerKind, TcpV4ConnectionKey, TcpWorkerEvent,
};
use hammer_runtime::protocol::tcp::TcpControlPlane;
use hammer_runtime::{ControlThread, MetricsRegistry};

fn run_control_thread(thread: ControlThread) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build control runtime");
        runtime.block_on(thread.run());
    })
}

#[test]
fn tcp_control_plane_tracks_connection_lifecycle_actions() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), |_| {});
    let connection = TcpConnectionId::new(21);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7000,
        Ipv4Addr::new(198, 51, 100, 21),
        40_000,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::SynSent,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install connection");
    assert_eq!(
        plane.connection_state_for_test(connection),
        Some(TcpState::SynSent)
    );

    plane
        .apply(TcpControlPlaneAction::TransitionConnection {
            connection_id: connection,
            state: TcpState::Established,
        })
        .expect("transition connection");
    assert_eq!(
        plane.connection_state_for_test(connection),
        Some(TcpState::Established)
    );

    plane
        .apply(TcpControlPlaneAction::CloseConnection {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        })
        .expect("close connection");
    assert_eq!(plane.connection_state_for_test(connection), None);

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_emits_connection_lifecycle_events() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let connection = TcpConnectionId::new(61);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7300,
        Ipv4Addr::new(198, 51, 100, 61),
        43_000,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::SynSent,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::SynSent,
        }
    );

    plane
        .apply(TcpControlPlaneAction::TransitionConnection {
            connection_id: connection,
            state: TcpState::Established,
        })
        .expect("transition connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive transition event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::Established,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ShutdownConnection {
            connection_id: connection,
            direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
            reason: TcpCloseReason::LocalShutdown,
        })
        .expect("shutdown connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive shutdown event"),
        TcpWorkerEvent::ShutdownObserved {
            connection_id: connection,
            direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
            reason: TcpCloseReason::LocalShutdown,
        }
    );

    plane
        .apply(TcpControlPlaneAction::CloseConnection {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        })
        .expect("close connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        }
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_upsert_connection_state_installs_and_transitions_atomically() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), |_| {});
    let connection = TcpConnectionId::new(81);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7400,
        Ipv4Addr::new(198, 51, 100, 81),
        44_000,
    ));

    plane
        .apply(TcpControlPlaneAction::UpsertConnectionState {
            connection_id: connection,
            key,
            state: TcpState::SynSent,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install through upsert");
    assert_eq!(
        plane.connection_state_for_test(connection),
        Some(TcpState::SynSent)
    );

    plane
        .apply(TcpControlPlaneAction::UpsertConnectionState {
            connection_id: connection,
            key,
            state: TcpState::Established,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("transition through upsert");
    assert_eq!(
        plane.connection_state_for_test(connection),
        Some(TcpState::Established)
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_shutdown_connection_is_idempotent_when_missing() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), |_| {});

    plane
        .apply(TcpControlPlaneAction::ShutdownConnection {
            connection_id: TcpConnectionId::new(91),
            direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
            reason: TcpCloseReason::LocalShutdown,
        })
        .expect("shutdown missing connection");

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_close_before_upsert_keeps_connection_closed() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let connection = TcpConnectionId::new(101);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7500,
        Ipv4Addr::new(198, 51, 100, 101),
        45_000,
    ));

    plane
        .apply(TcpControlPlaneAction::CloseConnection {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        })
        .expect("close missing connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        }
    );

    plane
        .apply(TcpControlPlaneAction::UpsertConnectionState {
            connection_id: connection,
            key,
            state: TcpState::Established,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("upsert closed connection");

    assert_eq!(plane.connection_state_for_test(connection), None);
    assert!(
        rx.recv_timeout(Duration::from_millis(120)).is_err(),
        "closed connection must not emit a late state-change upsert"
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_close_before_transition_ignores_late_transition() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let connection = TcpConnectionId::new(102);

    plane
        .apply(TcpControlPlaneAction::CloseConnection {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        })
        .expect("close missing connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        }
    );

    plane
        .apply(TcpControlPlaneAction::TransitionConnection {
            connection_id: connection,
            state: TcpState::Established,
        })
        .expect("ignore late transition after close");

    assert_eq!(plane.connection_state_for_test(connection), None);
    assert!(
        rx.recv_timeout(Duration::from_millis(120)).is_err(),
        "closed connection must not emit a late transition event"
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_close_cancels_all_armed_timers() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let connection = TcpConnectionId::new(103);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7600,
        Ipv4Addr::new(198, 51, 100, 103),
        46_000,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::Established,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::Established,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: connection,
            timer_id: TcpTimerId::new(7),
            kind: TcpTimerKind::Retransmit,
            timeout: Duration::from_millis(40),
        })
        .expect("arm retransmit timer");
    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: connection,
            timer_id: TcpTimerId::new(8),
            kind: TcpTimerKind::KeepAlive,
            timeout: Duration::from_millis(60),
        })
        .expect("arm keepalive timer");

    plane
        .apply(TcpControlPlaneAction::CloseConnection {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        })
        .expect("close connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::LocalRequest,
        }
    );

    assert!(
        rx.recv_timeout(Duration::from_millis(160)).is_err(),
        "timers must not fire after close"
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_clone_keeps_event_sink_alive() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let clone = plane.clone();
    drop(plane);

    let connection = TcpConnectionId::new(71);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7310,
        Ipv4Addr::new(198, 51, 100, 71),
        43_100,
    ));

    clone
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::SynSent,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install connection through cloned plane");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install event from cloned plane"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::SynSent,
        }
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_emits_timer_expiry_and_cancellation_from_control_thread() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward timer event");
    });

    let first = TcpConnectionId::new(31);
    let second = TcpConnectionId::new(32);
    let first_key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7100,
        Ipv4Addr::new(198, 51, 100, 31),
        41_000,
    ));
    let second_key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7200,
        Ipv4Addr::new(198, 51, 100, 32),
        42_000,
    ));

    for (connection, key) in [(first, first_key), (second, second_key)] {
        plane
            .apply(TcpControlPlaneAction::InstallConnection {
                connection_id: connection,
                key,
                state: TcpState::Established,
                capabilities: TcpCapabilities::default(),
                negotiated: TcpNegotiatedOptions::default(),
            })
            .expect("install connection");
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("receive install state event"),
            TcpWorkerEvent::StateChanged {
                connection_id: connection,
                key,
                state: TcpState::Established,
            }
        );
    }

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: first,
            timer_id: TcpTimerId::new(1),
            kind: TcpTimerKind::DelayedAck,
            timeout: Duration::from_millis(20),
        })
        .expect("arm delayed ack");

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: second,
            timer_id: TcpTimerId::new(2),
            kind: TcpTimerKind::Retransmit,
            timeout: Duration::from_millis(40),
        })
        .expect("arm retransmit");
    plane
        .apply(TcpControlPlaneAction::CancelTimer {
            connection_id: second,
            kind: TcpTimerKind::Retransmit,
        })
        .expect("cancel retransmit");

    let event = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive timer event");
    assert_eq!(
        event,
        TcpWorkerEvent::TimerExpired {
            connection_id: first,
            timer_id: TcpTimerId::new(1),
            kind: TcpTimerKind::DelayedAck,
        }
    );
    assert!(
        rx.recv_timeout(Duration::from_millis(120)).is_err(),
        "canceled retransmit timer must not fire"
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_terminal_timer_expiry_closes_connection_with_timeout_reason() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward timer event");
    });

    let cases = [
        (
            TcpConnectionId::new(111),
            TcpState::SynSent,
            TcpTimerId::new(11),
            TcpTimerKind::Connect,
            TcpCloseReason::ConnectTimeout,
            7111,
            41_111,
        ),
        (
            TcpConnectionId::new(112),
            TcpState::Established,
            TcpTimerId::new(12),
            TcpTimerKind::Retransmit,
            TcpCloseReason::RetransmitTimeout,
            7112,
            41_112,
        ),
        (
            TcpConnectionId::new(113),
            TcpState::Established,
            TcpTimerId::new(13),
            TcpTimerKind::KeepAlive,
            TcpCloseReason::KeepAliveTimeout,
            7113,
            41_113,
        ),
    ];

    for (connection, state, timer_id, kind, close_reason, local_port, remote_port) in cases {
        let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
            0,
            Ipv4Addr::new(127, 0, 0, 1),
            local_port,
            Ipv4Addr::new(198, 51, 100, connection.get() as u8),
            remote_port,
        ));

        plane
            .apply(TcpControlPlaneAction::InstallConnection {
                connection_id: connection,
                key,
                state,
                capabilities: TcpCapabilities::default(),
                negotiated: TcpNegotiatedOptions::default(),
            })
            .expect("install connection");
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("receive install state event"),
            TcpWorkerEvent::StateChanged {
                connection_id: connection,
                key,
                state,
            }
        );

        plane
            .apply(TcpControlPlaneAction::ArmTimer {
                connection_id: connection,
                timer_id,
                kind,
                timeout: Duration::from_millis(20),
            })
            .expect("arm terminal timer");

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("receive timer expiry event"),
            TcpWorkerEvent::TimerExpired {
                connection_id: connection,
                timer_id,
                kind,
            }
        );
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("receive close event"),
            TcpWorkerEvent::Closed {
                connection_id: connection,
                reason: close_reason,
            }
        );
        assert_eq!(plane.connection_state_for_test(connection), None);
    }

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_non_terminal_timer_expiry_keeps_connection_open() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward timer event");
    });
    let connection = TcpConnectionId::new(114);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7114,
        Ipv4Addr::new(198, 51, 100, 114),
        41_114,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::Established,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install state event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::Established,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: connection,
            timer_id: TcpTimerId::new(14),
            kind: TcpTimerKind::DelayedAck,
            timeout: Duration::from_millis(20),
        })
        .expect("arm delayed ack timer");

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive timer expiry event"),
        TcpWorkerEvent::TimerExpired {
            connection_id: connection,
            timer_id: TcpTimerId::new(14),
            kind: TcpTimerKind::DelayedAck,
        }
    );
    assert!(
        rx.recv_timeout(Duration::from_millis(120)).is_err(),
        "non-terminal timer expiry must not close the connection"
    );
    assert_eq!(
        plane.connection_state_for_test(connection),
        Some(TcpState::Established)
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_time_wait_timer_expiry_closes_connection() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward timer event");
    });
    let connection = TcpConnectionId::new(115);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7115,
        Ipv4Addr::new(198, 51, 100, 115),
        41_115,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::TimeWait,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install time-wait connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install state event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::TimeWait,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: connection,
            timer_id: TcpTimerId::new(15),
            kind: TcpTimerKind::TimeWait,
            timeout: Duration::from_millis(20),
        })
        .expect("arm time-wait timer");

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive timer expiry event"),
        TcpWorkerEvent::TimerExpired {
            connection_id: connection,
            timer_id: TcpTimerId::new(15),
            kind: TcpTimerKind::TimeWait,
        }
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::RemoteFin,
        }
    );
    assert_eq!(plane.connection_state_for_test(connection), None);

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_upsert_closed_state_emits_close_with_shutdown_reason() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let connection = TcpConnectionId::new(116);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7116,
        Ipv4Addr::new(198, 51, 100, 116),
        41_116,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::LastAck,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install last-ack connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install state event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::LastAck,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ShutdownConnection {
            connection_id: connection,
            direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
            reason: TcpCloseReason::LocalShutdown,
        })
        .expect("remember shutdown reason");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive shutdown event"),
        TcpWorkerEvent::ShutdownObserved {
            connection_id: connection,
            direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
            reason: TcpCloseReason::LocalShutdown,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: connection,
            timer_id: TcpTimerId::new(16),
            kind: TcpTimerKind::Retransmit,
            timeout: Duration::from_millis(80),
        })
        .expect("arm retransmit timer");
    assert!(plane.has_timer_for_test(connection, TcpTimerKind::Retransmit));

    plane
        .apply(TcpControlPlaneAction::UpsertConnectionState {
            connection_id: connection,
            key,
            state: TcpState::Closed,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("upsert closed state");

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::LocalShutdown,
        }
    );
    assert!(
        rx.recv_timeout(Duration::from_millis(120)).is_err(),
        "closed-state upsert must not emit an extra state-change event"
    );
    assert_eq!(plane.connection_state_for_test(connection), None);
    assert!(!plane.has_timer_for_test(connection, TcpTimerKind::Retransmit));

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_transition_closed_state_defaults_close_reason_and_cancels_timers() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let (tx, rx) = mpsc::channel();
    let plane = TcpControlPlane::new(Arc::clone(&control_handle), move |event| {
        tx.send(event).expect("forward lifecycle event");
    });
    let connection = TcpConnectionId::new(117);
    let key = TcpConnectionKey::V4(TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(127, 0, 0, 1),
        7117,
        Ipv4Addr::new(198, 51, 100, 117),
        41_117,
    ));

    plane
        .apply(TcpControlPlaneAction::InstallConnection {
            connection_id: connection,
            key,
            state: TcpState::TimeWait,
            capabilities: TcpCapabilities::default(),
            negotiated: TcpNegotiatedOptions::default(),
        })
        .expect("install time-wait connection");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive install state event"),
        TcpWorkerEvent::StateChanged {
            connection_id: connection,
            key,
            state: TcpState::TimeWait,
        }
    );

    plane
        .apply(TcpControlPlaneAction::ArmTimer {
            connection_id: connection,
            timer_id: TcpTimerId::new(17),
            kind: TcpTimerKind::TimeWait,
            timeout: Duration::from_millis(80),
        })
        .expect("arm time-wait timer");
    assert!(plane.has_timer_for_test(connection, TcpTimerKind::TimeWait));

    plane
        .apply(TcpControlPlaneAction::TransitionConnection {
            connection_id: connection,
            state: TcpState::Closed,
        })
        .expect("transition to closed state");

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1))
            .expect("receive close event"),
        TcpWorkerEvent::Closed {
            connection_id: connection,
            reason: TcpCloseReason::RemoteFin,
        }
    );
    assert!(
        rx.recv_timeout(Duration::from_millis(160)).is_err(),
        "closed-state transition must cancel timers and avoid extra events"
    );
    assert_eq!(plane.connection_state_for_test(connection), None);
    assert!(!plane.has_timer_for_test(connection, TcpTimerKind::TimeWait));

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}
