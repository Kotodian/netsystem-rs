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
