use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use hammer_core::log::{DiscardWriter, Level};
use hammer_runtime::protocol::tcp::{TcpConnectionId, TcpControlTimerSet, TcpTimerKind};
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
fn tcp_timer_set_replaces_existing_timer_for_same_connection_and_kind() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let timers = TcpControlTimerSet::new(Arc::clone(&control_handle));
    let connection = TcpConnectionId::new(7);
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));

    timers
        .arm_once(
            connection,
            TcpTimerKind::DelayedAck,
            Duration::from_millis(60),
            {
                let first = Arc::clone(&first);
                move || {
                    let first = Arc::clone(&first);
                    async move {
                        first.fetch_add(1, Ordering::SeqCst);
                    }
                }
            },
        )
        .expect("arm first delayed ack");

    std::thread::sleep(Duration::from_millis(10));

    timers
        .arm_once(
            connection,
            TcpTimerKind::DelayedAck,
            Duration::from_millis(20),
            {
                let second = Arc::clone(&second);
                move || {
                    let second = Arc::clone(&second);
                    async move {
                        second.fetch_add(1, Ordering::SeqCst);
                    }
                }
            },
        )
        .expect("replace delayed ack");

    std::thread::sleep(Duration::from_millis(90));

    assert_eq!(first.load(Ordering::SeqCst), 0);
    assert_eq!(second.load(Ordering::SeqCst), 1);
    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_timer_set_cancel_prevents_future_fire() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let timers = TcpControlTimerSet::new(Arc::clone(&control_handle));
    let connection = TcpConnectionId::new(11);
    let fired = Arc::new(AtomicUsize::new(0));

    timers
        .arm_once(
            connection,
            TcpTimerKind::Retransmit,
            Duration::from_millis(40),
            {
                let fired = Arc::clone(&fired);
                move || {
                    let fired = Arc::clone(&fired);
                    async move {
                        fired.fetch_add(1, Ordering::SeqCst);
                    }
                }
            },
        )
        .expect("arm rto");

    assert!(timers.cancel(connection, TcpTimerKind::Retransmit));
    std::thread::sleep(Duration::from_millis(80));

    assert_eq!(fired.load(Ordering::SeqCst), 0);
    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}
