use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hammer_core::log::{DiscardWriter, Level};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpCloseReason, TcpControlPlaneAction, TcpListenerId, TcpListenerKey,
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
fn tcp_control_plane_tracks_listener_install_and_remove() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let plane = TcpControlPlane::new(Arc::clone(&control_handle));
    let listener_id = TcpListenerId::new(7);
    let listener = TcpListenerKey::v4(3, Ipv4Addr::new(127, 0, 0, 1), 7000);
    let capabilities = TcpCapabilities {
        max_segment_size: Some(1400),
        window_scale: Some(6),
        sack: true,
        timestamps: true,
        ecn: false,
    };

    assert_eq!(plane.listener_for_test(listener_id), None);
    plane
        .apply(TcpControlPlaneAction::InstallListener {
            listener_id,
            listener,
            capabilities,
        })
        .expect("install listener");
    assert_eq!(
        plane.listener_for_test(listener_id),
        Some((listener, capabilities))
    );

    plane
        .apply(TcpControlPlaneAction::RemoveListener {
            listener_id,
            reason: TcpCloseReason::LocalRequest,
        })
        .expect("remove listener");
    assert_eq!(plane.listener_for_test(listener_id), None);

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}

#[test]
fn tcp_control_plane_clone_shares_listener_state() {
    let (control_handle, control_thread) = ControlThread::new(
        Instant::now(),
        Arc::new(DiscardWriter),
        MetricsRegistry::new(),
        Duration::from_secs(60),
        Level::Info,
    );
    let join = run_control_thread(control_thread);
    let plane = TcpControlPlane::new(Arc::clone(&control_handle));
    let clone = plane.clone();
    let listener_id = TcpListenerId::new(11);
    let first_listener =
        TcpListenerKey::v6(9, Ipv6Addr::new(0x2001, 0xdb8, 0, 11, 0, 0, 0, 1), 8443);
    let second_listener =
        TcpListenerKey::v6(9, Ipv6Addr::new(0x2001, 0xdb8, 0, 11, 0, 0, 0, 2), 9443);
    let first_capabilities = TcpCapabilities {
        max_segment_size: Some(1280),
        ..TcpCapabilities::default()
    };
    let second_capabilities = TcpCapabilities {
        max_segment_size: Some(1360),
        sack: true,
        ..TcpCapabilities::default()
    };

    plane
        .apply(TcpControlPlaneAction::InstallListener {
            listener_id,
            listener: first_listener,
            capabilities: first_capabilities,
        })
        .expect("install listener from original handle");
    assert_eq!(
        clone.listener_for_test(listener_id),
        Some((first_listener, first_capabilities))
    );

    clone
        .apply(TcpControlPlaneAction::InstallListener {
            listener_id,
            listener: second_listener,
            capabilities: second_capabilities,
        })
        .expect("replace listener from cloned handle");
    assert_eq!(
        plane.listener_for_test(listener_id),
        Some((second_listener, second_capabilities))
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}
