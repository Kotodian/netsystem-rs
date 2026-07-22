use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use hammer_plugin_tcp::{
    TcpCapabilities, TcpCloseReason, TcpControlPlane, TcpControlPlaneAction, TcpListenerId,
    TcpListenerKey,
};
use hammer_runtime::{ControlThread, log::Level};

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
    let (control_handle, control_thread) =
        ControlThread::new(std::time::Instant::now(), Level::Info);
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
        accurate_ecn: false,
        fast_open: false,
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
fn tcp_control_plane_handles_share_listener_state() {
    let (control_handle, control_thread) =
        ControlThread::new(std::time::Instant::now(), Level::Info);
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
        accurate_ecn: false,
        fast_open: false,
    };

    let shared_plane = plane.clone();
    shared_plane
        .apply(TcpControlPlaneAction::InstallListener {
            listener_id,
            listener,
            capabilities,
        })
        .expect("install listener through shared handle");
    assert_eq!(
        plane.listener_for_test(listener_id),
        Some((listener, capabilities))
    );

    assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
    join.join().expect("control thread join");
}
