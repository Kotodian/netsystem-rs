use std::net::{Shutdown, SocketAddr};
use std::panic::{self, PanicHookInfo};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use hammer_core::error::HammerResult;
use hammer_core::log::DiscardWriter;
use hammer_core::protocol::tcp::{TcpCloseReason, TcpShutdownDirection};
use hammer_runtime::adapter::PlatformInterface;
use hammer_runtime::app::{AppBufferLease, AppSend};
use hammer_runtime::spawn::with_data_plane_buffers;
use hammer_service::RuntimeService;

struct NoopPlatform;

impl PlatformInterface for NoopPlatform {
    fn open_tun(&self, _options: hammer_runtime::adapter::TunOptions) -> HammerResult<i32> {
        Ok(42)
    }

    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }

    fn auto_detect_interface_control(&self, _fd: i32) -> HammerResult<()> {
        Ok(())
    }

    fn start_default_interface_monitor(
        &self,
        _listener: Arc<dyn hammer_runtime::adapter::DefaultInterfaceUpdateListener>,
    ) -> HammerResult<()> {
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _listener: Arc<dyn hammer_runtime::adapter::DefaultInterfaceUpdateListener>,
    ) -> HammerResult<()> {
        Ok(())
    }

    fn get_interfaces(&self) -> HammerResult<Vec<hammer_runtime::adapter::NetworkInterface>> {
        Ok(Vec::new())
    }

    fn read_wifi_state(&self) -> Option<hammer_runtime::adapter::WifiState> {
        None
    }
}

fn minimal_config() -> &'static str {
    r#"
[log]
level = "debug"

[tun]
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
auto_route = false
strict_route = true
mtu = 1400
stack = "disabled"

[dns]
server = "udp://1.1.1.1"

[[outbounds]]
type = "direct"
id = "direct"

[route]
final = "direct"
"#
}

fn new_test_service() -> Arc<RuntimeService> {
    RuntimeService::new(
        minimal_config(),
        Arc::new(NoopPlatform),
        Arc::new(DiscardWriter),
    )
    .expect("test service should build")
}

fn request_shutdown(
    app: &hammer_runtime::app::AppContext,
    flow: hammer_runtime::app::AppFlowId,
    how: Shutdown,
) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                worker
                    .runtime()
                    .shutdown(how)
                    .await
                    .expect("enqueue shutdown");
            })
            .await
            .expect("spawn flow task");
        })
}

fn request_send(
    app: &hammer_runtime::app::AppContext,
    flow: hammer_runtime::app::AppFlowId,
    payload: &[u8],
) {
    let payload = payload.to_vec();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async move {
            app.spawn_on_flow(flow, move |worker| async move {
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), &payload)
                    .expect("alloc tcp send buffer");
                worker
                    .runtime()
                    .send(AppSend::new(AppBufferLease::from_buffer(buffers, index)))
                    .await
                    .expect("enqueue send");
            })
            .await
            .expect("spawn flow task");
        })
}

fn wait_for(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(condition(), "condition did not become true before deadline");
}

fn panic_capture_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn panic_message(info: &PanicHookInfo<'_>) -> String {
    if let Some(message) = info.payload().downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    if let Some(message) = info.payload().downcast_ref::<String>() {
        return message.clone();
    }
    format!("{info}")
}

fn capture_panics<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
    let _guard = panic_capture_lock()
        .lock()
        .expect("panic capture lock poisoned");
    let messages = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&messages);
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        captured
            .lock()
            .expect("captured panic messages poisoned")
            .push(panic_message(info));
    }));
    let result = f();
    panic::set_hook(previous);
    let messages = match Arc::try_unwrap(messages) {
        Ok(messages) => messages
            .into_inner()
            .expect("captured panic messages poisoned"),
        Err(messages) => messages
            .lock()
            .expect("captured panic messages poisoned")
            .clone(),
    };
    (result, messages)
}

#[test]
fn service_tcp_connect_registers_requested_owner_worker() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_allocates_distinct_pending_flows() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let first = app.connect_tcp_stream(peer, 0).expect("first connect");
    let second = app.connect_tcp_stream(peer, 1).expect("second connect");

    assert_ne!(first, second);
    assert_eq!(app.owner_worker_for_flow(first).expect("first owner"), 0);
    assert_eq!(app.owner_worker_for_flow(second).expect("second owner"), 1);

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_shutdown_requests_are_consumed_and_observed_by_service() {
    let ((), panics) = capture_panics(|| {
        let service = new_test_service();
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

        request_shutdown(&app, flow, Shutdown::Write);

        wait_for(|| {
            service.tcp_shutdown_for_flow_for_test(flow)
                == Some((TcpShutdownDirection::Write, TcpCloseReason::LocalShutdown))
        });

        assert_eq!(
            service.tcp_shutdown_for_flow_for_test(flow),
            Some((TcpShutdownDirection::Write, TcpCloseReason::LocalShutdown))
        );

        service.close().expect("close service");
    });

    assert!(
        panics.iter().all(|message| {
            !message.contains("there is no reactor running")
                && !message.contains("must be called from the context of a Tokio 1.x runtime")
        }),
        "unexpected background panic(s): {panics:#?}"
    );
}

#[test]
fn service_tcp_connect_send_and_shutdown_requests_are_consumed_without_late_write_buffering() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_send(&app, flow, b"first-payload");

    wait_for(|| {
        service.tcp_pending_send_payload_lens_for_flow_for_test(flow)
            == vec![b"first-payload".len()]
    });
    assert_eq!(
        service.tcp_pending_send_payload_lens_for_flow_for_test(flow),
        vec![b"first-payload".len()]
    );

    request_shutdown(&app, flow, Shutdown::Write);
    wait_for(|| {
        service.tcp_shutdown_for_flow_for_test(flow)
            == Some((TcpShutdownDirection::Write, TcpCloseReason::LocalShutdown))
    });

    request_send(&app, flow, b"late-after-shutdown");
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(
        service.tcp_pending_send_payload_lens_for_flow_for_test(flow),
        vec![b"first-payload".len()]
    );
    assert_eq!(
        service.tcp_transport_send_payload_lens_for_flow_for_test(flow),
        vec![b"first-payload".len()]
    );
    assert_eq!(
        service.tcp_shutdown_for_flow_for_test(flow),
        Some((TcpShutdownDirection::Write, TcpCloseReason::LocalShutdown))
    );

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_send_is_staged_for_transport_but_not_dequeued_before_established() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_send(&app, flow, b"first-payload");

    wait_for(|| {
        service.tcp_pending_send_payload_lens_for_flow_for_test(flow)
            == vec![b"first-payload".len()]
    });
    wait_for(|| {
        service.tcp_transport_send_payload_lens_for_flow_for_test(flow)
            == vec![b"first-payload".len()]
    });

    assert_eq!(
        service.tcp_take_transport_send_payload_len_for_flow_for_test(flow),
        None,
        "transport dequeue must stay gated until the connection becomes send-ready"
    );
    assert_eq!(
        service.tcp_transport_send_payload_lens_for_flow_for_test(flow),
        vec![b"first-payload".len()]
    );

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_read_shutdown_keeps_send_path_open_until_write_shutdown() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_shutdown(&app, flow, Shutdown::Read);
    wait_for(|| {
        service.tcp_shutdown_for_flow_for_test(flow)
            == Some((TcpShutdownDirection::Read, TcpCloseReason::LocalShutdown))
    });

    request_send(&app, flow, b"send-after-read-shutdown");
    wait_for(|| {
        service.tcp_pending_send_payload_lens_for_flow_for_test(flow)
            == vec![b"send-after-read-shutdown".len()]
    });

    request_shutdown(&app, flow, Shutdown::Write);
    wait_for(|| {
        service.tcp_shutdown_for_flow_for_test(flow)
            == Some((TcpShutdownDirection::Both, TcpCloseReason::LocalShutdown))
    });

    request_send(&app, flow, b"late-after-write-shutdown");
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(
        service.tcp_pending_send_payload_lens_for_flow_for_test(flow),
        vec![b"send-after-read-shutdown".len()]
    );
    assert_eq!(
        service.tcp_shutdown_for_flow_for_test(flow),
        Some((TcpShutdownDirection::Both, TcpCloseReason::LocalShutdown))
    );

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_write_shutdown_remains_sticky_after_later_read_shutdown() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_shutdown(&app, flow, Shutdown::Write);
    wait_for(|| {
        service.tcp_shutdown_for_flow_for_test(flow)
            == Some((TcpShutdownDirection::Write, TcpCloseReason::LocalShutdown))
    });

    request_shutdown(&app, flow, Shutdown::Read);
    wait_for(|| {
        service.tcp_shutdown_for_flow_for_test(flow)
            == Some((TcpShutdownDirection::Both, TcpCloseReason::LocalShutdown))
    });

    request_send(&app, flow, b"must-not-buffer-after-write-close");
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty(),
        "write-close must remain terminal for buffered sends"
    );
    assert_eq!(
        service.tcp_shutdown_for_flow_for_test(flow),
        Some((TcpShutdownDirection::Both, TcpCloseReason::LocalShutdown))
    );

    service.close().expect("close service");
}
