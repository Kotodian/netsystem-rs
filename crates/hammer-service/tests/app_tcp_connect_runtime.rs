use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use hammer_core::error::HammerResult;
use hammer_core::log::DiscardWriter;
use hammer_runtime::adapter::PlatformInterface;
use hammer_runtime::app::{AppBufferLease, AppObjectRef, AppOpcode, AppSend};
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

fn drain_send_payloads(
    app: &hammer_runtime::app::AppContext,
    flow: hammer_runtime::app::AppFlowId,
) -> Vec<Vec<u8>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let mut payloads = Vec::new();
                while let Some(entry) = backend.try_pop_submission_entry() {
                    let (descriptor, registered) = entry.into_parts();
                    assert_eq!(descriptor.opcode(), AppOpcode::Send);
                    assert_eq!(descriptor.object(), AppObjectRef::Flow(flow));
                    let registered = registered.expect("send entry attachment");
                    let payload = registered
                        .lease()
                        .copy_current()
                        .expect("copy send payload");
                    let (_index, lease) = registered.into_parts();
                    lease.release();
                    payloads.push(payload.into_iter().collect());
                }
                payloads
            })
            .await
            .expect("spawn flow task")
        })
}

fn drain_shutdowns(
    app: &hammer_runtime::app::AppContext,
    flow: hammer_runtime::app::AppFlowId,
) -> Vec<Shutdown> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let mut shutdowns = Vec::new();
                while let Some(shutdown) = backend.try_pop_tcp_shutdown() {
                    assert_eq!(shutdown.flow(), flow);
                    shutdowns.push(shutdown.how());
                }
                shutdowns
            })
            .await
            .expect("spawn flow task")
        })
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
fn service_tcp_connect_shutdown_request_stays_in_owner_ring_for_session_node() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_shutdown(&app, flow, Shutdown::Write);
    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(service.tcp_shutdown_for_flow_for_test(flow), None);
    assert_eq!(drain_shutdowns(&app, flow), vec![Shutdown::Write]);

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_send_and_shutdown_requests_stay_in_owner_ring() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_send(&app, flow, b"first-payload");
    request_shutdown(&app, flow, Shutdown::Write);
    request_send(&app, flow, b"late-after-shutdown");
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty()
    );
    assert!(
        service
            .tcp_transport_send_payload_lens_for_flow_for_test(flow)
            .is_empty()
    );
    assert_eq!(service.tcp_shutdown_for_flow_for_test(flow), None);
    assert_eq!(drain_shutdowns(&app, flow), vec![Shutdown::Write]);
    assert_eq!(
        drain_send_payloads(&app, flow),
        vec![b"first-payload".to_vec(), b"late-after-shutdown".to_vec()]
    );

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_send_is_left_for_session_node_not_service_transport_queue() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_send(&app, flow, b"first-payload");
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty()
    );
    assert!(
        service
            .tcp_transport_send_payload_lens_for_flow_for_test(flow)
            .is_empty()
    );
    assert_eq!(
        service.tcp_take_transport_send_payload_len_for_flow_for_test(flow),
        None,
        "service transport queue must not receive app send before session node polling"
    );
    assert_eq!(
        drain_send_payloads(&app, flow),
        vec![b"first-payload".to_vec()]
    );

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_read_and_write_shutdowns_remain_ordered_in_owner_ring() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_shutdown(&app, flow, Shutdown::Read);
    request_send(&app, flow, b"send-after-read-shutdown");
    request_shutdown(&app, flow, Shutdown::Write);
    request_send(&app, flow, b"late-after-write-shutdown");
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty()
    );
    assert_eq!(service.tcp_shutdown_for_flow_for_test(flow), None);
    assert_eq!(
        drain_shutdowns(&app, flow),
        vec![Shutdown::Read, Shutdown::Write]
    );
    assert_eq!(
        drain_send_payloads(&app, flow),
        vec![
            b"send-after-read-shutdown".to_vec(),
            b"late-after-write-shutdown".to_vec(),
        ]
    );

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_multiple_shutdowns_remain_in_owner_ring_for_session_node() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    request_shutdown(&app, flow, Shutdown::Write);
    request_shutdown(&app, flow, Shutdown::Read);
    request_send(&app, flow, b"must-not-buffer-after-write-close");
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty(),
        "service pump must not consume app sends after app shutdown"
    );
    assert_eq!(service.tcp_shutdown_for_flow_for_test(flow), None);
    assert_eq!(
        drain_shutdowns(&app, flow),
        vec![Shutdown::Write, Shutdown::Read]
    );
    assert_eq!(
        drain_send_payloads(&app, flow),
        vec![b"must-not-buffer-after-write-close".to_vec()]
    );

    service.close().expect("close service");
}
