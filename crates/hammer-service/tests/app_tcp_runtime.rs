use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hammer_adapter::{
    BufferPacketCursor, DataPlaneRuntime, Network, PlatformInterface, RouteMetadata, SocksAddr,
};
use hammer_core::error::HammerResult;
use hammer_core::log::DiscardWriter;
use hammer_runtime::app::{AppBufferLease, AppFlowId, AppObjectRef, AppOpcode, AppSend};
use hammer_runtime::spawn::with_data_plane_buffers;
use hammer_service::RuntimeService;
use hammer_service::app::AppIngressTarget;

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

#[test]
fn service_tcp_app_backend_delivers_recv_descriptor_into_service_app_flow() {
    let service = new_test_service();
    let app = service.app_context();
    let flow = AppFlowId::new(0x7001);
    let owner_worker = app
        .owner_worker_for_flow(flow)
        .expect("resolve app flow owner");
    let target = AppIngressTarget::new(app.clone(), flow);
    let expected_metadata = tcp_metadata();
    let deliver_payload = b"service-tcp-ingress".to_vec();
    let expected_payload = deliver_payload.clone();
    let deliver_metadata = expected_metadata.clone();

    let (tx, rx) = std::sync::mpsc::channel();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    service
        .spawn_app_on_worker(owner_worker, {
            let app = app.clone();
            move || async move {
                let result = app
                    .spawn_on_flow(flow, move |worker| async move {
                        let backend = worker.backend();
                        let recv_future = worker.runtime().recv();
                        let recv_sqe = backend
                            .next_sqe_descriptor()
                            .await
                            .expect("next recv sqe descriptor");
                        assert_eq!(recv_sqe.opcode(), hammer_runtime::app::AppOpcode::Recv);
                        ready_tx.send(()).expect("send ready signal");
                        let recv = recv_future.await.expect("recv cqe");
                        let payload = recv.lease().copy_current().expect("recv payload");
                        recv.release();
                        payload
                    })
                    .await
                    .expect("spawn flow recv task");
                tx.send(result).expect("send recv payload");
            }
        })
        .expect("spawn recv cqe task");

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("wait for recv sqe readiness");

    service
        .spawn_app_on_worker(owner_worker, move || {
            let target = target.clone();
            let payload = deliver_payload.clone();
            let metadata = deliver_metadata.clone();
            async move {
                let runtime = with_data_plane_buffers(|buffers| {
                    DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                        buffers.buffers().arena(),
                        16,
                        8,
                        buffers.instruction_set(),
                    )
                });
                let index = runtime
                    .alloc_index_with_bytes(metadata, &payload)
                    .expect("alloc TCP buffer");
                runtime
                    .get_buffer_mut(index)
                    .expect("buffer mut")
                    .set_packet_cursor(
                        BufferPacketCursor::new()
                            .with_packet_len(payload.len())
                            .with_network_header(0, 0)
                            .with_transport_header(0, 0)
                            .with_transport_payload_offset(0),
                    );
                target
                    .post_recv_cqe(&runtime, index)
                    .expect("complete ingress to app");
            }
        })
        .expect("spawn deliver task");

    let received = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive recv payload");
    assert_eq!(received, expected_payload);

    service.close().expect("close service");
}

#[test]
fn service_tcp_app_backend_rejects_descriptor_delivery_from_non_owner_worker() {
    let service = new_test_service();
    let app = service.app_context();
    let flow = AppFlowId::new(0x7003);
    let owner_worker = app
        .owner_worker_for_flow(flow)
        .expect("resolve app flow owner");
    let other_worker = (owner_worker + 1) % app.worker_count();
    let target = AppIngressTarget::new(app, flow);

    let (tx, rx) = std::sync::mpsc::channel();
    service
        .spawn_app_on_worker(other_worker, move || async move {
            let runtime = with_data_plane_buffers(|buffers| {
                DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                    buffers.buffers().arena(),
                    16,
                    8,
                    buffers.instruction_set(),
                )
            });
            let index = runtime
                .alloc_index_with_bytes(tcp_metadata(), b"owner-mismatch")
                .expect("alloc TCP buffer");
            let result = target.post_recv_cqe(&runtime, index);
            tx.send((
                result.map(|_| ()).map_err(|err| err.to_string()),
                runtime.in_use_buffers(),
            ))
            .expect("send result");
        })
        .expect("spawn non-owner task");

    let (result, buffers_after) = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive non-owner result");

    let err = result.expect_err("non-owner worker should fail");
    assert!(err.contains("owned by worker"), "unexpected error: {err}");
    assert_eq!(buffers_after, 1);

    service.close().expect("close service");
}

#[test]
fn service_tcp_app_target_enqueues_recv_cqe_descriptor_into_runtime_backend() {
    let service = new_test_service();
    let app = service.app_context();
    let flow = AppFlowId::new(0x7005);
    let owner_worker = app
        .owner_worker_for_flow(flow)
        .expect("resolve app flow owner");
    let target = AppIngressTarget::new(app.clone(), flow);
    let expected_payload = b"service-tcp-descriptor".to_vec();
    let expected_metadata = tcp_metadata();
    let deliver_payload = expected_payload.clone();
    let deliver_metadata = expected_metadata.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    let recv_thread = std::thread::spawn({
        let app = app.clone();
        move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime")
                .block_on(async {
                    app.spawn_on_flow(flow, move |worker| async move {
                        let backend = worker.backend();
                        let recv_future = worker.runtime().recv();
                        let recv_sqe = backend
                            .next_sqe_descriptor()
                            .await
                            .expect("next recv sqe descriptor");
                        assert_eq!(recv_sqe.opcode(), hammer_runtime::app::AppOpcode::Recv);
                        ready_tx.send(()).expect("send ready signal");
                        let descriptor = backend
                            .next_cqe_descriptor()
                            .await
                            .expect("next cqe descriptor");
                        drop(recv_future);
                        let payload_len = descriptor.result();
                        (descriptor, payload_len)
                    })
                    .await
                    .expect("spawn app flow")
                })
        }
    });

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("wait for recv sqe readiness");

    service
        .spawn_app_on_worker(owner_worker, move || {
            let target = target.clone();
            let payload = deliver_payload.clone();
            let metadata = deliver_metadata.clone();
            async move {
                let runtime = with_data_plane_buffers(|buffers| {
                    DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                        buffers.buffers().arena(),
                        16,
                        8,
                        buffers.instruction_set(),
                    )
                });
                let index = runtime
                    .alloc_index_with_bytes(metadata, &payload)
                    .expect("alloc TCP buffer");
                runtime
                    .get_buffer_mut(index)
                    .expect("buffer mut")
                    .set_packet_cursor(
                        BufferPacketCursor::new()
                            .with_packet_len(payload.len())
                            .with_network_header(0, 0)
                            .with_transport_header(0, 0)
                            .with_transport_payload_offset(0),
                    );
                target
                    .post_recv_cqe(&runtime, index)
                    .expect("complete ingress");
            }
        })
        .expect("spawn deliver task");

    let received = recv_thread.join().expect("join recv thread");

    assert_eq!(
        received.0.user_data(),
        hammer_runtime::app::AppUserData::new(0)
    );
    assert_eq!(received.0.result(), expected_payload.len() as i32);
    assert_eq!(
        received.0.object(),
        hammer_runtime::app::AppObjectRef::Flow(flow)
    );
    assert!(
        received
            .0
            .flags()
            .contains(hammer_runtime::app::AppCqeFlags::BUFFER)
    );
    match received.0.payload() {
        hammer_runtime::app::AppCqeData::Recv {
            flow: recv_flow,
            buffer,
        } => {
            assert_eq!(recv_flow, flow);
            let _ = buffer;
        }
        other => panic!("expected recv descriptor payload, got {other:?}"),
    }
    assert_eq!(received.1, expected_payload.len() as i32);

    service.close().expect("close service");
}

#[test]
fn service_tcp_app_send_stays_in_owner_ring_for_session_node_polling() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");
    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
    let _owner = app.owner_worker_for_flow(flow).expect("flow owner");

    let payload = b"session-node-owned-send".to_vec();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), &payload)
                    .expect("alloc app send buffer");
                worker
                    .runtime()
                    .send(AppSend::new(AppBufferLease::from_buffer(buffers, index)))
                    .await
                    .expect("submit app send");
            })
            .await
            .expect("spawn flow send task");
        });

    std::thread::sleep(Duration::from_millis(50));
    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty(),
        "service pump must not consume app SQEs"
    );

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let app_runtime = worker.runtime();
                let backend = worker.backend();
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), b"session-node-owned-send")
                    .expect("alloc app send buffer");
                app_runtime
                    .send(AppSend::new(AppBufferLease::from_buffer(buffers, index)))
                    .await
                    .expect("submit app send");
                backend
                    .try_pop_sqe_descriptor()
                    .expect("session node visible sqe descriptor")
            })
            .await
            .expect("spawn on flow owner")
        });

    assert_eq!(descriptor.opcode(), AppOpcode::Send);
    assert_eq!(descriptor.object(), AppObjectRef::Flow(flow));

    service.close().expect("close service");
}

fn tcp_metadata() -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(
            "198.51.100.41".parse().expect("src ip"),
            40_041,
        )),
        destination: Some(SocksAddr::ip("192.0.2.41".parse().expect("dst ip"), 443)),
        ..Default::default()
    }
}
