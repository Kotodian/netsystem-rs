use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use std::vec::Vec;

use hammer_adapter::{
    BufferPacketCursor, DataPlaneRuntime, Network, PlatformInterface, RouteMetadata, SocksAddr,
};
use hammer_core::error::HammerResult;
use hammer_core::log::DiscardWriter;
use hammer_runtime::app::AppFlowId;
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
fn service_udp_app_target_delivers_recv_descriptor_into_service_app_flow() {
    let service = new_test_service();
    let app = service.app_context();
    let flow = AppFlowId::new(0x8100);
    let owner_worker = app
        .owner_worker_for_flow(flow)
        .expect("resolve app flow owner");
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 11),
        40_011,
        Ipv4Addr::new(192, 0, 2, 11),
        9_999,
        b"service-udp-app",
    );
    let expected_metadata = udp_metadata();
    let deliver_metadata = expected_metadata.clone();
    let expected_packet = packet.clone();

    let (tx, rx) = std::sync::mpsc::channel();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    service
        .spawn_app_on_worker(owner_worker, {
            let app = app.clone();
            move || async move {
                let payload = app
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
                tx.send(payload).expect("send recv payload");
            }
        })
        .expect("spawn recv cqe task");

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("wait for recv sqe readiness");

    service
        .spawn_app_on_worker(owner_worker, move || async move {
            let runtime = with_data_plane_buffers(|buffers| {
                DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                    buffers.buffers().arena(),
                    16,
                    8,
                    buffers.instruction_set(),
                )
            });
            let index = runtime
                .alloc_index_with_bytes(deliver_metadata, &packet)
                .expect("alloc UDP buffer");
            stamp_udp_cursor(&runtime, index, &packet);
            AppIngressTarget::new(app.clone(), flow)
                .post_recv_cqe(&runtime, index)
                .expect("complete UDP ingress to app");
        })
        .expect("spawn deliver task");

    let received = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive recv payload");
    assert_eq!(received, expected_packet);

    service.close().expect("close service");
}

#[test]
fn service_udp_app_target_rejects_descriptor_delivery_from_non_owner_worker() {
    let service = new_test_service();
    let app = service.app_context();
    let flow = AppFlowId::new(0x8101);
    let owner_worker = app
        .owner_worker_for_flow(flow)
        .expect("resolve app flow owner");
    let other_worker = (owner_worker + 1) % app.worker_count();
    let target = AppIngressTarget::new(app, flow);
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 31),
        40_031,
        Ipv4Addr::new(192, 0, 2, 31),
        9_999,
        b"owner-mismatch",
    );
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
                .alloc_index_with_bytes(udp_metadata_for_ports(40_031, 9_999), &packet)
                .expect("alloc UDP buffer");
            stamp_udp_cursor(&runtime, index, &packet);
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

fn stamp_udp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, header_len)
                .with_transport_header(header_len, 8)
                .with_transport_payload_offset(header_len + 8),
        );
}

fn udp_metadata() -> RouteMetadata {
    udp_metadata_for_ports(40_011, 9_999)
}

fn udp_metadata_for_ports(source_port: u16, destination_port: u16) -> RouteMetadata {
    RouteMetadata {
        network: Network::Udp,
        source: Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 11)),
            source_port,
        )),
        destination: Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11)),
            destination_port,
        )),
        ..Default::default()
    }
}

fn ipv4_udp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    packet
}
