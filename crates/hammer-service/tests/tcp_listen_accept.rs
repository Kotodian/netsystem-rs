use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hammer_adapter::{BufferPacketCursor, DataPlaneRuntime, DataWorkerId, RouteMetadata};
use hammer_core::error::{CoreError, CoreResult, HammerError, HammerResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionKey, TcpListenerId, TcpListenerKey, TcpWorkerEvent,
};
use hammer_runtime::app::{
    AppContext, AppControl, AppControlBackend, AppCqeData, AppFlowId, AppObjectRef, AppOpcode,
    AppSocketId, AppSqeData, AppSqeDescriptor, AppUserData,
};
use hammer_runtime::spawn::DataRuntime;
use hammer_service::data_plane::DropNode;
use hammer_service::transport::tcp::{
    TcpAcceptBackend, TcpAcceptControlPlane, TcpAcceptNext, TcpAcceptRegistration,
    TcpEstablishedNext, TcpEstablishedNode, TcpInputControlPlane, TcpInputNext, TcpListenNext,
    TcpListenNode, TcpLookupId, TcpResetNext, TcpResetNode, TcpV4ListenerKey, TcpWorkerOwnedState,
};

const LISTENER_ID: u32 = 11;

#[derive(Clone)]
struct TestAppControlBackend {
    next_socket: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for TestAppControlBackend {
    fn default() -> Self {
        Self {
            next_socket: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
}

impl AppControlBackend for TestAppControlBackend {
    fn bind_tcp_listener(
        &self,
        _app: &AppContext,
        _bind: std::net::SocketAddr,
        _owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        Ok(AppSocketId::new(
            self.next_socket
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ))
    }

    fn bind_udp_socket(
        &self,
        _app: &AppContext,
        _bind: std::net::SocketAddr,
        _owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        Err(HammerError::internal(
            "udp bind is not used in tcp listen/accept tests",
        ))
    }

    fn close_socket(&self, _app: &AppContext, _socket: AppSocketId) -> HammerResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct RecordingTcpAcceptBackend {
    accepted_flow: AppFlowId,
    records: Arc<Mutex<Vec<(TcpLookupId, std::net::SocketAddr, std::net::SocketAddr)>>>,
    events: Arc<Mutex<Vec<TcpWorkerEvent>>>,
}

impl TcpAcceptBackend for RecordingTcpAcceptBackend {
    fn accept(
        &self,
        listener_id: TcpLookupId,
        registration: &TcpAcceptRegistration,
        remote: std::net::SocketAddr,
        local: std::net::SocketAddr,
        event: TcpWorkerEvent,
    ) -> CoreResult<()> {
        registration
            .app()
            .try_complete_accept(registration.listener(), self.accepted_flow)
            .map_err(|err| CoreError::internal(format!("complete accept cqe: {err}")))?;
        self.records
            .lock()
            .map_err(|_| CoreError::internal("accept records poisoned"))?
            .push((listener_id, remote, local));
        self.events
            .lock()
            .map_err(|_| CoreError::internal("accept events poisoned"))?
            .push(event);
        Ok(())
    }
}

#[test]
fn tcp_listen_accept_recovers_socket_addrs_from_packet_when_route_metadata_is_missing() {
    let data_runtime = DataRuntime::new(1, "tcp-listen-accept-metadata-fallback", 512 * 1024, 2)
        .expect("data runtime");
    let data_context = data_runtime.context();
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    app.install_control(AppControl::new(Arc::new(TestAppControlBackend::default())))
        .expect("install app control");
    let listener = app
        .bind_tcp_listener("127.0.0.1:7443".parse().expect("listener bind"), 0)
        .expect("bind listener");
    let accepted_flow = AppFlowId::new(0x7443);
    let accepted_records = Arc::new(Mutex::new(Vec::new()));
    let accepted_events = Arc::new(Mutex::new(Vec::new()));
    let accept_backend = Arc::new(RecordingTcpAcceptBackend {
        accepted_flow,
        records: Arc::clone(&accepted_records),
        events: Arc::clone(&accepted_events),
    });

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            data_context
                .spawn_local_on_worker(0, move || async move {
                    let listener_backend = app
                        .local_backend_for_socket(listener)
                        .expect("listener backend");
                    listener_backend
                        .try_push_sqe_descriptor(AppSqeDescriptor::new(
                            AppOpcode::Accept,
                            AppUserData::new(77),
                            AppObjectRef::Socket(listener),
                            AppSqeData::Accept,
                        ))
                        .expect("push accept sqe");

                    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
                    let drop = runtime.nodes().register_internal(DropNode::new());
                    let accept_control =
                        TcpAcceptControlPlane::new(accept_backend, TcpAcceptNext::nodes(drop));
                    accept_control
                        .publish_listeners([(
                            LISTENER_ID,
                            TcpAcceptRegistration::new(app.clone(), listener),
                        )])
                        .expect("publish tcp accept listener");
                    let accept = runtime.nodes().register_internal(accept_control.node());
                    let listen = runtime
                        .nodes()
                        .register_internal(TcpListenNode::new(TcpListenNext::nodes(accept)));
                    let reset = runtime
                        .nodes()
                        .register_internal(TcpResetNode::new(TcpResetNext::nodes(drop, drop)));
                    let established = runtime.nodes().register_internal(TcpEstablishedNode::new(
                        TcpEstablishedNext::nodes(drop),
                    ));
                    let tcp_control = TcpInputControlPlane::new(TcpInputNext::nodes(
                        drop,
                        drop,
                        listen,
                        drop,
                        drop,
                        established,
                        reset,
                    ));
                    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(0));
                    owner.insert_listener_v4(
                        TcpV4ListenerKey::new(0, Ipv4Addr::new(127, 0, 0, 1), 7443),
                        LISTENER_ID,
                    );
                    tcp_control
                        .publish_lookup(owner.publish_snapshot())
                        .expect("publish listener lookup");
                    let tcp_input = runtime.nodes().register_internal(tcp_control.node());

                    let packet = ipv4_tcp_packet(
                        Ipv4Addr::new(198, 51, 100, 74),
                        40_743,
                        Ipv4Addr::new(127, 0, 0, 1),
                        7443,
                        tcp_flags(false, true, false, false),
                        b"accept",
                    );
                    let frame = runtime.alloc_frame_index().expect("alloc frame");
                    let buffer = push_packet(&runtime, frame, &packet, RouteMetadata::default());
                    stamp_tcp_cursor(&runtime, buffer, &packet);
                    assert!(runtime.schedule_frame(tcp_input, frame).expect("schedule"));
                    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);

                    let accept_cqe = listener_backend
                        .next_cqe_descriptor()
                        .await
                        .expect("accept cqe");
                    tx.send(accept_cqe).expect("send accept cqe");
                })
                .expect("spawn worker-local accept task");
            tokio::time::timeout(Duration::from_secs(1), rx)
                .await
                .expect("wait accept cqe")
                .expect("receive accept cqe")
        });

    match result.payload() {
        AppCqeData::Accepted {
            listener: cqe_listener,
            flow,
        } => {
            assert_eq!(cqe_listener, listener);
            assert_eq!(flow, accepted_flow);
        }
        other => panic!("unexpected accept completion payload: {other:?}"),
    }
    assert_eq!(
        *accepted_records.lock().expect("accepted records"),
        vec![(
            LISTENER_ID,
            "198.51.100.74:40743".parse().expect("remote"),
            "127.0.0.1:7443".parse().expect("local"),
        )]
    );
    assert_eq!(
        *accepted_events.lock().expect("accepted events"),
        vec![TcpWorkerEvent::IncomingConnection {
            listener_id: TcpListenerId::new(LISTENER_ID as u64),
            listener: TcpListenerKey::v4(0, Ipv4Addr::new(127, 0, 0, 1), 7443),
            key: TcpConnectionKey::v4(
                0,
                Ipv4Addr::new(127, 0, 0, 1),
                7443,
                Ipv4Addr::new(198, 51, 100, 74),
                40_743,
            ),
            capabilities: TcpCapabilities::default(),
        }]
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

fn push_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    metadata: RouteMetadata,
) -> hammer_adapter::BufferIndex {
    let buffer = runtime
        .alloc_index_with_bytes(metadata, packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
    buffer
}

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let network_header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let tcp_offset = network_header_len;
    let tcp_header_len = ((packet[tcp_offset + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(tcp_offset, tcp_header_len)
                .with_transport_payload_offset(tcp_offset + tcp_header_len),
        );
}

fn ipv4_tcp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(
        &mut packet[20..],
        source_port,
        destination_port,
        flags,
        payload,
    );
    let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
    packet[36..38].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv4_packet(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    payload_len: usize,
) -> Vec<u8> {
    let total_len = 20 + payload_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = protocol;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet
}

fn write_tcp_segment(
    segment: &mut [u8],
    source_port: u16,
    destination_port: u16,
    flags: u8,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = flags;
    segment[20..].copy_from_slice(payload);
}

fn tcp_flags(fin: bool, syn: bool, rst: bool, ack: bool) -> u8 {
    u8::from(fin) | (u8::from(syn) << 1) | (u8::from(rst) << 2) | (u8::from(ack) << 4)
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn update_ipv4_header_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = match chunk {
            [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
            [hi] => u16::from_be_bytes([*hi, 0]) as u32,
            _ => unreachable!(),
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
