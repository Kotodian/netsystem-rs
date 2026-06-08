use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, BufferNodeError, BufferPacketCursor, DataPlaneRuntime, IcmpErrorMetadata,
    InternalNode, Network, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData, RouteMetadata,
    SocksAddr, TraceControlPlane, TraceInputPolicy, TracePolicy,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::icmp::IcmpErrorFamily;
use hammer_runtime::app::{AppContext, AppFlowId};
use hammer_runtime::spawn::DataRuntime;
use hammer_service::data_plane::DropNode;
use hammer_service::net::{IpLocalControlPlane, IpLocalNext};
use hammer_service::transport::udp::input::UdpAppRegistration;
use hammer_service::transport::udp::{
    UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace,
};
const REGISTERED_PORT: u16 = 5353;
const PUNT_PORT: u16 = 1900;
const UNKNOWN_PORT: u16 = 65_000;
const APP_PORT: u16 = 9_999;

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    metadata: Vec<RouteMetadata>,
    node_errors: Vec<Option<BufferNodeError>>,
    frame_lens: Vec<usize>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states()
            .lock()
            .expect("capture state registry poisoned");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture state slot"),
        }
    }
}

impl Node for CaptureNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "capture node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CaptureNode {}

fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn capture_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = {
        let states = capture_states()
            .lock()
            .map_err(|_| CoreError::internal("capture state registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("capture state slot is invalid"))?,
        )
    };
    state
        .lock()
        .map_err(|_| CoreError::internal("capture state poisoned"))?
        .frame_lens
        .push(frame.pending_len());
    for index in frame.drain_pending() {
        let packet = runtime.copy_current_chain(index)?;
        let metadata = runtime.metadata(index)?;
        let node_error = runtime.node_error(index)?;
        let mut state = state
            .lock()
            .map_err(|_| CoreError::internal("capture state poisoned"))?;
        state.packets.push(packet.into_iter().collect());
        state.metadata.push(metadata);
        state.node_errors.push(node_error);
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode,
{
    let _ = node;
}

#[test]
fn udp_input_dispatches_registered_ipv4_and_ipv6_ports() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = UdpGraph::new(&runtime);
    graph
        .udp_control
        .register_port(REGISTERED_PORT, graph.service)
        .expect("register UDP service port");
    let local_control = IpLocalControlPlane::new(IpLocalNext::nodes(
        graph.drop,
        graph.punt,
        graph.drop,
        graph.udp_input,
        graph.drop,
        graph.drop,
    ));
    let local = runtime.nodes().register_internal(local_control.node());
    let ipv4 = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        10_001,
        Ipv4Addr::new(192, 0, 2, 1),
        REGISTERED_PORT,
        b"registered-v4",
    );
    let ipv6 = ipv6_udp_packet(
        "2001:db8::1".parse().expect("IPv6 source"),
        10_002,
        "2001:db8::2".parse().expect("IPv6 destination"),
        REGISTERED_PORT,
        b"registered-v6",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &ipv4, RouteMetadata::default());
    push_packet(&runtime, frame, &ipv6, RouteMetadata::default());

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_capture_packets(&graph.service_state, &[ipv4.clone(), ipv6.clone()]);
    assert!(graph.punt_state.lock().unwrap().packets.is_empty());
    assert!(graph.icmp_error_state.lock().unwrap().packets.is_empty());
    let state = graph.service_state.lock().unwrap();
    assert_metadata(
        &state.metadata[0],
        Ipv4Addr::new(10, 0, 0, 1).into(),
        10_001,
        Ipv4Addr::new(192, 0, 2, 1).into(),
        REGISTERED_PORT,
    );
    assert_metadata(
        &state.metadata[1],
        "2001:db8::1".parse().expect("IPv6 source"),
        10_002,
        "2001:db8::2".parse().expect("IPv6 destination"),
        REGISTERED_PORT,
    );
    assert_eq!(state.node_errors, vec![None, None]);
    assert!(
        state
            .metadata
            .iter()
            .all(|metadata| metadata.icmp_error.is_none())
    );
    drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn udp_input_routes_registered_punt_port_without_icmp_metadata() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = UdpGraph::new(&runtime);
    graph
        .udp_control
        .register_punt_port(PUNT_PORT)
        .expect("register UDP punt port");
    let trace_control = TraceControlPlane::new(4);
    trace_control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: graph.udp_input,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(trace_control.handle()), 2);
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 3),
        10_003,
        Ipv4Addr::new(192, 0, 2, 3),
        PUNT_PORT,
        b"punt",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_udp_packet(
        &runtime,
        frame,
        graph.udp_input,
        &packet,
        UdpFlow::new(
            Ipv4Addr::new(10, 0, 0, 3).into(),
            10_003,
            Ipv4Addr::new(192, 0, 2, 3).into(),
            PUNT_PORT,
        ),
    );

    assert!(
        runtime
            .schedule_frame(graph.udp_input, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_capture_packets(&graph.punt_state, &[packet]);
    assert!(graph.service_state.lock().unwrap().packets.is_empty());
    assert!(graph.icmp_error_state.lock().unwrap().packets.is_empty());
    let punt_state = graph.punt_state.lock().unwrap();
    assert_eq!(punt_state.node_errors, vec![None]);
    assert_eq!(punt_state.metadata[0].icmp_error, None);
    assert_eq!(trace_control.drain_completed(), 1);
    let records = trace_control.take_records();
    let trace =
        UdpInputTrace::decode(&records[0].entries[0].payload_bytes).expect("UDP input trace");
    assert_eq!(trace.destination_port, Some(PUNT_PORT));
    assert_eq!(trace.error, None);
    assert_eq!(trace.next, graph.punt);
    drop(punt_state);
    assert_eq!(
        runtime
            .node_error_count(graph.udp_input, UdpInputError::UnknownPort.code())
            .expect("unknown counter"),
        0
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn udp_input_routes_unknown_port_to_icmp_error() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = UdpGraph::new(&runtime);
    let ipv4 = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 4),
        10_004,
        Ipv4Addr::new(192, 0, 2, 4),
        UNKNOWN_PORT,
        b"unknown-v4",
    );
    let ipv6 = ipv6_udp_packet(
        "2001:db8::4".parse().expect("IPv6 source"),
        10_005,
        "2001:db8::5".parse().expect("IPv6 destination"),
        UNKNOWN_PORT,
        b"unknown-v6",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_udp_packet(
        &runtime,
        frame,
        &ipv4,
        Ipv4Addr::new(10, 0, 0, 4).into(),
        10_004,
        Ipv4Addr::new(192, 0, 2, 4).into(),
        UNKNOWN_PORT,
    );
    push_udp_packet(
        &runtime,
        frame,
        &ipv6,
        "2001:db8::4".parse().expect("IPv6 source"),
        10_005,
        "2001:db8::5".parse().expect("IPv6 destination"),
        UNKNOWN_PORT,
    );

    assert!(
        runtime
            .schedule_frame(graph.udp_input, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_capture_packets(&graph.icmp_error_state, &[ipv4, ipv6]);
    assert!(graph.service_state.lock().unwrap().packets.is_empty());
    assert!(graph.punt_state.lock().unwrap().packets.is_empty());
    let state = graph.icmp_error_state.lock().unwrap();
    assert_eq!(
        state.node_errors,
        vec![
            Some(BufferNodeError::new(
                graph.udp_input,
                UdpInputError::UnknownPort.code()
            )),
            Some(BufferNodeError::new(
                graph.udp_input,
                UdpInputError::UnknownPort.code()
            )),
        ]
    );
    assert_icmp_error(state.metadata[0].icmp_error, IcmpErrorFamily::Ipv4, 3, 3, 0);
    assert_icmp_error(state.metadata[1].icmp_error, IcmpErrorFamily::Ipv6, 1, 4, 0);
    drop(state);
    assert_eq!(
        runtime
            .node_error_count(graph.udp_input, UdpInputError::UnknownPort.code())
            .expect("unknown counter"),
        2
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn udp_input_drops_packet_without_ip_local_cursor() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = UdpGraph::new(&runtime);
    graph
        .udp_control
        .register_port(REGISTERED_PORT, graph.service)
        .expect("register UDP service port");
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 5),
        10_005,
        Ipv4Addr::new(192, 0, 2, 5),
        REGISTERED_PORT,
        b"missing-cursor",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &packet,
        udp_metadata(
            Ipv4Addr::new(10, 0, 0, 5).into(),
            10_005,
            Ipv4Addr::new(192, 0, 2, 5).into(),
            REGISTERED_PORT,
        ),
    );

    assert!(
        runtime
            .schedule_frame(graph.udp_input, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(graph.service_state.lock().unwrap().packets.is_empty());
    assert!(graph.punt_state.lock().unwrap().packets.is_empty());
    assert!(graph.icmp_error_state.lock().unwrap().packets.is_empty());
    assert_eq!(
        runtime
            .node_error_count(graph.udp_input, UdpInputError::BadLength.code())
            .expect("bad length counter"),
        1
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn udp_input_unregister_port_falls_back_to_icmp_error() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = UdpGraph::new(&runtime);
    graph
        .udp_control
        .register_port(REGISTERED_PORT, graph.service)
        .expect("register UDP service port");
    graph
        .udp_control
        .register_punt_port(PUNT_PORT)
        .expect("register UDP punt port");

    let registered = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 6),
        10_006,
        Ipv4Addr::new(192, 0, 2, 6),
        REGISTERED_PORT,
        b"before-unregister-service",
    );
    let punted = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 7),
        10_007,
        Ipv4Addr::new(192, 0, 2, 7),
        PUNT_PORT,
        b"before-unregister-punt",
    );
    let first = runtime.alloc_frame_index().expect("alloc first frame");
    push_udp_packet(
        &runtime,
        first,
        &registered,
        Ipv4Addr::new(10, 0, 0, 6).into(),
        10_006,
        Ipv4Addr::new(192, 0, 2, 6).into(),
        REGISTERED_PORT,
    );
    push_udp_packet(
        &runtime,
        first,
        &punted,
        Ipv4Addr::new(10, 0, 0, 7).into(),
        10_007,
        Ipv4Addr::new(192, 0, 2, 7).into(),
        PUNT_PORT,
    );
    assert!(
        runtime
            .schedule_frame(graph.udp_input, first)
            .expect("schedule registered")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run registered"), 3);
    assert_capture_packets(&graph.service_state, &[registered]);
    assert_capture_packets(&graph.punt_state, &[punted]);
    assert!(graph.icmp_error_state.lock().unwrap().packets.is_empty());

    graph
        .udp_control
        .unregister_port(REGISTERED_PORT)
        .expect("unregister UDP service port");
    graph
        .udp_control
        .unregister_port(PUNT_PORT)
        .expect("unregister UDP punt port");

    let unregistered_service = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 8),
        10_008,
        Ipv4Addr::new(192, 0, 2, 8),
        REGISTERED_PORT,
        b"after-unregister-service",
    );
    let unregistered_punt = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 9),
        10_009,
        Ipv4Addr::new(192, 0, 2, 9),
        PUNT_PORT,
        b"after-unregister-punt",
    );
    let second = runtime.alloc_frame_index().expect("alloc second frame");
    push_udp_packet(
        &runtime,
        second,
        &unregistered_service,
        Ipv4Addr::new(10, 0, 0, 8).into(),
        10_008,
        Ipv4Addr::new(192, 0, 2, 8).into(),
        REGISTERED_PORT,
    );
    push_udp_packet(
        &runtime,
        second,
        &unregistered_punt,
        Ipv4Addr::new(10, 0, 0, 9).into(),
        10_009,
        Ipv4Addr::new(192, 0, 2, 9).into(),
        PUNT_PORT,
    );
    assert!(
        runtime
            .schedule_frame(graph.udp_input, second)
            .expect("schedule unregistered")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run unregistered"), 2);
    assert_eq!(graph.service_state.lock().unwrap().packets.len(), 1);
    assert_eq!(graph.punt_state.lock().unwrap().packets.len(), 1);
    assert_capture_packets(
        &graph.icmp_error_state,
        &[unregistered_service, unregistered_punt],
    );
    let state = graph.icmp_error_state.lock().unwrap();
    for metadata in &state.metadata {
        assert_icmp_error(metadata.icmp_error, IcmpErrorFamily::Ipv4, 3, 3, 0);
    }
    drop(state);
    assert_eq!(
        runtime
            .node_error_count(graph.udp_input, UdpInputError::UnknownPort.code())
            .expect("unknown counter"),
        2
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn udp_input_dispatches_selected_port_into_runtime_app_flow() {
    let data_runtime =
        DataRuntime::new(1, "udp-app-input-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 11),
        40_011,
        Ipv4Addr::new(192, 0, 2, 11),
        APP_PORT,
        b"app-first-touch",
    );
    let flow = AppFlowId::new(0x55aa);
    let received = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let recv_task = tokio::spawn({
                let app = app.clone();
                async move {
                    app.spawn_on_flow(flow, move |worker| async move {
                        let backend = worker.backend();
                        let recv_future = worker.runtime().recv();
                        let recv_sqe = backend
                            .next_sqe_descriptor()
                            .await
                            .expect("next recv sqe descriptor");
                        assert_eq!(recv_sqe.opcode(), hammer_runtime::app::AppOpcode::Recv);
                        ready_tx.send(()).expect("send recv-ready signal");
                        let recv = tokio::time::timeout(Duration::from_secs(1), recv_future)
                            .await
                            .expect("app recv timeout")
                            .expect("app recv");
                        let payload = recv.lease().copy_current().expect("copy app payload");
                        let metadata = recv
                            .lease()
                            .runtime()
                            .metadata(recv.lease().index())
                            .expect("app metadata");
                        let cursor = recv
                            .lease()
                            .runtime()
                            .packet_cursor(recv.lease().index())
                            .expect("app cursor");
                        recv.release();
                        (payload, metadata, cursor, worker.owner_worker())
                    })
                    .await
                    .expect("spawn recv flow task")
                }
            });

            tokio::time::timeout(Duration::from_secs(1), ready_rx)
                .await
                .expect("recv ready timeout")
                .expect("recv ready closed");

            data_runtime
                .context()
                .for_each_worker({
                    let packet = packet.clone();
                    let app = app.clone();
                    move |_| {
                        let runtime = hammer_runtime::spawn::with_data_plane_buffers(|buffers| {
                            DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                                buffers.buffers().arena(),
                                16,
                                8,
                                buffers.instruction_set(),
                            )
                        });
                        let graph = UdpGraph::new(&runtime);
                        graph
                            .udp_control
                            .register_app(APP_PORT, UdpAppRegistration::new(app.clone(), flow))
                            .expect("register UDP app port");
                        let frame = runtime.alloc_frame_index().expect("alloc frame");
                        push_udp_packet(
                            &runtime,
                            frame,
                            &packet,
                            Ipv4Addr::new(10, 0, 0, 11).into(),
                            40_011,
                            Ipv4Addr::new(192, 0, 2, 11).into(),
                            APP_PORT,
                        );
                        assert!(
                            runtime
                                .schedule_frame(graph.udp_input, frame)
                                .expect("schedule")
                        );
                        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
                        assert!(graph.service_state.lock().unwrap().packets.is_empty());
                        assert!(graph.punt_state.lock().unwrap().packets.is_empty());
                        assert!(graph.icmp_error_state.lock().unwrap().packets.is_empty());
                    }
                })
                .expect("run UDP input on worker");
            recv_task.await.expect("join recv task")
        });

    assert_eq!(received.0, packet);
    assert_metadata(
        &received.1,
        Ipv4Addr::new(10, 0, 0, 11).into(),
        40_011,
        Ipv4Addr::new(192, 0, 2, 11).into(),
        APP_PORT,
    );
    assert_eq!(received.1.icmp_error, None);
    assert_eq!(received.2.packet_len(), packet.len());
    assert_eq!(received.2.transport_header_offset(), 20);
    assert_eq!(received.2.transport_header_len(), 8);
    assert_eq!(received.2.transport_payload_offset(), 28);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn udp_input_app_dispatch_releases_runtime_buffers_after_app_recv_release() {
    let data_runtime =
        DataRuntime::new(1, "udp-app-buffer-lifetime", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 21),
        40_021,
        Ipv4Addr::new(192, 0, 2, 21),
        APP_PORT,
        b"lease-lifetime",
    );
    let flow = AppFlowId::new(0x55ab);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let recv_task = tokio::spawn({
                let app = app.clone();
                async move {
                    app.spawn_on_flow(flow, move |worker| async move {
                        let backend = worker.backend();
                        let recv_future = worker.runtime().recv();
                        let recv_sqe = backend
                            .next_sqe_descriptor()
                            .await
                            .expect("next recv sqe descriptor");
                        assert_eq!(recv_sqe.opcode(), hammer_runtime::app::AppOpcode::Recv);
                        ready_tx.send(()).expect("send recv-ready signal");
                        let recv = tokio::time::timeout(Duration::from_secs(1), recv_future)
                            .await
                            .expect("app recv timeout")
                            .expect("app recv");
                        let before_release = recv.lease().runtime().in_use_buffers();
                        recv.release();
                        let after_release = worker
                            .spawn_local(move || async move {
                                hammer_runtime::spawn::with_data_plane_buffers(|runtime| {
                                    runtime.in_use_buffers()
                                })
                            })
                            .await
                            .expect("inspect after release");
                        (before_release, after_release, worker.owner_worker())
                    })
                    .await
                    .expect("spawn recv flow task")
                }
            });

            tokio::time::timeout(Duration::from_secs(1), ready_rx)
                .await
                .expect("recv ready timeout")
                .expect("recv ready closed");

            data_runtime
                .context()
                .for_each_worker({
                    let packet = packet.clone();
                    let app = app.clone();
                    move |_| {
                        let runtime = hammer_runtime::spawn::with_data_plane_buffers(|buffers| {
                            DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                                buffers.buffers().arena(),
                                16,
                                8,
                                buffers.instruction_set(),
                            )
                        });
                        let graph = UdpGraph::new(&runtime);
                        graph
                            .udp_control
                            .register_app(APP_PORT, UdpAppRegistration::new(app.clone(), flow))
                            .expect("register UDP app port");
                        let frame = runtime.alloc_frame_index().expect("alloc frame");
                        push_udp_packet(
                            &runtime,
                            frame,
                            &packet,
                            Ipv4Addr::new(10, 0, 0, 21).into(),
                            40_021,
                            Ipv4Addr::new(192, 0, 2, 21).into(),
                            APP_PORT,
                        );
                        assert!(
                            runtime
                                .schedule_frame(graph.udp_input, frame)
                                .expect("schedule")
                        );
                        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
                        assert_eq!(runtime.frames_in_use(), 0);
                        assert_eq!(runtime.in_use_buffers(), 1);
                    }
                })
                .expect("run UDP input on worker");
            let (before_release, after_release, owner_worker) =
                recv_task.await.expect("join recv task");
            assert_eq!(owner_worker, 0);
            assert_eq!(before_release, 1);
            assert_eq!(after_release, 0);
        });

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn udp_input_app_dispatch_rejects_non_owner_worker_without_copying_packet() {
    let data_runtime =
        DataRuntime::new(2, "udp-app-non-owner", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 31),
        40_031,
        Ipv4Addr::new(192, 0, 2, 31),
        APP_PORT,
        b"owner-mismatch",
    );
    let flow = AppFlowId::new(0x55ab);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let recv_task = tokio::spawn({
                let app = app.clone();
                async move {
                    app.spawn_on_flow(flow, move |worker| async move {
                        let backend = worker.backend();
                        let recv_future = worker.runtime().recv();
                        let recv_sqe = backend
                            .next_sqe_descriptor()
                            .await
                            .expect("next recv sqe descriptor");
                        assert_eq!(recv_sqe.opcode(), hammer_runtime::app::AppOpcode::Recv);
                        ready_tx.send(()).expect("send recv-ready signal");
                        let recv = tokio::time::timeout(Duration::from_secs(1), recv_future)
                            .await
                            .expect("app recv timeout")
                            .expect("app recv");
                        recv.release();
                        worker.owner_worker()
                    })
                    .await
                    .expect("spawn recv flow task")
                }
            });

            tokio::time::timeout(Duration::from_secs(1), ready_rx)
                .await
                .expect("recv ready timeout")
                .expect("recv ready closed");

            let results = data_runtime
                .context()
                .for_each_worker({
                    let packet = packet.clone();
                    let app = app.clone();
                    move |worker| {
                        let runtime = hammer_runtime::spawn::with_data_plane_buffers(|buffers| {
                            DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
                                buffers.buffers().arena(),
                                16,
                                8,
                                buffers.instruction_set(),
                            )
                        });
                        let graph = UdpGraph::new(&runtime);
                        graph
                            .udp_control
                            .register_app(APP_PORT, UdpAppRegistration::new(app.clone(), flow))
                            .expect("register UDP app port");
                        let frame = runtime.alloc_frame_index().expect("alloc frame");
                        push_udp_packet(
                            &runtime,
                            frame,
                            &packet,
                            Ipv4Addr::new(10, 0, 0, 31).into(),
                            40_031,
                            Ipv4Addr::new(192, 0, 2, 31).into(),
                            APP_PORT,
                        );
                        assert!(
                            runtime
                                .schedule_frame(graph.udp_input, frame)
                                .expect("schedule")
                        );
                        let result = runtime.run_ready_nodes();
                        (
                            worker,
                            result,
                            runtime.frames_in_use(),
                            runtime.in_use_buffers(),
                        )
                    }
                })
                .expect("run UDP input on workers");
            let owner_worker = recv_task.await.expect("join recv task");
            let non_owner = (owner_worker + 1) % 2;
            assert!(results[non_owner].1.is_err());
            assert_eq!(results[non_owner].2, 0);
            assert_eq!(results[non_owner].3, 0);
            assert!(results[owner_worker].1.is_ok());
            assert_eq!(results[owner_worker].2, 0);
            assert_eq!(results[owner_worker].3, 1);
        });

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

struct UdpGraph {
    drop: NodeId,
    punt: NodeId,
    service: NodeId,
    udp_input: NodeId,
    udp_control: UdpInputControlPlane,
    punt_state: Arc<Mutex<CaptureState>>,
    service_state: Arc<Mutex<CaptureState>>,
    icmp_error_state: Arc<Mutex<CaptureState>>,
}

impl UdpGraph {
    fn new(runtime: &DataPlaneRuntime) -> Self {
        let punt_state = Arc::new(Mutex::new(CaptureState::default()));
        let service_state = Arc::new(Mutex::new(CaptureState::default()));
        let icmp_error_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let punt = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
        let service = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&service_state)));
        let icmp_error = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&icmp_error_state)));
        let udp_control = UdpInputControlPlane::new(UdpInputNext::nodes(drop, punt, icmp_error));
        let udp_node: UdpInputNode = udp_control.node();
        assert_internal_node(&udp_node);
        let udp_input = runtime.nodes().register_internal(udp_node);
        Self {
            drop,
            punt,
            service,
            udp_input,
            udp_control,
            punt_state,
            service_state,
            icmp_error_state,
        }
    }
}

fn push_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    metadata: RouteMetadata,
) {
    let buffer = runtime
        .alloc_index_with_bytes(metadata, packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

fn push_marked_udp_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    trace_input: hammer_adapter::NodeId,
    packet: &[u8],
    flow: UdpFlow,
) {
    let buffer = runtime
        .alloc_index_with_bytes(flow.metadata(), packet)
        .expect("alloc packet");
    stamp_udp_cursor(runtime, buffer, packet);
    runtime
        .try_mark_trace(trace_input, buffer)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

fn push_udp_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) {
    let flow = UdpFlow::new(source, source_port, destination, destination_port);
    let buffer = runtime
        .alloc_index_with_bytes(flow.metadata(), packet)
        .expect("alloc packet");
    stamp_udp_cursor(runtime, buffer, packet);
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

fn stamp_udp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let (packet_len, network_header_len) = match packet.first().map(|byte| byte >> 4) {
        Some(4) => {
            let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
            let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
            (packet_len, header_len)
        }
        Some(6) => {
            let payload_len = u16::from_be_bytes([packet[4], packet[5]]) as usize;
            (40 + payload_len, 40)
        }
        _ => panic!("UDP test packet must be IPv4 or IPv6"),
    };
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(network_header_len, 8)
                .with_transport_payload_offset(network_header_len + 8),
        );
}

#[derive(Debug, Clone, Copy)]
struct UdpFlow {
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
}

impl UdpFlow {
    fn new(source: IpAddr, source_port: u16, destination: IpAddr, destination_port: u16) -> Self {
        Self {
            source,
            source_port,
            destination,
            destination_port,
        }
    }

    fn metadata(self) -> RouteMetadata {
        udp_metadata(
            self.source,
            self.source_port,
            self.destination,
            self.destination_port,
        )
    }
}

fn udp_metadata(
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) -> RouteMetadata {
    RouteMetadata {
        network: Network::Udp,
        source: Some(SocksAddr::ip(source, source_port)),
        destination: Some(SocksAddr::ip(destination, destination_port)),
        ..RouteMetadata::default()
    }
}

fn assert_capture_packets(state: &Arc<Mutex<CaptureState>>, expected: &[Vec<u8>]) {
    assert_eq!(state.lock().unwrap().packets, expected);
}

fn assert_metadata(
    metadata: &RouteMetadata,
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) {
    assert_eq!(metadata.network, Network::Udp);
    assert_eq!(metadata.source, Some(SocksAddr::ip(source, source_port)));
    assert_eq!(
        metadata.destination,
        Some(SocksAddr::ip(destination, destination_port))
    );
}

fn assert_icmp_error(
    metadata: Option<IcmpErrorMetadata>,
    family: IcmpErrorFamily,
    icmp_type: u8,
    code: u8,
    data: u32,
) {
    let metadata = metadata.expect("ICMP error metadata");
    assert_eq!(metadata.family(), family);
    assert_eq!(metadata.icmp_type(), icmp_type);
    assert_eq!(metadata.code(), code);
    assert_eq!(metadata.data(), data);
}

fn ipv4_udp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 17, 8 + payload.len());
    let udp = 20;
    packet[udp..udp + 2].copy_from_slice(&source_port.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&destination_port.to_be_bytes());
    packet[udp + 4..udp + 6].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[udp + 8..].copy_from_slice(payload);
    let checksum = ipv4_l4_checksum(source, destination, 17, &packet[udp..]);
    packet[udp + 6..udp + 8].copy_from_slice(&udp_checksum_wire(checksum).to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_udp_packet(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 17, 8 + payload.len());
    let udp = 40;
    packet[udp..udp + 2].copy_from_slice(&source_port.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&destination_port.to_be_bytes());
    packet[udp + 4..udp + 6].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[udp + 8..].copy_from_slice(payload);
    let checksum = ipv6_l4_checksum(source, destination, 17, &packet[udp..]);
    packet[udp + 6..udp + 8].copy_from_slice(&udp_checksum_wire(checksum).to_be_bytes());
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

fn ipv6_packet(
    source: Ipv6Addr,
    destination: Ipv6Addr,
    protocol: u8,
    payload_len: usize,
) -> Vec<u8> {
    let mut packet = vec![0u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = protocol;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet
}

fn update_ipv4_header_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len());
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(40 + segment.len());
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, protocol]);
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn udp_checksum_wire(checksum: u16) -> u16 {
    if checksum == 0 { 0xffff } else { checksum }
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
