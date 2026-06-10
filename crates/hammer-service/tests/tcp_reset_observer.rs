use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, BufferNodeError, BufferPacketCursor, DataPlaneRuntime, Network, NodeProcessFn,
    NodeResult, NodeRuntimeData, RouteMetadata, SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_service::data_plane::DropNode;
use hammer_service::transport::tcp::reset::{
    TcpResetObservation, TcpResetObserver, TcpResetReason, TcpSynthesizedReset,
};
use hammer_service::transport::tcp::{
    TcpInputControlPlane, TcpInputError, TcpInputNext, TcpResetNext, TcpResetNode,
};

#[derive(Default)]
struct RecordingTcpResetObserver {
    observations: Mutex<Vec<TcpResetObservation>>,
}

impl TcpResetObserver for RecordingTcpResetObserver {
    fn observe_reset(&self, observation: TcpResetObservation) -> CoreResult<()> {
        self.observations
            .lock()
            .expect("tcp reset observations poisoned")
            .push(observation);
        Ok(())
    }
}

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    metadata: Vec<RouteMetadata>,
    node_errors: Vec<Option<BufferNodeError>>,
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

impl hammer_adapter::Node for CaptureNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        panic!("capture node must run through descriptor process");
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

impl hammer_adapter::InternalNode for CaptureNode {}

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
            .expect("capture state registry poisoned");
        Arc::clone(
            states
                .get(data.usize_word(0).expect("capture state slot"))
                .expect("capture state slot exists"),
        )
    };
    for index in frame.drain_pending() {
        let packet = runtime.copy_current_chain(index)?;
        let metadata = runtime.metadata(index)?;
        let node_error = runtime.node_error(index)?;
        let mut state = state.lock().expect("capture state poisoned");
        state.packets.push(packet.into_iter().collect());
        state.metadata.push(metadata);
        state.node_errors.push(node_error);
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

#[test]
fn tcp_reset_observer_records_local_remote_metadata_reason_and_synthesized_reset() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop_node, drop_node))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );
    let tcp_input = runtime.nodes().register_internal(
        TcpInputControlPlane::new(TcpInputNext::nodes(
            drop_node, drop_node, drop_node, drop_node, drop_node, drop_node, reset,
        ))
        .node(),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 50_002);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 20)), 443);
    let packet = ipv4_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0102_0304,
        0x1020_3040,
        tcp_flags(false, false, false, true),
        b"ack",
    );
    let expected_reset = ipv4_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0x1020_3040,
        0,
        tcp_flags(false, false, true, false),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);

    assert!(runtime.schedule_frame(tcp_input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        runtime
            .node_error_count(tcp_input, TcpInputError::AckInvalid.code())
            .expect("ack invalid counter"),
        1
    );
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset,
            }),
        }]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_observer_synthesizes_rst_ack_for_non_ack_segments() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop, drop))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)), 40_123);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 55)), 8080);
    let packet = ipv4_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x5566_7788,
        0,
        tcp_flags(false, false, false, false),
        b"closed",
    );
    let expected_reset = ipv4_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0,
        0x5566_778e,
        tcp_flags(false, false, true, true),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::ConnectionClosed.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::ConnectionClosed,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset,
            }),
        }]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_observer_synthesizes_wrapped_rst_ack_for_non_ack_segments() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop, drop))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 11)), 40_124);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 56)), 8081);
    let packet = ipv4_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        u32::MAX - 3,
        0,
        tcp_flags(false, false, false, false),
        b"wrapped",
    );
    let expected_reset = ipv4_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0,
        3,
        tcp_flags(false, false, true, true),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::ConnectionClosed.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::ConnectionClosed,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset,
            }),
        }]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_observer_does_not_synthesize_reset_in_response_to_rst_segment() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 13)), 40_126);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 58)), 8084);
    let packet = ipv4_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0abc_def0,
        0x1020_3040,
        tcp_flags(false, false, true, true),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: None,
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert!(state.packets.is_empty(), "rst input must not be reinjected");
    assert!(
        state.metadata.is_empty(),
        "rst input must not publish lookup metadata"
    );
    assert!(
        state.node_errors.is_empty(),
        "rst input must not hit lookup node"
    );
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_observer_does_not_synthesize_ipv6_reset_in_response_to_rst_segment() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(
        IpAddr::V6("2001:db8:100::13".parse::<Ipv6Addr>().expect("remote IPv6")),
        40_126,
    );
    let local = SocketAddr::new(
        IpAddr::V6("2001:db8:200::58".parse::<Ipv6Addr>().expect("local IPv6")),
        8084,
    );
    let packet = ipv6_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0abc_def0,
        0x1020_3040,
        tcp_flags(false, false, true, true),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: None,
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert!(state.packets.is_empty(), "rst input must not be reinjected");
    assert!(
        state.metadata.is_empty(),
        "rst input must not publish lookup metadata"
    );
    assert!(
        state.node_errors.is_empty(),
        "rst input must not hit lookup node"
    );
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_node_reinjects_synthesized_reset_into_lookup_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop_node, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 12)), 40_125);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 57)), 8082);
    let packet = ipv4_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0102_0304,
        0x1020_3040,
        tcp_flags(false, false, false, true),
        b"ack",
    );
    let expected_reset = ipv4_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0x1020_3040,
        0,
        tcp_flags(false, false, true, false),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset.clone(),
            }),
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert_eq!(state.packets, vec![expected_reset]);
    assert_eq!(
        state.metadata,
        vec![tcp_metadata(
            local.ip(),
            local.port(),
            remote.ip(),
            remote.port()
        )]
    );
    assert_eq!(state.node_errors, vec![None]);
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_node_reinjects_synthesized_reset_with_prefixed_ipv4_header() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop_node, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 22)), 40_135);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 67)), 8087);
    let packet = ipv4_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0102_3344,
        0x5566_7788,
        tcp_flags(false, false, false, true),
        b"ack",
    );
    let expected_reset = ipv4_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0x5566_7788,
        0,
        tcp_flags(false, false, true, false),
        b"",
    );
    let prefix = [0xaa, 0xbb, 0xcc];
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &prefixed_packet(&prefix, &packet),
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor_with_prefix(&runtime, buffer, &packet, prefix.len());
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset.clone(),
            }),
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert_eq!(state.packets, vec![expected_reset]);
    assert_eq!(
        state.metadata,
        vec![tcp_metadata(
            local.ip(),
            local.port(),
            remote.ip(),
            remote.port()
        )]
    );
    assert_eq!(state.node_errors, vec![None]);
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_node_reinjects_synthesized_ipv6_reset_into_lookup_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop_node, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(
        IpAddr::V6("2001:db8:100::12".parse::<Ipv6Addr>().expect("remote IPv6")),
        40_125,
    );
    let local = SocketAddr::new(
        IpAddr::V6("2001:db8:200::57".parse::<Ipv6Addr>().expect("local IPv6")),
        8082,
    );
    let packet = ipv6_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0102_0304,
        0x1020_3040,
        tcp_flags(false, false, false, true),
        b"ack",
    );
    let expected_reset = ipv6_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0x1020_3040,
        0,
        tcp_flags(false, false, true, false),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset.clone(),
            }),
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert_eq!(state.packets, vec![expected_reset]);
    assert_eq!(
        state.metadata,
        vec![tcp_metadata(
            local.ip(),
            local.port(),
            remote.ip(),
            remote.port()
        )]
    );
    assert_eq!(state.node_errors, vec![None]);
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_node_reinjects_synthesized_reset_with_prefixed_ipv6_header() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop_node, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(
        IpAddr::V6("2001:db8:100::44".parse::<Ipv6Addr>().expect("remote IPv6")),
        40_235,
    );
    let local = SocketAddr::new(
        IpAddr::V6("2001:db8:200::88".parse::<Ipv6Addr>().expect("local IPv6")),
        8088,
    );
    let packet = ipv6_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        0x0abc_def0,
        0x1122_3344,
        tcp_flags(false, false, false, true),
        b"ack6",
    );
    let expected_reset = ipv6_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0x1122_3344,
        0,
        tcp_flags(false, false, true, false),
        b"",
    );
    let prefix = [0xdd, 0xee, 0xff, 0x10];
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &prefixed_packet(&prefix, &packet),
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor_with_prefix(&runtime, buffer, &packet, prefix.len());
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset.clone(),
            }),
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert_eq!(state.packets, vec![expected_reset]);
    assert_eq!(
        state.metadata,
        vec![tcp_metadata(
            local.ip(),
            local.port(),
            remote.ip(),
            remote.port()
        )]
    );
    assert_eq!(state.node_errors, vec![None]);
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_reset_node_reinjects_synthesized_ipv6_reset_ack_for_non_ack_segments_with_wraparound() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop_node, lookup))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );

    let remote = SocketAddr::new(
        IpAddr::V6("2001:db8:100::34".parse::<Ipv6Addr>().expect("remote IPv6")),
        40_225,
    );
    let local = SocketAddr::new(
        IpAddr::V6("2001:db8:200::78".parse::<Ipv6Addr>().expect("local IPv6")),
        8083,
    );
    let packet = ipv6_tcp_packet_with_numbers(
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        u32::MAX - 1,
        0,
        tcp_flags(true, false, false, false),
        b"bye",
    );
    let expected_reset = ipv6_tcp_packet_with_numbers(
        match local.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        match remote.ip() {
            IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        0,
        2,
        tcp_flags(false, false, true, true),
        b"",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_node_error(BufferNodeError::new(
            reset,
            TcpInputError::AckInvalid.code(),
        ));

    assert!(runtime.schedule_frame(reset, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
            synthesized_reset: Some(TcpSynthesizedReset {
                metadata: tcp_metadata(local.ip(), local.port(), remote.ip(), remote.port()),
                packet: expected_reset.clone(),
            }),
        }]
    );
    let state = lookup_state.lock().expect("lookup capture state poisoned");
    assert_eq!(state.packets, vec![expected_reset]);
    assert_eq!(
        state.metadata,
        vec![tcp_metadata(
            local.ip(),
            local.port(),
            remote.ip(),
            remote.port()
        )]
    );
    assert_eq!(state.node_errors, vec![None]);
    std::mem::drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
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

fn prefixed_packet(prefix: &[u8], packet: &[u8]) -> Vec<u8> {
    let mut prefixed = Vec::with_capacity(prefix.len() + packet.len());
    prefixed.extend_from_slice(prefix);
    prefixed.extend_from_slice(packet);
    prefixed
}

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    stamp_tcp_cursor_with_prefix(runtime, buffer, packet, 0);
}

fn stamp_tcp_cursor_with_prefix(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
    prefix_len: usize,
) {
    let version = packet.first().expect("IP header") >> 4;
    let (network_header_len, packet_len) = match version {
        4 => (
            ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4,
            prefix_len + u16::from_be_bytes([packet[2], packet[3]]) as usize,
        ),
        6 => (
            40,
            prefix_len + 40 + u16::from_be_bytes([packet[4], packet[5]]) as usize,
        ),
        other => panic!("unsupported IP version {other}"),
    };
    let packet_tcp_offset = network_header_len;
    let tcp_offset = prefix_len + packet_tcp_offset;
    let tcp_header_len = ((packet[packet_tcp_offset + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(prefix_len, network_header_len)
                .with_transport_header(tcp_offset, tcp_header_len)
                .with_transport_payload_offset(tcp_offset + tcp_header_len),
        );
}

fn tcp_metadata(
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(source, source_port)),
        destination: Some(SocksAddr::ip(destination, destination_port)),
        ..RouteMetadata::default()
    }
}

fn ipv4_tcp_packet_with_numbers(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    sequence_number: u32,
    acknowledgment_number: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(
        &mut packet[20..],
        source_port,
        destination_port,
        sequence_number,
        acknowledgment_number,
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

fn ipv6_tcp_packet_with_numbers(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    sequence_number: u32,
    acknowledgment_number: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(
        &mut packet[40..],
        source_port,
        destination_port,
        sequence_number,
        acknowledgment_number,
        flags,
        payload,
    );
    let checksum = ipv6_l4_checksum(source, destination, 6, &packet[40..]);
    packet[56..58].copy_from_slice(&checksum.to_be_bytes());
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

fn write_tcp_segment(
    segment: &mut [u8],
    source_port: u16,
    destination_port: u16,
    sequence_number: u32,
    acknowledgment_number: u32,
    flags: u8,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[4..8].copy_from_slice(&sequence_number.to_be_bytes());
    segment[8..12].copy_from_slice(&acknowledgment_number.to_be_bytes());
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

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(40 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, protocol]);
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
