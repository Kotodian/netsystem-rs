use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeId, NodeProcessFn,
    NodeRegistration, NodeResult, NodeRuntimeData, PacketTrace, RouteDecision, RouteMetadata,
    RouteTarget, Router, SocksAddr, TraceControlPlane, TraceInputPolicy, TracePolicy,
    add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_runtime::spawn::DataRuntime;
use hammer_service::data_plane::{DropNode, DropTrace, Feature, FeatureArcControl, FeatureArcSpec};

const DATA_PLANE_SOURCE: &str = include_str!("../src/data_plane.rs");
const IP_REASSEMBLY_SOURCE: &str = include_str!("../src/net/ip/reassembly.rs");

#[hammer_component_macros::node_next]
enum StaticRouteNext {
    Lookup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticRouteTrace {
    network: Network,
    source: Option<SocksAddr>,
    destination: Option<SocksAddr>,
    decision_kind: RouteDecisionKind,
}

impl StaticRouteTrace {
    fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TestTraceCursor::new(bytes);
        let network = decode_test_network(cursor.read_u8()?)?;
        let source = cursor.read_socks_addr()?;
        let destination = cursor.read_socks_addr()?;
        let decision_kind = RouteDecisionKind::decode(cursor.read_u8()?)?;
        cursor.is_empty().then_some(Self {
            network,
            source,
            destination,
            decision_kind,
        })
    }
}

impl PacketTrace for StaticRouteTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        out.push(encode_test_network(self.network));
        put_test_socks_addr(out, self.source.as_ref());
        put_test_socks_addr(out, self.destination.as_ref());
        out.push(self.decision_kind.encode());
    }
}

struct TestTraceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TestTraceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn read_u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.position)?;
        self.position += 1;
        Some(value)
    }

    fn read_array<const LEN: usize>(&mut self) -> Option<[u8; LEN]> {
        let end = self.position.checked_add(LEN)?;
        let bytes = self.bytes.get(self.position..end)?;
        self.position = end;
        bytes.try_into().ok()
    }

    fn read_u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.read_array()?))
    }

    fn read_ip_addr(&mut self) -> Option<IpAddr> {
        match self.read_u8()? {
            4 => Some(IpAddr::V4(Ipv4Addr::from(self.read_array::<4>()?))),
            6 => Some(IpAddr::V6(Ipv6Addr::from(self.read_array::<16>()?))),
            _ => None,
        }
    }

    fn read_socks_addr(&mut self) -> Option<Option<SocksAddr>> {
        if self.read_u8()? == 0 {
            return Some(None);
        }
        let host = self.read_ip_addr()?;
        let port = self.read_u16()?;
        let domain = if self.read_u8()? == 1 {
            let len = usize::from(self.read_u16()?);
            let end = self.position.checked_add(len)?;
            let bytes = self.bytes.get(self.position..end)?;
            self.position = end;
            Some(String::from_utf8(bytes.to_vec()).ok()?)
        } else {
            None
        };
        Some(Some(SocksAddr { host, port, domain }))
    }
}

fn encode_test_network(value: Network) -> u8 {
    match value {
        Network::Tcp => 0,
        Network::Udp => 1,
        Network::Icmp => 2,
    }
}

fn decode_test_network(value: u8) -> Option<Network> {
    match value {
        0 => Some(Network::Tcp),
        1 => Some(Network::Udp),
        2 => Some(Network::Icmp),
        _ => None,
    }
}

fn put_test_socks_addr(out: &mut std::vec::Vec<u8>, value: Option<&SocksAddr>) {
    let Some(value) = value else {
        out.push(0);
        return;
    };
    out.push(1);
    put_test_ip_addr(out, value.host);
    out.extend_from_slice(&value.port.to_le_bytes());
    if let Some(domain) = &value.domain {
        out.push(1);
        let bytes = domain.as_bytes();
        let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&bytes[..usize::from(len)]);
    } else {
        out.push(0);
    }
}

fn put_test_ip_addr(out: &mut std::vec::Vec<u8>, value: IpAddr) {
    match value {
        IpAddr::V4(addr) => {
            out.push(4);
            out.extend_from_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
            out.push(6);
            out.extend_from_slice(&addr.octets());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteDecisionKind {
    Route,
    HijackDns,
    Reject,
}

impl RouteDecisionKind {
    fn encode(self) -> u8 {
        match self {
            Self::Route => 0,
            Self::HijackDns => 1,
            Self::Reject => 2,
        }
    }

    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Route),
            1 => Some(Self::HijackDns),
            2 => Some(Self::Reject),
            _ => None,
        }
    }
}

impl From<&RouteDecision> for RouteDecisionKind {
    fn from(value: &RouteDecision) -> Self {
        match value {
            RouteDecision::Route { .. } => Self::Route,
            RouteDecision::HijackDns => Self::HijackDns,
            RouteDecision::Reject { .. } => Self::Reject,
        }
    }
}

struct StaticRouter {
    decision: RouteDecision,
    prepare_count: AtomicUsize,
    match_count: AtomicUsize,
}

impl StaticRouter {
    fn new(decision: RouteDecision) -> Self {
        Self {
            decision,
            prepare_count: AtomicUsize::new(0),
            match_count: AtomicUsize::new(0),
        }
    }
}

impl Lifecycle for StaticRouter {
    fn name(&self) -> &str {
        "static-router"
    }

    fn start(&self, _stage: StartStage) -> CoreResult<()> {
        Ok(())
    }

    fn close(&self) -> CoreResult<()> {
        Ok(())
    }
}

impl Router for StaticRouter {
    fn reset_network(&self) {}

    fn match_route(&self, _metadata: &mut RouteMetadata) -> CoreResult<RouteDecision> {
        self.match_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.decision.clone())
    }

    fn prepare_route_metadata(&self, metadata: &mut RouteMetadata) -> CoreResult<()> {
        self.prepare_count.fetch_add(1, Ordering::SeqCst);
        metadata.protocol = "prepared".to_owned();
        Ok(())
    }

    fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
        None
    }

    fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
        false
    }
}

struct DecisionSinkNode {
    runtime_data: NodeRuntimeData,
}

struct StaticRouteNode {
    next: [NodeId; StaticRouteNext::COUNT],
    runtime_data: NodeRuntimeData,
}

impl DecisionSinkNode {
    fn new(decisions: Arc<Mutex<Vec<RouteDecision>>>) -> Self {
        let mut states = decision_sink_states()
            .lock()
            .expect("decision sink state registry poisoned");
        let slot = states.len();
        states.push(decisions);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("decision sink slot"),
        }
    }
}

impl StaticRouteNode {
    fn new(router: Arc<StaticRouter>, next: [NodeId; StaticRouteNext::COUNT]) -> Self {
        let mut states = static_route_match_states()
            .lock()
            .expect("route match state registry poisoned");
        let slot = states.len();
        states.push(Arc::clone(&router));
        Self {
            next,
            runtime_data: NodeRuntimeData::from_usize(slot).expect("route match slot"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TestFeatureArc {
    Probe,
}

impl FeatureArcSpec for TestFeatureArc {}

struct ProbeFeatureNode;

impl Feature<TestFeatureArc> for ProbeFeatureNode {
    #[inline]
    fn id() -> TestFeatureArc {
        TestFeatureArc::Probe
    }
}

impl Node for ProbeFeatureNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "probe feature node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        probe_feature_process
    }
}

impl InternalNode for ProbeFeatureNode {}

impl Node for DecisionSinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "decision sink node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        decision_sink_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for DecisionSinkNode {}

impl Node for StaticRouteNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "static route node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        static_route_match_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for StaticRouteNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("static-route-node", StaticRouteNext::COUNT)
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

fn decision_sink_states() -> &'static Mutex<Vec<Arc<Mutex<Vec<RouteDecision>>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<Vec<RouteDecision>>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn static_route_match_states() -> &'static Mutex<Vec<Arc<StaticRouter>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<StaticRouter>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn probe_feature_process(
    _runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    _frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    Ok(NodeResult::drop())
}

fn decision_sink_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let decisions = {
        let states = decision_sink_states()
            .lock()
            .map_err(|_| CoreError::internal("decision sink state registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("decision sink state slot is invalid"))?,
        )
    };
    for buffer in frame.drain_pending() {
        let metadata = runtime.metadata(buffer)?;
        let decision = metadata
            .route_decision
            .ok_or_else(|| CoreError::internal("missing route decision"))?;
        decisions
            .lock()
            .map_err(|_| CoreError::internal("decision sink state poisoned"))?
            .push(decision);
        runtime.free_index(buffer);
    }
    Ok(NodeResult::drop())
}

fn static_route_match_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let router = {
        let states = static_route_match_states()
            .lock()
            .map_err(|_| CoreError::internal("route match state registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("route match state slot is invalid"))?,
        )
    };
    let mut cursor = frame.batch_cursor(runtime.preferred_frame_batch_width());
    cursor.prefetch_next(runtime);
    while let Some(batch) = cursor.next() {
        cursor.prefetch_next(runtime);
        for index in batch.indices() {
            let mut buffer = runtime.get_buffer_mut(index)?;
            let metadata = buffer.metadata_mut();
            router.prepare_route_metadata(metadata)?;
            let decision = router.match_route(metadata)?;
            metadata.route_decision = Some(decision);
            drop(buffer);
            add_packet_trace!(runtime, index, {
                runtime.with_metadata(index, |metadata| {
                    let decision = metadata.route_decision.as_ref().ok_or_else(|| {
                        CoreError::internal("missing route decision for route trace")
                    })?;
                    Ok(StaticRouteTrace {
                        network: metadata.network,
                        source: metadata.source.clone(),
                        destination: metadata.destination.clone(),
                        decision_kind: RouteDecisionKind::from(decision),
                    })
                })??
            })?;
        }
    }
    let next = runtime.current_node_nexts::<{ StaticRouteNext::COUNT }>()?;
    Ok(NodeResult::next_current(
        hammer_adapter::NodeNextStorage::next(&next, StaticRouteNext::Lookup),
    ))
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode,
{
    let _ = node;
}

#[test]
fn static_route_node_is_service_internal_node() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 1, 2);
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let sink = runtime
        .nodes()
        .register_internal(DecisionSinkNode::new(Arc::clone(&decisions)));
    let router = Arc::new(StaticRouter::new(RouteDecision::Route {
        target: RouteTarget::Outbound("direct".to_owned()),
    }));
    let route_match = StaticRouteNode::new(Arc::clone(&router), StaticRouteNext::nodes(sink));
    assert_internal_node(&route_match);
    let route_match = runtime.nodes().register_internal(route_match);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(
            RouteMetadata {
                inbound: "tun".to_owned(),
                network: Network::Udp,
                ..Default::default()
            },
            b"packet",
        )
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(route_match, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(router.prepare_count.load(Ordering::SeqCst), 1);
    assert_eq!(router.match_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        &*decisions.lock().expect("decision sink state poisoned"),
        &[RouteDecision::Route {
            target: RouteTarget::Outbound("direct".to_owned())
        }]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn drop_node_frees_frame_buffers() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 2, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(first)
        .expect("push first");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(second)
        .expect("push second");

    assert_eq!(runtime.in_use_buffers(), 2);
    assert!(runtime.schedule_frame(drop, frame).expect("schedule drop"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn drop_node_adds_trace_payload_before_freeing_packet() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 2, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let control = TraceControlPlane::new(4);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: drop,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 2);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"drop")
        .expect("alloc packet");
    runtime.try_mark_trace(drop, packet).expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(packet)
        .expect("push packet");

    assert!(runtime.schedule_frame(drop, frame).expect("schedule drop"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].entries[0].node_name, Some("drop-node"));
    assert_eq!(
        DropTrace::decode(&records[0].entries[0].payload_bytes).expect("drop trace"),
        DropTrace { dropped: 1 }
    );
}

#[test]
fn drop_node_returns_trace_error_and_frees_scheduled_frame_with_stale_index() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 2, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let control = TraceControlPlane::new(4);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: drop,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 2);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first");
    let stale = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"stale")
        .expect("alloc stale");
    runtime.try_mark_trace(drop, stale).expect("mark stale");
    {
        let mut frame_ref = runtime.get_frame_mut(frame).expect("mutate frame");
        frame_ref.push_index(first).expect("push first");
        frame_ref.push_index(stale).expect("push stale");
    }
    runtime.free_index(stale);

    assert!(runtime.schedule_frame(drop, frame).expect("schedule drop"));

    let err = runtime
        .run_ready_nodes()
        .expect_err("stale packet should fail trace check");

    assert_eq!(err.to_string(), "buffer slot is free");
    let frame_err = match runtime.get_frame_mut(frame) {
        Ok(_) => panic!("frame should be freed"),
        Err(err) => err,
    };
    assert_eq!(frame_err.to_string(), "frame slot is free");
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn static_route_node_adds_trace_payload_for_route_decision() {
    let runtime = DataPlaneRuntime::with_capacities(128, 4, 2, 4);
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let sink = runtime
        .nodes()
        .register_internal(DecisionSinkNode::new(Arc::clone(&decisions)));
    let router = Arc::new(StaticRouter::new(RouteDecision::Reject {
        method: "blocked".to_owned(),
    }));
    let route_match = runtime.nodes().register_internal(StaticRouteNode::new(
        Arc::clone(&router),
        StaticRouteNext::nodes(sink),
    ));
    let control = TraceControlPlane::new(4);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: route_match,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 2);
    let metadata = RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::domain(
            "source.example",
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            1234,
        )),
        destination: Some(SocksAddr::domain(
            "dest.example",
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)),
            443,
        )),
        ..Default::default()
    };
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = runtime
        .alloc_index_with_bytes(metadata, b"route")
        .expect("alloc packet");
    runtime
        .try_mark_trace(route_match, packet)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(packet)
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(route_match, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].entries[0].node_name, Some("static-route-node"));
    let trace =
        StaticRouteTrace::decode(&records[0].entries[0].payload_bytes).expect("route trace");
    assert_eq!(trace.network, Network::Tcp);
    assert_eq!(
        trace.source,
        Some(SocksAddr::domain(
            "source.example",
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            1234
        ))
    );
    assert_eq!(
        trace.destination,
        Some(SocksAddr::domain(
            "dest.example",
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)),
            443
        ))
    );
    assert_eq!(trace.decision_kind, RouteDecisionKind::Reject);
}

#[test]
fn packet_trace_append_sites_use_macro_owned_trace_checks() {
    assert!(
        !DATA_PLANE_SOURCE.contains("let traced = runtime.should_trace_packet(index)?"),
        "route matching append sites should let add_packet_trace! own trace checks"
    );
    assert!(
        !DATA_PLANE_SOURCE.contains("if unlikely(traced)"),
        "route matching should construct trace payloads lazily inside add_packet_trace!"
    );
    assert!(
        !IP_REASSEMBLY_SOURCE.contains("fn add_trace"),
        "IpReassemblyNode append sites should call add_packet_trace! directly"
    );
    assert!(
        !IP_REASSEMBLY_SOURCE.contains("self.add_trace("),
        "IpReassemblyNode should not route trace appends through a node-local wrapper"
    );
}

#[test]
fn feature_arc_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "feature-arc-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 1, 2);
    let feature: NodeId = runtime.nodes().register_internal(ProbeFeatureNode);
    let mut control =
        FeatureArcControl::<TestFeatureArc>::new().with_data_plane_barrier(barrier.clone());

    control
        .register_feature::<ProbeFeatureNode>(feature)
        .expect("register feature");
    control
        .enable_feature::<ProbeFeatureNode>(7)
        .expect("enable feature");
    control
        .disable_feature::<ProbeFeatureNode>(7)
        .expect("disable feature");

    assert_eq!(barrier.sync_count(), 3);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn feature_arc_publish_requires_configured_runtime_data_plane_barrier() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 1, 2);
    let feature: NodeId = runtime.nodes().register_internal(ProbeFeatureNode);
    let mut control = FeatureArcControl::<TestFeatureArc>::new();

    let err = control
        .register_feature::<ProbeFeatureNode>(feature)
        .expect_err("feature arc publish should require barrier");

    assert!(
        err.to_string()
            .contains("feature arc publish requires data-plane barrier")
    );
}
