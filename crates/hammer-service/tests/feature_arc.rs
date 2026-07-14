use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use hammer_core::config::Config;
use hammer_core::data_plane::{BufferFrame, NodeRegistration};
use hammer_core::error::CoreResult;
use hammer_runtime::{
    DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_runtime::{new_worker_runtime, spawn::DataRuntime};
use hammer_service::data_plane::{
    Feature, FeatureArc, FeatureArcControl, FeatureArcSpec, FeatureArcStartHandle,
    FeatureArcStartNode, FeatureArcStartSlot, next_feature_frame,
};
use hammer_service::net::NetworkOpaque;
use hammer_infra::vec::Vec;
use std::mem::transmute;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TestArc {
    First,
    Second,
}

impl FeatureArcSpec for TestArc {}

struct FirstFeature;
impl Feature<TestArc> for FirstFeature {
    fn id() -> TestArc {
        TestArc::First
    }
}

struct SecondFeature;
impl Feature<TestArc> for SecondFeature {
    fn id() -> TestArc {
        TestArc::Second
    }

    fn runs_after() -> Vec<TestArc> {
        hammer_infra::vec![TestArc::First]
    }
}

#[derive(Default)]
struct VisitState {
    order: Vec<&'static str>,
}

fn test_runtime() -> DataPlaneRuntime {
    let mut config = Config::default();
    config.worker.buffer.slot_bytes = 2048;
    config.worker.buffer.slots_per_numa = 64;
    config.worker.buffer.frame_pool_size = 32;
    new_worker_runtime(&config).expect("create worker runtime")
}

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, sw_if_index: u32) {
    let index = runtime.alloc_index_with_bytes(&[0u8; 64]).expect("alloc");
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
        let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        network.sw_if_index[0] = sw_if_index;
    }
    frame.push_index(index).expect("push");
}

#[derive(Clone)]
struct StartRuntime {
    feature_arc: Option<FeatureArcStartHandle>,
    default_next: u16,
    visits: Arc<Mutex<VisitState>>,
    name: &'static str,
}

fn start_runtimes() -> &'static Mutex<Vec<StartRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<StartRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

struct StartNode {
    feature_arc: FeatureArcStartSlot<TestArc>,
    default_next: u16,
    visits: Arc<Mutex<VisitState>>,
    name: &'static str,
    runtime_data: NodeRuntimeData,
}

impl FeatureArcStartNode<TestArc> for StartNode {
    fn set_feature_arc(&mut self, arc: FeatureArc<TestArc>) {
        self.feature_arc.set(arc);
    }

    fn clear_feature_arc(&mut self) {
        self.feature_arc.clear();
    }
}

impl StartNode {
    fn new(name: &'static str, visits: Arc<Mutex<VisitState>>, default_next: u16) -> Self {
        let mut runtimes = start_runtimes().lock().unwrap();
        let slot = runtimes.len();
        runtimes.push(StartRuntime {
            feature_arc: None,
            default_next,
            visits: Arc::clone(&visits),
            name,
        });
        Self {
            feature_arc: FeatureArcStartSlot::new(),
            default_next,
            visits,
            name,
            runtime_data: NodeRuntimeData::from_usize(slot).expect("slot"),
        }
    }

    fn sync_runtime(&self) {
        let mut runtimes = start_runtimes().lock().unwrap();
        let slot = self.runtime_data.usize_word(0).expect("slot");
        let runtime = &mut runtimes[slot];
        runtime.feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        runtime.default_next = self.default_next;
    }
}

fn start_node_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let slot = data.usize_word(0).expect("slot");
    let state = start_runtimes().lock().unwrap()[slot].clone();
    state.visits.lock().unwrap().order.push(state.name);
    let mut nexts = hammer_infra::vec::Vec::with_capacity(frame.len());
    for index in frame.iter_indices() {
        let slot = match &state.feature_arc {
            Some(handle) => {
                let interface_index = {
                    let buffer = runtime.get_buffer(*index).expect("buffer");
                    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
                    network.sw_if_index[0]
                };
                if interface_index == 0 {
                    state.default_next
                } else {
                    handle.start_for_interface_or(
                        runtime,
                        *index,
                        interface_index,
                        state.default_next,
                    )
                }
            }
            None => state.default_next,
        };
        nexts.push(slot);
    }
    runtime.enqueue_to_next(frame, nexts.as_slice());
    NodeResult::drop()
}

impl Node for StartNode {
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        self.sync_runtime();
        start_node_process(runtime, self.runtime_data, frame)
    }

    fn node_process(&self) -> NodeProcessFn {
        start_node_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.sync_runtime();
        Ok(self.runtime_data)
    }
}

impl InternalNode for StartNode {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next(self.name, 0)
    }
}

#[derive(Clone)]
struct AdvanceRuntime {
    label: &'static str,
    visits: Arc<Mutex<VisitState>>,
    handle: Arc<Mutex<Option<FeatureArcStartHandle>>>,
}

fn advance_runtimes() -> &'static Mutex<Vec<AdvanceRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<AdvanceRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

struct LazyAdvance {
    label: &'static str,
    visits: Arc<Mutex<VisitState>>,
    handle: Arc<Mutex<Option<FeatureArcStartHandle>>>,
    runtime_data: NodeRuntimeData,
}

impl LazyAdvance {
    fn new(
        label: &'static str,
        visits: Arc<Mutex<VisitState>>,
        handle: Arc<Mutex<Option<FeatureArcStartHandle>>>,
    ) -> Self {
        let mut runtimes = advance_runtimes().lock().unwrap();
        let slot = runtimes.len();
        runtimes.push(AdvanceRuntime {
            label,
            visits: Arc::clone(&visits),
            handle: Arc::clone(&handle),
        });
        Self {
            label,
            visits,
            handle,
            runtime_data: NodeRuntimeData::from_usize(slot).expect("slot"),
        }
    }
}

fn advance_node_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let slot = data.usize_word(0).expect("slot");
    let state = advance_runtimes().lock().unwrap()[slot].clone();
    state.visits.lock().unwrap().order.push(state.label);
    let handle = state.handle.lock().unwrap().clone().expect("handle");
    next_feature_frame(&handle, runtime, frame)
}

impl Node for LazyAdvance {
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        advance_node_process(runtime, self.runtime_data, frame)
    }

    fn node_process(&self) -> NodeProcessFn {
        advance_node_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for LazyAdvance {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next(self.label, 0)
    }
}

#[derive(Clone)]
struct EndRuntime {
    label: &'static str,
    visits: Arc<Mutex<VisitState>>,
}

fn end_runtimes() -> &'static Mutex<Vec<EndRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<EndRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

struct EndSink {
    label: &'static str,
    visits: Arc<Mutex<VisitState>>,
    runtime_data: NodeRuntimeData,
}

impl EndSink {
    fn new(label: &'static str, visits: Arc<Mutex<VisitState>>) -> Self {
        let mut runtimes = end_runtimes().lock().unwrap();
        let slot = runtimes.len();
        runtimes.push(EndRuntime {
            label,
            visits: Arc::clone(&visits),
        });
        Self {
            label,
            visits,
            runtime_data: NodeRuntimeData::from_usize(slot).expect("slot"),
        }
    }
}

fn end_node_process(
    _runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let slot = data.usize_word(0).expect("slot");
    let state = end_runtimes().lock().unwrap()[slot].clone();
    state.visits.lock().unwrap().order.push(state.label);
    let _ = frame;
    NodeResult::drop()
}

impl Node for EndSink {
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        end_node_process(runtime, self.runtime_data, frame)
    }

    fn node_process(&self) -> NodeProcessFn {
        end_node_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for EndSink {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next(self.label, 0)
    }
}

fn build_control(
    runtime: &DataPlaneRuntime,
    barrier: hammer_runtime::DataPlaneBarrierHandle,
) -> FeatureArcControl<TestArc> {
    FeatureArcControl::<TestArc>::new()
        .with_data_plane_barrier(barrier)
        .with_nodes(runtime.nodes().clone())
}

fn register_start(
    runtime: &DataPlaneRuntime,
    control: &mut FeatureArcControl<TestArc>,
    visits: &Arc<Mutex<VisitState>>,
    name: &'static str,
    end: hammer_core::data_plane::NodeId,
) -> hammer_core::data_plane::NodeId {
    let mut start = StartNode::new(name, Arc::clone(visits), 0);
    control.attach_start(&mut start);
    start.sync_runtime();
    let start_id = runtime.nodes().register_internal(start);
    let default_slot = runtime
        .nodes()
        .add_node_next_slot(start_id, end)
        .expect("default slot");
    assert_eq!(default_slot, 0);
    control.add_start_node(start_id).expect("record start");
    start_id
}

#[test]
fn empty_chain_preserves_caller_default_next() {
    let data_runtime = DataRuntime::new(1, "feature-empty", 512 * 1024, 2).expect("data runtime");
    let runtime = test_runtime();
    let visits = Arc::new(Mutex::new(VisitState::default()));
    let end = runtime
        .nodes()
        .register_internal(EndSink::new("end", Arc::clone(&visits)));
    let mut control = build_control(&runtime, data_runtime.data_plane_barrier());
    control.set_default_end_node(end).expect("default end");
    let start_id = register_start(&runtime, &mut control, &visits, "start-a", end);

    let mut frame = runtime.buffers().get_next_frame(start_id).expect("frame");
    push_packet(&runtime, &mut frame, 3);
    runtime.put_next_frame(frame).expect("schedule");
    assert_eq!(runtime.run_ready_nodes().expect("run"), 2);
    assert_eq!(visits.lock().unwrap().order, vec!["start-a", "end"]);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn two_feature_chain_traverses_in_order_through_graph_runtime() {
    let data_runtime = DataRuntime::new(1, "feature-chain", 512 * 1024, 2).expect("data runtime");
    let runtime = test_runtime();
    let visits = Arc::new(Mutex::new(VisitState::default()));
    let end = runtime
        .nodes()
        .register_internal(EndSink::new("end", Arc::clone(&visits)));
    let handle_slot = Arc::new(Mutex::new(None::<FeatureArcStartHandle>));
    let mut control = build_control(&runtime, data_runtime.data_plane_barrier());
    control.set_default_end_node(end).expect("default end");

    let first = runtime.nodes().register_internal(LazyAdvance::new(
        "feature-first",
        Arc::clone(&visits),
        Arc::clone(&handle_slot),
    ));
    let second = runtime.nodes().register_internal(LazyAdvance::new(
        "feature-second",
        Arc::clone(&visits),
        Arc::clone(&handle_slot),
    ));
    control
        .register_feature::<FirstFeature>(first)
        .expect("register first");
    control
        .register_feature::<SecondFeature>(second)
        .expect("register second");

    let start_id = register_start(&runtime, &mut control, &visits, "start-a", end);
    control
        .enable_feature::<FirstFeature>(9)
        .expect("enable first");
    control
        .enable_feature::<SecondFeature>(9)
        .expect("enable second");
    *handle_slot.lock().unwrap() = Some(control.arc().start_handle());

    let mut frame = runtime.buffers().get_next_frame(start_id).expect("frame");
    push_packet(&runtime, &mut frame, 9);
    runtime.put_next_frame(frame).expect("schedule");
    let ran = runtime.run_ready_nodes().expect("run");
    assert!(ran >= 4, "ran={ran}");
    assert_eq!(
        visits.lock().unwrap().order,
        vec!["start-a", "feature-first", "feature-second", "end"]
    );
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn per_interface_end_override_compiles_on_final_predecessor() {
    let data_runtime =
        DataRuntime::new(1, "feature-end-override", 512 * 1024, 2).expect("data runtime");
    let runtime = test_runtime();
    let visits = Arc::new(Mutex::new(VisitState::default()));
    let default_end = runtime
        .nodes()
        .register_internal(EndSink::new("default-end", Arc::clone(&visits)));
    let override_end = runtime
        .nodes()
        .register_internal(EndSink::new("override-end", Arc::clone(&visits)));
    let handle_slot = Arc::new(Mutex::new(None::<FeatureArcStartHandle>));
    let mut control = build_control(&runtime, data_runtime.data_plane_barrier());
    control
        .set_default_end_node(default_end)
        .expect("default end");

    let first = runtime.nodes().register_internal(LazyAdvance::new(
        "feature-first",
        Arc::clone(&visits),
        Arc::clone(&handle_slot),
    ));
    control
        .register_feature::<FirstFeature>(first)
        .expect("register");
    let start_id = register_start(&runtime, &mut control, &visits, "start-a", default_end);
    control
        .set_end_node_for_interface(4, override_end)
        .expect("override end");
    control.enable_feature::<FirstFeature>(4).expect("enable");
    *handle_slot.lock().unwrap() = Some(control.arc().start_handle());

    let mut frame = runtime.buffers().get_next_frame(start_id).expect("frame");
    push_packet(&runtime, &mut frame, 4);
    runtime.put_next_frame(frame).expect("schedule");
    assert!(runtime.run_ready_nodes().expect("run") >= 3);
    assert_eq!(
        visits.lock().unwrap().order,
        vec!["start-a", "feature-first", "override-end"]
    );
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn multiple_start_nodes_compile_distinct_first_transitions() {
    let data_runtime =
        DataRuntime::new(1, "feature-multi-start", 512 * 1024, 2).expect("data runtime");
    let runtime = test_runtime();
    let visits = Arc::new(Mutex::new(VisitState::default()));
    let end = runtime
        .nodes()
        .register_internal(EndSink::new("end", Arc::clone(&visits)));
    let handle_slot = Arc::new(Mutex::new(None::<FeatureArcStartHandle>));
    let mut control = build_control(&runtime, data_runtime.data_plane_barrier());
    control.set_default_end_node(end).expect("default end");

    let first = runtime.nodes().register_internal(LazyAdvance::new(
        "feature-first",
        Arc::clone(&visits),
        Arc::clone(&handle_slot),
    ));
    control
        .register_feature::<FirstFeature>(first)
        .expect("register");

    let start_a = register_start(&runtime, &mut control, &visits, "start-a", end);
    let start_b = register_start(&runtime, &mut control, &visits, "start-b", end);
    control.enable_feature::<FirstFeature>(2).expect("enable");
    *handle_slot.lock().unwrap() = Some(control.arc().start_handle());

    for (start_id, label) in [(start_a, "start-a"), (start_b, "start-b")] {
        visits.lock().unwrap().order.clear();
        let mut frame = runtime.buffers().get_next_frame(start_id).expect("frame");
        push_packet(&runtime, &mut frame, 2);
        runtime.put_next_frame(frame).expect("schedule");
        assert!(runtime.run_ready_nodes().expect("run") >= 3);
        assert_eq!(
            visits.lock().unwrap().order,
            vec![label, "feature-first", "end"]
        );
    }
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
