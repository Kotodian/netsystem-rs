use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_core::config::Config;
use hammer_core::data_plane::{NodeId, NodeKind, NodeRegistration};
use hammer_core::error::{CoreResult, HammerResult};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::init::InitFunction;
use hammer_runtime::node::NodeFunctionRegistration;
use hammer_runtime::{
    DataPlaneRuntime, Engine, EnginePool, NodeDescriptor, NodeEntry, NodeProcessFn, NodeResult,
    NodeRuntimeData,
};

static GRAPH_UPDATE_PLUGIN_IMAGE: hammer_runtime::__private::RegistrationImage =
    hammer_runtime::__private::RegistrationImage::new();
static GRAPH_UPDATE_WORKER_INIT_CALLS: AtomicUsize = AtomicUsize::new(0);

fn plugin_graph_update_probe_process(
    _: &DataPlaneRuntime,
    _: NodeRuntimeData,
    _: &mut hammer_core::data_plane::BufferFrame,
) -> NodeResult {
    NodeResult::drop()
}

fn register_plugin_graph_update_probe(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            plugin_graph_update_probe_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("plugin-graph-update-probe", 0),
            &[],
            None,
        ),
    )
}

fn initialize_plugin_graph_update_probe_worker(engine: &mut Engine) -> HammerResult<()> {
    assert_ne!(engine.thread_index, 0);
    assert!(
        engine
            .runtime
            .node_by_name("plugin-graph-update-probe")
            .is_some()
    );
    GRAPH_UPDATE_WORKER_INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

static GRAPH_UPDATE_WORKER_INITS: [InitFunction; 1] = [InitFunction {
    name: "plugin_graph_update_probe_worker_init",
    runs_before: &[],
    runs_after: &[],
    func: initialize_plugin_graph_update_probe_worker,
}];

static GRAPH_UPDATE_NODES: [NodeEntry; 1] = [NodeEntry {
    registration: NodeRegistration::next("plugin-graph-update-probe", 0),
    kind: NodeKind::Internal,
    init: register_plugin_graph_update_probe,
}];

#[test]
fn live_workers_install_the_published_graph_before_worker_init() {
    GRAPH_UPDATE_WORKER_INIT_CALLS.store(0, Ordering::Relaxed);
    let mut config = Config::default();
    config.worker.count = 1;
    let config = Arc::new(config);
    let registry = RuntimeRegistry::new();
    registry.set(Arc::clone(&config));
    let runtime = hammer_runtime::new_worker_runtime(&config).expect("runtime");
    let mut pool = EnginePool::new(Engine::new(runtime, registry));

    hammer_runtime::memory::memory_init(pool.main_engine_mut(), Arc::clone(&config))
        .expect("memory init");
    pool.main_engine_mut()
        .load_plugins(std::path::Path::new("unused"), &[])
        .expect("install startup registrations");
    hammer_runtime::init::run_main_loop_enter(pool.main_engine_mut()).expect("start data worker");

    // SAFETY: every referenced inventory is test-binary static and remains
    // mapped until process exit, matching a successful DSO constructor.
    unsafe {
        GRAPH_UPDATE_PLUGIN_IMAGE.link(
            &[],
            &[],
            &[],
            &[],
            &[],
            &GRAPH_UPDATE_WORKER_INITS,
            &GRAPH_UPDATE_NODES,
            &[] as &[NodeFunctionRegistration],
            &[],
        );
    }

    pool.main_engine_mut()
        .load_plugins(std::path::Path::new("unused"), &[])
        .expect("materialize late registration image");

    assert_eq!(GRAPH_UPDATE_WORKER_INIT_CALLS.load(Ordering::Relaxed), 1);
    pool.close().expect("stop data worker");
}
