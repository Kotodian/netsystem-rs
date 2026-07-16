//! Cross-crate live graph publication integration coverage.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use hammer_core::config::Config;
use hammer_core::data_plane::{NodeId, NodeKind, NodeRegistration, NodeState};
use hammer_core::error::{CoreError, CoreResult, HammerResult};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::init::InitFunction;
use hammer_runtime::node::NodeFunctionRegistration;
use hammer_runtime::{
    DataPlaneRuntime, Engine, EnginePool, File, FileFunctions, NodeDescriptor, NodeEntry,
    NodeProcessFn, NodeResult, NodeRuntimeData,
};

const RESOURCE_RUNTIME_SENTINEL: u64 = 0x51_11_51_11;

static RESOURCE_PLUGIN_IMAGE: hammer_runtime::__private::RegistrationImage =
    hammer_runtime::__private::RegistrationImage::new();
static ADDITIVE_PLUGIN_IMAGE: hammer_runtime::__private::RegistrationImage =
    hammer_runtime::__private::RegistrationImage::new();
static FAILING_PLUGIN_IMAGE: hammer_runtime::__private::RegistrationImage =
    hammer_runtime::__private::RegistrationImage::new();
static RESOURCE_SOURCE: OnceLock<(UnixStream, UnixStream)> = OnceLock::new();
static RESOURCE_WORKER_INIT_CALLS: AtomicUsize = AtomicUsize::new(0);
static ADDITIVE_WORKER_INIT_CALLS: AtomicUsize = AtomicUsize::new(0);
static FAILING_WORKER_INIT_CALLS: AtomicUsize = AtomicUsize::new(0);
static OBSERVED_RESOURCE_RUNTIME_DATA: AtomicU64 = AtomicU64::new(0);

fn descriptor_identity(fd: RawFd) -> std::io::Result<(libc::dev_t, libc::ino_t)> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: status is writable for one stat and fstat initializes it on success.
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } == 0 {
        // SAFETY: successful fstat initialized the complete stat value.
        let status = unsafe { status.assume_init() };
        Ok((status.st_dev, status.st_ino))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn count_open_identity(identity: (libc::dev_t, libc::ino_t)) -> usize {
    std::fs::read_dir("/dev/fd")
        .expect("read /dev/fd")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<RawFd>().ok())
        .filter(|fd| descriptor_identity(*fd).ok() == Some(identity))
        .count()
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::yield_now();
    }
    predicate()
}

fn resource_owner_process(
    _: &DataPlaneRuntime,
    runtime_data: NodeRuntimeData,
    _: &mut hammer_core::data_plane::BufferFrame,
) -> NodeResult {
    OBSERVED_RESOURCE_RUNTIME_DATA.store(runtime_data.word(0), Ordering::Release);
    NodeResult::drop()
}

fn additive_probe_process(
    _: &DataPlaneRuntime,
    _: NodeRuntimeData,
    _: &mut hammer_core::data_plane::BufferFrame,
) -> NodeResult {
    NodeResult::drop()
}

fn failing_probe_process(
    _: &DataPlaneRuntime,
    _: NodeRuntimeData,
    _: &mut hammer_core::data_plane::BufferFrame,
) -> NodeResult {
    NodeResult::drop()
}

fn register_resource_owner(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Driver,
        NodeDescriptor::new(
            resource_owner_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("graph-update-resource-owner", 0),
            &[],
            None,
        ),
    )
}

fn register_additive_probe(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            additive_probe_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("plugin-graph-update-probe", 0),
            &[],
            None,
        ),
    )
}

fn register_failing_probe(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            failing_probe_process as NodeProcessFn,
            NodeRuntimeData::empty(),
            NodeRegistration::next("failing-graph-update-probe", 0),
            &[],
            None,
        ),
    )
}

fn ignore_resource_readiness(_: &mut File) -> HammerResult<()> {
    Ok(())
}

fn initialize_resource_owner_worker(engine: &mut Engine) -> HammerResult<()> {
    let worker = engine.data_worker_id()?;
    let source = RESOURCE_SOURCE.get().expect("resource source initialized");
    // SAFETY: F_DUPFD_CLOEXEC duplicates the live test source descriptor and
    // returns a fresh descriptor whose ownership transfers immediately below.
    let duplicated = unsafe { libc::fcntl(source.0.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    assert!(duplicated >= 0, "duplicate resource source descriptor");
    // SAFETY: fcntl returned a fresh descriptor with unique ownership.
    let descriptor = unsafe { OwnedFd::from_raw_fd(duplicated) };
    engine.file_main_mut()?.add(File::new(
        descriptor,
        worker,
        "graph update resource".to_owned(),
        0,
        FileFunctions {
            read: Some(ignore_resource_readiness),
            ..FileFunctions::default()
        },
    ))?;

    let node = engine
        .runtime
        .node_by_name("graph-update-resource-owner")
        .ok_or(CoreError::WorkerGraphUpdateMissing)?;
    engine.set_worker_node_runtime_data(
        node,
        NodeRuntimeData::from_words([RESOURCE_RUNTIME_SENTINEL, 0, 0, 0]),
    )?;
    engine
        .runtime
        .nodes()
        .set_node_state(node, NodeState::Interrupt)?;
    RESOURCE_WORKER_INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn initialize_additive_probe_worker(engine: &mut Engine) -> HammerResult<()> {
    let resource = engine
        .runtime
        .node_by_name("graph-update-resource-owner")
        .ok_or(CoreError::WorkerGraphUpdateMissing)?;
    if engine.runtime.nodes().node_state(resource)? != NodeState::Interrupt {
        return Err(CoreError::WorkerGraphUpdateMissing);
    }
    engine.runtime.schedule_empty_frame(resource)?;
    ADDITIVE_WORKER_INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn initialize_failing_probe_worker(engine: &mut Engine) -> HammerResult<()> {
    if engine
        .runtime
        .node_by_name("failing-graph-update-probe")
        .is_none()
    {
        return Err(CoreError::WorkerGraphUpdateMissing);
    }
    FAILING_WORKER_INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    if engine.thread_index == 1 {
        Err(CoreError::WorkerGraphUpdateMissing)
    } else {
        Ok(())
    }
}

static RESOURCE_WORKER_INITS: [InitFunction; 1] = [InitFunction {
    name: "graph_update_resource_worker_init",
    runs_before: &[],
    runs_after: &[],
    func: initialize_resource_owner_worker,
}];

static ADDITIVE_WORKER_INITS: [InitFunction; 1] = [InitFunction {
    name: "plugin_graph_update_probe_worker_init",
    runs_before: &[],
    runs_after: &["graph_update_resource_worker_init"],
    func: initialize_additive_probe_worker,
}];

static FAILING_WORKER_INITS: [InitFunction; 1] = [InitFunction {
    name: "failing_graph_update_probe_worker_init",
    runs_before: &[],
    runs_after: &["plugin_graph_update_probe_worker_init"],
    func: initialize_failing_probe_worker,
}];

static RESOURCE_NODES: [NodeEntry; 1] = [NodeEntry {
    registration: NodeRegistration::next("graph-update-resource-owner", 0),
    kind: NodeKind::Driver,
    init: register_resource_owner,
}];

static ADDITIVE_NODES: [NodeEntry; 1] = [NodeEntry {
    registration: NodeRegistration::next("plugin-graph-update-probe", 0),
    kind: NodeKind::Internal,
    init: register_additive_probe,
}];

static FAILING_NODES: [NodeEntry; 1] = [NodeEntry {
    registration: NodeRegistration::next("failing-graph-update-probe", 0),
    kind: NodeKind::Internal,
    init: register_failing_probe,
}];

unsafe fn link_registration_image(
    image: &'static hammer_runtime::__private::RegistrationImage,
    worker_inits: &'static [InitFunction],
    nodes: &'static [NodeEntry],
) {
    // SAFETY: every referenced inventory is test-binary static and remains
    // mapped until process exit, matching a successful DSO constructor.
    unsafe {
        image.link(
            &[],
            &[],
            &[],
            &[],
            &[],
            worker_inits,
            nodes,
            &[] as &[NodeFunctionRegistration],
            &[],
        );
    }
}

#[test]
fn additive_graph_updates_preserve_resources_and_terminate_all_workers_on_failure() {
    RESOURCE_WORKER_INIT_CALLS.store(0, Ordering::Relaxed);
    ADDITIVE_WORKER_INIT_CALLS.store(0, Ordering::Relaxed);
    FAILING_WORKER_INIT_CALLS.store(0, Ordering::Relaxed);
    OBSERVED_RESOURCE_RUNTIME_DATA.store(0, Ordering::Relaxed);
    let source = RESOURCE_SOURCE.get_or_init(|| UnixStream::pair().expect("resource source pair"));
    let resource_identity = descriptor_identity(source.0.as_raw_fd()).expect("resource identity");
    let baseline_resources = count_open_identity(resource_identity);

    let mut config = Config::default();
    config.worker.count = 2;
    let config = Arc::new(config);
    let registry = RuntimeRegistry::new();
    registry.set(Arc::clone(&config));
    let runtime = hammer_runtime::new_worker_runtime(&config).expect("runtime");
    let mut pool = EnginePool::new(Engine::new(runtime, registry));

    hammer_runtime::memory::memory_init(pool.main_engine_mut(), Arc::clone(&config))
        .expect("memory init");
    // SAFETY: the linked registrations have process lifetime.
    unsafe {
        link_registration_image(
            &RESOURCE_PLUGIN_IMAGE,
            &RESOURCE_WORKER_INITS,
            &RESOURCE_NODES,
        );
    }
    pool.main_engine_mut()
        .load_plugins(std::path::Path::new("unused"), &[])
        .expect("install startup registrations");
    hammer_runtime::init::run_main_loop_enter(pool.main_engine_mut()).expect("start data workers");

    // SAFETY: the linked registrations have process lifetime.
    unsafe {
        link_registration_image(
            &ADDITIVE_PLUGIN_IMAGE,
            &ADDITIVE_WORKER_INITS,
            &ADDITIVE_NODES,
        );
    }
    pool.main_engine_mut()
        .load_plugins(std::path::Path::new("unused"), &[])
        .expect("materialize additive registration image");
    let resource_calls_after_add = RESOURCE_WORKER_INIT_CALLS.load(Ordering::Relaxed);
    let additive_calls_after_add = ADDITIVE_WORKER_INIT_CALLS.load(Ordering::Relaxed);
    let resources_after_add = count_open_identity(resource_identity);
    let runtime_data_preserved = wait_until(Duration::from_secs(2), || {
        OBSERVED_RESOURCE_RUNTIME_DATA.load(Ordering::Acquire) == RESOURCE_RUNTIME_SENTINEL
    });

    // SAFETY: the linked registrations have process lifetime.
    unsafe {
        link_registration_image(&FAILING_PLUGIN_IMAGE, &FAILING_WORKER_INITS, &FAILING_NODES);
    }
    let update_error = pool
        .main_engine_mut()
        .load_plugins(std::path::Path::new("unused"), &[])
        .expect_err("one worker update must fail");
    let all_workers_terminated = wait_until(Duration::from_secs(2), || {
        count_open_identity(resource_identity) == baseline_resources
    });
    pool.close().expect("join data workers");

    assert!(matches!(update_error, CoreError::WorkerGraphUpdateMissing));
    assert_eq!(resource_calls_after_add, 2);
    assert_eq!(additive_calls_after_add, 2);
    assert_eq!(resources_after_add, baseline_resources + 2);
    assert!(runtime_data_preserved);
    assert!(all_workers_terminated);
    assert_eq!(RESOURCE_WORKER_INIT_CALLS.load(Ordering::Relaxed), 2);
    assert_eq!(ADDITIVE_WORKER_INIT_CALLS.load(Ordering::Relaxed), 2);
    assert_eq!(FAILING_WORKER_INIT_CALLS.load(Ordering::Relaxed), 2);
    assert_eq!(count_open_identity(resource_identity), baseline_resources);
}
