use std::cell::UnsafeCell;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use hammer_adapter::{DataWorkerId, NodeHandle};
use hammer_control::{
    CertificateProviderManager, CertificateStore, ConnectionManager, NetworkManager, PauseManager,
    ServiceManager,
};
use hammer_core::config::{self, Trace};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::{ALL_STAGES, LIFECYCLE_ORDER};
use hammer_core::log::{DiscardWriter, Factory, LogWriter, Logger};
use hammer_core::registry::RuntimeRegistry;
use hammer_infra::map::FlatHashTable;
use hammer_infra::segment::{Local, Segment};
use hammer_runtime::adapter::{
    Lifecycle, NetworkManager as _, PlatformInterface, TraceControlPlane, TraceRecordSink,
};
use hammer_runtime::app::AppContext;
use hammer_runtime::spawn::{DataRuntime, DataRuntimeContext};
use hammer_runtime::{
    ControlEventSubscriptionHandle, ControlThread, ControlThreadHandle, ControlTimerHandle,
    EventSubscriberBuilder, MetricSample, MetricsRegistry,
};
use std::time::Duration;

use crate::app::AppHost;
use crate::transport::tcp::TcpInputControlPlane;
use crate::transport::tcp::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpListenerAddress, TcpListenerLookupAccess,
    TcpLookupId, TcpLookupSnapshot, TcpLookupValue, TcpV4ListenerKey, TcpV6ListenerKey,
};
use hammer_core::protocol::tcp::TcpCapabilities;

const CONTROL_THREAD_STACK_SIZE: usize = 512 * 1024;
const METRICS_LOG_INTERVAL: Duration = Duration::from_secs(30);
const TRACE_DRAIN_INTERVAL: Duration = Duration::from_secs(1);
/// Time budget for the control thread to drain queued logs and emit a
/// final metrics dump on shutdown. 500ms was too tight when the log queue
/// (4096 entries) is full and each iOS write costs tens of milliseconds;
/// 2s leaves headroom for the worst-case drain without making `close()`
/// feel stuck on the FFI side.
const CONTROL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Time budget the data-plane runtime gets to abort in-flight tasks
/// during `close()`. Data tasks (TCP/UDP forwarders) are expected to drop fast
/// once their lifecycles are closed; tasks still running past this deadline are
/// forcibly aborted by the runtime.
const DATA_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

fn debug_assert_lifecycle_order(lifecycles: &[Arc<dyn Lifecycle>]) {
    let mut previous = None;
    for lifecycle in lifecycles {
        let name = lifecycle.name();
        let Some(index) = LIFECYCLE_ORDER
            .iter()
            .position(|expected| *expected == name)
        else {
            debug_assert!(false, "unknown lifecycle '{name}'");
            continue;
        };
        if let Some(previous) = previous {
            debug_assert!(
                previous <= index,
                "Service lifecycles must follow LIFECYCLE_ORDER"
            );
        }
        previous = Some(index);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    Closed,
}

pub struct RuntimeService<S: Segment = Local> {
    inner: Arc<Mutex<ServiceInner<S>>>,
}

struct ServiceInner<S: Segment> {
    state: ServiceState,
    log_factory: Arc<Factory>,
    control_handle: Option<Arc<ControlThreadHandle>>,
    control_thread: Option<JoinHandle<()>>,
    trace_drain_timer: Option<ControlTimerHandle>,
    event_subscriptions: Vec<ControlEventSubscriptionHandle>,
    metrics: Arc<MetricsRegistry>,
    registry: Arc<RuntimeRegistry>,
    lifecycles: Vec<Arc<dyn Lifecycle>>,
    pause: Arc<PauseManager>,
    network: Arc<NetworkManager>,
    /// Data-plane runtime that hosts every business future spawned via
    /// `hammer_runtime::spawn::spawn`. Held in `Option` so `finish_close` can
    /// `take()` and consume it via `Runtime::shutdown_timeout` to bound
    /// the abort latency of in-flight tasks. If `None`, shutdown has
    /// already happened or the runtime was never installed.
    data_runtime: Option<DataRuntime>,
    data_context: DataRuntimeContext,
    app_context: AppContext<S>,
    tcp_listener_control: RuntimeTcpListenerControlHandle,
}

#[derive(Clone)]
struct TcpListenerRegistration {
    lookup_id: TcpLookupId,
    owner_worker: DataWorkerId,
    bind: SocketAddr,
    capabilities: TcpCapabilities,
}

struct RuntimeTcpListenerControlState {
    next_tcp_lookup_id: TcpLookupId,
    tcp_control: TcpInputControlPlane,
    tcp_lookup: TcpLookupSnapshot,
    tcp_listeners: hammer_infra::vec::Vec<TcpListenerRegistration>,
    tcp_listener_slots: FlatHashTable<u64, usize>,
}

struct RuntimeTcpListenerControlCell {
    inner: UnsafeCell<RuntimeTcpListenerControlState>,
}

impl RuntimeTcpListenerControlCell {
    #[inline]
    fn new(state: RuntimeTcpListenerControlState) -> Self {
        Self {
            inner: UnsafeCell::new(state),
        }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut RuntimeTcpListenerControlState {
        unsafe { &mut *self.inner.get() }
    }
}

// SAFETY: access is serialized by RuntimeTcpListenerControlHandle through the
// single control thread. The cell is never mutated concurrently from multiple
// threads.
unsafe impl Send for RuntimeTcpListenerControlCell {}
// SAFETY: shared references may cross threads, but all dereferences route
// through the control-thread serialization contract above.
unsafe impl Sync for RuntimeTcpListenerControlCell {}

#[derive(Clone)]
struct RuntimeTcpListenerControlHandle {
    state: Arc<RuntimeTcpListenerControlCell>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeTcpListenerControlSnapshot {
    tcp_listeners: hammer_infra::vec::Vec<RuntimeTcpListenerSnapshot>,
    tcp_lookup: TcpLookupSnapshot,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct RuntimeTcpListenerSnapshot {
    lookup_id: TcpLookupId,
}

impl RuntimeTcpListenerControlState {
    fn new() -> HammerResult<Self> {
        let tcp_control = crate::transport::tcp::TCP_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| HammerError::internal("tcp main not initialized"))?
            .control()
            .clone();

        Ok(Self {
            next_tcp_lookup_id: 1,
            tcp_control,
            tcp_lookup: TcpLookupSnapshot::empty(),
            tcp_listeners: hammer_infra::vec::Vec::new(),
            tcp_listener_slots: FlatHashTable::new(),
        })
    }

    fn bind_tcp_listener(
        &mut self,
        bind: SocketAddr,
        owner_worker: usize,
        capabilities: TcpCapabilities,
    ) -> HammerResult<TcpLookupId> {
        if self
            .tcp_listeners
            .iter()
            .any(|registration| registration.bind == bind)
        {
            return Err(HammerError::internal(format!(
                "tcp listener {bind} is already registered"
            )));
        }

        let lookup_id = self.alloc_tcp_lookup_id()?;
        self.tcp_listeners.push(TcpListenerRegistration {
            lookup_id,
            owner_worker: worker_id(owner_worker)?,
            bind,
            capabilities,
        });
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()?;

        Ok(lookup_id)
    }

    fn close_tcp_listener(&mut self, lookup_id: TcpLookupId) -> HammerResult<()> {
        let slot = self
            .tcp_listener_slots
            .lookup(&u64::from(lookup_id))
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "tcp listener {lookup_id} is not registered in runtime service"
                ))
            })?;
        self.tcp_listeners
            .drain(slot..slot + 1)
            .next()
            .expect("tcp listener exists at computed slot");
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()
    }

    fn alloc_tcp_lookup_id(&mut self) -> HammerResult<TcpLookupId> {
        let id = self.next_tcp_lookup_id;
        self.next_tcp_lookup_id = self
            .next_tcp_lookup_id
            .checked_add(1)
            .ok_or_else(|| HammerError::internal("tcp lookup id overflow"))?;
        Ok(id)
    }

    fn publish_tcp_lookup(&mut self) -> HammerResult<()> {
        let mut snapshot = TcpLookupSnapshot::empty();
        for registration in self.tcp_listeners.iter().cloned() {
            let value = TcpLookupValue {
                id: registration.lookup_id,
                owner_worker: registration.owner_worker,
                capabilities: registration.capabilities,
            };
            insert_tcp_listener_key(&mut snapshot, registration.bind, value);
        }
        self.tcp_control
            .publish_lookup(snapshot.clone())
            .map_err(|err| HammerError::internal(format!("publish tcp lookup snapshot: {err}")))?;
        self.tcp_lookup = snapshot;
        Ok(())
    }

    fn rebuild_tcp_listener_slots(&mut self) {
        let mut slots = FlatHashTable::new();
        for (index, registration) in self.tcp_listeners.iter().cloned().enumerate() {
            slots.insert(u64::from(registration.lookup_id), index);
        }
        self.tcp_listener_slots = slots;
    }
}

fn insert_tcp_listener_key(
    snapshot: &mut TcpLookupSnapshot,
    bind: SocketAddr,
    value: TcpLookupValue,
) {
    match bind.ip() {
        IpAddr::V4(addr) => insert_typed_tcp_listener::<TcpIpv4ListenerAddress>(
            snapshot,
            TcpV4ListenerKey::new(0, addr, bind.port()),
            value,
        ),
        IpAddr::V6(addr) => insert_typed_tcp_listener::<TcpIpv6ListenerAddress>(
            snapshot,
            TcpV6ListenerKey::new(0, addr, bind.port()),
            value,
        ),
    }
}

fn insert_typed_tcp_listener<A>(
    snapshot: &mut TcpLookupSnapshot,
    key: A::Key,
    value: TcpLookupValue,
) where
    A: TcpListenerAddress,
    TcpLookupSnapshot: TcpListenerLookupAccess<A>,
{
    snapshot.insert_listener::<A>(key, value);
}

impl RuntimeTcpListenerControlHandle {
    #[inline]
    fn new(state: RuntimeTcpListenerControlState) -> Self {
        Self {
            state: Arc::new(RuntimeTcpListenerControlCell::new(state)),
        }
    }

    fn bind_tcp_listener_on_control(
        &self,
        bind: SocketAddr,
        owner_worker: usize,
        capabilities: TcpCapabilities,
    ) -> HammerResult<TcpLookupId> {
        // SAFETY: callers use this only from RuntimeService::control_call, so
        // execution is already serialized on the control thread.
        let state = unsafe { self.state.get_mut() };
        state.bind_tcp_listener(bind, owner_worker, capabilities)
    }

    fn close_tcp_listener_on_control(&self, lookup_id: TcpLookupId) -> HammerResult<()> {
        // SAFETY: callers use this only from RuntimeService::control_call, so
        // execution is already serialized on the control thread.
        let state = unsafe { self.state.get_mut() };
        state.close_tcp_listener(lookup_id)
    }

    #[cfg(test)]
    fn snapshot_for_test_on_control(&self) -> RuntimeTcpListenerControlSnapshot {
        // SAFETY: callers use this only from RuntimeService::control_call, so
        // execution is already serialized on the control thread.
        let state = unsafe { &*self.state.inner.get() };
        let mut tcp_listeners = hammer_infra::vec::Vec::new();
        for registration in state.tcp_listeners.iter() {
            tcp_listeners.push(RuntimeTcpListenerSnapshot {
                lookup_id: registration.lookup_id,
            });
        }
        RuntimeTcpListenerControlSnapshot {
            tcp_listeners,
            tcp_lookup: state.tcp_lookup.clone(),
        }
    }
}

impl RuntimeService<Local> {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
    ) -> HammerResult<Arc<Self>> {
        Self::new_with_event_subscribers(
            config_content,
            platform,
            writer,
            crate::event_subscribers::build_standard_event_subscribers,
        )
    }

    pub fn new_with_event_subscribers(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
        build_event_subscribers: EventSubscriberBuilder,
    ) -> HammerResult<Arc<Self>> {
        let config = config::parse_config(config_content)?;
        let metrics = MetricsRegistry::new();
        let trace = TraceControlPlane::new(config.trace.record_capacity);
        let base_time = Instant::now();
        let worker = &config.worker;
        let data_runtime = DataRuntime::from_config(worker, "hammer-data")?;
        let data_context = data_runtime.context();
        let app_context = AppContext::new(
            data_context.clone(),
            hammer_runtime::app::AppSessionConfig::new(
                worker.app_session.fifo_capacity,
                worker.app_session.evt_q_capacity,
            ),
        );
        let registry = RuntimeRegistry::new();
        registry.set::<config::Config>(Arc::new(config.clone()));
        let mut engine = hammer_runtime::Engine::new(
            hammer_runtime::new_worker_runtime(
                worker.buffer.slot_bytes,
                worker.buffer.slots_per_numa,
            ),
            Arc::clone(&registry),
        );
        hammer_runtime::init::run_init_functions(&mut engine)?;
        let listener_state = RuntimeTcpListenerControlState::new()?;
        let handoff_node_handle = NodeHandle::new(worker.handoff.node_handle);
        let _ = crate::packet_graph::WORKER_HANDOFF_NODE_HANDLE.set(handoff_node_handle);

        let worker_graph_nodes = data_context.install_on_workers(move |worker, runtime| {
            let runtime = runtime
                .clone()
                .with_handoff_node_handle(handoff_node_handle);
            let worker_id = DataWorkerId::new(u32::try_from(worker).map_err(|_| {
                hammer_core::error::CoreError::internal("worker index does not fit into u32")
            })?);
            crate::transport::tcp::install_tcp_worker_state(
                crate::transport::tcp::TcpWorkerOwnedState::new(worker_id),
            );
            runtime.init_graph(worker, &crate::packet_graph::SERVICE_GRAPH_NODES)?;
            crate::net::wire_ip_lookup_drop(&runtime)?;
            crate::transport::tcp::wire_worker_graph(&runtime, worker)?;
            Ok::<_, hammer_core::error::CoreError>(
                crate::packet_graph::SERVICE_GRAPH_NODES
                    .iter()
                    .filter_map(|entry| {
                        entry
                            .registration
                            .name()
                            .and_then(|name| runtime.node_by_name(name).map(|id| (name, id)))
                    })
                    .collect::<Vec<_>>(),
            )
        })?;
        let mut graph_node_ids = Vec::new();
        for nodes in worker_graph_nodes {
            let nodes = nodes.map_err(HammerError::from)?;
            if graph_node_ids.is_empty() {
                graph_node_ids = nodes;
            }
        }
        let writer: Arc<dyn LogWriter> = if config.log.disabled {
            Arc::new(DiscardWriter)
        } else {
            writer
        };
        let (control_handle, control_loop) = ControlThread::new(
            base_time,
            writer,
            Arc::clone(&metrics),
            METRICS_LOG_INTERVAL,
            config.log.level,
        );
        let control_thread = match spawn_control_thread(control_loop) {
            Ok(handle) => handle,
            Err(err) => {
                // Avoid leaking the data runtime if control setup fails —
                // its workers and reactor would otherwise live until the
                // process exits.
                data_runtime.shutdown_timeout(DATA_SHUTDOWN_TIMEOUT);
                return Err(err);
            }
        };
        let tcp_listener_control = RuntimeTcpListenerControlHandle::new(listener_state);
        let writer: Arc<dyn LogWriter> = Arc::clone(&control_handle) as Arc<dyn LogWriter>;
        let log_factory = Factory::new_with_min_level(base_time, writer, config.log.level);
        let trace_enabled_with_inputs = trace_worker_control(&config.trace);
        trace.publish_options(&config.trace, |name| {
            graph_node_ids
                .iter()
                .find_map(|(node_name, id)| (*node_name == name).then_some(*id))
        })?;
        data_context.set_trace_control_on_workers(
            trace_enabled_with_inputs.then(|| trace.handle()),
            config.trace.packet_capacity,
        )?;
        let event_subscriptions = build_event_subscribers(
            new_logger(&log_factory, "control-event"),
            Arc::clone(&control_handle),
        )?;
        let pause = Arc::new(PauseManager::new());

        let cert_store = Arc::new(CertificateStore::new(
            new_logger(&log_factory, "certificate-store"),
            false,
        ));
        let cert_provider = Arc::new(CertificateProviderManager::new(new_logger(
            &log_factory,
            "certificate-provider",
        )));
        let connection = Arc::new(ConnectionManager::new());
        let network = NetworkManager::with_platform(
            new_logger(&log_factory, "network"),
            false,
            Arc::clone(&platform),
            Arc::clone(&pause),
            Arc::clone(&connection),
        );
        let service_mgr = Arc::new(ServiceManager::new(new_logger(&log_factory, "service")));

        registry.set::<CertificateStore>(Arc::clone(&cert_store));
        registry.set::<CertificateProviderManager>(Arc::clone(&cert_provider));
        registry.set::<NetworkManager>(Arc::clone(&network));
        registry.set::<ServiceManager>(Arc::clone(&service_mgr));
        registry.set::<ConnectionManager>(Arc::clone(&connection));
        registry.set::<PauseManager>(Arc::clone(&pause));
        registry.set::<MetricsRegistry>(Arc::clone(&metrics));

        let lifecycles: Vec<Arc<dyn Lifecycle>> = vec![
            cert_store as Arc<dyn Lifecycle>,
            cert_provider as Arc<dyn Lifecycle>,
            Arc::clone(&network) as Arc<dyn Lifecycle>,
            service_mgr as Arc<dyn Lifecycle>,
            connection as Arc<dyn Lifecycle>,
        ];

        debug_assert_lifecycle_order(&lifecycles);

        let trace_drain_timer = if trace_enabled_with_inputs {
            Some(schedule_trace_drain(
                Arc::clone(&control_handle),
                data_context.clone(),
                trace.sink(),
                new_logger(&log_factory, "trace-control"),
            )?)
        } else {
            None
        };

        Ok(Arc::new(Self {
            inner: Arc::new(Mutex::new(ServiceInner {
                state: ServiceState::NotStarted,
                log_factory,
                control_handle: Some(control_handle),
                control_thread: Some(control_thread),
                trace_drain_timer,
                event_subscriptions,
                metrics,
                registry,
                lifecycles,
                pause,
                network,
                data_runtime: Some(data_runtime),
                data_context,
                app_context,
                tcp_listener_control,
            })),
        }))
    }
}

impl<S: Segment> RuntimeService<S> {
    #[inline]
    pub fn app_context(&self) -> AppContext<S> {
        self.inner
            .lock()
            .expect("service mutex poisoned")
            .app_context
            .clone()
    }

    pub fn bind_tcp_listener(
        &self,
        bind: SocketAddr,
        owner_worker: usize,
        capabilities: TcpCapabilities,
    ) -> HammerResult<TcpLookupId> {
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            inner.tcp_listener_control.bind_tcp_listener_on_control(
                bind,
                owner_worker,
                capabilities,
            )
        })?
    }

    pub fn close_tcp_listener(&self, listener: TcpLookupId) -> HammerResult<()> {
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            inner
                .tcp_listener_control
                .close_tcp_listener_on_control(listener)
        })?
    }

    pub fn start(&self) -> HammerResult<()> {
        let result = self.control_blocking_call(start_inner)?;
        if result.is_err() {
            let _ = self.close();
        }
        result
    }

    pub fn close(&self) -> HammerResult<()> {
        let needs_lifecycle_close =
            { self.inner.lock().expect("service mutex poisoned").state != ServiceState::Closed };
        let result = if needs_lifecycle_close {
            let Some(control_handle) = self.control_handle() else {
                return Ok(());
            };
            let inner = Arc::clone(&self.inner);
            match control_handle.call_blocking(move || {
                let mut inner = inner.lock().expect("service mutex poisoned");
                let _dispatch_guard =
                    tracing::dispatcher::set_default(inner.log_factory.dispatch());
                close_inner(&mut inner)
            }) {
                Ok(result) => result,
                Err(err) => Err(err),
            }
        } else {
            Ok(())
        };
        finish_close(&self.inner, result)
    }

    pub fn pause(&self) {
        let _ = self.control_call(|inner| inner.pause.pause());
    }

    pub fn wake(&self) {
        let _ = self.control_call(|inner| inner.pause.wake());
    }

    pub fn reset_network(&self) {
        let _ = self.control_call(|inner| {
            if inner.state != ServiceState::Running {
                return;
            }
            inner.network.reset_network();
        });
    }

    pub fn need_wifi_state(&self) -> bool {
        self.control_call(|inner| inner.network.need_wifi_state())
            .unwrap_or(false)
    }

    pub fn update_wifi_state(&self) {
        let _ = self.control_call(|inner| inner.network.update_wifi_state());
    }

    /// Snapshot the live metric registry. Returns an empty vector when the
    /// service is closed — the control thread already emits a final dump
    /// during shutdown, so callers querying after `close()` should consult
    /// the log instead of digging into post-mortem registry state.
    pub fn metrics_snapshot(&self) -> Vec<MetricSample> {
        self.control_call(|inner| inner.metrics.snapshot())
            .unwrap_or_default()
    }

    pub fn spawn_app_on_worker<F, Fut>(&self, worker: usize, factory: F) -> HammerResult<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            inner.data_context.spawn_local_on_worker(worker, factory)
        })?
    }

    pub fn register_app_host<T>(&self, host: Arc<T>) -> HammerResult<()>
    where
        T: AppHost + 'static,
    {
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }

            inner.registry.set::<T>(Arc::clone(&host));
            let lifecycle = host as Arc<dyn Lifecycle>;
            if inner.state == ServiceState::Running {
                start_lifecycle_now(&lifecycle)?;
            }
            inner.lifecycles.push(lifecycle);
            Ok(())
        })?
    }

    fn control_call<R>(
        &self,
        f: impl FnOnce(&mut ServiceInner<S>) -> R + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let control_handle = self
            .control_handle()
            .ok_or_else(HammerError::service_closed)?;
        let inner = Arc::clone(&self.inner);
        control_handle.call(move || {
            let mut inner = inner.lock().expect("service mutex poisoned");
            let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
            let data_context = inner.data_context.clone();
            data_context.enter(|| f(&mut inner))
        })
    }

    fn control_blocking_call<R>(
        &self,
        f: impl FnOnce(&mut ServiceInner<S>) -> R + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let control_handle = self
            .control_handle()
            .ok_or_else(HammerError::service_closed)?;
        let inner = Arc::clone(&self.inner);
        control_handle.call_blocking(move || {
            let mut inner = inner.lock().expect("service mutex poisoned");
            let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
            let data_context = inner.data_context.clone();
            data_context.enter(|| f(&mut inner))
        })
    }

    fn control_handle(&self) -> Option<Arc<ControlThreadHandle>> {
        self.inner
            .lock()
            .expect("service mutex poisoned")
            .control_handle
            .clone()
    }
}

fn new_logger(factory: &Arc<Factory>, id: &str) -> Logger {
    factory.new_logger(id.to_owned())
}

#[inline]
fn worker_id(worker: usize) -> HammerResult<DataWorkerId> {
    u32::try_from(worker)
        .map(DataWorkerId::new)
        .map_err(|_| HammerError::internal(format!("worker index {worker} does not fit into u32")))
}

fn trace_worker_control(trace: &Trace) -> bool {
    trace.enabled && !trace.inputs.is_empty()
}

fn schedule_trace_drain(
    control_handle: Arc<ControlThreadHandle>,
    data_context: DataRuntimeContext,
    sink: TraceRecordSink,
    logger: Logger,
) -> HammerResult<ControlTimerHandle> {
    control_handle.schedule_interval(TRACE_DRAIN_INTERVAL, TRACE_DRAIN_INTERVAL, move || {
        let data_context = data_context.clone();
        let sink = sink.clone();
        let logger = logger.clone();
        async move {
            if let Err(err) =
                data_context.drain_trace_records_on_workers_with_logger(sink, logger.clone())
            {
                logger.warn(format!("drain packet trace records: {err}"));
            }
        }
    })
}

#[cfg(test)]
fn assert_service_graph_tcp_nodes_declared() {
    let names: Vec<&'static str> = crate::packet_graph::SERVICE_GRAPH_NODES
        .iter()
        .filter_map(|entry| entry.registration.name())
        .collect();
    for want in [
        "session-queue",
        "handoff",
        "tcp-input",
        "tcp-listen",
        "tcp-rcv-process",
        "tcp-syn-sent",
        "tcp-established",
        "tcp-reset",
    ] {
        assert!(names.iter().any(|name| *name == want), "missing {want}");
    }
}

#[cfg(test)]
#[test]
fn service_packet_graph_resolves_tcp_nodes() {
    assert_service_graph_tcp_nodes_declared();
}

fn start_inner<S: Segment>(inner: &mut ServiceInner<S>) -> HammerResult<()> {
    match inner.state {
        ServiceState::Closed => return Err(HammerError::service_closed()),
        ServiceState::Running => return Ok(()),
        ServiceState::NotStarted => {}
    }
    inner.state = ServiceState::Running;
    let lifecycles = inner.lifecycles.clone();
    for stage in ALL_STAGES {
        for lc in &lifecycles {
            if let Err(err) = lc.start(stage) {
                inner.state = ServiceState::Closed;
                let close_err = close_lifecycles(&lifecycles);
                let combined = HammerError::lifecycle(stage.name(), err.to_string());
                return match close_err {
                    Ok(()) => Err(combined),
                    Err(close_err) => Err(HammerError::lifecycle(
                        stage.name(),
                        format!("{combined}; close after failure: {close_err}"),
                    )),
                };
            }
        }
    }
    Ok(())
}

fn close_inner<S: Segment>(inner: &mut ServiceInner<S>) -> HammerResult<()> {
    if inner.state == ServiceState::Closed {
        Ok(())
    } else {
        inner.state = ServiceState::Closed;
        close_lifecycles(&inner.lifecycles)
    }
}

fn close_lifecycles(lifecycles: &[Arc<dyn Lifecycle>]) -> HammerResult<()> {
    let mut errors = Vec::new();
    for lc in lifecycles.iter().rev() {
        if let Err(err) = lc.close() {
            errors.push(format!("{}: {}", lc.name(), err));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(HammerError::internal(errors.join("; ")))
    }
}

fn start_lifecycle_now(lifecycle: &Arc<dyn Lifecycle>) -> HammerResult<()> {
    for stage in ALL_STAGES {
        lifecycle
            .start(stage)
            .map_err(|err| HammerError::lifecycle(stage.name(), err.to_string()))?;
    }
    Ok(())
}

fn finish_close<S: Segment>(
    inner: &Arc<Mutex<ServiceInner<S>>>,
    close_result: HammerResult<()>,
) -> HammerResult<()> {
    // Lifecycles have already been closed on hammer-main, so worker-owned tasks
    // have had a chance to emit their shutdown logs while the control handle was
    // still open. The Tokio runtime itself must outlive this step because the
    // control loop is driven by a handle to that same runtime.
    let mut result = close_result;
    let event_subscriptions = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .event_subscriptions
            .drain(..)
            .collect::<Vec<_>>()
    };
    for subscription in &event_subscriptions {
        subscription.cancel();
    }
    drop(event_subscriptions);
    let trace_drain_timer = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .trace_drain_timer
            .take()
    };
    if let Some(timer) = trace_drain_timer {
        timer.cancel_timeout(CONTROL_SHUTDOWN_TIMEOUT);
    }
    let control_handle = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .control_handle
            .clone()
    };
    if let Some(control_handle) = control_handle
        && !control_handle.is_closed()
    {
        if !control_handle.flush_timeout(CONTROL_SHUTDOWN_TIMEOUT) {
            result = combine_close_error(result, "control thread log flush timed out");
        }
        if !control_handle.shutdown_timeout(CONTROL_SHUTDOWN_TIMEOUT) {
            result = combine_close_error(result, "control thread shutdown timed out");
        }
    }
    let control_thread = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .control_thread
            .take()
    };
    let mut control_finished = control_thread.is_none();
    if let Some(control_thread) = control_thread {
        match join_control_thread_timeout(control_thread, CONTROL_SHUTDOWN_TIMEOUT) {
            Ok(()) => {
                inner
                    .lock()
                    .expect("service mutex poisoned")
                    .control_handle
                    .take();
                control_finished = true;
            }
            Err(JoinControlThreadError::Timeout(control_thread)) => {
                inner.lock().expect("service mutex poisoned").control_thread = Some(control_thread);
                result = combine_close_error(result, "control thread join timed out");
            }
            Err(JoinControlThreadError::Panicked) => {
                inner
                    .lock()
                    .expect("service mutex poisoned")
                    .control_handle
                    .take();
                result = combine_close_error(result, "control thread panicked");
                // A panicked control thread is dead — its `block_on` has
                // unwound, so the data runtime is safe to abort below.
                control_finished = true;
            }
        }
    }
    // Tear down the data-plane runtime explicitly so any in-flight task
    // is bounded by `DATA_SHUTDOWN_TIMEOUT` rather than the (unbounded)
    // implicit drop. Skipped if the control thread join timed out —
    // its log/metrics work would race with abort. The next `close()`
    // attempt will retry the join and reach this branch.
    if control_finished {
        let data_runtime = {
            let mut inner = inner.lock().expect("service mutex poisoned");
            inner.data_runtime.take()
        };
        if let Some(data_runtime) = data_runtime {
            data_runtime.shutdown_timeout(DATA_SHUTDOWN_TIMEOUT);
        }
    }
    result
}

fn spawn_control_thread(control_loop: ControlThread) -> HammerResult<JoinHandle<()>> {
    // Build the control-plane runtime on the caller's thread so a build
    // failure surfaces synchronously as `HammerError`. Doing it inside
    // the spawned closure would force a panic on a background thread,
    // which the FFI layer cannot translate into a typed error.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| HammerError::internal(format!("build control runtime: {e}")))?;
    thread::Builder::new()
        .name("hammer-main".to_owned())
        .stack_size(CONTROL_THREAD_STACK_SIZE)
        .spawn(move || {
            runtime.block_on(control_loop.run());
            // `runtime` drops here, after the control loop exits. A
            // current_thread runtime drop is non-blocking (it has no
            // worker threads to join), so this is the natural teardown
            // point for the control plane.
        })
        .map_err(|e| HammerError::internal(format!("spawn control thread: {e}")))
}

fn join_control_thread_timeout(
    control_thread: JoinHandle<()>,
    timeout: Duration,
) -> Result<(), JoinControlThreadError> {
    let deadline = Instant::now() + timeout;
    while !control_thread.is_finished() {
        let now = Instant::now();
        if now >= deadline {
            return Err(JoinControlThreadError::Timeout(control_thread));
        }
        thread::sleep((deadline - now).min(Duration::from_millis(10)));
    }
    control_thread
        .join()
        .map_err(|_| JoinControlThreadError::Panicked)
}

enum JoinControlThreadError {
    Timeout(JoinHandle<()>),
    Panicked,
}

impl fmt::Display for JoinControlThreadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinControlThreadError::Timeout(_) => f.write_str("control thread join timed out"),
            JoinControlThreadError::Panicked => f.write_str("control thread panicked"),
        }
    }
}

fn combine_close_error(result: HammerResult<()>, message: impl Into<String>) -> HammerResult<()> {
    let message = message.into();
    match result {
        Ok(()) => Err(HammerError::internal(message)),
        Err(err) => Err(HammerError::internal(format!("{err}; {message}"))),
    }
}

#[cfg(test)]
impl RuntimeService<Local> {
    fn tcp_listener_control_snapshot_for_test(&self) -> RuntimeTcpListenerControlSnapshot {
        self.control_call(|inner| inner.tcp_listener_control.snapshot_for_test_on_control())
            .expect("snapshot runtime tcp listener control")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::StartStage;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

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

    struct TestAppHost {
        starts: AtomicUsize,
        closes: AtomicUsize,
    }

    impl TestAppHost {
        fn new() -> Self {
            Self {
                starts: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
            }
        }
    }

    impl Lifecycle for TestAppHost {
        fn name(&self) -> &str {
            "test-app-host"
        }

        fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn close(&self) -> Result<(), HammerError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn minimal_config(extra: &str) -> String {
        format!(
            r#"
[log]
level = "debug"

{extra}
"#
        )
    }

    fn new_test_service(trace: &str) -> TestService {
        let guard = SERVICE_TEST_GUARD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        crate::reset_subsystem_mains_for_test();
        let service = RuntimeService::new(
            &minimal_config(trace),
            Arc::new(NoopPlatform),
            Arc::new(DiscardWriter),
        )
        .expect("test service should build");
        TestService {
            service,
            _guard: guard,
        }
    }

    static SERVICE_TEST_GUARD: Mutex<()> = Mutex::new(());

    struct TestService {
        service: Arc<RuntimeService<Local>>,
        _guard: MutexGuard<'static, ()>,
    }

    impl std::ops::Deref for TestService {
        type Target = RuntimeService<Local>;

        fn deref(&self) -> &RuntimeService<Local> {
            &self.service
        }
    }

    fn has_trace_drain_timer(service: &RuntimeService) -> bool {
        service
            .inner
            .lock()
            .expect("service mutex poisoned")
            .trace_drain_timer
            .is_some()
    }

    #[test]
    fn join_control_thread_timeout_returns_without_waiting_forever() {
        let handle = thread::spawn(|| thread::sleep(Duration::from_millis(200)));
        let start = Instant::now();

        let result = join_control_thread_timeout(handle, Duration::from_millis(20));

        assert!(result.is_err(), "slow join should time out");
        assert!(
            start.elapsed() < Duration::from_millis(120),
            "join timeout should return promptly"
        );
    }

    #[test]
    fn join_control_thread_timeout_returns_handle_on_timeout() {
        let handle = thread::spawn(|| thread::sleep(Duration::from_millis(80)));

        let result = join_control_thread_timeout(handle, Duration::from_millis(10));

        let Err(JoinControlThreadError::Timeout(handle)) = result else {
            panic!("expected timeout with recoverable join handle");
        };
        handle.join().expect("thread should still be joinable");
    }

    #[test]
    fn trace_worker_control_is_only_installed_for_enabled_inputs() {
        let mut trace = Trace {
            enabled: false,
            record_capacity: 8,
            packet_capacity: 4,
            inputs: vec![config::TraceInput {
                node: "tun-input-driver".to_owned(),
                count: 1,
            }],
        };
        assert!(!trace_worker_control(&trace));

        trace.enabled = true;
        trace.inputs.clear();
        assert!(!trace_worker_control(&trace));

        trace.inputs.push(config::TraceInput {
            node: "tun-input-driver".to_owned(),
            count: 1,
        });
        assert!(trace_worker_control(&trace));
    }

    #[test]
    fn trace_drain_timer_is_only_installed_for_enabled_inputs() {
        {
            let disabled = new_test_service(
                r#"[trace]
enabled = false

[[trace.inputs]]
node = "tun-input-driver"
count = 1
"#,
            );
            assert!(!has_trace_drain_timer(&disabled.service));
            disabled.close().expect("close disabled trace service");
        }

        let empty = new_test_service(
            r#"[trace]
enabled = true
"#,
        );
        assert!(!has_trace_drain_timer(&empty.service));
        empty.close().expect("close empty trace service");
    }

    #[test]
    fn service_packet_graph_resolves_tcp_nodes() {
        assert_service_graph_tcp_nodes_declared();
    }

    #[test]
    fn runtime_service_installs_session_queue_on_data_workers() {
        let service = new_test_service("");
        let data_context = service
            .inner
            .lock()
            .expect("service mutex poisoned")
            .data_runtime
            .as_ref()
            .expect("data runtime installed")
            .context();

        let stats = data_context
            .runtime_stats_on_workers()
            .expect("snapshot data worker nodes");

        assert_eq!(stats.len(), hammer_core::config::Worker::default().count);
        for (worker, rows) in stats {
            assert!(
                rows.iter()
                    .any(|row| row.node_name == Some("session-queue")),
                "worker {worker} nodes: {rows:?}"
            );
        }

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_bind_tcp_listener_updates_listener_lookup() {
        let service = new_test_service("");
        let bind: SocketAddr = "127.0.0.1:7301".parse().expect("listener bind");
        let capabilities = TcpCapabilities {
            max_segment_size: Some(1440),
            window_scale: Some(6),
            sack: true,
            timestamps: true,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };

        let listener = service
            .bind_tcp_listener(bind, 1, capabilities)
            .expect("bind tcp listener");
        let lookup_id = {
            let state = service.tcp_listener_control_snapshot_for_test();
            let registration = state
                .tcp_listeners
                .iter()
                .find(|registration| registration.lookup_id == listener)
                .expect("listener registration");
            let published = state
                .tcp_lookup
                .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                    0,
                    Ipv4Addr::new(127, 0, 0, 1),
                    7301,
                ))
                .expect("published listener lookup");
            assert_eq!(published.owner_worker, DataWorkerId::new(1));
            assert_eq!(published.id, registration.lookup_id);
            assert_eq!(published.capabilities, capabilities);
            registration.lookup_id
        };

        let duplicate = service
            .bind_tcp_listener(bind, 1, capabilities)
            .expect_err("duplicate tcp listener must fail");
        assert!(
            duplicate.to_string().contains("already registered"),
            "err={duplicate}"
        );

        service
            .close_tcp_listener(listener)
            .expect("close tcp listener");
        let state = service.tcp_listener_control_snapshot_for_test();
        assert!(
            state
                .tcp_lookup
                .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                    0,
                    Ipv4Addr::new(127, 0, 0, 1),
                    7301,
                ))
                .is_none(),
            "closed listener lookup must be removed"
        );
        assert!(
            state
                .tcp_listeners
                .iter()
                .all(|registration| registration.lookup_id != lookup_id),
            "closed listener registration must be removed"
        );

        let rebound = service
            .bind_tcp_listener(bind, 1, capabilities)
            .expect("rebind tcp listener");
        assert_ne!(
            rebound, lookup_id,
            "listener lookup ids are not socket slots"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_spawns_app_task_on_requested_worker() {
        let service = new_test_service("");
        let (tx, rx) = std::sync::mpsc::channel();

        service
            .spawn_app_on_worker(1, move || async move {
                tx.send(
                    std::thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default(),
                )
                .expect("send worker thread");
            })
            .expect("spawn app task");

        let thread = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive worker thread");
        assert!(thread.contains("hammer-data-1"), "thread={thread}");
        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_rejects_invalid_app_worker_index() {
        let service = new_test_service("");
        let err = service
            .spawn_app_on_worker(99, || async {})
            .expect_err("invalid worker must fail");
        assert!(err.to_string().contains("invalid app worker"), "err={err}");
        assert!(err.to_string().contains("worker_count="), "err={err}");
        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_register_app_host_defers_start_until_service_start() {
        let service = new_test_service("");
        let host = Arc::new(TestAppHost::new());

        service
            .register_app_host(Arc::clone(&host))
            .expect("register app host");

        assert_eq!(host.starts.load(Ordering::SeqCst), 0);
        service.start().expect("start service");
        assert_eq!(host.starts.load(Ordering::SeqCst), ALL_STAGES.len());
        service.close().expect("close service");
        assert_eq!(host.closes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_service_register_app_host_starts_immediately_when_running() {
        let service = new_test_service("");
        service.start().expect("start service");
        let host = Arc::new(TestAppHost::new());

        service
            .register_app_host(Arc::clone(&host))
            .expect("register running app host");

        assert_eq!(host.starts.load(Ordering::SeqCst), ALL_STAGES.len());
        service.close().expect("close service");
        assert_eq!(host.closes.load(Ordering::SeqCst), 1);
    }
}
