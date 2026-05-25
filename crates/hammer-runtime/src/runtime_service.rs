use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use hammer_adapter::{Lifecycle, NetworkManager as _, OutboundManager as _, PlatformInterface};
use hammer_core::config::{self, Options};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::{ALL_STAGES, LIFECYCLE_ORDER};
use hammer_core::log::{DiscardWriter, Factory, LogWriter, Logger};
use hammer_core::registry::RuntimeRegistry;

#[cfg(feature = "endpoint")]
use crate::EndpointManager;
#[cfg(feature = "probe")]
use crate::ProbeManager;
#[cfg(feature = "endpoint")]
use crate::endpoints::EndpointOutboundAdapter;
#[cfg(feature = "endpoint")]
use crate::protocol::tun::RuntimeTunInbound;
use crate::spawn::DataRuntimeContext;
use crate::{
    CertificateProviderManager, CertificateStore, ConnectionManager, ControlThread,
    ControlThreadHandle, DnsRouter, DnsTransportManager, InboundManager, MetricSample,
    MetricsRegistry, NetworkManager, OutboundManager, PauseManager, Router, ServiceManager,
};
use crate::control_thread::ControlEventSubscriptionHandle;
#[cfg(feature = "endpoint")]
use hammer_adapter::{EndpointManager as _, InboundManager as _};

#[cfg(feature = "probe")]
use hammer_adapter::ProbeProtocolComponent;
use hammer_adapter::ProbeReport;
use std::time::Duration;

const CONTROL_THREAD_STACK_SIZE: usize = 512 * 1024;
const DATA_WORKER_THREADS: usize = 1;
const DATA_WORKER_STACK_SIZE: usize = 512 * 1024;
const DATA_MAX_BLOCKING_THREADS: usize = 4;
const METRICS_LOG_INTERVAL: Duration = Duration::from_secs(30);
/// Time budget for the control thread to drain queued logs and emit a
/// final metrics dump on shutdown. 500ms was too tight when the log queue
/// (4096 entries) is full and each iOS write costs tens of milliseconds;
/// 2s leaves headroom for the worst-case drain without making `close()`
/// feel stuck on the FFI side.
const CONTROL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Time budget the data-plane runtime gets to abort in-flight tasks
/// during `close()`. Data tasks (TCP/UDP forwarders, DNS, probes) are
/// expected to drop fast once their lifecycles are closed; tasks still
/// running past this deadline are forcibly aborted by the runtime.
const DATA_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Slack added on top of an inner async timeout when bouncing it through
/// the control thread, so the inner future has a chance to time out
/// cleanly and report a result before the outer wait gives up.
const CONTROL_ASYNC_BUFFER: Duration = Duration::from_secs(5);

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

pub struct RuntimeService {
    inner: Arc<Mutex<ServiceInner>>,
}

struct ServiceInner {
    state: ServiceState,
    log_factory: Arc<Factory>,
    control_handle: Option<Arc<ControlThreadHandle>>,
    control_thread: Option<JoinHandle<()>>,
    event_subscriptions: Vec<ControlEventSubscriptionHandle>,
    metrics: Arc<MetricsRegistry>,
    #[allow(dead_code)]
    platform: Arc<dyn PlatformInterface>,
    #[allow(dead_code)]
    registry: Arc<RuntimeRegistry>,
    lifecycles: Vec<Arc<dyn Lifecycle>>,
    pause: Arc<PauseManager>,
    network: Arc<NetworkManager>,
    dns_router: Arc<DnsRouter>,
    outbound: Arc<OutboundManager>,
    #[cfg(feature = "endpoint")]
    endpoint: Arc<EndpointManager>,
    #[cfg(feature = "probe")]
    probe: Arc<ProbeManager>,
    /// Data-plane runtime that hosts every business future spawned via
    /// `crate::spawn::spawn`. Held in `Option` so `finish_close` can
    /// `take()` and consume it via `Runtime::shutdown_timeout` to bound
    /// the abort latency of in-flight tasks. If `None`, shutdown has
    /// already happened or the runtime was never installed.
    data_runtime: Option<tokio::runtime::Runtime>,
    data_context: DataRuntimeContext,
    _options: Options,
}

impl RuntimeService {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
    ) -> HammerResult<Arc<Self>> {
        crate::install_default_crypto_provider();

        let options = config::parse_config(config_content)?;
        let metrics = MetricsRegistry::new();
        let base_time = Instant::now();
        // Data-plane runtime: hosts every business future. Keep a single
        // worker for iOS NetExt until we have device-side evidence that extra
        // workers reduce latency; the control plane (below) runs on its own
        // dedicated current_thread runtime so worker stalls cannot delay log/
        // metrics/command dispatch.
        let data_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DATA_WORKER_THREADS)
            .thread_name("hammer-data")
            .thread_stack_size(DATA_WORKER_STACK_SIZE)
            .max_blocking_threads(DATA_MAX_BLOCKING_THREADS)
            .enable_all()
            .build()
            .map_err(|e| HammerError::internal(format!("init data runtime: {e}")))?;
        let data_context = DataRuntimeContext::new(data_runtime.handle().clone());
        let writer: Arc<dyn LogWriter> = if options.log.disabled {
            Arc::new(DiscardWriter)
        } else {
            writer
        };
        let (control_handle, control_loop) = ControlThread::new(
            base_time,
            writer,
            Arc::clone(&metrics),
            METRICS_LOG_INTERVAL,
            options.log.level,
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
        let writer: Arc<dyn LogWriter> = Arc::clone(&control_handle) as Arc<dyn LogWriter>;
        let log_factory = Factory::new_with_min_level(base_time, writer, options.log.level);
        let event_subscriptions = crate::control_thread::build_standard_event_subscribers(
            new_logger(&log_factory, "control-event"),
            Arc::clone(&control_handle),
        )?;

        let registry = RuntimeRegistry::new();
        let pause = Arc::new(PauseManager::new());

        let cert_store = Arc::new(CertificateStore::new(
            new_logger(&log_factory, "certificate-store"),
            false,
        ));
        let cert_provider = Arc::new(CertificateProviderManager::new(new_logger(
            &log_factory,
            "certificate-provider",
        )));
        #[cfg(feature = "endpoint")]
        let endpoint = Arc::new(EndpointManager::from_options_with_platform_and_control(
            new_logger(&log_factory, "endpoint"),
            &options.endpoints,
            Arc::clone(&platform),
            Arc::clone(&control_handle),
        )?);
        let connection = Arc::new(ConnectionManager::new());
        let network = NetworkManager::with_platform(
            new_logger(&log_factory, "network"),
            options.route.auto_detect_interface,
            Arc::clone(&platform),
            Arc::clone(&pause),
            Arc::clone(&connection),
        );
        let outbound = Arc::new(OutboundManager::from_options_with_platform_and_metrics(
            new_logger(&log_factory, "outbound"),
            options.route.final_.clone(),
            &options.outbounds,
            Arc::clone(&platform),
            Arc::clone(&metrics),
        )?);
        // Auto-register an `EndpointOutboundAdapter` for every declared
        // endpoint **before** `bind_aggregates`: urltest and friends will
        // resolve `<endpoint-id>` against the same OutboundManager when
        // their children include endpoint ids, and DNS `via = "<endpoint-id>"`
        // requires the adapter to be findable through `outbound.get()`.
        #[cfg(feature = "endpoint")]
        register_endpoint_outbound_adapters(&outbound, &endpoint, &log_factory)?;
        // Aggregate outbounds (urltest) need a `Weak<dyn OutboundManager>`
        // before any children can be looked up. Bind once here, after every
        // declared outbound has been registered, so the urltest's first dial /
        // probe can resolve every outbound child without races.
        outbound.bind_aggregates();
        let default_domain_resolver = options
            .route
            .default_domain_resolver
            .as_ref()
            .map(|d| d.server.as_str());
        let dns_transport = Arc::new(DnsTransportManager::from_options_with_runtime(
            new_logger(&log_factory, "dns-transport"),
            &options.dns,
            Arc::clone(&outbound),
            Arc::clone(&platform),
            default_domain_resolver,
        )?);
        let dns_router = Arc::new(
            DnsRouter::new_with_manager(
                new_logger(&log_factory, "dns-router"),
                Arc::clone(&dns_transport),
                options.dns.strategy,
            )
            .with_rules(&options.dns.rules)?,
        );
        #[cfg(feature = "endpoint")]
        let router = Arc::new(Router::from_options_with_metrics_and_endpoint_ids(
            new_logger(&log_factory, "router"),
            options.route.clone(),
            Arc::clone(&outbound),
            Arc::clone(&metrics),
            options.endpoints.iter().map(|endpoint| endpoint.id.clone()),
        )?);
        #[cfg(not(feature = "endpoint"))]
        let router = Arc::new(Router::from_options_with_metrics(
            new_logger(&log_factory, "router"),
            options.route.clone(),
            Arc::clone(&outbound),
            Arc::clone(&metrics),
        )?);
        let inbound = Arc::new(InboundManager::from_options_with_runtime_and_metrics(
            new_logger(&log_factory, "inbound"),
            &options.inbounds,
            Arc::clone(&router),
            Arc::clone(&dns_router),
            Arc::clone(&outbound),
            Arc::clone(&platform),
            Arc::clone(&metrics),
        )?);
        // Wire the EndpointManager into every TUN inbound *before* lifecycle
        // Start so the packet loop's L3 fast path (`L3DispatchTable`) is built
        // with the actual endpoint set rather than running with an empty
        // dispatcher.
        //
        // The endpoint API exposes inbound packets via a one-shot
        // `ip_recv_take()` — only one consumer can drain decrypted ingress.
        // Wiring the same `EndpointManager` into multiple TUN stacks means
        // whichever TUN starts first wins the receiver while the others
        // can still drive egress, splitting reply paths across interfaces.
        // Fail fast on this misconfiguration instead of silently degrading.
        #[cfg(feature = "endpoint")]
        {
            let system_tuns: Vec<_> = inbound
                .list()
                .into_iter()
                .filter(|comp| {
                    comp.runtime()
                        .as_any()
                        .downcast_ref::<RuntimeTunInbound>()
                        .is_some_and(RuntimeTunInbound::uses_system_stack)
                })
                .collect();
            if !endpoint.list().is_empty() && system_tuns.len() > 1 {
                return Err(HammerError::config_validation(format!(
                    "multiple system TUN inbounds cannot share endpoints (found {})",
                    system_tuns.len()
                )));
            }
            for comp in inbound.list() {
                if let Some(tun) = comp.runtime().as_any().downcast_ref::<RuntimeTunInbound>() {
                    tun.set_endpoint_manager(Arc::clone(&endpoint));
                }
            }
        }
        let service_mgr = Arc::new(ServiceManager::new(new_logger(&log_factory, "service")));

        registry.set::<CertificateStore>(Arc::clone(&cert_store));
        registry.set::<CertificateProviderManager>(Arc::clone(&cert_provider));
        #[cfg(feature = "endpoint")]
        registry.set::<EndpointManager>(Arc::clone(&endpoint));
        registry.set::<NetworkManager>(Arc::clone(&network));
        registry.set::<DnsTransportManager>(Arc::clone(&dns_transport));
        registry.set::<OutboundManager>(Arc::clone(&outbound));
        registry.set::<DnsRouter>(Arc::clone(&dns_router));
        registry.set::<Router>(Arc::clone(&router));
        registry.set::<InboundManager>(Arc::clone(&inbound));
        registry.set::<ServiceManager>(Arc::clone(&service_mgr));
        registry.set::<ConnectionManager>(Arc::clone(&connection));
        registry.set::<PauseManager>(Arc::clone(&pause));
        registry.set::<MetricsRegistry>(Arc::clone(&metrics));

        let mut lifecycles: Vec<Arc<dyn Lifecycle>> = vec![
            cert_store as Arc<dyn Lifecycle>,
            cert_provider as Arc<dyn Lifecycle>,
        ];
        #[cfg(feature = "endpoint")]
        lifecycles.push(Arc::clone(&endpoint) as Arc<dyn Lifecycle>);
        lifecycles.extend([
            Arc::clone(&network) as Arc<dyn Lifecycle>,
            dns_transport as Arc<dyn Lifecycle>,
            Arc::clone(&outbound) as Arc<dyn Lifecycle>,
            Arc::clone(&dns_router) as Arc<dyn Lifecycle>,
            router as Arc<dyn Lifecycle>,
            inbound as Arc<dyn Lifecycle>,
            service_mgr as Arc<dyn Lifecycle>,
            connection as Arc<dyn Lifecycle>,
        ]);

        debug_assert_lifecycle_order(&lifecycles);

        #[cfg(feature = "probe")]
        let probe = Arc::new(ProbeManager::new(Arc::clone(&outbound)));
        Ok(Arc::new(Self {
            inner: Arc::new(Mutex::new(ServiceInner {
                state: ServiceState::NotStarted,
                log_factory,
                control_handle: Some(control_handle),
                control_thread: Some(control_thread),
                event_subscriptions,
                metrics,
                platform,
                registry,
                lifecycles,
                pause,
                network,
                dns_router,
                outbound,
                #[cfg(feature = "endpoint")]
                endpoint,
                #[cfg(feature = "probe")]
                probe,
                data_runtime: Some(data_runtime),
                data_context,
                _options: options,
            })),
        }))
    }

    /// Run a one-shot latency probe to every registered outbound's probe endpoint
    /// and return one report per outbound (order matches
    /// `OutboundManager::list()`). `protocol` selects the probe
    /// implementation (V1 only `"icmp"`); `timeout` applies per
    /// outbound, not to the batch.
    ///
    /// Probe failures (timeout, connection refused, unsupported
    /// network) live inside each `ProbeReport.result` so the caller
    /// always sees the full outbound list. Only invalid arguments
    /// (unknown protocol) bubble up as `Err`.
    #[cfg(feature = "probe")]
    pub fn probe_outbounds(
        &self,
        protocol: &str,
        timeout: Duration,
    ) -> HammerResult<Vec<ProbeReport>> {
        let protocol = protocol.to_owned();
        let outer_timeout = timeout.saturating_add(CONTROL_ASYNC_BUFFER);
        self.control_async_call(outer_timeout, move |inner, done| {
            let probe = build_probe_protocol(&protocol)?;
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            let probe_mgr = Arc::clone(&inner.probe);
            crate::spawn::spawn(async move {
                let reports = probe_mgr.probe_all(probe, timeout).await;
                let _ = done.send(Ok(reports));
            });
            Ok(())
        })
    }

    /// Return the id of the child currently selected by an aggregate
    /// outbound (e.g. urltest). Leaf outbounds — and any unknown id —
    /// return `None` so the FFI layer can map it to a nullable string.
    pub fn current_selection(&self, outbound_id: &str) -> Option<String> {
        let outbound_id = outbound_id.to_owned();
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return None;
            }
            inner
                .outbound
                .get(&outbound_id)
                .and_then(|o| o.runtime().now())
        })
        .ok()
        .flatten()
    }

    /// Trigger a one-shot probe sweep on an aggregate outbound and
    /// collect per-child latency reports. Mirrors sing-box's
    /// `URLTest()`: the call drives the same probe path used by the
    /// PostStart kickoff, then returns the resulting samples.
    ///
    /// `timeout` is forwarded to each per-child probe. A zero value
    /// means "use the value baked into the outbound config".
    pub fn urltest(&self, outbound_id: &str, timeout: Duration) -> HammerResult<Vec<ProbeReport>> {
        let timeout_outbound_id = outbound_id.to_owned();
        let effective_timeout = self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            let outbound = inner.outbound.get(&timeout_outbound_id).ok_or_else(|| {
                HammerError::config_validation(format!(
                    "outbound '{timeout_outbound_id}' is not registered"
                ))
            })?;
            Ok(outbound.runtime().probe_group_timeout(timeout))
        })??;

        let outbound_id = outbound_id.to_owned();
        let outer_timeout = effective_timeout.saturating_add(CONTROL_ASYNC_BUFFER);
        self.control_async_call(outer_timeout, move |inner, done| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            let outbound = inner.outbound.get(&outbound_id).ok_or_else(|| {
                HammerError::config_validation(format!(
                    "outbound '{outbound_id}' is not registered"
                ))
            })?;
            crate::spawn::spawn(async move {
                let reports = outbound.runtime().probe_group(timeout).await;
                let _ = done.send(reports);
            });
            Ok(())
        })
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
            // Drop cached outbound clients (e.g. hysteria2 QUIC connections)
            // alongside the inbound + DNS reset. Without this, sing-box-style
            // `InterfaceUpdated` semantics never reach our outbounds, so a
            // stale cached_client survives every network reset and the next
            // dial blocks on the dead QUIC connection's max_idle_timeout.
            inner.outbound.reset_network();
            #[cfg(feature = "endpoint")]
            inner.endpoint.reset_network();
            inner.dns_router.reset_network();
            inner.outbound.ensure_connected();
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

    fn control_call<R>(
        &self,
        f: impl FnOnce(&mut ServiceInner) -> R + Send + 'static,
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
        f: impl FnOnce(&mut ServiceInner) -> R + Send + 'static,
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

    fn control_async_call<R>(
        &self,
        timeout: Duration,
        f: impl FnOnce(&mut ServiceInner, std::sync::mpsc::Sender<HammerResult<R>>) -> HammerResult<()>
        + Send
        + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let control_handle = self
            .control_handle()
            .ok_or_else(HammerError::service_closed)?;
        let inner = Arc::clone(&self.inner);
        control_handle.call_async(timeout, move |done| {
            let mut inner = inner.lock().expect("service mutex poisoned");
            let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
            let data_context = inner.data_context.clone();
            data_context.enter(|| f(&mut inner, done))
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

/// Wraps every declared `Endpoint` in an `EndpointOutboundAdapter` and
/// registers it into the `OutboundManager` under the endpoint's id. This
/// is what lets `[[dns.servers]] via = "<endpoint-id>"` resolve through
/// the same `outbound.get(...)` path used by every other transport.
///
/// Must run after the user's `[[outbounds]]` are loaded (so duplicate-id
/// validation lines up with the parse-time `validate_unique_ids` check)
/// and before `bind_aggregates()` (so urltest children that reference an
/// endpoint id resolve to the adapter rather than an empty slot).
#[cfg(feature = "endpoint")]
fn register_endpoint_outbound_adapters(
    outbound: &Arc<OutboundManager>,
    endpoint: &Arc<EndpointManager>,
    log_factory: &Arc<Factory>,
) -> HammerResult<()> {
    for component in endpoint.list() {
        let id = component.meta().id().to_owned();
        let logger = new_logger(log_factory, &format!("endpoint-outbound/{id}"));
        let adapter =
            EndpointOutboundAdapter::arc(logger, id.clone(), Arc::clone(component.runtime()));
        outbound.register_outbound(adapter)?;
    }
    Ok(())
}

fn start_inner(inner: &mut ServiceInner) -> HammerResult<()> {
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

fn close_inner(inner: &mut ServiceInner) -> HammerResult<()> {
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

fn finish_close(
    inner: &Arc<Mutex<ServiceInner>>,
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

#[cfg(feature = "probe")]
fn build_probe_protocol(protocol: &str) -> HammerResult<ProbeProtocolComponent> {
    crate::probe::ProbeProtocolFactorySet::standard().build(protocol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
}
