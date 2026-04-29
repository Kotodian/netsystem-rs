use std::sync::{Arc, Mutex};
use std::time::Instant;

use hammer_adapter::{Lifecycle, NetworkManager as _, PlatformInterface};
use hammer_core::config::{self, Options};
use hammer_core::lifecycle::{ALL_STAGES, LIFECYCLE_ORDER};
use hammer_core::log::{DiscardWriter, Factory, Level, LogWriter, Logger};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{
    CertificateProviderManager, CertificateStore, ConnectionManager, DnsRouter,
    DnsTransportManager, EndpointManager, InboundManager, NetworkManager, OutboundManager,
    PauseManager, Router, ServiceManager,
};

use crate::HammerPlatform;
use crate::error::HammerError;
use crate::platform::{PlatformAdapter, PlatformLogWriter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    Closed,
}

pub struct HammerService {
    inner: Mutex<ServiceInner>,
}

struct ServiceInner {
    state: ServiceState,
    log_factory: Arc<Factory>,
    #[allow(dead_code)]
    platform: Arc<PlatformAdapter>,
    #[allow(dead_code)]
    registry: Arc<RuntimeRegistry>,
    lifecycles: Vec<Arc<dyn Lifecycle>>,
    pause: Arc<PauseManager>,
    network: Arc<NetworkManager>,
    _runtime: tokio::runtime::Runtime,
    _options: Options,
}

impl HammerService {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn HammerPlatform>,
    ) -> Result<Arc<Self>, HammerError> {
        // reqwest ships without a bundled crypto provider (we trim aws-lc-rs
        // out at the workspace level), so DoH and any other TLS path needs the
        // ring provider installed before the first request.
        hammer_runtime::install_default_crypto_provider();

        let options = config::parse_config(config_content)?;
        let adapter = Arc::new(PlatformAdapter::new(platform));
        let writer: Arc<dyn LogWriter> = if options.log.disabled {
            Arc::new(DiscardWriter)
        } else {
            Arc::new(PlatformLogWriter::new(Arc::clone(&adapter)))
        };
        let log_factory = Factory::new_with_min_level(
            Instant::now(),
            writer,
            parse_log_level(&options.log.level),
        );

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(512 * 1024)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .map_err(|e| HammerError::internal(format!("init tokio runtime: {e}")))?;

        let registry = RuntimeRegistry::new();

        // 1:1 with hammer/service.go: build the eleven managers in the order the
        // Go reference declares them, register each one in the typed runtime
        // registry, and finally clip them into a single Lifecycle vec ordered
        // by LIFECYCLE_ORDER (so start/close walk in the same sequence).
        let pause = Arc::new(PauseManager::new());

        let cert_store = Arc::new(CertificateStore::new(
            new_logger(&log_factory, "certificate-store"),
            false,
        ));
        let cert_provider = Arc::new(CertificateProviderManager::new(new_logger(
            &log_factory,
            "certificate-provider",
        )));
        let endpoint = Arc::new(EndpointManager::from_options_with_platform(
            new_logger(&log_factory, "endpoint"),
            &options.endpoints,
            Arc::clone(&adapter) as Arc<dyn PlatformInterface>,
        )?);
        let connection = Arc::new(ConnectionManager::new(new_logger(
            &log_factory,
            "connection",
        )));
        let network = NetworkManager::with_platform(
            new_logger(&log_factory, "network"),
            options.route.auto_detect_interface,
            Arc::clone(&adapter) as Arc<dyn PlatformInterface>,
            Arc::clone(&pause),
            Arc::clone(&connection),
        );
        let outbound = Arc::new(OutboundManager::from_options_with_platform(
            new_logger(&log_factory, "outbound"),
            options.route.final_.clone(),
            &options.outbounds,
            Arc::clone(&adapter) as Arc<dyn PlatformInterface>,
        ));
        let dns_transport = Arc::new(DnsTransportManager::from_options_with_runtime(
            new_logger(&log_factory, "dns-transport"),
            &options.dns,
            Arc::clone(&outbound),
            Arc::clone(&adapter) as Arc<dyn PlatformInterface>,
        )?);
        let dns_router = Arc::new(DnsRouter::new_with_manager(
            new_logger(&log_factory, "dns-router"),
            Arc::clone(&dns_transport),
            options.dns.strategy,
        ));
        let router = Arc::new(Router::from_options(
            new_logger(&log_factory, "router"),
            options.route.clone(),
            Arc::clone(&outbound),
        )?);
        let inbound = Arc::new(InboundManager::from_options_with_runtime(
            new_logger(&log_factory, "inbound"),
            &options.inbounds,
            Arc::clone(&router),
            Arc::clone(&dns_router),
            Arc::clone(&outbound),
            Arc::clone(&adapter) as Arc<dyn PlatformInterface>,
        )?);
        let service_mgr = Arc::new(ServiceManager::new(new_logger(&log_factory, "service")));

        registry.set::<CertificateStore>(Arc::clone(&cert_store));
        registry.set::<CertificateProviderManager>(Arc::clone(&cert_provider));
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

        // Order in `lifecycles` MUST match LIFECYCLE_ORDER — Service::start /
        // Service::close walk this slice forward / in reverse.
        let lifecycles: Vec<Arc<dyn Lifecycle>> = vec![
            cert_store as Arc<dyn Lifecycle>,
            cert_provider as Arc<dyn Lifecycle>,
            endpoint as Arc<dyn Lifecycle>,
            Arc::clone(&network) as Arc<dyn Lifecycle>,
            dns_transport as Arc<dyn Lifecycle>,
            outbound as Arc<dyn Lifecycle>,
            dns_router as Arc<dyn Lifecycle>,
            router as Arc<dyn Lifecycle>,
            inbound as Arc<dyn Lifecycle>,
            service_mgr as Arc<dyn Lifecycle>,
            connection as Arc<dyn Lifecycle>,
        ];

        debug_assert_eq!(
            lifecycles.iter().map(|lc| lc.name()).collect::<Vec<_>>(),
            LIFECYCLE_ORDER.to_vec(),
            "Service lifecycles must match LIFECYCLE_ORDER",
        );

        Ok(Arc::new(Self {
            inner: Mutex::new(ServiceInner {
                state: ServiceState::NotStarted,
                log_factory,
                platform: adapter,
                registry,
                lifecycles,
                pause,
                network,
                _runtime: runtime,
                _options: options,
            }),
        }))
    }

    pub fn start(&self) -> Result<(), HammerError> {
        let (lifecycles, log_factory, runtime_handle) = {
            let mut inner = self.inner.lock().expect("service mutex poisoned");
            match inner.state {
                ServiceState::Closed => return Err(HammerError::ServiceClosed),
                ServiceState::Running => return Ok(()),
                ServiceState::NotStarted => {}
            }
            inner.state = ServiceState::Running;
            (
                inner.lifecycles.clone(),
                Arc::clone(&inner.log_factory),
                inner._runtime.handle().clone(),
            )
        };

        log_factory.close();
        let _runtime_guard = runtime_handle.enter();

        for stage in ALL_STAGES {
            for lc in &lifecycles {
                if let Err(err) = lc.start(stage) {
                    let close_err = self.close();
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

    pub fn close(&self) -> Result<(), HammerError> {
        let lifecycles = {
            let mut inner = self.inner.lock().expect("service mutex poisoned");
            if inner.state == ServiceState::Closed {
                return Ok(());
            }
            inner.state = ServiceState::Closed;
            inner.lifecycles.clone()
        };

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

    pub fn pause(&self) {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.pause.pause();
    }

    pub fn wake(&self) {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.pause.wake();
    }

    pub fn reset_network(&self) {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.network.reset_network();
    }

    pub fn need_wifi_state(&self) -> bool {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.network.need_wifi_state()
    }

    pub fn update_wifi_state(&self) {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.network.update_wifi_state();
    }
}

fn parse_log_level(level: &str) -> Level {
    match level.to_ascii_lowercase().as_str() {
        "panic" => Level::Panic,
        "fatal" => Level::Fatal,
        "error" => Level::Error,
        "warn" | "warning" => Level::Warn,
        "debug" => Level::Debug,
        "trace" => Level::Trace,
        _ => Level::Info,
    }
}

fn new_logger(factory: &Arc<Factory>, tag: &str) -> Logger {
    factory.new_logger(tag.to_owned())
}
