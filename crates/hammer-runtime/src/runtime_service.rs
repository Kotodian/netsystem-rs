use std::sync::{Arc, Mutex};
use std::time::Instant;

use hammer_adapter::{Lifecycle, NetworkManager as _, PlatformInterface};
use hammer_core::config::{self, Options};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{ALL_STAGES, LIFECYCLE_ORDER};
use hammer_core::log::{DiscardWriter, Factory, LogWriter, Logger};
use hammer_core::registry::RuntimeRegistry;

#[cfg(feature = "endpoint")]
use crate::EndpointManager;
use crate::{
    CertificateProviderManager, CertificateStore, ConnectionManager, DnsRouter,
    DnsTransportManager, InboundManager, NetworkManager, OutboundManager, PauseManager, Router,
    ServiceManager,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    Closed,
}

pub struct RuntimeService {
    inner: Mutex<ServiceInner>,
}

struct ServiceInner {
    state: ServiceState,
    log_factory: Arc<Factory>,
    #[allow(dead_code)]
    platform: Arc<dyn PlatformInterface>,
    #[allow(dead_code)]
    registry: Arc<RuntimeRegistry>,
    lifecycles: Vec<Arc<dyn Lifecycle>>,
    pause: Arc<PauseManager>,
    network: Arc<NetworkManager>,
    _runtime: tokio::runtime::Runtime,
    _options: Options,
}

impl RuntimeService {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
    ) -> Result<Arc<Self>, HammerError> {
        crate::install_default_crypto_provider();

        let options = config::parse_config(config_content)?;
        let writer: Arc<dyn LogWriter> = if options.log.disabled {
            Arc::new(DiscardWriter)
        } else {
            writer
        };
        let log_factory = Factory::new_with_min_level(Instant::now(), writer, options.log.level);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(512 * 1024)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .map_err(|e| HammerError::internal(format!("init tokio runtime: {e}")))?;

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
        let endpoint = Arc::new(EndpointManager::from_options_with_platform(
            new_logger(&log_factory, "endpoint"),
            &options.endpoints,
            Arc::clone(&platform),
        )?);
        let connection = Arc::new(ConnectionManager::new());
        let network = NetworkManager::with_platform(
            new_logger(&log_factory, "network"),
            options.route.auto_detect_interface,
            Arc::clone(&platform),
            Arc::clone(&pause),
            Arc::clone(&connection),
        );
        let outbound = Arc::new(OutboundManager::from_options_with_platform(
            new_logger(&log_factory, "outbound"),
            options.route.final_.clone(),
            &options.outbounds,
            Arc::clone(&platform),
        )?);
        #[cfg(feature = "endpoint")]
        {
            for (id, view) in endpoint.outbound_view() {
                outbound.register_outbound(id, view);
            }
        }
        let dns_transport = Arc::new(DnsTransportManager::from_options_with_runtime(
            new_logger(&log_factory, "dns-transport"),
            &options.dns,
            Arc::clone(&outbound),
            Arc::clone(&platform),
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
            Arc::clone(&platform),
        )?);
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

        let mut lifecycles: Vec<Arc<dyn Lifecycle>> = vec![
            cert_store as Arc<dyn Lifecycle>,
            cert_provider as Arc<dyn Lifecycle>,
        ];
        #[cfg(feature = "endpoint")]
        lifecycles.push(endpoint as Arc<dyn Lifecycle>);
        lifecycles.extend([
            Arc::clone(&network) as Arc<dyn Lifecycle>,
            dns_transport as Arc<dyn Lifecycle>,
            outbound as Arc<dyn Lifecycle>,
            dns_router as Arc<dyn Lifecycle>,
            router as Arc<dyn Lifecycle>,
            inbound as Arc<dyn Lifecycle>,
            service_mgr as Arc<dyn Lifecycle>,
            connection as Arc<dyn Lifecycle>,
        ]);

        debug_assert_eq!(
            lifecycles.iter().map(|lc| lc.name()).collect::<Vec<_>>(),
            LIFECYCLE_ORDER.to_vec(),
            "Service lifecycles must match LIFECYCLE_ORDER",
        );

        Ok(Arc::new(Self {
            inner: Mutex::new(ServiceInner {
                state: ServiceState::NotStarted,
                log_factory,
                platform,
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
                ServiceState::Closed => return Err(HammerError::service_closed()),
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

        let _runtime_guard = runtime_handle.enter();
        let _dispatch_guard = tracing::dispatcher::set_default(log_factory.dispatch());

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
        let (lifecycles, log_factory) = {
            let mut inner = self.inner.lock().expect("service mutex poisoned");
            if inner.state == ServiceState::Closed {
                return Ok(());
            }
            inner.state = ServiceState::Closed;
            (inner.lifecycles.clone(), Arc::clone(&inner.log_factory))
        };
        let _dispatch_guard = tracing::dispatcher::set_default(log_factory.dispatch());

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
        self.with_inner(|inner| inner.pause.pause());
    }

    pub fn wake(&self) {
        self.with_inner(|inner| inner.pause.wake());
    }

    pub fn reset_network(&self) {
        self.with_inner(|inner| inner.network.reset_network());
    }

    pub fn need_wifi_state(&self) -> bool {
        self.with_inner(|inner| inner.network.need_wifi_state())
    }

    pub fn update_wifi_state(&self) {
        self.with_inner(|inner| inner.network.update_wifi_state());
    }

    /// Lock the inner state and pin this Service's tracing Dispatch onto the
    /// calling thread for the duration of `f`. Every short sync FFI entry
    /// point that touches inner state goes through here so they all route
    /// tracing events to this Factory's writer.
    fn with_inner<R>(&self, f: impl FnOnce(&ServiceInner) -> R) -> R {
        let inner = self.inner.lock().expect("service mutex poisoned");
        let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
        f(&inner)
    }
}

fn new_logger(factory: &Arc<Factory>, id: &str) -> Logger {
    factory.new_logger(id.to_owned())
}
