mod lifecycle;
mod registry;

pub use lifecycle::{ALL_STAGES, LIFECYCLE_ORDER, Lifecycle, StartStage};
pub use registry::{PauseManager, StubManager};

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::config::{self, Options};
use crate::error::HammerError;
use crate::log::{Factory, Logger};
use crate::platform::{PlatformAdapter, PlatformLogWriter};
use crate::Platform;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    Closed,
}

pub struct Service {
    inner: Mutex<ServiceInner>,
}

struct ServiceInner {
    state: ServiceState,
    log_factory: Arc<Factory>,
    // Held now so M2 can clone it into the real Manager set without changing
    // the ServiceInner construction order.
    #[allow(dead_code)]
    platform: Arc<PlatformAdapter>,
    lifecycles: Vec<Arc<dyn Lifecycle>>,
    pause: Arc<PauseManager>,
    _runtime: tokio::runtime::Runtime,
    _options: Options,
}

impl Service {
    pub fn new(config_content: &str, platform: Arc<dyn Platform>) -> Result<Arc<Self>, HammerError> {
        let options = config::parse_config(config_content)?;
        let adapter = Arc::new(PlatformAdapter::new(platform));
        let writer = Arc::new(PlatformLogWriter::new(Arc::clone(&adapter)));
        let log_factory = Factory::new(Instant::now(), writer);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(512 * 1024)
            .enable_all()
            .build()
            .map_err(|e| HammerError::internal(format!("init tokio runtime: {e}")))?;

        let lifecycles: Vec<Arc<dyn Lifecycle>> = LIFECYCLE_ORDER
            .iter()
            .map(|name| Self::stub_for(name, &log_factory))
            .collect();

        let pause = Arc::new(PauseManager::new());

        Ok(Arc::new(Self {
            inner: Mutex::new(ServiceInner {
                state: ServiceState::NotStarted,
                log_factory,
                platform: adapter,
                lifecycles,
                pause,
                _runtime: runtime,
                _options: options,
            }),
        }))
    }

    fn stub_for(name: &'static str, factory: &Arc<Factory>) -> Arc<dyn Lifecycle> {
        let logger: Logger = factory.new_logger(name.to_owned());
        Arc::new(StubManager::new(name, logger))
    }

    pub fn start(&self) -> Result<(), HammerError> {
        // Snapshot the lifecycles & state under the lock, then execute outside it.
        let (lifecycles, log_factory) = {
            let mut inner = self.inner.lock().expect("service mutex poisoned");
            match inner.state {
                ServiceState::Closed => return Err(HammerError::ServiceClosed),
                ServiceState::Running => return Ok(()),
                ServiceState::NotStarted => {}
            }
            inner.state = ServiceState::Running;
            (inner.lifecycles.clone(), Arc::clone(&inner.log_factory))
        };

        // No-op for M1; reserved for future flushing/file-rotation logic.
        log_factory.close();

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
        // M1: no NetworkManager yet.
    }

    pub fn need_wifi_state(&self) -> bool {
        false
    }

    pub fn update_wifi_state(&self) {
        // M1: no WiFi monitoring yet.
    }

}
