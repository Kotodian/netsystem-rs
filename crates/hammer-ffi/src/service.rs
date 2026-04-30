use std::sync::Arc;

use hammer_core::log::LogWriter;
use hammer_runtime::RuntimeService;

use crate::HammerPlatform;
use crate::error::HammerError;
use crate::platform::{PlatformAdapter, PlatformLogWriter};

pub struct HammerService {
    inner: Arc<RuntimeService>,
}

impl HammerService {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn HammerPlatform>,
    ) -> Result<Arc<Self>, HammerError> {
        let adapter = Arc::new(PlatformAdapter::new(platform));
        let writer: Arc<dyn LogWriter> = Arc::new(PlatformLogWriter::new(Arc::clone(&adapter)));
        let inner = RuntimeService::new(config_content, adapter, writer)?;
        Ok(Arc::new(Self { inner }))
    }

    pub fn start(&self) -> Result<(), HammerError> {
        self.inner.start()?;
        Ok(())
    }

    pub fn close(&self) -> Result<(), HammerError> {
        self.inner.close()?;
        Ok(())
    }

    pub fn pause(&self) {
        self.inner.pause();
    }

    pub fn wake(&self) {
        self.inner.wake();
    }

    pub fn reset_network(&self) {
        self.inner.reset_network();
    }

    pub fn need_wifi_state(&self) -> bool {
        self.inner.need_wifi_state()
    }

    pub fn update_wifi_state(&self) {
        self.inner.update_wifi_state();
    }
}
