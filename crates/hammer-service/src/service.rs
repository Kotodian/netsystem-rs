use std::sync::Arc;
use std::time::Duration;

use hammer_core::MetricSample;
use hammer_core::error::HammerResult;
use hammer_core::log::LogWriter;
use hammer_runtime::adapter::{PlatformInterface, ProbeReport};

pub struct RuntimeService {
    inner: Arc<hammer_runtime::RuntimeService>,
}

impl RuntimeService {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
    ) -> HammerResult<Arc<Self>> {
        let inner = hammer_runtime::RuntimeService::new(config_content, platform, writer)?;
        Ok(Arc::new(Self { inner }))
    }

    pub fn start(&self) -> HammerResult<()> {
        self.inner.start()
    }

    pub fn close(&self) -> HammerResult<()> {
        self.inner.close()
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

    #[cfg(feature = "probe")]
    pub fn probe_outbounds(
        &self,
        protocol: &str,
        timeout: Duration,
    ) -> HammerResult<Vec<ProbeReport>> {
        self.inner.probe_outbounds(protocol, timeout)
    }

    pub fn current_selection(&self, outbound_id: &str) -> Option<String> {
        self.inner.current_selection(outbound_id)
    }

    pub fn urltest(&self, outbound_id: &str, timeout: Duration) -> HammerResult<Vec<ProbeReport>> {
        self.inner.urltest(outbound_id, timeout)
    }

    pub fn metrics_snapshot(&self) -> Vec<MetricSample> {
        self.inner.metrics_snapshot()
    }
}
