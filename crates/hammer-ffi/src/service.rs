use std::sync::Arc;

use hammer_core::log::LogWriter;
use hammer_service::RuntimeService;

use crate::HammerPlatform;
use crate::error::HammerError;
use crate::platform::{PlatformAdapter, PlatformLogWriter};

#[derive(Debug, Clone)]
pub struct HammerMetricLabel {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct HammerMetricSample {
    pub module: String,
    pub component_type: String,
    pub component_id: String,
    pub name: String,
    pub kind: String,
    pub value: u64,
    pub labels: Vec<HammerMetricLabel>,
}

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

    pub fn metrics(&self) -> Vec<HammerMetricSample> {
        self.inner
            .metrics_snapshot()
            .into_iter()
            .map(Into::into)
            .collect()
    }
}

impl From<hammer_core::MetricLabel> for HammerMetricLabel {
    fn from(label: hammer_core::MetricLabel) -> Self {
        Self {
            key: label.key,
            value: label.value,
        }
    }
}

impl From<hammer_core::MetricSample> for HammerMetricSample {
    fn from(sample: hammer_core::MetricSample) -> Self {
        Self {
            module: sample.module,
            component_type: sample.component_type,
            component_id: sample.component_id,
            name: sample.name,
            kind: sample.kind.to_string(),
            value: sample.value,
            labels: sample.labels.into_iter().map(Into::into).collect(),
        }
    }
}
