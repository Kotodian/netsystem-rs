use std::sync::Arc;
use std::time::Duration;

use hammer_core::log::LogWriter;
use hammer_runtime::RuntimeService;

use crate::HammerPlatform;
use crate::error::HammerError;
use crate::platform::{PlatformAdapter, PlatformLogWriter};

/// FFI-friendly latency probe report (one per outbound).
///
/// `ok=true` means the probe completed within `timeout_ms` and
/// `latency_ms` carries the measured RTT. `ok=false` carries the
/// transport error in `error` and `latency_ms=0`.
#[derive(Debug, Clone)]
pub struct HammerProbeReport {
    pub outbound_id: String,
    pub protocol: String,
    pub ok: bool,
    pub latency_ms: u64,
    pub error: String,
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

    pub fn probe_outbounds(
        &self,
        protocol: String,
        timeout_ms: u64,
    ) -> Result<Vec<HammerProbeReport>, HammerError> {
        let timeout = Duration::from_millis(timeout_ms);
        let reports = self
            .inner
            .probe_outbounds(&protocol, timeout)
            .map_err(HammerError::from)?;
        Ok(reports
            .into_iter()
            .map(|report| {
                let outbound_id = report.outbound_id;
                let protocol = report.protocol;
                match report.result {
                    Ok(elapsed) => HammerProbeReport {
                        outbound_id,
                        protocol,
                        ok: true,
                        latency_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                        error: String::new(),
                    },
                    Err(err) => HammerProbeReport {
                        outbound_id,
                        protocol,
                        ok: false,
                        latency_ms: 0,
                        error: err.to_string(),
                    },
                }
            })
            .collect())
    }
}
