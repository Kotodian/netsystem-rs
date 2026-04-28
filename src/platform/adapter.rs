use std::sync::Arc;

use crate::log::{Level, LogWriter};
use crate::{Platform, WifiState};

/// Thin facade over the Swift-implemented [`Platform`] callback interface.
/// M1 only forwards methods that the Service / log path actually needs;
/// later milestones (network manager, TUN open, etc.) extend this surface.
pub struct PlatformAdapter {
    platform: Arc<dyn Platform>,
}

// Methods marked `dead_code` here are wired up in M2 once the real Manager
// implementations replace the StubManager set. Keeping them now lets us hold
// the Go-equivalent surface in one place without scattering allow attributes.
#[allow(dead_code)]
impl PlatformAdapter {
    pub fn new(platform: Arc<dyn Platform>) -> Self {
        Self { platform }
    }

    pub fn write_log(&self, level: Level, message: String) {
        self.platform.write_log(level as i32, message);
    }

    pub fn read_wifi_state(&self) -> Option<WifiState> {
        self.platform.read_wifi_state()
    }

    pub fn under_network_extension(&self) -> bool {
        self.platform.under_network_extension()
    }

    pub fn include_all_networks(&self) -> bool {
        self.platform.include_all_networks()
    }

    pub fn system_certificates(&self) -> Vec<String> {
        self.platform.system_certificates()
    }

    pub fn clear_dns_cache(&self) {
        self.platform.clear_dns_cache()
    }
}

/// Bridges the in-memory log pipeline to Swift via `Platform.writeLog(level, message)`.
pub struct PlatformLogWriter {
    adapter: Arc<PlatformAdapter>,
}

impl PlatformLogWriter {
    pub fn new(adapter: Arc<PlatformAdapter>) -> Self {
        Self { adapter }
    }
}

impl LogWriter for PlatformLogWriter {
    fn write_message(&self, level: Level, message: String) {
        self.adapter.write_log(level, message);
    }
}
