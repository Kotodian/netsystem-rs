use std::sync::Arc;

use hammer_adapter::{
    DefaultInterfaceUpdateListener as AdapterListener,
    NetworkInterface as AdapterNetworkInterface, PlatformInterface, WifiState as AdapterWifiState,
};
use hammer_core::error::CoreError;
use hammer_core::log::{Level, LogWriter};

use crate::platform::types::{
    HammerDefaultInterfaceUpdateListener, HammerNetworkInterface, HammerPlatform, HammerWIFIState,
};

/// Thin facade over the Swift-implemented [`HammerPlatform`] callback interface,
/// adapting it to the workspace-internal [`PlatformInterface`] trait.
pub struct PlatformAdapter {
    platform: Arc<dyn HammerPlatform>,
}

impl PlatformAdapter {
    pub fn new(platform: Arc<dyn HammerPlatform>) -> Self {
        Self { platform }
    }

    pub fn write_log(&self, level: Level, message: String) {
        self.platform.write_log(level as i32, message);
    }
}

impl PlatformInterface for PlatformAdapter {
    fn use_platform_auto_detect_interface_control(&self) -> bool {
        self.platform.use_platform_auto_detect_interface_control()
    }

    fn auto_detect_interface_control(&self, fd: i32) -> Result<(), CoreError> {
        self.platform
            .auto_detect_interface_control(fd)
            .map_err(into_core_error)
    }

    fn start_default_interface_monitor(
        &self,
        listener: Arc<dyn AdapterListener>,
    ) -> Result<(), CoreError> {
        let bridge = HammerDefaultInterfaceUpdateListener::from_adapter(listener);
        self.platform
            .start_default_interface_monitor(bridge)
            .map_err(into_core_error)
    }

    fn close_default_interface_monitor(
        &self,
        listener: Arc<dyn AdapterListener>,
    ) -> Result<(), CoreError> {
        let bridge = HammerDefaultInterfaceUpdateListener::from_adapter(listener);
        self.platform
            .close_default_interface_monitor(bridge)
            .map_err(into_core_error)
    }

    fn get_interfaces(&self) -> Result<Vec<AdapterNetworkInterface>, CoreError> {
        let iter = self
            .platform
            .get_interfaces()
            .map_err(into_core_error)?;
        let mut out = Vec::new();
        while iter.has_next() {
            if let Some(item) = iter.next() {
                out.push(item.into());
            }
        }
        Ok(out)
    }

    fn read_wifi_state(&self) -> Option<AdapterWifiState> {
        self.platform.read_wifi_state().map(Into::into)
    }
}

impl From<HammerNetworkInterface> for AdapterNetworkInterface {
    fn from(value: HammerNetworkInterface) -> Self {
        Self {
            index: value.index,
            mtu: value.mtu,
            name: value.name,
            type_: value.type_,
            flags: value.flags,
            expensive: value.expensive,
            constrained: value.constrained,
            addresses: value.addresses,
            dns_servers: value.dns_servers,
        }
    }
}

impl From<HammerWIFIState> for AdapterWifiState {
    fn from(value: HammerWIFIState) -> Self {
        Self {
            ssid: value.ssid,
            bssid: value.bssid,
        }
    }
}

fn into_core_error(err: crate::HammerError) -> CoreError {
    CoreError::internal(err.to_string())
}

/// Bridges the in-memory log pipeline to Swift via `HammerPlatform.writeLog(level, message)`.
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
