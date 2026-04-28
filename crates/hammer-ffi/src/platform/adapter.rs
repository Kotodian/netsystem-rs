use std::sync::Arc;

use hammer_adapter::{
    DefaultInterfaceUpdateListener as AdapterDefaultInterfaceUpdateListener,
    NetworkInterface as AdapterNetworkInterface, PlatformInterface, WifiState as AdapterWifiState,
};
use hammer_core::error::CoreError;
use hammer_core::log::{Level, LogWriter};

use crate::{NetworkInterface, Platform, WifiState};

/// Thin facade over the Swift-implemented [`Platform`] callback interface.
/// M2 keeps the M1 surface intact while real Manager implementations land
/// behind it; the methods marked `dead_code` get wired up incrementally.
pub struct PlatformAdapter {
    platform: Arc<dyn Platform>,
}

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

    pub fn get_interfaces(&self) -> Result<Vec<NetworkInterface>, crate::HammerError> {
        self.platform.get_interfaces()
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

impl PlatformInterface for PlatformAdapter {
    fn use_platform_auto_detect_interface_control(&self) -> bool {
        self.platform.use_platform_auto_detect_interface_control()
    }

    fn auto_detect_interface_control(&self, fd: i32) -> Result<(), CoreError> {
        self.platform
            .auto_detect_interface_control(fd)
            .map_err(platform_error)
    }

    fn start_default_interface_monitor(
        &self,
        _listener: Arc<dyn AdapterDefaultInterfaceUpdateListener>,
    ) -> Result<(), CoreError> {
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _listener: Arc<dyn AdapterDefaultInterfaceUpdateListener>,
    ) -> Result<(), CoreError> {
        Ok(())
    }

    fn get_interfaces(&self) -> Result<Vec<AdapterNetworkInterface>, CoreError> {
        self.platform
            .get_interfaces()
            .map(|interfaces| interfaces.into_iter().map(Into::into).collect())
            .map_err(platform_error)
    }

    fn read_wifi_state(&self) -> Option<AdapterWifiState> {
        self.platform.read_wifi_state().map(Into::into)
    }
}

impl From<NetworkInterface> for AdapterNetworkInterface {
    fn from(value: NetworkInterface) -> Self {
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

impl From<WifiState> for AdapterWifiState {
    fn from(value: WifiState) -> Self {
        Self {
            ssid: value.ssid,
            bssid: value.bssid,
        }
    }
}

fn platform_error(err: crate::HammerError) -> CoreError {
    CoreError::internal(err.to_string())
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
