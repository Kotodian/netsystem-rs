use std::sync::Arc;

use hammer_core::error::CoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkInterface {
    pub index: i32,
    pub mtu: i32,
    pub name: String,
    pub type_: i32,
    pub flags: i32,
    pub expensive: bool,
    pub constrained: bool,
    pub addresses: Vec<String>,
    pub dns_servers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WifiState {
    pub ssid: String,
    pub bssid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunOptions {
    pub name: String,
    pub mtu: i32,
    pub address: Vec<String>,
    pub route: Vec<String>,
}

pub trait DefaultInterfaceUpdateListener: Send + Sync + 'static {
    fn update_default_interface(
        &self,
        interface_name: String,
        interface_index: i32,
        is_expensive: bool,
        is_constrained: bool,
    );
}

pub trait PlatformInterface: Send + Sync + 'static {
    fn open_tun(&self, options: TunOptions) -> Result<i32, CoreError>;
    fn use_platform_auto_detect_interface_control(&self) -> bool;
    fn auto_detect_interface_control(&self, fd: i32) -> Result<(), CoreError>;
    fn start_default_interface_monitor(
        &self,
        listener: Arc<dyn DefaultInterfaceUpdateListener>,
    ) -> Result<(), CoreError>;
    fn close_default_interface_monitor(
        &self,
        listener: Arc<dyn DefaultInterfaceUpdateListener>,
    ) -> Result<(), CoreError>;
    fn get_interfaces(&self) -> Result<Vec<NetworkInterface>, CoreError>;
    fn read_wifi_state(&self) -> Option<WifiState>;
    fn system_certificates(&self) -> Vec<String> {
        Vec::new()
    }
}
