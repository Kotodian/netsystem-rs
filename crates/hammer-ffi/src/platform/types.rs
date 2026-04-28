// uniffi 0.31 UDL mode: the macros emitted from `src/hammer.udl` reference these
// types/traits but do NOT define them — they decorate definitions we own here
// with the FFI Lower/Lift impls. Keep field names, ordering and method
// signatures aligned with `hammer.udl` or scaffolding will fail to compile.

use crate::error::HammerError;

#[derive(Debug, Clone)]
pub struct TunOptions {
    pub name: String,
    pub mtu: i32,
    pub address: Vec<String>,
    pub route: Vec<String>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct WifiState {
    pub ssid: String,
    pub bssid: String,
}

pub trait Platform: Send + Sync {
    fn open_tun(&self, options: TunOptions) -> Result<i32, HammerError>;
    fn use_platform_auto_detect_interface_control(&self) -> bool;
    fn auto_detect_interface_control(&self, fd: i32) -> Result<(), HammerError>;
    fn get_interfaces(&self) -> Result<Vec<NetworkInterface>, HammerError>;
    fn under_network_extension(&self) -> bool;
    fn include_all_networks(&self) -> bool;
    fn read_wifi_state(&self) -> Option<WifiState>;
    fn system_certificates(&self) -> Vec<String>;
    fn clear_dns_cache(&self);
    fn write_log(&self, level: i32, message: String);
}
