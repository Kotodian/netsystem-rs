// uniffi 0.31 UDL mode: the macros emitted from `src/hammer.udl` reference these
// types/traits but do NOT define them — they decorate definitions we own here
// with the FFI Lower/Lift impls. Keep field names, ordering and method
// signatures aligned with `hammer.udl` or scaffolding will fail to compile.

use std::sync::Arc;

use hammer_adapter::DefaultInterfaceUpdateListener as AdapterListener;

use crate::error::HammerError;

#[derive(Debug, Clone)]
pub struct HammerSetupOptions {
    pub base_path: String,
    pub temp_path: String,
    pub debug: bool,
}

#[derive(Debug, Clone)]
pub struct HammerTunOptions {
    pub name: String,
    pub mtu: i32,
    pub address: Vec<String>,
    pub route: Vec<String>,
    pub route_exclude: Vec<String>,
    pub auto_route: bool,
    pub strict_route: bool,
    pub tap: bool,
}

#[derive(Debug, Clone)]
pub struct HammerNetworkInterface {
    pub index: i32,
    pub mtu: i32,
    pub name: String,
    pub type_: i32,
    pub flags: i32,
    pub expensive: bool,
    pub constrained: bool,
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct HammerWIFIState {
    pub ssid: String,
    pub bssid: String,
}

/// Swift-implemented iterator returned by `HammerPlatform.systemCertificates`.
/// Mirrors the gomobile `HammerStringIteratorProtocol` shape.
pub trait HammerStringIterator: Send + Sync {
    fn has_next(&self) -> bool;
    fn next(&self) -> String;
}

/// Swift-implemented iterator returned by `HammerPlatform.getInterfaces`.
pub trait HammerNetworkInterfaceIterator: Send + Sync {
    fn has_next(&self) -> bool;
    fn next(&self) -> Option<HammerNetworkInterface>;
}

/// Rust-implemented listener handed to the Swift platform via
/// `HammerPlatform.startDefaultInterfaceMonitor`. Swift retains the instance
/// and calls `updateDefaultInterface` whenever its `NWPathMonitor` fires.
pub struct HammerDefaultInterfaceUpdateListener {
    inner: Arc<dyn AdapterListener>,
}

impl HammerDefaultInterfaceUpdateListener {
    pub fn from_adapter(inner: Arc<dyn AdapterListener>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    pub fn update_default_interface(
        &self,
        name: String,
        interface_index: i32,
        is_expensive: bool,
        is_constrained: bool,
    ) {
        self.inner
            .update_default_interface(name, interface_index, is_expensive, is_constrained);
    }
}

pub trait HammerPlatform: Send + Sync {
    fn open_tun(&self, options: HammerTunOptions) -> Result<i32, HammerError>;
    fn use_platform_auto_detect_interface_control(&self) -> bool;
    fn auto_detect_interface_control(&self, fd: i32) -> Result<(), HammerError>;
    fn start_default_interface_monitor(
        &self,
        listener: Arc<HammerDefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError>;
    fn close_default_interface_monitor(
        &self,
        listener: Arc<HammerDefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError>;
    fn get_interfaces(&self) -> Result<Box<dyn HammerNetworkInterfaceIterator>, HammerError>;
    fn under_network_extension(&self) -> bool;
    fn include_all_networks(&self) -> bool;
    fn read_wifi_state(&self) -> Option<HammerWIFIState>;
    fn system_certificates(&self) -> Option<Box<dyn HammerStringIterator>>;
    fn write_log(&self, level: i32, message: String);
}
