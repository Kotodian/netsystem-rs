use std::sync::{Arc, Mutex};

use hammer_adapter::{
    DefaultInterfaceUpdateListener as AdapterListener, NetworkInterface as AdapterNetworkInterface,
    PlatformInterface, TunOptions as AdapterTunOptions, WifiState as AdapterWifiState,
};
use hammer_core::error::CoreError;
use hammer_core::log::{Level, LogWriter};

use crate::platform::types::{
    HammerDefaultInterfaceUpdateListener, HammerNetworkInterface, HammerPlatform, HammerTunOptions,
    HammerWIFIState,
};

/// Thin facade over the Swift-implemented [`HammerPlatform`] callback interface,
/// adapting it to the workspace-internal [`PlatformInterface`] trait.
pub struct PlatformAdapter {
    platform: Arc<dyn HammerPlatform>,
    default_interface_listener: Mutex<Option<Arc<HammerDefaultInterfaceUpdateListener>>>,
}

impl PlatformAdapter {
    pub fn new(platform: Arc<dyn HammerPlatform>) -> Self {
        Self {
            platform,
            default_interface_listener: Mutex::new(None),
        }
    }

    pub fn write_log(&self, level: Level, message: String) {
        self.platform.write_log(level as i32, message);
    }
}

impl PlatformInterface for PlatformAdapter {
    fn open_tun(&self, options: AdapterTunOptions) -> Result<i32, CoreError> {
        self.platform
            .open_tun(options.into())
            .map_err(into_core_error)
    }

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
            .start_default_interface_monitor(Arc::clone(&bridge))
            .map_err(into_core_error)?;
        *self
            .default_interface_listener
            .lock()
            .expect("default interface listener poisoned") = Some(bridge);
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        listener: Arc<dyn AdapterListener>,
    ) -> Result<(), CoreError> {
        let bridge = self
            .default_interface_listener
            .lock()
            .expect("default interface listener poisoned")
            .take()
            .unwrap_or_else(|| HammerDefaultInterfaceUpdateListener::from_adapter(listener));
        self.platform
            .close_default_interface_monitor(bridge)
            .map_err(into_core_error)
    }

    fn get_interfaces(&self) -> Result<Vec<AdapterNetworkInterface>, CoreError> {
        let iter = self.platform.get_interfaces().map_err(into_core_error)?;
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

    fn system_certificates(&self) -> Vec<String> {
        let Some(iter) = self.platform.system_certificates() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while iter.has_next() {
            out.push(iter.next());
        }
        out
    }
}

impl From<AdapterTunOptions> for HammerTunOptions {
    fn from(value: AdapterTunOptions) -> Self {
        Self {
            name: value.name,
            mtu: value.mtu,
            address: value.address,
            route: value.route,
            route_exclude: value.route_exclude,
            auto_route: value.auto_route,
            strict_route: value.strict_route,
        }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hammer_adapter::{DefaultInterfaceUpdateListener as AdapterListener, PlatformInterface};

    use crate::platform::types::{
        HammerDefaultInterfaceUpdateListener, HammerNetworkInterface,
        HammerNetworkInterfaceIterator, HammerPlatform, HammerStringIterator, HammerTunOptions,
        HammerWIFIState,
    };

    use super::PlatformAdapter;

    #[derive(Default)]
    struct RecordingPlatform {
        started: Mutex<Vec<Arc<HammerDefaultInterfaceUpdateListener>>>,
        closed: Mutex<Vec<usize>>,
    }

    impl HammerPlatform for RecordingPlatform {
        fn open_tun(&self, _options: HammerTunOptions) -> Result<i32, crate::HammerError> {
            Ok(42)
        }

        fn use_platform_auto_detect_interface_control(&self) -> bool {
            false
        }

        fn auto_detect_interface_control(&self, _fd: i32) -> Result<(), crate::HammerError> {
            Ok(())
        }

        fn start_default_interface_monitor(
            &self,
            listener: Arc<HammerDefaultInterfaceUpdateListener>,
        ) -> Result<(), crate::HammerError> {
            self.started
                .lock()
                .expect("started poisoned")
                .push(listener);
            Ok(())
        }

        fn close_default_interface_monitor(
            &self,
            listener: Arc<HammerDefaultInterfaceUpdateListener>,
        ) -> Result<(), crate::HammerError> {
            self.closed
                .lock()
                .expect("closed poisoned")
                .push(Arc::as_ptr(&listener) as usize);
            Ok(())
        }

        fn get_interfaces(
            &self,
        ) -> Result<Box<dyn HammerNetworkInterfaceIterator>, crate::HammerError> {
            Ok(Box::new(EmptyInterfaceIterator))
        }

        fn under_network_extension(&self) -> bool {
            false
        }

        fn include_all_networks(&self) -> bool {
            false
        }

        fn read_wifi_state(&self) -> Option<HammerWIFIState> {
            None
        }

        fn system_certificates(&self) -> Option<Box<dyn HammerStringIterator>> {
            None
        }

        fn clear_dns_cache(&self) {}

        fn write_log(&self, _level: i32, _message: String) {}
    }

    struct EmptyInterfaceIterator;

    impl HammerNetworkInterfaceIterator for EmptyInterfaceIterator {
        fn has_next(&self) -> bool {
            false
        }

        fn next(&self) -> Option<HammerNetworkInterface> {
            None
        }
    }

    struct Listener;

    impl AdapterListener for Listener {
        fn update_default_interface(
            &self,
            _interface_name: String,
            _interface_index: i32,
            _is_expensive: bool,
            _is_constrained: bool,
        ) {
        }
    }

    #[test]
    fn default_interface_monitor_close_reuses_started_bridge() {
        let platform = Arc::new(RecordingPlatform::default());
        let adapter = PlatformAdapter::new(Arc::clone(&platform) as Arc<dyn HammerPlatform>);
        let listener = Arc::new(Listener);

        adapter
            .start_default_interface_monitor(Arc::clone(&listener) as Arc<dyn AdapterListener>)
            .expect("start monitor");
        adapter
            .close_default_interface_monitor(listener as Arc<dyn AdapterListener>)
            .expect("close monitor");

        let started = platform.started.lock().unwrap();
        let closed = platform.closed.lock().unwrap();
        assert_eq!(started.len(), 1);
        assert_eq!(closed.len(), 1);
        assert_eq!(Arc::as_ptr(&started[0]) as usize, closed[0]);
    }
}
