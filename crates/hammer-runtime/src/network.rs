use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tracing::{debug, error, info, trace};

use hammer_adapter::{
    DefaultInterfaceUpdateListener, NetworkInterface, NetworkManager as NetworkManagerTrait,
    PlatformInterface, WifiState,
};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::Logger;

use crate::{ConnectionManager, PauseManager};

/// `route.NetworkManager` skeleton. The real interface monitor + autoDetect
/// path land in M4 once the Platform.get_interfaces / start_default_interface_monitor
/// callbacks are wired in.
pub struct NetworkManager {
    auto_detect_interface: bool,
    need_wifi_state: AtomicBool,
    platform: Option<Arc<dyn PlatformInterface>>,
    pause: Option<Arc<PauseManager>>,
    connection: Option<Arc<ConnectionManager>>,
    interfaces: Mutex<Vec<NetworkInterface>>,
    default_interface: Mutex<Option<NetworkInterface>>,
    wifi_state: Mutex<Option<WifiState>>,
    listener: Mutex<Option<Arc<dyn DefaultInterfaceUpdateListener>>>,
    started: AtomicBool,
}

impl NetworkManager {
    pub fn new(_logger: Logger, auto_detect_interface: bool) -> Self {
        Self {
            auto_detect_interface,
            need_wifi_state: AtomicBool::new(false),
            platform: None,
            pause: None,
            connection: None,
            interfaces: Mutex::new(Vec::new()),
            default_interface: Mutex::new(None),
            wifi_state: Mutex::new(None),
            listener: Mutex::new(None),
            started: AtomicBool::new(false),
        }
    }

    pub fn with_platform(
        _logger: Logger,
        auto_detect_interface: bool,
        platform: Arc<dyn PlatformInterface>,
        pause: Arc<PauseManager>,
        connection: Arc<ConnectionManager>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            auto_detect_interface,
            need_wifi_state: AtomicBool::new(false),
            platform: Some(platform),
            pause: Some(pause),
            connection: Some(connection),
            interfaces: Mutex::new(Vec::new()),
            default_interface: Mutex::new(None),
            wifi_state: Mutex::new(None),
            listener: Mutex::new(Some(Arc::new(NetworkUpdateListener {
                manager: weak.clone(),
            }))),
            started: AtomicBool::new(false),
        })
    }

    pub fn set_need_wifi_state(&self, need: bool) {
        self.need_wifi_state.store(need, Ordering::Relaxed);
    }

    pub fn default_network_interface(&self) -> Option<NetworkInterface> {
        self.default_interface
            .lock()
            .expect("NetworkManager default interface poisoned")
            .clone()
    }

    pub fn network_interfaces(&self) -> Vec<NetworkInterface> {
        self.interfaces
            .lock()
            .expect("NetworkManager interfaces poisoned")
            .clone()
    }

    pub fn wifi_state(&self) -> Option<WifiState> {
        self.wifi_state
            .lock()
            .expect("NetworkManager wifi state poisoned")
            .clone()
    }

    pub fn update_interfaces(&self) -> Result<(), HammerError> {
        let Some(platform) = &self.platform else {
            return Ok(());
        };
        let mut interfaces = platform.get_interfaces()?;
        interfaces.retain(|interface| interface.flags & 1 != 0);
        *self
            .interfaces
            .lock()
            .expect("NetworkManager interfaces poisoned") = interfaces;
        Ok(())
    }
}

impl Lifecycle for NetworkManager {
    fn name(&self) -> &str {
        "network"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        debug!("stage {}", stage.name());
        match stage {
            StartStage::Initialize => {
                self.update_interfaces()?;
                if let (Some(platform), Some(listener)) = (
                    self.platform.as_ref(),
                    self.listener
                        .lock()
                        .expect("NetworkManager listener poisoned")
                        .as_ref()
                        .cloned(),
                ) {
                    platform.start_default_interface_monitor(listener)?;
                }
            }
            StartStage::Started => {
                self.started.store(true, Ordering::Relaxed);
            }
            _ => {}
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        if let (Some(platform), Some(listener)) = (
            self.platform.as_ref(),
            self.listener
                .lock()
                .expect("NetworkManager listener poisoned")
                .as_ref()
                .cloned(),
        ) {
            platform.close_default_interface_monitor(listener)?;
        }
        debug!("close");
        Ok(())
    }
}

impl NetworkManagerTrait for NetworkManager {
    fn auto_detect_interface(&self) -> bool {
        self.auto_detect_interface
    }

    fn need_wifi_state(&self) -> bool {
        self.need_wifi_state.load(Ordering::Relaxed)
    }

    fn update_wifi_state(&self) {
        let Some(platform) = &self.platform else {
            return;
        };
        *self
            .wifi_state
            .lock()
            .expect("NetworkManager wifi state poisoned") = platform.read_wifi_state();
    }

    fn reset_network(&self) {
        if let Some(connection) = &self.connection {
            connection.close_all();
        }
        debug!("reset_network");
    }
}

struct NetworkUpdateListener {
    manager: Weak<NetworkManager>,
}

impl DefaultInterfaceUpdateListener for NetworkUpdateListener {
    fn update_default_interface(
        &self,
        interface_name: String,
        interface_index: i32,
        is_expensive: bool,
        is_constrained: bool,
    ) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        if let Err(err) = manager.update_interfaces() {
            error!("update interfaces: {err}");
        }
        manager.set_default_interface(
            interface_name,
            interface_index,
            is_expensive,
            is_constrained,
        );
    }
}

impl NetworkManager {
    fn set_default_interface(
        &self,
        interface_name: String,
        interface_index: i32,
        is_expensive: bool,
        is_constrained: bool,
    ) {
        if interface_index < 0 || interface_name.is_empty() {
            let mut current = self
                .default_interface
                .lock()
                .expect("NetworkManager default interface poisoned");
            if current.is_none() {
                trace!("default interface is still missing");
                return;
            }
            *current = None;
            if let Some(pause) = &self.pause {
                pause.network_pause();
            }
            error!("missing default interface");
            return;
        }

        let mut default = self
            .interfaces
            .lock()
            .expect("NetworkManager interfaces poisoned")
            .iter()
            .find(|interface| interface.index == interface_index)
            .cloned()
            .unwrap_or_else(|| NetworkInterface {
                index: interface_index,
                mtu: 0,
                name: interface_name,
                type_: 0,
                flags: 0,
                expensive: false,
                constrained: false,
                addresses: Vec::new(),
                dns_servers: Vec::new(),
            });
        default.expensive |= is_expensive;
        default.constrained |= is_constrained;
        {
            let mut current = self
                .default_interface
                .lock()
                .expect("NetworkManager default interface poisoned");
            if current.as_ref() == Some(&default) {
                trace!(
                    "default interface unchanged {}, index {}",
                    default.name, default.index
                );
                return;
            }
            *current = Some(default.clone());
        }
        if let Some(pause) = &self.pause {
            pause.network_wake();
        }
        self.update_wifi_state();
        info!(
            "updated default interface {}, index {}",
            default.name, default.index
        );
        if self.started.load(Ordering::Relaxed) {
            self.reset_network();
        }
    }
}
