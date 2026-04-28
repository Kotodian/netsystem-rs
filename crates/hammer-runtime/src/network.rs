use std::sync::atomic::{AtomicBool, Ordering};

use hammer_adapter::NetworkManager as NetworkManagerTrait;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

/// `route.NetworkManager` skeleton. The real interface monitor + autoDetect
/// path land in M4 once the Platform.get_interfaces / start_default_interface_monitor
/// callbacks are wired in.
pub struct NetworkManager {
    logger: Logger,
    auto_detect_interface: bool,
    need_wifi_state: AtomicBool,
}

impl NetworkManager {
    pub fn new(logger: Logger, auto_detect_interface: bool) -> Self {
        Self {
            logger,
            auto_detect_interface,
            need_wifi_state: AtomicBool::new(false),
        }
    }

    pub fn set_need_wifi_state(&self, need: bool) {
        self.need_wifi_state.store(need, Ordering::SeqCst);
    }
}

impl_logging_lifecycle!(NetworkManager, "network");

impl NetworkManagerTrait for NetworkManager {
    fn auto_detect_interface(&self) -> bool {
        self.auto_detect_interface
    }

    fn need_wifi_state(&self) -> bool {
        self.need_wifi_state.load(Ordering::SeqCst)
    }

    fn update_wifi_state(&self) {
        self.logger.debug("update_wifi_state (M2 stub)");
    }

    fn reset_network(&self) {
        self.logger.debug("reset_network (M2 stub)");
    }
}
