use hammer_core::lifecycle::Lifecycle;

/// `adapter.NetworkManager` — surfaces the platform's view of network
/// interfaces and the auto-detected default. Tracks WiFi state on iOS / macOS
/// and notifies subscribers when the default interface changes.
///
/// M2 captures the lifecycle + the few accessors Service.{reset_network,
/// need_wifi_state, update_wifi_state} touches today. The interface monitor
/// callback API and the address/MTU snapshots arrive in M4 with the real
/// NetworkManager.
pub trait NetworkManager: Lifecycle {
    fn auto_detect_interface(&self) -> bool;
    fn need_wifi_state(&self) -> bool;
    fn update_wifi_state(&self);
    fn reset_network(&self);
}
