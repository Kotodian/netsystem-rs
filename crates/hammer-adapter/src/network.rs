use hammer_core::lifecycle::Lifecycle;

/// `adapter.NetworkManager` — surfaces the platform's view of network
/// interfaces and the auto-detected default. Tracks WiFi state on iOS / macOS
/// and notifies subscribers when the default interface changes.
///
pub trait NetworkManager: Lifecycle {
    fn auto_detect_interface(&self) -> bool;
    fn need_wifi_state(&self) -> bool;
    fn update_wifi_state(&self);
    fn reset_network(&self);
}
