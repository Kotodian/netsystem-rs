use std::sync::{Arc, Mutex};

use hammer::{
    HammerDefaultInterfaceUpdateListener, HammerError, HammerNetworkInterface,
    HammerNetworkInterfaceIterator, HammerPlatform, HammerService, HammerStringIterator,
    HammerTunOptions, HammerWIFIState,
};
use hammer_core::lifecycle::LIFECYCLE_ORDER;

const MIN_TOML: &str = r#"
[log]
level = "debug"

[tun]
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
mtu = 1400
stack = "disabled"
[hysteria2]
server = "example.com"
password = "x"
sni = "example.com"
[dns]
server = "udp://1.1.1.1"
[route]
final = "hysteria2"
"#;

#[derive(Default)]
struct CapturePlatform {
    lines: Mutex<Vec<String>>,
    open_tun_calls: Mutex<Vec<HammerTunOptions>>,
}

impl HammerPlatform for CapturePlatform {
    fn open_tun(&self, options: HammerTunOptions) -> Result<i32, HammerError> {
        self.open_tun_calls
            .lock()
            .expect("CapturePlatform poisoned")
            .push(options);
        Ok(42)
    }
    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }
    fn auto_detect_interface_control(&self, _fd: i32) -> Result<(), HammerError> {
        Ok(())
    }
    fn start_default_interface_monitor(
        &self,
        _listener: Arc<HammerDefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError> {
        Ok(())
    }
    fn close_default_interface_monitor(
        &self,
        _listener: Arc<HammerDefaultInterfaceUpdateListener>,
    ) -> Result<(), HammerError> {
        Ok(())
    }
    fn get_interfaces(&self) -> Result<Box<dyn HammerNetworkInterfaceIterator>, HammerError> {
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
    fn write_log(&self, _level: i32, message: String) {
        self.lines
            .lock()
            .expect("CapturePlatform poisoned")
            .push(message);
    }
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

fn make_service() -> (Arc<CapturePlatform>, Arc<HammerService>) {
    let platform = Arc::new(CapturePlatform::default());
    let svc = HammerService::new(MIN_TOML, Arc::clone(&platform) as Arc<dyn HammerPlatform>)
        .expect("HammerService::new should accept the minimal config");
    (platform, svc)
}

fn count(lines: &[String], needle: &str) -> usize {
    lines.iter().filter(|l| l.contains(needle)).count()
}

#[test]
fn service_start_opens_tun_with_configured_options() {
    let (platform, svc) = make_service();
    svc.start().expect("start should open TUN");
    svc.close().expect("close should succeed");

    let calls = platform.open_tun_calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "open_tun calls = {calls:?}");
    let options = &calls[0];
    assert_eq!(options.name, "utun");
    assert_eq!(options.mtu, 1400);
    assert_eq!(options.address, vec!["172.19.0.1/30"]);
    assert_eq!(options.route, vec!["0.0.0.0/0"]);
}

#[test]
fn service_walks_all_stages_and_managers() {
    let (platform, svc) = make_service();
    svc.start().expect("start should succeed");
    svc.close().expect("close should succeed");

    let lines = platform.lines.lock().unwrap();

    let lifecycle_count = LIFECYCLE_ORDER.len();
    assert_eq!(
        count(&lines, "stage initialize"),
        lifecycle_count,
        "lines = {lines:?}"
    );
    assert_eq!(count(&lines, "stage post-start"), lifecycle_count);
    assert_eq!(count(&lines, "stage started"), lifecycle_count);
    // "stage start" matches "stage started" too — subtract.
    let raw_start = count(&lines, "stage start");
    let started = count(&lines, "stage started");
    assert_eq!(raw_start - started, lifecycle_count);

    assert!(
        count(&lines, ": close") >= lifecycle_count,
        "lines = {lines:?}"
    );
}

#[test]
fn double_start_is_idempotent() {
    let (platform, svc) = make_service();
    svc.start().expect("first start");
    svc.start().expect("second start should be a no-op");
    svc.close().expect("close");

    let lines = platform.lines.lock().unwrap();
    let lifecycle_count = LIFECYCLE_ORDER.len();
    assert_eq!(count(&lines, "stage initialize"), lifecycle_count);
    assert_eq!(count(&lines, "stage post-start"), lifecycle_count);
}

#[test]
fn start_after_close_returns_service_closed() {
    let (_platform, svc) = make_service();
    svc.start().expect("start");
    svc.close().expect("close");
    let err = svc.start().expect_err("start after close must fail");
    assert!(matches!(err, HammerError::ServiceClosed), "got = {err:?}");
}

#[test]
fn pause_and_wake_round_trip_does_not_panic() {
    let (_platform, svc) = make_service();
    svc.start().expect("start");
    svc.pause();
    svc.wake();
    svc.pause();
    svc.wake();
    svc.close().expect("close");
}

#[test]
fn need_wifi_state_starts_false_and_update_does_not_panic() {
    let (_platform, svc) = make_service();
    svc.start().expect("start");
    assert!(!svc.need_wifi_state());
    svc.update_wifi_state();
    svc.reset_network();
    svc.close().expect("close");
}

/// Booting a config with a `[[endpoints]]` block must walk the configured
/// lifecycle graph and register the endpoint's outbound view.
#[cfg(feature = "wireguard")]
#[test]
fn service_starts_with_wireguard_endpoint() {
    const PLACEHOLDER_PRIVATE: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    const PLACEHOLDER_PEER: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=";
    let toml = format!(
        r#"
[log]
level = "debug"

[tun]
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
mtu = 1400
stack = "disabled"
[hysteria2]
server = "example.com"
password = "x"
sni = "example.com"
[dns]
server = "udp://1.1.1.1"
[route]
final = "hysteria2"

[[endpoints]]
type = "wireguard"
id = "wg-out"
private_key = "{PLACEHOLDER_PRIVATE}"
address = ["10.66.0.2/32"]

[[endpoints.peers]]
public_key = "{PLACEHOLDER_PEER}"
address = "1.2.3.4"
port = 51820
allowed_ips = ["0.0.0.0/0"]
"#,
    );
    let platform = Arc::new(CapturePlatform::default());
    let svc = HammerService::new(&toml, Arc::clone(&platform) as Arc<dyn HammerPlatform>)
        .expect("config with wireguard endpoint must build");
    svc.start().expect("start should still walk all stages");
    svc.close().expect("close should still walk all stages");

    let lines = platform.lines.lock().unwrap();
    let lifecycle_count = LIFECYCLE_ORDER.len();
    assert_eq!(
        count(&lines, "stage initialize"),
        lifecycle_count,
        "lines = {lines:?}"
    );
    assert_eq!(count(&lines, "stage post-start"), lifecycle_count);
    assert_eq!(count(&lines, "stage started"), lifecycle_count);
}

#[test]
fn close_is_idempotent() {
    let (_platform, svc) = make_service();
    svc.start().expect("start");
    svc.close().expect("first close");
    svc.close().expect("second close should be a no-op");
}
