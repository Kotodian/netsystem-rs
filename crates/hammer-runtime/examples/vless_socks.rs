use std::env;
use std::fs;
use std::sync::Arc;

use hammer_adapter::{
    DefaultInterfaceUpdateListener, NetworkInterface, PlatformInterface, TunOptions, WifiState,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::log::{Level, LogWriter};
use hammer_runtime::RuntimeService;

struct ExamplePlatform;

impl PlatformInterface for ExamplePlatform {
    fn open_tun(&self, _options: TunOptions) -> CoreResult<i32> {
        Err(CoreError::internal(
            "vless_socks example supports proxy inbounds only",
        ))
    }

    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }

    fn auto_detect_interface_control(&self, _fd: i32) -> CoreResult<()> {
        Ok(())
    }

    fn start_default_interface_monitor(
        &self,
        _listener: Arc<dyn DefaultInterfaceUpdateListener>,
    ) -> CoreResult<()> {
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _listener: Arc<dyn DefaultInterfaceUpdateListener>,
    ) -> CoreResult<()> {
        Ok(())
    }

    fn get_interfaces(&self) -> CoreResult<Vec<NetworkInterface>> {
        Ok(Vec::new())
    }

    fn read_wifi_state(&self) -> Option<WifiState> {
        None
    }
}

struct StderrWriter;

impl LogWriter for StderrWriter {
    fn write_message(&self, _level: Level, message: String) {
        eprint!("{message}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = env::args().nth(1).ok_or_else(|| {
        CoreError::config_validation(
            "usage: cargo run -p hammer-runtime --example vless_socks -- <config.toml>",
        )
    })?;
    let config = fs::read_to_string(&config_path)?;
    let service = RuntimeService::new(&config, Arc::new(ExamplePlatform), Arc::new(StderrWriter))?;
    service.start()?;
    eprintln!("hammer example proxy is running with config {config_path}");
    loop {
        std::thread::park();
    }
}
