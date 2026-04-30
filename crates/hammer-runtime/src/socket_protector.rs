use std::os::fd::AsRawFd;
use std::sync::Arc;

use hammer_adapter::PlatformInterface;
use hammer_core::error::HammerError;

#[allow(dead_code)]
#[derive(Clone, Default)]
pub(crate) struct SocketProtector {
    platform: Option<Arc<dyn PlatformInterface>>,
}

#[allow(dead_code)]
impl SocketProtector {
    pub(crate) fn new(platform: Arc<dyn PlatformInterface>) -> Self {
        Self {
            platform: Some(platform),
        }
    }

    pub(crate) fn platform(&self) -> Option<Arc<dyn PlatformInterface>> {
        self.platform.clone()
    }

    pub(crate) fn protect<T: AsRawFd>(&self, socket: &T) -> Result<(), HammerError> {
        let Some(platform) = &self.platform else {
            return Ok(());
        };
        if !platform.use_platform_auto_detect_interface_control() {
            return Ok(());
        }
        platform.auto_detect_interface_control(socket.as_raw_fd())
    }
}

impl From<Option<Arc<dyn PlatformInterface>>> for SocketProtector {
    fn from(platform: Option<Arc<dyn PlatformInterface>>) -> Self {
        Self { platform }
    }
}
