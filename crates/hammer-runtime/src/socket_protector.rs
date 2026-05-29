use std::os::fd::AsRawFd;
use std::sync::Arc;

use hammer_adapter::PlatformInterface;
use hammer_core::error::HammerResult;

#[derive(Clone)]
pub struct RuntimePlatform(Arc<dyn PlatformInterface>);

impl RuntimePlatform {
    pub(crate) fn into_inner(self) -> Arc<dyn PlatformInterface> {
        self.0
    }
}

impl<P> From<Arc<P>> for RuntimePlatform
where
    P: PlatformInterface + 'static,
{
    fn from(platform: Arc<P>) -> Self {
        let platform: Arc<dyn PlatformInterface> = platform;
        Self(platform)
    }
}

impl From<Arc<dyn PlatformInterface>> for RuntimePlatform {
    fn from(platform: Arc<dyn PlatformInterface>) -> Self {
        Self(platform)
    }
}

#[derive(Clone, Default)]
pub struct SocketProtector {
    platform: Option<Arc<dyn PlatformInterface>>,
}

impl SocketProtector {
    pub fn new(platform: impl Into<Self>) -> Self {
        platform.into()
    }

    pub fn platform(&self) -> Option<Arc<dyn PlatformInterface>> {
        self.platform.clone()
    }

    pub fn protect<T: AsRawFd>(&self, socket: &T) -> HammerResult<()> {
        let Some(platform) = &self.platform else {
            return Ok(());
        };
        if !platform.use_platform_auto_detect_interface_control() {
            return Ok(());
        }
        platform.auto_detect_interface_control(socket.as_raw_fd())
    }
}

impl From<RuntimePlatform> for SocketProtector {
    fn from(platform: RuntimePlatform) -> Self {
        Self {
            platform: Some(platform.into_inner()),
        }
    }
}

impl<P> From<Arc<P>> for SocketProtector
where
    P: PlatformInterface + 'static,
{
    fn from(platform: Arc<P>) -> Self {
        RuntimePlatform::from(platform).into()
    }
}

impl From<Arc<dyn PlatformInterface>> for SocketProtector {
    fn from(platform: Arc<dyn PlatformInterface>) -> Self {
        RuntimePlatform::from(platform).into()
    }
}

impl From<Option<Arc<dyn PlatformInterface>>> for SocketProtector {
    fn from(platform: Option<Arc<dyn PlatformInterface>>) -> Self {
        Self { platform }
    }
}

impl<P> From<Option<Arc<P>>> for SocketProtector
where
    P: PlatformInterface + 'static,
{
    fn from(platform: Option<Arc<P>>) -> Self {
        Self {
            platform: platform.map(|platform| platform as Arc<dyn PlatformInterface>),
        }
    }
}
