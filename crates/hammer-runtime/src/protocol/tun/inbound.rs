use std::io;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

use hammer_adapter::{
    ComponentMetadata, DnsRouter as DnsRouterTrait, Inbound,
    OutboundManager as OutboundManagerTrait, PlatformInterface, Router as RouterTrait, TunOptions,
};
use hammer_core::config::{InboundKind, TunInboundOptions, TunStack};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::Logger;
use hammer_core::metrics::MetricsRegistry;

use super::stack::*;

use crate::{DnsRouter, OutboundManager, Router};

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
type PlatformTunDevice = crate::apple_utun::AppleTunDevice;

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
type PlatformTunDevice = AsyncTunDevice;

pub(crate) type RuntimeTunInbound = TunInbound<Router, DnsRouter, OutboundManager>;

#[hammer_component_macros::hammer_component(
    inbound,
    name = "tun",
    builder = build_inbound,
    runtime = RuntimeTunInbound,
    metrics = ("inbound", "tun")
)]
pub struct TunInbound<R, Q, O>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    id: String,
    logger: Logger,
    options: TunInboundOptions,
    router: Arc<R>,
    dns_router: Option<Arc<Q>>,
    outbound: Option<Arc<O>>,
    platform: Option<Arc<dyn PlatformInterface>>,
    metrics: Arc<MetricsRegistry>,
    tun_fd: Mutex<Option<i32>>,
    system_stack: Mutex<Option<Arc<SystemTunStack<PlatformTunDevice, R, Q, O>>>>,
}

impl<R, Q, O> TunInbound<R, Q, O>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    pub fn new(
        id: impl Into<String>,
        logger: Logger,
        options: TunInboundOptions,
        router: Arc<R>,
    ) -> Self {
        Self {
            id: id.into(),
            logger,
            options,
            router,
            dns_router: None,
            outbound: None,
            platform: None,
            metrics: MetricsRegistry::new(),
            tun_fd: Mutex::new(None),
            system_stack: Mutex::new(None),
        }
    }

    pub fn new_with_runtime(
        id: impl Into<String>,
        logger: Logger,
        options: TunInboundOptions,
        router: Arc<R>,
        dns_router: Arc<Q>,
        outbound: Arc<O>,
        platform: Arc<dyn PlatformInterface>,
    ) -> Self {
        Self::new_with_runtime_and_metrics(
            id,
            logger,
            options,
            router,
            dns_router,
            outbound,
            platform,
            MetricsRegistry::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_runtime_and_metrics(
        id: impl Into<String>,
        logger: Logger,
        options: TunInboundOptions,
        router: Arc<R>,
        dns_router: Arc<Q>,
        outbound: Arc<O>,
        platform: Arc<dyn PlatformInterface>,
        metrics: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            id: id.into(),
            logger,
            options,
            router,
            dns_router: Some(dns_router),
            outbound: Some(outbound),
            platform: Some(platform),
            metrics,
            tun_fd: Mutex::new(None),
            system_stack: Mutex::new(None),
        }
    }

    pub fn mtu(&self) -> u32 {
        self.options.mtu
    }

    fn open_tun(&self) -> HammerResult<()> {
        if self
            .tun_fd
            .lock()
            .expect("TunInbound fd poisoned")
            .is_some()
        {
            return Ok(());
        }

        let Some(platform) = &self.platform else {
            return Ok(());
        };

        let options = self.platform_options()?;
        info!("opening TUN {}, mtu {}", options.name, options.mtu);
        let fd = platform.open_tun(options)?;
        let stack = self.build_system_stack(fd)?;
        *self.tun_fd.lock().expect("TunInbound fd poisoned") = Some(fd);
        *self.system_stack.lock().expect("TunInbound stack poisoned") = stack;
        info!("opened TUN fd {fd}");
        Ok(())
    }

    fn build_system_stack(
        &self,
        fd: i32,
    ) -> HammerResult<Option<Arc<SystemTunStack<PlatformTunDevice, R, Q, O>>>> {
        match self.options.stack {
            TunStack::Disabled => {
                debug!("skip disabled TUN data path");
                return Ok(None);
            }
            TunStack::System => {}
        }
        let (Some(dns_router), Some(outbound)) = (&self.dns_router, &self.outbound) else {
            return Ok(None);
        };
        let dup_fd = duplicate_fd(fd)?;
        let mtu = usize::try_from(self.options.mtu)
            .map_err(|_| HammerError::internal("TUN MTU does not fit in usize"))?;
        let tun_interface_index = tun_interface_index_from_fd(fd);
        if let Some(index) = tun_interface_index {
            info!("TUN interface index {index}");
        } else {
            debug!("TUN interface index unavailable; listener will not bind to TUN");
        }
        let device = unsafe { open_system_tun_device(dup_fd, mtu)? };
        Ok(Some(Arc::new(SystemTunStack::new_with_interface_index(
            self.logger.clone(),
            Arc::clone(&self.router),
            Arc::clone(dns_router),
            Arc::clone(outbound),
            self.id.clone(),
            self.options.clone(),
            device,
            tun_interface_index,
            {
                let meta = self.component_meta();
                let metrics = meta
                    .metrics()
                    .expect("TunInbound component macro must declare metrics");
                self.metrics
                    .scope(metrics.module, metrics.component_type, meta.id().to_owned())
            },
        ))))
    }

    fn start_system_stack(&self) -> HammerResult<()> {
        let stack = self
            .system_stack
            .lock()
            .expect("TunInbound stack poisoned")
            .clone();
        if let Some(stack) = stack {
            stack.start()?;
        }
        Ok(())
    }

    fn platform_options(&self) -> HammerResult<TunOptions> {
        let mtu = i32::try_from(self.options.mtu)
            .map_err(|_| HammerError::internal("TUN MTU does not fit in i32"))?;
        let name = if self.options.interface_name.is_empty() {
            self.id.clone()
        } else {
            self.options.interface_name.clone()
        };
        Ok(TunOptions {
            name,
            mtu,
            address: self
                .options
                .address
                .iter()
                .map(ToString::to_string)
                .collect(),
            route: self
                .options
                .route_address
                .iter()
                .map(ToString::to_string)
                .collect(),
            route_exclude: self
                .options
                .route_exclude_address
                .iter()
                .map(ToString::to_string)
                .collect(),
            auto_route: self.options.auto_route,
            strict_route: self.options.strict_route,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_inbound(
    id: String,
    logger: Logger,
    kind: &InboundKind,
    router: Arc<Router>,
    dns_router: Option<Arc<DnsRouter>>,
    outbound: Option<Arc<OutboundManager>>,
    platform: Option<Arc<dyn PlatformInterface>>,
    metrics: Arc<MetricsRegistry>,
) -> HammerResult<Arc<RuntimeTunInbound>> {
    match kind {
        InboundKind::Tun(options) => {
            let inbound: Arc<RuntimeTunInbound> = match (dns_router, outbound, platform) {
                (Some(dns_router), Some(outbound), Some(platform)) => {
                    Arc::new(RuntimeTunInbound::new_with_runtime_and_metrics(
                        id,
                        logger,
                        options.clone(),
                        router,
                        dns_router,
                        outbound,
                        platform,
                        metrics,
                    ))
                }
                _ => Arc::new(RuntimeTunInbound::new(id, logger, options.clone(), router)),
            };
            Ok(inbound)
        }
    }
}

impl<R, Q, O> Lifecycle for TunInbound<R, Q, O>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
    fn name(&self) -> &str {
        "inbound"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        if matches!(stage, StartStage::Start) {
            self.open_tun()?;
        }
        if matches!(stage, StartStage::PostStart) {
            self.start_system_stack()?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        if let Some(stack) = self
            .system_stack
            .lock()
            .expect("TunInbound stack poisoned")
            .take()
        {
            stack.close();
        }
        *self.tun_fd.lock().expect("TunInbound fd poisoned") = None;
        debug!("close");
        Ok(())
    }
}

fn duplicate_fd(fd: i32) -> HammerResult<i32> {
    let dup_fd = unsafe { libc::dup(fd) };
    if dup_fd < 0 {
        return Err(HammerError::internal(format!(
            "duplicate TUN fd: {}",
            io::Error::last_os_error()
        )));
    }
    Ok(dup_fd)
}

/// # Safety
/// `fd` must be an exclusively-owned utun file descriptor; the returned device
/// closes it on drop.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
unsafe fn open_system_tun_device(fd: i32, mtu: usize) -> HammerResult<Arc<PlatformTunDevice>> {
    let device = unsafe { crate::apple_utun::AppleTunDevice::from_fd(fd, mtu)? };
    Ok(device)
}

/// # Safety
/// `fd` must be an exclusively-owned TUN file descriptor; the returned device
/// closes it on drop.
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
unsafe fn open_system_tun_device(fd: i32, mtu: usize) -> HammerResult<Arc<PlatformTunDevice>> {
    let device = unsafe { AsyncTunDevice::from_fd(fd, mtu)? };
    Ok(device)
}

impl<R, Q, O> Inbound for TunInbound<R, Q, O>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
{
}
