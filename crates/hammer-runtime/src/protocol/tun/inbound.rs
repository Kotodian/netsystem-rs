use std::io;
#[cfg(not(feature = "endpoint"))]
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

#[cfg(feature = "endpoint")]
use hammer_adapter::Endpoint as EndpointTrait;
use hammer_adapter::{
    ComponentMetadata, DnsRouter as DnsRouterTrait, EndpointManager as EndpointManagerTrait,
    Inbound, OutboundManager as OutboundManagerTrait, PlatformInterface, Router as RouterTrait,
    TunOptions,
};
use hammer_core::config::{InboundKind, TunInboundOptions, TunStack};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::Logger;
use hammer_core::metrics::MetricsRegistry;

use super::stack::*;

use crate::inbounds::RuntimeDnsRouter;
use crate::{EndpointManager, OutboundManager, Router};

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]
type PlatformTunDevice = crate::apple_utun::AppleTunDevice;

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos")))]
type PlatformTunDevice = AsyncTunDevice;

pub(crate) type RuntimeTunInbound =
    TunInbound<Router, RuntimeDnsRouter, OutboundManager, EndpointManager>;

#[hammer_component_macros::hammer_component(
    inbound,
    name = "tun",
    builder = build_inbound,
    runtime = RuntimeTunInbound,
    metrics = ("inbound", "tun")
)]
pub struct TunInbound<R, Q, O, E>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
    E: EndpointManagerTrait + 'static,
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
    /// L3 endpoints registered for the TUN fast path. Populated by the
    /// service builder via `set_endpoint_manager` before `start`. The
    /// generic `E` matches the rest of the manager-injection style on this
    /// component (`R / Q / O`) — `RuntimeTunInbound` fixes it to
    /// `EndpointManager`, monomorphizing the dispatch.
    #[cfg(feature = "endpoint")]
    endpoint_manager: Mutex<Option<Arc<E>>>,
    /// When the `endpoint` feature is off there's no endpoint manager to
    /// store, but the generic `E` is still on the struct so callers don't
    /// have to flip type parameters on a feature switch. `PhantomData`
    /// keeps the compiler happy without consuming any space.
    #[cfg(not(feature = "endpoint"))]
    _endpoint_marker: PhantomData<E>,
}

impl<R, Q, O, E> TunInbound<R, Q, O, E>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
    E: EndpointManagerTrait + 'static,
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
            #[cfg(feature = "endpoint")]
            endpoint_manager: Mutex::new(None),
            #[cfg(not(feature = "endpoint"))]
            _endpoint_marker: PhantomData,
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
            #[cfg(feature = "endpoint")]
            endpoint_manager: Mutex::new(None),
            #[cfg(not(feature = "endpoint"))]
            _endpoint_marker: PhantomData,
        }
    }

    pub fn mtu(&self) -> u32 {
        self.options.mtu
    }

    /// Reports whether this TUN inbound runs the system stack (and would
    /// therefore drain `Endpoint::ip_recv_take`). Used by the runtime
    /// service to detect the unsupported "multiple system TUNs share one
    /// endpoint" configuration without reaching into private fields.
    #[cfg(feature = "endpoint")]
    pub fn uses_system_stack(&self) -> bool {
        matches!(self.options.stack, TunStack::System)
    }

    /// Inject the L3 endpoint manager. The TUN packet loop consults the
    /// manager's `list()` at stack-start time to build its L3 fast path
    /// (`L3DispatchTable`). Must be called before `start` to take effect.
    #[cfg(feature = "endpoint")]
    pub fn set_endpoint_manager(&self, manager: Arc<E>) {
        *self
            .endpoint_manager
            .lock()
            .expect("TunInbound endpoint_manager poisoned") = Some(manager);
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
        let system_stack = self.build_system_stack(fd)?;
        *self.tun_fd.lock().expect("TunInbound fd poisoned") = Some(fd);
        *self.system_stack.lock().expect("TunInbound stack poisoned") = system_stack;
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
        let stack = SystemTunStack::new_with_interface_index(
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
        );
        // Wire in any L3 endpoints registered through the service builder.
        // `set_endpoints` is a no-op on an empty list, and the dispatch table
        // is built lazily inside `SystemTunStack::start`, so calling here
        // before the stack is started is the correct sequencing.
        #[cfg(feature = "endpoint")]
        if let Some(manager) = self
            .endpoint_manager
            .lock()
            .expect("TunInbound endpoint_manager poisoned")
            .as_ref()
        {
            let endpoints: Vec<Arc<dyn EndpointTrait>> = manager
                .list()
                .into_iter()
                .map(|comp| Arc::clone(comp.runtime()))
                .collect();
            if !endpoints.is_empty() {
                stack.set_endpoints(endpoints);
            }
        }
        Ok(Some(Arc::new(stack)))
    }

    fn start_data_stack(&self) -> HammerResult<()> {
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
    dns_router: Option<Arc<RuntimeDnsRouter>>,
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
        _ => Err(HammerError::internal("tun factory received wrong options")),
    }
}

impl<R, Q, O, E> Lifecycle for TunInbound<R, Q, O, E>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
    E: EndpointManagerTrait + 'static,
{
    fn name(&self) -> &str {
        "inbound"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        if matches!(stage, StartStage::Start) {
            self.open_tun()?;
        }
        if matches!(stage, StartStage::PostStart) {
            self.start_data_stack()?;
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

impl<R, Q, O, E> Inbound for TunInbound<R, Q, O, E>
where
    R: RouterTrait + 'static,
    Q: DnsRouterTrait + 'static,
    O: OutboundManagerTrait + 'static,
    E: EndpointManagerTrait + 'static,
{
}
