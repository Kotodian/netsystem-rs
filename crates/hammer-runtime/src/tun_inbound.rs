use std::any::Any;
use std::sync::Arc;

use hammer_adapter::Inbound;
use hammer_core::config::TunInboundOptions;
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::Logger;

use crate::tun::SmoltcpTunStack;
use crate::{DnsRouter, OutboundManager, Router};

pub struct TunInbound {
    tag: String,
    logger: Logger,
    options: TunInboundOptions,
    router: Arc<Router>,
    dns_router: Option<Arc<DnsRouter>>,
    outbound: Option<Arc<OutboundManager>>,
}

impl TunInbound {
    pub fn new(
        tag: impl Into<String>,
        logger: Logger,
        options: TunInboundOptions,
        router: Arc<Router>,
    ) -> Self {
        Self {
            tag: tag.into(),
            logger,
            options,
            router,
            dns_router: None,
            outbound: None,
        }
    }

    pub fn new_with_runtime(
        tag: impl Into<String>,
        logger: Logger,
        options: TunInboundOptions,
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
        outbound: Arc<OutboundManager>,
    ) -> Self {
        Self {
            tag: tag.into(),
            logger,
            options,
            router,
            dns_router: Some(dns_router),
            outbound: Some(outbound),
        }
    }

    pub fn stack(&self) -> SmoltcpTunStack {
        match (&self.dns_router, &self.outbound) {
            (Some(dns_router), Some(outbound)) => SmoltcpTunStack::new_with_runtime(
                self.logger.clone(),
                Arc::clone(&self.router),
                Arc::clone(dns_router),
                Arc::clone(outbound),
                self.tag.clone(),
            ),
            _ => SmoltcpTunStack::new(
                self.logger.clone(),
                Arc::clone(&self.router),
                self.tag.clone(),
            ),
        }
    }

    pub fn mtu(&self) -> u32 {
        self.options.mtu
    }
}

impl Lifecycle for TunInbound {
    fn name(&self) -> &str {
        "inbound"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        self.logger.debug(format!("stage {}", stage.name()));
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.logger.debug("close");
        Ok(())
    }
}

impl Inbound for TunInbound {
    fn type_name(&self) -> &str {
        "tun"
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
