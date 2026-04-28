use hammer_adapter::Router as RouterTrait;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

/// `route.Router` skeleton. The rule engine + connection routing hot path
/// arrive in M4.
pub struct Router {
    logger: Logger,
}

impl Router {
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl_logging_lifecycle!(Router, "router");

impl RouterTrait for Router {
    fn reset_network(&self) {
        self.logger.debug("reset_network (M2 stub)");
    }
}
