use hammer_adapter::HttpClientManager as HttpClientManagerTrait;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

/// `hammer.httpClientManager` placeholder. M3 wires the actual hyper-based
/// transport once HTTPS DNS needs it.
pub struct HttpClientManager {
    logger: Logger,
}

impl HttpClientManager {
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl_logging_lifecycle!(HttpClientManager, "http-client");

impl HttpClientManagerTrait for HttpClientManager {
    fn reset_network(&self) {
        self.logger.debug("reset_network (M2 stub)");
    }
}
