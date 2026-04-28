use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_adapter::ConnectionManager as ConnectionManagerTrait;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

pub struct ConnectionManager {
    logger: Logger,
    count: AtomicUsize,
}

impl ConnectionManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            count: AtomicUsize::new(0),
        }
    }
}

impl_logging_lifecycle!(ConnectionManager, "connection");

impl ConnectionManagerTrait for ConnectionManager {
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn close_all(&self) {
        self.count.store(0, Ordering::SeqCst);
        self.logger.debug("close_all (M2 stub)");
    }
}
