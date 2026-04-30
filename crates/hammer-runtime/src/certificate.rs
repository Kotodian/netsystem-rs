use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{
    CertificateProviderManager as CertificateProviderManagerTrait, CertificateProviderService,
    CertificateStore as CertificateStoreTrait,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

/// `common/certificate.Store` placeholder. M3+ wires the real rustls-backed
/// certificate pool + system trust roots ingestion.
pub struct CertificateStore {
    exclusive_anchors: bool,
}

impl CertificateStore {
    pub fn new(_logger: Logger, exclusive_anchors: bool) -> Self {
        Self { exclusive_anchors }
    }
}

impl_logging_lifecycle!(CertificateStore, "certificate-store");

impl CertificateStoreTrait for CertificateStore {
    fn exclusive_anchors(&self) -> bool {
        self.exclusive_anchors
    }
}

pub struct CertificateProviderManager {
    items: Mutex<HashMap<String, Arc<dyn CertificateProviderService>>>,
}

impl CertificateProviderManager {
    pub fn new(_logger: Logger) -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
        }
    }
}

impl_logging_lifecycle!(CertificateProviderManager, "certificate-provider");

impl CertificateProviderManagerTrait for CertificateProviderManager {
    fn list(&self) -> Vec<Arc<dyn CertificateProviderService>> {
        self.items
            .lock()
            .expect("CertificateProviderManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<Arc<dyn CertificateProviderService>> {
        self.items
            .lock()
            .expect("CertificateProviderManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("CertificateProviderManager poisoned")
            .remove(id);
        Ok(())
    }
}
