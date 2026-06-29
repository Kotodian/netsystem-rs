use std::sync::Arc;

use hammer_core::error::CoreResult;

use crate::CertificateProviderService;

/// `adapter.ServiceManager` — extra lifecycle services registered alongside
/// the core runtime managers.
pub trait ServiceManager {
    fn list(&self) -> Vec<Arc<dyn CertificateProviderService>>;
    fn get(&self, id: &str) -> Option<Arc<dyn CertificateProviderService>>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
