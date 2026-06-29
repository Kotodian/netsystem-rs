use std::sync::Arc;

use hammer_core::error::CoreResult;

/// System + user-supplied trust roots exposed to TLS users.
pub trait CertificateStore {
    fn exclusive_anchors(&self) -> bool;
}

/// Trait implemented by certificate providers such as ACME or file providers.
pub trait CertificateProviderService {
    fn id(&self) -> &str;
}

pub trait CertificateProviderManager {
    fn list(&self) -> Vec<Arc<dyn CertificateProviderService>>;
    fn get(&self, id: &str) -> Option<Arc<dyn CertificateProviderService>>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
