use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::{Lifecycle, LifecycleService};

/// System + user-supplied trust roots exposed to TLS users.
pub trait CertificateStore: LifecycleService {
    fn exclusive_anchors(&self) -> bool;
}

/// Trait implemented by certificate providers such as ACME or file providers.
pub trait CertificateProviderService: Lifecycle {
    fn type_name(&self) -> &str;
    fn id(&self) -> &str;
}

pub trait CertificateProviderManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn CertificateProviderService>>;
    fn get(&self, id: &str) -> Option<Arc<dyn CertificateProviderService>>;
    fn remove(&self, id: &str) -> Result<(), CoreError>;
}
