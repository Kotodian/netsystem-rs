use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::{Lifecycle, LifecycleService};

/// `adapter.CertificateStore` — system + user-supplied trust roots. M2 ships
/// a placeholder that exposes an empty pool; M3+ gradually wires up the real
/// rustls-backed store.
pub trait CertificateStore: LifecycleService {
    fn exclusive_anchors(&self) -> bool;
}

/// `adapter.CertificateProviderService` — trait the providers (ACME, file,
/// etc.) implement; full method set lands when M5+ needs server-side TLS.
pub trait CertificateProviderService: Lifecycle {
    fn type_name(&self) -> &str;
    fn tag(&self) -> &str;
}

pub trait CertificateProviderManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn CertificateProviderService>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn CertificateProviderService>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}
