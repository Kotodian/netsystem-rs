mod service;

pub mod adapter {
    pub use hammer_runtime::adapter::*;
}

pub use hammer_core::error::{HammerError, HammerResult};
pub use hammer_runtime::{RuntimePlatform, install_default_crypto_provider};
pub use service::RuntimeService;
