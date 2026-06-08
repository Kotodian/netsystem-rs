mod backend;
mod ingress;
mod registry;

use std::any::Any;

use hammer_core::Lifecycle;

pub use backend::AppIngressBackend;
pub use ingress::AppIngressTarget;
pub(crate) use registry::AppIngressRegistry;

pub trait AppHost: Lifecycle + Any + Send + Sync {}

impl<T> AppHost for T where T: Lifecycle + Any + Send + Sync {}
