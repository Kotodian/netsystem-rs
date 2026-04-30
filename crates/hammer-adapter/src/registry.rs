use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::RwLock;

use hammer_core::error::CoreError;

/// Generic Registry template that mirrors Go's per-domain registries
/// (out/in/endpoint/dns transport/service/certificate). A concrete registry
/// provides:
///   - `Options` — owned options struct deserialized from TOML
///   - `Output` — produced trait object (e.g. `Arc<dyn Outbound>`)
///   - `register::<O: Into<Options>>(name, ctor)` — wires a concrete protocol
/// then the manager calls `create(ctx, id, type_name, options)` at config
/// time. M2 ships the framework + signatures; concrete protocol registrations
/// land alongside their implementations in M3-M7.
pub trait Constructor<Output>: Send + Sync + 'static {
    fn create(
        &self,
        ctx: &RegistryContext,
        id: &str,
        options: Box<dyn Any + Send>,
    ) -> Result<Output, CoreError>;
}

/// Loosely-typed bag of services passed into constructors. M2 keeps it empty
/// because no protocol is registered yet; M3+ extend with the manager handles
/// each protocol needs (router, logger factory, dns router, …).
pub struct RegistryContext;

pub struct Registry<Output: 'static> {
    constructors: RwLock<HashMap<String, Box<dyn Constructor<Output>>>>,
    _marker: PhantomData<Output>,
}

impl<Output> Registry<Output> {
    pub fn new() -> Self {
        Self {
            constructors: RwLock::new(HashMap::new()),
            _marker: PhantomData,
        }
    }

    pub fn register(&self, type_name: impl Into<String>, ctor: Box<dyn Constructor<Output>>) {
        self.constructors
            .write()
            .expect("Registry lock poisoned")
            .insert(type_name.into(), ctor);
    }

    pub fn create(
        &self,
        ctx: &RegistryContext,
        id: &str,
        type_name: &str,
        options: Box<dyn Any + Send>,
    ) -> Result<Output, CoreError> {
        let guard = self.constructors.read().expect("Registry lock poisoned");
        let ctor = guard
            .get(type_name)
            .ok_or_else(|| CoreError::config_validation(format!("unknown type: {type_name}")))?;
        ctor.create(ctx, id, options)
    }
}

impl<Output> Default for Registry<Output> {
    fn default() -> Self {
        Self::new()
    }
}
