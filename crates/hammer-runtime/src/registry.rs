use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::RuntimeError;

/// Runtime-owned typed service registry.
///
/// Each manager registers itself by its concrete struct type; consumers use
/// `require::<T>()` to obtain a cloneable `Arc<T>`.
pub struct RuntimeRegistry {
    inner: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl RuntimeRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
        })
    }

    pub fn set<T: Any + Send + Sync>(&self, value: Arc<T>) {
        self.inner
            .write()
            .expect("RuntimeRegistry poisoned")
            .insert(TypeId::of::<T>(), value);
    }

    pub fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.inner
            .read()
            .expect("RuntimeRegistry poisoned")
            .get(&TypeId::of::<T>())
            .map(Arc::clone)
            .and_then(|any| any.downcast::<T>().ok())
    }

    pub fn require<T: Any + Send + Sync>(&self) -> Result<Arc<T>, RuntimeError> {
        self.get::<T>()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: type_name::<T>(),
            })
    }
}

impl Default for RuntimeRegistry {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}
