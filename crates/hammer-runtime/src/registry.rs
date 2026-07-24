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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[derive(Debug)]
    struct CountingManager(std::sync::atomic::AtomicUsize);

    impl CountingManager {
        fn new() -> Self {
            Self(std::sync::atomic::AtomicUsize::new(0))
        }

        fn bump(&self) -> usize {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[test]
    fn set_and_require_round_trip() {
        let registry = RuntimeRegistry::new();
        let manager = Arc::new(CountingManager::new());
        registry.set(Arc::clone(&manager));
        let pulled = registry.require::<CountingManager>().unwrap();
        pulled.bump();
        manager.bump();
        assert_eq!(manager.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn require_missing_service_yields_error() {
        let registry = RuntimeRegistry::new();
        let error = registry.require::<CountingManager>().unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("required service not registered"),
            "got = {message}"
        );
        assert!(message.contains("CountingManager"), "got = {message}");
    }

    #[test]
    fn concurrent_set_and_get() {
        let registry = RuntimeRegistry::new();
        registry.set(Arc::new(CountingManager::new()));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let registry = Arc::clone(&registry);
            handles.push(thread::spawn(move || {
                let manager = registry.require::<CountingManager>().unwrap();
                for _ in 0..100 {
                    manager.bump();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let manager = registry.require::<CountingManager>().unwrap();
        assert_eq!(manager.0.load(std::sync::atomic::Ordering::SeqCst), 1600);
    }
}
