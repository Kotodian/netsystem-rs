use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::HammerError;
use hammer_infra::vec::Vec;

/// Typed service registry — Rust counterpart of Go's `service.ContextWith[T]`
/// / `service.FromContext[T]`. Each manager registers itself by its concrete
/// struct type; consumers `require::<T>()` to fetch a clone-able `Arc<T>`.
///
/// We deliberately key by *concrete* `TypeId` rather than by `dyn Trait`
/// because `TypeId::of::<T>` requires `T: Sized`; trait abstractions can wrap
/// the concrete type at a higher layer when tests need them.
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

    pub fn require<T: Any + Send + Sync>(&self) -> Result<Arc<T>, HammerError> {
        self.get::<T>().ok_or_else(|| {
            HammerError::internal(format!(
                "required service not registered in RuntimeRegistry: {}",
                type_name::<T>()
            ))
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
        let reg = RuntimeRegistry::new();
        let m = Arc::new(CountingManager::new());
        reg.set(Arc::clone(&m));
        let pulled = reg.require::<CountingManager>().unwrap();
        pulled.bump();
        m.bump();
        assert_eq!(m.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn require_missing_service_yields_error() {
        let reg = RuntimeRegistry::new();
        let err = reg.require::<CountingManager>().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("required service not registered"),
            "got = {msg}"
        );
        assert!(msg.contains("CountingManager"), "got = {msg}");
    }

    #[test]
    fn concurrent_set_and_get() {
        let reg = RuntimeRegistry::new();
        reg.set(Arc::new(CountingManager::new()));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let r = Arc::clone(&reg);
            handles.push(thread::spawn(move || {
                let m = r.require::<CountingManager>().unwrap();
                for _ in 0..100 {
                    m.bump();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let m = reg.require::<CountingManager>().unwrap();
        assert_eq!(m.0.load(std::sync::atomic::Ordering::SeqCst), 1600);
    }
}
