use std::cell::UnsafeCell;
use std::collections::HashMap;

use hammer_infra::pool::{Pool, PoolIter};

pub struct AppNamespace<B> {
    id: String,
    secret: u64,
    binding: B,
}

impl<B> AppNamespace<B> {
    #[inline(always)]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    pub const fn secret(&self) -> u64 {
        self.secret
    }

    #[inline(always)]
    pub const fn binding(&self) -> &B {
        &self.binding
    }
}

struct AppNamespaceState<B> {
    entries: Pool<AppNamespace<B>>,
    index_by_id: HashMap<String, u32>,
}

pub struct AppNamespaceMain<B> {
    state: UnsafeCell<AppNamespaceState<B>>,
}

// SAFETY: mutation is restricted to the Main Thread during startup or a
// WorkerBarrier interval. Readers borrow only published pool entries.
unsafe impl<B: Send + Sync> Sync for AppNamespaceMain<B> {}

impl<B> Default for AppNamespaceMain<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B> AppNamespaceMain<B> {
    pub fn new() -> Self {
        Self {
            state: UnsafeCell::new(AppNamespaceState {
                entries: Pool::new(),
                index_by_id: HashMap::new(),
            }),
        }
    }

    pub fn insert(&self, id: String, secret: u64, binding: B) -> u32 {
        let state = self.state_mut();
        assert!(
            !state.index_by_id.contains_key(&id),
            "Application Namespace id must be unique"
        );
        let index = state.entries.insert(AppNamespace {
            id: id.clone(),
            secret,
            binding,
        });
        assert!(state.index_by_id.insert(id, index).is_none());
        index
    }

    pub fn replace(&self, index: u32, secret: u64, binding: B) -> B {
        let namespace = self
            .state_mut()
            .entries
            .get_mut(index)
            .expect("replacement index names an Application Namespace");
        namespace.secret = secret;
        std::mem::replace(&mut namespace.binding, binding)
    }

    pub fn remove(&self, id: &str) -> Option<AppNamespace<B>> {
        let state = self.state_mut();
        let index = state.index_by_id.remove(id)?;
        Some(
            state
                .entries
                .remove(index)
                .expect("Application Namespace id index names a live pool entry"),
        )
    }

    #[inline]
    pub fn get(&self, index: u32) -> Option<&AppNamespace<B>> {
        self.state().entries.get(index)
    }

    pub fn find(&self, id: &str) -> Option<(u32, &AppNamespace<B>)> {
        let state = self.state();
        let index = *state.index_by_id.get(id)?;
        let namespace = state
            .entries
            .get(index)
            .expect("Application Namespace id index names a live pool entry");
        Some((index, namespace))
    }

    #[inline]
    pub fn iter(&self) -> PoolIter<'_, AppNamespace<B>> {
        self.state().entries.iter()
    }

    #[inline]
    fn state(&self) -> &AppNamespaceState<B> {
        // SAFETY: publication and mutation obey the owner contract above.
        unsafe { &*self.state.get() }
    }

    #[allow(clippy::mut_from_ref)]
    #[inline]
    fn state_mut(&self) -> &mut AppNamespaceState<B> {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("Application Namespace mutation requires the Main Thread and WorkerBarrier");
        // SAFETY: the runtime check excludes every Data Worker reader.
        unsafe { &mut *self.state.get() }
    }
}
