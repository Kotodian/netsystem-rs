use hammer_infra::map::{FlatHashKey, FlatHashTable};

use crate::app::AppIngressTarget;

#[derive(Clone, Debug, Default)]
pub struct AppIngressRegistry<K: FlatHashKey> {
    slots: FlatHashTable<K, u32>,
    targets: hammer_infra::vec::Vec<AppIngressTarget>,
}

impl<K: FlatHashKey> AppIngressRegistry<K> {
    #[inline]
    pub fn new() -> Self {
        Self {
            slots: FlatHashTable::new(),
            targets: hammer_infra::vec::Vec::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, key: K, target: AppIngressTarget) {
        let slot = self.targets.len() as u32;
        if let Some(existing) = self.slots.lookup(&key) {
            self.targets[existing as usize] = target;
            return;
        }
        self.targets.push(target);
        self.slots.insert(key, slot);
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<&AppIngressTarget> {
        let slot = self.slots.lookup(key)? as usize;
        self.targets.get(slot)
    }
}
