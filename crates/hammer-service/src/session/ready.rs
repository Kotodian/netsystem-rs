use crate::session::AppSessionId;

pub struct AppSessionReadyQueue {
    ready: hammer_infra::vec::Vec<AppSessionId>,
    slots: hammer_infra::map::FlatHashTable<u64, usize>,
}

impl AppSessionReadyQueue {
    #[inline]
    pub fn new() -> Self {
        Self {
            ready: hammer_infra::vec::Vec::new(),
            slots: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn mark_ready(&mut self, session_id: AppSessionId) {
        if self.slots.lookup(&session_id.get()).is_some() {
            return;
        }
        let slot = self.ready.len();
        self.ready.push(session_id);
        self.slots.insert(session_id.get(), slot);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ready.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    pub fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<AppSessionId> {
        let ready = self.ready.iter().copied().collect();
        self.ready.clear();
        self.slots = hammer_infra::map::FlatHashTable::new();
        ready
    }
}
