use crate::session::SessionId;

const DEFAULT_READY_QUEUE_CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct SessionReadyQueue {
    ready: hammer_infra::vec::Vec<SessionId>,
    slots: hammer_infra::map::FlatHashTable<u64, usize>,
}

impl SessionReadyQueue {
    #[inline]
    pub fn new() -> Self {
        Self {
            ready: hammer_infra::vec::Vec::with_capacity(DEFAULT_READY_QUEUE_CAPACITY),
            slots: hammer_infra::map::FlatHashTable::with_capacity(DEFAULT_READY_QUEUE_CAPACITY),
        }
    }

    pub fn mark_ready(&mut self, session_id: SessionId) {
        if self.slots.lookup(&session_id.get()).is_some() {
            return;
        }
        let slot = self.ready.len();
        self.ready.push(session_id);
        self.slots.insert(session_id.get(), slot);
    }

    pub fn take(&mut self, session_id: SessionId) -> bool {
        let Some(slot) = self.slots.remove(&session_id.get()) else {
            return false;
        };
        let last = self.ready.len() - 1;
        self.ready.swap(slot, last);
        let removed = self.ready.pop().expect("ready slot must exist");
        if slot != last {
            let moved = self.ready[slot];
            self.slots.insert(moved.get(), slot);
        }
        debug_assert_eq!(removed, session_id);
        true
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ready.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    pub fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        let ready = self.ready.iter().copied().collect();
        self.ready.clear();
        self.slots.clear();
        ready
    }
}
