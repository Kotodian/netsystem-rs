use hammer_infra::fifo_queue::FifoQueue;

use crate::session::SessionId;

const DEFAULT_READY_QUEUE_CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct SessionReadyQueue {
    ready: FifoQueue<SessionId>,
    queued: hammer_infra::map::FlatHashTable<u64, ()>,
}

impl SessionReadyQueue {
    #[inline]
    pub fn new() -> Self {
        Self {
            ready: FifoQueue::with_capacity(DEFAULT_READY_QUEUE_CAPACITY),
            queued: hammer_infra::map::FlatHashTable::with_capacity(DEFAULT_READY_QUEUE_CAPACITY),
        }
    }

    pub fn mark_ready(&mut self, session_id: SessionId) {
        if self.queued.lookup(&session_id.get()).is_some() {
            return;
        }
        self.ready.push_back(session_id);
        self.queued.insert(session_id.get(), ());
    }

    #[inline]
    pub fn take(&mut self, session_id: SessionId) -> bool {
        self.queued.remove(&session_id.get()).is_some()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.queued.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    pub fn pop_front(&mut self) -> Option<SessionId> {
        while let Some(session_id) = self.ready.pop_front() {
            if self.queued.remove(&session_id.get()).is_some() {
                return Some(session_id);
            }
        }
        None
    }

    pub fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        let mut ready = hammer_infra::vec::Vec::with_capacity(self.len());
        while let Some(session_id) = self.pop_front() {
            ready.push(session_id);
        }
        ready
    }
}
