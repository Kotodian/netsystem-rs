use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;

use super::FibTable;

#[derive(Clone)]
pub struct FibTableHandle {
    inner: Arc<FibTableSlot>,
}

struct FibTableSlot {
    table: UnsafeCell<FibTable>,
}

impl FibTableHandle {
    #[inline]
    pub fn new(table: FibTable) -> Self {
        Self {
            inner: Arc::new(FibTableSlot::new(table)),
        }
    }

    #[inline]
    pub fn table(&self) -> &FibTable {
        self.inner.table()
    }

    #[inline]
    pub fn publish(&self, table: FibTable) {
        self.inner.publish(table);
    }
}

impl fmt::Debug for FibTableHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FibTableHandle").finish_non_exhaustive()
    }
}

impl FibTableSlot {
    #[inline]
    fn new(table: FibTable) -> Self {
        Self {
            table: UnsafeCell::new(table),
        }
    }

    #[inline]
    fn table(&self) -> &FibTable {
        // SAFETY: FIB table writes are serialized by the runtime data-plane
        // barrier before publication. Data-plane nodes only take immutable
        // references while workers are running.
        unsafe { &*self.table.get() }
    }

    #[inline]
    fn publish(&self, table: FibTable) {
        // SAFETY: callers replace the table either while the runtime
        // data-plane barrier is held, or during single-threaded graph setup in
        // tests before packets are processed.
        unsafe {
            *self.table.get() = table;
        }
    }
}

// SAFETY: FibTableSlot mutation is restricted to publish; packet
// workers only read immutable snapshots while the barrier is released.
unsafe impl Send for FibTableSlot {}
// SAFETY: See the Send implementation. Concurrent access is read-only outside
// the data-plane barrier.
unsafe impl Sync for FibTableSlot {}
