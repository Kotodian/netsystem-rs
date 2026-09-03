use std::fmt;
use std::sync::Arc;

use super::FibTable;

#[derive(Clone)]
pub struct FibTableHandle {
    inner: Arc<FibTable>,
}

impl FibTableHandle {
    #[inline]
    pub fn new(table: FibTable) -> Self {
        Self {
            inner: Arc::new(table),
        }
    }

    #[inline]
    pub fn table(&self) -> &FibTable {
        &self.inner
    }
}

impl fmt::Debug for FibTableHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FibTableHandle").finish_non_exhaustive()
    }
}
