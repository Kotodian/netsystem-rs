use crate::StatsResult;
use crate::protocol::{DirectoryIndex, RingConfig};
use std::marker::PhantomData;

/// A registered gauge directory entry.
pub struct Gauge {
    pub index: DirectoryIndex,
}

/// A registered scalar timestamp directory entry.
pub struct Timestamp {
    pub index: DirectoryIndex,
}

/// A registered simple counter vector directory entry.
pub struct SimpleCounter {
    pub index: DirectoryIndex,
}

/// A registered combined counter vector directory entry.
pub struct CombinedCounter {
    pub index: DirectoryIndex,
}

/// A registered log2 histogram directory entry.
pub struct Histogram {
    pub index: DirectoryIndex,
}

/// A registered name vector directory entry.
pub struct NameVector {
    pub index: DirectoryIndex,
}

/// The declaration input for a stats ring buffer.
pub struct Ring<T> {
    name: String,
    config: RingConfig,
    schema: Box<[u8]>,
    marker: PhantomData<fn() -> T>,
}

impl<T> Ring<T> {
    pub fn new(name: impl Into<String>, config: RingConfig, schema: Box<[u8]>) -> Self {
        Self {
            name: name.into(),
            config,
            schema,
            marker: PhantomData,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn config(&self) -> RingConfig {
        self.config
    }

    pub(crate) fn schema(&self) -> &[u8] {
        &self.schema
    }
}

/// One typed ring entry codec.
pub trait RingSchema: Sized {
    const ENTRY_SIZE: u32;
    const SCHEMA_VERSION: u32;

    fn schema() -> &'static [u8];
    fn encode(&self, destination: &mut [u8]) -> StatsResult<()>;
    fn decode(source: &[u8]) -> StatsResult<Self>;
}
