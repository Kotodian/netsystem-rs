use thiserror::Error;

use hammer_infra::physmem::PhysmemError;

/// Buffer-state failures representable by the packet-graph ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BufferInvariant {
    #[error("buffer bytes {length} exceed slot capacity {capacity}")]
    BytesExceedCapacity { length: usize, capacity: usize },
    #[error("buffer headroom exceeds slot capacity")]
    HeadroomExceedsCapacity,
    #[error("buffer commit exceeds writable tail")]
    CommitExceedsWritableTail,
    #[error("buffer truncate extends current length")]
    TruncateExtendsCurrentLength,
    #[error("buffer rewind exceeds headroom")]
    RewindExceedsHeadroom,
    #[error("buffer advance displacement is out of range")]
    AdvanceDisplacementOutOfRange,
    #[error("buffer advance exceeds current length")]
    AdvanceExceedsCurrentLength,
    #[error("buffer prepend exceeds headroom")]
    PrependExceedsHeadroom,
    #[error("buffer current_data exceeds pre-data headroom")]
    CurrentDataExceedsPreData,
    #[error("buffer current_data does not fit i16")]
    CurrentDataOutOfRange,
    #[error("buffer current length does not fit u16")]
    CurrentLengthOutOfRange,
    #[error("buffer chain tail length does not fit u32")]
    ChainTailLengthOutOfRange,
    #[error("buffer slot offset overflow")]
    SlotOffsetOverflow,
    #[error("buffer data pointer overflow")]
    DataPointerOverflow,
    #[error("buffer chain length overflow")]
    ChainLengthOverflow,
    #[error("buffer chain advance lost its current segment")]
    ChainAdvanceLostSegment,
    #[error("buffer slot capacity must be nonzero")]
    SlotCapacityZero,
    #[error("buffer pool exhausted")]
    PoolExhausted,
    #[error("shared buffer requires exclusive header ownership")]
    HeaderNotExclusive,
    #[error("buffer attach clone requires distinct head and tail")]
    CloneRequiresDistinctBuffers,
    #[error("buffer attach clone requires a head without next buffer")]
    CloneHeadHasNextBuffer,
    #[error("buffer refcount overflow")]
    RefCountOverflow,
}

#[derive(Debug, Error)]
pub enum DataPlaneError {
    #[error(transparent)]
    BufferInvariant(#[from] BufferInvariant),
    #[error("buffer frame capacity exceeded")]
    BufferFrameCapacityExceeded,
    #[error("buffer arena must contain at least one usable slot")]
    BufferArenaSlotsZero,
    #[error("buffer arena size overflow")]
    BufferArenaSizeOverflow,
    #[error("buffer arena mapping failed")]
    BufferArenaMapping {
        #[source]
        source: PhysmemError,
    },
    #[error("frame pool exhausted")]
    FramePoolExhausted,
    #[error("frame slot is checked out")]
    FrameSlotCheckedOut,
    #[error(
        "index belongs to another pool: expected pool {expected_pool_id}, got pool {actual_pool_id}"
    )]
    ForeignIndex {
        expected_pool_id: u64,
        actual_pool_id: u64,
    },
    #[error(
        "stale index: slot {slot} generation {index_generation} != current {current_generation}"
    )]
    StaleIndex {
        slot: u32,
        index_generation: u32,
        current_generation: u32,
    },
    #[error("index slot {slot} out of bounds for pool {pool_id}")]
    IndexSlotOutOfBounds { pool_id: u64, slot: u32 },
    #[error("index slot {slot} is free in pool {pool_id}")]
    IndexSlotFree { pool_id: u64, slot: u32 },
    #[error("frame slot already has a frame")]
    FrameSlotAlreadyHasFrame,
    #[error("frame pool available-list overflow")]
    FramePoolAvailableOverflow,
    #[error("scheduled frame queue exhausted")]
    ScheduledFrameQueueExhausted,
    #[error("data plane handoff target worker out of bounds")]
    HandoffTargetWorkerOutOfBounds,
    #[error("data plane handoff queue exhausted")]
    HandoffQueueExhausted,
    #[error("data plane handoff is not configured")]
    HandoffNotConfigured,
    #[error("data plane handoff node handle is not configured")]
    HandoffNodeHandleMissing,
    #[error("named next fallback node is not registered")]
    NamedNextFallbackMissing,
    #[error("duplicate node function for `{node}` at SIMD width {simd_bytes} bytes")]
    DuplicateNodeFunction {
        node: &'static str,
        simd_bytes: usize,
    },
    #[error("constructor-published graph registration is unnamed")]
    UnnamedGraphRegistration,
    #[error("active NUMA buffer pool is missing")]
    ActiveNumaBufferPoolMissing,
    #[error("NUMA node {numa_node} does not fit usize")]
    NumaNodeDoesNotFitUsize { numa_node: u32 },
    #[error("NUMA node {numa_node} exceeds static memory table capacity {capacity}")]
    NumaNodeExceedsStaticMemoryTable { numa_node: u32, capacity: usize },
    #[error("duplicate NUMA memory entry for node {numa_node}")]
    DuplicateNumaMemoryEntry { numa_node: u32 },
    #[error("no static buffer arena configured for thread {thread_index} on NUMA node {numa_node}")]
    StaticBufferArenaMissing { thread_index: u32, numa_node: u32 },
}

pub type DataPlaneResult<T> = Result<T, DataPlaneError>;
