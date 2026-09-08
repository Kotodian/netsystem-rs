use thiserror::Error;

use hammer_infra::physmem::PhysmemError;

/// Buffer-state failures representable by the packet-graph ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BufferInvariant {
    #[error("buffer bytes {length} exceed slot capacity {capacity}")]
    BytesExceedCapacity { length: usize, capacity: usize },
    #[error("buffer headroom exceeds slot capacity")]
    HeadroomExceedsCapacity,
    #[error("buffer truncate extends current length")]
    TruncateExtendsCurrentLength,
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
    #[error("Buffer allocation size overflows for data size {data_size}")]
    BufferAllocationSizeOverflow { data_size: usize },
    #[error("Buffer allocation {allocation_size} exceeds backing page size {page_size}")]
    BufferAllocationExceedsPage {
        allocation_size: usize,
        page_size: usize,
    },
    #[error("requested {requested} Buffer Pools exceeds maximum {maximum}")]
    BufferPoolCountExceeded { requested: usize, maximum: usize },
    #[error("Buffer address span {bytes} exceeds maximum {maximum}")]
    BufferMemorySpanExceeded { bytes: usize, maximum: usize },
    #[error("no usable Buffer Pools were constructed")]
    BufferPoolsUnavailable,
    #[error("failed to create Buffer Pool mapping on NUMA node {numa_node}")]
    BufferPoolMapping {
        numa_node: u32,
        #[source]
        source: PhysmemError,
    },
    #[error(transparent)]
    BufferInvariant(#[from] BufferInvariant),
    #[error("buffer frame capacity exceeded")]
    FrameCapacityExceeded,
    #[error("data plane handoff target worker out of bounds")]
    HandoffTargetWorkerOutOfBounds,
    #[error("data plane handoff queue exhausted")]
    HandoffQueueExhausted,
    #[error("data plane handoff is not configured")]
    HandoffNotConfigured,
    #[error("named next fallback node is not registered")]
    NamedNextFallbackMissing,
    #[error("duplicate node function for `{node}` at SIMD width {simd_bytes} bytes")]
    DuplicateNodeFunction {
        node: &'static str,
        simd_bytes: usize,
    },
    #[error("constructor-published graph registration is unnamed")]
    UnnamedGraphRegistration,
    #[error("NUMA node {numa_node} exceeds static memory table capacity {capacity}")]
    NumaNodeExceedsStaticMemoryTable { numa_node: u32, capacity: usize },
}

pub type DataPlaneResult<T> = Result<T, DataPlaneError>;
