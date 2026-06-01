use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, ForwardingDpoType, ForwardingMetadata,
    FrameBatchWidth, InternalNode, Node, NodeId, NodeNextFrames, NodeResult,
};
use hammer_core::error::{CoreResult, HammerResult};
use hammer_core::forwarding::{
    Adjacency as CoreAdjacency, DpoId as CoreDpoId, FibLookupResult as CoreFibLookupResult,
    FibSnapshot as CoreFibSnapshot, FibSnapshotBuilder as CoreFibSnapshotBuilder,
    LoadBalance as CoreLoadBalance,
};
pub use hammer_core::forwarding::{AdjacencyIndex, DpoType, FibEntry, LoadBalanceIndex};
use hammer_core::protocol::ip::parse_ip_packet_with_chain_len;
use hammer_runtime::ControlThreadHandle;

const FORWARDING_MISS_INDEX: u32 = u32::MAX;
const FORWARDING_MISS_BUCKET: u16 = u16::MAX;

pub type Adjacency = CoreAdjacency<NodeId>;
pub type DpoId = CoreDpoId<NodeId>;
pub type FibLookupResult = CoreFibLookupResult<NodeId>;
pub type FibSnapshot = CoreFibSnapshot<NodeId>;
pub type FibSnapshotBuilder = CoreFibSnapshotBuilder<NodeId>;
pub type LoadBalance = CoreLoadBalance<NodeId>;

#[derive(Debug, Clone)]
pub struct FibSnapshotHandle {
    inner: Arc<ArcSwap<FibSnapshot>>,
}

impl FibSnapshotHandle {
    #[inline]
    pub fn new(snapshot: FibSnapshot) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(snapshot)),
        }
    }

    #[inline]
    pub fn load(&self) -> arc_swap::Guard<Arc<FibSnapshot>> {
        self.inner.load()
    }

    #[inline]
    fn store(&self, snapshot: FibSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }
}

pub struct IpLookupControlPlane {
    snapshot: FibSnapshotHandle,
    control_handle: Option<Arc<ControlThreadHandle>>,
}

impl IpLookupControlPlane {
    #[inline]
    pub fn new(snapshot: FibSnapshot) -> Self {
        Self {
            snapshot: FibSnapshotHandle::new(snapshot),
            control_handle: None,
        }
    }

    #[inline]
    pub fn from_handle(snapshot: FibSnapshotHandle) -> Self {
        Self {
            snapshot,
            control_handle: None,
        }
    }

    #[inline]
    pub fn with_control_handle(mut self, control_handle: Arc<ControlThreadHandle>) -> Self {
        self.control_handle = Some(control_handle);
        self
    }

    #[inline]
    pub fn snapshot_handle(&self) -> FibSnapshotHandle {
        self.snapshot.clone()
    }

    #[inline]
    pub fn node(&self) -> IpLookupNode {
        IpLookupNode::new(self.snapshot_handle())
    }

    #[inline]
    pub fn publish(&self, snapshot: FibSnapshot) -> HammerResult<()> {
        let snapshot_handle = self.snapshot.clone();
        if let Some(control_handle) = &self.control_handle {
            control_handle.call(move || snapshot_handle.store(snapshot))?;
        } else {
            snapshot_handle.store(snapshot);
        }
        Ok(())
    }
}

pub struct IpLookupNode {
    snapshot: FibSnapshotHandle,
}

impl IpLookupNode {
    #[inline]
    pub fn new(snapshot: FibSnapshotHandle) -> Self {
        Self { snapshot }
    }

    #[inline(always)]
    fn process_index<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        snapshot: &FibSnapshot,
        next_frames: &mut NodeNextFrames,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let next_node = {
            let mut buffer = runtime.get_buffer_mut(index)?;
            let parsed = match parse_ip_packet_with_chain_len(
                buffer.current(),
                buffer.total_len_not_including_first(),
            ) {
                Ok(parsed) => parsed,
                Err(_) => {
                    buffer.metadata_mut().forwarding = None;
                    return next_frames.enqueue(runtime, snapshot.drop_next(), index);
                }
            };
            let result = snapshot.lookup_packet(&parsed).unwrap_or(FibLookupResult {
                load_balance: LoadBalanceIndex::new(FORWARDING_MISS_INDEX),
                bucket_index: FORWARDING_MISS_BUCKET,
                dpo: snapshot.drop_dpo(parsed.version),
            });
            buffer.metadata_mut().forwarding = Some(ForwardingMetadata {
                fib_index: 0,
                load_balance_index: result.load_balance.get(),
                bucket_index: result.bucket_index,
                dpo_type: forwarding_dpo_type(result.dpo.dpo_type),
                dpo_index: result.dpo.index,
            });
            result.dpo.next
        };
        next_frames.enqueue(runtime, next_node, index)
    }

    #[inline(always)]
    fn prefetch_index<G>(
        runtime: &DataPlaneRuntime<G>,
        snapshot: &FibSnapshot,
        index: BufferIndex,
    ) {
        let Ok(buffer) = runtime.get_buffer(index) else {
            return;
        };
        let Ok(parsed) = parse_ip_packet_with_chain_len(
            buffer.current(),
            buffer.total_len_not_including_first(),
        ) else {
            return;
        };
        snapshot.prefetch_packet(&parsed);
    }

    #[inline(always)]
    fn prefetch_range<G>(
        runtime: &DataPlaneRuntime<G>,
        snapshot: &FibSnapshot,
        indices: &[BufferIndex],
        offset: usize,
        width: usize,
    ) {
        if offset >= indices.len() {
            return;
        }
        let end = (offset + width).min(indices.len());
        for index in indices[offset..end].iter().copied() {
            runtime.prefetch_read(index);
            Self::prefetch_index(runtime, snapshot, index);
        }
    }

    #[inline(always)]
    fn process_range<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        snapshot: &FibSnapshot,
        next_frames: &mut NodeNextFrames,
        indices: &[BufferIndex],
        offset: usize,
        width: usize,
    ) -> CoreResult<()> {
        if offset >= indices.len() {
            return Ok(());
        }
        let end = (offset + width).min(indices.len());
        for index in indices[offset..end].iter().copied() {
            self.process_index(runtime, snapshot, next_frames, index)?;
        }
        Ok(())
    }
}

impl<G> Node<G> for IpLookupNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let snapshot = self.snapshot.load();
        let mut next_frames = NodeNextFrames::default();
        let indices = frame.pending_indices();
        let width = match runtime.preferred_frame_batch_width() {
            FrameBatchWidth::Quad => 4,
            FrameBatchWidth::Pair => 2,
        };
        Self::prefetch_range(runtime, &snapshot, indices, 0, width);
        let mut offset = 0usize;
        while offset < indices.len() {
            Self::prefetch_range(runtime, &snapshot, indices, offset + width, width);
            self.process_range(runtime, &snapshot, &mut next_frames, indices, offset, width)?;
            offset += width;
        }
        frame.clear();
        next_frames.schedule(runtime)?;
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for IpLookupNode {}

#[inline(always)]
fn forwarding_dpo_type(dpo_type: DpoType) -> ForwardingDpoType {
    match dpo_type {
        DpoType::Drop => ForwardingDpoType::Drop,
        DpoType::Punt => ForwardingDpoType::Punt,
        DpoType::Adjacency => ForwardingDpoType::Adjacency,
    }
}
