use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime,
    ForwardingMetadata, InternalNode, Network, Node, NodeId, NodeNextVectorEnqueue, NodeResult,
    RouteMetadata,
};
use hammer_core::error::{CoreResult, HammerResult};
use hammer_core::forwarding::{
    Adjacency as CoreAdjacency, DpoId as CoreDpoId, FibEntry as CoreFibEntry,
    FibLookupResult as CoreFibLookupResult, FibSnapshot as CoreFibSnapshot,
    FibSnapshotBuilder as CoreFibSnapshotBuilder, LoadBalance as CoreLoadBalance,
};
pub use hammer_core::forwarding::{
    AdjacencyIndex, Dpo, DpoClass, DpoProto, DpoStackRegistry, DpoType, DpoTypeRegistry,
    FibRouteDpoError, LoadBalanceError, LoadBalanceIndex,
};
use hammer_core::protocol::ip::{
    IpProtocol, IpVersion, ParsedIpPacket, parse_ip_packet_with_chain_len,
};
use hammer_runtime::ControlThreadHandle;

pub type Adjacency = CoreAdjacency<NodeId>;
pub type DpoId = CoreDpoId<NodeId>;
pub type FibEntry = CoreFibEntry<NodeId>;
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

#[hammer_component_macros::node]
pub struct IpLookupNode {
    snapshot: FibSnapshotHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl IpLookupNode {
    #[inline(always)]
    fn process_index_with_batch(
        &self,
        batch: &mut BufferBatchMut<'_>,
        snapshot: &FibSnapshot,
        index: BufferIndex,
    ) -> CoreResult<NodeId> {
        let buffer = batch.buffer_mut(index)?;
        let parsed = match packet_from_cached_metadata(
            buffer.metadata(),
            buffer.packet_cursor(),
            buffer.current_ptr(),
            buffer.current_len(),
            buffer.total_len_not_including_first(),
        )
        .or_else(|| {
            parse_ip_packet_with_chain_len(buffer.current(), buffer.total_len_not_including_first())
                .ok()
        }) {
            Some(parsed) => parsed,
            None => {
                buffer.metadata_mut().forwarding = None;
                return Ok(snapshot.drop_next());
            }
        };
        let result = snapshot
            .lookup_packet(&parsed)
            .unwrap_or_else(|| FibLookupResult::terminal(snapshot.drop_dpo(parsed.version)));
        buffer.metadata_mut().forwarding = Some(ForwardingMetadata {
            fib_index: 0,
            route_dpo_type: result.route_dpo.kind(),
            route_dpo_index: result.route_dpo.forwarding_index(),
            load_balance_index: result.forwarding_load_balance_index(),
            bucket_index: result.forwarding_bucket_index(),
            dpo_type: result.dpo.kind(),
            dpo_index: result.dpo.forwarding_index(),
        });
        Ok(result.dpo.next())
    }

    #[inline(always)]
    fn prefetch_index_with_batch(
        batch: &mut BufferBatchMut<'_>,
        snapshot: &FibSnapshot,
        index: BufferIndex,
    ) {
        batch.prefetch_read(index);
        let Ok(buffer) = batch.buffer(index) else {
            return;
        };
        let Some(parsed) = packet_from_cached_metadata(
            buffer.metadata(),
            buffer.packet_cursor(),
            buffer.current_ptr(),
            buffer.current_len(),
            buffer.total_len_not_including_first(),
        ) else {
            return;
        };
        snapshot.prefetch_packet(&parsed);
    }

    #[inline(always)]
    fn prefetch_range_with_batch(
        batch: &mut BufferBatchMut<'_>,
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
            Self::prefetch_index_with_batch(batch, snapshot, index);
        }
    }

    #[inline(always)]
    fn prefetch_indices_with_batch(
        batch: &mut BufferBatchMut<'_>,
        snapshot: &FibSnapshot,
        indices: &[BufferIndex],
    ) {
        for index in indices.iter().copied() {
            Self::prefetch_index_with_batch(batch, snapshot, index);
        }
    }

    #[inline(always)]
    fn process_indices_with_batch(
        &self,
        batch: &mut BufferBatchMut<'_>,
        snapshot: &FibSnapshot,
        indices: &[BufferIndex],
        nexts: &mut [NodeId; 4],
        start_offset: usize,
    ) -> CoreResult<()> {
        for (offset, index) in indices.iter().copied().enumerate().skip(start_offset) {
            nexts[offset] = self.process_index_with_batch(batch, snapshot, index)?;
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
        let indices = frame.pending_indices();
        let Some(first) = indices.first().copied() else {
            return Ok(NodeResult::drop());
        };
        let width = frame_batch_width(runtime);
        let first_next = {
            let mut batch = runtime.buffer_batch_mut();
            Self::prefetch_range_with_batch(&mut batch, &snapshot, indices, 0, width);
            self.process_index_with_batch(&mut batch, &snapshot, first)?
        };
        let cached_next = self.cached_next.unwrap_or(first_next);
        let mut first_chunk = true;
        let (result, cached_next) = NodeNextVectorEnqueue::new(cached_next)
            .enqueue_frame_with_buffer_batch_chunks(
                runtime,
                frame,
                |batch, indices| {
                    Self::prefetch_indices_with_batch(batch, &snapshot, indices);
                },
                |batch, indices, nexts| {
                    let start_offset = if first_chunk {
                        first_chunk = false;
                        nexts[0] = first_next;
                        1
                    } else {
                        0
                    };
                    self.process_indices_with_batch(batch, &snapshot, indices, nexts, start_offset)
                },
            )?;
        self.cached_next = Some(cached_next);
        Ok(result)
    }
}

impl<G> InternalNode<G> for IpLookupNode {}

#[inline(always)]
fn frame_batch_width<G>(runtime: &DataPlaneRuntime<G>) -> usize {
    match runtime.preferred_frame_batch_width() {
        hammer_adapter::FrameBatchWidth::Quad => 4,
        hammer_adapter::FrameBatchWidth::Pair => 2,
    }
}

#[inline(always)]
fn packet_from_cached_metadata(
    metadata: &RouteMetadata,
    cursor: BufferPacketCursor,
    current_ptr: *const u8,
    current_len: usize,
    tail_len: usize,
) -> Option<ParsedIpPacket> {
    if cursor.packet_len() == 0 || current_ptr.is_null() {
        return None;
    }
    let chain_len = current_len.checked_add(tail_len)?;
    if cursor.network_header_offset() > current_len || cursor.packet_len() > chain_len {
        return None;
    }
    let source = metadata.source.as_ref()?.host;
    let destination = metadata.destination.as_ref()?.host;
    let version = match (source, destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => IpVersion::V4,
        (IpAddr::V6(_), IpAddr::V6(_)) => IpVersion::V6,
        _ => return None,
    };
    Some(ParsedIpPacket {
        version,
        protocol: protocol_from_network(metadata.network, version),
        input_target: hammer_core::protocol::ip::IpInputTarget::Lookup,
        input_error: hammer_core::protocol::ip::IpInputError::None,
        source,
        destination,
        packet_len: cursor.packet_len(),
        network_header_offset: cursor.network_header_offset(),
        network_header_len: cursor.network_header_len(),
        transport_header_offset: cursor.transport_header_offset(),
        transport_header_len: cursor.transport_header_len(),
    })
}

#[inline(always)]
fn protocol_from_network(network: Network, version: IpVersion) -> IpProtocol {
    match network {
        Network::Tcp => IpProtocol::Tcp,
        Network::Udp => IpProtocol::Udp,
        Network::Icmp => match version {
            IpVersion::V4 => IpProtocol::Icmpv4,
            IpVersion::V6 => IpProtocol::Icmpv6,
        },
    }
}
