use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, DataWorkerId, Node, NodeHandle, NodeId,
    NodeNextFrames, NodeNextStorage, NodeResult, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

use crate::net::ip::{
    IpFragmentKey, IpProtocol, IpVersion, ParsedIpFragment, network_for_protocol,
    parse_ip_fragment_with_chain_len, parse_ip_packet_with_chain_len,
};

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV6_FRAGMENT_HEADER_LEN: usize = 8;
const IPV4_FLAGS_FRAGMENT_OFFSET: usize = 6;
const IPV4_TOTAL_LENGTH_OFFSET: usize = 2;
const IPV4_HEADER_CHECKSUM_OFFSET: usize = 10;
const IPV6_PAYLOAD_LENGTH_OFFSET: usize = 4;
const IPV6_NEXT_HEADER_OFFSET: usize = 6;
const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_millis(100);
const DEFAULT_MAX_REASSEMBLIES: usize = 1024;
const DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY: usize = 64;

#[hammer_component_macros::node_next]
pub enum IpReassemblyNext {
    Lookup,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = IpReassemblyNext)]
pub struct IpReassemblyNode {
    #[node(default)]
    handoff: Option<IpReassemblyHandoff>,
    #[node(default = DEFAULT_REASSEMBLY_TIMEOUT)]
    timeout: Duration,
    #[node(default = DEFAULT_MAX_REASSEMBLIES)]
    max_reassemblies: usize,
    #[node(default = DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY)]
    max_fragments_per_reassembly: usize,
    #[node(default)]
    contexts: HashMap<IpFragmentKey, ReassemblyContext>,
    #[node(default)]
    failed_keys: Vec<IpFragmentKey>,
}

#[derive(Debug, Clone)]
pub struct IpReassemblyHandoff {
    reassembly: NodeHandle,
    lookup: NodeHandle,
    worker: DataWorkerId,
    directory: IpReassemblyDirectory,
}

#[derive(Debug, Clone, Default)]
pub struct IpReassemblyDirectory {
    inner: Arc<ArcSwap<HashMap<IpFragmentKey, DataWorkerId>>>,
}

impl IpReassemblyHandoff {
    #[inline]
    pub fn new(
        reassembly: NodeHandle,
        lookup: NodeHandle,
        worker: DataWorkerId,
        directory: IpReassemblyDirectory,
    ) -> Self {
        Self {
            reassembly,
            lookup,
            worker,
            directory,
        }
    }
}

impl IpReassemblyNode {
    #[inline]
    pub fn with_handoff(mut self, handoff: IpReassemblyHandoff) -> Self {
        self.handoff = Some(handoff);
        self
    }

    #[inline]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[inline]
    pub fn with_max_fragments_per_reassembly(mut self, max_fragments: usize) -> Self {
        self.max_fragments_per_reassembly = max_fragments;
        self
    }

    #[inline]
    pub fn expire<G>(&mut self, runtime: &DataPlaneRuntime<G>, now: Instant) -> usize {
        let timeout = self.timeout;
        let expired = self
            .contexts
            .iter()
            .filter_map(|(key, context)| {
                (now.duration_since(context.updated_at) > timeout).then_some(*key)
            })
            .collect::<Vec<_>>();

        let expired_len = expired.len();
        for key in expired {
            if let Some(context) = self.contexts.remove(&key) {
                context.free(runtime);
            }
            if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
        }
        expired_len
    }

    #[inline]
    fn process_index<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        next_frames: &mut NodeNextFrames,
        current_next: &mut Option<NodeId>,
        next: [NodeId; IpReassemblyNext::COUNT],
        index: BufferIndex,
        now: Instant,
    ) -> CoreResult<Option<BufferIndex>> {
        let buffer = runtime.get_buffer(index)?;
        let fragment = match parse_ip_fragment_with_chain_len(
            buffer.current(),
            buffer.total_len_not_including_first(),
        ) {
            Ok(fragment) => fragment,
            Err(_) => {
                drop(buffer);
                return self.emit_output(
                    runtime,
                    next_frames,
                    current_next,
                    NodeNextStorage::next(&next, IpReassemblyNext::Drop),
                    index,
                );
            }
        };
        drop(buffer);

        let key = fragment.key;
        if self.failed_keys.contains(&key) {
            next_frames.enqueue(
                runtime,
                NodeNextStorage::next(&next, IpReassemblyNext::Drop),
                index,
            )?;
            return Ok(None);
        }
        let fragment_first_worker = self.fragment_first_worker(runtime, index, fragment)?;
        if let Some(handoff) = &self.handoff {
            let owner = handoff.directory.owner_or_insert(key, handoff.worker);
            if owner != handoff.worker {
                runtime.handoff_index(owner, handoff.reassembly, index)?;
                return Ok(None);
            }
        }
        if !self.contexts.contains_key(&key) {
            if self.contexts.len() == self.max_reassemblies {
                return self.emit_output(
                    runtime,
                    next_frames,
                    current_next,
                    NodeNextStorage::next(&next, IpReassemblyNext::Drop),
                    index,
                );
            }
            self.contexts.insert(
                key,
                ReassemblyContext::new(
                    fragment.version,
                    now,
                    fragment_first_worker.unwrap_or_else(|| self.current_worker()),
                ),
            );
        }

        let mut reassembled = None;
        let mut failed = None;
        {
            let context = self
                .contexts
                .get_mut(&key)
                .ok_or_else(|| CoreError::internal("missing reassembly context"))?;
            if let Some(worker) = fragment_first_worker {
                context.first_fragment_worker = worker;
            }
            let outcome = context.insert_fragment(
                runtime,
                index,
                fragment,
                now,
                self.max_fragments_per_reassembly,
            )?;
            match outcome {
                ReassemblyInsert::Pending => {}
                ReassemblyInsert::Drop(index) => {
                    return self.emit_output(
                        runtime,
                        next_frames,
                        current_next,
                        NodeNextStorage::next(&next, IpReassemblyNext::Drop),
                        index,
                    );
                }
                ReassemblyInsert::Reassembled(index) => reassembled = Some(index),
                ReassemblyInsert::Failed(index) => failed = Some(index),
            }
        }

        if let Some(failed_index) = failed {
            let drop_node = NodeNextStorage::next(&next, IpReassemblyNext::Drop);
            if let Some(context) = self.contexts.remove(&key) {
                for fragment in context.fragments {
                    next_frames.enqueue(runtime, drop_node, fragment.index)?;
                }
            }
            next_frames.enqueue(runtime, drop_node, failed_index)?;
            if !self.failed_keys.contains(&key) {
                self.failed_keys.push(key);
            }
            if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
            return Ok(None);
        }

        if let Some(index) = reassembled {
            let first_worker = self
                .contexts
                .get(&key)
                .map(|context| context.first_fragment_worker);
            self.contexts.remove(&key);
            if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
            refresh_metadata(runtime, index)?;
            if let Some(handoff) = &self.handoff {
                let first_worker = first_worker.unwrap_or(handoff.worker);
                if first_worker != handoff.worker {
                    runtime.handoff_index(first_worker, handoff.lookup, index)?;
                } else {
                    return self.emit_output(
                        runtime,
                        next_frames,
                        current_next,
                        NodeNextStorage::next(&next, IpReassemblyNext::Lookup),
                        index,
                    );
                }
            } else {
                return self.emit_output(
                    runtime,
                    next_frames,
                    current_next,
                    NodeNextStorage::next(&next, IpReassemblyNext::Lookup),
                    index,
                );
            }
        }
        Ok(None)
    }

    #[inline(always)]
    fn emit_output<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        next_frames: &mut NodeNextFrames,
        current_next: &mut Option<NodeId>,
        node: NodeId,
        index: BufferIndex,
    ) -> CoreResult<Option<BufferIndex>> {
        match *current_next {
            Some(current) if current == node => Ok(Some(index)),
            Some(_) => {
                next_frames.enqueue(runtime, node, index)?;
                Ok(None)
            }
            None => {
                *current_next = Some(node);
                Ok(Some(index))
            }
        }
    }

    #[inline(always)]
    fn fragment_first_worker<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
        fragment: ParsedIpFragment,
    ) -> CoreResult<Option<DataWorkerId>> {
        if fragment.payload_offset != 0 {
            return Ok(None);
        }
        if fragment.payload_offset == 0
            && let Some(worker) = runtime.handoff_source_worker(index)?
        {
            return Ok(Some(worker));
        }
        Ok(Some(self.current_worker()))
    }

    #[inline(always)]
    fn current_worker(&self) -> DataWorkerId {
        self.handoff
            .as_ref()
            .map(|handoff| handoff.worker)
            .unwrap_or_else(|| DataWorkerId::new(0))
    }
}

impl IpReassemblyDirectory {
    #[inline]
    fn owner_or_insert(&self, key: IpFragmentKey, worker: DataWorkerId) -> DataWorkerId {
        if let Some(owner) = self.inner.load().get(&key).copied() {
            return owner;
        }

        let mut inserted = None;
        self.inner.rcu(|current| {
            let mut next = HashMap::clone(current);
            match next.entry(key) {
                Entry::Occupied(entry) => inserted = Some(*entry.get()),
                Entry::Vacant(entry) => {
                    entry.insert(worker);
                    inserted = Some(worker);
                }
            }
            next
        });
        inserted.unwrap_or(worker)
    }

    #[inline]
    fn remove(&self, key: IpFragmentKey) {
        if !self.inner.load().contains_key(&key) {
            return;
        }
        self.inner.rcu(|current| {
            let mut next = HashMap::clone(current);
            next.remove(&key);
            next
        });
    }
}

impl<G> Node<G> for IpReassemblyNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let now = Instant::now();
        let next = Self::runtime_nexts(runtime)?;
        let mut next_frames = NodeNextFrames::default();
        let mut current_next = None;
        self.failed_keys.clear();
        frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
            self.process_index(
                runtime,
                &mut next_frames,
                &mut current_next,
                next,
                index,
                now,
            )
        })?;
        next_frames.schedule(runtime)?;
        if frame.has_pending()
            && let Some(node) = current_next
        {
            Ok(NodeResult::next_current(node))
        } else {
            Ok(NodeResult::drop())
        }
    }
}

struct ReassemblyContext {
    version: IpVersion,
    first_fragment_worker: DataWorkerId,
    updated_at: Instant,
    total_payload_len: Option<usize>,
    fragments: Vec<ReassemblyFragment>,
}

impl ReassemblyContext {
    #[inline]
    fn new(version: IpVersion, now: Instant, first_fragment_worker: DataWorkerId) -> Self {
        Self {
            version,
            first_fragment_worker,
            updated_at: now,
            total_payload_len: None,
            fragments: Vec::new(),
        }
    }

    #[inline]
    fn insert_fragment<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
        fragment: ParsedIpFragment,
        now: Instant,
        max_fragments: usize,
    ) -> CoreResult<ReassemblyInsert> {
        self.updated_at = now;
        let start = fragment.payload_offset;
        let end = start
            .checked_add(fragment.payload_len)
            .ok_or_else(|| CoreError::internal("fragment payload length overflow"))?;
        if start == end {
            return Ok(ReassemblyInsert::Drop(index));
        }
        if self.is_duplicate_covered(start, end) {
            return Ok(ReassemblyInsert::Drop(index));
        }
        if self.overlaps_existing(start, end) {
            return Ok(ReassemblyInsert::Failed(index));
        }
        if self.fragments.len() == max_fragments {
            return Ok(ReassemblyInsert::Failed(index));
        }
        if !fragment.more_fragments {
            if self.total_payload_len.is_some_and(|total| total != end) {
                return Ok(ReassemblyInsert::Failed(index));
            }
            self.total_payload_len = Some(end);
        }

        self.fragments.push(ReassemblyFragment {
            index,
            start,
            end,
            header_len: fragment.header_len,
        });
        self.fragments.sort_by_key(|fragment| fragment.start);

        let Some(total_payload_len) = self.total_payload_len else {
            return Ok(ReassemblyInsert::Pending);
        };
        if !self.is_complete(total_payload_len) {
            return Ok(ReassemblyInsert::Pending);
        }

        self.assemble(runtime, total_payload_len)
    }

    #[inline]
    fn is_duplicate_covered(&self, start: usize, end: usize) -> bool {
        self.fragments
            .iter()
            .any(|fragment| start >= fragment.start && end <= fragment.end)
    }

    #[inline]
    fn overlaps_existing(&self, start: usize, end: usize) -> bool {
        self.fragments
            .iter()
            .any(|fragment| start < fragment.end && end > fragment.start)
    }

    #[inline]
    fn is_complete(&self, total_payload_len: usize) -> bool {
        let mut next = 0usize;
        for fragment in &self.fragments {
            if fragment.start != next {
                return false;
            }
            next = fragment.end;
        }
        next == total_payload_len
    }

    #[inline]
    fn assemble<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        total_payload_len: usize,
    ) -> CoreResult<ReassemblyInsert> {
        match self.version {
            IpVersion::V4 => self.assemble_ipv4_chain(runtime, total_payload_len),
            IpVersion::V6 => self.assemble_ipv6_chain(runtime, total_payload_len),
        }
    }

    #[inline]
    fn assemble_ipv4_chain<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        total_payload_len: usize,
    ) -> CoreResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        let header_len = first.header_len;
        if header_len < IPV4_HEADER_MIN_LEN || runtime.current_len(first.index)? < header_len {
            return Err(CoreError::internal("invalid IPv4 fragment header"));
        }
        let total_len = header_len
            .checked_add(total_payload_len)
            .ok_or_else(|| CoreError::internal("IPv4 reassembled length overflow"))?;
        if total_len > u16::MAX as usize {
            return Err(CoreError::internal("IPv4 reassembled packet too large"));
        }

        let complete = first.index;
        let mut fragments = std::mem::take(&mut self.fragments);
        fragments.sort_by_key(|fragment| fragment.start);
        for fragment in fragments.iter().copied() {
            if fragment.index == complete {
                runtime.truncate_chain(
                    complete,
                    fragment.header_len + (fragment.end - fragment.start),
                )?;
            } else {
                trim_fragment_payload_chain(runtime, fragment)?;
                runtime.append_existing_chain(complete, fragment.index)?;
            }
        }
        runtime.truncate_chain(complete, total_len)?;
        {
            let mut buffer = runtime.get_buffer_mut(complete)?;
            let header = buffer.current();
            if header.len() < header_len {
                return Err(CoreError::internal("invalid IPv4 reassembled header"));
            }
            let header = &mut buffer.current_mut()[..header_len];
            header[IPV4_TOTAL_LENGTH_OFFSET..IPV4_TOTAL_LENGTH_OFFSET + 2]
                .copy_from_slice(&(total_len as u16).to_be_bytes());
            header[IPV4_FLAGS_FRAGMENT_OFFSET..IPV4_FLAGS_FRAGMENT_OFFSET + 2]
                .copy_from_slice(&0u16.to_be_bytes());
            update_ipv4_header_checksum(header, header_len);
        }
        Ok(ReassemblyInsert::Reassembled(complete))
    }

    #[inline]
    fn assemble_ipv6_chain<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        total_payload_len: usize,
    ) -> CoreResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        if runtime.current_len(first.index)? < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN {
            return Err(CoreError::internal("invalid IPv6 fragment header"));
        }
        let payload_len = total_payload_len;
        if payload_len > u16::MAX as usize {
            return Err(CoreError::internal("IPv6 reassembled packet too large"));
        }
        let complete = first.index;
        let fragment_next_header = {
            let buffer = runtime.get_buffer(complete)?;
            buffer.current()[IPV6_HEADER_LEN]
        };
        let mut fragments = std::mem::take(&mut self.fragments);
        fragments.sort_by_key(|fragment| fragment.start);
        for fragment in fragments.iter().copied() {
            if fragment.index == complete {
                runtime.remove_current_range(
                    complete,
                    IPV6_HEADER_LEN,
                    IPV6_FRAGMENT_HEADER_LEN,
                )?;
                let mut buffer = runtime.get_buffer_mut(complete)?;
                let header = &mut buffer.current_mut()[..IPV6_HEADER_LEN];
                header[IPV6_PAYLOAD_LENGTH_OFFSET..IPV6_PAYLOAD_LENGTH_OFFSET + 2]
                    .copy_from_slice(&(payload_len as u16).to_be_bytes());
                header[IPV6_NEXT_HEADER_OFFSET] = fragment_next_header;
                drop(buffer);
                runtime
                    .truncate_chain(complete, IPV6_HEADER_LEN + (fragment.end - fragment.start))?;
            } else {
                trim_fragment_payload_chain(runtime, fragment)?;
                runtime.append_existing_chain(complete, fragment.index)?;
            }
        }
        let total_len = IPV6_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| CoreError::internal("IPv6 reassembled length overflow"))?;
        runtime.truncate_chain(complete, total_len)?;
        Ok(ReassemblyInsert::Reassembled(complete))
    }

    #[inline]
    fn first_fragment_offset(&self) -> CoreResult<usize> {
        self.fragments
            .iter()
            .position(|fragment| fragment.start == 0)
            .ok_or_else(|| CoreError::internal("missing first IP fragment"))
    }

    #[inline]
    fn free<G>(self, runtime: &DataPlaneRuntime<G>) {
        for fragment in self.fragments {
            runtime.free_index(fragment.index);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ReassemblyFragment {
    index: BufferIndex,
    start: usize,
    end: usize,
    header_len: usize,
}

enum ReassemblyInsert {
    Pending,
    Drop(BufferIndex),
    Reassembled(BufferIndex),
    Failed(BufferIndex),
}

#[inline(always)]
fn refresh_metadata<G>(runtime: &DataPlaneRuntime<G>, index: BufferIndex) -> CoreResult<()> {
    let buffer = runtime.get_buffer(index)?;
    let parsed =
        parse_ip_packet_with_chain_len(buffer.current(), buffer.total_len_not_including_first())?;
    drop(buffer);
    let network = match network_for_protocol(parsed.protocol) {
        Some(network) => network,
        None => {
            let IpProtocol::Other(protocol) = parsed.protocol else {
                return Err(CoreError::internal(format!(
                    "unsupported reassembled transport protocol: {:?}",
                    parsed.protocol
                )));
            };
            return Err(CoreError::internal(format!(
                "unsupported reassembled transport protocol: {protocol}"
            )));
        }
    };
    let mut buffer = runtime.get_buffer_mut(index)?;
    let metadata = buffer.metadata_mut();
    metadata.network = network;
    metadata.source = Some(SocksAddr::ip(parsed.source, 0));
    metadata.destination = Some(SocksAddr::ip(parsed.destination, 0));
    Ok(())
}

#[inline(always)]
fn trim_fragment_payload_chain<G>(
    runtime: &DataPlaneRuntime<G>,
    fragment: ReassemblyFragment,
) -> CoreResult<()> {
    let payload_len = fragment.end - fragment.start;
    runtime.advance(fragment.index, fragment.header_len)?;
    runtime.truncate_chain(fragment.index, payload_len)
}

#[inline(always)]
fn update_ipv4_header_checksum(packet: &mut [u8], header_len: usize) {
    packet[IPV4_HEADER_CHECKSUM_OFFSET] = 0;
    packet[IPV4_HEADER_CHECKSUM_OFFSET + 1] = 0;
    let checksum = internet_checksum(&packet[..header_len]);
    packet[IPV4_HEADER_CHECKSUM_OFFSET..IPV4_HEADER_CHECKSUM_OFFSET + 2]
        .copy_from_slice(&checksum.to_be_bytes());
}

#[inline(always)]
fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
