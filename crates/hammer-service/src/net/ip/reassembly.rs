use std::collections::HashMap;
use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Network, Node, NodeId,
    NodeNextFrames, NodeResult, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::net::ip::{
    IpFragmentKey, IpVersion, ParsedIpFragment, parse_ip_fragment, parse_ip_packet,
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
const DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY: usize = 3;

pub struct IpReassemblyNode {
    next: NodeId,
    timeout: Duration,
    max_reassemblies: usize,
    max_fragments_per_reassembly: usize,
    contexts: HashMap<IpFragmentKey, ReassemblyContext>,
}

impl IpReassemblyNode {
    #[inline]
    pub fn new(next: NodeId) -> Self {
        Self {
            next,
            timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            max_reassemblies: DEFAULT_MAX_REASSEMBLIES,
            max_fragments_per_reassembly: DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY,
            contexts: HashMap::new(),
        }
    }

    #[inline]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
        }
        expired_len
    }

    #[inline]
    pub fn process_at<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        now: Instant,
    ) -> CoreResult<NodeResult> {
        let mut next_frames = NodeNextFrames::default();
        let indices = frame.pending_indices().to_vec();
        frame.clear();
        let mut cursor = indices.as_slice().chunks_exact(2);
        for batch in cursor.by_ref() {
            for index in batch.iter().copied() {
                self.process_index(runtime, &mut next_frames, index, now)?;
            }
        }
        for index in cursor.remainder().iter().copied() {
            self.process_index(runtime, &mut next_frames, index, now)?;
        }
        next_frames.schedule(runtime)?;
        Ok(NodeResult::drop())
    }

    #[inline]
    fn process_index<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        next_frames: &mut NodeNextFrames,
        index: BufferIndex,
        now: Instant,
    ) -> CoreResult<()> {
        let packet = runtime.copy_current_chain(index)?;
        let fragment = match parse_ip_fragment(&packet) {
            Ok(fragment) => fragment,
            Err(_) => {
                runtime.free_index(index);
                return Ok(());
            }
        };

        let key = fragment.key;
        if !self.contexts.contains_key(&key) {
            if self.contexts.len() == self.max_reassemblies {
                runtime.free_index(index);
                return Ok(());
            }
            self.contexts
                .insert(key, ReassemblyContext::new(fragment.version, now));
        }

        let mut completed = None;
        let mut failed = false;
        {
            let context = self
                .contexts
                .get_mut(&key)
                .ok_or_else(|| CoreError::internal("missing reassembly context"))?;
            let outcome = context.insert_fragment(
                runtime,
                index,
                fragment,
                now,
                self.max_fragments_per_reassembly,
            )?;
            match outcome {
                ReassemblyInsert::Pending => {}
                ReassemblyInsert::Complete(index) => completed = Some(index),
                ReassemblyInsert::Failed => failed = true,
            }
        }

        if failed {
            if let Some(context) = self.contexts.remove(&key) {
                context.free(runtime);
            }
            return Ok(());
        }

        if let Some(index) = completed {
            self.contexts.remove(&key);
            refresh_metadata(runtime, index)?;
            next_frames.enqueue(runtime, self.next, index)?;
        }
        Ok(())
    }
}

impl<G> Node<G> for IpReassemblyNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.process_at(runtime, frame, Instant::now())
    }
}

impl<G> InternalNode<G> for IpReassemblyNode {}

struct ReassemblyContext {
    version: IpVersion,
    updated_at: Instant,
    total_payload_len: Option<usize>,
    fragments: Vec<ReassemblyFragment>,
}

impl ReassemblyContext {
    #[inline]
    fn new(version: IpVersion, now: Instant) -> Self {
        Self {
            version,
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
            runtime.free_index(index);
            return Ok(ReassemblyInsert::Pending);
        }
        if self.is_duplicate_covered(start, end) {
            runtime.free_index(index);
            return Ok(ReassemblyInsert::Pending);
        }
        if self.overlaps_existing(start, end) {
            runtime.free_index(index);
            return Ok(ReassemblyInsert::Failed);
        }
        if self.fragments.len() == max_fragments {
            runtime.free_index(index);
            return Ok(ReassemblyInsert::Failed);
        }
        if !fragment.more_fragments {
            if self.total_payload_len.is_some_and(|total| total != end) {
                runtime.free_index(index);
                return Ok(ReassemblyInsert::Failed);
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
        let packet = match self.version {
            IpVersion::V4 => self.assemble_ipv4(runtime, total_payload_len)?,
            IpVersion::V6 => self.assemble_ipv6(runtime, total_payload_len)?,
        };
        let metadata = runtime.metadata(self.fragments[0].index)?;
        let complete = runtime.alloc_index_with_bytes(metadata, &packet)?;
        for fragment in self.fragments.drain(..) {
            runtime.free_index(fragment.index);
        }
        Ok(ReassemblyInsert::Complete(complete))
    }

    #[inline]
    fn assemble_ipv4<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        total_payload_len: usize,
    ) -> CoreResult<Vec<u8>> {
        let first = self
            .fragments
            .iter()
            .find(|fragment| fragment.start == 0)
            .ok_or_else(|| CoreError::internal("missing first IPv4 fragment"))?;
        let first_packet = runtime.copy_current_chain(first.index)?;
        let header_len = first.header_len;
        if header_len < IPV4_HEADER_MIN_LEN || first_packet.len() < header_len {
            return Err(CoreError::internal("invalid IPv4 fragment header"));
        }
        let total_len = header_len
            .checked_add(total_payload_len)
            .ok_or_else(|| CoreError::internal("IPv4 reassembled length overflow"))?;
        if total_len > u16::MAX as usize {
            return Err(CoreError::internal("IPv4 reassembled packet too large"));
        }

        let mut packet = Vec::with_capacity(total_len);
        packet.extend_from_slice(&first_packet[..header_len]);
        for fragment in &self.fragments {
            let fragment_packet = runtime.copy_current_chain(fragment.index)?;
            let start = fragment.header_len;
            let end = start + (fragment.end - fragment.start);
            packet.extend_from_slice(&fragment_packet[start..end]);
        }
        packet[IPV4_TOTAL_LENGTH_OFFSET..IPV4_TOTAL_LENGTH_OFFSET + 2]
            .copy_from_slice(&(total_len as u16).to_be_bytes());
        packet[IPV4_FLAGS_FRAGMENT_OFFSET..IPV4_FLAGS_FRAGMENT_OFFSET + 2]
            .copy_from_slice(&0u16.to_be_bytes());
        update_ipv4_header_checksum(&mut packet, header_len);
        Ok(packet)
    }

    #[inline]
    fn assemble_ipv6<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        total_payload_len: usize,
    ) -> CoreResult<Vec<u8>> {
        let first = self
            .fragments
            .iter()
            .find(|fragment| fragment.start == 0)
            .ok_or_else(|| CoreError::internal("missing first IPv6 fragment"))?;
        let first_packet = runtime.copy_current_chain(first.index)?;
        if first_packet.len() < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN {
            return Err(CoreError::internal("invalid IPv6 fragment header"));
        }
        let payload_len = total_payload_len;
        if payload_len > u16::MAX as usize {
            return Err(CoreError::internal("IPv6 reassembled packet too large"));
        }
        let fragment_next_header = first_packet[IPV6_HEADER_LEN];
        let total_len = IPV6_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| CoreError::internal("IPv6 reassembled length overflow"))?;
        let mut packet = Vec::with_capacity(total_len);
        packet.extend_from_slice(&first_packet[..IPV6_HEADER_LEN]);
        packet[IPV6_PAYLOAD_LENGTH_OFFSET..IPV6_PAYLOAD_LENGTH_OFFSET + 2]
            .copy_from_slice(&(payload_len as u16).to_be_bytes());
        packet[IPV6_NEXT_HEADER_OFFSET] = fragment_next_header;
        for fragment in &self.fragments {
            let fragment_packet = runtime.copy_current_chain(fragment.index)?;
            let start = fragment.header_len;
            let end = start + (fragment.end - fragment.start);
            packet.extend_from_slice(&fragment_packet[start..end]);
        }
        Ok(packet)
    }

    #[inline]
    fn free<G>(self, runtime: &DataPlaneRuntime<G>) {
        for fragment in self.fragments {
            runtime.free_index(fragment.index);
        }
    }
}

struct ReassemblyFragment {
    index: BufferIndex,
    start: usize,
    end: usize,
    header_len: usize,
}

enum ReassemblyInsert {
    Pending,
    Complete(BufferIndex),
    Failed,
}

#[inline(always)]
fn refresh_metadata<G>(runtime: &DataPlaneRuntime<G>, index: BufferIndex) -> CoreResult<()> {
    let packet = runtime.copy_current_chain(index)?;
    let parsed = parse_ip_packet(&packet)?;
    let network = match parsed.protocol {
        crate::net::ip::IpProtocol::Tcp => Network::Tcp,
        crate::net::ip::IpProtocol::Udp => Network::Udp,
        crate::net::ip::IpProtocol::Icmpv4 | crate::net::ip::IpProtocol::Icmpv6 => Network::Icmp,
        crate::net::ip::IpProtocol::Other(protocol) => {
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
