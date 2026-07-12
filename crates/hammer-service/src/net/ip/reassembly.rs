use std::cell::RefCell;
use std::mem::transmute;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeHandle, NodeId, NodeNext,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::bihash::{Bihash, FREE_U64};
use hammer_infra::checksum::internet_checksum;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::vec::Vec;
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace,
};

use crate::trace::codec::{
    TraceDecodeCursor, put_option_ip_fragment_key, put_option_u16, put_option_u32, put_u8, put_u32,
};

use crate::net::NetworkOpaque;
use crate::net::ip::{
    IpFragmentKey, IpProtocol, IpVersion, ParsedIpFragment, ip_header, network_for_protocol,
    parse_ip_fragment_with_chain_len,
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

#[inline]
pub fn pack_fragment_owner_value(index: PoolIndex, owner: DataWorkerId) -> u64 {
    debug_assert!(owner.slot() <= u16::MAX as usize);
    debug_assert!(index.generation() <= u16::MAX as u32);
    let value = u64::from(index.slot())
        | (u64::from(index.generation() as u16) << 32)
        | (u64::from(owner.slot() as u16) << 48);
    debug_assert_ne!(value, FREE_U64);
    value
}

#[inline]
pub fn unpack_fragment_owner_value(value: u64) -> (PoolIndex, DataWorkerId) {
    let slot = value as u32;
    let generation = ((value >> 32) as u16) as u32;
    let owner = DataWorkerId::new(u32::from((value >> 48) as u16));
    (PoolIndex::new(slot, generation), owner)
}

#[hammer_component_macros::node_next]
pub enum IpReassemblyNext {
    Input,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpReassemblyTraceAction {
    Pending,
    Drop,
    Reassembled,
    Handoff,
    Failed,
}

impl IpReassemblyTraceAction {
    #[inline]
    fn encode(self) -> u8 {
        match self {
            Self::Pending => 0,
            Self::Drop => 1,
            Self::Reassembled => 2,
            Self::Handoff => 3,
            Self::Failed => 4,
        }
    }

    #[inline]
    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Pending),
            1 => Some(Self::Drop),
            2 => Some(Self::Reassembled),
            3 => Some(Self::Handoff),
            4 => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpReassemblyTrace {
    pub key: Option<IpFragmentKey>,
    pub action: IpReassemblyTraceAction,
    pub current_worker: DataWorkerId,
    pub owner_worker: Option<DataWorkerId>,
    pub next: Option<u16>,
}

impl IpReassemblyTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            key: cursor.read_option_ip_fragment_key()?,
            action: IpReassemblyTraceAction::decode(cursor.read_u8()?)?,
            current_worker: DataWorkerId::new(cursor.read_u32()?),
            owner_worker: cursor.read_option_u32()?.map(DataWorkerId::new),
            next: cursor.read_option_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IpReassemblyTrace {
    #[inline]
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
        put_option_ip_fragment_key(out, self.key);
        put_u8(out, self.action.encode());
        put_u32(out, self.current_worker.slot() as u32);
        put_option_u32(out, self.owner_worker.map(|worker| worker.slot() as u32));
        put_option_u16(out, self.next);
    }
}

fn format_ip_reassembly_trace(bytes: &[u8]) -> String {
    match IpReassemblyTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IpReassemblyTrace invalid={bytes:?}"),
    }
}

#[derive(Clone)]
pub struct IpReassemblyDirectory {
    inner: Arc<Bihash<IpFragmentKey, 1>>,
}

impl IpReassemblyDirectory {
    #[inline]
    pub fn new(nbuckets: u32) -> Self {
        Self {
            inner: Arc::new(Bihash::new(nbuckets)),
        }
    }

    #[inline]
    pub fn claim_or_lookup(
        &self,
        key: IpFragmentKey,
        index: PoolIndex,
        worker: DataWorkerId,
    ) -> (DataWorkerId, bool) {
        let value = pack_fragment_owner_value(index, worker);
        match self.inner.insert_if_absent(key, value) {
            Ok(()) => (worker, true),
            Err(existing) => {
                let (_, owner) = unpack_fragment_owner_value(existing);
                (owner, false)
            }
        }
    }

    #[inline]
    pub fn lookup(&self, key: IpFragmentKey) -> Option<(PoolIndex, DataWorkerId)> {
        self.inner.lookup(&key).map(unpack_fragment_owner_value)
    }

    #[inline]
    pub fn remove(&self, key: IpFragmentKey) {
        let _ = self.inner.remove(&key);
    }
}

#[derive(Clone)]
pub struct IpReassemblyHandoff {
    reassembly: NodeHandle,
    input: NodeHandle,
    worker: DataWorkerId,
    directory: IpReassemblyDirectory,
}

impl IpReassemblyHandoff {
    #[inline]
    pub fn new(
        reassembly: NodeHandle,
        input: NodeHandle,
        worker: DataWorkerId,
        directory: IpReassemblyDirectory,
    ) -> Self {
        Self {
            reassembly,
            input,
            worker,
            directory,
        }
    }

    #[inline]
    pub fn reassembly(&self) -> NodeHandle {
        self.reassembly
    }

    #[inline]
    pub fn input(&self) -> NodeHandle {
        self.input
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn directory(&self) -> &IpReassemblyDirectory {
        &self.directory
    }
}

struct IpReassemblyWorker {
    worker: DataWorkerId,
    contexts: Pool<FragmentContext>,
    directory: Option<Arc<IpReassemblyDirectory>>,
    handoff: Option<IpReassemblyHandoff>,
    timeout: Duration,
    max_reassemblies: usize,
    max_fragments_per_reassembly: usize,
}

thread_local! {
    static WORKER: RefCell<Option<IpReassemblyWorker>> = const { RefCell::new(None) };
}

fn with_worker_mut<R>(f: impl FnOnce(&mut IpReassemblyWorker) -> R) -> Option<R> {
    WORKER.with(|slot| {
        let mut slot = slot.borrow_mut();
        slot.as_mut().map(f)
    })
}

fn ensure_worker(config: &IpReassemblyNode) {
    WORKER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            let worker = config
                .handoff
                .as_ref()
                .map(|h| h.worker)
                .unwrap_or_else(|| DataWorkerId::new(0));
            let directory = config
                .directory
                .clone()
                .or_else(|| config.handoff.as_ref().map(|h| Arc::new(h.directory.clone())));
            *slot = Some(IpReassemblyWorker {
                worker,
                contexts: Pool::with_capacity(config.max_reassemblies.max(1)),
                directory,
                handoff: config.handoff.clone(),
                timeout: config.timeout,
                max_reassemblies: config.max_reassemblies,
                max_fragments_per_reassembly: config.max_fragments_per_reassembly,
            });
        } else if let Some(worker) = slot.as_mut() {
            worker.timeout = config.timeout;
            worker.max_reassemblies = config.max_reassemblies;
            worker.max_fragments_per_reassembly = config.max_fragments_per_reassembly;
            if config.handoff.is_some() {
                worker.handoff = config.handoff.clone();
                worker.worker = config.handoff.as_ref().unwrap().worker;
                worker.directory = Some(Arc::new(config.handoff.as_ref().unwrap().directory.clone()));
            } else if let Some(directory) = &config.directory {
                worker.directory = Some(Arc::clone(directory));
            }
        }
    });
}

#[hammer_component_macros::node(role = internal, next = IpReassemblyNext)]
#[derive(Clone)]
pub struct IpReassemblyNode {
    #[node(default)]
    handoff: Option<IpReassemblyHandoff>,
    #[node(default)]
    directory: Option<Arc<IpReassemblyDirectory>>,
    #[node(default = DEFAULT_REASSEMBLY_TIMEOUT)]
    timeout: Duration,
    #[node(default = DEFAULT_MAX_REASSEMBLIES)]
    max_reassemblies: usize,
    #[node(default = DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY)]
    max_fragments_per_reassembly: usize,
}

impl IpReassemblyNode {
    #[inline]
    pub fn with_handoff(mut self, handoff: IpReassemblyHandoff) -> Self {
        self.directory = Some(Arc::new(handoff.directory.clone()));
        self.handoff = Some(handoff);
        self
    }

    #[inline]
    pub fn with_directory(mut self, directory: Arc<IpReassemblyDirectory>) -> Self {
        self.directory = Some(directory);
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
    pub fn expire(&mut self, runtime: &DataPlaneRuntime, now: Instant) -> usize {
        ensure_worker(self);
        with_worker_mut(|worker| worker.expire(runtime, now)).unwrap_or(0)
    }
}

#[hammer_component_macros::node(role = internal)]
#[derive(Clone, Default)]
pub struct IpReassemblyExpireWalk {
    #[node(default)]
    reassembly: Option<IpReassemblyNode>,
}

impl IpReassemblyExpireWalk {
    #[inline]
    pub fn with_reassembly(mut self, reassembly: IpReassemblyNode) -> Self {
        self.reassembly = Some(reassembly);
        self
    }
}

impl Node for IpReassemblyExpireWalk {
    #[inline]
    fn process(&mut self, runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        if let Some(node) = self.reassembly.as_mut() {
            let _ = node.expire(runtime, Instant::now());
        }
        NodeResult::drop()
    }
}


impl IpReassemblyWorker {
    fn expire(&mut self, runtime: &DataPlaneRuntime, now: Instant) -> usize {
        let timeout = self.timeout;
        let mut expired_keys = Vec::new();
        for (index, context) in self.contexts.iter() {
            if now.duration_since(context.updated_at) > timeout {
                expired_keys.push((index, context.key));
            }
        }
        let count = expired_keys.len();
        for (index, key) in expired_keys {
            if let Some(context) = self.contexts.remove(index) {
                let _ = context.drop_fragments(runtime);
            }
            if let Some(directory) = &self.directory {
                directory.remove(key);
            } else if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
        }
        count
    }

    fn process_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        now: Instant,
    ) -> NodeResult {
        let input_len = frame.len();
        debug_assert!(input_len <= DEFAULT_BUFFER_FRAME_CAPACITY);
        let mut inputs = [core::mem::MaybeUninit::<Index>::uninit(); DEFAULT_BUFFER_FRAME_CAPACITY];
        for (offset, &index) in frame.indices().iter().enumerate() {
            inputs[offset].write(index);
        }
        frame.discard_prefix(input_len);

        let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        let mut out_len = 0usize;
        for offset in 0..input_len {
            let index = unsafe { inputs[offset].assume_init() };
            let _ = self.process_index(runtime, index, now, frame, &mut nexts, &mut out_len);
        }
        if out_len != 0 {
            runtime.enqueue_to_next(frame, &nexts[..out_len]);
        }
        NodeResult::drop()
    }

    #[inline]
    fn emit_local(
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
        out_len: &mut usize,
        next: IpReassemblyNext,
        index: Index,
    ) -> CoreResult<()> {
        if *out_len == DEFAULT_BUFFER_FRAME_CAPACITY {
            runtime.enqueue_to_next(frame, &nexts[..*out_len]);
            *out_len = 0;
        }
        nexts[*out_len] = NodeNext::slot(next);
        frame.push_index(index)?;
        *out_len += 1;
        Ok(())
    }

    fn process_index(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: Index,
        now: Instant,
        out_frame: &mut BufferFrame,
        nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
        out_len: &mut usize,
    ) -> CoreResult<()> {
        let current_worker = self.worker;
        let buffer = runtime.get_buffer(index)?;
        let fragment = match parse_ip_fragment_with_chain_len(
            buffer.current(),
            buffer.total_len_not_including_first(),
        ) {
            Ok(fragment) => fragment,
            Err(_) => {
                drop(buffer);
                let drop_next = IpReassemblyNext::Drop;
                let _ = add_packet_trace!(
                    runtime,
                    index,
                    IpReassemblyTrace {
                        key: None,
                        action: IpReassemblyTraceAction::Drop,
                        current_worker,
                        owner_worker: None,
                        next: Some(NodeNext::slot(drop_next)),
                    },
                );
                Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
                return Ok(());
            }
        };
        drop(buffer);

        let key = fragment.key;
        let directory = self
            .directory
            .as_ref()
            .map(|d| d.as_ref())
            .or_else(|| self.handoff.as_ref().map(|h| &h.directory));

        // Memory-owner handoff before touching local pool.
        if let (Some(directory), Some(handoff)) = (directory, self.handoff.as_ref()) {
            if let Some((_, owner)) = directory.lookup(key) {
                if owner != current_worker {
                    let _ = add_packet_trace!(
                        runtime,
                        index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Handoff,
                            current_worker,
                            owner_worker: Some(owner),
                            next: None,
                        },
                    );
                    runtime.handoff_index(owner, handoff.reassembly, index, None::<u16>)?;
                    return Ok(());
                }
            }
        }

        let pool_index = match directory.and_then(|d| d.lookup(key)) {
            Some((pool_index, owner)) if owner == current_worker => pool_index,
            Some(_) => {
                // Owned elsewhere — should have handed off above.
                let drop_next = IpReassemblyNext::Drop;
                Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
                return Ok(());
            }
            None => {
                if self.contexts.len() >= self.max_reassemblies {
                    let drop_next = IpReassemblyNext::Drop;
                    let _ = add_packet_trace!(
                        runtime,
                        index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Drop,
                            current_worker,
                            owner_worker: Some(current_worker),
                            next: Some(NodeNext::slot(drop_next)),
                        },
                    );
                    Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
                    return Ok(());
                }
                let ctx_index = self
                    .contexts
                    .insert(FragmentContext::new(key, fragment.version, now))
                    .ok_or_else(|| CoreError::internal("reassembly pool full"))?;
                if let Some(directory) = directory {
                    let (owner, created) = directory.claim_or_lookup(key, ctx_index, current_worker);
                    if !created {
                        let _ = self.contexts.remove(ctx_index);
                        if owner != current_worker {
                            if let Some(handoff) = &self.handoff {
                                let _ = add_packet_trace!(
                                    runtime,
                                    index,
                                    IpReassemblyTrace {
                                        key: Some(key),
                                        action: IpReassemblyTraceAction::Handoff,
                                        current_worker,
                                        owner_worker: Some(owner),
                                        next: None,
                                    },
                                );
                                runtime.handoff_index(owner, handoff.reassembly, index, None::<u16>)?;
                                return Ok(());
                            }
                        }
                        // Lost race to same worker — look up again.
                        if let Some((idx, _)) = directory.lookup(key) {
                            idx
                        } else {
                            return Ok(());
                        }
                    } else {
                        ctx_index
                    }
                } else {
                    ctx_index
                }
            }
        };

        let mut reassembled = None;
        let mut failed = None;
        let mut pending_sendout = None;
        let mut drop_trace = None;
        {
            let context = self
                .contexts
                .get_mut(pool_index)
                .ok_or_else(|| CoreError::internal("missing fragment context"))?;
            if fragment.payload_offset == 0 {
                context.sendout_worker = Some(current_worker);
            }
            let outcome = context.insert_fragment(
                runtime,
                index,
                fragment,
                now,
                self.max_fragments_per_reassembly,
            )?;
            match outcome {
                ReassemblyInsert::Pending => {
                    pending_sendout = context.sendout_worker.or(Some(current_worker));
                }
                ReassemblyInsert::Drop(index) => {
                    drop_trace = Some((index, context.sendout_worker.unwrap_or(current_worker)));
                }
                ReassemblyInsert::Reassembled(index) => reassembled = Some(index),
                ReassemblyInsert::Failed(index) => failed = Some(index),
            }
        }

        if let Some(owner) = pending_sendout {
            let _ = add_packet_trace!(
                runtime,
                index,
                IpReassemblyTrace {
                    key: Some(key),
                    action: IpReassemblyTraceAction::Pending,
                    current_worker,
                    owner_worker: Some(owner),
                    next: None,
                },
            );
        }

        if let Some((index, owner)) = drop_trace {
            let drop_next = IpReassemblyNext::Drop;
            let _ = add_packet_trace!(
                runtime,
                index,
                IpReassemblyTrace {
                    key: Some(key),
                    action: IpReassemblyTraceAction::Drop,
                    current_worker,
                    owner_worker: Some(owner),
                    next: Some(NodeNext::slot(drop_next)),
                },
            );
            Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
            return Ok(());
        }

        if let Some(failed_index) = failed {
            let drop_next = IpReassemblyNext::Drop;
            let drop_slot = NodeNext::slot(drop_next);
            if let Some(context) = self.contexts.remove(pool_index) {
                let sendout = context.sendout_worker.unwrap_or(current_worker);
                for fragment in context.fragments {
                    let _ = add_packet_trace!(
                        runtime,
                        fragment.index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Failed,
                            current_worker,
                            owner_worker: Some(sendout),
                            next: Some(drop_slot),
                        },
                    );
                    Self::emit_local(
                        runtime,
                        out_frame,
                        nexts,
                        out_len,
                        drop_next,
                        fragment.index,
                    )?;
                }
            }
            let _ = add_packet_trace!(
                runtime,
                failed_index,
                IpReassemblyTrace {
                    key: Some(key),
                    action: IpReassemblyTraceAction::Failed,
                    current_worker,
                    owner_worker: Some(current_worker),
                    next: Some(drop_slot),
                },
            );
            Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, failed_index)?;
            if let Some(directory) = &self.directory {
                directory.remove(key);
            } else if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
            return Ok(());
        }

        if let Some(index) = reassembled {
            let sendout = self
                .contexts
                .get(pool_index)
                .and_then(|context| context.sendout_worker)
                .unwrap_or(current_worker);
            let _ = self.contexts.remove(pool_index);
            if let Some(directory) = &self.directory {
                directory.remove(key);
            } else if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
            refresh_metadata(runtime, index)?;
            if let Some(handoff) = &self.handoff {
                if sendout != current_worker {
                    let _ = add_packet_trace!(
                        runtime,
                        index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Handoff,
                            current_worker,
                            owner_worker: Some(sendout),
                            next: None,
                        },
                    );
                    runtime.handoff_index(sendout, handoff.input, index, None::<u16>)?;
                    return Ok(());
                }
            }
            let input_next = IpReassemblyNext::Input;
            let _ = add_packet_trace!(
                runtime,
                index,
                IpReassemblyTrace {
                    key: Some(key),
                    action: IpReassemblyTraceAction::Reassembled,
                    current_worker,
                    owner_worker: Some(sendout),
                    next: Some(NodeNext::slot(input_next)),
                },
            );
            Self::emit_local(runtime, out_frame, nexts, out_len, input_next, index)?;
        }
        Ok(())
    }
}

impl Node for IpReassemblyNode {
    #[inline(always)]
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_reassembly_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        ensure_worker(self);
        Ok(NodeRuntimeData::empty())
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_reassembly_trace)
    }
}

fn ip_reassembly_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    // Config is on TLS worker; node_process closure cannot access &self easily
    // beyond ensuring worker exists via prior node_runtime_data / expire.
    WORKER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(worker) = slot.as_mut() {
            worker.process_frame(runtime, frame, Instant::now())
        } else {
            NodeResult::drop()
        }
    })
}

struct FragmentContext {
    key: IpFragmentKey,
    version: IpVersion,
    sendout_worker: Option<DataWorkerId>,
    updated_at: Instant,
    total_payload_len: Option<usize>,
    fragments: Vec<ReassemblyFragment>,
}

impl FragmentContext {
    #[inline]
    fn new(key: IpFragmentKey, version: IpVersion, now: Instant) -> Self {
        Self {
            key,
            version,
            sendout_worker: None,
            updated_at: now,
            total_payload_len: None,
            fragments: Vec::new(),
        }
    }

    #[inline]
    fn insert_fragment(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: Index,
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
    fn assemble(
        &mut self,
        runtime: &DataPlaneRuntime,
        total_payload_len: usize,
    ) -> CoreResult<ReassemblyInsert> {
        match self.version {
            IpVersion::V4 => self.assemble_ipv4_chain(runtime, total_payload_len),
            IpVersion::V6 => self.assemble_ipv6_chain(runtime, total_payload_len),
        }
    }

    #[inline]
    fn assemble_ipv4_chain(
        &mut self,
        runtime: &DataPlaneRuntime,
        total_payload_len: usize,
    ) -> CoreResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        let header_len = first.header_len;
        if header_len < IPV4_HEADER_MIN_LEN
            || runtime.get_buffer(first.index)?.current_len() < header_len
        {
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
                let mut buffer = runtime.get_buffer_mut(complete)?;
                buffer.truncate(fragment.header_len + (fragment.end - fragment.start))?;
            } else {
                trim_fragment_payload_chain(runtime, fragment)?;
                runtime.buffers().chain_buffer(complete, fragment.index)?;
            }
        }
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
    fn assemble_ipv6_chain(
        &mut self,
        runtime: &DataPlaneRuntime,
        total_payload_len: usize,
    ) -> CoreResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        if runtime.get_buffer(first.index)?.current_len()
            < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN
        {
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
                let mut buffer = runtime.get_buffer_mut(complete)?;
                {
                    let packet = buffer.current_mut();
                    packet.copy_within(
                        IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN
                            ..IPV6_HEADER_LEN
                                + (fragment.end - fragment.start)
                                + IPV6_FRAGMENT_HEADER_LEN,
                        IPV6_HEADER_LEN,
                    );
                }
                let header = &mut buffer.current_mut()[..IPV6_HEADER_LEN];
                header[IPV6_PAYLOAD_LENGTH_OFFSET..IPV6_PAYLOAD_LENGTH_OFFSET + 2]
                    .copy_from_slice(&(payload_len as u16).to_be_bytes());
                header[IPV6_NEXT_HEADER_OFFSET] = fragment_next_header;
                drop(buffer);
                runtime
                    .get_buffer_mut(complete)?
                    .truncate(IPV6_HEADER_LEN + (fragment.end - fragment.start))?;
            } else {
                trim_fragment_payload_chain(runtime, fragment)?;
                runtime.buffers().chain_buffer(complete, fragment.index)?;
            }
        }
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
    fn drop_fragments(self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        let mut owner = runtime.buffers().get_next_frame(NodeId::new(0))?;
        for fragment in self.fragments {
            if owner.push_index(fragment.index).is_err() {
                owner = runtime.buffers().get_next_frame(NodeId::new(0))?;
                owner.push_index(fragment.index)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ReassemblyFragment {
    index: Index,
    start: usize,
    end: usize,
    header_len: usize,
}

enum ReassemblyInsert {
    Pending,
    Drop(Index),
    Reassembled(Index),
    Failed(Index),
}

#[inline(always)]
fn refresh_metadata(runtime: &DataPlaneRuntime, index: Index) -> CoreResult<()> {
    let buffer = runtime.get_buffer(index)?;
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let parsed = ip_header(buffer.current(), network.packet_cursor())?;
    drop(buffer);
    match network_for_protocol(parsed.protocol) {
        Some(_) => Ok(()),
        None => {
            let IpProtocol::Other(protocol) = parsed.protocol else {
                return Err(CoreError::internal(format!(
                    "unsupported reassembled transport protocol: {:?}",
                    parsed.protocol
                )));
            };
            Err(CoreError::internal(format!(
                "unsupported reassembled transport protocol: {protocol}"
            )))
        }
    }
}

#[inline(always)]
fn trim_fragment_payload_chain(
    runtime: &DataPlaneRuntime,
    fragment: ReassemblyFragment,
) -> CoreResult<()> {
    let payload_len = fragment.end - fragment.start;
    let mut buffer = runtime.get_buffer_mut(fragment.index)?;
    buffer.advance(fragment.header_len as isize)?;
    buffer.truncate(payload_len)
}

#[inline(always)]
fn update_ipv4_header_checksum(packet: &mut [u8], header_len: usize) {
    packet[IPV4_HEADER_CHECKSUM_OFFSET] = 0;
    packet[IPV4_HEADER_CHECKSUM_OFFSET + 1] = 0;
    let checksum = internet_checksum(&packet[..header_len]);
    packet[IPV4_HEADER_CHECKSUM_OFFSET..IPV4_HEADER_CHECKSUM_OFFSET + 2]
        .copy_from_slice(&checksum.to_be_bytes());
}

fn expire_ip_reassembly_main_loop_callback() {
    let _ = hammer_runtime::with_data_plane_runtime(|runtime| {
        WORKER.with(|slot| {
            if let Some(worker) = slot.borrow_mut().as_mut() {
                let _ = worker.expire(runtime, Instant::now());
            }
        });
    });
}

#[linkme::distributed_slice(hammer_runtime::init::MAIN_LOOP_CALLBACKS)]
static EXPIRE_IP_REASSEMBLY: fn() = expire_ip_reassembly_main_loop_callback;
