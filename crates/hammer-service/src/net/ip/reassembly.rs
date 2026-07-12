use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem::transmute;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeHandle, NodeId, NodeNext,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::checksum::internet_checksum;
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

#[hammer_component_macros::node_next]
pub enum IpReassemblyNext {
    Lookup,
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

#[hammer_component_macros::node(role = internal, next = IpReassemblyNext)]
#[derive(Clone)]
pub struct IpReassemblyNode {
    #[node(default = register_ip_reassembly_runtime())]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    handoff: Option<IpReassemblyHandoff>,
    #[node(default = DEFAULT_REASSEMBLY_TIMEOUT)]
    timeout: Duration,
    #[node(default = DEFAULT_MAX_REASSEMBLIES)]
    max_reassemblies: usize,
    #[node(default = DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY)]
    max_fragments_per_reassembly: usize,
}

struct IpReassemblyRuntime {
    handoff: Option<IpReassemblyHandoff>,
    timeout: Duration,
    max_reassemblies: usize,
    max_fragments_per_reassembly: usize,
    contexts: HashMap<IpFragmentKey, ReassemblyContext>,
    failed_keys: Vec<IpFragmentKey>,
}

impl Default for IpReassemblyRuntime {
    fn default() -> Self {
        Self {
            handoff: None,
            timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            max_reassemblies: DEFAULT_MAX_REASSEMBLIES,
            max_fragments_per_reassembly: DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY,
            contexts: HashMap::new(),
            failed_keys: Vec::new(),
        }
    }
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
        set_ip_reassembly_runtime_handoff(self.runtime_data, Some(handoff.clone()))
            .expect("IP reassembly runtime slot");
        self.handoff = Some(handoff);
        self
    }

    #[inline]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        set_ip_reassembly_runtime_timeout(self.runtime_data, timeout)
            .expect("IP reassembly runtime slot");
        self.timeout = timeout;
        self
    }

    #[inline]
    pub fn with_max_fragments_per_reassembly(mut self, max_fragments: usize) -> Self {
        set_ip_reassembly_runtime_max_fragments(self.runtime_data, max_fragments)
            .expect("IP reassembly runtime slot");
        self.max_fragments_per_reassembly = max_fragments;
        self
    }

    #[inline]
    pub fn expire(&mut self, runtime: &DataPlaneRuntime, now: Instant) -> usize {
        let Ok(expired) = expire_ip_reassembly_runtime(self.runtime_data, runtime, now) else {
            return 0;
        };
        expired
    }
}

impl IpReassemblyRuntime {
    #[inline]
    fn sync_config(
        &mut self,
        handoff: Option<IpReassemblyHandoff>,
        timeout: Duration,
        max_reassemblies: usize,
        max_fragments_per_reassembly: usize,
    ) {
        self.handoff = handoff;
        self.timeout = timeout;
        self.max_reassemblies = max_reassemblies;
        self.max_fragments_per_reassembly = max_fragments_per_reassembly;
    }

    #[inline]
    fn expire(&mut self, runtime: &DataPlaneRuntime, now: Instant) -> CoreResult<usize> {
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
                context.drop_fragments(runtime)?;
            }
            if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
        }
        Ok(expired_len)
    }

    fn process_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        now: Instant,
    ) -> NodeResult {
        self.failed_keys.clear();
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
        debug_assert_eq!(*out_len, frame.len());
        Ok(())
    }

    #[inline]
    fn process_index(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: Index,
        now: Instant,
        out_frame: &mut BufferFrame,
        nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
        out_len: &mut usize,
    ) -> CoreResult<()> {
        let buffer = runtime.get_buffer(index)?;
        let current_worker = self.current_worker();
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
        if self.failed_keys.contains(&key) {
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
        let fragment_first_worker = self.fragment_first_worker(runtime, index, fragment)?;
        if let Some(handoff) = &self.handoff {
            let owner = handoff.directory.owner_or_insert(key, handoff.worker);
            if owner != handoff.worker {
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
        if !self.contexts.contains_key(&key) {
            if self.contexts.len() == self.max_reassemblies {
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
        let mut pending_trace_owner = None;
        let mut drop_trace = None;
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
                ReassemblyInsert::Pending => {
                    pending_trace_owner = Some(context.first_fragment_worker);
                }
                ReassemblyInsert::Drop(index) => {
                    drop_trace = Some((index, context.first_fragment_worker));
                }
                ReassemblyInsert::Reassembled(index) => reassembled = Some(index),
                ReassemblyInsert::Failed(index) => failed = Some(index),
            }
        }

        if let Some(owner) = pending_trace_owner {
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
            if let Some(context) = self.contexts.remove(&key) {
                for fragment in context.fragments {
                    let _ = add_packet_trace!(
                        runtime,
                        fragment.index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Failed,
                            current_worker,
                            owner_worker: Some(context.first_fragment_worker),
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
            if !self.failed_keys.contains(&key) {
                self.failed_keys.push(key);
            }
            if let Some(handoff) = &self.handoff {
                handoff.directory.remove(key);
            }
            return Ok(());
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
                    let _ = add_packet_trace!(
                        runtime,
                        index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Handoff,
                            current_worker,
                            owner_worker: Some(first_worker),
                            next: None,
                        },
                    );
                    runtime.handoff_index(first_worker, handoff.lookup, index, None::<u16>)?;
                } else {
                    let lookup_next = IpReassemblyNext::Lookup;
                    let _ = add_packet_trace!(
                        runtime,
                        index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Reassembled,
                            current_worker,
                            owner_worker: Some(first_worker),
                            next: Some(NodeNext::slot(lookup_next)),
                        },
                    );
                    Self::emit_local(runtime, out_frame, nexts, out_len, lookup_next, index)?;
                    return Ok(());
                }
            } else {
                let lookup_next = IpReassemblyNext::Lookup;
                let _ = add_packet_trace!(
                    runtime,
                    index,
                    IpReassemblyTrace {
                        key: Some(key),
                        action: IpReassemblyTraceAction::Reassembled,
                        current_worker,
                        owner_worker: first_worker,
                        next: Some(NodeNext::slot(lookup_next)),
                    },
                );
                Self::emit_local(runtime, out_frame, nexts, out_len, lookup_next, index)?;
                return Ok(());
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn fragment_first_worker(
        &self,
        runtime: &DataPlaneRuntime,
        index: Index,
        fragment: ParsedIpFragment,
    ) -> CoreResult<Option<DataWorkerId>> {
        if fragment.payload_offset != 0 {
            return Ok(None);
        }
        if fragment.payload_offset == 0 {
            let buffer = runtime.get_buffer(index)?;
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
            if let Some(worker) = network.handoff_source_worker() {
                return Ok(Some(DataWorkerId::new(u32::from(worker))));
            }
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
        sync_ip_reassembly_runtime(
            self.runtime_data,
            self.handoff.clone(),
            self.timeout,
            self.max_reassemblies,
            self.max_fragments_per_reassembly,
        )?;
        Ok(self.runtime_data)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_reassembly_trace)
    }
}

fn ip_reassembly_runtimes() -> &'static Mutex<Vec<IpReassemblyRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<IpReassemblyRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_ip_reassembly_runtime() -> NodeRuntimeData {
    let mut runtimes = ip_reassembly_runtimes()
        .lock()
        .expect("IP reassembly runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(IpReassemblyRuntime::default());
    NodeRuntimeData::from_usize(slot).expect("IP reassembly runtime slot overflow")
}

fn set_ip_reassembly_runtime_handoff(
    data: NodeRuntimeData,
    handoff: Option<IpReassemblyHandoff>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_reassembly_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP reassembly runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP reassembly runtime slot is invalid"))?;
    runtime.handoff = handoff;
    Ok(())
}

fn set_ip_reassembly_runtime_timeout(data: NodeRuntimeData, timeout: Duration) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_reassembly_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP reassembly runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP reassembly runtime slot is invalid"))?;
    runtime.timeout = timeout;
    Ok(())
}

fn set_ip_reassembly_runtime_max_fragments(
    data: NodeRuntimeData,
    max_fragments: usize,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_reassembly_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP reassembly runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP reassembly runtime slot is invalid"))?;
    runtime.max_fragments_per_reassembly = max_fragments;
    Ok(())
}

fn sync_ip_reassembly_runtime(
    data: NodeRuntimeData,
    handoff: Option<IpReassemblyHandoff>,
    timeout: Duration,
    max_reassemblies: usize,
    max_fragments_per_reassembly: usize,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_reassembly_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP reassembly runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP reassembly runtime slot is invalid"))?;
    runtime.sync_config(
        handoff,
        timeout,
        max_reassemblies,
        max_fragments_per_reassembly,
    );
    Ok(())
}

fn expire_ip_reassembly_runtime(
    data: NodeRuntimeData,
    runtime: &DataPlaneRuntime,
    now: Instant,
) -> CoreResult<usize> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_reassembly_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP reassembly runtime registry poisoned"))?;
    let state = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP reassembly runtime slot is invalid"))?;
    state.expire(runtime, now)
}

fn ip_reassembly_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    (|| -> CoreResult<NodeResult> {
        let slot = data.usize_word(0)?;
        let mut runtimes = ip_reassembly_runtimes()
            .lock()
            .map_err(|_| CoreError::internal("IP reassembly runtime registry poisoned"))?;
        let state = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("IP reassembly runtime slot is invalid"))?;
        Ok(state.process_frame(runtime, frame, Instant::now()))
    })()
    .unwrap_or_else(|_| NodeResult::drop())
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
