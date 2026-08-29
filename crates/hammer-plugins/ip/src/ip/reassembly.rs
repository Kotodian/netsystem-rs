use std::mem::transmute;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeHandle, NodeId, NodeNext,
};
use hammer_infra::bihash::{Bihash, FREE_U64};
use hammer_infra::checksum::internet_checksum;
use hammer_infra::pool::Pool;
use hammer_runtime::sync::SpinLock;
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
    TraceFormatter, add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use crate::config::{NetworkIpConfig, ReassemblyConfig};
use crate::ip::{
    IpFragmentKey, IpProtocol, IpVersion, ParsedIpFragment, ip_header, network_for_protocol,
    parse_ip_fragment_with_chain_len,
};
use hammer_service::opaque::NetworkOpaque;

const IPV4_HEADER_MIN_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV6_FRAGMENT_HEADER_LEN: usize = 8;
const IPV4_FLAGS_FRAGMENT_OFFSET: usize = 6;
const IPV4_TOTAL_LENGTH_OFFSET: usize = 2;
const IPV4_HEADER_CHECKSUM_OFFSET: usize = 10;
const IPV6_PAYLOAD_LENGTH_OFFSET: usize = 4;
const IPV6_NEXT_HEADER_OFFSET: usize = 6;
const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_millis(100);
const REASSEMBLY_EXPIRE_WALK_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_MAX_REASSEMBLIES: usize = 1024;
const DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY: usize = 64;

#[inline]
pub fn pack_fragment_owner_value(index: u32, owner: DataWorkerId) -> u64 {
    debug_assert!(owner.slot() <= u16::MAX as usize);
    let value = u64::from(index) | (u64::from(owner.slot() as u16) << 48);
    debug_assert_ne!(value, FREE_U64);
    value
}

#[inline]
pub fn unpack_fragment_owner_value(value: u64) -> (u32, DataWorkerId) {
    let index = value as u32;
    let owner = DataWorkerId::new(u32::from((value >> 48) as u16));
    (index, owner)
}

#[hammer_component_macros::node_next]
pub enum IpReassemblyNext {
    #[next("ip-input")]
    Input,
    #[next("drop")]
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IpReassemblyTraceAction {
    Pending,
    Drop,
    Reassembled,
    Handoff,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IpReassemblyTrace {
    pub key: Option<IpFragmentKey>,
    pub action: IpReassemblyTraceAction,
    pub current_worker: DataWorkerId,
    pub owner_worker: Option<DataWorkerId>,
    pub next: Option<u16>,
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
        index: u32,
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
    pub fn lookup(&self, key: IpFragmentKey) -> Option<(u32, DataWorkerId)> {
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
    last_id: usize,
}

struct IpReassemblyMain {
    per_thread_data: Vec<SpinLock<IpReassemblyWorker>>,
}

static IP_REASSEMBLY_MAIN: ArcSwapOption<IpReassemblyMain> = ArcSwapOption::const_empty();

impl IpReassemblyMain {
    fn new(worker_count: usize, config: &ReassemblyConfig) -> Arc<Self> {
        let directory = Arc::new(IpReassemblyDirectory::new(
            u32::try_from(config.max_reassemblies).unwrap_or(u32::MAX),
        ));
        let mut per_thread_data = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            per_thread_data.push(SpinLock::new(IpReassemblyWorker {
                worker: DataWorkerId::new(worker as u32),
                contexts: Pool::with_capacity(config.max_reassemblies),
                directory: Some(Arc::clone(&directory)),
                handoff: None,
                timeout: config.timeout,
                max_reassemblies: config.max_reassemblies,
                max_fragments_per_reassembly: config.max_fragments_per_reassembly,
                last_id: 0,
            }));
        }
        Arc::new(Self { per_thread_data })
    }

    fn worker_slot(runtime: &DataPlaneRuntime) -> usize {
        runtime.thread_index().saturating_sub(1) as usize
    }

    fn expire_all(&self, runtime: &DataPlaneRuntime, now: Instant) -> usize {
        self.per_thread_data
            .iter()
            .map(|worker| worker.lock().expire(runtime, now))
            .sum()
    }

    fn expire_worker(&self, runtime: &DataPlaneRuntime, now: Instant) -> usize {
        self.per_thread_data
            .get(Self::worker_slot(runtime))
            .map(|worker| worker.lock().expire(runtime, now))
            .unwrap_or(0)
    }

    fn process_frame(&self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let Some(worker) = self.per_thread_data.get(Self::worker_slot(runtime)) else {
            return NodeResult::drop();
        };
        worker.lock().process_frame(runtime, frame, Instant::now())
    }
}

#[hammer_component_macros::config_function(
    name = "ip_reassembly_config",
    section = "network",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_ip_reassembly(
    config: NetworkIpConfig,
    engine: &mut hammer_runtime::Engine,
) -> RuntimeResult<()> {
    config.ip.reassembly.validate()?;
    let main = IpReassemblyMain::new(engine.configured_worker_count(), &config.ip.reassembly);
    IP_REASSEMBLY_MAIN.store(Some(main));
    Ok(())
}

/// Recoverable IP reassembly failures surfaced to the node drop path.
#[hammer_component_macros::runtime_error(subsystem = "ip")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum IpReassemblyError {
    #[error("IP reassembly context pool is exhausted")]
    ContextPoolExhausted,
    #[error("IP fragment context is missing")]
    FragmentContextMissing,
    #[error("IP fragment payload range overflows")]
    FragmentRangeOverflow,
    #[error("reassembled IP packet has an invalid fragment header")]
    FragmentHeaderInvalid,
    #[error("reassembled IP packet exceeds the maximum IP length")]
    PacketTooLarge,
    #[error("IP fragments have no zero-offset first fragment")]
    FirstFragmentMissing,
    #[error("reassembled IP packet carries unsupported transport protocol {protocol}")]
    Unsupportedu8 { protocol: u8 },
}

#[hammer_component_macros::init_function(
    name = "ip_reassembly_init",
    runs_before = ["install_packet_graph"]
)]
fn init_ip_reassembly() -> RuntimeResult<()> {
    if IP_REASSEMBLY_MAIN.load().is_none() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "ip" });
    }
    Ok(())
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip_reassembly,
    role = internal,
    next = IpReassemblyNext,
)]
#[derive(Clone)]
pub struct IpReassemblyNode {
    #[node(default)]
    handoff: Option<IpReassemblyHandoff>,
    #[node(default)]
    directory: Option<Arc<IpReassemblyDirectory>>,
    #[node(default = DEFAULT_REASSEMBLY_TIMEOUT)]
    timeout: Duration,
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
        IP_REASSEMBLY_MAIN
            .load_full()
            .map(|main| main.expire_worker(runtime, now))
            .unwrap_or(0)
    }
}

fn register_ip_reassembly(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        IpReassemblyNode::new(),
        &IpReassemblyNext::NEXT_NAMES,
    )
}

#[hammer_component_macros::process_node(name = "ip-reassembly-expire-walk")]
async fn ip_reassembly_expire_process(
    mut context: hammer_runtime::ProcessContext,
) -> RuntimeResult<()> {
    // VPP `ip4_full_reass_walk_expired` reads the module-global main directly;
    // the config phase stores it before Process Nodes start.
    let main = IP_REASSEMBLY_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    loop {
        let _ = context
            .wait_for_event_or_clock(REASSEMBLY_EXPIRE_WALK_INTERVAL)
            .await;
        let _ = main.expire_all(context.data_plane_runtime(), Instant::now());
    }
}

impl IpReassemblyWorker {
    fn expire(&mut self, runtime: &DataPlaneRuntime, now: Instant) -> usize {
        let timeout = self.timeout;
        let capacity = self.contexts.capacity();
        let walk_len = self
            .max_reassemblies
            .saturating_mul(REASSEMBLY_EXPIRE_WALK_INTERVAL.as_millis() as usize)
            / 1_000
            + 1;
        let begin = self.last_id.min(capacity);
        let end = begin.saturating_add(walk_len).min(capacity);
        self.last_id = if end == capacity { 0 } else { end };
        let mut expired_keys = Vec::new();
        for (index, context) in self.contexts.iter() {
            let position = index as usize;
            if position >= begin
                && position < end
                && now.duration_since(context.updated_at) > timeout
            {
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
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<()> {
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
                let ctx_index =
                    self.contexts
                        .insert(FragmentContext::new(key, fragment.version, now));
                if let Some(directory) = directory {
                    let (owner, created) =
                        directory.claim_or_lookup(key, ctx_index, current_worker);
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
                                runtime.handoff_index(
                                    owner,
                                    handoff.reassembly,
                                    index,
                                    None::<u16>,
                                )?;
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
                .ok_or(IpReassemblyError::FragmentContextMissing)?;
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
            let Some(context) = self.contexts.remove(pool_index) else {
                return Err(IpReassemblyError::FragmentContextMissing.into());
            };
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
            let _ = self
                .contexts
                .remove(pool_index)
                .ok_or(IpReassemblyError::FragmentContextMissing)?;
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
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::empty())
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpReassemblyTrace))
    }
}

fn ip_reassembly_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    IP_REASSEMBLY_MAIN
        .load_full()
        .map(|main| main.process_frame(runtime, frame))
        .unwrap_or_else(NodeResult::drop)
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
    ) -> RuntimeResult<ReassemblyInsert> {
        self.updated_at = now;
        let start = fragment.payload_offset;
        let end = start
            .checked_add(fragment.payload_len)
            .ok_or(IpReassemblyError::FragmentRangeOverflow)?;
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
    ) -> RuntimeResult<ReassemblyInsert> {
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
    ) -> RuntimeResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        let header_len = first.header_len;
        if header_len < IPV4_HEADER_MIN_LEN
            || runtime.get_buffer(first.index)?.current_len() < header_len
        {
            return Err(IpReassemblyError::FragmentHeaderInvalid.into());
        }
        let total_len = header_len
            .checked_add(total_payload_len)
            .ok_or(IpReassemblyError::FragmentRangeOverflow)?;
        if total_len > u16::MAX as usize {
            return Err(IpReassemblyError::PacketTooLarge.into());
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
                return Err(IpReassemblyError::FragmentHeaderInvalid.into());
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
    ) -> RuntimeResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        if runtime.get_buffer(first.index)?.current_len()
            < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN
        {
            return Err(IpReassemblyError::FragmentHeaderInvalid.into());
        }
        let payload_len = total_payload_len;
        if payload_len > u16::MAX as usize {
            return Err(IpReassemblyError::PacketTooLarge.into());
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
    fn first_fragment_offset(&self) -> RuntimeResult<usize> {
        self.fragments
            .iter()
            .position(|fragment| fragment.start == 0)
            .ok_or_else(|| IpReassemblyError::FirstFragmentMissing.into())
    }

    #[inline]
    fn drop_fragments(self, runtime: &DataPlaneRuntime) -> RuntimeResult<()> {
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
fn refresh_metadata(runtime: &DataPlaneRuntime, index: Index) -> RuntimeResult<()> {
    let buffer = runtime.get_buffer(index)?;
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let parsed = ip_header(buffer.current(), network.packet_cursor())?;
    drop(buffer);
    match network_for_protocol(parsed.protocol) {
        Some(_) => Ok(()),
        None => {
            let protocol = match parsed.protocol {
                IpProtocol::Other(protocol) => protocol,
                IpProtocol::Icmpv4 => 1,
                IpProtocol::Tcp => 6,
                IpProtocol::Udp => 17,
                IpProtocol::Icmpv6 => 58,
            };
            Err(IpReassemblyError::Unsupportedu8 { protocol }.into())
        }
    }
}

#[inline(always)]
fn trim_fragment_payload_chain(
    runtime: &DataPlaneRuntime,
    fragment: ReassemblyFragment,
) -> RuntimeResult<()> {
    let payload_len = fragment.end - fragment.start;
    let mut buffer = runtime.get_buffer_mut(fragment.index)?;
    buffer.advance(fragment.header_len as isize)?;
    Ok(buffer.truncate(payload_len)?)
}

#[inline(always)]
fn update_ipv4_header_checksum(packet: &mut [u8], header_len: usize) {
    packet[IPV4_HEADER_CHECKSUM_OFFSET] = 0;
    packet[IPV4_HEADER_CHECKSUM_OFFSET + 1] = 0;
    let checksum = internet_checksum(&packet[..header_len]);
    packet[IPV4_HEADER_CHECKSUM_OFFSET..IPV4_HEADER_CHECKSUM_OFFSET + 2]
        .copy_from_slice(&checksum.to_be_bytes());
}
