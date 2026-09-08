use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_infra::bihash::{Bihash, FREE_U64};
use hammer_infra::checksum::internet_checksum;
use hammer_infra::pool::Pool;
use hammer_runtime::sync::SpinLock;
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, GlobalMain, Node, NodeProcessFn, NodeRuntime, TraceFormatter,
    add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use crate::config::{NetworkIpConfig, ReassemblyConfig};
use crate::ip::{
    IpFragmentKey, IpProtocol, IpVersion, ParsedIpFragment, ip_header,
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
pub enum Ip4ReassemblyNext {
    #[next("ip4-input")]
    Input,
    #[next("drop")]
    Drop,
}

#[hammer_component_macros::node_next]
pub enum Ip6ReassemblyNext {
    #[next("ip6-input")]
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
    reassembly: NodeId,
    input: NodeId,
    worker: DataWorkerId,
    directory: IpReassemblyDirectory,
}

impl IpReassemblyHandoff {
    #[inline]
    pub fn new(
        reassembly: NodeId,
        input: NodeId,
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
    pub fn reassembly(&self) -> NodeId {
        self.reassembly
    }

    #[inline]
    pub fn input(&self) -> NodeId {
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

static IP_REASSEMBLY_MAIN: OnceLock<IpReassemblyMain> = OnceLock::new();

impl IpReassemblyMain {
    fn new(worker_count: usize, config: &ReassemblyConfig) -> Self {
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
        Self { per_thread_data }
    }

    fn worker_slot(runtime: &DataPlaneMain) -> usize {
        runtime.thread_index().saturating_sub(1) as usize
    }

    fn expire_all(&self, runtime: &mut DataPlaneMain, now: Instant) -> usize {
        self.per_thread_data
            .iter()
            .map(|worker| worker.lock().expire(runtime, now))
            .sum()
    }

    fn expire_worker(&self, runtime: &mut DataPlaneMain, now: Instant) -> usize {
        self.per_thread_data
            .get(Self::worker_slot(runtime))
            .map(|worker| worker.lock().expire(runtime, now))
            .unwrap_or(0)
    }

    fn process_frame(
        &self,
        runtime: &mut DataPlaneMain,
        frame: &mut Frame,
        version: IpVersion,
    ) -> () {
        let Some(worker) = self.per_thread_data.get(Self::worker_slot(runtime)) else {
            return ();
        };
        worker
            .lock()
            .process_frame(runtime, frame, Instant::now(), version)
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
    engine: &mut hammer_runtime::GlobalMain,
) -> RuntimeResult<()> {
    config.ip.reassembly.validate()?;
    let main = IpReassemblyMain::new(engine.configured_worker_count(), &config.ip.reassembly);
    IP_REASSEMBLY_MAIN
        .set(main)
        .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
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
    if IP_REASSEMBLY_MAIN.get().is_none() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "ip" });
    }
    Ok(())
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_reassembly,
    role = internal,
    name = "ip4-reassembly",
    next = Ip4ReassemblyNext,
)]
#[derive(Clone)]
pub struct Ip4ReassemblyNode {
    #[node(default)]
    handoff: Option<IpReassemblyHandoff>,
    #[node(default)]
    directory: Option<Arc<IpReassemblyDirectory>>,
    #[node(default = DEFAULT_REASSEMBLY_TIMEOUT)]
    timeout: Duration,
    #[node(default = DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY)]
    max_fragments_per_reassembly: usize,
}

impl Ip4ReassemblyNode {
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
    pub fn expire(&mut self, runtime: &mut DataPlaneMain, now: Instant) -> usize {
        IP_REASSEMBLY_MAIN
            .get()
            .map(|main| main.expire_worker(runtime, now))
            .unwrap_or(0)
    }
}

fn register_ip4_reassembly(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip4ReassemblyNode::new(),
        &Ip4ReassemblyNext::NEXT_NAMES,
    )
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_reassembly,
    role = internal,
    name = "ip6-reassembly",
    next = Ip6ReassemblyNext,
)]
#[derive(Clone)]
pub struct Ip6ReassemblyNode {
    #[node(default)]
    handoff: Option<IpReassemblyHandoff>,
    #[node(default)]
    directory: Option<Arc<IpReassemblyDirectory>>,
    #[node(default = DEFAULT_REASSEMBLY_TIMEOUT)]
    timeout: Duration,
    #[node(default = DEFAULT_MAX_FRAGMENTS_PER_REASSEMBLY)]
    max_fragments_per_reassembly: usize,
}

impl Ip6ReassemblyNode {
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
    pub fn expire(&mut self, runtime: &mut DataPlaneMain, now: Instant) -> usize {
        IP_REASSEMBLY_MAIN
            .get()
            .map(|main| main.expire_worker(runtime, now))
            .unwrap_or(0)
    }
}

fn register_ip6_reassembly(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip6ReassemblyNode::new(),
        &Ip6ReassemblyNext::NEXT_NAMES,
    )
}

#[hammer_component_macros::process_node(name = "ip-reassembly-expire-walk")]
async fn ip_reassembly_expire_process(
    mut context: hammer_runtime::ProcessContext,
) -> RuntimeResult<()> {
    // VPP `ip4_full_reass_walk_expired` reads the module-global main directly;
    // the config phase stores it before Process Nodes start.
    let main = IP_REASSEMBLY_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    loop {
        let _ = context
            .wait_for_event_or_clock(REASSEMBLY_EXPIRE_WALK_INTERVAL)
            .await;
        let _ = GlobalMain::with_current(|engine| {
            main.expire_all(engine.data_plane_main_mut(), Instant::now())
        });
    }
}

impl IpReassemblyWorker {
    fn expire(&mut self, runtime: &mut DataPlaneMain, now: Instant) -> usize {
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
                // Like ip4_full_reass_drop_all, release each retained chain
                // before dropping its reassembly context.
                let mut indices = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
                for fragments in context.fragments.chunks(indices.len()) {
                    for (index, fragment) in indices.iter_mut().zip(fragments) {
                        *index = fragment.index;
                    }
                    runtime.buffer_free(&indices[..fragments.len()]);
                }
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
        runtime: &mut DataPlaneMain,
        frame: &mut Frame,
        now: Instant,
        version: IpVersion,
    ) -> () {
        let mut output = Frame::<(), u32, ()>::new(0);

        let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        let mut out_len = 0usize;
        for &index in frame.vector_args() {
            let _ = self.process_index(
                runtime,
                index,
                now,
                &mut output,
                &mut nexts,
                &mut out_len,
                version,
            );
        }
        if out_len != 0 {
            runtime.enqueue_to_next(&mut output, &nexts[..out_len]);
        }
        ()
    }

    #[inline]
    fn emit_local(
        runtime: &mut DataPlaneMain,
        frame: &mut Frame,
        nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
        out_len: &mut usize,
        next: u16,
        index: u32,
    ) -> RuntimeResult<()> {
        if *out_len == DEFAULT_BUFFER_FRAME_CAPACITY {
            runtime.enqueue_to_next(frame, &nexts[..*out_len]);
            frame.set_vector_count(0);
            *out_len = 0;
        }
        nexts[*out_len] = next;
        {
            let count = frame.len();
            frame.set_vector_count(count + 1);
            frame.vector_args_mut()[count] = index;
        }
        *out_len += 1;
        Ok(())
    }

    fn process_index(
        &mut self,
        runtime: &mut DataPlaneMain,
        index: u32,
        now: Instant,
        out_frame: &mut Frame,
        nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
        out_len: &mut usize,
        version: IpVersion,
    ) -> RuntimeResult<()> {
        let current_worker = self.worker;
        let buffer = runtime.buffer(index);
        let fragment = match parse_ip_fragment_with_chain_len(
            buffer.current(),
            buffer.total_len_not_including_first(),
        ) {
            Ok(fragment) => fragment,
            Err(_) => {
                let drop_next = match version {
                    IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Drop),
                    IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Drop),
                };
                let _ = add_packet_trace!(
                    runtime,
                    index,
                    IpReassemblyTrace {
                        key: None,
                        action: IpReassemblyTraceAction::Drop,
                        current_worker,
                        owner_worker: None,
                        next: Some(drop_next),
                    },
                );
                Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
                return Ok(());
            }
        };
        if fragment.version != version {
            let next = match version {
                IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Drop),
            };
            return Self::emit_local(runtime, out_frame, nexts, out_len, next, index);
        }

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
                    if let Err(error) = runtime.handoff_index(owner, handoff.reassembly, index) {
                        // A rejected enqueue leaves this Worker owning the chain.
                        runtime.buffer_free_one(index);
                        return Err(error);
                    }
                    return Ok(());
                }
            }
        }

        let pool_index = match directory.and_then(|d| d.lookup(key)) {
            Some((pool_index, owner)) if owner == current_worker => pool_index,
            Some(_) => {
                // Owned elsewhere — should have handed off above.
                let drop_next = match version {
                    IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Drop),
                    IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Drop),
                };
                Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
                return Ok(());
            }
            None => {
                if self.contexts.len() >= self.max_reassemblies {
                    let drop_next = match version {
                        IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Drop),
                        IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Drop),
                    };
                    let _ = add_packet_trace!(
                        runtime,
                        index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Drop,
                            current_worker,
                            owner_worker: Some(current_worker),
                            next: Some(drop_next),
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
                                if let Err(error) =
                                    runtime.handoff_index(owner, handoff.reassembly, index)
                                {
                                    // A rejected enqueue leaves this Worker owning the chain.
                                    runtime.buffer_free_one(index);
                                    return Err(error);
                                }
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
            let outcome = match context.insert_fragment(
                runtime,
                index,
                fragment,
                now,
                self.max_fragments_per_reassembly,
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    // Finalization errors occur after insertion. The context
                    // still owns every root, including any partially built chain.
                    let context = self
                        .contexts
                        .remove(pool_index)
                        .expect("reassembly context remains installed during insertion");
                    let mut indices = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
                    for fragments in context.fragments.chunks(indices.len()) {
                        for (index, fragment) in indices.iter_mut().zip(fragments) {
                            *index = fragment.index;
                        }
                        runtime.buffer_free(&indices[..fragments.len()]);
                    }
                    if let Some(directory) = directory {
                        directory.remove(key);
                    }
                    return Err(error);
                }
            };
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
            let drop_next = match version {
                IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Drop),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpReassemblyTrace {
                    key: Some(key),
                    action: IpReassemblyTraceAction::Drop,
                    current_worker,
                    owner_worker: Some(owner),
                    next: Some(drop_next),
                },
            );
            Self::emit_local(runtime, out_frame, nexts, out_len, drop_next, index)?;
            return Ok(());
        }

        if let Some(failed_index) = failed {
            let drop_next = match version {
                IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Drop),
                IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Drop),
            };
            let drop_slot = drop_next;
            let Some(context) = self.contexts.remove(pool_index) else {
                return Err(IpReassemblyError::FragmentContextMissing.into());
            };
            let sendout = context.sendout_worker.unwrap_or(current_worker);
            let mut indices = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
            for fragments in context.fragments.chunks(indices.len()) {
                for (index, fragment) in indices.iter_mut().zip(fragments) {
                    let _ = add_packet_trace!(
                        runtime,
                        fragment.index,
                        IpReassemblyTrace {
                            key: Some(key),
                            action: IpReassemblyTraceAction::Failed,
                            current_worker,
                            owner_worker: Some(sendout),
                            next: None,
                        },
                    );
                    *index = fragment.index;
                }
                runtime.buffer_free(&indices[..fragments.len()]);
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
            if let Err(error) = refresh_metadata(runtime, index) {
                runtime.buffer_free_one(index);
                return Err(error);
            }
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
                    if let Err(error) = runtime.handoff_index(sendout, handoff.input, index) {
                        // A rejected enqueue leaves this Worker owning the chain.
                        runtime.buffer_free_one(index);
                        return Err(error);
                    }
                    return Ok(());
                }
            }
            let input_next = match version {
                IpVersion::V4 => NodeNext::slot(Ip4ReassemblyNext::Input),
                IpVersion::V6 => NodeNext::slot(Ip6ReassemblyNext::Input),
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpReassemblyTrace {
                    key: Some(key),
                    action: IpReassemblyTraceAction::Reassembled,
                    current_worker,
                    owner_worker: Some(sendout),
                    next: Some(input_next),
                },
            );
            Self::emit_local(runtime, out_frame, nexts, out_len, input_next, index)?;
        }
        Ok(())
    }
}

impl Node for Ip4ReassemblyNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, data, frame| {
            let processed_vectors = frame.len();
            ip_reassembly_process(runtime, data, frame, IpVersion::V4);
            processed_vectors
        };
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(NodeRuntime::empty())
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpReassemblyTrace))
    }
}

impl Node for Ip6ReassemblyNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, data, frame| {
            let processed_vectors = frame.len();
            ip_reassembly_process(runtime, data, frame, IpVersion::V6);
            processed_vectors
        };
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(NodeRuntime::empty())
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpReassemblyTrace))
    }
}

fn ip_reassembly_process(
    runtime: &mut DataPlaneMain,
    _data: &mut NodeRuntime,
    frame: &mut Frame,
    version: IpVersion,
) -> usize {
    let processed_vectors = frame.len();
    (|| {
        if let Some(main) = IP_REASSEMBLY_MAIN.get() {
            main.process_frame(runtime, frame, version);
        }
    })();
    processed_vectors
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
        runtime: &mut DataPlaneMain,
        index: u32,
        fragment: ParsedIpFragment,
        now: Instant,
        max_fragments: usize,
    ) -> RuntimeResult<ReassemblyInsert> {
        self.updated_at = now;
        let start = fragment.payload_offset;
        let Some(end) = start.checked_add(fragment.payload_len) else {
            return Ok(ReassemblyInsert::Failed(index));
        };
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
        runtime: &mut DataPlaneMain,
        total_payload_len: usize,
    ) -> RuntimeResult<ReassemblyInsert> {
        // Validate every retained range before changing any links. Both VPP
        // full-reassembly finalizers trim against the entire sub-chain length.
        for fragment in &self.fragments {
            let required = fragment
                .header_len
                .checked_add(fragment.end - fragment.start)
                .ok_or(IpReassemblyError::FragmentRangeOverflow)?;
            let mut length = 0usize;
            for buffer in runtime.chain(fragment.index) {
                assert_eq!(buffer.ref_count(), 1, "reassembly owns exclusive fragments");
                length = length
                    .checked_add(buffer.current_len())
                    .ok_or(IpReassemblyError::FragmentRangeOverflow)?;
            }
            if required > length {
                return Err(IpReassemblyError::FragmentHeaderInvalid.into());
            }
        }
        match self.version {
            IpVersion::V4 => self.assemble_ipv4_chain(runtime, total_payload_len),
            IpVersion::V6 => self.assemble_ipv6_chain(runtime, total_payload_len),
        }
    }

    #[inline]
    fn assemble_ipv4_chain(
        &mut self,
        runtime: &mut DataPlaneMain,
        total_payload_len: usize,
    ) -> RuntimeResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        let header_len = first.header_len;
        if header_len < IPV4_HEADER_MIN_LEN
            || runtime.buffer(first.index).current_len() < header_len
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
        let mut last = trim_fragment_payload_chain(runtime, &mut self.fragments[0], true);
        while self.fragments.len() > 1 {
            let tail = trim_fragment_payload_chain(runtime, &mut self.fragments[1], false);
            runtime
                .buffer_mut(last)
                .set_next_buffer(Some(self.fragments[1].index));
            last = tail;
            // The completed head now owns every kept segment of this range.
            self.fragments.remove(1);
        }
        {
            let buffer = runtime.buffer_mut(complete);
            let header = &mut buffer.current_mut()[..header_len];
            header[IPV4_TOTAL_LENGTH_OFFSET..IPV4_TOTAL_LENGTH_OFFSET + 2]
                .copy_from_slice(&(total_len as u16).to_be_bytes());
            header[IPV4_FLAGS_FRAGMENT_OFFSET..IPV4_FLAGS_FRAGMENT_OFFSET + 2]
                .copy_from_slice(&0u16.to_be_bytes());
            update_ipv4_header_checksum(header, header_len);
        }
        let tail_len = total_len - runtime.buffer(complete).current_len();
        runtime
            .buffer_mut(complete)
            .set_total_len_not_including_first(tail_len)
            .expect("validated IPv4 packet length fits Buffer chain length");
        self.fragments.clear();
        Ok(ReassemblyInsert::Reassembled(complete))
    }

    #[inline]
    fn assemble_ipv6_chain(
        &mut self,
        runtime: &mut DataPlaneMain,
        total_payload_len: usize,
    ) -> RuntimeResult<ReassemblyInsert> {
        let first_offset = self.first_fragment_offset()?;
        let first = self.fragments[first_offset];
        if runtime.buffer(first.index).current_len() < IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN {
            return Err(IpReassemblyError::FragmentHeaderInvalid.into());
        }
        let payload_len = total_payload_len;
        if payload_len > u16::MAX as usize {
            return Err(IpReassemblyError::PacketTooLarge.into());
        }
        let complete = first.index;
        let fragment_next_header = {
            let buffer = runtime.buffer(complete);
            buffer.current()[IPV6_HEADER_LEN]
        };
        let mut last = trim_fragment_payload_chain(runtime, &mut self.fragments[0], true);
        while self.fragments.len() > 1 {
            let tail = trim_fragment_payload_chain(runtime, &mut self.fragments[1], false);
            runtime
                .buffer_mut(last)
                .set_next_buffer(Some(self.fragments[1].index));
            last = tail;
            // The completed head now owns every kept segment of this range.
            self.fragments.remove(1);
        }
        {
            let buffer = runtime.buffer_mut(complete);
            let segment_len = buffer.current_len();
            // ip6_full_reass_finalize moves only bytes in the first Buffer;
            // subsequent payload segments retain their windows and storage.
            buffer.current_mut().copy_within(
                IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN..segment_len,
                IPV6_HEADER_LEN,
            );
            buffer
                .truncate(segment_len - IPV6_FRAGMENT_HEADER_LEN)
                .expect("removing the Fragment Header shortens the first segment");
            let header = &mut buffer.current_mut()[..IPV6_HEADER_LEN];
            header[IPV6_PAYLOAD_LENGTH_OFFSET..IPV6_PAYLOAD_LENGTH_OFFSET + 2]
                .copy_from_slice(&(payload_len as u16).to_be_bytes());
            header[IPV6_NEXT_HEADER_OFFSET] = fragment_next_header;
        }
        let tail_len = IPV6_HEADER_LEN + payload_len - runtime.buffer(complete).current_len();
        runtime
            .buffer_mut(complete)
            .set_total_len_not_including_first(tail_len)
            .expect("validated IPv6 packet length fits Buffer chain length");
        self.fragments.clear();
        Ok(ReassemblyInsert::Reassembled(complete))
    }

    #[inline]
    fn first_fragment_offset(&self) -> RuntimeResult<usize> {
        self.fragments
            .iter()
            .position(|fragment| fragment.start == 0)
            .ok_or_else(|| IpReassemblyError::FirstFragmentMissing.into())
    }
}

#[derive(Debug, Clone, Copy)]
struct ReassemblyFragment {
    index: u32,
    start: usize,
    end: usize,
    header_len: usize,
}

enum ReassemblyInsert {
    Pending,
    Drop(u32),
    Reassembled(u32),
    Failed(u32),
}

#[inline(always)]
fn refresh_metadata(runtime: &DataPlaneMain, index: u32) -> RuntimeResult<()> {
    let buffer = runtime.buffer(index);
    let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
    let parsed = ip_header(buffer.current(), network.packet_cursor())?;
    if !matches!(parsed.protocol, IpProtocol::Other(_)) {
        Ok(())
    } else {
        let protocol = u8::from(parsed.protocol);
        Err(IpReassemblyError::Unsupportedu8 { protocol }.into())
    }
}

#[inline(always)]
fn trim_fragment_payload_chain(
    runtime: &mut DataPlaneMain,
    fragment: &mut ReassemblyFragment,
    keep_header: bool,
) -> u32 {
    let mut skip = if keep_header { 0 } else { fragment.header_len };
    let mut remaining =
        fragment.end - fragment.start + if keep_header { fragment.header_len } else { 0 };
    let mut next = Some(fragment.index);
    let mut last = None;
    let mut discarded = [0; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut discarded_len = 0;
    while let Some(index) = next {
        let buffer = runtime.buffer_mut(index);
        next = buffer.next_buffer_slot();
        buffer.set_next_buffer(None);
        if skip > buffer.current_len() {
            skip -= buffer.current_len();
        } else if remaining != 0 {
            buffer.advance(skip as isize);
            skip = 0;
            let keep = remaining.min(buffer.current_len());
            buffer
                .truncate(keep)
                .expect("fragment trimming only shortens a segment");
            remaining -= keep;
            if let Some(last) = last {
                runtime.buffer_mut(last).set_next_buffer(Some(index));
            } else {
                fragment.index = index;
            }
            last = Some(index);
            continue;
        }
        // Full prefix/suffix segments are detached before their batch release.
        discarded[discarded_len] = index;
        discarded_len += 1;
        if discarded_len == discarded.len() {
            runtime.buffer_free_no_next(&discarded);
            discarded_len = 0;
        }
    }
    assert_eq!(
        remaining, 0,
        "validated fragment chain contains its entire range"
    );
    runtime.buffer_free_no_next(&discarded[..discarded_len]);
    last.expect("nonempty fragment range retains a segment")
}

#[inline(always)]
fn update_ipv4_header_checksum(packet: &mut [u8], header_len: usize) {
    packet[IPV4_HEADER_CHECKSUM_OFFSET] = 0;
    packet[IPV4_HEADER_CHECKSUM_OFFSET + 1] = 0;
    let checksum = internet_checksum(&packet[..header_len]);
    packet[IPV4_HEADER_CHECKSUM_OFFSET..IPV4_HEADER_CHECKSUM_OFFSET + 2]
        .copy_from_slice(&checksum.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_runtime::DataPlaneBufferConfig;
    use std::net::Ipv4Addr;

    #[test]
    fn reassembly_transfers_complete_chains_and_expires_incomplete_chains() {
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_infra::main_heap::init_default().unwrap();
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0, 1],
                2,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(DataPlaneBufferConfig {
            thread_index: 2,
            ..Default::default()
        });
        let now = Instant::now();
        let key = IpFragmentKey::V4 {
            source: Ipv4Addr::new(192, 0, 2, 1),
            destination: Ipv4Addr::new(192, 0, 2, 2),
            protocol: 17,
            identification: 7,
        };
        let directory = Arc::new(IpReassemblyDirectory::new(1));
        let mut worker = IpReassemblyWorker {
            worker: DataWorkerId::new(1),
            contexts: Pool::with_capacity(1),
            directory: Some(Arc::clone(&directory)),
            handoff: None,
            timeout: Duration::from_millis(100),
            max_reassemblies: 1,
            max_fragments_per_reassembly: 4,
            last_id: 0,
        };
        let mut indices = [0; 2];
        assert_eq!(runtime.buffer_alloc(&mut indices), 2);
        runtime.buffer_mut(indices[0]).put_uninit(28).fill(0);
        runtime.buffer_chain_buffer(indices[0], indices[1]);
        runtime.buffer_mut(indices[1]).put_uninit(8).fill(0);
        let mut context = FragmentContext::new(key, IpVersion::V4, now);
        assert!(matches!(
            context
                .insert_fragment(
                    &mut runtime,
                    indices[0],
                    ParsedIpFragment {
                        version: IpVersion::V4,
                        key,
                        payload_offset: 0,
                        payload_len: 16,
                        more_fragments: true,
                        header_len: 20,
                    },
                    now,
                    4
                )
                .unwrap(),
            ReassemblyInsert::Pending
        ));
        let slot = worker.contexts.insert(context);
        assert_eq!(
            directory.claim_or_lookup(key, slot, worker.worker),
            (worker.worker, true)
        );
        let cached_free = runtime.cached_free_buffers();
        // test_reassembly.py::test_timeout_cleanup: omit the last fragment,
        // expire the context, then deliver the last fragment too late.
        assert_eq!(
            worker.expire(&mut runtime, now + Duration::from_millis(50)),
            0
        );
        assert!(directory.lookup(key).is_some());
        assert_eq!(
            worker.expire(&mut runtime, now + Duration::from_millis(250)),
            1
        );
        assert!(worker.contexts.is_empty());
        assert!(directory.lookup(key).is_none());
        assert_eq!(runtime.cached_free_buffers(), cached_free + 2);
        assert_eq!(
            worker.expire(&mut runtime, now + Duration::from_millis(300)),
            0
        );
        assert_eq!(runtime.cached_free_buffers(), cached_free + 2);

        let mut last = [0];
        assert_eq!(runtime.buffer_alloc(&mut last), 1);
        runtime.buffer_mut(last[0]).put_uninit(28).fill(0);
        let mut context = FragmentContext::new(key, IpVersion::V4, now);
        assert!(matches!(
            context
                .insert_fragment(
                    &mut runtime,
                    last[0],
                    ParsedIpFragment {
                        version: IpVersion::V4,
                        key,
                        payload_offset: 16,
                        payload_len: 8,
                        more_fragments: false,
                        header_len: 20,
                    },
                    now,
                    4
                )
                .unwrap(),
            ReassemblyInsert::Pending
        ));
        assert_eq!(context.fragments.len(), 1);
        let slot = worker.contexts.insert(context);
        assert_eq!(
            directory.claim_or_lookup(key, slot, worker.worker),
            (worker.worker, true)
        );
        assert_eq!(
            worker.expire(&mut runtime, now + Duration::from_millis(350)),
            1
        );
        assert!(worker.contexts.is_empty());
        assert!(directory.lookup(key).is_none());
        assert_eq!(runtime.cached_free_buffers(), cached_free + 2);

        // test_reassembly.py::test_reassembly repeats successful reassembly.
        // Finalization transfers the complete chain without retaining a clone.
        for _ in 0..2 {
            let mut indices = [0; 2];
            assert_eq!(runtime.buffer_alloc(&mut indices), 2);
            for (offset, index) in indices.iter().copied().enumerate() {
                let packet = runtime.buffer_mut(index).put_uninit(28);
                packet.fill(0);
                packet[0] = 0x45;
                packet[2..4].copy_from_slice(&28u16.to_be_bytes());
                packet[9] = 17;
                packet[20..].fill(offset as u8 + 1);
            }
            let cached_free = runtime.cached_free_buffers();
            let mut context = FragmentContext::new(key, IpVersion::V4, now);
            assert!(matches!(
                context
                    .insert_fragment(
                        &mut runtime,
                        indices[0],
                        ParsedIpFragment {
                            version: IpVersion::V4,
                            key,
                            payload_offset: 0,
                            payload_len: 8,
                            more_fragments: true,
                            header_len: 20,
                        },
                        now,
                        4
                    )
                    .unwrap(),
                ReassemblyInsert::Pending
            ));
            let ReassemblyInsert::Reassembled(head) = context
                .insert_fragment(
                    &mut runtime,
                    indices[1],
                    ParsedIpFragment {
                        version: IpVersion::V4,
                        key,
                        payload_offset: 8,
                        payload_len: 8,
                        more_fragments: false,
                        header_len: 20,
                    },
                    now,
                    4,
                )
                .unwrap()
            else {
                panic!("complete IPv4 fragment range produces one chain");
            };
            assert_eq!(head, indices[0]);
            assert!(context.fragments.is_empty());
            drop(context);
            assert_eq!(runtime.buffer(head).next_buffer_slot(), Some(indices[1]));
            assert_eq!(&runtime.buffer(head).current()[20..], &[1; 8]);
            assert_eq!(runtime.buffer(indices[1]).current(), &[2; 8]);
            assert_eq!(&runtime.buffer(head).current()[2..4], &36u16.to_be_bytes());
            assert_eq!(runtime.buffer(head).total_len_not_including_first(), 8);
            assert_eq!(runtime.buffer(head).ref_count(), 1);
            assert_eq!(runtime.buffer(indices[1]).ref_count(), 1);
            assert_eq!(runtime.cached_free_buffers(), cached_free);
            runtime.buffer_free(core::slice::from_ref(&head));
            assert_eq!(runtime.cached_free_buffers(), cached_free + 2);
        }

        // test_reassembly.py repeats complete IPv4/IPv6 packets and reverses
        // arrival order. The physical segment/padding boundaries here derive
        // specifically from ip4_full_reass_finalize/ip6_full_reass_finalize's
        // trim_front/keep_data loops, not a claimed standalone upstream test.
        for version in [IpVersion::V4, IpVersion::V6] {
            for reversed in [false, true] {
                let header_len = match version {
                    IpVersion::V4 => IPV4_HEADER_MIN_LEN,
                    IpVersion::V6 => IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
                };
                let mut indices = [0; 6];
                assert_eq!(runtime.buffer_alloc(&mut indices), indices.len());
                for (offset, chain) in indices.chunks_exact(3).enumerate() {
                    let packet = runtime
                        .buffer_mut(chain[0])
                        .put_uninit((header_len + 4) as u16);
                    packet.fill(0);
                    match version {
                        IpVersion::V4 => {
                            packet[0] = 0x45;
                            packet[2..4].copy_from_slice(&36u16.to_be_bytes());
                            packet[4..6].copy_from_slice(&7u16.to_be_bytes());
                            packet[6..8].copy_from_slice(
                                &(if offset == 0 { 0x2000u16 } else { 2u16 }).to_be_bytes(),
                            );
                            packet[9] = 17;
                            packet[12..16].copy_from_slice(&[192, 0, 2, 1]);
                            packet[16..20].copy_from_slice(&[192, 0, 2, 2]);
                            update_ipv4_header_checksum(packet, header_len);
                        }
                        IpVersion::V6 => {
                            packet[0] = 0x60;
                            packet[4..6].copy_from_slice(&24u16.to_be_bytes());
                            packet[6] = 44;
                            packet[40] = 17;
                            packet[42..44].copy_from_slice(
                                &(if offset == 0 { 1u16 } else { 16u16 }).to_be_bytes(),
                            );
                            packet[44..48].copy_from_slice(&7u32.to_be_bytes());
                        }
                    }
                    packet[header_len..].fill(offset as u8 + 1);
                    let payload = runtime.buffer_mut(chain[1]).put_uninit(16);
                    payload[..12].fill(offset as u8 + 1);
                    payload[12..].fill(0xee);
                    runtime.buffer_mut(chain[2]).put_uninit(7).fill(0xee);
                    runtime.buffer_mut(chain[0]).set_next_buffer(Some(chain[1]));
                    runtime.buffer_mut(chain[1]).set_next_buffer(Some(chain[2]));
                    runtime
                        .buffer_mut(chain[0])
                        .set_total_len_not_including_first(23)
                        .unwrap();
                }
                let parsed =
                    parse_ip_fragment_with_chain_len(runtime.buffer(indices[0]).current(), 23)
                        .unwrap();
                let mut context = FragmentContext::new(parsed.key, version, now);
                let cached_free = runtime.cached_free_buffers();
                let order = if reversed {
                    [indices[3], indices[0]]
                } else {
                    [indices[0], indices[3]]
                };
                for (position, index) in order.into_iter().enumerate() {
                    let parsed =
                        parse_ip_fragment_with_chain_len(runtime.buffer(index).current(), 23)
                            .unwrap();
                    let result = context
                        .insert_fragment(&mut runtime, index, parsed, now, 4)
                        .unwrap();
                    if position == 0 {
                        assert!(matches!(result, ReassemblyInsert::Pending));
                    } else {
                        assert!(
                            matches!(result, ReassemblyInsert::Reassembled(head) if head == indices[0])
                        );
                    }
                }
                assert!(context.fragments.is_empty());
                let head = indices[0];
                let packet_header_len = match version {
                    IpVersion::V4 => IPV4_HEADER_MIN_LEN,
                    IpVersion::V6 => IPV6_HEADER_LEN,
                };
                assert_eq!(runtime.chain(head).count(), 4);
                assert!(
                    runtime
                        .chain(head)
                        .flat_map(|buffer| buffer.current().iter().copied())
                        .skip(packet_header_len)
                        .eq(std::iter::repeat_n(1, 16).chain(std::iter::repeat_n(2, 16)))
                );
                assert_eq!(runtime.buffer(head).total_len_not_including_first(), 28);
                assert_eq!(runtime.buffer(indices[1]).current_len(), 12);
                assert_eq!(runtime.buffer(indices[4]).current_len(), 12);
                assert_eq!(runtime.buffer(indices[1]).current_data_offset(), 0);
                assert_eq!(runtime.buffer(indices[4]).current_data_offset(), 0);
                assert_eq!(runtime.buffer(indices[4]).next_buffer_slot(), None);
                assert!(runtime.chain(head).all(|buffer| buffer.ref_count() == 1));
                match version {
                    IpVersion::V4 => {
                        assert_eq!(&runtime.buffer(head).current()[2..4], &52u16.to_be_bytes());
                        assert_eq!(&runtime.buffer(head).current()[6..8], &[0, 0]);
                        assert_eq!(internet_checksum(&runtime.buffer(head).current()[..20]), 0);
                    }
                    IpVersion::V6 => {
                        assert_eq!(&runtime.buffer(head).current()[4..6], &32u16.to_be_bytes());
                        assert_eq!(runtime.buffer(head).current()[6], 17);
                    }
                }
                assert_eq!(runtime.cached_free_buffers(), cached_free + 2);
                runtime.buffer_free_one(head);
                assert_eq!(runtime.cached_free_buffers(), cached_free + 6);
            }
        }

        // VPP's fragment-limit path drops all retained ranges before freeing
        // the context. The rejected incoming fragment remains a drop output.
        worker.max_fragments_per_reassembly = 1;
        let mut indices = [0; 2];
        assert_eq!(runtime.buffer_alloc(&mut indices), 2);
        for (offset, index) in indices.iter().copied().enumerate() {
            let packet = runtime.buffer_mut(index).put_uninit(28);
            packet.fill(0);
            packet[0] = 0x45;
            packet[2..4].copy_from_slice(&28u16.to_be_bytes());
            packet[4..6].copy_from_slice(&7u16.to_be_bytes());
            packet[6..8].copy_from_slice(&(0x2000u16 | offset as u16).to_be_bytes());
            packet[9] = 17;
            packet[12..16].copy_from_slice(&[192, 0, 2, 1]);
            packet[16..20].copy_from_slice(&[192, 0, 2, 2]);
        }
        let cached_free = runtime.cached_free_buffers();
        let mut output = Frame::<(), u32, ()>::new(0);
        let mut nexts = [0; DEFAULT_BUFFER_FRAME_CAPACITY];
        let mut output_len = 0;
        worker
            .process_index(
                &mut runtime,
                indices[0],
                now,
                &mut output,
                &mut nexts,
                &mut output_len,
                IpVersion::V4,
            )
            .unwrap();
        assert_eq!(worker.contexts.len(), 1);
        assert!(directory.lookup(key).is_some());
        assert_eq!(output_len, 0);
        worker
            .process_index(
                &mut runtime,
                indices[1],
                now,
                &mut output,
                &mut nexts,
                &mut output_len,
                IpVersion::V4,
            )
            .unwrap();
        assert!(worker.contexts.is_empty());
        assert!(directory.lookup(key).is_none());
        assert_eq!(runtime.cached_free_buffers(), cached_free + 1);
        assert_eq!(output.vector_args(), &indices[1..]);
        assert_eq!(output_len, 1);
        assert_eq!(nexts[0], NodeNext::slot(Ip4ReassemblyNext::Drop));
        assert_eq!(runtime.buffer(indices[1]).ref_count(), 1);
        runtime.buffer_free(output.vector_args());
        assert_eq!(runtime.cached_free_buffers(), cached_free + 2);
    }
}
