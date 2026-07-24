use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::Rc;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, BufferFrame, BufferNodeError, BufferPoolArena, BufferRef, BufferRefMut,
    DEFAULT_BUFFER_FRAME_POOL_SIZE, DataPlaneBuffers, Frame, FrameBatchWidth, Index, Next,
    NodeHandle, NodeId, NodeNext, NodeRegistration, Pending,
};
use hammer_core::error::{DataPlaneError, DataPlaneResult};
use hammer_infra::PageSize;

use crate::config::Worker;
use crate::file::FileMain;
use crate::handoff::{DataPlaneHandoffWorker, DataWorkerId, HANDOFF_SLOT_CAPACITY, HandoffSlot};
use crate::node::{NodeEntry, NodeFunctionRegistration, NodeRuntime, NodeRuntimeInner};
use crate::runtime_simd::{native_simd_bytes, preferred_frame_batch_width};
use crate::trace::{DataPlaneTrace, PacketTrace, TraceControlHandle};

#[derive(Debug, Clone, Default)]
pub struct DataPlaneRuntimeConfig {
    pub buffers: DataPlaneBufferConfig,
}

/// Runtime-owned buffer arena policy.
///
/// Core owns packet storage and frame ownership. Runtime selects the worker and
/// NUMA layout that constructs those storage arenas.
#[derive(Debug, Clone)]
pub struct DataPlaneBufferConfig {
    pub buffer_slot_capacity: usize,
    pub buffer_slots: usize,
    pub frame_slots: usize,
    pub numa_nodes: &'static [u32],
    pub thread_index: u32,
    pub active_numa_node: u32,
    pub page_size: PageSize,
}

impl Default for DataPlaneBufferConfig {
    #[inline]
    fn default() -> Self {
        Self {
            buffer_slot_capacity: BUFFER_CACHE_LINE_SIZE,
            buffer_slots: 1024,
            frame_slots: DEFAULT_BUFFER_FRAME_POOL_SIZE,
            numa_nodes: &[0],
            thread_index: 0,
            active_numa_node: 0,
            page_size: PageSize::Default,
        }
    }
}

impl TryFrom<DataPlaneBufferConfig> for DataPlaneBuffers {
    type Error = DataPlaneError;

    fn try_from(config: DataPlaneBufferConfig) -> Result<Self, Self::Error> {
        config.create_buffers(config.numa_nodes.iter().copied())
    }
}

impl DataPlaneBufferConfig {
    fn create_buffers(
        &self,
        numa_nodes: impl IntoIterator<Item = u32>,
    ) -> DataPlaneResult<DataPlaneBuffers> {
        let arenas = numa_nodes
            .into_iter()
            .map(|numa_node| {
                BufferPoolArena::with_capacity_on_numa(
                    self.buffer_slot_capacity,
                    self.buffer_slots,
                    self.page_size,
                    numa_node,
                )
            })
            .collect::<DataPlaneResult<Vec<_>>>()?;
        Ok(DataPlaneBuffers::from_arenas(
            arenas,
            self.frame_slots,
            self.thread_index,
            self.active_numa_node,
        ))
    }
}

pub struct DataPlaneRuntime {
    buffers: DataPlaneBuffers,
    nodes: NodeRuntime,
    current_node: Rc<Cell<Option<NodeId>>>,
    /// Worker-local appendable Next Frame per (current node × local slot).
    pub(crate) appendable_next_frames: RefCell<Vec<(NodeId, u16, Frame<Next>)>>,
    handoff: Option<DataPlaneHandoffWorker>,
    handoff_node_handle: Option<NodeHandle>,
    active_numa_node: u32,
    trace: DataPlaneTrace,
    simd_bytes: usize,
    file_main: Rc<RefCell<FileMain>>,
}

impl fmt::Debug for DataPlaneRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneRuntime")
            .field("buffers", &self.buffers)
            .field("nodes", &self.nodes)
            .field("current_node", &self.current_node.get())
            .field(
                "appendable_next_frames",
                &self.appendable_next_frames.borrow().len(),
            )
            .field("handoff", &self.handoff)
            .field("handoff_node_handle", &self.handoff_node_handle)
            .field("active_numa_node", &self.active_numa_node)
            .field("trace", &self.trace)
            .field("simd_bytes", &self.simd_bytes)
            .finish()
    }
}

pub(crate) type RuntimeDataPlaneRuntime = DataPlaneRuntime;

pub(crate) struct DataPlaneRuntimeWorkerSeed {
    buffer_arenas: Vec<BufferPoolArena>,
    frame_slots: usize,
    nodes: NodeRuntimeInner,
    simd_bytes: usize,
    handoff: Option<DataPlaneHandoffWorker>,
    handoff_node_handle: Option<NodeHandle>,
    trace_control: Option<TraceControlHandle>,
}

pub(crate) struct DataPlaneRuntimeWorkerConfig {
    pub(crate) seed: DataPlaneRuntimeWorkerSeed,
    pub(crate) thread_index: u32,
    pub(crate) numa_node: u32,
}

impl fmt::Debug for DataPlaneRuntimeWorkerSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneRuntimeWorkerSeed")
            .field("buffer_arenas", &self.buffer_arenas.len())
            .field("frame_slots", &self.frame_slots)
            .field("simd_bytes", &self.simd_bytes)
            .field("handoff_node_handle", &self.handoff_node_handle)
            .finish()
    }
}

struct HandoffSlotGuard<'runtime> {
    runtime: &'runtime DataPlaneRuntime,
    slot: Option<HandoffSlot>,
}

impl<'runtime> HandoffSlotGuard<'runtime> {
    #[inline]
    fn new(runtime: &'runtime DataPlaneRuntime, slot: HandoffSlot) -> Self {
        Self {
            runtime,
            slot: Some(slot),
        }
    }

    #[inline]
    fn push_into_frame(&mut self, frame: &mut Frame<Next>) -> RuntimeResult<()> {
        match self.slot.as_ref() {
            Some(slot) => frame.push_indices(slot.iter())?,
            None => return Ok(()),
        }
        self.slot = None;
        Ok(())
    }
}

impl Drop for HandoffSlotGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.runtime.drop_handoff_slot_owned(slot);
        }
    }
}

impl Clone for DataPlaneRuntime {
    fn clone(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            nodes: self.nodes.clone(),
            current_node: Rc::clone(&self.current_node),
            appendable_next_frames: RefCell::new(Vec::with_capacity(
                hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY,
            )),
            handoff: self.handoff.clone(),
            handoff_node_handle: self.handoff_node_handle,
            active_numa_node: self.active_numa_node,
            trace: self.trace.clone(),
            simd_bytes: self.simd_bytes,
            file_main: self.file_main.clone(),
        }
    }
}

impl From<&DataPlaneRuntime> for DataPlaneRuntimeWorkerSeed {
    fn from(runtime: &DataPlaneRuntime) -> Self {
        Self {
            buffer_arenas: runtime.buffers.buffer_arenas().collect(),
            frame_slots: runtime.buffers.frame_slots(),
            nodes: runtime.nodes.snapshot(),
            simd_bytes: runtime.simd_bytes,
            handoff: runtime.handoff.clone(),
            handoff_node_handle: runtime.handoff_node_handle,
            trace_control: runtime.trace.control(),
        }
    }
}

impl TryFrom<DataPlaneRuntimeWorkerConfig> for DataPlaneRuntime {
    type Error = RuntimeError;

    fn try_from(config: DataPlaneRuntimeWorkerConfig) -> RuntimeResult<Self> {
        let DataPlaneRuntimeWorkerConfig {
            seed,
            thread_index,
            numa_node,
        } = config;
        let DataPlaneRuntimeWorkerSeed {
            buffer_arenas,
            frame_slots,
            nodes,
            simd_bytes,
            handoff,
            handoff_node_handle,
            trace_control,
        } = seed;
        let buffers =
            DataPlaneBuffers::from_arenas(buffer_arenas, frame_slots, thread_index, numa_node);
        let mut runtime = Self::from_buffers(buffers, simd_bytes)?;
        runtime.nodes = nodes.into();
        runtime.handoff = handoff;
        runtime.handoff_node_handle = handoff_node_handle;
        runtime.trace.set_control(trace_control);
        let configured_arena = runtime
            .handoff
            .as_ref()
            .and_then(DataPlaneHandoffWorker::configured_buffer_arena);
        if let Some(arena) = configured_arena {
            runtime.buffers = runtime.buffers.with_active_buffer_arena(arena);
            runtime.active_numa_node = runtime.buffers.active_numa_node();
        }
        Ok(runtime)
    }
}

impl DataPlaneRuntime {
    #[inline]
    pub fn new(config: DataPlaneRuntimeConfig) -> Self {
        Self::try_new(config).expect("create ordinary-page data-plane runtime")
    }

    #[inline]
    pub fn try_new(config: DataPlaneRuntimeConfig) -> RuntimeResult<Self> {
        Self::from_buffers(config.buffers.try_into()?, native_simd_bytes())
    }

    #[inline]
    fn from_buffers(buffers: DataPlaneBuffers, simd_bytes: usize) -> RuntimeResult<Self> {
        Ok(Self {
            active_numa_node: buffers.active_numa_node(),
            buffers,
            nodes: NodeRuntime::default(),
            current_node: Rc::new(Cell::new(None)),
            appendable_next_frames: RefCell::new(Vec::with_capacity(
                hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY,
            )),
            handoff: None,
            handoff_node_handle: None,
            trace: DataPlaneTrace::default(),
            simd_bytes,
            file_main: Rc::new(RefCell::new(FileMain::new()?)),
        })
    }

    #[inline]
    pub fn for_worker(&self, thread_index: u32, numa_node: u32) -> RuntimeResult<Self> {
        DataPlaneRuntimeWorkerConfig {
            seed: DataPlaneRuntimeWorkerSeed::from(self),
            thread_index,
            numa_node,
        }
        .try_into()
    }

    #[inline]
    pub fn attach_handoff_worker(mut runtime: Self, handoff: DataPlaneHandoffWorker) -> Self {
        if let Some(arena) = handoff.configured_buffer_arena() {
            runtime.buffers = runtime.buffers.with_active_buffer_arena(arena);
            runtime.active_numa_node = runtime.buffers.active_numa_node();
        }
        runtime.handoff = Some(handoff);
        runtime
    }

    #[inline]
    pub fn set_handoff_node_handle(&mut self, handle: NodeHandle) {
        self.handoff_node_handle = Some(handle);
    }

    #[inline]
    pub fn handoff_node_handle(&self) -> RuntimeResult<NodeHandle> {
        self.handoff_node_handle
            .ok_or(DataPlaneError::HandoffNodeHandleMissing.into())
    }

    #[inline]
    pub fn active_numa_node(&self) -> u32 {
        self.active_numa_node
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    /// VPP-style runtime thread index: zero for main, one-based for workers.
    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.buffers.thread_index()
    }

    #[inline]
    pub fn in_use_buffers(&self) -> usize {
        self.buffers.in_use_buffers()
    }

    #[inline]
    pub fn cached_free_buffers(&self) -> usize {
        self.buffers.cached_free_buffers()
    }

    #[inline]
    pub fn frames_in_use(&self) -> usize {
        self.buffers.frames_in_use()
    }

    #[inline]
    pub fn alloc_index(&self) -> RuntimeResult<Index> {
        Ok(self.buffers.alloc_index()?)
    }

    #[inline]
    pub fn alloc_index_with_bytes(&self, bytes: &[u8]) -> RuntimeResult<Index> {
        Ok(self.buffers.alloc_index_with_bytes(bytes)?)
    }

    #[inline]
    fn drop_index_owned(&self, index: Index) {
        self.buffers
            .drop_index_owned_with_trace(index, |handle| self.trace.finalize(handle));
    }

    #[inline]
    pub(crate) fn drop_pending_frame_owned(&self, frame: Frame<Pending>) {
        frame.return_with_trace_release(|handle| self.trace.finalize(handle));
    }

    #[inline]
    pub fn prefetch_header(&self, index: Index) {
        self.buffers.prefetch_header(index);
    }

    #[inline]
    pub fn prefetch_read(&self, index: Index) {
        self.buffers.prefetch_read(index);
    }

    #[inline]
    pub fn prefetch_write(&self, index: Index) {
        self.buffers.prefetch_write(index);
    }

    #[inline]
    pub fn chain(
        &self,
        index: Index,
    ) -> impl Iterator<Item = Result<BufferRef<'_>, DataPlaneError>> + '_ {
        self.buffers.chain(index)
    }

    #[inline]
    pub fn current_config(&self, index: Index) -> RuntimeResult<NodeId> {
        Ok(self.buffers.current_config(index)?)
    }

    #[inline]
    pub fn put_next_frame(&self, frame: Frame<Next>) -> RuntimeResult<()> {
        let next = frame.next();
        let pending = frame.into_pending()?;
        if !pending.has_pending() {
            return Ok(());
        }
        self.nodes.schedule_frame(next, pending, false)
    }

    #[inline]
    pub fn get_buffer(&self, index: Index) -> RuntimeResult<BufferRef<'_>> {
        Ok(self.buffers.get_buffer(index)?)
    }

    #[inline]
    pub fn get_buffer_mut(&self, index: Index) -> RuntimeResult<BufferRefMut<'_>> {
        Ok(self.buffers.get_buffer_mut(index)?)
    }

    #[inline]
    pub fn nodes(&self) -> &NodeRuntime {
        &self.nodes
    }

    pub fn file_main(&self) -> std::cell::Ref<'_, FileMain> {
        self.file_main.borrow()
    }

    pub fn file_main_mut(&self) -> std::cell::RefMut<'_, FileMain> {
        self.file_main.borrow_mut()
    }

    pub fn init_graph(&self, entries: &[NodeEntry]) -> RuntimeResult<()> {
        let node_functions = crate::builtin_registration_image()
            .node_functions()
            .to_vec();
        self.init_graph_with_node_functions(entries, &node_functions)
    }

    pub fn init_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        let owners = entries
            .iter()
            .filter(|entry| !matches!(entry.registration, NodeRegistration::Sibling { .. }));
        let siblings = entries
            .iter()
            .filter(|entry| matches!(entry.registration, NodeRegistration::Sibling { .. }));
        for entry in owners.chain(siblings) {
            let node = (entry.init)(self).map_err(|source| {
                RuntimeError::GraphNodeInitialization {
                    node: entry.registration.name().unwrap_or("?"),
                    source: Box::new(source),
                }
            })?;
            self.nodes
                .install_node_function(node, self.simd_bytes, node_functions)?;
        }
        self.nodes.resolve_named_next_nodes()
    }

    pub(crate) fn extend_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        for register_siblings in [false, true] {
            for entry in entries {
                let is_sibling = matches!(entry.registration, NodeRegistration::Sibling { .. });
                if is_sibling != register_siblings {
                    continue;
                }
                let name = entry
                    .registration
                    .name()
                    .ok_or(DataPlaneError::UnnamedGraphRegistration)?;
                if self.nodes.node_by_name(name).is_some() {
                    continue;
                }
                let node = (entry.init)(self)?;
                self.nodes
                    .install_node_function(node, self.simd_bytes, node_functions)?;
            }
        }
        self.nodes.resolve_named_next_nodes()?;
        Ok(())
    }

    /// Global Graph Transaction: drain residual scheduled frames, detach the
    /// live topology, rebuild and renumber from `entries`, then publish the
    /// updated topology to workers.
    ///
    /// This is a graph transaction, not a plugin unload operation; it neither
    /// changes the registration authority nor releases DSO handles. Business
    /// state must rebind by name, not `NodeId`.
    pub fn rebuild_graph(&self, entries: &[NodeEntry]) -> RuntimeResult<()> {
        let node_functions = crate::builtin_registration_image()
            .node_functions()
            .to_vec();
        self.rebuild_graph_with_node_functions(entries, &node_functions)
    }

    pub fn rebuild_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        self.set_current_node(None);
        self.nodes.detach_graph_for_rebuild()?;
        self.init_graph_with_node_functions(entries, node_functions)
    }

    #[inline]
    pub fn set_trace_control(&self, control: Option<TraceControlHandle>) {
        self.trace.set_control(control);
    }

    #[inline]
    pub fn node_by_name(&self, name: &str) -> Option<NodeId> {
        self.nodes.node_by_name(name)
    }

    #[inline]
    pub(crate) fn set_current_node(&self, node: Option<NodeId>) {
        self.current_node.set(node);
    }

    #[inline]
    pub fn current_node(&self) -> Option<NodeId> {
        self.current_node.get()
    }

    /// Run `f` with `node` installed as the ambient current graph node.
    ///
    /// Used by Session Queue test helpers and any non-dispatch path that must
    /// flush through Graph Fanout against a concrete owner node's local nexts.
    #[inline]
    pub fn with_current_node<R>(&self, node: NodeId, f: impl FnOnce() -> R) -> R {
        let previous = self.current_node.get();
        self.current_node.set(Some(node));
        let result = f();
        self.flush_fanout_appendable();
        self.current_node.set(previous);
        result
    }

    #[inline]
    pub fn may_mark_trace(&self, node: NodeId) -> bool {
        self.trace.may_mark(node)
    }

    #[inline]
    pub fn try_mark_trace(&self, node: NodeId, index: Index) -> RuntimeResult<()> {
        if !self.trace.may_mark(node) {
            return Ok(());
        }
        if self.get_buffer(index)?.trace_handle().is_some() {
            return Ok(());
        }
        let node_name = self.nodes.node_name(node)?;
        if let Some(handle) = self.trace.try_mark(node, node_name) {
            self.get_buffer_mut(index)?.set_trace_handle(handle);
        }
        Ok(())
    }

    #[inline]
    pub fn add_trace<T: PacketTrace>(&self, index: Index, trace: T) -> RuntimeResult<()> {
        let Some(node) = self.current_node() else {
            return Ok(());
        };
        let Some(handle) = self.get_buffer(index)?.trace_handle() else {
            return Ok(());
        };
        let node_name = self.nodes.node_name(node)?;
        let formatter = self.nodes.node_trace_formatter(node)?;
        let payload_bytes = bincode::serialize(&trace)
            .map_err(|source| RuntimeError::PacketTraceSerialization { source })?;
        self.trace
            .add_entry(handle, node, node_name, formatter, payload_bytes);
        Ok(())
    }

    #[inline(always)]
    pub fn should_trace_packet(&self, index: Index) -> RuntimeResult<bool> {
        Ok(crate::unlikely(
            self.get_buffer(index)?.trace_handle().is_some(),
        ))
    }

    #[inline]
    pub fn record_current_node_error(&self, code: u16) -> RuntimeResult<u16> {
        let node = self
            .current_node()
            .ok_or(RuntimeError::NodeDispatchContextMissing)?;
        self.nodes.increment_node_error(node, code)
    }

    #[inline]
    pub fn node_error_count(&self, node: NodeId, code: u16) -> RuntimeResult<u64> {
        self.nodes.node_error_count(node, code)
    }

    #[inline]
    pub fn node_error(&self, index: Index) -> RuntimeResult<Option<BufferNodeError>> {
        let code = self.buffers.node_error_code(index)?;
        match code {
            Some(code) => self.nodes.decode_node_error(code),
            None => Ok(None),
        }
    }

    #[inline]
    pub fn preferred_frame_batch_width(&self) -> FrameBatchWidth {
        preferred_frame_batch_width(self.simd_bytes)
    }

    #[inline]
    pub fn schedule_empty_frame(&self, node: NodeId) -> RuntimeResult<()> {
        let frame = self.buffers.get_next_frame(node)?;
        let pending = frame.into_pending()?;
        self.nodes.schedule_frame(node, pending, true)
    }

    #[inline]
    pub fn schedule_polling_driver_nodes(&self) -> RuntimeResult<usize> {
        let nodes = self.nodes.polling_driver_nodes()?;
        let scheduled = nodes.len();
        for node in nodes {
            self.schedule_empty_frame(node)?;
        }
        Ok(scheduled)
    }

    pub(crate) fn schedule_interrupt_driver_nodes(&self) -> RuntimeResult<usize> {
        let mut scheduled = 0;
        let mut start = 0;
        while let Some(node) = self.nodes.next_interrupt_pending(start) {
            start = node.slot() as usize + 1;
            self.schedule_empty_frame(node)?;
            scheduled += 1;
        }
        Ok(scheduled)
    }

    #[inline]
    pub fn set_node_interrupt_pending(&self, node: NodeId) -> RuntimeResult<bool> {
        if !self.nodes.mark_interrupt_pending(node)? {
            return Ok(false);
        }
        if let Err(err) = self.schedule_empty_frame(node) {
            let _ = self.nodes.clear_interrupt_pending(node);
            return Err(err);
        }
        Ok(true)
    }

    #[inline]
    pub fn run_ready_nodes(&self) -> RuntimeResult<usize> {
        self.drain_handoff_frames()?;
        self.nodes.run_ready_function_nodes(self)
    }

    #[inline]
    fn drain_handoff_frames(&self) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Ok(());
        };
        while let Some(handoff_frame) = handoff.pop() {
            let mut slot = HandoffSlotGuard::new(self, handoff_frame.slot);
            let node = self.nodes.node_for_handle(handoff_frame.target)?;
            let mut frame = self.buffers.get_next_frame(node)?;
            slot.push_into_frame(&mut frame)?;
            self.put_next_frame(frame)?;
        }
        Ok(())
    }

    #[inline]
    fn drop_handoff_slot_owned(&self, slot: HandoffSlot) {
        for index in slot.iter() {
            self.drop_index_owned(index);
        }
    }

    #[inline]
    pub fn handoff_frame(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        frame: &mut BufferFrame,
    ) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        let pending = frame.pending_indices().len();
        if pending == 0 {
            return Ok(());
        }
        let slots = pending.div_ceil(HANDOFF_SLOT_CAPACITY);
        handoff.ensure_enqueue_slots(worker, slots)?;
        while !frame.pending_indices().is_empty() {
            let slot = HandoffSlot::from_prefix(frame.pending_indices());
            let slot_len = slot.len();
            match handoff.enqueue_slot(worker, target, slot) {
                Ok(()) => frame.discard_prefix(slot_len),
                Err(err) => {
                    let (error, _) = err.into_parts();
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn handoff_indices(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        indices: impl IntoIterator<Item = Index>,
    ) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        let indices = indices.into_iter();
        if let (_, Some(upper)) = indices.size_hint()
            && upper == 0
        {
            return Ok(());
        }
        if let (_, Some(upper)) = indices.size_hint() {
            handoff.ensure_enqueue_slots(worker, upper.div_ceil(HANDOFF_SLOT_CAPACITY))?;
        }
        let mut slot = HandoffSlot::new();
        for index in indices {
            if !slot.push(index) {
                self.enqueue_handoff_slot_or_drop(worker, target, slot)?;
                slot = HandoffSlot::single(index);
            }
        }
        if !slot.is_empty() {
            self.enqueue_handoff_slot_or_drop(worker, target, slot)?;
        }
        Ok(())
    }

    #[inline]
    pub fn handoff_index<N: NodeNext>(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        index: Index,
        continuation: Option<N>,
    ) -> RuntimeResult<()> {
        if let Some(next) = continuation {
            let node = self
                .current_node()
                .ok_or(RuntimeError::HandoffDispatchContextMissing)?;
            let resolved = self.nodes.node_next(node, next)?;
            self.get_buffer_mut(index)?.set_current_config(resolved);
        }
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        handoff.ensure_enqueue_slots(worker, 1)?;
        match handoff.enqueue_index(worker, target, index) {
            Ok(()) => Ok(()),
            Err(err) => {
                let (error, slot) = err.into_parts();
                let guard = HandoffSlotGuard::new(self, slot);
                drop(guard);
                Err(error.into())
            }
        }
    }

    #[inline]
    fn enqueue_handoff_slot_or_drop(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        slot: HandoffSlot,
    ) -> RuntimeResult<()> {
        let Some(handoff) = &self.handoff else {
            return Err(DataPlaneError::HandoffNotConfigured.into());
        };
        match handoff.enqueue_slot(worker, target, slot) {
            Ok(()) => Ok(()),
            Err(err) => {
                let (error, slot) = err.into_parts();
                let guard = HandoffSlotGuard::new(self, slot);
                drop(guard);
                Err(error.into())
            }
        }
    }
}

impl Worker {
    pub fn create_runtime(&self) -> RuntimeResult<DataPlaneRuntime> {
        let buffer = &self.buffer;
        let numa_nodes = self.buffer_numa_nodes()?;
        let create_buffers = |page_size| -> DataPlaneResult<DataPlaneBuffers> {
            DataPlaneBufferConfig {
                buffer_slot_capacity: buffer.slot_bytes,
                buffer_slots: buffer.slots_per_numa,
                frame_slots: buffer.frame_pool_size,
                active_numa_node: numa_nodes[0],
                page_size,
                ..DataPlaneBufferConfig::default()
            }
            .create_buffers(numa_nodes.iter().copied())
        };

        let buffers = match buffer.page_size {
            Some(page_size) => create_buffers(page_size)?,
            None => {
                #[cfg(target_os = "linux")]
                {
                    match create_buffers(PageSize::DefaultHuge) {
                        Ok(buffers) => buffers,
                        Err(source) => {
                            tracing::warn!(%source, "default HugeTLB Buffer Arena unavailable; using ordinary pages");
                            create_buffers(PageSize::Default)?
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    create_buffers(PageSize::Default)?
                }
            }
        };
        DataPlaneRuntime::from_buffers(buffers, native_simd_bytes())
    }

    fn buffer_numa_nodes(&self) -> RuntimeResult<Vec<u32>> {
        #[cfg(target_os = "linux")]
        {
            if self.numa.enabled {
                return self.buffer_numa_nodes_with(crate::numa::node_for_cpu);
            }
        }
        Ok(vec![0])
    }

    #[cfg(target_os = "linux")]
    fn buffer_numa_nodes_with(
        &self,
        mut node_for_cpu: impl FnMut(usize) -> RuntimeResult<u32>,
    ) -> RuntimeResult<Vec<u32>> {
        let mut nodes = Vec::with_capacity(self.count);
        for worker in 0..self.count {
            let core = crate::worker_thread::worker_core(worker, &self.cpu).ok_or_else(|| {
                RuntimeError::config_validation(format!(
                    "worker {worker} has no available CPU core"
                ))
            })?;
            let node = node_for_cpu(core)?;
            if node >= 32 {
                return Err(RuntimeError::config_validation(format!(
                    "worker CPU {core} resolves to unsupported NUMA node {node}"
                )));
            }
            nodes.push(node);
        }
        nodes.sort_unstable();
        nodes.dedup();
        Ok(nodes)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use crate::config::Worker;

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_buffer_nodes_follow_configured_worker_cores() {
        let mut worker = Worker::default();
        worker.count = 3;
        worker.numa.enabled = true;
        worker.cpu.worker_cores = vec![2, 4, 6];

        let nodes = worker
            .buffer_numa_nodes_with(|core| match core {
                2 | 6 => Ok(1),
                4 => Ok(3),
                _ => unreachable!("unexpected configured core"),
            })
            .expect("resolve worker NUMA nodes");

        assert_eq!(nodes, vec![1, 3]);
    }
}
