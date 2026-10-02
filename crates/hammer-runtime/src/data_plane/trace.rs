use std::time::{SystemTime, UNIX_EPOCH};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::*;
use crate::trace::{
    HandoffTrace, TRACE_INDEX_LIMIT, TRACE_THREAD_LIMIT, TRACE_THREAD_SHIFT, TraceHeader,
};
use hammer_stats::StatsMain;

impl DataPlaneMain {
    pub(crate) fn initialize_trace_clock(&mut self) {
        self.seconds_per_cpu_tick = 1.0 / hammer_infra::time::cpu_clock_frequency();
        self.unix_reference_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock follows Unix epoch")
            .as_secs_f64();
        self.cpu_reference_ticks = hammer_infra::time::cpu_time_now();
        self.main_loop_start_ticks = self.cpu_reference_ticks;
    }

    pub(crate) fn start_main_loop_trace_clock(&mut self) {
        assert_eq!(self.thread_index, 0, "thread zero sets trace display origin");
        self.main_loop_start_ticks = hammer_infra::time::cpu_time_now();
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

    /// Existing dispatch context for non-Node tests and Session Queue paths.
    #[inline]
    pub fn with_current_node<R>(&mut self, node: NodeId, f: impl FnOnce(&mut Self) -> R) -> R {
        let previous = self.current_node.get();
        self.current_node.set(Some(node));
        let result = f(self);
        self.current_node.set(previous);
        result
    }

    /// VPP `vlib_get_trace_count`; only a source Node consumes this quota.
    #[inline(always)]
    pub fn trace_count(&self, node: &NodeRuntime) -> u32 {
        let Some(trace_node) = self.trace_main.nodes.get(node.node_index().slot() as usize) else {
            return 0;
        };
        assert!(trace_node.count <= trace_node.limit);
        trace_node.limit - trace_node.count
    }

    /// VPP `vlib_set_trace_count`; caller writes back its remaining quota.
    #[inline(always)]
    pub fn set_trace_count(&mut self, node: &NodeRuntime, remaining: u32) {
        let trace_node = self
            .trace_main
            .nodes
            .get_mut(node.node_index().slot() as usize)
            .expect("source Node owns an active trace quota");
        assert!(remaining <= trace_node.limit);
        trace_node.count = trace_node.limit - remaining;
    }

    /// VPP `vlib_trace_buffer`: allocate one packet record and mark its chain.
    #[inline(always)]
    pub fn trace_buffer(
        &mut self,
        node: &NodeRuntime,
        next_index: u16,
        buffer_index: u32,
        follow_chain: bool,
    ) -> bool {
        if crate::unlikely(!self.trace_main.trace_enable) {
            return false;
        }
        if crate::unlikely(self.trace_main.trace_buffer_pool.len() >= TRACE_INDEX_LIMIT as usize) {
            return false;
        }
        assert!(self.thread_index < TRACE_THREAD_LIMIT);
        self.nodes.trace_next_frame(node.node_index(), next_index);
        // The pool slot owns one packet's empty trace vector; Vec::new does
        // not allocate backing storage. add_trace appends records to it.
        let index = self.trace_main.trace_buffer_pool.insert(Vec::new());
        assert!(index < TRACE_INDEX_LIMIT);
        let handle = (self.thread_index << TRACE_THREAD_SHIFT) | index;
        let mut current = Some(buffer_index);
        while let Some(index) = current {
            current = if follow_chain {
                self.buffer(index).next_buffer_slot()
            } else {
                None
            };
            self.buffer_mut(index).set_trace_handle(handle);
        }
        true
    }

    /// VPP `vlib_add_trace_inline`: borrow the new record payload directly.
    #[inline(always)]
    pub fn add_trace<T>(&mut self, node: &NodeRuntime, buffer_index: u32) -> Option<&mut T>
    where
        T: KnownLayout + FromBytes + IntoBytes + Immutable,
    {
        const {
            assert!(core::mem::size_of::<T>() > 0);
            assert!(core::mem::align_of::<T>() <= 16);
        }
        let handle = self.buffer(buffer_index).trace_handle();
        if crate::unlikely(handle.is_none()) {
            return None;
        }
        let handle = handle.expect("checked traced Buffer");
        if crate::unlikely(!self.trace_main.trace_enable) {
            return None;
        }
        let previous_thread = handle >> TRACE_THREAD_SHIFT;
        if crate::unlikely(previous_thread != self.thread_index) {
            let previous_index = handle & TRACE_INDEX_LIMIT;
            let handoff_node = self
                .nodes
                .node_runtime_data(self.handoff_trace_node)
                .expect("handoff trace Node registered before graph freeze");
            if !self.trace_buffer(&handoff_node, 0, buffer_index, true) {
                return None;
            }
            let handoff = self
                .add_trace::<HandoffTrace>(&handoff_node, buffer_index)
                .expect("new local handoff trace owns a pool slot");
            handoff.prev_thread = previous_thread;
            handoff.prev_trace_index = previous_index;
        }
        let handle = self.buffer(buffer_index).trace_handle()?;
        let pool_index = handle & TRACE_INDEX_LIMIT;
        if crate::unlikely(!self.trace_main.trace_buffer_pool.contains_key(pool_index)) {
            return None;
        }
        let dispatch_time = self.last_time_stamp;
        let node_index = node.node_index().slot();
        let trace = self
            .trace_main
            .trace_buffer_pool
            .get_mut(pool_index)
            .expect("checked occupied trace index");
        let words = core::mem::size_of::<T>().div_ceil(16);
        let start = trace.len();
        let end = start.checked_add(1 + words).expect("trace length fits usize");
        trace.resize(
            end,
            TraceHeader {
                time: 0,
                node_index: 0,
                n_data: 0,
            },
        );
        trace[start] = TraceHeader {
            time: dispatch_time,
            node_index,
            n_data: u32::try_from(words).expect("trace payload count fits u32"),
        };
        let bytes = trace[start + 1..end].as_mut_bytes();
        let (payload, _) = T::mut_from_prefix(bytes)
            .expect("zeroed trace payload has T's size and alignment");
        Some(payload)
    }

    /// VPP `vlib_trace_frame_buffers_only`: retain a fixed packet prefix for
    /// each already-traced Buffer in a Frame.
    pub fn trace_frame_buffers_only<T>(&mut self, node: &NodeRuntime, buffers: &[u32])
    where
        T: KnownLayout + FromBytes + IntoBytes + Immutable,
    {
        const { assert!(core::mem::size_of::<T>() > 0) };
        let mut offset = 0;
        while offset + 4 <= buffers.len() {
            self.prefetch_header(buffers[offset + 2]);
            self.prefetch_header(buffers[offset + 3]);
            self.trace_frame_buffer_only::<T>(node, buffers[offset]);
            self.trace_frame_buffer_only::<T>(node, buffers[offset + 1]);
            offset += 2;
        }
        while offset < buffers.len() {
            self.trace_frame_buffer_only::<T>(node, buffers[offset]);
            offset += 1;
        }
    }

    #[inline(always)]
    fn trace_frame_buffer_only<T>(&mut self, node: &NodeRuntime, index: u32)
    where
        T: KnownLayout + FromBytes + IntoBytes + Immutable,
    {
        if crate::unlikely(self.buffer(index).trace_handle().is_some()) {
            let packet = self.buffer(index).current();
            let packet_ptr = packet.as_ptr();
            let copy_len = packet.len().min(core::mem::size_of::<T>());
            if let Some(trace) = self.add_trace::<T>(node, index) {
                // SAFETY: add_trace mutates only trace storage and the Buffer
                // handle; packet storage remains allocated and disjoint.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        packet_ptr,
                        trace.as_mut_bytes().as_mut_ptr(),
                        copy_len,
                    );
                }
            }
        }
    }

    /// Record the current node's generated local error and return its global index.
    #[inline]
    pub fn record_current_node_error<E: NodeErrorCode>(
        &self,
        error: E,
    ) -> RuntimeResult<NodeErrorIndex> {
        self.record_current_node_error_count(error, 1)
    }

    /// Record a batch of occurrences classified by the current node.
    #[inline]
    pub fn record_current_node_error_count<E: NodeErrorCode>(
        &self,
        error: E,
        count: u64,
    ) -> RuntimeResult<NodeErrorIndex> {
        let node = self
            .current_node()
            .ok_or(RuntimeError::NodeDispatchContextMissing)?;
        let index = self.nodes.node_error_index(node, error.local_code())?;
        if count != 0 && let Some(entry) = self.node_error_stats_entry_index.get() {
            StatsMain::global()?.segment.increment_simple_counter(
                entry,
                self.thread_index(),
                u32::from(index.get()),
                count,
            );
        }
        Ok(index)
    }

    #[inline]
    pub fn preferred_frame_batch_width(&self) -> FrameBatchWidth {
        preferred_frame_batch_width(self.simd_bytes)
    }
}
