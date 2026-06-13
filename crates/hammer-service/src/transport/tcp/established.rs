use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, FrameIndex, NextFrame, Node, NodeId, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpCloseReason, TcpConnectionId, TcpSeq, TcpState};

use super::TcpLookupId;
use super::connection::{TcpConnectionView, TcpReceiveProgress, TcpSessionAccessSlot};
use super::input::{mark_pending_tcp_app_ingress, take_pending_tcp_app_ingress};

// Established receive keeps only a small per-worker reorder table: at most 64
// active connections and 7 buffered payload segments per connection. One gap
// fill can therefore expand into at most 8 app deliveries, matching the node
// runtime's fixed next-frame fanout while avoiding unbounded hot-path growth.
const TCP_ESTABLISHED_REORDER_CONNECTION_CAP: usize = 64;
const TCP_ESTABLISHED_REORDER_SEGMENT_CAP: usize = 7;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    RcvProcess,
}

pub trait TcpEstablishedBackend: Send + Sync {
    #[inline]
    fn observe_ack_progress(&self, _observation: TcpEstablishedAckObservation) -> CoreResult<()> {
        Ok(())
    }

    #[inline]
    fn observe_receive_ack(&self, _observation: TcpReceiveAckObservation) -> CoreResult<()> {
        Ok(())
    }

    fn observe_close(&self, observation: TcpEstablishedObservation) -> CoreResult<()>;
}

struct TcpEstablishedBackendHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    observe_ack_progress: fn(*const (), TcpEstablishedAckObservation) -> CoreResult<()>,
    observe_receive_ack: fn(*const (), TcpReceiveAckObservation) -> CoreResult<()>,
    observe_close: fn(*const (), TcpEstablishedObservation) -> CoreResult<()>,
}

unsafe impl Send for TcpEstablishedBackendHandle {}
unsafe impl Sync for TcpEstablishedBackendHandle {}

impl Default for TcpEstablishedBackendHandle {
    #[inline]
    fn default() -> Self {
        Self::noop()
    }
}

impl Clone for TcpEstablishedBackendHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            observe_ack_progress: self.observe_ack_progress,
            observe_receive_ack: self.observe_receive_ack,
            observe_close: self.observe_close,
        }
    }
}

impl Drop for TcpEstablishedBackendHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpEstablishedBackendHandle {
    #[inline]
    fn noop() -> Self {
        Self {
            raw: std::ptr::null(),
            clone_raw: clone_noop_established_backend,
            drop_raw: drop_noop_established_backend,
            observe_ack_progress: observe_noop_ack_progress,
            observe_receive_ack: observe_noop_receive_ack,
            observe_close: observe_noop_close,
        }
    }

    #[inline]
    fn new<O>(backend: Arc<O>) -> Self
    where
        O: TcpEstablishedBackend + 'static,
    {
        Self {
            raw: Arc::into_raw(backend) as *const (),
            clone_raw: clone_established_backend_arc::<O>,
            drop_raw: drop_established_backend_arc::<O>,
            observe_ack_progress: observe_ack_progress_with::<O>,
            observe_receive_ack: observe_receive_ack_with::<O>,
            observe_close: observe_close_with::<O>,
        }
    }

    #[inline]
    fn observe_ack_progress(&self, observation: TcpEstablishedAckObservation) -> CoreResult<()> {
        (self.observe_ack_progress)(self.raw, observation)
    }

    #[inline]
    fn observe_receive_ack(&self, observation: TcpReceiveAckObservation) -> CoreResult<()> {
        (self.observe_receive_ack)(self.raw, observation)
    }

    #[inline]
    fn observe_close(&self, observation: TcpEstablishedObservation) -> CoreResult<()> {
        (self.observe_close)(self.raw, observation)
    }
}

#[inline]
fn clone_noop_established_backend(_raw: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_established_backend(_raw: *const ()) {}

#[inline]
fn observe_noop_ack_progress(
    _raw: *const (),
    _observation: TcpEstablishedAckObservation,
) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn observe_noop_receive_ack(
    _raw: *const (),
    _observation: TcpReceiveAckObservation,
) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn observe_noop_close(_raw: *const (), _observation: TcpEstablishedObservation) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn clone_established_backend_arc<O>(raw: *const ()) -> *const ()
where
    O: TcpEstablishedBackend + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            Arc::increment_strong_count(raw);
        }
    }
    raw.cast()
}

#[inline]
fn drop_established_backend_arc<O>(raw: *const ())
where
    O: TcpEstablishedBackend + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            drop(Arc::from_raw(raw));
        }
    }
}

#[inline]
fn observe_ack_progress_with<O>(
    raw: *const (),
    observation: TcpEstablishedAckObservation,
) -> CoreResult<()>
where
    O: TcpEstablishedBackend + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_ack_progress(observation) }
}

#[inline]
fn observe_receive_ack_with<O>(
    raw: *const (),
    observation: TcpReceiveAckObservation,
) -> CoreResult<()>
where
    O: TcpEstablishedBackend + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_receive_ack(observation) }
}

#[inline]
fn observe_close_with<O>(raw: *const (), observation: TcpEstablishedObservation) -> CoreResult<()>
where
    O: TcpEstablishedBackend + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_close(observation) }
}

#[derive(Clone, Default)]
pub struct TcpEstablishedBackendSlot {
    inner: Arc<ArcSwapOption<TcpEstablishedBackendHandle>>,
}

impl TcpEstablishedBackendSlot {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn install<O>(&self, backend: Arc<O>)
    where
        O: TcpEstablishedBackend + 'static,
    {
        self.inner
            .store(Some(Arc::new(TcpEstablishedBackendHandle::new(backend))));
    }

    #[inline]
    fn load(&self) -> TcpEstablishedBackendHandle {
        self.inner
            .load_full()
            .as_deref()
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEstablishedAckObservation {
    pub lookup_id: TcpLookupId,
    pub connection_id: TcpConnectionId,
    pub accepted_acknowledgment: u32,
    pub advertised_window: u32,
    pub previous_state: TcpState,
    pub ack_state_transition: Option<TcpState>,
    pub acknowledges_local_fin: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpReceiveAckKind {
    Ack,
    Challenge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpReceiveAckReason {
    Data,
    Fin,
    Gap,
    InvalidAck,
    NotAcceptable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpReceiveAckObservation {
    pub lookup_id: TcpLookupId,
    pub connection_id: TcpConnectionId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub send_sequence: u32,
    pub receive_acknowledgment: u32,
    pub advertised_window: u32,
    pub kind: TcpReceiveAckKind,
    pub reason: TcpReceiveAckReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEstablishedObservation {
    pub lookup_id: TcpLookupId,
    pub connection_id: TcpConnectionId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub reason: TcpCloseReason,
}

#[derive(Clone)]
struct TcpEstablishedStateRuntime {
    access: TcpSessionAccessSlot,
    backend: TcpEstablishedBackendSlot,
}

thread_local! {
    static TCP_ESTABLISHED_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpEstablishedStateRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
    static TCP_ESTABLISHED_REORDER: RefCell<TcpEstablishedReorderStore> =
        RefCell::new(TcpEstablishedReorderStore::new());
}

#[inline]
fn has_tcp_established_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_established_runtime(
    access: TcpSessionAccessSlot,
    backend: TcpEstablishedBackendSlot,
) -> CoreResult<NodeRuntimeData> {
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpEstablishedStateRuntime { access, backend });
        Ok(NodeRuntimeData::from_words([
            u64::try_from(slot)
                .map_err(|_| CoreError::internal("TCP established runtime slot overflow"))?,
            1,
            0,
            0,
        ]))
    })
}

fn tcp_established_runtime(data: NodeRuntimeData) -> CoreResult<TcpEstablishedStateRuntime> {
    if !has_tcp_established_runtime(data) {
        return Ok(TcpEstablishedStateRuntime {
            access: TcpSessionAccessSlot::new(),
            backend: TcpEstablishedBackendSlot::new(),
        });
    }
    let slot = data.usize_word(0)?;
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP established runtime slot is invalid"))
    })
}

fn sync_tcp_established_runtime(
    data: NodeRuntimeData,
    access: TcpSessionAccessSlot,
    backend: TcpEstablishedBackendSlot,
) -> CoreResult<()> {
    if !has_tcp_established_runtime(data) {
        return Ok(());
    }
    let slot = data.usize_word(0)?;
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP established runtime slot is invalid"))?;
        runtime.access = access;
        runtime.backend = backend;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpEstablishedNext)]
pub struct TcpEstablishedNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    access: TcpSessionAccessSlot,
    #[node(default)]
    backend: TcpEstablishedBackendSlot,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpEstablishedNode {
    #[inline]
    pub(crate) fn with_runtime(
        mut self,
        access: TcpSessionAccessSlot,
        backend: TcpEstablishedBackendSlot,
    ) -> Self {
        if has_tcp_established_runtime(self.runtime_data) {
            let _ =
                sync_tcp_established_runtime(self.runtime_data, access.clone(), backend.clone());
        } else if let Ok(runtime_data) =
            register_tcp_established_runtime(access.clone(), backend.clone())
        {
            self.runtime_data = runtime_data;
        }
        self.access = access;
        self.backend = backend;
        self
    }
}

impl Node for TcpEstablishedNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_established_runtime(self.runtime_data, self.access.clone(), self.backend.clone())?;
        let next = Self::runtime_nexts(runtime)?;
        let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
        let result = tcp_established_route_frame(
            runtime,
            frame,
            rcv_process,
            &self.access,
            &self.backend.load(),
        )?;
        self.cached_next = Some(rcv_process);
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_established_runtime(self.runtime_data, self.access.clone(), self.backend.clone())?;
        Ok(self.runtime_data)
    }
}

fn tcp_established_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_established_runtime(data)?;
    let next = TcpEstablishedNode::runtime_nexts(runtime)?;
    let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
    tcp_established_route_frame(
        runtime,
        frame,
        rcv_process,
        &state.access,
        &state.backend.load(),
    )
}

fn tcp_established_route_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    rcv_process: NodeId,
    access: &TcpSessionAccessSlot,
    backend: &TcpEstablishedBackendHandle,
) -> CoreResult<NodeResult> {
    let original = frame.drain_pending().collect::<hammer_infra::vec::Vec<_>>();
    let mut output = TcpEstablishedRouteOutput::new(rcv_process);
    for (offset, index) in original.iter().copied().enumerate() {
        let action = match tcp_established_action_for_index(runtime, index, access, backend) {
            Ok(action) => action,
            Err(err) => {
                output.free(runtime);
                runtime.free_index(index);
                tcp_established_free_route_tail(runtime, &original, offset + 1);
                return Err(err);
            }
        };
        match action {
            TcpEstablishedAction::Deliver {
                connection_id,
                segments,
            } => {
                for (segment_offset, segment) in segments.iter().copied().enumerate() {
                    if let Err(err) = output.push_delivery(runtime, frame, segment, connection_id) {
                        runtime.free_index(segment.index);
                        tcp_established_free_deliveries(runtime, &segments, segment_offset + 1);
                        output.free(runtime);
                        tcp_established_free_route_tail(runtime, &original, offset + 1);
                        return Err(err);
                    }
                }
            }
            TcpEstablishedAction::Consumed => {}
            TcpEstablishedAction::Drop => runtime.free_index(index),
            TcpEstablishedAction::PassThrough => {
                if let Err(err) = output.push_index(runtime, frame, index) {
                    output.free(runtime);
                    runtime.free_index(index);
                    tcp_established_free_route_tail(runtime, &original, offset + 1);
                    return Err(err);
                }
            }
        }
    }
    output.result(frame)
}

fn tcp_established_free_route_tail(
    runtime: &DataPlaneRuntime,
    indices: &[BufferIndex],
    start: usize,
) {
    for index in indices[start.min(indices.len())..].iter().copied() {
        runtime.free_index(index);
    }
}

fn tcp_established_free_deliveries(
    runtime: &DataPlaneRuntime,
    deliveries: &[TcpEstablishedDelivery],
    start: usize,
) {
    for delivery in deliveries[start.min(deliveries.len())..].iter().copied() {
        runtime.free_index(delivery.index);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TcpEstablishedAction {
    PassThrough,
    Deliver {
        connection_id: TcpLookupId,
        segments: hammer_infra::vec::Vec<TcpEstablishedDelivery>,
    },
    Consumed,
    Drop,
}

struct TcpEstablishedRouteOutput {
    rcv_process: NodeId,
    frames: hammer_infra::vec::Vec<FrameIndex>,
}

impl TcpEstablishedRouteOutput {
    #[inline]
    fn new(rcv_process: NodeId) -> Self {
        Self {
            rcv_process,
            frames: hammer_infra::vec::Vec::new(),
        }
    }

    fn push_delivery(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        segment: TcpEstablishedDelivery,
        connection_id: TcpLookupId,
    ) -> CoreResult<()> {
        let target = self.ensure_target(runtime, frame)?;
        mark_pending_tcp_app_ingress(segment.index, connection_id, segment.fin)?;
        self.push_index_to_target(runtime, frame, target, segment.index)
    }

    fn push_index(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let target = self.ensure_target(runtime, frame)?;
        self.push_index_to_target(runtime, frame, target, index)
    }

    fn ensure_target(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &BufferFrame,
    ) -> CoreResult<TcpEstablishedRouteTarget> {
        if frame.remaining_capacity() != 0 {
            return Ok(TcpEstablishedRouteTarget::Current);
        }
        let frame_index = runtime.alloc_frame_index()?;
        self.frames.push(frame_index);
        Ok(TcpEstablishedRouteTarget::Extra(frame_index))
    }

    fn push_index_to_target(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        target: TcpEstablishedRouteTarget,
        index: BufferIndex,
    ) -> CoreResult<()> {
        match target {
            TcpEstablishedRouteTarget::Current => frame.push_index(index),
            TcpEstablishedRouteTarget::Extra(frame_index) => {
                runtime.get_frame_mut(frame_index)?.push_index(index)
            }
        }
    }

    fn result(self, frame: &BufferFrame) -> CoreResult<NodeResult> {
        if frame.has_pending() && self.frames.is_empty() {
            return Ok(NodeResult::next_current(self.rcv_process));
        }
        let mut nexts = hammer_infra::vec::Vec::with_capacity(
            usize::from(frame.has_pending()) + self.frames.len(),
        );
        if frame.has_pending() {
            nexts.push(NextFrame::Current(self.rcv_process));
        }
        for frame in self.frames {
            nexts.push(NextFrame::Frame {
                node: self.rcv_process,
                frame,
            });
        }
        NodeResult::try_next_frames(nexts)
    }

    fn free(&mut self, runtime: &DataPlaneRuntime) {
        for frame in self.frames.drain(..) {
            let _ = runtime.free_frame_index(frame);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpEstablishedRouteTarget {
    Current,
    Extra(FrameIndex),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpEstablishedDelivery {
    index: BufferIndex,
    fin: bool,
}

fn tcp_established_action_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    access: &TcpSessionAccessSlot,
    backend: &TcpEstablishedBackendHandle,
) -> CoreResult<TcpEstablishedAction> {
    let Some(pending) = take_pending_tcp_app_ingress(index)? else {
        return Ok(TcpEstablishedAction::PassThrough);
    };
    let connection = access.connection_view(pending.connection_id)?;
    let connection_state = connection.map(|connection| connection.state);
    let segment = tcp_observed_segment(runtime, index)?;
    if !tcp_segment_is_sequence_acceptable(connection, segment) {
        tcp_observe_receive_ack(
            runtime,
            index,
            backend,
            connection,
            TcpReceiveAckKind::Challenge,
            TcpReceiveAckReason::NotAcceptable,
            connection
                .map(|connection| connection.rcv_nxt)
                .unwrap_or_default(),
        )?;
        return Ok(TcpEstablishedAction::Drop);
    }
    let ack_validation = tcp_validate_acknowledgment(connection, segment);
    let ack_state_transition = tcp_state_after_remote_ack(connection_state, ack_validation);
    if segment.rst() {
        tcp_established_reorder_remove_connection(pending.connection_id, runtime)?;
        access.apply_receive_progress(
            pending.connection_id,
            TcpReceiveProgress {
                state: Some(TcpState::Closed),
                sequence: segment.sequence,
                acknowledgment: ack_validation.accepted_acknowledgment(),
                advertised_window: segment.advertised_window,
                next_receive_sequence: None,
            },
        );
        if let Some(observation) = tcp_established_close_observation(
            runtime,
            index,
            connection,
            TcpCloseReason::RemoteReset,
        )? {
            backend.observe_close(observation)?;
        }
        runtime.free_index(index);
        return Ok(TcpEstablishedAction::Consumed);
    }
    if segment.syn() {
        return Ok(TcpEstablishedAction::Drop);
    }
    if ack_validation.is_invalid() {
        tcp_observe_receive_ack(
            runtime,
            index,
            backend,
            connection,
            TcpReceiveAckKind::Challenge,
            TcpReceiveAckReason::InvalidAck,
            connection
                .map(|connection| connection.rcv_nxt)
                .unwrap_or_default(),
        )?;
        return Ok(TcpEstablishedAction::Drop);
    }
    let starts_at_receive_next = tcp_segment_starts_at_receive_next(connection, segment);
    let next_state = tcp_state_after_receive_segment(
        connection_state,
        segment,
        ack_validation,
        ack_state_transition,
        starts_at_receive_next,
    );
    let deliver_to_app = tcp_segment_is_deliverable_to_app(
        connection_state,
        segment,
        ack_validation,
        starts_at_receive_next,
    );
    let should_apply_progress = deliver_to_app
        || next_state.is_some()
        || ack_validation.accepted_acknowledgment().is_some();
    if tcp_segment_can_buffer_out_of_order(
        connection,
        connection_state,
        segment,
        ack_validation,
        starts_at_receive_next,
    ) {
        tcp_observe_receive_ack(
            runtime,
            index,
            backend,
            connection,
            TcpReceiveAckKind::Ack,
            TcpReceiveAckReason::Gap,
            connection
                .map(|connection| connection.rcv_nxt)
                .unwrap_or_default(),
        )?;
        tcp_established_reorder_buffer_segment(pending.connection_id, index, segment, runtime)?;
        if should_apply_progress {
            access.apply_receive_progress(
                pending.connection_id,
                TcpReceiveProgress {
                    state: next_state,
                    sequence: segment.sequence,
                    acknowledgment: ack_validation.accepted_acknowledgment(),
                    advertised_window: segment.advertised_window,
                    next_receive_sequence: None,
                },
            );
            if let Some(observation) = tcp_established_ack_observation(
                connection,
                ack_validation,
                segment.advertised_window,
                ack_state_transition,
            ) {
                backend.observe_ack_progress(observation)?;
            }
        }
        return Ok(TcpEstablishedAction::Consumed);
    }
    if !should_apply_progress {
        runtime.free_index(index);
        return Ok(TcpEstablishedAction::Consumed);
    }
    let mut next_receive_sequence = if deliver_to_app {
        segment.next_receive_sequence()
    } else {
        None
    };
    let delivered_buffered = if deliver_to_app && !segment.fin() {
        let receive_next_after_segment =
            next_receive_sequence.expect("deliverable TCP segment advances receive sequence");
        tcp_established_reorder_take_contiguous(
            pending.connection_id,
            receive_next_after_segment,
            runtime,
        )?
    } else {
        hammer_infra::vec::Vec::new()
    };
    if let Some(last) = delivered_buffered.last() {
        next_receive_sequence = Some(last.end_sequence);
    }
    if let Some(receive_acknowledgment) = tcp_receive_ack_for_segment(
        connection,
        segment,
        starts_at_receive_next,
        next_receive_sequence,
    ) {
        let reason = tcp_receive_ack_reason(segment, starts_at_receive_next);
        tcp_observe_receive_ack(
            runtime,
            index,
            backend,
            connection,
            TcpReceiveAckKind::Ack,
            reason,
            receive_acknowledgment,
        )?;
    }
    access.apply_receive_progress(
        pending.connection_id,
        TcpReceiveProgress {
            state: next_state,
            sequence: segment.sequence,
            acknowledgment: ack_validation.accepted_acknowledgment(),
            advertised_window: segment.advertised_window,
            next_receive_sequence,
        },
    );
    if let Some(observation) = tcp_established_ack_observation(
        connection,
        ack_validation,
        segment.advertised_window,
        ack_state_transition,
    ) {
        backend.observe_ack_progress(observation)?;
    }
    if tcp_should_observe_remote_fin(
        connection_state,
        next_state,
        segment,
        ack_validation,
        starts_at_receive_next,
    ) {
        if let Some(observation) = tcp_established_close_observation(
            runtime,
            index,
            connection,
            TcpCloseReason::RemoteFin,
        )? {
            backend.observe_close(observation)?;
        }
    }
    if next_state == Some(TcpState::Closed) || (deliver_to_app && segment.fin()) {
        tcp_established_reorder_remove_connection(pending.connection_id, runtime)?;
    }
    Ok(if deliver_to_app {
        let mut segments = hammer_infra::vec::Vec::with_capacity(1 + delivered_buffered.len());
        segments.push(TcpEstablishedDelivery {
            index,
            fin: segment.fin(),
        });
        segments.extend(
            delivered_buffered
                .into_iter()
                .map(TcpEstablishedBufferedSegment::into_delivery),
        );
        TcpEstablishedAction::Deliver {
            connection_id: pending.connection_id,
            segments,
        }
    } else if segment.fin() || segment.has_payload() {
        runtime.free_index(index);
        TcpEstablishedAction::Consumed
    } else {
        TcpEstablishedAction::PassThrough
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpAckValidation {
    Missing,
    Accepted {
        acknowledgment: u32,
        acknowledges_local_fin: bool,
    },
    Stale,
    Invalid,
}

impl TcpAckValidation {
    #[inline]
    fn accepted_acknowledgment(self) -> Option<u32> {
        match self {
            Self::Accepted { acknowledgment, .. } => Some(acknowledgment),
            Self::Missing | Self::Stale | Self::Invalid => None,
        }
    }

    #[inline]
    fn is_invalid(self) -> bool {
        matches!(self, Self::Invalid)
    }

    #[inline]
    fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    #[inline]
    fn acknowledges_local_fin(self) -> bool {
        matches!(
            self,
            Self::Accepted {
                acknowledges_local_fin: true,
                ..
            }
        )
    }
}

#[inline]
fn tcp_state_allows_app_delivery(state: TcpState) -> bool {
    matches!(
        state,
        TcpState::Established | TcpState::FinWait1 | TcpState::FinWait2
    )
}

fn tcp_established_close_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    connection: Option<TcpConnectionView>,
    reason: TcpCloseReason,
) -> CoreResult<Option<TcpEstablishedObservation>> {
    let Some(connection) = connection else {
        return Ok(None);
    };
    let Some(connection_id) = connection.connection_id else {
        return Ok(None);
    };
    Ok(
        if let Some((local, remote)) =
            tcp_established_observation_endpoints(runtime, index, connection)?
        {
            Some(TcpEstablishedObservation {
                lookup_id: connection.lookup_id,
                connection_id,
                local,
                remote,
                reason,
            })
        } else {
            None
        },
    )
}

fn tcp_observe_receive_ack(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    backend: &TcpEstablishedBackendHandle,
    connection: Option<TcpConnectionView>,
    kind: TcpReceiveAckKind,
    reason: TcpReceiveAckReason,
    receive_acknowledgment: u32,
) -> CoreResult<()> {
    if let Some(observation) = tcp_receive_ack_observation(
        runtime,
        index,
        connection,
        kind,
        reason,
        receive_acknowledgment,
    )? {
        backend.observe_receive_ack(observation)?;
    }
    Ok(())
}

fn tcp_receive_ack_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    connection: Option<TcpConnectionView>,
    kind: TcpReceiveAckKind,
    reason: TcpReceiveAckReason,
    receive_acknowledgment: u32,
) -> CoreResult<Option<TcpReceiveAckObservation>> {
    let Some(connection) = connection else {
        return Ok(None);
    };
    let Some(connection_id) = connection.connection_id else {
        return Ok(None);
    };
    Ok(
        if let Some((local, remote)) =
            tcp_established_observation_endpoints(runtime, index, connection)?
        {
            Some(TcpReceiveAckObservation {
                lookup_id: connection.lookup_id,
                connection_id,
                local,
                remote,
                send_sequence: connection.snd_nxt,
                receive_acknowledgment,
                advertised_window: connection.rcv_wnd,
                kind,
                reason,
            })
        } else {
            None
        },
    )
}

fn tcp_established_observation_endpoints(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    connection: TcpConnectionView,
) -> CoreResult<Option<(SocketAddr, SocketAddr)>> {
    let metadata = runtime.metadata(index)?;
    let local = connection.local.or_else(|| {
        metadata
            .destination
            .as_ref()
            .map(|destination| SocketAddr::new(destination.host, destination.port))
    });
    let remote = Some(connection.remote).or_else(|| {
        metadata
            .source
            .as_ref()
            .map(|source| SocketAddr::new(source.host, source.port))
    });
    Ok(match (local, remote) {
        (Some(local), Some(remote)) => Some((local, remote)),
        _ => None,
    })
}

#[inline]
fn tcp_receive_ack_for_segment(
    connection: Option<TcpConnectionView>,
    segment: TcpObservedSegment,
    starts_at_receive_next: bool,
    next_receive_sequence: Option<u32>,
) -> Option<u32> {
    if !segment.fin() && !segment.has_payload() {
        return None;
    }
    if starts_at_receive_next {
        return next_receive_sequence;
    }
    connection.map(|connection| connection.rcv_nxt)
}

#[inline]
fn tcp_receive_ack_reason(
    segment: TcpObservedSegment,
    starts_at_receive_next: bool,
) -> TcpReceiveAckReason {
    if !starts_at_receive_next {
        TcpReceiveAckReason::Gap
    } else if segment.fin() {
        TcpReceiveAckReason::Fin
    } else {
        TcpReceiveAckReason::Data
    }
}

fn tcp_established_ack_observation(
    connection: Option<TcpConnectionView>,
    acknowledgment: TcpAckValidation,
    advertised_window: u32,
    ack_state_transition: Option<TcpState>,
) -> Option<TcpEstablishedAckObservation> {
    let connection = connection?;
    let connection_id = connection.connection_id?;
    let accepted_acknowledgment = acknowledgment.accepted_acknowledgment()?;
    Some(TcpEstablishedAckObservation {
        lookup_id: connection.lookup_id,
        connection_id,
        accepted_acknowledgment,
        advertised_window,
        previous_state: connection.state,
        ack_state_transition,
        acknowledges_local_fin: acknowledgment.acknowledges_local_fin(),
    })
}

#[derive(Debug, Clone, Copy)]
struct TcpObservedSegment {
    flags: u8,
    sequence: u32,
    acknowledgment: Option<u32>,
    advertised_window: u32,
    payload_len: u32,
}

impl TcpObservedSegment {
    #[inline]
    fn fin(self) -> bool {
        self.flags & 0x01 != 0
    }

    #[inline]
    fn rst(self) -> bool {
        self.flags & 0x04 != 0
    }

    #[inline]
    fn has_payload(self) -> bool {
        self.payload_len != 0
    }

    #[inline]
    fn next_receive_sequence(self) -> Option<u32> {
        let advance = self.payload_len + u32::from(self.fin());
        (advance != 0).then_some(self.sequence.wrapping_add(advance))
    }

    #[inline]
    fn sequence_len(self) -> u32 {
        self.payload_len + u32::from(self.fin()) + u32::from(self.syn())
    }

    #[inline]
    fn syn(self) -> bool {
        self.flags & 0x02 != 0
    }
}

fn tcp_observed_segment(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpObservedSegment> {
    let cursor = runtime.get_buffer(index)?.packet_cursor();
    runtime.with_current_chain_io_segments(index, |segments, _total_len| {
        let flags_offset = cursor.transport_header_offset() + 13;
        let sequence_offset = cursor.transport_header_offset() + 4;
        let acknowledgment_offset = cursor.transport_header_offset() + 8;
        let window_offset = cursor.transport_header_offset() + 14;
        let Some(flags) = tcp_chain_byte(segments, flags_offset) else {
            return Ok(TcpObservedSegment {
                flags: 0,
                sequence: 0,
                acknowledgment: None,
                advertised_window: 0,
                payload_len: 0,
            });
        };
        let sequence = tcp_chain_u32(segments, sequence_offset)?.unwrap_or_default();
        let acknowledgment =
            tcp_chain_u32(segments, acknowledgment_offset)?.filter(|_| flags & 0x10 != 0);
        let advertised_window = tcp_chain_u16(segments, window_offset)?
            .map(u32::from)
            .unwrap_or_default();
        let payload_len = (cursor
            .packet_len()
            .saturating_sub(cursor.transport_payload_offset())) as u32;
        Ok(TcpObservedSegment {
            flags,
            sequence,
            acknowledgment,
            advertised_window,
            payload_len,
        })
    })
}

fn tcp_chain_byte(segments: &[&[u8]], offset: usize) -> Option<u8> {
    let mut remaining = offset;
    for segment in segments {
        if remaining < segment.len() {
            return Some(segment[remaining]);
        }
        remaining -= segment.len();
    }
    None
}

fn tcp_chain_array<const N: usize>(
    segments: &[&[u8]],
    offset: usize,
) -> CoreResult<Option<[u8; N]>> {
    let mut bytes = [0u8; N];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let Some(value) = tcp_chain_byte(segments, offset + i) else {
            return Ok(None);
        };
        *byte = value;
    }
    Ok(Some(bytes))
}

#[inline]
fn tcp_chain_u16(segments: &[&[u8]], offset: usize) -> CoreResult<Option<u16>> {
    Ok(tcp_chain_array::<2>(segments, offset)?.map(u16::from_be_bytes))
}

#[inline]
fn tcp_chain_u32(segments: &[&[u8]], offset: usize) -> CoreResult<Option<u32>> {
    Ok(tcp_chain_array::<4>(segments, offset)?.map(u32::from_be_bytes))
}

fn tcp_segment_is_sequence_acceptable(
    connection: Option<TcpConnectionView>,
    segment: TcpObservedSegment,
) -> bool {
    let Some(connection) = connection else {
        return true;
    };
    if !tcp_connection_has_initialized_receive_state(connection) {
        return true;
    }
    let segment_len = segment.sequence_len();
    let receive_window = connection.rcv_wnd;
    if segment_len == 0 {
        if receive_window == 0 {
            return segment.sequence == connection.rcv_nxt;
        }
        return tcp_sequence_in_window(segment.sequence, connection.rcv_nxt, receive_window);
    }
    if receive_window == 0 {
        return false;
    }
    let last_sequence = segment.sequence.wrapping_add(segment_len.wrapping_sub(1));
    tcp_sequence_in_window(segment.sequence, connection.rcv_nxt, receive_window)
        || tcp_sequence_in_window(last_sequence, connection.rcv_nxt, receive_window)
}

fn tcp_validate_acknowledgment(
    connection: Option<TcpConnectionView>,
    segment: TcpObservedSegment,
) -> TcpAckValidation {
    let Some(acknowledgment) = segment.acknowledgment else {
        return TcpAckValidation::Missing;
    };
    let Some(connection) = connection else {
        return TcpAckValidation::Accepted {
            acknowledgment,
            acknowledges_local_fin: true,
        };
    };
    if !tcp_connection_has_initialized_send_state(connection) {
        return TcpAckValidation::Accepted {
            acknowledgment,
            acknowledges_local_fin: true,
        };
    }
    let acknowledgment = TcpSeq::new(acknowledgment);
    let snd_una = TcpSeq::new(connection.snd_una);
    let snd_nxt = TcpSeq::new(connection.snd_nxt);
    if acknowledgment.before(snd_una) {
        return TcpAckValidation::Stale;
    }
    if acknowledgment.after(snd_nxt) {
        return TcpAckValidation::Invalid;
    }
    TcpAckValidation::Accepted {
        acknowledgment: acknowledgment.raw(),
        acknowledges_local_fin: !acknowledgment.before(snd_nxt),
    }
}

#[inline]
fn tcp_connection_has_initialized_receive_state(connection: TcpConnectionView) -> bool {
    connection.irs != 0 || connection.rcv_nxt != 0
}

#[inline]
fn tcp_segment_starts_at_receive_next(
    connection: Option<TcpConnectionView>,
    segment: TcpObservedSegment,
) -> bool {
    let Some(connection) = connection else {
        return true;
    };
    !tcp_connection_has_initialized_receive_state(connection)
        || segment.sequence == connection.rcv_nxt
}

#[inline]
fn tcp_connection_has_initialized_send_state(connection: TcpConnectionView) -> bool {
    connection.iss != 0 || connection.snd_una != 0 || connection.snd_nxt != 0
}

#[inline]
fn tcp_sequence_in_window(sequence: u32, window_start: u32, window_len: u32) -> bool {
    TcpSeq::new(window_start).distance_to(TcpSeq::new(sequence)) < window_len
}

#[derive(Debug)]
struct TcpEstablishedReorderStore {
    connections: hammer_infra::vec::Vec<TcpEstablishedReorderConnection>,
}

impl TcpEstablishedReorderStore {
    #[inline]
    fn new() -> Self {
        Self {
            connections: hammer_infra::vec::Vec::with_capacity(
                TCP_ESTABLISHED_REORDER_CONNECTION_CAP,
            ),
        }
    }

    fn insert(
        &mut self,
        connection_id: TcpLookupId,
        segment: TcpEstablishedBufferedSegment,
        runtime: &DataPlaneRuntime,
    ) {
        if let Some(connection) = self.connection_mut(connection_id) {
            connection.insert(segment, runtime);
            return;
        }
        if self.connections.len() == TCP_ESTABLISHED_REORDER_CONNECTION_CAP {
            let mut removed = tcp_established_vec_remove(&mut self.connections, 0)
                .expect("reorder connection cap reached with non-empty table");
            removed.free(runtime);
        }
        let mut connection = TcpEstablishedReorderConnection::new(connection_id);
        connection.insert(segment, runtime);
        self.connections.push(connection);
    }

    fn take_contiguous(
        &mut self,
        connection_id: TcpLookupId,
        receive_next: u32,
    ) -> hammer_infra::vec::Vec<TcpEstablishedBufferedSegment> {
        let Some(position) = self.position(connection_id) else {
            return hammer_infra::vec::Vec::new();
        };
        let delivered = self.connections[position].take_contiguous(receive_next);
        if self.connections[position].segments.is_empty() {
            let _ = tcp_established_vec_remove(&mut self.connections, position);
        }
        delivered
    }

    fn remove_connection(&mut self, connection_id: TcpLookupId, runtime: &DataPlaneRuntime) {
        let Some(position) = self.position(connection_id) else {
            return;
        };
        let mut removed = tcp_established_vec_remove(&mut self.connections, position)
            .expect("reorder connection position should be valid");
        removed.free(runtime);
    }

    #[inline]
    fn position(&self, connection_id: TcpLookupId) -> Option<usize> {
        self.connections
            .iter()
            .position(|connection| connection.connection_id == connection_id)
    }

    #[inline]
    fn connection_mut(
        &mut self,
        connection_id: TcpLookupId,
    ) -> Option<&mut TcpEstablishedReorderConnection> {
        self.connections
            .iter_mut()
            .find(|connection| connection.connection_id == connection_id)
    }
}

#[derive(Debug)]
struct TcpEstablishedReorderConnection {
    connection_id: TcpLookupId,
    segments: hammer_infra::vec::Vec<TcpEstablishedBufferedSegment>,
}

impl TcpEstablishedReorderConnection {
    #[inline]
    fn new(connection_id: TcpLookupId) -> Self {
        Self {
            connection_id,
            segments: hammer_infra::vec::Vec::with_capacity(TCP_ESTABLISHED_REORDER_SEGMENT_CAP),
        }
    }

    fn insert(&mut self, segment: TcpEstablishedBufferedSegment, runtime: &DataPlaneRuntime) {
        let mut insert_at = self.segments.len();
        let mut replace_at = None;
        for (offset, existing) in self.segments.iter().enumerate() {
            if existing.start_sequence == segment.start_sequence {
                replace_at = Some(offset);
                break;
            }
            if TcpSeq::new(segment.start_sequence).before(TcpSeq::new(existing.start_sequence)) {
                insert_at = offset;
                break;
            }
        }
        if let Some(offset) = replace_at {
            let replaced = tcp_established_vec_remove(&mut self.segments, offset)
                .expect("replace offset should be valid");
            runtime.free_index(replaced.index);
            insert_at = offset;
        } else if self.segments.len() == TCP_ESTABLISHED_REORDER_SEGMENT_CAP {
            if insert_at == self.segments.len() {
                runtime.free_index(segment.index);
                return;
            }
            let evicted = self
                .segments
                .pop()
                .expect("reorder segment cap reached with non-empty table");
            runtime.free_index(evicted.index);
        }
        tcp_established_vec_insert(&mut self.segments, insert_at, segment);
    }

    fn take_contiguous(
        &mut self,
        receive_next: u32,
    ) -> hammer_infra::vec::Vec<TcpEstablishedBufferedSegment> {
        let mut delivered = hammer_infra::vec::Vec::new();
        let mut current = receive_next;
        while let Some(segment) = self.segments.first().copied() {
            if segment.start_sequence != current {
                break;
            }
            let segment = tcp_established_vec_remove(&mut self.segments, 0)
                .expect("first reorder segment should be removable");
            current = segment.end_sequence;
            delivered.push(segment);
        }
        delivered
    }

    fn free(&mut self, runtime: &DataPlaneRuntime) {
        for segment in self.segments.drain(..) {
            runtime.free_index(segment.index);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpEstablishedBufferedSegment {
    index: BufferIndex,
    start_sequence: u32,
    end_sequence: u32,
    fin: bool,
}

impl TcpEstablishedBufferedSegment {
    #[inline]
    fn into_delivery(self) -> TcpEstablishedDelivery {
        TcpEstablishedDelivery {
            index: self.index,
            fin: self.fin,
        }
    }
}

#[inline]
fn tcp_segment_can_buffer_out_of_order(
    connection: Option<TcpConnectionView>,
    state: Option<TcpState>,
    segment: TcpObservedSegment,
    acknowledgment: TcpAckValidation,
    starts_at_receive_next: bool,
) -> bool {
    connection.is_some()
        && !starts_at_receive_next
        && segment.has_payload()
        && !segment.fin()
        && acknowledgment.is_accepted()
        && state.is_none_or(tcp_state_allows_app_delivery)
}

fn tcp_established_reorder_buffer_segment(
    connection_id: TcpLookupId,
    index: BufferIndex,
    segment: TcpObservedSegment,
    runtime: &DataPlaneRuntime,
) -> CoreResult<()> {
    let Some(end_sequence) = segment.next_receive_sequence() else {
        return Ok(());
    };
    TCP_ESTABLISHED_REORDER.with(|store| {
        store
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("TCP established reorder store borrowed"))?
            .insert(
                connection_id,
                TcpEstablishedBufferedSegment {
                    index,
                    start_sequence: segment.sequence,
                    end_sequence,
                    fin: segment.fin(),
                },
                runtime,
            );
        Ok(())
    })
}

fn tcp_established_reorder_take_contiguous(
    connection_id: TcpLookupId,
    receive_next: u32,
    runtime: &DataPlaneRuntime,
) -> CoreResult<hammer_infra::vec::Vec<TcpEstablishedBufferedSegment>> {
    let delivered = TCP_ESTABLISHED_REORDER.with(|store| {
        Ok(store
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("TCP established reorder store borrowed"))?
            .take_contiguous(connection_id, receive_next))
    })?;
    for segment in &delivered {
        runtime.prefetch_read(segment.index);
    }
    Ok(delivered)
}

fn tcp_established_vec_remove<T>(
    values: &mut hammer_infra::vec::Vec<T>,
    index: usize,
) -> Option<T> {
    if index >= values.len() {
        return None;
    }
    let mut tail = values.drain(index..);
    let removed = tail.next()?;
    let mut kept = hammer_infra::vec::Vec::with_capacity(tail.len());
    kept.extend(tail);
    values.extend(kept);
    Some(removed)
}

fn tcp_established_vec_insert<T>(values: &mut hammer_infra::vec::Vec<T>, index: usize, value: T) {
    debug_assert!(index <= values.len());
    let tail = values.drain(index..);
    let mut kept = hammer_infra::vec::Vec::with_capacity(tail.len());
    kept.extend(tail);
    values.push(value);
    values.extend(kept);
}

fn tcp_established_reorder_remove_connection(
    connection_id: TcpLookupId,
    runtime: &DataPlaneRuntime,
) -> CoreResult<()> {
    TCP_ESTABLISHED_REORDER.with(|store| {
        store
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("TCP established reorder store borrowed"))?
            .remove_connection(connection_id, runtime);
        Ok(())
    })
}

#[inline]
fn tcp_state_after_receive_segment(
    state: Option<TcpState>,
    segment: TcpObservedSegment,
    acknowledgment: TcpAckValidation,
    ack_state_transition: Option<TcpState>,
    starts_at_receive_next: bool,
) -> Option<TcpState> {
    let state = state?;
    match state {
        TcpState::Established => {
            (segment.fin() && starts_at_receive_next && acknowledgment.is_accepted())
                .then_some(TcpState::CloseWait)
        }
        TcpState::FinWait1 => {
            if segment.fin() {
                if !starts_at_receive_next {
                    ack_state_transition
                } else if !acknowledgment.is_accepted() {
                    None
                } else if acknowledgment.acknowledges_local_fin() {
                    Some(TcpState::TimeWait)
                } else {
                    Some(TcpState::Closing)
                }
            } else {
                ack_state_transition
            }
        }
        TcpState::FinWait2 => {
            (segment.fin() && starts_at_receive_next && acknowledgment.is_accepted())
                .then_some(TcpState::TimeWait)
        }
        TcpState::Closing | TcpState::LastAck => ack_state_transition,
        TcpState::CloseWait
        | TcpState::TimeWait
        | TcpState::Listen
        | TcpState::SynSent
        | TcpState::SynRcvd
        | TcpState::Closed => None,
    }
}

#[inline]
fn tcp_state_after_remote_ack(
    state: Option<TcpState>,
    acknowledgment: TcpAckValidation,
) -> Option<TcpState> {
    let state = state?;
    if !acknowledgment.acknowledges_local_fin() {
        return None;
    }
    match state {
        TcpState::FinWait1 => Some(TcpState::FinWait2),
        TcpState::Closing => Some(TcpState::TimeWait),
        TcpState::LastAck => Some(TcpState::Closed),
        _ => None,
    }
}

#[inline]
fn tcp_segment_is_deliverable_to_app(
    state: Option<TcpState>,
    segment: TcpObservedSegment,
    acknowledgment: TcpAckValidation,
    starts_at_receive_next: bool,
) -> bool {
    starts_at_receive_next
        && (segment.fin() || segment.has_payload())
        && acknowledgment.is_accepted()
        && state.is_none_or(tcp_state_allows_app_delivery)
}

#[inline]
fn tcp_should_observe_remote_fin(
    state: Option<TcpState>,
    next_state: Option<TcpState>,
    segment: TcpObservedSegment,
    acknowledgment: TcpAckValidation,
    starts_at_receive_next: bool,
) -> bool {
    starts_at_receive_next
        && segment.fin()
        && acknowledgment.is_accepted()
        && next_state.is_some()
        && matches!(
            state,
            Some(TcpState::Established | TcpState::FinWait1 | TcpState::FinWait2)
        )
}
