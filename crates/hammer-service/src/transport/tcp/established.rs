use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, FrameIndex, NextFrame, Node, NodeId, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpCloseReason, TcpConnectionId, TcpSeq, TcpState};
use hammer_infra::vec::Vec as InfraVec;

use super::TcpLookupId;
use super::connection::{
    TcpConnectionSnapshot, TcpEstablishedSnapshotHandle, TcpReceiveProgress,
    default_established_snapshot_handle,
};
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

    fn observe_close(&self, observation: TcpEstablishedObservation) -> CoreResult<()>;
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
    snapshot: TcpEstablishedSnapshotHandle,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
}

thread_local! {
    static TCP_ESTABLISHED_RUNTIMES: RefCell<InfraVec<TcpEstablishedStateRuntime>> =
        const { RefCell::new(InfraVec::new()) };
    static TCP_ESTABLISHED_REORDER: RefCell<TcpEstablishedReorderStore> =
        RefCell::new(TcpEstablishedReorderStore::new());
}

#[inline]
fn has_tcp_established_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_established_runtime(
    snapshot: TcpEstablishedSnapshotHandle,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
) -> CoreResult<NodeRuntimeData> {
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpEstablishedStateRuntime { snapshot, backend });
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
            snapshot: default_established_snapshot_handle(),
            backend: None,
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
    snapshot: TcpEstablishedSnapshotHandle,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
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
        runtime.snapshot = snapshot;
        runtime.backend = backend;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpEstablishedNext)]
pub struct TcpEstablishedNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default = default_established_snapshot_handle())]
    snapshot: TcpEstablishedSnapshotHandle,
    #[node(default)]
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpEstablishedNode {
    #[inline]
    pub(crate) fn with_runtime(
        mut self,
        snapshot: TcpEstablishedSnapshotHandle,
        backend: Option<Arc<dyn TcpEstablishedBackend>>,
    ) -> Self {
        if has_tcp_established_runtime(self.runtime_data) {
            let _ =
                sync_tcp_established_runtime(self.runtime_data, snapshot.clone(), backend.clone());
        } else if let Ok(runtime_data) =
            register_tcp_established_runtime(snapshot.clone(), backend.clone())
        {
            self.runtime_data = runtime_data;
        }
        self.snapshot = snapshot;
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
        sync_tcp_established_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            self.backend.clone(),
        )?;
        let next = Self::runtime_nexts(runtime)?;
        let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
        let result = tcp_established_route_frame(
            runtime,
            frame,
            rcv_process,
            &self.snapshot,
            self.backend.as_deref(),
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
        sync_tcp_established_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            self.backend.clone(),
        )?;
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
        &state.snapshot,
        state.backend.as_deref(),
    )
}

fn tcp_established_route_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    rcv_process: NodeId,
    snapshot: &TcpEstablishedSnapshotHandle,
    backend: Option<&dyn TcpEstablishedBackend>,
) -> CoreResult<NodeResult> {
    let original = frame.drain_pending().collect::<std::vec::Vec<_>>();
    let mut output = TcpEstablishedRouteOutput::new(rcv_process);
    for (offset, index) in original.iter().copied().enumerate() {
        let action = match tcp_established_action_for_index(runtime, index, snapshot, backend) {
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
        segments: std::vec::Vec<TcpEstablishedDelivery>,
    },
    Consumed,
    Drop,
}

struct TcpEstablishedRouteOutput {
    rcv_process: NodeId,
    frames: std::vec::Vec<FrameIndex>,
}

impl TcpEstablishedRouteOutput {
    #[inline]
    fn new(rcv_process: NodeId) -> Self {
        Self {
            rcv_process,
            frames: std::vec::Vec::new(),
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
        let mut nexts =
            std::vec::Vec::with_capacity(usize::from(frame.has_pending()) + self.frames.len());
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
    snapshot: &TcpEstablishedSnapshotHandle,
    backend: Option<&dyn TcpEstablishedBackend>,
) -> CoreResult<TcpEstablishedAction> {
    let Some(pending) = take_pending_tcp_app_ingress(index)? else {
        return Ok(TcpEstablishedAction::PassThrough);
    };
    let connection = snapshot.connection(pending.connection_id);
    let connection_state = connection.map(|connection| connection.state);
    let segment = tcp_observed_segment(runtime, index)?;
    if !tcp_segment_is_sequence_acceptable(connection, segment) {
        return Ok(TcpEstablishedAction::Drop);
    }
    let ack_validation = tcp_validate_acknowledgment(connection, segment);
    let ack_state_transition = tcp_state_after_remote_ack(connection_state, ack_validation);
    if segment.rst() {
        tcp_established_reorder_remove_connection(pending.connection_id, runtime)?;
        snapshot.apply_receive_progress(
            pending.connection_id,
            TcpReceiveProgress {
                state: Some(TcpState::Closed),
                sequence: segment.sequence,
                acknowledgment: ack_validation.accepted_acknowledgment(),
                advertised_window: segment.advertised_window,
                next_receive_sequence: None,
            },
        );
        if let Some(backend) = backend
            && let Some(observation) = tcp_established_close_observation(
                runtime,
                index,
                connection,
                TcpCloseReason::RemoteReset,
            )?
        {
            backend.observe_close(observation)?;
        }
        runtime.free_index(index);
        return Ok(TcpEstablishedAction::Consumed);
    }
    if segment.syn() {
        return Ok(TcpEstablishedAction::Drop);
    }
    if ack_validation.is_invalid() {
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
        tcp_established_reorder_buffer_segment(pending.connection_id, index, segment, runtime)?;
        if should_apply_progress {
            snapshot.apply_receive_progress(
                pending.connection_id,
                TcpReceiveProgress {
                    state: next_state,
                    sequence: segment.sequence,
                    acknowledgment: ack_validation.accepted_acknowledgment(),
                    advertised_window: segment.advertised_window,
                    next_receive_sequence: None,
                },
            );
            if let Some(backend) = backend
                && let Some(observation) = tcp_established_ack_observation(
                    connection,
                    ack_validation,
                    segment.advertised_window,
                    ack_state_transition,
                )
            {
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
        std::vec::Vec::new()
    };
    if let Some(last) = delivered_buffered.last() {
        next_receive_sequence = Some(last.end_sequence);
    }
    snapshot.apply_receive_progress(
        pending.connection_id,
        TcpReceiveProgress {
            state: next_state,
            sequence: segment.sequence,
            acknowledgment: ack_validation.accepted_acknowledgment(),
            advertised_window: segment.advertised_window,
            next_receive_sequence,
        },
    );
    if let Some(backend) = backend
        && let Some(observation) = tcp_established_ack_observation(
            connection,
            ack_validation,
            segment.advertised_window,
            ack_state_transition,
        )
    {
        backend.observe_ack_progress(observation)?;
    }
    if tcp_should_observe_remote_fin(
        connection_state,
        next_state,
        segment,
        ack_validation,
        starts_at_receive_next,
    ) {
        if let Some(backend) = backend
            && let Some(observation) = tcp_established_close_observation(
                runtime,
                index,
                connection,
                TcpCloseReason::RemoteFin,
            )?
        {
            backend.observe_close(observation)?;
        }
    }
    if deliver_to_app && segment.fin() {
        tcp_established_reorder_remove_connection(pending.connection_id, runtime)?;
    }
    Ok(if deliver_to_app {
        let mut segments = std::vec::Vec::with_capacity(1 + delivered_buffered.len());
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
    connection: Option<TcpConnectionSnapshot>,
    reason: TcpCloseReason,
) -> CoreResult<Option<TcpEstablishedObservation>> {
    let Some(connection) = connection else {
        return Ok(None);
    };
    let Some(connection_id) = connection.connection_id else {
        return Ok(None);
    };
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
        (Some(local), Some(remote)) => Some(TcpEstablishedObservation {
            lookup_id: connection.lookup_id,
            connection_id,
            local,
            remote,
            reason,
        }),
        _ => None,
    })
}

fn tcp_established_ack_observation(
    connection: Option<TcpConnectionSnapshot>,
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
    let packet: std::vec::Vec<u8> = runtime.copy_current_chain(index)?.into_iter().collect();
    let flags_offset = cursor.transport_header_offset() + 13;
    let sequence_offset = cursor.transport_header_offset() + 4;
    let acknowledgment_offset = cursor.transport_header_offset() + 8;
    let window_offset = cursor.transport_header_offset() + 14;
    let Some(flags) = packet.get(flags_offset) else {
        return Ok(TcpObservedSegment {
            flags: 0,
            sequence: 0,
            acknowledgment: None,
            advertised_window: 0,
            payload_len: 0,
        });
    };
    let sequence = packet
        .get(sequence_offset..sequence_offset + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("sequence bytes")))
        .unwrap_or_default();
    let acknowledgment = packet
        .get(acknowledgment_offset..acknowledgment_offset + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("ack bytes")))
        .filter(|_| *flags & 0x10 != 0);
    let advertised_window = packet
        .get(window_offset..window_offset + 2)
        .map(|bytes| u16::from_be_bytes(bytes.try_into().expect("window bytes")) as u32)
        .unwrap_or_default();
    let payload_len = (cursor
        .packet_len()
        .saturating_sub(cursor.transport_payload_offset())) as u32;
    Ok(TcpObservedSegment {
        flags: *flags,
        sequence,
        acknowledgment,
        advertised_window,
        payload_len,
    })
}

fn tcp_segment_is_sequence_acceptable(
    connection: Option<TcpConnectionSnapshot>,
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
    connection: Option<TcpConnectionSnapshot>,
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
fn tcp_connection_has_initialized_receive_state(connection: TcpConnectionSnapshot) -> bool {
    connection.irs != 0 || connection.rcv_nxt != 0
}

#[inline]
fn tcp_segment_starts_at_receive_next(
    connection: Option<TcpConnectionSnapshot>,
    segment: TcpObservedSegment,
) -> bool {
    let Some(connection) = connection else {
        return true;
    };
    !tcp_connection_has_initialized_receive_state(connection)
        || segment.sequence == connection.rcv_nxt
}

#[inline]
fn tcp_connection_has_initialized_send_state(connection: TcpConnectionSnapshot) -> bool {
    connection.iss != 0 || connection.snd_una != 0 || connection.snd_nxt != 0
}

#[inline]
fn tcp_sequence_in_window(sequence: u32, window_start: u32, window_len: u32) -> bool {
    TcpSeq::new(window_start).distance_to(TcpSeq::new(sequence)) < window_len
}

#[derive(Debug)]
struct TcpEstablishedReorderStore {
    connections: std::vec::Vec<TcpEstablishedReorderConnection>,
}

impl TcpEstablishedReorderStore {
    #[inline]
    fn new() -> Self {
        Self {
            connections: std::vec::Vec::with_capacity(TCP_ESTABLISHED_REORDER_CONNECTION_CAP),
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
            let mut removed = self.connections.remove(0);
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
    ) -> std::vec::Vec<TcpEstablishedBufferedSegment> {
        let Some(position) = self.position(connection_id) else {
            return std::vec::Vec::new();
        };
        let delivered = self.connections[position].take_contiguous(receive_next);
        if self.connections[position].segments.is_empty() {
            self.connections.remove(position);
        }
        delivered
    }

    fn remove_connection(&mut self, connection_id: TcpLookupId, runtime: &DataPlaneRuntime) {
        let Some(position) = self.position(connection_id) else {
            return;
        };
        let mut removed = self.connections.remove(position);
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
    segments: std::vec::Vec<TcpEstablishedBufferedSegment>,
}

impl TcpEstablishedReorderConnection {
    #[inline]
    fn new(connection_id: TcpLookupId) -> Self {
        Self {
            connection_id,
            segments: std::vec::Vec::with_capacity(TCP_ESTABLISHED_REORDER_SEGMENT_CAP),
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
            let replaced = self.segments.remove(offset);
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
        self.segments.insert(insert_at, segment);
    }

    fn take_contiguous(
        &mut self,
        receive_next: u32,
    ) -> std::vec::Vec<TcpEstablishedBufferedSegment> {
        let mut delivered = std::vec::Vec::new();
        let mut current = receive_next;
        while let Some(segment) = self.segments.first().copied() {
            if segment.start_sequence != current {
                break;
            }
            let segment = self.segments.remove(0);
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
    connection: Option<TcpConnectionSnapshot>,
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
) -> CoreResult<std::vec::Vec<TcpEstablishedBufferedSegment>> {
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

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, DataWorkerId,
        NodeProcessFn, NodeResult, NodeRuntimeData, RouteMetadata, SocksAddr,
    };
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::protocol::tcp::{TcpCloseReason, TcpConnectionId};
    use hammer_infra::vec::Vec as InfraVec;

    use super::{TcpEstablishedAckObservation, TcpEstablishedBackend, TcpEstablishedObservation};
    use crate::transport::tcp::input::{
        mark_pending_tcp_app_ingress, take_pending_tcp_app_ingress,
    };
    use crate::transport::tcp::{
        TcpConnectionSnapshot, TcpEstablishedControlPlane, TcpEstablishedNext, TcpState,
    };

    const LOOKUP_ID: u32 = 37;
    const CONNECTION_ID: u64 = 73;

    #[derive(Default)]
    struct CaptureState {
        packets: Vec<Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states()
                .lock()
                .expect("capture state registry poisoned");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture state slot"),
            }
        }
    }

    impl hammer_adapter::Node for CaptureNode {
        #[inline(always)]
        fn process(
            &mut self,
            _runtime: &DataPlaneRuntime,
            _frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must run through descriptor process",
            ))
        }

        #[inline]
        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        #[inline]
        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl hammer_adapter::InternalNode for CaptureNode {}

    fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let state = {
            let states = capture_states()
                .lock()
                .map_err(|_| CoreError::internal("capture state registry poisoned"))?;
            Arc::clone(
                states
                    .get(data.usize_word(0)?)
                    .ok_or_else(|| CoreError::internal("capture state slot is invalid"))?,
            )
        };
        for index in frame.drain_pending() {
            let pending = take_pending_tcp_app_ingress(index)?;
            if pending.is_some() {
                let packet = runtime.copy_current_chain(index)?;
                state
                    .lock()
                    .map_err(|_| CoreError::internal("capture state poisoned"))?
                    .packets
                    .push(packet.into_iter().collect());
            }
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    #[derive(Default)]
    struct RecordingTcpEstablishedBackend {
        ack_observations: Arc<Mutex<Vec<TcpEstablishedAckObservation>>>,
        observations: Arc<Mutex<Vec<TcpEstablishedObservation>>>,
    }

    impl TcpEstablishedBackend for RecordingTcpEstablishedBackend {
        fn observe_ack_progress(
            &self,
            observation: TcpEstablishedAckObservation,
        ) -> CoreResult<()> {
            self.ack_observations
                .lock()
                .map_err(|_| CoreError::internal("ack observations poisoned"))?
                .push(observation);
            Ok(())
        }

        fn observe_close(&self, observation: TcpEstablishedObservation) -> CoreResult<()> {
            self.observations
                .lock()
                .map_err(|_| CoreError::internal("established observations poisoned"))?
                .push(observation);
            Ok(())
        }
    }

    #[test]
    fn tcp_established_control_plane_installs_backend_after_construction() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture));
        let backend = Arc::new(RecordingTcpEstablishedBackend::default());
        control.install_backend(backend.clone());

        let local: SocketAddr = "192.0.2.90:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.90:40090".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::FinWait1,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 90),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 90),
            local.port(),
            0x0102_0304,
            0x1020_3044,
            0x2000,
            tcp_flags(false, false, false, true),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
        assert_eq!(backend.ack_observations.lock().unwrap().len(), 1);
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_control_plane_replaces_backend_before_node_construction() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture));
        let stale_backend = Arc::new(RecordingTcpEstablishedBackend::default());
        let active_backend = Arc::new(RecordingTcpEstablishedBackend::default());
        control.install_backend(stale_backend.clone());
        control.install_backend(active_backend.clone());

        let local: SocketAddr = "192.0.2.94:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.94:40094".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::FinWait1,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 94),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 94),
            local.port(),
            0x0102_0304,
            0x1020_3048,
            0x1000,
            tcp_flags(false, false, false, true),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
        assert!(stale_backend.ack_observations.lock().unwrap().is_empty());
        assert_eq!(active_backend.ack_observations.lock().unwrap().len(), 1);
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_observes_partial_ack_progress_without_ack_state_transition() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let backend = Arc::new(RecordingTcpEstablishedBackend::default());
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture))
            .with_backend(backend.clone());

        let local: SocketAddr = "192.0.2.91:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.91:40091".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::FinWait1,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 91),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 91),
            local.port(),
            0x0102_0304,
            0x1020_3044,
            0x2000,
            tcp_flags(false, false, false, true),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
        assert_eq!(
            *backend.ack_observations.lock().unwrap(),
            vec![TcpEstablishedAckObservation {
                lookup_id: LOOKUP_ID,
                connection_id: TcpConnectionId::new(CONNECTION_ID),
                accepted_acknowledgment: 0x1020_3044,
                advertised_window: 0x2000,
                previous_state: TcpState::FinWait1,
                ack_state_transition: None,
                acknowledges_local_fin: false,
            }]
        );
        assert!(backend.observations.lock().unwrap().is_empty());
        assert_eq!(
            control
                .connection_snapshot_for_test(LOOKUP_ID)
                .expect("snapshot")
                .snd_una,
            0x1020_3044
        );
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_observes_ack_driven_state_transition_when_fin_is_fully_acked() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let backend = Arc::new(RecordingTcpEstablishedBackend::default());
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture))
            .with_backend(backend.clone());

        let local: SocketAddr = "192.0.2.92:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.92:40092".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::FinWait1,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 92),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 92),
            local.port(),
            0x0102_0304,
            0x1020_3048,
            0x1000,
            tcp_flags(false, false, false, true),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
        assert_eq!(
            *backend.ack_observations.lock().unwrap(),
            vec![TcpEstablishedAckObservation {
                lookup_id: LOOKUP_ID,
                connection_id: TcpConnectionId::new(CONNECTION_ID),
                accepted_acknowledgment: 0x1020_3048,
                advertised_window: 0x1000,
                previous_state: TcpState::FinWait1,
                ack_state_transition: Some(TcpState::FinWait2),
                acknowledges_local_fin: true,
            }]
        );
        assert_eq!(
            control.connection_state_for_test(LOOKUP_ID),
            Some(TcpState::FinWait2)
        );
        assert!(backend.observations.lock().unwrap().is_empty());
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_suppresses_ack_progress_observation_for_stale_invalid_and_missing_ack()
    {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let backend = Arc::new(RecordingTcpEstablishedBackend::default());
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture))
            .with_backend(backend.clone());

        let local: SocketAddr = "192.0.2.93:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.93:40093".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::FinWait1,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        for packet in [
            ipv4_tcp_packet_with_seq_ack_window(
                Ipv4Addr::new(198, 51, 100, 93),
                remote.port(),
                Ipv4Addr::new(192, 0, 2, 93),
                local.port(),
                0x0102_0304,
                0x1020_303f,
                0x2000,
                tcp_flags(false, false, false, true),
                b"",
            ),
            ipv4_tcp_packet_with_seq_ack_window(
                Ipv4Addr::new(198, 51, 100, 93),
                remote.port(),
                Ipv4Addr::new(192, 0, 2, 93),
                local.port(),
                0x0102_0304,
                0x1020_3049,
                0x2000,
                tcp_flags(false, false, false, true),
                b"",
            ),
            ipv4_tcp_packet_with_seq_ack_window(
                Ipv4Addr::new(198, 51, 100, 93),
                remote.port(),
                Ipv4Addr::new(192, 0, 2, 93),
                local.port(),
                0x0102_0304,
                0,
                0x2000,
                tcp_flags(false, false, false, false),
                b"",
            ),
        ] {
            let frame = runtime.alloc_frame_index().expect("alloc frame");
            let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
            stamp_tcp_cursor(&runtime, buffer, &packet);
            mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false)
                .expect("mark pending app ingress");

            assert!(
                runtime
                    .schedule_frame(established, frame)
                    .expect("schedule")
            );
            assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
        }

        assert!(backend.ack_observations.lock().unwrap().is_empty());
        assert!(backend.observations.lock().unwrap().is_empty());
        let snapshot = control
            .connection_snapshot_for_test(LOOKUP_ID)
            .expect("snapshot after invalid ack traffic");
        assert_eq!(snapshot.snd_una, 0x1020_3040);
        assert_eq!(snapshot.snd_nxt, 0x1020_3048);
        assert_eq!(snapshot.state, TcpState::FinWait1);
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_observes_remote_reset_and_skips_rcv_process_delivery() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let backend = Arc::new(RecordingTcpEstablishedBackend::default());
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture))
            .with_backend(backend.clone());

        let local: SocketAddr = "192.0.2.73:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.73:40073".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::Established,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet(
            Ipv4Addr::new(198, 51, 100, 73),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 73),
            local.port(),
            tcp_flags(false, false, true, false),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
        assert_eq!(
            *backend.observations.lock().unwrap(),
            vec![TcpEstablishedObservation {
                lookup_id: LOOKUP_ID,
                connection_id: TcpConnectionId::new(CONNECTION_ID),
                local,
                remote,
                reason: TcpCloseReason::RemoteReset,
            }]
        );
        assert!(
            capture_state.lock().unwrap().packets.is_empty(),
            "remote reset must not continue into rcv-process delivery"
        );
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_suppresses_retransmitted_fin_delivery_after_remote_half_close() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture));

        let local: SocketAddr = "192.0.2.81:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.81:40081".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::CloseWait,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet(
            Ipv4Addr::new(198, 51, 100, 81),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 81),
            local.port(),
            tcp_flags(true, false, false, true),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
        assert!(
            capture_state.lock().unwrap().packets.is_empty(),
            "close-wait FIN retransmits must not be delivered as repeated app EOF"
        );
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_withholds_out_of_order_payload_from_app_delivery() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture));

        let local: SocketAddr = "192.0.2.95:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.95:40095".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::Established,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: 0x4000,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 95),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 95),
            local.port(),
            0x0102_0308,
            0x1020_3040,
            0x4000,
            tcp_flags(false, false, false, true),
            b"hole",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID, false).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
        assert!(
            capture_state.lock().unwrap().packets.is_empty(),
            "out-of-order payload must not be delivered to the app"
        );
        let snapshot = control
            .connection_snapshot_for_test(LOOKUP_ID)
            .expect("snapshot after out-of-order payload");
        assert_eq!(snapshot.state, TcpState::Established);
        assert_eq!(snapshot.rcv_nxt, 0x0102_0304);
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 1);

        let reset = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 95),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 95),
            local.port(),
            0x0102_0304,
            0x1020_3040,
            0x4000,
            tcp_flags(false, false, true, true),
            b"",
        );
        let reset_frame = runtime.alloc_frame_index().expect("alloc reset frame");
        let reset_buffer = push_packet(&runtime, reset_frame, &reset, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, reset_buffer, &reset);
        mark_pending_tcp_app_ingress(reset_buffer, LOOKUP_ID, false)
            .expect("mark reset pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, reset_frame)
                .expect("schedule reset")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run reset"), 1);
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    #[test]
    fn tcp_established_node_delivers_buffered_out_of_order_payload_after_gap_arrives() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture));

        let local: SocketAddr = "192.0.2.96:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.96:40096".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::Established,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0x1020_303f,
            irs: 0x0102_0303,
            snd_una: 0x1020_3040,
            snd_nxt: 0x1020_3048,
            snd_wnd: 0x4000,
            rcv_nxt: 0x0102_0304,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let late = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 96),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 96),
            local.port(),
            0x0102_0308,
            0x1020_3040,
            0x4000,
            tcp_flags(false, false, false, true),
            b"late",
        );
        let late_frame = runtime.alloc_frame_index().expect("alloc late frame");
        let late_buffer = push_packet(&runtime, late_frame, &late, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, late_buffer, &late);
        mark_pending_tcp_app_ingress(late_buffer, LOOKUP_ID, false)
            .expect("mark late pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, late_frame)
                .expect("schedule late")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run late packet"), 1);
        assert!(
            capture_state.lock().unwrap().packets.is_empty(),
            "out-of-order payload must wait for the missing gap"
        );
        assert_eq!(
            control
                .connection_snapshot_for_test(LOOKUP_ID)
                .expect("snapshot after late payload")
                .rcv_nxt,
            0x0102_0304
        );
        assert_eq!(runtime.in_use_buffers(), 1);

        let gap = ipv4_tcp_packet_with_seq_ack_window(
            Ipv4Addr::new(198, 51, 100, 96),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 96),
            local.port(),
            0x0102_0304,
            0x1020_3040,
            0x4000,
            tcp_flags(false, false, false, true),
            b"gap!",
        );
        let gap_frame = runtime.alloc_frame_index().expect("alloc gap frame");
        let gap_buffer = push_packet(&runtime, gap_frame, &gap, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, gap_buffer, &gap);
        mark_pending_tcp_app_ingress(gap_buffer, LOOKUP_ID, false)
            .expect("mark gap pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, gap_frame)
                .expect("schedule gap")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run gap packet"), 2);
        let captured = capture_state.lock().unwrap().packets.clone();
        assert_eq!(captured, vec![gap, late]);
        assert_eq!(
            control
                .connection_snapshot_for_test(LOOKUP_ID)
                .expect("snapshot after gap fill")
                .rcv_nxt,
            0x0102_030c
        );
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    fn push_packet(
        runtime: &DataPlaneRuntime,
        frame: hammer_adapter::FrameIndex,
        packet: &[u8],
        metadata: RouteMetadata,
    ) -> BufferIndex {
        let buffer = runtime
            .alloc_index_with_bytes(metadata, packet)
            .expect("alloc packet");
        runtime
            .get_frame_mut(frame)
            .expect("mutate frame")
            .push_index(buffer)
            .expect("push packet");
        buffer
    }

    fn stamp_tcp_cursor(runtime: &DataPlaneRuntime, buffer: BufferIndex, packet: &[u8]) {
        let header_len = ((*packet.first().expect("ipv4 header") & 0x0f) as usize) * 4;
        let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        let tcp_offset = header_len;
        let tcp_header_len = ((packet[tcp_offset + 12] >> 4) as usize) * 4;
        runtime
            .get_buffer_mut(buffer)
            .expect("buffer mut")
            .set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, header_len)
                    .with_transport_header(tcp_offset, tcp_header_len)
                    .with_transport_payload_offset(tcp_offset + tcp_header_len),
            );
    }

    fn tcp_metadata(remote: SocketAddr, local: SocketAddr) -> RouteMetadata {
        RouteMetadata {
            source: Some(SocksAddr::ip(remote.ip(), remote.port())),
            destination: Some(SocksAddr::ip(local.ip(), local.port())),
            ..RouteMetadata::default()
        }
    }

    fn ipv4_tcp_packet(
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
        flags: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        ipv4_tcp_packet_with_seq_ack_window(
            source,
            source_port,
            destination,
            destination_port,
            0x0102_0304,
            0x1020_3040,
            u16::MAX,
            flags,
            payload,
        )
    }

    fn ipv4_tcp_packet_with_seq_ack_window(
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
        sequence: u32,
        acknowledgment: u32,
        window: u16,
        flags: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
        write_tcp_segment(
            &mut packet[20..],
            source_port,
            destination_port,
            sequence,
            acknowledgment,
            window,
            flags,
            payload,
        );
        let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
        packet[36..38].copy_from_slice(&checksum.to_be_bytes());
        update_ipv4_header_checksum(&mut packet);
        packet
    }

    fn ipv4_packet(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        transport_len: usize,
    ) -> Vec<u8> {
        let total_len = 20 + transport_len;
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
        packet[8] = 64;
        packet[9] = protocol;
        packet[12..16].copy_from_slice(&source.octets());
        packet[16..20].copy_from_slice(&destination.octets());
        packet
    }

    fn write_tcp_segment(
        out: &mut [u8],
        source_port: u16,
        destination_port: u16,
        sequence: u32,
        acknowledgment: u32,
        window: u16,
        flags: u8,
        payload: &[u8],
    ) {
        out[..2].copy_from_slice(&source_port.to_be_bytes());
        out[2..4].copy_from_slice(&destination_port.to_be_bytes());
        out[4..8].copy_from_slice(&sequence.to_be_bytes());
        out[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
        out[12] = 0x50;
        out[13] = flags;
        out[14..16].copy_from_slice(&window.to_be_bytes());
        out[20..20 + payload.len()].copy_from_slice(payload);
    }

    fn tcp_flags(fin: bool, syn: bool, rst: bool, ack: bool) -> u8 {
        u8::from(fin) | (u8::from(syn) << 1) | (u8::from(rst) << 2) | (u8::from(ack) << 4)
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let mut words = InfraVec::new();
        words.push(u16::from_be_bytes([source.octets()[0], source.octets()[1]]));
        words.push(u16::from_be_bytes([source.octets()[2], source.octets()[3]]));
        words.push(u16::from_be_bytes([
            destination.octets()[0],
            destination.octets()[1],
        ]));
        words.push(u16::from_be_bytes([
            destination.octets()[2],
            destination.octets()[3],
        ]));
        words.push(u16::from(protocol));
        words.push(segment.len() as u16);
        for chunk in segment.chunks(2) {
            let word = if chunk.len() == 2 {
                u16::from_be_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], 0])
            };
            words.push(word);
        }
        internet_checksum(&words)
    }

    fn update_ipv4_header_checksum(packet: &mut [u8]) {
        packet[10] = 0;
        packet[11] = 0;
        let mut words = InfraVec::new();
        for chunk in packet[..20].chunks(2) {
            words.push(u16::from_be_bytes([chunk[0], chunk[1]]));
        }
        let checksum = internet_checksum(&words);
        packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    }

    fn internet_checksum(words: &[u16]) -> u16 {
        let mut sum = 0u32;
        for word in words {
            sum = sum.wrapping_add(u32::from(*word));
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        !(sum as u16)
    }
}
