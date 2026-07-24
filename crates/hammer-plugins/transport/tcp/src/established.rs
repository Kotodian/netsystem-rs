use crate::{TcpWorkerStore, publish_tcp_connection, read_session_id, with_tcp_worker_mut};
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::runtime::RxDelivery;

use super::TcpNodeError;
use super::segment::tcp_packet;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::established::register_tcp_established,
    name = "tcp-established",
    next = TcpEstablishedNext,
    role = internal,
)]
pub struct TcpEstablishedNode {
    process: NodeProcessFn,
}

pub fn register_tcp_established(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    let main = crate::TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp-established") {
        return Ok(node);
    }
    runtime
        .nodes()
        .try_register_internal_with_next_names(
            TcpEstablishedNode::new(main.established_process),
            &TcpEstablishedNext::NEXT_NAMES,
        )
}

impl Node for TcpEstablishedNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        (self.process)(runtime, NodeRuntimeData::empty(), frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }
}

pub(crate) fn tcp_established_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    tcp_established_frame::<C, Seg>(runtime, frame)
}

fn tcp_established_frame<C, Seg>(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
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
        if tcp_established_index(runtime, index, frame, &mut nexts, &mut out_len).is_err() {
            let _ = emit_local(
                runtime,
                frame,
                &mut nexts,
                &mut out_len,
                TcpEstablishedNext::Drop,
                index,
            );
        }
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
    next: TcpEstablishedNext,
    index: Index,
) -> RuntimeResult<()> {
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

fn tcp_established_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<()>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let tx_segment = with_tcp_worker_mut::<C, Seg, _>(|state| {
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime
                .record_current_node_error(TcpNodeError::EstablishedSessionRouteMissing.code());
            TcpNodeError::EstablishedSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_established`/`accept_payload` work below gives
        // the prefetch lead time.
        state.sessions.prefetch_session(session_id);
        let (_, connection_index) = state
            .sessions
            .session_transport(session_id)
            .ok_or(TcpNodeError::EstablishedSessionMissing)?;
        let (
            control,
            acked_tx_len,
            ack_advanced,
            accept_payload,
            accepted_sequence,
            duplicate_payload,
        ) = {
            let crate::worker::TcpWorker {
                connections,
                timers,
                ..
            } = &mut state.tcp;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let previous_snd_una = connection.snd_una();
            let control = connection.receive_established_with_timers(
                connection_index,
                timers,
                &packet,
                std::time::Instant::now(),
            )?;
            let accept_payload = connection.accept_payload(&packet);
            let duplicate_payload = accept_payload.is_none() && packet.payload_len != 0;
            (
                control,
                connection.take_acked_tx_len(previous_snd_una),
                connection.snd_una() != previous_snd_una,
                accept_payload,
                packet.sequence,
                duplicate_payload,
            )
        };
        if acked_tx_len != 0 {
            state
                .sessions
                .ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && state.sessions.app().pending_send_len(session_id)?.is_some() {
            state.sessions.mark_ready(session_id);
        }
        let mut immediate_ack = false;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim) as u32;
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset.saturating_add(trim) as isize)?;
                buffer.truncate(accepted_len as usize)?;
            }
            let delivery = state.sessions.enqueue_rx(
                session_id,
                index,
                offset,
                packet.flags.contains(crate::TcpSegmentFlags::URG),
            )?;
            let rx_available = match delivery {
                RxDelivery::NotAccepted { rx_available }
                | RxDelivery::InOrder { rx_available, .. }
                | RxDelivery::OutOfOrder { rx_available, .. } => rx_available as usize,
            };
            let clean_in_order = trim == 0
                && offset == 0
                && matches!(
                    delivery,
                    RxDelivery::InOrder {
                        accepted,
                        promoted,
                        ..
                    } if promoted == 0 && accepted.get() == accepted_len
                );
            immediate_ack = {
                let crate::worker::TcpWorker {
                    connections,
                    timers,
                    ..
                } = &mut state.tcp;
                let connection = connections.get_mut(connection_index).ok_or_else(|| {
                    let _ = runtime
                        .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                    TcpNodeError::EstablishedSessionMissing
                })?;
                connection.receive_payload(accepted_sequence, trim as u32, delivery);
                if clean_in_order {
                    connection.on_clean_in_order_payload(connection_index, timers)?
                } else {
                    true
                }
            };
            if matches!(delivery, RxDelivery::InOrder { .. }) {
                state.sessions.mark_ready(session_id);
            }
            match delivery {
                RxDelivery::NotAccepted { .. } => {}
                RxDelivery::InOrder {
                    accepted, promoted, ..
                } => {
                    if accepted.get() != accepted_len || promoted != 0 {
                        immediate_ack = true;
                    }
                }
                RxDelivery::OutOfOrder { accepted, .. } => {
                    if accepted.get() != accepted_len {
                        immediate_ack = true;
                    }
                }
            }
            let connection = state.tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.set_rcv_wnd(rx_available);
        } else if duplicate_payload {
            let connection = state.tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let sequence = packet.sequence;
            let end_sequence = sequence.advance(packet.payload_len as u32);
            connection.observe_duplicate_payload(sequence, end_sequence);
            immediate_ack = true;
        }

        let tx_segment = if immediate_ack {
            let connection = state.tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            Some(connection.control_segment(
                packet.local,
                packet.remote,
                crate::TcpSegmentFlags::ACK,
                None,
                crate::TcpCapabilities::default(),
            ))
        } else {
            None
        }
        .or(control);
        publish_tcp_connection(state, session_id)?;
        Ok(tx_segment)
    })?;
    if let Some(segment) = tx_segment {
        let allocated = runtime.buffers().alloc_index()?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        emit_local(
            runtime,
            out_frame,
            nexts,
            out_len,
            TcpEstablishedNext::Output,
            allocated,
        )?;
    }
    Ok(())
}
