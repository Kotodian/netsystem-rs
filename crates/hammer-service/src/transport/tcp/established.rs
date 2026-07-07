use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use super::segment::tcp_packet;
use super::tcp_worker_state_mut;
use super::{
    TCP_MAIN, TCP_TIMER_DELAYED_ACK, TCP_TIMER_KEEP_ALIVE, TCP_TIMER_PACING, TCP_TIMER_PERSIST,
    TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TLP, TcpNodeError, TcpQueue,
    TcpWorkerOwnedState, ensure_tcp_session_queue, read_session_id,
};
use crate::session::protocol::SessionQueueControlContext;
use crate::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::transport::tcp::established::register_tcp_established,
    name = "tcp-established",
    next = TcpEstablishedNext,
    role = internal,
)]
pub struct TcpEstablishedNode<C: CongestionController + 'static> {
    session_queue: TcpQueue<C>,
}

pub fn register_tcp_established(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId> {
    crate::with_congestion!(|C| {
        let queue_data = ensure_tcp_session_queue::<C>(runtime, worker)?;
        let queue = TcpQueue::<C>::new(queue_data);
        TCP_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| CoreError::internal("tcp main not initialized"))?;
        runtime.nodes().try_register_internal_with_next_names(
            TcpEstablishedNode::<C>::new(queue, [NodeId::new(0); TcpEstablishedNext::COUNT]),
            &TcpEstablishedNext::NEXT_NAMES,
        )
    })
}

impl<C> Node for TcpEstablishedNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let next = match Self::runtime_nexts(runtime) {
            Ok(next) => next,
            Err(_) => return NodeResult::drop(),
        };
        tcp_established_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_established_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
{
    let next = match TcpEstablishedNode::<C>::runtime_nexts(runtime) {
        Ok(next) => next,
        Err(_) => return NodeResult::drop(),
    };
    tcp_established_frame::<C>(runtime, frame, TcpQueue::<C>::new(data), next)
}

fn tcp_established_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueue<C>,
    next: [NodeId; TcpEstablishedNext::COUNT],
) -> NodeResult
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpEstablishedNext::Output as usize];
    let drop_next = next[TcpEstablishedNext::Drop as usize];
    let width = runtime.preferred_frame_batch_width();
    let _ = frame.rewrite_indices_batched(width, |index| {
        if tcp_established_index(runtime, index, session_queue, tcp_output).is_err()
            && let Ok(mut drop_frame) = runtime.buffers().get_next_frame(drop_next)
            && drop_frame.push_index(index).is_ok()
        {
            let _ = runtime.put_next_frame(drop_frame);
        }
        Ok(None)
    });
    NodeResult::drop()
}

fn tcp_established_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueue<C>,
    tcp_output: NodeId,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = tcp_packet(runtime, index)?;
    let mut tx_segment = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime
                .record_current_node_error(TcpNodeError::EstablishedSessionRouteMissing.code());
            TcpNodeError::EstablishedSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_established`/`accept_payload` work below gives
        // the prefetch lead time.
        queue.prefetch_session(session_id);
        let (
            control,
            acked_tx_len,
            ack_advanced,
            accept_payload,
            accepted_sequence,
            duplicate_payload,
        ) = {
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let previous_snd_una = connection.snd_una();
            let (control, _) = connection.receive_established(&packet)?;
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
            queue.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && queue.app().pending_send_len(session_id)?.is_some() {
            queue.mark_ready(session_id);
        }
        let mut immediate_ack = false;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim);
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset.saturating_add(trim) as isize)?;
                buffer.truncate(accepted_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, offset, false)?;
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.receive_payload(
                accepted_sequence,
                trim as u32,
                enqueue.delivered_len,
                enqueue.newest_ooo_start,
                enqueue.newest_ooo_len,
            );
            let clean_in_order = trim == 0
                && offset == 0
                && enqueue.delivered_len == accepted_len as u32
                && enqueue.newest_ooo_start.is_none();
            immediate_ack = if clean_in_order {
                connection.on_clean_in_order_payload()
            } else {
                true
            };
            if enqueue.delivered_len != 0 {
                queue.mark_ready(session_id);
            }
            if enqueue.accepted_len != accepted_len as u32 {
                immediate_ack = true;
            }
            if let Some(available) = queue.rx_available_len(session_id) {
                let connection = queue.session_mut(session_id).ok_or_else(|| {
                    let _ = runtime
                        .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                    TcpNodeError::EstablishedSessionMissing
                })?;
                connection.set_rcv_wnd(available);
            }
        } else if duplicate_payload {
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let sequence = packet.sequence;
            let end_sequence = sequence.advance(packet.payload_len as u32);
            connection.observe_duplicate_payload(sequence, end_sequence);
            immediate_ack = true;
        }

        if immediate_ack {
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            tx_segment = Some(connection.control_segment(
                packet.local,
                packet.remote,
                hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
                None,
                hammer_core::protocol::tcp::TcpCapabilities::default(),
            ));
        }
        let has_pending_tx = queue.app().has_pending_send(session_id);
        let connection: *const crate::transport::tcp::TcpConnection<C> =
            queue.session(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })? as *const _;
        let mut context = SessionQueueControlContext::new(
            queue.timers_mut() as *mut _,
            queue.ready_mut_ptr(),
            queue.buffers() as *const _,
            session_id,
            has_pending_tx,
        );
        let now = std::time::Instant::now();
        let connection = unsafe { &*connection };
        // Established-state timer allowlist excludes TIME_WAIT (only armed in
        // TimeWait). `timer_ticks` self-gates on `timer_is_active`, so the
        // bare allowlist is the correct keep-mask: an allowlisted-but-inactive
        // timer yields `None` and is cancelled, matching the prior per-site
        // gate.
        const ESTABLISHED_TIMER_KEEP_MASK: u16 = (1u16 << TCP_TIMER_RETRANSMIT)
            | (1u16 << TCP_TIMER_RACK)
            | (1u16 << TCP_TIMER_TLP)
            | (1u16 << TCP_TIMER_DELAYED_ACK)
            | (1u16 << TCP_TIMER_PERSIST)
            | (1u16 << TCP_TIMER_KEEP_ALIVE)
            | (1u16 << TCP_TIMER_PACING);
        context.refresh_tcp_timers(connection, ESTABLISHED_TIMER_KEEP_MASK, now)?;
        let connection = queue.session(session_id).ok_or_else(|| {
            let _ =
                runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
            TcpNodeError::EstablishedSessionMissing
        })? as *const _;
        let protocol = tcp_worker_state_mut() as *mut TcpWorkerOwnedState;
        if unsafe { (*protocol).publish_connection(session_id, &*connection) } {
            let _ = queue.close_session(session_id)?;
        }
        if tx_segment.is_none() {
            tx_segment = control;
        }
        Ok(())
    };
    if let Err(error) = result {
        return Err(error);
    }
    if let Some(segment) = tx_segment.take() {
        let mut owner = runtime.buffers().get_next_frame(tcp_output)?;
        let allocated = runtime.buffers().alloc_index()?;
        owner.push_index(allocated)?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        runtime.put_next_frame(owner)?;
    }
    Ok(())
}
