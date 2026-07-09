use hammer_core::data_plane::{BufferFrame, BufferIndex, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use crate::session::runtime::RxDelivery;
use crate::transport::congestion::CongestionController;

use super::segment::tcp_packet;
use super::{
    TCP_MAIN, TCP_TIMER_DELAYED_ACK, TCP_TIMER_KEEP_ALIVE, TCP_TIMER_PACING, TCP_TIMER_PERSIST,
    TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TIME_WAIT, TCP_TIMER_TLP, TcpNodeError,
    TcpQueue, ensure_tcp_session_queue, publish_tcp_connection, read_session_id,
};

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::transport::tcp::rcv_process::register_tcp_rcv_process,
    name = "tcp-rcv-process",
    next = TcpRcvProcessNext,
    role = internal,
)]
pub struct TcpRcvProcessNode<C: CongestionController + 'static> {
    session_queue: TcpQueue<C>,
}

pub fn register_tcp_rcv_process(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId> {
    crate::with_congestion!(|C| {
        let queue_data = ensure_tcp_session_queue::<C>(runtime, worker)?;
        let queue = TcpQueue::<C>::new(queue_data);
        TCP_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| CoreError::internal("tcp main not initialized"))?;
        runtime.nodes().try_register_internal_with_next_names(
            TcpRcvProcessNode::<C>::new(queue, [NodeId::new(0); TcpRcvProcessNext::COUNT]),
            &TcpRcvProcessNext::NEXT_NAMES,
        )
    })
}

impl<C> Node for TcpRcvProcessNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let next = match Self::runtime_nexts(runtime) {
            Ok(next) => next,
            Err(_) => return NodeResult::drop(),
        };
        tcp_rcv_process_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_rcv_process_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_rcv_process_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
{
    let next = match TcpRcvProcessNode::<C>::runtime_nexts(runtime) {
        Ok(next) => next,
        Err(_) => return NodeResult::drop(),
    };
    tcp_rcv_process_frame::<C>(runtime, frame, TcpQueue::<C>::new(data), next)
}

fn tcp_rcv_process_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueue<C>,
    next: [NodeId; TcpRcvProcessNext::COUNT],
) -> NodeResult
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpRcvProcessNext::Output as usize];
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let width = runtime.preferred_frame_batch_width();
    let _ = frame.rewrite_indices_batched(width, |index| {
        if tcp_rcv_process_index(runtime, index, session_queue, tcp_output).is_err()
            && let Ok(mut drop_frame) = runtime.buffers().get_next_frame(drop_next)
            && drop_frame.push_index(index).is_ok()
        {
            let _ = runtime.put_next_frame(drop_frame);
        }
        Ok(None)
    });
    NodeResult::drop()
}

fn tcp_rcv_process_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueue<C>,
    tcp_output: NodeId,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = tcp_packet(runtime, index)?;
    let result: CoreResult<_> = {
        let mut queue = session_queue.borrow_mut()?;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime
                .record_current_node_error(TcpNodeError::RcvProcessSessionRouteMissing.code());
            TcpNodeError::RcvProcessSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_close_side` work below gives the prefetch
        // lead time.
        queue.prefetch_session(session_id);
        let (control, ack_advanced, acked_tx_len, established, established_with_payload) = {
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::RcvProcessSessionMissing.code());
                TcpNodeError::RcvProcessSessionMissing
            })?;
            let previous_state = connection.state();
            let previous_snd_una = connection.snd_una();
            let control = connection.receive_close_side(&packet)?;
            let established = connection.state() == crate::transport::tcp::TcpState::Established;
            (
                control,
                connection.snd_una() != previous_snd_una,
                connection.take_acked_tx_len(previous_snd_una),
                established,
                previous_state == crate::transport::tcp::TcpState::SynRcvd
                    && established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            queue.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && queue.app().pending_send_len(session_id)?.is_some() {
            let mut context = queue.session_control_context(session_id);
            context.mark_ready();
        }
        if established_with_payload {
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                let mut context = queue.session_control_context(session_id);
                context.mark_ready();
            }
        }
        let now = std::time::Instant::now();
        let connection: *const crate::transport::tcp::TcpConnection<C> =
            queue.session(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::RcvProcessSessionMissing.code());
                TcpNodeError::RcvProcessSessionMissing
            })? as *const _;
        let connection = unsafe { &*connection };
        crate::transport::tcp::sync_all_tcp_timers(
            queue.timers_mut(),
            connection,
            session_id.pool_index(),
            now,
        )?;
        publish_tcp_connection(&mut queue, session_id)?;
        if established {
            queue.app().connected(session_id)?;
        }
        Ok(control)
    };
    let control = result?;
    if let Some(segment) = control {
        let mut owner = runtime.buffers().get_next_frame(tcp_output)?;
        let allocated = runtime.buffers().alloc_index()?;
        owner.push_index(allocated)?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        runtime.put_next_frame(owner)?;
    }
    Ok(())
}
