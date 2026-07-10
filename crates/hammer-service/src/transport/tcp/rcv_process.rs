use hammer_core::data_plane::{BufferFrame, BufferIndex, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::segment::Segment;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use crate::session::runtime::RxDelivery;
use crate::transport::congestion::CongestionController;

use super::segment::tcp_packet;
use super::{
    TCP_TIMER_DELAYED_ACK, TCP_TIMER_KEEP_ALIVE, TCP_TIMER_PACING, TCP_TIMER_PERSIST,
    TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TIME_WAIT, TCP_TIMER_TLP, TcpNodeError,
    TcpWorker, publish_tcp_connection, read_session_id,
};
use crate::session::SessionQueueHandle;
use crate::session::app::SessionAppRuntimeCreate;
use crate::session::runtime::SessionDriverRuntime;

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
pub struct TcpRcvProcessNode<C: CongestionController + 'static, Seg: Segment> {
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
}

pub fn register_tcp_rcv_process(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime
        .nodes()
        .node_by_name("tcp-rcv-process")
        .ok_or_else(|| CoreError::internal("TCP worker graph is not registered"))
}

impl<C, Seg> Node for TcpRcvProcessNode<C, Seg>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
        tcp_rcv_process_process::<C, Seg>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_rcv_process_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let next = match TcpRcvProcessNode::<C, Seg>::runtime_nexts(runtime) {
        Ok(next) => next,
        Err(_) => return NodeResult::drop(),
    };
    tcp_rcv_process_frame::<C, Seg>(
        runtime,
        frame,
        SessionQueueHandle::<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>::new(data),
        next,
    )
}

fn tcp_rcv_process_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    next: [NodeId; TcpRcvProcessNext::COUNT],
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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

fn tcp_rcv_process_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    tcp_output: NodeId,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
            queue.mark_session_ready(session_id);
        }
        if established_with_payload {
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                queue.mark_session_ready(session_id);
            }
        }
        let now = std::time::Instant::now();
        let timer_ticks = {
            let connection = queue.session(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::RcvProcessSessionMissing.code());
                TcpNodeError::RcvProcessSessionMissing
            })?;
            std::array::from_fn(|timer_id| {
                let timer_id = timer_id as u32;
                connection
                    .timer_is_active(timer_id)
                    .then(|| connection.timer_ticks(timer_id, now))
                    .flatten()
            })
        };
        crate::transport::tcp::sync_all_tcp_timers(
            queue.timers_mut(),
            timer_ticks,
            session_id.pool_index(),
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
