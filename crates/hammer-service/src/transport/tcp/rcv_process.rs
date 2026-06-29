use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

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
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
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
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpRcvProcessNode::<C>::runtime_nexts(runtime)?;
    tcp_rcv_process_frame::<C>(runtime, frame, TcpQueue::<C>::new(data), next)
}

fn tcp_rcv_process_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueue<C>,
    next: [NodeId; TcpRcvProcessNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpRcvProcessNext::Output as usize];
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    let indices = frame.pending_indices();
    let len = indices.len();
    let mut read = 0usize;
    while read + 4 <= len {
        runtime.prefetch_header(indices[read]);
        runtime.prefetch_header(indices[read + 1]);
        runtime.prefetch_header(indices[read + 2]);
        runtime.prefetch_header(indices[read + 3]);
        if tcp_rcv_process_index(
            runtime,
            indices[read],
            session_queue,
            tcp_output,
            &mut next_frames,
        )
        .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(
                runtime,
                next_frames,
                drop_next,
                indices[read]
            )?;
        }
        if tcp_rcv_process_index(
            runtime,
            indices[read + 1],
            session_queue,
            tcp_output,
            &mut next_frames,
        )
        .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(
                runtime,
                next_frames,
                drop_next,
                indices[read + 1]
            )?;
        }
        if tcp_rcv_process_index(
            runtime,
            indices[read + 2],
            session_queue,
            tcp_output,
            &mut next_frames,
        )
        .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(
                runtime,
                next_frames,
                drop_next,
                indices[read + 2]
            )?;
        }
        if tcp_rcv_process_index(
            runtime,
            indices[read + 3],
            session_queue,
            tcp_output,
            &mut next_frames,
        )
        .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(
                runtime,
                next_frames,
                drop_next,
                indices[read + 3]
            )?;
        }
        read += 4;
    }
    if read + 2 <= len {
        runtime.prefetch_header(indices[read]);
        runtime.prefetch_header(indices[read + 1]);
        if tcp_rcv_process_index(
            runtime,
            indices[read],
            session_queue,
            tcp_output,
            &mut next_frames,
        )
        .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(
                runtime,
                next_frames,
                drop_next,
                indices[read]
            )?;
        }
        if tcp_rcv_process_index(
            runtime,
            indices[read + 1],
            session_queue,
            tcp_output,
            &mut next_frames,
        )
        .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(
                runtime,
                next_frames,
                drop_next,
                indices[read + 1]
            )?;
        }
        read += 2;
    }
    while read < len {
        let index = indices[read];
        runtime.prefetch_header(index);
        if tcp_rcv_process_index(runtime, index, session_queue, tcp_output, &mut next_frames)
            .is_err()
        {
            hammer_adapter::validate_buffer_enqueue_x1!(runtime, next_frames, drop_next, index)?;
        }
        read += 1;
    }
    frame.clear();
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

fn tcp_rcv_process_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueue<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = tcp_packet(runtime, index)?;
    let mut release_input = true;
    let result: CoreResult<_> = {
        let mut queue = session_queue.borrow_mut()?;
        let session_id =
            read_session_id(runtime, index)?.ok_or(TcpNodeError::RcvProcessSessionRouteMissing)?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_close_side` work below gives the prefetch
        // lead time.
        queue.prefetch_session(session_id);
        let (control, ack_advanced, acked_tx_len, established, established_with_payload) = {
            let connection = queue
                .session_mut(session_id)
                .ok_or(TcpNodeError::RcvProcessSessionMissing)?;
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
            queue.release_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && queue.app().pending_send_len(session_id)?.is_some() {
            queue.mark_ready(session_id);
        }
        if established_with_payload {
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if enqueue.delivered_len != 0 {
                queue.mark_ready(session_id);
            }
            release_input = false;
        }
        let timer_mask = (1u16 << TCP_TIMER_RETRANSMIT)
            | (1u16 << TCP_TIMER_RACK)
            | (1u16 << TCP_TIMER_TLP)
            | (1u16 << TCP_TIMER_DELAYED_ACK)
            | (1u16 << TCP_TIMER_PERSIST)
            | (1u16 << TCP_TIMER_KEEP_ALIVE)
            | (1u16 << TCP_TIMER_PACING)
            | (1u16 << TCP_TIMER_TIME_WAIT);
        let now = std::time::Instant::now();
        let connection: *const crate::transport::tcp::TcpConnection<C> =
            queue
                .session(session_id)
                .ok_or(TcpNodeError::RcvProcessSessionMissing)? as *const _;
        let connection = unsafe { &*connection };
        // Prior per-site predicate was `(timer_mask & bit) != 0 ||
        // timer_is_active(id)`, i.e. keep_mask = timer_mask | active.
        // `timer_ticks` self-gates on active, so an allowlisted-but-inactive
        // timer yields `None` and is cancelled.
        let keep_mask = timer_mask | connection.active_timer_mask();
        crate::session::protocol::refresh_tcp_timers(
            queue.timers_mut(),
            connection,
            session_id.pool_index(),
            keep_mask,
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
        let allocated = runtime.buffers().alloc_index()?;
        if let Err(error) = segment.write_to_buffer(runtime.buffers(), allocated) {
            runtime.free_index(allocated);
            return Err(error);
        }
        if let Err(error) = next_frames.enqueue(runtime, tcp_output, allocated) {
            runtime.free_index(allocated);
            return Err(error);
        }
    }
    if release_input {
        runtime.free_index(index);
    }
    Ok(())
}
