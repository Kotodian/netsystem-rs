use hammer_core::data_plane::{
    BufferFrame, Index, NodeId, NodeNext, DEFAULT_BUFFER_FRAME_CAPACITY,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::segment::Segment;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use crate::session::runtime::RxDelivery;
use crate::transport::congestion::CongestionController;

use super::segment::tcp_packet;
use super::{TcpNodeError, TcpWorker, publish_tcp_connection, read_session_id};
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
        tcp_rcv_process_frame(runtime, frame, self.session_queue)
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
    tcp_rcv_process_frame::<C, Seg>(
        runtime,
        frame,
        SessionQueueHandle::<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>::new(data),
    )
}

fn tcp_rcv_process_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let input_len = frame.len();
    debug_assert!(input_len <= DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut inputs =
        [core::mem::MaybeUninit::<Index>::uninit(); DEFAULT_BUFFER_FRAME_CAPACITY];
    for (offset, &index) in frame.indices().iter().enumerate() {
        inputs[offset].write(index);
    }
    frame.discard_prefix(input_len);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for offset in 0..input_len {
        let index = unsafe { inputs[offset].assume_init() };
        if tcp_rcv_process_index(
            runtime,
            index,
            session_queue,
            frame,
            &mut nexts,
            &mut out_len,
        )
        .is_err()
        {
            let _ = emit_local(
                runtime,
                frame,
                &mut nexts,
                &mut out_len,
                TcpRcvProcessNext::Drop,
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
    next: TcpRcvProcessNext,
    index: Index,
) -> CoreResult<()> {
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

fn tcp_rcv_process_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
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
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::RcvProcessSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let crate::transport::tcp::worker::TcpWorker {
                connections,
                timers,
                ..
            } = worker;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::RcvProcessSessionMissing.code());
                TcpNodeError::RcvProcessSessionMissing
            })?;
            let previous_state = connection.state();
            let previous_snd_una = connection.snd_una();
            let control = connection.receive_close_side(
                connection_index,
                timers,
                &packet,
                std::time::Instant::now(),
            )?;
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
        publish_tcp_connection(&mut queue, session_id)?;
        if established {
            queue.app().connected(session_id)?;
        }
        Ok(control)
    };
    let control = result?;
    if let Some(segment) = control {
        let allocated = runtime.buffers().alloc_index()?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        emit_local(
            runtime,
            out_frame,
            nexts,
            out_len,
            TcpRcvProcessNext::Output,
            allocated,
        )?;
    }
    Ok(())
}
