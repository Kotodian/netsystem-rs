use crate::mark_tcp_session_ready;
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Index as PoolIndex;
#[cfg(test)]
use hammer_infra::vec::Vec;
use hammer_runtime::app::SessionSegment;

use super::publish_tcp_connection;
use super::segment::tcp_packet;
use super::{TcpNodeError, TcpWorker, read_session_id};
use hammer_service::session::SessionQueueHandle;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::session::runtime::RxDelivery;
use hammer_service::session::runtime::SessionDriverRuntime;
use hammer_service::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::syn_sent::register_tcp_syn_sent,
    name = "tcp-syn-sent",
    next = TcpSynSentNext,
    role = internal,
)]
pub struct TcpSynSentNode<C: CongestionController + 'static, Seg: SessionSegment> {
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
}

pub fn register_tcp_syn_sent(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime
        .nodes()
        .node_by_name("tcp-syn-sent")
        .ok_or_else(|| CoreError::internal("TCP worker graph is not registered"))
}

impl<C, Seg> Node for TcpSynSentNode<C, Seg>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        tcp_syn_sent_frame(runtime, frame, self.session_queue)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_syn_sent_process::<C, Seg>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_syn_sent_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    tcp_syn_sent_frame::<C, Seg>(
        runtime,
        frame,
        SessionQueueHandle::<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>::new(data),
    )
}

fn tcp_syn_sent_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
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
    let mut keep = [core::mem::MaybeUninit::<Index>::uninit(); DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut keep_len = 0usize;

    for offset in 0..input_len {
        let index = unsafe { inputs[offset].assume_init() };
        match tcp_syn_sent_index(
            runtime,
            index,
            session_queue,
            frame,
            &mut nexts,
            &mut out_len,
        ) {
            Ok(true) => {
                keep[keep_len].write(index);
                keep_len += 1;
            }
            Ok(false) => {}
            Err(_) => {
                let _ = emit_local(
                    runtime,
                    frame,
                    &mut nexts,
                    &mut out_len,
                    TcpSynSentNext::Drop,
                    index,
                );
            }
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(frame, &nexts[..out_len]);
    }
    for offset in 0..keep_len {
        let index = unsafe { keep[offset].assume_init() };
        let _ = frame.push_index(index);
    }
    NodeResult::drop()
}

#[inline]
fn emit_local(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
    next: TcpSynSentNext,
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

fn tcp_syn_sent_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> CoreResult<bool>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let mut keep_current = true;
    let mut control_segment = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ =
                runtime.record_current_node_error(TcpNodeError::SynSentSessionRouteMissing.code());
            TcpNodeError::SynSentSessionRouteMissing
        })?;
        let (control, acked_tx_len, established, established_with_payload) = {
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::SynSentSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let TcpWorker {
                connections,
                lookup,
                timers,
            } = worker;
            let local_capabilities = lookup
                .pending_open_capabilities(session_id)
                .unwrap_or_default();
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ =
                    runtime.record_current_node_error(TcpNodeError::SynSentSessionMissing.code());
                TcpNodeError::SynSentSessionMissing
            })?;
            let previous_snd_una = connection.snd_una();
            let previous_state = connection.state();
            let control = connection.receive_open_reply(
                connection_index,
                timers,
                &packet,
                local_capabilities,
                std::time::Instant::now(),
            )?;
            let established = connection.state() == crate::TcpState::Established;
            (
                control,
                connection.take_acked_tx_len(previous_snd_una),
                established,
                previous_state == crate::TcpState::SynSent
                    && established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            queue.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if let Some(cookie) = packet.fast_open_cookie.filter(|cookie| !cookie.is_empty()) {
            queue.transports_mut().0.lookup.remember_fast_open_cookie(
                packet.local,
                packet.remote,
                cookie,
                packet.capabilities.max_segment_size,
            );
        }
        if established_with_payload {
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                mark_tcp_session_ready(&mut *queue, session_id);
            }
            keep_current = false;
        };
        publish_tcp_connection(&mut queue, session_id)?;
        if established {
            queue.app().connected(session_id)?;
        }
        control_segment = control;
        Ok(())
    };
    if let Err(error) = result {
        return Err(error);
    }
    if let Some(segment) = control_segment.take() {
        let allocated = runtime.buffers().alloc_index()?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        emit_local(
            runtime,
            out_frame,
            nexts,
            out_len,
            TcpSynSentNext::Output,
            allocated,
        )?;
    }
    Ok(keep_current)
}
