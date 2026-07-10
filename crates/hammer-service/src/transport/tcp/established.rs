use crate::session::runtime::RxDelivery;
use hammer_core::data_plane::{BufferFrame, BufferIndex, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::segment::Segment;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use super::segment::tcp_packet;
use super::{TcpNodeError, TcpWorker, publish_tcp_connection, read_session_id};
use crate::session::SessionQueueHandle;
use crate::session::app::SessionAppRuntimeCreate;
use crate::session::runtime::SessionDriverRuntime;
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
pub struct TcpEstablishedNode<C: CongestionController + 'static, Seg: Segment> {
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
}

pub fn register_tcp_established(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime
        .nodes()
        .node_by_name("tcp-established")
        .ok_or_else(|| CoreError::internal("TCP worker graph is not registered"))
}

impl<C, Seg> Node for TcpEstablishedNode<C, Seg>
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
        tcp_established_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process::<C, Seg>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_established_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let next = match TcpEstablishedNode::<C, Seg>::runtime_nexts(runtime) {
        Ok(next) => next,
        Err(_) => return NodeResult::drop(),
    };
    tcp_established_frame::<C, Seg>(
        runtime,
        frame,
        SessionQueueHandle::<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>::new(data),
        next,
    )
}

fn tcp_established_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    next: [NodeId; TcpEstablishedNext::COUNT],
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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

fn tcp_established_index<C, Seg>(
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
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::EstablishedSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let crate::transport::tcp::worker::TcpWorker {
                connections,
                timers,
                ..
            } = worker;
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
            queue.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && queue.app().pending_send_len(session_id)?.is_some() {
            queue.mark_session_ready(session_id);
        }
        let mut immediate_ack = false;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim) as u32;
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset.saturating_add(trim) as isize)?;
                buffer.truncate(accepted_len as usize)?;
            }
            let delivery = queue.enqueue_rx(session_id, index, offset, false)?;
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::EstablishedSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let crate::transport::tcp::worker::TcpWorker {
                connections,
                timers,
                ..
            } = worker;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let rx_available = match delivery {
                RxDelivery::NotAccepted { rx_available }
                | RxDelivery::InOrder { rx_available, .. }
                | RxDelivery::OutOfOrder { rx_available, .. } => rx_available as usize,
            };
            connection.receive_payload(accepted_sequence, trim as u32, delivery);
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
            immediate_ack = if clean_in_order {
                connection.on_clean_in_order_payload(connection_index, timers)?
            } else {
                true
            };
            if matches!(delivery, RxDelivery::InOrder { .. }) {
                queue.mark_session_ready(session_id);
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
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.set_rcv_wnd(rx_available);
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
        publish_tcp_connection(&mut queue, session_id)?;
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
