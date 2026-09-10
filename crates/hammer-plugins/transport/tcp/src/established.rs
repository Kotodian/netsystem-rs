use crate::{publish_tcp_connection, read_session_id};
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::runtime::{RxDelivery, session_main};

use super::TcpNodeError;
use super::segment::tcp_packet;

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
pub struct TcpEstablishedNode {}

pub fn register_tcp_established(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp-established") {
        return Ok(node);
    }
    runtime.nodes().try_register_internal_with_next_names(
        TcpEstablishedNode::new(),
        &TcpEstablishedNext::NEXT_NAMES,
    )
}

impl Node for TcpEstablishedNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_established_process;
        process(runtime, node_runtime, frame)
    }
}

pub(crate) fn tcp_established_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_established_frame(runtime, node_runtime, frame);
    processed_vectors
}

fn tcp_established_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for &index in frame.vector_args() {
        if tcp_established_index(
            runtime,
            node_runtime,
            index,
            &mut output,
            &mut nexts,
            &mut out_len,
        )
        .is_err()
        {
            let _ = emit_local(
                runtime,
                node_runtime,
                &mut output,
                &mut nexts,
                &mut out_len,
                TcpEstablishedNext::Drop,
                index,
            );
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(node_runtime, &mut output, &nexts[..out_len]);
    }
    ()
}

#[inline]
fn emit_local(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
    next: TcpEstablishedNext,
    index: u32,
) -> RuntimeResult<()> {
    if *out_len == DEFAULT_BUFFER_FRAME_CAPACITY {
        runtime.enqueue_to_next(node_runtime, frame, &nexts[..*out_len]);
        frame.set_vector_count(0);
        *out_len = 0;
    }
    nexts[*out_len] = NodeNext::slot(next);
    {
        let count = frame.len();
        frame.set_vector_count(count + 1);
        frame.vector_args_mut()[count] = index;
    }
    *out_len += 1;
    debug_assert_eq!(*out_len, frame.len());
    Ok(())
}

fn tcp_established_index(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    index: u32,
    out_frame: &mut Frame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<()> {
    let packet = tcp_packet(runtime, index)?;
    let main = crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    // SAFETY: this Node executes on the DataPlaneMain's owning runtime thread.
    let mut sessions = unsafe { session_main().worker(runtime.thread_index()) }?;
    let mut tcp = main.worker(runtime.thread_index())?;
    let tx_segment = {
        let sessions = &mut *sessions;
        let tcp = &mut *tcp;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionRouteMissing);
            TcpNodeError::EstablishedSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_established`/`accept_payload` work below gives
        // the prefetch lead time.
        sessions.prefetch_session(session_id);
        let connection_index = sessions
            .transport_connection_index(session_id)
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
            } = tcp;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing);
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
            sessions.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && sessions.pending_send_len(session_id)?.is_some() {
            sessions.mark_ready(session_id);
        }
        let mut immediate_ack = false;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim) as u32;
            {
                let buffer = runtime.buffer_mut(index);
                buffer.advance(packet.payload_offset.saturating_add(trim) as isize);
                buffer.truncate(accepted_len as usize)?;
            }
            let delivery = sessions.enqueue_rx(runtime, session_id, index, offset)?;
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
                } = tcp;
                let connection = connections.get_mut(connection_index).ok_or_else(|| {
                    let _ =
                        runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing);
                    TcpNodeError::EstablishedSessionMissing
                })?;
                connection.receive_payload(accepted_sequence, trim as u32, delivery)?;
                if clean_in_order {
                    connection.on_clean_in_order_payload(connection_index, timers)?
                } else {
                    true
                }
            };
            if matches!(delivery, RxDelivery::InOrder { .. }) {
                sessions.mark_ready(session_id);
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
            let connection = tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.set_rcv_wnd(rx_available);
        } else if duplicate_payload {
            let connection = tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            let sequence = packet.sequence;
            let end_sequence = sequence.advance(packet.payload_len as u32);
            connection.observe_duplicate_payload(sequence, end_sequence);
            immediate_ack = true;
        }

        let fin_control = {
            let connection = tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.process_fin_after_payload(&packet)?
        };
        if fin_control.is_some() {
            sessions.notify_transport_closing(Some(runtime), session_id, connection_index)?;
        }

        let tx_segment = if immediate_ack {
            let connection = tcp.connection_mut(connection_index).ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::EstablishedSessionMissing);
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
        .or(fin_control)
        .or(control);
        publish_tcp_connection(sessions, tcp, session_id)?;
        tx_segment
    };
    if let Some(segment) = tx_segment {
        let mut allocated = 0;
        if runtime.buffer_alloc(core::slice::from_mut(&mut allocated)) != 1 {
            return Err(hammer_core::error::DataPlaneError::from(
                hammer_core::error::BufferInvariant::PoolExhausted,
            )
            .into());
        }
        segment.write_to_buffer(&mut *runtime.buffer_mut(allocated))?;
        emit_local(
            runtime,
            node_runtime,
            out_frame,
            nexts,
            out_len,
            TcpEstablishedNext::Output,
            allocated,
        )?;
    }
    Ok(())
}
