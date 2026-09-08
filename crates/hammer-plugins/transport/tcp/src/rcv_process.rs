use crate::{publish_tcp_connection, read_session_id};
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::session::runtime::RxDelivery;

use super::TcpNodeError;
use super::segment::tcp_packet;

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::rcv_process::register_tcp_rcv_process,
    name = "tcp-rcv-process",
    next = TcpRcvProcessNext,
    role = internal,
)]
pub struct TcpRcvProcessNode {}

pub fn register_tcp_rcv_process(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp-rcv-process") {
        return Ok(node);
    }
    runtime.nodes().try_register_internal_with_next_names(
        TcpRcvProcessNode::new(),
        &TcpRcvProcessNext::NEXT_NAMES,
    )
}

impl Node for TcpRcvProcessNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_rcv_process_process;
        process(runtime, node_runtime, frame)
    }
}

pub(crate) fn tcp_rcv_process_process(
    runtime: &mut DataPlaneMain,
    _: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_rcv_process_frame(runtime, frame);
    processed_vectors
}

fn tcp_rcv_process_frame(runtime: &mut DataPlaneMain, frame: &mut Frame) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for &index in frame.vector_args() {
        if tcp_rcv_process_index(runtime, index, &mut output, &mut nexts, &mut out_len).is_err() {
            let _ = emit_local(
                runtime,
                &mut output,
                &mut nexts,
                &mut out_len,
                TcpRcvProcessNext::Drop,
                index,
            );
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(&mut output, &nexts[..out_len]);
    }
    ()
}

#[inline]
fn emit_local(
    runtime: &mut DataPlaneMain,
    frame: &mut Frame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
    next: TcpRcvProcessNext,
    index: u32,
) -> RuntimeResult<()> {
    if *out_len == DEFAULT_BUFFER_FRAME_CAPACITY {
        runtime.enqueue_to_next(frame, &nexts[..*out_len]);
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

fn tcp_rcv_process_index(
    runtime: &mut DataPlaneMain,
    index: u32,
    out_frame: &mut Frame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<()> {
    let packet = tcp_packet(runtime, index)?;
    let main = crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let control = main.with_worker(runtime.thread_index(), |sessions, tcp| {
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime.record_current_node_error(TcpNodeError::RcvProcessSessionRouteMissing);
            TcpNodeError::RcvProcessSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_close_side` work below gives the prefetch
        // lead time.
        sessions.prefetch_session(session_id);
        let connection_index = sessions
            .transport_connection_index(session_id)
            .ok_or(TcpNodeError::RcvProcessSessionMissing)?;
        let (control, ack_advanced, acked_tx_len, established_with_payload) = {
            let crate::worker::TcpWorker {
                connections,
                timers,
                ..
            } = tcp;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::RcvProcessSessionMissing);
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
            let established = connection.state() == crate::TcpState::Established;
            (
                control,
                connection.snd_una() != previous_snd_una,
                connection.take_acked_tx_len(previous_snd_una),
                previous_state == crate::TcpState::SynRcvd
                    && established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            sessions.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && sessions.pending_send_len(session_id)?.is_some() {
            sessions.mark_ready(session_id);
        }
        if established_with_payload {
            {
                let buffer = runtime.buffer_mut(index);
                buffer.advance(packet.payload_offset as isize);
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = sessions.enqueue_rx(runtime, session_id, index, 0)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                sessions.mark_ready(session_id);
            }
        }
        publish_tcp_connection(sessions, tcp, session_id)?;
        Ok(control)
    })?;
    if let Some(segment) = control {
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
            out_frame,
            nexts,
            out_len,
            TcpRcvProcessNext::Output,
            allocated,
        )?;
    }
    Ok(())
}
