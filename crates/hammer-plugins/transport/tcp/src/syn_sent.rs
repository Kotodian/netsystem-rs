use crate::{publish_tcp_connection, read_session_id};
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime};

use hammer_runtime::{RuntimeError, RuntimeResult};

use super::TcpNodeError;
use super::segment::tcp_packet;
use hammer_service::session::runtime::RxDelivery;

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
pub struct TcpSynSentNode {}

pub fn register_tcp_syn_sent(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp-syn-sent") {
        return Ok(node);
    }
    runtime
        .nodes()
        .try_register_internal_with_next_names(TcpSynSentNode::new(), &TcpSynSentNext::NEXT_NAMES)
}

impl Node for TcpSynSentNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_syn_sent_process;
        process(runtime, node_runtime, frame)
    }
}

pub(crate) fn tcp_syn_sent_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_syn_sent_frame(runtime, node_runtime, frame);
    processed_vectors
}

fn tcp_syn_sent_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for &index in frame.vector_args() {
        match tcp_syn_sent_index(
            runtime,
            node_runtime,
            index,
            &mut output,
            &mut nexts,
            &mut out_len,
        ) {
            Ok(true) => {
                emit_local(
                    runtime,
                    node_runtime,
                    &mut output,
                    &mut nexts,
                    &mut out_len,
                    TcpSynSentNext::Drop,
                    index,
                )
                .expect("output Frame has fixed capacity");
            }
            Ok(false) => {}
            Err(_) => {
                let _ = emit_local(
                    runtime,
                    node_runtime,
                    &mut output,
                    &mut nexts,
                    &mut out_len,
                    TcpSynSentNext::Drop,
                    index,
                );
            }
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
    next: TcpSynSentNext,
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

fn tcp_syn_sent_index(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    index: u32,
    out_frame: &mut Frame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<bool> {
    let packet = tcp_packet(runtime, index)?;
    let main = crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let (keep_current, control_segment) =
        main.with_worker(runtime.thread_index(), |sessions, tcp| {
            let mut keep_current = true;
            let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
                let _ = runtime.record_current_node_error(TcpNodeError::SynSentSessionRouteMissing);
                TcpNodeError::SynSentSessionRouteMissing
            })?;
            let connection_index = sessions
                .transport_connection_index(session_id)
                .ok_or(TcpNodeError::SynSentSessionMissing)?;
            let (control, acked_tx_len, established_with_payload) = {
                let crate::worker::TcpWorker {
                    connections,
                    lookup,
                    timers,
                    ..
                } = tcp;
                let local_capabilities = lookup
                    .pending_open_capabilities(session_id)
                    .unwrap_or_default();
                let connection = connections.get_mut(connection_index).ok_or_else(|| {
                    let _ = runtime.record_current_node_error(TcpNodeError::SynSentSessionMissing);
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
                    previous_state == crate::TcpState::SynSent
                        && established
                        && packet.payload_len != 0,
                )
            };
            if acked_tx_len != 0 {
                sessions.ack_tx_up_to(session_id, acked_tx_len as usize)?;
            }
            if let Some(cookie) = packet.fast_open_cookie.filter(|cookie| !cookie.is_empty()) {
                tcp.lookup.remember_fast_open_cookie(
                    packet.local,
                    packet.remote,
                    cookie,
                    packet.capabilities.max_segment_size,
                );
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
                keep_current = false;
            };
            publish_tcp_connection(sessions, tcp, session_id)?;
            Ok((keep_current, control))
        })?;
    if let Some(segment) = control_segment {
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
            TcpSynSentNext::Output,
            allocated,
        )?;
    }
    Ok(keep_current)
}
