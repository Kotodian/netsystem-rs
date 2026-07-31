use crate::{publish_tcp_connection, read_session_id};
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

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
pub struct TcpSynSentNode {
    process: NodeProcessFn,
}

pub fn register_tcp_syn_sent(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    let main = crate::TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp-syn-sent") {
        return Ok(node);
    }
    runtime.nodes().try_register_internal_with_next_names(
        TcpSynSentNode::new(main.syn_sent_process),
        &TcpSynSentNext::NEXT_NAMES,
    )
}

impl Node for TcpSynSentNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        (self.process)(runtime, NodeRuntimeData::empty(), frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }
}

pub(crate) fn tcp_syn_sent_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    tcp_syn_sent_frame(runtime, frame)
}

fn tcp_syn_sent_frame(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
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
        match tcp_syn_sent_index(runtime, index, frame, &mut nexts, &mut out_len) {
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
) -> RuntimeResult<()> {
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

fn tcp_syn_sent_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<bool> {
    let packet = tcp_packet(runtime, index)?;
    let main = crate::TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let (keep_current, control_segment) = main.with_worker(runtime, |sessions, tcp| {
        let mut keep_current = true;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ =
                runtime.record_current_node_error(TcpNodeError::SynSentSessionRouteMissing.code());
            TcpNodeError::SynSentSessionRouteMissing
        })?;
        let (_, connection_index) = sessions
            .session_transport(session_id)
            .ok_or(TcpNodeError::SynSentSessionMissing)?;
        let (control, acked_tx_len, established_with_payload) = {
            let crate::worker::TcpWorker {
                connections,
                lookup,
                timers,
            } = tcp;
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
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = sessions.enqueue_rx(runtime.buffers(), session_id, index, 0, false)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                sessions.mark_ready(session_id);
            }
            keep_current = false;
        };
        publish_tcp_connection(sessions, tcp, session_id)?;
        Ok((keep_current, control))
    })?;
    if let Some(segment) = control_segment {
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
