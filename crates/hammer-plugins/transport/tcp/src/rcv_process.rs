use crate::{TcpWorkerStore, publish_tcp_connection, read_session_id, with_tcp_worker_mut};
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::session::runtime::RxDelivery;
use hammer_service::transport::congestion::CongestionController;

use super::TcpNodeError;
use super::segment::tcp_packet;
use hammer_service::session::app::SessionAppRuntimeCreate;

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
pub struct TcpRcvProcessNode {
    process: NodeProcessFn,
}

pub fn register_tcp_rcv_process(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    let main = crate::TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp-rcv-process") {
        return Ok(node);
    }
    runtime
        .nodes()
        .try_register_internal_with_next_names(
            TcpRcvProcessNode::new(main.rcv_process),
            &TcpRcvProcessNext::NEXT_NAMES,
        )
}

impl Node for TcpRcvProcessNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        (self.process)(runtime, NodeRuntimeData::empty(), frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }
}

pub(crate) fn tcp_rcv_process_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    tcp_rcv_process_frame::<C, Seg>(runtime, frame)
}

fn tcp_rcv_process_frame<C, Seg>(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
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
    for offset in 0..input_len {
        let index = unsafe { inputs[offset].assume_init() };
        if tcp_rcv_process_index(runtime, index, frame, &mut nexts, &mut out_len).is_err() {
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

fn tcp_rcv_process_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<()>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let control = with_tcp_worker_mut::<C, Seg, _>(|state| {
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime
                .record_current_node_error(TcpNodeError::RcvProcessSessionRouteMissing.code());
            TcpNodeError::RcvProcessSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_close_side` work below gives the prefetch
        // lead time.
        state.sessions.prefetch_session(session_id);
        let (_, connection_index) = state
            .sessions
            .session_transport(session_id)
            .ok_or(TcpNodeError::RcvProcessSessionMissing)?;
        let (control, ack_advanced, acked_tx_len, established, established_with_payload) = {
            let crate::worker::TcpWorker {
                connections,
                timers,
                ..
            } = &mut state.tcp;
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
            let established = connection.state() == crate::TcpState::Established;
            (
                control,
                connection.snd_una() != previous_snd_una,
                connection.take_acked_tx_len(previous_snd_una),
                established,
                previous_state == crate::TcpState::SynRcvd
                    && established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            state
                .sessions
                .ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && state.sessions.app().pending_send_len(session_id)?.is_some() {
            state.sessions.mark_ready(session_id);
        }
        if established_with_payload {
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = state.sessions.enqueue_rx(session_id, index, 0, false)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                state.sessions.mark_ready(session_id);
            }
        }
        publish_tcp_connection(state, session_id)?;
        if established {
            state.sessions.app().connected(session_id)?;
        }
        Ok(control)
    })?;
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
