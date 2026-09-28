use crate::read_session_id;
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime};

use hammer_runtime::{RuntimeError, RuntimeResult};

use super::TcpError;
use super::TcpNodeError;
use super::segment::tcp_packet;
use hammer_service::session::{ApplicationMain, RxDelivery, SessionEventType, SessionHandle, SessionState};

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    #[next("tcp4-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::syn_sent::register_tcp4_syn_sent,
    name = "tcp4-syn-sent",
    next = TcpSynSentNext,
    role = internal,
)]
pub struct Tcp4SynSentNode {}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::syn_sent::register_tcp6_syn_sent,
    name = "tcp6-syn-sent",
    next = TcpSynSentNext,
    role = internal,
)]
pub struct Tcp6SynSentNode {}

pub fn register_tcp4_syn_sent(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp4-syn-sent") {
        return Ok(node);
    }
    runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp4SynSentNode::new(), &TcpSynSentNext::NEXT_NAMES)
}

pub fn register_tcp6_syn_sent(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp6SynSentNode::new(), &["tcp6-output", "drop"])
}

impl Node for Tcp4SynSentNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_syn_sent_process::<true>;
        process(runtime, node_runtime, frame)
    }
}

impl Node for Tcp6SynSentNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_syn_sent_process::<false>;
        process(runtime, node_runtime, frame)
    }
}

pub(crate) fn tcp_syn_sent_process<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_syn_sent_frame::<IS_IP4>(runtime, node_runtime, frame);
    processed_vectors
}

fn tcp_syn_sent_frame<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for &index in frame.vector_args() {
        match tcp_syn_sent_index::<IS_IP4>(
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

fn tcp_syn_sent_index<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    _: &mut hammer_runtime::NodeRuntime,
    index: u32,
    _: &mut Frame,
    _: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    _: &mut usize,
) -> RuntimeResult<bool> {
    let packet = tcp_packet(runtime, index)?;
    if packet.local.is_ipv4() != IS_IP4 {
        return Err(TcpError::SegmentInvalid.into());
    }
    let main = crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let session_main = hammer_service::session::SessionMain::global()?;
    // SAFETY: this Node executes on the DataPlaneMain's owning runtime thread.
    let sessions = unsafe { session_main.worker_mut(runtime) }?;
    let mut tcp = main.worker(runtime.thread_index())?;
    let (keep_current, control_segment, connection_index) = {
        let sessions = &mut *sessions;
        let tcp = &mut *tcp;
        let mut keep_current = true;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime.record_current_node_error(TcpNodeError::SynSentSessionRouteMissing);
            TcpNodeError::SynSentSessionRouteMissing
        })?;
        let handle = SessionHandle {
            worker_index: sessions.worker_index(),
            session_index: session_id,
        };
        let connection_index = sessions
            .session_from_handle(handle)
            .map(|session| session.connection_index())
            .ok_or(TcpNodeError::SynSentSessionMissing)?;
        let (control, acked_tx_len, established, established_with_payload) = {
            let crate::worker::TcpWorker {
                connections,
                lookup,
                timer_wheel: timers,
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
                previous_state == crate::TcpState::SynSent && established,
                previous_state == crate::TcpState::SynSent
                    && established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            let tx = sessions
                .session_from_handle(handle)
                .and_then(|session| session.tx_fifo())
                .ok_or(TcpNodeError::SynSentSessionMissing)?;
            assert_eq!(tx.drop_dequeue(acked_tx_len as usize), acked_tx_len as usize);
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
            let enqueue = sessions.enqueue_rx(runtime, handle, index, 0)?;
            if matches!(enqueue, RxDelivery::InOrder { .. }) {
                sessions.enqueue_ready(handle, main.protocol())?;
            }
            keep_current = false;
        };
        if established {
            let crate::worker::TcpWorker {
                connections,
                lookup,
                ..
            } = tcp;
            let connection = connections
                .get(connection_index)
                .ok_or(TcpNodeError::SynSentSessionMissing)?;
            assert!(!lookup.publish_connection(session_id, connection));
            let session = sessions
                .session_from_handle(handle)
                .ok_or(TcpNodeError::SynSentSessionMissing)?;
            session.store_state(SessionState::Ready);
            if let Some(app_worker_index) = session.app_worker_index() {
                let applications = ApplicationMain::global()
                    .expect("Application Main initializes before TCP input");
                let app_worker = unsafe { applications.worker(app_worker_index) }
                    .expect("connected Session retains its Application Worker");
                app_worker.add_event(session, SessionEventType::Connected);
            }
        }
        (keep_current, control, connection_index)
    };
    if let Some(segment) = control_segment {
        let mut allocated = 0;
        if runtime.buffer_alloc(core::slice::from_mut(&mut allocated)) != 1 {
            return Err(hammer_core::error::DataPlaneError::from(
                hammer_core::error::BufferInvariant::PoolExhausted,
            )
            .into());
        }
        if let Err(source) = segment.write_to_buffer(&mut *runtime.buffer_mut(allocated)) {
            runtime.buffer_free_one(allocated);
            return Err(source);
        }
        let worker_index = runtime.data_worker_id()?.slot() as u32;
        let egress = hammer_core::buffer_opaque!(mut runtime.buffer_mut(allocated) => crate::TcpSecondaryOpaque)
            .egress_mut();
        egress.connection_index = connection_index;
        egress.worker_index = worker_index;
        let core_sessions = hammer_service::session::SessionMain::global()?;
        // SAFETY: TCP output runs on this Data Worker's owning thread.
        let core_worker = unsafe { core_sessions.worker_mut(runtime) }?;
        core_worker.add_pending_tx_buffer(
            runtime,
            allocated,
            tcp.tco_next_node[usize::from(!packet.local.is_ipv4())],
        );
    }
    Ok(keep_current)
}
