use crate::read_session_id;
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, TraceFormatter};

use hammer_runtime::{RuntimeError, RuntimeResult};

use super::TcpError;
use super::TcpNodeError;
use super::input::{TcpReceiveTrace, format_tcp_receive_trace};
use super::segment::tcp_packet;
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::{ApplicationMain, SessionEventType, SessionHandle, SessionState};

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
    let node = runtime.nodes().try_register_internal_with_next_names(
        Tcp4SynSentNode::new(),
        &TcpSynSentNext::NEXT_NAMES,
    )?;
    crate::register_tcp_node_errors(runtime, node)?;
    Ok(node)
}

pub fn register_tcp6_syn_sent(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp6SynSentNode::new(), &["tcp6-output", "drop"])?;
    crate::register_tcp_node_errors(runtime, node)?;
    Ok(node)
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

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
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

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
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
    // VPP tcp_input.c:1590-1607,1692-1693: capture the half-open
    // connection before SYN-SENT processing changes its state.
    if hammer_runtime::unlikely(node_runtime.trace_enabled()) {
        let tcp = crate::TCP_MAIN
            .get()
            .expect("TCP Main initializes before SYN-SENT input");
        let worker = tcp
            .worker(runtime.thread_index())
            .expect("SYN-SENT trace reads its owner TCP worker");
        for &index in frame.vector_args() {
            if !hammer_runtime::unlikely(runtime.buffer(index).trace_handle().is_some()) {
                continue;
            }
            let (header_ptr, header_len, connection) = {
                let buffer = runtime.buffer(index);
                let cursor = hammer_core::buffer_opaque!(buffer => NetworkOpaque).packet_cursor();
                let header = buffer
                    .current()
                    .get(cursor.transport_header_offset()..)
                    .unwrap_or(&[]);
                let route =
                    hammer_core::buffer_opaque!(buffer => crate::TcpSecondaryOpaque).route();
                let connection = if crate::read_session_route_opaque(route).is_some() {
                    worker.connection(route.connection_index)
                } else {
                    None
                };
                (
                    header.as_ptr(),
                    header.len().min(core::mem::size_of::<crate::TcpHeader>()),
                    connection,
                )
            };
            if let Some(trace) = runtime.add_trace::<TcpReceiveTrace>(node_runtime, index) {
                trace.record_connection(connection);
                trace.header_len =
                    u8::try_from(header_len).expect("TCP base header length fits u8");
                // SAFETY: add_trace changes only trace storage/Buffer metadata;
                // the received packet remains live in disjoint packet storage.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        header_ptr,
                        trace.tcp_header.as_mut_ptr(),
                        header_len,
                    );
                }
            }
        }
    }
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    // VPP tcp_input.c:1700-1921 classifies one packet before its drop exit.
    for &index in frame.vector_args() {
        let mut error = None;
        match tcp_syn_sent_index::<IS_IP4>(
            runtime,
            node_runtime,
            index,
            &mut output,
            &mut nexts,
            &mut out_len,
            &mut error,
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
        if let Some(error) = error {
            runtime
                .record_current_node_error(error)
                .expect("TCP SYN-SENT node registers its packet error");
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(node_runtime, &mut output, &nexts[..out_len]);
    }
    let session_main = hammer_service::session::SessionMain::global()
        .expect("Session Main initializes before TCP SYN-SENT input");
    let sessions = unsafe { session_main.worker_mut(runtime) }
        .expect("TCP SYN-SENT input runs on its Session worker");
    let main = crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before SYN-SENT input");
    sessions.flush_enqueue_events(runtime, main.protocol());
    if main
        .worker(runtime.thread_index())
        .expect("SYN-SENT input runs on its TCP worker")
        .handle_postponed_dequeues(runtime, sessions)
        .is_err()
    {
        runtime
            .record_current_node_error(crate::TcpNodeError::TimerUpdateFailed)
            .expect("TCP input owns its timer node error");
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
    error: &mut Option<TcpNodeError>,
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
            *error = Some(TcpNodeError::SynSentSessionRouteMissing);
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
        let (mut control, acked_tx_len, established, established_with_payload) = {
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
                *error = Some(TcpNodeError::SynSentSessionMissing);
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
            tcp.program_dequeue(connection_index, acked_tx_len);
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
            let payload_sequence = packet.sequence.advance(1);
            let decision = tcp
                .connections
                .get(connection_index)
                .expect("SYN-ACK input retains its TCP connection")
                .accept_payload_from(payload_sequence, packet.payload_len);
            if let Some((trim, offset)) = decision {
                crate::expose_payload(runtime, index, &packet, trim)?;
                let delivery = sessions.enqueue_rx(runtime, handle, index, offset)?;
                let requested = packet.payload_len.saturating_sub(trim) as u32;
                let send_mss = tcp
                    .connections
                    .get(connection_index)
                    .expect("SYN-ACK input retains its TCP connection")
                    .send_mss;
                *error = Some(crate::payload_node_error(
                    delivery, requested, send_mss, offset,
                ));
                tcp.connections
                    .get_mut(connection_index)
                    .expect("SYN-ACK input retains its TCP connection")
                    .receive_payload(payload_sequence, trim as u32, delivery)?;
            } else {
                *error = Some(TcpNodeError::SegmentOld);
            }
            let local_capabilities = tcp
                .lookup
                .pending_open_capabilities(session_id)
                .unwrap_or_default();
            let connection = tcp
                .connections
                .get_mut(connection_index)
                .expect("SYN-ACK input retains its TCP connection");
            control = Some(connection.control_segment(
                packet.local,
                packet.remote,
                crate::TcpSegmentFlags::ACK,
                None,
                local_capabilities,
            ));
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
        sessions.add_pending_tx_buffer(
            runtime,
            allocated,
            tcp.tco_next_node[usize::from(!packet.local.is_ipv4())],
        );
    }
    Ok(keep_current)
}
