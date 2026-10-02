use crate::read_session_id;
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, TraceFormatter};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::session::SessionHandle;

use super::TcpError;
use super::TcpNodeError;
use super::input::{TcpReceiveTrace, format_tcp_receive_trace};
use super::segment::tcp_packet;
use hammer_service::opaque::NetworkOpaque;

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    #[next("tcp4-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::rcv_process::register_tcp4_rcv_process,
    name = "tcp4-rcv-process",
    next = TcpRcvProcessNext,
    role = internal,
)]
pub struct Tcp4RcvProcessNode {}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::rcv_process::register_tcp6_rcv_process,
    name = "tcp6-rcv-process",
    next = TcpRcvProcessNext,
    role = internal,
)]
pub struct Tcp6RcvProcessNode {}

pub fn register_tcp4_rcv_process(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp4-rcv-process") {
        return Ok(node);
    }
    let node = runtime.nodes().try_register_internal_with_next_names(
        Tcp4RcvProcessNode::new(),
        &TcpRcvProcessNext::NEXT_NAMES,
    )?;
    crate::register_tcp_node_errors(runtime, node)?;
    Ok(node)
}

pub fn register_tcp6_rcv_process(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Tcp6RcvProcessNode::new(),
        &["tcp6-output", "drop"],
    )?;
    crate::register_tcp_node_errors(runtime, node)?;
    Ok(node)
}

impl Node for Tcp4RcvProcessNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_rcv_process_process::<true>;
        process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
    }
}

impl Node for Tcp6RcvProcessNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_rcv_process_process::<false>;
        process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
    }
}

pub(crate) fn tcp_rcv_process_process<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_rcv_process_frame::<IS_IP4>(runtime, node_runtime, frame);
    processed_vectors
}

fn tcp_rcv_process_frame<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    // VPP tcp_input.c:1967-2005: trace the received packet before the
    // state-specific receive path can change its connection.
    if hammer_runtime::unlikely(node_runtime.trace_enabled()) {
        let tcp = crate::TCP_MAIN
            .get()
            .expect("TCP Main initializes before receive processing");
        let worker = tcp
            .worker(runtime.thread_index())
            .expect("receive trace reads its owner TCP worker");
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
    // VPP tcp_input.c:2009-2365 classifies one packet before its drop exit.
    for &index in frame.vector_args() {
        let mut error = None;
        if tcp_rcv_process_index::<IS_IP4>(
            runtime,
            node_runtime,
            index,
            &mut output,
            &mut nexts,
            &mut out_len,
            &mut error,
        )
        .is_err()
        {
            let _ = emit_local(
                runtime,
                node_runtime,
                &mut output,
                &mut nexts,
                &mut out_len,
                TcpRcvProcessNext::Drop,
                index,
            );
        }
        if let Some(error) = error {
            runtime
                .record_current_node_error(error)
                .expect("TCP receive-process node registers its packet error");
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(node_runtime, &mut output, &nexts[..out_len]);
    }
    let session_main = hammer_service::session::SessionMain::global()
        .expect("Session Main initializes before TCP receive processing");
    let sessions = unsafe { session_main.worker_mut(runtime) }
        .expect("TCP receive processing runs on its Session worker");
    let protocol = crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before receive processing")
        .protocol();
    sessions.flush_enqueue_events(runtime, protocol);
    if crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before receive processing")
        .worker(runtime.thread_index())
        .expect("receive processing runs on its TCP worker")
        .handle_postponed_dequeues(runtime, sessions)
        .is_err()
    {
        runtime
            .record_current_node_error(TcpNodeError::TimerUpdateFailed)
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
    next: TcpRcvProcessNext,
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

fn tcp_rcv_process_index<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    _: &mut hammer_runtime::NodeRuntime,
    index: u32,
    _: &mut Frame,
    _: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    _: &mut usize,
    error: &mut Option<TcpNodeError>,
) -> RuntimeResult<()> {
    let packet = tcp_packet(runtime, index)?;
    if packet.local.is_ipv4() != IS_IP4 {
        return Err(TcpError::SegmentInvalid.into());
    }
    let main = crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let ip_session = hammer_plugin_session::IpSessionMain::global()?;
    // SAFETY: this Node executes on the DataPlaneMain's owning runtime thread.
    let sessions = unsafe { ip_session.session().worker_mut(runtime)? };
    let mut tcp = main.worker(runtime.thread_index())?;
    let (control, connection_index) = {
        let sessions = &mut *sessions;
        let tcp = &mut *tcp;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            *error = Some(TcpNodeError::RcvProcessSessionRouteMissing);
            TcpNodeError::RcvProcessSessionRouteMissing
        })?;
        let handle = SessionHandle {
            worker_index: sessions.worker_index(),
            session_index: session_id,
        };
        let connection_index = sessions
            .session_from_handle(handle)
            .map(|session| session.connection_index())
            .ok_or(TcpNodeError::RcvProcessSessionMissing)?;
        let (control, acked_tx_len, receive_payload, prior_state, state) = {
            let crate::worker::TcpWorker {
                connections,
                timer_wheel: timers,
                ..
            } = tcp;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                *error = Some(TcpNodeError::RcvProcessSessionMissing);
                TcpNodeError::RcvProcessSessionMissing
            })?;
            let previous_snd_una = connection.snd_una();
            let prior_state = connection.state();
            let control = connection.receive_close_side(
                connection_index,
                timers,
                &packet,
                std::time::Instant::now(),
            )?;
            (
                control,
                connection.take_acked_tx_len(previous_snd_una),
                packet.payload_len != 0
                    && matches!(
                        connection.state(),
                        crate::TcpState::Established
                            | crate::TcpState::FinWait1
                            | crate::TcpState::FinWait2
                    ),
                prior_state,
                connection.state(),
            )
        };
        if acked_tx_len != 0 {
            tcp.program_dequeue(connection_index, acked_tx_len);
        }
        if prior_state != state && state == crate::TcpState::Closed {
            if packet.flags.contains(crate::TcpSegmentFlags::RST) {
                sessions.transport_reset(runtime, handle, connection_index)?;
            }
            sessions.transport_closed(runtime, handle, connection_index)?;
            tcp.program_cleanup(connection_index);
        } else if prior_state == crate::TcpState::Closing && state == crate::TcpState::TimeWait {
            sessions.transport_closed(runtime, handle, connection_index)?;
        }
        if receive_payload {
            let decision = tcp
                .connections
                .get(connection_index)
                .expect("receive input retains its TCP connection")
                .accept_payload(&packet);
            if let Some((trim, offset)) = decision {
                crate::expose_payload(runtime, index, &packet, trim)?;
                let delivery = sessions.enqueue_rx(runtime, handle, index, offset)?;
                let requested = packet.payload_len.saturating_sub(trim) as u32;
                let send_mss = tcp
                    .connections
                    .get(connection_index)
                    .expect("receive input retains its TCP connection")
                    .send_mss;
                *error = Some(crate::payload_node_error(
                    delivery, requested, send_mss, offset,
                ));
                tcp.connections
                    .get_mut(connection_index)
                    .expect("receive input retains its TCP connection")
                    .receive_payload(packet.sequence, trim as u32, delivery)?;
            } else {
                *error = Some(TcpNodeError::SegmentOld);
            }
            tcp.program_ack(
                runtime,
                sessions,
                connection_index,
                decision.is_none_or(|(_, offset)| offset != 0),
            );
        }
        let fin_control = {
            let crate::worker::TcpWorker {
                connections,
                timer_wheel,
                ..
            } = &mut *tcp;
            let connection = connections
                .get_mut(connection_index)
                .expect("receive input retains its TCP connection");
            connection.process_fin_after_payload(connection_index, timer_wheel, &packet)?
        };
        if fin_control.is_some() {
            tcp.program_ack(runtime, sessions, connection_index, false);
            let state = tcp
                .connections
                .get(connection_index)
                .expect("FIN retains its TCP connection")
                .state();
            if state == crate::TcpState::TimeWait {
                if sessions
                    .session_from_handle(handle)
                    .and_then(|session| session.rx_fifo())
                    .is_some_and(|fifo| fifo.max_dequeue() != 0)
                {
                    sessions.flush_enqueue_events(runtime, tcp.protocol);
                }
                sessions.transport_closed(runtime, handle, connection_index)?;
            }
        }
        (control, connection_index)
    };
    if let Some(segment) = control {
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
    // VPP tcp_input.c:2370-2375 frees every consumed rcv-process input.
    runtime.buffer_free_one(index);
    Ok(())
}
