use crate::read_session_id;
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::{RxDelivery, SessionHandle};

use super::TcpError;
use super::TcpNodeError;
use super::segment::tcp_packet;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    #[next("tcp4-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::established::register_tcp4_established,
    name = "tcp4-established",
    next = TcpEstablishedNext,
    role = internal,
)]
pub struct Tcp4EstablishedNode {}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::established::register_tcp6_established,
    name = "tcp6-established",
    next = TcpEstablishedNext,
    role = internal,
)]
pub struct Tcp6EstablishedNode {}

pub fn register_tcp4_established(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    if let Some(node) = runtime.nodes().node_by_name("tcp4-established") {
        return Ok(node);
    }
    let node = runtime.nodes().try_register_internal_with_next_names(
        Tcp4EstablishedNode::new(),
        &TcpEstablishedNext::NEXT_NAMES,
    )?;
    crate::register_tcp_node_errors(runtime, node)?;
    Ok(node)
}

pub fn register_tcp6_established(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp6EstablishedNode::new(), &["tcp6-output", "drop"])?;
    crate::register_tcp_node_errors(runtime, node)?;
    Ok(node)
}

impl Node for Tcp4EstablishedNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_established_process::<true>;
        process(runtime, node_runtime, frame)
    }
}

impl Node for Tcp6EstablishedNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_established_process::<false>;
        process(runtime, node_runtime, frame)
    }
}

pub(crate) fn tcp_established_process<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_established_frame::<IS_IP4>(runtime, node_runtime, frame);
    processed_vectors
}

fn tcp_established_frame<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    let mut error_counts = [0u16; super::TCP_NODE_ERRORS.len()];
    let mut error_codes = [None; super::TCP_NODE_ERRORS.len()];
    let mut error_mask = 0u64;
    for (position, &index) in frame.vector_args().iter().enumerate() {
        // VPP tcp_input.c:1359-1369 prefetches the next packet in the
        // single-packet established loop.
        if let Some(&next) = frame.vector_args().get(position + 1) {
            runtime.prefetch_header(next);
            let buffer = runtime.buffer(next);
            let data = buffer
                .current()
                .as_ptr()
                .wrapping_offset(-isize::from(buffer.current_data_offset()));
            hammer_infra::prefetch::prefetch_read_l1_bytes(
                data,
                2 * hammer_infra::align::CACHE_LINE,
            );
        }
        let mut error = None;
        if tcp_established_index::<IS_IP4>(
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
                TcpEstablishedNext::Drop,
                index,
            );
        }
        if let Some(error) = error {
            let code = error.code() as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(node_runtime, &mut output, &nexts[..out_len]);
    }
    let session_main = hammer_service::session::SessionMain::global()
        .expect("Session Main initializes before TCP established input");
    let sessions = unsafe { session_main.worker_mut(runtime) }
        .expect("TCP established input runs on its Session worker");
    let protocol = crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before established input")
        .protocol();
    sessions.flush_enqueue_events(runtime, protocol);
    if crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before established input")
        .worker(runtime.thread_index())
        .expect("established input runs on its TCP worker")
        .handle_postponed_dequeues(runtime, sessions)
        .is_err()
    {
        let error = TcpNodeError::TimerUpdateFailed;
        let code = error.code() as usize;
        error_counts[code] += 1;
        error_codes[code] = Some(error);
        error_mask |= 1u64 << code;
    }
    // VPP tcp_inlines.h:28-53 stores only nonzero frame-local counters.
    while error_mask != 0 {
        let code = error_mask.trailing_zeros() as usize;
        error_mask &= error_mask - 1;
        runtime.record_current_node_error_count(
            error_codes[code].expect("nonzero TCP error has its typed code"),
            u64::from(error_counts[code]),
        )
        .expect("TCP established node registers its error counters");
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

fn tcp_established_index<const IS_IP4: bool>(
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
    let (tx_segment, connection_index) = {
        let sessions = &mut *sessions;
        let tcp = &mut *tcp;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            *error = Some(TcpNodeError::EstablishedSessionRouteMissing);
            TcpNodeError::EstablishedSessionRouteMissing
        })?;
        let handle = SessionHandle {
            worker_index: sessions.worker_index(),
            session_index: session_id,
        };
        let connection_index = sessions
            .session_from_handle(handle)
            .map(|session| session.connection_index())
            .ok_or(TcpNodeError::EstablishedSessionMissing)?;
        let (
            control,
            acked_tx_len,
            accept_payload,
            accepted_sequence,
            duplicate_payload,
        ) = {
            let crate::worker::TcpWorker {
                connections,
                timer_wheel: timers,
                ..
            } = tcp;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                *error = Some(TcpNodeError::EstablishedSessionMissing);
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
                accept_payload,
                packet.sequence,
                duplicate_payload,
            )
        };
        if acked_tx_len != 0 {
            tcp.program_dequeue(connection_index, acked_tx_len);
        }
        if packet.flags.contains(crate::TcpSegmentFlags::RST)
            && tcp
                .connection(connection_index)
                .is_some_and(|connection| connection.state() == crate::TcpState::Closed)
        {
            // VPP tcp_input.c:159-191, tcp46_established_inline: notify
            // Session before delaying the transport connection release.
            sessions.transport_reset(runtime, handle, connection_index)?;
            sessions.transport_closed(runtime, handle, connection_index)?;
            tcp.program_cleanup(connection_index);
            runtime.buffer_free_one(index);
            return Ok(());
        }
        let retransmit_pending = tcp.connection(connection_index)
            .is_some_and(|connection| {
                connection.recovery.in_recovery()
                    && (connection.recovery.retransmit_candidate().is_some()
                        || (!connection.negotiated_options().sack
                            && connection.recovery.oldest_unacked()
                                .is_some_and(|sample| !sample.retransmitted)))
            });
        if retransmit_pending {
            tcp.program_retransmit(runtime, sessions, connection_index);
        }
        let mut immediate_ack = false;
        let mut duplicate_ack = duplicate_payload;
        if let Some((trim, offset)) = accept_payload {
            duplicate_ack = offset != 0;
            let accepted_len = packet.payload_len.saturating_sub(trim) as u32;
            crate::expose_payload(runtime, index, &packet, trim)?;
            let delivery = sessions.enqueue_rx(runtime, handle, index, offset)?;
            let send_mss = tcp.connection(connection_index)
                .expect("payload retains its TCP connection").send_mss;
            *error = Some(crate::payload_node_error(
                delivery, accepted_len, send_mss, offset,
            ));
            let rx_available = match delivery {
                RxDelivery::NotAccepted { rx_available }
                | RxDelivery::InOrder { rx_available, .. }
                | RxDelivery::OutOfOrder { rx_available, .. } => rx_available as usize,
            };
            let connection = tcp.connection_mut(connection_index).ok_or_else(|| {
                *error = Some(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.receive_payload(accepted_sequence, trim as u32, delivery)?;
            // VPP tcp_input.c:1207-1208 programs an ACK for every in-order
            // enqueue; session custom TX coalesces requests in a burst.
            immediate_ack = true;
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
                *error = Some(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.set_rcv_wnd(rx_available);
        } else if duplicate_payload {
            *error = Some(TcpNodeError::SegmentOld);
            let connection = tcp.connection_mut(connection_index).ok_or_else(|| {
                *error = Some(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            let sequence = packet.sequence;
            let end_sequence = sequence.advance(packet.payload_len as u32);
            connection.observe_duplicate_payload(sequence, end_sequence);
            immediate_ack = true;
        }

        let fin_control = {
            let crate::worker::TcpWorker { connections, timer_wheel, .. } = tcp;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                *error = Some(TcpNodeError::EstablishedSessionMissing);
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.process_fin_after_payload(connection_index, timer_wheel, &packet)?
        };
        if fin_control.is_some() {
            tcp.program_ack(runtime, sessions, connection_index, false);
            sessions.transport_closing(runtime, handle, connection_index)?;
        }
        if immediate_ack {
            tcp.program_ack(runtime, sessions, connection_index, duplicate_ack);
        }
        let tx_segment = control;
        (tx_segment, connection_index)
    };
    if let Some(segment) = tx_segment {
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
    // VPP tcp_input.c:1408-1414 frees every consumed established input.
    runtime.buffer_free_one(index);
    Ok(())
}
