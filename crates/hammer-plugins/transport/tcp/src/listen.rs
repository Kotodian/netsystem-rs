use crate::{
    TcpCapabilities, TcpError, TcpPacket, TcpSegmentFlags, TcpSeq, publish_tcp_connection,
};
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData, RuntimeError,
};

use super::connection::TcpConnection;
use super::segment::{TcpSegment, tcp_packet};
use super::{TcpInputNext, TcpNodeError, write_session_route_opaque};
use hammer_service::session::runtime::{RxDelivery, SessionTransport, SessionWorker};

const TCP_LISTENER_BACKLOG: usize = 128;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    #[next("tcp-output")]
    Output,
    #[next("tcp-established")]
    Established,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::listen::register_tcp_listen,
    name = "tcp-listen",
    next = TcpListenNext,
    role = internal,
)]
pub struct TcpListenNode {
    process: NodeProcessFn,
}

pub fn register_tcp_listen(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("tcp-listen") {
        return Ok(node);
    }
    runtime.nodes().try_register_internal_with_next_names(
        TcpListenNode::new(tcp_listen_process),
        &TcpListenNext::NEXT_NAMES,
    )
}

impl Node for TcpListenNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        (self.process)(runtime, NodeRuntimeData::empty(), frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }
}

pub(crate) fn tcp_listen_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let main = crate::TCP_MAIN.load();
    let Some(main) = main.as_deref() else {
        return NodeResult::drop();
    };
    tcp_listen_process_frame(runtime, frame, main)
}

#[inline]
fn tcp_listen_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    main: &crate::TcpMain,
) -> NodeResult {
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
        if tcp_listen_index(runtime, index, main, frame, &mut nexts, &mut out_len).is_err() {
            let _ = emit_local(
                runtime,
                frame,
                &mut nexts,
                &mut out_len,
                TcpListenNext::Drop,
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
    next: TcpListenNext,
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

fn tcp_listen_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    main: &crate::TcpMain,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<()> {
    let packet = tcp_packet(runtime, index)?;
    let listener = main
        .control()
        .lookup_listener(packet.local)
        .ok_or_else(|| {
            let _ = runtime.record_current_node_error(TcpNodeError::NoListener);
            TcpError::NoListener
        })?;
    let (control_segment, established_session) = main.with_worker(runtime, |sessions, tcp| {
        TcpListener::new(
            sessions,
            tcp,
            listener.id,
            listener.session_listener,
            listener.capabilities,
        )
        .handle_packet(runtime, index, &packet)
    })?;

    if let Some(segment) = control_segment {
        let allocated = runtime.buffers().alloc_index()?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        emit_local(
            runtime,
            out_frame,
            nexts,
            out_len,
            TcpListenNext::Output,
            allocated,
        )?;
    }
    if let Some(session_id) = established_session
        && packet.payload_len != 0
        && packet.flags != TcpSegmentFlags::SYN
    {
        let mut buffer = runtime.get_buffer_mut(index)?;
        write_session_route_opaque(
            buffer.opaque2_mut(),
            session_id,
            listener.owner_worker,
            TcpInputNext::Established,
        );
        drop(buffer);
        emit_local(
            runtime,
            out_frame,
            nexts,
            out_len,
            TcpListenNext::Established,
            index,
        )?;
    }
    Ok(())
}

struct TcpListener<'a> {
    sessions: &'a mut SessionWorker<u32>,
    tcp: &'a mut crate::TcpWorker,
    id: u32,
    session_listener: hammer_runtime::app::SessionHandle,
    capabilities: TcpCapabilities,
}

impl<'a> TcpListener<'a> {
    fn new(
        sessions: &'a mut SessionWorker<u32>,
        tcp: &'a mut crate::TcpWorker,
        id: u32,
        session_listener: hammer_runtime::app::SessionHandle,
        capabilities: TcpCapabilities,
    ) -> Self {
        Self {
            sessions,
            tcp,
            id,
            session_listener,
            capabilities,
        }
    }

    fn handle_packet(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: Index,
        packet: &TcpPacket,
    ) -> RuntimeResult<(Option<TcpSegment>, Option<u32>)> {
        if packet.flags == TcpSegmentFlags::SYN {
            return self.issue_challenge(runtime, index, packet);
        }
        if packet.flags.contains(TcpSegmentFlags::ACK)
            && !packet.flags.contains(TcpSegmentFlags::RST)
        {
            return self.complete_open(packet);
        }
        Ok((None, None))
    }

    fn issue_challenge(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: Index,
        packet: &TcpPacket,
    ) -> RuntimeResult<(Option<TcpSegment>, Option<u32>)> {
        let fast_open_valid = packet.payload_len != 0
            && self.capabilities.fast_open
            && packet.fast_open_cookie.as_ref().is_some_and(|cookie| {
                self.tcp.lookup.validate_fast_open_cookie(
                    self.id,
                    packet.local,
                    packet.remote,
                    cookie.as_slice(),
                )
            });
        if fast_open_valid {
            return self.accept_fast_open(runtime, index, packet);
        }
        // VPP `tcp_make_synack_options`: always advertise the local MSS, but
        // echo window scale, timestamps, SACK, ECN, and fast-open only when
        // the SYN offered them, matching what `tcp_negotiate_options` accepts
        // once the handshake completes.
        let offered = packet.capabilities;
        let capabilities = TcpCapabilities {
            max_segment_size: self.capabilities.max_segment_size,
            window_scale: offered.window_scale.and(self.capabilities.window_scale),
            sack: self.capabilities.sack && offered.sack,
            timestamps: self.capabilities.timestamps && offered.timestamps,
            ecn: self.capabilities.ecn && offered.ecn,
            accurate_ecn: self.capabilities.accurate_ecn && offered.accurate_ecn,
            fast_open: self.capabilities.fast_open && offered.fast_open,
        };
        let (begin_ok, sequence, fast_open_cookie) = {
            let lookup = &mut self.tcp.lookup;
            let begin_ok = lookup.begin_listener_pending(
                self.id,
                packet.local,
                packet.remote,
                packet.sequence.raw(),
                packet.advertised_window,
                packet.capabilities,
                packet.timestamp,
                TCP_LISTENER_BACKLOG,
            );
            if !begin_ok {
                (false, 0, None)
            } else {
                let sequence = lookup.listener_cookie_for_syn(
                    self.id,
                    packet.local,
                    packet.remote,
                    packet.sequence.raw(),
                );
                let fast_open_cookie = capabilities.fast_open.then(|| {
                    lookup.fast_open_cookie_for_listener(self.id, packet.local, packet.remote)
                });
                (true, sequence, fast_open_cookie)
            }
        };
        if !begin_ok {
            return Ok((None, None));
        }
        let flags = if capabilities.ecn {
            TcpSegmentFlags::SYN | TcpSegmentFlags::ACK | TcpSegmentFlags::ECE
        } else {
            TcpSegmentFlags::SYN | TcpSegmentFlags::ACK
        };
        Ok((
            Some(TcpSegment::new(
                packet.local,
                packet.remote,
                sequence,
                packet.sequence.advance(1).raw(),
                packet.advertised_window,
                flags,
                capabilities,
                None,
                packet.timestamp.map(|timestamp| crate::TcpTimestampOption {
                    tsval: timestamp.tsecr.max(1),
                    tsecr: timestamp.tsval,
                }),
                fast_open_cookie,
                None,
                0,
            )),
            None,
        ))
    }

    fn accept_fast_open(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: Index,
        packet: &TcpPacket,
    ) -> RuntimeResult<(Option<TcpSegment>, Option<u32>)> {
        let worker_id = self.sessions.worker();
        let capabilities = self.capabilities;
        let (control, session_id) = self.accept_session(
            packet,
            || {
                TcpConnection::new(
                    None,
                    worker_id,
                    packet.local.port(),
                    Some(packet.local),
                    packet.remote,
                )
            },
            |session_id, connection_index, sessions, tcp| {
                let control = {
                    let connection = tcp
                        .connection_mut(connection_index)
                        .ok_or(TcpNodeError::SessionMissing)?;
                    connection.receive_syn(
                        packet.local,
                        packet.remote,
                        packet.flags,
                        packet.sequence,
                        packet.advertised_window,
                        packet.capabilities,
                        packet.timestamp,
                        packet.payload_len,
                        capabilities,
                    )?
                };
                {
                    let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                    buffer.advance(packet.payload_offset as isize)?;
                    buffer.truncate(packet.payload_len)?;
                }
                let enqueue = sessions.enqueue_rx(runtime.buffers(), session_id, index, 0)?;
                if matches!(enqueue, RxDelivery::InOrder { .. }) {
                    sessions.mark_ready(session_id);
                }
                Ok(control)
            },
        )?;
        Ok((control, Some(session_id)))
    }

    fn complete_open(
        &mut self,
        packet: &TcpPacket,
    ) -> RuntimeResult<(Option<TcpSegment>, Option<u32>)> {
        let Some(acknowledgment) = packet.acknowledgment else {
            return Ok((None, None));
        };
        let cookie = acknowledgment.raw().wrapping_sub(1);
        let pending = {
            let lookup = &mut self.tcp.lookup;
            match lookup.listener_pending(self.id, packet.local, packet.remote) {
                Some((client_sequence, advertised_window, syn_capabilities, syn_timestamp))
                    if lookup.validate_listener_cookie(
                        self.id,
                        packet.local,
                        packet.remote,
                        client_sequence,
                        cookie,
                    ) =>
                {
                    Some((
                        client_sequence,
                        advertised_window,
                        syn_capabilities,
                        syn_timestamp,
                    ))
                }
                _ => None,
            }
        };
        let Some((client_sequence, advertised_window, syn_capabilities, syn_timestamp)) = pending
        else {
            return Ok((None, None));
        };
        let worker_id = self.sessions.worker();
        let capabilities = self.capabilities;
        let (control, session_id) = self.accept_session(
            packet,
            || {
                let mut connection = TcpConnection::new(
                    None,
                    worker_id,
                    packet.local.port(),
                    Some(packet.local),
                    packet.remote,
                );
                connection.connect_state(cookie);
                connection
            },
            |_, connection_index, _, tcp| {
                let crate::worker::TcpWorker {
                    connections,
                    timers,
                    ..
                } = tcp;
                let connection = connections
                    .get_mut(connection_index)
                    .ok_or(TcpNodeError::SessionMissing)?;
                let _ = connection.receive_syn(
                    packet.local,
                    packet.remote,
                    TcpSegmentFlags::SYN,
                    TcpSeq::from(client_sequence),
                    advertised_window,
                    syn_capabilities,
                    syn_timestamp,
                    0,
                    capabilities,
                )?;
                connection.receive_final_ack(
                    connection_index,
                    timers,
                    packet,
                    std::time::Instant::now(),
                )
            },
        )?;
        Ok((control, Some(session_id)))
    }

    fn accept_session<R, C, P>(
        &mut self,
        packet: &TcpPacket,
        create: C,
        prepare: P,
    ) -> RuntimeResult<(R, u32)>
    where
        C: FnOnce() -> TcpConnection,
        P: FnOnce(u32, u32, &mut SessionWorker<u32>, &mut crate::TcpWorker) -> RuntimeResult<R>,
    {
        let connection_index = self.tcp.insert_connection(create());
        let listener = self.session_listener;
        let session_id = match self.sessions.stream_accept(
            <crate::TcpWorker as SessionTransport<u32>>::ID,
            connection_index,
            listener,
        ) {
            Ok(session_id) => session_id,
            Err(error) => {
                self.tcp.remove_connection(connection_index);
                self.finish_pending(packet);
                return Err(error);
            }
        };
        let attached = match self.tcp.connection_mut(connection_index) {
            Some(connection) => connection.attach_session(session_id),
            None => Err(TcpNodeError::SessionMissing.into()),
        };
        if let Err(error) = attached {
            if let Err(cleanup_error) = self.rollback_session(session_id, connection_index) {
                tracing::error!(
                    ?session_id,
                    %cleanup_error,
                    "TCP listener Session attachment rollback failed"
                );
            }
            self.finish_pending(packet);
            return Err(error);
        }
        self.finish_pending(packet);
        let output = match prepare(session_id, connection_index, self.sessions, self.tcp) {
            Ok(output) => output,
            Err(error) => {
                if let Err(cleanup_error) = self.rollback_session(session_id, connection_index) {
                    tracing::error!(
                        ?session_id,
                        %cleanup_error,
                        "TCP listener Session preparation rollback failed"
                    );
                }
                return Err(error);
            }
        };
        publish_tcp_connection(self.sessions, self.tcp, session_id)?;
        Ok((output, session_id))
    }

    fn rollback_session(&mut self, session_id: u32, connection_index: u32) -> RuntimeResult<()> {
        self.tcp.lookup.forget_session(session_id);
        self.tcp.lookup.forget_pending_open(session_id);
        let session_cleanup = self.sessions.rollback_session_creation(session_id);
        self.tcp.remove_connection(connection_index);
        match session_cleanup {
            Err(error) => Err(error),
            Ok(Some(index)) if index != connection_index => {
                Err(TcpNodeError::SessionMissing.into())
            }
            Ok(_) => Ok(()),
        }
    }

    fn finish_pending(&mut self, packet: &TcpPacket) {
        self.tcp
            .lookup
            .finish_listener_pending(self.id, packet.local, packet.remote);
    }
}
