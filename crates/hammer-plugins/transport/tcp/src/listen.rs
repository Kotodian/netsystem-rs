use crate::{TcpCapabilities, TcpError, TcpPacket, TcpSegmentFlags, TcpSeq};
use hammer_core::data_plane::{DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext};
use hammer_plugin_session::{IpSessionEndpoint, IpSessionMain};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime};

use super::connection::TcpConnection;
use super::segment::{TcpSegment, tcp_packet};
use super::{TcpInputNext, TcpNodeError, write_session_route_opaque};
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::{RxDelivery, SessionHandle, SessionWorker};

const TCP_LISTENER_BACKLOG: usize = 128;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    #[next("tcp4-output")]
    Output,
    #[next("tcp4-established")]
    Established,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::listen::register_tcp4_listen,
    name = "tcp4-listen",
    next = TcpListenNext,
    role = internal,
)]
pub struct Tcp4ListenNode {}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::listen::register_tcp6_listen,
    name = "tcp6-listen",
    next = TcpListenNext,
    role = internal,
)]
pub struct Tcp6ListenNode {}

pub fn register_tcp4_listen(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name("tcp4-listen") {
        return Ok(node);
    }
    runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp4ListenNode::new(), &TcpListenNext::NEXT_NAMES)
}

pub fn register_tcp6_listen(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Tcp6ListenNode::new(),
        &["tcp6-output", "tcp6-established", "drop"],
    )
}

impl Node for Tcp4ListenNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_listen_process::<true>;
        process(runtime, node_runtime, frame)
    }
}

impl Node for Tcp6ListenNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_listen_process::<false>;
        process(runtime, node_runtime, frame)
    }
}

pub(crate) fn tcp_listen_process<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    (|| {
        let Some(main) = crate::TCP_MAIN.get() else {
            return ();
        };
        tcp_listen_process_frame::<IS_IP4>(runtime, node_runtime, frame, main)
    })();
    processed_vectors
}

#[inline]
fn tcp_listen_process_frame<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    main: &crate::TcpMain,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for &index in frame.vector_args() {
        if tcp_listen_index::<IS_IP4>(
            runtime,
            node_runtime,
            index,
            main,
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
                TcpListenNext::Drop,
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
    next: TcpListenNext,
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

fn tcp_listen_index<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    index: u32,
    main: &crate::TcpMain,
    out_frame: &mut Frame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> RuntimeResult<()> {
    let packet = tcp_packet(runtime, index)?;
    if packet.local.is_ipv4() != IS_IP4 {
        return Err(TcpError::SegmentInvalid.into());
    }
    let ip_session = IpSessionMain::global()?;
    let fib_index = hammer_core::buffer_opaque!(runtime.buffer(index) => NetworkOpaque)
        .ip()
        .fib_index()
        .unwrap_or(0);
    let mut transport = crate::tcp_endpoint_pair(packet.local, packet.remote).0;
    transport.local.fib_index = fib_index;
    let endpoint = IpSessionEndpoint::new(transport, main.protocol());
    let listener = main
        .listener_control
        .listener_for_session(ip_session.lookup_listener(&endpoint, true).ok_or_else(|| {
            let _ = runtime.record_current_node_error(TcpNodeError::NoListener);
            TcpError::NoListener
        })?)
        .ok_or_else(|| {
            let _ = runtime.record_current_node_error(TcpNodeError::NoListener);
            TcpError::NoListener
        })?;
    // SAFETY: this Node executes on the DataPlaneMain's owning runtime thread.
    let sessions = unsafe { ip_session.session().worker_mut(runtime) }?;
    let mut tcp = main.worker(runtime.thread_index())?;
    let listener_connection = main
        .listener_connection(listener.lookup_id)
        .expect("published TCP listener retains its transport connection");
    let (control_segment, established_session) = TcpListener::new(
        sessions,
        &mut tcp,
        ip_session,
        listener.lookup_id,
        listener.session_listener.into(),
        listener_connection.base.endpoint.fib_index(),
        listener.capabilities,
    )
    .handle_packet(runtime, index, &packet)?;

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
        hammer_core::buffer_opaque!(mut runtime.buffer_mut(allocated) => crate::TcpSecondaryOpaque)
            .egress_mut()
            .fib_index = listener_connection.base.endpoint.fib_index();
        sessions.add_pending_tx_buffer(
            runtime,
            allocated,
            tcp.tco_next_node[usize::from(!packet.local.is_ipv4())],
        );
    }
    if let Some(session_id) = established_session
        && packet.payload_len != 0
        && packet.flags != TcpSegmentFlags::SYN
    {
        let buffer = runtime.buffer_mut(index);
        write_session_route_opaque(
            hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut(),
            session_id,
            listener.owner_worker,
            TcpInputNext::Established,
        );
        emit_local(
            runtime,
            node_runtime,
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
    sessions: &'a mut SessionWorker,
    tcp: &'a mut crate::TcpWorker,
    ip_session: &'a IpSessionMain,
    id: u32,
    session_listener: SessionHandle,
    fib_index: u32,
    capabilities: TcpCapabilities,
}

impl<'a> TcpListener<'a> {
    fn new(
        sessions: &'a mut SessionWorker,
        tcp: &'a mut crate::TcpWorker,
        ip_session: &'a IpSessionMain,
        id: u32,
        session_listener: SessionHandle,
        fib_index: u32,
        capabilities: TcpCapabilities,
    ) -> Self {
        Self {
            sessions,
            tcp,
            ip_session,
            id,
            session_listener,
            fib_index,
            capabilities,
        }
    }

    fn handle_packet(
        &mut self,
        runtime: &mut DataPlaneMain,
        index: u32,
        packet: &TcpPacket,
    ) -> RuntimeResult<(Option<TcpSegment>, Option<u32>)> {
        if packet.flags == TcpSegmentFlags::SYN {
            return self.issue_challenge(runtime, index, packet);
        }
        if packet.flags.contains(TcpSegmentFlags::ACK)
            && !packet.flags.contains(TcpSegmentFlags::RST)
        {
            return self.complete_open(runtime, packet);
        }
        Ok((None, None))
    }

    fn issue_challenge(
        &mut self,
        runtime: &mut DataPlaneMain,
        index: u32,
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
        runtime: &mut DataPlaneMain,
        index: u32,
        packet: &TcpPacket,
    ) -> RuntimeResult<(Option<TcpSegment>, Option<u32>)> {
        let mut connection = TcpConnection::new(
            None,
            hammer_runtime::DataWorkerId::new(self.sessions.worker_index()),
            self.tcp.protocol(),
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        );
        let control = connection.receive_syn(
            packet.local,
            packet.remote,
            packet.flags,
            packet.sequence,
            packet.advertised_window,
            packet.capabilities,
            packet.timestamp,
            packet.payload_len,
            self.capabilities,
        )?;
        match &mut connection.base.endpoint {
            hammer_plugin_session::IpTransportConnectionId::Ip4 { fib_index, .. }
            | hammer_plugin_session::IpTransportConnectionId::Ip6 { fib_index, .. } => {
                *fib_index = self.fib_index;
            }
        }
        let connection_index = self.tcp.insert_connection(connection);
        let Some(session) = self.accept_connection(runtime, packet, connection_index)? else {
            return Ok((None, None));
        };
        {
            let buffer = runtime.buffer_mut(index);
            buffer.advance(packet.payload_offset as isize);
            buffer.truncate(packet.payload_len)?;
        }
        let delivery = self.sessions.enqueue_rx(runtime, session, index, 0)?;
        if matches!(delivery, RxDelivery::InOrder { .. }) {
            self.sessions.enqueue_notify(runtime, session);
        }
        Ok((control, Some(session.session_index)))
    }

    fn complete_open(
        &mut self,
        runtime: &mut DataPlaneMain,
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
        let mut connection = TcpConnection::new(
            None,
            hammer_runtime::DataWorkerId::new(self.sessions.worker_index()),
            self.tcp.protocol(),
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        );
        connection.connect_state(cookie);
        let _ = connection.receive_syn(
            packet.local,
            packet.remote,
            TcpSegmentFlags::SYN,
            TcpSeq::from(client_sequence),
            advertised_window,
            syn_capabilities,
            syn_timestamp,
            0,
            self.capabilities,
        )?;
        match &mut connection.base.endpoint {
            hammer_plugin_session::IpTransportConnectionId::Ip4 { fib_index, .. }
            | hammer_plugin_session::IpTransportConnectionId::Ip6 { fib_index, .. } => {
                *fib_index = self.fib_index;
            }
        }
        let connection_index = self.tcp.insert_connection(connection);
        let control = {
            let crate::worker::TcpWorker {
                connections,
                timer_wheel: timers,
                ..
            } = &mut *self.tcp;
            let connection = connections
                .get_mut(connection_index)
                .expect("new TCP child remains allocated during handshake");
            connection.receive_final_ack(
                connection_index,
                timers,
                packet,
                std::time::Instant::now(),
            )
        };
        let control = match control {
            Ok(control) => control,
            Err(source) => {
                self.tcp.remove_connection(connection_index);
                self.finish_pending(packet);
                return Err(source);
            }
        };
        if self
            .tcp
            .connection(connection_index)
            .expect("TCP handshake child remains allocated")
            .state()
            != crate::TcpState::Established
        {
            self.tcp.remove_connection(connection_index);
            self.finish_pending(packet);
            return Ok((control, None));
        }
        let session = self.accept_connection(runtime, packet, connection_index)?;
        Ok((control, session.map(|handle| handle.session_index)))
    }

    fn accept_connection(
        &mut self,
        runtime: &mut DataPlaneMain,
        packet: &TcpPacket,
        connection_index: u32,
    ) -> RuntimeResult<Option<SessionHandle>> {
        let connection_id = self
            .tcp
            .connection(connection_index)
            .expect("TCP child remains allocated before Session acceptance")
            .base
            .endpoint;
        // SAFETY: the TCP listener runs on the Session worker that owns this
        // child; service allocates the Session and its Application FIFO pair.
        let accepted = unsafe {
            self.ip_session.accept(
                runtime,
                self.session_listener,
                connection_index,
                self.tcp.protocol(),
            )
        };
        let session = match accepted {
            Ok(Some(session)) => session,
            Ok(None) => {
                self.tcp.remove_connection(connection_index);
                self.finish_pending(packet);
                return Ok(None);
            }
            Err(source) => {
                self.tcp.remove_connection(connection_index);
                self.finish_pending(packet);
                return Err(source.into());
            }
        };
        self.tcp
            .connection_mut(connection_index)
            .expect("accepted TCP child remains allocated")
            .attach_session(session.session_index)
            .expect("new TCP child accepts its Session handle");
        let closed = {
            let crate::worker::TcpWorker {
                connections,
                lookup,
                ..
            } = &mut *self.tcp;
            lookup.publish_connection(
                session.session_index,
                connections
                    .get(connection_index)
                    .expect("accepted TCP child remains allocated"),
            )
        };
        assert!(
            !closed,
            "accepted TCP child is not closed before publication"
        );
        self.ip_session.publish(connection_id, session)?;
        self.finish_pending(packet);
        Ok(Some(session))
    }

    fn finish_pending(&mut self, packet: &TcpPacket) {
        self.tcp
            .lookup
            .finish_listener_pending(self.id, packet.local, packet.remote);
    }
}
