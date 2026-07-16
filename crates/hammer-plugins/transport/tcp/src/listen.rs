use crate::{
    TcpWorkerState, TcpWorkerStore, insert_tcp_session, publish_tcp_connection,
    rollback_tcp_session, with_tcp_worker_mut,
};
use std::cell::{Cell, RefCell};

use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpError, TcpPacket, TcpSegmentFlags, TcpSeq};
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use super::connection::TcpConnection;
use super::segment::{TcpSegment, tcp_packet};
use super::{TcpInputControlPlane, TcpInputNext, TcpNodeError, write_session_route_opaque};
#[cfg(test)]
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::SessionId;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::session::runtime::RxDelivery;
use hammer_service::transport::congestion::CongestionController;

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
    control: TcpInputControlPlane,
    process: NodeProcessFn,
    #[node(default = Cell::new(None))]
    control_slot: Cell<Option<usize>>,
}

impl TcpListenNode {
    pub(crate) fn for_worker<C, Seg>(
        control: TcpInputControlPlane,
        next: [NodeId; TcpListenNext::COUNT],
    ) -> Self
    where
        C: CongestionController + 'static,
        Seg: TcpWorkerStore<C>,
        hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        Self::new(control, tcp_listen_process::<C, Seg>, next)
    }
}

pub fn register_tcp_listen(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime
        .nodes()
        .node_by_name("tcp-listen")
        .ok_or_else(|| CoreError::internal("TCP worker graph is not registered"))
}

thread_local! {
    static TCP_LISTEN_CONTROLS: RefCell<Vec<TcpInputControlPlane>> =
        const { RefCell::new(Vec::new()) };
}

fn register_tcp_listen_control(
    control_slot: &Cell<Option<usize>>,
    control: &TcpInputControlPlane,
) -> CoreResult<usize> {
    TCP_LISTEN_CONTROLS.with(|controls| {
        let mut controls = controls.borrow_mut();
        if let Some(slot) = control_slot.get() {
            let Some(current) = controls.get_mut(slot) else {
                return Err(CoreError::internal("tcp listen control slot is invalid"));
            };
            *current = control.clone();
            Ok(slot)
        } else {
            let slot = controls.len();
            controls.push(control.clone());
            control_slot.set(Some(slot));
            Ok(slot)
        }
    })
}

fn tcp_listen_runtime_data(
    control_slot: &Cell<Option<usize>>,
    control: &TcpInputControlPlane,
) -> CoreResult<NodeRuntimeData> {
    let control_slot = register_tcp_listen_control(control_slot, control)?;
    Ok(NodeRuntimeData::from_words([
        0,
        u64::try_from(control_slot)
            .map_err(|_| CoreError::internal("tcp listen control slot overflow"))?,
        0,
        0,
    ]))
}

fn tcp_listen_control(data: NodeRuntimeData) -> CoreResult<TcpInputControlPlane> {
    let slot = data.usize_word(1)?;
    TCP_LISTEN_CONTROLS.with(|controls| {
        controls
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("tcp listen control is missing"))
    })
}

impl Node for TcpListenNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let data = match tcp_listen_runtime_data(&self.control_slot, &self.control) {
            Ok(data) => data,
            Err(_) => return NodeResult::drop(),
        };
        (self.process)(runtime, data, frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        tcp_listen_runtime_data(&self.control_slot, &self.control)
    }
}

fn tcp_listen_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let control = match tcp_listen_control(data) {
        Ok(c) => c,
        Err(_) => return NodeResult::drop(),
    };
    tcp_listen_process_frame::<C, Seg>(runtime, frame, &control)
}

#[inline]
fn tcp_listen_process_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    control: &TcpInputControlPlane,
) -> NodeResult
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
        if tcp_listen_index(runtime, index, control, frame, &mut nexts, &mut out_len).is_err() {
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
) -> CoreResult<()> {
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

fn tcp_listen_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    control: &TcpInputControlPlane,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let listener = control.lookup_listener(packet.local).ok_or_else(|| {
        let _ = runtime.record_current_node_error(TcpNodeError::NoListener.code());
        TcpError::NoListener
    })?;
    let (control_segment, established_session) = with_tcp_worker_mut::<C, Seg, _>(|state| {
        let (control, session_id) = tcp_handle_listener_packet(
            runtime,
            index,
            state,
            listener.id,
            listener.capabilities,
            &packet,
        )?;
        Ok((control, session_id))
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
    {
        if packet.flags == TcpSegmentFlags::SYN {
        } else {
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
            return Ok(());
        }
    }
    Ok(())
}

fn tcp_handle_listener_packet<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    state: &mut TcpWorkerState<C, Seg>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    if packet.flags == TcpSegmentFlags::SYN {
        return tcp_issue_listener_challenge(
            runtime,
            index,
            state,
            listener_id,
            capabilities,
            packet,
        );
    }
    if packet.flags.contains(TcpSegmentFlags::ACK) && !packet.flags.contains(TcpSegmentFlags::RST) {
        return tcp_complete_listener_open(state, listener_id, capabilities, packet);
    }
    Ok((None, None))
}

fn tcp_issue_listener_challenge<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    state: &mut TcpWorkerState<C, Seg>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let fast_open_valid = if packet.payload_len != 0 && capabilities.fast_open {
        match packet.fast_open_cookie.as_ref() {
            Some(cookie) => state.tcp.lookup.validate_fast_open_cookie(
                listener_id,
                packet.local,
                packet.remote,
                cookie.as_slice(),
            ),
            None => false,
        }
    } else {
        false
    };
    if fast_open_valid {
        return tcp_accept_listener_fast_open(
            runtime,
            index,
            state,
            listener_id,
            capabilities,
            packet,
        );
    }
    let (begin_ok, sequence, fast_open_cookie) = {
        let lookup = &mut state.tcp.lookup;
        let begin_ok = lookup.begin_listener_pending(
            listener_id,
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
                listener_id,
                packet.local,
                packet.remote,
                packet.sequence.raw(),
            );
            let fast_open_cookie = capabilities.fast_open.then(|| {
                lookup.fast_open_cookie_for_listener(listener_id, packet.local, packet.remote)
            });
            (true, sequence, fast_open_cookie)
        }
    };
    if !begin_ok {
        return Ok((None, None));
    }
    let syn_ack_capabilities = capabilities;
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
            syn_ack_capabilities,
            None,
            packet
                .timestamp
                .map(|timestamp| hammer_core::protocol::tcp::TcpTimestampOption {
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

fn tcp_accept_listener_fast_open<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    state: &mut TcpWorkerState<C, Seg>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let worker_id = state.sessions.worker();
    let session_id = insert_tcp_session(state, |session_id: SessionId| {
        let connection_id = TcpConnectionId::new(session_id.get());
        TcpConnection::new(
            Some(connection_id),
            worker_id,
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        )
    })?;
    let result = (|| -> CoreResult<(Option<TcpSegment>, Option<SessionId>)> {
        let control = {
            let (_, connection_index) = state
                .sessions
                .session_transport(session_id)
                .ok_or(TcpNodeError::SessionMissing)?;
            let connection = state
                .tcp
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
        publish_tcp_connection(state, session_id)?;
        state.sessions.app().connected(session_id)?;
        {
            let mut buffer = runtime.buffers().get_buffer_mut(index)?;
            buffer.advance(packet.payload_offset as isize)?;
            buffer.truncate(packet.payload_len)?;
        }
        let enqueue = state.sessions.enqueue_rx(session_id, index, 0, false)?;
        if matches!(enqueue, RxDelivery::InOrder { .. }) {
            state.sessions.mark_ready(session_id);
        }
        state
            .tcp
            .lookup
            .finish_listener_pending(listener_id, packet.local, packet.remote);
        Ok((control, Some(session_id)))
    })();
    if result.is_err() {
        state.tcp.lookup.forget_session(session_id);
        state.tcp.lookup.forget_pending_open(session_id);
        let _ = rollback_tcp_session(state, session_id)?;
    }
    result
}

fn tcp_complete_listener_open<C, Seg>(
    state: &mut TcpWorkerState<C, Seg>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let Some(acknowledgment) = packet.acknowledgment else {
        return Ok((None, None));
    };
    let cookie = acknowledgment.raw().wrapping_sub(1);
    let pending = {
        let lookup = &mut state.tcp.lookup;
        match lookup.listener_pending(listener_id, packet.local, packet.remote) {
            Some((client_sequence, advertised_window, syn_capabilities, syn_timestamp))
                if lookup.validate_listener_cookie(
                    listener_id,
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
    let worker_id = state.sessions.worker();
    let session_id = insert_tcp_session(state, |session_id: SessionId| {
        let connection_id = TcpConnectionId::new(session_id.get());
        let mut connection = TcpConnection::new(
            Some(connection_id),
            worker_id,
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        );
        connection.connect_state(cookie);
        connection
    })?;
    let result = (|| -> CoreResult<(Option<TcpSegment>, Option<SessionId>)> {
        let control = {
            let (_, connection_index) = state
                .sessions
                .session_transport(session_id)
                .ok_or(TcpNodeError::SessionMissing)?;
            let crate::worker::TcpWorker {
                connections,
                timers,
                ..
            } = &mut state.tcp;
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
            )?
        };
        state
            .tcp
            .lookup
            .finish_listener_pending(listener_id, packet.local, packet.remote);
        publish_tcp_connection(state, session_id)?;
        Ok((control, Some(session_id)))
    })();
    if result.is_err() {
        state.tcp.lookup.forget_session(session_id);
        state.tcp.lookup.forget_pending_open(session_id);
        let _ = rollback_tcp_session(state, session_id)?;
    }
    result
}
