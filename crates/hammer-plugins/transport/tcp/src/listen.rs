use crate::{insert_tcp_session, mark_tcp_session_ready, rollback_tcp_session, tcp_session_mut};
use std::cell::{Cell, RefCell};

use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpError, TcpPacket, TcpSegmentFlags, TcpSeq};
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::vec::Vec;
use hammer_runtime::app::SessionSegment;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use super::connection::TcpConnection;
use super::segment::{TcpSegment, tcp_packet};
use super::{
    TcpInputControlPlane, TcpInputNext, TcpNodeError, TcpWorker, publish_tcp_connection,
    write_session_route_opaque,
};
#[cfg(test)]
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::SessionId;
use hammer_service::session::SessionQueueHandle;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::session::runtime::{RxDelivery, SessionDriverRuntime};
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
pub struct TcpListenNode<C: CongestionController + 'static, Seg: SessionSegment> {
    control: TcpInputControlPlane,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    #[node(default = Cell::new(None))]
    control_slot: Cell<Option<usize>>,
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

fn tcp_listen_runtime_data<C, Seg>(
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    control_slot: &Cell<Option<usize>>,
    control: &TcpInputControlPlane,
) -> CoreResult<NodeRuntimeData>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
{
    let queue_data = session_queue.runtime_data();
    let control_slot = register_tcp_listen_control(control_slot, control)?;
    Ok(NodeRuntimeData::from_words([
        queue_data.word(0),
        u64::try_from(control_slot)
            .map_err(|_| CoreError::internal("tcp listen control slot overflow"))?,
        queue_data.word(2),
        queue_data.word(3),
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

impl<C, Seg> Node for TcpListenNode<C, Seg>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        tcp_listen_process_frame(runtime, frame, &self.control, self.session_queue)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process::<C, Seg>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        tcp_listen_runtime_data(self.session_queue, &self.control_slot, &self.control)
    }
}

fn tcp_listen_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let control = match tcp_listen_control(data) {
        Ok(c) => c,
        Err(_) => return NodeResult::drop(),
    };
    tcp_listen_process_frame::<C, Seg>(
        runtime,
        frame,
        &control,
        SessionQueueHandle::new(NodeRuntimeData::from_words([data.word(0), 0, 0, 0])),
    )
}

#[inline]
fn tcp_listen_process_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    control: &TcpInputControlPlane,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
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
        if tcp_listen_index(
            runtime,
            index,
            control,
            session_queue,
            frame,
            &mut nexts,
            &mut out_len,
        )
        .is_err()
        {
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
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let listener = control.lookup_listener(packet.local).ok_or_else(|| {
        let _ = runtime.record_current_node_error(TcpNodeError::NoListener.code());
        TcpError::NoListener
    })?;
    let mut control_segment = None;
    let established_session;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let (control, session_id) = tcp_handle_listener_packet(
            runtime,
            index,
            &mut queue,
            listener.id,
            listener.capabilities,
            &packet,
        )?;
        established_session = session_id;
        control_segment = control;
        Ok(())
    };
    if let Err(error) = result {
        return Err(error);
    }

    if let Some(segment) = control_segment.take() {
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
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    if packet.flags == TcpSegmentFlags::SYN {
        return tcp_issue_listener_challenge(
            runtime,
            index,
            queue,
            listener_id,
            capabilities,
            packet,
        );
    }
    if packet.flags.contains(TcpSegmentFlags::ACK) && !packet.flags.contains(TcpSegmentFlags::RST) {
        return tcp_complete_listener_open(queue, listener_id, capabilities, packet);
    }
    Ok((None, None))
}

fn tcp_issue_listener_challenge<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let fast_open_valid = if packet.payload_len != 0 && capabilities.fast_open {
        match packet.fast_open_cookie.as_ref() {
            Some(cookie) => queue.transports_mut().0.lookup.validate_fast_open_cookie(
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
            queue,
            listener_id,
            capabilities,
            packet,
        );
    }
    let (begin_ok, sequence, fast_open_cookie) = {
        let state = &mut queue.transports_mut().0.lookup;
        let begin_ok = state.begin_listener_pending(
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
            let sequence = state.listener_cookie_for_syn(
                listener_id,
                packet.local,
                packet.remote,
                packet.sequence.raw(),
            );
            let fast_open_cookie = capabilities.fast_open.then(|| {
                state.fast_open_cookie_for_listener(listener_id, packet.local, packet.remote)
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
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let worker = queue.worker();
    let session_id = insert_tcp_session(queue, |session_id: SessionId| {
        let connection_id = TcpConnectionId::new(session_id.get());
        TcpConnection::new(
            Some(connection_id),
            worker,
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        )
    })?;
    let result = (|| -> CoreResult<(Option<TcpSegment>, Option<SessionId>)> {
        let control = {
            let connection = tcp_session_mut(queue, session_id)
                .ok_or_else(|| CoreError::internal("tcp fast-open session is missing"))?;
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
        publish_tcp_connection(queue, session_id)?;
        queue.app().connected(session_id)?;
        {
            let mut buffer = runtime.buffers().get_buffer_mut(index)?;
            buffer.advance(packet.payload_offset as isize)?;
            buffer.truncate(packet.payload_len)?;
        }
        let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
        if matches!(enqueue, RxDelivery::InOrder { .. }) {
            mark_tcp_session_ready(queue, session_id);
        }
        queue.transports_mut().0.lookup.finish_listener_pending(
            listener_id,
            packet.local,
            packet.remote,
        );
        Ok((control, Some(session_id)))
    })();
    if result.is_err() {
        queue.transports_mut().0.lookup.forget_session(session_id);
        queue
            .transports_mut()
            .0
            .lookup
            .forget_pending_open(session_id);
        let _ = rollback_tcp_session(queue, session_id)?;
    }
    result
}

fn tcp_complete_listener_open<C, Seg>(
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let Some(acknowledgment) = packet.acknowledgment else {
        return Ok((None, None));
    };
    let cookie = acknowledgment.raw().wrapping_sub(1);
    let pending = {
        let state = &mut queue.transports_mut().0.lookup;
        match state.listener_pending(listener_id, packet.local, packet.remote) {
            Some((client_sequence, advertised_window, syn_capabilities, syn_timestamp))
                if state.validate_listener_cookie(
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
    let worker = queue.worker();
    let session_id = insert_tcp_session(queue, |session_id: SessionId| {
        let connection_id = TcpConnectionId::new(session_id.get());
        let mut connection = TcpConnection::new(
            Some(connection_id),
            worker,
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        );
        connection.connect_state(cookie);
        connection
    })?;
    let result = (|| -> CoreResult<(Option<TcpSegment>, Option<SessionId>)> {
        let control = {
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or_else(|| CoreError::internal("tcp listen session is missing"))?;
            let worker = &mut queue.transports_mut().0;
            let TcpWorker {
                connections,
                timers,
                ..
            } = worker;
            let connection = connections
                .get_mut(connection_index)
                .ok_or_else(|| CoreError::internal("tcp listen session is missing"))?;
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
        queue.transports_mut().0.lookup.finish_listener_pending(
            listener_id,
            packet.local,
            packet.remote,
        );
        publish_tcp_connection(queue, session_id)?;
        Ok((control, Some(session_id)))
    })();
    if result.is_err() {
        queue.transports_mut().0.lookup.forget_session(session_id);
        queue
            .transports_mut()
            .0
            .lookup
            .forget_pending_open(session_id);
        let _ = rollback_tcp_session(queue, session_id)?;
    }
    result
}
