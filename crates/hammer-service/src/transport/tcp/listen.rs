use std::cell::{Cell, RefCell};

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpError, TcpPacket, TcpSegmentFlags, TcpSeq};
use hammer_infra::vec::Vec;

use super::connection::TcpConnection;
use super::segment::{TcpSegment, parse_tcp_packet};
use super::{
    TCP_MAIN, TcpInputControlPlane, TcpInputNext, TcpQueue, ensure_tcp_session_queue,
    publish_tcp_connection, tcp_worker_state_mut, write_session_route_opaque,
};
#[cfg(test)]
use super::{set_tcp_worker_state, tcp_worker_state};
use crate::session::SessionId;
use crate::transport::congestion::CongestionController;

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
    graph = service,
    init = crate::transport::tcp::listen::register_tcp_listen,
    name = "tcp-listen",
    next = TcpListenNext,
)]
#[hammer_component_macros::node(role = internal, next = TcpListenNext)]
pub struct TcpListenNode<C: CongestionController + 'static> {
    control: TcpInputControlPlane,
    session_queue: TcpQueue<C>,
    #[node(default = Cell::new(None))]
    control_slot: Cell<Option<usize>>,
}

pub fn register_tcp_listen(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId> {
    crate::with_congestion!(|C| {
        let queue_data = ensure_tcp_session_queue::<C>(runtime, worker)?;
        let queue = TcpQueue::<C>::new(queue_data);
        let control = TCP_MAIN
            .get()
            .ok_or_else(|| CoreError::internal("tcp main not initialized"))?
            .control()
            .clone();
        runtime.nodes().try_register_internal_with_next_names(
            TcpListenNode::<C>::new(control, queue, [NodeId::new(0); TcpListenNext::COUNT]),
            &TcpListenNext::NEXT_NAMES,
        )
    })
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

fn tcp_listen_runtime_data<C>(
    session_queue: TcpQueue<C>,
    control_slot: &Cell<Option<usize>>,
    control: &TcpInputControlPlane,
) -> CoreResult<NodeRuntimeData>
where
    C: CongestionController + 'static,
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

impl<C> Node for TcpListenNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_listen_process_frame(runtime, frame, &self.control, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        tcp_listen_runtime_data(self.session_queue, &self.control_slot, &self.control)
    }
}

fn tcp_listen_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpListenNode::<C>::runtime_nexts(runtime)?;
    let control = tcp_listen_control(data)?;
    tcp_listen_process_frame::<C>(
        runtime,
        frame,
        &control,
        TcpQueue::<C>::new(NodeRuntimeData::from_words([data.word(0), 0, 0, 0])),
        next,
    )
}

#[inline]
fn tcp_listen_process_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    control: &TcpInputControlPlane,
    session_queue: TcpQueue<C>,
    next: [NodeId; TcpListenNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpListenNext::Output as usize];
    let tcp_established = next[TcpListenNext::Established as usize];
    let drop_next = next[TcpListenNext::Drop as usize];
    hammer_adapter::node_rewrite_frame!(runtime, frame, drop_next, |index, next_frames| {
        tcp_listen_index(
            runtime,
            index,
            control,
            session_queue,
            tcp_output,
            tcp_established,
            &mut next_frames,
        )
    })
}

fn tcp_listen_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    control: &TcpInputControlPlane,
    session_queue: TcpQueue<C>,
    tcp_output: NodeId,
    tcp_established: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let mut release_input = true;
    let listener = control
        .lookup_listener(packet.local)
        .ok_or(TcpError::NoListener)?;
    let mut tx_index = None;
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
        if let Some(segment) = control {
            let allocated = runtime.packet_buffers().alloc_index()?;
            if let Err(error) = segment.write_to_buffer(runtime.packet_buffers(), allocated) {
                runtime.free_index(allocated);
                return Err(error);
            }
            tx_index = Some(allocated);
        }
        Ok(())
    };
    if let Err(error) = result {
        if let Some(tx_index) = tx_index.take() {
            runtime.free_index(tx_index);
        }
        return Err(error);
    }

    if let Some(tx_index) = tx_index.take() {
        if let Err(error) = next_frames.enqueue(runtime, tcp_output, tx_index) {
            runtime.free_index(tx_index);
            return Err(error);
        }
    }
    if let Some(session_id) = established_session
        && packet.payload_len != 0
    {
        if packet.flags == TcpSegmentFlags::SYN {
            release_input = false;
        } else {
            let mut buffer = runtime.get_buffer_mut(index)?;
            write_session_route_opaque(
                buffer.opaque2_mut(),
                session_id,
                listener.owner_worker,
                TcpInputNext::Established,
            );
            drop(buffer);
            if let Err(error) = next_frames.enqueue(runtime, tcp_established, index) {
                return Err(error);
            }
            release_input = false;
        }
    }
    if release_input {
        runtime.free_index(index);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferFrame, DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeId, NodeProcessFn,
        NodeRegistration, NodeResult, NodeRuntimeData,
    };
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::protocol::tcp::{TcpCapabilities, TcpFastOpenCookie};
    use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
    use hammer_runtime::app::{AppCqeKind, AppOpId, AppSqe};

    use super::*;
    use crate::data_plane::DropNode;
    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::input::TcpInputControlPlane;
    use crate::transport::tcp::lookup::{
        TcpIpv4ListenerAddress, TcpV4ListenerKey, TcpWorkerOwnedState,
    };
    use crate::transport::tcp::output::{TcpOutputNext, TcpOutputNode};
    use crate::transport::tcp::tcp_control_cursor;
    use crate::transport::tcp::{
        TCP_FLAG_ACK, TCP_FLAG_SYN, TcpEstablishedNext, TcpEstablishedNode, TcpInputNext,
        TcpSessionDriver,
    };

    const LOCAL_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    const REMOTE_IP: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
    const LOCAL_PORT: u16 = 443;
    const REMOTE_PORT: u16 = 50_001;
    const LISTENER_ID: u32 = 7;
    const CLIENT_ISN: u32 = 10_000;

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(
            &mut self,
            _runtime: &DataPlaneRuntime,
            _frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must use descriptor process",
            ))
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {
        fn node_registration(&self) -> NodeRegistration
        where
            Self: Sized,
        {
            NodeRegistration::Plain
        }
    }

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let slot = data.usize_word(0)?;
        let state = {
            let states = capture_states().lock().expect("capture registry");
            Arc::clone(
                states
                    .get(slot)
                    .ok_or_else(|| CoreError::internal("capture slot is invalid"))?,
            )
        };
        let mut state = state.lock().expect("capture state");
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index)?;
            state.packets.push(packet.to_vec());
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    #[test]
    fn initial_syn_emits_cookie_syn_ack_without_creating_session_route() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        send_packet(&runtime, input, syn_packet());
        assert!(runtime.run_ready_nodes().expect("run input") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run output chain") >= 1);
        }

        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), 1);
        let syn_ack = &packets.packets[0];
        assert_eq!(tcp_flags(syn_ack), TCP_FLAG_SYN | TCP_FLAG_ACK);
        assert_eq!(tcp_acknowledgment(syn_ack), CLIENT_ISN + 1);
        assert_ne!(tcp_sequence(syn_ack), 0);

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            tcp_worker_state().session_route_by_tuple(local_addr(), remote_addr(None)),
            None
        );
        assert!(tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
    }

    #[test]
    fn final_ack_creates_real_session_after_cookie_validation() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        send_packet(&runtime, input, syn_packet());
        assert!(runtime.run_ready_nodes().expect("run input") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run output chain") >= 1);
        }
        let cookie_sequence = {
            let packets = output_state.lock().expect("capture");
            tcp_sequence(&packets.packets[0])
        };

        send_packet(&runtime, input, ack_packet(cookie_sequence.wrapping_add(1)));
        assert!(runtime.run_ready_nodes().expect("run final ack") >= 1);
        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), 1);
        drop(packets);

        let mut queue = handle.borrow_mut().expect("tcp queue");
        let route = tcp_worker_state()
            .session_route_by_tuple(local_addr(), remote_addr(None))
            .expect("established session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(
            connection.state(),
            crate::transport::tcp::TcpState::Established
        );
        assert!(!tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
    }

    #[test]
    fn invalid_cookie_does_not_create_real_session_route() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        send_packet(&runtime, input, syn_packet());
        assert!(runtime.run_ready_nodes().expect("run input") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run output chain") >= 1);
        }
        let cookie_sequence = {
            let packets = output_state.lock().expect("capture");
            tcp_sequence(&packets.packets[0])
        };

        send_packet(&runtime, input, ack_packet(cookie_sequence.wrapping_add(2)));
        let _ = runtime.run_ready_nodes().expect("run invalid final ack");
        let _ = runtime
            .run_ready_nodes()
            .expect("run invalid final ack output");

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            tcp_worker_state().session_route_by_tuple(local_addr(), remote_addr(None)),
            None
        );
        assert!(tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
    }

    #[test]
    fn final_ack_payload_is_not_folded_into_listener_syn_state() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        send_packet(&runtime, input, syn_packet());
        assert!(runtime.run_ready_nodes().expect("run input") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run output chain") >= 1);
        }
        let cookie_sequence = {
            let packets = output_state.lock().expect("capture");
            tcp_sequence(&packets.packets[0])
        };

        send_packet(
            &runtime,
            input,
            ack_packet_with_payload(cookie_sequence.wrapping_add(1), 5),
        );
        assert!(runtime.run_ready_nodes().expect("run final ack") >= 1);

        let queue = handle.borrow_mut().expect("tcp queue");
        let route = tcp_worker_state()
            .session_route_by_tuple(local_addr(), remote_addr(None))
            .expect("established session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(
            connection.state(),
            crate::transport::tcp::TcpState::Established
        );
        assert_eq!(connection.rcv_nxt(), CLIENT_ISN + 6);
    }

    #[test]
    fn passive_tfo_syn_data_creates_session_and_enqueues_payload() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);
        let ring = hammer_runtime::app::AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let op = AppOpId::new(11);

        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            let cookie = tcp_worker_state_mut().fast_open_cookie_for_listener(
                LISTENER_ID,
                local_addr(),
                remote_addr(None),
            );
            send_packet(
                &runtime,
                input,
                syn_packet_with_payload_and_cookie(remote_addr(None), CLIENT_ISN, b"hello", cookie),
            );
        }
        assert!(runtime.run_ready_nodes().expect("run passive tfo syn") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run passive tfo output") >= 1);
        }

        let session_id = {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            let route = tcp_worker_state()
                .session_route_by_tuple(local_addr(), remote_addr(None))
                .expect("tfo session route");
            assert!(queue.bind_session_app_ring(route.0, op, ring.clone()));
            assert!(!tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
            let connection = queue.session(route.0).expect("tcp session");
            assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynRcvd);
            assert_eq!(connection.rcv_nxt(), CLIENT_ISN + 6);
            route.0
        };

        ring.push_test_submission(AppSqe::recv(None, op, 64))
            .expect("queue recv");
        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            queue
                .flush_session_rx(session_id)
                .expect("flush session rx");
        }
        let completions = ring.take_test_completions(4);
        assert_eq!(completions.len(), 1);
        match completions
            .into_iter()
            .next()
            .expect("recv completion")
            .kind()
        {
            AppCqeKind::Recv { recv, .. } => {
                assert_eq!(recv.copy_current().expect("recv payload"), b"hello");
            }
            other => panic!("expected recv completion, got {other:?}"),
        }

        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), 1);
        let syn_ack = &packets.packets[0];
        assert_eq!(tcp_flags(syn_ack), TCP_FLAG_SYN | TCP_FLAG_ACK);
        assert_eq!(tcp_acknowledgment(syn_ack), CLIENT_ISN + 6);
    }

    #[test]
    fn passive_tfo_clears_existing_listener_pending_tuple() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        send_packet(&runtime, input, syn_packet());
        assert!(runtime.run_ready_nodes().expect("run initial syn") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run initial syn output") >= 1);
        }

        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            assert!(tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
            let cookie = tcp_worker_state_mut().fast_open_cookie_for_listener(
                LISTENER_ID,
                local_addr(),
                remote_addr(None),
            );
            send_packet(
                &runtime,
                input,
                syn_packet_with_payload_and_cookie(remote_addr(None), CLIENT_ISN, b"hello", cookie),
            );
        }
        assert!(runtime.run_ready_nodes().expect("run passive tfo syn") >= 1);
        if output_state.lock().expect("capture").packets.len() < 2 {
            assert!(runtime.run_ready_nodes().expect("run passive tfo output") >= 1);
        }

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert!(!tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
        let route = tcp_worker_state()
            .session_route_by_tuple(local_addr(), remote_addr(None))
            .expect("tfo session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynRcvd);
    }

    #[test]
    fn passive_tfo_invalid_cookie_does_not_create_session() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);
        let mut cookie_bytes = [0u8; TcpFastOpenCookie::MAX_LEN];
        cookie_bytes.fill(0xaa);
        let cookie: TcpFastOpenCookie = cookie_bytes.into();

        send_packet(
            &runtime,
            input,
            syn_packet_with_payload_and_cookie(remote_addr(None), CLIENT_ISN, b"hello", cookie),
        );
        assert!(runtime.run_ready_nodes().expect("run invalid tfo syn") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run invalid tfo output") >= 1);
        }

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            tcp_worker_state().session_route_by_tuple(local_addr(), remote_addr(None)),
            None
        );
        assert!(tcp_worker_state_mut().has_listener_pending(local_addr(), remote_addr(None)));
        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), 1);
        let syn_ack = &packets.packets[0];
        assert_eq!(tcp_flags(syn_ack), TCP_FLAG_SYN | TCP_FLAG_ACK);
        assert_eq!(tcp_acknowledgment(syn_ack), CLIENT_ISN + 1);
    }

    #[test]
    fn backlog_full_rejects_new_listener_tuple() {
        let runtime = DataPlaneRuntime::with_capacities(4096, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        for offset in 0..TCP_LISTENER_BACKLOG {
            let remote = remote_addr(Some(REMOTE_PORT + offset as u16));
            send_packet(
                &runtime,
                input,
                syn_packet_from(remote, CLIENT_ISN + offset as u32),
            );
            let _ = runtime.run_ready_nodes().expect("run listener syn");
            let _ = runtime.run_ready_nodes().expect("run listener syn output");
        }

        let overflow_remote = remote_addr(Some(REMOTE_PORT + TCP_LISTENER_BACKLOG as u16));
        send_packet(
            &runtime,
            input,
            syn_packet_from(overflow_remote, CLIENT_ISN + TCP_LISTENER_BACKLOG as u32),
        );
        let _ = runtime
            .run_ready_nodes()
            .expect("run overflow listener syn");
        let _ = runtime
            .run_ready_nodes()
            .expect("run overflow listener syn output");

        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), TCP_LISTENER_BACKLOG);
        drop(packets);

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            tcp_worker_state_mut().listener_pending_len(LISTENER_ID),
            TCP_LISTENER_BACKLOG
        );
        assert!(!tcp_worker_state_mut().has_listener_pending(local_addr(), overflow_remote));
        assert_eq!(
            tcp_worker_state().session_route_by_tuple(local_addr(), overflow_remote),
            None
        );
    }

    #[test]
    fn passive_tfo_valid_cookie_bypasses_listener_backlog() {
        let runtime = DataPlaneRuntime::with_capacities(4096, 16, 8, 8);
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        for offset in 0..TCP_LISTENER_BACKLOG {
            let remote = remote_addr(Some(REMOTE_PORT + offset as u16));
            send_packet(
                &runtime,
                input,
                syn_packet_from(remote, CLIENT_ISN + offset as u32),
            );
            let _ = runtime.run_ready_nodes().expect("run listener syn");
            let _ = runtime.run_ready_nodes().expect("run listener syn output");
        }

        let tfo_remote = remote_addr(Some(REMOTE_PORT + TCP_LISTENER_BACKLOG as u16 + 1));
        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            let cookie = tcp_worker_state_mut().fast_open_cookie_for_listener(
                LISTENER_ID,
                local_addr(),
                tfo_remote,
            );
            send_packet(
                &runtime,
                input,
                syn_packet_with_payload_and_cookie(
                    tfo_remote,
                    CLIENT_ISN + 9_999,
                    b"hello",
                    cookie,
                ),
            );
        }
        assert!(runtime.run_ready_nodes().expect("run passive tfo syn") >= 1);
        if output_state.lock().expect("capture").packets.len() < TCP_LISTENER_BACKLOG + 1 {
            assert!(runtime.run_ready_nodes().expect("run passive tfo output") >= 1);
        }

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            tcp_worker_state_mut().listener_pending_len(LISTENER_ID),
            TCP_LISTENER_BACKLOG
        );
        assert!(!tcp_worker_state_mut().has_listener_pending(local_addr(), tfo_remote));
        let route = tcp_worker_state()
            .session_route_by_tuple(local_addr(), tfo_remote)
            .expect("tfo session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynRcvd);
    }

    fn install_listener_runtime(
        runtime: &DataPlaneRuntime,
    ) -> (NodeId, TcpQueue<BbrController>, Arc<Mutex<CaptureState>>) {
        let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        super::set_tcp_worker_state(&mut owner);
        let handle =
            crate::session::node::register_session_queue(TcpSessionDriver::<BbrController>::new(
                DataWorkerId::new(0),
                runtime.packet_buffers().clone(),
            ))
            .expect("register queue");
        owner.insert_listener::<TcpIpv4ListenerAddress>(
            TcpV4ListenerKey::new(0, LOCAL_IP, LOCAL_PORT),
            LISTENER_ID,
            TcpCapabilities {
                fast_open: true,
                ..TcpCapabilities::default()
            },
        );
        let control = TcpInputControlPlane::new();
        control
            .publish_lookup(owner.publish_snapshot())
            .expect("publish lookup");

        let output_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&output_state)));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, capture)));
        let established = runtime.nodes().register_internal(TcpEstablishedNode::new(
            handle,
            TcpEstablishedNext::nodes(output, drop),
        ));
        let listen = runtime.nodes().register_internal(TcpListenNode::new(
            control.clone(),
            handle,
            TcpListenNext::nodes(output, established, drop),
        ));
        let input = runtime.nodes().register_internal(control.node(
            TcpInputNext::nodes(drop, drop, listen, drop, established, drop, drop),
            Some(handle),
            None,
        ));
        (input, handle, output_state)
    }

    fn send_packet(runtime: &DataPlaneRuntime, node: NodeId, packet: std::vec::Vec<u8>) {
        let frame = runtime.alloc_frame_index().expect("frame");
        let buffer = runtime.alloc_index_with_bytes(&packet).expect("packet");
        let cursor = tcp_control_cursor(&packet).expect("cursor");
        runtime
            .get_buffer_mut(buffer)
            .expect("buffer mut")
            .set_packet_cursor(cursor);
        runtime
            .get_frame_mut(frame)
            .expect("frame mut")
            .push_index(buffer)
            .expect("push packet");
        assert!(runtime.schedule_frame(node, frame).expect("schedule"));
    }

    fn syn_packet() -> std::vec::Vec<u8> {
        syn_packet_from(remote_addr(None), CLIENT_ISN)
    }

    fn syn_packet_from(remote: SocketAddr, sequence: u32) -> std::vec::Vec<u8> {
        tcp_control_packet(
            remote,
            local_addr(),
            hammer_core::protocol::tcp::TcpSegmentHeader {
                source_port: remote.port(),
                destination_port: local_addr().port(),
                sequence_number: sequence,
                acknowledgment_number: 0,
                flags: TcpSegmentFlags::SYN,
                advertised_window: u16::MAX,
                capabilities: TcpCapabilities::default(),
                timestamp: None,
                fast_open_cookie: None,
            },
            &[],
        )
        .expect("syn packet")
    }

    fn syn_packet_with_payload_and_cookie(
        remote: SocketAddr,
        sequence: u32,
        payload: &[u8],
        cookie: TcpFastOpenCookie,
    ) -> std::vec::Vec<u8> {
        tcp_control_packet(
            remote,
            local_addr(),
            hammer_core::protocol::tcp::TcpSegmentHeader {
                source_port: remote.port(),
                destination_port: local_addr().port(),
                sequence_number: sequence,
                acknowledgment_number: 0,
                flags: TcpSegmentFlags::SYN,
                advertised_window: u16::MAX,
                capabilities: TcpCapabilities {
                    fast_open: true,
                    ..TcpCapabilities::default()
                },
                timestamp: None,
                fast_open_cookie: Some(&cookie),
            },
            payload,
        )
        .expect("tfo syn")
    }

    fn ack_packet(acknowledgment: u32) -> std::vec::Vec<u8> {
        tcp_control_packet(
            remote_addr(None),
            local_addr(),
            hammer_core::protocol::tcp::TcpSegmentHeader {
                source_port: remote_addr(None).port(),
                destination_port: local_addr().port(),
                sequence_number: CLIENT_ISN + 1,
                acknowledgment_number: acknowledgment,
                flags: TcpSegmentFlags::ACK,
                advertised_window: u16::MAX,
                capabilities: TcpCapabilities::default(),
                timestamp: None,
                fast_open_cookie: None,
            },
            &[],
        )
        .expect("ack packet")
    }

    fn ack_packet_with_payload(acknowledgment: u32, payload_len: usize) -> std::vec::Vec<u8> {
        tcp_control_packet(
            remote_addr(None),
            local_addr(),
            hammer_core::protocol::tcp::TcpSegmentHeader {
                source_port: remote_addr(None).port(),
                destination_port: local_addr().port(),
                sequence_number: CLIENT_ISN + 1,
                acknowledgment_number: acknowledgment,
                flags: TcpSegmentFlags::ACK,
                advertised_window: u16::MAX,
                capabilities: TcpCapabilities::default(),
                timestamp: None,
                fast_open_cookie: None,
            },
            &std::vec![b'x'; payload_len],
        )
        .expect("ack packet")
    }

    fn tcp_control_packet(
        local: SocketAddr,
        remote: SocketAddr,
        header: hammer_core::protocol::tcp::TcpSegmentHeader<'_>,
        payload: &[u8],
    ) -> Result<std::vec::Vec<u8>, TcpError> {
        let mut tcp = [0u8; 60];
        let tcp_header_len =
            hammer_core::protocol::tcp::write_tcp_segment_header(&mut tcp, header, None)?;
        let tcp_len = tcp_header_len
            .checked_add(payload.len())
            .ok_or(TcpError::Dispatch)?;
        match (local.ip(), remote.ip()) {
            (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => {
                let packet_len = 20usize.checked_add(tcp_len).ok_or(TcpError::Dispatch)?;
                let total_len = u16::try_from(packet_len).map_err(|_| TcpError::Length)?;
                let mut packet = std::vec![0u8; packet_len];
                packet[0] = 0x45;
                packet[2..4].copy_from_slice(&total_len.to_be_bytes());
                packet[8] = 64;
                packet[9] = 6;
                packet[12..16].copy_from_slice(&local_ip.octets());
                packet[16..20].copy_from_slice(&remote_ip.octets());
                packet[20..20 + tcp_header_len].copy_from_slice(&tcp[..tcp_header_len]);
                if !payload.is_empty() {
                    packet[20 + tcp_header_len..20 + tcp_header_len + payload.len()]
                        .copy_from_slice(payload);
                }
                let tcp_checksum = internet_checksum_parts(&[
                    &local_ip.octets(),
                    &remote_ip.octets(),
                    &[0, 6],
                    &(packet_len as u16 - 20).to_be_bytes(),
                    &packet[20..],
                ]);
                packet[36..38].copy_from_slice(&tcp_checksum.to_be_bytes());
                let ip_checksum = internet_checksum(&packet[..20]);
                packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
                Ok(packet)
            }
            (IpAddr::V6(local_ip), IpAddr::V6(remote_ip)) => {
                let packet_len = 40usize.checked_add(tcp_len).ok_or(TcpError::Dispatch)?;
                let payload_len = u16::try_from(tcp_len).map_err(|_| TcpError::Length)?;
                let mut packet = std::vec![0u8; packet_len];
                packet[0] = 0x60;
                packet[4..6].copy_from_slice(&payload_len.to_be_bytes());
                packet[6] = 6;
                packet[7] = 64;
                packet[8..24].copy_from_slice(&local_ip.octets());
                packet[24..40].copy_from_slice(&remote_ip.octets());
                packet[40..40 + tcp_header_len].copy_from_slice(&tcp[..tcp_header_len]);
                if !payload.is_empty() {
                    packet[40 + tcp_header_len..40 + tcp_header_len + payload.len()]
                        .copy_from_slice(payload);
                }
                let tcp_checksum = internet_checksum_parts(&[
                    &local_ip.octets(),
                    &remote_ip.octets(),
                    &(tcp_len as u32).to_be_bytes(),
                    &[0, 0, 0, 6],
                    &packet[40..],
                ]);
                packet[56..58].copy_from_slice(&tcp_checksum.to_be_bytes());
                Ok(packet)
            }
            _ => Err(TcpError::SegmentInvalid),
        }
    }

    fn tcp_sequence(packet: &[u8]) -> u32 {
        u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]])
    }

    fn tcp_acknowledgment(packet: &[u8]) -> u32 {
        u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]])
    }

    fn tcp_flags(packet: &[u8]) -> u8 {
        packet[13]
    }

    fn local_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(LOCAL_IP), LOCAL_PORT)
    }

    fn remote_addr(port: Option<u16>) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(REMOTE_IP), port.unwrap_or(REMOTE_PORT))
    }
}

fn tcp_handle_listener_packet<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    queue: &mut crate::session::runtime::SessionDriverRuntime<TcpConnection<C>>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
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

fn tcp_issue_listener_challenge<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    queue: &mut crate::session::runtime::SessionDriverRuntime<TcpConnection<C>>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
{
    let fast_open_valid = if packet.payload_len != 0 && capabilities.fast_open {
        match packet.fast_open_cookie.as_ref() {
            Some(cookie) => tcp_worker_state_mut().validate_fast_open_cookie(
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
        let state = tcp_worker_state_mut();
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

fn tcp_accept_listener_fast_open<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    queue: &mut crate::session::runtime::SessionDriverRuntime<TcpConnection<C>>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
{
    let worker = queue.worker();
    let session_id = queue.insert_session_with_id(|session_id: SessionId| {
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
            let connection = queue
                .session_mut(session_id)
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
        if let Some(op) = queue.session_app_op(session_id) {
            queue.app().complete_connected(op)?;
        }
        {
            let mut buffer = runtime.packet_buffers().get_buffer_mut(index)?;
            buffer.advance(packet.payload_offset as isize)?;
            buffer.truncate_chain(packet.payload_len)?;
        }
        let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
        if enqueue.delivered_len != 0 {
            queue.mark_ready(session_id);
        }
        tcp_worker_state_mut().finish_listener_pending(listener_id, packet.local, packet.remote);
        Ok((control, Some(session_id)))
    })();
    if result.is_err() {
        tcp_worker_state_mut().forget_session(session_id);
        tcp_worker_state_mut().forget_pending_open(session_id);
        let _ = queue.close_session(session_id)?;
    }
    result
}

fn tcp_complete_listener_open<C>(
    queue: &mut crate::session::runtime::SessionDriverRuntime<TcpConnection<C>>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
{
    let Some(acknowledgment) = packet.acknowledgment else {
        return Ok((None, None));
    };
    let cookie = acknowledgment.raw().wrapping_sub(1);
    let pending = {
        let state = tcp_worker_state_mut();
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
    let session_id = queue.insert_session_with_id(|session_id: SessionId| {
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
            let connection = queue
                .session_mut(session_id)
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
            connection.receive_final_ack(packet)?
        };
        tcp_worker_state_mut().finish_listener_pending(listener_id, packet.local, packet.remote);
        publish_tcp_connection(queue, session_id)?;
        Ok((control, Some(session_id)))
    })();
    if result.is_err() {
        tcp_worker_state_mut().forget_session(session_id);
        tcp_worker_state_mut().forget_pending_open(session_id);
        let _ = queue.close_session(session_id)?;
    }
    result
}
