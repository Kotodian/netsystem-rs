use std::cell::{Cell, RefCell};

use hammer_core::data_plane::{BufferFrame, BufferIndex, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpError, TcpPacket, TcpSegmentFlags, TcpSeq};
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::segment::Segment;
use hammer_infra::vec::Vec;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use super::connection::TcpConnection;
use super::segment::{TcpSegment, tcp_packet};
use super::{
    TCP_MAIN, TcpInputControlPlane, TcpInputNext, TcpNodeError, TcpWorker,
    ensure_tcp_session_queue, publish_tcp_connection, write_session_route_opaque,
};
#[cfg(test)]
use crate::net::NetworkOpaque;
use crate::session::SessionId;
use crate::session::SessionQueueHandle;
use crate::session::app::SessionAppRuntimeCreate;
use crate::session::runtime::{RxDelivery, SessionDriverRuntime};
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
    role = internal,
)]
pub struct TcpListenNode<C: CongestionController + 'static, Seg: Segment> {
    control: TcpInputControlPlane,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    #[node(default = Cell::new(None))]
    control_slot: Cell<Option<usize>>,
}

pub fn register_tcp_listen(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId> {
    crate::with_congestion!(|C| {
        crate::with_segment!(|Seg| {
            let queue_data = ensure_tcp_session_queue::<C>(runtime, worker)?;
            let queue: SessionQueueHandle<
                SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
            > = SessionQueueHandle::new(queue_data);
            let control = TCP_MAIN
                .load()
                .as_deref()
                .ok_or_else(|| CoreError::internal("tcp main not initialized"))?
                .control()
                .clone();
            runtime.nodes().try_register_internal_with_next_names(
                TcpListenNode::<C, Seg>::new(
                    control,
                    queue,
                    [NodeId::new(0); TcpListenNext::COUNT],
                ),
                &TcpListenNext::NEXT_NAMES,
            )
        })
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

fn tcp_listen_runtime_data<C, Seg>(
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    control_slot: &Cell<Option<usize>>,
    control: &TcpInputControlPlane,
) -> CoreResult<NodeRuntimeData>
where
    C: CongestionController + 'static,
    Seg: Segment,
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
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let next = match Self::runtime_nexts(runtime) {
            Ok(next) => next,
            Err(_) => return NodeResult::drop(),
        };
        tcp_listen_process_frame(runtime, frame, &self.control, self.session_queue, next)
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
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let next = match TcpListenNode::<C, Seg>::runtime_nexts(runtime) {
        Ok(next) => next,
        Err(_) => return NodeResult::drop(),
    };
    let control = match tcp_listen_control(data) {
        Ok(c) => c,
        Err(_) => return NodeResult::drop(),
    };
    tcp_listen_process_frame::<C, Seg>(
        runtime,
        frame,
        &control,
        SessionQueueHandle::new(NodeRuntimeData::from_words([data.word(0), 0, 0, 0])),
        next,
    )
}

#[inline]
fn tcp_listen_process_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    control: &TcpInputControlPlane,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    next: [NodeId; TcpListenNext::COUNT],
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let tcp_output = next[TcpListenNext::Output as usize];
    let tcp_established = next[TcpListenNext::Established as usize];
    let drop_next = next[TcpListenNext::Drop as usize];
    let width = runtime.preferred_frame_batch_width();
    let _ = frame.rewrite_indices_batched(width, |index| {
        if tcp_listen_index(
            runtime,
            index,
            control,
            session_queue,
            tcp_output,
            tcp_established,
        )
        .is_err()
            && let Ok(mut drop_frame) = runtime.buffers().get_next_frame(drop_next)
            && drop_frame.push_index(index).is_ok()
        {
            let _ = runtime.put_next_frame(drop_frame);
        }
        Ok(None)
    });
    NodeResult::drop()
}

fn tcp_listen_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    control: &TcpInputControlPlane,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    tcp_output: NodeId,
    tcp_established: NodeId,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let listener = control.lookup_listener(packet.local).ok_or_else(|| {
        let _ = runtime.record_current_node_error(TcpNodeError::NoListener.code());
        TcpError::NoListener
    })?;
    let mut tx_frame = None;
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
            let mut owner = runtime.buffers().get_next_frame(tcp_output)?;
            let allocated = runtime.buffers().alloc_index()?;
            owner.push_index(allocated)?;
            segment.write_to_buffer(runtime.buffers(), allocated)?;
            tx_frame = Some(owner);
        }
        Ok(())
    };
    if let Err(error) = result {
        return Err(error);
    }

    if let Some(tx_frame) = tx_frame.take() {
        runtime.put_next_frame(tx_frame)?;
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
            let mut established_frame = runtime.buffers().get_next_frame(tcp_established)?;
            established_frame.push_index(index)?;
            runtime.put_next_frame(established_frame)?;
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod legacy_tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use super::*;
    use crate::data_plane::DropNode;
    use crate::session::SessionQueueHandle;
    use crate::session::runtime::SessionDriverRuntime;
    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::input::TcpInputControlPlane;
    use crate::transport::tcp::lookup::{TcpIpv4ListenerAddress, TcpV4ListenerKey};
    use crate::transport::tcp::output::{TcpOutputNext, TcpOutputNode};
    use crate::transport::tcp::tcp_control_cursor;
    use crate::transport::tcp::{
        TCP_FLAG_ACK, TCP_FLAG_SYN, TcpEstablishedNext, TcpEstablishedNode, TcpInputNext, TcpWorker,
    };
    use hammer_core::data_plane::{BufferFrame, NodeId, NodeRegistration};
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::protocol::tcp::{TcpCapabilities, TcpFastOpenCookie};
    use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
    use hammer_runtime::{
        DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeProcessFn, NodeResult,
        NodeRuntimeData,
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
        fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
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
    ) -> NodeResult {
        let slot = match data.usize_word(0) {
            Ok(s) => s,
            Err(_) => return NodeResult::drop(),
        };
        let state = {
            let states = capture_states().lock().expect("capture registry");
            match states.get(slot) {
                Some(s) => Arc::clone(s),
                None => return NodeResult::drop(),
            }
        };
        let mut state = state.lock().expect("capture state");
        for &index in frame.pending_indices() {
            let packet = match runtime.get_buffer(index) {
                Ok(buf) => buf.current().to_vec(),
                Err(_) => return NodeResult::drop(),
            };
            state.packets.push(packet);
        }
        NodeResult::drop()
    }

    fn run_until_captured(
        runtime: &DataPlaneRuntime,
        output_state: &Arc<Mutex<CaptureState>>,
        expected_packets: usize,
        context: &str,
    ) {
        let mut attempts = 0usize;
        while output_state.lock().expect("capture").packets.len() < expected_packets {
            attempts += 1;
            assert!(
                attempts <= 3,
                "{context}: expected {expected_packets} captured packets"
            );
            assert!(runtime.run_ready_nodes().expect(context) >= 1);
        }
    }

    fn drain_ready_nodes(runtime: &DataPlaneRuntime, context: &str) {
        for _ in 0..3 {
            if runtime.run_ready_nodes().expect(context) == 0 {
                return;
            }
        }
        panic!("{context}: graph still had ready nodes after drain budget");
    }

    #[test]
    fn initial_syn_emits_cookie_syn_ack_without_creating_session_route() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
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
            queue
                .transports()
                .0
                .lookup
                .session_route_by_tuple(local_addr(), remote_addr(None)),
            None
        );
        assert!(
            queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), remote_addr(None))
        );
    }

    #[test]
    fn final_ack_creates_real_session_after_cookie_validation() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
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
        let route = queue
            .transports()
            .0
            .lookup
            .session_route_by_tuple(local_addr(), remote_addr(None))
            .expect("established session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(
            connection.state(),
            crate::transport::tcp::TcpState::Established
        );
        assert!(
            !queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), remote_addr(None))
        );
    }

    #[test]
    fn invalid_cookie_does_not_create_real_session_route() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
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
            queue
                .transports()
                .0
                .lookup
                .session_route_by_tuple(local_addr(), remote_addr(None)),
            None
        );
        assert!(
            queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), remote_addr(None))
        );
    }

    #[test]
    fn final_ack_payload_is_not_folded_into_listener_syn_state() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
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
        let route = queue
            .transports()
            .0
            .lookup
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
    fn passive_tfo_clears_existing_listener_pending_tuple() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        send_packet(&runtime, input, syn_packet());
        assert!(runtime.run_ready_nodes().expect("run initial syn") >= 1);
        if output_state.lock().expect("capture").packets.is_empty() {
            assert!(runtime.run_ready_nodes().expect("run initial syn output") >= 1);
        }

        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            assert!(
                queue
                    .transports_mut()
                    .0
                    .lookup
                    .has_listener_pending(local_addr(), remote_addr(None))
            );
            let cookie = queue
                .transports_mut()
                .0
                .lookup
                .fast_open_cookie_for_listener(LISTENER_ID, local_addr(), remote_addr(None));
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
        assert!(
            !queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), remote_addr(None))
        );
        let route = queue
            .transports()
            .0
            .lookup
            .session_route_by_tuple(local_addr(), remote_addr(None))
            .expect("tfo session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynRcvd);
    }

    #[test]
    fn passive_tfo_invalid_cookie_does_not_create_session() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
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
            queue
                .transports()
                .0
                .lookup
                .session_route_by_tuple(local_addr(), remote_addr(None)),
            None
        );
        assert!(
            queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), remote_addr(None))
        );
        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), 1);
        let syn_ack = &packets.packets[0];
        assert_eq!(tcp_flags(syn_ack), TCP_FLAG_SYN | TCP_FLAG_ACK);
        assert_eq!(tcp_acknowledgment(syn_ack), CLIENT_ISN + 1);
    }

    #[test]
    fn backlog_full_rejects_new_listener_tuple() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 4096,
                    buffer_slots: 256,
                    frame_capacity: 8,
                    frame_slots: 32,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        for offset in 0..TCP_LISTENER_BACKLOG {
            let remote = remote_addr(Some(REMOTE_PORT + offset as u16));
            send_packet(
                &runtime,
                input,
                syn_packet_from(remote, CLIENT_ISN + offset as u32),
            );
            let _ = runtime.run_ready_nodes().expect("run listener syn");
            run_until_captured(
                &runtime,
                &output_state,
                offset + 1,
                "run listener syn output",
            );
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
        drain_ready_nodes(&runtime, "drain overflow listener syn output");

        let packets = output_state.lock().expect("capture");
        assert_eq!(packets.packets.len(), TCP_LISTENER_BACKLOG);
        drop(packets);

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            queue
                .transports_mut()
                .0
                .lookup
                .listener_pending_len(LISTENER_ID),
            TCP_LISTENER_BACKLOG
        );
        assert!(
            !queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), overflow_remote)
        );
        assert_eq!(
            queue
                .transports()
                .0
                .lookup
                .session_route_by_tuple(local_addr(), overflow_remote),
            None
        );
    }

    #[test]
    fn passive_tfo_valid_cookie_bypasses_listener_backlog() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 4096,
                    buffer_slots: 256,
                    frame_capacity: 8,
                    frame_slots: 32,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
        let (input, handle, output_state) = install_listener_runtime(&runtime);

        for offset in 0..TCP_LISTENER_BACKLOG {
            let remote = remote_addr(Some(REMOTE_PORT + offset as u16));
            send_packet(
                &runtime,
                input,
                syn_packet_from(remote, CLIENT_ISN + offset as u32),
            );
            let _ = runtime.run_ready_nodes().expect("run listener syn");
            run_until_captured(
                &runtime,
                &output_state,
                offset + 1,
                "run listener syn output",
            );
        }

        let tfo_remote = remote_addr(Some(REMOTE_PORT + TCP_LISTENER_BACKLOG as u16 + 1));
        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            let cookie = queue
                .transports_mut()
                .0
                .lookup
                .fast_open_cookie_for_listener(LISTENER_ID, local_addr(), tfo_remote);
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
        run_until_captured(
            &runtime,
            &output_state,
            TCP_LISTENER_BACKLOG + 1,
            "run passive tfo output",
        );

        let mut queue = handle.borrow_mut().expect("tcp queue");
        assert_eq!(
            queue
                .transports_mut()
                .0
                .lookup
                .listener_pending_len(LISTENER_ID),
            TCP_LISTENER_BACKLOG
        );
        assert!(
            !queue
                .transports_mut()
                .0
                .lookup
                .has_listener_pending(local_addr(), tfo_remote)
        );
        let route = queue
            .transports()
            .0
            .lookup
            .session_route_by_tuple(local_addr(), tfo_remote)
            .expect("tfo session route");
        let connection = queue.session(route.0).expect("tcp session");
        assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynRcvd);
    }

    fn install_listener_runtime(
        runtime: &DataPlaneRuntime,
    ) -> (
        NodeId,
        SessionQueueHandle<
            SessionDriverRuntime<
                (TcpWorker<BbrController>, ()),
                hammer_infra::segment::Local,
                PoolIndex,
            >,
        >,
        Arc<Mutex<CaptureState>>,
    ) {
        let worker = DataWorkerId::new(0);
        let mut driver = SessionDriverRuntime::new(
            worker,
            runtime.buffers().clone(),
            (TcpWorker::<BbrController>::new(worker), ()),
        );
        driver
            .transports_mut()
            .0
            .lookup
            .insert_listener::<TcpIpv4ListenerAddress>(
                TcpV4ListenerKey::new(0, LOCAL_IP, LOCAL_PORT),
                LISTENER_ID,
                TcpCapabilities {
                    fast_open: true,
                    ..TcpCapabilities::default()
                },
            );
        let snapshot = driver.transports().0.lookup.publish_snapshot();
        let handle = crate::session::node::register_session_queue(driver).expect("register queue");
        let control = TcpInputControlPlane::new();
        control.publish_lookup(snapshot).expect("publish lookup");

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
        let mut frame = runtime.buffers().get_next_frame(node).expect("frame");
        let buffer = runtime.alloc_index_with_bytes(&packet).expect("packet");
        let cursor = tcp_control_cursor(&packet).expect("cursor");
        let mut data_buffer = runtime.get_buffer_mut(buffer).expect("buffer mut");
        let network =
            unsafe { std::mem::transmute::<_, &mut NetworkOpaque>(data_buffer.opaque_mut()) };
        network.set_packet_cursor(cursor);
        let ip_version = (packet[0] >> 4) as u8;
        network.ip_mut().set_ip_version(Some(ip_version));
        network.ip_mut().set_ip_protocol(Some(6));
        frame.push_index(buffer).expect("push packet");
        runtime.put_next_frame(frame).expect("put next frame");
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
                write_be_u16(&mut packet, 2, total_len);
                packet[8] = 64;
                packet[9] = 6;
                write_bytes(&mut packet, 12, &local_ip.octets());
                write_bytes(&mut packet, 16, &remote_ip.octets());
                write_bytes(&mut packet, 20, &tcp[..tcp_header_len]);
                if !payload.is_empty() {
                    write_bytes(&mut packet, 20 + tcp_header_len, payload);
                }
                let tcp_len_bytes = be_u16(packet_len as u16 - 20);
                let tcp_checksum = internet_checksum_parts(&[
                    &local_ip.octets(),
                    &remote_ip.octets(),
                    &[0, 6],
                    &tcp_len_bytes,
                    &packet[20..],
                ]);
                write_be_u16(&mut packet, 36, tcp_checksum);
                let ip_checksum = internet_checksum(&packet[..20]);
                write_be_u16(&mut packet, 10, ip_checksum);
                Ok(packet)
            }
            (IpAddr::V6(local_ip), IpAddr::V6(remote_ip)) => {
                let packet_len = 40usize.checked_add(tcp_len).ok_or(TcpError::Dispatch)?;
                let payload_len = u16::try_from(tcp_len).map_err(|_| TcpError::Length)?;
                let mut packet = std::vec![0u8; packet_len];
                packet[0] = 0x60;
                write_be_u16(&mut packet, 4, payload_len);
                packet[6] = 6;
                packet[7] = 64;
                write_bytes(&mut packet, 8, &local_ip.octets());
                write_bytes(&mut packet, 24, &remote_ip.octets());
                write_bytes(&mut packet, 40, &tcp[..tcp_header_len]);
                if !payload.is_empty() {
                    write_bytes(&mut packet, 40 + tcp_header_len, payload);
                }
                let tcp_len_bytes = be_u32(tcp_len as u32);
                let tcp_checksum = internet_checksum_parts(&[
                    &local_ip.octets(),
                    &remote_ip.octets(),
                    &tcp_len_bytes,
                    &[0, 0, 0, 6],
                    &packet[40..],
                ]);
                write_be_u16(&mut packet, 56, tcp_checksum);
                Ok(packet)
            }
            _ => Err(TcpError::SegmentInvalid),
        }
    }

    fn write_bytes(output: &mut [u8], offset: usize, bytes: &[u8]) {
        let mut index = 0usize;
        while index < bytes.len() {
            output[offset + index] = bytes[index];
            index += 1;
        }
    }

    fn write_be_u16(output: &mut [u8], offset: usize, value: u16) {
        output[offset] = (value >> 8) as u8;
        output[offset + 1] = value as u8;
    }

    fn be_u16(value: u16) -> [u8; 2] {
        [(value >> 8) as u8, value as u8]
    }

    fn be_u32(value: u32) -> [u8; 4] {
        [
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ]
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

fn tcp_handle_listener_packet<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
    index: BufferIndex,
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
    index: BufferIndex,
    queue: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    listener_id: u32,
    capabilities: hammer_core::protocol::tcp::TcpCapabilities,
    packet: &TcpPacket,
) -> CoreResult<(Option<TcpSegment>, Option<SessionId>)>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
        queue.app().connected(session_id)?;
        {
            let mut buffer = runtime.buffers().get_buffer_mut(index)?;
            buffer.advance(packet.payload_offset as isize)?;
            buffer.truncate(packet.payload_len)?;
        }
        let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
        if matches!(enqueue, RxDelivery::InOrder { .. }) {
            queue.mark_session_ready(session_id);
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
        let _ = queue.rollback_session(session_id)?;
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
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
        let _ = queue.rollback_session(session_id)?;
    }
    result
}
