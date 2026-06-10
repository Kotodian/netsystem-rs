use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpCloseReason, TcpConnectionId};
use hammer_infra::vec::Vec as InfraVec;

use super::TcpLookupId;
use super::connection::{
    TcpConnectionSnapshotPool, TcpEstablishedSnapshotHandle, default_established_snapshot_handle,
};
use super::input::{mark_pending_tcp_app_ingress, take_pending_tcp_app_ingress};

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    RcvProcess,
}

pub trait TcpEstablishedBackend: Send + Sync {
    fn observe_close(&self, observation: TcpEstablishedObservation) -> CoreResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEstablishedObservation {
    pub lookup_id: TcpLookupId,
    pub connection_id: TcpConnectionId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub reason: TcpCloseReason,
}

#[derive(Clone)]
struct TcpEstablishedStateRuntime {
    snapshot: TcpEstablishedSnapshotHandle,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
}

thread_local! {
    static TCP_ESTABLISHED_RUNTIMES: RefCell<InfraVec<TcpEstablishedStateRuntime>> =
        const { RefCell::new(InfraVec::new()) };
}

#[inline]
fn has_tcp_established_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_established_runtime(
    snapshot: TcpEstablishedSnapshotHandle,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
) -> CoreResult<NodeRuntimeData> {
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpEstablishedStateRuntime { snapshot, backend });
        Ok(NodeRuntimeData::from_words([
            u64::try_from(slot)
                .map_err(|_| CoreError::internal("TCP established runtime slot overflow"))?,
            1,
            0,
            0,
        ]))
    })
}

fn tcp_established_runtime(data: NodeRuntimeData) -> CoreResult<TcpEstablishedStateRuntime> {
    if !has_tcp_established_runtime(data) {
        return Ok(TcpEstablishedStateRuntime {
            snapshot: default_established_snapshot_handle(),
            backend: None,
        });
    }
    let slot = data.usize_word(0)?;
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP established runtime slot is invalid"))
    })
}

fn sync_tcp_established_runtime(
    data: NodeRuntimeData,
    snapshot: TcpEstablishedSnapshotHandle,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
) -> CoreResult<()> {
    if !has_tcp_established_runtime(data) {
        return Ok(());
    }
    let slot = data.usize_word(0)?;
    TCP_ESTABLISHED_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP established runtime slot is invalid"))?;
        runtime.snapshot = snapshot;
        runtime.backend = backend;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpEstablishedNext)]
pub struct TcpEstablishedNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default = default_established_snapshot_handle())]
    snapshot: TcpEstablishedSnapshotHandle,
    #[node(default)]
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpEstablishedNode {
    #[inline]
    pub(crate) fn with_runtime(
        mut self,
        snapshot: TcpEstablishedSnapshotHandle,
        backend: Option<Arc<dyn TcpEstablishedBackend>>,
    ) -> Self {
        if has_tcp_established_runtime(self.runtime_data) {
            let _ =
                sync_tcp_established_runtime(self.runtime_data, snapshot.clone(), backend.clone());
        } else if let Ok(runtime_data) =
            register_tcp_established_runtime(snapshot.clone(), backend.clone())
        {
            self.runtime_data = runtime_data;
        }
        self.snapshot = snapshot;
        self.backend = backend;
        self
    }
}

impl Node for TcpEstablishedNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_established_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            self.backend.clone(),
        )?;
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| {
                tcp_established_next_for_index(
                    runtime,
                    index,
                    rcv_process,
                    &snapshot.connections,
                    self.backend.as_deref(),
                )
            },
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_established_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            self.backend.clone(),
        )?;
        Ok(self.runtime_data)
    }
}

fn tcp_established_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_established_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = TcpEstablishedNode::runtime_nexts(runtime)?;
    let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_established_next_for_index(
            runtime,
            index,
            rcv_process,
            &snapshot.connections,
            state.backend.as_deref(),
        )
    })?;
    Ok(result)
}

fn tcp_established_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    rcv_process: NodeId,
    connections: &TcpConnectionSnapshotPool,
    backend: Option<&dyn TcpEstablishedBackend>,
) -> CoreResult<Option<NodeId>> {
    let Some(lookup_id) = take_pending_tcp_app_ingress(index)? else {
        return Ok(Some(rcv_process));
    };
    if packet_has_rst(runtime, index)? {
        if let Some(backend) = backend
            && let Some(observation) =
                tcp_established_close_observation(runtime, index, lookup_id, connections)?
        {
            backend.observe_close(observation)?;
        }
        runtime.free_index(index);
        return Ok(None);
    }
    mark_pending_tcp_app_ingress(index, lookup_id)?;
    Ok(Some(rcv_process))
}

fn tcp_established_close_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    lookup_id: TcpLookupId,
    connections: &TcpConnectionSnapshotPool,
) -> CoreResult<Option<TcpEstablishedObservation>> {
    let Some(connection) = connections.lookup_by_lookup_id(lookup_id) else {
        return Ok(None);
    };
    let Some(connection_id) = connection.connection_id else {
        return Ok(None);
    };
    let metadata = runtime.metadata(index)?;
    let local = connection.local.or_else(|| {
        metadata
            .destination
            .as_ref()
            .map(|destination| SocketAddr::new(destination.host, destination.port))
    });
    let remote = Some(connection.remote).or_else(|| {
        metadata
            .source
            .as_ref()
            .map(|source| SocketAddr::new(source.host, source.port))
    });
    Ok(match (local, remote) {
        (Some(local), Some(remote)) => Some(TcpEstablishedObservation {
            lookup_id,
            connection_id,
            local,
            remote,
            reason: TcpCloseReason::RemoteReset,
        }),
        _ => None,
    })
}

fn packet_has_rst(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<bool> {
    let cursor = runtime.get_buffer(index)?.packet_cursor();
    let packet: std::vec::Vec<u8> = runtime.copy_current_chain(index)?.into_iter().collect();
    let flags_offset = cursor.transport_header_offset() + 13;
    let Some(flags) = packet.get(flags_offset) else {
        return Ok(false);
    };
    Ok(flags & 0x04 != 0)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, DataWorkerId,
        NodeProcessFn, NodeResult, NodeRuntimeData, RouteMetadata, SocksAddr,
    };
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::protocol::tcp::{TcpCloseReason, TcpConnectionId};
    use hammer_infra::vec::Vec as InfraVec;

    use super::{TcpEstablishedBackend, TcpEstablishedObservation};
    use crate::transport::tcp::input::{
        mark_pending_tcp_app_ingress, take_pending_tcp_app_ingress,
    };
    use crate::transport::tcp::{
        TcpConnectionSnapshot, TcpEstablishedControlPlane, TcpEstablishedNext, TcpState,
    };

    const LOOKUP_ID: u32 = 37;
    const CONNECTION_ID: u64 = 73;

    #[derive(Default)]
    struct CaptureState {
        packets: Vec<Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states()
                .lock()
                .expect("capture state registry poisoned");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture state slot"),
            }
        }
    }

    impl hammer_adapter::Node for CaptureNode {
        #[inline(always)]
        fn process(
            &mut self,
            _runtime: &DataPlaneRuntime,
            _frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must run through descriptor process",
            ))
        }

        #[inline]
        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        #[inline]
        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl hammer_adapter::InternalNode for CaptureNode {}

    fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let state = {
            let states = capture_states()
                .lock()
                .map_err(|_| CoreError::internal("capture state registry poisoned"))?;
            Arc::clone(
                states
                    .get(data.usize_word(0)?)
                    .ok_or_else(|| CoreError::internal("capture state slot is invalid"))?,
            )
        };
        for index in frame.drain_pending() {
            let pending = take_pending_tcp_app_ingress(index)?;
            if pending.is_some() {
                let packet = runtime.copy_current_chain(index)?;
                state
                    .lock()
                    .map_err(|_| CoreError::internal("capture state poisoned"))?
                    .packets
                    .push(packet.into_iter().collect());
            }
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    #[derive(Default)]
    struct RecordingTcpEstablishedBackend {
        observations: Arc<Mutex<Vec<TcpEstablishedObservation>>>,
    }

    impl TcpEstablishedBackend for RecordingTcpEstablishedBackend {
        fn observe_close(&self, observation: TcpEstablishedObservation) -> CoreResult<()> {
            self.observations
                .lock()
                .map_err(|_| CoreError::internal("established observations poisoned"))?
                .push(observation);
            Ok(())
        }
    }

    #[test]
    fn tcp_established_node_observes_remote_reset_and_skips_rcv_process_delivery() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let capture_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
        let backend = Arc::new(RecordingTcpEstablishedBackend::default());
        let control = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(capture))
            .with_backend(backend.clone());

        let local: SocketAddr = "192.0.2.73:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.73:40073".parse().expect("remote");
        let mut connections =
            crate::transport::tcp::TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
        connections.insert(TcpConnectionSnapshot {
            lookup_id: LOOKUP_ID,
            connection_id: Some(TcpConnectionId::new(CONNECTION_ID)),
            owner_worker: DataWorkerId::new(0),
            state: TcpState::Established,
            local_port: local.port(),
            local: Some(local),
            remote,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: u16::MAX as u32,
            rcv_nxt: 0,
            rcv_wnd: u16::MAX as u32,
        });
        control
            .publish_connections(connections.publish_snapshot())
            .expect("publish established connection snapshot");
        let established = runtime.nodes().register_internal(control.node());

        let packet = ipv4_tcp_packet(
            Ipv4Addr::new(198, 51, 100, 73),
            remote.port(),
            Ipv4Addr::new(192, 0, 2, 73),
            local.port(),
            tcp_flags(false, false, true, false),
            b"",
        );
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = push_packet(&runtime, frame, &packet, tcp_metadata(remote, local));
        stamp_tcp_cursor(&runtime, buffer, &packet);
        mark_pending_tcp_app_ingress(buffer, LOOKUP_ID).expect("mark pending app ingress");

        assert!(
            runtime
                .schedule_frame(established, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
        assert_eq!(
            *backend.observations.lock().unwrap(),
            vec![TcpEstablishedObservation {
                lookup_id: LOOKUP_ID,
                connection_id: TcpConnectionId::new(CONNECTION_ID),
                local,
                remote,
                reason: TcpCloseReason::RemoteReset,
            }]
        );
        assert!(
            capture_state.lock().unwrap().packets.is_empty(),
            "remote reset must not continue into rcv-process delivery"
        );
        assert_eq!(runtime.frames_in_use(), 0);
        assert_eq!(runtime.in_use_buffers(), 0);
    }

    fn push_packet(
        runtime: &DataPlaneRuntime,
        frame: hammer_adapter::FrameIndex,
        packet: &[u8],
        metadata: RouteMetadata,
    ) -> BufferIndex {
        let buffer = runtime
            .alloc_index_with_bytes(metadata, packet)
            .expect("alloc packet");
        runtime
            .get_frame_mut(frame)
            .expect("mutate frame")
            .push_index(buffer)
            .expect("push packet");
        buffer
    }

    fn stamp_tcp_cursor(runtime: &DataPlaneRuntime, buffer: BufferIndex, packet: &[u8]) {
        let header_len = ((*packet.first().expect("ipv4 header") & 0x0f) as usize) * 4;
        let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        let tcp_offset = header_len;
        let tcp_header_len = ((packet[tcp_offset + 12] >> 4) as usize) * 4;
        runtime
            .get_buffer_mut(buffer)
            .expect("buffer mut")
            .set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, header_len)
                    .with_transport_header(tcp_offset, tcp_header_len)
                    .with_transport_payload_offset(tcp_offset + tcp_header_len),
            );
    }

    fn tcp_metadata(remote: SocketAddr, local: SocketAddr) -> RouteMetadata {
        RouteMetadata {
            source: Some(SocksAddr::ip(remote.ip(), remote.port())),
            destination: Some(SocksAddr::ip(local.ip(), local.port())),
            ..RouteMetadata::default()
        }
    }

    fn ipv4_tcp_packet(
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
        flags: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
        write_tcp_segment(
            &mut packet[20..],
            source_port,
            destination_port,
            flags,
            payload,
        );
        let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
        packet[36..38].copy_from_slice(&checksum.to_be_bytes());
        update_ipv4_header_checksum(&mut packet);
        packet
    }

    fn ipv4_packet(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        transport_len: usize,
    ) -> Vec<u8> {
        let total_len = 20 + transport_len;
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
        packet[8] = 64;
        packet[9] = protocol;
        packet[12..16].copy_from_slice(&source.octets());
        packet[16..20].copy_from_slice(&destination.octets());
        packet
    }

    fn write_tcp_segment(
        out: &mut [u8],
        source_port: u16,
        destination_port: u16,
        flags: u8,
        payload: &[u8],
    ) {
        out[..2].copy_from_slice(&source_port.to_be_bytes());
        out[2..4].copy_from_slice(&destination_port.to_be_bytes());
        out[12] = 0x50;
        out[13] = flags;
        out[14..16].copy_from_slice(&u16::MAX.to_be_bytes());
        out[20..20 + payload.len()].copy_from_slice(payload);
    }

    fn tcp_flags(fin: bool, syn: bool, rst: bool, ack: bool) -> u8 {
        u8::from(fin) | (u8::from(syn) << 1) | (u8::from(rst) << 2) | (u8::from(ack) << 4)
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let mut words = InfraVec::new();
        words.push(u16::from_be_bytes([source.octets()[0], source.octets()[1]]));
        words.push(u16::from_be_bytes([source.octets()[2], source.octets()[3]]));
        words.push(u16::from_be_bytes([
            destination.octets()[0],
            destination.octets()[1],
        ]));
        words.push(u16::from_be_bytes([
            destination.octets()[2],
            destination.octets()[3],
        ]));
        words.push(u16::from(protocol));
        words.push(segment.len() as u16);
        for chunk in segment.chunks(2) {
            let word = if chunk.len() == 2 {
                u16::from_be_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], 0])
            };
            words.push(word);
        }
        internet_checksum(&words)
    }

    fn update_ipv4_header_checksum(packet: &mut [u8]) {
        packet[10] = 0;
        packet[11] = 0;
        let mut words = InfraVec::new();
        for chunk in packet[..20].chunks(2) {
            words.push(u16::from_be_bytes([chunk[0], chunk[1]]));
        }
        let checksum = internet_checksum(&words);
        packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    }

    fn internet_checksum(words: &[u16]) -> u16 {
        let mut sum = 0u32;
        for word in words {
            sum = sum.wrapping_add(u32::from(*word));
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        !(sum as u16)
    }
}
