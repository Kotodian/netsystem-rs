//! Functional Path MTU tests (IP/FIB-side ownership + ICMP ingress).
//! Assert typed path-MTU state, not error strings.

use std::mem::transmute;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index};
use hammer_plugin_ip::protocol::ip::IpVersion;
use hammer_plugin_ip::{IcmpInputControlPlane, IcmpPathMtuNode};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, InternalNode, Node,
    NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_service::net::pmtu::{
    PathMtuCache, apply_ipv4_frag_needed_icmp, path_mtu_cache, process_ipv4_icmp_path_mtu_packet,
    publish_path_mtu_cache, reset_path_mtu_cache_for_test,
};
use hammer_service::opaque::NetworkOpaque;

static PATH_MTU_CACHE_TEST_LOCK: Mutex<()> = Mutex::new(());

fn with_path_mtu_cache_lock<T>(f: impl FnOnce() -> T) -> T {
    let _guard = PATH_MTU_CACHE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f()
}

#[test]
fn ipv4_frag_needed_updates_per_route_path_mtu_cache() {
    let cache = PathMtuCache::new();
    let dst = Ipv4Addr::new(10, 66, 77, 2);

    assert_eq!(cache.path_mtu(IpAddr::V4(dst)), None);

    cache.apply_ipv4_fragmentation_needed(dst, 576);

    assert_eq!(cache.path_mtu(IpAddr::V4(dst)), Some(576));
    assert_eq!(
        cache.path_mtu(IpAddr::V4(Ipv4Addr::new(10, 66, 77, 1))),
        None
    );
}

#[test]
fn smaller_frag_needed_mtu_wins_larger_does_not_raise() {
    let cache = PathMtuCache::new();
    let dst = Ipv4Addr::new(10, 66, 77, 2);

    cache.apply_ipv4_fragmentation_needed(dst, 576);
    cache.apply_ipv4_fragmentation_needed(dst, 1_280);
    assert_eq!(cache.path_mtu(IpAddr::V4(dst)), Some(576));

    cache.apply_ipv4_fragmentation_needed(dst, 512);
    assert_eq!(cache.path_mtu(IpAddr::V4(dst)), Some(512));
}

#[test]
fn path_mtu_below_minimum_is_clamped_to_ipv4_min_pmtu() {
    let cache = PathMtuCache::new();
    let dst = Ipv4Addr::new(10, 66, 77, 2);

    cache.apply_ipv4_fragmentation_needed(dst, 40);
    assert_eq!(cache.path_mtu(IpAddr::V4(dst)), Some(68));
}

#[test]
fn ipv4_frag_needed_icmp_updates_cache_from_quoted_destination() {
    let cache = PathMtuCache::new();
    let original_dst = Ipv4Addr::new(10, 66, 77, 2);
    let icmp = ipv4_frag_needed_icmp(original_dst, 576);

    let applied = apply_ipv4_frag_needed_icmp(&cache, &icmp).expect("frag-needed");
    assert_eq!(applied, (original_dst, 576));
    assert_eq!(cache.path_mtu(IpAddr::V4(original_dst)), Some(576));
}

#[test]
fn ipv4_non_frag_needed_icmp_does_not_update_cache() {
    let cache = PathMtuCache::new();
    let original_dst = Ipv4Addr::new(10, 66, 77, 2);
    let mut icmp = ipv4_frag_needed_icmp(original_dst, 576);
    icmp[1] = 3;

    assert!(apply_ipv4_frag_needed_icmp(&cache, &icmp).is_none());
    assert_eq!(cache.path_mtu(IpAddr::V4(original_dst)), None);
}

#[test]
fn ipv4_icmp_path_mtu_packet_publishes_into_global_cache() {
    with_path_mtu_cache_lock(|| {
        reset_path_mtu_cache_for_test();
        publish_path_mtu_cache(PathMtuCache::new());

        let original_dst = Ipv4Addr::new(10, 66, 77, 2);
        let packet = ipv4_icmp_frag_needed_packet(original_dst, 576);

        let applied = process_ipv4_icmp_path_mtu_packet(&packet).expect("path mtu");
        assert_eq!(applied, (original_dst, 576));

        let cache = path_mtu_cache().expect("cache published");
        assert_eq!(cache.path_mtu(IpAddr::V4(original_dst)), Some(576));
        reset_path_mtu_cache_for_test();
    });
}

#[test]
fn icmp_input_dest_unreach_frag_needed_updates_path_mtu_cache() {
    with_path_mtu_cache_lock(|| {
        reset_path_mtu_cache_for_test();
        publish_path_mtu_cache(PathMtuCache::new());

        let runtime = test_runtime();
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let punt = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let path_mtu = runtime.nodes().register_internal(IcmpPathMtuNode::new());
        let mut control = IcmpInputControlPlane::new(punt).with_nodes(runtime.nodes().clone());
        let icmp_input = runtime.nodes().register_internal(control.node());
        control
            .attach_consumer(icmp_input)
            .expect("attach icmp input");
        control
            .register_type(IpVersion::V4, 3, path_mtu)
            .expect("register dest unreachable");

        let original_dst = Ipv4Addr::new(10, 66, 77, 2);
        let packet = ipv4_icmp_frag_needed_packet(original_dst, 576);
        let mut frame = runtime
            .buffers()
            .get_next_frame(icmp_input)
            .expect("alloc frame");
        push_packet(&runtime, &mut frame, &packet);
        runtime.put_next_frame(frame).expect("schedule");

        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

        let cache = path_mtu_cache().expect("cache published");
        assert_eq!(cache.path_mtu(IpAddr::V4(original_dst)), Some(576));
        assert!(drop_state.lock().unwrap().packets.is_empty());
        reset_path_mtu_cache_for_test();
    });
}

fn ipv4_frag_needed_icmp(original_dst: Ipv4Addr, next_hop_mtu: u16) -> Vec<u8> {
    let mut icmp = vec![0u8; 8 + 20];
    icmp[0] = 3;
    icmp[1] = 4;
    icmp[6..8].copy_from_slice(&next_hop_mtu.to_be_bytes());

    let quoted = &mut icmp[8..];
    quoted[0] = 0x45;
    quoted[9] = 6;
    quoted[12..16].copy_from_slice(&Ipv4Addr::new(10, 66, 77, 1).octets());
    quoted[16..20].copy_from_slice(&original_dst.octets());
    icmp
}

fn ipv4_icmp_frag_needed_packet(original_dst: Ipv4Addr, next_hop_mtu: u16) -> Vec<u8> {
    let icmp = ipv4_frag_needed_icmp(original_dst, next_hop_mtu);
    let total = 20 + icmp.len();
    let mut packet = vec![0u8; total];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 1;
    packet[12..16].copy_from_slice(&Ipv4Addr::new(198, 51, 100, 1).octets());
    packet[16..20].copy_from_slice(&Ipv4Addr::new(10, 66, 77, 1).octets());
    packet[20..].copy_from_slice(&icmp);
    packet
}

fn test_runtime() -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 16,
            frame_slots: 8,
            ..DataPlaneBufferConfig::default()
        },
    })
}

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

impl Node for CaptureNode {
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CaptureNode {}

fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
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
    let state = match capture_states().lock() {
        Ok(states) => match states.get(slot) {
            Some(s) => Arc::clone(s),
            None => return NodeResult::drop(),
        },
        Err(_) => return NodeResult::drop(),
    };
    for index in frame.pending_indices().iter().copied() {
        let packet = match chain_bytes(runtime, index) {
            Ok(bytes) => bytes,
            Err(_) => return NodeResult::drop(),
        };
        match state.lock() {
            Ok(mut guard) => guard.packets.push(packet.to_vec()),
            Err(_) => return NodeResult::drop(),
        }
    }
    NodeResult::drop()
}

fn chain_bytes(runtime: &DataPlaneRuntime, index: Index) -> RuntimeResult<Vec<u8>> {
    let mut bytes = Vec::new();
    for buffer in runtime.buffers().chain(index) {
        bytes.extend_from_slice(buffer?.current());
    }
    Ok(bytes)
}

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, packet: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    set_ip_cursor(runtime, buffer, packet);
    frame.push_index(buffer).expect("push packet");
}

fn set_ip_cursor(runtime: &DataPlaneRuntime, index: Index, packet: &[u8]) {
    let Some(first) = packet.first().copied() else {
        return;
    };
    let ihl = usize::from(first & 0x0f) * 4;
    let cursor = BufferPacketCursor::new()
        .with_packet_len(packet.len())
        .with_network_header(0, ihl)
        .with_transport_header(ihl, 8)
        .with_transport_payload_offset(ihl + 8);
    let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.set_packet_cursor(cursor);
}
