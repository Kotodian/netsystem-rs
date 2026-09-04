use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;

use hammer_core::data_plane::{BufferFrame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntimeData, RuntimeResult};
use hammer_service::net::{DpoId, DpoProto, DpoType};
use hammer_service::opaque::NetworkOpaque;

use crate::fib::{Ip4FibTable, Ip6FibTable};
use crate::ip::ip_header;
use crate::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};

#[derive(Clone)]
pub struct Ip4Main {
    unicast_tables: Vec<Ip4FibTable>,
    fib_index_by_sw_if_index: Vec<u32>,
}

impl Ip4Main {
    pub fn new() -> Self {
        Self {
            unicast_tables: vec![Ip4FibTable::new(Default::default())],
            fib_index_by_sw_if_index: vec![0],
        }
    }

    #[inline(always)]
    pub fn fib_index(&self, sw_if_index: u32) -> Option<u32> {
        self.fib_index_by_sw_if_index
            .get(sw_if_index as usize)
            .copied()
    }

    #[inline(always)]
    pub fn forwarding_dpo(&self, fib_index: u32, address: Ipv4Addr) -> Option<DpoId> {
        self.unicast_tables
            .get(fib_index as usize)
            .and_then(|table| table.forwarding_lookup(address))
    }
}

#[derive(Clone)]
pub struct Ip6Main {
    unicast_tables: Vec<Ip6FibTable>,
    fib_index_by_sw_if_index: Vec<u32>,
}

impl Ip6Main {
    pub fn new() -> Self {
        Self {
            unicast_tables: vec![Ip6FibTable::new(Default::default())],
            fib_index_by_sw_if_index: vec![0],
        }
    }

    #[inline(always)]
    pub fn fib_index(&self, sw_if_index: u32) -> Option<u32> {
        self.fib_index_by_sw_if_index
            .get(sw_if_index as usize)
            .copied()
    }

    #[inline(always)]
    pub fn forwarding_dpo(&self, fib_index: u32, address: Ipv6Addr) -> Option<DpoId> {
        self.unicast_tables
            .get(fib_index as usize)
            .and_then(|table| table.forwarding_lookup(address))
    }
}

pub static IP4_MAIN: OnceLock<Ip4Main> = OnceLock::new();
pub static IP6_MAIN: OnceLock<Ip6Main> = OnceLock::new();

#[inline(always)]
pub(crate) fn fib_index_for(version: IpVersion, sw_if_index: u32) -> Option<u32> {
    match version {
        IpVersion::V4 => IP4_MAIN.get().and_then(|main| main.fib_index(sw_if_index)),
        IpVersion::V6 => IP6_MAIN.get().and_then(|main| main.fib_index(sw_if_index)),
    }
}

#[hammer_component_macros::init_function(
    name = "ip_lookup_init",
    runs_before = ["install_packet_graph"]
)]
fn init_lookup() -> RuntimeResult<()> {
    IP4_MAIN
        .set(Ip4Main::new())
        .map_err(|_| hammer_runtime::RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    IP6_MAIN
        .set(Ip6Main::new())
        .map_err(|_| hammer_runtime::RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    Ok(())
}

#[hammer_component_macros::node_next]
enum IpLookupNext {
    #[next("drop")]
    Drop,
    #[next("ip4-load-balance")]
    LoadBalanceV4,
    #[next("ip6-load-balance")]
    LoadBalanceV6,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct LookupMetadata {
    fib_index: u32,
    forwarding: DpoId,
    flow_hash: u32,
}

impl Default for LookupMetadata {
    fn default() -> Self {
        Self {
            fib_index: u32::MAX,
            forwarding: DpoId::INVALID,
            flow_hash: 0,
        }
    }
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_lookup,
    role = internal,
    name = "ip4-lookup",
    next = IpLookupNext,
)]
pub struct Ip4LookupNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_lookup,
    role = internal,
    name = "ip6-lookup",
    next = IpLookupNext,
)]
pub struct Ip6LookupNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_load_balance,
    role = internal,
    name = "ip4-load-balance",
    sibling_of = Ip4LookupNode,
)]
pub struct Ip4LoadBalanceNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_load_balance,
    role = internal,
    name = "ip6-load-balance",
    sibling_of = Ip6LookupNode,
)]
pub struct Ip6LoadBalanceNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

fn register_ip4_lookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4LookupNode::new(), &IpLookupNext::NEXT_NAMES)
}

fn register_ip6_lookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6LookupNode::new(), &IpLookupNext::NEXT_NAMES)
}

fn register_ip4_load_balance(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal(Ip4LoadBalanceNode::new())
}

fn register_ip6_load_balance(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal(Ip6LoadBalanceNode::new())
}

impl Node for Ip4LookupNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_lookup_frame(runtime, frame, IpVersion::V4)
    }

    fn node_process(&self) -> NodeProcessFn {
        process_lookup_v4
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip6LookupNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_lookup_frame(runtime, frame, IpVersion::V6)
    }

    fn node_process(&self) -> NodeProcessFn {
        process_lookup_v6
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip4LoadBalanceNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_load_balance_frame(runtime, frame, IpVersion::V4)
    }

    fn node_process(&self) -> NodeProcessFn {
        process_load_balance_v4
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip6LoadBalanceNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        process_load_balance_frame(runtime, frame, IpVersion::V6)
    }

    fn node_process(&self) -> NodeProcessFn {
        process_load_balance_v6
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

fn process_lookup_frame(runtime: &DataPlaneMain, frame: &mut BufferFrame, version: IpVersion) {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        lookup_index(runtime, index, version)
    })
}

const IP_FLOW_HASH_SRC_ADDR: u16 = 1 << 0;
const IP_FLOW_HASH_DST_ADDR: u16 = 1 << 1;
const IP_FLOW_HASH_SRC_PORT: u16 = 1 << 2;
const IP_FLOW_HASH_DST_PORT: u16 = 1 << 3;
const IP_FLOW_HASH_PROTO: u16 = 1 << 4;
const IP_FLOW_HASH_REVERSE_SRC_DST: u16 = 1 << 5;
const IP_FLOW_HASH_SYMMETRIC: u16 = 1 << 6;
const IP_FLOW_HASH_FLOW_LABEL: u16 = 1 << 7;
const IP_FLOW_HASH_GTPV1_TEID: u16 = 1 << 8;
const GTPV1_PORT_BE: u16 = 2152u16.to_be();

#[inline(always)]
fn hash_v3_mix32(mut a: u32, mut b: u32, mut c: u32) -> u32 {
    a = a.wrapping_sub(c) ^ c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a) ^ a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b) ^ b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c) ^ c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a) ^ a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b) ^ b.rotate_left(4);
    b = b.wrapping_add(a);

    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    c
}

#[inline(always)]
fn hash_mix64(mut a: u64, mut b: u64, mut c: u64) -> u64 {
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 43);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 9);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 8);
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 38);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 23);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 5);
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 35);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 49);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 11);
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 12);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 18);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 22);
    c
}

#[inline(always)]
fn transport_ports(packet: &[u8], offset: usize, protocol: IpProtocol) -> (u16, u16) {
    if !matches!(protocol, IpProtocol::Tcp | IpProtocol::Udp) {
        return (0, 0);
    }
    let Some(bytes) = packet.get(offset..offset.saturating_add(4)) else {
        return (0, 0);
    };
    (
        u16::from_ne_bytes([bytes[0], bytes[1]]),
        u16::from_ne_bytes([bytes[2], bytes[3]]),
    )
}

#[inline(always)]
fn ip4_flow_hash(packet: &[u8], parsed: ParsedIpPacket, config: u16) -> u32 {
    let (source, destination) = match (parsed.source, parsed.destination) {
        (std::net::IpAddr::V4(source), std::net::IpAddr::V4(destination)) => (
            u32::from_ne_bytes(source.octets()),
            u32::from_ne_bytes(destination.octets()),
        ),
        _ => return 0,
    };
    let (source_port, destination_port) =
        transport_ports(packet, parsed.transport_header_offset, parsed.protocol);
    let transport_destination_port = destination_port;
    let gtp_teid =
        if config & IP_FLOW_HASH_GTPV1_TEID != 0 && transport_destination_port == GTPV1_PORT_BE {
            packet
                .get(
                    parsed.transport_header_offset.saturating_add(8)
                        ..parsed.transport_header_offset.saturating_add(12),
                )
                .map(|bytes| u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                .unwrap_or(0)
        } else {
            0
        };
    let source = if config & IP_FLOW_HASH_SRC_ADDR != 0 {
        source
    } else {
        0
    };
    let destination = if config & IP_FLOW_HASH_DST_ADDR != 0 {
        destination
    } else {
        0
    };
    let mut a = source;
    let mut b = destination;
    let mut source_port = if config & IP_FLOW_HASH_SRC_PORT != 0 {
        source_port
    } else {
        0
    };
    let mut destination_port = if config & IP_FLOW_HASH_DST_PORT != 0 {
        destination_port
    } else {
        0
    };
    if config & IP_FLOW_HASH_REVERSE_SRC_DST != 0 {
        (a, b) = (b, a);
        (source_port, destination_port) = (destination_port, source_port);
    }
    if config & IP_FLOW_HASH_SYMMETRIC != 0 {
        if b < a {
            (a, b) = (b, a);
        }
        if destination_port < source_port {
            (source_port, destination_port) = (destination_port, source_port);
        }
    }
    if config & IP_FLOW_HASH_PROTO != 0 {
        b ^= protocol_number(parsed.protocol);
    }
    let c = (u32::from(destination_port) << 16) | u32::from(source_port);
    a ^= gtp_teid;
    hash_v3_mix32(a, b, c)
}

#[inline(always)]
fn ip6_flow_hash(packet: &[u8], parsed: ParsedIpPacket, config: u16) -> u32 {
    let (source, destination) = match (parsed.source, parsed.destination) {
        (std::net::IpAddr::V6(source), std::net::IpAddr::V6(destination)) => (source, destination),
        _ => return 0,
    };
    let source = source.octets();
    let destination = destination.octets();
    let (source_port, destination_port) =
        transport_ports(packet, parsed.transport_header_offset, parsed.protocol);
    let transport_destination_port = destination_port;
    let source = if config & IP_FLOW_HASH_SRC_ADDR != 0 {
        u64::from_ne_bytes(source[..8].try_into().unwrap())
            ^ u64::from_ne_bytes(source[8..].try_into().unwrap())
    } else {
        0
    };
    let destination = if config & IP_FLOW_HASH_DST_ADDR != 0 {
        u64::from_ne_bytes(destination[..8].try_into().unwrap())
            ^ u64::from_ne_bytes(destination[8..].try_into().unwrap())
    } else {
        0
    };
    let mut a = source;
    let mut b = destination;
    let mut source_port = if config & IP_FLOW_HASH_SRC_PORT != 0 {
        source_port
    } else {
        0
    };
    let mut destination_port = if config & IP_FLOW_HASH_DST_PORT != 0 {
        destination_port
    } else {
        0
    };
    if config & IP_FLOW_HASH_REVERSE_SRC_DST != 0 {
        (a, b) = (b, a);
        (source_port, destination_port) = (destination_port, source_port);
    }
    if config & IP_FLOW_HASH_SYMMETRIC != 0 {
        if b < a {
            (a, b) = (b, a);
        }
        if destination_port < source_port {
            (source_port, destination_port) = (destination_port, source_port);
        }
    }
    if config & IP_FLOW_HASH_PROTO != 0 {
        b ^= u64::from(protocol_number(parsed.protocol));
    }
    let mut c = (u64::from(destination_port) << 16) | u64::from(source_port);
    if config & IP_FLOW_HASH_FLOW_LABEL != 0 {
        let offset = parsed.network_header_offset;
        if let Some(bytes) = packet.get(offset..offset.saturating_add(4)) {
            let word = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            c ^= u64::from(word & 0x000f_ffff);
        }
    }
    if config & IP_FLOW_HASH_GTPV1_TEID != 0
        && transport_destination_port == GTPV1_PORT_BE
        && let Some(bytes) = packet.get(
            parsed.transport_header_offset.saturating_add(8)
                ..parsed.transport_header_offset.saturating_add(12),
        )
    {
        a ^= u64::from(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    c = hash_mix64(a, b, c);
    c as u32
}

#[inline(always)]
fn protocol_number(protocol: IpProtocol) -> u32 {
    match protocol {
        IpProtocol::Icmpv4 => 1,
        IpProtocol::Tcp => 6,
        IpProtocol::Udp => 17,
        IpProtocol::Icmpv6 => 58,
        IpProtocol::Other(value) => u32::from(value),
    }
}

fn process_lookup_v4(runtime: &DataPlaneMain, _data: NodeRuntimeData, frame: &mut BufferFrame) {
    process_lookup_frame(runtime, frame, IpVersion::V4)
}

fn process_load_balance_v4(
    runtime: &DataPlaneMain,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) {
    process_load_balance_frame(runtime, frame, IpVersion::V4)
}

fn process_load_balance_v6(
    runtime: &DataPlaneMain,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) {
    process_load_balance_frame(runtime, frame, IpVersion::V6)
}

fn process_load_balance_frame(
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    version: IpVersion,
) {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        load_balance_index(runtime, index, version)
    })
}

#[inline(always)]
fn load_balance_index(
    runtime: &DataPlaneMain,
    index: hammer_core::data_plane::Index,
    version: IpVersion,
) -> u16 {
    let drop_next = NodeNext::slot(IpLookupNext::Drop);
    let Ok(mut buffer) = runtime.get_buffer_mut(index) else {
        return drop_next;
    };
    let opaque = unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) };
    let metadata = unsafe { &mut *(buffer.opaque2_mut() as *mut _ as *mut LookupMetadata) };
    let current = metadata.forwarding;
    let expected_proto = match version {
        IpVersion::V4 => DpoProto::IP4,
        IpVersion::V6 => DpoProto::IP6,
    };
    if current.class() != DpoType::LOAD_BALANCE || current.proto() != expected_proto {
        return drop_next;
    }
    let Some(load_balance) = hammer_service::net::NetMain::global()
        .ok()
        .and_then(|net| net.dpo_main().load_balance(current.index()))
    else {
        return drop_next;
    };
    let parsed = ip_header(buffer.current(), opaque.packet_cursor()).ok();
    let hash = if load_balance.bucket_count <= 1 {
        0
    } else if metadata.flow_hash != 0 {
        metadata.flow_hash >> 1
    } else {
        let Some(parsed) = parsed.filter(|packet| packet.version == version) else {
            return drop_next;
        };
        match version {
            IpVersion::V4 => ip4_flow_hash(buffer.current(), parsed, load_balance.flow_hash_config),
            IpVersion::V6 => ip6_flow_hash(buffer.current(), parsed, load_balance.flow_hash_config),
        }
    };
    if load_balance.bucket_count > 1 {
        metadata.flow_hash = hash;
    }
    let Some(selected) = hammer_service::net::NetMain::global()
        .ok()
        .and_then(|net| net.dpo_main().select_load_balance(current, hash))
    else {
        return drop_next;
    };
    metadata.forwarding = selected;
    if selected.class() == DpoType::LOAD_BALANCE {
        match version {
            IpVersion::V4 => NodeNext::slot(IpLookupNext::LoadBalanceV4),
            IpVersion::V6 => NodeNext::slot(IpLookupNext::LoadBalanceV6),
        }
    } else {
        selected.next()
    }
}

fn process_lookup_v6(runtime: &DataPlaneMain, _data: NodeRuntimeData, frame: &mut BufferFrame) {
    process_lookup_frame(runtime, frame, IpVersion::V6)
}

#[inline(always)]
fn lookup_index(
    runtime: &DataPlaneMain,
    index: hammer_core::data_plane::Index,
    version: IpVersion,
) -> u16 {
    let drop_next = NodeNext::slot(IpLookupNext::Drop);
    let Ok(mut buffer) = runtime.get_buffer_mut(index) else {
        return drop_next;
    };
    let opaque = unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) };
    let fib_index = opaque
        .ip()
        .fib_index_override()
        .or_else(|| opaque.ip().fib_index())
        .or_else(|| match version {
            IpVersion::V4 => IP4_MAIN
                .get()
                .and_then(|main| main.fib_index(opaque.sw_if_index[0])),
            IpVersion::V6 => IP6_MAIN
                .get()
                .and_then(|main| main.fib_index(opaque.sw_if_index[0])),
        });
    let Some(fib_index) = fib_index else {
        return drop_next;
    };
    let forwarding = match ip_header(buffer.current(), opaque.packet_cursor()) {
        Ok(packet) if packet.version == version => match (version, packet.destination) {
            (IpVersion::V4, std::net::IpAddr::V4(address)) => IP4_MAIN
                .get()
                .and_then(|main| main.forwarding_dpo(fib_index, address)),
            (IpVersion::V6, std::net::IpAddr::V6(address)) => IP6_MAIN
                .get()
                .and_then(|main| main.forwarding_dpo(fib_index, address)),
            _ => None,
        },
        _ => None,
    };
    let metadata = unsafe { &mut *(buffer.opaque2_mut() as *mut _ as *mut LookupMetadata) };
    *metadata = LookupMetadata {
        fib_index,
        forwarding: forwarding.unwrap_or(DpoId::INVALID),
        flow_hash: 0,
    };
    match forwarding {
        Some(dpo) if dpo.class() == DpoType::LOAD_BALANCE => match version {
            IpVersion::V4 => NodeNext::slot(IpLookupNext::LoadBalanceV4),
            IpVersion::V6 => NodeNext::slot(IpLookupNext::LoadBalanceV6),
        },
        Some(dpo) => dpo.next(),
        None => drop_next,
    }
}
