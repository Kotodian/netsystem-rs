use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use hammer_core::data_plane::{BufferPacketCursor, Frame, NodeId, NodeState};
use hammer_infra::checksum::internet_checksum_parts;
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, RuntimeResult};
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::node::SessionQueueNode;

const UDP_PROTOCOL: u8 = 17;
const UDP_HEADER_LEN: usize = 8;
const IPV4_HEADER_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const UDP_EGRESS_TAG: u32 = 0x5544_5045; // "UDPE"

#[derive(Clone, Copy)]
#[hammer_component_macros::buffer_opaque(secondary)]
#[repr(C)]
pub(crate) struct UdpEgressOpaque {
    tag: u32,
    version: u8,
    pad: [u8; 3],
    local: [u8; 16],
    remote: [u8; 16],
    reserved: [u8; 16],
}

const _: () = assert!(std::mem::size_of::<UdpEgressOpaque>() == 56);

#[inline(always)]
pub(crate) fn write_udp_egress_endpoints(
    opaque: &mut UdpEgressOpaque,
    local: IpAddr,
    remote: IpAddr,
) {
    let (version, local_bytes, remote_bytes) = match (local, remote) {
        (IpAddr::V4(local), IpAddr::V4(remote)) => {
            let mut local_bytes = [0u8; 16];
            let mut remote_bytes = [0u8; 16];
            local_bytes[..4].copy_from_slice(&local.octets());
            remote_bytes[..4].copy_from_slice(&remote.octets());
            (4u8, local_bytes, remote_bytes)
        }
        (IpAddr::V6(local), IpAddr::V6(remote)) => (6u8, local.octets(), remote.octets()),
        _ => return,
    };
    *opaque = UdpEgressOpaque {
        tag: UDP_EGRESS_TAG,
        version,
        pad: [0; 3],
        local: local_bytes,
        remote: remote_bytes,
        reserved: [0; 16],
    };
}

#[inline(always)]
fn read_udp_egress_endpoints(opaque: &UdpEgressOpaque) -> Option<(IpAddr, IpAddr)> {
    if opaque.tag != UDP_EGRESS_TAG {
        return None;
    }
    match opaque.version {
        4 => Some((
            IpAddr::V4(Ipv4Addr::new(
                opaque.local[0],
                opaque.local[1],
                opaque.local[2],
                opaque.local[3],
            )),
            IpAddr::V4(Ipv4Addr::new(
                opaque.remote[0],
                opaque.remote[1],
                opaque.remote[2],
                opaque.remote[3],
            )),
        )),
        6 => Some((
            IpAddr::V6(Ipv6Addr::from(opaque.local)),
            IpAddr::V6(Ipv6Addr::from(opaque.remote)),
        )),
        _ => None,
    }
}

#[hammer_component_macros::node_next]
pub enum UdpOutputNext {
    Drop,
    #[next("ip4-lookup")]
    LookupV4,
    #[next("ip6-lookup")]
    LookupV6,
}

#[hammer_component_macros::graph_node(
    graph = udp_worker,
    init = register_udp_output,
    next = UdpOutputNext,
    role = internal,
)]
#[derive(Clone, Copy)]
pub struct UdpOutputNode;

pub fn register_udp_output(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name(UdpOutputNode::NODE_NAME) {
        return Ok(node);
    }
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(UdpOutputNode::new(), &UdpOutputNext::NEXT_NAMES)?;
    let session_queue = runtime
        .nodes()
        .node_by_name("session-queue")
        .ok_or(hammer_runtime::RuntimeError::PluginStateNotInitialized { plugin: "udp" })?;
    SessionQueueNode::compile_output_next(runtime, session_queue, node)?;
    runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    Ok(node)
}

impl Node for UdpOutputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = udp_output_process;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(NodeRuntime::default())
    }
}

fn udp_output_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    udp_output_process_frame(runtime, node_runtime, frame);
    processed_vectors
}

fn udp_output_process_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
        udp_output_next_for_index(runtime, index).unwrap_or(UdpOutputNext::Drop)
    })
}

fn udp_output_next_for_index(
    runtime: &mut DataPlaneMain,
    index: u32,
) -> RuntimeResult<UdpOutputNext> {
    let buffer = runtime.buffer(index);
    let udp_len = buffer
        .current_len()
        .checked_add(buffer.total_len_not_including_first());
    let endpoints =
        read_udp_egress_endpoints(hammer_core::buffer_opaque!(buffer => UdpEgressOpaque));

    let Some(udp_len) = udp_len else {
        return Ok(UdpOutputNext::Drop);
    };
    let Some((local, remote)) = endpoints else {
        return Ok(UdpOutputNext::Drop);
    };

    match (local, remote) {
        (IpAddr::V4(src), IpAddr::V4(dst)) => {
            let Some(total_len) = udp_len
                .checked_add(IPV4_HEADER_LEN)
                .and_then(|length| u16::try_from(length).ok())
            else {
                return Ok(UdpOutputNext::Drop);
            };
            udp_output_push_ipv4(runtime, index, src, dst, total_len)?;
            Ok(UdpOutputNext::LookupV4)
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) => {
            let Some(payload_len) = u16::try_from(udp_len).ok() else {
                return Ok(UdpOutputNext::Drop);
            };
            udp_output_push_ipv6(runtime, index, src, dst, payload_len)?;
            Ok(UdpOutputNext::LookupV6)
        }
        _ => Ok(UdpOutputNext::Drop),
    }
}

fn udp_output_push_ipv4(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv4Addr,
    dst: Ipv4Addr,
    total_len: u16,
) -> RuntimeResult<()> {
    let udp_len =
        u16::try_from(usize::from(total_len) - IPV4_HEADER_LEN).expect("IPv4 UDP length fits u16");
    let checksum = {
        let buffer = runtime.buffer(index);
        let datagram = buffer.current();
        internet_checksum_parts(&[
            &src.octets(),
            &dst.octets(),
            &[0, UDP_PROTOCOL],
            &udp_len.to_be_bytes(),
            datagram,
        ])
    };
    {
        let buffer = runtime.buffer_mut(index);
        buffer.current_mut()[6..8].copy_from_slice(&checksum.to_be_bytes());
    }

    let buffer = runtime.buffer_mut(index);
    {
        let header = buffer.push_uninit(IPV4_HEADER_LEN as u8);
        hammer_plugin_ip::write_ipv4_push_header(header, src, dst, UDP_PROTOCOL, total_len, true)?;
    }
    let packet_len = usize::from(total_len);
    let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
    network.sw_if_index = [u32::MAX; 2];
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, IPV4_HEADER_LEN)
            .with_transport_header(IPV4_HEADER_LEN, UDP_HEADER_LEN)
            .with_transport_payload_offset(IPV4_HEADER_LEN + UDP_HEADER_LEN),
    );
    network.ip_mut().set_ip_version(Some(4));
    network.ip_mut().set_ip_protocol(Some(UDP_PROTOCOL));
    Ok(())
}

fn udp_output_push_ipv6(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv6Addr,
    dst: Ipv6Addr,
    payload_len: u16,
) -> RuntimeResult<()> {
    let checksum = {
        let buffer = runtime.buffer(index);
        let datagram = buffer.current();
        internet_checksum_parts(&[
            &src.octets(),
            &dst.octets(),
            &u32::from(payload_len).to_be_bytes(),
            &[0, 0, 0, UDP_PROTOCOL],
            datagram,
        ])
    };
    {
        let buffer = runtime.buffer_mut(index);
        buffer.current_mut()[6..8].copy_from_slice(&checksum.to_be_bytes());
    }

    let buffer = runtime.buffer_mut(index);
    {
        let header = buffer.push_uninit(IPV6_HEADER_LEN as u8);
        hammer_plugin_ip::write_ipv6_push_header(header, src, dst, UDP_PROTOCOL, payload_len)?;
    }
    let packet_len = IPV6_HEADER_LEN + usize::from(payload_len);
    let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
    network.sw_if_index = [u32::MAX; 2];
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, IPV6_HEADER_LEN)
            .with_transport_header(IPV6_HEADER_LEN, UDP_HEADER_LEN)
            .with_transport_payload_offset(IPV6_HEADER_LEN + UDP_HEADER_LEN),
    );
    network.ip_mut().set_ip_version(Some(6));
    network.ip_mut().set_ip_protocol(Some(UDP_PROTOCOL));
    Ok(())
}

#[cfg(test)]
mod opaque_tests {
    use super::*;

    // Derived from vnet/udp/udp_output.c: output consumes transport endpoint
    // facts, prepends IP, and publishes primary metadata for IP lookup.
    #[test]
    fn datagram_metadata_reaches_ip_output() -> RuntimeResult<()> {
        hammer_infra::main_heap::init_default().unwrap();
        hammer_core::buffer::BufferMain::new(2048, 16, &[0], 1, hammer_infra::PageSize::Default)?;
        let mut runtime = DataPlaneMain::new(hammer_runtime::DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 16,
            ..Default::default()
        });
        let mut index = u32::MAX;
        assert_eq!(
            runtime.buffer_add_data(&mut index, &[0x04, 0xd2, 0x10, 0xe1, 0, 8, 0, 0]),
            (&[0x04, 0xd2, 0x10, 0xe1, 0, 8, 0, 0]).len()
        );
        write_udp_egress_endpoints(
            hammer_core::buffer_opaque!(mut runtime.buffer_mut(index) => UdpEgressOpaque),
            "192.0.2.1".parse().unwrap(),
            "192.0.2.2".parse().unwrap(),
        );
        assert!(matches!(
            udp_output_next_for_index(&mut runtime, index)?,
            UdpOutputNext::LookupV4
        ));
        {
            let buffer = runtime.buffer(index);
            assert_eq!(&buffer.current()[12..16], &[192, 0, 2, 1]);
            assert_eq!(&buffer.current()[16..20], &[192, 0, 2, 2]);
            let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
            assert_eq!(network.ip().ip_protocol(), Some(17));
            assert_eq!(network.packet_cursor().transport_header_offset(), 20);
        }
        runtime.buffer_free_one(index);
        Ok(())
    }
}
