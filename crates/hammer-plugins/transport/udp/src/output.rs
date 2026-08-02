use std::mem::transmute;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, NodeId, NodeState, SecondaryOpaque,
};
use hammer_infra::checksum::internet_checksum_parts;
use hammer_runtime::{
    DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData, RuntimeResult,
};
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::node::SessionQueueNode;

const UDP_PROTOCOL: u8 = 17;
const UDP_HEADER_LEN: usize = 8;
const IPV4_HEADER_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const UDP_EGRESS_TAG: u32 = 0x5544_5045; // "UDPE"

#[derive(Clone, Copy)]
#[repr(C)]
struct UdpEgressOpaque {
    tag: u32,
    version: u8,
    pad: [u8; 3],
    local: [u8; 16],
    remote: [u8; 16],
    reserved: [u8; 16],
}

const _: () =
    assert!(std::mem::size_of::<UdpEgressOpaque>() == std::mem::size_of::<SecondaryOpaque>());

#[inline(always)]
pub(crate) fn write_udp_egress_endpoints(
    opaque: &mut SecondaryOpaque,
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
    let egress = unsafe { transmute::<&mut SecondaryOpaque, &mut UdpEgressOpaque>(opaque) };
    *egress = UdpEgressOpaque {
        tag: UDP_EGRESS_TAG,
        version,
        pad: [0; 3],
        local: local_bytes,
        remote: remote_bytes,
        reserved: [0; 16],
    };
}

#[inline(always)]
fn read_udp_egress_endpoints(opaque: &SecondaryOpaque) -> Option<(IpAddr, IpAddr)> {
    let egress = unsafe { *transmute::<&SecondaryOpaque, &UdpEgressOpaque>(opaque) };
    if egress.tag != UDP_EGRESS_TAG {
        return None;
    }
    match egress.version {
        4 => Some((
            IpAddr::V4(Ipv4Addr::new(
                egress.local[0],
                egress.local[1],
                egress.local[2],
                egress.local[3],
            )),
            IpAddr::V4(Ipv4Addr::new(
                egress.remote[0],
                egress.remote[1],
                egress.remote[2],
                egress.remote[3],
            )),
        )),
        6 => Some((
            IpAddr::V6(Ipv6Addr::from(egress.local)),
            IpAddr::V6(Ipv6Addr::from(egress.remote)),
        )),
        _ => None,
    }
}

#[hammer_component_macros::node_next]
pub enum UdpOutputNext {
    Drop,
    #[next("ip-lookup")]
    Lookup,
}

#[hammer_component_macros::graph_node(
    graph = udp_worker,
    init = register_udp_output,
    next = UdpOutputNext,
    role = internal,
)]
#[derive(Clone, Copy)]
pub struct UdpOutputNode;

pub fn register_udp_output(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
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
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        udp_output_process_frame(runtime, frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        udp_output_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn udp_output_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    udp_output_process_frame(runtime, frame)
}

fn udp_output_process_frame(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        udp_output_next_for_index(runtime, index).unwrap_or(UdpOutputNext::Drop)
    })
}

fn udp_output_next_for_index(
    runtime: &DataPlaneRuntime,
    index: Index,
) -> RuntimeResult<UdpOutputNext> {
    let buffer = runtime.get_buffer(index)?;
    let udp_len = buffer
        .current_len()
        .checked_add(buffer.total_len_not_including_first());
    let endpoints = read_udp_egress_endpoints(buffer.opaque2());
    drop(buffer);

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
            Ok(UdpOutputNext::Lookup)
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) => {
            let Some(payload_len) = u16::try_from(udp_len).ok() else {
                return Ok(UdpOutputNext::Drop);
            };
            udp_output_push_ipv6(runtime, index, src, dst, payload_len)?;
            Ok(UdpOutputNext::Lookup)
        }
        _ => Ok(UdpOutputNext::Drop),
    }
}

fn udp_output_push_ipv4(
    runtime: &DataPlaneRuntime,
    index: Index,
    src: Ipv4Addr,
    dst: Ipv4Addr,
    total_len: u16,
) -> RuntimeResult<()> {
    let udp_len =
        u16::try_from(usize::from(total_len) - IPV4_HEADER_LEN).expect("IPv4 UDP length fits u16");
    let checksum = {
        let buffer = runtime.get_buffer(index)?;
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
        let mut buffer = runtime.get_buffer_mut(index)?;
        buffer.current_mut()[6..8].copy_from_slice(&checksum.to_be_bytes());
    }

    let mut buffer = runtime.get_buffer_mut(index)?;
    {
        let header = buffer.prepend_mut(IPV4_HEADER_LEN)?;
        hammer_plugin_ip::write_ipv4_push_header(header, src, dst, UDP_PROTOCOL, total_len)?;
    }
    let packet_len = usize::from(total_len);
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
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
    runtime: &DataPlaneRuntime,
    index: Index,
    src: Ipv6Addr,
    dst: Ipv6Addr,
    payload_len: u16,
) -> RuntimeResult<()> {
    let checksum = {
        let buffer = runtime.get_buffer(index)?;
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
        let mut buffer = runtime.get_buffer_mut(index)?;
        buffer.current_mut()[6..8].copy_from_slice(&checksum.to_be_bytes());
    }

    let mut buffer = runtime.get_buffer_mut(index)?;
    {
        let header = buffer.prepend_mut(IPV6_HEADER_LEN)?;
        hammer_plugin_ip::write_ipv6_push_header(header, src, dst, UDP_PROTOCOL, payload_len)?;
    }
    let packet_len = IPV6_HEADER_LEN + usize::from(payload_len);
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
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
mod tests {
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};

    use super::*;
    use crate::wire::write_udp_header;

    #[test]
    fn udp_output_pushes_ipv4_header_and_valid_checksum() -> RuntimeResult<()> {
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let buffer = runtime.alloc_index_with_bytes(b"hello")?;
        {
            let mut output = runtime.get_buffer_mut(buffer)?;
            let header = output.prepend_mut(UDP_HEADER_LEN)?;
            write_udp_header(header, 9000, 50000, 5).expect("UDP header");
            write_udp_egress_endpoints(
                output.opaque2_mut(),
                Ipv4Addr::LOCALHOST.into(),
                Ipv4Addr::new(192, 0, 2, 1).into(),
            );
        }
        udp_output_push_ipv4(
            &runtime,
            buffer,
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::new(192, 0, 2, 1),
            33,
        )?;

        let packet = runtime.get_buffer(buffer)?.current().to_vec();
        assert_eq!(packet.len(), 33);
        assert_eq!(packet[0], 0x45);
        assert_eq!(packet[9], UDP_PROTOCOL);
        assert_eq!(&packet[20..22], &9000_u16.to_be_bytes());
        assert_eq!(&packet[22..24], &50000_u16.to_be_bytes());
        assert_eq!(&packet[24..26], &13_u16.to_be_bytes());
        assert_eq!(&packet[28..], b"hello");
        assert_eq!(
            internet_checksum_parts(&[
                &Ipv4Addr::LOCALHOST.octets(),
                &Ipv4Addr::new(192, 0, 2, 1).octets(),
                &[0, UDP_PROTOCOL],
                &13_u16.to_be_bytes(),
                &packet[20..],
            ]),
            0
        );
        Ok(())
    }
}
