use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use hammer_adapter::{BufferPacketCursor, DataPlaneRuntime, Network, RouteMetadata, SocksAddr};
use hammer_core::error::CoreResult;
use hammer_service::data_plane::DropNode;
use hammer_service::transport::tcp::reset::{
    TcpResetObservation, TcpResetObserver, TcpResetReason,
};
use hammer_service::transport::tcp::{
    TcpInputControlPlane, TcpInputError, TcpInputNext, TcpResetNext, TcpResetNode,
};

#[derive(Default)]
struct RecordingTcpResetObserver {
    observations: Mutex<Vec<TcpResetObservation>>,
}

impl TcpResetObserver for RecordingTcpResetObserver {
    fn observe_reset(&self, observation: TcpResetObservation) -> CoreResult<()> {
        self.observations
            .lock()
            .expect("tcp reset observations poisoned")
            .push(observation);
        Ok(())
    }
}

#[test]
fn tcp_reset_observer_records_local_remote_metadata_and_reason() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let observer = Arc::new(RecordingTcpResetObserver::default());
    let reset = runtime.nodes().register_internal(
        TcpResetNode::new(TcpResetNext::nodes(drop))
            .with_observer(Arc::clone(&observer))
            .expect("attach tcp reset observer"),
    );
    let tcp_input = runtime.nodes().register_internal(
        TcpInputControlPlane::new(TcpInputNext::nodes(
            drop, drop, drop, drop, drop, drop, reset,
        ))
        .node(),
    );

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 50_002);
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 20)), 443);
    let packet = ipv4_tcp_packet(
        match remote.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        remote.port(),
        match local.ip() {
            IpAddr::V4(ip) => ip,
            _ => unreachable!(),
        },
        local.port(),
        tcp_flags(false, false, false, true),
        b"ack",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(remote.ip(), remote.port(), local.ip(), local.port()),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);

    assert!(runtime.schedule_frame(tcp_input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        runtime
            .node_error_count(tcp_input, TcpInputError::AckInvalid.code())
            .expect("ack invalid counter"),
        1
    );
    assert_eq!(
        observer
            .observations
            .lock()
            .expect("tcp reset observations poisoned")
            .as_slice(),
        &[TcpResetObservation {
            local,
            remote,
            reason: TcpResetReason::AckInvalid,
        }]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn push_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    metadata: RouteMetadata,
) -> hammer_adapter::BufferIndex {
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

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let network_header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let tcp_offset = network_header_len;
    let tcp_header_len = ((packet[tcp_offset + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(tcp_offset, tcp_header_len)
                .with_transport_payload_offset(tcp_offset + tcp_header_len),
        );
}

fn tcp_metadata(
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(source, source_port)),
        destination: Some(SocksAddr::ip(destination, destination_port)),
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
    payload_len: usize,
) -> Vec<u8> {
    let total_len = 20 + payload_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = protocol;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet
}

fn write_tcp_segment(
    segment: &mut [u8],
    source_port: u16,
    destination_port: u16,
    flags: u8,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = flags;
    segment[20..].copy_from_slice(payload);
}

fn tcp_flags(fin: bool, syn: bool, rst: bool, ack: bool) -> u8 {
    u8::from(fin) | (u8::from(syn) << 1) | (u8::from(rst) << 2) | (u8::from(ack) << 4)
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn update_ipv4_header_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = match chunk {
            [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
            [hi] => u16::from_be_bytes([*hi, 0]) as u32,
            _ => unreachable!(),
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
