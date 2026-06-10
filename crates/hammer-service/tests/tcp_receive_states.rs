use std::net::{IpAddr, Ipv4Addr};

use hammer_adapter::{
    BufferIndex, BufferPacketCursor, DataPlaneRuntime, DataWorkerId, Network, RouteMetadata,
    SocksAddr,
};
use hammer_service::data_plane::DropNode;
use hammer_service::transport::tcp::{
    TcpConnectionSnapshot, TcpEstablishedControlPlane, TcpEstablishedNext, TcpInputControlPlane,
    TcpInputNext, TcpRcvProcessControlPlane, TcpRcvProcessNext, TcpState, TcpV4ConnectionKey,
    TcpWorkerOwnedConnectionState, TcpWorkerOwnedState,
};

const LOOKUP_ID: u32 = 0x4401;
const LISTEN_PORT: u16 = 7443;

#[test]
fn tcp_remote_fin_transitions_established_flow_to_close_wait_once() {
    let result = run_fin_delivery_case(TcpState::Established, 41);

    assert_eq!(result.first_run_count, 4);
    assert_eq!(result.second_run_count, 4);
    assert_eq!(result.state_after_first, Some(TcpState::CloseWait));
    assert_eq!(result.state_after_second, Some(TcpState::CloseWait));
    assert_snapshot_progress(
        result
            .snapshot_after_first
            .expect("snapshot after first FIN"),
        TcpState::CloseWait,
        0x0102_0305,
    );
    assert_snapshot_progress(
        result
            .snapshot_after_second
            .expect("snapshot after second FIN"),
        TcpState::CloseWait,
        0x0102_0305,
    );
}

#[test]
fn tcp_receive_ack_advances_fin_wait1_to_fin_wait2_without_app_delivery() {
    let result = run_ack_only_case(TcpState::FinWait1, 42);

    assert_eq!(result.run_count, 4);
    assert_eq!(result.state_after_packet, Some(TcpState::FinWait2));
    assert_snapshot_progress(
        result.snapshot_after_packet.expect("snapshot after ACK"),
        TcpState::FinWait2,
        0x0102_0304,
    );
}

#[test]
fn tcp_remote_fin_with_partial_ack_transitions_fin_wait1_to_closing_once() {
    let octet = 43;
    let snapshot = connected_snapshot(
        TcpState::FinWait1,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
    );
    let packet = tcp_packet_with_seq_ack_flags(
        octet,
        0x0102_0304,
        0x1020_3044,
        tcp_flags(true, false, false, true),
    );
    let result = run_snapshot_packet_case(snapshot, packet, octet);

    assert_eq!(result.first_run_count, 4);
    assert_eq!(result.second_run_count, 4);
    assert_eq!(result.state_after_first, Some(TcpState::Closing));
    assert_eq!(result.state_after_second, Some(TcpState::Closing));
    assert_snapshot_progress_with_ack(
        result
            .snapshot_after_first
            .expect("snapshot after first FIN"),
        TcpState::Closing,
        0x1020_3044,
        0x1020_3048,
        0x0102_0305,
    );
    assert_snapshot_progress_with_ack(
        result
            .snapshot_after_second
            .expect("snapshot after second FIN"),
        TcpState::Closing,
        0x1020_3044,
        0x1020_3048,
        0x0102_0305,
    );
}

#[test]
fn tcp_remote_fin_transitions_fin_wait2_to_time_wait_once() {
    let result = run_fin_delivery_case(TcpState::FinWait2, 41);

    assert_eq!(result.first_run_count, 4);
    assert_eq!(result.second_run_count, 4);
    assert_eq!(result.state_after_first, Some(TcpState::TimeWait));
    assert_eq!(result.state_after_second, Some(TcpState::TimeWait));
    assert_snapshot_progress(
        result
            .snapshot_after_first
            .expect("snapshot after first FIN"),
        TcpState::TimeWait,
        0x0102_0305,
    );
    assert_snapshot_progress(
        result
            .snapshot_after_second
            .expect("snapshot after second FIN"),
        TcpState::TimeWait,
        0x0102_0305,
    );
}

#[test]
fn tcp_out_of_order_fin_does_not_transition_established_flow() {
    let octet = 44;
    let snapshot = connected_snapshot(
        TcpState::Established,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
    );
    let packet = tcp_packet_with_seq_ack_flags(
        octet,
        0x0102_0308,
        0x1020_3040,
        tcp_flags(true, false, false, true),
    );
    let result = run_snapshot_packet_case(snapshot, packet, octet);

    assert_eq!(result.first_run_count, 4);
    assert_eq!(result.second_run_count, 4);
    assert_eq!(result.state_after_first, Some(TcpState::Established));
    assert_eq!(result.state_after_second, Some(TcpState::Established));
    assert_snapshot_progress_with_ack(
        result
            .snapshot_after_first
            .expect("snapshot after first out-of-order FIN"),
        TcpState::Established,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
    );
    assert_snapshot_progress_with_ack(
        result
            .snapshot_after_second
            .expect("snapshot after second out-of-order FIN"),
        TcpState::Established,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
    );
}

struct FinDeliveryResult {
    state_after_first: Option<TcpState>,
    state_after_second: Option<TcpState>,
    snapshot_after_first: Option<TcpConnectionSnapshot>,
    snapshot_after_second: Option<TcpConnectionSnapshot>,
    first_run_count: usize,
    second_run_count: usize,
}

fn run_fin_delivery_case(initial_state: TcpState, octet: u8) -> FinDeliveryResult {
    run_snapshot_packet_case(
        TcpConnectionSnapshot::with_default_windows(
            LOOKUP_ID,
            None,
            DataWorkerId::new(0),
            initial_state,
            LISTEN_PORT,
            Some(
                ("192.0.2.".to_owned() + &octet.to_string() + ":7443")
                    .parse()
                    .expect("local"),
            ),
            ("198.51.100.".to_owned()
                + &octet.to_string()
                + ":"
                + &(40_000 + u16::from(octet)).to_string())
                .parse()
                .expect("remote"),
        ),
        fin_packet(octet),
        octet,
    )
}

fn run_snapshot_packet_case(
    snapshot: TcpConnectionSnapshot,
    packet: Vec<u8>,
    octet: u8,
) -> FinDeliveryResult {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let tcp_rcv_process = runtime
        .nodes()
        .register_internal(TcpRcvProcessControlPlane::new(TcpRcvProcessNext::nodes(drop)).node());
    let established = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(tcp_rcv_process));
    let connections = published_connections(snapshot);
    established
        .publish_connections(connections.publish_snapshot())
        .expect("publish established connections");
    let established_node = runtime.nodes().register_internal(established.node());
    let tcp_input = tcp_input_node(
        &runtime,
        drop,
        tcp_rcv_process,
        established_node,
        connections,
        octet,
    );

    let first_run_count = schedule_tcp_packet(&runtime, tcp_input, &packet, octet);
    let state_after_first = established.connection_state_for_test(LOOKUP_ID);
    let snapshot_after_first = established.connection_snapshot_for_test(LOOKUP_ID);

    let second_run_count = schedule_tcp_packet(&runtime, tcp_input, &packet, octet);
    let state_after_second = established.connection_state_for_test(LOOKUP_ID);
    let snapshot_after_second = established.connection_snapshot_for_test(LOOKUP_ID);

    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);

    FinDeliveryResult {
        state_after_first,
        state_after_second,
        snapshot_after_first,
        snapshot_after_second,
        first_run_count,
        second_run_count,
    }
}

struct AckOnlyResult {
    state_after_packet: Option<TcpState>,
    snapshot_after_packet: Option<TcpConnectionSnapshot>,
    run_count: usize,
}

fn run_ack_only_case(initial_state: TcpState, octet: u8) -> AckOnlyResult {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let tcp_rcv_process = runtime
        .nodes()
        .register_internal(TcpRcvProcessControlPlane::new(TcpRcvProcessNext::nodes(drop)).node());
    let established = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(tcp_rcv_process));
    let connections = published_connections(TcpConnectionSnapshot::with_default_windows(
        LOOKUP_ID,
        None,
        DataWorkerId::new(0),
        initial_state,
        LISTEN_PORT,
        Some(
            ("192.0.2.".to_owned() + &octet.to_string() + ":7443")
                .parse()
                .expect("local"),
        ),
        ("198.51.100.".to_owned()
            + &octet.to_string()
            + ":"
            + &(40_000 + u16::from(octet)).to_string())
            .parse()
            .expect("remote"),
    ));
    established
        .publish_connections(connections.publish_snapshot())
        .expect("publish established connections");
    let established_node = runtime.nodes().register_internal(established.node());
    let tcp_input = tcp_input_node(
        &runtime,
        drop,
        tcp_rcv_process,
        established_node,
        connections,
        octet,
    );
    let packet = ack_only_packet(octet);

    let run_count = schedule_tcp_packet(&runtime, tcp_input, &packet, octet);

    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);

    AckOnlyResult {
        state_after_packet: established.connection_state_for_test(LOOKUP_ID),
        snapshot_after_packet: established.connection_snapshot_for_test(LOOKUP_ID),
        run_count,
    }
}

fn tcp_input_node(
    runtime: &DataPlaneRuntime,
    drop: hammer_adapter::NodeId,
    tcp_rcv_process: hammer_adapter::NodeId,
    established: hammer_adapter::NodeId,
    connections: TcpWorkerOwnedConnectionState,
    octet: u8,
) -> hammer_adapter::NodeId {
    let tcp_input = TcpInputControlPlane::new(TcpInputNext::nodes(
        drop,
        drop,
        drop,
        tcp_rcv_process,
        drop,
        established,
        drop,
    ));
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(0));
    owner.insert_connection_v4(
        TcpV4ConnectionKey::new(
            0,
            Ipv4Addr::new(192, 0, 2, octet),
            LISTEN_PORT,
            Ipv4Addr::new(198, 51, 100, octet),
            40_000 + u16::from(octet),
        ),
        LOOKUP_ID,
    );
    tcp_input
        .publish_lookup(owner.publish_snapshot())
        .expect("publish input lookup");
    tcp_input
        .publish_connections(connections.publish_snapshot())
        .expect("publish input connections");
    tcp_input
        .publish_app_ingress([LOOKUP_ID])
        .expect("publish input app ingress");
    runtime.nodes().register_internal(tcp_input.node())
}

fn published_connections(snapshot: TcpConnectionSnapshot) -> TcpWorkerOwnedConnectionState {
    let mut connections = TcpWorkerOwnedConnectionState::new(DataWorkerId::new(0));
    connections.insert(snapshot);
    connections
}

fn schedule_tcp_packet(
    runtime: &DataPlaneRuntime,
    tcp_input: hammer_adapter::NodeId,
    packet: &[u8],
    octet: u8,
) -> usize {
    let metadata = tcp_metadata(
        Ipv4Addr::new(198, 51, 100, octet).into(),
        40_000 + u16::from(octet),
        Ipv4Addr::new(192, 0, 2, octet).into(),
        LISTEN_PORT,
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(runtime, frame, packet, metadata);
    stamp_tcp_cursor(runtime, buffer, packet);
    assert!(runtime.schedule_frame(tcp_input, frame).expect("schedule"));
    runtime.run_ready_nodes().expect("run nodes")
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

fn fin_packet(octet: u8) -> Vec<u8> {
    ipv4_tcp_packet(
        Ipv4Addr::new(198, 51, 100, octet),
        40_000 + u16::from(octet),
        Ipv4Addr::new(192, 0, 2, octet),
        LISTEN_PORT,
        tcp_flags(true, false, false, true),
        b"",
    )
}

fn ack_only_packet(octet: u8) -> Vec<u8> {
    ipv4_tcp_packet(
        Ipv4Addr::new(198, 51, 100, octet),
        40_000 + u16::from(octet),
        Ipv4Addr::new(192, 0, 2, octet),
        LISTEN_PORT,
        tcp_flags(false, false, false, true),
        b"",
    )
}

fn tcp_packet_with_seq_ack_flags(
    octet: u8,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
) -> Vec<u8> {
    let source = Ipv4Addr::new(198, 51, 100, octet);
    let destination = Ipv4Addr::new(192, 0, 2, octet);
    let source_port = 40_000 + u16::from(octet);
    let mut packet = ipv4_packet(source, destination, 6, 20);
    write_tcp_segment_with_seq_ack(
        &mut packet[20..],
        source_port,
        LISTEN_PORT,
        sequence,
        acknowledgment,
        flags,
        &[],
    );
    let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
    packet[20 + 16..20 + 18].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
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
    packet[20 + 16..20 + 18].copy_from_slice(&checksum.to_be_bytes());
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
    packet[6..8].copy_from_slice(&0x4000u16.to_be_bytes());
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
    write_tcp_segment_with_seq_ack(
        segment,
        source_port,
        destination_port,
        0x0102_0304,
        0x1020_3040,
        flags,
        payload,
    );
}

fn write_tcp_segment_with_seq_ack(
    segment: &mut [u8],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[4..8].copy_from_slice(&sequence.to_be_bytes());
    segment[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = flags;
    segment[14..16].copy_from_slice(&0x4000u16.to_be_bytes());
    segment[20..].copy_from_slice(payload);
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

fn tcp_flags(fin: bool, syn: bool, rst: bool, ack: bool) -> u8 {
    u8::from(fin) | (u8::from(syn) << 1) | (u8::from(rst) << 2) | (u8::from(ack) << 4)
}

fn assert_snapshot_progress(
    snapshot: TcpConnectionSnapshot,
    expected_state: TcpState,
    expected_rcv_nxt: u32,
) {
    assert_eq!(snapshot.state, expected_state);
    assert_eq!(snapshot.iss, 0x1020_303f);
    assert_eq!(snapshot.irs, 0x0102_0303);
    assert_eq!(snapshot.snd_una, 0x1020_3040);
    assert_eq!(snapshot.snd_nxt, 0x1020_3040);
    assert_eq!(snapshot.snd_wnd, 0x4000);
    assert_eq!(snapshot.rcv_nxt, expected_rcv_nxt);
    assert_eq!(snapshot.rcv_wnd, u16::MAX as u32);
}

fn assert_snapshot_progress_with_ack(
    snapshot: TcpConnectionSnapshot,
    expected_state: TcpState,
    expected_snd_una: u32,
    expected_snd_nxt: u32,
    expected_rcv_nxt: u32,
) {
    assert_eq!(snapshot.state, expected_state);
    assert_eq!(snapshot.iss, 0x1020_303f);
    assert_eq!(snapshot.irs, 0x0102_0303);
    assert_eq!(snapshot.snd_una, expected_snd_una);
    assert_eq!(snapshot.snd_nxt, expected_snd_nxt);
    assert_eq!(snapshot.snd_wnd, 0x4000);
    assert_eq!(snapshot.rcv_nxt, expected_rcv_nxt);
    assert_eq!(snapshot.rcv_wnd, u16::MAX as u32);
}

fn connected_snapshot(
    state: TcpState,
    octet: u8,
    snd_una: u32,
    snd_nxt: u32,
    rcv_nxt: u32,
) -> TcpConnectionSnapshot {
    TcpConnectionSnapshot {
        lookup_id: LOOKUP_ID,
        connection_id: None,
        owner_worker: DataWorkerId::new(0),
        state,
        local_port: LISTEN_PORT,
        local: Some(
            ("192.0.2.".to_owned() + &octet.to_string() + ":7443")
                .parse()
                .expect("local"),
        ),
        remote: ("198.51.100.".to_owned()
            + &octet.to_string()
            + ":"
            + &(40_000 + u16::from(octet)).to_string())
            .parse()
            .expect("remote"),
        iss: snd_una.wrapping_sub(1),
        irs: rcv_nxt.wrapping_sub(1),
        snd_una,
        snd_nxt,
        snd_wnd: 0x4000,
        rcv_nxt,
        rcv_wnd: u16::MAX as u32,
    }
}
