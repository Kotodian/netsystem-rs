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

const LOOKUP_ID: u32 = 0x4411;
const LISTEN_PORT: u16 = 7443;
const DEFAULT_WINDOW: u32 = 0x4000;

#[test]
fn tcp_bootstrap_ack_still_advances_fin_wait1() {
    let octet = 60;
    let initial = TcpConnectionSnapshot::with_default_windows(
        LOOKUP_ID,
        None,
        DataWorkerId::new(0),
        TcpState::FinWait1,
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
    );
    let packet = tcp_packet(
        octet,
        0x0102_0304,
        Some(0x1020_3040),
        tcp_flags(false, false, false, true),
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::FinWait2));
    assert_eq!(result.snapshot_after_packet.iss, 0x1020_303f);
    assert_eq!(result.snapshot_after_packet.irs, 0x0102_0303);
    assert_eq!(result.snapshot_after_packet.snd_una, 0x1020_3040);
    assert_eq!(result.snapshot_after_packet.snd_nxt, 0x1020_3040);
    assert_eq!(result.snapshot_after_packet.snd_wnd, DEFAULT_WINDOW);
    assert_eq!(result.snapshot_after_packet.rcv_nxt, 0x0102_0304);
}

#[test]
fn tcp_stale_ack_does_not_advance_fin_wait1() {
    let octet = 61;
    let initial = established_snapshot(
        TcpState::FinWait1,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        0x0102_0304,
        Some(0x1020_303f),
        tcp_flags(false, false, false, true),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::FinWait1));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_duplicate_ack_updates_window_without_advancing_fin_wait1() {
    let octet = 68;
    let initial = established_snapshot(
        TcpState::FinWait1,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        0x0102_0304,
        Some(initial.snd_una),
        tcp_flags(false, false, false, true),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::FinWait1));
    assert_eq!(result.snapshot_after_packet.snd_una, initial.snd_una);
    assert_eq!(result.snapshot_after_packet.snd_nxt, initial.snd_nxt);
    assert_eq!(result.snapshot_after_packet.snd_wnd, 0x2000);
    assert_eq!(result.snapshot_after_packet.rcv_nxt, initial.rcv_nxt);
}

#[test]
fn tcp_ack_past_snd_nxt_is_dropped_without_state_progress() {
    let octet = 62;
    let initial = established_snapshot(
        TcpState::FinWait1,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        0x0102_0304,
        Some(0x1020_3049),
        tcp_flags(false, false, false, true),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::FinWait1));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_out_of_window_fin_does_not_advance_receive_state() {
    let octet = 63;
    let initial = established_snapshot(
        TcpState::Established,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        16,
    );
    let packet = tcp_packet(
        octet,
        initial.rcv_nxt + initial.rcv_wnd,
        Some(initial.snd_una),
        tcp_flags(true, false, false, true),
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::Established));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_in_window_out_of_order_payload_does_not_advance_receive_state() {
    let octet = 71;
    let initial = established_snapshot(
        TcpState::Established,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet(
        octet,
        initial.rcv_nxt + 4,
        Some(initial.snd_una),
        tcp_flags(false, false, false, true),
        b"late",
    );
    let reset = tcp_packet(
        octet,
        initial.rcv_nxt,
        Some(initial.snd_una),
        tcp_flags(false, false, true, true),
        b"",
    );

    let result = run_receive_cases(initial, &[(&packet, octet), (&reset, octet)]);

    assert_eq!(result.state_after_packet, Some(TcpState::Closed));
    assert_eq!(result.snapshot_after_packet.rcv_nxt, initial.rcv_nxt);
}

#[test]
fn tcp_in_window_out_of_order_fin_does_not_advance_receive_state() {
    let octet = 72;
    let initial = established_snapshot(
        TcpState::Established,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet(
        octet,
        initial.rcv_nxt + 4,
        Some(initial.snd_una),
        tcp_flags(true, false, false, true),
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::Established));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_out_of_window_rst_does_not_close_established_flow() {
    let octet = 64;
    let initial = established_snapshot(
        TcpState::Established,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        16,
    );
    let packet = tcp_packet(
        octet,
        initial.rcv_nxt + initial.rcv_wnd,
        None,
        tcp_flags(false, false, true, false),
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::Established));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_partial_ack_does_not_advance_last_ack_but_tracks_forward_progress() {
    let octet = 69;
    let initial = established_snapshot(
        TcpState::LastAck,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        0x0102_0304,
        Some(0x1020_3044),
        tcp_flags(false, false, false, true),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::LastAck));
    assert_eq!(result.snapshot_after_packet.snd_una, 0x1020_3044);
    assert_eq!(result.snapshot_after_packet.snd_nxt, initial.snd_nxt);
    assert_eq!(result.snapshot_after_packet.snd_wnd, 0x2000);
    assert_eq!(result.snapshot_after_packet.rcv_nxt, initial.rcv_nxt);
}

#[test]
fn tcp_partial_ack_does_not_advance_closing_but_tracks_forward_progress() {
    let octet = 70;
    let initial = established_snapshot(
        TcpState::Closing,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0305,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        initial.rcv_nxt,
        Some(0x1020_3044),
        tcp_flags(false, false, false, true),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::Closing));
    assert_eq!(result.snapshot_after_packet.snd_una, 0x1020_3044);
    assert_eq!(result.snapshot_after_packet.snd_nxt, initial.snd_nxt);
    assert_eq!(result.snapshot_after_packet.snd_wnd, 0x2000);
    assert_eq!(result.snapshot_after_packet.rcv_nxt, initial.rcv_nxt);
}

#[test]
fn tcp_missing_ack_fin_does_not_advance_fin_wait1() {
    let octet = 66;
    let initial = established_snapshot(
        TcpState::FinWait1,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        0x0102_0304,
        None,
        tcp_flags(true, false, false, false),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::FinWait1));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_in_window_syn_does_not_advance_fin_wait1_or_update_window() {
    let octet = 67;
    let initial = established_snapshot(
        TcpState::FinWait1,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        0x0102_0304,
        Some(0x1020_3048),
        tcp_flags(false, true, false, true),
        0x2000,
        b"",
    );

    let result = run_receive_case(initial, &packet, octet);

    assert_eq!(result.state_after_packet, Some(TcpState::FinWait1));
    assert_eq!(result.snapshot_after_packet, initial);
}

#[test]
fn tcp_in_window_out_of_order_payload_advances_when_gap_arrives() {
    let octet = 71;
    let initial = established_snapshot(
        TcpState::Established,
        octet,
        0x1020_3040,
        0x1020_3048,
        0x0102_0304,
        DEFAULT_WINDOW,
    );
    let packet = tcp_packet_with_window(
        octet,
        initial.rcv_nxt + 4,
        Some(initial.snd_una),
        tcp_flags(false, false, false, true),
        DEFAULT_WINDOW as u16,
        b"hole",
    );
    let gap = tcp_packet_with_window(
        octet,
        initial.rcv_nxt,
        Some(initial.snd_una),
        tcp_flags(false, false, false, true),
        DEFAULT_WINDOW as u16,
        b"gap!",
    );

    let result = run_receive_cases(initial, &[(&packet, octet), (&gap, octet)]);

    assert_eq!(result.state_after_packet, Some(TcpState::Established));
    assert_eq!(result.snapshot_after_packet.rcv_nxt, initial.rcv_nxt + 8);
    assert_eq!(result.snapshot_after_packet.snd_una, initial.snd_una);
    assert_eq!(result.snapshot_after_packet.snd_nxt, initial.snd_nxt);
}

#[derive(Debug)]
struct ReceiveCaseResult {
    state_after_packet: Option<TcpState>,
    snapshot_after_packet: TcpConnectionSnapshot,
}

fn run_receive_case(
    initial_snapshot: TcpConnectionSnapshot,
    packet: &[u8],
    octet: u8,
) -> ReceiveCaseResult {
    run_receive_cases(initial_snapshot, &[(packet, octet)])
}

fn run_receive_cases(
    initial_snapshot: TcpConnectionSnapshot,
    packets: &[(&[u8], u8)],
) -> ReceiveCaseResult {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let tcp_rcv_process = runtime
        .nodes()
        .register_internal(TcpRcvProcessControlPlane::new(TcpRcvProcessNext::nodes(drop)).node());
    let established = TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(tcp_rcv_process));
    let connections = published_connections(initial_snapshot);
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
        packets.first().map(|(_, octet)| *octet).unwrap_or_default(),
    );

    for (packet, octet) in packets {
        schedule_tcp_packet(&runtime, tcp_input, packet, *octet);
    }

    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);

    ReceiveCaseResult {
        state_after_packet: established.connection_state_for_test(LOOKUP_ID),
        snapshot_after_packet: established
            .connection_snapshot_for_test(LOOKUP_ID)
            .expect("snapshot after packet"),
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

fn established_snapshot(
    state: TcpState,
    octet: u8,
    snd_una: u32,
    snd_nxt: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
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
        snd_wnd: DEFAULT_WINDOW,
        rcv_nxt,
        rcv_wnd,
    }
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
) {
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
    runtime.run_ready_nodes().expect("run nodes");
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

fn tcp_packet(
    octet: u8,
    sequence: u32,
    acknowledgment: Option<u32>,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    tcp_packet_with_window(
        octet,
        sequence,
        acknowledgment,
        flags,
        DEFAULT_WINDOW as u16,
        payload,
    )
}

fn tcp_packet_with_window(
    octet: u8,
    sequence: u32,
    acknowledgment: Option<u32>,
    flags: u8,
    window: u16,
    payload: &[u8],
) -> Vec<u8> {
    let source = Ipv4Addr::new(198, 51, 100, octet);
    let destination = Ipv4Addr::new(192, 0, 2, octet);
    let source_port = 40_000 + u16::from(octet);
    let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(
        &mut packet[20..],
        source_port,
        LISTEN_PORT,
        sequence,
        acknowledgment.unwrap_or_default(),
        flags,
        window,
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
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    window: u16,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[4..8].copy_from_slice(&sequence.to_be_bytes());
    segment[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = flags;
    segment[14..16].copy_from_slice(&window.to_be_bytes());
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
