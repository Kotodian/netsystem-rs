use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use hammer_adapter::{DataWorkerId, Network, RouteMetadata, SocksAddr};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::transport::tcp::output::{
    tcp_available_send_window, tcp_payload_len_in_send_window,
};
use hammer_service::transport::tcp::{
    TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN, TcpConnectionSnapshot,
    TcpOutputBackend, TcpOutputBackendSlot, TcpOutputRetransmitQueue, TcpOutputSegment, TcpState,
    build_tcp_output_segment, build_tcp_output_segment_with_flags,
};

#[derive(Default)]
struct RecordingBackend {
    emitted: Mutex<Vec<TcpOutputSegment>>,
}

impl TcpOutputBackend for RecordingBackend {
    fn emit_segment(&self, segment: TcpOutputSegment) -> CoreResult<()> {
        self.emitted
            .lock()
            .expect("recording backend poisoned")
            .push(segment);
        Ok(())
    }
}

#[test]
fn tcp_output_builder_emits_ipv4_ack_psh_segment_with_metadata_and_payload() {
    let local: SocketAddr = "192.0.2.10:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let mut snapshot = established_snapshot(local, remote);
    snapshot.iss = 10_000;
    snapshot.irs = 20_000;
    snapshot.snd_una = 10_001;
    snapshot.snd_nxt = 10_017;
    snapshot.rcv_nxt = 20_057;
    snapshot.rcv_wnd = 32_768;

    let payload = b"hello";
    let segment = build_tcp_output_segment(snapshot, local, payload).expect("build ipv4 segment");

    assert_eq!(segment.lookup_id, 17);
    assert_eq!(segment.connection_id, TcpConnectionId::new(1701));
    assert_eq!(segment.local, local);
    assert_eq!(segment.remote, remote);
    assert_eq!(segment.sequence, 10_017);
    assert_eq!(segment.acknowledgment, 20_057);
    assert_eq!(segment.flags, TCP_FLAG_ACK | TCP_FLAG_PSH);
    assert_eq!(segment.advertised_window, 32_768);
    assert_eq!(segment.payload, payload);
    assert_eq!(segment.sequence_len(), payload.len() as u32);
    assert!(segment.consumes_sequence_space());
    assert_eq!(segment.next_send_sequence(), 10_022);
    assert_eq!(segment.metadata.network, Network::Tcp);
    assert_eq!(
        segment.metadata.source,
        Some(SocksAddr::ip(local.ip(), local.port()))
    );
    assert_eq!(
        segment.metadata.destination,
        Some(SocksAddr::ip(remote.ip(), remote.port()))
    );

    assert_eq!(segment.packet.len(), 20 + 20 + payload.len());
    assert_eq!(segment.packet[0] >> 4, 4);
    assert_eq!(
        u16::from_be_bytes([segment.packet[2], segment.packet[3]]) as usize,
        segment.packet.len()
    );
    assert_eq!(&segment.packet[20..22], &local.port().to_be_bytes());
    assert_eq!(&segment.packet[22..24], &remote.port().to_be_bytes());
    assert_eq!(&segment.packet[24..28], &segment.sequence.to_be_bytes());
    assert_eq!(
        &segment.packet[28..32],
        &segment.acknowledgment.to_be_bytes()
    );
    assert_eq!(segment.packet[33], TCP_FLAG_ACK | TCP_FLAG_PSH);
    assert_eq!(&segment.packet[40..], payload);
}

#[test]
fn tcp_output_builder_falls_back_to_initial_sequences_for_empty_ipv6_ack() {
    let local: SocketAddr = "[2001:db8::10]:55000".parse().expect("local");
    let remote: SocketAddr = "[2001:db8::20]:443".parse().expect("remote");
    let mut snapshot = established_snapshot(local, remote);
    snapshot.iss = 4_096;
    snapshot.irs = 8_192;
    snapshot.snd_una = 0;
    snapshot.snd_nxt = 0;
    snapshot.rcv_nxt = 0;
    snapshot.rcv_wnd = 100_000;

    let segment = build_tcp_output_segment(snapshot, local, &[]).expect("build ipv6 segment");

    assert_eq!(segment.sequence, 4_097);
    assert_eq!(segment.acknowledgment, 8_193);
    assert_eq!(segment.flags, TCP_FLAG_ACK);
    assert_eq!(segment.advertised_window, u16::MAX);
    assert_eq!(segment.sequence_len(), 0);
    assert!(!segment.consumes_sequence_space());
    assert_eq!(segment.next_send_sequence(), 4_097);
    assert!(segment.to_retransmit_segment().is_none());

    assert_eq!(segment.packet.len(), 40 + 20);
    assert_eq!(segment.packet[0] >> 4, 6);
    assert_eq!(
        u16::from_be_bytes([segment.packet[4], segment.packet[5]]) as usize,
        20
    );
    assert_eq!(segment.packet[53], TCP_FLAG_ACK);
}

#[test]
fn tcp_output_builder_with_fin_flag_consumes_sequence_space_without_payload() {
    let local: SocketAddr = "192.0.2.10:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let mut snapshot = established_snapshot(local, remote);
    snapshot.iss = 10_000;
    snapshot.irs = 20_000;
    snapshot.snd_una = 10_001;
    snapshot.snd_nxt = 10_017;
    snapshot.rcv_nxt = 20_057;

    let segment =
        build_tcp_output_segment_with_flags(snapshot, local, &[], TCP_FLAG_ACK | TCP_FLAG_FIN)
            .expect("build fin segment");

    assert_eq!(segment.sequence, 10_017);
    assert_eq!(segment.acknowledgment, 20_057);
    assert_eq!(segment.flags, TCP_FLAG_ACK | TCP_FLAG_FIN);
    assert!(segment.payload.is_empty());
    assert_eq!(segment.sequence_len(), 1);
    assert!(segment.consumes_sequence_space());
    assert_eq!(segment.next_send_sequence(), 10_018);
    assert_eq!(segment.packet[33], TCP_FLAG_ACK | TCP_FLAG_FIN);
}

#[test]
fn tcp_output_builder_strips_psh_for_empty_payload_even_with_explicit_flags() {
    let local: SocketAddr = "192.0.2.10:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let snapshot = established_snapshot(local, remote);

    let segment =
        build_tcp_output_segment_with_flags(snapshot, local, &[], TCP_FLAG_ACK | TCP_FLAG_PSH)
            .expect("build empty ack segment");

    assert_eq!(segment.flags, TCP_FLAG_ACK);
    assert_eq!(segment.packet[33], TCP_FLAG_ACK);
}

#[test]
fn tcp_output_segment_sequence_space_counts_control_bits_and_wraps() {
    let segment = manual_segment(
        u32::MAX - 2,
        TCP_FLAG_ACK | TCP_FLAG_SYN | TCP_FLAG_FIN,
        b"abc",
    );

    assert_eq!(segment.sequence_len(), 5);
    assert!(segment.consumes_sequence_space());
    assert_eq!(segment.next_send_sequence(), 2);

    let retransmit = segment
        .to_retransmit_segment()
        .expect("segment should enter retransmit bookkeeping");
    assert_eq!(retransmit.segment.sequence, u32::MAX - 2);
    assert_eq!(retransmit.next_sequence, 2);
    assert!(!retransmit.is_fully_acked_by(1));
    assert!(retransmit.is_fully_acked_by(2));
}

#[test]
fn tcp_output_retransmit_queue_tracks_unacked_segments_and_prunes_on_ack() {
    let mut queue = TcpOutputRetransmitQueue::new();
    let first = manual_segment(10, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rust");
    let second = manual_segment(14, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rs");
    let ack_only = manual_segment(99, TCP_FLAG_ACK, b"");

    assert!(queue.track_segment(&first).is_some());
    assert!(queue.track_segment(&second).is_some());
    assert!(queue.track_segment(&ack_only).is_none());
    assert_eq!(queue.len(), 2);
    assert_eq!(
        queue
            .iter()
            .map(|segment| segment.segment.sequence)
            .collect::<Vec<_>>(),
        vec![10, 14]
    );

    assert_eq!(queue.acknowledge_through(13), 0);
    assert_eq!(
        queue.front().expect("first outstanding").segment.sequence,
        10
    );
    assert_eq!(queue.acknowledge_through(14), 1);
    assert_eq!(
        queue.front().expect("second outstanding").segment.sequence,
        14
    );
    assert_eq!(queue.acknowledge_through(16), 1);
    assert!(queue.is_empty());
}

#[test]
fn tcp_output_retransmit_queue_ignores_duplicate_sequence_ranges() {
    let mut queue = TcpOutputRetransmitQueue::new();
    let original = manual_segment(10, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rust");
    let retransmit = manual_segment(10, TCP_FLAG_ACK, b"rust");

    assert!(queue.track_segment(&original).is_some());
    assert!(queue.track_segment(&retransmit).is_some());
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.front().expect("tracked segment").segment.sequence,
        original.sequence
    );
    assert_eq!(
        queue.front().expect("tracked segment").next_sequence,
        original.next_send_sequence()
    );
}

#[test]
fn tcp_output_send_window_helpers_account_for_inflight_bytes_and_control_len() {
    let local: SocketAddr = "192.0.2.10:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let mut snapshot = established_snapshot(local, remote);
    snapshot.snd_una = 10_000;
    snapshot.snd_nxt = 10_020;
    snapshot.snd_wnd = 40;

    assert_eq!(tcp_available_send_window(snapshot), 20);
    assert_eq!(tcp_payload_len_in_send_window(snapshot, 32, 0), 20);
    assert_eq!(tcp_payload_len_in_send_window(snapshot, 32, 1), 19);

    snapshot.snd_wnd = 20;
    assert_eq!(tcp_available_send_window(snapshot), 0);
    assert_eq!(tcp_payload_len_in_send_window(snapshot, 32, 0), 0);

    snapshot.snd_una = u32::MAX - 4;
    snapshot.snd_nxt = 7;
    snapshot.snd_wnd = 20;
    assert_eq!(tcp_available_send_window(snapshot), 8);
    assert_eq!(tcp_payload_len_in_send_window(snapshot, 32, 1), 7);
}

#[test]
fn tcp_output_backend_slot_forwards_segments_to_installed_backend() {
    let backend = Arc::new(RecordingBackend::default());
    let slot = TcpOutputBackendSlot::new();
    slot.install(backend.clone());

    let segment = manual_segment(512, TCP_FLAG_ACK | TCP_FLAG_PSH, b"emit");
    slot.emit_segment(segment.clone()).expect("emit segment");

    let emitted = backend.emitted.lock().expect("recording backend poisoned");
    assert_eq!(emitted.as_slice(), &[segment]);
}

fn established_snapshot(local: SocketAddr, remote: SocketAddr) -> TcpConnectionSnapshot {
    TcpConnectionSnapshot {
        lookup_id: 17,
        connection_id: Some(TcpConnectionId::new(1701)),
        owner_worker: DataWorkerId::new(2),
        state: TcpState::Established,
        local_port: local.port(),
        local: Some(local),
        remote,
        iss: 0,
        irs: 0,
        snd_una: 0,
        snd_nxt: 0,
        snd_wnd: 65_535,
        rcv_nxt: 0,
        rcv_wnd: 65_535,
    }
}

fn manual_segment(sequence: u32, flags: u8, payload: &[u8]) -> TcpOutputSegment {
    let local: SocketAddr = "192.0.2.30:50000".parse().expect("manual local");
    let remote: SocketAddr = "198.51.100.30:443".parse().expect("manual remote");
    TcpOutputSegment {
        lookup_id: 33,
        connection_id: TcpConnectionId::new(3301),
        local,
        remote,
        sequence,
        acknowledgment: 90,
        flags,
        advertised_window: 4_096,
        payload: payload.to_vec(),
        metadata: RouteMetadata {
            network: Network::Tcp,
            source: Some(SocksAddr::ip(local.ip(), local.port())),
            destination: Some(SocksAddr::ip(remote.ip(), remote.port())),
            ..RouteMetadata::default()
        },
        packet: payload.to_vec(),
    }
}
