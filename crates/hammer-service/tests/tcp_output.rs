use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataWorkerId, Network, RouteMetadata, SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::transport::tcp::output::{
    TcpOutputConnectionView, TcpOutputDecision, TcpQueuedPayload, tcp_available_send_window,
    tcp_output_decision, tcp_payload_len_in_send_window,
};
use hammer_service::transport::tcp::{
    TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN, TcpConnectionSnapshot,
    TcpOutputBackend, TcpOutputBackendSlot, TcpOutputRecord, TcpOutputRetransmitQueue,
    TcpOutputSendView, TcpState, tcp_output_packet, tcp_output_packet_flags,
};

#[derive(Default)]
struct RecordingBackend {
    emitted: Mutex<Vec<Vec<u8>>>,
}

impl TcpOutputBackend for RecordingBackend {
    fn emit_buffer(&self, buffers: &DataPlaneBuffers, index: BufferIndex) -> CoreResult<()> {
        let packet = buffers.copy_packet(index)?.to_vec();
        self.emitted
            .lock()
            .expect("recording backend poisoned")
            .push(packet);
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
    let record = tcp_output_packet(snapshot, local, payload).expect("build ipv4 packet");
    let packet = packet_for_record(&record, payload);

    assert_eq!(record.lookup_id, 17);
    assert_eq!(record.connection_id, TcpConnectionId::new(1701));
    assert_eq!(record.local, local);
    assert_eq!(record.remote, remote);
    assert_eq!(record.sequence, 10_017);
    assert_eq!(record.acknowledgment, 20_057);
    assert_eq!(record.flags, TCP_FLAG_ACK | TCP_FLAG_PSH);
    assert_eq!(record.advertised_window, 32_768);
    assert_eq!(record.payload_len, payload.len());
    assert_eq!(record.sequence_len(), payload.len() as u32);
    assert!(record.consumes_sequence_space());
    assert_eq!(record.next_send_sequence(), 10_022);
    assert_eq!(record.metadata.network, Network::Tcp);
    assert_eq!(
        record.metadata.source,
        Some(SocksAddr::ip(local.ip(), local.port()))
    );
    assert_eq!(
        record.metadata.destination,
        Some(SocksAddr::ip(remote.ip(), remote.port()))
    );

    assert_eq!(packet.len(), 20 + 20 + payload.len());
    assert_eq!(packet[0] >> 4, 4);
    assert_eq!(
        u16::from_be_bytes([packet[2], packet[3]]) as usize,
        packet.len()
    );
    assert_eq!(&packet[20..22], &local.port().to_be_bytes());
    assert_eq!(&packet[22..24], &remote.port().to_be_bytes());
    assert_eq!(&packet[24..28], &record.sequence.to_be_bytes());
    assert_eq!(&packet[28..32], &record.acknowledgment.to_be_bytes());
    assert_eq!(packet[33], TCP_FLAG_ACK | TCP_FLAG_PSH);
    assert_eq!(&packet[40..], payload);
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

    let record = tcp_output_packet(snapshot, local, &[]).expect("build ipv6 packet");
    let packet = packet_for_record(&record, &[]);

    assert_eq!(record.sequence, 4_097);
    assert_eq!(record.acknowledgment, 8_193);
    assert_eq!(record.flags, TCP_FLAG_ACK);
    assert_eq!(record.advertised_window, u16::MAX);
    assert_eq!(record.sequence_len(), 0);
    assert!(!record.consumes_sequence_space());
    assert_eq!(record.next_send_sequence(), 4_097);
    assert!(record.to_retransmit_record().is_none());

    assert_eq!(packet.len(), 40 + 20);
    assert_eq!(packet[0] >> 4, 6);
    assert_eq!(u16::from_be_bytes([packet[4], packet[5]]) as usize, 20);
    assert_eq!(packet[53], TCP_FLAG_ACK);
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

    let record = tcp_output_packet_flags(snapshot, local, &[], TCP_FLAG_ACK | TCP_FLAG_FIN)
        .expect("build fin packet");
    let packet = packet_for_record(&record, &[]);

    assert_eq!(record.sequence, 10_017);
    assert_eq!(record.acknowledgment, 20_057);
    assert_eq!(record.flags, TCP_FLAG_ACK | TCP_FLAG_FIN);
    assert_eq!(record.payload_len, 0);
    assert_eq!(record.sequence_len(), 1);
    assert!(record.consumes_sequence_space());
    assert_eq!(record.next_send_sequence(), 10_018);
    assert_eq!(packet[33], TCP_FLAG_ACK | TCP_FLAG_FIN);
}

#[test]
fn tcp_output_builder_strips_psh_for_empty_payload_even_with_explicit_flags() {
    let local: SocketAddr = "192.0.2.10:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let snapshot = established_snapshot(local, remote);

    let record = tcp_output_packet_flags(snapshot, local, &[], TCP_FLAG_ACK | TCP_FLAG_PSH)
        .expect("build empty ack packet");
    let packet = packet_for_record(&record, &[]);

    assert_eq!(record.flags, TCP_FLAG_ACK);
    assert_eq!(packet[33], TCP_FLAG_ACK);
}

#[test]
fn tcp_output_record_sequence_space_counts_control_bits_and_wraps() {
    let record = manual_record(
        u32::MAX - 2,
        TCP_FLAG_ACK | TCP_FLAG_SYN | TCP_FLAG_FIN,
        b"abc",
    );

    assert_eq!(record.sequence_len(), 5);
    assert!(record.consumes_sequence_space());
    assert_eq!(record.next_send_sequence(), 2);

    let retransmit = record
        .to_retransmit_record()
        .expect("record should enter retransmit bookkeeping");
    assert_eq!(retransmit.record.sequence, u32::MAX - 2);
    assert_eq!(retransmit.next_sequence, 2);
    assert!(!retransmit.is_fully_acked_by(1));
    assert!(retransmit.is_fully_acked_by(2));
}

#[test]
fn tcp_output_retransmit_queue_tracks_unacked_segments_and_prunes_on_ack() {
    let mut queue = TcpOutputRetransmitQueue::new();
    let first = manual_record(10, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rust");
    let second = manual_record(14, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rs");
    let ack_only = manual_record(99, TCP_FLAG_ACK, b"");

    assert!(queue.track_output(&first).is_some());
    assert!(queue.track_output(&second).is_some());
    assert!(queue.track_output(&ack_only).is_none());
    assert_eq!(queue.len(), 2);
    assert_eq!(
        queue
            .iter()
            .map(|record| record.record.sequence)
            .collect::<Vec<_>>(),
        vec![10, 14]
    );

    assert_eq!(queue.acknowledge_through(13), 0);
    assert_eq!(
        queue.front().expect("first outstanding").record.sequence,
        10
    );
    assert_eq!(queue.acknowledge_through(14), 1);
    assert_eq!(
        queue.front().expect("second outstanding").record.sequence,
        14
    );
    assert_eq!(queue.acknowledge_through(16), 1);
    assert!(queue.is_empty());
}

#[test]
fn retransmit_queue_ack_sample_counts_acked_bytes_and_latest_rtt() {
    let mut queue = TcpOutputRetransmitQueue::new();
    let first = manual_record(10, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rust");
    let second = manual_record(14, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rs");
    let third = manual_record(16, TCP_FLAG_ACK | TCP_FLAG_PSH, b"tcp");
    let now = Instant::now();

    assert!(
        queue
            .track_output_with_sent_at(&first, now - Duration::from_millis(50))
            .is_some()
    );
    assert!(
        queue
            .track_output_with_sent_at(&second, now - Duration::from_millis(20))
            .is_some()
    );
    assert!(
        queue
            .track_output_with_sent_at(&third, now - Duration::from_millis(5))
            .is_some()
    );

    let sample = queue.acknowledge_through_with_sample(16, now);

    assert_eq!(sample.bytes_acked, 6);
    assert_eq!(sample.latest_rtt, Some(Duration::from_millis(20)));
    assert_eq!(sample.released_segments, 2);
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.front().expect("third outstanding").record.sequence,
        16
    );
}

#[test]
fn tcp_output_retransmit_queue_ignores_duplicate_sequence_ranges() {
    let mut queue = TcpOutputRetransmitQueue::new();
    let original = manual_record(10, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rust");
    let retransmit = manual_record(10, TCP_FLAG_ACK, b"rust");

    assert!(queue.track_output(&original).is_some());
    assert!(queue.track_output(&retransmit).is_some());
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.front().expect("tracked record").record.sequence,
        original.sequence
    );
    assert_eq!(
        queue.front().expect("tracked record").next_sequence,
        original.next_send_sequence()
    );
}

#[test]
fn tcp_output_retransmit_queue_refreshes_duplicate_sent_at_without_duplicate_entry() {
    let mut queue = TcpOutputRetransmitQueue::new();
    let original = manual_record(10, TCP_FLAG_ACK | TCP_FLAG_PSH, b"rust");
    let retransmit = manual_record(10, TCP_FLAG_ACK, b"rust");
    let first_sent = Instant::now();
    let latest_sent = first_sent + Duration::from_millis(25);

    assert!(
        queue
            .track_output_with_sent_at(&original, first_sent)
            .is_some()
    );
    assert!(
        queue
            .track_output_with_sent_at(&retransmit, latest_sent)
            .is_some()
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.front().expect("tracked record").record.sequence,
        original.sequence
    );
    assert_eq!(
        queue.front().expect("tracked record").next_sequence,
        original.next_send_sequence()
    );
    assert_eq!(
        queue.front().expect("tracked record").sent_at,
        Some(latest_sent)
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

    let mut view = TcpOutputSendView::from_snapshot(snapshot);

    assert_eq!(tcp_available_send_window(view), 20);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 0), 20);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 1), 19);

    snapshot.snd_wnd = 20;
    view = TcpOutputSendView::from_snapshot(snapshot);
    assert_eq!(tcp_available_send_window(view), 0);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 0), 0);

    snapshot.snd_una = u32::MAX - 4;
    snapshot.snd_nxt = 7;
    snapshot.snd_wnd = 20;
    view = TcpOutputSendView::from_snapshot(snapshot);
    assert_eq!(tcp_available_send_window(view), 8);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 1), 7);
}

#[test]
fn tcp_output_send_view_uses_min_of_peer_window_and_congestion_window() {
    let mut view = TcpOutputSendView {
        snd_una: 1000,
        snd_nxt: 1200,
        snd_wnd: 8000,
        congestion_window: 1000,
    };

    assert_eq!(tcp_available_send_window(view), 800);

    view.snd_wnd = 700;

    assert_eq!(tcp_available_send_window(view), 500);
}

#[test]
fn tcp_output_payload_len_is_zero_when_congestion_window_is_full() {
    let view = TcpOutputSendView {
        snd_una: 1000,
        snd_nxt: 2000,
        snd_wnd: 8000,
        congestion_window: 1000,
    };

    assert_eq!(tcp_payload_len_in_send_window(view, 512, 0), 0);
}

#[test]
fn tcp_output_decision_emits_syn_for_active_open_before_receive_state() {
    let local: SocketAddr = "0.0.0.0:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let mut snapshot = established_snapshot(local, remote);
    snapshot.state = TcpState::SynSent;
    snapshot.local = None;
    snapshot.iss = 0x1020_3040;
    snapshot.snd_una = snapshot.iss;
    snapshot.snd_nxt = snapshot.iss;

    let decision = tcp_output_decision(
        snapshot,
        TcpOutputConnectionView {
            connection_id: snapshot.connection_id,
            state: TcpState::SynSent,
            local: None,
            local_port: local.port(),
            remote,
            send_state_initialized: true,
            receive_state_initialized: false,
            pending_fin: false,
            output_payload_len: 1440,
            next_output_at: None,
            persist_armed: false,
            persist_deadline: None,
            send_view: TcpOutputSendView {
                snd_una: snapshot.snd_una,
                snd_nxt: snapshot.snd_nxt,
                snd_wnd: 0,
                congestion_window: 1440,
            },
            iss: snapshot.iss,
            snd_nxt: snapshot.snd_nxt,
        },
        None,
        false,
        None,
        Instant::now(),
    )
    .expect("plan syn output");

    let TcpOutputDecision::Work(work) = decision else {
        panic!("expected SYN work item, got {decision:?}");
    };
    assert_eq!(work.record.sequence, snapshot.iss);
    assert_eq!(work.record.flags, TCP_FLAG_SYN);
    assert_eq!(work.record.payload_len, 0);
    assert_eq!(work.record.remote, remote);
    assert_eq!(work.record.local.port(), local.port());
    assert!(work.send_id.is_none());
}

#[test]
fn tcp_output_decision_does_not_emit_fin_before_queued_tail_payload() {
    let local: SocketAddr = "192.0.2.30:49152".parse().expect("local");
    let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
    let mut snapshot = established_snapshot(local, remote);
    snapshot.snd_una = 0x1020_3041;
    snapshot.snd_nxt = 0x1020_3041;
    snapshot.rcv_nxt = 0x5566_7789;

    let decision = tcp_output_decision(
        snapshot,
        TcpOutputConnectionView {
            connection_id: snapshot.connection_id,
            state: TcpState::Established,
            local: Some(local),
            local_port: local.port(),
            remote,
            send_state_initialized: true,
            receive_state_initialized: true,
            pending_fin: true,
            output_payload_len: 1440,
            next_output_at: None,
            persist_armed: false,
            persist_deadline: None,
            send_view: TcpOutputSendView {
                snd_una: snapshot.snd_una,
                snd_nxt: snapshot.snd_nxt,
                snd_wnd: 65_535,
                congestion_window: 65_535,
            },
            iss: snapshot.iss,
            snd_nxt: snapshot.snd_nxt,
        },
        Some(TcpQueuedPayload {
            id: 41,
            len: 4,
            offset: 0,
            tail_queued: true,
        }),
        false,
        None,
        Instant::now(),
    )
    .expect("plan queued payload output");

    let TcpOutputDecision::Work(work) = decision else {
        panic!("expected queued payload work item, got {decision:?}");
    };
    assert_eq!(work.payload_len, 4);
    assert!(!work.include_fin);
    assert_eq!(work.record.flags & TCP_FLAG_FIN, 0);
}

#[test]
fn tcp_output_backend_slot_forwards_buffers_to_installed_backend() {
    let backend = Arc::new(RecordingBackend::default());
    let slot = TcpOutputBackendSlot::new();
    slot.install(backend.clone());

    let record = manual_record(512, TCP_FLAG_ACK | TCP_FLAG_PSH, b"emit");
    let buffers = DataPlaneBuffers::with_buffer_capacity(2048, 8);
    let index = buffer_for_record(&buffers, &record, b"emit");
    slot.emit_buffer(&buffers, index).expect("emit buffer");
    buffers.free_index(index);

    let emitted = backend.emitted.lock().expect("recording backend poisoned");
    assert_eq!(emitted.as_slice(), &[packet_for_record(&record, b"emit")]);
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

fn manual_record(sequence: u32, flags: u8, payload: &[u8]) -> TcpOutputRecord {
    let local: SocketAddr = "192.0.2.30:50000".parse().expect("manual local");
    let remote: SocketAddr = "198.51.100.30:443".parse().expect("manual remote");
    TcpOutputRecord {
        lookup_id: 33,
        connection_id: TcpConnectionId::new(3301),
        local,
        remote,
        sequence,
        acknowledgment: 90,
        flags,
        advertised_window: 4_096,
        payload_len: payload.len(),
        metadata: RouteMetadata {
            network: Network::Tcp,
            source: Some(SocksAddr::ip(local.ip(), local.port())),
            destination: Some(SocksAddr::ip(remote.ip(), remote.port())),
            ..RouteMetadata::default()
        },
    }
}

fn packet_for_record(record: &TcpOutputRecord, payload: &[u8]) -> Vec<u8> {
    let buffers = DataPlaneBuffers::with_buffer_capacity(2048, 8);
    let index = buffer_for_record(&buffers, record, payload);
    let packet = buffers
        .copy_packet(index)
        .expect("copy output packet for assertion")
        .to_vec();
    buffers.free_index(index);
    packet
}

fn buffer_for_record(
    buffers: &DataPlaneBuffers,
    record: &TcpOutputRecord,
    payload: &[u8],
) -> BufferIndex {
    assert_eq!(record.payload_len, payload.len());
    let index = record
        .alloc_header_buffer(buffers)
        .expect("alloc output header buffer");
    buffers.append(index, payload).expect("append payload");
    record
        .finalize_buffer_checksums(buffers, index)
        .expect("finalize output checksums");
    index
}
