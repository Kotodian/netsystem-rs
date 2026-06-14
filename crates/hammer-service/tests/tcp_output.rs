use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::{Network, RouteMetadata, SocksAddr};
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::transport::tcp::output::{
    tcp_available_send_window, tcp_payload_len_in_send_window,
};
use hammer_service::transport::tcp::{
    TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN, TcpOutputRecord,
    TcpOutputRetransmitQueue, TcpOutputSendView,
};

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
    let mut view = TcpOutputSendView {
        snd_una: 10_000,
        snd_nxt: 10_020,
        snd_wnd: 40,
        congestion_window: 40,
    };

    assert_eq!(tcp_available_send_window(view), 20);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 0), 20);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 1), 19);

    view.snd_wnd = 20;
    view.congestion_window = 20;
    assert_eq!(tcp_available_send_window(view), 0);
    assert_eq!(tcp_payload_len_in_send_window(view, 32, 0), 0);

    view.snd_una = u32::MAX - 4;
    view.snd_nxt = 7;
    view.snd_wnd = 20;
    view.congestion_window = 20;
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

fn manual_record(sequence: u32, flags: u8, payload: &[u8]) -> TcpOutputRecord {
    let local: SocketAddr = "192.0.2.30:50000".parse().expect("manual local");
    let remote: SocketAddr = "198.51.100.30:443".parse().expect("manual remote");
    TcpOutputRecord {
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
