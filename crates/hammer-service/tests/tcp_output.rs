use hammer_service::transport::tcp::output::{
    tcp_available_send_window, tcp_output_next_sequence, tcp_output_sequence_len,
    tcp_payload_len_in_send_window,
};
use hammer_service::transport::tcp::{TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_SYN};

#[test]
fn tcp_output_sequence_space_counts_control_bits_and_wraps() {
    let sequence = u32::MAX - 2;
    let sequence_len = tcp_output_sequence_len(TCP_FLAG_ACK | TCP_FLAG_SYN | TCP_FLAG_FIN, 3);

    assert_eq!(sequence_len, 5);
    assert_eq!(tcp_output_next_sequence(sequence, sequence_len), 2);
}

#[test]
fn tcp_output_send_window_helpers_account_for_inflight_bytes_and_control_len() {
    assert_eq!(tcp_available_send_window(10_000, 10_020, 40, 40), 20);
    assert_eq!(
        tcp_payload_len_in_send_window(10_000, 10_020, 40, 40, 32, 0),
        20
    );
    assert_eq!(
        tcp_payload_len_in_send_window(10_000, 10_020, 40, 40, 32, 1),
        19
    );

    assert_eq!(tcp_available_send_window(10_000, 10_020, 20, 20), 0);
    assert_eq!(
        tcp_payload_len_in_send_window(10_000, 10_020, 20, 20, 32, 0),
        0
    );

    assert_eq!(tcp_available_send_window(u32::MAX - 4, 7, 20, 20), 8);
    assert_eq!(
        tcp_payload_len_in_send_window(u32::MAX - 4, 7, 20, 20, 32, 1),
        7
    );
}

#[test]
fn tcp_output_send_window_uses_min_of_peer_window_and_congestion_window() {
    assert_eq!(tcp_available_send_window(1000, 1200, 8000, 1000), 800);
    assert_eq!(tcp_available_send_window(1000, 1200, 700, 1000), 500);
}

#[test]
fn tcp_output_payload_len_is_zero_when_congestion_window_is_full() {
    assert_eq!(
        tcp_payload_len_in_send_window(1000, 2000, 8000, 1000, 512, 0),
        0
    );
}
