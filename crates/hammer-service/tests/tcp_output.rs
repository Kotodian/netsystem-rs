use hammer_service::transport::tcp::output::{
    tcp_available_send_window, tcp_output_next_sequence, tcp_output_sequence_len,
    tcp_payload_len_in_send_window,
};
use hammer_service::transport::tcp::{TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_SYN, TcpOutputSendView};

#[test]
fn tcp_output_sequence_space_counts_control_bits_and_wraps() {
    let sequence = u32::MAX - 2;
    let sequence_len = tcp_output_sequence_len(TCP_FLAG_ACK | TCP_FLAG_SYN | TCP_FLAG_FIN, 3);

    assert_eq!(sequence_len, 5);
    assert_eq!(tcp_output_next_sequence(sequence, sequence_len), 2);
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
