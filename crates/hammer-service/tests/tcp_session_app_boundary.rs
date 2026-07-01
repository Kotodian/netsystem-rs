#[test]
fn session_ooo_rx_path_does_not_allocate_payload_vec() {
    let source = include_str!("../src/session/app.rs");

    assert!(
        !source.contains("let mut bytes = Vec::new()"),
        "OOO RX must stream buffer-chain slices into the session FIFO without a payload Vec"
    );
    assert!(
        !source.contains("bytes.extend_from_slice"),
        "OOO RX must not gather payload bytes before FIFO enqueue"
    );
}

#[test]
fn session_rx_enqueue_reports_partial_delivery_without_claiming_full_accept() {
    let runtime_source = include_str!("../src/session/runtime.rs");
    let tcp_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        runtime_source.contains("accepted_len"),
        "SessionRxEnqueue should report accepted_len separately from delivered_len"
    );
    assert!(
        tcp_source.contains("enqueue.accepted_len"),
        "TCP established path must branch on exact accepted_len"
    );
}
