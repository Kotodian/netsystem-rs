#[test]
fn session_queue_node_does_not_clear_input_frame() {
    let source = include_str!("../src/session/node.rs");

    assert!(
        !source.contains("frame.clear()"),
        "SessionQueueNode is a polling input driver and must not consume input frames"
    );
}

#[test]
fn tun_boundary_does_not_use_public_copy_packet_api() {
    let source = include_str!("../src/tun/mod.rs");

    assert!(
        !source.contains("copy_packet"),
        "TUN boundary must read buffer chains locally instead of calling copy_packet"
    );
}
