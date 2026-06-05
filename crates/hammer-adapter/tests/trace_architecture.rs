#[test]
fn trace_adapter_does_not_own_node_payload_shapes() {
    let source = include_str!("../src/trace/mod.rs");
    let central_payload_enum = concat!("enum ", "Trace", "Payload");

    assert!(
        !source.contains(central_payload_enum),
        "adapter trace must not centralize node payload variants"
    );
    assert!(
        !source.contains("Arc<dyn") && !source.contains("Box<dyn") && !source.contains("Rc<dyn"),
        "trace payload storage must not use trait objects"
    );
    assert!(
        !source.contains("pub fn clear("),
        "declarative trace control must not expose clear()"
    );
}

#[test]
fn trace_has_no_global_enable_or_enum_plumbing() {
    let adapter_trace = include_str!("../src/trace/mod.rs");
    let adapter_buffer = include_str!("../src/buffer.rs");
    let runtime_spawn = include_str!("../../hammer-runtime/src/spawn.rs");
    let global_enable = concat!("set_", "trace_", "enabled");
    let enum_payload_api = concat!("add_", "trace_", "payload");
    let closure_payload_api = concat!("add_", "trace_", "with");

    for source in [adapter_trace, adapter_buffer, runtime_spawn] {
        assert!(
            !source.contains(global_enable),
            "trace must be configured through declarative publish, not a global enable switch"
        );
        assert!(
            !source.contains(enum_payload_api),
            "adapter/runtime must not expose enum-payload trace plumbing"
        );
        assert!(
            !source.contains(closure_payload_api),
            "trace append should use the packet trace macro, not closure payload plumbing"
        );
    }
    assert!(
        adapter_trace.contains("macro_rules! add_packet_trace"),
        "adapter trace API should expose the packet trace append macro"
    );
}

#[test]
fn trace_is_not_wired_through_legacy_inbound_paths() {
    let runtime_inbounds = include_str!("../../hammer-runtime/src/inbounds.rs");
    let legacy_tun_inbound = include_str!("../../hammer-runtime/src/protocol/tun/inbound.rs");
    let proxy_inbound = include_str!("../../hammer-runtime/src/protocol/proxy/inbound.rs");

    for source in [runtime_inbounds, legacy_tun_inbound, proxy_inbound] {
        assert!(
            !source.contains("TraceControl")
                && !source.contains("TracePolicy")
                && !source.contains("set_trace_control")
                && !source.contains("try_mark_trace")
                && !source.contains("add_trace("),
            "trace must remain packet-graph node plumbing, not legacy inbound-manager wiring"
        );
    }
}
