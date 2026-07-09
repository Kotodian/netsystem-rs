#[test]
fn trace_runtime_does_not_own_node_payload_shapes() {
    let source = include_str!("../src/trace.rs");
    let central_payload_enum = concat!("enum ", "Trace", "Payload");

    assert!(
        !source.contains(central_payload_enum),
        "runtime trace must not centralize node payload variants"
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
    let runtime_trace = include_str!("../src/trace.rs");
    let runtime_data_plane = include_str!("../src/data_plane.rs");
    let runtime_spawn = include_str!("../src/spawn.rs");
    let global_enable = concat!("set_", "trace_", "enabled");
    let enum_payload_api = concat!("add_", "trace_", "payload");
    let closure_payload_api = concat!("add_", "trace_", "with");

    for source in [runtime_trace, runtime_data_plane, runtime_spawn] {
        assert!(
            !source.contains(global_enable),
            "trace must be configured through declarative publish, not a global enable switch"
        );
        assert!(
            !source.contains(enum_payload_api),
            "runtime must not expose enum-payload trace plumbing"
        );
        assert!(
            !source.contains(closure_payload_api),
            "trace append should use the packet trace macro, not closure payload plumbing"
        );
    }
    assert!(
        runtime_trace.contains("macro_rules! add_packet_trace"),
        "runtime trace API should expose the packet trace append macro"
    );
}
