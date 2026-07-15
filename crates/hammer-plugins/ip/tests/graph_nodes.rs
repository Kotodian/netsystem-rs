use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::registry::RuntimeRegistry;
use hammer_plugin_ip::{
    IcmpEchoRequestNext, IcmpErrorNext, IpInputNext, IpLocalNext, IpReassemblyNext,
    reset_ip_main_for_test,
};
use hammer_runtime::init::run_init_functions;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

#[test]
fn ip_plugin_installs_its_vpp_style_packet_graph() {
    hammer_service::reset_subsystem_mains_for_plugin_test();
    reset_ip_main_for_test();

    let registry = RuntimeRegistry::new();
    registry.set(Arc::new(Config::default()));
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut engine = Engine::new(runtime, registry);

    run_init_functions(&mut engine).expect("initialize the packet graph");

    for name in [
        "device-input",
        "ip-input",
        "ip-lookup",
        "ip-local",
        "ip-receive",
        "icmp-input",
        "icmp-echo-request",
        "icmp-path-mtu",
        "icmp-error",
        "ip-reassembly",
    ] {
        assert!(
            engine.runtime.node_by_name(name).is_some(),
            "missing {name}"
        );
    }

    let node = |name| engine.runtime.node_by_name(name).expect("registered node");
    let nodes = engine.runtime.nodes();
    let drop = node("drop");
    let lookup = node("ip-lookup");
    let input = node("ip-input");
    let local = node("ip-local");
    let receive = node("ip-receive");
    let icmp_input = node("icmp-input");
    let echo_request = node("icmp-echo-request");
    let icmp_error = node("icmp-error");
    let reassembly = node("ip-reassembly");

    assert_eq!(nodes.node_next(input, IpInputNext::Lookup).unwrap(), lookup);
    assert_eq!(
        nodes.node_next(input, IpInputNext::IcmpError).unwrap(),
        icmp_error
    );
    assert_eq!(
        nodes.node_next(input, IpInputNext::Reassembly).unwrap(),
        reassembly
    );
    assert_eq!(
        nodes.node_next(local, IpLocalNext::Icmp).unwrap(),
        icmp_input
    );
    assert_eq!(
        nodes.node_next(receive, IpLocalNext::Icmp).unwrap(),
        icmp_input
    );
    assert_eq!(
        nodes
            .node_next(echo_request, IcmpEchoRequestNext::Lookup)
            .unwrap(),
        lookup
    );
    assert_eq!(
        nodes.node_next(icmp_error, IcmpErrorNext::Drop).unwrap(),
        drop
    );
    assert_eq!(
        nodes
            .node_next(reassembly, IpReassemblyNext::Input)
            .unwrap(),
        input
    );
    assert_eq!(nodes.node_next_slot(icmp_input, 0).unwrap(), drop);
}
