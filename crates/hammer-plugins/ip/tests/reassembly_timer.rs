use hammer_plugin_ip::IpReassemblyNode;
use hammer_runtime::PROCESS_NODES;

#[test]
fn reassembly_expiry_is_a_main_process_node() {
    let _ = core::mem::size_of::<IpReassemblyNode>();
    let process = PROCESS_NODES
        .iter()
        .find(|process| process.name == "ip-reassembly-expire-walk")
        .expect("IP reassembly expiry process");

    assert_eq!(process.plugin, Some("ip"));
}
