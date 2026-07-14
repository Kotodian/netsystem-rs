use hammer_core::config::Config;
use hammer_core::data_plane::{NodeKind, NodeRegistration, NodeState};
use hammer_plugin_ip::IpReassemblyExpireWalk;
use hammer_runtime::{GRAPH_NODES, new_worker_runtime};

fn expire_walk_entry() -> &'static hammer_runtime::NodeEntry {
    GRAPH_NODES
        .iter()
        .find(|entry| entry.registration.name() == Some(IpReassemblyExpireWalk::NODE_NAME))
        .expect("ip-reassembly-expire-walk")
}

#[test]
fn reassembly_expiry_is_an_ip_driver_node() {
    let expire_walk = expire_walk_entry();

    assert_eq!(expire_walk.kind, NodeKind::Driver);
    assert_eq!(
        expire_walk.registration,
        NodeRegistration::next("ip-reassembly-expire-walk", 0)
    );
    assert_eq!(expire_walk.plugin, Some("ip"));
}

#[test]
fn reassembly_expiry_runs_through_polling_driver_dispatch() {
    let runtime = new_worker_runtime(&Config::default()).expect("create runtime");
    let node = (expire_walk_entry().init)(&runtime, 0).expect("register expire walk");

    assert_eq!(runtime.nodes().node_kind(node).unwrap(), NodeKind::Driver);
    assert_eq!(
        runtime.nodes().node_state(node).unwrap(),
        NodeState::Polling
    );
    assert_eq!(runtime.schedule_polling_driver_nodes().unwrap(), 1);
    assert_eq!(runtime.run_ready_nodes().unwrap(), 1);
}
