//! Service packet graph: linkme `SERVICE_GRAPH_NODES` for graph registration.
//! Control-plane init migrated to `#[init_function]` in the init system.

use hammer_component_macros::worker_init_function;
use hammer_core::config::Config;
use hammer_core::data_plane::NodeHandle;
use hammer_core::error::HammerResult;
use hammer_infra::vec::Vec;
use hammer_runtime::Engine;
use hammer_runtime::NodeEntry;

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [NodeEntry] = [..];

const TCP_TYPED_WORKER_GRAPH_NODES: [&str; 7] = [
    "session-queue",
    "tcp-output",
    "tcp-input",
    "tcp-listen",
    "tcp-established",
    "tcp-rcv-process",
    "tcp-syn-sent",
];

fn deferred_worker_graph_nodes() -> Vec<NodeEntry> {
    let mut entries = Vec::with_capacity(SERVICE_GRAPH_NODES.len());
    for entry in SERVICE_GRAPH_NODES.iter().copied() {
        if entry
            .registration
            .name()
            .is_none_or(|name| !TCP_TYPED_WORKER_GRAPH_NODES.contains(&name))
        {
            entries.push(entry);
        }
    }
    entries
}

#[worker_init_function(name = "install_worker_graph")]
pub fn install_worker_graph(engine: &mut Engine) -> HammerResult<()> {
    let config = engine.registry.require::<Config>()?;
    let handle = NodeHandle::new(config.worker.handoff.node_handle);
    engine.runtime.set_handoff_node_handle(handle);

    let worker = engine.thread_index as usize;
    crate::transport::tcp::wire_worker_graph(&engine.runtime, worker)?;
    let entries = deferred_worker_graph_nodes();
    engine.runtime.init_graph(worker, entries.as_slice())?;
    crate::net::wire_ip_lookup_drop(&engine.runtime)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_graph_contains_tcp_nodes() {
        let names: Vec<&'static str> = SERVICE_GRAPH_NODES
            .iter()
            .filter_map(|e| e.registration.name())
            .collect();
        for want in [
            "drop",
            "handoff",
            "ip-lookup",
            "tcp-input",
            "tcp-listen",
            "tcp-established",
            "tcp-rcv-process",
            "tcp-syn-sent",
            "tcp-output",
            "session-queue",
        ] {
            assert!(names.iter().any(|n| *n == want), "missing {want}");
        }
    }

    #[test]
    fn tcp_typed_worker_graph_nodes_are_filtered_once() {
        let deferred = deferred_worker_graph_nodes();
        for name in TCP_TYPED_WORKER_GRAPH_NODES {
            assert_eq!(
                SERVICE_GRAPH_NODES
                    .iter()
                    .filter(|entry| entry.registration.name() == Some(name))
                    .count(),
                1,
                "expected one static registration for {name}"
            );
            assert!(
                deferred
                    .iter()
                    .all(|entry| entry.registration.name() != Some(name)),
                "typed TCP worker graph node {name} must not be registered twice"
            );
        }
    }
}
