//! Service packet graph: linkme `SERVICE_GRAPH_NODES` for graph registration.
//! Control-plane init migrated to `#[init_function]` in the init system.

use std::sync::OnceLock;

use hammer_adapter::NodeEntry;
use hammer_component_macros::worker_init_function;
use hammer_core::data_plane::NodeHandle;
use hammer_core::error::HammerResult;
use hammer_runtime::Engine;

pub(crate) static WORKER_HANDOFF_NODE_HANDLE: OnceLock<NodeHandle> = OnceLock::new();

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [NodeEntry] = [..];

#[worker_init_function(name = "install_worker_graph")]
pub fn install_worker_graph(engine: &mut Engine) -> HammerResult<()> {
    let handle = *WORKER_HANDOFF_NODE_HANDLE.get().ok_or_else(|| {
        hammer_core::error::CoreError::internal("install_worker_graph: handoff node handle not set")
    })?;

    engine.runtime.set_handoff_node_handle(handle);

    let worker = engine.thread_index as usize;
    let worker_id = hammer_adapter::DataWorkerId::new(engine.thread_index);

    crate::transport::tcp::install_tcp_worker_state(
        crate::transport::tcp::TcpWorkerOwnedState::new(worker_id),
    );
    engine.runtime.init_graph(worker, &SERVICE_GRAPH_NODES)?;
    crate::net::wire_ip_lookup_drop(&engine.runtime)?;
    crate::transport::tcp::wire_worker_graph(&engine.runtime, worker)?;
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
            "session-queue",
        ] {
            assert!(names.iter().any(|n| *n == want), "missing {want}");
        }
    }
}
