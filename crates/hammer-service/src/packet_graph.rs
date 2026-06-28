//! Service packet graph: linkme `SERVICE_GRAPH_NODES` for graph registration.
//! Control-plane init migrated to `#[init_function]` in the init system.

use hammer_adapter::NodeEntry;

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [NodeEntry] = [..];

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
