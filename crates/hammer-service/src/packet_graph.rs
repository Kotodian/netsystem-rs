//! Service packet graph: linkme `SERVICE_GRAPH_NODES` + control-plane init
//! registry. No Boot, no TLS, no free assemble functions.

use hammer_adapter::NodeEntry;
use hammer_core::error::HammerResult;
use hammer_core::registry::RuntimeRegistry;

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [NodeEntry] = [..];

#[linkme::distributed_slice]
pub static CONTROL_INITS: [fn(&RuntimeRegistry) -> HammerResult<()>] = [..];

pub fn init_control_planes(reg: &RuntimeRegistry) -> HammerResult<()> {
    for init in CONTROL_INITS {
        init(reg)?;
    }
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
