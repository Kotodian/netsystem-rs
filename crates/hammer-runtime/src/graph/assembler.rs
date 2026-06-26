//! Config-driven packet-graph assembler (VPP `vlib` semantics, simplified).
//!
//! VPP resolves name-based edges in a separate `vlib_node_main_init` phase,
//! but Hammer's `NodeRuntime` validates every `initial_nexts` `NodeId` at
//! registration time (`node.rs:738`) and requires `initial_nexts.len() ==
//! next_count` (`node.rs:751`). Deferred placeholder edges are therefore not
//! expressible on the current registration surface. The existing
//! `install_service_packet_graph_on_workers` already registers in dependency
//! order (no cycles), so this assembler mirrors that: it invokes builder fns
//! in spec order, and each builder resolves its own next edges by name via
//! `NodeCtx::node(name)` against nodes registered earlier in the same pass.
//! Config selects which registered builders participate and in what order; it
//! never defines new node types (VPP invariant).
//!
//! `D` is the service-defined dependency bag; the graph layer is generic over
//! it and never names its fields.

use std::collections::HashMap;

use hammer_adapter::{NodeId, NodeRuntime};
use hammer_core::error::{CoreError, CoreResult};

use super::{NodeBuilder, NodeCtx, NodeRegistry};

/// A declarative packet-graph specification: which registered builders to
/// invoke, in order. Edges are resolved inside each builder via
/// `NodeCtx::node(name)` against nodes registered earlier in the same pass,
/// so the spec only needs the builder names in dependency order.
#[derive(Debug, Clone, Default)]
pub struct GraphSpec {
    pub nodes: Vec<&'static str>,
}

impl GraphSpec {
    pub fn from_names(names: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            nodes: names.into_iter().collect(),
        }
    }
}

/// Result of assembling a `GraphSpec` on one worker runtime: the `NodeId`s
/// produced, in spec order.
#[derive(Debug, Clone, Default)]
pub struct AssembledGraph {
    pub nodes: Vec<NodeId>,
}

/// Assembles a [`GraphSpec`] onto worker runtimes using a [`NodeRegistry`].
pub struct PacketGraphAssembler<'a, D> {
    registry: &'a NodeRegistry<D>,
    spec: GraphSpec,
}

impl<'a, D> PacketGraphAssembler<'a, D> {
    pub fn new(registry: &'a NodeRegistry<D>, spec: GraphSpec) -> Self {
        Self { registry, spec }
    }

    /// Assemble on one worker: invoke each spec builder fn in order. Each
    /// builder resolves its own next edges via `ctx.node(name)` against nodes
    /// registered earlier in this pass and registers the node, returning its
    /// `NodeId`. `deps` carries service-owned shared dependencies; the graph
    /// layer never names their type.
    pub fn assemble_on(
        &self,
        runtime: &'a NodeRuntime,
        worker_id: usize,
        deps: &'a D,
    ) -> CoreResult<AssembledGraph> {
        let mut resolved: HashMap<&'static str, NodeId> =
            HashMap::with_capacity(self.spec.nodes.len());
        let mut nodes = Vec::with_capacity(self.spec.nodes.len());
        for name in &self.spec.nodes {
            let ctx = NodeCtx::new(runtime, worker_id, &resolved, deps);
            let builder: NodeBuilder<D> = self.registry.get(name)?;
            let id = builder(&ctx)
                .map_err(|err| CoreError::internal(format!("assemble node `{name}`: {err}")))?;
            resolved.insert(name, id);
            nodes.push(id);
        }
        Ok(AssembledGraph { nodes })
    }
}
