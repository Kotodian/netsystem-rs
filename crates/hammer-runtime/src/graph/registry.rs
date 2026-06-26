//! Name-keyed node-builder inventory (VPP `VLIB_REGISTER_NODE` analogue).
//!
//! One node type = one builder fn registered under a name. VPP registers a
//! `vlib_node_t` per node; Hammer registers a
//! `fn(&NodeCtx<'_, D>) -> CoreResult<NodeId>` per node. The builder constructs
//! the node (resolving next edges by name against nodes registered earlier in
//! the same pass) and registers it on the worker's `NodeRuntime`, returning
//! its `NodeId`.
//!
//! `D` is the service-defined dependency bag carried through assembly. The
//! graph layer is generic over `D` and never names its fields — no trait
//! object, no `dyn Any`. Service code concretizes it (e.g.
//! `NodeRegistry<ServiceGraphDeps>`).

use std::collections::HashMap;

use hammer_adapter::{NodeId, NodeRuntime};
use hammer_core::error::{CoreError, CoreResult};

/// Per-worker context handed to a node builder fn during assembly.
///
/// Carries the worker's `NodeRuntime`, the 0-based worker index, the map of
/// nodes already registered earlier in this assembly pass (name → `NodeId`),
/// and the service-defined dependency bag `D`. The graph layer is generic
/// over `D` and never names its contents; builders downcast by reading `D`'s
/// concrete fields directly.
pub struct NodeCtx<'a, D> {
    runtime: &'a NodeRuntime,
    worker_id: usize,
    resolved: &'a HashMap<&'static str, NodeId>,
    deps: &'a D,
}

impl<'a, D> NodeCtx<'a, D> {
    #[inline]
    pub fn new(
        runtime: &'a NodeRuntime,
        worker_id: usize,
        resolved: &'a HashMap<&'static str, NodeId>,
        deps: &'a D,
    ) -> Self {
        Self {
            runtime,
            worker_id,
            resolved,
            deps,
        }
    }

    #[inline]
    pub fn runtime(&self) -> &'a NodeRuntime {
        self.runtime
    }

    #[inline]
    pub fn worker_id(&self) -> usize {
        self.worker_id
    }

    /// Look up a node registered earlier in this assembly pass by name.
    /// Builders use this to wire next edges in dependency order.
    #[inline]
    pub fn node(&self, name: &str) -> CoreResult<NodeId> {
        self.resolved
            .get(name)
            .copied()
            .ok_or_else(|| CoreError::internal(format!("node `{name}` not registered yet")))
    }

    /// Shared, service-owned dependencies. Builders read `D`'s concrete
    /// fields directly; the graph layer never names them.
    #[inline]
    pub fn deps(&self) -> &'a D {
        self.deps
    }
}

/// Builder function type registered per node type, parameterized by the
/// service dependency bag `D`.
pub type NodeBuilder<D> = fn(&NodeCtx<'_, D>) -> CoreResult<NodeId>;

/// Frozen, name-keyed inventory of node builders, parameterized by the
/// service dependency bag `D`.
///
/// Built once at startup, shared read-only across workers (VPP
/// `vlib_global_main_t.node_registrations` analogue). Lookup is by the
/// builder's registered name.
///
/// `std::collections::HashMap` deliberately: control-path, build-once /
/// read-many, small (~tens) `&'static str` key set. `hammer-infra`'s
/// `FlatHashKey` map targets `Copy` integer keys on the data path and does
/// not fit string keys.
pub struct NodeRegistry<D> {
    builders: HashMap<&'static str, NodeBuilder<D>>,
}

impl<D> NodeRegistry<D> {
    pub fn new() -> Self {
        Self {
            builders: HashMap::new(),
        }
    }

    /// Register a builder under `name`. Errors if the name is already taken.
    pub fn register(&mut self, name: &'static str, builder: NodeBuilder<D>) -> CoreResult<()> {
        if self.builders.insert(name, builder).is_some() {
            return Err(CoreError::internal("node builder already registered"));
        }
        Ok(())
    }

    /// Look up a builder by name.
    #[inline]
    pub fn get(&self, name: &str) -> CoreResult<NodeBuilder<D>> {
        self.builders
            .get(name)
            .copied()
            .ok_or_else(|| CoreError::internal("node builder not registered"))
    }

    /// Iterate registered (name, builder) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, NodeBuilder<D>)> {
        self.builders.iter().map(|(name, builder)| (*name, *builder))
    }
}

impl<D> Default for NodeRegistry<D> {
    fn default() -> Self {
        Self::new()
    }
}
