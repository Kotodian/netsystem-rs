use super::*;

impl DataPlaneMain {
    pub fn init_graph(&self, entries: &[NodeEntry]) -> RuntimeResult<()> {
        let node_functions = crate::builtin_registration_image()
            .node_functions()
            .to_vec();
        self.init_graph_with_node_functions(entries, &node_functions)
    }

    pub fn init_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        let owners = entries
            .iter()
            .filter(|entry| !matches!(entry.registration, Some(NodeRegistration::Sibling { .. })));
        let siblings = entries
            .iter()
            .filter(|entry| matches!(entry.registration, Some(NodeRegistration::Sibling { .. })));
        let mut nodes = Vec::with_capacity(entries.len());
        for entry in owners.chain(siblings) {
            let node =
                (entry.init)(self).map_err(|source| RuntimeError::GraphNodeInitialization {
                    node: entry
                        .registration
                        .map(NodeRegistration::name)
                        .unwrap_or("?"),
                    source: Box::new(source),
                })?;
            nodes.push((node, entry.error_counters));
        }
        self.nodes.validate_node_error_batch(&nodes)?;
        for (node, error_counters) in nodes {
            self.nodes.materialize_node_errors(node, error_counters)?;
            self.nodes
                .install_node_function(node, self.simd_bytes, node_functions)?;
        }
        self.nodes.resolve_named_next_nodes()
    }

    pub(crate) fn extend_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        let mut nodes = Vec::with_capacity(entries.len());
        for register_siblings in [false, true] {
            for entry in entries {
                let is_sibling =
                    matches!(entry.registration, Some(NodeRegistration::Sibling { .. }));
                if is_sibling != register_siblings {
                    continue;
                }
                let name = entry
                    .registration
                    .map(NodeRegistration::name)
                    .ok_or(DataPlaneError::UnnamedGraphRegistration)?;
                if self.nodes.node_by_name(name).is_some() {
                    continue;
                }
                let node = (entry.init)(self)?;
                nodes.push((node, entry.error_counters));
            }
        }
        self.nodes.validate_node_error_batch(&nodes)?;
        for (node, error_counters) in nodes {
            self.nodes.materialize_node_errors(node, error_counters)?;
            self.nodes
                .install_node_function(node, self.simd_bytes, node_functions)?;
        }
        self.nodes.resolve_named_next_nodes()?;
        Ok(())
    }

    /// Global Graph Transaction: drain residual scheduled frames, detach the
    /// live topology, rebuild and renumber from `entries`, then publish the
    /// updated topology to workers.
    ///
    /// This is a graph transaction, not a plugin unload operation; it neither
    /// changes the registration authority nor releases DSO handles. Business
    /// state must rebind by name, not `NodeId`.
    pub fn rebuild_graph(&self, entries: &[NodeEntry]) -> RuntimeResult<()> {
        let node_functions = crate::builtin_registration_image()
            .node_functions()
            .to_vec();
        self.rebuild_graph_with_node_functions(entries, &node_functions)
    }

    pub fn rebuild_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        self.set_current_node(None);
        self.nodes.detach_graph_for_rebuild()?;
        self.init_graph_with_node_functions(entries, node_functions)
    }
}
