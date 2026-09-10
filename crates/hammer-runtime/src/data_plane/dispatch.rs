use super::*;

impl DataPlaneMain {
    pub fn init_graph(&self, entries: &[NodeEntry]) -> RuntimeResult<()> {
        let node_functions = crate::builtin_registration_image()
            .node_functions()
            .collect::<Vec<_>>();
        self.init_graph_from_declarations(entries.iter(), node_functions.into_iter())
    }

    pub fn init_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        self.init_graph_from_declarations(entries.iter(), node_functions.iter())
    }

    pub(crate) fn init_graph_from_declarations<'entry, 'function>(
        &self,
        entries: impl Clone + Iterator<Item = &'entry NodeEntry>,
        node_functions: impl Clone + Iterator<Item = &'function NodeFunctionRegistration>,
    ) -> RuntimeResult<()> {
        let mut nodes = Vec::with_capacity(entries.clone().count());
        let mut processes = Vec::with_capacity(nodes.capacity());
        for register_siblings in [false, true] {
            for entry in entries.clone() {
                let is_sibling =
                    matches!(entry.registration, Some(NodeRegistration::Sibling { .. }));
                if is_sibling != register_siblings {
                    continue;
                }
                let node =
                    (entry.init)(self).map_err(|source| RuntimeError::GraphNodeInitialization {
                        node: entry
                            .registration
                            .map(NodeRegistration::name)
                            .unwrap_or("?"),
                        source: Box::new(source),
                    })?;
                nodes.push((node, entry.error_counters));
                processes.push(entry.process);
            }
        }
        self.nodes.validate_node_error_batch(&nodes)?;
        for ((node, error_counters), process) in nodes.into_iter().zip(processes) {
            self.nodes.materialize_node_errors(node, error_counters)?;
            self.nodes.install_node_function(
                node,
                self.simd_bytes,
                node_functions.clone(),
                process,
            )?;
        }
        self.nodes.resolve_named_next_nodes()
    }

    pub(crate) fn extend_graph_with_node_functions(
        &self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        let mut nodes = Vec::with_capacity(entries.len());
        let mut processes = Vec::with_capacity(entries.len());
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
                processes.push(entry.process);
            }
        }
        self.nodes.validate_node_error_batch(&nodes)?;
        for ((node, error_counters), process) in nodes.into_iter().zip(processes) {
            self.nodes.materialize_node_errors(node, error_counters)?;
            self.nodes.install_node_function(
                node,
                self.simd_bytes,
                node_functions.iter(),
                process,
            )?;
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
    pub fn rebuild_graph(&mut self, entries: &[NodeEntry]) -> RuntimeResult<()> {
        let node_functions = crate::builtin_registration_image()
            .node_functions()
            .collect::<Vec<_>>();
        self.nodes.ensure_topology_owner()?;
        assert!(
            self.current_node().is_none(),
            "graph rebuild occurs outside Node dispatch"
        );
        while self.run_ready_nodes()? != 0 {}
        self.nodes.detach_graph_for_rebuild()?;
        self.init_graph_from_declarations(entries.iter(), node_functions.into_iter())
    }

    pub fn rebuild_graph_with_node_functions(
        &mut self,
        entries: &[NodeEntry],
        node_functions: &[NodeFunctionRegistration],
    ) -> RuntimeResult<()> {
        self.nodes.ensure_topology_owner()?;
        assert!(
            self.current_node().is_none(),
            "graph rebuild occurs outside Node dispatch"
        );
        while self.run_ready_nodes()? != 0 {}
        self.nodes.detach_graph_for_rebuild()?;
        self.init_graph_with_node_functions(entries, node_functions)
    }
}
