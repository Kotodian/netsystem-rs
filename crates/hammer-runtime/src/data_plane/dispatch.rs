use super::*;
use crate::thread_main::ThreadMain;
use hammer_stats::{DirectoryType, StatsError, StatsMain};

/// The counter vector every registered node error column is published through
/// (VPP `vlib_stats_add_counter_vector ("/node/errors")`, `error.c:158-159`).
const NODE_ERROR_COUNTERS_NAME: &str = "/node/errors";

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
                entry.install_process_node_index(node)?;
                nodes.push((node, entry.error_counters));
                processes.push(entry.process);
            }
        }
        self.nodes.validate_node_error_batch(&nodes)?;
        for ((node, error_counters), process) in nodes.into_iter().zip(processes) {
            self.register_node_errors(node, error_counters)?;
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
            self.register_node_errors(node, error_counters)?;
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

    /// Register a node's ordered error descriptors and publish them in the
    /// stats segment.
    ///
    /// VPP analogue: `vlib_register_errors`
    /// (`third_party/vpp/src/vlib/error.c:113-200`) — thread zero reserves the
    /// node's contiguous column range, lazily adds the `/node/errors` counter
    /// vector on the first node that declares errors, publishes the shape for
    /// one row per runtime thread, and aliases every error as
    /// `/err/<node>/<error>`.
    ///
    /// Registration is a startup/plugin-load step: it must complete before the
    /// freeze point (`crate::start_workers`) installs the per-thread entries,
    /// because `validate` may grow the row vectors and must not race a record.
    pub fn register_node_errors(
        &self,
        node: NodeId,
        descriptors: &[NodeErrorDescriptor],
    ) -> RuntimeResult<()> {
        self.nodes.register_node_errors(node, descriptors)?;
        if descriptors.is_empty() {
            return Ok(());
        }
        let name = self
            .nodes
            .node_name(node)?
            .expect("a node that declares errors is named");
        assert!(
            !name.contains('/'),
            "a node error alias path segment must not contain '/': {name}"
        );

        let stats_main = match StatsMain::global() {
            Ok(stats_main) => stats_main,
            // A runtime built outside the daemon lifecycle (unit tests, benches)
            // has no segment to publish into. The node domain still owns the
            // column range, and the record path counts nothing because no entry
            // was installed — the same absence as a process whose nodes declare
            // no errors.
            Err(StatsError::NotInitialized) => return Ok(()),
            Err(source) => return Err(RuntimeError::Stats(source)),
        };
        let segment = &stats_main.segment;
        let entry = match self.node_error_stats_entry_index.get() {
            Some(entry) => entry,
            None => {
                let entry = match segment
                    .find(NODE_ERROR_COUNTERS_NAME, DirectoryType::CounterVectorSimple)
                {
                    Ok(entry) => entry,
                    Err(StatsError::MetricNotFound { .. }) => {
                        segment.add_simple_counter(NODE_ERROR_COUNTERS_NAME)?.index
                    }
                    Err(source) => return Err(RuntimeError::Stats(source)),
                };
                self.node_error_stats_entry_index.set(Some(entry));
                entry
            }
        };

        // One row per runtime thread that owns a node graph: thread zero plus
        // the Data Workers, exactly like every other per-thread vector.
        let rows = ThreadMain::global().worker_count() + 1;
        let columns = self.nodes.node_error_columns();
        segment.validate(entry, rows - 1, columns - 1)?;

        for (local_code, descriptor) in descriptors.iter().enumerate() {
            let local_code = u16::try_from(local_code)
                .expect("a registered node error code fits its u16 local space");
            let index = self.nodes.node_error_index(node, local_code)?;
            let path = format!("/err/{name}/{}", descriptor.name);
            match segment.find(&path, DirectoryType::Symlink) {
                Ok(_) => {}
                Err(StatsError::MetricNotFound { .. }) => {
                    segment.add_symlink(entry, u32::from(index.get()), &path)?;
                }
                Err(source) => return Err(RuntimeError::Stats(source)),
            }
        }
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
