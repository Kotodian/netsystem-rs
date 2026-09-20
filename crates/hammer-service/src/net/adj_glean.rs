use std::collections::HashMap;

use hammer_core::data_plane::NodeId;
use hammer_runtime::DataPlaneMain;

use super::NetMain;
use super::adj::{AdjacencyIndex, AdjacencyLookupNext, AdjacencyMain, FibProtocol};
use super::fib_node::{FibNodeBackWalkContext, FibNodeMain, FibWalkReason};

pub struct AdjacencyGleanMain<P: FibProtocol> {
    tables: HashMap<u32, HashMap<P::Address, AdjacencyIndex>>,
    node: NodeId,
}

impl<P: FibProtocol> AdjacencyGleanMain<P> {
    pub fn new(node: NodeId) -> Self {
        Self {
            tables: HashMap::new(),
            node,
        }
    }

    pub fn find(&self, sw_if_index: u32, network: P::Address) -> Option<AdjacencyIndex> {
        self.tables.get(&sw_if_index)?.get(&network).copied()
    }

    pub fn add_or_lock(
        &mut self,
        main: &mut DataPlaneMain,
        adjacency: &mut AdjacencyMain<P>,
        link: P::Link,
        sw_if_index: u32,
        connected: P::Prefix,
        mtu: u16,
    ) -> AdjacencyIndex {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("glean publication requires the main-thread barrier");
        let network = P::prefix_address(connected);
        let index = match self.find(sw_if_index, network) {
            Some(index) => index,
            None => {
                let index = adjacency.insert(
                    P::glean_subtype(connected),
                    link,
                    sw_if_index,
                    mtu,
                    self.node.slot(),
                    AdjacencyLookupNext::Glean,
                );
                let interfaces =
                    NetMain::global().expect("glean rewrite requires the network Main");
                let object = adjacency.get_mut(index);
                object.rewrite.init(main, self.node, P::dpo_protocol(link));
                let length = P::build_rewrite(
                    interfaces.interface_main(),
                    sw_if_index,
                    link,
                    None,
                    &mut object.rewrite.data,
                );
                object.rewrite.data_bytes =
                    u16::try_from(length).expect("adjacency rewrite length fits its storage");
                self.tables
                    .entry(sw_if_index)
                    .or_default()
                    .insert(network, index);
                index
            }
        };
        adjacency.node_lock(index);
        NetMain::global()
            .expect("glean delegate requires the network Main")
            .adjacency_delegates_mut()
            .created(index);
        index
    }

    pub fn update_source(
        &mut self,
        adjacency: &mut AdjacencyMain<P>,
        index: AdjacencyIndex,
        source: P::Address,
    ) {
        P::set_glean_source(&mut adjacency.get_mut(index).subtype, source);
    }

    pub fn indices(&self, sw_if_index: u32) -> Vec<AdjacencyIndex> {
        self.tables
            .get(&sw_if_index)
            .map(|table| table.values().copied().collect())
            .unwrap_or_default()
    }

    pub fn interface_walk(
        &self,
        main: &mut DataPlaneMain,
        nodes: &mut FibNodeMain,
        adjacency: &AdjacencyMain<P>,
        sw_if_index: u32,
        reason: FibWalkReason,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("glean interface walk requires publication ownership");
        for index in self.indices(sw_if_index) {
            nodes.walk_sync(
                main,
                adjacency.node(index),
                &mut FibNodeBackWalkContext::new(reason),
            );
        }
    }

    pub fn remove(&mut self, adjacency: &AdjacencyMain<P>, index: AdjacencyIndex) {
        let object = adjacency.get(index);
        assert_eq!(object.lookup_next, AdjacencyLookupNext::Glean);
        let sw_if_index = object.rewrite.sw_if_index;
        let connected = P::glean_prefix(&object.subtype);
        let table = self
            .tables
            .get_mut(&sw_if_index)
            .expect("glean interface table must exist");
        assert_eq!(table.remove(&P::prefix_address(connected)), Some(index));
        if table.is_empty() {
            self.tables.remove(&sw_if_index);
        }
    }
}
