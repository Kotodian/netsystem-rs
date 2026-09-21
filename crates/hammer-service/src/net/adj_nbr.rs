use std::collections::HashMap;

use hammer_core::data_plane::NodeId;
use hammer_runtime::DataPlaneMain;

use super::NetMain;
use super::adj::{AdjacencyFlags, AdjacencyIndex, AdjacencyLookupNext, AdjacencyMain, FibProtocol};
use super::fib_node::{
    FibNodeBackWalkContext, FibNodeMain, FibWalkFlags, FibWalkPriority, FibWalkReason,
};

pub struct AdjacencyNeighborMain<P: FibProtocol> {
    tables: HashMap<u32, HashMap<(P::Address, P::Link), AdjacencyIndex>>,
    incomplete_node: NodeId,
    rewrite_node: NodeId,
}

impl<P: FibProtocol> AdjacencyNeighborMain<P> {
    pub fn new(incomplete_node: NodeId, rewrite_node: NodeId) -> Self {
        Self {
            tables: HashMap::new(),
            incomplete_node,
            rewrite_node,
        }
    }

    pub fn find(
        &self,
        link: P::Link,
        address: P::Address,
        sw_if_index: u32,
    ) -> Option<AdjacencyIndex> {
        self.tables
            .get(&sw_if_index)?
            .get(&(address, link))
            .copied()
    }

    pub fn add_or_lock(
        &mut self,
        main: &mut DataPlaneMain,
        adjacency: &mut AdjacencyMain<P>,
        link: P::Link,
        address: P::Address,
        sw_if_index: u32,
    ) -> AdjacencyIndex {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("neighbor publication requires the main-thread barrier");
        let index = match self.find(link, address, sw_if_index) {
            Some(index) => index,
            None => {
                let index = adjacency.insert(
                    P::neighbor_subtype(address),
                    link,
                    self.incomplete_node.slot(),
                    AdjacencyLookupNext::Incomplete,
                );
                let interfaces = NetMain::global()
                    .expect("neighbor rewrite requires the network Main")
                    .interface_main();
                let target = interfaces.tx_node_index_for_sw_interface(sw_if_index);
                adjacency.get_mut(index).rewrite_header.init(
                    main,
                    sw_if_index,
                    P::mtu_kind(link),
                    self.incomplete_node,
                    target,
                );
                let object = adjacency.get_mut(index);
                object.rewrite_header.clear_data(&mut object.rewrite_data);
                self.tables
                    .entry(sw_if_index)
                    .or_default()
                    .insert((address, link), index);
                NetMain::global()
                    .expect("neighbor delegate requires the network Main")
                    .adjacency_delegates_mut()
                    .created(index);
                index
            }
        };
        adjacency.node_lock(index);
        index
    }

    pub fn indices(&self, sw_if_index: u32) -> Vec<AdjacencyIndex> {
        self.tables
            .get(&sw_if_index)
            .map(|table| table.values().copied().collect())
            .unwrap_or_default()
    }

    pub fn next_hop_indices(&self, sw_if_index: u32, address: P::Address) -> Vec<AdjacencyIndex> {
        self.tables
            .get(&sw_if_index)
            .map(|table| {
                table
                    .iter()
                    .filter(|((next_hop, _), _)| *next_hop == address)
                    .map(|(_, index)| *index)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn interface_walk(
        &self,
        main: &mut DataPlaneMain,
        nodes: &mut FibNodeMain,
        adjacency: &mut AdjacencyMain<P>,
        sw_if_index: u32,
        reason: FibWalkReason,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("neighbor interface walk requires publication ownership");
        for index in self.indices(sw_if_index) {
            let node = adjacency.node(index);
            if reason == FibWalkReason::ADJ_MTU {
                let object = adjacency.get_mut(index);
                let interfaces = NetMain::global().expect("MTU walk requires the network Main");
                object
                    .rewrite_header
                    .update_mtu(interfaces.interface_main(), P::mtu_kind(object.link));
                interfaces
                    .adjacency_delegates_mut()
                    .modified(adjacency, index);
                nodes.walk_async(
                    main,
                    node,
                    FibWalkPriority::Low,
                    FibNodeBackWalkContext::new(reason),
                );
                continue;
            }
            nodes.lock(node);
            adjacency
                .get_mut(index)
                .flags
                .insert(AdjacencyFlags::SYNC_WALK_ACTIVE);
            let mut context = FibNodeBackWalkContext::new(reason);
            if reason == FibWalkReason::INTERFACE_DOWN {
                context.flags.insert(FibWalkFlags::FORCE_SYNC);
            }
            nodes.walk_sync(main, node, &mut context);
            adjacency
                .get_mut(index)
                .flags
                .remove(AdjacencyFlags::SYNC_WALK_ACTIVE);
            nodes.unlock(node);
        }
    }

    pub fn update_rewrite(
        &mut self,
        main: &mut DataPlaneMain,
        nodes: &mut FibNodeMain,
        adjacency: &mut AdjacencyMain<P>,
        index: AdjacencyIndex,
        complete: bool,
        bytes: &[u8],
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("neighbor rewrite publication requires the main-thread barrier");
        let node = adjacency.node(index);
        let old = adjacency.get(index).lookup_next;
        assert_ne!(old, AdjacencyLookupNext::Glean);
        let new = if complete {
            AdjacencyLookupNext::Rewrite
        } else {
            AdjacencyLookupNext::Incomplete
        };
        if old != new {
            nodes.lock(node);
            adjacency
                .get_mut(index)
                .flags
                .insert(AdjacencyFlags::SYNC_WALK_ACTIVE);
            let mut context = FibNodeBackWalkContext::new(FibWalkReason::ADJ_DOWN);
            context.flags.insert(FibWalkFlags::FORCE_SYNC);
            nodes.walk_sync(main, node, &mut context);
        }
        let object = adjacency.get_mut(index);
        if complete {
            let interfaces = NetMain::global()
                .expect("neighbor rewrite requires the network Main")
                .interface_main();
            let target =
                interfaces.tx_node_index_for_sw_interface(object.rewrite_header.sw_if_index);
            object.rewrite_header.init(
                main,
                object.rewrite_header.sw_if_index,
                P::mtu_kind(object.link),
                self.rewrite_node,
                target,
            );
            object
                .rewrite_header
                .set_data(&mut object.rewrite_data, bytes);
            object.node_index = self.rewrite_node.slot();
            object.lookup_next = AdjacencyLookupNext::Rewrite;
        } else {
            object.rewrite_header.clear_data(&mut object.rewrite_data);
            object.node_index = self.incomplete_node.slot();
            object.lookup_next = AdjacencyLookupNext::Incomplete;
        }
        if old != new {
            nodes.walk_sync(
                main,
                node,
                &mut FibNodeBackWalkContext::new(FibWalkReason::ADJ_UPDATE),
            );
            adjacency
                .get_mut(index)
                .flags
                .remove(AdjacencyFlags::SYNC_WALK_ACTIVE);
        }
        NetMain::global()
            .expect("neighbor delegate requires the network Main")
            .adjacency_delegates_mut()
            .modified(adjacency, index);
        if old != new {
            nodes.unlock(node);
        }
    }

    pub fn remove(&mut self, adjacency: &AdjacencyMain<P>, index: AdjacencyIndex) {
        let object = adjacency.get(index);
        assert_ne!(object.lookup_next, AdjacencyLookupNext::Glean);
        let sw_if_index = object.rewrite_header.sw_if_index;
        let address = P::neighbor_address(&object.subtype);
        let Some(table) = self.tables.get_mut(&sw_if_index) else {
            return;
        };
        table.remove(&(address, object.link));
        if table.is_empty() {
            self.tables.remove(&sw_if_index);
        }
    }
}
