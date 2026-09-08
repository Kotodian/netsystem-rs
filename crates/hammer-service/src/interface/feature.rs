use std::collections::{BTreeMap, HashMap};

use hammer_core::data_plane::{Buffer, NodeId};
use hammer_infra::{bitmap::Bitmap, pool::Pool};
use hammer_runtime::{DataPlaneMain, NodeMain, RuntimeError};

use super::InterfaceMain;

#[derive(Debug, thiserror::Error)]
pub enum FeatureError {
    #[error("feature arc limit exceeded: {requested}")]
    ArcLimit { requested: usize },
    #[error("feature arc {name} is absent")]
    ArcNotFound { name: &'static str },
    #[error("duplicate feature arc {name}")]
    DuplicateArc { name: &'static str },
    #[error("feature arc {arc} has no start nodes")]
    EmptyStartNodes { arc: &'static str },
    #[error("feature arc {arc} has no features")]
    EmptyArc { arc: &'static str },
    #[error("graph node {name} is absent")]
    NodeNotFound { name: &'static str },
    #[error("graph node {node:?} is absent")]
    GraphNodeNotFound { node: NodeId },
    #[error("duplicate start node {node:?} in arc {arc}")]
    DuplicateStartNode { arc: &'static str, node: NodeId },
    #[error("duplicate feature {feature} in arc {arc}")]
    DuplicateFeature {
        arc: &'static str,
        feature: &'static str,
    },
    #[error("node {node:?} used by multiple features in arc {arc}")]
    FeatureNodeConflict { arc: &'static str, node: NodeId },
    #[error("feature {feature} in arc {arc} references missing target {target}")]
    ConstraintTargetMissing {
        arc: &'static str,
        feature: &'static str,
        target: &'static str,
    },
    #[error("feature ordering cycle in arc {arc}")]
    OrderCycle { arc: &'static str },
    #[error("feature registration is closed")]
    RegistrationClosed,
    #[error("feature arcs are not installed")]
    NotInstalled,
    #[error("feature arc index {arc_index} is invalid")]
    ArcIndexInvalid { arc_index: u8 },
    #[error("feature index {feature_index} is invalid in arc {arc_index}")]
    FeatureIndexInvalid { arc_index: u8, feature_index: u32 },
    #[error("software interface {sw_if_index} is absent")]
    InterfaceNotFound { sw_if_index: u32 },
    #[error("feature count overflow on arc {arc_index}, interface {sw_if_index}")]
    FeatureCountOverflow { arc_index: u8, sw_if_index: u32 },
    #[error("arc {arc_index} start {node:?} next {actual} differs from {expected}")]
    StartNextMismatch {
        arc_index: u8,
        node: NodeId,
        expected: u16,
        actual: u16,
    },
    #[error("node {node:?} next count {requested} exceeds u16")]
    NextSlotOverflow { node: NodeId, requested: usize },
    #[error("feature configuration storage exhausted: {requested_words} words")]
    StorageExhausted { requested_words: usize },
}

#[derive(Default)]
pub(super) struct FeatureState {
    arc_registrations: Vec<FeatureArcRegistration>,
    feature_registrations: Vec<FeatureRegistration>,
    arcs: Vec<FeatureArc>,
    arc_index_by_name: BTreeMap<&'static str, u8>,
    shared_config_heap: Vec<u32>,
    free_config_ranges: BTreeMap<u32, u32>,
    installed: bool,
}

struct FeatureArcRegistration {
    name: &'static str,
    start_nodes: Box<[NodeId]>,
    last_in_arc: Option<&'static str>,
}

struct FeatureRegistration {
    arc_name: &'static str,
    name: &'static str,
    node: NodeId,
    runs_before: Box<[&'static str]>,
    runs_after: Box<[&'static str]>,
}

struct FeatureArc {
    name: &'static str,
    start_nodes: Box<[NodeId]>,
    default_end_node: NodeId,
    feature_nodes: Box<[NodeId]>,
    feature_names: Box<[&'static str]>,
    feature_index_by_name: BTreeMap<&'static str, u32>,
    config_index_by_sw_if_index: Vec<u32>,
    feature_count_by_sw_if_index: Vec<i16>,
    sw_if_index_has_features: Bitmap,
    chains: Pool<FeatureChain>,
    chain_by_words: HashMap<Box<[u32]>, u32>,
}

struct FeatureChain {
    occurrences: Vec<FeatureOccurrence>,
    end_node: NodeId,
    heap_index: u32,
    heap_len: u32,
    reference_count: u32,
}

#[derive(Clone, PartialEq, Eq)]
struct FeatureOccurrence {
    feature_index: u32,
    words: Box<[u32]>,
}

impl InterfaceMain {
    pub fn register_feature_arc(
        &self,
        name: &'static str,
        start_nodes: &[NodeId],
        last_in_arc: Option<&'static str>,
    ) -> Result<u8, FeatureError> {
        hammer_runtime::ensure_main_thread()
            .expect("feature declarations belong to the main thread");
        let state = &mut self.state_mut().feature;
        if state.installed
            || hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0)
        {
            return Err(FeatureError::RegistrationClosed);
        }
        if state.arc_index_by_name.contains_key(name) {
            return Err(FeatureError::DuplicateArc { name });
        }
        if start_nodes.is_empty() {
            return Err(FeatureError::EmptyStartNodes { arc: name });
        }
        for (position, &node) in start_nodes.iter().enumerate() {
            if start_nodes[..position].contains(&node) {
                return Err(FeatureError::DuplicateStartNode { arc: name, node });
            }
        }
        let requested = state.arc_registrations.len() + 1;
        if requested >= 256 {
            return Err(FeatureError::ArcLimit { requested });
        }
        let index = state.arc_registrations.len() as u8;
        state.arc_registrations.push(FeatureArcRegistration {
            name,
            start_nodes: start_nodes.into(),
            last_in_arc,
        });
        state.arc_index_by_name.insert(name, index);
        Ok(index)
    }

    pub fn register_feature(
        &self,
        arc_name: &'static str,
        name: &'static str,
        node: NodeId,
        runs_before: &[&'static str],
        runs_after: &[&'static str],
    ) -> Result<(), FeatureError> {
        hammer_runtime::ensure_main_thread()
            .expect("feature declarations belong to the main thread");
        let state = &mut self.state_mut().feature;
        if state.installed
            || hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0)
        {
            return Err(FeatureError::RegistrationClosed);
        }
        for registration in state
            .feature_registrations
            .iter()
            .filter(|registration| registration.arc_name == arc_name)
        {
            if registration.name == name {
                return Err(FeatureError::DuplicateFeature {
                    arc: arc_name,
                    feature: name,
                });
            }
            if registration.node == node {
                return Err(FeatureError::FeatureNodeConflict {
                    arc: arc_name,
                    node,
                });
            }
        }
        state.feature_registrations.push(FeatureRegistration {
            arc_name,
            name,
            node,
            runs_before: runs_before.into(),
            runs_after: runs_after.into(),
        });
        Ok(())
    }

    pub fn install_feature_arcs(&self, nodes: &NodeMain) -> Result<(), FeatureError> {
        hammer_runtime::ensure_main_thread()
            .expect("feature installation belongs to the main thread");
        let state = &mut self.state_mut().feature;
        if state.installed
            || hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0)
        {
            return Err(FeatureError::RegistrationClosed);
        }
        for feature in &state.feature_registrations {
            if !state.arc_index_by_name.contains_key(feature.arc_name) {
                return Err(FeatureError::ArcNotFound {
                    name: feature.arc_name,
                });
            }
        }
        let mut arcs = Vec::with_capacity(state.arc_registrations.len());
        for registration in &state.arc_registrations {
            let features: Vec<_> = state
                .feature_registrations
                .iter()
                .filter(|feature| feature.arc_name == registration.name)
                .collect();
            if features.is_empty() {
                return Err(FeatureError::EmptyArc {
                    arc: registration.name,
                });
            }
            for &node in registration
                .start_nodes
                .iter()
                .chain(features.iter().map(|feature| &feature.node))
            {
                if nodes.node_kind(node).is_err() {
                    return Err(FeatureError::GraphNodeNotFound { node });
                }
            }
            let mut constraints = vec![Vec::new(); features.len()];
            for (index, feature) in features.iter().enumerate() {
                for (&target, before) in feature
                    .runs_before
                    .iter()
                    .map(|name| (name, true))
                    .chain(feature.runs_after.iter().map(|name| (name, false)))
                {
                    let target_index = features
                        .iter()
                        .position(|feature| feature.name == target)
                        .ok_or(FeatureError::ConstraintTargetMissing {
                        arc: registration.name,
                        feature: feature.name,
                        target,
                    })?;
                    let (from, to) = if before {
                        (index, target_index)
                    } else {
                        (target_index, index)
                    };
                    if !constraints[from].contains(&to) {
                        constraints[from].push(to);
                    }
                }
            }
            if let Some(last) = registration.last_in_arc {
                let last_index = features
                    .iter()
                    .position(|feature| feature.name == last)
                    .ok_or(FeatureError::ConstraintTargetMissing {
                        arc: registration.name,
                        feature: last,
                        target: last,
                    })?;
                for (index, successors) in constraints.iter_mut().enumerate() {
                    if index != last_index && !successors.contains(&last_index) {
                        successors.push(last_index);
                    }
                }
            }
            let mut indegrees = vec![0; features.len()];
            for successors in &constraints {
                for &index in successors {
                    indegrees[index] += 1;
                }
            }
            let mut ordered = Vec::with_capacity(features.len());
            while ordered.len() < features.len() {
                let index = (0..features.len())
                    .find(|index| indegrees[*index] == 0 && !ordered.contains(index))
                    .ok_or(FeatureError::OrderCycle {
                        arc: registration.name,
                    })?;
                ordered.push(index);
                for &next in &constraints[index] {
                    indegrees[next] -= 1;
                }
            }
            let feature_nodes = ordered
                .iter()
                .map(|&index| features[index].node)
                .collect::<Box<[_]>>();
            let feature_names = ordered
                .iter()
                .map(|&index| features[index].name)
                .collect::<Box<[_]>>();
            let feature_index_by_name = feature_names
                .iter()
                .enumerate()
                .map(|(index, &name)| (name, index as u32))
                .collect();
            arcs.push(FeatureArc {
                name: registration.name,
                start_nodes: registration.start_nodes.clone(),
                default_end_node: *feature_nodes.last().expect("nonempty feature ordering"),
                feature_nodes,
                feature_names,
                feature_index_by_name,
                config_index_by_sw_if_index: Vec::new(),
                feature_count_by_sw_if_index: Vec::new(),
                sw_if_index_has_features: Bitmap::default(),
                chains: Pool::new(),
                chain_by_words: HashMap::new(),
            });
        }
        state.arcs = arcs;
        state.installed = true;
        Ok(())
    }

    pub fn feature_arc_index(&self, name: &str) -> Option<u8> {
        self.state().feature.arc_index_by_name.get(name).copied()
    }

    pub fn feature_index(&self, arc_index: u8, name: &str) -> Option<u32> {
        self.state()
            .feature
            .arcs
            .get(usize::from(arc_index))?
            .feature_index_by_name
            .get(name)
            .copied()
    }

    pub fn enable_feature(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
        config: &[u32],
    ) -> Result<(), FeatureError> {
        let (mut occurrences, end_node) =
            self.feature_configuration(arc_index, sw_if_index, Some(feature_index))?;
        if occurrences.len() == i16::MAX as usize {
            return Err(FeatureError::FeatureCountOverflow {
                arc_index,
                sw_if_index,
            });
        }
        occurrences.push(FeatureOccurrence {
            feature_index,
            words: config.into(),
        });
        occurrences.sort_by_key(|occurrence| occurrence.feature_index);
        self.replace_feature_chain(main, arc_index, sw_if_index, occurrences, end_node)
    }

    pub fn disable_feature(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
        config: &[u32],
    ) -> Result<(), FeatureError> {
        let (mut occurrences, end_node) =
            self.feature_configuration(arc_index, sw_if_index, Some(feature_index))?;
        let Some(position) = occurrences.iter().position(|occurrence| {
            occurrence.feature_index == feature_index && occurrence.words.as_ref() == config
        }) else {
            return Ok(());
        };
        occurrences.remove(position);
        self.replace_feature_chain(main, arc_index, sw_if_index, occurrences, end_node)
    }

    pub fn is_feature_enabled(
        &self,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
    ) -> Result<bool, FeatureError> {
        let state = &self.state().feature;
        let arc = state.arc(arc_index)?;
        if feature_index as usize >= arc.feature_nodes.len() {
            return Err(FeatureError::FeatureIndexInvalid {
                arc_index,
                feature_index,
            });
        }
        if self.software_interface(sw_if_index).is_none() {
            return Err(FeatureError::InterfaceNotFound { sw_if_index });
        }
        Ok(state.chain(arc_index, sw_if_index).is_some_and(|chain| {
            chain
                .occurrences
                .iter()
                .any(|occurrence| occurrence.feature_index == feature_index)
        }))
    }

    pub fn modify_feature_arc_end(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
        end_node: NodeId,
    ) -> Result<(), FeatureError> {
        let (occurrences, current_end) =
            self.feature_configuration(arc_index, sw_if_index, None)?;
        if current_end == end_node {
            return Ok(());
        }
        self.replace_feature_chain(main, arc_index, sw_if_index, occurrences, end_node)
    }

    pub fn reset_feature_arc_end(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<(), FeatureError> {
        let end = self.state().feature.arc(arc_index)?.default_end_node;
        self.modify_feature_arc_end(main, arc_index, sw_if_index, end)
    }

    fn feature_configuration(
        &self,
        arc_index: u8,
        sw_if_index: u32,
        feature_index: Option<u32>,
    ) -> Result<(Vec<FeatureOccurrence>, NodeId), FeatureError> {
        hammer_runtime::ensure_main_thread()
            .expect("feature configuration belongs to the main thread");
        let state = &self.state().feature;
        let arc = state.arc(arc_index)?;
        if let Some(feature_index) = feature_index {
            if feature_index as usize >= arc.feature_nodes.len() {
                return Err(FeatureError::FeatureIndexInvalid {
                    arc_index,
                    feature_index,
                });
            }
        }
        if self.software_interface(sw_if_index).is_none() {
            return Err(FeatureError::InterfaceNotFound { sw_if_index });
        }
        Ok(match state.chain(arc_index, sw_if_index) {
            Some(chain) => (chain.occurrences.clone(), chain.end_node),
            None => (Vec::new(), arc.default_end_node),
        })
    }

    fn replace_feature_chain(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
        occurrences: Vec<FeatureOccurrence>,
        end_node: NodeId,
    ) -> Result<(), FeatureError> {
        let nodes = main.nodes().clone();
        if main.nodes().node_kind(end_node).is_err() {
            return Err(FeatureError::GraphNodeNotFound { node: end_node });
        }
        let arc = self.state().feature.arc(arc_index)?;
        let mut edges = Vec::new();
        let first = occurrences.first().map_or(end_node, |occurrence| {
            arc.feature_nodes[occurrence.feature_index as usize]
        });
        for &start in &arc.start_nodes {
            edges.push((start, first));
        }
        let starts = edges.len();
        let mut words = vec![0];
        let mut next_positions = vec![0];
        for (position, occurrence) in occurrences.iter().enumerate() {
            words.extend_from_slice(&occurrence.words);
            let node = arc.feature_nodes[occurrence.feature_index as usize];
            if position + 1 == occurrences.len() && node == end_node {
                break;
            }
            next_positions.push(words.len());
            words.push(0);
            let next = occurrences.get(position + 1).map_or(end_node, |next| {
                arc.feature_nodes[next.feature_index as usize]
            });
            edges.push((node, next));
        }
        words.push(end_node.slot());
        let requested_words = words
            .len()
            .checked_add(1)
            .ok_or(FeatureError::StorageExhausted {
                requested_words: usize::MAX,
            })?;
        let extent_len = u32::try_from(requested_words)
            .map_err(|_| FeatureError::StorageExhausted { requested_words })?;
        let publish = || -> Result<(), FeatureError> {
            let state = &mut self.state_mut().feature;
            let position = sw_if_index as usize;
            let arc = &mut state.arcs[usize::from(arc_index)];
            let additional = (position + 1).saturating_sub(arc.config_index_by_sw_if_index.len());
            arc.config_index_by_sw_if_index
                .try_reserve(additional)
                .map_err(|_| FeatureError::StorageExhausted { requested_words })?;
            arc.feature_count_by_sw_if_index
                .try_reserve(additional)
                .map_err(|_| FeatureError::StorageExhausted { requested_words })?;
            arc.chain_by_words
                .try_reserve(1)
                .map_err(|_| FeatureError::StorageExhausted { requested_words })?;
            let range = state
                .free_config_ranges
                .iter()
                .find(|(_, length)| **length >= extent_len)
                .map(|(&start, &length)| (start, length));
            let extent = if let Some((start, length)) = range {
                state.free_config_ranges.remove(&start);
                if length != extent_len {
                    state
                        .free_config_ranges
                        .insert(start + extent_len, length - extent_len);
                }
                start
            } else {
                let start = u32::try_from(state.shared_config_heap.len())
                    .map_err(|_| FeatureError::StorageExhausted { requested_words })?;
                start
                    .checked_add(extent_len)
                    .ok_or(FeatureError::StorageExhausted { requested_words })?;
                state
                    .shared_config_heap
                    .try_reserve(requested_words)
                    .map_err(|_| FeatureError::StorageExhausted { requested_words })?;
                state
                    .shared_config_heap
                    .resize(state.shared_config_heap.len() + requested_words, 0);
                start
            };
            // Candidate remains unreachable until graph validation succeeds. Pool
            // and BTreeMap allocation use the process Main Heap's fatal OOM policy.
            let candidate = arc.chains.insert(FeatureChain {
                occurrences,
                end_node,
                heap_index: extent + 1,
                heap_len: extent_len,
                reference_count: 1,
            });
            let slots = match nodes.add_node_next_slots(&edges, &[0..starts]) {
                Ok(slots) => slots,
                Err(error) => {
                    arc.chains.remove(candidate);
                    state.free_extent(extent, extent_len);
                    return Err(match error {
                        RuntimeError::NodeNextSlotMismatch {
                            node,
                            expected,
                            actual,
                            ..
                        } => FeatureError::StartNextMismatch {
                            arc_index,
                            node,
                            expected,
                            actual,
                        },
                        RuntimeError::NodeNextCountOverflow { count } => {
                            FeatureError::NextSlotOverflow {
                                node: edges[0].0,
                                requested: count,
                            }
                        }
                        error => panic!(
                            "validated feature graph mutation violated its owner contract: {error}"
                        ),
                    });
                }
            };
            words[0] = u32::from(slots[0]);
            for (position, &slot) in next_positions.iter().skip(1).zip(&slots[starts..]) {
                words[*position] = u32::from(slot);
            }
            let arc = &mut state.arcs[usize::from(arc_index)];
            let chain_index = if let Some(&existing) = arc.chain_by_words.get(words.as_slice()) {
                let chain = arc
                    .chains
                    .get_mut(existing)
                    .expect("configuration hash names an occupied chain");
                chain.reference_count = chain
                    .reference_count
                    .checked_add(1)
                    .expect("interface feature reference count overflow");
                arc.chains.remove(candidate);
                existing
            } else {
                state.shared_config_heap[extent as usize] = candidate;
                state.shared_config_heap[extent as usize + 1..extent as usize + requested_words]
                    .copy_from_slice(&words);
                arc.chain_by_words
                    .insert(words.into_boxed_slice(), candidate);
                candidate
            };
            let chain = arc
                .chains
                .get(chain_index)
                .expect("compiled feature chain exists");
            let config_index = chain.heap_index;
            let count = chain.occurrences.len() as i16;
            let old = arc
                .config_index_by_sw_if_index
                .get(position)
                .copied()
                .unwrap_or(u32::MAX);
            if position >= arc.config_index_by_sw_if_index.len() {
                arc.config_index_by_sw_if_index
                    .resize(position + 1, u32::MAX);
                arc.feature_count_by_sw_if_index.resize(position + 1, 0);
            }
            arc.config_index_by_sw_if_index[position] = config_index;
            arc.feature_count_by_sw_if_index[position] = count;
            if count != 0 {
                arc.sw_if_index_has_features.set(position);
            } else {
                arc.sw_if_index_has_features.clear(position);
            }
            if chain_index != candidate {
                state.free_extent(extent, extent_len);
            }
            if old != u32::MAX {
                state.release_chain(arc_index, old);
            }
            Ok(())
        };
        if hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0) {
            hammer_runtime::worker_thread_barrier_sync!(main, { publish() })
        } else {
            publish()
        }
    }

    #[inline(always)]
    pub fn start_feature_arc(
        &self,
        arc_index: u8,
        sw_if_index: u32,
        buffer: &mut Buffer,
        default_next: u16,
    ) -> u16 {
        let state = &self.state().feature;
        let arc = &state.arcs[usize::from(arc_index)];
        if !arc.sw_if_index_has_features.is_set(sw_if_index as usize) {
            return default_next;
        }
        let index = arc.config_index_by_sw_if_index[sw_if_index as usize];
        let next = state.shared_config_heap[index as usize];
        buffer.set_current_config_index(index + 1);
        u16::try_from(next).expect("published feature next fits a graph slot")
    }

    #[inline(always)]
    pub fn next_feature(&self, buffer: &mut Buffer) -> u16 {
        self.next_feature_with_config::<0>(buffer).1
    }

    #[inline(always)]
    pub fn next_feature_with_config<const N: usize>(&self, buffer: &mut Buffer) -> ([u32; N], u16) {
        let index = buffer.current_config_index() as usize;
        let end = index
            .checked_add(N)
            .expect("published feature cursor is representable");
        let words = &self.state().feature.shared_config_heap;
        let next = u16::try_from(words[end]).expect("published feature next fits a graph slot");
        let mut config = [0; N];
        config.copy_from_slice(&words[index..end]);
        buffer.set_current_config_index(
            u32::try_from(end + 1).expect("published feature cursor fits u32"),
        );
        (config, next)
    }
}

impl FeatureState {
    fn arc(&self, arc_index: u8) -> Result<&FeatureArc, FeatureError> {
        if !self.installed {
            return Err(FeatureError::NotInstalled);
        }
        self.arcs
            .get(usize::from(arc_index))
            .ok_or(FeatureError::ArcIndexInvalid { arc_index })
    }

    fn chain(&self, arc_index: u8, sw_if_index: u32) -> Option<&FeatureChain> {
        let arc = &self.arcs[usize::from(arc_index)];
        let &index = arc.config_index_by_sw_if_index.get(sw_if_index as usize)?;
        if index == u32::MAX {
            return None;
        }
        let pool_index = self.shared_config_heap[index as usize - 1];
        Some(
            arc.chains
                .get(pool_index)
                .expect("configuration back-pointer names an occupied chain"),
        )
    }

    fn release_chain(&mut self, arc_index: u8, index: u32) {
        let arc = &mut self.arcs[usize::from(arc_index)];
        let pool_index = self.shared_config_heap[index as usize - 1];
        let chain = arc
            .chains
            .get_mut(pool_index)
            .expect("interface owns its feature chain");
        chain.reference_count -= 1;
        if chain.reference_count != 0 {
            return;
        }
        let extent = chain.heap_index - 1;
        let len = chain.heap_len;
        arc.chain_by_words
            .remove(&self.shared_config_heap[index as usize..(extent + len) as usize]);
        arc.chains.remove(pool_index);
        self.free_extent(extent, len);
    }

    fn free_extent(&mut self, mut start: u32, mut length: u32) {
        if let Some((&before, &len)) = self.free_config_ranges.range(..start).next_back() {
            if before + len == start {
                self.free_config_ranges.remove(&before);
                start = before;
                length += len;
            }
        }
        if let Some((&after, &len)) = self.free_config_ranges.range(start..).next() {
            if start + length == after {
                self.free_config_ranges.remove(&after);
                length += len;
            }
        }
        self.free_config_ranges.insert(start, length);
    }

    pub(super) fn remove_interface(&mut self, sw_if_index: u32) {
        for index in 0..self.arcs.len() {
            let arc = &mut self.arcs[index];
            let Some(config) = arc
                .config_index_by_sw_if_index
                .get_mut(sw_if_index as usize)
            else {
                continue;
            };
            let old = std::mem::replace(config, u32::MAX);
            arc.feature_count_by_sw_if_index[sw_if_index as usize] = 0;
            arc.sw_if_index_has_features.clear(sw_if_index as usize);
            if old != u32::MAX {
                self.release_chain(index as u8, old);
            }
        }
    }
}
#[hammer_component_macros::init_function(name = "interface_feature_init", runs_after = ["install_packet_graph"])]
fn interface_feature_init(
    engine: &mut hammer_runtime::GlobalMain,
) -> hammer_runtime::RuntimeResult<()> {
    crate::net::NetMain::global()?
        .interface_main()
        .install_feature_arcs(engine.data_plane_main().nodes())
        .map_err(
            |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
                node: "interface-output",
                source: Box::new(source),
            },
        )
}
