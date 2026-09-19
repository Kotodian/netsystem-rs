use std::cell::UnsafeCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use hammer_core::data_plane::{Buffer, NodeId};
use hammer_infra::{bitmap::Bitmap, heap::Heap, pool::Pool};
use hammer_runtime::{DataPlaneMain, NodeMain, RuntimeError, RuntimeResult};

use crate::interface::{InterfaceCallbackRegistration, InterfaceMain, InterfaceResult};
use crate::net::NetMain;
use crate::{device::DeviceInputNode, ethernet::EthernetInputNode};

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
    #[error("feature arc index {arc_index} is invalid")]
    ArcIndexInvalid { arc_index: u8 },
    #[error("feature index {feature_index} is invalid in arc {arc_index}")]
    FeatureIndexInvalid { arc_index: u8, feature_index: u32 },
    #[error("software interface {sw_if_index} is absent")]
    InterfaceNotFound { sw_if_index: u32 },
    #[error("arc {arc_index} start {node:?} next {actual} differs from {expected}")]
    StartNextMismatch {
        arc_index: u8,
        node: NodeId,
        expected: u16,
        actual: u16,
    },
    #[error("node {node:?} next count {requested} exceeds u16")]
    NextSlotOverflow { node: NodeId, requested: usize },
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

struct FeatureConfigMain {
    config_main: ConfigMain,
    config_index_by_sw_if_index: Vec<u32>,
}

struct ConfigMain {
    start_nodes: Box<[NodeId]>,
    default_end_node: NodeId,
    feature_node_by_index: Box<[NodeId]>,
    config_pool: Pool<ConfigEntry>,
    config_by_words: HashMap<Box<[u32]>, u32>,
}

struct ConfigEntry {
    features: Vec<ConfigFeature>,
    end_node: NodeId,
    heap_index: u32,
    reference_count: u32,
}

#[derive(Clone, PartialEq, Eq)]
struct ConfigFeature {
    feature_index: u32,
    words: Box<[u32]>,
}

/// Process-global authority for Feature Arc declarations and configuration.
pub struct FeatureMain {
    arc_registrations: UnsafeCell<Vec<FeatureArcRegistration>>,
    feature_registrations: UnsafeCell<Vec<FeatureRegistration>>,
    arc_index_by_name: UnsafeCell<BTreeMap<&'static str, u8>>,
    feature_index_by_name: UnsafeCell<Vec<BTreeMap<&'static str, u32>>>,
    feature_config_mains: UnsafeCell<Vec<FeatureConfigMain>>,
    shared_feature_config_heap: UnsafeCell<Heap<u32>>,
    feature_nodes_by_arc: UnsafeCell<Vec<Box<[NodeId]>>>,
    feature_count_by_sw_if_index: UnsafeCell<Vec<Vec<i16>>>,
    sw_if_index_has_features: UnsafeCell<Vec<Bitmap>>,
    device_input_feature_arc_index: UnsafeCell<u8>,
}

pub static FEATURE_MAIN: OnceLock<FeatureMain> = OnceLock::new();

// SAFETY: startup declaration and construction are single-threaded. Later
// mutation is main-thread-only while every Data Worker has acknowledged the
// WorkerBarrier; workers perform only synchronous reads between barriers.
unsafe impl Send for FeatureMain {}
// SAFETY: the same publication contract prevents mutable access from racing
// packet-path reads. No borrow from an UnsafeCell escapes an owner operation.
unsafe impl Sync for FeatureMain {}

impl FeatureMain {
    fn new() -> Self {
        Self {
            arc_registrations: UnsafeCell::new(Vec::new()),
            feature_registrations: UnsafeCell::new(Vec::new()),
            arc_index_by_name: UnsafeCell::new(BTreeMap::new()),
            feature_index_by_name: UnsafeCell::new(Vec::new()),
            feature_config_mains: UnsafeCell::new(Vec::new()),
            shared_feature_config_heap: UnsafeCell::new(Heap::new()),
            feature_nodes_by_arc: UnsafeCell::new(Vec::new()),
            feature_count_by_sw_if_index: UnsafeCell::new(Vec::new()),
            sw_if_index_has_features: UnsafeCell::new(Vec::new()),
            device_input_feature_arc_index: UnsafeCell::new(u8::MAX),
        }
    }

    /// Publishes the empty Feature authority before declaration callbacks run.
    pub fn init() -> RuntimeResult<()> {
        assert!(
            FEATURE_MAIN.set(Self::new()).is_ok(),
            "Feature initialization callback executes once"
        );
        Ok(())
    }

    /// Returns the process-global Feature authority.
    pub fn global() -> RuntimeResult<&'static Self> {
        FEATURE_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::feature::FeatureMain",
            })
    }

    pub fn register_feature_arc(
        &self,
        name: &'static str,
        start_nodes: &[NodeId],
        last_in_arc: Option<&'static str>,
    ) -> Result<u8, FeatureError> {
        self.assert_declaration_phase();
        let arc_index_by_name = self.arc_index_by_name_mut();
        if arc_index_by_name.contains_key(name) {
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
        let registrations = self.arc_registrations_mut();
        let requested = registrations.len() + 1;
        if requested >= 256 {
            return Err(FeatureError::ArcLimit { requested });
        }
        let index = registrations.len() as u8;
        registrations.push(FeatureArcRegistration {
            name,
            start_nodes: start_nodes.into(),
            last_in_arc,
        });
        arc_index_by_name.insert(name, index);
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
        self.assert_declaration_phase();
        let registrations = self.feature_registrations_mut();
        for registration in registrations
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
        registrations.push(FeatureRegistration {
            arc_name,
            name,
            node,
            runs_before: runs_before.into(),
            runs_after: runs_after.into(),
        });
        Ok(())
    }

    fn init_config_mains(&self, nodes: &NodeMain) -> Result<(), FeatureError> {
        hammer_runtime::ensure_main_thread().expect("Feature Arc init belongs to the main thread");
        assert!(
            !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0),
            "Feature Arc init precedes Data Worker startup"
        );
        assert!(
            self.feature_config_mains().is_empty(),
            "Feature Arc init executes once"
        );

        let arc_registrations = self.arc_registrations();
        let feature_registrations = self.feature_registrations();
        let arc_index_by_name = self.arc_index_by_name();
        for feature in feature_registrations {
            if !arc_index_by_name.contains_key(feature.arc_name) {
                return Err(FeatureError::ArcNotFound {
                    name: feature.arc_name,
                });
            }
        }

        let mut feature_config_mains = Vec::with_capacity(arc_registrations.len());
        let mut feature_index_by_name = Vec::with_capacity(arc_registrations.len());
        let mut feature_nodes_by_arc = Vec::with_capacity(arc_registrations.len());
        for registration in arc_registrations {
            let features = feature_registrations
                .iter()
                .filter(|feature| feature.arc_name == registration.name)
                .collect::<Vec<_>>();
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
            let indexes = ordered
                .iter()
                .enumerate()
                .map(|(feature_index, &registration_index)| {
                    (features[registration_index].name, feature_index as u32)
                })
                .collect();
            let default_end_node = *feature_nodes.last().expect("a Feature Arc is nonempty");
            feature_config_mains.push(FeatureConfigMain {
                config_main: ConfigMain {
                    start_nodes: registration.start_nodes.clone(),
                    default_end_node,
                    feature_node_by_index: feature_nodes.clone(),
                    config_pool: Pool::new(),
                    config_by_words: HashMap::new(),
                },
                config_index_by_sw_if_index: Vec::new(),
            });
            feature_index_by_name.push(indexes);
            feature_nodes_by_arc.push(feature_nodes);
        }

        let arc_count = feature_config_mains.len();
        *self.feature_config_mains_mut() = feature_config_mains;
        *self.feature_index_by_name_mut() = feature_index_by_name;
        *self.feature_nodes_by_arc_mut() = feature_nodes_by_arc;
        *self.feature_count_by_sw_if_index_mut() = vec![Vec::new(); arc_count];
        *self.sw_if_index_has_features_mut() = (0..arc_count).map(|_| Bitmap::new()).collect();
        Ok(())
    }

    pub fn feature_arc_index(&self, name: &str) -> Option<u8> {
        self.arc_index_by_name().get(name).copied()
    }

    pub fn feature_index(&self, arc_index: u8, name: &str) -> Option<u32> {
        self.feature_index_by_name()
            .get(usize::from(arc_index))?
            .get(name)
            .copied()
    }

    pub fn feature_count(&self, arc_index: u8, sw_if_index: u32) -> Result<u32, FeatureError> {
        self.validate_interface(sw_if_index)?;
        self.config_main(arc_index)?;
        let count = self
            .feature_count_by_sw_if_index()
            .get(usize::from(arc_index))
            .and_then(|counts| counts.get(sw_if_index as usize))
            .copied()
            .unwrap_or(0);
        Ok(u32::try_from(count).expect("published Feature count is nonnegative"))
    }

    #[inline(always)]
    pub fn has_features(&self, arc_index: u8, sw_if_index: u32) -> bool {
        self.sw_if_index_has_features()
            .get(usize::from(arc_index))
            .expect("packet path uses a registered Feature Arc")
            .is_set(sw_if_index as usize)
    }

    pub fn feature_config_index(
        &self,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<Option<u32>, FeatureError> {
        self.validate_interface(sw_if_index)?;
        let config = self.config_main(arc_index)?;
        Ok(config
            .config_index_by_sw_if_index
            .get(sw_if_index as usize)
            .copied()
            .filter(|index| *index != u32::MAX))
    }

    pub fn feature_arc_end_node(
        &self,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<NodeId, FeatureError> {
        self.validate_interface(sw_if_index)?;
        let config = self.config_main(arc_index)?;
        Ok(self
            .config_entry(config, sw_if_index)
            .map_or(config.config_main.default_end_node, |entry| entry.end_node))
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
        assert!(
            occurrences.len() < i16::MAX as usize,
            "Feature occurrence count fits its published representation"
        );
        occurrences.push(ConfigFeature {
            feature_index,
            words: config.into(),
        });
        occurrences.sort_by_key(|occurrence| occurrence.feature_index);
        self.replace_config(main, arc_index, sw_if_index, occurrences, end_node)
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
        self.replace_config(main, arc_index, sw_if_index, occurrences, end_node)
    }

    pub fn is_feature_enabled(
        &self,
        arc_index: u8,
        feature_index: u32,
        sw_if_index: u32,
    ) -> Result<bool, FeatureError> {
        let config = self.config_main(arc_index)?;
        if feature_index as usize >= config.config_main.feature_node_by_index.len() {
            return Err(FeatureError::FeatureIndexInvalid {
                arc_index,
                feature_index,
            });
        }
        self.validate_interface(sw_if_index)?;
        Ok(self.config_entry(config, sw_if_index).is_some_and(|entry| {
            entry
                .features
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
        self.replace_config(main, arc_index, sw_if_index, occurrences, end_node)
    }

    pub fn reset_feature_arc_end(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
    ) -> Result<(), FeatureError> {
        let end = self.config_main(arc_index)?.config_main.default_end_node;
        self.modify_feature_arc_end(main, arc_index, sw_if_index, end)
    }

    fn feature_configuration(
        &self,
        arc_index: u8,
        sw_if_index: u32,
        feature_index: Option<u32>,
    ) -> Result<(Vec<ConfigFeature>, NodeId), FeatureError> {
        hammer_runtime::ensure_main_thread()
            .expect("Feature configuration belongs to the main thread");
        let config = self.config_main(arc_index)?;
        if let Some(feature_index) = feature_index
            && feature_index as usize >= config.config_main.feature_node_by_index.len()
        {
            return Err(FeatureError::FeatureIndexInvalid {
                arc_index,
                feature_index,
            });
        }
        self.validate_interface(sw_if_index)?;
        Ok(match self.config_entry(config, sw_if_index) {
            Some(entry) => (entry.features.clone(), entry.end_node),
            None => (Vec::new(), config.config_main.default_end_node),
        })
    }

    fn replace_config(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        sw_if_index: u32,
        occurrences: Vec<ConfigFeature>,
        end_node: NodeId,
    ) -> Result<(), FeatureError> {
        if main.nodes().node_kind(end_node).is_err() {
            return Err(FeatureError::GraphNodeNotFound { node: end_node });
        }
        let config = self.config_main(arc_index)?;

        let feature_nodes = self
            .feature_nodes_by_arc()
            .get(usize::from(arc_index))
            .expect("a constructed Feature Arc has sorted nodes");
        let first = occurrences.first().map_or(end_node, |occurrence| {
            feature_nodes[occurrence.feature_index as usize]
        });
        let mut edges = config
            .config_main
            .start_nodes
            .iter()
            .map(|&start| (start, first))
            .collect::<Vec<_>>();
        let starts = edges.len();
        let mut words = vec![0];
        let mut next_positions = vec![0];
        for (position, occurrence) in occurrences.iter().enumerate() {
            words.extend_from_slice(&occurrence.words);
            let node = feature_nodes[occurrence.feature_index as usize];
            if position + 1 == occurrences.len() && node == end_node {
                break;
            }
            next_positions.push(words.len());
            words.push(0);
            let next = occurrences
                .get(position + 1)
                .map_or(end_node, |next| feature_nodes[next.feature_index as usize]);
            edges.push((node, next));
        }
        words.push(end_node.slot());
        let extent_len = u32::try_from(
            words
                .len()
                .checked_add(1)
                .expect("Feature configuration word count is representable"),
        )
        .expect("Feature configuration word count fits its heap offset");

        if hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0) {
            hammer_runtime::worker_thread_barrier_sync!(main, {
                self.publish_config(
                    main.nodes(),
                    arc_index,
                    sw_if_index,
                    occurrences,
                    end_node,
                    edges,
                    starts,
                    words,
                    next_positions,
                    extent_len,
                )
            })
        } else {
            self.publish_config(
                main.nodes(),
                arc_index,
                sw_if_index,
                occurrences,
                end_node,
                edges,
                starts,
                words,
                next_positions,
                extent_len,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_config(
        &self,
        nodes: &NodeMain,
        arc_index: u8,
        sw_if_index: u32,
        occurrences: Vec<ConfigFeature>,
        end_node: NodeId,
        edges: Vec<(NodeId, NodeId)>,
        starts: usize,
        mut words: Vec<u32>,
        next_positions: Vec<usize>,
        extent_len: u32,
    ) -> Result<(), FeatureError> {
        let arc_position = usize::from(arc_index);
        let interface_position = sw_if_index as usize;
        let feature_config_mains = self.feature_config_mains_mut();
        let config = &mut feature_config_mains[arc_position];
        let heap = self.shared_feature_config_heap_mut();
        let extent = heap.alloc(extent_len, 0);
        let count = i16::try_from(occurrences.len())
            .expect("Feature occurrence count fits its published representation");
        let candidate = config.config_main.config_pool.insert(ConfigEntry {
            features: occurrences,
            end_node,
            heap_index: extent + 1,
            reference_count: 1,
        });

        let slots = match nodes.add_node_next_slots(&edges, &[0..starts]) {
            Ok(slots) => slots,
            Err(error) => {
                config.config_main.config_pool.remove(candidate);
                heap.remove(extent);
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
                        "validated Feature graph mutation violated its owner contract: {error}"
                    ),
                });
            }
        };
        words[0] = u32::from(slots[0]);
        for (position, &slot) in next_positions.iter().skip(1).zip(&slots[starts..]) {
            words[*position] = u32::from(slot);
        }

        let config_pool_index =
            if let Some(&existing) = config.config_main.config_by_words.get(words.as_slice()) {
                let entry = config
                    .config_main
                    .config_pool
                    .get_mut(existing)
                    .expect("configuration hash names an occupied config entry");
                entry.reference_count = entry
                    .reference_count
                    .checked_add(1)
                    .expect("interface Feature reference count overflow");
                config.config_main.config_pool.remove(candidate);
                heap.remove(extent);
                existing
            } else {
                let run = heap
                    .get_slice_mut(extent, extent_len)
                    .expect("new Feature heap allocation is live");
                run[0] = candidate;
                run[1..].copy_from_slice(&words);
                config
                    .config_main
                    .config_by_words
                    .insert(words.into_boxed_slice(), candidate);
                candidate
            };
        let entry = config
            .config_main
            .config_pool
            .get(config_pool_index)
            .expect("compiled Feature config exists");
        let config_index = entry.heap_index;
        let old = config
            .config_index_by_sw_if_index
            .get(interface_position)
            .copied()
            .unwrap_or(u32::MAX);
        if interface_position >= config.config_index_by_sw_if_index.len() {
            config
                .config_index_by_sw_if_index
                .resize(interface_position + 1, u32::MAX);
        }
        config.config_index_by_sw_if_index[interface_position] = config_index;

        let counts = &mut self.feature_count_by_sw_if_index_mut()[arc_position];
        if interface_position >= counts.len() {
            counts.resize(interface_position + 1, 0);
        }
        counts[interface_position] = count;
        let bitmap = &mut self.sw_if_index_has_features_mut()[arc_position];
        if count != 0 {
            bitmap.set(interface_position);
        } else {
            bitmap.clear(interface_position);
        }
        if old != u32::MAX {
            Self::release_config(config, heap, old);
        }
        Ok(())
    }

    fn release_config(config: &mut FeatureConfigMain, heap: &mut Heap<u32>, index: u32) {
        let allocation = index
            .checked_sub(1)
            .expect("a Feature config index follows its heap back-pointer");
        let pool_index = *heap
            .get(allocation)
            .expect("Feature config index names a live heap allocation");
        let entry = config
            .config_main
            .config_pool
            .get_mut(pool_index)
            .expect("an interface owns its Feature config");
        entry.reference_count = entry
            .reference_count
            .checked_sub(1)
            .expect("Feature config reference count is positive");
        if entry.reference_count != 0 {
            return;
        }
        assert_eq!(
            entry.heap_index, index,
            "Feature heap back-pointer names the selected config"
        );
        config
            .config_main
            .config_by_words
            .retain(|_, config_index| *config_index != pool_index);
        config.config_main.config_pool.remove(pool_index);
        heap.remove(allocation);
    }

    fn remove_interface(&self, sw_if_index: u32) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("Feature interface cleanup requires the publication scope");
        let position = sw_if_index as usize;
        let configs = self.feature_config_mains_mut();
        let counts = self.feature_count_by_sw_if_index_mut();
        let bitmaps = self.sw_if_index_has_features_mut();
        let heap = self.shared_feature_config_heap_mut();
        for arc_position in 0..configs.len() {
            let Some(config_index) = configs[arc_position]
                .config_index_by_sw_if_index
                .get_mut(position)
            else {
                continue;
            };
            let old = std::mem::replace(config_index, u32::MAX);
            counts[arc_position][position] = 0;
            bitmaps[arc_position].clear(position);
            if old != u32::MAX {
                Self::release_config(&mut configs[arc_position], heap, old);
            }
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
        if !self.has_features(arc_index, sw_if_index) {
            return default_next;
        }
        let config = &self.feature_config_mains()[usize::from(arc_index)];
        let index = config.config_index_by_sw_if_index[sw_if_index as usize];
        self.start_feature_arc_at_config(arc_index, index, buffer)
    }

    #[inline(always)]
    pub fn start_device_input(
        &self,
        sw_if_index: u32,
        buffer: &mut Buffer,
        default_next: u16,
    ) -> u16 {
        // SAFETY: device_input_feature_init publishes the scalar before Data
        // Workers start, after which packet code only reads it.
        let arc_index = unsafe { *self.device_input_feature_arc_index.get() };
        assert_ne!(
            arc_index,
            u8::MAX,
            "device-input Feature Arc exists before packet processing"
        );
        self.start_feature_arc(arc_index, sw_if_index, buffer, default_next)
    }

    #[inline(always)]
    pub fn start_feature_arc_at_config(
        &self,
        arc_index: u8,
        config_index: u32,
        buffer: &mut Buffer,
    ) -> u16 {
        let config = self
            .feature_config_mains()
            .get(usize::from(arc_index))
            .expect("packet path uses a registered Feature Arc");
        let heap = self.shared_feature_config_heap();
        let allocation = config_index
            .checked_sub(1)
            .expect("a cached Feature config follows its heap back-pointer");
        let pool_index = *heap
            .get(allocation)
            .expect("a cached Feature config names a live heap allocation");
        let entry = config
            .config_main
            .config_pool
            .get(pool_index)
            .expect("a cached Feature config belongs to the selected arc");
        assert_eq!(
            entry.heap_index, config_index,
            "a cached Feature config belongs to the selected arc"
        );
        let next = *heap
            .get(config_index)
            .expect("a Feature config contains its first next slot");
        buffer.set_current_config_index(config_index + 1);
        u16::try_from(next).expect("published Feature next fits a graph slot")
    }

    #[inline(always)]
    pub fn next_feature(&self, buffer: &mut Buffer) -> u16 {
        self.next_feature_with_config::<0>(buffer).1
    }

    #[inline(always)]
    pub fn next_feature_with_config<const N: usize>(&self, buffer: &mut Buffer) -> ([u32; N], u16) {
        let index = buffer.current_config_index();
        let words = u32::try_from(N)
            .expect("Feature config width fits u32")
            .checked_add(1)
            .expect("Feature config cursor advance fits u32");
        let run = self
            .shared_feature_config_heap()
            .get_slice(index, words)
            .expect("published Feature cursor names a live config run");
        let mut config = [0; N];
        config.copy_from_slice(&run[..N]);
        buffer.set_current_config_index(
            index
                .checked_add(words)
                .expect("published Feature cursor fits u32"),
        );
        (
            config,
            u16::try_from(run[N]).expect("published Feature next fits a graph slot"),
        )
    }

    fn validate_interface(&self, sw_if_index: u32) -> Result<(), FeatureError> {
        let interfaces = NetMain::global()
            .expect("Feature mutation requires initialized NetMain")
            .interface_main();
        if interfaces.software_interface(sw_if_index).is_none() {
            return Err(FeatureError::InterfaceNotFound { sw_if_index });
        }
        Ok(())
    }

    fn config_entry<'a>(
        &self,
        config: &'a FeatureConfigMain,
        sw_if_index: u32,
    ) -> Option<&'a ConfigEntry> {
        let &index = config
            .config_index_by_sw_if_index
            .get(sw_if_index as usize)?;
        if index == u32::MAX {
            return None;
        }
        let pool_index = *self
            .shared_feature_config_heap()
            .get(index.checked_sub(1)?)?;
        config.config_main.config_pool.get(pool_index)
    }

    fn config_main(&self, arc_index: u8) -> Result<&FeatureConfigMain, FeatureError> {
        self.feature_config_mains()
            .get(usize::from(arc_index))
            .ok_or(FeatureError::ArcIndexInvalid { arc_index })
    }

    fn assert_declaration_phase(&self) {
        hammer_runtime::ensure_main_thread()
            .expect("Feature declarations belong to the main thread");
        assert!(
            !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0),
            "Feature declarations precede Data Worker startup"
        );
        assert!(
            self.feature_config_mains().is_empty(),
            "Feature declarations precede feature_arc_init"
        );
    }

    fn arc_registrations(&self) -> &[FeatureArcRegistration] {
        // SAFETY: declarations are immutable after startup construction.
        unsafe { &*self.arc_registrations.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn arc_registrations_mut(&self) -> &mut Vec<FeatureArcRegistration> {
        // SAFETY: declaration callbacks are serialized on the main thread.
        unsafe { &mut *self.arc_registrations.get() }
    }

    fn feature_registrations(&self) -> &[FeatureRegistration] {
        // SAFETY: declarations are immutable after startup construction.
        unsafe { &*self.feature_registrations.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn feature_registrations_mut(&self) -> &mut Vec<FeatureRegistration> {
        // SAFETY: declaration callbacks are serialized on the main thread.
        unsafe { &mut *self.feature_registrations.get() }
    }

    fn arc_index_by_name(&self) -> &BTreeMap<&'static str, u8> {
        // SAFETY: the name index is immutable once workers can read Feature state.
        unsafe { &*self.arc_index_by_name.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn arc_index_by_name_mut(&self) -> &mut BTreeMap<&'static str, u8> {
        // SAFETY: declaration callbacks are serialized on the main thread.
        unsafe { &mut *self.arc_index_by_name.get() }
    }

    fn feature_index_by_name(&self) -> &[BTreeMap<&'static str, u32>] {
        // SAFETY: startup construction publishes this immutable index once.
        unsafe { &*self.feature_index_by_name.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn feature_index_by_name_mut(&self) -> &mut Vec<BTreeMap<&'static str, u32>> {
        // SAFETY: startup construction is single-threaded.
        unsafe { &mut *self.feature_index_by_name.get() }
    }

    fn feature_config_mains(&self) -> &[FeatureConfigMain] {
        // SAFETY: packet reads occur only outside a mutation barrier interval.
        unsafe { &*self.feature_config_mains.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn feature_config_mains_mut(&self) -> &mut Vec<FeatureConfigMain> {
        // SAFETY: mutations are startup-only or occur while workers are stopped.
        unsafe { &mut *self.feature_config_mains.get() }
    }

    fn shared_feature_config_heap(&self) -> &Heap<u32> {
        // SAFETY: packet reads occur only outside a mutation barrier interval.
        unsafe { &*self.shared_feature_config_heap.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn shared_feature_config_heap_mut(&self) -> &mut Heap<u32> {
        // SAFETY: mutations are startup-only or occur while workers are stopped.
        unsafe { &mut *self.shared_feature_config_heap.get() }
    }

    fn feature_nodes_by_arc(&self) -> &[Box<[NodeId]>] {
        // SAFETY: startup construction publishes this immutable table once.
        unsafe { &*self.feature_nodes_by_arc.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn feature_nodes_by_arc_mut(&self) -> &mut Vec<Box<[NodeId]>> {
        // SAFETY: startup construction is single-threaded.
        unsafe { &mut *self.feature_nodes_by_arc.get() }
    }

    fn feature_count_by_sw_if_index(&self) -> &[Vec<i16>] {
        // SAFETY: reads occur outside a mutation barrier interval.
        unsafe { &*self.feature_count_by_sw_if_index.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn feature_count_by_sw_if_index_mut(&self) -> &mut Vec<Vec<i16>> {
        // SAFETY: mutations occur while workers are stopped.
        unsafe { &mut *self.feature_count_by_sw_if_index.get() }
    }

    fn sw_if_index_has_features(&self) -> &[Bitmap] {
        // SAFETY: packet reads occur only outside a mutation barrier interval.
        unsafe { &*self.sw_if_index_has_features.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn sw_if_index_has_features_mut(&self) -> &mut Vec<Bitmap> {
        // SAFETY: mutations occur while workers are stopped.
        unsafe { &mut *self.sw_if_index_has_features.get() }
    }
}

pub(crate) const FEATURE_SW_INTERFACE_CALLBACKS: [InterfaceCallbackRegistration; 1] =
    [InterfaceCallbackRegistration {
        callback: feature_sw_interface_add_del,
        priority: 1,
    }];

fn feature_sw_interface_add_del(
    _: &mut DataPlaneMain,
    _: &InterfaceMain,
    sw_if_index: u32,
    is_create: bool,
) -> InterfaceResult<()> {
    if is_create {
        return Ok(());
    }
    FeatureMain::global()
        .expect("FeatureMain exists before software interface deletion")
        .remove_interface(sw_if_index);
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "feature_main_init",
    runs_after = ["net_main_init"]
)]
fn feature_main_init() -> RuntimeResult<()> {
    FeatureMain::init()
}

#[doc(hidden)]
#[hammer_component_macros::main_loop_enter_function(
    name = "device_input_feature_init",
    runs_before = ["feature_arc_init"]
)]
pub fn device_input_feature_init(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let features = FeatureMain::global()?;
    let arc_index = (|| {
        let arc_index = DeviceInputNode::register_feature_arc(features, main.nodes())?;
        EthernetInputNode::register_feature(features, main.nodes())?;
        Ok::<u8, FeatureError>(arc_index)
    })()
    .map_err(|source| RuntimeError::GraphNodeInitialization {
        node: DeviceInputNode::NODE_NAME,
        source: Box::new(source),
    })?;
    // SAFETY: main-loop-enter callbacks are serialized before workers start.
    unsafe {
        *features.device_input_feature_arc_index.get() = arc_index;
    }
    Ok(())
}

/// Constructs Feature Arcs after graph nodes and declarations exist.
#[doc(hidden)]
#[hammer_component_macros::main_loop_enter_function(
    name = "feature_arc_init",
    runs_before = ["start_workers"]
)]
pub fn feature_arc_init(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    FeatureMain::global()?
        .init_config_mains(main.nodes())
        .map_err(|source| RuntimeError::GraphNodeInitialization {
            node: "feature",
            source: Box::new(source),
        })
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use hammer_core::data_plane::NodeId;
    use hammer_runtime::NodeMain;

    use super::{FeatureError, FeatureMain};
    use crate::data_plane::{DropNode, PuntNode};
    use crate::interface::InterfaceOutputNode;

    #[test]
    fn declaration_errors_do_not_publish_config_mains() -> Result<(), Box<dyn std::error::Error>> {
        hammer_runtime::ThreadMain::new()?;
        let nodes = NodeMain::default();
        let start = nodes.try_register_internal(InterfaceOutputNode)?;
        let punt = nodes.try_register_internal(PuntNode::new())?;
        let drop_node = nodes.try_register_internal(DropNode::new())?;

        let duplicate = FeatureMain::new();
        duplicate.register_feature_arc("duplicate", &[start], None)?;
        assert!(matches!(
            duplicate.register_feature_arc("duplicate", &[start], None),
            Err(FeatureError::DuplicateArc { name: "duplicate" })
        ));
        duplicate.register_feature("duplicate", "punt", punt, &[], &[])?;
        assert!(matches!(
            duplicate.register_feature("duplicate", "punt", punt, &[], &[]),
            Err(FeatureError::DuplicateFeature {
                arc: "duplicate",
                feature: "punt"
            })
        ));

        let missing_target = FeatureMain::new();
        missing_target.register_feature_arc("missing-target", &[start], None)?;
        missing_target.register_feature("missing-target", "punt", punt, &["absent"], &[])?;
        assert!(matches!(
            missing_target.init_config_mains(&nodes),
            Err(FeatureError::ConstraintTargetMissing {
                arc: "missing-target",
                feature: "punt",
                target: "absent"
            })
        ));
        assert!(missing_target.feature_config_mains().is_empty());

        let cycle = FeatureMain::new();
        cycle.register_feature_arc("cycle", &[start], None)?;
        cycle.register_feature("cycle", "punt", punt, &["drop"], &[])?;
        cycle.register_feature("cycle", "drop", drop_node, &["punt"], &[])?;
        assert!(matches!(
            cycle.init_config_mains(&nodes),
            Err(FeatureError::OrderCycle { arc: "cycle" })
        ));
        assert!(cycle.feature_config_mains().is_empty());

        let empty = FeatureMain::new();
        empty.register_feature_arc("empty", &[start], None)?;
        assert!(matches!(
            empty.init_config_mains(&nodes),
            Err(FeatureError::EmptyArc { arc: "empty" })
        ));
        assert!(empty.feature_config_mains().is_empty());

        let missing_node = FeatureMain::new();
        missing_node.register_feature_arc("missing-node", &[start], None)?;
        let absent = NodeId::new(u32::from(u16::MAX));
        missing_node.register_feature("missing-node", "absent", absent, &[], &[])?;
        assert!(matches!(
            missing_node.init_config_mains(&nodes),
            Err(FeatureError::GraphNodeNotFound { node }) if node == absent
        ));
        assert!(missing_node.feature_config_mains().is_empty());

        let initialized = FeatureMain::new();
        initialized.register_feature_arc("initialized", &[start], Some("drop"))?;
        initialized.register_feature("initialized", "drop", drop_node, &[], &[])?;
        initialized.init_config_mains(&nodes)?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                initialized
                    .register_feature("initialized", "punt", punt, &[], &[])
                    .expect("late declaration cannot return")
            }))
            .is_err()
        );

        Ok(())
    }
}
