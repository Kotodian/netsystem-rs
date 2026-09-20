use std::sync::Arc;

use hammer_core::data_plane::{Buffer, Frame, NodeRegistration};
use hammer_runtime::{DataPlaneBufferConfig, DataPlaneMain, InternalNode, Node, NodeRuntime};
use hammer_service::feature::{FeatureError, FeatureMain, feature_arc_init};
use hammer_service::interface::{INTERFACE_MAIN, interface_main_init};
use hammer_service::net::NetMain;

#[hammer_component_macros::feature_arc(
    name = "interface-output",
    start_nodes = [OutputFeatureStartNode],
    last_in_arc = OutputDropNode,
)]
struct OutputFeatureStartNode;

impl OutputFeatureStartNode {
    const NODE_NAME: &'static str = "output-feature-start";
}

impl Node for OutputFeatureStartNode {
    fn process(_: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        frame.len()
    }
}

impl InternalNode for OutputFeatureStartNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next(Self::NODE_NAME, 0))
    }
}

#[hammer_component_macros::feature(
    arc = OutputFeatureStartNode,
    runs_before = [OutputDropNode],
)]
struct OutputPuntNode;

impl OutputPuntNode {
    const NODE_NAME: &'static str = "output-punt";
}

impl Node for OutputPuntNode {
    fn process(_: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        frame.len()
    }
}

impl InternalNode for OutputPuntNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next(Self::NODE_NAME, 0))
    }
}

#[hammer_component_macros::feature(arc = OutputFeatureStartNode)]
struct OutputDropNode;

impl OutputDropNode {
    const NODE_NAME: &'static str = "output-drop";
}

impl Node for OutputDropNode {
    fn process(_: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        frame.len()
    }
}

impl InternalNode for OutputDropNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next(Self::NODE_NAME, 0))
    }
}

fn empty_packet_buffer() -> Buffer {
    // SAFETY: Buffer's fixed-layout scalar, atomic, bitflag, optional-index,
    // and opaque byte fields all accept zero. This test only exercises the
    // Feature config cursor and never gives this value Buffer Pool ownership.
    unsafe { std::mem::MaybeUninit::<Buffer>::zeroed().assume_init() }
}

#[test]
fn feature_config_lifecycle_preserves_arc_contracts() -> Result<(), Box<dyn std::error::Error>> {
    hammer_runtime::ThreadMain::new()?;
    let mut runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
    interface_main_init()?;
    let interfaces = Arc::clone(
        INTERFACE_MAIN
            .get()
            .expect("interface_main_init publishes InterfaceMain"),
    );
    let net = NetMain::init(&mut runtime, interfaces)?;
    FeatureMain::init()?;
    let features = FeatureMain::global()?;
    let interfaces = net.interface_main();
    let device_class_index = interfaces.device_class_index("local");
    let hw_class_index = interfaces.hw_class_index("local");

    let first_hardware = interfaces.register_hardware_interface(
        &mut runtime,
        device_class_index,
        1,
        hw_class_index,
        0,
    )?;
    let first_interface = interfaces.hardware_interface(first_hardware).sw_if_index();
    let second_hardware = interfaces.register_hardware_interface(
        &mut runtime,
        device_class_index,
        2,
        hw_class_index,
        0,
    )?;
    let second_interface = interfaces.hardware_interface(second_hardware).sw_if_index();

    let output = runtime
        .nodes()
        .try_register_internal(OutputFeatureStartNode)?;
    let punt = runtime.nodes().try_register_internal(OutputPuntNode)?;
    let drop_node = runtime.nodes().try_register_internal(OutputDropNode)?;
    let output_arc = OutputFeatureStartNode::register_feature_arc(features, runtime.nodes())?;
    OutputPuntNode::register_feature(features, runtime.nodes())?;
    OutputDropNode::register_feature(features, runtime.nodes())?;
    let mirror_arc =
        features.register_feature_arc("interface-output-mirror", &[output], Some("output-drop"))?;
    features.register_feature(
        "interface-output-mirror",
        "output-punt",
        punt,
        &["output-drop"],
        &[],
    )?;
    features.register_feature(
        "interface-output-mirror",
        "output-drop",
        drop_node,
        &[],
        &[],
    )?;
    let mismatched_arc = features.register_feature_arc(
        "mismatched-start-slots",
        &[output, punt],
        Some("output-drop"),
    )?;
    features.register_feature("mismatched-start-slots", "output-drop", drop_node, &[], &[])?;
    feature_arc_init(&mut runtime)?;

    let punt_feature = features
        .feature_index(output_arc, "output-punt")
        .expect("the macro registers the punt feature");
    let drop_feature = features
        .feature_index(output_arc, "output-drop")
        .expect("the macro registers the drop feature");
    let mirror_punt = features
        .feature_index(mirror_arc, "output-punt")
        .expect("the mirror arc registers the punt feature");
    let mismatched_drop = features
        .feature_index(mismatched_arc, "output-drop")
        .expect("the mismatched arc registers the drop feature");
    let config = [17, 23];

    assert_eq!(features.feature_count(output_arc, first_interface)?, 0);
    assert!(!features.has_features(output_arc, first_interface));
    assert_eq!(
        features.feature_config_index(output_arc, first_interface)?,
        None
    );
    assert_eq!(
        features.feature_arc_end_node(output_arc, first_interface)?,
        drop_node
    );

    runtime.nodes().add_node_next_slot(punt, output)?;
    assert!(matches!(
        features.enable_feature(
            &mut runtime,
            mismatched_arc,
            mismatched_drop,
            first_interface,
            &[],
        ),
        Err(FeatureError::StartNextMismatch { arc_index, .. })
            if arc_index == mismatched_arc
    ));
    assert_eq!(
        features.feature_config_index(mismatched_arc, first_interface)?,
        None
    );
    assert_eq!(features.feature_count(mismatched_arc, first_interface)?, 0);
    assert!(!features.has_features(mismatched_arc, first_interface));
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot_for_target(output, drop_node)?,
        None
    );
    assert_eq!(
        runtime.nodes().node_next_slot_for_target(punt, drop_node)?,
        None
    );
    assert!(
        runtime
            .nodes()
            .node_next_slot_for_target(punt, output)?
            .is_some(),
        "the pre-existing edge survives rejected batch publication"
    );

    features.enable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        first_interface,
        &config,
    )?;
    features.enable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        first_interface,
        &config,
    )?;
    assert_eq!(features.feature_count(output_arc, first_interface)?, 2);

    let mut buffer = empty_packet_buffer();
    let first_next = features.start_feature_arc(output_arc, first_interface, &mut buffer, u16::MAX);
    let (first_words, duplicate_next) = features.next_feature_with_config::<2>(&mut buffer);
    let (second_words, end_next) = features.next_feature_with_config::<2>(&mut buffer);
    assert_eq!(first_words, config);
    assert_eq!(second_words, config);
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(output, usize::from(first_next))?,
        punt
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(punt, usize::from(duplicate_next))?,
        punt
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(punt, usize::from(end_next))?,
        drop_node
    );
    features.disable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        first_interface,
        &[99],
    )?;
    assert_eq!(features.feature_count(output_arc, first_interface)?, 2);
    features.disable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        first_interface,
        &config,
    )?;
    assert_eq!(features.feature_count(output_arc, first_interface)?, 1);
    let shared_config_index = features
        .feature_config_index(output_arc, first_interface)?
        .expect("one enabled feature has a compiled config");

    features.enable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        second_interface,
        &config,
    )?;
    assert_eq!(
        features.feature_config_index(output_arc, second_interface)?,
        Some(shared_config_index),
        "equal configurations in one arc share one config entry"
    );
    features.modify_feature_arc_end(&mut runtime, mirror_arc, first_interface, punt)?;
    assert_eq!(
        features.feature_arc_end_node(mirror_arc, first_interface)?,
        punt
    );
    assert_eq!(features.feature_count(mirror_arc, first_interface)?, 0);
    assert!(!features.has_features(mirror_arc, first_interface));
    assert!(
        features
            .feature_config_index(mirror_arc, first_interface)?
            .is_some()
    );
    features.reset_feature_arc_end(&mut runtime, mirror_arc, first_interface)?;
    assert_eq!(
        features.feature_arc_end_node(mirror_arc, first_interface)?,
        drop_node
    );
    assert!(
        features
            .feature_config_index(mirror_arc, first_interface)?
            .is_some(),
        "reset retains the compiled empty/default configuration"
    );
    features.enable_feature(
        &mut runtime,
        mirror_arc,
        mirror_punt,
        first_interface,
        &config,
    )?;
    assert_ne!(
        features.feature_config_index(mirror_arc, first_interface)?,
        Some(shared_config_index),
        "each FeatureConfigMain owns its own config pool"
    );

    features.enable_feature(&mut runtime, output_arc, drop_feature, first_interface, &[])?;
    assert_eq!(features.feature_count(output_arc, first_interface)?, 2);
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot_for_target(drop_node, drop_node)?,
        None,
        "an explicitly enabled end node does not acquire a self edge"
    );

    let mut buffer = empty_packet_buffer();
    let ordinary_next =
        features.start_feature_arc(output_arc, first_interface, &mut buffer, u16::MAX);
    buffer.set_current_config_index(u32::MAX);
    let cached_next =
        features.start_feature_arc_at_config(output_arc, shared_config_index, &mut buffer);
    assert_eq!(ordinary_next, cached_next);
    let (words, next) = features.next_feature_with_config::<2>(&mut buffer);
    assert_eq!(words, config);
    assert_eq!(
        runtime.nodes().node_next_slot(punt, usize::from(next))?,
        drop_node
    );
    interfaces.delete_hardware_interface(&mut runtime, first_hardware)?;
    assert_eq!(features.feature_count(output_arc, second_interface)?, 1);
    assert_eq!(
        features.feature_config_index(output_arc, second_interface)?,
        Some(shared_config_index),
        "deleting one interface retains a shared config"
    );
    let replacement_hardware = interfaces.register_hardware_interface(
        &mut runtime,
        device_class_index,
        3,
        hw_class_index,
        0,
    )?;
    let replacement_interface = interfaces
        .hardware_interface(replacement_hardware)
        .sw_if_index();
    assert_eq!(replacement_interface, first_interface);
    assert_eq!(
        features.feature_count(output_arc, replacement_interface)?,
        0
    );
    assert!(!features.has_features(output_arc, replacement_interface));
    assert_eq!(
        features.feature_config_index(output_arc, replacement_interface)?,
        None
    );
    assert_eq!(
        features.feature_config_index(mirror_arc, replacement_interface)?,
        None,
        "interface deletion clears every arc before index reuse"
    );

    interfaces.delete_hardware_interface(&mut runtime, second_hardware)?;
    features.enable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        replacement_interface,
        &config,
    )?;
    assert_eq!(
        features.feature_config_index(output_arc, replacement_interface)?,
        Some(shared_config_index),
        "the shared Heap reuses a fully released extent"
    );
    features.disable_feature(
        &mut runtime,
        output_arc,
        punt_feature,
        replacement_interface,
        &config,
    )?;
    assert_eq!(
        features.feature_count(output_arc, replacement_interface)?,
        0
    );
    assert!(!features.has_features(output_arc, replacement_interface));
    let empty_config_index = features
        .feature_config_index(output_arc, replacement_interface)?
        .expect("deleting the final feature retains the compiled empty config");

    let mut buffer = empty_packet_buffer();
    let cursor = buffer.current_config_index();
    assert_eq!(
        features.start_feature_arc(output_arc, replacement_interface, &mut buffer, 7),
        7
    );
    assert_eq!(buffer.current_config_index(), cursor);
    let empty_next =
        features.start_feature_arc_at_config(output_arc, empty_config_index, &mut buffer);
    assert_eq!(
        runtime
            .nodes()
            .node_next_slot(output, usize::from(empty_next))?,
        drop_node
    );
    interfaces.delete_hardware_interface(&mut runtime, replacement_hardware)?;
    let final_hardware = interfaces.register_hardware_interface(
        &mut runtime,
        device_class_index,
        4,
        hw_class_index,
        0,
    )?;
    let final_interface = interfaces.hardware_interface(final_hardware).sw_if_index();
    assert_eq!(final_interface, replacement_interface);
    assert_eq!(
        features.feature_config_index(output_arc, final_interface)?,
        None,
        "deletion releases even a count-zero compiled config"
    );
    interfaces.delete_hardware_interface(&mut runtime, final_hardware)?;

    Ok(())
}
