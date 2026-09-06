use crate::ip::input::{Ip4InputNode, Ip6InputNode};
use crate::ip::local::{Ip4LocalEndOfArcNode, Ip4LocalNode, Ip6LocalEndOfArcNode, Ip6LocalNode};
use crate::lookup::{IP4_MAIN, IP6_MAIN, Ip4LookupNode, Ip6LookupNode};
use hammer_core::data_plane::{BufferFrame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, RuntimeResult};
use hammer_service::interface::feature::FeatureError;
use hammer_service::net::{DpoProto, DpoType, NetMain};
use hammer_service::opaque::NetworkOpaque;

#[hammer_component_macros::node_next]
pub enum Ip4PuntNext {
    #[next("punt")]
    Punt,
}

#[hammer_component_macros::feature_arc(name = "ip4-punt", start_nodes = [Ip4PuntNode], last_in_arc = hammer_service::data_plane::PuntNode)]
#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_punt, role = internal, name = "ip4-punt", next = Ip4PuntNext)]
pub struct Ip4PuntNode;

fn register_ip4_punt(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4PuntNode::new(), &Ip4PuntNext::NEXT_NAMES)?;
    NetMain::global()?
        .register_dpo(
            Some(DpoType::PUNT),
            &[(DpoProto::IP4, &[node])],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .map_err(
            |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
                node: Ip4PuntNode::NODE_NAME,
                source: Box::new(source),
            },
        )?;
    Ok(node)
}

impl Node for Ip4PuntNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        (self.node_process())(runtime, Default::default(), frame)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| {
            let net = NetMain::global().expect("IP node requires initialized NetMain");
            // SAFETY: startup installs this scalar before workers execute nodes.
            let arc = unsafe {
                *IP4_MAIN
                    .get()
                    .expect("IP main initialized")
                    .punt_feature_arc_index
                    .get()
            };
            hammer_runtime::process_frame!(runtime, frame, |index| {
                let mut buffer = runtime.get_buffer_mut(index).expect("frame owns buffer");
                // SAFETY: IP ingress initializes the network overlay.
                let sw_if_index =
                    unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) }.sw_if_index
                        [0];
                net.interface_main().start_feature_arc(
                    arc,
                    sw_if_index,
                    &mut buffer,
                    NodeNext::slot(Ip4PuntNext::Punt),
                )
            });
        }
    }
}

#[hammer_component_macros::node_next]
pub enum Ip4DropNext {
    #[next("drop")]
    Drop,
}

#[hammer_component_macros::feature_arc(name = "ip4-drop", start_nodes = [Ip4DropNode], last_in_arc = hammer_service::data_plane::DropNode)]
#[hammer_component_macros::graph_node(graph = ip, kind = internal, name = "ip4-drop", next = Ip4DropNext)]
pub struct Ip4DropNode;

impl Node for Ip4DropNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        (self.node_process())(runtime, Default::default(), frame)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| {
            let net = NetMain::global().expect("IP node requires initialized NetMain");
            // SAFETY: startup installs this scalar before workers execute nodes.
            let arc = unsafe {
                *IP4_MAIN
                    .get()
                    .expect("IP main initialized")
                    .drop_feature_arc_index
                    .get()
            };
            hammer_runtime::process_frame!(runtime, frame, |index| {
                let mut buffer = runtime.get_buffer_mut(index).expect("frame owns buffer");
                // SAFETY: IP ingress initializes the network overlay.
                let sw_if_index =
                    unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) }.sw_if_index
                        [0];
                net.interface_main().start_feature_arc(
                    arc,
                    sw_if_index,
                    &mut buffer,
                    NodeNext::slot(Ip4DropNext::Drop),
                )
            });
        }
    }
}

#[hammer_component_macros::node_next]
pub enum Ip6PuntNext {
    #[next("punt")]
    Punt,
}

#[hammer_component_macros::feature_arc(name = "ip6-punt", start_nodes = [Ip6PuntNode], last_in_arc = hammer_service::data_plane::PuntNode)]
#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_punt, role = internal, name = "ip6-punt", next = Ip6PuntNext)]
pub struct Ip6PuntNode;

fn register_ip6_punt(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6PuntNode::new(), &Ip6PuntNext::NEXT_NAMES)?;
    NetMain::global()?
        .register_dpo(
            Some(DpoType::PUNT),
            &[(DpoProto::IP6, &[node])],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .map_err(
            |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
                node: Ip6PuntNode::NODE_NAME,
                source: Box::new(source),
            },
        )?;
    Ok(node)
}

impl Node for Ip6PuntNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        (self.node_process())(runtime, Default::default(), frame)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| {
            let net = NetMain::global().expect("IP node requires initialized NetMain");
            // SAFETY: startup installs this scalar before workers execute nodes.
            let arc = unsafe {
                *IP6_MAIN
                    .get()
                    .expect("IP main initialized")
                    .punt_feature_arc_index
                    .get()
            };
            hammer_runtime::process_frame!(runtime, frame, |index| {
                let mut buffer = runtime.get_buffer_mut(index).expect("frame owns buffer");
                // SAFETY: IP ingress initializes the network overlay.
                let sw_if_index =
                    unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) }.sw_if_index
                        [0];
                net.interface_main().start_feature_arc(
                    arc,
                    sw_if_index,
                    &mut buffer,
                    NodeNext::slot(Ip6PuntNext::Punt),
                )
            });
        }
    }
}

#[hammer_component_macros::node_next]
pub enum Ip6DropNext {
    #[next("drop")]
    Drop,
}

#[hammer_component_macros::feature_arc(name = "ip6-drop", start_nodes = [Ip6DropNode], last_in_arc = hammer_service::data_plane::DropNode)]
#[hammer_component_macros::graph_node(graph = ip, kind = internal, name = "ip6-drop", next = Ip6DropNext)]
pub struct Ip6DropNode;

impl Node for Ip6DropNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        (self.node_process())(runtime, Default::default(), frame)
    }
    fn node_process(&self) -> NodeProcessFn {
        |runtime, _, frame| {
            let net = NetMain::global().expect("IP node requires initialized NetMain");
            // SAFETY: startup installs this scalar before workers execute nodes.
            let arc = unsafe {
                *IP6_MAIN
                    .get()
                    .expect("IP main initialized")
                    .drop_feature_arc_index
                    .get()
            };
            hammer_runtime::process_frame!(runtime, frame, |index| {
                let mut buffer = runtime.get_buffer_mut(index).expect("frame owns buffer");
                // SAFETY: IP ingress initializes the network overlay.
                let sw_if_index =
                    unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) }.sw_if_index
                        [0];
                net.interface_main().start_feature_arc(
                    arc,
                    sw_if_index,
                    &mut buffer,
                    NodeNext::slot(Ip6DropNext::Drop),
                )
            });
        }
    }
}

#[hammer_component_macros::init_function(name = "ip_feature_init", runs_after = ["install_packet_graph"], runs_before = ["interface_feature_init"])]
fn ip_feature_init(engine: &mut hammer_runtime::GlobalMain) -> RuntimeResult<()> {
    let net = NetMain::global()?;
    let interfaces = net.interface_main();
    let nodes = engine.data_plane_main().nodes();
    let install = || -> Result<(), FeatureError> {
        let main = IP4_MAIN.get().expect("IP main initialized before graph");
        let arc = Ip4InputNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.unicast_feature_arc_index.get() = arc;
        }
        let arc = Ip4LocalNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.local_feature_arc_index.get() = arc;
        }
        let arc = Ip4PuntNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.punt_feature_arc_index.get() = arc;
        }
        let arc = Ip4DropNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.drop_feature_arc_index.get() = arc;
        }
        Ip4LookupNode::register_feature(interfaces, nodes)?;
        Ip4LocalEndOfArcNode::register_feature(interfaces, nodes)?;
        let node = nodes
            .node_by_name(hammer_service::data_plane::PuntNode::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: hammer_service::data_plane::PuntNode::NODE_NAME,
            })?;
        interfaces.register_feature(
            Ip4PuntNode::FEATURE_ARC_NAME,
            hammer_service::data_plane::PuntNode::NODE_NAME,
            node,
            &[],
            &[],
        )?;
        let node = nodes
            .node_by_name(hammer_service::data_plane::DropNode::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: hammer_service::data_plane::DropNode::NODE_NAME,
            })?;
        interfaces.register_feature(
            Ip4DropNode::FEATURE_ARC_NAME,
            hammer_service::data_plane::DropNode::NODE_NAME,
            node,
            &[],
            &[],
        )?;
        let main = IP6_MAIN.get().expect("IP main initialized before graph");
        let arc = Ip6InputNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.unicast_feature_arc_index.get() = arc;
        }
        let arc = Ip6LocalNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.local_feature_arc_index.get() = arc;
        }
        let arc = Ip6PuntNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.punt_feature_arc_index.get() = arc;
        }
        let arc = Ip6DropNode::register_feature_arc(interfaces, nodes)?;
        // SAFETY: feature declarations are installed before workers start.
        unsafe {
            *main.drop_feature_arc_index.get() = arc;
        }
        Ip6LookupNode::register_feature(interfaces, nodes)?;
        Ip6LocalEndOfArcNode::register_feature(interfaces, nodes)?;
        let node = nodes
            .node_by_name(hammer_service::data_plane::PuntNode::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: hammer_service::data_plane::PuntNode::NODE_NAME,
            })?;
        interfaces.register_feature(
            Ip6PuntNode::FEATURE_ARC_NAME,
            hammer_service::data_plane::PuntNode::NODE_NAME,
            node,
            &[],
            &[],
        )?;
        let node = nodes
            .node_by_name(hammer_service::data_plane::DropNode::NODE_NAME)
            .ok_or(FeatureError::NodeNotFound {
                name: hammer_service::data_plane::DropNode::NODE_NAME,
            })?;
        interfaces.register_feature(
            Ip6DropNode::FEATURE_ARC_NAME,
            hammer_service::data_plane::DropNode::NODE_NAME,
            node,
            &[],
            &[],
        )?;
        Ok(())
    };
    install().map_err(
        |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
            node: "ip4-input",
            source: Box::new(source),
        },
    )
}
