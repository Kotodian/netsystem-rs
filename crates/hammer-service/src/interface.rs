use hammer_core::data_plane::{Frame, NodeId, NodeRegistration};
use hammer_runtime::{
    DataPlaneMain, InternalNode, Node, NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity,
    NodeProcessFn, NodeRuntime, RuntimeError, RuntimeResult, process_frame,
};

pub use crate::interface_model::*;
use crate::net::NetMain;
use crate::opaque::NetworkOpaque;

pub const DEFAULT_INTERFACE_MTU: u32 = 9_000;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterfaceConfig {
    pub name: String,
    #[serde(default)]
    pub mtu: InterfaceConfigMtu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct InterfaceConfigMtu {
    pub l3: u32,
    pub ip4: u32,
    pub ip6: u32,
    pub mpls: u32,
}

impl Default for InterfaceConfigMtu {
    fn default() -> Self {
        Self {
            l3: DEFAULT_INTERFACE_MTU,
            ip4: DEFAULT_INTERFACE_MTU,
            ip6: DEFAULT_INTERFACE_MTU,
            mpls: DEFAULT_INTERFACE_MTU,
        }
    }
}

#[hammer_component_macros::runtime_error(subsystem = "interface")]
#[derive(Debug, thiserror::Error)]
pub enum InterfaceError {
    #[error("interface name is empty")]
    NameEmpty,
    #[error("interface index space is exhausted at {interface_count} interfaces")]
    IndexSpaceExhausted { interface_count: usize },
    #[error("interface {interface_index} is not registered")]
    NotRegistered { interface_index: u32 },
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

pub type InterfaceResult<T> = Result<T, InterfaceError>;

impl InterfaceConfig {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.name.is_empty() {
            return Err(RuntimeError::config_validation(
                "interface.name must be non-empty",
            ));
        }
        let mtu = self.mtu;
        if mtu.l3 == 0 || mtu.ip4 == 0 || mtu.ip6 == 0 || mtu.mpls == 0 {
            return Err(RuntimeError::config_validation(
                "interface.mtu values must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterfaceMtu {
    values: [u32; InterfaceMtuKind::COUNT],
}

impl InterfaceMtu {
    pub const fn new(l3: u32, ip4: u32, ip6: u32, mpls: u32) -> Self {
        Self {
            values: [l3, ip4, ip6, mpls],
        }
    }
    pub fn get(&self, kind: InterfaceMtuKind) -> u32 {
        self.values[kind.slot()]
    }
    pub fn set(&mut self, kind: InterfaceMtuKind, value: u32) {
        self.values[kind.slot()] = value;
    }
    pub fn l3(&self) -> u32 {
        self.get(InterfaceMtuKind::L3)
    }
    pub fn ip4(&self) -> u32 {
        self.get(InterfaceMtuKind::Ip4)
    }
    pub fn ip6(&self) -> u32 {
        self.get(InterfaceMtuKind::Ip6)
    }
    pub fn mpls(&self) -> u32 {
        self.get(InterfaceMtuKind::Mpls)
    }
}

impl Default for InterfaceMtu {
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceMtuKind {
    L3,
    Ip4,
    Ip6,
    Mpls,
}

impl InterfaceMtuKind {
    const COUNT: usize = 4;
    const fn slot(self) -> usize {
        match self {
            Self::L3 => 0,
            Self::Ip4 => 1,
            Self::Ip6 => 2,
            Self::Mpls => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum InterfaceOutputError {
    InterfaceDown,
    InterfaceDeleted,
    NoTxQueue,
}

impl NodeErrorCode for InterfaceOutputError {
    fn local_code(self) -> u16 {
        self as u16
    }
}

pub(crate) const INTERFACE_OUTPUT_ERROR_DESCRIPTORS: [NodeErrorDescriptor; 3] = [
    NodeErrorDescriptor::new(
        "interface-down",
        NodeErrorSeverity::Error,
        "Interface is down",
    ),
    NodeErrorDescriptor::new(
        "interface-deleted",
        NodeErrorSeverity::Error,
        "Interface is deleted",
    ),
    NodeErrorDescriptor::new(
        "no-tx-queue",
        NodeErrorSeverity::Error,
        "No transmit queue is assigned to this worker",
    ),
];

#[hammer_component_macros::graph_node(graph = service, init = register_interface_output_graph, name = "interface-output")]
#[derive(Debug, Clone, Copy)]
pub struct InterfaceOutputNode;

fn register_interface_output_graph(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal(InterfaceOutputNode)
}

impl InterfaceOutputNode {
    fn next_for_index(runtime: &DataPlaneMain, index: u32) -> u16 {
        let interface_index = {
            let buffer = runtime.buffer(index);
            let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
            network.sw_if_index[1]
        };
        assert_ne!(
            interface_index,
            u32::MAX,
            "interface-output requires a TX software interface"
        );
        NetMain::global()
            .expect("interface-output requires the network Main")
            .interface_main()
            .output_node_next_index_for_sw_interface(interface_index)
    }
}

impl Node for InterfaceOutputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = interface_output_process;
        process(runtime, node_runtime, frame)
    }
}

impl InternalNode for InterfaceOutputNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next("interface-output", 0))
    }
}

fn interface_output_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    (|| {
        process_frame!(runtime, node_runtime, frame, |index| {
            InterfaceOutputNode::next_for_index(runtime, index)
        });
    })();
    processed_vectors
}

#[hammer_component_macros::graph_node(
    graph = service,
    kind = internal,
    name = "interface-output-arc-end"
)]
pub struct InterfaceOutputArcEndNode;

impl Node for InterfaceOutputArcEndNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let processed = frame.len();
        let interfaces = NetMain::global()
            .expect("interface-output-arc-end requires the network Main")
            .interface_main();
        process_frame!(runtime, node_runtime, frame, |index| {
            let sw_if_index =
                hammer_core::buffer_opaque!(runtime.buffer(index) => NetworkOpaque).sw_if_index[1];
            let entry = interfaces.interface_lookup_entry(sw_if_index);
            assert!(
                entry.has_hardware(),
                "arc-end requires a live hardware mapping"
            );
            assert!(entry.has_arc_end(), "arc-end requires a published TX next");
            let hardware = interfaces.hardware_interface(entry.hw_if_index);
            let has_queue = hardware.tx_queue_indices.is_empty()
                || runtime.data_worker_id().is_ok_and(|worker| {
                    interfaces.has_tx_queue_for_worker(worker, entry.hw_if_index)
                });
            if has_queue {
                runtime.buffer_mut(index).clear_node_error();
                entry.if_out_arc_end_next_index
            } else {
                let error_index = runtime
                    .record_current_node_error(InterfaceOutputError::NoTxQueue)
                    .expect("interface arc-end errors are registered before dispatch");
                runtime.buffer_mut(index).set_node_error_index(error_index);
                0
            }
        });
        processed
    }
}

pub(crate) fn interface_output_template(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed = frame.len();
    let hw_if_index = u32::try_from(node_runtime.word(0))
        .expect("interface output runtime hardware index fits u32");
    let interfaces = NetMain::global()
        .expect("interface output requires the network Main")
        .interface_main();
    let deleted = node_runtime.word(3) != 0;
    if deleted {
        process_frame!(runtime, node_runtime, frame, |index| {
            let error_index = runtime
                .record_current_node_error(InterfaceOutputError::InterfaceDeleted)
                .expect("interface output errors are registered before dispatch");
            runtime.buffer_mut(index).set_node_error_index(error_index);
            0
        });
        return processed;
    }

    let hardware = interfaces.hardware_interface(hw_if_index);
    let sw_if_index = hardware.sw_if_index;
    let is_up = hardware.flags.contains(HwInterfaceFlags::LINK_UP)
        && interfaces
            .software_interface(sw_if_index)
            .expect("hardware software interface remains live")
            .flags
            .contains(SwInterfaceFlags::ADMIN_UP);
    let has_queue = hardware.tx_queue_indices.is_empty()
        || runtime
            .data_worker_id()
            .is_ok_and(|worker| interfaces.has_tx_queue_for_worker(worker, hw_if_index));
    let arc_index = interfaces.output_feature_arc_index();
    let features = crate::feature::FeatureMain::global()
        .expect("FeatureMain exists before interface output dispatch");
    process_frame!(runtime, node_runtime, frame, |index| {
        let error = if !is_up {
            Some(InterfaceOutputError::InterfaceDown)
        } else if !has_queue {
            Some(InterfaceOutputError::NoTxQueue)
        } else {
            None
        };
        if let Some(error) = error {
            let error_index = runtime
                .record_current_node_error(error)
                .expect("interface output errors are registered before dispatch");
            runtime.buffer_mut(index).set_node_error_index(error_index);
            0
        } else {
            runtime.buffer_mut(index).clear_node_error();
            features.start_feature_arc(arc_index, sw_if_index, runtime.buffer_mut(index), 1)
        }
    });
    processed
}

#[hammer_component_macros::main_loop_enter_function(
    name = "interface_output_feature_init",
    runs_after = ["device_input_feature_init"],
    runs_before = ["feature_arc_init"]
)]
fn interface_output_feature_init(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let features = crate::feature::FeatureMain::global()?;
    let arc_end = main
        .nodes()
        .node_by_name("interface-output-arc-end")
        .expect("interface-output-arc-end is materialized");
    main.register_node_errors(arc_end, &INTERFACE_OUTPUT_ERROR_DESCRIPTORS)?;
    let arc_index = features
        .register_feature_arc("interface-output", &[], Some("interface-output-arc-end"))
        .map_err(|source| RuntimeError::GraphNodeInitialization {
            node: "interface-output",
            source: Box::new(source),
        })?;
    features
        .register_feature(
            "interface-output",
            "interface-output-arc-end",
            arc_end,
            &[],
            &[],
        )
        .map_err(|source| RuntimeError::GraphNodeInitialization {
            node: "interface-output-arc-end",
            source: Box::new(source),
        })?;
    let public_output = main
        .nodes()
        .node_by_name("interface-output")
        .expect("interface-output is materialized");
    let drop_node = main
        .nodes()
        .node_by_name("drop")
        .expect("drop is materialized");
    NetMain::global()?.interface_main().complete_output_graph(
        main,
        arc_index,
        public_output,
        arc_end,
        drop_node,
    );
    Ok(())
}
