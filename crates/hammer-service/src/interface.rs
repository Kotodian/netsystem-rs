use hammer_core::data_plane::{Frame, NodeId, NodeRegistration};
use hammer_runtime::{
    DataPlaneMain, InternalNode, Node, NodeProcessFn, NodeRuntime, RuntimeError, RuntimeResult,
    add_packet_trace, process_frame,
};
use ipnet::IpNet;

pub use crate::interface_model::*;
use crate::net::NetMain;
use crate::opaque::NetworkOpaque;

pub const DEFAULT_INTERFACE_MTU: u32 = 9_000;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterfaceConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<IpNet>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InterfaceOutputTraceError {
    MissingEgressInterface,
    MissingTxNode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct InterfaceOutputTrace {
    pub egress_interface: Option<u32>,
    pub tx_next: Option<u16>,
    pub error: Option<InterfaceOutputTraceError>,
    pub next: Option<u16>,
}

#[hammer_component_macros::graph_node(graph = service, init = register_interface_output_graph, name = "interface-output")]
#[derive(Debug, Clone, Copy)]
pub struct InterfaceOutputNode;

fn register_interface_output_graph(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal(InterfaceOutputNode)?;
    let net = NetMain::global()?;
    net.register_dpo(
        Some(crate::net::DpoType::INTERFACE_TX),
        &[],
        None,
        Some(InterfaceMain::interface_tx_nodes),
        None,
        None,
        None,
        None,
        None,
    )
    .map_err(|source| RuntimeError::GraphNodeInitialization {
        node: "interface-output",
        source: Box::new(source),
    })?;
    net.interface_main().initialize_output_node(node);
    Ok(node)
}

impl InterfaceOutputNode {
    fn tx_for_index(runtime: &DataPlaneMain, index: u32, drop_next: u16) -> RuntimeResult<u16> {
        let interface_index = {
            let buffer = runtime.buffer(index);
            let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
            network.sw_if_index[1]
        };
        if interface_index == u32::MAX {
            let _ = add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: None,
                    tx_next: None,
                    error: Some(InterfaceOutputTraceError::MissingEgressInterface),
                    next: Some(drop_next)
                }
            );
            return Ok(drop_next);
        }
        let worker = runtime.data_worker_id()?;
        let Some(net) = NetMain::global().ok() else {
            return Ok(drop_next);
        };
        let Some(tx) = net
            .interface_main()
            .tx_slot_for_worker(worker, interface_index)
        else {
            let _ = add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: Some(interface_index),
                    tx_next: None,
                    error: Some(InterfaceOutputTraceError::MissingTxNode),
                    next: Some(drop_next)
                }
            );
            return Ok(drop_next);
        };
        let _ = add_packet_trace!(
            runtime,
            index,
            InterfaceOutputTrace {
                egress_interface: Some(interface_index),
                tx_next: Some(tx),
                error: None,
                next: Some(tx)
            }
        );
        Ok(tx)
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
    _: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    (|| {
        process_frame!(runtime, frame, |index| InterfaceOutputNode::tx_for_index(
            runtime, index, 0
        )
        .unwrap_or(0));
    })();
    processed_vectors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{DpoError, DpoId, DpoProto, DpoType};
    use hammer_runtime::{DataPlaneBufferConfig, GlobalMain, RuntimeRegistry};
    use std::sync::Arc;

    #[test]
    fn interface_tx_stack_uses_the_interface_output_node() -> Result<(), DpoError> {
        hammer_runtime::config::Memory::default().ensure_main_heap()?;
        hammer_core::buffer::BufferMain::new(64, 1024, &[0], 2, hammer_infra::PageSize::Default)
            .unwrap();
        let mut main = GlobalMain::new(
            DataPlaneMain::new(DataPlaneBufferConfig::default()),
            RuntimeRegistry::new(),
        );
        main.install_current();
        let net = NetMain::init(Arc::new(InterfaceMain::new()))?;
        let runtime = main.data_plane_main_mut();
        let child = crate::data_plane::register_drop(runtime)?;
        let output = register_interface_output_graph(runtime)?;
        let interfaces = net.interface_main();
        let hardware = interfaces.register_hardware_interface(0, 1, 0, 0).unwrap();
        let software = interfaces.hardware_interface(hardware).unwrap().sw_if_index;
        assert_eq!(
            interfaces
                .software_interface(software)
                .unwrap()
                .sup_sw_if_index,
            software
        );
        for software in [net.local_interface_sw_index(), software] {
            let dpo = net
                .dpo_main()
                .identity(DpoType::INTERFACE_TX, DpoProto::IP4, software)?;
            let stacked = net.dpo_main_mut().stack_from_node(runtime, child, dpo)?;
            assert_eq!(stacked.index(), software);
            assert_eq!(
                Some(stacked.next()),
                runtime.nodes().node_next_slot_for_target(child, output)?
            );
            net.lock_dpo(dpo);
            net.unlock_dpo(dpo);
        }
        interfaces.delete_hardware_interface(hardware).unwrap();
        assert!(matches!(
            net.dpo_main_mut().stack_from_node(
                runtime,
                child,
                DpoId::interface_tx(DpoProto::IP4, software)
            ),
            Err(DpoError::NodeMissing { .. })
        ));
        main.close()?;
        GlobalMain::uninstall_current();
        Ok(())
    }
}
