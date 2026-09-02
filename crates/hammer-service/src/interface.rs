use std::mem::transmute;

use hammer_core::data_plane::{BufferFrame, Index, NodeId, NodeRegistration};
use hammer_runtime::{
    DataPlaneMain, InternalNode, Node, NodeProcessFn, NodeRuntimeData, RuntimeError, RuntimeResult,
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
    runtime.nodes().try_register_internal(InterfaceOutputNode)
}

impl InterfaceOutputNode {
    fn tx_for_index(runtime: &DataPlaneMain, index: Index, drop_next: u16) -> RuntimeResult<u16> {
        let interface_index = {
            let buffer = runtime.get_buffer(index)?;
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
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
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        interface_output_process(runtime, NodeRuntimeData::empty(), frame)
    }
    fn node_process(&self) -> NodeProcessFn {
        interface_output_process
    }
}

impl InternalNode for InterfaceOutputNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next("interface-output", 0))
    }
}

fn interface_output_process(runtime: &DataPlaneMain, _: NodeRuntimeData, frame: &mut BufferFrame) {
    process_frame!(runtime, frame, |index| InterfaceOutputNode::tx_for_index(
        runtime, index, 0
    )
    .unwrap_or(0));
}
