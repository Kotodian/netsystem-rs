//! Device class abstraction for VPP-aligned hardware interface drivers.
//!
//! Mirrors VPP `vnet_device_class_t` + `vnet_hw_if_rxq`/`txq` + polling/interrupt
//! input node layer. Each device class (TUN, future af_packet, ...)
//! bundles its input/output driver node ids with a `DeviceMain` queue registry.
//!
//! # Synchronization contract (VPP-style, lock-free dataplane)
//!
//! All mutation of `DeviceMain` shared state follows VPP's barrier discipline,
//! not per-field mutexes:
//!
//! Queue registration is a control-plane operation completed before workers
//! compile their immutable RX/TX queue vectors. Packet nodes consume only those
//! worker-local vectors and never access this registry on the hot path.

use std::cell::UnsafeCell;
use std::sync::Arc;

use hammer_core::data_plane::{BufferFrame, NodeId};
use hammer_infra::bitmap::Bitmap;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, GlobalMain, Node, NodeProcessFn, NodeRuntimeData,
};

use crate::interface::InterfaceOutputNode;

#[hammer_component_macros::node_next]
pub enum DeviceInputNext {
    #[next("ip-input")]
    Ip4Input,
    #[next("ip-input")]
    Ip6Input,
    #[next("drop")]
    Drop,
}

/// Abstract device-input owner node. Concrete drivers (tun, …) register as siblings.
/// Builtin of the shared device abstraction — not a loadable plugin.
#[hammer_component_macros::graph_node(
    graph = service,
    name = "device-input",
    next = DeviceInputNext,
    kind = driver,
    state = disabled,
)]
#[derive(Debug, Clone, Copy)]
pub struct DeviceInputNode;

impl Node for DeviceInputNode {
    #[inline(always)]
    fn process(&mut self, _: &DataPlaneMain, _: &mut BufferFrame) -> () {
        ()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        device_input_process
    }
}

fn device_input_process(_: &DataPlaneMain, _: NodeRuntimeData, _: &mut BufferFrame) -> () {
    ()
}

/// Hardware interface RX/TX queue schedule mode.
///
/// VPP `vnet_hw_if_rxq` supports polling (node always polls), interrupt (node
/// scheduled on RX event), and adaptive (poll under load, interrupt otherwise).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverScheduleMode {
    Poll,
    Interrupt,
    Adaptive,
}

/// Device queue ownership registry, analogous to VPP's hardware RX/TX queues.
pub struct DeviceMain {
    nodes: hammer_runtime::NodeRuntime,
    devices: UnsafeCell<Vec<DeviceRegistration>>,
    rx_queues: UnsafeCell<Vec<DeviceRxQueue>>,
    tx_queues: UnsafeCell<Vec<DeviceTxQueue>>,
}

#[derive(Debug, thiserror::Error)]
pub enum DeviceError {
    #[error("interface {interface_index} already has a device")]
    InterfaceAlreadyRegistered { interface_index: u32 },
    #[error("device instance space is exhausted")]
    InstanceSpaceExhausted,
    #[error("device instance {device_instance} is not registered")]
    DeviceNotRegistered { device_instance: u32 },
    #[error("RX queue {queue_id} is already registered for device {device_instance}")]
    RxQueueAlreadyRegistered { device_instance: u32, queue_id: u32 },
    #[error("TX queue {queue_id} is already registered for device {device_instance}")]
    TxQueueAlreadyRegistered { device_instance: u32, queue_id: u32 },
    #[error("TX queue {queue_id} is not registered for device {device_instance}")]
    TxQueueNotRegistered { device_instance: u32, queue_id: u32 },
    #[error("required graph node `{name}` is not registered")]
    GraphNodeMissing { name: &'static str },
    #[error(transparent)]
    Runtime(#[from] hammer_runtime::RuntimeError),
}

/// One device instance owned by the service-wide device registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceRegistration {
    pub instance: u32,
    pub interface_index: u32,
    pub input_node: NodeId,
    pub output_node: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceRxQueue {
    pub device_instance: u32,
    pub queue_id: u32,
    pub owner: DataWorkerId,
    pub mode: DriverScheduleMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTxQueue {
    pub interface_index: u32,
    pub device_instance: u32,
    pub queue_id: u32,
    pub(crate) output_slot: u16,
    pub(crate) drop_slot: u16,
    assigned_workers: Bitmap<DataWorkerId>,
}

impl DeviceTxQueue {
    #[inline]
    pub fn is_assigned_to(&self, worker: DataWorkerId) -> bool {
        self.assigned_workers.is_set(worker)
    }

    #[inline]
    pub fn is_shared(&self) -> bool {
        self.assigned_workers.count_set() > 1
    }
}

// SAFETY: queue registration finishes before worker startup. Published queue
// records are immutable while data workers compile and use their local views.
unsafe impl Send for DeviceMain {}
unsafe impl Sync for DeviceMain {}

impl DeviceMain {
    #[inline]
    pub fn new(nodes: hammer_runtime::NodeRuntime) -> Arc<Self> {
        Arc::new(Self {
            nodes,
            devices: UnsafeCell::new(Vec::new()),
            rx_queues: UnsafeCell::new(Vec::new()),
            tx_queues: UnsafeCell::new(Vec::new()),
        })
    }

    /// Allocate and publish one device instance before registering its queues.
    pub fn register_device(
        &self,
        interface_index: u32,
        input_node: NodeId,
        output_node: NodeId,
    ) -> Result<DeviceRegistration, DeviceError> {
        let devices = unsafe { &mut *self.devices.get() };
        if devices
            .iter()
            .any(|device| device.interface_index == interface_index)
        {
            return Err(DeviceError::InterfaceAlreadyRegistered { interface_index });
        }
        let instance =
            u32::try_from(devices.len()).map_err(|_| DeviceError::InstanceSpaceExhausted)?;
        let registration = DeviceRegistration {
            instance,
            interface_index,
            input_node,
            output_node,
        };
        devices.push(registration);
        Ok(registration)
    }

    #[inline]
    pub fn device(&self, instance: u32) -> Option<DeviceRegistration> {
        let devices = unsafe { &*self.devices.get() };
        devices.get(instance as usize).copied()
    }

    pub fn register_rx_queue(
        &self,
        device_instance: u32,
        queue_id: u32,
        owner: DataWorkerId,
        mode: DriverScheduleMode,
    ) -> Result<(), DeviceError> {
        if self.device(device_instance).is_none() {
            return Err(DeviceError::DeviceNotRegistered { device_instance });
        }
        let queues = unsafe { &mut *self.rx_queues.get() };
        if queues
            .iter()
            .any(|queue| queue.device_instance == device_instance && queue.queue_id == queue_id)
        {
            return Err(DeviceError::RxQueueAlreadyRegistered {
                device_instance,
                queue_id,
            });
        }
        queues.push(DeviceRxQueue {
            device_instance,
            queue_id,
            owner,
            mode,
        });
        Ok(())
    }

    pub fn register_tx_queue(
        &self,
        device_instance: u32,
        queue_id: u32,
        owner: DataWorkerId,
    ) -> Result<(), DeviceError> {
        let Some(device) = self.device(device_instance) else {
            return Err(DeviceError::DeviceNotRegistered { device_instance });
        };
        let queues = unsafe { &mut *self.tx_queues.get() };
        if queues
            .iter()
            .any(|queue| queue.device_instance == device_instance && queue.queue_id == queue_id)
        {
            return Err(DeviceError::TxQueueAlreadyRegistered {
                device_instance,
                queue_id,
            });
        }
        let interface_output =
            self.nodes
                .node_by_name("interface-output")
                .ok_or(DeviceError::GraphNodeMissing {
                    name: "interface-output",
                })?;
        let drop = self
            .nodes
            .node_by_name("drop")
            .ok_or(DeviceError::GraphNodeMissing { name: "drop" })?;
        let output_slot = self
            .nodes
            .add_node_next_slot(interface_output, device.output_node)?;
        let drop_slot = self.nodes.add_node_next_slot(interface_output, drop)?;
        let mut assigned_workers = Bitmap::new();
        assigned_workers.set(owner);
        queues.push(DeviceTxQueue {
            interface_index: device.interface_index,
            device_instance,
            queue_id,
            output_slot,
            drop_slot,
            assigned_workers,
        });
        Ok(())
    }

    pub fn assign_tx_queue_to_worker(
        &self,
        device_instance: u32,
        queue_id: u32,
        worker: DataWorkerId,
    ) -> Result<(), DeviceError> {
        let queues = unsafe { &mut *self.tx_queues.get() };
        let queue = queues
            .iter_mut()
            .find(|queue| queue.device_instance == device_instance && queue.queue_id == queue_id)
            .ok_or(DeviceError::TxQueueNotRegistered {
                device_instance,
                queue_id,
            })?;
        queue.assigned_workers.set(worker);
        Ok(())
    }

    pub fn rx_poll_vector(&self, owner: DataWorkerId) -> Vec<DeviceRxQueue> {
        let queues = unsafe { &*self.rx_queues.get() };
        queues
            .iter()
            .copied()
            .filter(|queue| queue.owner == owner)
            .collect()
    }

    pub fn tx_queues(&self) -> Vec<DeviceTxQueue> {
        let queues = unsafe { &*self.tx_queues.get() };
        queues.to_vec()
    }

    pub fn tx_queues_for_interface(&self, interface_index: u32) -> Vec<DeviceTxQueue> {
        let queues = unsafe { &*self.tx_queues.get() };
        queues
            .iter()
            .cloned()
            .filter(|queue| queue.interface_index == interface_index)
            .collect()
    }

    pub fn tx_queues_for_worker(&self, owner: DataWorkerId) -> Vec<DeviceTxQueue> {
        let queues = unsafe { &*self.tx_queues.get() };
        queues
            .iter()
            .filter(|queue| queue.is_assigned_to(owner))
            .cloned()
            .collect()
    }

    /// Bind this worker's assigned TX queues into its interface-output runtime.
    ///
    /// Queue assignment is published by the control plane before workers start.
    /// The plugin calls this service-owned method during worker init; it cannot
    /// select output nodes or mutate another worker's graph directly.
    pub fn install_worker_output_runtime(&self, worker: DataWorkerId) {
        InterfaceOutputNode::install_worker_queues(worker, &self.tx_queues());
    }
}
#[hammer_component_macros::init_function(name = "device_init")]
fn init_device(engine: &mut GlobalMain) -> RuntimeResult<Arc<DeviceMain>> {
    Ok(DeviceMain::new(engine.data_plane_main().nodes().clone()))
}
