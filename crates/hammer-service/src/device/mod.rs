//! Device class abstraction for VPP-aligned hardware interface drivers.
//!
//! Mirrors VPP `vnet_device_class_t` + `vnet_hw_if_rxq`/`txq` + polling/interrupt
//! input node layer. Each device class (TUN, future af_packet, WireGuard tun, ...)
//! bundles its input/output driver node ids, a `DeviceMain` queue registry, and
//! per-slot node-runtime state via `DeviceRuntimeSlot<T>`.
//!
//! # Synchronization contract (VPP-style, lock-free dataplane)
//!
//! All mutation of `DeviceMain` / `DeviceRuntimeSlot` shared state follows VPP's
//! barrier discipline, not per-field mutexes:
//!
//! - **Dataplane hot path** (node process functions, RX/TX dispatch) accesses
//!   per-slot state via `UnsafeCell` with no locks. Each `vlib_node_runtime_t` is
//!   dispatched on exactly one worker; the `NodeRuntimeData` blob is only touched
//!   by that worker, so per-slot access is single-writer by dispatch construction.
//! - **Control plane** (register queue, bind RX/TX queue, mutate per-slot fields
//!   after registration) must hold the runtime data-plane barrier
//!   (`DataPlaneBarrierHandle::sync`) so all workers park at frame boundaries
//!   before mutation. Pre-registration construction (builder chains before the
//!   node is handed to the runtime) is single-threaded and needs no barrier.
//! - **Interrupt pending flags** are the one genuinely concurrent field (set by
//!   the OS event source, consumed by the dataplane) and use `AtomicBool`, as in
//!   VPP `vnet_hw_if_rxq::interrupt_pending`.
//!
//! This mirrors `interface.rs::InterfaceStateSlot::replace_after_barrier`: the
//! borrow checker's `&mut T` exclusivity is proven by `UnsafeCell` + the SAFETY
//! contract (barrier or single-threaded construction), not by `Mutex`.

use std::cell::UnsafeCell;
use std::sync::Arc;

use hammer_core::data_plane::{BufferFrame, NodeId};
use hammer_core::error::{CoreError, CoreResult, HammerResult};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
};

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
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        device_input_process
    }
}

fn device_input_process(
    _: &DataPlaneRuntime,
    _: NodeRuntimeData,
    _: &mut BufferFrame,
) -> NodeResult {
    NodeResult::drop()
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
    rx_queues: UnsafeCell<Vec<DeviceRxQueue>>,
    tx_queues: UnsafeCell<Vec<DeviceTxQueue>>,
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
    pub output_node: NodeId,
    assigned_workers: Vec<DataWorkerId>,
}

impl DeviceTxQueue {
    #[inline]
    pub fn assigned_workers(&self) -> &[DataWorkerId] {
        &self.assigned_workers
    }

    #[inline]
    pub fn is_shared(&self) -> bool {
        self.assigned_workers.len() > 1
    }
}

// SAFETY: queue registration finishes before worker startup. Published queue
// records are immutable while data workers compile and use their local views.
unsafe impl Send for DeviceMain {}
unsafe impl Sync for DeviceMain {}

impl DeviceMain {
    #[inline]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            rx_queues: UnsafeCell::new(Vec::new()),
            tx_queues: UnsafeCell::new(Vec::new()),
        })
    }

    pub fn register_rx_queue(
        &self,
        device_instance: u32,
        queue_id: u32,
        owner: DataWorkerId,
        mode: DriverScheduleMode,
    ) -> CoreResult<()> {
        let queues = unsafe { &mut *self.rx_queues.get() };
        if queues
            .iter()
            .any(|queue| queue.device_instance == device_instance && queue.queue_id == queue_id)
        {
            return Err(CoreError::internal("device RX queue is already registered"));
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
        interface_index: u32,
        device_instance: u32,
        queue_id: u32,
        owner: DataWorkerId,
        output_node: NodeId,
    ) -> CoreResult<()> {
        let queues = unsafe { &mut *self.tx_queues.get() };
        if queues
            .iter()
            .any(|queue| queue.device_instance == device_instance && queue.queue_id == queue_id)
        {
            return Err(CoreError::internal("device TX queue is already registered"));
        }
        queues.push(DeviceTxQueue {
            interface_index,
            device_instance,
            queue_id,
            output_node,
            assigned_workers: vec![owner],
        });
        Ok(())
    }

    pub fn assign_tx_queue_to_worker(
        &self,
        device_instance: u32,
        queue_id: u32,
        worker: DataWorkerId,
    ) -> CoreResult<()> {
        let queues = unsafe { &mut *self.tx_queues.get() };
        let queue = queues
            .iter_mut()
            .find(|queue| queue.device_instance == device_instance && queue.queue_id == queue_id)
            .ok_or_else(|| CoreError::internal("device TX queue is not registered"))?;
        if !queue.assigned_workers.contains(&worker) {
            queue.assigned_workers.push(worker);
        }
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
            .filter(|queue| queue.assigned_workers.contains(&owner))
            .cloned()
            .collect()
    }
}

#[hammer_component_macros::init_function(name = "device_init")]
fn init_device() -> HammerResult<Arc<DeviceMain>> {
    Ok(DeviceMain::new())
}
