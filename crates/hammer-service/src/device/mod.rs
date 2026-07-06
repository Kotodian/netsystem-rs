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
use std::sync::atomic::{AtomicBool, Ordering};

use hammer_adapter::{DataPlaneRuntime, NodeId, NodeRuntimeData, TraceFormatter};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

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

/// Per-device-class queue registry. Mirrors VPP `vnet_device_main_t`'s rx/tx
/// queue vector. Each RX queue binds an input node id + schedule mode + interrupt
/// pending flag; each TX queue binds an output node id.
///
/// Queue registration is control-plane (barrier-held or pre-runtime). Dataplane
/// reads (`consume_rx_interrupt_pending`, `tx_node`) are lock-free on immutable
/// post-registration fields; `interrupt_pending` is `AtomicBool`.
///
/// The shared state lives behind an `Arc<DeviceMain>` handle: cloning the handle
/// (via `Arc::clone`) shares the registry without copying the queues.
pub struct DeviceMain {
    rx_queues: UnsafeCell<Vec<RxQueue>>,
    tx_queues: UnsafeCell<Vec<TxQueue>>,
}

struct RxQueue {
    input_node: NodeId,
    mode: DriverScheduleMode,
    interrupt_pending: AtomicBool,
}

struct TxQueue {
    output_node: NodeId,
}

// SAFETY: DeviceMain's mutable state is gated by the runtime data-plane
// barrier (control plane) or single-threaded construction. The only field
// touched outside the barrier is `interrupt_pending`, which is `AtomicBool`.
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

    /// Register an RX queue and return its index.
    ///
    /// # Control-plane contract
    ///
    /// Caller must hold the runtime data-plane barrier if the runtime is
    /// dispatching; pre-runtime construction is single-threaded and needs no
    /// barrier.
    pub fn register_rx_queue(&self, input_node: NodeId, mode: DriverScheduleMode) -> u32 {
        // SAFETY: control-plane mutation under barrier or pre-runtime construction.
        let queues = unsafe { &mut *self.rx_queues.get() };
        let index = queues.len() as u32;
        queues.push(RxQueue {
            input_node,
            mode,
            interrupt_pending: AtomicBool::new(false),
        });
        index
    }

    /// Register a TX queue and return its index.
    ///
    /// # Control-plane contract
    ///
    /// As [`Self::register_rx_queue`].
    pub fn register_tx_queue(&self, output_node: NodeId) -> u32 {
        // SAFETY: control-plane mutation under barrier or pre-runtime construction.
        let queues = unsafe { &mut *self.tx_queues.get() };
        let index = queues.len() as u32;
        queues.push(TxQueue { output_node });
        index
    }

    /// Mark an RX queue's interrupt as pending and return its input node id so
    /// the caller can schedule the input driver node.
    ///
    /// Called by the OS event source (control plane). The `interrupt_pending`
    /// store is `AtomicBool::Release`, paired with the dataplane's `AcqRel` swap.
    /// The `input_node` read is safe lock-free: `RxQueue` fields are immutable
    /// after registration.
    pub fn mark_rx_interrupt_pending(&self, rx_queue: u32) -> CoreResult<NodeId> {
        // SAFETY: rx_queues is immutable post-registration; reads are lock-free.
        let queues = unsafe { &*self.rx_queues.get() };
        let queue = queues
            .get(rx_queue as usize)
            .ok_or_else(|| CoreError::internal("device RX queue is not registered"))?;
        queue.interrupt_pending.store(true, Ordering::Release);
        Ok(queue.input_node)
    }

    /// Consume an RX queue's interrupt pending flag from the dataplane.
    ///
    /// - `Poll`: always returns `true` (the node polls unconditionally).
    /// - `Interrupt`/`Adaptive`: atomically swaps the flag to `false` and
    ///   returns the previous value.
    pub fn consume_rx_interrupt_pending(&self, rx_queue: u32) -> CoreResult<bool> {
        // SAFETY: rx_queues is immutable post-registration; reads are lock-free.
        let queues = unsafe { &*self.rx_queues.get() };
        let queue = queues
            .get(rx_queue as usize)
            .ok_or_else(|| CoreError::internal("device RX queue is not registered"))?;
        match queue.mode {
            DriverScheduleMode::Poll => Ok(true),
            DriverScheduleMode::Interrupt | DriverScheduleMode::Adaptive => {
                Ok(queue.interrupt_pending.swap(false, Ordering::AcqRel))
            }
        }
    }

    /// Look up a TX queue's output node id. Lock-free (`TxQueue` fields are
    /// immutable post-registration).
    pub fn tx_node(&self, tx_queue: u32) -> CoreResult<NodeId> {
        // SAFETY: tx_queues is immutable post-registration; reads are lock-free.
        let queues = unsafe { &*self.tx_queues.get() };
        queues
            .get(tx_queue as usize)
            .map(|queue| queue.output_node)
            .ok_or_else(|| CoreError::internal("device TX queue is not registered"))
    }
}

/// Generic per-slot node-runtime registry. Replaces the per-device-class
/// `XxxMain` state-holder pattern: a driver node registers its per-instance
/// runtime state (`T`) into a slot, embeds the slot's `NodeRuntimeData` into
/// the node descriptor, and the `NodeProcessFn` recovers `&mut T` from the
/// runtime data to drive the per-instance state.
///
/// `NodeRuntimeData` word 0 carries the `Arc<DeviceRuntimeSlot<T>>` raw pointer
/// (the address of the `DeviceRuntimeSlot` itself, valid for the slot's lifetime
/// — the runtime owns the slot for its lifetime). Word 1 carries the slot index.
///
/// The shared state lives behind an `Arc<DeviceRuntimeSlot<T>>` handle: cloning
/// the handle (via `Arc::clone`) shares the registry without copying slots.
///
/// # Synchronization
///
/// - Dataplane ([`Self::borrow_for_runtime_data`]) is lock-free: returns `&mut T`
///   via `UnsafeCell`. SAFETY rests on each node runtime being dispatched on
///   exactly one worker (single-writer per slot) and control-plane mutation
///   being barrier-gated.
/// - Control plane ([`Self::register`], [`Self::with_mut`]) mutates under the
///   runtime barrier or during single-threaded pre-runtime construction.
///
/// This is the Rust analogue of VPP `vlib_node_runtime_t::runtime_data`
/// carrying a pointer to per-node state; the node function casts the blob back
/// to its expected struct without locking.
pub struct DeviceRuntimeSlot<T> {
    items: UnsafeCell<Vec<UnsafeCell<T>>>,
}

// SAFETY: per-slot mutation is single-writer (one worker per node runtime) on
// the dataplane; cross-thread mutation is barrier-gated. The `UnsafeCell<T>`
// per slot is only mutated through `with_mut` (control plane, barrier) or
// `borrow_for_runtime_data` (dataplane, single worker per slot).
unsafe impl<T: Send> Send for DeviceRuntimeSlot<T> {}
unsafe impl<T: Send> Sync for DeviceRuntimeSlot<T> {}

impl<T: Send> DeviceRuntimeSlot<T> {
    #[inline]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            items: UnsafeCell::new(Vec::new()),
        })
    }

    /// Register a per-instance state item and return its slot index.
    ///
    /// # Control-plane contract
    ///
    /// Caller must hold the runtime data-plane barrier if the runtime is
    /// dispatching; pre-runtime construction is single-threaded and needs no
    /// barrier. Push may reallocate the slot vector, which is safe only when no
    /// dataplane borrow is outstanding (the barrier guarantees this).
    pub fn register(&self, item: T) -> usize {
        // SAFETY: control-plane mutation under barrier or pre-runtime construction.
        let items = unsafe { &mut *self.items.get() };
        let slot = items.len();
        items.push(UnsafeCell::new(item));
        slot
    }

    /// Build the `NodeRuntimeData` blob for a registered slot. The blob carries
    /// the slot's raw pointer (word 0) and the slot index (word 1).
    ///
    /// The raw pointer is the address of this `DeviceRuntimeSlot<T>` (the
    /// `Arc`'s heap allocation), obtained from `&self`; it remains valid as long
    /// as the `Arc` (held by the slot and any clones) is alive, i.e. for the
    /// runtime's lifetime.
    pub fn runtime_data(&self, slot: usize) -> CoreResult<NodeRuntimeData> {
        let raw = (self as *const Self) as *const () as u64;
        if raw == 0 {
            return Err(CoreError::internal("device runtime slot pointer is null"));
        }
        let slot_u64 = u64::try_from(slot)
            .map_err(|_| CoreError::internal("device runtime slot index overflow"))?;
        Ok(NodeRuntimeData::from_words([raw, slot_u64, 0, 0]))
    }

    /// Mutate a slot's per-instance state from the control plane.
    ///
    /// # Control-plane contract
    ///
    /// Caller must hold the runtime data-plane barrier if the runtime is
    /// dispatching; pre-runtime construction is single-threaded and needs no
    /// barrier. The `with_*` builder chains on driver nodes run pre-registration
    /// and need no barrier; `bind_*`-style post-registration mutations do.
    #[inline]
    pub fn with_mut<R>(&self, slot: usize, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: control-plane mutation under barrier or pre-runtime construction.
        let items = unsafe { &*self.items.get() };
        let cell = items
            .get(slot)
            .expect("device runtime slot index out of range");
        let item = unsafe { &mut *cell.get() };
        f(item)
    }

    /// Recover `&mut T` for a slot from a `NodeRuntimeData` blob produced by
    /// [`Self::runtime_data`]. Dataplane hot path — lock-free.
    ///
    /// # Safety contract (enforced by the call site, not Rust)
    ///
    /// `data` must have been produced by `DeviceRuntimeSlot::<T>::runtime_data`
    /// on a slot that is still alive when the caller dereferences the returned
    /// reference. The runtime guarantees this: node process functions only run
    /// while the runtime (and thus all registered device slots) is alive, and
    /// each node runtime is dispatched on exactly one worker so the `&mut T` is
    /// single-writer.
    #[inline]
    pub fn borrow_for_runtime_data<'a>(data: NodeRuntimeData) -> CoreResult<&'a mut T> {
        let slot = data.usize_word(1)?;
        let raw = data.word(0) as *const DeviceRuntimeSlot<T>;
        if raw.is_null() {
            return Err(CoreError::internal("device runtime data pointer is null"));
        }
        // SAFETY: raw was produced from `&self` of the slot's `Arc` allocation.
        // The Arc is held by the slot for the runtime's lifetime; node process
        // functions run within the runtime, so the pointer is non-dangling for
        // the duration of the borrow. The caller ties 'a to its own frame,
        // shorter than the runtime's lifetime.
        let slot_ref: &'a DeviceRuntimeSlot<T> = unsafe { &*raw };
        // SAFETY: items vector is immutable on the dataplane (mutation is
        // barrier-gated control plane). Shared ref is sound.
        let items: &'a Vec<UnsafeCell<T>> = unsafe { &*slot_ref.items.get() };
        let cell = items
            .get(slot)
            .ok_or_else(|| CoreError::internal("device runtime slot index is invalid"))?;
        // SAFETY: single-writer per slot — each node runtime is dispatched on
        // exactly one worker, so no concurrent &mut T for the same slot.
        let item: &'a mut T = unsafe { &mut *cell.get() };
        Ok(item)
    }
}

/// Source of RX/TX interrupt scheduling for a device queue pair. Mirrors VPP
/// `vnet_hw_if_rxq` interrupt mode: the OS-side event source calls
/// `schedule_readable`/`schedule_writable` to enqueue an empty driver frame for
/// the input/output node, which then polls the queue.
#[derive(Clone)]
pub struct DeviceEventSource {
    device_main: Arc<DeviceMain>,
    rx_queue: Option<u32>,
    tx_queue: Option<u32>,
}

impl DeviceEventSource {
    #[inline]
    pub fn new(device_main: Arc<DeviceMain>, rx_queue: Option<u32>, tx_queue: Option<u32>) -> Self {
        Self {
            device_main,
            rx_queue,
            tx_queue,
        }
    }

    #[inline]
    pub fn input(device_main: Arc<DeviceMain>, rx_queue: u32) -> Self {
        Self::new(device_main, Some(rx_queue), None)
    }

    #[inline]
    pub fn output(device_main: Arc<DeviceMain>, tx_queue: u32) -> Self {
        Self::new(device_main, None, Some(tx_queue))
    }

    #[inline]
    pub fn schedule_readable(&self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        let rx_queue = self
            .rx_queue
            .ok_or_else(|| CoreError::internal("device RX queue is not configured"))?;
        let input = self.device_main.mark_rx_interrupt_pending(rx_queue)?;
        schedule_empty_driver_frame(runtime, input)
    }

    #[inline]
    pub fn schedule_writable(&self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        let tx_queue = self
            .tx_queue
            .ok_or_else(|| CoreError::internal("device TX queue is not configured"))?;
        let output = self.device_main.tx_node(tx_queue)?;
        schedule_empty_driver_frame(runtime, output)
    }
}

/// VPP `vnet_device_class_t` analogue: metadata for a class of hardware
/// interface drivers. Each concrete device class (TUN, future af_packet, ...)
/// implements this trait and registers an instance with the runtime.
///
/// The trait is intentionally narrow: it exposes the class name, the
/// `DeviceMain` queue registry, and the registered input/output driver node
/// ids + trace formatters. Per-instance RX/TX byte exchange and per-slot
/// runtime state live on the concrete type, not the trait.
pub trait DeviceClass: Send + Sync {
    fn name(&self) -> &'static str;
    fn device_main(&self) -> &DeviceMain;
    fn input_node(&self) -> NodeId;
    fn output_node(&self) -> NodeId;
    fn input_trace_formatter(&self) -> Option<TraceFormatter>;
    fn output_trace_formatter(&self) -> Option<TraceFormatter>;
}

#[inline]
fn schedule_empty_driver_frame(runtime: &DataPlaneRuntime, node: NodeId) -> CoreResult<()> {
    runtime.schedule_empty_frame(node)
}
