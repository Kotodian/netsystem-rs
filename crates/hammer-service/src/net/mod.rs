use std::cell::UnsafeCell;
use std::sync::{Arc, OnceLock};

use hammer_infra::pool::Pool;
use hammer_runtime::{DataPlaneMain, RuntimeError, RuntimeResult};

use crate::interface::InterfaceMain;

pub mod dpo;
pub mod fib;

pub use dpo::{
    AdjacencyDpo, DpoError, DpoId, DpoMain, DpoProto, DpoType, InterfaceRxDpo, InterfaceTxDpo,
    LoadBalanceDpo, LoadBalanceFlags, LookupCast, LookupDpo, LookupInput, LookupTable, ReceiveDpo,
    ReplicateDpo, ReplicateFlags,
};
pub use fib::{
    FibEntry, FibEntryFlags, FibEntrySrc, FibEntrySrcFlags, FibPath, FibPathExt, FibPathExtList,
    FibPathList, FibPathListFlags, FibSource, FibSourceBehavior, FibTable, FibTableBackend,
};

pub struct NetMain {
    interface_main: Arc<InterfaceMain>,
    dpo_main: UnsafeCell<DpoMain>,
    load_balances: UnsafeCell<Pool<LoadBalanceDpo>>,
    local_interface_hw_index: u32,
    local_interface_sw_index: u32,
}

// SAFETY: the control thread is the sole mutator of `state`; live mutations
// are performed only while WorkerBarrier has stopped all Data Workers. Workers
// read published DPO/pool values between barrier scopes.
unsafe impl Send for NetMain {}
// SAFETY: the publication and lifetime rules above prevent concurrent mutable
// access while workers hold shared references into the state.
unsafe impl Sync for NetMain {}

impl NetMain {
    pub fn global() -> RuntimeResult<&'static NetMain> {
        NET_MAIN
            .get()
            .map(Arc::as_ref)
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::net::NetMain",
            })
    }

    pub fn init(interface_main: Arc<InterfaceMain>) -> RuntimeResult<Arc<NetMain>> {
        let local_hw = interface_main
            .register_hardware_interface(0, 0, 0, 0)
            .map_err(RuntimeError::from)?;
        interface_main
            .set_interface_name(local_hw, "local0")
            .map_err(RuntimeError::from)?;
        let local_sw = interface_main
            .hardware_interface(local_hw)
            .map(|interface| interface.sw_if_index)
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "local0",
            })?;
        let shared = Arc::new(NetMain {
            interface_main,
            dpo_main: UnsafeCell::new(DpoMain::new()),
            load_balances: UnsafeCell::new(Pool::new()),
            local_interface_hw_index: local_hw,
            local_interface_sw_index: local_sw,
        });
        NET_MAIN
            .set(Arc::clone(&shared))
            .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "net" })?;
        Ok(shared)
    }

    fn load_balances(&self) -> &Pool<LoadBalanceDpo> {
        // SAFETY: workers borrow this pool only outside a barrier. Pool backing
        // growth and reclamation stop workers before invalidating references.
        unsafe { &*self.load_balances.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn dpo_main_mut(&self) -> &mut DpoMain {
        // SAFETY: DPO class and edge metadata has one Main Thread writer.
        // Graph-edge insertion enters WorkerBarrier before graph mutation.
        unsafe { &mut *self.dpo_main.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn load_balances_mut(&self) -> &mut Pool<LoadBalanceDpo> {
        // SAFETY: callers are the Main Thread; any backing-storage growth or
        // reclamation that can invalidate worker references holds WorkerBarrier.
        unsafe { &mut *self.load_balances.get() }
    }

    pub fn interface_main(&self) -> &InterfaceMain {
        &self.interface_main
    }
    pub fn dpo_main(&self) -> &DpoMain {
        // SAFETY: DPO metadata is mutated only by the Main Thread. Data Workers
        // do not mutate it, and graph publication is barrier protected.
        unsafe { &*self.dpo_main.get() }
    }

    pub fn register_dpo_class(
        &self,
        nodes: &[(DpoProto, &[hammer_core::data_plane::NodeId])],
    ) -> Result<DpoType, DpoError> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        self.dpo_main_mut().register_new_type(nodes)
    }

    pub fn create_load_balance(
        &self,
        runtime: &mut DataPlaneMain,
        proto: DpoProto,
        load_balance: LoadBalanceDpo,
    ) -> Result<DpoId, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if load_balance.proto != proto {
            return Err(DpoError::ProtocolMismatch {
                actual: load_balance.proto.get(),
                expected: proto.get(),
            });
        }

        let bucket_count = usize::from(load_balance.bucket_count);
        if bucket_count == 0
            || !bucket_count.is_power_of_two()
            || bucket_count > LoadBalanceDpo::MAX_BUCKETS
        {
            return Err(DpoError::InvalidBucketCount);
        }
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);

        // VPP synchronizes load_balance_alloc_i when pool or counter backing
        // storage grows because its packet path dereferences an already-valid
        // pool index without reading allocation metadata. Hammer Pool::get
        // also reads pool length and occupancy, so every insertion must stop
        // readers even when the backing allocation stays in place.
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.create_load_balance_inner(runtime, proto, load_balance)
            })
        } else {
            self.create_load_balance_inner(runtime, proto, load_balance)
        }
    }

    fn create_load_balance_inner(
        &self,
        runtime: &mut DataPlaneMain,
        proto: DpoProto,
        mut load_balance: LoadBalanceDpo,
    ) -> Result<DpoId, DpoError> {
        let bucket_count = usize::from(load_balance.bucket_count);
        for bucket in
            &mut load_balance.inline_buckets[..bucket_count.min(LoadBalanceDpo::INLINE_BUCKETS)]
        {
            *bucket = self
                .dpo_main_mut()
                .stack(runtime, DpoType::LOAD_BALANCE, proto, *bucket)?;
        }
        for bucket in &mut load_balance.overflow_buckets {
            *bucket = self
                .dpo_main_mut()
                .stack(runtime, DpoType::LOAD_BALANCE, proto, *bucket)?;
        }
        let index = self.load_balances_mut().insert(load_balance);
        Ok(DpoId::load_balance(proto, index))
    }

    #[inline(always)]
    pub fn load_balance(&self, index: u32) -> Option<&LoadBalanceDpo> {
        self.load_balances().get(index)
    }

    #[inline(always)]
    pub fn select_load_balance(&self, dpo: DpoId, hash: u32) -> Option<DpoId> {
        (dpo.class() == DpoType::LOAD_BALANCE)
            .then(|| self.load_balance(dpo.index()))
            .flatten()
            .and_then(|load_balance| load_balance.select_bucket(hash))
    }
    pub fn interface_main_arc(&self) -> Arc<InterfaceMain> {
        Arc::clone(&self.interface_main)
    }
    pub fn local_interface_hw_index(&self) -> u32 {
        self.local_interface_hw_index
    }
    pub fn local_interface_sw_index(&self) -> u32 {
        self.local_interface_sw_index
    }
}

pub static NET_MAIN: OnceLock<Arc<NetMain>> = OnceLock::new();

#[hammer_component_macros::init_function(name = "net_main_init", runs_after = ["interface_main_init"])]
fn init_net_main(interface_main: Arc<InterfaceMain>) -> RuntimeResult<Arc<NetMain>> {
    NetMain::init(interface_main)
}
