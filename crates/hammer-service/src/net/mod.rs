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

#[derive(Clone)]
pub struct NetMain {
    interface_main: Arc<InterfaceMain>,
    dpo_main: DpoMain,
    load_balances: Pool<LoadBalanceDpo>,
    local_interface_hw_index: u32,
    local_interface_sw_index: u32,
}

impl NetMain {
    pub fn global() -> RuntimeResult<&'static NetMain> {
        NET_MAIN
            .get()
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
        let main = NetMain {
            interface_main,
            dpo_main: DpoMain::new(),
            load_balances: Pool::new(),
            local_interface_hw_index: local_hw,
            local_interface_sw_index: local_sw,
        };
        let shared = Arc::new(main.clone());
        NET_MAIN
            .set(main)
            .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "net" })?;
        Ok(shared)
    }

    pub fn interface_main(&self) -> &InterfaceMain {
        &self.interface_main
    }
    pub fn dpo_main(&self) -> &DpoMain {
        &self.dpo_main
    }

    pub fn create_load_balance(
        &mut self,
        runtime: &mut DataPlaneMain,
        proto: DpoProto,
        load_balance: LoadBalanceDpo,
    ) -> Result<DpoId, DpoError> {
        // VPP's load_balance_create combines two independent publication
        // points: pool growth and first-time dpo_stack graph-edge creation.
        // One owner transaction covers both, while an outer Binary API
        // barrier is reused through the macro's nested-scope semantics.
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
        let graph_edge_missing = self.load_balance_bucket_ids(&load_balance).any(|parent| {
            self.dpo_main
                .stack_requires_graph_edge(DpoType::LOAD_BALANCE, proto, parent)
        });
        let needs_barrier = self.load_balances.will_get_grow() || graph_edge_missing;
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);

        if needs_barrier
            && workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.create_load_balance_inner(runtime, proto, load_balance)
            })
        } else {
            self.create_load_balance_inner(runtime, proto, load_balance)
        }
    }

    fn load_balance_bucket_ids<'a>(
        &self,
        load_balance: &'a LoadBalanceDpo,
    ) -> impl Iterator<Item = DpoId> + 'a {
        let count = usize::from(load_balance.bucket_count);
        load_balance.inline_buckets[..count.min(LoadBalanceDpo::INLINE_BUCKETS)]
            .iter()
            .copied()
            .chain(load_balance.overflow_buckets.iter().copied())
    }

    fn create_load_balance_inner(
        &mut self,
        runtime: &DataPlaneMain,
        proto: DpoProto,
        mut load_balance: LoadBalanceDpo,
    ) -> Result<DpoId, DpoError> {
        let bucket_count = usize::from(load_balance.bucket_count);
        for bucket in
            &mut load_balance.inline_buckets[..bucket_count.min(LoadBalanceDpo::INLINE_BUCKETS)]
        {
            *bucket =
                self.dpo_main
                    .stack(runtime.nodes(), DpoType::LOAD_BALANCE, proto, *bucket)?;
        }
        for bucket in &mut load_balance.overflow_buckets {
            *bucket =
                self.dpo_main
                    .stack(runtime.nodes(), DpoType::LOAD_BALANCE, proto, *bucket)?;
        }
        let index = self.load_balances.insert(load_balance);
        Ok(DpoId::load_balance(proto, index))
    }

    #[inline(always)]
    pub fn load_balance(&self, index: u32) -> Option<&LoadBalanceDpo> {
        self.load_balances.get(index)
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

pub static NET_MAIN: OnceLock<NetMain> = OnceLock::new();

#[hammer_component_macros::init_function(name = "net_main_init", runs_after = ["interface_main_init"])]
fn init_net_main(interface_main: Arc<InterfaceMain>) -> RuntimeResult<Arc<NetMain>> {
    NetMain::init(interface_main)
}
