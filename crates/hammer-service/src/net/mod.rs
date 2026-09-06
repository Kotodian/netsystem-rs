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
    replicates: UnsafeCell<Pool<ReplicateDpo>>,
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
            replicates: UnsafeCell::new(Pool::new()),
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

    fn replicates(&self) -> &Pool<ReplicateDpo> {
        // SAFETY: workers borrow this pool only outside a barrier. Pool backing
        // growth and reclamation stop workers before invalidating references.
        unsafe { &*self.replicates.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn replicates_mut(&self) -> &mut Pool<ReplicateDpo> {
        // SAFETY: callers are the Main Thread; any backing-storage growth or
        // reclamation that can invalidate worker references holds WorkerBarrier.
        unsafe { &mut *self.replicates.get() }
    }

    fn validate_bucket_identity(&self, bucket: DpoId) -> Result<(), DpoError> {
        if !bucket.is_valid() {
            return Err(DpoError::ObjectMissing {
                dpo_type: bucket.class().get(),
                index: bucket.index(),
            });
        }
        match bucket.class() {
            DpoType::LOAD_BALANCE if self.load_balance(bucket.index()).is_none() => {
                Err(DpoError::ObjectMissing {
                    dpo_type: bucket.class().get(),
                    index: bucket.index(),
                })
            }
            DpoType::REPLICATE if self.replicate(bucket.index()).is_none() => {
                Err(DpoError::ObjectMissing {
                    dpo_type: bucket.class().get(),
                    index: bucket.index(),
                })
            }
            _ => Ok(()),
        }
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
        locks: Option<(fn(DpoId), fn(DpoId))>,
    ) -> Result<DpoType, DpoError> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        self.dpo_main_mut().register_new_type(nodes, locks)
    }

    pub fn register_builtin_dpo(
        &self,
        dpo_type: DpoType,
        nodes: &[(DpoProto, &[hammer_core::data_plane::NodeId])],
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let main = self.dpo_main_mut();
        main.register_builtin(dpo_type, nodes)?;
        let index = usize::from(dpo_type.get());
        if main.locks.len() <= index {
            main.locks.resize(index + 1, None);
            main.unlocks.resize(index + 1, None);
        }
        match dpo_type {
            DpoType::LOAD_BALANCE => {
                main.locks[index] = Some(Self::lock_load_balance);
                main.unlocks[index] = Some(Self::unlock_load_balance);
            }
            DpoType::REPLICATE => {
                main.locks[index] = Some(Self::lock_replicate);
                main.unlocks[index] = Some(Self::unlock_replicate);
            }
            _ => {}
        }
        Ok(())
    }

    /// Acquires a child reference for a concrete DPO owner. Packet-path copies
    /// do not call this. The owner must hold the main-thread publication scope.
    pub fn lock_dpo(&self, dpo: DpoId) {
        if !dpo.is_valid() {
            return;
        }
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("DPO reference acquisition requires the main-thread publication scope");
        assert!(
            self.dpo_main().nodes(dpo.class(), dpo.proto()).is_some(),
            "referenced DPO class/protocol must be registered: {:?}",
            dpo
        );
        // End the registry borrow before entering a concrete pool owner.
        let lock = self
            .dpo_main()
            .locks
            .get(usize::from(dpo.class().get()))
            .copied()
            .flatten();
        if let Some(lock) = lock {
            lock(dpo);
        }
    }

    /// Consumes a reference previously acquired by an owning DPO field or root.
    /// Concrete owners call this from their destruction/publication paths, not
    /// once per copied identity. Final destruction may recursively enter owners.
    pub fn unlock_dpo(&self, dpo: DpoId) {
        if !dpo.is_valid() {
            return;
        }
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("DPO retirement requires the main-thread publication scope");
        assert!(
            self.dpo_main().nodes(dpo.class(), dpo.proto()).is_some(),
            "referenced DPO class/protocol must be registered: {:?}",
            dpo
        );
        let unlock = self
            .dpo_main()
            .unlocks
            .get(usize::from(dpo.class().get()))
            .copied()
            .flatten();
        if let Some(unlock) = unlock {
            unlock(dpo);
        }
    }

    fn lock_load_balance(dpo: DpoId) {
        let main = Self::global().expect("load-balance owner must be initialized");
        main.load_balances_mut()
            .get_mut(dpo.index())
            .expect("referenced load-balance must exist")
            .publish_root();
    }

    fn unlock_load_balance(dpo: DpoId) {
        let main = Self::global().expect("load-balance owner must be initialized");
        let remove = main
            .load_balances_mut()
            .get_mut(dpo.index())
            .expect("locked load-balance must exist")
            .withdraw_root();
        if remove {
            let object = main.load_balances_mut().remove(dpo.index());
            drop(object);
        }
    }

    fn lock_replicate(dpo: DpoId) {
        let main = Self::global().expect("replicate owner must be initialized");
        main.replicates_mut()
            .get_mut(dpo.index())
            .expect("referenced replicate must exist")
            .publish_root();
    }

    fn unlock_replicate(dpo: DpoId) {
        let main = Self::global().expect("replicate owner must be initialized");
        let remove = main
            .replicates_mut()
            .get_mut(dpo.index())
            .expect("locked replicate must exist")
            .withdraw_root();
        if remove {
            let object = main.replicates_mut().remove(dpo.index());
            drop(object);
        }
    }

    pub fn create_load_balance(
        &self,
        runtime: &mut DataPlaneMain,
        proto: DpoProto,
        mut load_balance: LoadBalanceDpo,
    ) -> Result<DpoId, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if load_balance.proto != proto {
            return Err(DpoError::ProtocolMismatch {
                actual: load_balance.proto.get(),
                expected: proto.get(),
            });
        }
        if self
            .dpo_main()
            .nodes(DpoType::LOAD_BALANCE, proto)
            .is_none()
        {
            return Err(DpoError::NodeMissing {
                dpo_type: DpoType::LOAD_BALANCE.get(),
                proto: proto.get(),
            });
        }

        load_balance.validate_storage()?;
        let bucket_count = usize::from(load_balance.bucket_count());
        if bucket_count == 0 {
            return Err(DpoError::EmptyDpoPublication {
                dpo_type: DpoType::LOAD_BALANCE.get(),
            });
        }
        for bucket_index in 0..bucket_count {
            let bucket = load_balance
                .bucket_mut(bucket_index)
                .ok_or(DpoError::InvalidBucketStorage)?;
            self.validate_bucket_identity(*bucket)?;
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
        let bucket_count = usize::from(load_balance.bucket_count());
        for bucket_index in 0..bucket_count {
            let bucket = load_balance
                .bucket_mut(bucket_index)
                .ok_or(DpoError::InvalidBucketStorage)?;
            *bucket = self
                .dpo_main_mut()
                .stack(runtime, DpoType::LOAD_BALANCE, proto, *bucket)?;
        }
        for bucket_index in 0..bucket_count {
            let child = load_balance
                .select_bucket(bucket_index as u32)
                .ok_or(DpoError::InvalidBucketStorage)?;
            self.lock_dpo(child);
            load_balance.locked_buckets += 1;
        }
        load_balance.lock_count = 1;
        let index = self.load_balances_mut().insert(load_balance);
        Ok(DpoId::load_balance(proto, index))
    }

    /// Replaces all buckets of an existing load-balance object.
    ///
    /// VPP updates the bucket storage and the reported bucket count as one
    /// worker-visible transaction. The detached child identities are stacked
    /// before the pool object is changed; the barrier therefore never exposes
    /// a count whose backing storage is incomplete.
    pub fn update_load_balance(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        buckets: &[DpoId],
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::LOAD_BALANCE.get(),
            });
        }
        LoadBalanceDpo::validate_bucket_count(buckets.len())?;
        if buckets.is_empty() {
            return Err(DpoError::EmptyDpoPublication {
                dpo_type: DpoType::LOAD_BALANCE.get(),
            });
        }
        for &bucket in buckets {
            self.validate_bucket_identity(bucket)?;
        }
        if self.load_balance(dpo.index()).is_none() {
            return Err(DpoError::ObjectMissing {
                dpo_type: dpo.class().get(),
                index: dpo.index(),
            });
        }

        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.update_load_balance_inner(runtime, dpo, buckets)
            })
        } else {
            self.update_load_balance_inner(runtime, dpo, buckets)
        }
    }

    /// Records another long-lived forwarding root for a load-balance object.
    /// Plain `DpoId` copies remain non-owning; callers use this operation only
    /// when a concrete FIB/interface slot is about to publish the identity.
    pub fn publish_load_balance_root(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::LOAD_BALANCE.get(),
            });
        }
        let operation = || {
            let load_balance =
                self.load_balances_mut()
                    .get_mut(dpo.index())
                    .ok_or(DpoError::ObjectMissing {
                        dpo_type: dpo.class().get(),
                        index: dpo.index(),
                    })?;
            load_balance.publish_root();
            Ok(())
        };
        if hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0)
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, { operation() })
        } else {
            operation()
        }
    }

    /// Withdraws one published load-balance root. The concrete object is
    /// retired from its owner pool only after the last root is withdrawn and
    /// all Data Workers have acknowledged the barrier.
    pub fn retire_load_balance_root(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::LOAD_BALANCE.get(),
            });
        }
        let operation = || {
            let load_balance =
                self.load_balances_mut()
                    .get_mut(dpo.index())
                    .ok_or(DpoError::ObjectMissing {
                        dpo_type: dpo.class().get(),
                        index: dpo.index(),
                    })?;
            let should_remove = load_balance.withdraw_root();
            if should_remove {
                let object = self.load_balances_mut().remove(dpo.index());
                drop(object);
            }
            Ok(())
        };
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, { operation() })
        } else {
            operation()
        }
    }

    fn update_load_balance_inner(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        buckets: &[DpoId],
    ) -> Result<(), DpoError> {
        let proto = dpo.proto();
        let mut stacked = Vec::with_capacity(buckets.len());
        for &bucket in buckets {
            stacked.push(self.dpo_main_mut().stack(
                runtime,
                DpoType::LOAD_BALANCE,
                proto,
                bucket,
            )?);
        }
        let load_balance =
            self.load_balances()
                .get(dpo.index())
                .ok_or(DpoError::ObjectMissing {
                    dpo_type: dpo.class().get(),
                    index: dpo.index(),
                })?;
        let mut replacement = LoadBalanceDpo::new(
            proto,
            &stacked,
            load_balance.flags,
            load_balance.flow_hash_config(),
        )?;
        replacement.lock_count = load_balance.lock_count;
        replacement.map_index = load_balance.map_index;
        replacement.urpf_index = load_balance.urpf_index;
        replacement.fib_entry_flags = load_balance.fib_entry_flags;
        for &child in &stacked {
            self.lock_dpo(child);
            replacement.locked_buckets += 1;
        }
        // A child may refer to this same object; preserve counts acquired above.
        let slot =
            self.load_balances_mut()
                .get_mut(dpo.index())
                .ok_or(DpoError::ObjectMissing {
                    dpo_type: dpo.class().get(),
                    index: dpo.index(),
                })?;
        replacement.lock_count = slot.lock_count;
        let old = std::mem::replace(slot, replacement);
        drop(old);
        Ok(())
    }

    pub fn create_replicate(
        &self,
        runtime: &mut DataPlaneMain,
        proto: DpoProto,
        mut replicate: ReplicateDpo,
    ) -> Result<DpoId, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if replicate.proto != proto {
            return Err(DpoError::ProtocolMismatch {
                actual: replicate.proto.get(),
                expected: proto.get(),
            });
        }
        if self.dpo_main().nodes(DpoType::REPLICATE, proto).is_none() {
            return Err(DpoError::NodeMissing {
                dpo_type: DpoType::REPLICATE.get(),
                proto: proto.get(),
            });
        }
        replicate.validate_storage()?;
        if replicate.bucket_count() == 0 {
            return Err(DpoError::EmptyDpoPublication {
                dpo_type: DpoType::REPLICATE.get(),
            });
        }
        for bucket_index in 0..usize::from(replicate.bucket_count()) {
            let bucket = replicate
                .bucket_mut(bucket_index)
                .ok_or(DpoError::InvalidBucketStorage)?;
            self.validate_bucket_identity(*bucket)?;
        }
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.create_replicate_inner(runtime, proto, replicate)
            })
        } else {
            self.create_replicate_inner(runtime, proto, replicate)
        }
    }

    fn create_replicate_inner(
        &self,
        runtime: &mut DataPlaneMain,
        proto: DpoProto,
        mut replicate: ReplicateDpo,
    ) -> Result<DpoId, DpoError> {
        let bucket_count = usize::from(replicate.bucket_count());
        let mut has_local = false;
        for bucket_index in 0..bucket_count {
            let bucket = replicate
                .bucket_mut(bucket_index)
                .ok_or(DpoError::InvalidBucketStorage)?;
            has_local |= bucket.class() == DpoType::RECEIVE;
            *bucket = self
                .dpo_main_mut()
                .stack(runtime, DpoType::REPLICATE, proto, *bucket)?;
        }
        replicate.flags.set(ReplicateFlags::HAS_LOCAL, has_local);
        for bucket_index in 0..bucket_count {
            let child = replicate
                .bucket(bucket_index as u16)
                .ok_or(DpoError::InvalidBucketStorage)?;
            self.lock_dpo(child);
            replicate.locked_buckets += 1;
        }
        replicate.lock_count = 1;
        let index = self.replicates_mut().insert(replicate);
        Ok(DpoId::replicate(proto, index))
    }

    pub fn update_replicate(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        buckets: &[DpoId],
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::REPLICATE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::REPLICATE.get(),
            });
        }
        ReplicateDpo::validate_bucket_count(buckets.len())?;
        if buckets.is_empty() {
            return Err(DpoError::EmptyDpoPublication {
                dpo_type: DpoType::REPLICATE.get(),
            });
        }
        for &bucket in buckets {
            self.validate_bucket_identity(bucket)?;
        }
        if self.replicates().get(dpo.index()).is_none() {
            return Err(DpoError::ObjectMissing {
                dpo_type: dpo.class().get(),
                index: dpo.index(),
            });
        }
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.update_replicate_inner(runtime, dpo, buckets)
            })
        } else {
            self.update_replicate_inner(runtime, dpo, buckets)
        }
    }

    pub fn publish_replicate_root(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::REPLICATE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::REPLICATE.get(),
            });
        }
        let operation = || {
            let replicate =
                self.replicates_mut()
                    .get_mut(dpo.index())
                    .ok_or(DpoError::ObjectMissing {
                        dpo_type: dpo.class().get(),
                        index: dpo.index(),
                    })?;
            replicate.publish_root();
            Ok(())
        };
        if hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0)
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, { operation() })
        } else {
            operation()
        }
    }

    pub fn retire_replicate_root(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::REPLICATE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::REPLICATE.get(),
            });
        }
        let operation = || {
            let replicate =
                self.replicates_mut()
                    .get_mut(dpo.index())
                    .ok_or(DpoError::ObjectMissing {
                        dpo_type: dpo.class().get(),
                        index: dpo.index(),
                    })?;
            let should_remove = replicate.withdraw_root();
            if should_remove {
                let object = self.replicates_mut().remove(dpo.index());
                drop(object);
            }
            Ok(())
        };
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, { operation() })
        } else {
            operation()
        }
    }

    fn update_replicate_inner(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        buckets: &[DpoId],
    ) -> Result<(), DpoError> {
        let proto = dpo.proto();
        let mut stacked = Vec::with_capacity(buckets.len());
        for &bucket in buckets {
            stacked.push(
                self.dpo_main_mut()
                    .stack(runtime, DpoType::REPLICATE, proto, bucket)?,
            );
        }
        let replicate = self
            .replicates()
            .get(dpo.index())
            .ok_or(DpoError::ObjectMissing {
                dpo_type: dpo.class().get(),
                index: dpo.index(),
            })?;
        let mut flags = replicate.flags;
        flags.set(
            ReplicateFlags::HAS_LOCAL,
            stacked
                .iter()
                .any(|child| child.class() == DpoType::RECEIVE),
        );
        let mut replacement = ReplicateDpo::new(proto, &stacked, flags)?;
        replacement.lock_count = replicate.lock_count;
        for &child in &stacked {
            self.lock_dpo(child);
            replacement.locked_buckets += 1;
        }
        let slot = self
            .replicates_mut()
            .get_mut(dpo.index())
            .ok_or(DpoError::ObjectMissing {
                dpo_type: dpo.class().get(),
                index: dpo.index(),
            })?;
        replacement.lock_count = slot.lock_count;
        let old = std::mem::replace(slot, replacement);
        drop(old);
        Ok(())
    }

    #[inline(always)]
    pub fn load_balance(&self, index: u32) -> Option<&LoadBalanceDpo> {
        self.load_balances().get(index)
    }

    #[inline(always)]
    pub fn replicate(&self, index: u32) -> Option<&ReplicateDpo> {
        self.replicates().get(index)
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
