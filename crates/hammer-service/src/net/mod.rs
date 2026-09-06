use std::cell::{Ref, RefCell, RefMut};
use std::sync::{Arc, OnceLock};

use hammer_infra::pool::Pool;
use hammer_runtime::{DataPlaneMain, RuntimeError, RuntimeResult};

use crate::interface::InterfaceMain;

pub mod dpo;
pub mod fib;

pub use dpo::{
    AdjacencyDpo, DpoError, DpoId, DpoMain, DpoProto, DpoType, InterfaceRxDpo, LoadBalanceDpo,
    LoadBalanceFlags, LoadBalancePath, LookupCast, LookupDpo, LookupInput, LookupTable, ReceiveDpo,
    ReplicateDpo, ReplicateFlags,
};
pub use fib::{
    FibEntry, FibEntryFlags, FibEntrySrc, FibEntrySrcFlags, FibPath, FibPathExt, FibPathExtList,
    FibPathList, FibPathListFlags, FibSource, FibSourceBehavior, FibTable, FibTableBackend,
    FibUrpfList,
};

pub struct NetMain {
    interface_main: Arc<InterfaceMain>,
    dpo_main: RefCell<DpoMain>,
    load_balances: RefCell<Pool<LoadBalanceDpo>>,
    replicates: RefCell<Pool<ReplicateDpo>>,
    urpf_lists: RefCell<Pool<FibUrpfList>>,
    local_interface_hw_index: u32,
    local_interface_sw_index: u32,
}

// SAFETY: registry access and RefCell borrow bookkeeping are main-thread-only.
// Pool mutations additionally require all Data Workers to acknowledge the
// WorkerBarrier. Workers read the pool payload without accessing the RefCell
// borrow flag, only within synchronous selection which returns copied values.
unsafe impl Send for NetMain {}
// SAFETY: the scopes above exclude worker reads during mutation. Main-thread
// guards also prevent mutation while a control-plane reference remains borrowed.
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
            dpo_main: RefCell::new(DpoMain::new()),
            load_balances: RefCell::new(Pool::new()),
            replicates: RefCell::new(Pool::new()),
            urpf_lists: RefCell::new(Pool::new()),
            local_interface_hw_index: local_hw,
            local_interface_sw_index: local_sw,
        });
        NET_MAIN
            .set(Arc::clone(&shared))
            .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "net" })?;
        Ok(shared)
    }

    fn load_balances(&self) -> Ref<'_, Pool<LoadBalanceDpo>> {
        hammer_runtime::ensure_main_thread()
            .expect("control-plane pool borrow requires the main thread");
        self.load_balances.borrow()
    }

    /// Main-thread registry access. Instance resolvers must consult their
    /// concrete owner, not recursively mutate or borrow this registry.
    pub fn dpo_main_mut(&self) -> RefMut<'_, DpoMain> {
        hammer_runtime::ensure_main_thread()
            .expect("DPO registry mutation requires the main thread");
        self.dpo_main.borrow_mut()
    }

    fn load_balances_mut(&self) -> RefMut<'_, Pool<LoadBalanceDpo>> {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("pool mutation requires the publication scope");
        self.load_balances.borrow_mut()
    }

    fn replicates(&self) -> Ref<'_, Pool<ReplicateDpo>> {
        hammer_runtime::ensure_main_thread()
            .expect("control-plane pool borrow requires the main thread");
        self.replicates.borrow()
    }

    fn replicates_mut(&self) -> RefMut<'_, Pool<ReplicateDpo>> {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("pool mutation requires the publication scope");
        self.replicates.borrow_mut()
    }

    fn validate_bucket_identity(&self, bucket: DpoId) -> Result<(), DpoError> {
        self.dpo_main()
            .identity(bucket.class(), bucket.proto(), bucket.index())
            .map(|_| ())
    }

    pub fn interface_main(&self) -> &InterfaceMain {
        &self.interface_main
    }
    pub fn dpo_main(&self) -> Ref<'_, DpoMain> {
        hammer_runtime::ensure_main_thread().expect("DPO registry reads require the main thread");
        self.dpo_main.borrow()
    }

    pub fn dpo_mtu(&self, dpo: DpoId) -> u16 {
        if !dpo.is_valid() {
            return u16::MAX;
        }
        let operation = self
            .dpo_main()
            .mtus
            .get(usize::from(dpo.class().get()))
            .copied()
            .flatten();
        operation.map_or(u16::MAX, |mtu| mtu(dpo))
    }

    pub fn dpo_urpf(&self, dpo: DpoId) -> u32 {
        if !dpo.is_valid() {
            return u32::MAX;
        }
        let operation = self
            .dpo_main()
            .urpfs
            .get(usize::from(dpo.class().get()))
            .copied()
            .flatten();
        operation.map_or(u32::MAX, |urpf| urpf(dpo))
    }

    /// Each row reports class, object size, occupied objects and allocated slots.
    pub fn dpo_memory(&self) -> Vec<(DpoType, usize, usize, usize)> {
        let operations = self.dpo_main().memory.clone();
        operations
            .into_iter()
            .enumerate()
            .filter_map(|(class, operation)| {
                operation.map(|memory| {
                    let (size, occupied, allocated) = memory();
                    (DpoType::new(class as u8), size, occupied, allocated)
                })
            })
            .collect()
    }

    /// Returns one owned reference. The concrete caller must eventually unlock
    /// it; copying its identity neither acquires nor releases that reference.
    pub fn interpose_dpo(&self, original: DpoId, parent: DpoId) -> Result<DpoId, DpoError> {
        if !original.is_valid() {
            return Ok(DpoId::INVALID);
        }
        hammer_runtime::ensure_main_thread_with_barrier()?;
        // Do not hold a registry borrow across a callback which may stack a DPO.
        let operation = self
            .dpo_main()
            .interposes
            .get(usize::from(original.class().get()))
            .copied()
            .flatten();
        match operation {
            Some(interpose) => interpose(original, parent),
            None => {
                self.lock_dpo(original);
                Ok(original)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_dpo(
        &self,
        dpo_type: Option<DpoType>,
        nodes: &[(DpoProto, &[hammer_core::data_plane::NodeId])],
        locks: Option<(fn(DpoId), fn(DpoId))>,
        next_nodes: Option<fn(DpoId) -> Vec<hammer_core::data_plane::NodeId>>,
        mtu: Option<fn(DpoId) -> u16>,
        urpf: Option<fn(DpoId) -> u32>,
        interpose: Option<fn(DpoId, DpoId) -> Result<DpoId, DpoError>>,
        format: Option<fn(DpoId, &mut std::fmt::Formatter<'_>) -> std::fmt::Result>,
        memory: Option<fn() -> (usize, usize, usize)>,
    ) -> Result<DpoType, DpoError> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        self.dpo_main_mut().register(
            dpo_type, nodes, locks, next_nodes, mtu, urpf, interpose, format, memory,
        )
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
            self.dpo_main()
                .identity(dpo.class(), dpo.proto(), dpo.index())
                .is_ok(),
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
            self.dpo_main()
                .identity(dpo.class(), dpo.proto(), dpo.index())
                .is_ok(),
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
        // Reject a retained control-plane borrow before graph or child changes.
        drop(self.load_balances_mut());
        let bucket_count = usize::from(load_balance.bucket_count());
        let mut stacked = Vec::with_capacity(bucket_count);
        for bucket_index in 0..bucket_count {
            stacked.push(
                *load_balance
                    .bucket_mut(bucket_index)
                    .ok_or(DpoError::InvalidBucketStorage)?,
            );
        }
        self.dpo_main_mut()
            .stack_buckets(runtime, DpoType::LOAD_BALANCE, proto, &mut stacked)?;
        for (bucket_index, &bucket) in stacked.iter().enumerate() {
            *load_balance
                .bucket_mut(bucket_index)
                .expect("validated bucket storage") = bucket;
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

    /// Normalizes resolved paths and replaces an existing load-balance object.
    ///
    /// VPP updates the bucket storage and the reported bucket count as one
    /// worker-visible transaction. The detached child identities are stacked
    /// before the pool object is changed; the barrier therefore never exposes
    /// a count whose backing storage is incomplete.
    pub fn update_load_balance(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        paths: &[LoadBalancePath],
    ) -> Result<Option<()>, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::LOAD_BALANCE.get(),
            });
        }
        for path in paths {
            self.validate_bucket_identity(path.dpo)?;
        }
        {
            let Some(object) = self.load_balance(dpo.index()) else {
                return Ok(None);
            };
            if object.proto != dpo.proto() {
                return Err(DpoError::ProtocolMismatch {
                    actual: dpo.proto().get(),
                    expected: object.proto.get(),
                });
            }
        }

        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.update_load_balance_inner(runtime, dpo, paths)
            })
        } else {
            self.update_load_balance_inner(runtime, dpo, paths)
        }
        .map(Some)
    }

    fn update_load_balance_inner(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        paths: &[LoadBalancePath],
    ) -> Result<(), DpoError> {
        drop(self.load_balances_mut());
        let proto = dpo.proto();
        let load_balance = self
            .load_balance(dpo.index())
            .expect("validated load-balance remains present during the update scope");
        let mut replacement = LoadBalanceDpo::new(
            proto,
            paths,
            load_balance.flags,
            load_balance.flow_hash_config(),
        )?;
        replacement.lock_count = load_balance.lock_count;
        replacement.map_index = load_balance.map_index;
        let urpf_index = load_balance.urpf_index;
        replacement.fib_entry_flags = load_balance.fib_entry_flags;
        drop(load_balance);
        if urpf_index != u32::MAX {
            // Reject retained control-plane list borrows before graph changes.
            drop(self.urpf_lists.borrow_mut());
        }
        let mut stacked: Vec<_> = (0..replacement.bucket_count())
            .map(|bucket| {
                replacement
                    .select_bucket(u32::from(bucket))
                    .expect("normalized bucket lies within constructed storage")
            })
            .collect();
        self.dpo_main_mut()
            .stack_buckets(runtime, DpoType::LOAD_BALANCE, proto, &mut stacked)?;
        for (bucket, &child) in stacked.iter().enumerate() {
            *replacement
                .bucket_mut(bucket)
                .expect("stacking preserves the normalized bucket count") = child;
        }
        for &child in &stacked {
            self.lock_dpo(child);
            replacement.locked_buckets += 1;
        }
        if urpf_index != u32::MAX {
            self.lock_urpf_list(urpf_index);
            replacement.urpf_index = urpf_index;
        }
        // A child may refer to this same object; preserve counts acquired above.
        let mut pool = self.load_balances_mut();
        let slot = pool
            .get_mut(dpo.index())
            .expect("child reference acquisition must not retire the update target");
        replacement.lock_count = slot.lock_count;
        let old = std::mem::replace(slot, replacement);
        drop(pool);
        drop(old);
        Ok(())
    }

    pub fn set_load_balance_urpf(
        &self,
        dpo: DpoId,
        urpf_index: u32,
    ) -> Result<Option<()>, DpoError> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(DpoError::TypeMismatch {
                actual: dpo.class().get(),
                expected: DpoType::LOAD_BALANCE.get(),
            });
        }
        let mut pool = self.load_balances_mut();
        let Some(object) = pool.get_mut(dpo.index()) else {
            return Ok(None);
        };
        if object.proto != dpo.proto() {
            return Err(DpoError::ProtocolMismatch {
                actual: dpo.proto().get(),
                expected: object.proto.get(),
            });
        }
        if urpf_index != u32::MAX && self.urpf_list(urpf_index).is_none() {
            return Ok(None);
        }
        // Clearing an association also releases the old list. Validate that
        // borrow boundary before changing the owning field, not during unlock.
        drop(self.urpf_lists.borrow_mut());
        if urpf_index != u32::MAX {
            self.lock_urpf_list(urpf_index);
        }
        let old = std::mem::replace(&mut object.urpf_index, urpf_index);
        drop(pool);
        self.unlock_urpf_list(old);
        Ok(Some(()))
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
        drop(self.replicates_mut());
        let bucket_count = usize::from(replicate.bucket_count());
        let mut has_local = false;
        let mut stacked = Vec::with_capacity(bucket_count);
        for bucket_index in 0..bucket_count {
            let bucket = replicate
                .bucket_mut(bucket_index)
                .ok_or(DpoError::InvalidBucketStorage)?;
            has_local |= bucket.class() == DpoType::RECEIVE;
            stacked.push(*bucket);
        }
        self.dpo_main_mut()
            .stack_buckets(runtime, DpoType::REPLICATE, proto, &mut stacked)?;
        for (bucket_index, &bucket) in stacked.iter().enumerate() {
            *replicate
                .bucket_mut(bucket_index)
                .expect("validated bucket storage") = bucket;
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
    ) -> Result<Option<()>, DpoError> {
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
        {
            let Some(object) = self.replicate(dpo.index()) else {
                return Ok(None);
            };
            if object.proto != dpo.proto() {
                return Err(DpoError::ProtocolMismatch {
                    actual: dpo.proto().get(),
                    expected: object.proto.get(),
                });
            }
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
        .map(Some)
    }

    fn update_replicate_inner(
        &self,
        runtime: &mut DataPlaneMain,
        dpo: DpoId,
        buckets: &[DpoId],
    ) -> Result<(), DpoError> {
        drop(self.replicates_mut());
        let proto = dpo.proto();
        let mut stacked = buckets.to_vec();
        self.dpo_main_mut()
            .stack_buckets(runtime, DpoType::REPLICATE, proto, &mut stacked)?;
        let replicate = self
            .replicate(dpo.index())
            .expect("validated replicate remains present during the update scope");
        let mut flags = replicate.flags;
        flags.set(
            ReplicateFlags::HAS_LOCAL,
            stacked
                .iter()
                .any(|child| child.class() == DpoType::RECEIVE),
        );
        let mut replacement = ReplicateDpo::new(proto, &stacked, flags)?;
        replacement.lock_count = replicate.lock_count;
        drop(replicate);
        for &child in &stacked {
            self.lock_dpo(child);
            replacement.locked_buckets += 1;
        }
        let mut pool = self.replicates_mut();
        let slot = pool
            .get_mut(dpo.index())
            .expect("child reference acquisition must not retire the update target");
        replacement.lock_count = slot.lock_count;
        let old = std::mem::replace(slot, replacement);
        drop(pool);
        drop(old);
        Ok(())
    }

    #[inline(always)]
    pub fn load_balance(&self, index: u32) -> Option<Ref<'_, LoadBalanceDpo>> {
        Ref::filter_map(self.load_balances(), |pool| pool.get(index)).ok()
    }

    #[inline(always)]
    pub fn replicate(&self, index: u32) -> Option<Ref<'_, ReplicateDpo>> {
        Ref::filter_map(self.replicates(), |pool| pool.get(index)).ok()
    }

    #[inline(always)]
    pub fn select_load_balance(
        &self,
        dpo: DpoId,
        hash: impl FnOnce(u16, u16) -> Option<u32>,
    ) -> Option<DpoId> {
        if dpo.class() != DpoType::LOAD_BALANCE {
            return None;
        }
        let select = |pool: &Pool<LoadBalanceDpo>| {
            let object = pool.get(dpo.index())?;
            let hash = hash(object.bucket_count(), object.flow_hash_config())?;
            object.select_bucket(hash)
        };
        if hammer_runtime::ensure_main_thread().is_ok() {
            return select(&self.load_balances());
        }
        hammer_runtime::with_data_plane_main(|runtime| {
            assert_ne!(
                runtime.thread_index(),
                0,
                "packet selection requires an installed Data Worker"
            );
            // SAFETY: this worker cannot acknowledge a barrier during this
            // synchronous selection. Only the main thread mutates the pool,
            // after every worker acknowledges. No pool reference escapes.
            unsafe { select(&*self.load_balances.as_ptr()) }
        })
    }

    #[inline(always)]
    pub fn load_balance_urpf(&self, dpo: DpoId) -> Option<u32> {
        if dpo.class() != DpoType::LOAD_BALANCE {
            return None;
        }
        let index = if hammer_runtime::ensure_main_thread().is_ok() {
            self.load_balance(dpo.index())?.urpf_index
        } else {
            hammer_runtime::with_data_plane_main(|runtime| {
                assert_ne!(
                    runtime.thread_index(),
                    0,
                    "uRPF lookup requires a Data Worker"
                );
                // SAFETY: no barrier acknowledgement can occur during this
                // read; only the retained list index leaves the worker scope.
                unsafe {
                    (&*self.load_balances.as_ptr())
                        .get(dpo.index())
                        .map(|object| object.urpf_index)
                }
            })?
        };
        (index != u32::MAX).then_some(index)
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
