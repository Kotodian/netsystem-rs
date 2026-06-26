use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::descriptor::Descriptor;
use hammer_infra::map::FlatHashTable;

use crate::app::session::AppSessionConfig;
use crate::spawn::{DataLocalJoinHandle, DataRuntimeContext, spawn_local};

pub enum AppOpTag {}
pub type AppOpId = Descriptor<AppOpTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppUserData(u64);

impl AppUserData {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }
}

static NEXT_APP_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);
const CLOSED_OWNER_WORKER: usize = usize::MAX;
const NOT_WIRED: &str = "vpp app boundary not wired (C2)";

thread_local! {
    static CURRENT_APP_CONTEXT: RefCell<Option<AppContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct AppContext {
    id: usize,
    data_context: DataRuntimeContext,
    app_session_config: AppSessionConfig,
    op_owners: Arc<Mutex<FlatHashTable<u64, usize>>>,
}

impl AppContext {
    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            id: next_app_context_id(),
            data_context,
            app_session_config: AppSessionConfig::new(64 * 1024, ring_capacity.max(4)),
            op_owners: Arc::new(Mutex::new(FlatHashTable::new())),
        }
    }

    #[inline]
    pub fn app_session_config(&self) -> AppSessionConfig {
        let _ = self.id;
        self.app_session_config
    }

    pub async fn send_on_op(&self, _op: AppOpId) -> HammerResult<()> {
        Err(HammerError::internal(NOT_WIRED))
    }

    pub async fn spawn_on_op<F, Fut, T>(
        &self,
        op: AppOpId,
        owner_worker: usize,
        f: F,
    ) -> HammerResult<T>
    where
        F: FnOnce(AppWorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.register_op_owner(op, owner_worker)?;
        self.data_context
            .call_local_on_worker(owner_worker, move || async move {
                let worker = AppWorkerContext { owner_worker, op };
                f(worker).await
            })
            .await
    }

    pub fn spawn_detached_on_op<F, Fut>(
        &self,
        op: AppOpId,
        owner_worker: usize,
        f: F,
    ) -> HammerResult<()>
    where
        F: FnOnce(AppWorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        self.register_op_owner(op, owner_worker)?;
        self.data_context
            .spawn_local_on_worker(owner_worker, move || async move {
                let worker = AppWorkerContext { owner_worker, op };
                f(worker).await;
            })
    }

    #[inline]
    pub fn owner_worker_for_op(&self, op: AppOpId) -> HammerResult<usize> {
        self.owner_for_op(op)
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.data_context.worker_count()
    }

    #[inline]
    pub fn current_worker_owns_op(&self, op: AppOpId) -> bool {
        self.data_context
            .current_worker_index()
            .is_some_and(|current| self.owner_for_op(op).is_ok_and(|owner| current == owner))
    }

    #[inline]
    pub fn try_complete_recv_buffer(
        &self,
        _op: AppOpId,
        _buffers: DataPlaneBuffers,
        _index: BufferIndex,
        _fin: bool,
    ) -> HammerResult<()> {
        Err(HammerError::internal(NOT_WIRED))
    }

    #[inline]
    pub fn try_complete_closed_op(&self, _op: AppOpId) -> HammerResult<bool> {
        Err(HammerError::internal(NOT_WIRED))
    }

    fn validate_owner_worker(&self, owner_worker: usize) -> HammerResult<()> {
        if owner_worker < self.worker_count() {
            return Ok(());
        }
        Err(HammerError::internal(format!(
            "app owner worker {owner_worker} is out of range for {} workers",
            self.worker_count()
        )))
    }

    fn owner_for_op(&self, op: AppOpId) -> HammerResult<usize> {
        if let Some(owner) = self.registered_op_owner(op)? {
            return Ok(owner);
        }
        let mut owners = self
            .op_owners
            .lock()
            .map_err(|_| HammerError::internal("app op owner map poisoned"))?;
        if let Some(owner) = owners.lookup(&op.value()) {
            if owner != CLOSED_OWNER_WORKER {
                return Ok(owner);
            }
            return Err(HammerError::internal(format!(
                "app op {} owner is not registered",
                op.value()
            )));
        }
        let owner = (op.value() as usize) % self.data_context.worker_count();
        owners.insert(op.value(), owner);
        Ok(owner)
    }

    fn register_op_owner(&self, op: AppOpId, owner_worker: usize) -> HammerResult<()> {
        self.validate_owner_worker(owner_worker)?;
        let mut owners = self
            .op_owners
            .lock()
            .map_err(|_| HammerError::internal("app op owner map poisoned"))?;
        owners.insert(op.value(), owner_worker);
        Ok(())
    }

    fn registered_op_owner(&self, op: AppOpId) -> HammerResult<Option<usize>> {
        let owners = self
            .op_owners
            .lock()
            .map_err(|_| HammerError::internal("app op owner map poisoned"))?;
        Ok(owners
            .lookup(&op.value())
            .filter(|owner| *owner != CLOSED_OWNER_WORKER))
    }
}

#[derive(Clone)]
pub struct AppWorkerContext {
    owner_worker: usize,
    op: AppOpId,
}

impl AppWorkerContext {
    #[inline]
    pub fn owner_worker(&self) -> usize {
        self.owner_worker
    }

    #[inline]
    pub fn op(&self) -> AppOpId {
        self.op
    }

    #[inline]
    pub fn runtime(&self) -> AppRuntime {
        AppRuntime { op: self.op }
    }

    #[inline]
    pub fn spawn_local<F, Fut, T>(&self, factory: F) -> DataLocalJoinHandle<T>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        spawn_local(factory)
    }
}

#[derive(Clone)]
pub struct AppRuntime {
    op: AppOpId,
}

impl AppRuntime {
    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        AppRecvFuture { op: self.op }
    }

    #[inline]
    pub async fn send(&self, _bytes: &[u8]) -> HammerResult<()> {
        Err(HammerError::internal(NOT_WIRED))
    }

    #[inline]
    pub async fn shutdown(&self) -> HammerResult<()> {
        Err(HammerError::internal(NOT_WIRED))
    }

    #[inline]
    pub async fn complete_recv_buffer(
        &self,
        _buffers: DataPlaneBuffers,
        _index: BufferIndex,
    ) -> HammerResult<()> {
        Err(HammerError::internal(NOT_WIRED))
    }

    #[inline]
    pub async fn complete_recv_buffer_with_fin(
        &self,
        _buffers: DataPlaneBuffers,
        _index: BufferIndex,
        _fin: bool,
    ) -> HammerResult<()> {
        Err(HammerError::internal(NOT_WIRED))
    }
}

pub type AppTaskContext = AppWorkerContext;
pub type AppWorkerLocalExecutor = ();

pub struct AppRecvFuture {
    op: AppOpId,
}

impl Future for AppRecvFuture {
    type Output = HammerResult<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _ = self.op;
        Poll::Pending
    }
}

#[inline]
fn next_app_context_id() -> usize {
    NEXT_APP_CONTEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[inline]
pub fn set_current_app_context(ctx: AppContext) {
    CURRENT_APP_CONTEXT.with(|slot| *slot.borrow_mut() = Some(ctx));
}

#[inline]
pub fn clear_current_app_context() {
    CURRENT_APP_CONTEXT.with(|slot| *slot.borrow_mut() = None);
}

#[inline]
pub fn current_app_context() -> Option<AppContext> {
    CURRENT_APP_CONTEXT.with(|slot| slot.borrow().clone())
}
