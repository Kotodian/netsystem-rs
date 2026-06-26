use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::vec::Vec;

use crate::app::data::AppDataAddr;
use crate::app::ring::{
    AppCompletionEntry, AppCqe, AppCqeDescriptor, AppObjectRef, AppOpId, AppRecv, AppRingHandle,
    AppSend, AppSendData, AppSqe, AppSqeData, AppSqeDescriptor, AppSubmissionEntry,
};
use crate::spawn::{DataLocalJoinHandle, DataRuntimeContext, spawn_local};

static NEXT_APP_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);
const CLOSED_OWNER_WORKER: usize = usize::MAX;

thread_local! {
    static APP_WORKER_RINGS: RefCell<AppWorkerRingRegistry> =
        RefCell::new(AppWorkerRingRegistry::new());
    static CURRENT_APP_CONTEXT: RefCell<Option<AppContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct AppContext {
    id: usize,
    data_context: DataRuntimeContext,
    ring_capacity: usize,
    op_owners: Arc<Mutex<FlatHashTable<u64, usize>>>,
}

impl AppContext {
    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            id: next_app_context_id(),
            data_context,
            ring_capacity,
            op_owners: Arc::new(Mutex::new(FlatHashTable::new())),
        }
    }

    #[inline]
    pub fn worker_ring(&self) -> AppRingHandle {
        worker_app_ring(self.id, self.ring_capacity)
    }

    pub async fn send_on_op(&self, op: AppOpId, send: AppSend) -> HammerResult<()> {
        if self.current_worker_owns_op(op) {
            let ring = self.local_ring_for_op(op)?;
            return AppRuntime { op, ring }.send(send).await;
        }

        let send: AppSendData = send.try_into()?;
        let owner_worker = self.owner_for_op(op)?;
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        self.data_context
            .call_local_on_worker(owner_worker, move || async move {
                let ring = worker_app_ring(app_context_id, ring_capacity);
                let data = ring.copy_data_from_send(&send)?;
                send.release();
                ring.try_push_submission_descriptor(AppSqeDescriptor::new(
                    crate::app::ring::AppOpcode::Send,
                    None,
                    AppObjectRef::Operation(op),
                    AppSqeData::Send { data },
                ))
            })
            .await?
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
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        self.data_context
            .call_local_on_worker(owner_worker, move || async move {
                let worker = AppWorkerContext {
                    owner_worker,
                    op,
                    ring: worker_app_ring(app_context_id, ring_capacity),
                };
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
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        self.data_context
            .spawn_local_on_worker(owner_worker, move || async move {
                let worker = AppWorkerContext {
                    owner_worker,
                    op,
                    ring: worker_app_ring(app_context_id, ring_capacity),
                };
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
    fn local_ring_for_op(&self, op: AppOpId) -> HammerResult<AppRingHandle> {
        let owner_worker = self.owner_for_op(op)?;
        let current_worker = self.current_worker_index()?;
        if current_worker != owner_worker {
            return Err(HammerError::internal(format!(
                "app op {} is owned by worker {owner_worker}, not worker {current_worker}",
                op.value()
            )));
        }
        Ok(worker_app_ring(self.id, self.ring_capacity))
    }

    #[inline]
    pub fn try_complete_recv_buffer(
        &self,
        op: AppOpId,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        let ring = self.local_ring_for_op(op)?;
        let _ = ring.try_complete_recv_buffer(op, buffers, index, fin)?;
        Ok(())
    }

    #[inline]
    pub fn try_complete_closed_op(&self, op: AppOpId) -> HammerResult<bool> {
        let Some(owner_worker) = self.registered_op_owner(op)? else {
            return Ok(false);
        };
        if self.current_worker_index().ok() == Some(owner_worker) {
            let ring = self.local_ring_for_op(op)?;
            ring.try_push_completion(AppCqe::closed(None, Some(op)))?;
        } else {
            let app_context_id = self.id;
            let ring_capacity = self.ring_capacity;
            self.data_context
                .call_blocking_on_worker(owner_worker, move || {
                    worker_app_ring(app_context_id, ring_capacity)
                        .try_push_completion(AppCqe::closed(None, Some(op)))
                })?;
        }
        self.unregister_op_owner(op)?;
        Ok(true)
    }

    #[inline]
    fn current_worker_index(&self) -> HammerResult<usize> {
        self.data_context.current_worker_index().ok_or_else(|| {
            HammerError::internal("local app op lookup requires a data worker thread")
        })
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

    fn unregister_op_owner(&self, op: AppOpId) -> HammerResult<()> {
        let mut owners = self
            .op_owners
            .lock()
            .map_err(|_| HammerError::internal("app op owner map poisoned"))?;
        owners.insert(op.value(), CLOSED_OWNER_WORKER);
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
    ring: AppRingHandle,
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
        AppRuntime {
            op: self.op,
            ring: self.ring.clone(),
        }
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
    ring: AppRingHandle,
}

impl AppRuntime {
    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        let submit = self
            .ring
            .try_push_submission_descriptor(AppSqeDescriptor::new(
                crate::app::ring::AppOpcode::Recv,
                None,
                AppObjectRef::Operation(self.op),
                AppSqeData::Recv { max_len: u32::MAX },
            ));
        AppRecvFuture {
            ring: self.ring.clone(),
            submit: Some(submit),
        }
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.ring
            .try_push_submission(AppSqe::send(None, self.op, send))
    }

    #[inline]
    pub fn send_from_bytes(&self, bytes: &[u8]) -> HammerResult<AppSend> {
        let data = self.ring.alloc_data_for_bytes(bytes)?;
        Ok(self.ring.send_from_data(data))
    }

    #[inline]
    pub async fn shutdown(&self) -> HammerResult<()> {
        self.ring
            .try_push_submission_descriptor(AppSqeDescriptor::new(
                crate::app::ring::AppOpcode::Close,
                None,
                AppObjectRef::Operation(self.op),
                AppSqeData::Close,
            ))
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, sqe: AppSqeDescriptor) -> HammerResult<()> {
        self.ring.try_push_submission_descriptor(sqe)
    }

    #[inline]
    pub fn try_push_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        self.ring.try_push_submission(sqe)
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.ring.try_push_submission_entry(entry)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.ring.next_submission_entry().await
    }

    #[inline]
    pub fn try_pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.ring.pop_submission_entry()
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.ring.next_submission_descriptor().await
    }

    #[inline]
    pub fn try_pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.ring.pop_submission_descriptor()
    }

    #[inline]
    pub async fn next_send(&self) -> Option<AppSend> {
        self.ring
            .next_submission()
            .await
            .and_then(AppSqe::into_send)
    }

    #[inline]
    pub fn read_data(&self, data: AppDataAddr) -> HammerResult<Vec<u8>> {
        self.ring.read_data(data)
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, cqe: AppCqeDescriptor) -> HammerResult<()> {
        self.ring.try_push_completion_descriptor(cqe)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.ring.try_push_completion_entry(entry)
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.ring.next_completion_entry().await
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.ring.next_completion_descriptor().await
    }

    #[inline]
    pub async fn next_completion(&self) -> Option<AppCqe> {
        self.ring.next_completion().await
    }

    #[inline]
    pub async fn complete_recv_buffer(
        &self,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
    ) -> HammerResult<()> {
        let _ = self
            .ring
            .try_complete_recv_buffer(self.op, buffers, index, false)?;
        Ok(())
    }

    #[inline]
    pub async fn complete_recv_buffer_with_fin(
        &self,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        let _ = self
            .ring
            .try_complete_recv_buffer(self.op, buffers, index, fin)?;
        Ok(())
    }
}

pub type AppTaskContext = AppWorkerContext;
pub type AppWorkerLocalExecutor = ();

pub struct AppRecvFuture {
    ring: crate::app::ring::AppRingHandle,
    submit: Option<HammerResult<()>>,
}

impl Future for AppRecvFuture {
    type Output = HammerResult<AppRecv>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(submit) = this.submit.take() {
            submit?;
        }
        match this.ring.poll_next_completion(cx) {
            Poll::Ready(Some(cqe)) => Poll::Ready(
                cqe.into_recv()
                    .ok_or_else(|| HammerError::internal("expected recv cqe")),
            ),
            Poll::Ready(None) => Poll::Ready(Err(HammerError::internal(
                "app completion ring closed while waiting for recv cqe",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[inline]
fn next_app_context_id() -> usize {
    NEXT_APP_CONTEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[inline]
fn worker_app_ring(app_context_id: usize, ring_capacity: usize) -> AppRingHandle {
    APP_WORKER_RINGS.with(|slot| {
        slot.borrow_mut()
            .get_or_insert(app_context_id as u64, ring_capacity)
    })
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

struct AppWorkerRingRegistry {
    index_by_context: FlatHashTable<u64, usize>,
    rings: Vec<AppRingHandle>,
}

impl AppWorkerRingRegistry {
    #[inline]
    fn new() -> Self {
        Self {
            index_by_context: FlatHashTable::new(),
            rings: Vec::new(),
        }
    }

    #[inline]
    fn get_or_insert(&mut self, key: u64, ring_capacity: usize) -> AppRingHandle {
        if let Some(index) = self.index_by_context.lookup(&key)
            && let Some(ring) = self.rings.get(index).cloned()
        {
            return ring;
        }

        let index = self.rings.len();
        let ring = AppRingHandle::new(ring_capacity, ring_capacity);
        self.rings.push(ring.clone());
        self.index_by_context.insert(key, index);
        ring
    }
}
