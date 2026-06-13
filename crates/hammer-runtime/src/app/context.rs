use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::vec::Vec;

use crate::app::ring::{
    AppBufferLease, AppCompletionEntry, AppCqe, AppCqeDescriptor, AppObjectRef, AppOpId, AppRecv,
    AppRegisteredBuffer, AppRingHandle, AppSend, AppSqe, AppSqeData, AppSqeDescriptor,
    AppSubmissionEntry,
};
use crate::spawn::{DataLocalJoinHandle, DataRuntimeContext, spawn_local};

static NEXT_APP_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);
const CLOSED_OWNER_WORKER: usize = usize::MAX;

thread_local! {
    static APP_OP_RINGS: RefCell<AppWorkerOpRegistry> =
        RefCell::new(AppWorkerOpRegistry::new());
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

    pub async fn send_on_op(&self, op: AppOpId, send: AppSend) -> HammerResult<()> {
        if self.current_worker_owns_op(op) {
            let ring = self.local_ring_for_op(op)?;
            return AppRuntime { op, ring }.send(send).await;
        }

        let registered = CrossWorkerAppRegisteredBuffer::new(app_send_registered_buffer(send)?);
        let descriptor = app_send_descriptor(op, registered.index());
        let owner_worker = self.owner_for_op(op)?;
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        self.data_context
            .call_local_on_worker(owner_worker, move || async move {
                let registered = registered.into_inner();
                worker_op_ring(app_context_id, op, ring_capacity).try_push_submission_entry(
                    AppSubmissionEntry::with_attachment(descriptor, registered),
                )
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
                    ring: worker_op_ring(app_context_id, op, ring_capacity),
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
                    ring: worker_op_ring(app_context_id, op, ring_capacity),
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
        Ok(worker_op_ring(self.id, op, self.ring_capacity))
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
        ring.try_complete_recv_buffer(op, buffers, index, fin)
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
                    worker_op_ring(app_context_id, op, ring_capacity)
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

struct CrossWorkerAppRegisteredBuffer {
    inner: AppRegisteredBuffer,
}

impl CrossWorkerAppRegisteredBuffer {
    #[inline]
    fn new(inner: AppRegisteredBuffer) -> Self {
        Self { inner }
    }

    #[inline]
    fn index(&self) -> BufferIndex {
        self.inner.index()
    }

    #[inline]
    fn into_inner(self) -> AppRegisteredBuffer {
        self.inner
    }
}

// SAFETY: this wrapper is used after the caller gives up ownership of AppSend.
// The remote-local closure immediately unwraps the registered buffer and
// attaches it to the owner worker's submission ring.
unsafe impl Send for CrossWorkerAppRegisteredBuffer {}

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
            .try_push_submission_entry(app_send_submission_entry(self.op, send)?)
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
    pub fn take_submission_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.ring.take_send_buffer(index)
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
    pub async fn complete_recv(&self, lease: AppBufferLease) -> HammerResult<()> {
        self.ring.try_complete_recv_lease(self.op, lease, false)
    }

    #[inline]
    pub fn take_completion_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.ring.take_recv_buffer(index)
    }
}

#[inline]
fn app_send_submission_entry(op: AppOpId, send: AppSend) -> HammerResult<AppSubmissionEntry> {
    let registered = app_send_registered_buffer(send)?;
    let descriptor = app_send_descriptor(op, registered.index());
    Ok(AppSubmissionEntry::with_attachment(descriptor, registered))
}

#[inline]
fn app_send_registered_buffer(send: AppSend) -> HammerResult<AppRegisteredBuffer> {
    AppRegisteredBuffer::from_lease(send.into_lease())
}

#[inline]
fn app_send_descriptor(op: AppOpId, buffer: BufferIndex) -> AppSqeDescriptor {
    AppSqeDescriptor::new(
        crate::app::ring::AppOpcode::Send,
        None,
        AppObjectRef::Operation(op),
        AppSqeData::Send { buffer },
    )
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
fn worker_op_ring(app_context_id: usize, op: AppOpId, ring_capacity: usize) -> AppRingHandle {
    APP_OP_RINGS.with(|slot| {
        slot.borrow_mut()
            .get_or_insert(app_op_key(app_context_id, op), ring_capacity)
    })
}

#[inline]
fn app_op_key(app_context_id: usize, op: AppOpId) -> u128 {
    ((app_context_id as u128) << 64) | u128::from(op.value())
}

struct AppWorkerOpRegistry {
    index_by_op: FlatHashTable<u128, usize>,
    rings: Vec<AppRingHandle>,
}

impl AppWorkerOpRegistry {
    #[inline]
    fn new() -> Self {
        Self {
            index_by_op: FlatHashTable::new(),
            rings: Vec::new(),
        }
    }

    #[inline]
    fn get_or_insert(&mut self, key: u128, ring_capacity: usize) -> AppRingHandle {
        if let Some(index) = self.index_by_op.lookup(&key)
            && let Some(ring) = self.rings.get(index).cloned()
        {
            return ring;
        }

        let index = self.rings.len();
        let ring = AppRingHandle::new(ring_capacity, ring_capacity);
        self.rings.push(ring.clone());
        self.index_by_op.insert(key, index);
        ring
    }
}
