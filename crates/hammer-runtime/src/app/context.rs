use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::vec::Vec;

use crate::app::backend::{AppBackend, AppBackendRecvQueue, AppBackendSendQueue};
use crate::app::ring::{
    AppCompletionEntry, AppCqeDescriptor, AppObjectRef, AppRecv, AppRegisteredBuffer, AppSend,
    AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};
use crate::spawn::{DataLocalJoinHandle, DataRuntimeContext, spawn_local};
use hammer_adapter::{BufferIndex, DataPlaneBuffers};

static NEXT_APP_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    static APP_FLOW_BACKENDS: RefCell<AppWorkerFlowRegistry> =
        RefCell::new(AppWorkerFlowRegistry::new());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppFlowId(u64);

impl AppFlowId {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone)]
pub struct AppContext {
    id: usize,
    data_context: DataRuntimeContext,
    ring_capacity: usize,
    owners: Arc<Mutex<FlatHashTable<u64, usize>>>,
}

impl AppContext {
    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            id: next_app_context_id(),
            data_context,
            ring_capacity,
            owners: Arc::new(Mutex::new(FlatHashTable::new())),
        }
    }

    pub async fn spawn_on_flow<F, Fut, T>(&self, flow: AppFlowId, f: F) -> HammerResult<T>
    where
        F: FnOnce(AppWorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let owner_worker = self.owner_for(flow)?;
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        let context = self.data_context.clone();
        let handle = context.execute_on_worker(owner_worker, async move {
            let local = spawn_local(move || async move {
                let worker = AppWorkerContext {
                    owner_worker,
                    backend: worker_backend(app_context_id, flow, ring_capacity),
                };
                f(worker).await
            });
            local.await.expect("app local worker task")
        });
        handle
            .await
            .map_err(|err| HammerError::internal(format!("join app flow task: {err}")))
    }

    #[inline]
    pub fn owner_worker_for_flow(&self, flow: AppFlowId) -> HammerResult<usize> {
        self.owner_for(flow)
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.data_context.worker_count()
    }

    #[inline]
    pub fn current_worker_owns_flow(&self, flow: AppFlowId) -> bool {
        self.data_context
            .current_worker_index()
            .is_some_and(|current| self.owner_for(flow).is_ok_and(|owner| current == owner))
    }

    #[inline]
    pub fn local_backend_for_flow(&self, flow: AppFlowId) -> HammerResult<AppBackend> {
        let owner_worker = self.owner_for(flow)?;
        let current_worker = self.current_worker_index()?;
        if current_worker != owner_worker {
            return Err(HammerError::internal(format!(
                "app flow {} is owned by worker {owner_worker}, not worker {current_worker}",
                flow.value()
            )));
        }
        Ok(worker_backend(self.id, flow, self.ring_capacity))
    }

    #[inline]
    pub fn try_complete_recv_buffer(
        &self,
        flow: AppFlowId,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        let backend = self.local_backend_for_flow(flow)?;
        backend
            .ring_handle()
            .try_complete_recv_buffer(flow, buffers, index, fin)
    }

    #[inline]
    fn current_worker_index(&self) -> HammerResult<usize> {
        self.data_context.current_worker_index().ok_or_else(|| {
            HammerError::internal("local app backend lookup requires a data worker thread")
        })
    }

    fn owner_for(&self, flow: AppFlowId) -> HammerResult<usize> {
        let mut owners = self
            .owners
            .lock()
            .map_err(|_| HammerError::internal("app owner map poisoned"))?;
        if let Some(owner) = owners.lookup(&flow.value()) {
            return Ok(owner);
        }
        let owner = (flow.value() as usize) % self.data_context.worker_count();
        owners.insert(flow.value(), owner);
        Ok(owner)
    }
}

#[derive(Clone)]
pub struct AppWorkerContext {
    owner_worker: usize,
    backend: AppBackend,
}

impl AppWorkerContext {
    #[inline]
    pub fn owner_worker(&self) -> usize {
        self.owner_worker
    }

    #[inline]
    pub fn backend(&self) -> AppBackend {
        self.backend.clone()
    }

    #[inline]
    pub fn runtime(&self) -> AppRuntime {
        AppRuntime {
            flow: self.backend.flow(),
            recv: self.backend.recv_queue(),
            send: self.backend.send_queue(),
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
    flow: AppFlowId,
    recv: AppBackendRecvQueue,
    send: AppBackendSendQueue,
}

impl AppRuntime {
    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        let submit = self
            .send
            .ring()
            .try_push_submission_descriptor(AppSqeDescriptor::new(
                crate::app::ring::AppOpcode::Recv,
                AppUserData::new(0),
                AppObjectRef::Flow(self.flow),
                AppSqeData::Recv { max_len: u32::MAX },
            ));
        AppRecvFuture {
            ring: self.recv.ring(),
            submit: Some(submit),
        }
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        let lease = send.into_lease();
        let registered = AppRegisteredBuffer::from_lease(lease)?;
        let descriptor = AppSqeDescriptor::new(
            crate::app::ring::AppOpcode::Send,
            AppUserData::new(0),
            AppObjectRef::Flow(self.flow),
            AppSqeData::Send {
                buffer: registered.index(),
            },
        );
        self.send
            .ring()
            .try_push_submission_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, sqe: AppSqeDescriptor) -> HammerResult<()> {
        self.send.ring().try_push_submission_descriptor(sqe)
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.send.ring().try_push_submission_entry(entry)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.send.ring().next_submission_entry().await
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.send.ring().next_submission_descriptor().await
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.recv.ring().next_completion_entry().await
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.recv.ring().next_completion_descriptor().await
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
fn worker_backend(app_context_id: usize, flow: AppFlowId, ring_capacity: usize) -> AppBackend {
    APP_FLOW_BACKENDS.with(|slot| {
        slot.borrow_mut()
            .get_or_insert(app_flow_key(app_context_id, flow), flow, ring_capacity)
    })
}

#[inline]
fn app_flow_key(app_context_id: usize, flow: AppFlowId) -> u128 {
    ((app_context_id as u128) << 64) | u128::from(flow.value())
}

struct AppWorkerFlowRegistry {
    index_by_flow: FlatHashTable<u128, usize>,
    backends: Vec<AppBackend>,
}

impl AppWorkerFlowRegistry {
    #[inline]
    fn new() -> Self {
        Self {
            index_by_flow: FlatHashTable::new(),
            backends: Vec::new(),
        }
    }

    #[inline]
    fn get_or_insert(&mut self, key: u128, flow: AppFlowId, ring_capacity: usize) -> AppBackend {
        if let Some(index) = self.index_by_flow.lookup(&key)
            && let Some(backend) = self.backends.get(index).cloned()
        {
            return backend;
        }

        let index = self.backends.len();
        let backend = AppBackend::with_flow(ring_capacity, flow);
        self.backends.push(backend.clone());
        self.index_by_flow.insert(key, index);
        backend
    }
}
