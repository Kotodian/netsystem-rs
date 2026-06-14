//! Thin application-facing wrapper around Hammer's op-owned app ring runtime.
//!
//! `hammer-app` intentionally exposes the same operation-descriptor model as
//! `hammer-runtime::app`: app workers submit SQEs/CQEs on an app-worker ring,
//! descriptors identify opaque operations, and payloads reference app data-area
//! addresses rather than dataplane packet buffers.

pub mod echo;
pub mod ring;
pub mod tcp;
pub mod udp;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hammer_core::error::{HammerError, HammerResult};
use hammer_runtime::app as runtime_app;
pub use hammer_runtime::spawn::DataRuntimeContext;

pub use crate::ring::{
    AppCompletionEntry, AppCqe, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppCqeKind, AppDataAddr,
    AppDataArea, AppDataAreaConfig, AppObjectRef, AppOpId, AppOpcode, AppRecv, AppRing,
    AppRingExport, AppRingHandle, AppRingIpcReservation, AppRingLayout, AppRingMemoryKind, AppSend,
    AppSqe, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};

#[derive(Clone)]
pub struct AppContext {
    inner: runtime_app::AppContext,
}

impl AppContext {
    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            inner: runtime_app::AppContext::with_ring_capacity(data_context, ring_capacity),
        }
    }

    #[inline]
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
        self.inner
            .spawn_on_op(op, owner_worker, move |worker| {
                f(AppWorkerContext::from_inner(worker))
            })
            .await
    }

    #[inline]
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
        self.inner
            .spawn_detached_on_op(op, owner_worker, move |worker| {
                f(AppWorkerContext::from_inner(worker))
            })
    }

    #[inline]
    pub async fn send_on_op(&self, op: AppOpId, send: AppSend) -> HammerResult<()> {
        self.inner.send_on_op(op, send).await
    }

    #[inline]
    pub fn owner_worker_for_op(&self, op: AppOpId) -> HammerResult<usize> {
        self.inner.owner_worker_for_op(op)
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.inner.worker_count()
    }

    #[inline]
    pub fn current_worker_owns_op(&self, op: AppOpId) -> bool {
        self.inner.current_worker_owns_op(op)
    }

    #[inline]
    pub fn try_complete_recv_buffer(
        &self,
        op: AppOpId,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: hammer_adapter::BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        self.inner.try_complete_recv_buffer(op, buffers, index, fin)
    }

    #[inline]
    pub fn try_complete_closed_op(&self, op: AppOpId) -> HammerResult<bool> {
        self.inner.try_complete_closed_op(op)
    }
}

#[derive(Clone)]
pub struct App {
    context: AppContext,
}

impl App {
    #[inline]
    pub fn new(data_context: DataRuntimeContext) -> Self {
        Self::with_ring_capacity(data_context, 256)
    }

    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            context: AppContext::with_ring_capacity(data_context, ring_capacity),
        }
    }

    #[inline]
    pub fn context(&self) -> &AppContext {
        &self.context
    }

    #[inline]
    pub async fn spawn_on_op<F, Fut, T>(
        &self,
        op: AppOpId,
        owner_worker: usize,
        f: F,
    ) -> HammerResult<T>
    where
        F: FnOnce(AppOp) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let app = self.clone();
        self.context
            .spawn_on_op(op, owner_worker, move |worker| {
                f(AppOp {
                    app,
                    op,
                    worker: Some(worker),
                })
            })
            .await
    }

    #[inline]
    pub fn op(&self, op: AppOpId) -> AppOp {
        AppOp {
            app: self.clone(),
            op,
            worker: None,
        }
    }
}

impl From<AppContext> for App {
    #[inline]
    fn from(context: AppContext) -> Self {
        Self { context }
    }
}

#[derive(Clone)]
pub struct AppOp {
    app: App,
    op: AppOpId,
    worker: Option<AppWorkerContext>,
}

impl AppOp {
    #[inline]
    pub fn id(&self) -> AppOpId {
        self.op
    }

    #[inline]
    pub fn app(&self) -> &App {
        &self.app
    }

    #[inline]
    pub async fn owner(&self) -> HammerResult<usize> {
        self.app.context.owner_worker_for_op(self.op)
    }

    #[inline]
    pub async fn run<F, Fut, T>(&self, owner_worker: usize, f: F) -> HammerResult<T>
    where
        F: FnOnce(AppOp) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.app.spawn_on_op(self.op, owner_worker, f).await
    }

    #[inline]
    pub fn owner_worker(&self) -> usize {
        self.worker
            .as_ref()
            .expect("app op owner_worker requires worker context")
            .owner_worker()
    }

    #[inline]
    pub fn runtime(&self) -> AppRuntime {
        self.worker
            .as_ref()
            .expect("app op runtime requires worker context")
            .runtime()
    }

    #[inline]
    pub fn ring(&self) -> AppRing {
        AppRing::new(self.runtime().into_inner())
    }

    #[inline]
    pub fn recv(&self) -> runtime_app::AppRecvFuture {
        self.runtime().recv()
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.runtime().send(send).await
    }

    #[inline]
    pub async fn shutdown(&self) -> HammerResult<()> {
        self.runtime().shutdown().await
    }

    #[inline]
    pub fn spawn_local<F, Fut, T>(&self, factory: F) -> AppLocalJoinHandle<T>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.worker
            .as_ref()
            .expect("app op spawn_local requires worker context")
            .spawn_local(factory)
    }
}

#[derive(Clone)]
pub struct AppWorkerContext {
    inner: runtime_app::AppWorkerContext,
}

impl AppWorkerContext {
    #[inline]
    pub fn owner_worker(&self) -> usize {
        self.inner.owner_worker()
    }

    #[inline]
    pub fn op(&self) -> AppOpId {
        self.inner.op()
    }

    #[inline]
    pub fn runtime(&self) -> AppRuntime {
        AppRuntime::from_inner(self.inner.runtime())
    }

    #[inline]
    pub fn ring(&self) -> AppRing {
        AppRing::new(self.inner.runtime())
    }

    #[inline]
    pub fn spawn_local<F, Fut, T>(&self, factory: F) -> AppLocalJoinHandle<T>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        AppLocalJoinHandle::from_inner(self.inner.spawn_local(factory))
    }

    #[inline]
    fn from_inner(inner: runtime_app::AppWorkerContext) -> Self {
        Self { inner }
    }
}

pub type AppTaskContext = AppWorkerContext;
pub type AppWorkerLocalExecutor = ();

#[derive(Clone)]
pub struct AppRuntime {
    inner: runtime_app::AppRuntime,
}

impl AppRuntime {
    #[inline]
    pub fn recv(&self) -> runtime_app::AppRecvFuture {
        self.inner.recv()
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.inner.send(send).await
    }

    #[inline]
    pub fn send_from_bytes(&self, bytes: &[u8]) -> HammerResult<AppSend> {
        self.inner.send_from_bytes(bytes)
    }

    #[inline]
    pub async fn shutdown(&self) -> HammerResult<()> {
        self.inner.shutdown().await
    }

    #[inline]
    pub fn read_data(&self, data: AppDataAddr) -> HammerResult<std::vec::Vec<u8>> {
        self.inner.read_data(data)
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, descriptor: AppSqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_submission_descriptor(descriptor)
    }

    #[inline]
    pub fn try_push_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        self.inner.try_push_submission(sqe)
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.inner.try_push_submission_entry(entry)
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner.next_submission_descriptor().await
    }

    #[inline]
    pub fn try_pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner.try_pop_submission_descriptor()
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner.next_submission_entry().await
    }

    #[inline]
    pub fn try_pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner.try_pop_submission_entry()
    }

    #[inline]
    pub async fn next_send(&self) -> Option<AppSend> {
        self.inner.next_send().await
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, cqe: AppCqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_completion_descriptor(cqe)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.inner.try_push_completion_entry(entry)
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.inner.next_completion_descriptor().await
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.inner.next_completion_entry().await
    }

    #[inline]
    pub async fn next_completion(&self) -> Option<AppCqe> {
        self.inner.next_completion().await
    }

    #[inline]
    pub async fn complete_recv_buffer(
        &self,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: hammer_adapter::BufferIndex,
    ) -> HammerResult<()> {
        self.inner.complete_recv_buffer(buffers, index).await
    }

    #[inline]
    pub async fn complete_recv_buffer_with_fin(
        &self,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: hammer_adapter::BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        self.inner
            .complete_recv_buffer_with_fin(buffers, index, fin)
            .await
    }

    #[inline]
    fn from_inner(inner: runtime_app::AppRuntime) -> Self {
        Self { inner }
    }

    #[inline]
    fn into_inner(self) -> runtime_app::AppRuntime {
        self.inner
    }
}

impl TryFrom<AppOp> for AppWorkerContext {
    type Error = HammerError;

    fn try_from(op: AppOp) -> Result<Self, Self::Error> {
        op.worker
            .ok_or_else(|| HammerError::internal("app op requires worker context"))
    }
}

#[derive(Debug)]
pub struct AppLocalJoinError {
    inner: hammer_runtime::spawn::DataLocalJoinError,
}

impl AppLocalJoinError {
    #[inline]
    fn from_inner(inner: hammer_runtime::spawn::DataLocalJoinError) -> Self {
        Self { inner }
    }
}

impl Display for AppLocalJoinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error for AppLocalJoinError {}

pub struct AppLocalJoinHandle<T> {
    inner: hammer_runtime::spawn::DataLocalJoinHandle<T>,
}

impl<T> AppLocalJoinHandle<T> {
    #[inline]
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    #[inline]
    fn from_inner(inner: hammer_runtime::spawn::DataLocalJoinHandle<T>) -> Self {
        Self { inner }
    }
}

impl<T> Unpin for AppLocalJoinHandle<T> {}

impl<T> Future for AppLocalJoinHandle<T> {
    type Output = Result<T, AppLocalJoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|result| result.map_err(AppLocalJoinError::from_inner))
    }
}
