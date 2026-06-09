//! `hammer-app` keeps the low-level app ring surface thin and runtime-shaped.
//!
//! Legacy queue wrappers and standalone backend construction are intentionally
//! not part of the public API:
//!
//! ```compile_fail
//! use hammer_app::AppBackendRecvQueue;
//! let _ = std::any::type_name::<AppBackendRecvQueue>();
//! ```
//!
//! ```compile_fail
//! use hammer_app::AppBackendSendQueue;
//! let _ = std::any::type_name::<AppBackendSendQueue>();
//! ```
//!
//! ```compile_fail
//! let _ = hammer_app::AppBackend::new(4);
//! ```
//!
pub mod echo;
pub mod ring;
pub mod tcp;
pub mod udp;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hammer_adapter::BufferIndex;
use hammer_core::error::{HammerError, HammerResult};
pub use hammer_runtime::app::{AppControl, AppControlBackend};
pub use hammer_runtime::spawn::DataRuntimeContext;

pub use crate::ring::{
    AppBufferLease, AppCompletionEntry, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppObjectRef,
    AppOpcode, AppRecv, AppRegisteredBuffer, AppRing, AppSend, AppSocketId, AppSqeData,
    AppSqeDescriptor, AppSubmissionEntry, AppUserData, TransportKind,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AppFlowId {
    inner: hammer_runtime::app::AppFlowId,
}

impl AppFlowId {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self {
            inner: hammer_runtime::app::AppFlowId::new(value),
        }
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.inner.value()
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.inner.slot()
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.inner.generation()
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> hammer_runtime::app::AppFlowId {
        self.inner
    }
}

#[derive(Clone)]
pub struct AppContext {
    inner: hammer_runtime::app::AppContext,
}

impl AppContext {
    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            inner: hammer_runtime::app::AppContext::with_ring_capacity(data_context, ring_capacity),
        }
    }

    pub async fn spawn_on_flow<F, Fut, T>(&self, flow: AppFlowId, f: F) -> HammerResult<T>
    where
        F: FnOnce(AppWorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.inner
            .spawn_on_flow(flow.into_inner(), move |worker| {
                f(AppWorkerContext::from_inner(worker))
            })
            .await
    }

    #[inline]
    pub fn install_control(&self, control: AppControl) -> HammerResult<()> {
        self.inner.install_control(control)
    }

    #[inline]
    pub fn bind_tcp_listener(
        &self,
        bind: std::net::SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        self.inner
            .bind_tcp_listener(bind, owner_worker)
            .map(|socket| AppSocketId::new(socket.value()))
    }

    #[inline]
    pub fn bind_udp_socket(
        &self,
        bind: std::net::SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        self.inner
            .bind_udp_socket(bind, owner_worker)
            .map(|socket| AppSocketId::new(socket.value()))
    }

    #[inline]
    pub fn close_socket(&self, socket: AppSocketId) -> HammerResult<()> {
        self.inner.close_socket(socket.into_inner())
    }

    #[inline]
    pub fn owner_worker_for_socket(&self, socket: AppSocketId) -> HammerResult<usize> {
        self.inner.owner_worker_for_socket(socket.into_inner())
    }

    #[inline]
    pub fn owner_worker_for_flow(&self, flow: AppFlowId) -> HammerResult<usize> {
        self.inner.owner_worker_for_flow(flow.into_inner())
    }

    #[inline]
    pub fn local_backend_for_socket(&self, socket: AppSocketId) -> HammerResult<AppBackend> {
        self.inner
            .local_backend_for_socket(socket.into_inner())
            .map(AppBackend::from_inner)
    }

    #[inline]
    pub fn local_backend_for_flow(&self, flow: AppFlowId) -> HammerResult<AppBackend> {
        self.inner
            .local_backend_for_flow(flow.into_inner())
            .map(AppBackend::from_inner)
    }

    #[inline]
    pub fn try_complete_recv_buffer(
        &self,
        flow: AppFlowId,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: BufferIndex,
        fin: bool,
    ) -> HammerResult<()> {
        self.inner
            .try_complete_recv_buffer(flow.into_inner(), buffers, index, fin)
    }

    #[inline]
    pub fn try_complete_recv_from_buffer(
        &self,
        socket: AppSocketId,
        source: std::net::SocketAddr,
        buffers: hammer_adapter::DataPlaneBuffers,
        index: BufferIndex,
        truncated: bool,
    ) -> HammerResult<()> {
        self.inner.try_complete_recv_from_buffer(
            socket.into_inner(),
            source,
            buffers,
            index,
            truncated,
        )
    }

    #[inline]
    pub fn try_complete_accept(&self, listener: AppSocketId, flow: AppFlowId) -> HammerResult<()> {
        self.inner
            .try_complete_accept(listener.into_inner(), flow.into_inner())
    }
}

#[derive(Clone, Debug)]
pub struct AppBackend {
    inner: hammer_runtime::app::AppBackend,
}

impl AppBackend {
    #[inline]
    pub async fn complete_recv(&self, lease: AppBufferLease) -> HammerResult<()> {
        self.inner.complete_recv(lease.into_inner()).await
    }

    #[inline]
    pub async fn next_send(&self) -> Option<AppSend> {
        self.inner.next_send().await.map(AppSend::from_inner)
    }

    #[inline]
    pub fn try_push_sqe_descriptor(&self, descriptor: AppSqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_sqe_descriptor(descriptor.into_inner())
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.inner.try_push_submission_entry(entry.into_inner())
    }

    #[inline]
    pub async fn next_sqe_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner
            .next_sqe_descriptor()
            .await
            .map(AppSqeDescriptor::from_inner)
    }

    #[inline]
    pub fn take_submission_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.inner
            .take_submission_buffer(index)
            .map(AppSend::from_inner)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner
            .next_submission_entry()
            .await
            .map(crate::ring::AppSubmissionEntry::from_inner)
    }

    #[inline]
    pub fn try_push_cqe_descriptor(&self, descriptor: AppCqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_cqe_descriptor(descriptor.into_inner())
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.inner.try_push_completion_entry(entry.into_inner())
    }

    #[inline]
    pub async fn next_cqe_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.inner
            .next_cqe_descriptor()
            .await
            .map(crate::ring::AppCqeDescriptor::from_inner)
    }

    #[inline]
    pub fn take_completion_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.inner
            .take_completion_buffer(index)
            .map(AppRecv::from_inner)
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.inner
            .next_completion_entry()
            .await
            .map(crate::ring::AppCompletionEntry::from_inner)
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppBackend) -> Self {
        Self { inner }
    }
}

#[derive(Clone)]
pub struct AppRuntime {
    inner: hammer_runtime::app::AppRuntime,
}

impl AppRuntime {
    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        AppRecvFuture {
            inner: self.inner.recv(),
        }
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.inner.send(send.into_inner()).await
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, descriptor: AppSqeDescriptor) -> HammerResult<()> {
        self.inner
            .try_push_submission_descriptor(descriptor.into_inner())
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.inner.try_push_submission_entry(entry.into_inner())
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner
            .next_submission_descriptor()
            .await
            .map(crate::ring::AppSqeDescriptor::from_inner)
    }

    #[inline]
    pub fn take_submission_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.inner
            .take_submission_buffer(index)
            .map(AppSend::from_inner)
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner
            .next_submission_entry()
            .await
            .map(crate::ring::AppSubmissionEntry::from_inner)
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, descriptor: AppCqeDescriptor) -> HammerResult<()> {
        self.inner
            .try_push_completion_descriptor(descriptor.into_inner())
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.inner.try_push_completion_entry(entry.into_inner())
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.inner
            .next_completion_descriptor()
            .await
            .map(crate::ring::AppCqeDescriptor::from_inner)
    }

    #[inline]
    pub fn take_completion_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.inner
            .take_completion_buffer(index)
            .map(AppRecv::from_inner)
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.inner
            .next_completion_entry()
            .await
            .map(crate::ring::AppCompletionEntry::from_inner)
    }

    #[inline]
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppRuntime) -> Self {
        Self { inner }
    }
}

pub struct AppRecvFuture {
    inner: hammer_runtime::app::AppRecvFuture,
}

impl Future for AppRecvFuture {
    type Output = HammerResult<AppRecv>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|result| result.map(AppRecv::from_inner))
    }
}

#[derive(Clone)]
pub struct AppWorkerContext {
    inner: hammer_runtime::app::AppWorkerContext,
}

impl AppWorkerContext {
    #[inline]
    pub fn owner_worker(&self) -> usize {
        self.inner.owner_worker()
    }

    #[inline]
    pub fn backend(&self) -> AppBackend {
        AppBackend::from_inner(self.inner.backend())
    }

    #[inline]
    pub fn runtime(&self) -> AppRuntime {
        AppRuntime::from_inner(self.inner.runtime())
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
    pub(crate) fn from_inner(inner: hammer_runtime::app::AppWorkerContext) -> Self {
        Self { inner }
    }
}

pub type AppTaskContext = AppWorkerContext;
pub type AppWorkerLocalExecutor = ();

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

#[derive(Clone)]
pub struct App {
    inner: AppContext,
}

impl App {
    #[inline]
    pub fn new(data_context: DataRuntimeContext) -> Self {
        Self::with_ring_capacity(data_context, 256)
    }

    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            inner: AppContext::with_ring_capacity(data_context, ring_capacity),
        }
    }

    #[inline]
    pub fn context(&self) -> &AppContext {
        &self.inner
    }

    #[inline]
    pub fn install_control(&self, control: AppControl) -> HammerResult<()> {
        self.inner.install_control(control)
    }

    #[inline]
    pub fn flow(&self, flow: AppFlowId) -> AppFlow {
        AppFlow::new(self.clone(), flow)
    }

    pub async fn spawn<F, Fut, T>(&self, flow: AppFlowId, f: F) -> HammerResult<T>
    where
        F: FnOnce(AppFlow) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let app = self.clone();
        self.inner
            .spawn_on_flow(flow, move |worker| {
                let flow = AppFlowRuntime {
                    app,
                    flow,
                    worker: Some(worker),
                };
                f(AppFlow { inner: flow })
            })
            .await
    }
}

#[derive(Clone)]
pub struct AppFlow {
    inner: AppFlowRuntime,
}

impl AppFlow {
    #[inline]
    pub fn new(app: App, flow: AppFlowId) -> Self {
        Self {
            inner: AppFlowRuntime::pending(app, flow),
        }
    }

    #[inline]
    pub fn id(&self) -> AppFlowId {
        self.inner.flow
    }

    #[inline]
    pub fn app(&self) -> &App {
        &self.inner.app
    }

    pub async fn owner(&self) -> HammerResult<usize> {
        self.run(|flow| async move { flow.owner_worker() }).await
    }

    pub async fn run<F, Fut, T>(&self, f: F) -> HammerResult<T>
    where
        F: FnOnce(AppFlow) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let flow = self.id();
        let app = self.app().clone();
        app.spawn(flow, f).await
    }

    #[inline]
    pub fn owner_worker(&self) -> usize {
        self.inner
            .worker
            .as_ref()
            .expect("app flow owner_worker requires runtime context")
            .owner_worker()
    }

    #[inline]
    pub fn backend(&self) -> AppBackend {
        self.inner
            .worker
            .as_ref()
            .expect("app flow backend requires runtime context")
            .backend()
    }

    #[inline]
    pub fn runtime(&self) -> AppRuntime {
        self.inner
            .worker
            .as_ref()
            .expect("app flow runtime requires runtime context")
            .runtime()
    }

    #[inline]
    pub fn ring(&self) -> AppRing {
        AppRing::new(self.runtime())
    }

    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        self.runtime().recv()
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.runtime().send(send).await
    }

    #[inline]
    pub fn spawn_local<F, Fut, T>(&self, factory: F) -> AppLocalJoinHandle<T>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.inner
            .worker
            .as_ref()
            .expect("app flow spawn_local requires runtime context")
            .spawn_local(factory)
    }
}

#[derive(Clone)]
struct AppFlowRuntime {
    app: App,
    flow: AppFlowId,
    worker: Option<AppWorkerContext>,
}

impl AppFlowRuntime {
    #[inline]
    fn pending(app: App, flow: AppFlowId) -> Self {
        Self {
            app,
            flow,
            worker: None,
        }
    }
}

impl From<AppContext> for App {
    #[inline]
    fn from(inner: AppContext) -> Self {
        Self { inner }
    }
}

impl TryFrom<AppFlow> for AppWorkerContext {
    type Error = HammerError;

    fn try_from(flow: AppFlow) -> Result<Self, Self::Error> {
        flow.inner
            .worker
            .ok_or_else(|| HammerError::internal("app flow requires runtime context"))
    }
}
