use std::cell::RefCell;
use std::future::Future;
use std::net::{Shutdown, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::descriptor::Descriptor;
use hammer_infra::map::FlatHashTable;
use hammer_infra::vec::Vec;

use crate::app::backend::{AppBackend, AppBackendRecvQueue, AppBackendSendQueue};
use crate::app::ring::{
    AppCompletionEntry, AppCqeDescriptor, AppObjectRef, AppRecv, AppRegisteredBuffer, AppSend,
    AppSocketId, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};
use crate::spawn::{DataLocalJoinHandle, DataRuntimeContext, spawn_local};
use hammer_adapter::{BufferIndex, DataPlaneBuffers};

static NEXT_APP_CONTEXT_ID: AtomicUsize = AtomicUsize::new(1);
const CLOSED_OWNER_WORKER: usize = usize::MAX;

thread_local! {
    static APP_FLOW_BACKENDS: RefCell<AppWorkerFlowRegistry> =
        RefCell::new(AppWorkerFlowRegistry::new());
    static APP_SOCKET_BACKENDS: RefCell<AppWorkerSocketRegistry> =
        RefCell::new(AppWorkerSocketRegistry::new());
}

pub enum AppFlowTag {}
pub type AppFlowId = Descriptor<AppFlowTag>;

pub trait AppControlBackend: Send + Sync {
    fn bind_tcp_listener(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId>;

    fn connect_tcp_stream(
        &self,
        _app: &AppContext,
        _peer: SocketAddr,
        _owner_worker: usize,
    ) -> HammerResult<AppFlowId> {
        Err(HammerError::internal(
            "app tcp connect is not implemented by the control backend",
        ))
    }

    fn bind_udp_socket(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId>;

    fn close_socket(&self, app: &AppContext, socket: AppSocketId) -> HammerResult<()>;
}

#[derive(Clone)]
pub struct AppControl {
    backend: Arc<dyn AppControlBackend>,
}

impl AppControl {
    #[inline]
    pub fn new(backend: Arc<dyn AppControlBackend>) -> Self {
        Self { backend }
    }

    #[inline]
    pub fn bind_tcp_listener(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        self.backend.bind_tcp_listener(app, bind, owner_worker)
    }

    #[inline]
    pub fn connect_tcp_stream(
        &self,
        app: &AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppFlowId> {
        self.backend.connect_tcp_stream(app, peer, owner_worker)
    }

    #[inline]
    pub fn bind_udp_socket(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        self.backend.bind_udp_socket(app, bind, owner_worker)
    }

    #[inline]
    pub fn close_socket(&self, app: &AppContext, socket: AppSocketId) -> HammerResult<()> {
        self.backend.close_socket(app, socket)
    }
}

#[derive(Clone)]
pub struct AppContext {
    id: usize,
    data_context: DataRuntimeContext,
    ring_capacity: usize,
    control: Arc<Mutex<Option<AppControl>>>,
    owners: Arc<Mutex<FlatHashTable<u64, usize>>>,
    socket_owners: Arc<Mutex<FlatHashTable<u64, usize>>>,
}

impl AppContext {
    #[inline]
    pub fn with_ring_capacity(data_context: DataRuntimeContext, ring_capacity: usize) -> Self {
        Self {
            id: next_app_context_id(),
            data_context,
            ring_capacity,
            control: Arc::new(Mutex::new(None)),
            owners: Arc::new(Mutex::new(FlatHashTable::new())),
            socket_owners: Arc::new(Mutex::new(FlatHashTable::new())),
        }
    }

    #[inline]
    pub fn install_control(&self, control: AppControl) -> HammerResult<()> {
        let mut slot = self
            .control
            .lock()
            .map_err(|_| HammerError::internal("app control backend poisoned"))?;
        *slot = Some(control);
        Ok(())
    }

    pub fn bind_tcp_listener(
        &self,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        self.validate_owner_worker(owner_worker)?;
        let socket = self
            .control()?
            .bind_tcp_listener(self, bind, owner_worker)?;
        self.register_socket_owner(socket, owner_worker)?;
        Ok(socket)
    }

    pub fn connect_tcp_stream(
        &self,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppFlowId> {
        self.validate_owner_worker(owner_worker)?;
        let flow = self
            .control()?
            .connect_tcp_stream(self, peer, owner_worker)?;
        self.register_flow_owner(flow, owner_worker)?;
        Ok(flow)
    }

    pub fn bind_udp_socket(
        &self,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        self.validate_owner_worker(owner_worker)?;
        let socket = self.control()?.bind_udp_socket(self, bind, owner_worker)?;
        self.register_socket_owner(socket, owner_worker)?;
        Ok(socket)
    }

    #[inline]
    pub fn close_socket(&self, socket: AppSocketId) -> HammerResult<()> {
        self.control()?.close_socket(self, socket)?;
        self.unregister_socket_owner(socket)
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
        self.data_context
            .call_local_on_worker(owner_worker, move || async move {
                let worker = AppWorkerContext {
                    owner_worker,
                    backend: worker_backend(app_context_id, flow, ring_capacity),
                };
                f(worker).await
            })
            .await
    }

    #[inline]
    pub fn owner_worker_for_flow(&self, flow: AppFlowId) -> HammerResult<usize> {
        self.owner_for(flow)
    }

    #[inline]
    pub fn owner_worker_for_socket(&self, socket: AppSocketId) -> HammerResult<usize> {
        self.owner_for_socket(socket)
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
    pub fn current_worker_owns_socket(&self, socket: AppSocketId) -> bool {
        self.data_context
            .current_worker_index()
            .is_some_and(|current| {
                self.owner_for_socket(socket)
                    .is_ok_and(|owner| current == owner)
            })
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
    pub fn local_backend_for_socket(&self, socket: AppSocketId) -> HammerResult<AppBackend> {
        let owner_worker = self.owner_for_socket(socket)?;
        let current_worker = self.current_worker_index()?;
        if current_worker != owner_worker {
            return Err(HammerError::internal(format!(
                "app socket {} is owned by worker {owner_worker}, not worker {current_worker}",
                socket.value()
            )));
        }
        Ok(worker_socket_backend(self.id, socket, self.ring_capacity))
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
    pub fn try_complete_recv_from_buffer(
        &self,
        socket: AppSocketId,
        source: SocketAddr,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
        truncated: bool,
    ) -> HammerResult<()> {
        let backend = self.local_backend_for_socket(socket)?;
        backend
            .ring_handle()
            .try_complete_recv_from_buffer(socket, source, buffers, index, truncated)
    }

    #[inline]
    pub fn try_complete_accept(&self, listener: AppSocketId, flow: AppFlowId) -> HammerResult<()> {
        let owner_worker = self.owner_for_socket(listener)?;
        self.register_flow_owner(flow, owner_worker)?;
        if self.current_worker_index().ok() == Some(owner_worker) {
            let backend = self.local_backend_for_socket(listener)?;
            return backend.ring_handle().try_complete_accept(listener, flow);
        }
        let app_context_id = self.id;
        let ring_capacity = self.ring_capacity;
        self.data_context
            .call_blocking_on_worker(owner_worker, move || {
                worker_socket_backend(app_context_id, listener, ring_capacity)
                    .ring_handle()
                    .try_complete_accept(listener, flow)
            })
    }

    #[inline]
    fn current_worker_index(&self) -> HammerResult<usize> {
        self.data_context.current_worker_index().ok_or_else(|| {
            HammerError::internal("local app backend lookup requires a data worker thread")
        })
    }

    fn control(&self) -> HammerResult<AppControl> {
        self.control
            .lock()
            .map_err(|_| HammerError::internal("app control backend poisoned"))?
            .clone()
            .ok_or_else(|| HammerError::internal("app control backend is not installed"))
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

    fn register_socket_owner(&self, socket: AppSocketId, owner_worker: usize) -> HammerResult<()> {
        self.validate_owner_worker(owner_worker)?;
        let mut owners = self
            .socket_owners
            .lock()
            .map_err(|_| HammerError::internal("app socket owner map poisoned"))?;
        owners.insert(socket.value(), owner_worker);
        Ok(())
    }

    fn unregister_socket_owner(&self, socket: AppSocketId) -> HammerResult<()> {
        let mut owners = self
            .socket_owners
            .lock()
            .map_err(|_| HammerError::internal("app socket owner map poisoned"))?;
        owners.insert(socket.value(), CLOSED_OWNER_WORKER);
        Ok(())
    }

    fn register_flow_owner(&self, flow: AppFlowId, owner_worker: usize) -> HammerResult<()> {
        self.validate_owner_worker(owner_worker)?;
        let mut owners = self
            .owners
            .lock()
            .map_err(|_| HammerError::internal("app owner map poisoned"))?;
        owners.insert(flow.value(), owner_worker);
        Ok(())
    }

    fn owner_for_socket(&self, socket: AppSocketId) -> HammerResult<usize> {
        self.socket_owners
            .lock()
            .map_err(|_| HammerError::internal("app socket owner map poisoned"))?
            .lookup(&socket.value())
            .filter(|owner| *owner != CLOSED_OWNER_WORKER)
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "app socket {} owner is not registered",
                    socket.value()
                ))
            })
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
    pub async fn shutdown(&self, how: Shutdown) -> HammerResult<()> {
        self.send
            .ring()
            .try_push_tcp_shutdown(crate::app::ring::AppTcpShutdown::new(self.flow, how))
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
    pub fn take_submission_buffer(&self, index: BufferIndex) -> HammerResult<AppSend> {
        self.send.ring().take_send_buffer(index)
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, cqe: AppCqeDescriptor) -> HammerResult<()> {
        self.recv.ring().try_push_completion_descriptor(cqe)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.recv.ring().try_push_completion_entry(entry)
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.recv.ring().next_completion_entry().await
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.recv.ring().next_completion_descriptor().await
    }

    #[inline]
    pub fn take_completion_buffer(&self, index: BufferIndex) -> HammerResult<AppRecv> {
        self.recv.ring().take_recv_buffer(index)
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

#[inline]
fn worker_socket_backend(
    app_context_id: usize,
    socket: AppSocketId,
    ring_capacity: usize,
) -> AppBackend {
    APP_SOCKET_BACKENDS.with(|slot| {
        slot.borrow_mut()
            .get_or_insert(app_socket_key(app_context_id, socket), ring_capacity)
    })
}

#[inline]
fn app_socket_key(app_context_id: usize, socket: AppSocketId) -> u128 {
    (1u128 << 127) | ((app_context_id as u128) << 64) | u128::from(socket.value())
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

struct AppWorkerSocketRegistry {
    index_by_socket: FlatHashTable<u128, usize>,
    backends: Vec<AppBackend>,
}

impl AppWorkerSocketRegistry {
    #[inline]
    fn new() -> Self {
        Self {
            index_by_socket: FlatHashTable::new(),
            backends: Vec::new(),
        }
    }

    #[inline]
    fn get_or_insert(&mut self, key: u128, ring_capacity: usize) -> AppBackend {
        if let Some(index) = self.index_by_socket.lookup(&key)
            && let Some(backend) = self.backends.get(index).cloned()
        {
            return backend;
        }

        let index = self.backends.len();
        let backend = AppBackend::new(ring_capacity);
        self.backends.push(backend.clone());
        self.index_by_socket.insert(key, index);
        backend
    }
}
