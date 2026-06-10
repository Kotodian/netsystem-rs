use std::net::{Shutdown, SocketAddr};

use hammer_core::error::{HammerError, HammerResult};

use crate::{
    App, AppBufferLease, AppContext, AppCqeData, AppFlowId, AppObjectRef, AppOpcode,
    AppRegisteredBuffer, AppRing, AppSend, AppSocketId, AppSqeData, AppSqeDescriptor,
    AppSubmissionEntry, AppUserData,
};

fn missing_completion(op: &str) -> HammerError {
    HammerError::internal(format!("app ring completion missing for {op}"))
}

#[derive(Clone)]
pub struct TcpListener {
    app: AppContext,
    listener: AppSocketId,
}

impl TcpListener {
    #[inline]
    pub fn bind(app: &App, bind: SocketAddr, owner_worker: usize) -> HammerResult<Self> {
        let listener = app.context().bind_tcp_listener(bind, owner_worker)?;
        Ok(Self {
            app: app.context().clone(),
            listener,
        })
    }

    #[inline]
    pub fn new(app: AppContext, listener: AppSocketId) -> Self {
        Self { app, listener }
    }

    #[inline]
    pub fn app(&self) -> &AppContext {
        &self.app
    }

    #[inline]
    pub fn listener(&self) -> AppSocketId {
        self.listener
    }

    #[inline]
    pub fn accept_descriptor(&self, user_data: AppUserData) -> AppSqeDescriptor {
        AppSqeDescriptor::new(
            AppOpcode::Accept,
            user_data,
            AppObjectRef::Socket(self.listener),
            AppSqeData::Accept,
        )
    }

    pub async fn accept(&self) -> HammerResult<TcpStream> {
        let backend = self.app.local_backend_for_socket(self.listener)?;
        backend.try_push_sqe_descriptor(self.accept_descriptor(AppUserData::new(0)))?;
        let completion = backend
            .next_cqe_descriptor()
            .await
            .ok_or_else(|| missing_completion("tcp accept"))?;
        let AppCqeData::Accepted { listener, flow } = completion.payload() else {
            return Err(HammerError::internal(format!(
                "expected tcp accept cqe, got {:?}",
                completion.payload()
            )));
        };
        if listener != self.listener {
            return Err(HammerError::internal(format!(
                "tcp accept cqe listener {} did not match listener {}",
                listener.value(),
                self.listener.value()
            )));
        }
        Ok(TcpStream::from_context(self.app.clone(), flow))
    }

    #[inline]
    pub async fn close(self) -> HammerResult<()> {
        self.app.close_socket(self.listener)
    }
}

#[derive(Clone)]
enum TcpStreamInner {
    Local { ring: AppRing },
    Context { app: AppContext },
}

#[derive(Clone)]
pub struct TcpStream {
    inner: TcpStreamInner,
    flow: AppFlowId,
}

impl TcpStream {
    #[inline]
    pub fn connect(app: &App, peer: SocketAddr, owner_worker: usize) -> HammerResult<Self> {
        let flow = app.context().inner.connect_tcp_stream(peer, owner_worker)?;
        Ok(Self::from_context(
            app.context().clone(),
            AppFlowId::new(flow.value()),
        ))
    }

    #[inline]
    pub fn new(ring: AppRing, flow: AppFlowId) -> Self {
        Self {
            inner: TcpStreamInner::Local { ring },
            flow,
        }
    }

    #[inline]
    pub(crate) fn from_context(app: AppContext, flow: AppFlowId) -> Self {
        Self {
            inner: TcpStreamInner::Context { app },
            flow,
        }
    }

    #[inline]
    pub fn ring(&self) -> &AppRing {
        match &self.inner {
            TcpStreamInner::Local { ring } => ring,
            TcpStreamInner::Context { .. } => {
                panic!("tcp stream ring handle requires a local in-worker stream")
            }
        }
    }

    #[inline]
    pub fn flow(&self) -> AppFlowId {
        self.flow
    }

    #[inline]
    pub fn recv_descriptor(&self, user_data: AppUserData, max_len: u32) -> AppSqeDescriptor {
        AppSqeDescriptor::new(
            AppOpcode::Recv,
            user_data,
            AppObjectRef::Flow(self.flow),
            AppSqeData::Recv { max_len },
        )
    }

    #[inline]
    pub fn send_entry(
        &self,
        user_data: AppUserData,
        lease: AppBufferLease,
    ) -> HammerResult<AppSubmissionEntry> {
        let registered = AppRegisteredBuffer::from_lease(lease)?;
        Ok(AppSubmissionEntry::with_attachment(
            AppSqeDescriptor::new(
                AppOpcode::Send,
                user_data,
                AppObjectRef::Flow(self.flow),
                AppSqeData::Send {
                    buffer: registered.index(),
                },
            ),
            registered,
        ))
    }

    #[inline]
    pub fn close_descriptor(&self, user_data: AppUserData) -> AppSqeDescriptor {
        AppSqeDescriptor::new(
            AppOpcode::Close,
            user_data,
            AppObjectRef::Flow(self.flow),
            AppSqeData::Close,
        )
    }

    pub async fn recv_buffer(&self) -> HammerResult<AppBufferLease> {
        match &self.inner {
            TcpStreamInner::Local { ring } => {
                ring.try_push_submission_descriptor(
                    self.recv_descriptor(AppUserData::new(0), u32::MAX),
                )?;
                let completion = ring
                    .next_completion_descriptor()
                    .await
                    .ok_or_else(|| missing_completion("tcp recv"))?;
                recv_buffer_from_completion(self.flow, completion.payload(), || {
                    ring.take_completion_buffer(match completion.payload() {
                        AppCqeData::Recv { buffer, .. } => buffer,
                        _ => unreachable!(),
                    })
                })
            }
            TcpStreamInner::Context { app } => {
                let backend = app.local_backend_for_flow(self.flow)?;
                backend
                    .try_push_sqe_descriptor(self.recv_descriptor(AppUserData::new(0), u32::MAX))?;
                let completion = backend
                    .next_cqe_descriptor()
                    .await
                    .ok_or_else(|| missing_completion("tcp recv"))?;
                recv_buffer_from_completion(self.flow, completion.payload(), || {
                    backend.take_completion_buffer(match completion.payload() {
                        AppCqeData::Recv { buffer, .. } => buffer,
                        _ => unreachable!(),
                    })
                })
            }
        }
    }

    pub async fn send_buffer(&self, lease: AppBufferLease) -> HammerResult<()> {
        match &self.inner {
            TcpStreamInner::Local { ring } => {
                ring.try_push_submission_entry(self.send_entry(AppUserData::new(0), lease)?)
            }
            TcpStreamInner::Context { app } => {
                app.send_on_flow(self.flow, AppSend::new(lease)).await
            }
        }
    }

    pub async fn shutdown(&self, how: Shutdown) -> HammerResult<()> {
        match &self.inner {
            TcpStreamInner::Local { ring } => ring.runtime().shutdown(how).await,
            TcpStreamInner::Context { app } => {
                app.spawn_on_flow(self.flow, move |worker| async move {
                    worker.runtime().shutdown(how).await
                })
                .await?
            }
        }
    }

    #[inline]
    pub async fn shutdown_read(&self) -> HammerResult<()> {
        self.shutdown(Shutdown::Read).await
    }

    #[inline]
    pub async fn shutdown_write(&self) -> HammerResult<()> {
        self.shutdown(Shutdown::Write).await
    }

    #[inline]
    pub async fn shutdown_both(&self) -> HammerResult<()> {
        self.shutdown(Shutdown::Both).await
    }

    #[inline]
    pub async fn close(self) -> HammerResult<()> {
        match &self.inner {
            TcpStreamInner::Local { ring } => {
                ring.try_push_submission_descriptor(self.close_descriptor(AppUserData::new(0)))
            }
            TcpStreamInner::Context { app } => app.close_tcp_flow(self.flow),
        }
    }
}

fn recv_buffer_from_completion(
    flow: AppFlowId,
    payload: AppCqeData,
    take: impl FnOnce() -> HammerResult<crate::AppRecv>,
) -> HammerResult<AppBufferLease> {
    let recv_flow = match payload {
        AppCqeData::Recv {
            flow: recv_flow, ..
        } => recv_flow,
        AppCqeData::Closed {
            flow: Some(closed_flow),
            ..
        } if closed_flow == flow => {
            return Err(HammerError::internal("tcp stream closed"));
        }
        _ => {
            return Err(HammerError::internal(format!(
                "expected tcp recv cqe, got {payload:?}"
            )));
        }
    };
    if recv_flow != flow {
        return Err(HammerError::internal(format!(
            "tcp recv cqe flow {} did not match stream {}",
            recv_flow.value(),
            flow.value()
        )));
    }
    Ok(take()?.into_lease())
}
