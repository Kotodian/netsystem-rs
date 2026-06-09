use std::net::SocketAddr;

use hammer_core::error::{HammerError, HammerResult};

use crate::{
    App, AppBufferLease, AppContext, AppCqeData, AppObjectRef, AppOpcode, AppRegisteredBuffer,
    AppRing, AppSocketId, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};

fn missing_completion(op: &str) -> HammerError {
    HammerError::internal(format!("app ring completion missing for {op}"))
}

#[derive(Clone)]
enum UdpSocketInner {
    Local { ring: AppRing },
    Context { app: AppContext },
}

#[derive(Clone)]
pub struct UdpSocket {
    inner: UdpSocketInner,
    socket: AppSocketId,
}

impl UdpSocket {
    #[inline]
    pub fn bind(app: &App, bind: SocketAddr, owner_worker: usize) -> HammerResult<Self> {
        let socket = app.context().bind_udp_socket(bind, owner_worker)?;
        Ok(Self {
            inner: UdpSocketInner::Context {
                app: app.context().clone(),
            },
            socket,
        })
    }

    #[inline]
    pub fn new(ring: AppRing, socket: AppSocketId) -> Self {
        Self {
            inner: UdpSocketInner::Local { ring },
            socket,
        }
    }

    #[inline]
    pub fn ring(&self) -> &AppRing {
        match &self.inner {
            UdpSocketInner::Local { ring } => ring,
            UdpSocketInner::Context { .. } => {
                panic!("udp socket ring handle requires a local in-worker socket")
            }
        }
    }

    #[inline]
    pub fn socket(&self) -> AppSocketId {
        self.socket
    }

    #[inline]
    pub fn recv_from_descriptor(&self, user_data: AppUserData, max_len: u32) -> AppSqeDescriptor {
        AppSqeDescriptor::new(
            AppOpcode::RecvFrom,
            user_data,
            AppObjectRef::Socket(self.socket),
            AppSqeData::RecvFrom { max_len },
        )
    }

    #[inline]
    pub fn send_to_entry(
        &self,
        user_data: AppUserData,
        target: SocketAddr,
        lease: AppBufferLease,
    ) -> HammerResult<AppSubmissionEntry> {
        let registered = AppRegisteredBuffer::from_lease(lease)?;
        Ok(AppSubmissionEntry::with_attachment(
            AppSqeDescriptor::new(
                AppOpcode::SendTo,
                user_data,
                AppObjectRef::Socket(self.socket),
                AppSqeData::SendTo {
                    buffer: registered.index(),
                    target,
                },
            ),
            registered,
        ))
    }

    pub async fn recv_from_buffer(&self) -> HammerResult<(AppBufferLease, SocketAddr)> {
        match &self.inner {
            UdpSocketInner::Local { ring } => {
                ring.try_push_submission_descriptor(
                    self.recv_from_descriptor(AppUserData::new(0), u32::MAX),
                )?;
                let completion = ring
                    .next_completion_descriptor()
                    .await
                    .ok_or_else(|| missing_completion("udp recv_from"))?;
                recv_from_buffer_from_completion(self.socket, completion.payload(), || {
                    ring.take_completion_buffer(match completion.payload() {
                        AppCqeData::RecvFrom { buffer, .. } => buffer,
                        _ => unreachable!(),
                    })
                })
            }
            UdpSocketInner::Context { app } => {
                let backend = app.local_backend_for_socket(self.socket)?;
                backend.try_push_sqe_descriptor(
                    self.recv_from_descriptor(AppUserData::new(0), u32::MAX),
                )?;
                let completion = backend
                    .next_cqe_descriptor()
                    .await
                    .ok_or_else(|| missing_completion("udp recv_from"))?;
                recv_from_buffer_from_completion(self.socket, completion.payload(), || {
                    backend.take_completion_buffer(match completion.payload() {
                        AppCqeData::RecvFrom { buffer, .. } => buffer,
                        _ => unreachable!(),
                    })
                })
            }
        }
    }

    pub async fn send_buffer_to(
        &self,
        lease: AppBufferLease,
        peer: SocketAddr,
    ) -> HammerResult<()> {
        match &self.inner {
            UdpSocketInner::Local { ring } => ring.try_push_submission_entry(self.send_to_entry(
                AppUserData::new(0),
                peer,
                lease,
            )?),
            UdpSocketInner::Context { app } => {
                let backend = app.local_backend_for_socket(self.socket)?;
                backend.try_push_submission_entry(self.send_to_entry(
                    AppUserData::new(0),
                    peer,
                    lease,
                )?)
            }
        }
    }

    #[inline]
    pub async fn close(self) -> HammerResult<()> {
        match &self.inner {
            UdpSocketInner::Local { ring } => {
                ring.try_push_submission_descriptor(AppSqeDescriptor::new(
                    AppOpcode::Close,
                    AppUserData::new(0),
                    AppObjectRef::Socket(self.socket),
                    AppSqeData::Close,
                ))
            }
            UdpSocketInner::Context { app } => app.close_socket(self.socket),
        }
    }
}

fn recv_from_buffer_from_completion(
    socket: AppSocketId,
    payload: AppCqeData,
    take: impl FnOnce() -> HammerResult<crate::AppRecv>,
) -> HammerResult<(AppBufferLease, SocketAddr)> {
    let AppCqeData::RecvFrom {
        socket: recv_socket,
        source,
        buffer: _,
    } = payload
    else {
        return Err(HammerError::internal(format!(
            "expected udp recv_from cqe, got {payload:?}"
        )));
    };
    if recv_socket != socket {
        return Err(HammerError::internal(format!(
            "udp recv_from cqe socket {} did not match socket {}",
            recv_socket.value(),
            socket.value()
        )));
    }
    Ok((take()?.into_lease(), source))
}
