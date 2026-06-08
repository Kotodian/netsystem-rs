use hammer_core::error::HammerResult;

use crate::AppRecvFuture;
use crate::ring::{AppBufferLease, AppRing, AppSend};

#[derive(Clone)]
pub struct TcpStream {
    ring: AppRing,
}

impl TcpStream {
    #[inline]
    pub fn new(ring: AppRing) -> Self {
        Self { ring }
    }

    #[inline]
    pub fn ring(&self) -> &AppRing {
        &self.ring
    }

    #[inline]
    pub fn recv(&self) -> AppRecvFuture {
        self.ring.recv()
    }

    #[inline]
    pub async fn recv_buffer(&self) -> HammerResult<AppBufferLease> {
        Ok(self.recv().await?.into_send().into_lease())
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.ring.send(send).await
    }

    #[inline]
    pub async fn send_buffer(&self, lease: AppBufferLease) -> HammerResult<()> {
        self.send(AppSend::new(lease)).await
    }
}
