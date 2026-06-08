use std::net::SocketAddr;

use hammer_core::error::HammerResult;

use crate::ring::{AppBufferLease, AppRing, AppSend};

#[derive(Clone)]
pub struct UdpSocket {
    ring: AppRing,
    peer: SocketAddr,
}

impl UdpSocket {
    #[inline]
    pub fn new(ring: AppRing, peer: SocketAddr) -> Self {
        Self { ring, peer }
    }

    #[inline]
    pub fn ring(&self) -> &AppRing {
        &self.ring
    }

    #[inline]
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    #[inline]
    pub async fn recv_from_buffer(&self) -> HammerResult<(AppBufferLease, SocketAddr)> {
        let recv = self.ring.recv().await?;
        Ok((recv.into_send().into_lease(), self.peer))
    }

    #[inline]
    pub async fn send_buffer_to(
        &self,
        lease: AppBufferLease,
        peer: SocketAddr,
    ) -> HammerResult<()> {
        debug_assert_eq!(peer, self.peer);
        self.ring.send(AppSend::new(lease)).await
    }
}
