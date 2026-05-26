use std::net::{IpAddr, SocketAddr};

#[cfg(feature = "amneziawg")]
use boringtun::noise::AmneziaConfig;
use boringtun::noise::{Tunn, TunnResult};
use boringtun::x25519;
use ipnet::IpNet;

use crate::config::WireguardPeerOptions;

/// WireGuard peer metadata used by Hammer outside the mutable tunnel state.
pub struct Peer {
    public_key: [u8; 32],
    allowed_ips: Vec<IpNet>,
    endpoint: SocketAddr,
    reserved: [u8; 3],
}

/// Mutable boringtun state for one peer.
///
/// This type is intentionally separate from [`Peer`]. Runtime code should keep
/// it owned by one transport actor instead of sharing it across threads.
pub struct PeerTunnel {
    tunn: Tunn,
}

impl Peer {
    pub fn from_options(opts: &WireguardPeerOptions) -> Self {
        Self {
            public_key: opts.public_key,
            allowed_ips: opts.allowed_ips.clone(),
            endpoint: opts.endpoint,
            reserved: opts.reserved,
        }
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.public_key
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub fn reserved(&self) -> [u8; 3] {
        self.reserved
    }

    pub fn allowed_ips(&self) -> &[IpNet] {
        &self.allowed_ips
    }

    /// Longest-prefix match against this peer's `allowed_ips`. Returns the
    /// matching prefix length so the caller can pick the most specific peer
    /// when multiple have overlapping ranges.
    pub fn match_prefix(&self, dst: IpAddr) -> Option<u8> {
        self.allowed_ips
            .iter()
            .filter(|net| net.contains(&dst))
            .map(|net| net.prefix_len())
            .max()
    }
}

impl PeerTunnel {
    pub fn new(
        opts: &WireguardPeerOptions,
        local_private: &x25519::StaticSecret,
        index: u32,
        #[cfg(feature = "amneziawg")] amnezia: Option<AmneziaConfig>,
    ) -> Self {
        let public_key = x25519::PublicKey::from(opts.public_key);
        // boringtun stores the keepalive interval as `u16` seconds; clamp the
        // configured Duration into that window. None disables keepalive.
        let keepalive = opts
            .persistent_keepalive
            .map(|d| d.as_secs().min(u16::MAX as u64) as u16);
        let tunn = Tunn::new(
            local_private.clone(),
            public_key,
            opts.pre_shared_key,
            keepalive,
            index,
            None, // rate_limiter: None lets boringtun build a default per-peer one
            #[cfg(feature = "amneziawg")]
            amnezia,
        );
        Self { tunn }
    }

    pub fn encapsulate<'a>(&mut self, src: &[u8], dst: &'a mut [u8]) -> TunnResult<'a> {
        self.tunn.encapsulate(src, dst)
    }

    pub fn decapsulate<'a>(
        &mut self,
        src_addr: Option<IpAddr>,
        datagram: &[u8],
        dst: &'a mut [u8],
    ) -> TunnResult<'a> {
        self.tunn.decapsulate(src_addr, datagram, dst)
    }

    pub fn update_timers<'a>(&mut self, dst: &'a mut [u8]) -> TunnResult<'a> {
        self.tunn.update_timers(dst)
    }
}

/// Pick the peer that has the longest matching `allowed_ips` prefix for `dst`.
/// Returns the peer's index in the input slice — `None` when nothing matches,
/// which the caller should surface as "no route" (drop the packet).
pub fn route_outbound(peers: &[Peer], dst: IpAddr) -> Option<usize> {
    peers
        .iter()
        .enumerate()
        .filter_map(|(idx, peer)| peer.match_prefix(dst).map(|len| (idx, len)))
        .max_by_key(|(_, len)| *len)
        .map(|(idx, _)| idx)
}
