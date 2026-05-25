use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

#[cfg(feature = "amneziawg")]
use boringtun::noise::AmneziaConfig;
use boringtun::noise::Tunn;
use boringtun::x25519;
use ipnet::IpNet;

use crate::config::WireguardPeerOptions;

/// One WireGuard peer — owns the boringtun `Tunn` state machine plus the
/// metadata Hammer needs around it (resolved UDP endpoint, allowed_ips for
/// outbound LPM routing, the WARP-style 3-byte reserved prefix).
///
/// `Tunn` is interior-mutable but not `Sync`, so we lock it ourselves. boringtun
/// expects single-threaded access per peer; the lock keeps multiple async tasks
/// (rx loop, tx loop, timer tick) from clobbering session state.
pub struct Peer {
    tunn: Mutex<Tunn>,
    public_key: [u8; 32],
    allowed_ips: Vec<IpNet>,
    endpoint: SocketAddr,
    reserved: [u8; 3],
}

impl Peer {
    pub fn new(
        opts: WireguardPeerOptions,
        local_private: &x25519::StaticSecret,
        index: u32,
        #[cfg(feature = "amneziawg")] amnezia: Option<AmneziaConfig>,
    ) -> Self {
        let peer_public_key = opts.public_key;
        let public_key = x25519::PublicKey::from(peer_public_key);
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
        Self {
            tunn: Mutex::new(tunn),
            public_key: peer_public_key,
            allowed_ips: opts.allowed_ips,
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

    pub fn lock_tunn(&self) -> std::sync::MutexGuard<'_, Tunn> {
        self.tunn.lock().expect("wireguard peer Tunn poisoned")
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
