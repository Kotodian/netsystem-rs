//! Per-route Path MTU cache owned on the IP side (not TCP-private storage).
//!
//! VPP ownership (`vnet/ip/ip_path_mtu.{h,c}`):
//! - Control-plane tracker `ip_pmtu_t` / `ip_path_mtu_update()` writes path MTU.
//! - Attached neighbors clamp adjacency via `adj_nbr_set_mtu` →
//!   `rewrite_header.max_l3_packet_bytes`.
//! - Egress enforces MTU in `ip4_mtu_check` at rewrite (DF → Frag-Needed).
//! - Core VPP TCP does **not** yet consume ICMP for PMTU (`TODO consider PMTU
//!   discovery`); Hammer extends that gap by feeding Frag-Needed into this
//!   same IP-owned update path, then TCP reads the cache to clamp MSS.
//!
//! Do not store per-destination MTU inside `TcpConnection`.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_infra::bihash::Bihash;

/// IPv4 absolute minimum MTU (RFC 791).
pub const IPV4_MIN_PATH_MTU: u16 = 68;

/// Base IPv4(20) + TCP(20) overhead used when converting path MTU → MSS.
pub const IPV4_TCP_BASE_OVERHEAD: u16 = 40;

/// Published IP-owned path MTU cache (VPP `ip_pmtu_db` stand-in).
pub static PATH_MTU_CACHE: ArcSwapOption<PathMtuCache> = ArcSwapOption::const_empty();

/// Per-destination Path MTU cache (Hammer stand-in for VPP `ip_pmtu_db`).
pub struct PathMtuCache {
    v4: Bihash<u32, 7>,
}

impl Default for PathMtuCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PathMtuCache {
    pub fn new() -> Self {
        Self {
            v4: Bihash::new(64),
        }
    }

    /// VPP `ip_path_mtu_update` — shared by control plane and Frag-Needed ingress.
    /// Smaller reported MTUs win; larger reports do not raise an existing entry.
    pub fn path_mtu_update(&self, destination: Ipv4Addr, pmtu: u16) {
        let mtu = pmtu.max(IPV4_MIN_PATH_MTU);
        let key = u32::from(destination);
        match self.v4.lookup(&key) {
            Some(existing) if mtu >= existing as u16 => {}
            _ => {
                self.v4.insert(key, u64::from(mtu));
            }
        }
    }

    /// Record an IPv4 Fragmentation Needed next-hop MTU (ICMP code 4).
    #[inline]
    pub fn apply_ipv4_fragmentation_needed(&self, destination: Ipv4Addr, reported_mtu: u16) {
        self.path_mtu_update(destination, reported_mtu);
    }

    pub fn path_mtu(&self, destination: IpAddr) -> Option<u16> {
        match destination {
            IpAddr::V4(v4) => self.v4.lookup(&u32::from(v4)).map(|value| value as u16),
            IpAddr::V6(_) => None,
        }
    }
}

/// Convert an IPv4 path MTU into a TCP MSS using base header overhead.
#[inline]
pub fn ipv4_path_mtu_to_mss(path_mtu: u16) -> u16 {
    path_mtu.saturating_sub(IPV4_TCP_BASE_OVERHEAD).max(1)
}

/// Publish the process-wide path MTU cache (IP init / tests).
pub fn publish_path_mtu_cache(cache: PathMtuCache) {
    PATH_MTU_CACHE.store(Some(Arc::new(cache)));
}

/// Current published path MTU cache, if any.
pub fn path_mtu_cache() -> Option<Arc<PathMtuCache>> {
    PATH_MTU_CACHE.load_full()
}

/// Clear the published path MTU cache (tests / subsystem reset).
pub fn reset_path_mtu_cache_for_test() {
    PATH_MTU_CACHE.store(None);
}

const ICMP4_DEST_UNREACH: u8 = 3;
const ICMP4_FRAG_NEEDED: u8 = 4;
const ICMP_HEADER_LEN: usize = 8;
const IPV4_HEADER_MIN_LEN: usize = 20;

/// Parse an IPv4 ICMP Destination Unreachable / Fragmentation Needed message
/// (payload starting at the ICMP header) and update the path MTU cache.
pub fn apply_ipv4_frag_needed_icmp(cache: &PathMtuCache, icmp: &[u8]) -> Option<(Ipv4Addr, u16)> {
    if icmp.len() < ICMP_HEADER_LEN + IPV4_HEADER_MIN_LEN {
        return None;
    }
    if icmp[0] != ICMP4_DEST_UNREACH || icmp[1] != ICMP4_FRAG_NEEDED {
        return None;
    }
    let next_hop_mtu = u16::from_be_bytes([icmp[6], icmp[7]]);
    if next_hop_mtu == 0 {
        return None;
    }
    let quoted = &icmp[ICMP_HEADER_LEN..];
    if quoted[0] >> 4 != 4 {
        return None;
    }
    let destination = Ipv4Addr::new(quoted[16], quoted[17], quoted[18], quoted[19]);
    cache.apply_ipv4_fragmentation_needed(destination, next_hop_mtu);
    let path_mtu = cache.path_mtu(IpAddr::V4(destination))?;
    Some((destination, path_mtu))
}

/// Process a full IPv4 packet carrying ICMP Dest Unreach / Frag-Needed and
/// update the published path MTU cache (Hammer extension beyond VPP core).
pub fn process_ipv4_icmp_path_mtu_packet(packet: &[u8]) -> Option<(Ipv4Addr, u16)> {
    if packet.len() < IPV4_HEADER_MIN_LEN || packet[0] >> 4 != 4 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < IPV4_HEADER_MIN_LEN || packet.len() < ihl + ICMP_HEADER_LEN {
        return None;
    }
    if packet[9] != 1 {
        return None;
    }
    let cache = path_mtu_cache()?;
    apply_ipv4_frag_needed_icmp(cache.as_ref(), &packet[ihl..])
}
