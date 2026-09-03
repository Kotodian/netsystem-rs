use std::net::{IpAddr, Ipv4Addr};
use std::sync::OnceLock;

use hammer_infra::bihash::Bihash;

pub const IPV4_MIN_PATH_MTU: u16 = 68;

pub struct IpPathMtu {
    v4: Bihash<u32, 7>,
}

impl IpPathMtu {
    pub fn new() -> Self {
        Self {
            v4: Bihash::new(64),
        }
    }

    pub fn update_ipv4(&self, destination: Ipv4Addr, pmtu: u16) {
        let mtu = pmtu.max(IPV4_MIN_PATH_MTU);
        let key = u32::from(destination);
        match self.v4.lookup(&key) {
            Some(existing) if mtu >= existing as u16 => {}
            _ => self.v4.insert(key, u64::from(mtu)),
        }
    }

    pub fn path_mtu(&self, destination: IpAddr) -> Option<u16> {
        match destination {
            IpAddr::V4(v4) => self.v4.lookup(&u32::from(v4)).map(|value| value as u16),
            IpAddr::V6(_) => None,
        }
    }
}

static IP_PATH_MTU: OnceLock<IpPathMtu> = OnceLock::new();

pub fn init_path_mtu() -> &'static IpPathMtu {
    IP_PATH_MTU.get_or_init(IpPathMtu::new)
}

pub fn path_mtu() -> Option<&'static IpPathMtu> {
    IP_PATH_MTU.get()
}

pub fn apply_ipv4_frag_needed_icmp(cache: &IpPathMtu, icmp: &[u8]) -> Option<(Ipv4Addr, u16)> {
    if icmp.len() < 28 || icmp[0] != 3 || icmp[1] != 4 {
        return None;
    }
    let mtu = u16::from_be_bytes([icmp[6], icmp[7]]);
    if mtu == 0 || icmp[8] >> 4 != 4 {
        return None;
    }
    let destination = Ipv4Addr::new(icmp[24], icmp[25], icmp[26], icmp[27]);
    cache.update_ipv4(destination, mtu);
    cache
        .path_mtu(IpAddr::V4(destination))
        .map(|value| (destination, value))
}

pub fn process_ipv4_icmp_path_mtu_packet(packet: &[u8]) -> Option<(Ipv4Addr, u16)> {
    if packet.len() < 28 || packet[0] >> 4 != 4 || packet[9] != 1 {
        return None;
    }
    let header_len = usize::from(packet[0] & 0x0f) * 4;
    let cache = path_mtu()?;
    apply_ipv4_frag_needed_icmp(cache, packet.get(header_len..)?)
}
