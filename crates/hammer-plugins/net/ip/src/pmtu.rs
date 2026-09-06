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
