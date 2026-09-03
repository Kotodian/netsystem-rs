use crate::protocol::ip::IpVersion;

pub use hammer_service::net::{DpoId, DpoProto, DpoType};

impl From<IpVersion> for DpoProto {
    #[inline(always)]
    fn from(value: IpVersion) -> Self {
        match value {
            IpVersion::V4 => Self::IP4,
            IpVersion::V6 => Self::IP6,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdjacencyIndex(u32);

impl AdjacencyIndex {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyRewriteError {
    TooLarge { len: usize, max: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyRewrite {
    len: u8,
    bytes: [u8; Self::MAX_LEN],
}

impl AdjacencyRewrite {
    pub const MAX_LEN: usize = 64;

    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            len: 0,
            bytes: [0; Self::MAX_LEN],
        }
    }

    #[inline]
    pub fn try_new(bytes: &[u8]) -> Result<Self, AdjacencyRewriteError> {
        if bytes.len() > Self::MAX_LEN {
            return Err(AdjacencyRewriteError::TooLarge {
                len: bytes.len(),
                max: Self::MAX_LEN,
            });
        }

        let mut rewrite = Self::empty();
        rewrite.bytes[..bytes.len()].copy_from_slice(bytes);
        rewrite.len = bytes.len() as u8;
        Ok(rewrite)
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for AdjacencyRewrite {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency {
    pub next: u16,
    pub proto: DpoProto,
    pub egress_interface: Option<u32>,
    pub rewrite: AdjacencyRewrite,
    /// Operational L3 MTU in bytes — VPP `rewrite_header.max_l3_packet_bytes`.
    pub max_l3_packet_bytes: u16,
}

/// Default Ethernet L3 MTU used when interface MTU has not been applied yet.
pub const DEFAULT_ADJACENCY_L3_MTU: u16 = 1_500;

impl Adjacency {
    /// VPP `adj_nbr_set_mtu`: `path_mtu == 0` restores `link_mtu`; otherwise
    /// clamps to `min(link_mtu, path_mtu)`.
    #[inline]
    pub fn set_path_mtu(&mut self, link_mtu: u16, path_mtu: u16) {
        if path_mtu == 0 {
            self.max_l3_packet_bytes = link_mtu;
        } else {
            self.max_l3_packet_bytes = link_mtu.min(path_mtu);
        }
    }
}
