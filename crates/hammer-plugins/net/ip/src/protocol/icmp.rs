use std::num::NonZeroU64;

use hammer_core::data_plane::SecondaryOpaque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IcmpErrorFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[repr(transparent)]
pub struct IcmpErrorMetadata(NonZeroU64);

impl IcmpErrorMetadata {
    #[inline]
    fn new(family: IcmpErrorFamily, icmp_type: u8, code: u8, data: u32) -> Self {
        let family = match family {
            IcmpErrorFamily::Ipv4 => 4u64,
            IcmpErrorFamily::Ipv6 => 6u64,
        };
        let packed = (1u64 << 63)
            | (family << 48)
            | ((icmp_type as u64) << 40)
            | ((code as u64) << 32)
            | data as u64;
        Self(NonZeroU64::new(packed).expect("ICMP error metadata is presence-tagged"))
    }

    #[inline]
    pub fn ipv4_time_exceeded() -> Self {
        Self::new(IcmpErrorFamily::Ipv4, 11, 0, 0)
    }

    #[inline]
    pub fn ipv4_destination_unreachable(code: u8, data: u32) -> Self {
        Self::new(IcmpErrorFamily::Ipv4, 3, code, data)
    }

    #[inline]
    pub fn ipv6_time_exceeded() -> Self {
        Self::new(IcmpErrorFamily::Ipv6, 3, 0, 0)
    }

    #[inline]
    pub fn ipv6_packet_too_big(mtu: u32) -> Self {
        Self::new(IcmpErrorFamily::Ipv6, 2, 0, mtu)
    }

    #[inline]
    pub fn ipv6_port_unreachable() -> Self {
        Self::new(IcmpErrorFamily::Ipv6, 1, 4, 0)
    }

    #[inline]
    pub const fn family(self) -> IcmpErrorFamily {
        match (self.0.get() >> 48) & 0xff {
            4 => IcmpErrorFamily::Ipv4,
            6 => IcmpErrorFamily::Ipv6,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub const fn icmp_type(self) -> u8 {
        ((self.0.get() >> 40) & 0xff) as u8
    }

    #[inline]
    pub const fn code(self) -> u8 {
        ((self.0.get() >> 32) & 0xff) as u8
    }

    #[inline]
    pub const fn data(self) -> u32 {
        self.0.get() as u32
    }
}

impl IcmpErrorMetadata {
    /// Stores the error request in the final secondary-opaque word. The error
    /// producer writes it immediately before dispatch; lookup owns earlier words.
    pub fn write(self, opaque: &mut SecondaryOpaque) {
        // SAFETY: SecondaryOpaque is an aligned 56-byte union of integer arrays.
        let words = unsafe { &mut *(opaque as *mut SecondaryOpaque).cast::<[u64; 7]>() };
        words[6] = self.0.get();
    }

    pub fn read(opaque: &SecondaryOpaque) -> Option<Self> {
        // SAFETY: every bit pattern is valid for the integer-array representation.
        let words = unsafe { &*(opaque as *const SecondaryOpaque).cast::<[u64; 7]>() };
        let value = words[6];
        if value >> 63 == 0 || !matches!((value >> 48) & 0xff, 4 | 6) {
            return None;
        }
        NonZeroU64::new(value).map(Self)
    }

    pub fn clear(opaque: &mut SecondaryOpaque) {
        // SAFETY: the same integer-array representation as write; no borrow escapes.
        let words = unsafe { &mut *(opaque as *mut SecondaryOpaque).cast::<[u64; 7]>() };
        words[6] = 0;
    }
}
