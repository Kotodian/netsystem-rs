use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::data_plane::NodeId;
use hammer_core::forwarding::DpoType;
use hammer_core::protocol::icmp::IcmpErrorFamily;
use hammer_core::protocol::ip::{
    IpFragmentKey, IpInputError, IpInputTarget, IpProtocol, IpVersion,
};
use hammer_infra::vec::Vec;

pub(crate) fn put_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

pub(crate) fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_usize(out: &mut Vec<u8>, value: usize) {
    put_u64(out, value as u64);
}

pub(crate) fn put_node(out: &mut Vec<u8>, value: NodeId) {
    put_u32(out, value.slot());
}

pub(crate) fn put_option_u16(out: &mut Vec<u8>, value: Option<u16>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u16(out, value);
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn put_option_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u32(out, value);
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn put_option_usize(out: &mut Vec<u8>, value: Option<usize>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_usize(out, value);
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn put_option_node(out: &mut Vec<u8>, value: Option<NodeId>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_node(out, value);
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn encode_ip_version(value: IpVersion) -> u8 {
    match value {
        IpVersion::V4 => 4,
        IpVersion::V6 => 6,
    }
}

pub(crate) fn decode_ip_version(value: u8) -> Option<IpVersion> {
    match value {
        4 => Some(IpVersion::V4),
        6 => Some(IpVersion::V6),
        _ => None,
    }
}

pub(crate) fn put_option_ip_version(out: &mut Vec<u8>, value: Option<IpVersion>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u8(out, encode_ip_version(value));
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn encode_ip_protocol(value: IpProtocol) -> (u8, u8) {
    match value {
        IpProtocol::Icmpv4 => (1, 0),
        IpProtocol::Tcp => (6, 0),
        IpProtocol::Udp => (17, 0),
        IpProtocol::Icmpv6 => (58, 0),
        IpProtocol::Other(value) => (255, value),
    }
}

pub(crate) fn decode_ip_protocol(kind: u8, other: u8) -> Option<IpProtocol> {
    match kind {
        1 => Some(IpProtocol::Icmpv4),
        6 => Some(IpProtocol::Tcp),
        17 => Some(IpProtocol::Udp),
        58 => Some(IpProtocol::Icmpv6),
        255 => Some(IpProtocol::Other(other)),
        _ => None,
    }
}

pub(crate) fn put_option_ip_protocol(out: &mut Vec<u8>, value: Option<IpProtocol>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            let (kind, other) = encode_ip_protocol(value);
            put_u8(out, kind);
            put_u8(out, other);
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn encode_ip_input_target(value: IpInputTarget) -> u8 {
    match value {
        IpInputTarget::Drop => 0,
        IpInputTarget::Punt => 1,
        IpInputTarget::Options => 2,
        IpInputTarget::Lookup => 3,
        IpInputTarget::LookupMulticast => 4,
        IpInputTarget::IcmpError => 5,
        IpInputTarget::Reassembly => 6,
    }
}

pub(crate) fn decode_ip_input_target(value: u8) -> Option<IpInputTarget> {
    match value {
        0 => Some(IpInputTarget::Drop),
        1 => Some(IpInputTarget::Punt),
        2 => Some(IpInputTarget::Options),
        3 => Some(IpInputTarget::Lookup),
        4 => Some(IpInputTarget::LookupMulticast),
        5 => Some(IpInputTarget::IcmpError),
        6 => Some(IpInputTarget::Reassembly),
        _ => None,
    }
}

pub(crate) fn put_option_ip_input_target(out: &mut Vec<u8>, value: Option<IpInputTarget>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u8(out, encode_ip_input_target(value));
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn encode_ip_input_error(value: IpInputError) -> u8 {
    value as u8
}

pub(crate) fn decode_ip_input_error(value: u8) -> Option<IpInputError> {
    match value {
        0 => Some(IpInputError::None),
        1 => Some(IpInputError::Version),
        2 => Some(IpInputError::HeaderTooShort),
        3 => Some(IpInputError::Options),
        4 => Some(IpInputError::BadChecksum),
        5 => Some(IpInputError::TimeExpired),
        6 => Some(IpInputError::FragmentOffsetOne),
        7 => Some(IpInputError::TooShort),
        8 => Some(IpInputError::BadLength),
        _ => None,
    }
}

pub(crate) fn put_option_ip_input_error(out: &mut Vec<u8>, value: Option<IpInputError>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u8(out, encode_ip_input_error(value));
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn put_option_dpo_type(out: &mut Vec<u8>, value: Option<DpoType>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u16(out, value.get());
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn encode_icmp_error_family(value: IcmpErrorFamily) -> u8 {
    match value {
        IcmpErrorFamily::Ipv4 => 4,
        IcmpErrorFamily::Ipv6 => 6,
    }
}

pub(crate) fn decode_icmp_error_family(value: u8) -> Option<IcmpErrorFamily> {
    match value {
        4 => Some(IcmpErrorFamily::Ipv4),
        6 => Some(IcmpErrorFamily::Ipv6),
        _ => None,
    }
}

pub(crate) fn put_option_icmp_error_family(out: &mut Vec<u8>, value: Option<IcmpErrorFamily>) {
    match value {
        Some(value) => {
            put_bool(out, true);
            put_u8(out, encode_icmp_error_family(value));
        }
        None => put_bool(out, false),
    }
}

pub(crate) fn put_option_ip_fragment_key(out: &mut Vec<u8>, value: Option<IpFragmentKey>) {
    match value {
        Some(IpFragmentKey::V4 {
            source,
            destination,
            protocol,
            identification,
        }) => {
            put_u8(out, 4);
            out.extend_from_slice(&source.octets());
            out.extend_from_slice(&destination.octets());
            put_u8(out, protocol);
            put_u16(out, identification);
        }
        Some(IpFragmentKey::V6 {
            source,
            destination,
            next_header,
            identification,
        }) => {
            put_u8(out, 6);
            out.extend_from_slice(&source.octets());
            out.extend_from_slice(&destination.octets());
            put_u8(out, next_header);
            put_u32(out, identification);
        }
        None => put_u8(out, 0),
    }
}

pub(crate) struct TraceDecodeCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TraceDecodeCursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    pub(crate) fn read_bool(&mut self) -> Option<bool> {
        match self.read_u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    pub(crate) fn read_u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.position)?;
        self.position += 1;
        Some(value)
    }

    pub(crate) fn read_array<const LEN: usize>(&mut self) -> Option<[u8; LEN]> {
        let end = self.position.checked_add(LEN)?;
        let bytes = self.bytes.get(self.position..end)?;
        self.position = end;
        bytes.try_into().ok()
    }

    pub(crate) fn read_u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.read_array()?))
    }

    pub(crate) fn read_u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.read_array()?))
    }

    pub(crate) fn read_u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.read_array()?))
    }

    pub(crate) fn read_usize(&mut self) -> Option<usize> {
        usize::try_from(self.read_u64()?).ok()
    }

    pub(crate) fn read_node(&mut self) -> Option<NodeId> {
        Some(NodeId::new(self.read_u32()?))
    }

    pub(crate) fn read_option_u16(&mut self) -> Option<Option<u16>> {
        if self.read_bool()? {
            Some(Some(self.read_u16()?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_u32(&mut self) -> Option<Option<u32>> {
        if self.read_bool()? {
            Some(Some(self.read_u32()?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_usize(&mut self) -> Option<Option<usize>> {
        if self.read_bool()? {
            Some(Some(self.read_usize()?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_node(&mut self) -> Option<Option<NodeId>> {
        if self.read_bool()? {
            Some(Some(self.read_node()?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_ip_version(&mut self) -> Option<Option<IpVersion>> {
        if self.read_bool()? {
            Some(Some(decode_ip_version(self.read_u8()?)?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_ip_protocol(&mut self) -> Option<Option<IpProtocol>> {
        if self.read_bool()? {
            let kind = self.read_u8()?;
            let other = self.read_u8()?;
            Some(Some(decode_ip_protocol(kind, other)?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_ip_input_target(&mut self) -> Option<Option<IpInputTarget>> {
        if self.read_bool()? {
            Some(Some(decode_ip_input_target(self.read_u8()?)?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_ip_input_error(&mut self) -> Option<Option<IpInputError>> {
        if self.read_bool()? {
            Some(Some(decode_ip_input_error(self.read_u8()?)?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_dpo_type(&mut self) -> Option<Option<DpoType>> {
        if self.read_bool()? {
            Some(Some(DpoType::new(self.read_u16()?)))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_icmp_error_family(&mut self) -> Option<Option<IcmpErrorFamily>> {
        if self.read_bool()? {
            Some(Some(decode_icmp_error_family(self.read_u8()?)?))
        } else {
            Some(None)
        }
    }

    pub(crate) fn read_option_ip_fragment_key(&mut self) -> Option<Option<IpFragmentKey>> {
        match self.read_u8()? {
            0 => Some(None),
            4 => Some(Some(IpFragmentKey::V4 {
                source: Ipv4Addr::from(self.read_array::<4>()?),
                destination: Ipv4Addr::from(self.read_array::<4>()?),
                protocol: self.read_u8()?,
                identification: self.read_u16()?,
            })),
            6 => Some(Some(IpFragmentKey::V6 {
                source: Ipv6Addr::from(self.read_array::<16>()?),
                destination: Ipv6Addr::from(self.read_array::<16>()?),
                next_header: self.read_u8()?,
                identification: self.read_u32()?,
            })),
            _ => None,
        }
    }
}
