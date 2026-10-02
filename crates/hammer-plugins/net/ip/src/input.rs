use hammer_core::data_plane::{BufferPacketCursor, DEFAULT_BUFFER_FRAME_CAPACITY, Frame};
use hammer_infra::checksum::internet_checksum;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneMain, Node, TraceFormatter};

use crate::ip::{IpInputError, IpInputTarget, IpProtocol, IpVersion};
use crate::protocol::ip::{
    IPV4_FLAG_MORE_FRAGMENTS, IPV4_FRAGMENT_OFFSET_MASK, IPV4_HEADER_MIN_LEN,
    IPV6_FRAGMENT_HEADER_LEN, IPV6_HEADER_LEN, IPV6_NEXT_HEADER_FRAGMENT, Ipv4Header,
    Ipv6FragmentHeader, Ipv6Header,
};
use crate::protocol::ip_ecn::IpEcnCodepoint;
use hammer_service::feature::FeatureMain;
use hammer_service::opaque::NetworkOpaque;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[hammer_component_macros::node_next]
pub enum Ip4InputNext {
    #[next("ip4-drop")]
    Drop,
    #[next("ip4-punt")]
    Punt,
    #[next("ip4-punt")]
    Options,
    #[next("ip4-lookup")]
    Lookup,
    #[next("ip4-icmp-error")]
    IcmpError,
    #[next("ip4-reassembly")]
    Reassembly,
}

#[hammer_component_macros::node_next]
pub enum Ip6InputNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-punt")]
    Punt,
    #[next("ip6-punt")]
    Options,
    #[next("ip6-lookup")]
    Lookup,
    #[next("ip6-icmp-error")]
    IcmpError,
    #[next("ip6-reassembly")]
    Reassembly,
}

#[hammer_component_macros::feature_arc(name = "ip4-unicast", start_nodes = [Ip4InputNode], last_in_arc = crate::lookup::Ip4LookupNode)]
#[hammer_component_macros::graph_node(graph = ip, kind = internal, name = "ip4-input", next = Ip4InputNext)]
pub struct Ip4InputNode;

#[hammer_component_macros::feature_arc(name = "ip6-unicast", start_nodes = [Ip6InputNode], last_in_arc = crate::lookup::Ip6LookupNode)]
#[hammer_component_macros::graph_node(graph = ip, kind = internal, name = "ip6-input", next = Ip6InputNext)]
pub struct Ip6InputNode;

/// VPP `ip4_input_trace_t` and `ip6_input_trace_t`: packet bytes at Node entry.
#[repr(C)]
#[derive(Debug, Clone, KnownLayout, FromBytes, IntoBytes, Immutable)]
pub struct IpInputTrace {
    pub packet_data: [u8; 64],
}

fn format_ip_input_trace(bytes: &[u8]) -> String {
    let (trace, _) = IpInputTrace::ref_from_prefix(bytes)
        .expect("IP input trace has its registered layout");
    let packet = &trace.packet_data;
    match packet[0] >> 4 {
        4 => format!(
            "ip4 {} -> {} protocol {} length {}",
            std::net::Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]),
            std::net::Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]),
            packet[9],
            u16::from_be_bytes([packet[2], packet[3]]),
        ),
        6 => format!(
            "ip6 {} -> {} next-header {} payload-length {}",
            std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&packet[8..24]).expect("IP6 source has 16 bytes")),
            std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&packet[24..40]).expect("IP6 destination has 16 bytes")),
            packet[6],
            u16::from_be_bytes([packet[4], packet[5]]),
        ),
        version => format!("ip version {version} (truncated or invalid header)"),
    }
}

impl Node for Ip4InputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let processed_vectors = frame.len();
        ip_input_process_frame(runtime, node_runtime, frame, IpVersion::V4);
        processed_vectors
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_input_trace)
    }
}

impl Node for Ip6InputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let processed_vectors = frame.len();
        ip_input_process_frame(runtime, node_runtime, frame, IpVersion::V6);
        processed_vectors
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_input_trace)
    }
}

#[inline(always)]
fn ip_input_process_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    version: IpVersion,
) {
    let count = frame.len();
    let indices = frame.vector_args();
    if node_runtime.trace_enabled() {
        runtime.trace_frame_buffers_only::<IpInputTrace>(node_runtime, indices);
    }
    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut errors = [IpInputError::None; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut offset = 0;

    // VPP ip4_input_inline processes four buffers when the prefetch pipeline
    // is available. The two-buffer path is also used by ip6-input.
    if matches!(version, IpVersion::V4) {
        while offset + 4 <= count {
            if offset + 12 <= count {
                runtime.prefetch_header(indices[offset + 8]);
                runtime.prefetch_header(indices[offset + 9]);
                runtime.prefetch_header(indices[offset + 10]);
                runtime.prefetch_header(indices[offset + 11]);
                prefetch_input_data(runtime, indices[offset + 4], version);
                prefetch_input_data(runtime, indices[offset + 5], version);
                prefetch_input_data(runtime, indices[offset + 6], version);
                prefetch_input_data(runtime, indices[offset + 7], version);
            }
            process_input_packet(
                runtime,
                indices[offset],
                version,
                &mut nexts[offset],
                &mut errors[offset],
            )
            .expect("IP input graph state is initialized");
            process_input_packet(
                runtime,
                indices[offset + 1],
                version,
                &mut nexts[offset + 1],
                &mut errors[offset + 1],
            )
            .expect("IP input graph state is initialized");
            process_input_packet(
                runtime,
                indices[offset + 2],
                version,
                &mut nexts[offset + 2],
                &mut errors[offset + 2],
            )
            .expect("IP input graph state is initialized");
            process_input_packet(
                runtime,
                indices[offset + 3],
                version,
                &mut nexts[offset + 3],
                &mut errors[offset + 3],
            )
            .expect("IP input graph state is initialized");
            offset += 4;
        }
    }

    while offset + 2 <= count {
        if offset + 6 <= count {
            runtime.prefetch_header(indices[offset + 4]);
            runtime.prefetch_header(indices[offset + 5]);
            prefetch_input_data(runtime, indices[offset + 2], version);
            prefetch_input_data(runtime, indices[offset + 3], version);
        }
        process_input_packet(
            runtime,
            indices[offset],
            version,
            &mut nexts[offset],
            &mut errors[offset],
        )
        .expect("IP input graph state is initialized");
        process_input_packet(
            runtime,
            indices[offset + 1],
            version,
            &mut nexts[offset + 1],
            &mut errors[offset + 1],
        )
        .expect("IP input graph state is initialized");
        offset += 2;
    }

    if offset < count {
        process_input_packet(
            runtime,
            indices[offset],
            version,
            &mut nexts[offset],
            &mut errors[offset],
        )
        .expect("IP input graph state is initialized");
    }

    finish_input_errors(runtime, indices, &errors[..count]);
    runtime.enqueue_to_next(node_runtime, frame, &nexts[..count]);
}

#[inline(always)]
fn input_drop_slot(version: IpVersion) -> u16 {
    match version {
        IpVersion::V4 => Ip4InputNext::Drop.slot() as u16,
        IpVersion::V6 => Ip6InputNext::Drop.slot() as u16,
    }
}

#[inline(always)]
fn prefetch_input_data(runtime: &DataPlaneMain, index: u32, version: IpVersion) {
    let header_len = match version {
        IpVersion::V4 => IPV4_HEADER_MIN_LEN,
        IpVersion::V6 => IPV6_HEADER_LEN,
    };
    let packet = runtime.buffer(index).current();
    hammer_infra::prefetch::prefetch_read_l1_bytes(packet.as_ptr(), header_len);
}

#[inline(always)]
fn process_input_packet(
    runtime: &mut DataPlaneMain,
    index: u32,
    version: IpVersion,
    next: &mut u16,
    packet_error: &mut IpInputError,
) -> RuntimeResult<()> {
    let drop_next = input_drop_slot(version);
    let (classification, ip_ecn) = {
        let buffer = runtime.buffer(index);
        (
            match version {
                IpVersion::V4 => classify_ipv4_input(
                    buffer.current(),
                    buffer.current_len() + buffer.total_len_not_including_first(),
                ),
                IpVersion::V6 => classify_ipv6_input(buffer.current()),
            },
            ip_ecn_from_packet(buffer.current(), version),
        )
    };
    let (
        protocol,
        input_target,
        input_error,
        packet_len,
        network_header_len,
        transport_header_offset,
    ) = match classification {
        Err(error) => {
            runtime.buffer_mut(index).clear_node_error();
            *next = drop_next;
            *packet_error = error;
            return Ok(());
        }
        Ok(classification) => classification,
    };
    {
        let buffer = runtime.buffer_mut(index);
        buffer.clear_node_error();
        let sw_if_index = hammer_core::buffer_opaque!(buffer => NetworkOpaque).sw_if_index[0];
        let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
        network.set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(transport_header_offset, 0)
                .with_transport_payload_offset(transport_header_offset),
        );
        let ip = network.ip_mut();
        ip.set_ip_ecn(ip_ecn.map(|codepoint| codepoint as u8));
        ip.set_ip_version(Some(match version {
            IpVersion::V4 => 4,
            IpVersion::V6 => 6,
        }));
        ip.set_ip_protocol(Some(u8::from(protocol)));
        ip.set_fib_index(crate::lookup::fib_table_get_index_for_sw_if_index(
            version,
            sw_if_index,
        ));
    }
    let fallback_next = match input_target {
        IpInputTarget::Drop => drop_next,
        IpInputTarget::Punt => match version {
            IpVersion::V4 => Ip4InputNext::Punt.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Punt.slot() as u16,
        },
        IpInputTarget::Options => match version {
            IpVersion::V4 => Ip4InputNext::Options.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Options.slot() as u16,
        },
        IpInputTarget::Lookup => match version {
            IpVersion::V4 => Ip4InputNext::Lookup.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Lookup.slot() as u16,
        },
        IpInputTarget::LookupMulticast => match version {
            // MFIB is not installed yet; multicast must not enter the unicast FIB.
            IpVersion::V4 => Ip4InputNext::Punt.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Lookup.slot() as u16,
        },
        IpInputTarget::IcmpError => {
            let metadata = match version {
                IpVersion::V4 => crate::protocol::icmp::IcmpErrorMetadata::ipv4_time_exceeded(),
                IpVersion::V6 => crate::protocol::icmp::IcmpErrorMetadata::ipv6_time_exceeded(),
            };
            metadata.write(hammer_core::buffer_opaque!(mut runtime.buffer_mut(index) => crate::IpSecondaryOpaque));
            match version {
                IpVersion::V4 => Ip4InputNext::IcmpError.slot() as u16,
                IpVersion::V6 => Ip6InputNext::IcmpError.slot() as u16,
            }
        }
        IpInputTarget::Reassembly => match version {
            IpVersion::V4 => Ip4InputNext::Reassembly.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Reassembly.slot() as u16,
        },
    };
    let start_unicast_arc = matches!(input_target, IpInputTarget::Lookup)
        || matches!(version, IpVersion::V4)
            && matches!(
                input_target,
                IpInputTarget::LookupMulticast | IpInputTarget::Reassembly
            );
    let resolved = if start_unicast_arc {
        let default_next = match version {
            IpVersion::V4 => Ip4InputNext::Lookup.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Lookup.slot() as u16,
        };
        let arc_index = unsafe {
            match version {
                IpVersion::V4 => {
                    (*crate::lookup::IP4_MAIN
                        .get()
                        .ok_or(hammer_runtime::RuntimeError::PluginStateNotInitialized {
                            plugin: "ip",
                        })?
                        .lookup_main
                        .get())
                    .unicast_feature_arc_index
                }
                IpVersion::V6 => {
                    (*crate::lookup::IP6_MAIN
                        .get()
                        .ok_or(hammer_runtime::RuntimeError::PluginStateNotInitialized {
                            plugin: "ip",
                        })?
                        .lookup_main
                        .get())
                    .unicast_feature_arc_index
                }
            }
        };
        let features = FeatureMain::global()?;
        let mut buffer = runtime.buffer_mut(index);
        let interface_index = hammer_core::buffer_opaque!(buffer => NetworkOpaque).sw_if_index[0];
        let feature_next =
            features.start_feature_arc(arc_index, interface_index, &mut buffer, default_next);
        if feature_next == default_next {
            fallback_next
        } else {
            feature_next
        }
    } else {
        fallback_next
    };
    *next = resolved;
    *packet_error = input_error;
    Ok(())
}

#[inline(always)]
fn finish_input_errors(runtime: &mut DataPlaneMain, indices: &[u32], errors: &[IpInputError]) {
    const INPUT_ERRORS: [IpInputError; 9] = [
        IpInputError::None,
        IpInputError::Version,
        IpInputError::HeaderTooShort,
        IpInputError::Options,
        IpInputError::BadChecksum,
        IpInputError::TimeExpired,
        IpInputError::FragmentOffsetOne,
        IpInputError::TooShort,
        IpInputError::BadLength,
    ];
    let mut counts = [0u64; INPUT_ERRORS.len()];
    for &error in errors {
        if error != IpInputError::None {
            counts[error.code() as usize] += 1;
        }
    }
    let mut indexes = [None; INPUT_ERRORS.len()];
    for (code, &error) in INPUT_ERRORS.iter().enumerate().skip(1) {
        if counts[code] != 0 {
            indexes[code] = Some(
                runtime
                    .record_current_node_error_count(error, counts[code])
                    .expect("IP input node error registry remains installed"),
            );
        }
    }
    for (&index, &error) in indices.iter().zip(errors) {
        if let Some(error_index) = indexes[error.code() as usize] {
            runtime.buffer_mut(index).set_node_error_index(error_index);
        }
    }
}

#[inline(always)]
fn classify_ipv4_input(
    packet: &[u8],
    chain_len: usize,
) -> Result<(IpProtocol, IpInputTarget, IpInputError, usize, usize, usize), IpInputError> {
    let (header, _) =
        Ipv4Header::ref_from_prefix(packet).map_err(|_| IpInputError::HeaderTooShort)?;
    let header_len = header.header_len();
    if header.version() != 4 {
        return Err(IpInputError::Version);
    }
    if header_len < IPV4_HEADER_MIN_LEN || packet.len() < header_len {
        return Err(IpInputError::HeaderTooShort);
    }
    let packet_len = header.total_len();
    let fragment = header.flags_fragment();
    let fragment_offset = fragment & IPV4_FRAGMENT_OFFSET_MASK;
    let checksum_bad = internet_checksum(&packet[..header_len]) != 0;
    let destination = header.destination();
    // VPP ip4_input_check_x1/x2/x4 overwrites earlier classifications in this order.
    let mut error = if header_len != IPV4_HEADER_MIN_LEN {
        IpInputError::Options
    } else {
        IpInputError::None
    };
    if checksum_bad {
        error = IpInputError::BadChecksum;
    }
    if header.ttl() < 1 {
        error = IpInputError::TimeExpired;
    }
    if fragment_offset == 1 {
        error = IpInputError::FragmentOffsetOne;
    }
    if packet_len < IPV4_HEADER_MIN_LEN {
        error = IpInputError::TooShort;
    }
    if chain_len < packet_len || packet_len < header_len {
        error = IpInputError::BadLength;
    }
    let target = match error {
        IpInputError::TimeExpired => IpInputTarget::IcmpError,
        IpInputError::Options => IpInputTarget::Options,
        IpInputError::None
            if fragment & (IPV4_FLAG_MORE_FRAGMENTS | IPV4_FRAGMENT_OFFSET_MASK) != 0 =>
        {
            IpInputTarget::Reassembly
        }
        IpInputError::None if destination.is_multicast() => IpInputTarget::LookupMulticast,
        IpInputError::None => IpInputTarget::Lookup,
        _ => IpInputTarget::Drop,
    };
    Ok((
        IpProtocol::from(header.protocol()),
        target,
        error,
        packet_len,
        header_len,
        header_len,
    ))
}

#[inline(always)]
fn classify_ipv6_input(
    packet: &[u8],
) -> Result<(IpProtocol, IpInputTarget, IpInputError, usize, usize, usize), IpInputError> {
    let (header, _) =
        Ipv6Header::ref_from_prefix(packet).map_err(|_| IpInputError::HeaderTooShort)?;
    if header.version() != 6 {
        return Err(IpInputError::Version);
    }
    let payload_len = header.payload_len();
    let packet_len = IPV6_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(IpInputError::BadLength)?;
    let (protocol, target, error, transport_offset) =
        if header.next_protocol() == IPV6_NEXT_HEADER_FRAGMENT {
            if payload_len < IPV6_FRAGMENT_HEADER_LEN {
                return Err(IpInputError::HeaderTooShort);
            }
            let (fragment, _) = Ipv6FragmentHeader::ref_from_prefix(
                packet
                    .get(IPV6_HEADER_LEN..)
                    .ok_or(IpInputError::HeaderTooShort)?,
            )
            .map_err(|_| IpInputError::HeaderTooShort)?;
            (
                fragment.next_protocol(),
                if header.hop_limit() < 1 {
                    IpInputTarget::IcmpError
                } else {
                    IpInputTarget::Reassembly
                },
                if header.hop_limit() < 1 {
                    IpInputError::TimeExpired
                } else {
                    IpInputError::None
                },
                IPV6_HEADER_LEN + IPV6_FRAGMENT_HEADER_LEN,
            )
        } else if header.hop_limit() < 1 {
            (
                header.next_protocol(),
                IpInputTarget::IcmpError,
                IpInputError::TimeExpired,
                IPV6_HEADER_LEN,
            )
        } else if header.destination().is_multicast() {
            (
                header.next_protocol(),
                IpInputTarget::LookupMulticast,
                IpInputError::None,
                IPV6_HEADER_LEN,
            )
        } else {
            (
                header.next_protocol(),
                IpInputTarget::Lookup,
                IpInputError::None,
                IPV6_HEADER_LEN,
            )
        };
    Ok((
        IpProtocol::from(protocol),
        target,
        error,
        packet_len,
        IPV6_HEADER_LEN,
        transport_offset,
    ))
}

#[inline(always)]
fn ip_ecn_from_packet(packet: &[u8], version: IpVersion) -> Option<IpEcnCodepoint> {
    let traffic_class = match version {
        IpVersion::V4 => packet.get(1).copied()?,
        IpVersion::V6 => {
            let first = *packet.first()?;
            let second = *packet.get(1)?;
            ((first & 0x0f) << 4) | (second >> 4)
        }
    };
    match traffic_class & 0x03 {
        0 => Some(IpEcnCodepoint::NotEct),
        1 => Some(IpEcnCodepoint::Ect1),
        2 => Some(IpEcnCodepoint::Ect0),
        3 => Some(IpEcnCodepoint::Ce),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ipv4_packet(header_len: usize, ttl: u8, total_len: u16, fragment: u16) -> Vec<u8> {
        let mut packet = vec![0u8; header_len];
        packet[0] = 0x40 | (header_len / 4) as u8;
        packet[2..4].copy_from_slice(&total_len.to_be_bytes());
        packet[6..8].copy_from_slice(&fragment.to_be_bytes());
        packet[8] = ttl;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&[198, 18, 0, 2]);
        packet[16..20].copy_from_slice(&[198, 18, 0, 1]);
        let checksum = internet_checksum(&packet);
        packet[10..12].copy_from_slice(&checksum.to_be_bytes());
        packet
    }

    #[test]
    fn ipv4_input_checks_chain_length_and_header_errors() {
        let packet = ipv4_packet(20, 64, 64, 0);
        let (_, target, error, packet_len, _, _) = classify_ipv4_input(&packet, 64).unwrap();
        assert_eq!(
            (target, error, packet_len),
            (IpInputTarget::Lookup, IpInputError::None, 64)
        );
        assert_eq!(
            classify_ipv4_input(&packet, 20).unwrap().2,
            IpInputError::BadLength
        );
        assert_eq!(
            classify_ipv4_input(&packet[..19], 64),
            Err(IpInputError::HeaderTooShort)
        );
        let mut wrong_version = packet;
        wrong_version[0] = 0x65;
        assert_eq!(
            classify_ipv4_input(&wrong_version, 64),
            Err(IpInputError::Version)
        );
    }

    #[test]
    fn ipv4_input_uses_vpp_error_precedence() {
        let mut packet = ipv4_packet(24, 0, 19, 1);
        packet[10] ^= 1;
        assert_eq!(
            classify_ipv4_input(&packet, 24).unwrap().2,
            IpInputError::TooShort
        );
        assert_eq!(
            classify_ipv4_input(&packet, 18).unwrap().2,
            IpInputError::BadLength
        );

        let packet = ipv4_packet(24, 0, 24, 1);
        assert_eq!(
            classify_ipv4_input(&packet, 24).unwrap().2,
            IpInputError::FragmentOffsetOne
        );
        let packet = ipv4_packet(24, 0, 24, 0);
        assert_eq!(
            classify_ipv4_input(&packet, 24).unwrap().2,
            IpInputError::TimeExpired
        );
        let packet = ipv4_packet(24, 64, 24, 0);
        assert_eq!(
            classify_ipv4_input(&packet, 24).unwrap().2,
            IpInputError::Options
        );
    }
}
