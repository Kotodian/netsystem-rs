use hammer_core::data_plane::{BufferPacketCursor, Frame};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneMain, Node, NodeProcessFn, TraceFormatter, add_packet_trace, format_packet_trace,
    unlikely,
};

use crate::ip::{IpInputError, IpInputTarget, IpProtocol, IpVersion, parse_ip_header};
use crate::protocol::ip_ecn::IpEcnCodepoint;
use hammer_service::data_plane::set_index_node_error;
use hammer_service::opaque::NetworkOpaque;

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub input_target: Option<IpInputTarget>,
    pub input_error: Option<IpInputError>,
    pub packet_len: usize,
    pub next: u16,
}

impl Node for Ip4InputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, node_runtime, frame| {
            let processed_vectors = frame.len();
            ip_input_process_frame(runtime, node_runtime, frame, IpVersion::V4);
            processed_vectors
        };
        process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpInputTrace))
    }
}

impl Node for Ip6InputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, node_runtime, frame| {
            let processed_vectors = frame.len();
            ip_input_process_frame(runtime, node_runtime, frame, IpVersion::V6);
            processed_vectors
        };
        process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpInputTrace))
    }
}

#[inline(always)]
fn ip_input_process_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    version: IpVersion,
) -> () {
    let mut nexts = Vec::with_capacity(frame.len());
    let drop_slot = match version {
        IpVersion::V4 => Ip4InputNext::Drop.slot() as u16,
        IpVersion::V6 => Ip6InputNext::Drop.slot() as u16,
    };
    for index in frame.vector_args() {
        let slot = match next_slot_for_index(runtime, *index, version) {
            Ok(slot) => slot,
            Err(_) => drop_slot,
        };
        nexts.push(slot);
    }
    runtime.enqueue_to_next(node_runtime, frame, nexts.as_slice());
    ()
}

#[inline(always)]
fn next_slot_for_index(
    runtime: &mut DataPlaneMain,
    index: u32,
    version: IpVersion,
) -> RuntimeResult<u16> {
    let drop_next = match version {
        IpVersion::V4 => Ip4InputNext::Drop.slot() as u16,
        IpVersion::V6 => Ip6InputNext::Drop.slot() as u16,
    };
    let (traced, parsed) = {
        let buffer = runtime.buffer(index);
        (
            buffer.trace_handle().is_some(),
            parse_ip_header(buffer.current()),
        )
    };
    let parsed = match parsed {
        Err(_) => {
            set_index_node_error(runtime, index, IpInputError::BadLength)?;
            if unlikely(traced) {
                let _ = add_packet_trace!(
                    runtime,
                    index,
                    IpInputTrace {
                        version: None,
                        protocol: None,
                        input_target: None,
                        input_error: Some(IpInputError::BadLength),
                        packet_len: 0,
                        next: drop_next,
                    },
                );
            }
            return Ok(drop_next);
        }
        Ok(parsed) => parsed,
    };
    if parsed.version != version {
        set_index_node_error(runtime, index, IpInputError::BadLength)?;
        return Ok(drop_next);
    }
    // Resolve the Node error before borrowing the packet mutably. The Buffer
    // borrow must not overlap another access through its owning DataPlaneMain.
    let error = if parsed.input_error == IpInputError::None {
        None
    } else {
        Some(runtime.record_current_node_error(parsed.input_error)?)
    };
    {
        let buffer = runtime.buffer_mut(index);
        if let Some(error) = error {
            buffer.set_node_error_index(error);
        } else {
            buffer.clear_node_error();
        }
        let cursor = if !matches!(parsed.protocol, IpProtocol::Other(_)) {
            BufferPacketCursor::new()
                .with_packet_len(parsed.packet_len)
                .with_network_header(parsed.network_header_offset, parsed.network_header_len)
                .with_transport_header(parsed.transport_header_offset, parsed.transport_header_len)
                .with_transport_payload_offset(
                    parsed.transport_header_offset + parsed.transport_header_len,
                )
        } else {
            BufferPacketCursor::new()
        };
        hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).set_packet_cursor(cursor);
        let ip_ecn = ip_ecn_from_packet(buffer.current(), parsed.version);
        let sw_if_index = hammer_core::buffer_opaque!(buffer => NetworkOpaque).sw_if_index[0];
        let ip = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).ip_mut();
        ip.set_ip_ecn(ip_ecn.map(|codepoint| codepoint as u8));
        ip.set_ip_version(Some(match parsed.version {
            IpVersion::V4 => 4,
            IpVersion::V6 => 6,
        }));
        ip.set_ip_protocol(Some(u8::from(parsed.protocol)));
        ip.set_fib_index(crate::lookup::fib_index_for(parsed.version, sw_if_index));
    }
    let trace = traced.then_some(IpInputTrace {
        version: Some(parsed.version),
        protocol: Some(parsed.protocol),
        input_target: Some(parsed.input_target),
        input_error: Some(parsed.input_error),
        packet_len: parsed.packet_len,
        next: drop_next,
    });
    let resolved = match parsed.input_target {
        IpInputTarget::Drop => drop_next,
        IpInputTarget::Punt => match version {
            IpVersion::V4 => Ip4InputNext::Punt.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Punt.slot() as u16,
        },
        IpInputTarget::Options => match version {
            IpVersion::V4 => Ip4InputNext::Options.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Options.slot() as u16,
        },
        IpInputTarget::Lookup => {
            let default_next = match parsed.version {
                IpVersion::V4 => Ip4InputNext::Lookup.slot() as u16,
                IpVersion::V6 => Ip6InputNext::Lookup.slot() as u16,
            };
            let arc_index = unsafe {
                match version {
                    IpVersion::V4 => *crate::lookup::IP4_MAIN
                        .get()
                        .ok_or(hammer_runtime::RuntimeError::PluginStateNotInitialized {
                            plugin: "ip",
                        })?
                        .unicast_feature_arc_index
                        .get(),
                    IpVersion::V6 => *crate::lookup::IP6_MAIN
                        .get()
                        .ok_or(hammer_runtime::RuntimeError::PluginStateNotInitialized {
                            plugin: "ip",
                        })?
                        .unicast_feature_arc_index
                        .get(),
                }
            };
            let net = hammer_service::net::NetMain::global()?;
            let mut buffer = runtime.buffer_mut(index);
            let interface_index =
                hammer_core::buffer_opaque!(buffer => NetworkOpaque).sw_if_index[0];
            net.interface_main().start_feature_arc(
                arc_index,
                interface_index,
                &mut buffer,
                default_next,
            )
        }
        IpInputTarget::LookupMulticast => match parsed.version {
            IpVersion::V4 => Ip4InputNext::Lookup.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Lookup.slot() as u16,
        },
        IpInputTarget::IcmpError => {
            let metadata = match parsed.version {
                IpVersion::V4 => crate::protocol::icmp::IcmpErrorMetadata::ipv4_time_exceeded(),
                IpVersion::V6 => crate::protocol::icmp::IcmpErrorMetadata::ipv6_time_exceeded(),
            };
            metadata.write(hammer_core::buffer_opaque!(mut runtime.buffer_mut(index) => crate::IpSecondaryOpaque));
            match parsed.version {
                IpVersion::V4 => Ip4InputNext::IcmpError.slot() as u16,
                IpVersion::V6 => Ip6InputNext::IcmpError.slot() as u16,
            }
        }
        IpInputTarget::Reassembly => match version {
            IpVersion::V4 => Ip4InputNext::Reassembly.slot() as u16,
            IpVersion::V6 => Ip6InputNext::Reassembly.slot() as u16,
        },
    };
    if let Some(trace) = trace {
        let _ = add_packet_trace!(
            runtime,
            index,
            IpInputTrace {
                next: resolved,
                ..trace
            },
        );
    }
    Ok(resolved)
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
