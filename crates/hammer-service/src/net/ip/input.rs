use std::mem::transmute;
use std::sync::{Mutex, OnceLock};

use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::{
    DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace, unlikely,
};

use crate::data_plane::{FeatureArcSpec, FeatureArcStartHandle, set_buffer_node_error_code};
use crate::net::ip::{
    IpInputError, IpInputTarget, IpProtocol, IpVersion, network_for_protocol, parse_ip_header,
};
use crate::net::{IpEcnCodepoint, NetworkOpaque};
use crate::trace::codec::{
    TraceDecodeCursor, put_option_ip_input_error, put_option_ip_input_target,
    put_option_ip_protocol, put_option_ip_version, put_u16, put_usize,
};

#[hammer_component_macros::feature_arc]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpUnicastArc {}

#[hammer_component_macros::node_next]
pub enum IpInputNext {
    Drop,
    Punt,
    Options,
    Lookup,
    LookupMulticast,
    IcmpError,
    Reassembly,
}

#[hammer_component_macros::node(role = internal, next = IpInputNext, start_arc = A)]
pub struct IpInputNode<A: FeatureArcSpec = IpUnicastArc> {
    #[node(default = register_ip_input_runtime(None))]
    runtime_data: NodeRuntimeData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub input_target: Option<IpInputTarget>,
    pub input_error: Option<IpInputError>,
    pub packet_len: usize,
    pub next: u16,
}

impl IpInputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            version: cursor.read_option_ip_version()?,
            protocol: cursor.read_option_ip_protocol()?,
            input_target: cursor.read_option_ip_input_target()?,
            input_error: cursor.read_option_ip_input_error()?,
            packet_len: cursor.read_usize()?,
            next: cursor.read_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IpInputTrace {
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_ip_input_target(out, self.input_target);
        put_option_ip_input_error(out, self.input_error);
        put_usize(out, self.packet_len);
        put_u16(out, self.next);
    }
}

fn format_ip_input_trace(bytes: &[u8]) -> String {
    match IpInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IpInputTrace invalid={bytes:?}"),
    }
}

impl<A> Node for IpInputNode<A>
where
    A: FeatureArcSpec,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        ip_input_process_frame(runtime, frame, feature_arc.as_ref())
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_input_process::<A>
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_input_trace)
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        sync_ip_input_runtime(self.runtime_data, feature_arc)?;
        Ok(self.runtime_data)
    }
}

#[inline(always)]
fn ip_input_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    feature_arc: Option<&FeatureArcStartHandle>,
) -> NodeResult {
    let mut nexts = hammer_infra::vec::Vec::with_capacity(frame.len());
    let drop_slot = IpInputNext::Drop.slot() as u16;
    for index in frame.iter_indices() {
        let slot = match next_slot_for_index(runtime, *index, feature_arc) {
            Ok(slot) => slot,
            Err(_) => drop_slot,
        };
        nexts.push(slot);
    }
    runtime.enqueue_to_next(frame, nexts.as_slice());
    NodeResult::drop()
}

/// Per-instance state held in the global IP input registry. Mirrors the
/// `OnceLock<Mutex<Vec<...>>>` + `NodeRuntimeData::from_usize` pattern used by
/// the sibling migrated nodes (`IpLookupNode`, `IcmpInputNode`,
/// `InterfaceOutputNode`): word 0 of [`NodeRuntimeData`] is the registry slot.
///
/// `feature_arc` is `None` at construction (`FeatureArcStartSlot::new()` is
/// empty) and synced from `self.feature_arc` in [`Node::node_runtime_data`]
/// when the descriptor is built, so a `set_feature_arc` call made before
/// registration is captured.
#[derive(Clone)]
struct IpInputRuntime {
    feature_arc: Option<FeatureArcStartHandle>,
}

fn ip_input_runtimes() -> &'static Mutex<hammer_infra::vec::Vec<IpInputRuntime>> {
    static RUNTIMES: OnceLock<Mutex<hammer_infra::vec::Vec<IpInputRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(hammer_infra::vec::Vec::new()))
}

fn register_ip_input_runtime(feature_arc: Option<FeatureArcStartHandle>) -> NodeRuntimeData {
    let mut runtimes = ip_input_runtimes()
        .lock()
        .expect("IP input runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(IpInputRuntime { feature_arc });
    NodeRuntimeData::from_usize(slot).expect("IP input runtime slot overflow")
}

fn sync_ip_input_runtime(
    data: NodeRuntimeData,
    feature_arc: Option<FeatureArcStartHandle>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_input_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP input runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP input runtime slot is invalid"))?;
    runtime.feature_arc = feature_arc;
    Ok(())
}

fn ip_input_runtime(data: NodeRuntimeData) -> CoreResult<IpInputRuntime> {
    let slot = data.usize_word(0)?;
    ip_input_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP input runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("IP input runtime slot is invalid"))
}

fn ip_input_process<A: FeatureArcSpec>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = match ip_input_runtime(data) {
        Ok(state) => state,
        Err(_) => return NodeResult::drop(),
    };
    let feature_arc = state.feature_arc.as_ref();
    ip_input_process_frame(runtime, frame, feature_arc)
}

#[inline(always)]
fn next_slot_for_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    feature_arc: Option<&FeatureArcStartHandle>,
) -> CoreResult<u16> {
    let (trace, parsed, _) = {
        let mut buffer = runtime.get_buffer_mut(index)?;
        let traced = buffer.trace_handle().is_some();
        match parse_ip_header(buffer.current()) {
            Err(_) => {
                set_buffer_node_error_code(runtime, &mut buffer, IpInputError::BadLength.code())?;
                let resolved = IpInputNext::Drop.slot() as u16;
                drop(buffer);
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
                            next: resolved,
                        },
                    );
                }
                return Ok(resolved);
            }
            Ok(parsed) => {
                if parsed.input_error == IpInputError::None {
                    buffer.clear_node_error();
                } else {
                    set_buffer_node_error_code(runtime, &mut buffer, parsed.input_error.code())?;
                }
                let network = network_for_protocol(parsed.protocol);
                let cursor = if network.is_some() {
                    BufferPacketCursor::new()
                        .with_packet_len(parsed.packet_len)
                        .with_network_header(
                            parsed.network_header_offset,
                            parsed.network_header_len,
                        )
                        .with_transport_header(
                            parsed.transport_header_offset,
                            parsed.transport_header_len,
                        )
                        .with_transport_payload_offset(
                            parsed.transport_header_offset + parsed.transport_header_len,
                        )
                } else {
                    BufferPacketCursor::new()
                };
                unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }
                    .set_packet_cursor(cursor);
                let ip_ecn = ip_ecn_from_packet(buffer.current(), parsed.version);
                let ip =
                    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.ip_mut();
                ip.set_ip_ecn(ip_ecn.map(|codepoint| codepoint as u8));
                ip.set_ip_version(Some(match parsed.version {
                    IpVersion::V4 => 4,
                    IpVersion::V6 => 6,
                }));
                ip.set_ip_protocol(Some(match parsed.protocol {
                    IpProtocol::Icmpv4 => 1,
                    IpProtocol::Tcp => 6,
                    IpProtocol::Udp => 17,
                    IpProtocol::Icmpv6 => 58,
                    IpProtocol::Other(value) => value,
                }));
                (
                    traced.then_some(IpInputTrace {
                        version: Some(parsed.version),
                        protocol: Some(parsed.protocol),
                        input_target: Some(parsed.input_target),
                        input_error: Some(parsed.input_error),
                        packet_len: parsed.packet_len,
                        next: IpInputNext::Drop.slot() as u16,
                    }),
                    parsed,
                    network,
                )
            }
        }
    };
    let resolved = match parsed.input_target {
        IpInputTarget::Drop => IpInputNext::Drop.slot() as u16,
        IpInputTarget::Punt => IpInputNext::Punt.slot() as u16,
        IpInputTarget::Options => IpInputNext::Options.slot() as u16,
        IpInputTarget::Lookup => {
            let default_next = IpInputNext::Lookup.slot() as u16;
            if let Some(arc) = feature_arc {
                let Some(interface_index) = ({
                    let buffer = runtime.get_buffer(index)?;
                    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
                    let interface_index = network.sw_if_index[0];
                    (interface_index != 0).then_some(interface_index)
                }) else {
                    return Ok(default_next);
                };
                arc.start_for_interface_or(runtime, index, interface_index, default_next)
            } else {
                default_next
            }
        }
        IpInputTarget::LookupMulticast => IpInputNext::LookupMulticast.slot() as u16,
        IpInputTarget::IcmpError => IpInputNext::IcmpError.slot() as u16,
        IpInputTarget::Reassembly => IpInputNext::Reassembly.slot() as u16,
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
