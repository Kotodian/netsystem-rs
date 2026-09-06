use std::net::IpAddr;
use std::time::Duration;

use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId};
use hammer_core::error::{BufferInvariant, DataPlaneError};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_runtime::node::{NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity};
use hammer_runtime::{
    DataPlaneMain, Node, NodeProcessFn, NodeRuntimeData, RuntimeError, RuntimeResult,
};
use hammer_service::net::{NetMain, throttle::Throttle};
use hammer_service::opaque::NetworkOpaque;

use crate::lookup::{IP4_MAIN, IP6_MAIN};
use crate::protocol::icmp::{IcmpErrorFamily, IcmpErrorMetadata};
use crate::protocol::ip::{
    IpProtocol, Ipv4Header, Ipv6Header, write_ipv4_push_header, write_ipv6_push_header,
};
use crate::protocol::wire::read_header;

#[hammer_component_macros::node_next]
enum Ip4IcmpErrorNext {
    #[next("drop")]
    Drop,
    #[next("ip4-lookup")]
    Lookup,
}

#[hammer_component_macros::node_next]
enum Ip6IcmpErrorNext {
    #[next("drop")]
    Drop,
    #[next("ip6-lookup")]
    Lookup,
}

#[hammer_component_macros::graph_node(
    graph = ip, init = register_ip4_icmp_error, role = internal,
    name = "ip4-icmp-error", next = Ip4IcmpErrorNext,
)]
pub(crate) struct Ip4IcmpErrorNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

#[hammer_component_macros::graph_node(
    graph = ip, init = register_ip6_icmp_error, role = internal,
    name = "ip6-icmp-error", next = Ip6IcmpErrorNext,
)]
pub(crate) struct Ip6IcmpErrorNode {
    #[node(default = NodeRuntimeData::empty())]
    runtime_data: NodeRuntimeData,
}

#[derive(Clone, Copy)]
#[repr(u16)]
enum IcmpError {
    DestinationUnreachableSent,
    TimeExceededSent,
    ParameterProblemSent,
    PacketTooBigSent,
    Suppressed,
    NoSource,
    NoBuffer,
    BadRequest,
}

impl NodeErrorCode for IcmpError {
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl IcmpError {
    const DESCRIPTORS: [NodeErrorDescriptor; 8] = [
        NodeErrorDescriptor::new(
            "destination-unreachable-sent",
            NodeErrorSeverity::Info,
            "ICMP destination unreachable sent",
        ),
        NodeErrorDescriptor::new(
            "time-exceeded-sent",
            NodeErrorSeverity::Info,
            "ICMP time exceeded sent",
        ),
        NodeErrorDescriptor::new(
            "parameter-problem-sent",
            NodeErrorSeverity::Info,
            "ICMP parameter problem sent",
        ),
        NodeErrorDescriptor::new(
            "packet-too-big-sent",
            NodeErrorSeverity::Info,
            "ICMPv6 packet too big sent",
        ),
        NodeErrorDescriptor::new(
            "suppressed",
            NodeErrorSeverity::Info,
            "ICMP errors rate limited",
        ),
        NodeErrorDescriptor::new(
            "no-source",
            NodeErrorSeverity::Error,
            "No interface source address",
        ),
        NodeErrorDescriptor::new(
            "no-buffer",
            NodeErrorSeverity::Error,
            "ICMP response allocation failed",
        ),
        NodeErrorDescriptor::new(
            "bad-request",
            NodeErrorSeverity::Error,
            "Invalid ICMP error request",
        ),
    ];
}

fn register_ip4_icmp_error(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Ip4IcmpErrorNode::new(),
        &Ip4IcmpErrorNext::NEXT_NAMES,
    )?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IcmpError::DESCRIPTORS)?;
    Ok(node)
}

fn register_ip6_icmp_error(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Ip6IcmpErrorNode::new(),
        &Ip6IcmpErrorNext::NEXT_NAMES,
    )?;
    runtime
        .nodes()
        .materialize_node_errors(node, &IcmpError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for Ip4IcmpErrorNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        ip4_icmp_error(runtime, self.runtime_data, frame);
    }
    fn node_process(&self) -> NodeProcessFn {
        ip4_icmp_error
    }
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip6IcmpErrorNode {
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) {
        ip6_icmp_error(runtime, self.runtime_data, frame);
    }
    fn node_process(&self) -> NodeProcessFn {
        ip6_icmp_error
    }
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

#[hammer_component_macros::worker_init_function(name = "ip_icmp_error_worker_init")]
fn init_worker(runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    let ip4 = IP4_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    let ip6 = IP6_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    let worker = runtime.thread_index() as usize;
    assert!(
        ip4.icmp_throttle[worker]
            .install(Throttle::new(Duration::from_micros(10)))
            .is_ok(),
        "IPv4 ICMP throttle already installed for worker {worker}"
    );
    assert!(
        ip6.icmp_throttle[worker]
            .install(Throttle::new(Duration::from_millis(1)))
            .is_ok(),
        "IPv6 ICMP throttle already installed for worker {worker}"
    );
    runtime.register_worker_exit_function(exit_worker);
    Ok(())
}

fn exit_worker(runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    let worker = runtime.thread_index() as usize;
    IP4_MAIN
        .get()
        .expect("IP initialized before worker exit")
        .icmp_throttle[worker]
        .clear()
        .expect("IPv4 ICMP throttle released on its owner worker");
    IP6_MAIN
        .get()
        .expect("IP initialized before worker exit")
        .icmp_throttle[worker]
        .clear()
        .expect("IPv6 ICMP throttle released on its owner worker");
    Ok(())
}

fn ip4_icmp_error(runtime: &DataPlaneMain, _: NodeRuntimeData, frame: &mut BufferFrame) {
    let ip = IP4_MAIN
        .get()
        .expect("IP initialized before graph execution");
    let mut throttle = ip.icmp_throttle[runtime.thread_index() as usize]
        .borrow_mut()
        .expect("IPv4 ICMP throttle belongs to executing worker");
    let seed = throttle.seed(ip.clock_origin.elapsed());
    hammer_runtime::process_frame!(runtime, frame, |index| {
        let result = generate_error(
            runtime,
            index,
            IcmpErrorFamily::Ipv4,
            &mut throttle,
            seed,
            Ip4IcmpErrorNext::Lookup.slot(),
        );
        let error = match result {
            Ok(error) => error,
            Err(RuntimeError::DataPlane(
                DataPlaneError::BufferInvariant(BufferInvariant::PoolExhausted)
                | DataPlaneError::FramePoolExhausted,
            )) => IcmpError::NoBuffer,
            Err(error) => panic!("IPv4 ICMP graph ownership invariant: {error}"),
        };
        runtime
            .record_current_node_error(error)
            .expect("IPv4 ICMP node counters installed");
        Ip4IcmpErrorNext::Drop
    });
}

fn ip6_icmp_error(runtime: &DataPlaneMain, _: NodeRuntimeData, frame: &mut BufferFrame) {
    let ip = IP6_MAIN
        .get()
        .expect("IP initialized before graph execution");
    let mut throttle = ip.icmp_throttle[runtime.thread_index() as usize]
        .borrow_mut()
        .expect("IPv6 ICMP throttle belongs to executing worker");
    let seed = throttle.seed(ip.clock_origin.elapsed());
    hammer_runtime::process_frame!(runtime, frame, |index| {
        let result = generate_error(
            runtime,
            index,
            IcmpErrorFamily::Ipv6,
            &mut throttle,
            seed,
            Ip6IcmpErrorNext::Lookup.slot(),
        );
        let error = match result {
            Ok(error) => error,
            Err(RuntimeError::DataPlane(
                DataPlaneError::BufferInvariant(BufferInvariant::PoolExhausted)
                | DataPlaneError::FramePoolExhausted,
            )) => IcmpError::NoBuffer,
            Err(error) => panic!("IPv6 ICMP graph ownership invariant: {error}"),
        };
        runtime
            .record_current_node_error(error)
            .expect("IPv6 ICMP node counters installed");
        Ip6IcmpErrorNext::Drop
    });
}

fn generate_error(
    runtime: &DataPlaneMain,
    index: Index,
    family: IcmpErrorFamily,
    throttle: &mut Throttle,
    seed: u64,
    lookup_slot: usize,
) -> RuntimeResult<IcmpError> {
    let original = runtime.get_buffer(index)?;
    let metadata = IcmpErrorMetadata::read(original.opaque2());
    let Some(metadata) = metadata else {
        return Ok(IcmpError::BadRequest);
    };
    if metadata.family() != family {
        return Ok(IcmpError::BadRequest);
    }
    let bytes = original.current();
    let (destination, key, header_len, limit) = match family {
        IcmpErrorFamily::Ipv4 => {
            let Ok(header) = read_header::<Ipv4Header>(bytes, 0) else {
                return Ok(IcmpError::BadRequest);
            };
            (
                IpAddr::V4(header.source()),
                (u64::from(u32::from_ne_bytes(header.destination().octets())) << 32)
                    | u64::from(u32::from_ne_bytes(header.source().octets())),
                20,
                576,
            )
        }
        IcmpErrorFamily::Ipv6 => {
            let Ok(header) = read_header::<Ipv6Header>(bytes, 0) else {
                return Ok(IcmpError::BadRequest);
            };
            let source = u128::from(header.source());
            let destination = u128::from(header.destination());
            (
                IpAddr::V6(header.source()),
                (source as u64)
                    ^ ((source >> 64) as u64)
                    ^ (destination as u64)
                    ^ ((destination >> 64) as u64),
                40,
                1280,
            )
        }
    };
    if throttle.check(key, seed) {
        return Ok(IcmpError::Suppressed);
    }
    // SAFETY: the IP graph owns the initialized NetworkOpaque packet overlay.
    let network = unsafe { &*(original.opaque() as *const _ as *const NetworkOpaque) };
    let rx = network.sw_if_index[0];
    let interfaces = NetMain::global()?.interface_main();
    let Some(interface) = interfaces.software_interface(rx) else {
        return Ok(IcmpError::NoSource);
    };
    let interface = match interface.unnumbered_sw_if_index {
        Some(index) => match interfaces.software_interface(index) {
            Some(interface) => interface,
            None => return Ok(IcmpError::NoSource),
        },
        None => interface,
    };
    let mut source = None;
    let mut best_prefix = 0;
    for &address in &interface.addresses {
        let Some(address) = interfaces.interface_address(address) else {
            continue;
        };
        let address = address.addr();
        let prefix = match (address, destination) {
            (IpAddr::V4(address), IpAddr::V4(destination)) => {
                (u32::from(address) ^ u32::from(destination)).leading_zeros()
            }
            (IpAddr::V6(address), IpAddr::V6(destination)) => {
                if (destination.is_unicast_link_local() || destination.segments()[0] == 0xff02)
                    && !address.is_unicast_link_local()
                {
                    continue;
                }
                (u128::from(address) ^ u128::from(destination)).leading_zeros()
            }
            _ => continue,
        };
        if source.is_none() || prefix > best_prefix {
            source = Some(address);
            best_prefix = prefix;
        }
    }
    let Some(source) = source else {
        return Ok(IcmpError::NoSource);
    };
    let next = runtime.nodes().node_next_slot(
        runtime
            .current_node()
            .ok_or(RuntimeError::NodeDispatchContextMissing)?,
        lookup_slot,
    )?;
    drop(original);
    // The next frame owns the response immediately; any later failure frees it.
    let mut output = runtime.buffers().get_next_frame(next)?;
    let response = runtime.buffers().alloc_index_from(index)?;
    output
        .push_index(response)
        .expect("empty next frame accepts one response");
    let mut buffer = runtime.get_buffer_mut(response)?;
    let quote_len = buffer.current_len().min(limit - header_len - 8);
    let length = header_len + 8 + quote_len;
    buffer.truncate(quote_len)?;
    buffer.prepend_mut(header_len + 8)?.fill(0);
    let packet = buffer.current_mut();
    match (source, destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            write_ipv4_push_header(
                packet,
                source,
                destination,
                IpProtocol::Icmpv4.into(),
                length as u16,
                false,
            )?;
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            write_ipv6_push_header(
                packet,
                source,
                destination,
                IpProtocol::Icmpv6.into(),
                (length - 40) as u16,
            )?;
        }
        _ => unreachable!("source selection preserves the request address family"),
    }
    packet[header_len] = metadata.icmp_type();
    packet[header_len + 1] = metadata.code();
    packet[header_len + 4..header_len + 8].copy_from_slice(&metadata.data().to_be_bytes());
    let checksum = match family {
        IcmpErrorFamily::Ipv4 => internet_checksum(&packet[header_len..]),
        IcmpErrorFamily::Ipv6 => internet_checksum_parts(&[
            &packet[8..40],
            &((length - 40) as u32).to_be_bytes(),
            &[0, 0, 0, IpProtocol::Icmpv6.into()],
            &packet[40..],
        ]),
    };
    packet[header_len + 2..header_len + 4].copy_from_slice(&checksum.to_be_bytes());
    IcmpErrorMetadata::clear(buffer.opaque2_mut());
    // SAFETY: the response inherited the initialized IP overlay from its source.
    let network = unsafe { &mut *(buffer.opaque_mut() as *mut _ as *mut NetworkOpaque) };
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(length)
            .with_network_header(0, header_len)
            .with_transport_header(header_len, 8)
            .with_transport_payload_offset(header_len + 8),
    );
    drop(buffer);
    runtime.put_next_frame(output)?;
    Ok(match (family, metadata.icmp_type()) {
        (IcmpErrorFamily::Ipv4, 3) | (IcmpErrorFamily::Ipv6, 1) => {
            IcmpError::DestinationUnreachableSent
        }
        (IcmpErrorFamily::Ipv4, 11) | (IcmpErrorFamily::Ipv6, 3) => IcmpError::TimeExceededSent,
        (IcmpErrorFamily::Ipv4, 12) | (IcmpErrorFamily::Ipv6, 4) => IcmpError::ParameterProblemSent,
        (IcmpErrorFamily::Ipv6, 2) => IcmpError::PacketTooBigSent,
        _ => IcmpError::BadRequest,
    })
}
