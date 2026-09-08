use std::net::IpAddr;
use std::time::Duration;

use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, NodeId};
use hammer_core::error::{BufferInvariant, DataPlaneError};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_runtime::node::{NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity};
use hammer_runtime::{
    DataPlaneMain, Node, NodeProcessFn, NodeRuntimeData, RuntimeError, RuntimeResult,
};
use hammer_service::net::{NetMain, throttle::Throttle};
use hammer_service::opaque::{NetworkFlags, NetworkOffloadFlags, NetworkOpaque};

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
    fn process(&mut self, runtime: &mut DataPlaneMain, frame: &mut BufferFrame) {
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
    fn process(&mut self, runtime: &mut DataPlaneMain, frame: &mut BufferFrame) {
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

fn ip4_icmp_error(runtime: &mut DataPlaneMain, _: NodeRuntimeData, frame: &mut BufferFrame) {
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

fn ip6_icmp_error(runtime: &mut DataPlaneMain, _: NodeRuntimeData, frame: &mut BufferFrame) {
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
    runtime: &mut DataPlaneMain,
    index: u32,
    family: IcmpErrorFamily,
    throttle: &mut Throttle,
    seed: u64,
    lookup_slot: usize,
) -> RuntimeResult<IcmpError> {
    let original = runtime.buffer(index);
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
    let link_local = matches!(destination, IpAddr::V6(address)
        if address.is_unicast_link_local() || address.segments()[..2] == [0xff02, 0]);
    // ip6_sas_by_sw_if_index uses the original interface for link scope;
    // only ordinary address selection follows an unnumbered association.
    let interface = match interface.unnumbered_sw_if_index.filter(|_| !link_local) {
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
                if link_local && !address.is_unicast_link_local() {
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
    // The next frame owns the response immediately; any later failure frees it.
    let mut output = runtime.buffers().get_next_frame(next)?;
    let response = runtime.buffers().alloc_index_from(index)?;
    output
        .push_index(response)
        .expect("empty next frame accepts one response");
    let buffer = runtime.buffer_mut(response);
    let quote_len = buffer.current_len().min(limit - header_len - 8);
    let length = header_len + 8 + quote_len;
    buffer.truncate(quote_len)?;
    buffer
        .push_uninit(u8::try_from(header_len + 8).expect("IP and ICMP headers fit u8"))
        .fill(0);
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
    network.flags = NetworkFlags::LOCALLY_ORIGINATED
        | NetworkFlags::L4_CHECKSUM_COMPUTED
        | NetworkFlags::L4_CHECKSUM_CORRECT;
    network.oflags = NetworkOffloadFlags::empty();
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(length)
            .with_network_header(0, header_len)
            .with_transport_header(header_len, 8)
            .with_transport_payload_offset(header_len + 8),
    );
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

#[cfg(test)]
pub(crate) fn error_response_source_and_origin(runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    use hammer_core::data_plane::NodeKind;
    use hammer_runtime::node::NodeDescriptor;
    let interfaces = NetMain::global()?.interface_main();
    let hardware = interfaces.register_hardware_interface(0, 0, 0, 0)?;
    let rx = interfaces
        .hardware_interface(hardware)
        .unwrap()
        .sw_if_index();
    for address in ["192.0.2.1/24", "2001:db8::1/64", "fe80::1/64"] {
        interfaces.add_address(rx, address.parse().unwrap())?;
    }
    let output = runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            |runtime, _, frame| {
                assert_eq!(frame.len(), 1);
                let buffer = runtime.buffer(frame.indices()[0]);
                let packet = buffer.current();
                let network = unsafe { &*(buffer.opaque() as *const _ as *const NetworkOpaque) };
                assert!(network.flags.contains(NetworkFlags::LOCALLY_ORIGINATED));
                assert!(network.flags.contains(
                    NetworkFlags::L4_CHECKSUM_COMPUTED | NetworkFlags::L4_CHECKSUM_CORRECT
                ));
                assert!(network.oflags.is_empty());
                assert!(IcmpErrorMetadata::read(buffer.opaque2()).is_none());
                match packet[0] >> 4 {
                    4 => {
                        assert_eq!(&packet[12..16], &[192, 0, 2, 1]);
                        assert_eq!(&packet[16..20], &[192, 0, 2, 2]);
                        assert_eq!(internet_checksum(&packet[..20]), 0);
                        assert_eq!(internet_checksum(&packet[20..]), 0);
                        assert_eq!(&packet[20..22], &[11, 0]);
                        assert_eq!(packet[28] >> 4, 4);
                    }
                    6 => {
                        assert_eq!(
                            &packet[8..24],
                            &"fe80::1".parse::<std::net::Ipv6Addr>().unwrap().octets()
                        );
                        assert_eq!(
                            &packet[24..40],
                            &"fe80::2".parse::<std::net::Ipv6Addr>().unwrap().octets()
                        );
                        assert_eq!(
                            internet_checksum_parts(&[
                                &packet[8..40],
                                &((packet.len() - 40) as u32).to_be_bytes(),
                                &[0, 0, 0, 58],
                                &packet[40..],
                            ]),
                            0
                        );
                        assert_eq!(&packet[40..42], &[3, 0]);
                        assert_eq!(packet[48] >> 4, 6);
                    }
                    version => panic!("unexpected response IP version {version}"),
                }
            },
            NodeRuntimeData::empty(),
            None,
            &[],
            None,
        ),
    )?;
    for (family, node, metadata) in [
        (
            IcmpErrorFamily::Ipv4,
            register_ip4_icmp_error(runtime)?,
            IcmpErrorMetadata::ipv4_time_exceeded(),
        ),
        (
            IcmpErrorFamily::Ipv6,
            register_ip6_icmp_error(runtime)?,
            IcmpErrorMetadata::ipv6_time_exceeded(),
        ),
    ] {
        runtime.nodes().set_node_next_slot(node, 1, output)?;
        let mut packet = vec![
            0;
            if family == IcmpErrorFamily::Ipv4 {
                28
            } else {
                48
            }
        ];
        if family == IcmpErrorFamily::Ipv4 {
            packet[0] = 0x45;
            packet[2..4].copy_from_slice(&28u16.to_be_bytes());
            packet[9] = 17;
            packet[12..16].copy_from_slice(&[192, 0, 2, 2]);
            packet[16..20].copy_from_slice(&[198, 51, 100, 1]);
        } else {
            packet[0] = 0x60;
            packet[4..6].copy_from_slice(&8u16.to_be_bytes());
            packet[6] = 17;
            packet[8..24]
                .copy_from_slice(&"fe80::2".parse::<std::net::Ipv6Addr>().unwrap().octets());
            packet[24..40].copy_from_slice(
                &"2001:db8::2"
                    .parse::<std::net::Ipv6Addr>()
                    .unwrap()
                    .octets(),
            );
        }
        let mut frame = runtime.buffers().get_next_frame(node)?;
        let index = runtime.alloc_index_with_bytes(&packet)?;
        frame.push_index(index)?;
        {
            let mut buffer = runtime.buffer_mut(index);
            let mut network = NetworkOpaque::default();
            network.sw_if_index[0] = rx;
            network.oflags = NetworkOffloadFlags::UDP_CHECKSUM;
            unsafe { (buffer.opaque_mut() as *mut _ as *mut NetworkOpaque).write(network) };
            metadata.write(buffer.opaque2_mut());
        }
        let mut throttle = Throttle::new(Duration::from_millis(1));
        let seed = throttle.seed(Duration::from_secs(1));
        let error = runtime.with_current_node(node, || {
            generate_error(runtime, index, family, &mut throttle, seed, 1)
        })?;
        assert!(matches!(error, IcmpError::TimeExceededSent));
        let error = runtime.with_current_node(node, || {
            generate_error(runtime, index, family, &mut throttle, seed, 1)
        })?;
        assert!(matches!(error, IcmpError::Suppressed));
        assert_eq!(runtime.run_ready_nodes()?, 1);
        assert_eq!(runtime.buffer(index).current(), packet);
        drop(frame);
        assert_eq!(runtime.buffers().in_use_buffers(), 0);
    }
    interfaces.delete_hardware_interface(hardware)?;
    Ok(())
}
