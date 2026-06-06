use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use hammer_adapter::{DataPlaneRuntime, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_service::data_plane::DropNode;
use hammer_service::interface::{
    InterfaceConnectedRouteControl, InterfaceControlPlane, InterfaceMtu,
    InterfaceOutputControlPlane,
};
use hammer_service::net::{
    AdjacencyRewriteNode, FibTableBuilder, IcmpEchoRequestNext, IcmpEchoRequestNode,
    IcmpInputControlPlane, IpInputNext, IpInputNode, IpLocalArc, IpLocalControlPlane, IpLocalNext,
    IpLookupControlPlane, IpVersion,
};
use hammer_service::tun::{
    DeviceMain, DriverScheduleMode, RealTunInput, RealTunOpen, RealTunOpenOptions, RealTunOutput,
    TunDeviceEventSource, TunFdIo, TunInputDriverNode, TunOutputDriverNode,
};
use ipnet::{IpNet, Ipv4Net};

const HAMMER_IP: Ipv4Addr = Ipv4Addr::new(10, 255, 0, 1);
const HOST_IP: Ipv4Addr = Ipv4Addr::new(10, 255, 0, 2);
const PREFIX_LEN: u8 = 24;
const MTU: u32 = 1500;
const LOOP_IDLE_SLEEP: Duration = Duration::from_millis(5);
pub const TEST_TIMEOUT: Duration = Duration::from_secs(30);

struct HostPingGraph {
    runtime: DataPlaneRuntime,
    tun_io: TunFdIo,
    events: TunDeviceEventSource,
    interface_name: String,
    output_node: NodeId,
}

fn build_host_ping_graph(requested_name: Option<&str>) -> CoreResult<HostPingGraph> {
    let options = match requested_name {
        Some(name) => RealTunOpenOptions::new(MTU as usize).with_requested_name(name),
        None => RealTunOpenOptions::new(MTU as usize),
    };
    let tun = RealTunOpen::open(options)?;
    let interface_name = tun.name().to_owned();
    let tun_io = tun.into_io()?;

    let runtime = DataPlaneRuntime::with_capacities(2048, 64, 32, 16);
    let device_main = DeviceMain::new();
    let drop = runtime.nodes().register_internal(DropNode::new());
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(tun_io.clone())));
    let tx_queue = device_main.register_tx_queue(output);
    let interface_output_control = InterfaceOutputControlPlane::new();
    let interface_output = runtime
        .nodes()
        .register_internal(interface_output_control.node());

    let lookup_control = IpLookupControlPlane::new(FibTableBuilder::new(drop).build());
    let lookup = runtime.nodes().register_internal(lookup_control.node());
    let adjacency_rewrite = runtime
        .nodes()
        .register_internal(AdjacencyRewriteNode::new(lookup_control.table_handle()));

    let local_control =
        IpLocalControlPlane::new(IpLocalNext::nodes(drop, drop, drop, drop, drop, drop));
    runtime.nodes().register_internal(local_control.node());
    let receive = runtime
        .nodes()
        .register_internal(local_control.receive_node());
    let icmp_echo =
        runtime
            .nodes()
            .register_internal(IcmpEchoRequestNode::new(IcmpEchoRequestNext::nodes(
                lookup, drop,
            )));
    let icmp_control = IcmpInputControlPlane::new(drop);
    icmp_control.register_type(IpVersion::V4, 8, icmp_echo)?;
    let icmp_input = runtime.nodes().register_internal(icmp_control.node());
    local_control.register_protocol(1, icmp_input)?;

    let interface_control = InterfaceControlPlane::new().with_connected_routes(
        InterfaceConnectedRouteControl::new(lookup_control.table_handle(), drop, receive)
            .with_connected_adjacency(adjacency_rewrite, interface_output),
    );
    let tun_if = interface_control
        .register_interface_with_mtu(interface_name.clone(), InterfaceMtu::new(MTU, MTU, 0, 0))?;
    interface_output_control.register_tx(tun_if, output)?;
    interface_control.add_address(
        tun_if,
        IpNet::V4(Ipv4Net::new(HAMMER_IP, PREFIX_LEN).expect("Hammer-side TUN address")),
    )?;

    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpLocalArc>::new(IpInputNext::nodes(
            drop, drop, drop, lookup, drop, drop, drop,
        )));
    let input_node = TunInputDriverNode::new(
        RealTunInput::new(tun_io.clone()),
        interface_name.clone(),
        ip_input,
    )
    .with_interface_control(interface_control.handle());
    let input = runtime.nodes().register_driver(input_node.clone());
    let rx_queue = device_main.register_rx_queue(input, DriverScheduleMode::Interrupt);
    input_node.bind_rx_queue(device_main.clone(), rx_queue)?;
    let events = TunDeviceEventSource::new(device_main, Some(rx_queue), Some(tx_queue));

    Ok(HostPingGraph {
        runtime,
        tun_io,
        events,
        interface_name,
        output_node: output,
    })
}

pub async fn run_manual_harness(timeout: Option<Duration>) -> CoreResult<bool> {
    let requested_name = std::env::var("HAMMER_REAL_TUN_NAME").ok();
    let graph = build_host_ping_graph(requested_name.as_deref())?;
    print_host_commands(&graph.interface_name);
    println!("start ping from the host after configuring the interface");
    run_readiness_loop(&graph, timeout).await
}

async fn run_readiness_loop(graph: &HostPingGraph, timeout: Option<Duration>) -> CoreResult<bool> {
    let started = Instant::now();
    let mut observed_tx = false;
    let mut last_tx_vectors = tun_output_vectors(graph);
    loop {
        if let Some(timeout) = timeout
            && started.elapsed() >= timeout
        {
            println!("host-ping harness timed out after {timeout:?}");
            return Ok(observed_tx);
        }

        let mut should_flush_writable = false;
        tokio::select! {
            result = graph.tun_io.readable() => {
                result?;
                graph.events.schedule_readable(&graph.runtime)?;
                should_flush_writable = true;
            }
            result = tokio::signal::ctrl_c(), if timeout.is_none() => {
                result.map_err(|err| CoreError::internal(format!("wait for Ctrl-C: {err}")))?;
                println!("host-ping harness stopped");
                return Ok(observed_tx);
            }
            _ = tokio::time::sleep(LOOP_IDLE_SLEEP) => {}
        }

        let ran = graph.runtime.run_ready_nodes()?;
        observe_tun_output_vectors(graph, &mut last_tx_vectors, &mut observed_tx);
        if observed_tx && timeout.is_some() {
            return Ok(true);
        }

        if should_flush_writable {
            schedule_writable_once(graph).await?;
            graph.runtime.run_ready_nodes()?;
            observe_tun_output_vectors(graph, &mut last_tx_vectors, &mut observed_tx);
            if observed_tx && timeout.is_some() {
                return Ok(true);
            }
        } else if ran == 0 {
            tokio::time::sleep(LOOP_IDLE_SLEEP).await;
        }
    }
}

async fn schedule_writable_once(graph: &HostPingGraph) -> CoreResult<()> {
    tokio::select! {
        result = graph.tun_io.writable() => {
            result?;
            graph.events.schedule_writable(&graph.runtime)?;
        }
        _ = tokio::time::sleep(LOOP_IDLE_SLEEP) => {}
    }
    Ok(())
}

fn observe_tun_output_vectors(
    graph: &HostPingGraph,
    last_tx_vectors: &mut u64,
    observed_tx: &mut bool,
) {
    let tx_vectors = tun_output_vectors(graph);
    if tx_vectors > *last_tx_vectors {
        *last_tx_vectors = tx_vectors;
        *observed_tx = true;
        println!("observed tun-output packet vectors: {tx_vectors}");
    }
}

fn tun_output_vectors(graph: &HostPingGraph) -> u64 {
    graph
        .runtime
        .nodes()
        .node_runtime_stats_snapshot()
        .into_iter()
        .find(|row| row.node_id == graph.output_node)
        .map(|row| row.vectors)
        .unwrap_or(0)
}

pub fn env_timeout() -> Option<Duration> {
    std::env::var("HAMMER_REAL_TUN_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn print_host_commands(interface_name: &str) {
    println!("TUN interface: {interface_name}");
    println!("Hammer/VPP-side TUN address: {HAMMER_IP}/{PREFIX_LEN}");
    println!("macOS:");
    println!("  sudo ifconfig {interface_name} inet {HOST_IP} {HAMMER_IP} up");
    println!("  ping -n -c 3 -S {HOST_IP} {HAMMER_IP}");
    println!("Linux:");
    println!("  sudo ip addr add {HOST_IP}/{PREFIX_LEN} dev {interface_name}");
    println!("  sudo ip link set {interface_name} up mtu {MTU}");
    println!("  ping -n -c 3 -I {HOST_IP} {HAMMER_IP}");
}

#[cfg(not(test))]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run_manual_harness(env_timeout()).await {
        eprintln!("host-ping harness failed: {err}");
        std::process::exit(1);
    }
}
