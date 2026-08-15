//! IP reassembly expiry walk driven as a main-thread Process Node.
//!
//! The engine is built through the same configure -> load -> start sequence
//! as `EnginePool::main_loop_enter` (engine.rs:874-885), and the walk is
//! driven on the main-thread LocalSet while the daemon service future sleeps.
//! The walk's expiry work is observed through `IpReassemblyNode::expire`, the
//! public owner seam that runs the same worker expiry on the same slot: a
//! half-open context seeded before the drive must be gone afterwards.

use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use hammer_core::data_plane::BufferFrame;
use hammer_infra::checksum::internet_checksum;
use hammer_plugin_ip::IpReassemblyNode;
use hammer_runtime::config::Worker;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, Node, NodeRuntimeData,
    RuntimeRegistry, RuntimeResult,
};

/// Pool capacity 1 makes every expiry walk a full sweep (the walk cursor wraps
/// back to slot 0), so the before/after observations below do not depend on
/// the incremental walk window.
const REASSEMBLY_CONFIG: &str =
    "[network.ip.reassembly]\ntimeout = \"100ms\"\nmax_reassemblies = 1\n";

#[test]
fn reassembly_expiry_walk_is_driven_at_configured_cadence_by_the_daemon() -> RuntimeResult<()> {
    hammer_service::reset_subsystem_mains_for_plugin_test();
    hammer_plugin_ip::reset_ip_main_for_test();

    // Production-shaped engine: the daemon constructs the Engine from a Worker
    // inventory, then materializes the service and IP plugin registrations
    // through the early-config and plugin-loading steps.
    let registry = RuntimeRegistry::new();
    let mut engine = Engine::new_configured(registry, Worker::default())?;
    engine
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    let plugin = hammer_plugin_ip::plugin_module();
    engine
        .plugin_main_mut()
        .register_builtin_image(plugin.registration_image().get());
    engine.configure_early(REASSEMBLY_CONFIG)?;
    engine.load_plugins(&[], REASSEMBLY_CONFIG)?;
    engine.start_process_nodes()?;

    let walk = engine
        .process_handle("ip-reassembly-expire-walk")
        .expect("registered reassembly expiry walk");

    // Seed one half-open IPv4 reassembly through the public data path: a
    // single non-final fragment lands in worker slot 0 of the same global
    // main the walk expires on each wake.
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 256,
            buffer_slots: 8,
            frame_slots: 8,
            ..DataPlaneBufferConfig::default()
        },
        ..DataPlaneRuntimeConfig::default()
    });
    let mut frame = BufferFrame::with_capacity(8);
    let index = runtime
        .alloc_index_with_bytes(&ipv4_fragment_packet())
        .expect("alloc fragment buffer");
    frame.push_index(index).expect("push fragment");
    let process = IpReassemblyNode::new().node_process();
    let _ = process(&runtime, NodeRuntimeData::empty(), &mut frame);
    assert_eq!(frame.len(), 0, "seeded fragment was consumed");

    // The seam that runs the same worker expiry the walk invokes: a fresh
    // half-open context is left alone ...
    let mut node = IpReassemblyNode::new();
    assert_eq!(
        node.expire(&runtime, Instant::now()),
        0,
        "fresh half-open context is not expired"
    );

    // Daemon-shaped drive: while the daemon service future is dormant, the
    // walk is polled on its clock cadence. By the end of the drive the seeded
    // context is older than the 100ms timeout, and the walk is the only
    // reaper of this slot, so the walk must have expired it.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("main runtime");
    engine.run_processes_until(&rt, async {
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    walk.signal(1, 0).expect("walk is alive after being driven");

    // The walk removed the stale context: had it never run its expiry sweep
    // (or run on a slower cadence than the drive), this probe would return 1
    // exactly like the control below.
    assert_eq!(
        node.expire(&runtime, Instant::now() + Duration::from_secs(2)),
        0,
        "the walk expired the seeded half-open context"
    );

    // Control: the seam still detects a stale context, so the probe above was
    // zero because the walk removed it, not because the seam or the seeding
    // silently stopped working.
    let mut control_frame = BufferFrame::with_capacity(8);
    let control_index = runtime
        .alloc_index_with_bytes(&ipv4_fragment_packet())
        .expect("alloc control fragment buffer");
    control_frame
        .push_index(control_index)
        .expect("push control fragment");
    let _ = process(&runtime, NodeRuntimeData::empty(), &mut control_frame);
    assert_eq!(control_frame.len(), 0, "control fragment was consumed");
    assert_eq!(
        node.expire(&runtime, Instant::now() + Duration::from_secs(2)),
        1,
        "control half-open context is expired by the worker seam"
    );

    // Shutdown aborts and joins the walk; nothing polls afterwards.
    engine.shutdown_process_nodes(&rt)?;
    assert!(
        walk.signal(1, 0).is_err(),
        "shutdown aborts and joins the walk"
    );
    Ok(())
}

/// A single non-final IPv4 fragment (MF set, offset 0) of a UDP flow, with a
/// header checksum computed over the zeroed field by the same routine the
/// data path validates with (protocol/ip.rs:350).
fn ipv4_fragment_packet() -> Vec<u8> {
    let mut packet = vec![0u8; 20 + 8];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&((20 + 8) as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[6..8].copy_from_slice(&0x2000u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 1).octets());
    packet[16..20].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 2).octets());
    packet[10..12].copy_from_slice(&[0, 0]);
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    packet
}
