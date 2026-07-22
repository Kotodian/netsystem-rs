//! IP reassembly ownership: VPP-aligned Memory Owner / Sendout / bihash directory.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hammer_core::data_plane::{NodeHandle, NodeId, NodeNext};
use hammer_infra::pool::Index as PoolIndex;
use hammer_plugin_ip::protocol::ip::IpFragmentKey;
use hammer_plugin_ip::{
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode,
    pack_fragment_owner_value, unpack_fragment_owner_value,
};
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId,
};

fn test_runtime() -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 256,
            buffer_slots: 64,
            frame_slots: 16,
            ..DataPlaneBufferConfig::default()
        },
        ..DataPlaneRuntimeConfig::default()
    })
}

#[test]
fn fragment_owner_bihash_value_packs_context_index_and_memory_owner() {
    let index = PoolIndex::new(17, 3);
    let owner = DataWorkerId::new(5);
    let packed = pack_fragment_owner_value(index, owner);
    let (got_index, got_owner) = unpack_fragment_owner_value(packed);
    assert_eq!(got_index, index);
    assert_eq!(got_owner, owner);
}

#[test]
fn ip_reassembly_next_is_input_or_drop_not_lookup() {
    assert_eq!(NodeNext::slot(IpReassemblyNext::Input), 0);
    assert_eq!(NodeNext::slot(IpReassemblyNext::Drop), 1);
}

#[test]
fn fragment_owner_directory_first_writer_wins_on_shared_bihash() {
    let directory = IpReassemblyDirectory::new(64);
    let key = IpFragmentKey::V4 {
        source: Ipv4Addr::new(10, 0, 0, 1),
        destination: Ipv4Addr::new(10, 0, 0, 2),
        protocol: 6,
        identification: 9,
    };
    let worker0 = DataWorkerId::new(0);
    let worker1 = DataWorkerId::new(1);
    let index = PoolIndex::new(1, 1);

    let (owner, created) = directory.claim_or_lookup(key, index, worker0);
    assert!(created);
    assert_eq!(owner, worker0);

    let (owner_again, created_again) =
        directory.claim_or_lookup(key, PoolIndex::new(2, 1), worker1);
    assert!(!created_again);
    assert_eq!(owner_again, worker0);

    directory.remove(key);
    assert!(directory.lookup(key).is_none());
}

#[test]
fn ip_reassembly_handoff_targets_input_not_lookup() {
    let directory = IpReassemblyDirectory::new(16);
    let handoff = IpReassemblyHandoff::new(
        NodeHandle::new(1),
        NodeHandle::new(2),
        DataWorkerId::new(0),
        directory,
    );
    assert_eq!(handoff.input(), NodeHandle::new(2));
    assert_eq!(handoff.reassembly(), NodeHandle::new(1));
}

#[test]
fn failed_reassembly_does_not_sticky_deny_same_key() {
    let directory = IpReassemblyDirectory::new(16);
    let key = IpFragmentKey::V4 {
        source: Ipv4Addr::new(1, 1, 1, 1),
        destination: Ipv4Addr::new(2, 2, 2, 2),
        protocol: 17,
        identification: 1,
    };
    let worker = DataWorkerId::new(0);
    let _ = directory.claim_or_lookup(key, PoolIndex::new(1, 1), worker);
    directory.remove(key);
    let (owner, created) = directory.claim_or_lookup(key, PoolIndex::new(3, 1), worker);
    assert!(created);
    assert_eq!(owner, worker);
}

#[test]
fn expire_on_fresh_node_is_safe_without_global_mutex_runtime() {
    let runtime = test_runtime();
    let directory = Arc::new(IpReassemblyDirectory::new(32));
    let mut node = IpReassemblyNode::new([NodeId::new(0); IpReassemblyNext::COUNT])
        .with_timeout(Duration::from_millis(1))
        .with_directory(Arc::clone(&directory));
    let expired = node.expire(&runtime, Instant::now());
    assert_eq!(expired, 0);
}
