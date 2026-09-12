//! Behavior tests for the SVM hash map (ADR-0011 section 12.5).
//!
//! Slots and key bytes live in an SVM region heap, so every test builds an
//! arena and a heap explicitly. Semantics are checked against
//! `std::collections::HashMap` on the same operation sequence, because
//! matching that behavior is the contract the table is specified to keep.

use std::collections::HashMap;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use hammer_infra::svm::hash_map::{SvmEntry, SvmHashMap};
use hammer_infra::svm::region_heap::SvmRegionHeap;

const HEAP_START: u64 = 64;
const HEAP_SIZE: u64 = 1024 * 1024;
const MIN_BUCKETS: u64 = 64;

fn arena() -> Vec<u8> {
    vec![0u8; (HEAP_START + HEAP_SIZE) as usize]
}

fn table<V>() -> (Vec<u8>, SvmRegionHeap, SvmHashMap<V>) {
    let mut arena = arena();
    let mut heap = SvmRegionHeap::new();
    heap.initialize(&mut arena, HEAP_START, HEAP_START + HEAP_SIZE);
    (arena, heap, SvmHashMap::new())
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Bucket count implied by the reported element capacity.
fn buckets_of(capacity: usize) -> u64 {
    capacity as u64 * 4 / 3
}

#[test]
fn random_operations_match_std_hash_map() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    let mut reference: HashMap<String, u64> = HashMap::new();
    let mut state = 0x243f_6a88_85a3_08d3u64;

    for step in 0..4000 {
        let key = format!("service-{}", next_random(&mut state) % 96);
        match next_random(&mut state) % 5 {
            0 | 1 => {
                let value = next_random(&mut state);
                assert_eq!(
                    map.insert(&mut heap, &mut arena, &key, value),
                    reference.insert(key.clone(), value),
                    "insert {key} at step {step}"
                );
            }
            2 => {
                assert_eq!(
                    map.remove(&mut heap, &mut arena, &key),
                    reference.remove(&key),
                    "remove {key} at step {step}"
                );
            }
            3 => {
                assert_eq!(
                    map.get(&heap, &arena, &key).copied(),
                    reference.get(&key).copied(),
                    "get {key} at step {step}"
                );
            }
            _ => {
                assert_eq!(
                    map.contains_key(&heap, &arena, &key),
                    reference.contains_key(&key),
                    "contains {key} at step {step}"
                );
            }
        }

        assert_eq!(map.len(), reference.len(), "len at step {step}");
        assert_eq!(map.is_empty(), reference.is_empty(), "empty at step {step}");
        assert!(map.len() <= map.capacity(), "capacity at step {step}");
    }

    let mut stored: Vec<(String, u64)> = Vec::new();
    for (key, value) in map.iter(&heap, &arena) {
        stored.push((key.to_owned(), *value));
    }
    let mut expected: Vec<(String, u64)> = Vec::new();
    for (key, value) in reference.iter() {
        expected.push((key.clone(), *value));
    }
    stored.sort();
    expected.sort();
    assert_eq!(stored, expected, "final contents");
}

#[test]
fn keys_and_values_cover_every_entry() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for index in 0..200u64 {
        let key = format!("iface-{}", next_random(&mut state) % 128);
        map.insert(&mut heap, &mut arena, &key, index);
    }

    let mut keys: Vec<&str> = map.keys(&heap, &arena).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), map.len());

    let mut values: Vec<u64> = map.values(&heap, &arena).copied().collect();
    values.sort_unstable();
    let mut keyed: Vec<u64> = Vec::new();
    for (key, value) in map.iter(&heap, &arena) {
        assert!(map.contains_key(&heap, &arena, key));
        assert_eq!(map.get(&heap, &arena, key), Some(value));
        keyed.push(*value);
    }
    keyed.sort_unstable();
    assert_eq!(values, keyed);
}

#[test]
fn get_key_value_returns_the_stored_key() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    map.insert(&mut heap, &mut arena, "gre", 7);

    let (key, value) = map
        .get_key_value(&heap, &arena, "gre")
        .expect("stored entry");
    assert_eq!(key, "gre");
    assert_eq!(*value, 7);
    assert!(map.get_key_value(&heap, &arena, "gretap").is_none());
}

#[test]
fn table_owns_its_key_bytes() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    let mut owned = String::from("tap-0");
    map.insert(&mut heap, &mut arena, &owned, 1);
    owned.push_str("-changed");

    assert_eq!(map.get(&heap, &arena, "tap-0").copied(), Some(1));
    assert_eq!(map.get(&heap, &arena, &owned).copied(), None);
    assert_eq!(map.get(&heap, &arena, "tap-0").copied(), Some(1));
}

#[test]
fn zero_length_and_long_keys_round_trip() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    let long = "x".repeat(4096);

    map.insert(&mut heap, &mut arena, "", 1);
    map.insert(&mut heap, &mut arena, &long, 2);
    assert_eq!(map.get(&heap, &arena, "").copied(), Some(1));
    assert_eq!(map.get(&heap, &arena, &long).copied(), Some(2));
    assert_eq!(map.len(), 2);

    assert_eq!(map.remove(&mut heap, &mut arena, ""), Some(1));
    assert_eq!(map.remove(&mut heap, &mut arena, &long), Some(2));
    assert!(map.is_empty());
}

#[test]
fn values_mut_and_get_mut_update_stored_values() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    for index in 0..20u64 {
        map.insert(&mut heap, &mut arena, &format!("node-{index}"), index);
    }

    for value in map.values_mut(&mut heap, &mut arena) {
        *value += 100;
    }
    let mut total = 0;
    for value in map.values(&heap, &arena) {
        total += *value;
    }
    assert_eq!(total, (0..20u64).sum::<u64>() + 2000);

    let node = map
        .get_mut(&mut heap, &mut arena, "node-3")
        .expect("stored entry");
    *node = 5;
    assert_eq!(map.get(&heap, &arena, "node-3").copied(), Some(5));
}

#[test]
fn entry_matches_std_or_insert() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    let mut reference: HashMap<String, u64> = HashMap::new();
    let mut state = 0x0bad_c0de_dead_beefu64;

    for step in 0..600 {
        let key = format!("peer-{}", next_random(&mut state) % 64);
        let value = next_random(&mut state) % 1000;

        let ours = *map.entry(&mut heap, &mut arena, &key).or_insert(value);
        let theirs = *reference.entry(key.clone()).or_insert(value);
        assert_eq!(ours, theirs, "or_insert {key} at step {step}");

        *map.entry(&mut heap, &mut arena, &key).or_insert(0) += 1;
        *reference.entry(key.clone()).or_insert(0) += 1;

        assert_eq!(map.len(), reference.len(), "len at step {step}");
    }
}

#[test]
fn occupied_entry_operations_update_the_table() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    map.insert(&mut heap, &mut arena, "route", 10);

    match map.entry(&mut heap, &mut arena, "route") {
        SvmEntry::Occupied(mut entry) => {
            assert_eq!(entry.key(), "route");
            assert_eq!(*entry.get(), 10);
            *entry.get_mut() += 1;
            assert_eq!(entry.insert(30), 11);
            assert_eq!(*entry.into_mut(), 30);
        }
        SvmEntry::Vacant(_) => panic!("stored key reported as vacant"),
    }
    assert_eq!(map.get(&heap, &arena, "route").copied(), Some(30));

    match map.entry(&mut heap, &mut arena, "route") {
        SvmEntry::Occupied(entry) => assert_eq!(entry.remove(), 30),
        SvmEntry::Vacant(_) => panic!("stored key reported as vacant"),
    }
    assert!(map.get(&heap, &arena, "route").is_none());
    assert!(map.is_empty());
}

#[test]
fn vacant_entry_that_is_dropped_leaves_no_allocation() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    map.insert(&mut heap, &mut arena, "present", 1);
    let used = heap.used_bytes();

    match map.entry(&mut heap, &mut arena, "absent") {
        SvmEntry::Vacant(entry) => assert_eq!(entry.key(), "absent"),
        SvmEntry::Occupied(_) => panic!("absent key reported as occupied"),
    }

    assert_eq!(
        heap.used_bytes(),
        used,
        "a vacant entry that is dropped must not allocate"
    );
    assert!(!map.contains_key(&heap, &arena, "absent"));
}

#[test]
fn vacant_entry_insert_stores_the_key() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    match map.entry(&mut heap, &mut arena, "fresh") {
        SvmEntry::Vacant(entry) => {
            let value = entry.insert(9);
            *value += 1;
        }
        SvmEntry::Occupied(_) => panic!("absent key reported as occupied"),
    }
    assert_eq!(map.get(&heap, &arena, "fresh").copied(), Some(10));
    assert_eq!(map.len(), 1);
}

#[test]
fn repeated_insert_and_remove_does_not_leak_key_blocks() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    map.insert(&mut heap, &mut arena, "churn", 1);
    map.remove(&mut heap, &mut arena, "churn");
    let baseline = heap.used_bytes();

    for round in 0..200u64 {
        assert_eq!(map.insert(&mut heap, &mut arena, "churn", round), None);
        assert_eq!(map.remove(&mut heap, &mut arena, "churn"), Some(round));
    }

    assert_eq!(heap.used_bytes(), baseline, "every key block was released");
    assert_eq!(map.len(), 0);
    assert_eq!(map.capacity(), (MIN_BUCKETS * 3 / 4) as usize);
}

#[test]
fn capacity_grows_before_the_bucket_array_fills() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    map.insert(&mut heap, &mut arena, "first", 0);
    let initial = map.capacity();
    assert_eq!(initial, (MIN_BUCKETS * 3 / 4) as usize);

    let mut index = 0u64;
    while map.capacity() == initial {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
        index += 1;
        assert!(index < 1000, "capacity never grew");
    }
    assert_eq!(initial, (MIN_BUCKETS * 3 / 4) as usize);
    assert_eq!(map.capacity(), (buckets_of(initial) * 2 * 3 / 4) as usize);
    assert!(map.len() < map.capacity());
}

#[test]
fn with_capacity_avoids_growth_for_the_reserved_entries() {
    let (mut arena, mut heap, _) = table::<u64>();
    let mut map = SvmHashMap::<u64>::with_capacity(&mut heap, &mut arena, 500);
    assert!(map.capacity() >= 500);
    let reserved = map.capacity();

    for index in 0..500u64 {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
    }
    assert_eq!(map.capacity(), reserved, "no growth within the reservation");
    assert_eq!(map.len(), 500);

    map.reserve(&mut heap, &mut arena, 2000);
    assert!(map.capacity() >= 2500);
    let reserved = map.capacity();
    for index in 500..2500u64 {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
    }
    assert_eq!(map.capacity(), reserved);
    assert_eq!(map.len(), 2500);
}

#[test]
fn removal_shrinks_the_bucket_array() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    for index in 0..400u64 {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
    }
    let grown = map.capacity();

    for index in 0..380u64 {
        map.remove(&mut heap, &mut arena, &format!("entry-{index}"));
    }
    assert_eq!(map.len(), 20);
    assert!(map.capacity() < grown, "capacity {grown} did not shrink");
    assert!(map.len() <= map.capacity());

    for index in 0..20u64 {
        let key = format!("entry-{}", index + 380);
        assert_eq!(map.get(&heap, &arena, &key).copied(), Some(index + 380));
    }
}

#[test]
fn shrink_to_fit_matches_the_stored_entries() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    for index in 0..900u64 {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
    }
    for index in 0..880u64 {
        map.remove(&mut heap, &mut arena, &format!("entry-{index}"));
    }
    let before = map.capacity();

    map.shrink_to_fit(&mut heap, &mut arena);
    assert!(map.capacity() <= before);
    assert!(map.capacity() >= map.len());

    let mut stored: Vec<(&str, u64)> = map.iter(&heap, &arena).map(|(k, v)| (k, *v)).collect();
    stored.sort_unstable();
    assert_eq!(stored.len(), 20);
    assert_eq!(stored[0].1, 880);
    assert_eq!(stored[19].1, 899);
}

#[test]
fn clear_empties_the_table_but_keeps_the_bucket_array() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    for index in 0..120u64 {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
    }
    let capacity = map.capacity();
    map.clear(&mut heap, &mut arena);

    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
    assert_eq!(map.capacity(), capacity, "clear keeps the bucket array");
    assert_eq!(map.iter(&heap, &arena).count(), 0);
    assert!(map.get(&heap, &arena, "entry-1").is_none());

    for index in 0..120u64 {
        map.insert(&mut heap, &mut arena, &format!("entry-{index}"), index);
    }
    assert_eq!(map.len(), 120);
    assert_eq!(map.get(&heap, &arena, "entry-119").copied(), Some(119));
}

#[test]
fn tombstones_are_reused_before_growing() {
    let (mut arena, mut heap, mut map) = table::<u64>();
    for index in 0..40u64 {
        map.insert(&mut heap, &mut arena, &format!("old-{index}"), index);
    }
    let capacity = map.capacity();
    for index in 0..40u64 {
        map.remove(&mut heap, &mut arena, &format!("old-{index}"));
    }

    for index in 0..40u64 {
        map.insert(&mut heap, &mut arena, &format!("new-{index}"), index);
    }
    assert_eq!(map.capacity(), capacity, "tombstones were reused");
    assert_eq!(map.len(), 40);
    for index in 0..40u64 {
        let key = format!("new-{index}");
        assert_eq!(map.get(&heap, &arena, &key).copied(), Some(index));
    }
}

#[test]
fn u128_values_exercises_slot_alignment() {
    let (mut arena, mut heap, mut map) = table::<u128>();
    for index in 0..40u128 {
        map.insert(
            &mut heap,
            &mut arena,
            &format!("wide-{index}"),
            1u128 << index,
        );
    }
    for index in 0..40u128 {
        let key = format!("wide-{index}");
        assert_eq!(map.get(&heap, &arena, &key).copied(), Some(1u128 << index));
    }
    assert_eq!(map.iter(&heap, &arena).count(), 40);
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
struct Route {
    prefix: u32,
    length: u8,
    flags: [u8; 3],
    nexthop: u64,
}

#[test]
fn compound_values_match_std_hash_map() {
    let (mut arena, mut heap, mut map) = table::<Route>();
    let mut reference: HashMap<String, Route> = HashMap::new();

    for index in 0..120u32 {
        let key = format!("route-{index}");
        let route = Route {
            prefix: index << 8,
            length: 24,
            flags: [1, 2, 3],
            nexthop: u64::from(index) * 7,
        };
        assert_eq!(
            map.insert(&mut heap, &mut arena, &key, route),
            reference.insert(key.clone(), route)
        );
    }
    for index in 0..60u32 {
        let key = format!("route-{index}");
        assert_eq!(
            map.remove(&mut heap, &mut arena, &key),
            reference.remove(&key)
        );
    }

    assert_eq!(map.len(), reference.len());
    for (key, route) in map.iter(&heap, &arena) {
        assert_eq!(reference.get(key), Some(route));
    }
}
