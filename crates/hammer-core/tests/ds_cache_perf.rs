use std::hint::black_box;
use std::time::{Duration, Instant};

use hammer_core::ds::{FlatHashTable, MtrieEntry, PackedMtrie};

const HASH_ENTRY_COUNT: usize = 256 * 1024;
const ROUTE_COUNT: usize = 32 * 1024;
const LOOKUP_COUNT: usize = 512 * 1024;
const PREFETCH_DISTANCE: usize = 16;
const CACHE_EVICT_BYTES: usize = 64 * 1024 * 1024;

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-core --release --test ds_cache_perf -- --ignored --nocapture`"]
fn flat_hash_prefetch_cache_probe() {
    let (table, keys) = build_flat_hash_probe();
    let mut eviction = vec![0u8; CACHE_EVICT_BYTES];

    evict_cache(&mut eviction);
    let no_prefetch = measure_hash_lookup(&table, &keys);

    evict_cache(&mut eviction);
    let prefetch = measure_hash_lookup_with_prefetch(&table, &keys);

    evict_cache(&mut eviction);
    let hot = measure_hash_hot_lookup(&table, keys[0], keys.len());

    assert_eq!(no_prefetch.checksum, prefetch.checksum);
    assert_eq!(no_prefetch.lookups, LOOKUP_COUNT);
    assert_eq!(prefetch.lookups, LOOKUP_COUNT);
    assert_eq!(hot.lookups, LOOKUP_COUNT);

    eprintln!(
        "flat_hash cache probe: entries={HASH_ENTRY_COUNT} buckets={} lookups={LOOKUP_COUNT} prefetch_distance={PREFETCH_DISTANCE}",
        table.bucket_count()
    );
    print_probe_stats(no_prefetch, prefetch, hot);

    if !cfg!(debug_assertions) {
        assert!(
            hot.ns_per_lookup() < no_prefetch.ns_per_lookup(),
            "hot lookup should be faster than the randomized working-set lookup"
        );
    }
}

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-core --release --test ds_cache_perf -- --ignored --nocapture`"]
fn packed_mtrie_prefetch_cache_probe() {
    let (trie, keys) = build_packed_mtrie_probe();
    let mut eviction = vec![0u8; CACHE_EVICT_BYTES];

    evict_cache(&mut eviction);
    let no_prefetch = measure_lookup(&trie, &keys);

    evict_cache(&mut eviction);
    let prefetch = measure_lookup_with_prefetch(&trie, &keys);

    evict_cache(&mut eviction);
    let hot = measure_hot_lookup(&trie, keys[0], keys.len());

    assert_eq!(no_prefetch.checksum, prefetch.checksum);
    assert_eq!(no_prefetch.lookups, LOOKUP_COUNT);
    assert_eq!(prefetch.lookups, LOOKUP_COUNT);
    assert_eq!(hot.lookups, LOOKUP_COUNT);

    eprintln!(
        "packed_mtrie cache probe: routes={ROUTE_COUNT} lookups={LOOKUP_COUNT} prefetch_distance={PREFETCH_DISTANCE}"
    );
    print_probe_stats(no_prefetch, prefetch, hot);

    if !cfg!(debug_assertions) {
        assert!(
            hot.ns_per_lookup() < no_prefetch.ns_per_lookup(),
            "hot lookup should be faster than the randomized working-set lookup"
        );
    }
}

fn build_flat_hash_probe() -> (FlatHashTable<u64, u32>, Box<[u64]>) {
    let mut entries = Vec::with_capacity(HASH_ENTRY_COUNT);
    for index in 0..HASH_ENTRY_COUNT {
        entries.push((hash_key(index as u64), (index as u32) + 1));
    }

    let table = FlatHashTable::from_entries(entries);
    let mut keys = Vec::with_capacity(LOOKUP_COUNT);
    for index in 0..LOOKUP_COUNT {
        keys.push(hash_key((index % HASH_ENTRY_COUNT) as u64));
    }

    shuffle(&mut keys);
    (table, keys.into_boxed_slice())
}

fn measure_hash_lookup(table: &FlatHashTable<u64, u32>, keys: &[u64]) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    for key in keys.iter().copied() {
        checksum = checksum.wrapping_add(u64::from(
            black_box(table.lookup(black_box(&key))).unwrap_or_default(),
        ));
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups: keys.len(),
    }
}

fn measure_hash_lookup_with_prefetch(table: &FlatHashTable<u64, u32>, keys: &[u64]) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    for index in 0..keys.len() {
        if let Some(key) = keys.get(index + PREFETCH_DISTANCE) {
            table.prefetch_key(black_box(key));
        }
        checksum = checksum.wrapping_add(u64::from(
            black_box(table.lookup(black_box(&keys[index]))).unwrap_or_default(),
        ));
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups: keys.len(),
    }
}

fn measure_hash_hot_lookup(
    table: &FlatHashTable<u64, u32>,
    key: u64,
    lookups: usize,
) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    for _ in 0..lookups {
        checksum = checksum.wrapping_add(u64::from(
            black_box(table.lookup(black_box(&key))).unwrap_or_default(),
        ));
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups,
    }
}

fn print_probe_stats(no_prefetch: ProbeStats, prefetch: ProbeStats, hot: ProbeStats) {
    eprintln!(
        "  no_prefetch: {:>8.2} ns/lookup ({:?})",
        no_prefetch.ns_per_lookup(),
        no_prefetch.elapsed
    );
    eprintln!(
        "     prefetch: {:>8.2} ns/lookup ({:?}) ratio={:.3}",
        prefetch.ns_per_lookup(),
        prefetch.elapsed,
        prefetch.ns_per_lookup() / no_prefetch.ns_per_lookup()
    );
    eprintln!(
        "          hot: {:>8.2} ns/lookup ({:?}) ratio={:.3}",
        hot.ns_per_lookup(),
        hot.elapsed,
        hot.ns_per_lookup() / no_prefetch.ns_per_lookup()
    );
}

fn build_packed_mtrie_probe() -> (PackedMtrie<u32>, Box<[u32]>) {
    let mut entries = Vec::with_capacity(ROUTE_COUNT + 1);
    entries.push(MtrieEntry::new(0, 0, 1));

    for index in 0..ROUTE_COUNT {
        let key = route_key(index as u64);
        entries.push(MtrieEntry::new(key & 0xffff_ff00, 24, (index as u32) + 2));
    }

    let trie = PackedMtrie::from_entries(entries);
    let mut keys = Vec::with_capacity(LOOKUP_COUNT);
    for index in 0..LOOKUP_COUNT {
        let route = route_key((index % ROUTE_COUNT) as u64) & 0xffff_ff00;
        let host = splitmix64(index as u64) as u32 & 0xff;
        keys.push(route | host);
    }

    shuffle(&mut keys);
    (trie, keys.into_boxed_slice())
}

fn measure_lookup(trie: &PackedMtrie<u32>, keys: &[u32]) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    for key in keys.iter().copied() {
        checksum = checksum.wrapping_add(u64::from(
            black_box(trie.lookup(black_box(key))).unwrap_or_default(),
        ));
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups: keys.len(),
    }
}

fn measure_lookup_with_prefetch(trie: &PackedMtrie<u32>, keys: &[u32]) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    for index in 0..keys.len() {
        if let Some(key) = keys.get(index + PREFETCH_DISTANCE) {
            trie.prefetch(black_box(*key));
        }
        checksum = checksum.wrapping_add(u64::from(
            black_box(trie.lookup(black_box(keys[index]))).unwrap_or_default(),
        ));
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups: keys.len(),
    }
}

fn measure_hot_lookup(trie: &PackedMtrie<u32>, key: u32, lookups: usize) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    for _ in 0..lookups {
        checksum = checksum.wrapping_add(u64::from(
            black_box(trie.lookup(black_box(key))).unwrap_or_default(),
        ));
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups,
    }
}

fn evict_cache(bytes: &mut [u8]) {
    let mut value = 0u8;
    for byte in bytes.iter_mut().step_by(64) {
        value = value.wrapping_add(*byte).wrapping_add(1);
        *byte = value;
        black_box(*byte);
    }
}

fn shuffle<T>(values: &mut [T]) {
    let mut state = 0x243f_6a88_85a3_08d3u64;
    for index in (1..values.len()).rev() {
        state = splitmix64(state);
        values.swap(index, (state as usize) % (index + 1));
    }
}

fn route_key(index: u64) -> u32 {
    splitmix64(index ^ 0x9e37_79b9_7f4a_7c15) as u32
}

fn hash_key(index: u64) -> u64 {
    splitmix64(index ^ 0xa409_3822_299f_31d0)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Debug, Clone, Copy)]
struct ProbeStats {
    elapsed: Duration,
    checksum: u64,
    lookups: usize,
}

impl ProbeStats {
    fn ns_per_lookup(self) -> f64 {
        self.elapsed.as_secs_f64() * 1_000_000_000.0 / self.lookups as f64
    }
}
