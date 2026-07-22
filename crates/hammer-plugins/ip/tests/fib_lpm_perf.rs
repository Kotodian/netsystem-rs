//! IPv4 longest-prefix-match throughput probe for `Ip4Mtrie` (VPP-aligned
//! `PackedMtrie` backend). Run with:
//!
//! ```text
//! cargo test -p hammer-plugin-ip --release --test fib_lpm_perf -- --ignored --nocapture
//! ```
//!
//! Override route count via env `FIB_LPM_ROUTES` (e.g. `FIB_LPM_ROUTES=262144`).

use std::hint::black_box;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use hammer_plugin_ip::forwarding::{Ip4Mtrie, Ip4MtrieRoute};
use ipnet::Ipv4Net;

const ROUTE_COUNT: usize = 64 * 1024;
const LOOKUP_COUNT: usize = 512 * 1024;
const CHUNK: usize = 4;
const CACHE_EVICT_BYTES: usize = 64 * 1024 * 1024;

fn probed_route_count() -> usize {
    std::env::var("FIB_LPM_ROUTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&value| value > 0)
        .unwrap_or(ROUTE_COUNT)
}

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-plugin-ip --release --test fib_lpm_perf -- --ignored --nocapture`"]
fn ip4_lpm_mtrie_probe() {
    let route_count = probed_route_count();
    let (trie, keys) = build_lpm_probe(route_count);

    let mut eviction = vec![0u8; CACHE_EVICT_BYTES];

    evict_cache(&mut eviction);
    let no_prefetch = measure_lookup(&trie, &keys);
    evict_cache(&mut eviction);
    let prefetch = measure_dual_loop(&trie, &keys);
    evict_cache(&mut eviction);
    let hot = measure_hot_lookup(&trie, keys[0], keys.len());

    assert_eq!(no_prefetch.checksum, prefetch.checksum);
    assert_eq!(no_prefetch.lookups, LOOKUP_COUNT);
    assert_eq!(prefetch.lookups, LOOKUP_COUNT);
    assert_eq!(hot.lookups, LOOKUP_COUNT);

    eprintln!(
        "ip4 LPM probe: routes={route_count} lookups={LOOKUP_COUNT} chunk_width={CHUNK} (VPP dual-loop prefetch)"
    );
    print_probe_stats(no_prefetch, prefetch, hot);
}

fn build_lpm_probe(route_count: usize) -> (Ip4Mtrie<u32>, Box<[Ipv4Addr]>) {
    let mut routes = Vec::with_capacity(route_count + 1);

    routes.push(Ip4MtrieRoute::new(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        1,
    ));

    let lengths: [u8; 4] = [8, 16, 24, 32];
    for index in 0..route_count {
        let base = route_base(index as u64);
        let len = lengths[index % lengths.len()];
        let masked = mask_base(base, len);
        let value = (index as u32) + 2;

        routes.push(Ip4MtrieRoute::new(
            Ipv4Net::new(Ipv4Addr::from(masked), len).expect("route prefix"),
            value,
        ));
    }

    let trie = Ip4Mtrie::from_routes(routes);

    let mut keys = Vec::with_capacity(LOOKUP_COUNT);
    for index in 0..LOOKUP_COUNT {
        let route_index = (index % route_count) as u64;
        let base = route_base(route_index);
        let host = (splitmix64(index as u64) as u32)
            & host_mask(lengths[route_index as usize % lengths.len()]);
        keys.push(Ipv4Addr::from(base | host));
    }
    shuffle(&mut keys);

    (trie, keys.into_boxed_slice())
}

fn route_base(index: u64) -> u32 {
    splitmix64(index ^ 0x9e37_79b9_7f4a_7c15) as u32
}

fn mask_base(base: u32, len: u8) -> u32 {
    if len == 0 {
        return 0;
    }
    let mask = if len >= 32 {
        u32::MAX
    } else {
        u32::MAX << (32 - len)
    };
    base & mask
}

fn host_mask(len: u8) -> u32 {
    if len >= 32 {
        0
    } else {
        (1u32 << (32 - len)) - 1
    }
}

fn measure_lookup(trie: &Ip4Mtrie<u32>, keys: &[Ipv4Addr]) -> ProbeStats {
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

/// VPP dual-loop: process current quad, prefetch next quad.
fn measure_dual_loop(trie: &Ip4Mtrie<u32>, keys: &[Ipv4Addr]) -> ProbeStats {
    let start = Instant::now();
    let mut checksum = 0u64;
    let mut read = 0usize;
    if read + CHUNK <= keys.len() {
        for i in 0..CHUNK {
            trie.prefetch(black_box(keys[read + i]));
        }
    }
    while read + CHUNK <= keys.len() {
        let next = read + CHUNK;
        if next + CHUNK <= keys.len() {
            for i in 0..CHUNK {
                trie.prefetch(black_box(keys[next + i]));
            }
        }
        for i in 0..CHUNK {
            checksum = checksum.wrapping_add(u64::from(
                black_box(trie.lookup(black_box(keys[read + i]))).unwrap_or_default(),
            ));
        }
        read += CHUNK;
    }
    while read < keys.len() {
        checksum = checksum.wrapping_add(u64::from(
            black_box(trie.lookup(black_box(keys[read]))).unwrap_or_default(),
        ));
        read += 1;
    }
    ProbeStats {
        elapsed: start.elapsed(),
        checksum: black_box(checksum),
        lookups: keys.len(),
    }
}

fn measure_hot_lookup(trie: &Ip4Mtrie<u32>, key: Ipv4Addr, lookups: usize) -> ProbeStats {
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
