use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use hammer_infra::bihash::Bihash;

const ENTRY_COUNT: u64 = 4_096;
const LOOKUPS_PER_READER: u64 = 200_000;
const SAMPLE_COUNT: usize = 3;

#[test]
#[ignore = "release-only concurrent lookup performance gate"]
fn bihash_lookup_avoids_shared_reader_lock_regression() {
    let reader_count = thread::available_parallelism()
        .map_or(2, usize::from)
        .clamp(2, 4);
    let bihash = Arc::new(Bihash::<u64, 7>::new(ENTRY_COUNT as u32));
    let locked = Arc::new(RwLock::new(HashMap::with_capacity(ENTRY_COUNT as usize)));
    for key in 0..ENTRY_COUNT {
        bihash.insert(key, key + 1);
        locked.write().expect("write lock").insert(key, key + 1);
    }

    let mut bihash_samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut locked_samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        bihash_samples.push(measure_lookup(
            Arc::clone(&bihash),
            reader_count,
            |table, key| table.lookup(key),
        ));
        locked_samples.push(measure_lookup(
            Arc::clone(&locked),
            reader_count,
            |table, key| {
                table
                    .read()
                    .expect("read lock")
                    .get(key)
                    .copied()
            },
        ));
    }
    bihash_samples.sort_unstable();
    locked_samples.sort_unstable();
    let bihash_median = bihash_samples[SAMPLE_COUNT / 2];
    let locked_median = locked_samples[SAMPLE_COUNT / 2];

    eprintln!(
        "bihash concurrent lookup: readers={reader_count} bihash={bihash_median:?} rwlock_hashmap={locked_median:?}"
    );
    assert!(
        bihash_median <= locked_median.saturating_mul(2),
        "bihash lookup regressed beyond twice the shared-reader-lock baseline"
    );
}

fn measure_lookup<T, F>(table: Arc<T>, readers: usize, lookup: F) -> Duration
where
    T: Send + Sync + 'static,
    F: Fn(&T, &u64) -> Option<u64> + Copy + Send + Sync + 'static,
{
    let start = Arc::new(Barrier::new(readers + 1));
    let workers = (0..readers)
        .map(|reader| {
            let start = Arc::clone(&start);
            let table = Arc::clone(&table);
            thread::spawn(move || {
                start.wait();
                let mut sum = 0u64;
                for lookup_index in 0..LOOKUPS_PER_READER {
                    let key = (lookup_index + reader as u64) & (ENTRY_COUNT - 1);
                    sum = sum.wrapping_add(
                        lookup(&table, black_box(&key)).unwrap_or_default(),
                    );
                }
                black_box(sum);
            })
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    start.wait();
    for worker in workers {
        worker.join().expect("reader");
    }
    started.elapsed()
}
