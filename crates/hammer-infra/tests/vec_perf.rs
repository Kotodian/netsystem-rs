use std::hint::black_box;
use std::time::{Duration, Instant};

use hammer_infra::vec::Vec;

const ITEM_COUNT: usize = 1_000_000;
const SAMPLE_COUNT: usize = 7;

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-infra --release --test vec_perf -- --ignored --nocapture`"]
fn vec_vs_std_perf_probe() {
    let std_push = measure_samples(|| {
        let mut values = std::vec::Vec::with_capacity(ITEM_COUNT);
        for value in 0..ITEM_COUNT {
            values.push(black_box(value as u64));
        }
        sum_slice(&values)
    });
    let aligned_push = measure_samples(|| {
        let mut values = Vec::with_capacity(ITEM_COUNT);
        for value in 0..ITEM_COUNT {
            values.push(black_box(value as u64));
        }
        sum_slice(&values)
    });

    let std_values = (0..ITEM_COUNT as u64).collect::<std::vec::Vec<_>>();
    let aligned_values = (0..ITEM_COUNT as u64).collect::<Vec<_>>();

    let std_iter = measure_samples(|| sum_slice(&std_values));
    let aligned_iter = measure_samples(|| sum_slice(&aligned_values));

    let std_extend = measure_samples(|| {
        let mut values = std::vec::Vec::with_capacity(ITEM_COUNT);
        values.extend_from_slice(&std_values);
        sum_slice(&values)
    });
    let aligned_extend = measure_samples(|| {
        let mut values = Vec::with_capacity(ITEM_COUNT);
        values.extend_from_slice(&std_values);
        sum_slice(&values)
    });
    let aligned_copy_extend = measure_samples(|| {
        let mut values = Vec::with_capacity(ITEM_COUNT);
        values.extend_from_copy_slice(&std_values);
        sum_slice(&values)
    });

    assert_eq!(sum_slice(&std_values), sum_slice(&aligned_values));

    eprintln!("vec perf probe: items={ITEM_COUNT} samples={SAMPLE_COUNT}");
    print_comparison("push", std_push, aligned_push);
    print_comparison("iterate", std_iter, aligned_iter);
    print_comparison("extend_from_slice", std_extend, aligned_extend);
    print_comparison("extend_copy_slice", std_extend, aligned_copy_extend);
}

fn sum_slice(values: &[u64]) -> u64 {
    let mut sum = 0u64;
    for value in values {
        sum = sum.wrapping_add(black_box(*value));
    }
    black_box(sum)
}

fn measure_samples(mut f: impl FnMut() -> u64) -> ProbeSummary {
    let mut samples = std::vec::Vec::with_capacity(SAMPLE_COUNT);
    let mut checksum = 0u64;
    for _ in 0..SAMPLE_COUNT {
        let started = Instant::now();
        checksum ^= f();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    ProbeSummary {
        best: samples[0],
        median: samples[SAMPLE_COUNT / 2],
        checksum: black_box(checksum),
    }
}

fn print_comparison(label: &str, std: ProbeSummary, aligned: ProbeSummary) {
    eprintln!(
        "  {label:>17}: std_best={:>8.2} std_median={:>8.2} ns/item aligned_best={:>8.2} aligned_median={:>8.2} ns/item best_ratio={:.3} median_ratio={:.3} checksum={}",
        ns_per_item(std.best),
        ns_per_item(std.median),
        ns_per_item(aligned.best),
        ns_per_item(aligned.median),
        ns_per_item(aligned.best) / ns_per_item(std.best),
        ns_per_item(aligned.median) / ns_per_item(std.median),
        std.checksum ^ aligned.checksum,
    );
}

fn ns_per_item(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0 / ITEM_COUNT as f64
}

#[derive(Clone, Copy)]
struct ProbeSummary {
    best: Duration,
    median: Duration,
    checksum: u64,
}
