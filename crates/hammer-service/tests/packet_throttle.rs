use std::time::Duration;

use hammer_service::net::throttle::Throttle;

#[test]
fn duplicate_suppression_expires_per_worker_interval() {
    hammer_runtime::config::Memory::default()
        .ensure_main_heap()
        .unwrap();
    let period = Duration::from_micros(10);
    let mut ingress = Throttle::new(period);
    let mut egress = Throttle::new(period);
    let seed = ingress.seed(Duration::ZERO);
    assert!(!ingress.check(42, seed));
    assert!(ingress.check(42, seed));
    let other_seed = egress.seed(Duration::ZERO);
    assert!(!egress.check(42, other_seed));

    // vnet/util/throttle.h expires only after, not at, the period boundary.
    let seed = ingress.seed(period);
    assert!(ingress.check(42, seed));
    let seed = ingress.seed(period + Duration::from_nanos(1));
    assert!(!ingress.check(42, seed));
    assert!(ingress.check(42, seed));
    assert!(egress.check(42, other_seed));
}

#[test]
fn collisions_bound_admitted_keys_until_bitmap_reset() {
    hammer_runtime::config::Memory::default()
        .ensure_main_heap()
        .unwrap();
    let mut throttle = Throttle::new(Duration::from_millis(1));
    let seed = throttle.seed(Duration::ZERO);
    let mut admitted = 0;
    for key in 0..513 {
        if !throttle.check(key, seed) {
            admitted += 1;
        }
        assert!(throttle.check(key, seed));
    }
    // VPP uses a finite bitmap, not an exact set or a token bucket.
    assert!((1..=512).contains(&admitted));
    let seed = throttle.seed(Duration::from_millis(2));
    assert!(!throttle.check(0, seed));
}
