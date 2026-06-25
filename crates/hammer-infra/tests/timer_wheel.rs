use hammer_infra::timer_wheel::{
    TimerStartError, TimerWheel, TimerWheel1t1w32sl, TimerWheel1t2w32sl, TimerWheel1w32FastHint,
    TimerWheel2t1w2048sl,
};
use hammer_infra::vec::Vec;

fn expired_values(expired: &Vec<u32>) -> std::vec::Vec<u32> {
    expired.iter().copied().collect()
}

#[test]
fn timer_wheel_starts_relative_timers_and_expires_in_tick_order() {
    let mut wheel = TimerWheel1t1w32sl::<u32>::new(0);
    let mut expired = Vec::new();

    wheel.start(30, 3).unwrap();
    wheel.start(10, 1).unwrap();
    wheel.start(20, 2).unwrap();

    assert_eq!(wheel.expire(0, &mut expired), 0);
    assert!(expired.is_empty());
    assert_eq!(wheel.current_tick(), 0);

    assert_eq!(wheel.expire(1, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![10]);
    assert_eq!(wheel.current_tick(), 1);

    assert_eq!(wheel.expire(2, &mut expired), 2);
    assert_eq!(expired_values(&expired), vec![10, 20, 30]);
    assert_eq!(wheel.current_tick(), 3);
    assert!(wheel.is_empty());
}

#[test]
fn timer_wheel_stop_update_and_stale_handles_are_generation_checked() {
    let mut wheel = TimerWheel1t1w32sl::<u32>::new(0);
    let mut expired = Vec::new();

    let stopped = wheel.start(1, 2).unwrap();
    let moved = wheel.start(2, 3).unwrap();

    assert!(wheel.stop(stopped));
    assert!(!wheel.stop(stopped));
    assert!(!wheel.handle_is_live(stopped));

    assert_eq!(wheel.update(moved, 5), Ok(true));
    assert_eq!(wheel.expire(3, &mut expired), 0);
    assert!(expired.is_empty());

    assert_eq!(wheel.expire(2, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![2]);
    assert!(!wheel.handle_is_live(moved));
    assert_eq!(wheel.update(moved, 1), Ok(false));

    let reused = wheel.start(3, 1).unwrap();
    assert_eq!(moved.slot(), reused.slot());
    assert_ne!(moved.generation(), reused.generation());
}

#[test]
fn timer_wheel_rejects_zero_and_out_of_range_intervals_without_overflow() {
    let mut one_ring = TimerWheel1t1w32sl::<u32>::new(0);
    let mut two_ring = TimerWheel1t2w32sl::<u32>::new(0);

    assert_eq!(one_ring.start(1, 0), Err(TimerStartError::ZeroInterval));
    assert!(one_ring.start(2, 32).is_ok());
    assert_eq!(
        one_ring.start(3, 33),
        Err(TimerStartError::IntervalOutOfRange)
    );

    assert!(two_ring.start(4, 32 * 32 - 1).is_ok());
    assert_eq!(
        two_ring.start(5, 32 * 32),
        Err(TimerStartError::IntervalOutOfRange)
    );
}

#[test]
fn timer_wheel_cascades_from_slow_ring_to_fast_ring() {
    let mut wheel = TimerWheel1t2w32sl::new(0);
    let mut expired = Vec::new();

    wheel.start(7, 33).unwrap();

    assert_eq!(wheel.expire(32, &mut expired), 0);
    assert!(expired.is_empty());

    assert_eq!(wheel.expire(1, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![7]);
}

#[test]
fn timer_wheel_cascades_from_glacier_ring_through_slow_ring_to_fast_ring() {
    type ThreeRingWheel = TimerWheel<u32, 3, 8, false, false, true>;

    let mut wheel = ThreeRingWheel::new(0);
    let mut expired = Vec::new();

    wheel.start(9, 8 * 8 + 3).unwrap();

    assert_eq!(wheel.expire(8 * 8, &mut expired), 0);
    assert!(expired.is_empty());

    assert_eq!(wheel.expire(2, &mut expired), 0);
    assert!(expired.is_empty());

    assert_eq!(wheel.expire(1, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![9]);
}

#[test]
fn timer_wheel_overflow_parks_until_three_ring_horizon_then_reinserts() {
    type OverflowWheel = TimerWheel<u32, 3, 8, false, true, true>;

    let mut wheel = OverflowWheel::new(0);
    let mut expired = Vec::new();
    let horizon = 8 * 8 * 8;

    wheel.start(42, horizon + 5).unwrap();

    assert_eq!(wheel.expire(horizon as u32, &mut expired), 0);
    assert!(expired.is_empty());
    assert_eq!(wheel.current_tick(), horizon);

    assert_eq!(wheel.expire(4, &mut expired), 0);
    assert!(expired.is_empty());

    assert_eq!(wheel.expire(1, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![42]);
}

#[test]
fn timer_wheel_2048_slot_alias_handles_full_fast_ring_revolution() {
    let mut wheel = TimerWheel2t1w2048sl::<u32>::new(0);
    let mut expired = Vec::new();

    wheel.start(2_048, 2_048).unwrap();
    assert_eq!(wheel.expire(2_047, &mut expired), 0);
    assert!(expired.is_empty());

    assert_eq!(wheel.expire(1, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![2_048]);
}

#[test]
fn timer_wheel_max_expirations_stops_expire_call_between_ticks() {
    let mut wheel = TimerWheel1t1w32sl::<u32>::new(2);
    let mut expired = Vec::new();

    wheel.start(1, 1).unwrap();
    wheel.start(2, 2).unwrap();
    wheel.start(3, 3).unwrap();

    assert_eq!(wheel.expire(10, &mut expired), 2);
    assert_eq!(expired_values(&expired), vec![1, 2]);
    assert_eq!(wheel.current_tick(), 2);

    assert_eq!(wheel.expire(10, &mut expired), 1);
    assert_eq!(expired_values(&expired), vec![1, 2, 3]);
    assert_eq!(wheel.current_tick(), 12);
}

#[test]
fn timer_wheel_fast_hint_is_approximate_and_slot_based() {
    let mut wheel = TimerWheel1w32FastHint::<u32>::new(0);
    let mut expired = Vec::new();

    assert_eq!(wheel.first_expires_in_ticks(), Some(32));

    let handle = wheel.start(11, 5).unwrap();
    assert_eq!(wheel.first_expires_in_ticks(), Some(5));

    assert!(wheel.stop(handle));
    assert_eq!(wheel.first_expires_in_ticks(), Some(5));

    assert_eq!(wheel.expire(5, &mut expired), 0);
    assert!(expired.is_empty());
    assert_eq!(wheel.first_expires_in_ticks(), Some(32));
}

#[test]
fn timer_wheel_without_fast_hint_reports_no_hint() {
    let wheel = TimerWheel1t1w32sl::<u32>::new(0);

    assert_eq!(wheel.first_expires_in_ticks(), None);
}
