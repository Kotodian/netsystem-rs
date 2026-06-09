#[test]
fn control_thread_timer_dispatch_uses_tokio_timer_tasks_not_deadline_scanning() {
    let control_thread_source = include_str!("../src/control_thread.rs");
    let timer_source = include_str!("../src/control_thread/timer.rs");

    assert!(
        !control_thread_source.contains("_ = tokio::time::sleep_until(deadline)"),
        "control thread should not drive timers by sleeping on the next deadline"
    );
    assert!(
        !timer_source.contains("fn next_deadline("),
        "timer registry should not expose a deadline scanner"
    );
    assert!(
        !timer_source.contains("fn fire_due("),
        "timer registry should not batch-fire due timers from a side scheduler"
    );
}
