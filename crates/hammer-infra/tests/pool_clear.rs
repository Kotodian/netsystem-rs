use std::rc::Rc;

use hammer_infra::pool::Pool;

#[test]
fn dynamic_pool_clear_drops_live_entries_and_restarts_indices() {
    let entry = Rc::new(());
    let mut pool = Pool::new();
    assert_eq!(pool.insert(Rc::clone(&entry)), 0);
    assert_eq!(pool.insert(Rc::clone(&entry)), 1);
    assert_eq!(pool.insert(Rc::clone(&entry)), 2);
    drop(pool.remove(1));
    assert_eq!(Rc::strong_count(&entry), 3);

    let capacity = pool.capacity();
    pool.clear();
    assert_eq!(pool.len(), 0);
    assert_eq!(pool.capacity(), capacity);
    assert_eq!(Rc::strong_count(&entry), 1);
    assert_eq!(pool.insert(Rc::clone(&entry)), 0);
    pool.clear();
    assert_eq!(Rc::strong_count(&entry), 1);
}

#[test]
fn fixed_pool_clear_restores_all_reserved_indices() {
    let entry = Rc::new(());
    let mut pool = Pool::with_fixed_capacity(3);
    assert_eq!(pool.insert(Rc::clone(&entry)), 0);
    assert_eq!(pool.insert(Rc::clone(&entry)), 1);
    drop(pool.remove(0));

    pool.clear();
    assert_eq!(pool.len(), 0);
    assert_eq!(pool.capacity(), 3);
    assert_eq!(Rc::strong_count(&entry), 1);
    assert_eq!(pool.insert(Rc::clone(&entry)), 0);
    assert_eq!(pool.insert(Rc::clone(&entry)), 1);
    assert_eq!(pool.insert(Rc::clone(&entry)), 2);
}
