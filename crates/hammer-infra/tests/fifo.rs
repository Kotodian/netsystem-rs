use hammer_infra::fifo::FifoQueue;

#[test]
fn fifo_queue_pops_in_insert_order() {
    let mut queue = FifoQueue::new();

    queue.push_back(1);
    queue.push_back(2);
    queue.push_back(3);

    assert_eq!(queue.len(), 3);
    assert_eq!(queue.front(), Some(&1));
    assert_eq!(queue.pop_front(), Some(1));
    assert_eq!(queue.pop_front(), Some(2));
    assert_eq!(queue.pop_front(), Some(3));
    assert_eq!(queue.pop_front(), None);
    assert!(queue.is_empty());
}

#[test]
fn fifo_queue_front_mut_updates_front_without_pop() {
    let mut queue = FifoQueue::new();

    queue.push_back(String::from("ab"));
    queue.push_back(String::from("cd"));
    queue.front_mut().expect("front").push('x');

    assert_eq!(queue.front().map(String::as_str), Some("abx"));
    assert_eq!(queue.pop_front().as_deref(), Some("abx"));
    assert_eq!(queue.pop_front().as_deref(), Some("cd"));
}

#[test]
fn fifo_queue_can_reuse_storage_after_emptying() {
    let mut queue = FifoQueue::new();

    for value in 0..32 {
        queue.push_back(value);
    }
    for value in 0..32 {
        assert_eq!(queue.pop_front(), Some(value));
    }
    for value in 32..64 {
        queue.push_back(value);
    }
    for value in 32..64 {
        assert_eq!(queue.pop_front(), Some(value));
    }
}

#[test]
fn fifo_queue_insert_places_value_at_requested_position() {
    let mut queue = FifoQueue::new();

    queue.push_back(1);
    queue.push_back(3);
    queue.insert(1, 2);
    queue.insert(0, 0);
    queue.insert(4, 4);

    assert_eq!(
        queue.iter().copied().collect::<std::vec::Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn fifo_queue_iter_mut_updates_values_in_queue_order() {
    let mut queue = FifoQueue::new();

    queue.push_back(1);
    queue.push_back(2);
    queue.push_back(3);
    assert_eq!(queue.pop_front(), Some(1));
    queue.push_back(4);

    for value in queue.iter_mut() {
        *value *= 10;
    }

    assert_eq!(
        queue.iter().copied().collect::<std::vec::Vec<_>>(),
        vec![20, 30, 40]
    );
}

#[test]
fn fifo_queue_get_and_get_mut_follow_queue_order() {
    let mut queue = FifoQueue::new();

    queue.push_back(1);
    queue.push_back(2);
    queue.push_back(3);
    assert_eq!(queue.pop_front(), Some(1));
    queue.push_back(4);

    assert_eq!(queue.get(0), Some(&2));
    assert_eq!(queue.get(1), Some(&3));
    assert_eq!(queue.get(2), Some(&4));
    assert_eq!(queue.get(3), None);

    *queue.get_mut(1).expect("queue slot") = 30;
    assert_eq!(
        queue.iter().copied().collect::<std::vec::Vec<_>>(),
        vec![2, 30, 4]
    );
}
