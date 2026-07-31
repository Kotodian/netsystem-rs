use hammer_infra::linked_list::LinkedList;

#[test]
fn push_back_and_pop_front_preserve_fifo_order() {
    let mut list = LinkedList::new();

    assert_eq!(list.pop_front(), None);
    assert!(list.is_empty());

    list.push_back(1);
    list.push_back(2);
    list.push_back(3);

    assert_eq!(list.len(), 3);
    assert_eq!(list.pop_front(), Some(1));
    assert_eq!(list.pop_front(), Some(2));
    assert_eq!(list.pop_front(), Some(3));
    assert_eq!(list.pop_front(), None);
}

#[test]
fn push_front_and_pop_back_preserve_fifo_order() {
    let mut list = LinkedList::new();

    list.push_front(1);
    list.push_front(2);
    list.push_front(3);

    assert_eq!(list.pop_back(), Some(1));
    assert_eq!(list.pop_back(), Some(2));
    assert_eq!(list.pop_back(), Some(3));
    assert_eq!(list.pop_back(), None);
}

#[test]
fn front_and_back_are_available_for_mutation() {
    let mut list = LinkedList::new();
    list.push_back(1);
    list.push_back(2);

    assert_eq!(list.front(), Some(&1));
    assert_eq!(list.back(), Some(&2));

    *list.front_mut().expect("front") = 10;
    *list.back_mut().expect("back") = 20;

    assert_eq!(list.front(), Some(&10));
    assert_eq!(list.back(), Some(&20));
}

#[test]
fn iter_and_iter_mut_traverse_from_head_to_tail() {
    let mut list = LinkedList::new();
    list.push_back(1);
    list.push_back(2);
    list.push_back(3);

    assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);

    for value in list.iter_mut() {
        *value *= 10;
    }

    assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![10, 20, 30]);
}

#[test]
fn clear_drops_all_values() {
    let mut list = LinkedList::new();
    list.push_back(1);
    list.push_back(2);
    list.push_back(3);

    list.clear();

    assert!(list.is_empty());
    assert_eq!(list.len(), 0);
    assert_eq!(list.iter().count(), 0);
}
