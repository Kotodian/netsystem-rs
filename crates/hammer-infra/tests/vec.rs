use std::cell::Cell;
use std::rc::Rc;

use hammer_infra::vec::Vec;

#[test]
fn from_elem_copy_initializes_aligned_vec() {
    let values = Vec::from_elem_copy(4, 7_u8);

    assert_eq!(values.as_slice(), &[7, 7, 7, 7]);
    assert_eq!(values.len(), 4);
}

#[test]
fn remove_returns_element_and_shifts_tail() {
    let mut values = Vec::new();
    values.extend_from_slice(&[10, 20, 30, 40]);

    assert_eq!(values.remove(1), 20);
    assert_eq!(values.as_slice(), &[10, 30, 40]);

    assert_eq!(values.remove(2), 40);
    assert_eq!(values.as_slice(), &[10, 30]);

    assert_eq!(values.remove(0), 10);
    assert_eq!(values.as_slice(), &[30]);
}

#[test]
fn remove_drops_removed_element_once_and_keeps_retained_elements() {
    let drops = Rc::new(Cell::new(0));
    let mut values = Vec::new();
    values.push(DropCounter::new(1, drops.clone()));
    values.push(DropCounter::new(2, drops.clone()));
    values.push(DropCounter::new(3, drops.clone()));

    let removed = values.remove(1);

    assert_eq!(removed.value, 2);
    assert_eq!(drops.get(), 0);
    assert_eq!(values[0].value, 1);
    assert_eq!(values[1].value, 3);

    drop(removed);
    assert_eq!(drops.get(), 1);

    drop(values);
    assert_eq!(drops.get(), 3);
}

struct DropCounter {
    value: u32,
    drops: Rc<Cell<u32>>,
}

impl DropCounter {
    fn new(value: u32, drops: Rc<Cell<u32>>) -> Self {
        Self { value, drops }
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
