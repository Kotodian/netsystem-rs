use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

#[test]
fn drain_prefix_drop_restores_untouched_tail() {
    let mut values = Vec::with_capacity(4);
    values.extend_from_slice(&[10, 20, 30, 40]);
    {
        let mut drained = values.drain(0..2);
        assert_eq!(drained.next(), Some(10));
    }
    assert_eq!(values.as_slice(), &[30, 40]);
}

/// Element that records drops and optionally panics once from `Drop`.
struct PanicDrop {
    id: u32,
    panic_on_drop: bool,
    dropped: Arc<AtomicBool>,
}

impl PanicDrop {
    fn new(id: u32, panic_on_drop: bool) -> Self {
        Self {
            id,
            panic_on_drop,
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }

    fn track(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.dropped)
    }
}

impl Drop for PanicDrop {
    fn drop(&mut self) {
        assert!(
            !self.dropped.swap(true, Ordering::SeqCst),
            "element {} dropped more than once",
            self.id
        );
        if self.panic_on_drop {
            panic!("panic on drop {}", self.id);
        }
    }
}

#[test]
fn public_clear_is_panic_drop_safe() {
    let mut values = Vec::new();
    let a = PanicDrop::new(1, true);
    let b = PanicDrop::new(2, false);
    let track_a = a.track();
    let track_b = b.track();
    values.push(a);
    values.push(b);

    let result = catch_unwind(AssertUnwindSafe(|| values.clear()));
    assert!(result.is_err());
    assert!(track_a.load(Ordering::SeqCst));
    // Remaining elements may leak after a panic in drop_in_place (std rule);
    // they must never double-drop when the vector is later dropped.
    drop(values);
    assert!(track_a.load(Ordering::SeqCst));
    let _ = track_b.load(Ordering::SeqCst);
}

#[test]
fn public_truncate_is_panic_drop_safe() {
    let mut values = Vec::new();
    values.push(PanicDrop::new(1, false));
    values.push(PanicDrop::new(2, true));
    values.push(PanicDrop::new(3, false));
    let tracks: std::vec::Vec<_> = values.iter().map(PanicDrop::track).collect();

    let result = catch_unwind(AssertUnwindSafe(|| values.truncate(1)));
    assert!(result.is_err());
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].id, 1);
    drop(values);
    assert!(tracks[0].load(Ordering::SeqCst));
}

#[test]
fn public_drain_restores_tail_after_panic_in_drop() {
    let mut values = Vec::new();
    values.push(PanicDrop::new(1, false));
    values.push(PanicDrop::new(2, true));
    values.push(PanicDrop::new(3, false));
    let track_tail = values[2].track();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = values.drain(0..2);
    }));
    assert!(result.is_err());
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].id, 3);
    assert!(!track_tail.load(Ordering::SeqCst));
    drop(values);
    assert!(track_tail.load(Ordering::SeqCst));
}

#[test]
fn public_into_iter_is_panic_drop_safe() {
    let mut values = Vec::new();
    values.push(PanicDrop::new(1, false));
    values.push(PanicDrop::new(2, true));
    values.push(PanicDrop::new(3, false));
    let tracks: std::vec::Vec<_> = values.iter().map(PanicDrop::track).collect();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = values.into_iter();
    }));
    assert!(result.is_err());
    for track in tracks {
        let _ = track.load(Ordering::SeqCst);
    }
}
