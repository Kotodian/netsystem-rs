use hammer_infra::align::{CACHE_LINE, CacheLine, is_aligned};
use hammer_infra::{boxed, vec};

#[test]
fn boxed_slice_allocates_usable_storage() {
    let slice = boxed::Box::<[u8]>::from_elem(1500, 0xaa);

    assert_eq!(slice.len(), 1500);
    assert_eq!(slice.as_ref()[0], 0xaa);
    assert_eq!(slice.as_ref()[1499], 0xaa);
}

#[test]
fn boxed_slice_drops_elements_once() {
    use std::rc::Rc;

    #[derive(Clone)]
    struct Counted(#[allow(dead_code)] Rc<()>);

    let marker = Rc::new(());
    let slice = boxed::Box::<[Counted]>::from_elem(4, Counted(Rc::clone(&marker)));

    assert_eq!(Rc::strong_count(&marker), 5);
    drop(slice);
    assert_eq!(Rc::strong_count(&marker), 1);
}

#[test]
fn vec_supports_growth_and_clone() {
    let mut values = vec::Vec::new();
    for value in 0..100 {
        values.push(value);
    }
    let clone = values.clone();
    assert_eq!(values.len(), 100);
    assert_eq!(clone.len(), 100);
    assert_eq!(values[99], 99);
}

#[test]
fn cache_line_helper_types_remain_aligned() {
    let line = CacheLine::<[u8; 64]>::new([0; 64]);
    assert!(is_aligned(
        (&line as *const CacheLine<[u8; 64]>) as *const u8,
        CACHE_LINE
    ));
}
