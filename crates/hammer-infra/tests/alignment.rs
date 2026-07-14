use std::mem::{align_of, size_of};

use hammer_infra::align::{CACHE_LINE, CacheLine, is_aligned};
use hammer_infra::vec::{AlignedVec, CacheLineVec, Vec};
use hammer_infra::{boxed, vec};

/// VPP-shaped ordinary vector floor (`VEC_MIN_ALIGN`).
const VEC_MIN_ALIGN: usize = 8;

fn expected_default_align<T>() -> usize {
    align_of::<T>().max(VEC_MIN_ALIGN)
}

fn assert_default_vec_aligned<T>(values: &Vec<T>) {
    if values.capacity() == 0 {
        return;
    }
    assert!(
        is_aligned(values.as_ptr(), expected_default_align::<T>()),
        "default Vec<{}> ptr {:p} must be aligned to {}",
        std::any::type_name::<T>(),
        values.as_ptr(),
        expected_default_align::<T>()
    );
}

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

#[test]
fn default_vec_u8_uses_vpp_min_align() {
    let mut values = Vec::<u8>::with_capacity(1);
    values.push(1);
    assert_default_vec_aligned(&values);
    // Empty capacity stays dangling / unallocated — no alignment claim.
    let empty = Vec::<u8>::new();
    assert_eq!(empty.capacity(), 0);
    // Policy: ordinary Vec does not *require* cache-line alignment (mimalloc may
    // still return a stronger-aligned pointer; that is not an AC failure).
    assert_eq!(expected_default_align::<u8>(), VEC_MIN_ALIGN);
}

#[test]
fn default_vec_u64_and_growth_keep_min_align() {
    let mut values = Vec::<u64>::new();
    assert_eq!(values.capacity(), 0);
    for i in 0..64 {
        values.push(i);
        assert_default_vec_aligned(&values);
    }
}

#[repr(align(16))]
#[derive(Clone, Copy)]
struct OverAligned(u64);

#[test]
fn default_vec_over_aligned_elements_keep_element_align() {
    assert!(align_of::<OverAligned>() >= 16);
    let mut values = Vec::<OverAligned>::with_capacity(4);
    values.push(OverAligned(1));
    assert_eq!(values[0].0, 1);
    assert_default_vec_aligned(&values);
    assert!(is_aligned(
        values.as_ptr(),
        align_of::<OverAligned>().max(VEC_MIN_ALIGN)
    ));
}

#[test]
fn cache_line_vec_requests_explicit_cache_line_alignment() {
    assert_eq!(size_of::<CacheLineVec<u8>>(), size_of::<Vec<u8>>());
    let mut values = CacheLineVec::<u8>::with_capacity(8);
    values.push(7);
    assert!(is_aligned(values.as_ptr(), CACHE_LINE));
    assert!(is_aligned(
        values.as_ptr(),
        align_of::<u8>().max(CACHE_LINE)
    ));
}

#[test]
fn aligned_vec_const_align_raises_layout() {
    let mut values = AlignedVec::<u8, 32>::with_capacity(4);
    values.push(1);
    assert!(is_aligned(values.as_ptr(), 32));
}
