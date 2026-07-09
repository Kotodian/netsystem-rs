use std::mem::{align_of, size_of, transmute};

use hammer_core::data_plane::{
    BufferHeaderCacheline0, BufferHeaderCacheline1, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES,
    PrimaryOpaque, SecondaryOpaque,
};

#[test]
fn buffer_layout_matches_cacheline_budget() {
    assert_eq!(size_of::<BufferHeaderCacheline0>(), 64);
    assert_eq!(align_of::<BufferHeaderCacheline0>(), 64);
    assert_eq!(size_of::<BufferHeaderCacheline1>(), 64);
    assert_eq!(align_of::<BufferHeaderCacheline1>(), 64);
    assert_eq!(size_of::<PrimaryOpaque>(), 40);
    assert_eq!(align_of::<PrimaryOpaque>(), 8);
    assert_eq!(PRIMARY_OPAQUE_BYTES, size_of::<PrimaryOpaque>());
    assert_eq!(PRIMARY_OPAQUE_ALIGN, align_of::<PrimaryOpaque>());
    assert_eq!(size_of::<SecondaryOpaque>(), 56);
    assert_eq!(align_of::<SecondaryOpaque>(), 8);
}

#[test]
fn primary_and_secondary_opaque_clear_zeroes_storage() {
    let mut primary = unsafe { transmute::<[u64; 5], PrimaryOpaque>([1, 2, 3, 4, 5]) };
    primary.clear();
    assert_eq!(
        unsafe { transmute::<PrimaryOpaque, [u64; 5]>(primary) },
        [0; 5]
    );

    let mut secondary =
        unsafe { transmute::<[u64; 7], SecondaryOpaque>([11, 12, 13, 14, 15, 16, 17]) };
    secondary.clear();
    assert_eq!(
        unsafe { transmute::<SecondaryOpaque, [u64; 7]>(secondary) },
        [0; 7]
    );
}
