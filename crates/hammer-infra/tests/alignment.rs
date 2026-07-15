use hammer_infra::align::{CACHE_LINE, CacheLine, is_aligned};

#[test]
fn cache_line_helper_types_remain_aligned() {
    let line = CacheLine::<[u8; 64]>::new([0; 64]);
    assert!(is_aligned(
        (&line as *const CacheLine<[u8; 64]>) as *const u8,
        CACHE_LINE
    ));
}
