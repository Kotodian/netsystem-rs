use hammer_infra::segment::Segment;
use std::io;

/// The shared-segment creation seam must surface concrete errors: the
/// checked page-size query is exported at the crate root, and
/// reserved-prefix validation failures arrive as `InvalidInput` rather
/// than being replaced by a stale OS error.
#[test]
fn shared_segment_creation_failures_carry_concrete_errors() {
    let page = hammer_infra::page_size().expect("checked page-size query must succeed");
    assert!(
        page > 0 && page.is_power_of_two(),
        "page size must be a positive power of two"
    );

    for prefix in [100, page + 1, 3 * page] {
        let error =
            Segment::shared_with_reserved_prefix("hammer-test-prefix-error", 2 * page, prefix)
                .err()
                .expect("a reserved prefix must be validated at the creation seam");
        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidInput,
            "prefix validation failures must stay concrete InvalidInput errors"
        );
    }
}
