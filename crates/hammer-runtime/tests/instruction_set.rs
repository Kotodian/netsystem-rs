use hammer_core::data_plane::FrameBatchWidth;
use hammer_runtime::DataPlaneInstructionSet;

#[test]
fn instruction_set_selects_core_frame_batch_width() {
    let cases = [
        (DataPlaneInstructionSet::Scalar, FrameBatchWidth::Pair),
        (DataPlaneInstructionSet::Sse2, FrameBatchWidth::Pair),
        (DataPlaneInstructionSet::Avx2, FrameBatchWidth::Quad),
        (DataPlaneInstructionSet::Avx512, FrameBatchWidth::Octo),
        (DataPlaneInstructionSet::Neon, FrameBatchWidth::Quad),
    ];

    for (instruction_set, expected) in cases {
        assert_eq!(instruction_set.preferred_frame_batch_width(), expected);
    }
}
