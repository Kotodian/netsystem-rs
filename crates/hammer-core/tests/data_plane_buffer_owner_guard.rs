use std::fs;
use std::path::Path;

const MOVED_ADAPTER_PATTERNS: &[&str] = &[
    "hammer_adapter::DEFAULT_BUFFER_FRAME_CAPACITY",
    "hammer_adapter::DEFAULT_BUFFER_FRAME_POOL_SIZE",
    "hammer_adapter::BUFFER_CACHE_LINE_SIZE",
    "hammer_adapter::DEFAULT_PACKET_HEADROOM",
    "hammer_adapter::DEFAULT_PRE_DATA_SIZE",
    "hammer_adapter::BUFFER_INVALID_INDEX",
    "hammer_adapter::BUFFER_THREAD_CACHE_BATCH",
    "hammer_adapter::BUFFER_THREAD_CACHE_HIGH_WATER",
    "hammer_adapter::BUFFER_IN_USE_FOLD_THRESHOLD",
    "hammer_adapter::PrimaryOpaque",
    "hammer_adapter::SecondaryOpaque",
    "hammer_adapter::BufferFlags",
    "hammer_adapter::BufferHeaderCacheline0",
    "hammer_adapter::BufferHeaderCacheline1",
    "hammer_adapter::Buffer",
    "hammer_adapter::BufferIndex",
    "hammer_adapter::FrameIndex",
    "hammer_adapter::BufferFrame",
    "hammer_adapter::Frame",
    "hammer_adapter::Next",
    "hammer_adapter::Pending",
    "hammer_adapter::BufferPacketCursor",
    "hammer_adapter::BufferNodeError",
    "hammer_adapter::DataPlaneBuffers",
    "hammer_adapter::BufferPool",
    "hammer_adapter::BufferPoolArena",
    "hammer_adapter::BufferRef",
    "hammer_adapter::BufferRefMut",
    "hammer_adapter::BufferThreadCache",
    "hammer_adapter::DataPlaneBufferConfig",
    "hammer_adapter::BufferFrameDrain",
    "hammer_adapter::DataPlaneBufferChain",
    "hammer_adapter::BufferFramePending",
    "hammer_adapter::buffer::{",
    "hammer_adapter::buffer::DEFAULT_BUFFER_FRAME_CAPACITY",
    "hammer_adapter::buffer::DEFAULT_BUFFER_FRAME_POOL_SIZE",
    "hammer_adapter::buffer::BUFFER_CACHE_LINE_SIZE",
    "hammer_adapter::buffer::DEFAULT_PACKET_HEADROOM",
    "hammer_adapter::buffer::DEFAULT_PRE_DATA_SIZE",
    "hammer_adapter::buffer::BUFFER_INVALID_INDEX",
    "hammer_adapter::buffer::BUFFER_THREAD_CACHE_BATCH",
    "hammer_adapter::buffer::BUFFER_THREAD_CACHE_HIGH_WATER",
    "hammer_adapter::buffer::BUFFER_IN_USE_FOLD_THRESHOLD",
    "hammer_adapter::buffer::PrimaryOpaque",
    "hammer_adapter::buffer::SecondaryOpaque",
    "hammer_adapter::buffer::BufferFlags",
    "hammer_adapter::buffer::BufferHeaderCacheline0",
    "hammer_adapter::buffer::BufferHeaderCacheline1",
    "hammer_adapter::buffer::Buffer",
    "hammer_adapter::buffer::BufferIndex",
    "hammer_adapter::buffer::FrameIndex",
    "hammer_adapter::buffer::BufferFrame",
    "hammer_adapter::buffer::Frame",
    "hammer_adapter::buffer::Next",
    "hammer_adapter::buffer::Pending",
    "hammer_adapter::buffer::BufferPacketCursor",
    "hammer_adapter::buffer::BufferNodeError",
    "hammer_adapter::buffer::DataPlaneBuffers",
    "hammer_adapter::buffer::BufferPool",
    "hammer_adapter::buffer::BufferPoolArena",
    "hammer_adapter::buffer::BufferRef",
    "hammer_adapter::buffer::BufferRefMut",
    "hammer_adapter::buffer::BufferThreadCache",
    "hammer_adapter::buffer::DataPlaneBufferConfig",
    "hammer_adapter::buffer::BufferFrameDrain",
    "hammer_adapter::buffer::DataPlaneBufferChain",
    "hammer_adapter::buffer::BufferFramePending",
];

const SCANNED_ROOTS: &[&str] = &[
    "crates/hammer-runtime/src",
    "crates/hammer-runtime/tests",
    "crates/hammer-service/src",
    "crates/hammer-service/tests",
    "crates/hammer-app/src",
    "crates/hammer-ipc/src",
    "crates/hammer-component-macros/src",
];

#[test]
fn moved_buffer_and_frame_primitives_are_not_imported_from_adapter_surfaces() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let mut violations = std::vec::Vec::new();

    for root in SCANNED_ROOTS {
        collect_rs_violations(&workspace.join(root), &mut violations);
    }

    assert!(
        violations.is_empty(),
        "moved buffer/frame primitives must use hammer_core::data_plane, not adapter paths:\n{}",
        violations.join("\n")
    );
}

fn collect_rs_violations(path: &Path, violations: &mut std::vec::Vec<String>) {
    if !path.exists() {
        return;
    }
    if path.is_file() {
        if path.extension().is_some_and(|ext| ext == "rs") {
            scan_file(path, violations);
        }
        return;
    }

    for entry in fs::read_dir(path).expect("read scan dir") {
        let entry = entry.expect("scan entry");
        collect_rs_violations(&entry.path(), violations);
    }
}

fn scan_file(path: &Path, violations: &mut std::vec::Vec<String>) {
    let text = fs::read_to_string(path).expect("read rust file");
    for (line_index, line) in text.lines().enumerate() {
        for pattern in MOVED_ADAPTER_PATTERNS {
            if line.contains(pattern) {
                violations.push(format!(
                    "{}:{}: {}",
                    path.display(),
                    line_index + 1,
                    pattern
                ));
            }
        }
    }
}
