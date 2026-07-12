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
    "hammer_adapter::Index",
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
    "hammer_adapter::buffer::Index",
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

const MOVED_ROOT_IMPORTS: &[&str] = &[
    "DEFAULT_BUFFER_FRAME_CAPACITY",
    "DEFAULT_BUFFER_FRAME_POOL_SIZE",
    "BUFFER_CACHE_LINE_SIZE",
    "DEFAULT_PACKET_HEADROOM",
    "DEFAULT_PRE_DATA_SIZE",
    "BUFFER_INVALID_INDEX",
    "BUFFER_THREAD_CACHE_BATCH",
    "BUFFER_THREAD_CACHE_HIGH_WATER",
    "BUFFER_IN_USE_FOLD_THRESHOLD",
    "PrimaryOpaque",
    "SecondaryOpaque",
    "BufferFlags",
    "BufferHeaderCacheline0",
    "BufferHeaderCacheline1",
    "Buffer",
    "Index",
    "BufferFrame",
    "Frame",
    "Next",
    "Pending",
    "BufferPacketCursor",
    "BufferNodeError",
    "DataPlaneBuffers",
    "BufferPool",
    "BufferPoolArena",
    "BufferRef",
    "BufferRefMut",
    "BufferThreadCache",
    "DataPlaneBufferConfig",
    "BufferFrameDrain",
    "DataPlaneBufferChain",
    "BufferFramePending",
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
        if path.extension().is_some_and(|ext| ext == "rs") && !is_guard_fixture(path) {
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

    for (statement_line, statement) in use_statements(&text) {
        if let Some(moved_import) = moved_root_import_in_use_statement(&statement) {
            violations.push(format!(
                "{}:{}: hammer_adapter::{{{}}}",
                path.display(),
                statement_line,
                moved_import
            ));
        }
    }
}

fn use_statements(text: &str) -> std::vec::Vec<(usize, String)> {
    let mut statements = std::vec::Vec::new();
    let mut start = None;

    for (line_index, line) in text.lines().enumerate() {
        if start.is_none() && line.contains("use ") {
            start = Some(line_index);
        }

        if let Some(start_line) = start {
            if line.contains(';') {
                let statement = text
                    .lines()
                    .skip(start_line)
                    .take(line_index - start_line + 1)
                    .collect::<std::vec::Vec<_>>()
                    .join("\n");
                statements.push((start_line + 1, statement));
                start = None;
            }
        }
    }

    statements
}

fn is_guard_fixture(path: &Path) -> bool {
    let Some(relative_path) = path.strip_prefix(workspace_root()).ok() else {
        return false;
    };

    matches!(
        relative_path.to_string_lossy().as_ref(),
        "crates/hammer-runtime/tests/adapter_deletion_guard.rs"
            | "crates/hammer-core/tests/data_plane_buffer_owner_guard.rs"
            | "crates/hammer-core/tests/data_plane_graph_identity.rs"
            | "crates/hammer-runtime/tests/graph_runtime_owner_guard.rs"
    )
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("hammer-core lives under crates/hammer-core")
}

fn moved_root_import_in_use_statement(statement: &str) -> Option<&'static str> {
    let (_, remainder) = statement.split_once("use hammer_adapter::{")?;
    let (inside_braces, _) = remainder.split_once('}')?;
    for imported in inside_braces.split(',') {
        let imported = imported.trim();
        let imported = imported.split_whitespace().next().unwrap_or(imported);
        if let Some(moved_import) = MOVED_ROOT_IMPORTS
            .iter()
            .copied()
            .find(|item| *item == imported)
        {
            return Some(moved_import);
        }
    }

    None
}

#[test]
fn root_braced_adapter_imports_for_moved_primitives_are_rejected() {
    let statement = "use hammer_adapter::{Index, DataPlaneRuntime};";
    assert_eq!(
        moved_root_import_in_use_statement(statement),
        Some("Index")
    );
}

#[test]
fn multiline_root_braced_adapter_imports_are_rejected() {
    let statement = "use hammer_adapter::{\n    BufferFrame,\n    DataPlaneRuntime,\n};";
    assert_eq!(
        moved_root_import_in_use_statement(statement),
        Some("BufferFrame")
    );
}

#[test]
fn current_runtime_root_imports_do_not_trigger_buffer_owner_guard() {
    let statement = "use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};";
    assert_eq!(moved_root_import_in_use_statement(statement), None);
}
