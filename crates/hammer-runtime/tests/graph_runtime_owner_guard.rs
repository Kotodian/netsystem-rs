use std::fs;
use std::path::{Path, PathBuf};

const ROOT_BANNED_RUNTIME_IMPORTS: &[&str] = &[
    "DataPlaneRuntime",
    "DataPlaneRuntimeConfig",
    "FrameBatchWidth",
    "DataWorkerId",
    "DataPlaneHandoff",
    "DataPlaneHandoffWorker",
    "Node",
    "DriverNode",
    "InternalNode",
    "NodeDescriptor",
    "NodeEntry",
    "NodeProcessFn",
    "NodeResult",
    "NodeRuntime",
    "NodeRuntimeData",
    "NodeErrorCounters",
    "NodeRuntimeReady",
    "NodeRuntimeStatsRow",
    "NoopNode",
    "default_prefetch_indices",
    "PacketTrace",
    "TraceControlHandle",
    "TraceControlPlane",
    "DataPlaneTrace",
    "TraceEntry",
    "TraceFormatter",
    "TraceInputPolicy",
    "TracePolicy",
    "TraceRecord",
    "TraceRecordSink",
    "process_frame",
    "add_packet_trace",
];

const RETIRED_ADAPTER_CONTRACT_IMPORTS: &[&str] = &[
    "SocketProtector",
    "RuntimePlatform",
    "PlatformInterface",
    "NetworkInterface",
    "DefaultInterfaceUpdateListener",
    "TunOptions",
    "WifiState",
    "CertificateProviderService",
    "NetworkManager",
    "AsAnyComponent",
    "ConnectionHandle",
    "ConnectionManager",
    "ServiceManager",
    "WakeupFd",
];

const DIRECT_BANNED_RUNTIME_OWNER_PATHS: &[&str] = &[
    "hammer_adapter::DataPlaneRuntime",
    "hammer_adapter::DataPlaneRuntimeConfig",
    "hammer_adapter::FrameBatchWidth",
    "hammer_adapter::DataWorkerId",
    "hammer_adapter::DataPlaneHandoff",
    "hammer_adapter::DataPlaneHandoffWorker",
    "hammer_adapter::Node",
    "hammer_adapter::DriverNode",
    "hammer_adapter::InternalNode",
    "hammer_adapter::NodeDescriptor",
    "hammer_adapter::NodeEntry",
    "hammer_adapter::NodeProcessFn",
    "hammer_adapter::NodeResult",
    "hammer_adapter::NodeRuntime",
    "hammer_adapter::NodeRuntimeData",
    "hammer_adapter::NodeErrorCounters",
    "hammer_adapter::NodeRuntimeReady",
    "hammer_adapter::NodeRuntimeStatsRow",
    "hammer_adapter::NoopNode",
    "hammer_adapter::default_prefetch_indices",
    "hammer_adapter::DataPlaneTrace",
    "hammer_adapter::PacketTrace",
    "hammer_adapter::TraceControlHandle",
    "hammer_adapter::TraceControlPlane",
    "hammer_adapter::TraceEntry",
    "hammer_adapter::TraceFormatter",
    "hammer_adapter::TraceInputPolicy",
    "hammer_adapter::TracePolicy",
    "hammer_adapter::TraceRecord",
    "hammer_adapter::TraceRecordSink",
    "hammer_adapter::process_frame",
    "hammer_adapter::add_packet_trace",
    "hammer_adapter::node::",
    "hammer_adapter::handoff::",
    "hammer_adapter::trace::",
    "hammer_adapter::SocketProtector",
    "hammer_adapter::RuntimePlatform",
    "hammer_adapter::PlatformInterface",
    "hammer_adapter::NetworkInterface",
    "hammer_adapter::DefaultInterfaceUpdateListener",
    "hammer_adapter::TunOptions",
    "hammer_adapter::WifiState",
    "hammer_adapter::CertificateProviderService",
    "hammer_adapter::NetworkManager",
    "hammer_adapter::AsAnyComponent",
    "hammer_adapter::ConnectionHandle",
    "hammer_adapter::ConnectionManager",
    "hammer_adapter::ServiceManager",
    "hammer_adapter::WakeupFd",
];

const SCAN_ROOTS: &[&str] = &[
    ".",
    "../hammer-service",
    "../hammer-app",
    "../hammer-ipc",
    "../hammer-component-macros",
    "../hammer",
    "../hammerctl",
];

#[test]
fn graph_runtime_owner_paths_do_not_point_at_adapter() {
    let violations = collect_source_files()
        .into_iter()
        .flat_map(|path| scan_file_for_violations(&path))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "runtime graph contracts must be owned by hammer-runtime:\n{}",
        violations.join("\n")
    );
}

#[test]
fn grouped_adapter_runtime_and_retired_contract_imports_are_rejected() {
    let source = r#"
        use hammer_adapter::{DataPlaneRuntime, DataPlaneTrace, NodeRuntimeStatsRow, PlatformInterface};
        pub use hammer_adapter::{NodeEntry, TracePolicy};
    "#;

    let violations = scan_source_for_violations("sample.rs", source);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("DataPlaneRuntime")),
        "expected grouped root import to be rejected: {violations:#?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("DataPlaneTrace")),
        "expected grouped trace import to be rejected: {violations:#?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("NodeEntry")),
        "expected grouped pub use to be rejected: {violations:#?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("NodeRuntimeStatsRow")),
        "expected grouped stats import to be rejected: {violations:#?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("TracePolicy")),
        "expected grouped trace re-export to be rejected: {violations:#?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("PlatformInterface")),
        "expected retired adapter contract import to be rejected: {violations:#?}"
    );
}

fn collect_source_files() -> Vec<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for root in SCAN_ROOTS {
        walk_source_tree(&manifest_dir.join(root), &mut files);
    }
    files.sort();
    files.dedup();
    files
}

fn walk_source_tree(path: &Path, files: &mut Vec<PathBuf>) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if metadata.is_dir() {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            walk_source_tree(&entry.path(), files);
        }
        return;
    }

    if !is_relevant_source_file(path) || is_guard_test(path) {
        return;
    }

    files.push(path.to_path_buf());
}

fn is_relevant_source_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "Cargo.toml")
        || path.extension().and_then(|ext| ext.to_str()) == Some("rs")
}

fn is_guard_test(path: &Path) -> bool {
    let Some(relative_path) = path.strip_prefix(workspace_root()).ok() else {
        return false;
    };

    relative_path == Path::new("crates/hammer-runtime/tests/graph_runtime_owner_guard.rs")
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("hammer-runtime lives under crates/hammer-runtime")
}

fn scan_file_for_violations(path: &Path) -> Vec<String> {
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) => {
            return vec![format!("{} could not be read: {error}", path.display())];
        }
    };

    scan_source_for_violations(&path.display().to_string(), &source)
}

fn scan_source_for_violations(path: &str, source: &str) -> Vec<String> {
    let normalized = strip_ascii_whitespace(source);
    let mut violations = std::collections::BTreeSet::new();

    for banned in DIRECT_BANNED_RUNTIME_OWNER_PATHS {
        if normalized.contains(banned) {
            violations.insert(format!("{path} contains {banned}"));
        }
    }

    for group in extract_groups(&normalized, "hammer_adapter::{") {
        collect_grouped_root_violations(path, &group, &mut violations);
    }

    violations.into_iter().collect()
}

fn strip_ascii_whitespace(source: &str) -> String {
    source
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect()
}

fn extract_groups(source: &str, prefix: &str) -> Vec<String> {
    let mut groups = Vec::new();
    let mut search_start = 0;

    while let Some(relative_index) = source[search_start..].find(prefix) {
        let start = search_start + relative_index + prefix.len();
        if let Some((group, next_start)) = extract_balanced_group(source, start) {
            groups.push(group);
            search_start = next_start;
        } else {
            break;
        }
    }

    groups
}

fn extract_balanced_group(source: &str, start: usize) -> Option<(String, usize)> {
    let bytes = source.as_bytes();
    let mut depth = 1usize;
    let mut index = start;

    while index < bytes.len() {
        match bytes[index] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some((source[start..index].to_string(), index + 1));
                }
            }
            _ => {}
        }
        index += 1;
    }

    None
}

fn collect_grouped_root_violations(
    path: &str,
    group: &str,
    violations: &mut std::collections::BTreeSet<String>,
) {
    for entry in split_top_level_items(group) {
        let item = strip_alias(entry);
        if ROOT_BANNED_RUNTIME_IMPORTS.contains(&item)
            || RETIRED_ADAPTER_CONTRACT_IMPORTS.contains(&item)
        {
            violations.insert(format!("{path} contains hammer_adapter::{{{item}}}"));
            continue;
        }

        if let Some((module, rest)) = item.split_once("::") {
            if is_banned_group_module(module) {
                violations.insert(format!(
                    "{path} contains hammer_adapter::{{{module}::{rest}}}"
                ));
            }
        }
    }
}

fn split_top_level_items(group: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut item_start = 0usize;

    for (index, ch) in group.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                if item_start < index {
                    items.push(&group[item_start..index]);
                }
                item_start = index + 1;
            }
            _ => {}
        }
    }

    if item_start < group.len() {
        items.push(&group[item_start..]);
    }

    items.into_iter().filter(|item| !item.is_empty()).collect()
}

fn strip_alias(item: &str) -> &str {
    item.split_once("as").map_or(item, |(name, _)| name)
}

fn is_banned_group_module(module: &str) -> bool {
    matches!(module, "node" | "handoff" | "trace")
}
