use std::fs;
use std::path::{Path, PathBuf};

struct RetiredAdapterPattern {
    term: &'static str,
    reason: &'static str,
}

const RETIRED_ADAPTER_SOURCE_PATTERNS: &[RetiredAdapterPattern] = &[
    RetiredAdapterPattern {
        term: "hammer_adapter",
        reason: "deleted adapter crate paths must not remain in active sources",
    },
    RetiredAdapterPattern {
        term: "hammer-adapter",
        reason: "deleted adapter crate names must not remain in active sources",
    },
    RetiredAdapterPattern {
        term: "hammer_runtime::adapter",
        reason: "old runtime adapter module paths are deleted",
    },
    RetiredAdapterPattern {
        term: "SocketProtector",
        reason: "runtime socket-protection compatibility surfaces were deleted",
    },
    RetiredAdapterPattern {
        term: "RuntimePlatform",
        reason: "runtime platform compatibility wrappers were deleted",
    },
    RetiredAdapterPattern {
        term: "PlatformInterface",
        reason: "OS-facing platform traits were deleted",
    },
    RetiredAdapterPattern {
        term: "NetworkInterface",
        reason: "OS-facing network interface models were deleted",
    },
    RetiredAdapterPattern {
        term: "DefaultInterfaceUpdateListener",
        reason: "OS-facing default-interface listeners were deleted",
    },
    RetiredAdapterPattern {
        term: "TunOptions",
        reason: "OS-facing tun option models were deleted",
    },
    RetiredAdapterPattern {
        term: "WifiState",
        reason: "OS-facing wifi state models were deleted",
    },
    RetiredAdapterPattern {
        term: "CertificateProviderService",
        reason: "adapter certificate service traits were deleted",
    },
    RetiredAdapterPattern {
        term: "NetworkManager",
        reason: "adapter network manager traits were deleted",
    },
    RetiredAdapterPattern {
        term: "AsAnyComponent",
        reason: "adapter component compatibility traits were deleted",
    },
    RetiredAdapterPattern {
        term: "ConnectionHandle",
        reason: "adapter connection traits were deleted",
    },
    RetiredAdapterPattern {
        term: "ConnectionManager",
        reason: "adapter connection manager traits were deleted",
    },
    RetiredAdapterPattern {
        term: "ServiceManager",
        reason: "adapter service manager traits were deleted",
    },
    RetiredAdapterPattern {
        term: "WakeupFd",
        reason: "adapter wakeup traits were deleted",
    },
];

#[test]
fn hammer_adapter_is_not_an_active_workspace_crate_or_dependency() {
    let root = workspace_root();
    let root_manifest =
        fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest");
    let cargo_lock = fs::read_to_string(root.join("Cargo.lock")).expect("read Cargo.lock");
    let mut violations = Vec::new();

    if root_manifest.contains("\"crates/hammer-adapter\"") {
        violations.push("workspace members still include crates/hammer-adapter".to_owned());
    }
    if root.join("crates/hammer-adapter").exists() {
        violations.push("crates/hammer-adapter still exists".to_owned());
    }

    for manifest in crate_manifests(&root) {
        let text = fs::read_to_string(&manifest)
            .unwrap_or_else(|err| panic!("read {}: {err}", manifest.display()));
        if text.contains("hammer-adapter") {
            violations.push(format!(
                "{} still declares hammer-adapter",
                manifest.strip_prefix(&root).unwrap_or(&manifest).display()
            ));
        }
    }
    if cargo_lock.contains("name = \"hammer-adapter\"") {
        violations.push("Cargo.lock still contains a hammer-adapter package".to_owned());
    }

    assert!(
        violations.is_empty(),
        "hammer-adapter must be deleted as an active crate/dependency:\n{}",
        violations.join("\n")
    );
}

#[test]
fn current_active_surfaces_do_not_reference_deleted_adapter_contracts() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for file in source_files(&root) {
        let text = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("read {}: {err}", file.display()));
        for (line_index, line) in text.lines().enumerate() {
            if let Some(pattern) = find_retired_adapter_pattern(line) {
                violations.push(format!(
                    "{}:{}: `{}` ({})",
                    file.strip_prefix(&root).unwrap_or(&file).display(),
                    line_index + 1,
                    line.trim(),
                    pattern.reason
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "current active surfaces must not reference deleted adapter contracts:\n{}",
        violations.join("\n")
    );
}

#[test]
fn curated_retired_adapter_patterns_flag_deleted_os_facing_contracts() {
    assert_eq!(
        find_retired_adapter_pattern("fn bind(platform: PlatformInterface) {}")
            .map(|pattern| pattern.reason),
        Some("OS-facing platform traits were deleted")
    );
    assert_eq!(
        find_retired_adapter_pattern("pub struct SocketProtector;").map(|pattern| pattern.reason),
        Some("runtime socket-protection compatibility surfaces were deleted")
    );
    assert_eq!(
        find_retired_adapter_pattern("type Listener = DefaultInterfaceUpdateListener;")
            .map(|pattern| pattern.reason),
        Some("OS-facing default-interface listeners were deleted")
    );
}

#[test]
fn curated_retired_adapter_patterns_allow_current_runtime_surfaces() {
    assert!(find_retired_adapter_pattern("use hammer_runtime::DataPlaneRuntime;").is_none());
    assert!(find_retired_adapter_pattern("pub trait ComponentMetadata {").is_none());
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("hammer-runtime lives under crates/hammer-runtime")
        .to_path_buf()
}

fn crate_manifests(root: &Path) -> Vec<PathBuf> {
    let mut manifests = Vec::new();
    collect_named_files(&root.join("crates"), "Cargo.toml", &mut manifests);
    manifests.sort();
    manifests
}

fn source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for relative in [
        "crates/hammer-core",
        "crates/hammer-runtime",
        "crates/hammer-service",
        "crates/hammer-app",
        "crates/hammer-ipc",
        "crates/hammer-component-macros",
        "crates/hammer",
        "crates/hammerctl",
    ] {
        collect_rs_files(&root.join(relative), &mut files);
    }
    files.sort();
    files
}

fn find_retired_adapter_pattern(line: &str) -> Option<&'static RetiredAdapterPattern> {
    RETIRED_ADAPTER_SOURCE_PATTERNS
        .iter()
        .find(|pattern| line.contains(pattern.term))
}

fn collect_named_files(dir: &Path, name: &str, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("read directory entry").path();
        if path.is_dir() {
            collect_named_files(&path, name, files);
        } else if path.file_name().and_then(|file_name| file_name.to_str()) == Some(name) {
            files.push(path);
        }
    }
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("read directory entry").path();
        if should_skip_source_path(&path) {
            continue;
        }
        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn should_skip_source_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name == "adapter_deletion_guard.rs"
                || name == "data_plane_buffer_owner_guard.rs"
                || name == "data_plane_graph_identity.rs"
                || name == "graph_runtime_owner_guard.rs"
        })
}
