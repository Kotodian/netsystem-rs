use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn hammer_adapter_is_not_an_active_workspace_crate_or_dependency() {
    let root = workspace_root();
    let root_manifest =
        fs::read_to_string(root.join("Cargo.toml")).expect("read workspace manifest");
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

    assert!(
        violations.is_empty(),
        "hammer-adapter must be deleted as an active crate/dependency:\n{}",
        violations.join("\n")
    );
}

#[test]
fn current_source_surfaces_do_not_reference_hammer_adapter() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for file in source_files(&root) {
        let text = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("read {}: {err}", file.display()));
        for (line_index, line) in text.lines().enumerate() {
            if line.contains("hammer_adapter") || line.contains("::hammer_adapter") {
                violations.push(format!(
                    "{}:{}: `{}`",
                    file.strip_prefix(&root).unwrap_or(&file).display(),
                    line_index + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "current source surfaces must not reference hammer_adapter:\n{}",
        violations.join("\n")
    );
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
