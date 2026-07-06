use std::fs;
use std::path::{Path, PathBuf};

fn forbidden_lifetime_tokens() -> &'static [&'static str] {
    &[
        ".free(",
        ".free_index(",
        ".free_frame(",
        ".free_frame_index(",
        "free_index(",
        "free_frame(",
        "free_frame_index(",
        "release_pooled",
        ".release_",
        ".reclaim_",
        ".recycle_",
        "pub fn drop_owned_frame",
    ]
}

#[test]
fn runtime_service_and_node_hot_paths_use_frame_owner_cleanup() {
    let root = workspace_root();
    let tokens = forbidden_lifetime_tokens();
    let mut failures = String::new();
    for dir in [
        "crates/hammer-adapter/src",
        "crates/hammer-runtime/src",
        "crates/hammer-service/src",
    ] {
        visit_rust_files(&root.join(dir), &mut |path| {
            if allowed_pool_internal(path) {
                return;
            }
            let src = fs::read_to_string(path).expect("read source");
            for token in tokens {
                if src.contains(*token) {
                    failures.push_str(&format!("{} contains {}\n", path.display(), token));
                }
            }
        });
    }
    let buffer_src = fs::read_to_string(root.join("crates/hammer-adapter/src/buffer.rs"))
        .expect("read buffer.rs");
    for forbidden in [
        "pub fn drop_owned_frame",
        "pub(crate) fn drop_owned_frame",
        "pub fn free_",
        "pub(crate) fn free_",
        "pub fn release_",
        "pub(crate) fn release_",
        "pub fn reclaim_",
        "pub(crate) fn reclaim_",
        "pub fn recycle_",
        "pub(crate) fn recycle_",
    ] {
        assert!(
            !buffer_src.contains(forbidden),
            "buffer.rs must not expose lifetime helper `{forbidden}`"
        );
    }
    assert!(failures.is_empty(), "{failures}");
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates")
        .parent()
        .expect("workspace")
        .to_path_buf()
}

fn visit_rust_files(dir: &Path, f: &mut impl FnMut(&Path)) {
    for entry in fs::read_dir(dir).expect("read dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            visit_rust_files(&path, f);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            f(&path);
        }
    }
}

fn allowed_pool_internal(path: &Path) -> bool {
    path.ends_with("crates/hammer-adapter/src/buffer.rs")
}
