//! Source guard: production hammer daemon modules must not allocate via `std::vec`.

use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &[
    "std::vec::Vec",
    "std::vec!",
    "alloc::vec::Vec",
    "alloc::vec!",
];

#[test]
fn production_hammer_src_avoids_std_vec() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    collect_violations(&root, &mut violations);
    assert!(
        violations.is_empty(),
        "production hammer src must use hammer_infra::vec; found:\n{}",
        violations.join("\n")
    );
}

fn collect_violations(dir: &Path, violations: &mut Vec<String>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display()));
    for entry in entries {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_violations(&path, violations);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.contains("test") {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        for (line_no, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            for forbidden in FORBIDDEN {
                if line.contains(forbidden) {
                    violations.push(format!(
                        "{}:{}: contains `{forbidden}`",
                        path.display(),
                        line_no + 1
                    ));
                }
            }
        }
    }
}
