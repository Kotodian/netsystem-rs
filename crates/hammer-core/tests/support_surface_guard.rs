use std::fs;
use std::path::{Path, PathBuf};

struct RetiredSurfacePattern {
    phrases: &'static [&'static str],
    reason: &'static str,
}

const RETIRED_SURFACE_PATTERNS: &[RetiredSurfacePattern] = &[
    RetiredSurfacePattern {
        phrases: &["networkextension"],
        reason: "NetworkExtension is no longer a supported product surface",
    },
    RetiredSurfacePattern {
        phrases: &["nepackettunnelprovider"],
        reason: "NetworkExtension tunnel-provider integration is retired",
    },
    RetiredSurfacePattern {
        phrases: &["netext"],
        reason: "NetExt documentation is retired",
    },
    RetiredSurfacePattern {
        phrases: &["swift binding", "swift bindings", "swift/uniffi bindings"],
        reason: "Swift bindings are no longer a supported surface",
    },
    RetiredSurfacePattern {
        phrases: &[
            "uniffi binding",
            "uniffi bindings",
            "generate swift bindings with uniffi",
        ],
        reason: "UniFFI bindings are no longer a supported surface",
    },
    RetiredSurfacePattern {
        phrases: &["hammer-ffi"],
        reason: "hammer-ffi is no longer a supported crate surface",
    },
    RetiredSurfacePattern {
        phrases: &["hammer_ffi"],
        reason: "hammer_ffi identifiers should not remain in first-party support surfaces",
    },
    RetiredSurfacePattern {
        phrases: &["hammerffi"],
        reason: "generated HammerFFI naming is retired",
    },
    RetiredSurfacePattern {
        phrases: &["xcframework"],
        reason: "xcframework packaging is retired",
    },
    RetiredSurfacePattern {
        phrases: &["dist/ios"],
        reason: "dist/ios generated output is retired",
    },
    RetiredSurfacePattern {
        phrases: &["aarch64-apple-ios"],
        reason: "iOS Rust targets are not supported build targets",
    },
    RetiredSurfacePattern {
        phrases: &["iphoneos_deployment_target"],
        reason: "iOS deployment target configuration is retired",
    },
    RetiredSurfacePattern {
        phrases: &["build-xcframework"],
        reason: "xcframework build scripts are retired",
    },
    RetiredSurfacePattern {
        phrases: &["clean-ios-lib"],
        reason: "iOS generated-output cleanup targets are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ios packaging"],
        reason: "iOS packaging is not a supported path",
    },
    RetiredSurfacePattern {
        phrases: &["ios vpn"],
        reason: "iOS VPN is no longer a supported product identity",
    },
    RetiredSurfacePattern {
        phrases: &["ios / macos"],
        reason: "iOS should not be documented as a supported platform surface",
    },
    RetiredSurfacePattern {
        phrases: &["generated ios"],
        reason: "generated iOS output is retired",
    },
    RetiredSurfacePattern {
        phrases: &["generated framework output"],
        reason: "generated framework artifacts are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi path"],
        reason: "FFI product paths are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi/runtime"],
        reason: "FFI runtime callers are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi 调用方"],
        reason: "FFI callers are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi 命令"],
        reason: "FFI commands are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi 用法"],
        reason: "FFI usage documentation is retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi/打包"],
        reason: "FFI packaging entry points are retired",
    },
    RetiredSurfacePattern {
        phrases: &["ffi / log-dump"],
        reason: "FFI compatibility comments are retired",
    },
];

#[test]
fn first_party_surfaces_do_not_advertise_retired_ios_or_ffi_support() {
    let root = workspace_root();
    let files = scanned_files(&root);

    let mut violations = Vec::new();
    for file in files {
        let text = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("read {}: {err}", file.display()));
        for (line_number, line) in text.lines().enumerate() {
            if let Some(pattern) = find_retired_surface_pattern(line) {
                violations.push(format!(
                    "{}:{}: `{}` ({})",
                    file.strip_prefix(&root).unwrap_or(&file).display(),
                    line_number + 1,
                    line.trim(),
                    pattern.reason
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "retired iOS/FFI support-surface claims remain:\n{}",
        violations.join("\n")
    );
}

#[test]
fn scan_roots_include_current_support_surfaces_and_skip_history() {
    let root = workspace_root();
    let files = scanned_files(&root);

    assert!(contains_relative_path(&files, Path::new("README.md")));
    assert!(contains_relative_path(&files, Path::new("AGENTS.md")));
    assert!(contains_relative_path(&files, Path::new("CONTEXT.md")));
    assert!(contains_relative_path(
        &files,
        Path::new("docs/agents/issue-tracker.md")
    ));
    assert!(!contains_relative_path(
        &files,
        Path::new("docs/adr/0007-retire-ios-support.md")
    ));
    assert!(!contains_relative_path(
        &files,
        Path::new(".superpowers/sdd/2026-07-09-runtime-ownership-adapter-ios/task-35-report.md")
    ));
}

#[test]
fn curated_patterns_ignore_repo_identity_std_ffi_and_platform_cfgs() {
    assert!(find_retired_surface_pattern("repo = Kotodian/hammer-ios-rs").is_none());
    assert!(find_retired_surface_pattern("use std::ffi::CString;").is_none());
    assert!(find_retired_surface_pattern("#[cfg(target_os = \"ios\")]").is_none());
    assert!(find_retired_surface_pattern("swiftly move packets").is_none());
    assert!(find_retired_surface_pattern("unified queue ownership").is_none());
}

#[test]
fn curated_patterns_still_catch_retired_support_phrases() {
    assert_eq!(
        find_retired_surface_pattern("Generate Swift bindings with UniFFI")
            .map(|pattern| pattern.reason),
        Some("Swift bindings are no longer a supported surface")
    );
    assert_eq!(
        find_retired_surface_pattern("Package Hammer.xcframework into dist/ios")
            .map(|pattern| pattern.reason),
        Some("xcframework packaging is retired")
    );
    assert_eq!(
        find_retired_surface_pattern("This was an iOS VPN entry point")
            .map(|pattern| pattern.reason),
        Some("iOS VPN is no longer a supported product identity")
    );
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("hammer-core lives under crates/hammer-core")
        .to_path_buf()
}

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    const ROOT_SUPPORT_FILES: &[&str] = &[
        "README.md",
        "AGENTS.md",
        "CONTEXT.md",
        "Cargo.toml",
        "Cargo.lock",
        "Makefile",
        "rust-toolchain.toml",
    ];
    const SUPPORT_DIRS: &[&str] = &["docs/agents", "scripts", "crates"];

    let mut files = Vec::new();
    for relative in ROOT_SUPPORT_FILES {
        let path = root.join(relative);
        if path.is_file() {
            files.push(path);
        }
    }
    for relative in SUPPORT_DIRS {
        let path = root.join(relative);
        if path.is_dir() {
            collect_first_party_files(&path, &mut files);
        }
    }
    files.sort();
    files.dedup();
    files
}

fn collect_first_party_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|err| panic!("read entry under {}: {err}", dir.display()))
            .path();
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("tests") {
                continue;
            }
            collect_first_party_files(&path, files);
        } else if is_scanned_file(&path) {
            files.push(path);
        }
    }
}

fn contains_relative_path(files: &[PathBuf], relative: &Path) -> bool {
    files.iter().any(|path| path.ends_with(relative))
}

fn find_retired_surface_pattern(line: &str) -> Option<&'static RetiredSurfacePattern> {
    let lower = line.to_lowercase();
    RETIRED_SURFACE_PATTERNS
        .iter()
        .find(|pattern| pattern.phrases.iter().any(|phrase| lower.contains(phrase)))
}

fn is_scanned_file(path: &Path) -> bool {
    if path.starts_with("scripts")
        || path
            .components()
            .any(|component| component.as_os_str() == "scripts")
    {
        return true;
    }

    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("md" | "toml" | "rs" | "sh" | "py" | "zsh" | "bash")
    ) || path.file_name().and_then(|name| name.to_str()) == Some("Makefile")
}
