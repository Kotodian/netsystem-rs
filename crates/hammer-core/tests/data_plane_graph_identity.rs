use std::fs;
use std::path::{Path, PathBuf};

use hammer_core::data_plane::{
    NodeHandle, NodeId, NodeKind, NodeNext, NodeRegistration, NodeState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExampleNext {
    Drop,
    Punt,
}

impl ExampleNext {
    const COUNT: usize = 2;
}

impl NodeNext for ExampleNext {
    fn slot(self) -> u16 {
        match self {
            Self::Drop => 0,
            Self::Punt => 1,
        }
    }
}

#[test]
fn core_data_plane_graph_identity_items_have_expected_behavior() {
    let node = NodeId::new(7);
    assert_eq!(node.slot(), 7);

    let handle = NodeHandle::new(42);
    assert_eq!(handle, NodeHandle::new(42));

    let registration = NodeRegistration::next("ip-input", ExampleNext::COUNT);
    assert_eq!(registration.name(), Some("ip-input"));
    assert!(matches!(
        registration,
        NodeRegistration::Next {
            name: "ip-input",
            next_count: 2,
        }
    ));

    let sibling = NodeRegistration::sibling_of("ip-input-ipv6", "ip-input");
    assert_eq!(sibling.name(), Some("ip-input-ipv6"));
    assert!(matches!(
        sibling,
        NodeRegistration::Sibling {
            name: "ip-input-ipv6",
            sibling_of: "ip-input",
        }
    ));

    let nexts = [NodeId::new(3), NodeId::new(9)];
    assert_eq!(nexts[usize::from(NodeNext::slot(ExampleNext::Drop))], NodeId::new(3));
    assert_eq!(nexts[usize::from(NodeNext::slot(ExampleNext::Punt))], NodeId::new(9));

    assert_eq!(NodeKind::Driver, NodeKind::Driver);
    assert_eq!(NodeKind::Internal, NodeKind::Internal);
    assert_eq!(NodeState::Disabled, NodeState::Disabled);
    assert_eq!(NodeState::default(), NodeState::Polling);
    assert_eq!(NodeNext::slot(ExampleNext::Drop), 0u16);
    assert_eq!(NodeNext::slot(42u16), 42u16);
}

#[test]
fn runtime_and_macro_surfaces_do_not_use_adapter_graph_identity_paths() {
    let root = workspace_root();
    let files = scanned_files(&root);
    let mut violations = Vec::new();

    for file in files {
        let text = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("read {}: {err}", file.display()));

        for banned_path in BANNED_EXPLICIT_PATHS {
            for (line_number, line) in text.lines().enumerate() {
                if line.contains(banned_path) {
                    violations.push(format!(
                        "{}:{}: `{}`",
                        file.strip_prefix(&root).unwrap_or(&file).display(),
                        line_number + 1,
                        line.trim()
                    ));
                }
            }
        }

        let normalized_statements = normalized_statements(&text);
        for statement in &normalized_statements {
            if grouped_import_uses_adapter_identity(statement) {
                violations.push(format!(
                    "{}: `{}`",
                    file.strip_prefix(&root).unwrap_or(&file).display(),
                    statement
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "adapter graph identity owner paths remain:\n{}",
        violations.join("\n")
    );
}

const BANNED_IDENTITY_NAMES: &[&str] = &[
    "NodeId",
    "NodeHandle",
    "NodeKind",
    "NodeState",
    "NodeRegistration",
    "NodeNext",
];

const BANNED_EXPLICIT_PATHS: &[&str] = &[
    "hammer_adapter::NodeId",
    "hammer_adapter::NodeHandle",
    "hammer_adapter::NodeKind",
    "hammer_adapter::NodeState",
    "hammer_adapter::NodeRegistration",
    "hammer_adapter::NodeNext",
    "hammer_adapter::node::NodeId",
    "hammer_adapter::node::NodeHandle",
    "hammer_adapter::node::NodeKind",
    "hammer_adapter::node::NodeState",
    "hammer_adapter::node::NodeRegistration",
    "hammer_adapter::node::NodeNext",
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("hammer-core lives under crates/hammer-core")
        .to_path_buf()
}

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    const DIRS: &[&str] = &[
        "crates/hammer-runtime/src",
        "crates/hammer-service/src",
        "crates/hammer-service/tests",
        "crates/hammer-app/src",
        "crates/hammer-ipc/src",
        "crates/hammer-component-macros/src",
    ];

    let mut files = Vec::new();
    for relative in DIRS {
        collect_rs_files(&root.join(relative), &mut files);
    }
    files.sort();
    files.dedup();
    files
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if !dir.is_dir() {
        return;
    }

    let entries = fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|err| panic!("read entry under {}: {err}", dir.display()))
            .path();
        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn normalized_statements(text: &str) -> Vec<String> {
    text.split(';')
        .map(|statement| {
            statement
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>()
        })
        .filter(|statement| !statement.is_empty())
        .collect()
}

fn grouped_import_uses_adapter_identity(statement: &str) -> bool {
    let is_adapter_group = statement.starts_with("usehammer_adapter::{")
        || statement.starts_with("pubusehammer_adapter::{")
        || statement.starts_with("usehammer_adapter::node::{")
        || statement.starts_with("pubusehammer_adapter::node::{");
    is_adapter_group && contains_banned_identity_name(statement)
}

fn contains_banned_identity_name(statement: &str) -> bool {
    BANNED_IDENTITY_NAMES
        .iter()
        .any(|name| statement.contains(name))
}

#[test]
fn scan_roots_only_cover_current_active_sources() {
    let root = workspace_root();
    let files = scanned_files(&root);

    assert!(
        files
            .iter()
            .all(|path| !path.starts_with(root.join("crates/hammer-adapter/src"))),
        "deleted adapter sources must not be treated as active scan roots",
    );
}

#[test]
fn grouped_adapter_identity_imports_are_rejected() {
    assert!(
        grouped_import_uses_adapter_identity("usehammer_adapter::{NodeId,NodeHandle}"),
        "guard should reject grouped adapter imports for graph identity items",
    );
    assert!(
        !grouped_import_uses_adapter_identity("usehammer_core::data_plane::{NodeId,NodeHandle}"),
        "guard should allow current core-owned graph identity imports",
    );
}
