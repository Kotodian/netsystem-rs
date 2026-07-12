//! Architecture checks for Graph Fanout (spec Testing Decisions).
//!
//! Production service nodes must not perform worker-local next-frame
//! get/push/put. Direct frame acquisition remains limited to Handoff
//! continuation and RAII scratch frames keyed by `NodeId::new(0)`.

use std::fs;
use std::path::{Path, PathBuf};

fn crate_src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn walk_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| panic!("entry: {err}"));
        let path = entry.path();
        if path.is_dir() {
            walk_rs(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Drop `#[cfg(test)]` items so architecture checks only see production code.
fn strip_cfg_test(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let bytes = source.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(rel) = source[i..].find("#[cfg(test)]") {
            out.push_str(&source[i..i + rel]);
            let mut j = i + rel + "#[cfg(test)]".len();
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            // Skip additional attributes before the item.
            while source[j..].starts_with("#[") {
                if let Some(end) = source[j..].find(']') {
                    j += end + 1;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                } else {
                    break;
                }
            }
            let rest = &source[j..];
            let item = [
                "mod ", "fn ", "struct ", "impl ", "enum ", "trait ", "type ", "const ", "static ",
                "use ", "async ", "unsafe ", "pub ",
            ]
            .iter()
            .filter_map(|p| rest.find(p).map(|o| (o, *p)))
            .min_by_key(|(o, _)| *o);
            let Some((item_off, _)) = item else {
                break;
            };
            j += item_off;
            let mut k = j;
            while k < bytes.len() && bytes[k] != b'{' && bytes[k] != b';' {
                k += 1;
            }
            if k >= bytes.len() {
                break;
            }
            if bytes[k] == b';' {
                i = k + 1;
                continue;
            }
            let mut depth = 0i32;
            while k < bytes.len() {
                match bytes[k] {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            k += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                k += 1;
            }
            i = k;
        } else {
            out.push_str(&source[i..]);
            break;
        }
    }
    out
}

fn production_sources() -> Vec<(PathBuf, String)> {
    let mut paths = Vec::new();
    walk_rs(&crate_src(), &mut paths);
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let raw = fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
            (path, strip_cfg_test(&raw))
        })
        .collect()
}

fn rel_path(path: &Path) -> String {
    path.strip_prefix(crate_src())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn line_contexts<'a>(source: &'a str, needle: &str) -> Vec<(usize, &'a str)> {
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains(needle))
        .map(|(idx, line)| (idx + 1, line.trim()))
        .collect()
}

#[test]
fn production_put_next_frame_is_handoff_only() {
    let mut offenders = Vec::new();
    for (path, source) in production_sources() {
        for (line, text) in line_contexts(&source, "put_next_frame(") {
            let rel = rel_path(&path);
            let allowed = rel == "data_plane.rs" && source.contains("fn handoff_node_process");
            if !allowed {
                offenders.push(format!("{rel}:{line}: {text}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "production put_next_frame is limited to Handoff continuation; found:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn production_get_next_frame_is_handoff_or_raii_scratch() {
    let mut offenders = Vec::new();
    for (path, source) in production_sources() {
        for (line, text) in line_contexts(&source, "get_next_frame(") {
            let rel = rel_path(&path);
            let handoff = rel == "data_plane.rs" && text.contains("get_next_frame(next)");
            let raii_scratch = text.contains("get_next_frame(NodeId::new(0))")
                && matches!(
                    rel.as_str(),
                    "net/ip/reassembly.rs" | "tun/mod.rs"
                );
            if !(handoff || raii_scratch) {
                offenders.push(format!("{rel}:{line}: {text}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "production get_next_frame is limited to Handoff or NodeId::new(0) RAII scratch; found:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn production_nodes_do_not_one_buffer_get_push_put_by_target_node() {
    // Classic anti-pattern: acquire next by NodeId, push one index, put.
    let pattern_needles = [
        "get_next_frame(tcp_output)",
        "get_next_frame(drop_next)",
        "get_next_frame(tcp_established)",
        "get_next_frame(lookup)",
        "get_next_frame(output)",
    ];
    let mut offenders = Vec::new();
    for (path, source) in production_sources() {
        for needle in pattern_needles {
            for (line, text) in line_contexts(&source, needle) {
                offenders.push(format!("{}:{line}: {text}", rel_path(&path)));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "production nodes must not acquire Next Frames by target NodeId locals; found:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn feature_and_protocol_packet_paths_do_not_enqueue_by_node_id() {
    // Control may still resolve NodeId while compiling arcs. Packet paths must
    // not acquire or put Next Frames themselves.
    let hot_paths = [
        "src/feature_arc.rs",
        "src/transport/icmp/input.rs",
        "src/transport/udp/input.rs",
        "src/net/lookup/mod.rs",
        "src/transport/tcp/input.rs",
        "src/transport/tcp/established.rs",
        "src/transport/tcp/rcv_process.rs",
        "src/transport/tcp/listen.rs",
        "src/transport/tcp/syn_sent.rs",
        "src/net/ip/reassembly.rs",
        "src/tun/mod.rs",
    ];
    let forbidden = ["put_next_frame("];
    let mut offenders = Vec::new();
    for rel in hot_paths {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let source = strip_cfg_test(&raw);
        for needle in forbidden {
            for (line, text) in line_contexts(&source, needle) {
                offenders.push(format!("{rel}:{line}: {text}"));
            }
        }
        // get_next_frame is forbidden on these packet paths except documented
        // RAII scratch (NodeId::new(0)) in reassembly/tun.
        for (line, text) in line_contexts(&source, "get_next_frame(") {
            let raii_scratch = text.contains("get_next_frame(NodeId::new(0))")
                && matches!(rel, "src/net/ip/reassembly.rs" | "src/tun/mod.rs");
            if !raii_scratch {
                offenders.push(format!("{rel}:{line}: {text}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "migrated packet paths must not put Next Frames (and may only scratch-get NodeId::new(0)); found:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn production_sources_do_not_restore_frame_capacity_knob() {
    let mut offenders = Vec::new();
    for (path, source) in production_sources() {
        for (line, text) in line_contexts(&source, "frame_capacity") {
            // Debug impl / capacity getters on buffers are fine; reject config knobs.
            if text.contains("frame_capacity =")
                || text.contains("frame_capacity:")
                || text.contains(".frame_capacity")
            {
                offenders.push(format!("{}:{line}: {text}", rel_path(&path)));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "production service must not reintroduce frame_capacity configuration; found:\n{}",
        offenders.join("\n")
    );
}
