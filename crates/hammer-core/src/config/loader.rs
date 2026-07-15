//! Multi-file config loading via top-level `include`.
//!
//! `parse_config(content)` parses a single TOML string (no `include`).
//! `load_config(path)` reads a main file and recursively merges any
//! `include = [...]` entries — files are parsed directly, directories are
//! scanned for `*.toml` (sorted for deterministic order) and each merged.
//!
//! Merge policy:
//! - Scalars: the main file wins over included fragments; later fragments
//!   win over earlier ones. (Main is applied last.)
//! - Vec fields (`network.interface`, `network.route`, `trace.inputs`):
//!   concatenated (fragments first, main last).
//! - Sub-tables: merged recursively with the same scalar/vec policy.
//!
//! Cycle prevention: every loaded path is canonicalised and recorded; a
//! repeated path is an error.

use std::path::{Path, PathBuf};

use crate::error::{HammerError, HammerResult};

use super::{BootstrapConfig, Config, Memory};

#[derive(serde::Deserialize)]
#[serde(default)]
struct BootstrapDocument {
    include: Vec<String>,
    memory: Option<Memory>,
}

impl Default for BootstrapDocument {
    fn default() -> Self {
        Self {
            include: Vec::new(),
            memory: None,
        }
    }
}

/// Parse a single TOML string into a validated `Config`. The `include` key is
/// ignored here — use `load_config` for multi-file loading.
pub fn parse_config(content: &str) -> HammerResult<Config> {
    let de = toml::Deserializer::parse(content).map_err(translate_toml_error)?;
    let cfg: Config = serde_path_to_error::deserialize(de).map_err(translate_path_error)?;
    cfg.validate()?;
    Ok(cfg)
}

/// Parse only the allocation bootstrap fields from one TOML document.
///
/// The `include` key is ignored here; use [`load_bootstrap_config`] when the
/// startup configuration is split across files.
pub fn parse_bootstrap_config(content: &str) -> HammerResult<BootstrapConfig> {
    let document = parse_bootstrap_document(content)?;
    let mut config = BootstrapConfig::default();
    merge_bootstrap_document(&mut config, &document);
    config.memory.validate()?;
    Ok(config)
}

/// Load only the fields required to initialize the Main Heap.
///
/// The complete include chain is evaluated with the same priority as
/// [`load_config`], but non-bootstrap sections are left for the final config
/// load after the Main Heap has been published.
pub fn load_bootstrap_config(path: &Path) -> HammerResult<BootstrapConfig> {
    let mut seen = std::collections::HashSet::new();
    let main = parse_bootstrap_file(path)?;
    let mut merged = BootstrapConfig::default();
    for include in &main.include {
        let include_path = resolve_include(path, include);
        merge_bootstrap_include(&include_path, &mut seen, &mut merged)?;
    }
    merge_bootstrap_document(&mut merged, &main);
    merged.memory.validate()?;
    Ok(merged)
}

/// Load a `Config` from a main file path, recursively merging any `include`
/// entries (files or directories of `*.toml`).
pub fn load_config(path: &Path) -> HammerResult<Config> {
    let mut seen = std::collections::HashSet::new();
    let (main, main_has_memory) = parse_file(path)?;
    let mut merged = Config::default();
    // Includes first (earlier = lower priority), then main applied last.
    for inc in &main.include {
        let inc_path = resolve_include(path, inc);
        merge_include(&inc_path, &mut seen, &mut merged)?;
    }
    merge_config(&mut merged, &main, main_has_memory);
    merged.include = Vec::new();
    merged.validate()?;
    Ok(merged)
}

fn parse_file(path: &Path) -> HammerResult<(Config, bool)> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| HammerError::internal(format!("read config {}: {e}", path.display())))?;
    let memory_present = toml::from_str::<toml::Table>(&content)
        .map_err(translate_toml_error)?
        .contains_key("memory");
    parse_config(&content).map(|config| (config, memory_present))
}

fn parse_bootstrap_file(path: &Path) -> HammerResult<BootstrapDocument> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        HammerError::internal(format!("read config {}: {error}", path.display()))
    })?;
    parse_bootstrap_document(&content)
}

fn parse_bootstrap_document(content: &str) -> HammerResult<BootstrapDocument> {
    let deserializer = toml::Deserializer::parse(content).map_err(translate_toml_error)?;
    serde_path_to_error::deserialize(deserializer).map_err(translate_path_error)
}

fn merge_bootstrap_include(
    path: &Path,
    seen: &mut std::collections::HashSet<PathBuf>,
    into: &mut BootstrapConfig,
) -> HammerResult<()> {
    let canonical = path.canonicalize().map_err(|error| {
        HammerError::internal(format!("resolve config path {}: {error}", path.display()))
    })?;
    if !seen.insert(canonical.clone()) {
        return Err(HammerError::config_validation(format!(
            "config include cycle: {}",
            canonical.display()
        )));
    }
    if path.is_dir() {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(path)
            .map_err(|error| {
                HammerError::internal(format!("read dir {}: {error}", path.display()))
            })?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|entry| {
                entry
                    .extension()
                    .is_some_and(|extension| extension == "toml")
            })
            .collect();
        entries.sort();
        for entry in entries {
            let document = parse_bootstrap_file(&entry)?;
            for include in &document.include {
                let include_path = resolve_include(&entry, include);
                merge_bootstrap_include(&include_path, seen, into)?;
            }
            merge_bootstrap_document(into, &document);
        }
    } else {
        let document = parse_bootstrap_file(path)?;
        for include in &document.include {
            let include_path = resolve_include(path, include);
            merge_bootstrap_include(&include_path, seen, into)?;
        }
        merge_bootstrap_document(into, &document);
    }
    Ok(())
}

fn merge_bootstrap_document(into: &mut BootstrapConfig, document: &BootstrapDocument) {
    if let Some(memory) = document.memory {
        into.memory = memory;
    }
}

fn merge_include(
    path: &Path,
    seen: &mut std::collections::HashSet<PathBuf>,
    into: &mut Config,
) -> HammerResult<()> {
    let canonical = path.canonicalize().map_err(|e| {
        HammerError::internal(format!("resolve config path {}: {e}", path.display()))
    })?;
    if !seen.insert(canonical.clone()) {
        return Err(HammerError::config_validation(format!(
            "config include cycle: {}",
            canonical.display()
        )));
    }
    if path.is_dir() {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(path)
            .map_err(|e| HammerError::internal(format!("read dir {}: {e}", path.display())))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|ext| ext == "toml"))
            .collect();
        entries.sort();
        for entry in entries {
            let (frag, memory_present) = parse_file(&entry)?;
            // Nested includes inside a fragment are resolved relative to that
            // fragment's directory.
            for inc in &frag.include {
                let inc_path = resolve_include(&entry, inc);
                merge_include(&inc_path, seen, into)?;
            }
            merge_config(into, &frag, memory_present);
        }
    } else {
        let (frag, memory_present) = parse_file(path)?;
        for inc in &frag.include {
            let inc_path = resolve_include(path, inc);
            merge_include(&inc_path, seen, into)?;
        }
        merge_config(into, &frag, memory_present);
    }
    Ok(())
}

/// Apply `src` onto `dst`. Non-Vec sub-structs are replaced wholesale by
/// `src` (last writer wins). Vec fields (`interface`, `route`, `trace.inputs`)
/// append (`src` after `dst`) so fragments compose lists.
fn merge_config(dst: &mut Config, src: &Config, memory_present: bool) {
    dst.log = src.log.clone();
    if memory_present {
        dst.memory = src.memory;
    }
    dst.trace.enabled = src.trace.enabled;
    dst.trace.record_capacity = src.trace.record_capacity;
    dst.trace.packet_capacity = src.trace.packet_capacity;
    dst.trace.inputs.extend(src.trace.inputs.clone());
    dst.network.ip = src.network.ip.clone();
    if src.network.session.is_some() {
        dst.network.session = src.network.session.clone();
    }
    dst.network.interface.extend(src.network.interface.clone());
    dst.network.route.extend(src.network.route.clone());
    dst.worker = src.worker.clone();
}

fn resolve_include(parent: &Path, inc: &str) -> PathBuf {
    let p = PathBuf::from(inc);
    if p.is_absolute() {
        p
    } else {
        parent.parent().unwrap_or_else(|| Path::new("")).join(p)
    }
}

fn translate_path_error(err: serde_path_to_error::Error<toml::de::Error>) -> HammerError {
    let path = err.path().to_string();
    let inner = err.into_inner();
    if let Some(field) = extract_unknown_field(inner.message()) {
        return HammerError::config_validation(format!("unsupported config key: {field}"));
    }
    if path.is_empty() || path == "." {
        translate_toml_error(inner)
    } else {
        HammerError::config_parse(format!(
            "parse TOML: {path}: {}",
            toml_error_message(&inner)
        ))
    }
}

fn translate_toml_error(err: toml::de::Error) -> HammerError {
    let msg = toml_error_message(&err);
    if let Some(field) = extract_unknown_field(msg) {
        return HammerError::config_validation(format!("unsupported config key: {field}"));
    }
    HammerError::config_parse(format!("parse TOML: {msg}"))
}

fn toml_error_message(err: &toml::de::Error) -> &str {
    err.message()
        .strip_prefix("parse TOML: ")
        .unwrap_or_else(|| err.message())
}

fn extract_unknown_field(msg: &str) -> Option<String> {
    let needle = "unknown field ";
    let i = msg.find(needle)?;
    let rest = &msg[i + needle.len()..];
    let mut chars = rest.chars();
    let opener = chars.next()?;
    if opener != '`' && opener != '\'' && opener != '"' {
        return None;
    }
    let inner = &rest[opener.len_utf8()..];
    let close = inner.find(opener)?;
    Some(inner[..close].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn include_merges_directory_fragments() {
        let (root, main) = tempdir_with_main();
        let frag_dir = root.join("conf.d");
        fs::create_dir_all(&frag_dir).unwrap();
        fs::write(
            frag_dir.join("routes.toml"),
            r#"
            [[network.route]]
            prefix = "10.0.0.0/24"
            interface = "tun0"
            "#,
        )
        .unwrap();
        fs::write(
            frag_dir.join("iface.toml"),
            r#"
            [[network.interface]]
            name = "tun0"
            address = ["10.0.0.1/24"]
            "#,
        )
        .unwrap();
        fs::write(
            &main,
            r#"
            include = ["conf.d/"]
            [worker]
            count = 3
            "#,
        )
        .unwrap();
        let cfg = load_config(&main).expect("load");
        assert_eq!(cfg.worker.count, 3);
        assert_eq!(cfg.network.interface.len(), 1);
        assert_eq!(cfg.network.route.len(), 1);
        assert_eq!(
            cfg.network.route[0].prefix,
            ipnet::IpNet::V4(
                ipnet::Ipv4Net::new(std::net::Ipv4Addr::new(10, 0, 0, 0), 24).unwrap()
            )
        );
    }

    #[test]
    fn include_rejects_cycle() {
        let (root, a) = tempdir_with_main();
        let b = root.join("b.toml");
        fs::write(&a, format!("include = [\"{}\"]\n", b.display())).unwrap();
        fs::write(&b, format!("include = [\"{}\"]\n", a.display())).unwrap();
        let err = load_config(&a).expect_err("cycle");
        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn main_overrides_fragment_scalar() {
        let (root, main) = tempdir_with_main();
        let frag_dir = root.join("conf.d");
        fs::create_dir_all(&frag_dir).unwrap();
        fs::write(frag_dir.join("w.toml"), "[worker]\ncount = 8\n").unwrap();
        fs::write(&main, "include = [\"conf.d/\"]\n[worker]\ncount = 2\n").unwrap();
        let cfg = load_config(&main).expect("load");
        assert_eq!(cfg.worker.count, 2);
    }

    #[test]
    fn bootstrap_parse_does_not_construct_the_final_config() {
        let bootstrap = parse_bootstrap_config(
            r#"
            [memory]
            main_heap_size = "256 MiB"

            [worker]
            count = "validated only by the final config"
            "#,
        )
        .expect("parse bootstrap fields only");

        assert_eq!(bootstrap.memory.main_heap_size, 256 << 20);
    }

    /// Create a temp root and return `(root, root/main.toml)` so the main file
    /// lives outside any included fragment directory.
    fn tempdir_with_main() -> (PathBuf, PathBuf) {
        let root = tempdir();
        (root.clone(), root.join("main.toml"))
    }

    fn tempdir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let base = std::env::temp_dir();
        let dir = base.join(format!(
            "hammer-cfg-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            seq
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
