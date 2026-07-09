use std::fs;
use std::path::{Path, PathBuf};

use hammer_core::data_plane::{DataPlaneBufferConfig, NodeId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};
use hammer_service::tun::{
    RealTunInput, TunBufferIo, TunBufferSendResult, TunDriverMode, TunPacketSource,
};

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
        "pub fn free_",
        "pub(crate) fn free_",
        "pub fn release_",
        "pub(crate) fn release_",
        "pub fn reclaim_",
        "pub(crate) fn reclaim_",
        "pub fn recycle_",
        "pub(crate) fn recycle_",
        "sync_count",
        "pub fn drop_owned_frame",
        concat!("Node", "Next", "Frames"),
        concat!("submit", "_frame", "("),
        concat!("NodeResult::", "next_frame"),
        concat!("NodeResult::", "next_current"),
        concat!("pub enum ", "Next", "Frame"),
        concat!("Next", "Frame::"),
        concat!("Current", "(NodeId)"),
        concat!("set_", "next_node"),
        concat!("clear_", "next_node"),
        concat!("current_frame", ".next_node"),
        concat!("MAX_NODE_NEXT_", "FRAMES"),
        concat!("Node", "Next", "Frame"),
        concat!("Frame", "Owner", "Consumed"),
        concat!("Node", "Pending", "Process", "Fn"),
        concat!("Tun", "PendingTx"),
        concat!("unsafe impl Send for Tun", "PendingTx"),
        "drop enqueue optional frame",
        "free current indices frame",
        "drain_pending(",
        "transfer_pending(",
        "take_pending_frame",
        "take_scheduled_frame",
        "from_static_buffer_arena",
        "take_inner(&'static str)",
        "consumed_message",
        "next frame already consumed",
        "pending frame already consumed",
        "checked out during ownership transfer",
        "panic::panic_any",
        "std::panic::panic_any",
    ]
}

#[derive(Default)]
struct EmptyTunIo;

impl TunBufferIo for EmptyTunIo {
    fn try_recv_buffer(&mut self, _: &mut [u8]) -> CoreResult<Option<usize>> {
        Ok(None)
    }

    fn try_send_buffers(&mut self, _: &[&[u8]]) -> CoreResult<TunBufferSendResult> {
        Ok(TunBufferSendResult::Complete)
    }
}

#[derive(Default)]
struct FailingTunIo;

impl TunBufferIo for FailingTunIo {
    fn try_recv_buffer(&mut self, _: &mut [u8]) -> CoreResult<Option<usize>> {
        Err(CoreError::internal("scripted recv failure"))
    }

    fn try_send_buffers(&mut self, _: &[&[u8]]) -> CoreResult<TunBufferSendResult> {
        Ok(TunBufferSendResult::Complete)
    }
}

#[test]
fn runtime_service_and_node_hot_paths_use_frame_owner_cleanup() {
    let root = workspace_root();
    let tokens = forbidden_lifetime_tokens();
    let mut failures = String::new();
    for dir in ["crates/hammer-runtime/src", "crates/hammer-service/src"] {
        visit_rust_files(&root.join(dir), &mut |path| {
            let src = fs::read_to_string(path).expect("read source");
            for token in tokens {
                if src.contains(*token) {
                    failures.push_str(&format!("{} contains {}\n", path.display(), token));
                }
            }
        });
    }
    let buffer_src = fs::read_to_string(root.join("crates/hammer-core/src/data_plane/buffer.rs"))
        .expect("read core buffer.rs");
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
        "pub fn drain_pending",
        "pub(crate) fn drain_pending",
        "pub fn reset(",
        "pub fn discard_prefix",
        "pub fn transfer_pending",
        "pub fn take_pending_frame",
        "pub(crate) fn take_scheduled_frame",
        concat!("next_node", ": Option<NodeId>"),
        concat!("pub fn ", "next_node"),
        concat!("pub fn set_", "next_node"),
        concat!("pub(crate) fn clear_", "next_node"),
    ] {
        if forbidden == "pub fn discard_prefix" {
            continue;
        }
        assert!(
            !buffer_src.contains(forbidden),
            "core buffer.rs must not expose lifetime helper `{forbidden}`"
        );
    }
    assert_no_public_lifetime_helpers(&buffer_src, "crates/hammer-core/src/data_plane/buffer.rs");
    assert_no_panic_or_unreachable(
        &root.join("crates/hammer-runtime/src/node/next.rs"),
        "entire file",
    );
    assert_no_panic_or_unreachable_in_buffer_frame_owner_block(&buffer_src);
    assert!(failures.is_empty(), "{failures}");
}

#[test]
fn real_tun_input_releases_allocated_buffer_when_no_packet_arrives() {
    let runtime = tun_runtime();
    let mut frame = runtime
        .buffers()
        .get_next_frame(NodeId::new(0))
        .expect("guard frame");
    let mut input = RealTunInput::new(EmptyTunIo);

    let received = input
        .recv_frame(&runtime, &mut frame, "if0", TunDriverMode::Tun, 1)
        .expect("empty recv");

    assert_eq!(received, 0);
    assert_eq!(frame.pending_len(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn real_tun_input_releases_allocated_buffer_when_recv_errors() {
    let runtime = tun_runtime();
    let mut frame = runtime
        .buffers()
        .get_next_frame(NodeId::new(0))
        .expect("guard frame");
    let mut input = RealTunInput::new(FailingTunIo);

    let err = input
        .recv_frame(&runtime, &mut frame, "if0", TunDriverMode::Tun, 1)
        .expect_err("scripted recv must fail");

    assert!(err.to_string().contains("scripted recv failure"));
    assert_eq!(frame.pending_len(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
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

fn tun_runtime() -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 256,
            buffer_slots: 8,
            frame_capacity: 4,
            frame_slots: 4,
            ..DataPlaneBufferConfig::default()
        },
    })
}

fn assert_no_public_lifetime_helpers(src: &str, label: &str) {
    for line in src.lines() {
        let trimmed = line.trim_start();
        let is_visible = trimmed.starts_with("pub fn ")
            || trimmed.starts_with("pub(crate) fn ")
            || trimmed.starts_with("pub(super) fn ")
            || trimmed.starts_with("pub(in ");
        let is_lifetime_helper = trimmed.contains("fn free_")
            || trimmed.contains("fn release_")
            || trimmed.contains("fn reclaim_")
            || trimmed.contains("fn recycle_");
        assert!(
            !(is_visible && is_lifetime_helper),
            "{label} must not expose visible lifecycle helper `{trimmed}`"
        );
    }
}

fn assert_no_panic_or_unreachable(path: &Path, label: &str) {
    let src = fs::read_to_string(path).expect("read panic invariant source");
    assert!(
        !src.contains("panic!()"),
        "{} {label} must not contain panic!()",
        path.display()
    );
    assert!(
        !src.contains("unreachable!()"),
        "{} {label} must not contain unreachable!()",
        path.display()
    );
}

fn assert_no_panic_or_unreachable_in_buffer_frame_owner_block(buffer_src: &str) {
    let start = buffer_src
        .find("pub struct Next")
        .expect("buffer.rs frame ownership block start");
    let end = buffer_src
        .find("impl BufferPoolInner")
        .expect("buffer.rs frame ownership block end");
    let block = &buffer_src[start..end];
    assert!(
        !block.contains("panic!()"),
        "buffer.rs Frame ownership block must not contain panic!()"
    );
    assert!(
        !block.contains("unreachable!()"),
        "buffer.rs Frame ownership block must not contain unreachable!()"
    );
}
