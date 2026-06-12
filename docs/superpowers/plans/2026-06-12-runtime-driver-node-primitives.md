# Runtime Driver Node Primitives Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add generic vlib-style runtime primitives for DriverNode state, empty-frame scheduling, and interrupt pending so TCP session nodes can later reuse runtime behavior without TCP/worker-specific helper APIs.

**Architecture:** Keep `DriverNode` as the runtime role for external input/output boundaries. Extend `hammer-adapter` with reusable node state and interrupt scheduling primitives; do not add APIs named after TCP, sessions, workers, or driver installation. A node in `Interrupt` state can be woken by `set_node_interrupt_pending`, which coalesces multiple wakeups into one empty-frame dispatch.

**Tech Stack:** Rust 2024, `hammer-adapter` node runtime, existing `DataPlaneRuntime` frame queue, focused `cargo test -p hammer-adapter --test node_runtime`, shared `CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target`.

---

## Scope Guard

This plan only changes generic runtime behavior in `hammer-adapter`. It does not introduce TCP session nodes, worker install helpers, service/control-plane scheduling APIs, or timer wheel integration. In particular, runtime must not grow one-off APIs that install, register, or schedule a driver node for a specific worker.

## File Layout

- Modify: `crates/hammer-adapter/src/node.rs`
  - Add `NodeState`.
  - Store state and interrupt-pending bits per node.
  - Expose `node_kind`, `node_state`, and `set_node_state`.
  - Add crate-local interrupt coalescing helpers used by `DataPlaneRuntime`.
  - Make disabled nodes skip dispatch if a frame was already queued before disable.
- Modify: `crates/hammer-adapter/src/buffer.rs`
  - Add `schedule_empty_frame(node)`.
  - Add `set_node_interrupt_pending(node)` built on generic node state and empty-frame scheduling.
- Modify: `crates/hammer-adapter/src/lib.rs`
  - Re-export `NodeState`.
- Modify: `crates/hammer-adapter/tests/node_runtime.rs`
  - Add tests for kind/state visibility, empty-frame dispatch, interrupt coalescing, and disabled-node behavior.

## Task 1: Add Runtime Visibility and Empty-Frame Scheduling Tests

**Files:**
- Modify: `crates/hammer-adapter/tests/node_runtime.rs`

- [x] **Step 1: Write failing tests**

Add these imports:

```rust
use hammer_adapter::NodeState;
```

Add these tests after `register_driver_preserves_old_spelling_for_descriptor_nodes`:

```rust
#[test]
fn runtime_exposes_node_kind_and_state() {
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let internal = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::empty(),
    ));

    assert_eq!(runtime.nodes().node_kind(internal).unwrap(), NodeKind::Internal);
    assert_eq!(runtime.nodes().node_kind(driver).unwrap(), NodeKind::Driver);
    assert_eq!(runtime.nodes().node_state(driver).unwrap(), NodeState::Polling);

    runtime
        .nodes()
        .set_node_state(driver, NodeState::Interrupt)
        .expect("set driver interrupt state");
    assert_eq!(runtime.nodes().node_state(driver).unwrap(), NodeState::Interrupt);
}

#[test]
fn schedule_empty_frame_runs_driver_without_packet_vectors() {
    reset_calls(21);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 2);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([21, 0, 0, 0]),
    ));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule empty frame");
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(21), 1);
    assert_eq!(runtime.packet_buffers().frames_in_use(), 0);
}
```

- [x] **Step 2: Run RED test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime runtime_exposes_node_kind_and_state -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_empty_frame_runs_driver_without_packet_vectors -- --exact
```

Expected: FAIL because `NodeState`, `node_kind`, `node_state`, `set_node_state`, and `schedule_empty_frame` do not exist.

- [x] **Step 3: Implement minimal runtime visibility and empty-frame scheduling**

In `crates/hammer-adapter/src/node.rs`, add:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NodeState {
    Disabled,
    #[default]
    Polling,
    Interrupt,
}
```

Add `node_states: Vec<NodeState>` to `NodeRuntimeInner`, push `NodeState::Polling` in `push_node_slot`, initialize it in `Default`, and add:

```rust
pub fn node_kind(&self, node: NodeId) -> CoreResult<NodeKind> {
    let inner = self.inner.borrow();
    inner.validate_node(node)?;
    Ok(inner.nodes[node.0 as usize].kind)
}

pub fn node_state(&self, node: NodeId) -> CoreResult<NodeState> {
    let inner = self.inner.borrow();
    inner.validate_node(node)?;
    Ok(inner.node_states[node.0 as usize])
}

pub fn set_node_state(&self, node: NodeId, state: NodeState) -> CoreResult<()> {
    let mut inner = self.inner.borrow_mut();
    inner.validate_node(node)?;
    inner.node_states[node.0 as usize] = state;
    Ok(())
}
```

In `crates/hammer-adapter/src/buffer.rs`, add:

```rust
pub fn schedule_empty_frame(&self, node: NodeId) -> CoreResult<()> {
    let frame = self.alloc_frame_index()?;
    if let Err(err) = self
        .get_frame_mut(frame)
        .map(|mut frame_ref| frame_ref.set_next_node(node))
    {
        let _ = self.free_frame_index(frame);
        return Err(err);
    }
    if let Err(err) = self.nodes.schedule_frame(node, frame, true) {
        let _ = self.free_frame_index(frame);
        return Err(err);
    }
    Ok(())
}
```

In `crates/hammer-adapter/src/lib.rs`, add `NodeState` to the node re-export list.

- [x] **Step 4: Run GREEN test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime runtime_exposes_node_kind_and_state -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_empty_frame_runs_driver_without_packet_vectors -- --exact
```

Expected: PASS.

## Task 2: Add Interrupt Pending Coalescing

**Files:**
- Modify: `crates/hammer-adapter/src/node.rs`
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Modify: `crates/hammer-adapter/tests/node_runtime.rs`

- [x] **Step 1: Write failing interrupt tests**

Add these tests:

```rust
#[test]
fn interrupt_pending_coalesces_empty_driver_dispatch() {
    reset_calls(31);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([31, 0, 0, 0]),
    ));
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Interrupt)
        .expect("set interrupt state");

    assert!(
        runtime
            .set_node_interrupt_pending(driver)
            .expect("first interrupt schedules")
    );
    assert!(
        !runtime
            .set_node_interrupt_pending(driver)
            .expect("second interrupt coalesces")
    );
    assert_eq!(runtime.nodes().pending_len(), 1);

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
    assert_eq!(calls_for(31), 1);
    assert_eq!(runtime.nodes().pending_len(), 0);
    assert_eq!(runtime.packet_buffers().frames_in_use(), 0);
}

#[test]
fn disabled_driver_interrupt_does_not_schedule() {
    reset_calls(37);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([37, 0, 0, 0]),
    ));
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Disabled)
        .expect("disable driver");

    assert!(
        !runtime
            .set_node_interrupt_pending(driver)
            .expect("disabled interrupt is ignored")
    );
    assert_eq!(runtime.nodes().pending_len(), 0);
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 0);
    assert_eq!(calls_for(37), 0);
}
```

- [x] **Step 2: Run RED test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime interrupt_pending_coalesces_empty_driver_dispatch -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime disabled_driver_interrupt_does_not_schedule -- --exact
```

Expected: FAIL because `set_node_interrupt_pending` and interrupt coalescing do not exist.

- [x] **Step 3: Implement interrupt coalescing**

In `NodeRuntimeInner`, add:

```rust
interrupt_pending: Vec<bool>,
```

Push `false` in `push_node_slot` and initialize the vector in `Default`.

In `NodeRuntime`, add crate-local helpers:

```rust
pub(crate) fn mark_interrupt_pending(&self, node: NodeId) -> CoreResult<bool> {
    let mut inner = self.inner.borrow_mut();
    inner.validate_node(node)?;
    let slot = node.0 as usize;
    if inner.nodes[slot].kind != NodeKind::Driver {
        return Err(CoreError::internal("node is not a driver node"));
    }
    match inner.node_states[slot] {
        NodeState::Disabled | NodeState::Polling => Ok(false),
        NodeState::Interrupt => {
            if inner.interrupt_pending[slot] {
                Ok(false)
            } else {
                inner.interrupt_pending[slot] = true;
                Ok(true)
            }
        }
    }
}

pub(crate) fn clear_interrupt_pending(&self, node: NodeId) -> CoreResult<()> {
    let mut inner = self.inner.borrow_mut();
    inner.validate_node(node)?;
    inner.interrupt_pending[node.0 as usize] = false;
    Ok(())
}
```

In `DataPlaneRuntime`, add:

```rust
pub fn set_node_interrupt_pending(&self, node: NodeId) -> CoreResult<bool> {
    if !self.nodes.mark_interrupt_pending(node)? {
        return Ok(false);
    }
    if let Err(err) = self.schedule_empty_frame(node) {
        let _ = self.nodes.clear_interrupt_pending(node);
        return Err(err);
    }
    Ok(true)
}
```

In `NodeRuntime::run_ready_function_nodes`, call `self.clear_interrupt_pending(scheduled.node)?` after taking the frame and before dispatch.

- [x] **Step 4: Run GREEN test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime interrupt_pending_coalesces_empty_driver_dispatch -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime disabled_driver_interrupt_does_not_schedule -- --exact
```

Expected: PASS.

## Task 3: Make Disabled Queued Nodes Skip Dispatch

**Files:**
- Modify: `crates/hammer-adapter/src/node.rs`
- Modify: `crates/hammer-adapter/tests/node_runtime.rs`

- [x] **Step 1: Write failing disabled-dispatch test**

Add this test:

```rust
#[test]
fn disabled_node_skips_already_queued_empty_frame() {
    reset_calls(41);
    let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
    let driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([41, 0, 0, 0]),
    ));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule empty frame before disable");
    runtime
        .nodes()
        .set_node_state(driver, NodeState::Disabled)
        .expect("disable queued node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 0);
    assert_eq!(calls_for(41), 0);
    assert_eq!(runtime.packet_buffers().frames_in_use(), 0);
}
```

- [x] **Step 2: Run RED test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime disabled_node_skips_already_queued_empty_frame
```

Expected: FAIL because queued frames still dispatch after disable.

- [x] **Step 3: Skip disabled nodes in dispatch**

In `NodeRuntime::run_ready_function_nodes`, after taking the frame:

```rust
if self.node_state(scheduled.node)? == NodeState::Disabled {
    self.clear_interrupt_pending(scheduled.node)?;
    runtime.release_taken_frame_index(scheduled.frame, frame)?;
    continue;
}
```

- [x] **Step 4: Run GREEN test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime disabled_node_skips_already_queued_empty_frame
```

Expected: PASS.

## Task 4: Verify Runtime Scope and Absence of One-Off Helpers

**Files:**
- Verify only.

- [x] **Step 1: Run focused adapter runtime tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime
```

Expected: PASS.

- [x] **Step 2: Run adapter tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter
```

Expected: PASS.

- [x] **Step 3: Confirm forbidden helper APIs are absent**

Run:

```bash
rg -n "install_.*driver_.*worker|register_.*driver_.*worker|schedule_.*driver_.*worker|pub trait InputNode|NodeKind::Input" crates
```

Expected: no matches.

- [x] **Step 4: Confirm runtime exports are generic**

Run:

```bash
rg -n "NodeState|set_node_interrupt_pending|schedule_empty_frame|node_kind|set_node_state" crates/hammer-adapter/src crates/hammer-adapter/tests/node_runtime.rs
```

Expected: matches only in generic adapter runtime files and tests.
