# Shared App Ingress Registry Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace transport-private app target tables with one shared `hammer-service::app::AppIngressRegistry<K>` reused by both TCP and UDP.

**Architecture:** `hammer-service::app` owns generic ingress target storage and lookup. Transport code keeps only transport-specific classification and key extraction, then resolves `AppIngressTarget` through the shared registry before calling the common app delivery backend.

**Tech Stack:** Rust 2024, `hammer-service`, `hammer-runtime`, `hammer-infra::map::FlatHashTable`, `hammer_infra::vec::Vec`, focused TCP/UDP node tests

---

## Task 1: Generic App Registry

**Files:**
- Modify: `crates/hammer-service/src/app/registry.rs`
- Modify: `crates/hammer-service/src/app/mod.rs`
- Test: `crates/hammer-service/tests/app_tcp_runtime.rs`

- [ ] **Step 1: Add a failing test or compile usage that requires a generic app ingress registry**

Use `AppIngressRegistry<TcpLookupId>` from a TCP-facing call site so the current `TcpAppIngressRegistry` shape is insufficient.

- [ ] **Step 2: Run the focused test to verify RED**

Run: `cargo test -p hammer-service --test app_tcp_runtime`
Expected: fail or not compile until `AppIngressRegistry<K>` exists.

- [ ] **Step 3: Replace the TCP-specific registry with `AppIngressRegistry<K>`**

Implement generic key-to-slot lookup with:
- `FlatHashTable<K, u32>`
- `hammer_infra::vec::Vec<AppIngressTarget>`

- [ ] **Step 4: Re-export the shared registry from `hammer-service::app`**

Make the generic type available to both transport modules.

- [ ] **Step 5: Run the focused test to verify GREEN**

Run: `cargo test -p hammer-service --test app_tcp_runtime`
Expected: PASS.

## Task 2: TCP Migration

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/rcv_process.rs`
- Test: `crates/hammer-service/tests/tcp_input_nodes.rs`

- [ ] **Step 1: Write/adjust the focused TCP test expectation**

Keep coverage around app handoff behavior while ensuring the TCP path no longer depends on a private `TcpAppBridgeTable`.

- [ ] **Step 2: Run the focused TCP test to verify RED**

Run: `cargo test -p hammer-service --test tcp_input_nodes tcp_rcv_process_handoffs_selected_established_flow_to_app -- --exact`
Expected: fail or not compile until TCP uses the shared registry.

- [ ] **Step 3: Remove `TcpAppBridgeTable` and replace it with `AppIngressRegistry<TcpLookupId>`**

Keep the existing pending handoff behavior unchanged; only move target storage/lookup into the shared registry.

- [ ] **Step 4: Run the focused TCP test to verify GREEN**

Run: `cargo test -p hammer-service --test tcp_input_nodes tcp_rcv_process_handoffs_selected_established_flow_to_app -- --exact`
Expected: PASS.

## Task 3: UDP Migration

**Files:**
- Modify: `crates/hammer-service/src/transport/udp/input.rs`
- Test: `crates/hammer-service/tests/udp_input_nodes.rs`

- [ ] **Step 1: Write/adjust the UDP focused test to preserve behavior while forbidding UDP-private target storage**

Keep coverage for app dispatch, zero-copy release, and non-owner rejection.

- [ ] **Step 2: Run the focused UDP tests to verify RED**

Run: `cargo test -p hammer-service --test udp_input_nodes udp_input_dispatches_selected_port_into_runtime_app_flow -- --exact`
Expected: fail or not compile until UDP resolves app targets through the shared registry.

- [ ] **Step 3: Migrate UDP registered-app target lookup to `AppIngressRegistry<u16>`**

Keep per-port action classification if useful, but remove embedded `AppIngressTarget` from the UDP-private action payload.

- [ ] **Step 4: Run the focused UDP tests to verify GREEN**

Run: `cargo test -p hammer-service --test udp_input_nodes -- --nocapture`
Expected: PASS.

## Task 4: Final Verification

**Files:**
- Verify only

- [ ] **Step 1: Run runtime app tests**

Run: `cargo test -p hammer-runtime --test app_ring --test app_echo_loop`
Expected: PASS.

- [ ] **Step 2: Run focused service tests**

Run: `cargo test -p hammer-service --lib runtime_service_ && cargo test -p hammer-service --test tcp_input_nodes --test udp_input_nodes --test app_tcp_runtime --test app_udp_runtime`
Expected: PASS.

- [ ] **Step 3: Run hammer-app tests**

Run: `cargo test -p hammer-app`
Expected: PASS.
