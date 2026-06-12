# TCP Session io_uring Poll Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move TCP connection progression to a worker-local `TcpSessionNode` DriverNode that polls app SQ/CQ rings and a worker-local timer wheel, without TCP-specific runtime helper APIs or control-plane-owned connection progression.

**Architecture:** Hammer keeps `DriverNode` as the reusable runtime role for VPP-style input/output driver work. `TcpSessionNode` maps VPP's `session-queue` input node shape onto Hammer's graph runtime: it is scheduled with empty frames, polls app submission rings synchronously, advances its worker-local timer wheel, drains pending timer work, and marks ready connections. App interaction follows an io_uring-like SQ/CQ contract: app code submits SQEs to the flow owner's ring, the session node consumes SQEs on that owner worker, and completions are written back to the same app ring.

**Tech Stack:** Rust 2024, `hammer-adapter` graph runtime, `hammer-runtime` app rings and data worker loop, `hammer-service` TCP nodes, `hammer-infra::{vec,map,timer_wheel}`, VPP session/tcp source references, io_uring SQ/CQ model.

---

## Verified Design Inputs

- VPP `session-queue` is registered as an input node in `/private/tmp/vpp_session_node.c`: `VLIB_NODE_TYPE_INPUT`, disabled by default, and woken through `vlib_node_set_interrupt_pending` from timerfd readiness.
- VPP `session_queue_node_fn` updates worker time, updates transport time subscribers, drains the worker's internal message queue, dispatches control events, dispatches new/old IO events, flushes pending TX buffers, and updates adaptive worker state.
- VPP TCP timers are worker-local: `/private/tmp/vpp-tcp.c` expires the worker timer wheel in `tcp_update_time`, invalidates timer handles, records pending timer bits, pushes encoded timer handles into the worker pending timer FIFO, then dispatches timer handlers on the same worker.
- VPP session events are worker queue entries inside `session_worker_t`, not a Hammer TCP service shared event bus. Hammer must not keep a service-level shared inbox for session connection progression.
- Linux io_uring uses shared SQ/CQ rings: user/app code submits SQEs, kernel/driver consumes SQEs, and completions appear as CQEs. SQ polling mode lets the driver side poll submissions instead of requiring one syscall per SQE.
- Current Hammer runtime already has generic primitives: `DriverNode`, `NodeState::{Disabled, Polling, Interrupt}`, `DataPlaneRuntime::schedule_empty_frame`, and `DataPlaneRuntime::set_node_interrupt_pending`.
- Current Hammer app ring already has async `next_submission_entry`, `next_sqe_descriptor`, `next_tcp_shutdown`, completion APIs, and zero-copy `AppSubmissionEntry` attachments. It lacks synchronous `try_pop_*` APIs for a DriverNode process function.

## Scope Rules

- Stay on the current branch `codex/hammer-app-ring-zero-copy`.
- Reuse `CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target` for all cargo commands.
- Do not add `InputNode` or `NodeKind::Input`; Hammer's reusable role is `DriverNode`.
- Do not add APIs named after TCP worker installation or scheduling. The runtime API must stay generic: register nodes, assemble graph edges, schedule graph work.
- Do not keep a TCP service shared inbox for connection progression. Remove `TcpSessionEvent`, `TcpSessionShared`, shared `VecDeque` inboxes, and push/drain event methods from the TCP session path.
- Do not let the control plane drive app sends, app shutdowns, retransmit timers, or ready connection progression. The control plane may publish configuration/listener state, but session-owned connection work runs inside `TcpSessionNode`.

## File Structure

- Modify `docs/superpowers/plans/2026-06-12-tcp-session-node-connection-management.md`: remove by deleting this obsolete plan during execution because it encodes the shared-inbox design.
- Modify `docs/superpowers/plans/2026-06-12-tcp-worker-driver-node-redesign.md`: remove by deleting this obsolete worker-driver plan during execution.
- Modify `docs/superpowers/plans/2026-06-12-vlib-runtime-input-node.md`: remove by deleting this obsolete `InputNode` plan during execution.
- Keep `docs/superpowers/plans/2026-06-12-runtime-driver-node-primitives.md`: this is the landed generic runtime primitive plan and remains aligned with the target design.
- Modify `crates/hammer-adapter/src/node.rs`: expose a generic way to enumerate polling DriverNodes.
- Modify `crates/hammer-adapter/src/buffer.rs`: add generic scheduling of polling DriverNodes through empty frames.
- Modify `crates/hammer-adapter/tests/node_runtime.rs`: cover generic polling DriverNode scheduling.
- Modify `crates/hammer-runtime/src/spawn.rs`: let the data worker schedule polling DriverNodes when worker-local app/control tasks make progress, without introducing TCP-specific worker APIs.
- Modify `crates/hammer-runtime/src/app/ring.rs`: add synchronous pop APIs for app SQ descriptors, app SQ entries, and TCP shutdown submissions.
- Modify `crates/hammer-runtime/src/app/backend.rs`: expose backend-level `try_pop_*` wrappers for DriverNode polling.
- Modify `crates/hammer-runtime/tests/app_ring.rs`: cover synchronous app SQ and TCP shutdown polling.
- Replace `crates/hammer-service/src/transport/tcp/session.rs`: keep the worker-local runtime and timer wheel, remove the shared inbox draft, add app backend attachment and SQ/CQ polling.
- Modify `crates/hammer-service/src/transport/tcp/mod.rs`: export only the session node/runtime/timer/app command types that remain part of the poll-driven design.
- Replace `crates/hammer-service/tests/tcp_session_node.rs`: keep the useful connection/timer tests, remove the shared-inbox test, and add DriverNode/app ring/timer tests.
- Modify `crates/hammer-service/src/service.rs`: remove the app send/shutdown pump responsibilities after session runtime can consume app rings.

---

### Task 1: Delete Obsolete Design Docs

**Files:**
- Delete: `docs/superpowers/plans/2026-06-12-tcp-session-node-connection-management.md`
- Delete: `docs/superpowers/plans/2026-06-12-tcp-worker-driver-node-redesign.md`
- Delete: `docs/superpowers/plans/2026-06-12-vlib-runtime-input-node.md`
- Keep: `docs/superpowers/plans/2026-06-12-runtime-driver-node-primitives.md`

- [ ] **Step 1: Delete the obsolete plan files**

Run:

```bash
git rm docs/superpowers/plans/2026-06-12-tcp-session-node-connection-management.md \
  docs/superpowers/plans/2026-06-12-tcp-worker-driver-node-redesign.md \
  docs/superpowers/plans/2026-06-12-vlib-runtime-input-node.md
```

Expected: git stages the three deletions and does not touch `2026-06-12-runtime-driver-node-primitives.md`.

- [ ] **Step 2: Verify active docs no longer advertise the obsolete route**

Run:

```bash
rg -n "install_driver_node_on_workers|register_driver_node_on_worker|schedule_driver_node_on_worker|schedule_driver_node_on_current_worker|pub trait InputNode|NodeKind::Input|TcpSessionShared|TcpSessionEvent|push_event|drain_worker_events" docs/superpowers/plans docs/superpowers/specs
```

Expected: matches may remain only in this implementation plan as removal criteria or in historical non-active specs. There must be no active implementation plan that instructs adding those APIs or types.

- [ ] **Step 3: Commit the doc cleanup**

Run:

```bash
git add docs/superpowers/plans
git commit -m "docs(Refactor): remove obsolete tcp session helper plans"
```

Expected: one docs-only commit.

---

### Task 2: Add Generic Polling Driver Scheduling

**Files:**
- Modify: `crates/hammer-adapter/src/node.rs`
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Modify: `crates/hammer-adapter/tests/node_runtime.rs`
- Modify: `crates/hammer-runtime/src/spawn.rs`

- [ ] **Step 1: Write the failing adapter runtime test**

Add this test near the existing DriverNode scheduling tests in `crates/hammer-adapter/tests/node_runtime.rs`:

```rust
#[test]
fn schedule_polling_driver_nodes_schedules_only_polling_drivers() {
    reset_calls(31);
    reset_calls(32);
    reset_calls(33);

    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let polling_driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([31, 0, 0, 0]),
    ));
    let interrupt_driver = runtime.nodes().register_driver(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([32, 0, 0, 0]),
    ));
    let internal = runtime.nodes().register_internal(DescriptorNode::plain(
        count_process,
        NodeRuntimeData::from_words([33, 0, 0, 0]),
    ));

    runtime
        .nodes()
        .set_node_state(interrupt_driver, NodeState::Interrupt)
        .expect("set interrupt driver state");
    runtime
        .nodes()
        .set_node_state(internal, NodeState::Polling)
        .expect("set internal polling state");

    assert_eq!(
        runtime
            .schedule_polling_driver_nodes()
            .expect("schedule polling drivers"),
        1
    );
    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);

    assert_eq!(calls_for(31), 1);
    assert_eq!(calls_for(32), 0);
    assert_eq!(calls_for(33), 0);
    assert_eq!(runtime.nodes().node_state(polling_driver).unwrap(), NodeState::Polling);
}
```

- [ ] **Step 2: Run the failing adapter runtime test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_polling_driver_nodes_schedules_only_polling_drivers -- --exact
```

Expected: FAIL because `DataPlaneRuntime::schedule_polling_driver_nodes` does not exist.

- [ ] **Step 3: Implement generic DriverNode enumeration**

Add this method to `impl NodeRuntime` in `crates/hammer-adapter/src/node.rs`, next to `node_kind`/`node_state` methods:

```rust
pub fn polling_driver_nodes(&self) -> CoreResult<Vec<NodeId>> {
    let inner = self.inner.borrow();
    let mut nodes = Vec::new();
    for (slot, runtime_slot) in inner.nodes.iter().enumerate() {
        if runtime_slot.kind == NodeKind::Driver
            && inner.node_states[slot] == NodeState::Polling
        {
            let node_id = u32::try_from(slot)
                .map(NodeId::new)
                .map_err(|_| CoreError::internal("node id overflow"))?;
            nodes.push(node_id);
        }
    }
    Ok(nodes)
}
```

- [ ] **Step 4: Implement generic polling DriverNode scheduling**

Add this method to `impl DataPlaneRuntime` in `crates/hammer-adapter/src/buffer.rs`, next to `schedule_empty_frame`:

```rust
#[inline]
pub fn schedule_polling_driver_nodes(&self) -> CoreResult<usize> {
    let nodes = self.nodes.polling_driver_nodes()?;
    let scheduled = nodes.len();
    for node in nodes {
        self.schedule_empty_frame(node)?;
    }
    Ok(scheduled)
}
```

- [ ] **Step 5: Run the adapter runtime test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_polling_driver_nodes_schedules_only_polling_drivers -- --exact
```

Expected: PASS.

- [ ] **Step 6: Wire polling driver scheduling into the data worker loop after worker-local progress**

Add this helper to `crates/hammer-runtime/src/spawn.rs` near `poll_data_plane_nodes`:

```rust
fn schedule_polling_driver_nodes_after_progress(progressed: bool) -> bool {
    if !progressed {
        return false;
    }
    match with_data_plane_runtime(|runtime| runtime.schedule_polling_driver_nodes()) {
        Ok(count) => count > 0,
        Err(err) => {
            tracing::debug!("polling driver scheduling failed: {err}");
            false
        }
    }
}
```

Change `poll_data_worker_once` in `crates/hammer-runtime/src/spawn.rs` to:

```rust
fn poll_data_worker_once(
    worker: usize,
    barrier: &Arc<DataPlaneBarrierState>,
    remote_local: &DataRemoteLocalQueue,
    worker_waker: &Waker,
) -> bool {
    let mut cx = Context::from_waker(worker_waker);
    DATA_LOCAL_DRIVER_WAKER.with(|slot| {
        *slot.borrow_mut() = Some(worker_waker.clone());
    });
    if barrier.worker_poll(worker, &mut cx).is_pending() {
        return false;
    }

    let mut progressed = false;

    let remote_progress = poll_remote_local_tasks(remote_local);
    schedule_polling_driver_nodes_after_progress(remote_progress);
    progressed |= remote_progress;
    progressed |= poll_data_plane_nodes(&mut cx);

    let local_task_progress = poll_data_local_tasks(&mut cx);
    schedule_polling_driver_nodes_after_progress(local_task_progress);
    progressed |= local_task_progress;
    progressed |= poll_data_plane_nodes(&mut cx);

    progressed
}
```

This schedules polling DriverNodes after worker-local tasks make progress. It does not introduce TCP-specific runtime APIs and does not schedule polling drivers on idle worker loops.

- [ ] **Step 7: Run relevant runtime tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_empty_frame_runs_driver_without_packet_vectors -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_polling_driver_nodes_schedules_only_polling_drivers -- --exact
```

Expected: PASS for both tests.

- [ ] **Step 8: Commit generic runtime scheduling**

Run:

```bash
git add crates/hammer-adapter/src/node.rs crates/hammer-adapter/src/buffer.rs crates/hammer-adapter/tests/node_runtime.rs crates/hammer-runtime/src/spawn.rs
git commit -m "hammer-adapter(Feat): schedule polling driver nodes generically"
```

Expected: one commit containing only generic runtime scheduling changes.

---

### Task 3: Add Synchronous App Ring Poll APIs

**Files:**
- Modify: `crates/hammer-runtime/src/app/ring.rs`
- Modify: `crates/hammer-runtime/src/app/backend.rs`
- Modify: `crates/hammer-runtime/tests/app_ring.rs`

- [ ] **Step 1: Write the failing submission-entry poll test**

Add this test to `crates/hammer-runtime/tests/app_ring.rs` near the existing submission-entry tests:

```rust
#[test]
fn app_backend_try_pop_submission_entry_without_awaiting() {
    let data_runtime =
        DataRuntime::new(1, "app-backend-sync-entry-pop-test", 512 * 1024, 2)
            .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(101);

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let runtime = worker.runtime();
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), b"sync-entry-send")
                    .expect("alloc sync entry send buffer");
                let registered =
                    AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
                        .expect("registered buffer");
                let descriptor = AppSqeDescriptor::new(
                    AppOpcode::Send,
                    AppUserData::new(101),
                    AppObjectRef::Flow(flow),
                    AppSqeData::Send {
                        buffer: registered.index(),
                    },
                );

                runtime
                    .try_push_submission_entry(AppSubmissionEntry::with_attachment(
                        descriptor, registered,
                    ))
                    .expect("push submission entry");

                let entry = backend
                    .try_pop_submission_entry()
                    .expect("pop submission entry");
                assert!(backend.try_pop_submission_entry().is_none());
                let (descriptor_round_trip, registered) = entry.into_parts();
                let (_buffer_index, lease) = registered
                    .expect("submission entry attachment")
                    .into_parts();
                let payload = lease.copy_current().expect("copy send payload");
                lease.release();
                (descriptor_round_trip, payload)
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(round_trip.0.user_data(), AppUserData::new(101));
    assert_eq!(round_trip.0.object(), AppObjectRef::Flow(flow));
    assert_eq!(round_trip.1, b"sync-entry-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
```

- [ ] **Step 2: Write the failing TCP shutdown poll test**

Add this test to `crates/hammer-runtime/tests/app_ring.rs` near the existing shutdown tests:

```rust
#[test]
fn app_backend_try_pop_tcp_shutdown_without_awaiting() {
    let data_runtime =
        DataRuntime::new(1, "app-backend-sync-shutdown-pop-test", 512 * 1024, 2)
            .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(102);

    let shutdown = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                worker
                    .runtime()
                    .shutdown(Shutdown::Write)
                    .await
                    .expect("submit shutdown");

                let shutdown = backend
                    .try_pop_tcp_shutdown()
                    .expect("pop tcp shutdown");
                assert!(backend.try_pop_tcp_shutdown().is_none());
                shutdown
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(shutdown.flow(), flow);
    assert_eq!(shutdown.how(), Shutdown::Write);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
```

- [ ] **Step 3: Run the failing app ring tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-runtime --test app_ring app_backend_try_pop_submission_entry_without_awaiting -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-runtime --test app_ring app_backend_try_pop_tcp_shutdown_without_awaiting -- --exact
```

Expected: FAIL because backend synchronous pop methods do not exist.

- [ ] **Step 4: Implement synchronous AppRingHandle pops**

Add these methods to `impl AppRingHandle` in `crates/hammer-runtime/src/app/ring.rs`, next to the existing async `next_submission_entry`/`next_submission_descriptor` methods:

```rust
#[inline]
pub fn pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
    self.submissions.borrow_mut().pop()
}

#[inline]
pub fn pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
    let descriptor = self.pop_submission_descriptor()?;
    Some(submission_entry_from_descriptor(descriptor, &self.buffers))
}

#[inline]
pub fn pop_tcp_shutdown(&self) -> Option<AppTcpShutdown> {
    self.tcp_shutdowns.borrow_mut().pop()
}
```

- [ ] **Step 5: Implement backend-level synchronous pops**

Add these methods to `impl AppBackend` in `crates/hammer-runtime/src/app/backend.rs`, next to `next_submission_entry` and `next_tcp_shutdown`:

```rust
#[inline]
pub fn try_pop_sqe_descriptor(&self) -> Option<AppSqeDescriptor> {
    self.ring.pop_submission_descriptor()
}

#[inline]
pub fn try_pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
    self.ring.pop_submission_entry()
}

#[inline]
pub fn try_pop_tcp_shutdown(&self) -> Option<AppTcpShutdown> {
    self.ring.pop_tcp_shutdown()
}
```

- [ ] **Step 6: Run the app ring tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-runtime --test app_ring app_backend_try_pop_submission_entry_without_awaiting -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-runtime --test app_ring app_backend_try_pop_tcp_shutdown_without_awaiting -- --exact
```

Expected: PASS for both tests.

- [ ] **Step 7: Commit the app ring poll API**

Run:

```bash
git add crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/backend.rs crates/hammer-runtime/tests/app_ring.rs
git commit -m "hammer-runtime(Feat): add app ring poll APIs"
```

Expected: one commit scoped to app ring/backend synchronous polling.

---

### Task 4: Replace TCP Session Shared Inbox with Worker-Local Runtime

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/tests/tcp_session_node.rs`

- [ ] **Step 1: Replace the test imports**

Change the import block in `crates/hammer-service/tests/tcp_session_node.rs` from the shared-inbox imports to:

```rust
use std::net::SocketAddr;

use hammer_adapter::{DataPlaneRuntime, DataWorkerId};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_service::transport::tcp::{
    TcpDataPlaneConnection, TcpLookupId, TcpSessionNode, TcpSessionRuntime,
    TcpSessionTimerKind,
};
```

- [ ] **Step 2: Replace the DriverNode test**

Replace `tcp_session_node_runs_as_empty_frame_driver` with:

```rust
#[test]
fn tcp_session_node_runs_as_empty_frame_driver_without_shared_inbox() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let driver = runtime
        .nodes()
        .register_driver(TcpSessionNode::new(DataWorkerId::new(0)));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule session node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
}
```

- [ ] **Step 3: Delete the shared inbox test**

Remove the test named `tcp_session_shared_drains_worker_inbox_into_runtime` from `crates/hammer-service/tests/tcp_session_node.rs`.

- [ ] **Step 4: Run the failing TCP session test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node tcp_session_node_runs_as_empty_frame_driver_without_shared_inbox -- --exact
```

Expected: FAIL because `TcpSessionNode::new` still requires the shared inbox argument and the old exports still mention shared session event types.

- [ ] **Step 5: Remove shared-inbox types from session.rs**

In `crates/hammer-service/src/transport/tcp/session.rs`, change the imports from:

```rust
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
```

to:

```rust
use std::cell::RefCell;
```

Delete the `TcpSessionEvent` enum and the `TcpSessionShared` struct/impl from `session.rs`.

- [ ] **Step 6: Make TcpSessionNode worker-local**

Replace the `TcpSessionNode` struct and constructor in `session.rs` with:

```rust
#[derive(Clone, Debug)]
pub struct TcpSessionNode {
    worker: DataWorkerId,
}

impl TcpSessionNode {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self { worker }
    }
}
```

Keep `impl Node for TcpSessionNode` and `impl DriverNode for TcpSessionNode`, but remove all references to `shared`.

- [ ] **Step 7: Export only the poll-driven session types**

Change the session export in `crates/hammer-service/src/transport/tcp/mod.rs` to:

```rust
pub use session::{TcpSessionNode, TcpSessionRuntime, TcpSessionTimerKind};
```

- [ ] **Step 8: Run the TCP session tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node
```

Expected: PASS for connection ownership, timer wheel, cancel/rearm, and empty-frame DriverNode tests.

- [ ] **Step 9: Commit the shared-inbox removal**

Run:

```bash
git add crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/tests/tcp_session_node.rs
git commit -m "hammer-service(Refactor): remove tcp session shared inbox"
```

Expected: one commit that removes shared session event plumbing and keeps worker-local runtime behavior.

---

### Task 5: Add App SQ Polling to TcpSessionRuntime

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/tests/tcp_session_node.rs`

- [ ] **Step 1: Add test imports for app ring commands**

Extend imports in `crates/hammer-service/tests/tcp_session_node.rs`:

```rust
use std::net::{Shutdown, SocketAddr};

use hammer_adapter::{DataPlaneRuntime, DataWorkerId};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_runtime::app::{
    AppBackend, AppBufferLease, AppObjectRef, AppOpcode, AppRegisteredBuffer,
    AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown, AppUserData,
};
use hammer_runtime::spawn::with_data_plane_buffers;
use hammer_service::transport::tcp::{
    TcpAppCommand, TcpDataPlaneConnection, TcpLookupId, TcpSessionNode,
    TcpSessionRuntime, TcpSessionTimerKind,
};
```

- [ ] **Step 2: Write the failing app send SQ poll test**

Add this test to `crates/hammer-service/tests/tcp_session_node.rs`:

```rust
#[test]
fn tcp_session_runtime_polls_app_send_submission_into_ready_connection() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(21);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(21, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(connection_id, backend.clone())
        .expect("attach app backend");

    let buffers = with_data_plane_buffers(Clone::clone);
    let index = buffers
        .alloc_index_with_bytes(Default::default(), b"tcp-session-app-send")
        .expect("alloc app send buffer");
    let registered =
        AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
            .expect("registered buffer");
    let descriptor = AppSqeDescriptor::new(
        AppOpcode::Send,
        AppUserData::new(21),
        AppObjectRef::Flow(flow),
        AppSqeData::Send {
            buffer: registered.index(),
        },
    );

    backend
        .try_push_submission_entry(AppSubmissionEntry::with_attachment(
            descriptor, registered,
        ))
        .expect("push app send entry");

    assert_eq!(runtime.poll_app_rings().expect("poll app rings"), 1);
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);

    let commands = runtime.take_app_commands();
    assert_eq!(commands.len(), 1);
    match &commands[0] {
        TcpAppCommand::Send(send) => {
            assert_eq!(send.connection_id(), connection_id);
            assert_eq!(send.descriptor().user_data(), AppUserData::new(21));
            assert_eq!(
                send.registered()
                    .lease()
                    .copy_current()
                    .expect("copy app send payload"),
                b"tcp-session-app-send"
            );
        }
        other => panic!("unexpected app command: {other:?}"),
    }
}
```

- [ ] **Step 3: Write the failing app shutdown poll test**

Add this test to `crates/hammer-service/tests/tcp_session_node.rs`:

```rust
#[test]
fn tcp_session_runtime_polls_app_shutdown_into_ready_connection() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(22);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(22, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(connection_id, backend.clone())
        .expect("attach app backend");

    backend
        .try_push_tcp_shutdown(AppTcpShutdown::new(flow, Shutdown::Write))
        .expect("push app shutdown");

    assert_eq!(runtime.poll_app_rings().expect("poll app rings"), 1);
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);

    let commands = runtime.take_app_commands();
    assert_eq!(commands.len(), 1);
    match &commands[0] {
        TcpAppCommand::Shutdown(shutdown) => {
            assert_eq!(shutdown.connection_id(), connection_id);
            assert_eq!(shutdown.shutdown().flow(), flow);
            assert_eq!(shutdown.shutdown().how(), Shutdown::Write);
        }
        other => panic!("unexpected app command: {other:?}"),
    }
}
```

- [ ] **Step 4: Run the failing app poll tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node tcp_session_runtime_polls_app_send_submission_into_ready_connection -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node tcp_session_runtime_polls_app_shutdown_into_ready_connection -- --exact
```

Expected: FAIL because `TcpAppCommand`, `attach_app_backend`, `poll_app_rings`, and `take_app_commands` do not exist.

- [ ] **Step 5: Add app command types to session.rs**

Add these imports to `crates/hammer-service/src/transport/tcp/session.rs`:

```rust
use hammer_runtime::app::{
    AppBackend, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppObjectRef, AppOpcode,
    AppRegisteredBuffer, AppSqeData, AppSqeDescriptor, AppSubmissionEntry,
    AppTcpShutdown,
};
```

Add these types near `TcpSessionTimerKind`:

```rust
#[derive(Debug)]
pub enum TcpAppCommand {
    Send(TcpAppSend),
    Recv(TcpAppRecv),
    Close(TcpAppClose),
    Shutdown(TcpAppShutdownCommand),
}

#[derive(Debug)]
pub struct TcpAppSend {
    connection_id: TcpConnectionId,
    descriptor: AppSqeDescriptor,
    registered: AppRegisteredBuffer,
}

impl TcpAppSend {
    #[inline]
    pub fn connection_id(&self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn descriptor(&self) -> &AppSqeDescriptor {
        &self.descriptor
    }

    #[inline]
    pub fn registered(&self) -> &AppRegisteredBuffer {
        &self.registered
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAppRecv {
    connection_id: TcpConnectionId,
    descriptor: AppSqeDescriptor,
    max_len: u32,
}

impl TcpAppRecv {
    #[inline]
    pub fn connection_id(self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }

    #[inline]
    pub fn max_len(self) -> u32 {
        self.max_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAppClose {
    connection_id: TcpConnectionId,
    descriptor: AppSqeDescriptor,
}

impl TcpAppClose {
    #[inline]
    pub fn connection_id(self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpAppShutdownCommand {
    connection_id: TcpConnectionId,
    shutdown: AppTcpShutdown,
}

impl TcpAppShutdownCommand {
    #[inline]
    pub fn connection_id(self) -> TcpConnectionId {
        self.connection_id
    }

    #[inline]
    pub fn shutdown(self) -> AppTcpShutdown {
        self.shutdown
    }
}

#[derive(Clone, Debug)]
struct TcpSessionAppBackend {
    connection_id: TcpConnectionId,
    flow: hammer_runtime::app::AppFlowId,
    backend: AppBackend,
}
```

- [ ] **Step 6: Add app backend and command queues to TcpSessionRuntime**

Add these fields to `TcpSessionRuntime`:

```rust
app_backends: hammer_infra::vec::Vec<TcpSessionAppBackend>,
app_backend_slots: FlatHashTable<u64, usize>,
app_commands: hammer_infra::vec::Vec<TcpAppCommand>,
```

Initialize them in `TcpSessionRuntime::new`:

```rust
app_backends: hammer_infra::vec::Vec::new(),
app_backend_slots: FlatHashTable::new(),
app_commands: hammer_infra::vec::Vec::new(),
```

- [ ] **Step 7: Implement app backend attachment and command drain**

Add these methods to `impl TcpSessionRuntime`:

```rust
pub fn attach_app_backend(
    &mut self,
    connection_id: TcpConnectionId,
    backend: AppBackend,
) -> CoreResult<()> {
    if self.connection(connection_id).is_none() {
        return Err(CoreError::internal(format!(
            "TCP session app backend connection {} is missing",
            connection_id.get()
        )));
    }
    if self.app_backend_slots.lookup(&connection_id.get()).is_some() {
        return Err(CoreError::internal(format!(
            "TCP session app backend already attached for connection {}",
            connection_id.get()
        )));
    }
    let slot = self.app_backends.len();
    self.app_backends.push(TcpSessionAppBackend {
        connection_id,
        flow: backend.flow(),
        backend,
    });
    self.app_backend_slots.insert(connection_id.get(), slot);
    Ok(())
}

pub fn take_app_commands(&mut self) -> std::vec::Vec<TcpAppCommand> {
    self.app_commands.drain(..).collect()
}
```

- [ ] **Step 8: Implement app SQ/CQ polling**

Add these methods to `impl TcpSessionRuntime`:

```rust
pub fn poll_app_rings(&mut self) -> CoreResult<usize> {
    let backends: std::vec::Vec<TcpSessionAppBackend> =
        self.app_backends.iter().cloned().collect();
    let mut polled = 0usize;

    for app_backend in backends {
        while let Some(entry) = app_backend.backend.try_pop_submission_entry() {
            self.handle_app_submission(&app_backend, entry)?;
            polled += 1;
        }
        while let Some(shutdown) = app_backend.backend.try_pop_tcp_shutdown() {
            if shutdown.flow() != app_backend.flow {
                return Err(CoreError::internal(format!(
                    "TCP app shutdown flow {} does not match attached flow {}",
                    shutdown.flow().value(),
                    app_backend.flow.value()
                )));
            }
            self.app_commands
                .push(TcpAppCommand::Shutdown(TcpAppShutdownCommand {
                    connection_id: app_backend.connection_id,
                    shutdown,
                }));
            self.mark_connection_ready(app_backend.connection_id);
            polled += 1;
        }
    }

    Ok(polled)
}

fn handle_app_submission(
    &mut self,
    app_backend: &TcpSessionAppBackend,
    entry: AppSubmissionEntry,
) -> CoreResult<()> {
    let (descriptor, registered) = entry.into_parts();
    if descriptor.object() != AppObjectRef::Flow(app_backend.flow) {
        return Err(CoreError::internal(format!(
            "TCP app submission object {:?} does not match attached flow {}",
            descriptor.object(),
            app_backend.flow.value()
        )));
    }

    match descriptor.payload() {
        AppSqeData::Send { .. } => {
            let registered = registered.ok_or_else(|| {
                CoreError::internal("TCP app send submission is missing registered buffer")
            })?;
            self.app_commands.push(TcpAppCommand::Send(TcpAppSend {
                connection_id: app_backend.connection_id,
                descriptor,
                registered,
            }));
            self.mark_connection_ready(app_backend.connection_id);
        }
        AppSqeData::Recv { max_len } => {
            self.app_commands.push(TcpAppCommand::Recv(TcpAppRecv {
                connection_id: app_backend.connection_id,
                descriptor,
                max_len: *max_len,
            }));
            self.mark_connection_ready(app_backend.connection_id);
        }
        AppSqeData::Close => {
            self.app_commands.push(TcpAppCommand::Close(TcpAppClose {
                connection_id: app_backend.connection_id,
                descriptor,
            }));
            self.mark_connection_ready(app_backend.connection_id);
        }
        AppSqeData::Nop => {
            app_backend
                .backend
                .try_push_cqe_descriptor(AppCqeDescriptor::new(
                    descriptor.user_data(),
                    0,
                    AppCqeFlags::NONE,
                    AppObjectRef::Flow(app_backend.flow),
                    AppCqeData::None,
                ))
                .map_err(CoreError::from)?;
        }
        AppSqeData::Accept | AppSqeData::RecvFrom { .. } | AppSqeData::SendTo { .. } => {
            return Err(CoreError::internal(format!(
                "unsupported TCP app submission opcode {:?}",
                descriptor.opcode()
            )));
        }
    }

    Ok(())
}
```

- [ ] **Step 9: Export app command types**

Change the session export in `crates/hammer-service/src/transport/tcp/mod.rs` to:

```rust
pub use session::{
    TcpAppClose, TcpAppCommand, TcpAppRecv, TcpAppSend, TcpAppShutdownCommand,
    TcpSessionNode, TcpSessionRuntime, TcpSessionTimerKind,
};
```

- [ ] **Step 10: Run the app poll tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node tcp_session_runtime_polls_app_send_submission_into_ready_connection -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node tcp_session_runtime_polls_app_shutdown_into_ready_connection -- --exact
```

Expected: PASS for both tests.

- [ ] **Step 11: Commit app SQ polling**

Run:

```bash
git add crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/tests/tcp_session_node.rs
git commit -m "hammer-service(Feat): poll app rings from tcp session runtime"
```

Expected: one commit for TCP session app SQ/CQ integration.

---

### Task 6: Drive Timer Wheel and App Polling from TcpSessionNode

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/session.rs`
- Modify: `crates/hammer-service/tests/tcp_session_node.rs`

- [ ] **Step 1: Add a deterministic clock-driven poll test**

Add this import to `crates/hammer-service/tests/tcp_session_node.rs`:

```rust
use std::time::{Duration, Instant};
```

Add this test:

```rust
#[test]
fn tcp_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(23);
    let start = Instant::now();
    let mut runtime =
        TcpSessionRuntime::with_timer_clock(worker, Duration::from_millis(10), start);
    runtime
        .install_connection(connection(23, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();
    runtime
        .arm_timer_ticks(connection_id, TcpSessionTimerKind::OutputPacing, 2)
        .expect("arm output pacing timer");

    let first = runtime
        .poll_once_at(start + Duration::from_millis(10))
        .expect("first poll");
    assert_eq!(first.expired_timers, 0);
    assert!(runtime.take_ready_connections().is_empty());

    let second = runtime
        .poll_once_at(start + Duration::from_millis(20))
        .expect("second poll");
    assert_eq!(second.expired_timers, 1);
    assert_eq!(
        runtime.dispatch_pending_timers_for_test(),
        vec![(connection_id, TcpSessionTimerKind::OutputPacing)]
    );
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);
}
```

- [ ] **Step 2: Rename expired timer drain to pending timer dispatch in tests**

In `crates/hammer-service/tests/tcp_session_node.rs`, change timer assertions from:

```rust
assert_eq!(
    runtime.take_expired_timers(),
    vec![(connection_id, TcpSessionTimerKind::Retransmit)]
);
```

to:

```rust
assert_eq!(
    runtime.dispatch_pending_timers_for_test(),
    vec![(connection_id, TcpSessionTimerKind::Retransmit)]
);
```

Apply the same change in the rearm/cancel timer tests:

```rust
assert!(runtime.dispatch_pending_timers_for_test().is_empty());
```

- [ ] **Step 3: Run the failing timer dispatch tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node
```

Expected: FAIL because `with_timer_clock`, `poll_once_at`, and `dispatch_pending_timers_for_test` do not exist and the runtime still exposes expired timer drains.

- [ ] **Step 4: Add timer clock fields**

Add these imports to `crates/hammer-service/src/transport/tcp/session.rs`:

```rust
use std::time::{Duration, Instant};
```

Add this constant near the timer kind enum:

```rust
const DEFAULT_TCP_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
```

Add these fields to `TcpSessionRuntime`:

```rust
timer_tick_duration: Duration,
last_timer_tick: Instant,
```

- [ ] **Step 5: Add clock-aware constructors**

Change `TcpSessionRuntime::new` to:

```rust
#[inline]
pub fn new(worker: DataWorkerId) -> Self {
    Self::with_timer_clock(worker, DEFAULT_TCP_SESSION_TIMER_TICK, Instant::now())
}
```

Add this constructor:

```rust
pub fn with_timer_clock(
    worker: DataWorkerId,
    timer_tick_duration: Duration,
    last_timer_tick: Instant,
) -> Self {
    Self {
        worker,
        connections: TcpConnectionTable::empty(),
        ready_connections: hammer_infra::vec::Vec::new(),
        ready_slots: FlatHashTable::new(),
        timer_wheel: TimerWheel2t1w2048::new(0),
        timer_slots: hammer_infra::vec::Vec::new(),
        expired_timer_slots: hammer_infra::vec::Vec::new(),
        pending_timers: hammer_infra::vec::Vec::new(),
        app_backends: hammer_infra::vec::Vec::new(),
        app_backend_slots: FlatHashTable::new(),
        app_commands: hammer_infra::vec::Vec::new(),
        timer_tick_duration,
        last_timer_tick,
    }
}
```

- [ ] **Step 6: Rename expired timer queue to pending timer queue**

In `TcpSessionRuntime`, replace:

```rust
expired_timers: hammer_infra::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)>,
```

with:

```rust
pending_timers: hammer_infra::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)>,
```

Initialize it with:

```rust
pending_timers: hammer_infra::vec::Vec::new(),
```

In `expire_timers`, replace:

```rust
self.expired_timers.push((connection_id, kind));
```

with:

```rust
self.pending_timers.push((connection_id, kind));
```

- [ ] **Step 7: Add pending timer dispatch method**

Replace `take_expired_timers` with:

```rust
pub fn dispatch_pending_timers_for_test(
    &mut self,
) -> std::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)> {
    self.pending_timers.drain(..).collect()
}
```

- [ ] **Step 8: Add process-step result and session runtime step**

Add this type near the app command types:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpSessionStep {
    pub app_submissions: usize,
    pub expired_timers: usize,
    pub ready_connections: usize,
}
```

Add these methods to `impl TcpSessionRuntime`:

```rust
pub fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<TcpSessionStep> {
    let app_submissions = self.poll_app_rings()?;
    let expired_timers = self.expire_timers(timer_ticks)?;
    let ready_connections = self.ready_connections.len();
    Ok(TcpSessionStep {
        app_submissions,
        expired_timers,
        ready_connections,
    })
}

pub fn poll_once_at(&mut self, now: Instant) -> CoreResult<TcpSessionStep> {
    let timer_ticks = self.elapsed_timer_ticks(now);
    self.poll_once_for_ticks(timer_ticks)
}

fn elapsed_timer_ticks(&mut self, now: Instant) -> u32 {
    if self.timer_tick_duration.is_zero() {
        self.last_timer_tick = now;
        return 0;
    }

    let elapsed = now.saturating_duration_since(self.last_timer_tick);
    let tick_nanos = self.timer_tick_duration.as_nanos();
    let elapsed_ticks = elapsed.as_nanos() / tick_nanos;
    let ticks = elapsed_ticks.min(u32::MAX as u128) as u32;
    if ticks == 0 {
        return 0;
    }

    if let Some(advance) = self.timer_tick_duration.checked_mul(ticks) {
        self.last_timer_tick += advance;
    } else {
        self.last_timer_tick = now;
    }
    ticks
}
```

- [ ] **Step 9: Drive the session runtime from tcp_session_process**

Change `tcp_session_process` in `session.rs` to:

```rust
fn tcp_session_process(
    _runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    with_tcp_session_runtime(data, |session| {
        session.poll_once_at(Instant::now())?;
        Ok(NodeResult::drop())
    })
}
```

This makes the DriverNode process function poll app SQs and advance the worker-local timer wheel from elapsed clock ticks, matching VPP's "update worker time, then dispatch transport timers" shape.

- [ ] **Step 10: Export TcpSessionStep**

Change the session export in `crates/hammer-service/src/transport/tcp/mod.rs` to:

```rust
pub use session::{
    TcpAppClose, TcpAppCommand, TcpAppRecv, TcpAppSend, TcpAppShutdownCommand,
    TcpSessionNode, TcpSessionRuntime, TcpSessionStep, TcpSessionTimerKind,
};
```

- [ ] **Step 11: Run TCP session tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node
```

Expected: PASS.

- [ ] **Step 12: Commit DriverNode process integration**

Run:

```bash
git add crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/tests/tcp_session_node.rs
git commit -m "hammer-service(Feat): drive tcp session poll loop"
```

Expected: one commit for session DriverNode process behavior and pending timer dispatch naming.

---

### Task 7: Retire Service App Pumps from TCP Progression

**Files:**
- Modify: `crates/hammer-service/src/service.rs`
- Modify: `crates/hammer-service/tests/app_tcp_runtime.rs`
- Modify: `crates/hammer-service/tests/app_tcp_connect_runtime.rs`

- [ ] **Step 1: Find the service-owned app pump entry points**

Run:

```bash
rg -n "start_tcp_flow_pump|start_tcp_shutdown_pump|record_tcp_flow_send|shutdown_tcp_flow|tcp_output_signals" crates/hammer-service/src/service.rs crates/hammer-service/tests
```

Expected: matches show the old async pump path and its tests.

- [ ] **Step 2: Add a failing regression test for app send staying in the owner worker ring**

Change the app imports at the top of `crates/hammer-service/tests/app_tcp_runtime.rs` from:

```rust
use hammer_runtime::app::AppFlowId;
```

to:

```rust
use hammer_runtime::app::{AppBufferLease, AppFlowId, AppObjectRef, AppOpcode, AppSend};
```

Add this test to `crates/hammer-service/tests/app_tcp_runtime.rs`:

```rust
#[test]
fn service_tcp_app_send_stays_in_owner_ring_for_session_node_polling() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");
    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
    let owner = app.owner_worker_for_flow(flow).expect("flow owner");

    let payload = b"session-node-owned-send".to_vec();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), &payload)
                    .expect("alloc app send buffer");
                worker
                    .runtime()
                    .send(AppSend::new(AppBufferLease::from_buffer(buffers, index)))
                    .await
                    .expect("submit app send");
            })
            .await
            .expect("spawn flow send task");
        });

    std::thread::sleep(Duration::from_millis(50));
    assert!(
        service
            .tcp_pending_send_payload_lens_for_flow_for_test(flow)
            .is_empty(),
        "service pump must not consume app SQEs"
    );

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow_owner(flow, owner, move |worker| async move {
                let app_runtime = worker.runtime();
                let backend = worker.backend();
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), b"session-node-owned-send")
                    .expect("alloc app send buffer");
                app_runtime
                    .send(hammer_runtime::app::AppSend::new(
                        hammer_runtime::app::AppBufferLease::from_buffer(buffers, index),
                    ))
                    .await
                    .expect("submit app send");
                backend
                    .try_pop_sqe_descriptor()
                    .expect("session node visible sqe descriptor")
            })
            .await
            .expect("spawn on flow owner")
        });

    assert_eq!(descriptor.opcode(), AppOpcode::Send);
    assert_eq!(descriptor.object(), AppObjectRef::Flow(flow));

    service.close().expect("close service");
}
```

- [ ] **Step 3: Run the failing regression test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test app_tcp_runtime service_tcp_app_send_stays_in_owner_ring_for_session_node_polling -- --exact
```

Expected: FAIL while the service pump consumes the app send before the session node can poll it.

- [ ] **Step 4: Stop starting service-owned TCP app pumps**

In `RuntimeService::connect_tcp_stream` in `crates/hammer-service/src/service.rs`, remove these calls:

```rust
self.start_tcp_flow_pump(&app, result.flow, owner_worker)?;
self.start_tcp_shutdown_pump(&app, result.flow, owner_worker)?;
self.tcp_output_signals.signal(result.flow);
```

In accepted-flow setup paths in `service.rs`, remove the same pump-start and output-signal calls tied to `start_tcp_flow_pump` and `start_tcp_shutdown_pump`.

- [ ] **Step 5: Remove pump functions after call sites are gone**

Delete these functions from `RuntimeService` in `crates/hammer-service/src/service.rs`:

```rust
fn start_tcp_shutdown_pump(
    &self,
    app: &AppContext,
    flow: AppFlowId,
    owner_worker: usize,
) -> HammerResult<()>

fn start_tcp_flow_pump(
    &self,
    app: &AppContext,
    flow: AppFlowId,
    owner_worker: usize,
) -> HammerResult<()>
```

Run this call-site check:

```bash
rg -n "record_tcp_flow_send|shutdown_tcp_flow" crates/hammer-service/src/service.rs
```

Expected after deleting pump functions: only direct service methods and test-observation paths may remain. Do not leave any call from `start_tcp_flow_pump`, `start_tcp_shutdown_pump`, or app send/shutdown background tasks.

- [ ] **Step 6: Run the app TCP regression test**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test app_tcp_runtime service_tcp_app_send_stays_in_owner_ring_for_session_node_polling -- --exact
```

Expected: PASS; app sends remain as SQEs for `TcpSessionNode` polling.

- [ ] **Step 7: Run the TCP app test group**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test app_tcp_runtime
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test app_tcp_connect_runtime
```

Expected: PASS after tests are adjusted to assert session-node-owned SQ progression rather than service-owned pump progression.

- [ ] **Step 8: Commit service pump retirement**

Run:

```bash
git add crates/hammer-service/src/service.rs crates/hammer-service/tests/app_tcp_runtime.rs crates/hammer-service/tests/app_tcp_connect_runtime.rs
git commit -m "hammer-service(Refactor): move tcp app progression to session node"
```

Expected: one commit for removing service-owned app send/shutdown progression.

---

### Task 8: Final Scope Guard and Formatting

**Files:**
- Verify: `crates/hammer-adapter/src`
- Verify: `crates/hammer-runtime/src`
- Verify: `crates/hammer-service/src/transport/tcp`
- Verify: `crates/hammer-service/src/service.rs`
- Verify: `crates/hammer-service/tests`

- [ ] **Step 1: Run focused tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_empty_frame_runs_driver_without_packet_vectors -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-adapter --test node_runtime schedule_polling_driver_nodes_schedules_only_polling_drivers -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-runtime --test app_ring app_backend_try_pop_submission_entry_without_awaiting -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-runtime --test app_ring app_backend_try_pop_tcp_shutdown_without_awaiting -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_node
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test app_tcp_runtime
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test app_tcp_connect_runtime
```

Expected: PASS.

- [ ] **Step 2: Run format**

Run:

```bash
cargo fmt --all
```

Expected: rustfmt completes with no errors.

- [ ] **Step 3: Verify obsolete helper APIs and shared inbox types are gone from active code**

Run:

```bash
rg -n "TcpSessionShared|TcpSessionEvent|push_event|drain_worker_events|install_driver_node_on_workers|register_driver_node_on_worker|schedule_driver_node_on_worker|schedule_driver_node_on_current_worker|pub trait InputNode|NodeKind::Input" crates/hammer-adapter/src crates/hammer-runtime/src crates/hammer-service/src/transport/tcp crates/hammer-service/src/service.rs crates/hammer-service/tests/tcp_session_node.rs
```

Expected: no matches.

- [ ] **Step 4: Verify no service-owned TCP app pump remains**

Run:

```bash
rg -n "start_tcp_flow_pump|start_tcp_shutdown_pump" crates/hammer-service/src/service.rs crates/hammer-service/tests
```

Expected: no matches.

- [ ] **Step 5: Verify the new architecture terms are present in code**

Run:

```bash
rg -n "schedule_polling_driver_nodes|try_pop_submission_entry|try_pop_tcp_shutdown|poll_app_rings|dispatch_pending_timers_for_test|TcpAppCommand" crates/hammer-adapter/src crates/hammer-runtime/src crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_session_node.rs
```

Expected: matches show generic runtime scheduling, app ring sync polling, session app polling, and pending timer dispatch.

- [ ] **Step 6: Commit final formatting if rustfmt changed files**

Run:

```bash
git status --short
```

Expected: if `cargo fmt --all` changed files, stage only formatting changes from files touched by this plan:

```bash
git add crates/hammer-adapter/src/node.rs crates/hammer-adapter/src/buffer.rs crates/hammer-runtime/src/spawn.rs crates/hammer-runtime/src/app/ring.rs crates/hammer-runtime/src/app/backend.rs crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/mod.rs crates/hammer-service/src/service.rs crates/hammer-runtime/tests/app_ring.rs crates/hammer-service/tests/tcp_session_node.rs crates/hammer-service/tests/app_tcp_runtime.rs crates/hammer-service/tests/app_tcp_connect_runtime.rs
git commit -m "workspace(Refactor): format tcp session poll node changes"
```

Expected: formatting commit only if rustfmt produced changes after prior commits.

---

## Commit Order

1. `docs(Refactor): remove obsolete tcp session helper plans`
2. `hammer-adapter(Feat): schedule polling driver nodes generically`
3. `hammer-runtime(Feat): add app ring poll APIs`
4. `hammer-service(Refactor): remove tcp session shared inbox`
5. `hammer-service(Feat): poll app rings from tcp session runtime`
6. `hammer-service(Feat): drive tcp session poll loop`
7. `hammer-service(Refactor): move tcp app progression to session node`
8. `workspace(Refactor): format tcp session poll node changes` if formatting changes remain after the scoped commits

## Self-Review

- Spec coverage: the plan removes the old shared inbox/helper API route, keeps runtime APIs generic, maps VPP input node behavior onto Hammer `DriverNode`, consumes app SQEs from owner-worker rings, writes CQEs through the app backend, keeps timer wheel state worker-local, and moves service-owned app progression into session node responsibilities.
- Type consistency: `TcpSessionNode::new(DataWorkerId)`, `DataPlaneRuntime::schedule_polling_driver_nodes`, `AppBackend::try_pop_submission_entry`, `AppBackend::try_pop_tcp_shutdown`, `TcpSessionRuntime::attach_app_backend`, `TcpSessionRuntime::poll_app_rings`, `TcpSessionRuntime::poll_once`, and `TcpAppCommand` are introduced before later tasks use them.
- Control-plane boundary: the service may still publish listener/config state, but connection app sends, app shutdowns, timer expiry dispatch, and ready connection progression are tested through `TcpSessionRuntime` and `TcpSessionNode`.
