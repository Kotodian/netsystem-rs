# L5 Session Queue Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the TCP-owned app session polling surface with a protocol-neutral L5 `SessionQueueNode`, while TCP becomes a registered `SessionProtocolOps` implementation.

**Architecture:** Hammer keeps the reusable graph role as `DriverNode`; the new L5 node is semantically VPP's `session-queue` input node and is implemented as an empty-frame DriverNode. The L5 runtime owns app SQ polling, CQ completion helpers, worker-local ready ids, worker-local timer expiry, and protocol dispatch through a registry keyed by allocated `SessionProtocolId`. TCP owns only TCP connection lookup/state, TCP-private timer-token interpretation, and TCP output progression.

**Tech Stack:** Rust 2024, `hammer-adapter` `Node`/`DriverNode`/`DataPlaneRuntime`, `hammer-runtime::app` SQ/CQ rings, `hammer-infra::{vec,map,timer_wheel}`, `hammer-service` TCP transport state, VPP-style session queue dispatch.

---

## Current Inputs

- Current generic DriverNode scheduling already exists in `hammer-adapter`:
  - `NodeRuntime::polling_driver_nodes`
  - `DataPlaneRuntime::schedule_empty_frame`
  - `DataPlaneRuntime::schedule_polling_driver_nodes`
  - `DataPlaneRuntime::set_node_interrupt_pending`
- Current app ring sync polling APIs already exist in `hammer-runtime`:
  - `AppBackend::try_pop_submission_entry`
  - `AppBackend::try_pop_sqe_descriptor`
  - `AppBackend::try_pop_tcp_shutdown`
  - `AppBackend::try_push_cqe_descriptor`
- Current mixed implementation lives in `crates/hammer-service/src/transport/tcp/session.rs` and contains:
  - public TCP app wrappers: `TcpAppCommand`, `TcpAppSend`, `TcpAppRecv`, `TcpAppClose`, `TcpAppShutdownCommand`
  - public TCP node: `TcpSessionNode`
  - app backend attachment and SQ polling
  - ready connection queue
  - timer wheel
  - TCP connection table
- Target shape follows `docs/superpowers/specs/2026-06-13-l5-app-session-layer-design.md`: public app/ready/timer/node/session-queue behavior moves to `crates/hammer-service/src/session/`; TCP app wrappers and TCP session node stop being public API.

## File Structure

- Create `crates/hammer-service/src/session/mod.rs`
  - Re-export L5 session public types.
  - Do not export TCP or QUIC names.

- Create `crates/hammer-service/src/session/app.rs`
  - Define `AppSessionId`, `AppSessionSubmission`, `AppSessionSend`, `AppSessionRecv`, `AppSessionClose`, `AppSessionShutdown`, `AppSessionCompletion`.
  - Own `AppSessionAppIngress` for app backend attachment, sync SQ polling, shutdown polling, NOP completion, and CQ descriptor writing.

- Create `crates/hammer-service/src/session/ready.rs`
  - Define `AppSessionReadyQueue`.
  - Deduplicate `AppSessionId` with `hammer_infra::map::FlatHashTable`.

- Create `crates/hammer-service/src/session/timer.rs`
  - Define `AppSessionTimerToken`, `AppSessionTimerExpiry`, `AppSessionTimerWheel`.
  - Encode only opaque protocol timer tokens; do not define TCP or QUIC timer enums.

- Create `crates/hammer-service/src/session/protocol.rs`
  - Define `SessionProtocolId`, `SessionProtocolContext`, `SessionProtocolOps`, `SessionProtocolRegistry`.
  - Bind `AppSessionId` to allocated protocol ids without hard-coded `Tcp` or `Quic` enum variants.

- Create `crates/hammer-service/src/session/worker.rs`
  - Define `WorkerSessionRuntime`, `SessionQueueRuntime`, `SessionQueueStep`.
  - Compose app ingress, ready queue, timer wheel, protocol registry, and dispatch loops.

- Create `crates/hammer-service/src/session/node.rs`
  - Define `SessionQueueNode`.
  - Register as DriverNode with name `session-queue`.
  - Use thread-local `SessionQueueRuntime` slots through `NodeRuntimeData`.

- Modify `crates/hammer-service/src/lib.rs`
  - Add `pub mod session;`.

- Replace `crates/hammer-service/src/transport/tcp/session.rs` with `crates/hammer-service/src/transport/tcp/session_protocol.rs`
  - Keep TCP connection table ownership and TCP protocol glue.
  - Remove public app wrappers and public TCP session node.
  - Keep TCP timer kind private and encode it as `AppSessionTimerToken`.

- Modify `crates/hammer-service/src/transport/tcp/mod.rs`
  - Change `pub mod session;` to `pub mod session_protocol;`.
  - Re-export `TcpSessionProtocol`.
  - Stop re-exporting `TcpSessionNode`, `TcpSessionRuntime`, `TcpSessionStep`, `TcpAppCommand`, `TcpAppSend`, `TcpAppRecv`, `TcpAppClose`, `TcpAppShutdownCommand`, and `TcpSessionTimerKind`.

- Create `crates/hammer-service/tests/session_runtime.rs`
  - Cover L5 app ingress, completion, ready queue, timer wheel, and deterministic worker runtime behavior.

- Create `crates/hammer-service/tests/session_queue_node.rs`
  - Cover empty-frame DriverNode execution and protocol registry dispatch.

- Create `crates/hammer-service/tests/tcp_session_protocol.rs`
  - Cover TCP protocol binding and TCP readiness from L5 submissions/timers.

- Delete `crates/hammer-service/tests/tcp_session_node.rs`
  - Its useful cases move into the three new tests above.

---

### Task 1: Add L5 Session Module Shell

**Files:**
- Create: `crates/hammer-service/src/session/mod.rs`
- Create: `crates/hammer-service/src/session/app.rs`
- Create: `crates/hammer-service/src/session/ready.rs`
- Create: `crates/hammer-service/src/session/timer.rs`
- Create: `crates/hammer-service/src/session/protocol.rs`
- Create: `crates/hammer-service/src/session/worker.rs`
- Create: `crates/hammer-service/src/session/node.rs`
- Modify: `crates/hammer-service/src/lib.rs`

- [ ] **Step 1: Add the module entry point**

Create `crates/hammer-service/src/session/mod.rs` with this content:

```rust
pub mod app;
pub mod node;
pub mod protocol;
pub mod ready;
pub mod timer;
pub mod worker;

pub use app::{
    AppSessionAppIngress, AppSessionClose, AppSessionCompletion, AppSessionId, AppSessionRecv,
    AppSessionSend, AppSessionShutdown, AppSessionSubmission,
};
pub use node::SessionQueueNode;
pub use protocol::{
    SessionProtocolContext, SessionProtocolId, SessionProtocolOps, SessionProtocolRegistry,
};
pub use ready::AppSessionReadyQueue;
pub use timer::{AppSessionTimerExpiry, AppSessionTimerToken, AppSessionTimerWheel};
pub use worker::{SessionQueueRuntime, SessionQueueStep, WorkerSessionRuntime};
```

- [ ] **Step 2: Add the public service module**

In `crates/hammer-service/src/lib.rs`, insert this line with the other public modules:

```rust
pub mod session;
```

- [ ] **Step 3: Add temporary type shells**

Create `crates/hammer-service/src/session/app.rs` with this content:

```rust
use std::net::Shutdown;

use hammer_core::error::{CoreError, CoreResult};
use hammer_runtime::app::{
    AppBackend, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppObjectRef, AppRegisteredBuffer,
    AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown, AppUserData,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSessionId(u64);

impl AppSessionId {
    #[inline(always)]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub enum AppSessionSubmission {
    Send(AppSessionSend),
    Recv(AppSessionRecv),
    Close(AppSessionClose),
    Shutdown(AppSessionShutdown),
}

impl AppSessionSubmission {
    #[inline]
    pub fn session_id(&self) -> AppSessionId {
        match self {
            Self::Send(send) => send.session_id(),
            Self::Recv(recv) => recv.session_id(),
            Self::Close(close) => close.session_id(),
            Self::Shutdown(shutdown) => shutdown.session_id(),
        }
    }
}

#[derive(Debug)]
pub struct AppSessionSend {
    session_id: AppSessionId,
    descriptor: AppSqeDescriptor,
    registered: AppRegisteredBuffer,
}

impl AppSessionSend {
    #[inline]
    pub fn new(
        session_id: AppSessionId,
        descriptor: AppSqeDescriptor,
        registered: AppRegisteredBuffer,
    ) -> Self {
        Self {
            session_id,
            descriptor,
            registered,
        }
    }

    #[inline]
    pub const fn session_id(&self) -> AppSessionId {
        self.session_id
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
pub struct AppSessionRecv {
    session_id: AppSessionId,
    descriptor: AppSqeDescriptor,
    max_len: u32,
}

impl AppSessionRecv {
    #[inline]
    pub const fn new(session_id: AppSessionId, descriptor: AppSqeDescriptor, max_len: u32) -> Self {
        Self {
            session_id,
            descriptor,
            max_len,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub const fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }

    #[inline]
    pub const fn max_len(self) -> u32 {
        self.max_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionClose {
    session_id: AppSessionId,
    descriptor: AppSqeDescriptor,
}

impl AppSessionClose {
    #[inline]
    pub const fn new(session_id: AppSessionId, descriptor: AppSqeDescriptor) -> Self {
        Self {
            session_id,
            descriptor,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub const fn descriptor(self) -> AppSqeDescriptor {
        self.descriptor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionShutdown {
    session_id: AppSessionId,
    shutdown: AppTcpShutdown,
}

impl AppSessionShutdown {
    #[inline]
    pub const fn new(session_id: AppSessionId, shutdown: AppTcpShutdown) -> Self {
        Self {
            session_id,
            shutdown,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline]
    pub fn shutdown(self) -> AppTcpShutdown {
        self.shutdown
    }

    #[inline]
    pub fn how(self) -> Shutdown {
        self.shutdown.how()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionCompletion {
    session_id: AppSessionId,
    user_data: AppUserData,
    result: i32,
    flags: AppCqeFlags,
    data: AppCqeData,
}

impl AppSessionCompletion {
    #[inline]
    pub const fn new(
        session_id: AppSessionId,
        user_data: AppUserData,
        result: i32,
        flags: AppCqeFlags,
        data: AppCqeData,
    ) -> Self {
        Self {
            session_id,
            user_data,
            result,
            flags,
            data,
        }
    }

    #[inline]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }
}

#[derive(Clone, Debug)]
struct AppSessionBackend {
    session_id: AppSessionId,
    flow: hammer_runtime::app::AppFlowId,
    backend: AppBackend,
}

pub struct AppSessionAppIngress {
    backends: hammer_infra::vec::Vec<AppSessionBackend>,
    backend_slots: hammer_infra::map::FlatHashTable<u64, usize>,
}

impl AppSessionAppIngress {
    #[inline]
    pub fn new() -> Self {
        Self {
            backends: hammer_infra::vec::Vec::new(),
            backend_slots: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn attach_backend(&mut self, session_id: AppSessionId, backend: AppBackend) -> CoreResult<()> {
        if self.backend_slots.lookup(&session_id.get()).is_some() {
            return Err(CoreError::internal(format!(
                "app session backend already attached for session {}",
                session_id.get()
            )));
        }
        let slot = self.backends.len();
        self.backends.push(AppSessionBackend {
            session_id,
            flow: backend.flow(),
            backend,
        });
        self.backend_slots.insert(session_id.get(), slot);
        Ok(())
    }

    pub fn poll_submissions(
        &mut self,
        submissions: &mut hammer_infra::vec::Vec<AppSessionSubmission>,
    ) -> CoreResult<usize> {
        let backends: std::vec::Vec<AppSessionBackend> = self.backends.iter().cloned().collect();
        let mut polled = 0usize;
        for app_backend in backends {
            while let Some(entry) = app_backend.backend.try_pop_submission_entry() {
                self.handle_submission(&app_backend, entry, submissions)?;
                polled += 1;
            }
            while let Some(shutdown) = app_backend.backend.try_pop_tcp_shutdown() {
                if shutdown.flow() != app_backend.flow {
                    return Err(CoreError::internal(format!(
                        "app session shutdown flow {} does not match attached flow {}",
                        shutdown.flow().value(),
                        app_backend.flow.value()
                    )));
                }
                submissions.push(AppSessionSubmission::Shutdown(AppSessionShutdown::new(
                    app_backend.session_id,
                    shutdown,
                )));
                polled += 1;
            }
        }
        Ok(polled)
    }

    pub fn complete(&mut self, completion: AppSessionCompletion) -> CoreResult<()> {
        let slot = self
            .backend_slots
            .lookup(&completion.session_id.get())
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "app session completion backend missing for session {}",
                    completion.session_id.get()
                ))
            })?;
        let app_backend = self
            .backends
            .get(slot)
            .ok_or_else(|| CoreError::internal("app session backend slot is invalid"))?;
        app_backend
            .backend
            .try_push_cqe_descriptor(AppCqeDescriptor::new(
                completion.user_data,
                completion.result,
                completion.flags,
                AppObjectRef::Flow(app_backend.flow),
                completion.data,
            ))
            .map_err(CoreError::from)
    }

    fn handle_submission(
        &mut self,
        app_backend: &AppSessionBackend,
        entry: AppSubmissionEntry,
        submissions: &mut hammer_infra::vec::Vec<AppSessionSubmission>,
    ) -> CoreResult<()> {
        let (descriptor, registered) = entry.into_parts();
        if descriptor.object() != AppObjectRef::Flow(app_backend.flow) {
            return Err(CoreError::internal(format!(
                "app session submission object {:?} does not match attached flow {}",
                descriptor.object(),
                app_backend.flow.value()
            )));
        }
        match descriptor.payload() {
            AppSqeData::Send { buffer } => {
                let registered = registered.ok_or_else(|| {
                    CoreError::internal("app session send submission is missing registered buffer")
                })?;
                if registered.index() != buffer {
                    return Err(CoreError::internal("app session send buffer index mismatch"));
                }
                submissions.push(AppSessionSubmission::Send(AppSessionSend::new(
                    app_backend.session_id,
                    descriptor,
                    registered,
                )));
            }
            AppSqeData::Recv { max_len } => {
                submissions.push(AppSessionSubmission::Recv(AppSessionRecv::new(
                    app_backend.session_id,
                    descriptor,
                    max_len,
                )));
            }
            AppSqeData::Close => {
                submissions.push(AppSessionSubmission::Close(AppSessionClose::new(
                    app_backend.session_id,
                    descriptor,
                )));
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
                    "unsupported app session submission opcode {:?}",
                    descriptor.opcode()
                )));
            }
        }
        Ok(())
    }
}
```

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime app_session_ready_queue_dedupes_session_ids -- --exact
```

Expected: FAIL because the `session_runtime` test file does not exist yet. This confirms the next task starts from a red test.

- [ ] **Step 4: Add empty module files that compile with the module entry point**

Create `crates/hammer-service/src/session/ready.rs`:

```rust
use crate::session::AppSessionId;

pub struct AppSessionReadyQueue {
    ready: hammer_infra::vec::Vec<AppSessionId>,
    slots: hammer_infra::map::FlatHashTable<u64, usize>,
}

impl AppSessionReadyQueue {
    #[inline]
    pub fn new() -> Self {
        Self {
            ready: hammer_infra::vec::Vec::new(),
            slots: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn mark_ready(&mut self, session_id: AppSessionId) {
        if self.slots.lookup(&session_id.get()).is_some() {
            return;
        }
        let slot = self.ready.len();
        self.ready.push(session_id);
        self.slots.insert(session_id.get(), slot);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ready.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    pub fn take_ready_sessions(&mut self) -> std::vec::Vec<AppSessionId> {
        let ready = self.ready.iter().copied().collect();
        self.ready.clear();
        self.slots = hammer_infra::map::FlatHashTable::new();
        ready
    }
}
```

Create `crates/hammer-service/src/session/timer.rs`:

```rust
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::timer_wheel::{TimerHandle, TimerStartError, TimerWheel2t1w2048};

use crate::session::{AppSessionId, AppSessionReadyQueue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppSessionTimerToken(u32);

impl AppSessionTimerToken {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionTimerExpiry {
    session_id: AppSessionId,
    token: AppSessionTimerToken,
}

impl AppSessionTimerExpiry {
    #[inline(always)]
    pub const fn new(session_id: AppSessionId, token: AppSessionTimerToken) -> Self {
        Self { session_id, token }
    }

    #[inline(always)]
    pub const fn session_id(self) -> AppSessionId {
        self.session_id
    }

    #[inline(always)]
    pub const fn token(self) -> AppSessionTimerToken {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppSessionTimerSlot {
    session_id: AppSessionId,
    token: AppSessionTimerToken,
    handle: TimerHandle,
    live: bool,
}

pub struct AppSessionTimerWheel {
    wheel: TimerWheel2t1w2048,
    slots: hammer_infra::vec::Vec<AppSessionTimerSlot>,
    expired_slots: hammer_infra::vec::Vec<u32>,
    pending: hammer_infra::vec::Vec<AppSessionTimerExpiry>,
}

impl AppSessionTimerWheel {
    #[inline]
    pub fn new() -> Self {
        Self {
            wheel: TimerWheel2t1w2048::new(0),
            slots: hammer_infra::vec::Vec::new(),
            expired_slots: hammer_infra::vec::Vec::new(),
            pending: hammer_infra::vec::Vec::new(),
        }
    }

    pub fn arm_ticks(
        &mut self,
        session_id: AppSessionId,
        token: AppSessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.cancel(session_id, token);
        let user_handle = u32::try_from(self.slots.len())
            .map_err(|_| CoreError::internal("app session timer slot overflow"))?;
        let handle = self.wheel.start(user_handle, ticks).map_err(timer_start_error)?;
        self.slots.push(AppSessionTimerSlot {
            session_id,
            token,
            handle,
            live: true,
        });
        Ok(())
    }

    pub fn cancel(&mut self, session_id: AppSessionId, token: AppSessionTimerToken) -> bool {
        let Some(slot) = self.live_timer_slot(session_id, token) else {
            return false;
        };
        let timer = self
            .slots
            .get_mut(slot)
            .expect("live app session timer slot should be valid");
        timer.live = false;
        self.wheel.stop(timer.handle)
    }

    pub fn expire(
        &mut self,
        ticks: u32,
        ready: &mut AppSessionReadyQueue,
    ) -> CoreResult<usize> {
        self.expired_slots.clear();
        let expired = self.wheel.expire(ticks, &mut self.expired_slots);
        let expired_slots: std::vec::Vec<u32> = self.expired_slots.iter().copied().collect();
        for slot in expired_slots {
            let Some(timer) = self.slots.get_mut(slot as usize) else {
                return Err(CoreError::internal("app session timer slot is invalid"));
            };
            if !timer.live {
                continue;
            }
            timer.live = false;
            let expiry = AppSessionTimerExpiry::new(timer.session_id, timer.token);
            self.pending.push(expiry);
            ready.mark_ready(timer.session_id);
        }
        Ok(expired)
    }

    pub fn take_expiries(&mut self) -> std::vec::Vec<AppSessionTimerExpiry> {
        self.pending.drain(..).collect()
    }

    fn live_timer_slot(
        &self,
        session_id: AppSessionId,
        token: AppSessionTimerToken,
    ) -> Option<usize> {
        self.slots
            .iter()
            .position(|timer| timer.live && timer.session_id == session_id && timer.token == token)
    }
}

impl Default for AppSessionTimerWheel {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

fn timer_start_error(error: TimerStartError) -> CoreError {
    CoreError::internal(format!("start app session timer: {error:?}"))
}
```

Create `crates/hammer-service/src/session/protocol.rs`:

```rust
use hammer_adapter::DataWorkerId;
use hammer_core::error::{CoreError, CoreResult};

use crate::session::{
    AppSessionId, AppSessionSubmission, AppSessionTimerExpiry, WorkerSessionRuntime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionProtocolId(u16);

impl SessionProtocolId {
    #[inline(always)]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u16 {
        self.0
    }
}

pub struct SessionProtocolContext<'a> {
    worker: DataWorkerId,
    runtime: &'a mut WorkerSessionRuntime,
}

impl<'a> SessionProtocolContext<'a> {
    #[inline]
    pub fn new(worker: DataWorkerId, runtime: &'a mut WorkerSessionRuntime) -> Self {
        Self { worker, runtime }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn runtime(&mut self) -> &mut WorkerSessionRuntime {
        self.runtime
    }
}

pub trait SessionProtocolOps {
    fn handle_submission(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        submission: AppSessionSubmission,
    ) -> CoreResult<()>;

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()>;

    fn handle_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        session_id: AppSessionId,
    ) -> CoreResult<()>;
}

struct SessionProtocolSlot {
    name: &'static str,
    ops: std::boxed::Box<dyn SessionProtocolOps>,
}

pub struct SessionProtocolRegistry {
    protocols: hammer_infra::vec::Vec<SessionProtocolSlot>,
    session_protocols: hammer_infra::map::FlatHashTable<u64, u16>,
}

impl SessionProtocolRegistry {
    #[inline]
    pub fn new() -> Self {
        Self {
            protocols: hammer_infra::vec::Vec::new(),
            session_protocols: hammer_infra::map::FlatHashTable::new(),
        }
    }

    pub fn register(
        &mut self,
        name: &'static str,
        ops: std::boxed::Box<dyn SessionProtocolOps>,
    ) -> CoreResult<SessionProtocolId> {
        let slot = u16::try_from(self.protocols.len())
            .map_err(|_| CoreError::internal("session protocol registry overflow"))?;
        self.protocols.push(SessionProtocolSlot { name, ops });
        Ok(SessionProtocolId::new(slot))
    }

    pub fn bind_session(
        &mut self,
        session_id: AppSessionId,
        protocol_id: SessionProtocolId,
    ) -> CoreResult<()> {
        self.protocol_slot(protocol_id)?;
        self.session_protocols
            .insert(session_id.get(), protocol_id.get());
        Ok(())
    }

    pub fn protocol_for_session(&self, session_id: AppSessionId) -> CoreResult<SessionProtocolId> {
        self.session_protocols
            .lookup(&session_id.get())
            .map(SessionProtocolId::new)
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "session protocol binding missing for session {}",
                    session_id.get()
                ))
            })
    }

    pub fn protocol_mut(
        &mut self,
        protocol_id: SessionProtocolId,
    ) -> CoreResult<&mut dyn SessionProtocolOps> {
        let slot = self.protocol_slot(protocol_id)?;
        Ok(self.protocols[slot].ops.as_mut())
    }

    pub fn protocol_name(&self, protocol_id: SessionProtocolId) -> CoreResult<&'static str> {
        let slot = self.protocol_slot(protocol_id)?;
        Ok(self.protocols[slot].name)
    }

    pub fn dispatch_submission(
        &mut self,
        worker: DataWorkerId,
        runtime: &mut WorkerSessionRuntime,
        submission: AppSessionSubmission,
    ) -> CoreResult<()> {
        let protocol_id = self.protocol_for_session(submission.session_id())?;
        let protocol = self.protocol_mut(protocol_id)?;
        let mut context = SessionProtocolContext::new(worker, runtime);
        protocol.handle_submission(&mut context, submission)
    }

    pub fn dispatch_timer_expiry(
        &mut self,
        worker: DataWorkerId,
        runtime: &mut WorkerSessionRuntime,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()> {
        let protocol_id = self.protocol_for_session(expiry.session_id())?;
        let protocol = self.protocol_mut(protocol_id)?;
        let mut context = SessionProtocolContext::new(worker, runtime);
        protocol.handle_timer_expiry(&mut context, expiry)
    }

    pub fn dispatch_ready(
        &mut self,
        worker: DataWorkerId,
        runtime: &mut WorkerSessionRuntime,
        session_id: AppSessionId,
    ) -> CoreResult<()> {
        let protocol_id = self.protocol_for_session(session_id)?;
        let protocol = self.protocol_mut(protocol_id)?;
        let mut context = SessionProtocolContext::new(worker, runtime);
        protocol.handle_ready(&mut context, session_id)
    }

    fn protocol_slot(&self, protocol_id: SessionProtocolId) -> CoreResult<usize> {
        let slot = protocol_id.get() as usize;
        if slot >= self.protocols.len() {
            return Err(CoreError::internal(format!(
                "session protocol id {} is invalid",
                protocol_id.get()
            )));
        }
        Ok(slot)
    }
}
```

Create `crates/hammer-service/src/session/worker.rs`:

```rust
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;

use crate::session::{
    AppSessionAppIngress, AppSessionCompletion, AppSessionId, AppSessionReadyQueue,
    AppSessionSubmission, AppSessionTimerExpiry, AppSessionTimerToken, AppSessionTimerWheel,
    SessionProtocolContext, SessionProtocolRegistry,
};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionQueueStep {
    pub app_submissions: usize,
    pub expired_timers: usize,
    pub ready_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    app: AppSessionAppIngress,
    ready: AppSessionReadyQueue,
    timers: AppSessionTimerWheel,
    pending_submissions: hammer_infra::vec::Vec<AppSessionSubmission>,
    pending_timer_expiries: hammer_infra::vec::Vec<AppSessionTimerExpiry>,
    timer_tick_duration: Duration,
    last_timer_tick: Instant,
}

impl WorkerSessionRuntime {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self::with_timer_clock(worker, DEFAULT_SESSION_TIMER_TICK, Instant::now())
    }

    pub fn with_timer_clock(
        worker: DataWorkerId,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            worker,
            app: AppSessionAppIngress::new(),
            ready: AppSessionReadyQueue::new(),
            timers: AppSessionTimerWheel::new(),
            pending_submissions: hammer_infra::vec::Vec::new(),
            pending_timer_expiries: hammer_infra::vec::Vec::new(),
            timer_tick_duration,
            last_timer_tick,
        }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn attach_app_backend(
        &mut self,
        session_id: AppSessionId,
        backend: hammer_runtime::app::AppBackend,
    ) -> CoreResult<()> {
        self.app.attach_backend(session_id, backend)
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: AppSessionId) {
        self.ready.mark_ready(session_id);
    }

    #[inline]
    pub fn take_ready_sessions(&mut self) -> std::vec::Vec<AppSessionId> {
        self.ready.take_ready_sessions()
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: AppSessionId,
        token: AppSessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.timers.arm_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: AppSessionId, token: AppSessionTimerToken) -> bool {
        self.timers.cancel(session_id, token)
    }

    #[inline]
    pub fn complete(&mut self, completion: AppSessionCompletion) -> CoreResult<()> {
        self.app.complete(completion)
    }

    pub fn poll_app_submissions(&mut self) -> CoreResult<usize> {
        self.app.poll_submissions(&mut self.pending_submissions)
    }

    pub fn expire_timers(&mut self, ticks: u32) -> CoreResult<usize> {
        let expired = self.timers.expire(ticks, &mut self.ready)?;
        self.pending_timer_expiries
            .extend(self.timers.take_expiries());
        Ok(expired)
    }

    pub fn take_submissions(&mut self) -> std::vec::Vec<AppSessionSubmission> {
        self.pending_submissions.drain(..).collect()
    }

    pub fn take_timer_expiries(&mut self) -> std::vec::Vec<AppSessionTimerExpiry> {
        self.pending_timer_expiries.drain(..).collect()
    }

    pub fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let app_submissions = self.poll_app_submissions()?;
        let expired_timers = self.expire_timers(timer_ticks)?;
        Ok(SessionQueueStep {
            app_submissions,
            expired_timers,
            ready_sessions: self.ready.len(),
        })
    }

    pub fn poll_once_at(&mut self, now: Instant) -> CoreResult<SessionQueueStep> {
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
}

pub struct SessionQueueRuntime {
    sessions: WorkerSessionRuntime,
    protocols: SessionProtocolRegistry,
}

impl SessionQueueRuntime {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            protocols: SessionProtocolRegistry::new(),
        }
    }

    #[inline]
    pub fn with_protocols(
        worker: DataWorkerId,
        protocols: SessionProtocolRegistry,
    ) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            protocols,
        }
    }

    #[inline]
    pub fn sessions(&self) -> &WorkerSessionRuntime {
        &self.sessions
    }

    #[inline]
    pub fn sessions_mut(&mut self) -> &mut WorkerSessionRuntime {
        &mut self.sessions
    }

    #[inline]
    pub fn protocols_mut(&mut self) -> &mut SessionProtocolRegistry {
        &mut self.protocols
    }

    pub fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let mut step = self.sessions.poll_once_for_ticks(timer_ticks)?;
        let submissions = self.sessions.take_submissions();
        let expiries = self.sessions.take_timer_expiries();
        let worker = self.sessions.worker();

        for submission in submissions {
            self.protocols
                .dispatch_submission(worker, &mut self.sessions, submission)?;
        }

        for expiry in expiries {
            self.protocols
                .dispatch_timer_expiry(worker, &mut self.sessions, expiry)?;
        }

        let ready_sessions = self.sessions.take_ready_sessions();
        step.ready_sessions = ready_sessions.len();
        for session_id in ready_sessions {
            self.protocols
                .dispatch_ready(worker, &mut self.sessions, session_id)?;
        }

        Ok(step)
    }

    pub fn poll_once_at(&mut self, now: Instant) -> CoreResult<SessionQueueStep> {
        let timer_ticks = self.sessions.elapsed_timer_ticks(now);
        self.poll_once_for_ticks(timer_ticks)
    }
}
```

Create `crates/hammer-service/src/session/node.rs`:

```rust
use std::cell::RefCell;
use std::time::Instant;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DataWorkerId, DriverNode, Node, NodeProcessFn,
    NodeRegistration, NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::session::SessionQueueRuntime;

#[derive(Clone, Debug)]
pub struct SessionQueueNode {
    worker: DataWorkerId,
}

impl SessionQueueNode {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self { worker }
    }
}

impl Node for SessionQueueNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.clear();
        Ok(NodeResult::drop())
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        session_queue_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(register_session_queue_runtime(self.worker))
    }
}

impl DriverNode for SessionQueueNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("session-queue", 0)
    }
}

thread_local! {
    static SESSION_QUEUE_RUNTIMES: RefCell<hammer_infra::vec::Vec<SessionQueueRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_session_queue_runtime(worker: DataWorkerId) -> NodeRuntimeData {
    SESSION_QUEUE_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(SessionQueueRuntime::new(worker));
        NodeRuntimeData::from_usize(slot).expect("session queue runtime slot overflow")
    })
}

pub fn with_session_queue_runtime_for_test<R>(
    data: NodeRuntimeData,
    f: impl FnOnce(&mut SessionQueueRuntime) -> CoreResult<R>,
) -> CoreResult<R> {
    let slot = data.usize_word(0)?;
    SESSION_QUEUE_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes
            .try_borrow_mut()
            .map_err(|_| CoreError::internal("session queue runtimes borrowed"))?;
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("session queue runtime slot is invalid"))?;
        f(runtime)
    })
}

fn session_queue_process(
    _runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    with_session_queue_runtime_for_test(data, |session| {
        session.poll_once_at(Instant::now())?;
        Ok(NodeResult::drop())
    })
}
```

- [ ] **Step 5: Run format on the touched crate**

Run:

```bash
cargo fmt --all
```

Expected: PASS.

- [ ] **Step 6: Commit the module shell**

Run:

```bash
git add crates/hammer-service/src/lib.rs crates/hammer-service/src/session
git commit -m "hammer-service(Feat): add l5 session module shell"
```

Expected: one commit containing only the new L5 session module shell and `lib.rs`.

---

### Task 2: Add L5 Runtime Tests

**Files:**
- Create: `crates/hammer-service/tests/session_runtime.rs`

- [ ] **Step 1: Write ready queue and timer tests**

Create `crates/hammer-service/tests/session_runtime.rs` with this initial content:

```rust
use std::net::Shutdown;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_runtime::app::{
    AppBackend, AppBufferLease, AppCqeData, AppCqeFlags, AppObjectRef, AppOpcode,
    AppRegisteredBuffer, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown,
    AppUserData,
};
use hammer_runtime::spawn::with_data_plane_buffers;
use hammer_service::session::{
    AppSessionCompletion, AppSessionId, AppSessionReadyQueue, AppSessionSubmission,
    AppSessionTimerToken, WorkerSessionRuntime,
};

#[test]
fn app_session_ready_queue_dedupes_session_ids() {
    let mut ready = AppSessionReadyQueue::new();
    let first = AppSessionId::new(7);
    let second = AppSessionId::new(8);

    ready.mark_ready(first);
    ready.mark_ready(first);
    ready.mark_ready(second);

    assert_eq!(ready.take_ready_sessions(), vec![first, second]);
    assert!(ready.take_ready_sessions().is_empty());
}

#[test]
fn worker_session_runtime_expires_timer_into_expiry_and_ready_session() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(9);
    let token = AppSessionTimerToken::new(17);
    let mut runtime = WorkerSessionRuntime::new(worker);

    runtime
        .arm_timer_ticks(session_id, token, 3)
        .expect("arm app session timer");

    assert_eq!(runtime.expire_timers(2).expect("expire before deadline"), 0);
    assert!(runtime.take_timer_expiries().is_empty());
    assert!(runtime.take_ready_sessions().is_empty());

    assert_eq!(runtime.expire_timers(1).expect("expire at deadline"), 1);
    assert_eq!(
        runtime.take_timer_expiries(),
        vec![hammer_service::session::AppSessionTimerExpiry::new(session_id, token)]
    );
    assert_eq!(runtime.take_ready_sessions(), vec![session_id]);
}

#[test]
fn worker_session_runtime_rearming_same_timer_suppresses_stale_expiry() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(10);
    let token = AppSessionTimerToken::new(3);
    let mut runtime = WorkerSessionRuntime::new(worker);

    runtime
        .arm_timer_ticks(session_id, token, 2)
        .expect("arm first timer");
    runtime
        .arm_timer_ticks(session_id, token, 5)
        .expect("rearm timer");

    assert_eq!(runtime.expire_timers(2).expect("expire stale timer"), 0);
    assert!(runtime.take_timer_expiries().is_empty());
    assert!(runtime.take_ready_sessions().is_empty());

    assert_eq!(runtime.expire_timers(3).expect("expire rearmed timer"), 1);
    assert_eq!(runtime.take_ready_sessions(), vec![session_id]);
}

#[test]
fn worker_session_runtime_cancel_timer_suppresses_expiry() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(11);
    let token = AppSessionTimerToken::new(4);
    let mut runtime = WorkerSessionRuntime::new(worker);

    runtime
        .arm_timer_ticks(session_id, token, 2)
        .expect("arm timer");
    assert!(runtime.cancel_timer(session_id, token));

    assert_eq!(runtime.expire_timers(2).expect("expire canceled timer"), 0);
    assert!(runtime.take_timer_expiries().is_empty());
    assert!(runtime.take_ready_sessions().is_empty());
}
```

- [ ] **Step 2: Run the ready queue and timer tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime app_session_ready_queue_dedupes_session_ids -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_expires_timer_into_expiry_and_ready_session -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_rearming_same_timer_suppresses_stale_expiry -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_cancel_timer_suppresses_expiry -- --exact
```

Expected: PASS.

- [ ] **Step 3: Add app SQ polling tests**

Append these tests to `crates/hammer-service/tests/session_runtime.rs`:

```rust
#[test]
fn worker_session_runtime_polls_app_send_submission() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(21);
    let mut runtime = WorkerSessionRuntime::new(worker);
    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(session_id, backend.clone())
        .expect("attach app backend");

    let buffers = with_data_plane_buffers(Clone::clone);
    let index = buffers
        .alloc_index_with_bytes(Default::default(), b"l5-session-send")
        .expect("alloc app send buffer");
    let registered = AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
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
        .try_push_submission_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
        .expect("push app send entry");

    assert_eq!(runtime.poll_app_submissions().expect("poll app ring"), 1);
    let submissions = runtime.take_submissions();
    assert_eq!(submissions.len(), 1);

    match &submissions[0] {
        AppSessionSubmission::Send(send) => {
            assert_eq!(send.session_id(), session_id);
            assert_eq!(send.descriptor().user_data(), AppUserData::new(21));
            assert_eq!(
                send.registered()
                    .lease()
                    .copy_current()
                    .expect("copy send payload"),
                b"l5-session-send"
            );
        }
        other => panic!("unexpected submission: {other:?}"),
    }
}

#[test]
fn worker_session_runtime_polls_app_shutdown_submission() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(22);
    let mut runtime = WorkerSessionRuntime::new(worker);
    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(session_id, backend.clone())
        .expect("attach app backend");

    backend
        .try_push_tcp_shutdown(AppTcpShutdown::new(flow, Shutdown::Write))
        .expect("push app shutdown");

    assert_eq!(runtime.poll_app_submissions().expect("poll app ring"), 1);
    let submissions = runtime.take_submissions();
    assert_eq!(submissions.len(), 1);

    match submissions[0] {
        AppSessionSubmission::Shutdown(shutdown) => {
            assert_eq!(shutdown.session_id(), session_id);
            assert_eq!(shutdown.shutdown().flow(), flow);
            assert_eq!(shutdown.how(), Shutdown::Write);
        }
        ref other => panic!("unexpected submission: {other:?}"),
    }
}
```

- [ ] **Step 4: Run the app SQ polling tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_polls_app_send_submission -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_polls_app_shutdown_submission -- --exact
```

Expected: PASS.

- [ ] **Step 5: Add completion and deterministic clock tests**

Append these tests to `crates/hammer-service/tests/session_runtime.rs`:

```rust
#[test]
fn worker_session_runtime_completion_writes_cqe_descriptor() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(31);
    let mut runtime = WorkerSessionRuntime::new(worker);
    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(session_id, backend.clone())
        .expect("attach app backend");

    runtime
        .complete(AppSessionCompletion::new(
            session_id,
            AppUserData::new(31),
            7,
            AppCqeFlags::NONE,
            AppCqeData::None,
        ))
        .expect("complete app session");

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async { backend.next_cqe_descriptor().await.expect("cqe descriptor") });

    assert_eq!(descriptor.user_data(), AppUserData::new(31));
    assert_eq!(descriptor.result(), 7);
    assert_eq!(descriptor.object(), AppObjectRef::Flow(flow));
    assert_eq!(descriptor.payload(), AppCqeData::None);
}

#[test]
fn worker_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(32);
    let token = AppSessionTimerToken::new(5);
    let start = Instant::now();
    let mut runtime =
        WorkerSessionRuntime::with_timer_clock(worker, Duration::from_millis(10), start);

    runtime
        .arm_timer_ticks(session_id, token, 2)
        .expect("arm timer");

    let first = runtime
        .poll_once_at(start + Duration::from_millis(10))
        .expect("first poll");
    assert_eq!(first.expired_timers, 0);
    assert!(runtime.take_timer_expiries().is_empty());

    let second = runtime
        .poll_once_at(start + Duration::from_millis(20))
        .expect("second poll");
    assert_eq!(second.expired_timers, 1);
    assert_eq!(
        runtime.take_timer_expiries(),
        vec![hammer_service::session::AppSessionTimerExpiry::new(session_id, token)]
    );
}
```

- [ ] **Step 6: Run the completion and clock tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_completion_writes_cqe_descriptor -- --exact
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime worker_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks -- --exact
```

Expected: PASS.

- [ ] **Step 7: Commit L5 runtime tests**

Run:

```bash
git add crates/hammer-service/tests/session_runtime.rs
git commit -m "hammer-service(Test): cover l5 session runtime primitives"
```

Expected: one test commit.

---

### Task 3: Add Protocol Dispatch Tests

**Files:**
- Create: `crates/hammer-service/tests/session_queue_node.rs`
- Modify: `crates/hammer-service/src/session/protocol.rs`
- Modify: `crates/hammer-service/src/session/worker.rs`
- Modify: `crates/hammer-service/src/session/node.rs`

- [ ] **Step 1: Write protocol dispatch tests**

Create `crates/hammer-service/tests/session_queue_node.rs` with this content:

```rust
use std::cell::RefCell;
use std::rc::Rc;

use hammer_adapter::{DataPlaneRuntime, DataWorkerId};
use hammer_core::error::CoreResult;
use hammer_runtime::app::{
    AppBackend, AppBufferLease, AppObjectRef, AppOpcode, AppRegisteredBuffer, AppSqeData,
    AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};
use hammer_runtime::spawn::with_data_plane_buffers;
use hammer_service::session::{
    AppSessionId, AppSessionSubmission, AppSessionTimerExpiry, AppSessionTimerToken,
    SessionProtocolContext, SessionProtocolOps, SessionProtocolRegistry, SessionQueueNode,
    SessionQueueRuntime,
};

#[derive(Default)]
struct RecordingState {
    submissions: Vec<AppSessionId>,
    timers: Vec<AppSessionTimerExpiry>,
    ready: Vec<AppSessionId>,
}

struct RecordingProtocol {
    state: Rc<RefCell<RecordingState>>,
}

impl SessionProtocolOps for RecordingProtocol {
    fn handle_submission(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        submission: AppSessionSubmission,
    ) -> CoreResult<()> {
        let session_id = submission.session_id();
        self.state.borrow_mut().submissions.push(session_id);
        context.runtime().mark_ready(session_id);
        Ok(())
    }

    fn handle_timer_expiry(
        &mut self,
        _context: &mut SessionProtocolContext<'_>,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()> {
        self.state.borrow_mut().timers.push(expiry);
        Ok(())
    }

    fn handle_ready(
        &mut self,
        _context: &mut SessionProtocolContext<'_>,
        session_id: AppSessionId,
    ) -> CoreResult<()> {
        self.state.borrow_mut().ready.push(session_id);
        Ok(())
    }
}

#[test]
fn session_queue_node_runs_as_empty_frame_driver() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let driver = runtime
        .nodes()
        .register_driver(SessionQueueNode::new(DataWorkerId::new(0)));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule session queue node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
}

#[test]
fn session_queue_runtime_dispatches_submission_to_registered_protocol() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(41);
    let state = Rc::new(RefCell::new(RecordingState::default()));
    let mut registry = SessionProtocolRegistry::new();
    let protocol_id = registry
        .register(
            "recording",
            Box::new(RecordingProtocol {
                state: state.clone(),
            }),
        )
        .expect("register protocol");
    registry
        .bind_session(session_id, protocol_id)
        .expect("bind session protocol");

    let mut runtime = SessionQueueRuntime::with_protocols(worker, registry);
    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .sessions_mut()
        .attach_app_backend(session_id, backend.clone())
        .expect("attach app backend");

    let buffers = with_data_plane_buffers(Clone::clone);
    let index = buffers
        .alloc_index_with_bytes(Default::default(), b"dispatch-send")
        .expect("alloc app send buffer");
    let registered = AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
        .expect("registered buffer");
    let descriptor = AppSqeDescriptor::new(
        AppOpcode::Send,
        AppUserData::new(41),
        AppObjectRef::Flow(flow),
        AppSqeData::Send {
            buffer: registered.index(),
        },
    );
    backend
        .try_push_submission_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
        .expect("push app send entry");

    let step = runtime.poll_once_for_ticks(0).expect("poll session queue");
    assert_eq!(step.app_submissions, 1);
    assert_eq!(step.ready_sessions, 1);

    let state = state.borrow();
    assert_eq!(state.submissions, vec![session_id]);
    assert_eq!(state.ready, vec![session_id]);
}

#[test]
fn session_queue_runtime_dispatches_timer_to_registered_protocol() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(42);
    let token = AppSessionTimerToken::new(99);
    let state = Rc::new(RefCell::new(RecordingState::default()));
    let mut registry = SessionProtocolRegistry::new();
    let protocol_id = registry
        .register(
            "recording",
            Box::new(RecordingProtocol {
                state: state.clone(),
            }),
        )
        .expect("register protocol");
    registry
        .bind_session(session_id, protocol_id)
        .expect("bind session protocol");

    let mut runtime = SessionQueueRuntime::with_protocols(worker, registry);
    runtime
        .sessions_mut()
        .arm_timer_ticks(session_id, token, 2)
        .expect("arm timer");

    let step = runtime.poll_once_for_ticks(2).expect("poll session queue");
    assert_eq!(step.expired_timers, 1);
    assert_eq!(step.ready_sessions, 1);

    let state = state.borrow();
    assert_eq!(state.timers, vec![AppSessionTimerExpiry::new(session_id, token)]);
    assert_eq!(state.ready, vec![session_id]);
}
```

- [ ] **Step 2: Run protocol dispatch tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_queue_node
```

Expected: PASS.

- [ ] **Step 3: Commit protocol dispatch**

Run:

```bash
git add crates/hammer-service/src/session crates/hammer-service/tests/session_queue_node.rs
git commit -m "hammer-service(Feat): dispatch l5 session queue protocols"
```

Expected: one commit containing protocol registry dispatch and node tests.

---

### Task 4: Move TCP Session Runtime Behind Protocol Ops

**Files:**
- Move: `crates/hammer-service/src/transport/tcp/session.rs` -> `crates/hammer-service/src/transport/tcp/session_protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session_protocol.rs`
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Create: `crates/hammer-service/tests/tcp_session_protocol.rs`

- [ ] **Step 1: Move the TCP session file**

Run:

```bash
git mv crates/hammer-service/src/transport/tcp/session.rs crates/hammer-service/src/transport/tcp/session_protocol.rs
```

Expected: git records a rename.

- [ ] **Step 2: Replace TCP public app wrappers with protocol-private pending work**

In `crates/hammer-service/src/transport/tcp/session_protocol.rs`:

1. Remove imports for `BufferFrame`, `DataPlaneRuntime`, `DriverNode`, `Node`, `NodeProcessFn`, `NodeRegistration`, `NodeResult`, and `NodeRuntimeData`.
2. Remove `TcpSessionNode`, `register_tcp_session_runtime`, `with_tcp_session_runtime`, and `tcp_session_process`.
3. Remove `TcpSessionRuntime`, `TcpSessionStep`, public `TcpAppCommand`, `TcpAppSend`, `TcpAppRecv`, `TcpAppClose`, and `TcpAppShutdownCommand`.
4. Rename `TcpSessionRuntime` to `TcpSessionProtocol`.
5. Replace `app_backends`, `app_backend_slots`, `app_commands`, `timer_wheel`, `timer_slots`, `expired_timer_slots`, and elapsed-clock fields with protocol-private pending vectors:

```rust
#[derive(Debug)]
enum TcpPendingAppSubmission {
    Send {
        connection_id: TcpConnectionId,
        user_data: hammer_runtime::app::AppUserData,
    },
    Recv {
        connection_id: TcpConnectionId,
        user_data: hammer_runtime::app::AppUserData,
        max_len: u32,
    },
    Close {
        connection_id: TcpConnectionId,
        user_data: hammer_runtime::app::AppUserData,
    },
    Shutdown {
        connection_id: TcpConnectionId,
        how: std::net::Shutdown,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpSessionTimerKind {
    Retransmit,
    Persist,
    OutputPacing,
}

impl TcpSessionTimerKind {
    const fn token(self) -> crate::session::AppSessionTimerToken {
        match self {
            Self::Retransmit => crate::session::AppSessionTimerToken::new(1),
            Self::Persist => crate::session::AppSessionTimerToken::new(2),
            Self::OutputPacing => crate::session::AppSessionTimerToken::new(3),
        }
    }

    fn from_token(token: crate::session::AppSessionTimerToken) -> hammer_core::error::CoreResult<Self> {
        match token.get() {
            1 => Ok(Self::Retransmit),
            2 => Ok(Self::Persist),
            3 => Ok(Self::OutputPacing),
            other => Err(hammer_core::error::CoreError::internal(format!(
                "unknown TCP session timer token {other}"
            ))),
        }
    }
}

pub struct TcpSessionProtocol {
    worker: hammer_adapter::DataWorkerId,
    connections: TcpConnectionTable,
    app_session_to_connection: hammer_infra::map::FlatHashTable<u64, TcpConnectionId>,
    connection_to_app_session: hammer_infra::map::FlatHashTable<u64, AppSessionId>,
    ready_connections: hammer_infra::vec::Vec<TcpConnectionId>,
    ready_slots: hammer_infra::map::FlatHashTable<u64, usize>,
    pending_app_submissions: hammer_infra::vec::Vec<TcpPendingAppSubmission>,
    pending_timers: hammer_infra::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)>,
}
```

- [ ] **Step 3: Add TCP session protocol methods**

In `impl TcpSessionProtocol`, keep `new`, `worker`, `install_connection`, `connection`, `lookup_connection`, `mark_connection_ready`, and `take_ready_connections` with the same semantics as the old runtime. Add these methods:

```rust
pub fn bind_app_session(
    &mut self,
    session_id: crate::session::AppSessionId,
    connection_id: TcpConnectionId,
) -> CoreResult<()> {
    if self.connection(connection_id).is_none() {
        return Err(CoreError::internal(format!(
            "TCP session protocol connection {} is missing",
            connection_id.get()
        )));
    }
    self.app_session_to_connection
        .insert(session_id.get(), connection_id);
    self.connection_to_app_session
        .insert(connection_id.get(), session_id);
    Ok(())
}

    pub fn connection_for_session(
        &self,
        session_id: crate::session::AppSessionId,
    ) -> Option<TcpConnectionId> {
    self.app_session_to_connection.lookup(&session_id.get())
}

pub fn session_for_connection(
    &self,
    connection_id: TcpConnectionId,
) -> Option<crate::session::AppSessionId> {
    self.connection_to_app_session
        .lookup(&connection_id.get())
}

pub fn retransmit_timer_token() -> crate::session::AppSessionTimerToken {
    TcpSessionTimerKind::Retransmit.token()
}

pub fn persist_timer_token() -> crate::session::AppSessionTimerToken {
    TcpSessionTimerKind::Persist.token()
}

pub fn output_pacing_timer_token() -> crate::session::AppSessionTimerToken {
    TcpSessionTimerKind::OutputPacing.token()
}

pub fn take_pending_app_submissions_for_test(
    &mut self,
) -> std::vec::Vec<(TcpConnectionId, hammer_runtime::app::AppUserData)> {
    self.pending_app_submissions
        .drain(..)
        .map(|submission| match submission {
            TcpPendingAppSubmission::Send {
                connection_id,
                user_data,
            }
            | TcpPendingAppSubmission::Recv {
                connection_id,
                user_data,
                ..
            }
            | TcpPendingAppSubmission::Close {
                connection_id,
                user_data,
            } => (connection_id, user_data),
            TcpPendingAppSubmission::Shutdown { connection_id, .. } => {
                (connection_id, hammer_runtime::app::AppUserData::new(0))
            }
        })
        .collect()
}

pub fn take_pending_timers_for_test(
    &mut self,
) -> std::vec::Vec<(TcpConnectionId, crate::session::AppSessionTimerToken)> {
    self.pending_timers
        .drain(..)
        .map(|(connection_id, kind)| (connection_id, kind.token()))
        .collect()
}
```

- [ ] **Step 4: Implement `SessionProtocolOps` for TCP**

Add this impl in `crates/hammer-service/src/transport/tcp/session_protocol.rs`:

```rust
impl crate::session::SessionProtocolOps for TcpSessionProtocol {
    fn handle_submission(
        &mut self,
        context: &mut crate::session::SessionProtocolContext<'_>,
        submission: crate::session::AppSessionSubmission,
    ) -> CoreResult<()> {
        let session_id = submission.session_id();
        let connection_id = self.connection_for_session(session_id).ok_or_else(|| {
            CoreError::internal(format!(
                "TCP session protocol binding missing for session {}",
                session_id.get()
            ))
        })?;

        match submission {
            crate::session::AppSessionSubmission::Send(send) => {
                self.pending_app_submissions.push(TcpPendingAppSubmission::Send {
                    connection_id,
                    user_data: send.descriptor().user_data(),
                });
            }
            crate::session::AppSessionSubmission::Recv(recv) => {
                self.pending_app_submissions.push(TcpPendingAppSubmission::Recv {
                    connection_id,
                    user_data: recv.descriptor().user_data(),
                    max_len: recv.max_len(),
                });
            }
            crate::session::AppSessionSubmission::Close(close) => {
                self.pending_app_submissions.push(TcpPendingAppSubmission::Close {
                    connection_id,
                    user_data: close.descriptor().user_data(),
                });
            }
            crate::session::AppSessionSubmission::Shutdown(shutdown) => {
                self.pending_app_submissions.push(TcpPendingAppSubmission::Shutdown {
                    connection_id,
                    how: shutdown.how(),
                });
            }
        }

        self.mark_connection_ready(connection_id);
        context.runtime().mark_ready(session_id);
        Ok(())
    }

    fn handle_timer_expiry(
        &mut self,
        context: &mut crate::session::SessionProtocolContext<'_>,
        expiry: crate::session::AppSessionTimerExpiry,
    ) -> CoreResult<()> {
        let connection_id = self.connection_for_session(expiry.session_id()).ok_or_else(|| {
            CoreError::internal(format!(
                "TCP session protocol binding missing for session {}",
                expiry.session_id().get()
            ))
        })?;
        let kind = TcpSessionTimerKind::from_token(expiry.token())?;
        self.pending_timers.push((connection_id, kind));
        self.mark_connection_ready(connection_id);
        context.runtime().mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready(
        &mut self,
        _context: &mut crate::session::SessionProtocolContext<'_>,
        session_id: crate::session::AppSessionId,
    ) -> CoreResult<()> {
        let connection_id = self.connection_for_session(session_id).ok_or_else(|| {
            CoreError::internal(format!(
                "TCP session protocol binding missing for session {}",
                session_id.get()
            ))
        })?;
        self.mark_connection_ready(connection_id);
        Ok(())
    }
}
```

- [ ] **Step 5: Update TCP module exports**

In `crates/hammer-service/src/transport/tcp/mod.rs`:

Replace:

```rust
pub mod session;
```

with:

```rust
pub mod session_protocol;
```

Replace the `pub use session::{ ... }` block with:

```rust
pub use session_protocol::TcpSessionProtocol;
```

- [ ] **Step 6: Add TCP protocol tests**

Create `crates/hammer-service/tests/tcp_session_protocol.rs` with this content:

```rust
use std::net::SocketAddr;

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_runtime::app::{AppObjectRef, AppOpcode, AppSqeData, AppSqeDescriptor, AppUserData};
use hammer_service::session::{
    AppSessionClose, AppSessionId, AppSessionSubmission, AppSessionTimerExpiry,
    SessionProtocolContext, SessionProtocolOps, WorkerSessionRuntime,
};
use hammer_service::transport::tcp::{TcpDataPlaneConnection, TcpLookupId, TcpSessionProtocol};

fn connection(
    lookup_id: TcpLookupId,
    connection_id: TcpConnectionId,
    worker: DataWorkerId,
) -> TcpDataPlaneConnection {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpDataPlaneConnection::new(
        lookup_id,
        Some(connection_id),
        worker,
        TcpState::Established,
        local.port(),
        Some(local),
        remote,
    )
}

#[test]
fn tcp_session_protocol_owns_connections_by_lookup_and_session_id() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(7);
    let connection_id = TcpConnectionId::new(7);
    let mut protocol = TcpSessionProtocol::new(worker);

    protocol
        .install_connection(connection(11, connection_id, worker))
        .expect("install connection");
    protocol
        .bind_app_session(session_id, connection_id)
        .expect("bind app session");

    assert!(protocol.connection(connection_id).is_some());
    assert!(protocol.lookup_connection(11).is_some());
    assert_eq!(protocol.connection_for_session(session_id), Some(connection_id));
    assert_eq!(protocol.session_for_connection(connection_id), Some(session_id));
    assert_eq!(protocol.take_ready_connections(), vec![connection_id]);
    assert!(protocol.take_ready_connections().is_empty());
}

#[test]
fn tcp_session_protocol_marks_connection_ready_from_app_submission() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(21);
    let connection_id = TcpConnectionId::new(21);
    let mut protocol = TcpSessionProtocol::new(worker);
    let mut sessions = WorkerSessionRuntime::new(worker);
    protocol
        .install_connection(connection(21, connection_id, worker))
        .expect("install connection");
    protocol.take_ready_connections();
    protocol
        .bind_app_session(session_id, connection_id)
        .expect("bind app session");

    let descriptor = AppSqeDescriptor::new(
        AppOpcode::Close,
        AppUserData::new(21),
        AppObjectRef::Flow(hammer_runtime::app::AppFlowId::new(21)),
        AppSqeData::Close,
    );
    let mut context = SessionProtocolContext::new(worker, &mut sessions);
    protocol
        .handle_submission(
            &mut context,
            AppSessionSubmission::Close(AppSessionClose::new(session_id, descriptor)),
        )
        .expect("handle close submission");

    assert_eq!(protocol.take_ready_connections(), vec![connection_id]);
    assert_eq!(sessions.take_ready_sessions(), vec![session_id]);
    assert_eq!(
        protocol.take_pending_app_submissions_for_test(),
        vec![(connection_id, AppUserData::new(21))]
    );
}

#[test]
fn tcp_session_protocol_dispatches_timer_expiry() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(22);
    let connection_id = TcpConnectionId::new(22);
    let token = TcpSessionProtocol::retransmit_timer_token();
    let mut protocol = TcpSessionProtocol::new(worker);
    let mut sessions = WorkerSessionRuntime::new(worker);
    protocol
        .install_connection(connection(22, connection_id, worker))
        .expect("install connection");
    protocol.take_ready_connections();
    protocol
        .bind_app_session(session_id, connection_id)
        .expect("bind app session");

    let mut context = SessionProtocolContext::new(worker, &mut sessions);
    protocol
        .handle_timer_expiry(
            &mut context,
            AppSessionTimerExpiry::new(session_id, token),
        )
        .expect("handle timer expiry");

    assert_eq!(protocol.take_ready_connections(), vec![connection_id]);
    assert_eq!(sessions.take_ready_sessions(), vec![session_id]);
    assert_eq!(
        protocol.take_pending_timers_for_test(),
        vec![(connection_id, token)]
    );
}
```

- [ ] **Step 7: Run TCP protocol tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_protocol
```

Expected: PASS.

- [ ] **Step 8: Commit TCP protocol migration**

Run:

```bash
git add crates/hammer-service/src/transport/tcp crates/hammer-service/tests/tcp_session_protocol.rs
git commit -m "hammer-service(Refactor): move tcp session behind l5 protocol ops"
```

Expected: one commit containing TCP session protocol migration.

---

### Task 5: Remove Old TCP Session Node Test

**Files:**
- Delete: `crates/hammer-service/tests/tcp_session_node.rs`
- Modify: `crates/hammer-service/tests/session_runtime.rs`
- Modify: `crates/hammer-service/tests/session_queue_node.rs`
- Modify: `crates/hammer-service/tests/tcp_session_protocol.rs`

- [ ] **Step 1: Delete the old TCP node test**

Run:

```bash
git rm crates/hammer-service/tests/tcp_session_node.rs
```

Expected: the old test file is staged for deletion.

- [ ] **Step 2: Run the replacement tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_queue_node
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_protocol
```

Expected: PASS for all three commands.

- [ ] **Step 3: Commit test migration**

Run:

```bash
git add crates/hammer-service/tests
git commit -m "hammer-service(Test): replace tcp session node coverage"
```

Expected: one commit deleting `tcp_session_node.rs` and keeping replacement coverage in the new test files.

---

### Task 6: Remove Old Public TCP Session API

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/mod.rs`
- Modify: `crates/hammer-service/src/transport/tcp/session_protocol.rs`
- Search: `crates/hammer-service/src`
- Search: `crates/hammer-service/tests`

- [ ] **Step 1: Verify forbidden TCP session exports are gone**

Run:

```bash
rg -n "TcpSessionNode|TcpSessionRuntime|TcpSessionStep|TcpAppCommand|TcpAppSend|TcpAppRecv|TcpAppClose|TcpAppShutdownCommand|pub enum TcpSessionTimerKind|pub use session::" crates/hammer-service/src crates/hammer-service/tests
```

Expected: no matches.

- [ ] **Step 2: Verify L5 session API does not depend on TCP**

Run:

```bash
rg -n "tcp|Tcp|quic|Quic" crates/hammer-service/src/session
```

Expected: no matches except `AppTcpShutdown` imports in `session/app.rs`. The `AppTcpShutdown` match is allowed because it is the existing app ring shutdown type; do not introduce new TCP names in L5.

- [ ] **Step 3: Verify old plan is no longer the execution target**

Run:

```bash
rg -n "TCP Session io_uring Poll Node Implementation Plan|TcpSessionNode|TcpAppCommand" docs/superpowers/plans/2026-06-13-tcp-session-io-uring-poll-node.md docs/superpowers/plans/2026-06-13-l5-session-queue-node.md
```

Expected: matches in `2026-06-13-tcp-session-io-uring-poll-node.md` are historical. Matches in `2026-06-13-l5-session-queue-node.md` must only be removal criteria or migration instructions.

- [ ] **Step 4: Commit API cleanup if Step 1 required edits**

Run:

```bash
git add crates/hammer-service/src/transport/tcp crates/hammer-service/src/session
git commit -m "hammer-service(Refactor): drop public tcp session node api"
```

Expected: one cleanup commit if source edits were needed. If `git diff --cached --quiet` is true after Step 1, skip this commit.

---

### Task 7: Final Verification

**Files:**
- Verify: `crates/hammer-service/src/session`
- Verify: `crates/hammer-service/src/transport/tcp`
- Verify: `crates/hammer-service/tests`
- Verify: `docs/superpowers/specs/2026-06-13-l5-app-session-layer-design.md`

- [ ] **Step 1: Format the workspace**

Run:

```bash
cargo fmt --all
```

Expected: PASS.

- [ ] **Step 2: Run focused service tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_runtime
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test session_queue_node
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service --test tcp_session_protocol
```

Expected: PASS for all three commands.

- [ ] **Step 3: Run relevant broader test set**

Run:

```bash
CARGO_TARGET_DIR=/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/target cargo test -p hammer-service
```

Expected: PASS.

- [ ] **Step 4: Run workspace diff check**

Run:

```bash
git diff --check
```

Expected: PASS with no whitespace errors.

- [ ] **Step 5: Confirm final API shape**

Run:

```bash
rg -n "TcpSessionNode|TcpSessionRuntime|TcpSessionStep|TcpAppCommand|TcpAppSend|TcpAppRecv|TcpAppClose|TcpAppShutdownCommand|pub enum TcpSessionTimerKind|pub use session::" crates/hammer-service/src crates/hammer-service/tests
rg -n "pub mod session;|SessionQueueNode|SessionProtocolOps|SessionQueueRuntime" crates/hammer-service/src/lib.rs crates/hammer-service/src/session crates/hammer-service/tests
```

Expected:

- First command: no matches.
- Second command: matches in the new L5 session module and tests.

- [ ] **Step 6: Commit final verification adjustments**

Run:

```bash
git status --short
git add crates/hammer-service/src crates/hammer-service/tests
git commit -m "hammer-service(Refactor): finish l5 session queue split"
```

Expected: if there are staged source/test changes after verification, one final commit. If `git status --short` is empty, skip this commit.

---

## Acceptance Checklist

- [ ] `crates/hammer-service/src/session/` exists and does not depend on `transport::tcp`.
- [ ] Public L5 API exports `SessionQueueNode`, `SessionQueueRuntime`, `WorkerSessionRuntime`, `SessionProtocolOps`, `AppSessionSubmission`, `AppSessionCompletion`, `AppSessionTimerToken`, and `AppSessionTimerExpiry`.
- [ ] L5 protocol registry allocates `SessionProtocolId`; it does not expose `Tcp` or `Quic` enum variants.
- [ ] `SessionQueueNode` registers as `NodeRegistration::next("session-queue", 0)` and runs as an empty-frame DriverNode.
- [ ] TCP registers through `TcpSessionProtocol: SessionProtocolOps`.
- [ ] `transport::tcp` no longer re-exports public TCP app session wrappers or a TCP session node.
- [ ] TCP timer kind is private to `session_protocol.rs`; public callers use `AppSessionTimerToken`.
- [ ] `cargo test -p hammer-service --test session_runtime` passes.
- [ ] `cargo test -p hammer-service --test session_queue_node` passes.
- [ ] `cargo test -p hammer-service --test tcp_session_protocol` passes.
- [ ] `cargo test -p hammer-service` passes.
- [ ] `git diff --check` passes.
