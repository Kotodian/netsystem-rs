# VPP Feature Review: Attach Protocol v2

## Feature and changed surface

Issue #184 upgrades the external Application attach protocol to version 2.
Attach publishes one private Application Rx MQ per Hammer Data Worker, and an
accepted App Session selects that already-published MQ through
`SessionHandle.worker_index()`. Session publication no longer sends a private
per-session TX MQ segment or signal descriptor.

The changed production surface is in `hammer-runtime` attach and App Session
layout code, `hammer-app` attach parsing, `hammer-service` Application/Session
publication, and daemon attach wiring.

## VPP analog and evidence

- VPP Socket API attach is control work on thread 0. Its zero-initialized socket
  File records do not set `polling_thread_index`
  (`third_party/vpp/src/vnet/session/session_api.c:2319-2332`), and File polling
  resolves index 0 through `vlib_get_main_by_index`
  (`third_party/vpp/src/vlib/file.c:25-29`). The read callback takes the worker
  barrier and invokes `session_api_attach_handler`
  (`third_party/vpp/src/vnet/session/session_api.c:2217-2247`). The binary API
  counterpart is `vl_api_app_attach_t_handler`, which calls
  `vnet_application_attach`
  (`third_party/vpp/src/vnet/session/session_api.c:737-775`).
- VPP allocates `vlib_num_workers() + 1` private Rx MQs
  (`third_party/vpp/src/vnet/session/application.c:514-559`) and publishes all
  of their eventfds at attach
  (`third_party/vpp/src/vnet/session/session_api.c:808-816`).
- The extra queue is not caused by attach running on Main Thread. VPP allocates
  `1 /* main thread */ + vtm->n_threads` Session Workers
  (`third_party/vpp/src/vnet/session/session.c:2014-2049`), installs private MQ
  readiness on its matching global thread index
  (`third_party/vpp/src/vnet/session/application.c:428-484`), and explicitly
  runs `session-queue` against `session_worker[0]` on Main Thread
  (`third_party/vpp/src/vnet/session/session_node.c:2247-2306`). With Data
  Workers present, attach chooses global thread 1 as the control MQ
  (`third_party/vpp/src/vnet/session/session_api.c:823-831`).
- VPP accepted-session publication sends FIFO offsets plus the existing VPP
  event-queue offset and `mq_index = s->thread_index`
  (`third_party/vpp/src/vnet/session/session_api.c:241-301`). VCL attaches that
  MQ from the already-mounted segment by offset/index
  (`third_party/vpp/src/vcl/vcl_private.c:583-620` and
  `third_party/vpp/src/vcl/vppcom.c:364-385`); it does not receive a new
  per-session event-queue descriptor.

## Hammer comparison

- Hammer's Main Thread owns attach/control lifecycle, but it does not own a
  Session Worker or run the `session-queue` Graph Node. `SessionMain` allocates
  exactly `configured_worker_count()` worker slots
  (`crates/hammer-service/src/session/mod.rs:49-59` and
  `crates/hammer-service/src/session/runtime.rs:246-265`), while Session worker
  initialization runs only through the Data Worker hook
  (`crates/hammer-service/src/session/mod.rs:73-115`).
- Hammer runtime thread indices include Main Thread as 0, but `DataWorkerId`
  deliberately subtracts one (`crates/hammer-runtime/src/engine.rs:374-388`).
  Session handles store the zero-based `DataWorkerId.slot()` directly
  (`crates/hammer-service/src/session/runtime.rs:1149-1157`). Therefore the
  private MQ vector must contain exactly `configured_worker_count()` entries,
  indexed `0..N-1`; adding VPP's Main Thread queue would create an unused entry
  and make the published index contract ambiguous.
- Attach v2 publishes the shared MQ segment, exact worker count, all worker
  offsets, and one write signal descriptor per worker. The receiver validates
  the bounded variable descriptor count before mapping queues
  (`crates/hammer-runtime/src/attach/application.rs` and
  `crates/hammer-app/src/attach.rs`).
- Accepted-session publication now carries only the session segment and
  app-facing event signal. `AppClient::accept` selects its Application Rx MQ
  directly with `SessionHandle.worker_index()`
  (`crates/hammer-runtime/src/attach.rs:393-421` and
  `crates/hammer-app/src/attach.rs:292-354`).

## Verdict

**Aligned.** Hammer intentionally diverges from VPP's global thread-indexed
`N + 1` queue set because Hammer's Main Thread has no Session/dataplane work.
The worker-only queue set and direct zero-based handle mapping preserve VPP's
ownership and accepted-session semantics without publishing an unusable Main
Thread queue.

## Findings

### Blocking

None.

### Non-blocking

1. The inherited #183 attach path is not failure-atomic. `attach_with_mq`
   allocates the Application identity before resource creation/worker
   installation, and `install_application_mqs` returns the first installation
   error without removing earlier worker registrations
   (`crates/hammer-service/src/session/application.rs:303-340` and
   `crates/hammer-service/src/session/runtime.rs:456-495`). This behavior is
   already present at the #184 base commit `5d88d993`; it is not introduced by
   attach protocol v2. A follow-up must preserve the primary attach error while
   making every cleanup error observable; restoring the removed implementation
   that replaced the primary error is not acceptable.

## Commands run

- Vendored VPP `rg`, `sed`, and numbered source inspection across
  `vnet/session`, `vlib/file.c`, and `vcl`.
- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo test -p hammer-app` (8 integration tests passed)
- `cargo test -p hammer-runtime` (124 unit tests and all integration tests
  passed)
- `cargo test -p hammer-service --lib` (47 tests passed)
- `cargo test -p hammer-service --test svm_session_create` (3 tests passed)
- `cargo check -p hammer-runtime -p hammer-service -p hammer-app -p hammer`
- `cargo clippy -p hammer-runtime -p hammer-service -p hammer-app -p hammer --all-targets`
  (passed with existing warnings)
- The full workspace test suite was not run, per maintainer instruction.
