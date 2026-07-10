# Task 1 Spec Re-Review

## Verdict

PASS

The four findings from the review of `f653c2ea..ed5b4ba9` were fixed by
`59ec18a1`. The remaining Local `AppContext` regression was fixed by
`57df04a8`; no Task 1 blocking issues remain.

## AppContext Fix Re-Checked

The pre-existing `app_context: Option<AppContext<Local>>` ownership field is
restored on `SessionWorker` (`crates/hammer-service/src/session/runtime.rs:118`).
`SessionDriverRuntime::with_app_context` is also restored: it derives the
session configuration from the supplied context and then retains that context
on the worker (`crates/hammer-service/src/session/runtime.rs:611`). This
preserves both the caller's non-default `AppSessionConfig` and ownership of the
context's `DataRuntimeContext`.

The regression test constructs a context with a non-default configuration,
passes it through `with_app_context`, and verifies both the worker's effective
configuration and the retained context's configuration
(`crates/hammer-service/src/session/runtime/tests.rs:95`). The fix adds no new
public API and restores the prior ownership surface exactly.

## Original Findings Re-Checked

1. **App-first close ordering: fixed.** The queue snapshots the transport
   address, records app closure, and only then dispatches disconnect
   (`crates/hammer-service/src/session/runtime.rs:1142`). TCP disconnect does
   not report transport closure while the connection is merely FinWait1 or
   LastAck (`crates/hammer-service/src/transport/tcp/worker.rs:172`). The real
   SessionQueueNode/TcpWorker test verifies AppClosed + FinWait1 and no
   reflected app Close
   (`crates/hammer-service/src/session/runtime/tests.rs:129`).

2. **Transport-first close notification and deletion: fixed.** Closed TCP
   cleanup calls `notify_transport_closed`, removes the connection, then calls
   `notify_transport_deleted`
   (`crates/hammer-service/src/transport/tcp/worker.rs:73`). Packet-path
   publication follows the same order
   (`crates/hammer-service/src/transport/tcp/mod.rs:565`). The real-node test
   verifies one app Close, removed TCP storage, TransportDeleted retention,
   and final app cleanup without a second Close
   (`crates/hammer-service/src/session/runtime/tests.rs:177`).

3. **Production-seam lifecycle coverage: fixed.** The lifecycle tests allocate
   a concrete `SessionDriverRuntime<(TcpWorker<BbrController>, ()), Local,
   Index>`, attach it to a real SessionQueueNode, set the node polling, and run
   it through the runtime scheduler
   (`crates/hammer-service/src/session/runtime/tests.rs:53`).

4. **QUIC-shaped internal TX visibility: fixed.** The test transport copies
   both stream FIFO payloads into buffers and enqueues a frame
   (`crates/hammer-service/src/session/node/tests.rs:360`); the capture node
   observes exactly `one` and `four`
   (`crates/hammer-service/src/session/node/tests.rs:392`).

The `57df04a8` diff is limited to restoring the Local context ownership path
and its regression test, so it does not alter any of the four lifecycle and TX
fixes above.

## Verification

- `cargo test -p hammer-service --lib session:: -- --nocapture`: PASS, 16 tests.
- `cargo test -p hammer-service -- --test-threads=1`: PASS at `59ec18a1`,
  including 143 unit tests and all crate integration tests; two opt-in
  performance probes remained ignored as designed.
- `cargo fmt --all -- --check`: PASS.
- `git diff --check ed5b4ba9..57df04a8`: PASS.
