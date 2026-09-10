# Per-Thread State and Worker Dispatch Completion Review

## Feature and changed surface

ADR-0010 removes the generic `ThreadOwned<T>` lifecycle and the runtime-wide
main-to-worker closure queue. Session, TCP, UDP, TLS, HTTP, QUIC, IP, and Buffer
owners now construct their existing worker collections before launch and index
the current worker directly. Cross-thread Session operations use the Session
event queue; Application detach uses `WorkerBarrier`; IP reassembly expiry uses
the thread-zero Process walk.

The change also deletes the unused runtime metrics registry and dependencies,
and moves `Network` and `SocksAddr` from `hammer-runtime` to
`hammer-service::net` without a compatibility re-export. The Session event wire
record grows from 16 to 24 bytes through `control_data`, with attach protocol
version 5. QUIC's worker-local lower UDP connect calls the concrete UDP owner
API directly; no generic transport dispatch helper is added.

Changed owners are `hammer-infra`, `hammer-core`, `hammer-runtime`,
`hammer-service`, and the IP, TCP, UDP, TLS, HTTP, and QUIC plugins. No common
per-thread container, owner trait, capability token, closure accessor, or
replacement runtime metrics API was added.

## VPP analog and evidence

- `third_party/vpp/src/vnet/session/session.h`: `session_main_t::wrk`,
  `session_main_get_worker`, and current-thread assertions establish direct
  indexed Session worker access.
- `third_party/vpp/src/vnet/tcp/tcp.h`, `vnet/udp/udp.h`, and
  `plugins/quic/quic.h`: protocol mains select concrete worker vectors by
  thread index.
- `third_party/vpp/src/vnet/session/session.c` and `session_node.c`:
  cross-thread Session control work is queued to the target Session worker and
  dispatched by the Session node.
- `third_party/vpp/src/vnet/session/application.c`: `application_free`,
  `application_detach_process`, and `vnet_application_detach` perform
  Application cleanup from the control path and synchronize worker-visible
  teardown.
- `third_party/vpp/src/vnet/session/application.h` and `application.c`:
  `app_rx_mq_elt_t::flags`, `app_rx_mq_fd_read_ready`, and
  `appsl_rx_mqs_input_node` keep the MQ pending flag on the selected worker;
  the File callback and input Node access it serially while producers signal
  the queue.
- `third_party/vpp/src/vnet/ip/reass/ip4_full_reass.c`:
  `ip4_full_reass_walk_expired` iterates and locks per-thread reassembly state
  from the expiry Process. The IPv6 counterpart is in
  `ip6_full_reass.c::ip6_full_reass_walk_expired`.
- `third_party/vpp/src/vlib/buffer.h`, `buffer.c`, and `buffer_funcs.h`: Buffer
  Pools own per-thread cache vectors selected by the current `vlib_main_t`
  thread index.

VPP contains worker-to-main generic RPC, but the scoped search found no
symmetric runtime facility that sends an arbitrary closure receiving another
worker's `vlib_main_t *`. The removed Hammer queue was therefore not a VPP
runtime analog.

## Verdict

`Aligned` for ownership, scheduling, lifecycle boundaries, and API placement.

## Findings

No blocking findings remain.

`Non-blocking`: behavior tests were explicitly excluded from this delivery.
Compilation can prove that all targets migrate to the new signatures, but it
cannot prove worker exclusivity, Application MQ wakeup behavior, detach
failure recovery, Session event targeting, transport completion, reassembly
expiry, or Buffer cache behavior. These remain residual verification risk.

During completion review, four blocking implementation defects were corrected:

- Application MQ attach rollback now unregisters every File before releasing
  the boxed entry referenced by File private data. A failed unregister retains
  the resources instead of creating a dangling pointer.
- Application MQ pending state is a worker-local `Cell<bool>`, matching VPP's
  plain per-worker pending flag. File callbacks and input-node draining execute
  serially on the selected Data Worker; producers do not inspect that flag and
  signal the queue's empty-to-nonempty transition. Draining retains pending for
  a nonempty queue or dequeue error, while an enqueue after the empty check
  leaves a readable signal that schedules the Application on the next File
  poll.
- Application detach now completes the fallible Session listener cleanup before
  unregistering Application MQ Files, so a failed transport unlisten does not
  leave a still-attached Application without worker MQ readiness. Session
  Worker readiness cleanup also retains its File index when deletion fails.
- IP reassembly production expiry now has one entry point: the thread-zero
  Process walk. The unused public worker-local Node expiry methods were removed.

The process-global Session and Buffer direct-index APIs also state an explicit
unsafe contract: the caller must be the unique runtime execution thread for
that index for the full borrow. This exposes the real `RefCell`/`Sync`
condition without adding another capability type.

## Commands run

Final pre-commit gate passed:

```text
cargo fmt --all
cargo fmt --all -- --check
git diff --check
cargo check --locked --workspace --all-targets
```

All four commands exited successfully. `cargo check` emitted existing warning
categories but no errors. No test command was authorized or run.
