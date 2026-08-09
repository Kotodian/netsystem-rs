# Issue #219 VPP alignment and interface audit

## Status

Task #1's migration protocol decision is recorded. This document records facts
already verified against the current `main` branch and vendored VPP, plus
decisions confirmed during the session. It is not an implementation plan or
interface approval.

## Required result

Implement VPP's connected-datagram Session worker-migration semantics: the
first datagram for an `OPENED` Session may clone the UDP transport connection
and migrate the Session plus its existing FIFO storage to the arrival worker;
the old worker performs App notification and owner-local cleanup; and the
target remains migrating until the Session App accepts the migration. A
`READY` or `ACCEPTING` Session does not migrate again: VPP classifies a packet
received on another worker as `UDP_ERROR_WRONG_THREAD`, so Hammer accounts the
existing `UdpInputError::WrongWorker` and drops it. Unsupported migration
rejection may still use the owner handoff path after the source sends a typed
rejection. Keep QUIC-private connection migration in #238, and do not add a
QUIC graph node, handoff queue, or cross-worker mutable QUIC context.

## Current Hammer facts

- `UdpSessionLookup` currently publishes an exact UDP tuple to a
  `SessionHandle`, but the type, storage, publication, and cleanup all live in
  the UDP plugin. Its comment calls it VPP `session_lookup_safe4/6`, even
  though VPP's corresponding table is owned by Session. This is an ownership
  mismatch, not merely a naming mismatch.
- `SessionHandle::worker_index` is already the owning Data Worker fact needed
  by generic handoff. Like VPP's `session_handle_t`, it contains Session slot
  plus worker and does not contain a pool generation. Hammer can retain this
  existing routing identity and revalidate the generation-bearing UDP
  connection index after the packet reaches the owner.
- `UdpWorker::deliver_datagram` already consults the shared lookup after its
  worker-local tuple lookup misses, but it reduces a foreign owner to
  `UdpDelivery::WrongWorker`; `udp-input` then records
  `UdpInputError::WrongWorker` and drops the buffer.
- `DataPlaneRuntime::handoff_index` and the runtime handoff node already move a
  data-plane buffer to a target worker and node. TCP uses this path for the
  equivalent fixed-flow ownership transfer.
- `UdpInputNode` does not currently receive its own handoff target or the
  current `DataWorkerId`. `NodeRuntimeData` has four words and worker graph
  binding can install worker-specific node data without adding `thread_local!`
  state or a new runtime interface.
- Each `QuicWorker` owns one Quinn `Endpoint`, one QUIC context pool, and all
  mutable connection state. A lower UDP Session identifies exactly one QUIC
  connection context through the Session App callback.
- `SessionAppCallbacks::migrate` and `SessionApp::migrate` exist, and the proc
  macro wires the callback, but Session Runtime has no production call site
  for it. QUIC's manual callback table does not install `migrate`.
- Hammer's Session state machine has `Creating`, `Created`, `Published`, and
  `Active`, but no VPP `OPENED` half-open datagram state or migrating state.
  Active UDP connect and listener accept publish the Session immediately.
- The current QUIC server configuration retains Quinn's default
  `migration = true`; no Hammer code sets `ServerConfig::migration(false)`.
- Hammer has no cross-worker CID lookup. Quinn's CID issuance, retirement, and
  stale-CID lookup state is private to each worker-local `Endpoint`.

## Vendored VPP facts

- `session_lookup_add_connection`, `session_lookup_del_session`, and
  `session_lookup_safe4/6` live in `vnet/session/session_lookup.c`. Their key
  is the transport endpoint/protocol identity and their value is a Session
  Handle. UDP calls this Session lookup; it does not own an independent shared
  Session-owner table.
- `udp46_input_inline` clones and migrates an `OPENED` connected UDP Session
  when its first datagram arrives on another worker. A `READY` or `ACCEPTING`
  Session on another worker is classified as `UDP_ERROR_WRONG_THREAD`.
- `udp_connection_clone_safe` clones only the worker-local UDP transport
  connection. `session_dgram_connect_notify`, `session_clone_safe`,
  `session_switch_pool`, and `session_migrate_accept` own the new Session,
  shared FIFO attachment, lookup replacement, old-worker cleanup, and App
  notification.
- `quic_udp_session_migrate_callback` transfers the QUIC connection context
  after the lower UDP Session migration and completes migration on the new
  worker with `session_migrate_accept`.
- The quicly CID encryptor encodes `thread_id` into an authenticated 8-byte
  server CID. This is a stateless worker-selection fact, not a shared CID hash
  table.
- VPP sets `disable_active_migration = 1` in the QUIC transport parameters.

Relevant source locations:

- `third_party/vpp/src/vnet/udp/udp_input.c:298-345`
- `third_party/vpp/src/vnet/udp/udp.h:213-232`
- `third_party/vpp/src/vnet/session/session_lookup.c:271-430`
- `third_party/vpp/src/vnet/session/session.c:820-950`
- `third_party/vpp/src/plugins/quic/quic.c:522-539`
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly.c:1015-1041`
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly_crypto.c:674`
- `third_party/vpp/src/plugins/quic_quicly/quic_quicly_cid_enc.c:87-155`

## Confirmed scope decision

Issue #219 will not enable active connection migration, multipath QUIC, or
address-rebinding policy. The QUIC server configuration must call the existing
`quinn_proto::ServerConfig::migration(false)`, matching VPP and the parent
issue's out-of-scope statement. Hammer's client path does not initiate a local
path change. Full path migration remains owned by the later issue already
tracked by the user.

This decision needs no new Hammer configuration type, public function, wire
field, or ADR. It is a temporary feature-scope restriction and is intentionally
reversible by the later migration issue.

## Corrected ownership decision

The earlier UDP-owned owner-directory proposal is withdrawn. The user's
correction matches VPP: Session owns the shared transport-endpoint to Session
Handle lookup and the Session/FIFO lifecycle; UDP owns only UDP listener and
worker-local transport-connection facts.

For Hammer, a semantically aligned adaptation is:

- Session owns one transport-neutral shared connection lookup keyed by the
  existing `SessionTransportId` plus local and remote `SocketAddr` facts. The
  key representation remains private to Session; #219 does not need a new
  public key or identity type.
- UDP publishes and removes its connected Session through Session-owned
  operations. UDP no longer stores `UdpSessionLookup` or constructs raw shared
  `SessionHandle` values.
- `udp-input` asks Session for the existing Session Handle. It uses only the
  handle's worker fact for generic buffer handoff; it does not dereference a
  foreign worker's `SessionWorker` or mutate foreign Session state.
- The owner worker re-enters `udp-input`, resolves its generation-bearing
  worker-local `UdpConnection`, and only then enqueues the datagram into the
  Session FIFO and schedules QUIC.
- Session removes the lookup before reclaiming the Session entry. Publication
  uses `Bihash::insert_if_absent`, so a tuple cannot silently replace a live
  Session owner.

This ownership correction is necessary but not sufficient. The user confirmed
that #219 must also add the missing VPP-style UDP and Session worker-migration
semantics; generic handoff alone is not an acceptable replacement for the
`OPENED` migration lifecycle.

## Confirmed issue boundary

Issue #219 owns the complete generic lower-layer migration path:

- Session-owned transport-endpoint lookup and exact `SessionHandle` routing;
- a half-open connected datagram state equivalent to VPP `OPENED`;
- one-winner migration claim when the first datagram arrives on another Data
  Worker;
- creation of the target-worker UDP transport connection;
- creation of the target-worker Session using the same FIFO storage and
  Session-owned Application facts;
- atomic lookup replacement with the new Session Handle;
- notification and cleanup on the old Session/UDP owner;
- migration acceptance only after the new owner is ready;
- `READY`/`ACCEPTING` wrong-worker classification as the existing
  `UdpInputError::WrongWorker` drop path, because those states must not migrate
  a second time.

QUIC's private reaction is not part of #219. New issue #238 owns transferring
 the worker-local QUIC/Quinn connection context after its lower UDP Session
migrates. Until #238 is complete, unsupported QUIC application state is
rejected by the source and the pending datagram uses the migration
owner-handoff path; Hammer never mutates the original worker from the target.

Issue #235 remains limited to Quinn active network-path validation fallback
residency. It explicitly does not own VPP Data Worker Session migration.

## Confirmed packet-route decision

With migration disabled, CID lifecycle remains worker-local Quinn protocol
state and does not change the lower UDP Session owner. Issue #219 must not add
a cross-worker CID lookup, a CID route publication interface, or a vendored
Quinn event surface solely to mirror owner-local CID issuance and retirement.

The packet ownership contract is:

- An Initial without an existing connected UDP tuple is accepted on its
  arrival worker. That worker becomes the lower UDP Session and QUIC
  connection owner.
- The first datagram for an `OPENED` active UDP Session may migrate the UDP
  transport and Session/FIFO ownership to its arrival worker, matching VPP.
  Migration is claimed and published before FIFO enqueue.
- A `READY` or `ACCEPTING` Session never migrates again. A datagram received
  elsewhere follows VPP's wrong-thread path: `udp-input` records
  `UdpInputError::WrongWorker` and drops it without FIFO enqueue or App
  mutation.
- A Session App that cannot yet transfer its worker-local private state, such
  as QUIC before #238, is not partially migrated. The source returns a typed
  rejection and the pending first datagram uses the migration owner-handoff
  path; later wrong-worker datagrams still follow the `WrongWorker` drop path.
- On the owner, Quinn validates the destination CID against that exact
  connection. CID issuance and retirement remain inside the worker-local
  Quinn connection/endpoint state. A stale or foreign CID is a typed packet
  drop, not an ownership-route update.
- Connection cleanup removes the Session-owned endpoint route before the
  Session and worker-local QUIC context can be reclaimed. No shared CID route
  exists to clean up.

This uses the existing ownership facts and keeps future address migration free
to introduce its own explicitly approved design in the later tracked issue.

## Approved handoff failure contract

Deepen the existing `DataPlaneRuntime::handoff_index` contract instead of
adding a second handoff function or an ownership wrapper:

- `Ok(())` transfers buffer ownership to the target worker queue.
- Every `Err` leaves buffer ownership with the caller.
- The enqueue-race failure path must therefore stop constructing an internal
  `HandoffSlotGuard` that releases the buffer behind the caller's back.

This makes preflight queue exhaustion and enqueue-race exhaustion obey one
failure-atomic ownership rule. Existing callers can safely retain the index in
their input frame and apply their owner-local typed drop behavior.

`udp-input` follows VPP for a `WrongWorker` result and records the existing
`UdpInputError::WrongWorker` before dropping the still-owned buffer. The
migration rejection path is the one exception: the target hands the first
pending datagram back to the old owner through `handoff_index`; if that enqueue
fails, the target remains the owner and performs its terminal typed drop. No
control-plane `Result` is allocated or formatted per packet.

## Existing-interface audit

Existing surfaces that are sufficient:

- `SessionTransportId`, `SessionHandle`, `SocketAddr`, and `SessionId` already
  express the required transport, routing, endpoint, and owner-local identity
  facts. No new public identity or wrapper is needed.
- `Bihash::insert_if_absent` supports one-winner shared publication.
- `DataPlaneRuntime::handoff_index`, the generic handoff node, buffer
  `current_config`, `NodeRuntimeData`, and frame index rewriting can route the
  unchanged buffer back to `udp-input` on the owner.
- Session App FIFO/event dispatch already ensures QUIC mutation happens only
  after owner-worker Session RX enqueue.

Existing surfaces that are insufficient:

- Session has no shared transport-endpoint lookup authority or publish,
  lookup, and cleanup operations. The current equivalent is incorrectly
  private to UDP.
- UDP input has no worker-specific handoff target/current-worker binding and
  drops `WrongWorker` packets.
- `handoff_index` does not yet satisfy the approved failure-ownership contract.
- Full VPP Session migration is not available: there is no half-open datagram
  state, migration claim, target Session/UDP creation, lookup replacement,
  Session clone/switch operation, FIFO ownership transfer lifecycle, old-owner
  cleanup queue, or Session Runtime dispatch of the existing `migrate`
  callback.

## Rust ownership and wake audit

The migration path cannot treat `Arc` or a lock-free queue as an ownership
protocol by themselves:

- Rust's `Send` contract permits ownership of a value to move across thread
  boundaries. It does not require the value to be `Copy` or multiply owned.
- `Arc<T>` provides shared ownership of one allocation and is `Send`/`Sync`
  only when `T` has the corresponding properties. Rust's standard-library
  documentation explicitly states that `Arc` does not add thread safety to
  the contained data. The existing `Arc<Fifo>` values can therefore keep the
  FIFO storage alive across migration, but they do not transfer the Session's
  unique Data Worker ownership.
- `crossbeam_queue::ArrayQueue<T>` is a bounded preallocated MPMC queue whose
  cross-thread implementations require `T: Send`, not `T: Copy`.
  `push(value) -> Result<(), T>` returns the original move-only value when the
  queue is full. Its Release slot publication and Acquire consumption make the
  initialized payload visible to the consumer, and dropping the queue drops
  payloads still stored in its slots.
- `ArrayQueue` has no notification operation. Rust's `Thread::unpark` and
  `thread::park` form a Release/Acquire synchronization pair, retain one wake
  token when `unpark` wins the race with `park`, and permit spurious wakeups.
  A correct consumer must therefore re-check and drain the mailbox in a loop;
  the queue remains the work condition and the wake token is only a scheduling
  signal.

Authoritative web sources checked during the design:

- <https://doc.rust-lang.org/std/marker/trait.Send.html>
- <https://doc.rust-lang.org/std/sync/struct.Arc.html>
- <https://doc.rust-lang.org/std/thread/fn.park.html>
- <https://docs.rs/crossbeam-queue/0.3.12/crossbeam_queue/struct.ArrayQueue.html>
- <https://docs.rs/crossbeam-channel/0.5.15/crossbeam_channel/enum.TrySendError.html>

The Hammer audit finds a matching missing semantic. `DataPlaneHandoff` already
uses `ArrayQueue<HandoffFrame>`, and `run_ready_nodes` drains it before Graph
Node dispatch, but a successful handoff enqueue does not wake the target
worker. An idle worker notices the queue only after its configured
`idle_slice` timeout (1 ms by default). `DataRemoteLocalQueue` does wake its
worker, but it stores heap-allocated `FnOnce` tasks behind a `Mutex`; it is a
control/lifecycle queue and is not an acceptable packet-path or Session
migration primitive.

## Rejected generic mailbox proposal

The proposed `hammer_runtime::sync::DataWorkerMailbox<T>` is withdrawn. It
would make a generic runtime mailbox the architectural event seam, whereas VPP
keeps the event queue and migration request state in Session and uses Runtime
only to atomically mark the target Graph Node interrupt-pending and wake the
target worker. Reusing one generic mailbox for packet Handoff and Session
migration would conflate two distinct VPP mechanisms.

## VPP-aligned worker interrupt and Session request model

The exact VPP sequence is:

1. `session_send_evt_to_thread` enqueues the Session event into the target
   `session_worker_t.vpp_event_queue`.
2. If that Session Worker is in interrupt mode, it calls
   `vlib_node_set_interrupt_pending(wrk->vm, session_queue_node.index)`.
3. For a worker `vlib_main_t`, `vlib_node_set_interrupt_pending` atomically sets
   the target node's interrupt bit and calls `vlib_thread_wakeup` for that
   worker. The Runtime primitive moves no Session payload.
4. UDP's first-datagram path owns `udp_connection_clone_safe`, then calls
   Session's `session_dgram_connect_notify` for the Session/FIFO clone and
   lookup replacement.
5. Session stores `{ old_sh, new_sh }` in the old Session Worker's
   `session_migrate_requests`, sends a `SESSION_CTRL_EVT_RPC`, and the old
   worker runs `session_switch_pool`. That operation notifies the App and
   cleans up the old transport and Session on their owner worker.

Relevant source evidence:

- `third_party/vpp/src/vnet/session/session.c:22-86`
- `third_party/vpp/src/vlib/node_funcs.h:216-230`
- `third_party/vpp/src/vnet/session/session.c:820-950`
- `third_party/vpp/src/vnet/udp/udp_input.c:298-345`

Hammer should preserve this split:

- `hammer-runtime` adds only the VPP-equivalent remote Graph Node interrupt:
  atomically coalesce one exact `NodeId` in one `DataWorkerId`'s Runtime-owned
  interrupt set and wake that worker. The target Main Loop consumes the
  pending bit and schedules the exact Driver/PreInput node. Runtime never
  carries a generic `T`, Session request, UDP connection, or App callback.
- The proposed public operation is the infallible hot-path operation
  `DataPlaneRuntime::set_worker_node_interrupt_pending(worker, node)`, with
  `node: NodeId`. Hammer uses `DataWorkerId` because Rust must not lend a
  foreign worker's non-`Send` `DataPlaneRuntime`; the pair is the semantic
  equivalent of VPP's target `vlib_main_t *` plus `node_index`.
- `NodeId`, rather than `NodeHandle`, is correct here. Hammer publishes one
  graph topology and clones the same node slots to every Data Worker; additive
  graph refork preserves existing slots. Runtime validates and sizes the
  per-worker interrupt sets while workers are stopped by the existing Worker
  Barrier. Passing an unpublished worker or node after that validation is a
  Runtime invariant violation, not a recoverable packet-path `Result`.
- The operation returns neither a coalescing `bool` nor a capacity error.
  Duplicate marks collapse in the Runtime-owned interrupt bit, matching
  `clib_interrupt_set_atomic`; wakeup remains a scheduling signal rather than
  ownership or payload publication.
- Runtime keeps the worker wake registry private. Generic packet Handoff may
  use that same private wake operation after publishing a `HandoffFrame`, but
  Handoff remains its own ownership-transfer queue and is not the Session
  request channel.
- Session owns private per-worker migration-request lanes and drains the
  current lane from `session-queue`. Enqueue publishes the Session-owned
  request first; only then does Session mark the target `session-queue`
  interrupt pending. Queue publication owns payload visibility; the Runtime
  interrupt owns scheduling.
- UDP clones the old `UdpConnection` through Rust's standard `Clone` trait and
  installs the resulting target-worker UDP connection. It then submits the
  private Session migration request; UDP owns no migration lane or separate
  lifecycle state machine. VPP calls its equivalent C helper
  `udp_connection_clone_safe`; that C name is source evidence, not a Hammer
  interface to reproduce.
- The source Session Worker validates the request and publishes a bounded
  reply containing the source-owned migration facts and pending datagram. The
  target installs the clone, replaces the endpoint route by compare-current
  publication, accepts the bare Session, and sends a completion containing the
  old and new `SessionHandle` values. The old worker invokes the App migration
  callback when supported, then cleans its UDP transport and Session through
  owner-local APIs.

The Runtime interrupt set must be coalescing rather than a capacity-limited
message queue. VPP's atomic interrupt bit cannot fail because duplicate marks
collapse into one pending bit. Hammer graph publication sizes or extends the
per-worker interrupt sets under the existing Worker Barrier; the packet path
performs only an atomic mark followed by worker wake. Session's queue
publication, not the interrupt bit, makes the migration request visible to the
consumer. This is a Runtime graph-scheduling primitive, so it contains no
protocol payload and introduces no queue-full recovery category.

## Previously proposed operations are insufficient

The earlier two-operation proposal covered lookup plus handoff only:

- `publish_datagram_connection(session_id, local, remote) -> RuntimeResult<bool>`
  atomically claims the endpoint route for the Session and performs the
  existing connection-publication state transition. A duplicate live route is
  a typed Session error. Rollback removes only the route just claimed.
- `lookup_datagram_connection(transport, local, remote) -> Option<SessionHandle>`
  returns the routing identity used by UDP input. It never lends or exposes a
  foreign Session entry.

Those operations remain useful, but they cannot by themselves express VPP
`OPENED` migration, target-worker installation, old-owner cleanup, or migration
acceptance. They are not the complete #219 interface design.

## Approved Runtime interrupt surface

The user approved the VPP-shaped Runtime surface
`DataPlaneRuntime::set_worker_node_interrupt_pending(DataWorkerId, NodeId)`:
it is infallible after graph publication, atomically marks the exact target
worker/node and wakes that worker, while all Session migration requests and
payloads remain Session-owned and ordinary UDP connection state remains
UDP-owned.

## Selected owner-side asynchronous migration protocol

Issue #219 explicitly selects an owner-side asynchronous protocol. A target
worker never reads or mutates a foreign `SessionWorker`, `UdpWorker`, Session,
or UDP connection. The Runtime interrupt only wakes the worker that owns a
queue; it never carries a migration payload and it never substitutes for
queue publication.

### Source worker lifetime and quiescence

- The source Data Worker, its Session/UDP owner loops, and the source queue
  registration remain alive until the migration completes or is rejected.
  A successful claim does not permit the source Session, transport, or shared
  FIFO references to be reclaimed early. Worker shutdown must first drain or
  reject outstanding migrations at an owner-local barrier.
- The target claims a route only when the endpoint state is `Opened` and the
  current `SessionHandle` still equals the handle found by the target. The
  compare-current claim changes the shared route to `Migrating`; a stale
  handle, `Ready` route, or already-migrating route does not authorize a
  second migration.
- The source validates that exact old handle in its own `&mut SessionWorker`
  owner path before taking the migration snapshot. After the claim, no new
  datagram is enqueued into the old Session. The source retains the old
  Session, UDP transport, and FIFO ownership while it handles close/control
  events and waits for completion. A close that wins before source-side
  validation rejects the migration; a close observed after the claim follows
  the owner-local close-during-migration path.
- The source snapshot is created only by the source worker's mutable owner
  APIs. The source may clone or package Session and UDP facts for the target,
  but it never lends a foreign reference and the target never obtains a raw
  pointer or unchecked accessor.

### Request, migration-state record, and completion queues

Each Data Worker has bounded, lock-free migration lanes owned by Session. The
lanes carry move-only records with no foreign-worker references:

- A target-to-source **migration request** contains the expected old handle,
  the endpoint route identity, the target worker, and the pending datagram
  record. The request is published to the source worker after the target has
  claimed `Migrating`.
- A source-to-target **migration-state record** contains the old handle, the
  source-created Session/UDP migration facts, and the pending datagram record.
  The source creates this record while it owns the source Session/UDP state and
  publishes it only after that owner-local work is complete. The target assigns
  the new handle when it installs the state.
- A target-to-source **migration completion** contains the old and new
  handles plus a typed completion status. It tells the old owner that target
  publication and pending-datagram processing reached the cleanup point; it
  does not carry a packet buffer.

The Runtime interrupt is set only after the corresponding Session queue push
succeeds. The queue is the publication and ownership boundary; the interrupt
is only a coalesced scheduling signal. If any queue is full, `push` returns
the complete record to its sender. In particular:

- a full request queue leaves the pending buffer with the target; the claim is
  cancelled and the existing `WrongWorker` path performs the owner-local typed
  drop;
- a full migration-state record queue leaves the Session/UDP migration-state
  record and its pending buffer with the source, so the source retains the old
  owner state and retries publication without cleaning it up;
- a full completion queue leaves the completion record with the target. The
  target keeps retry state and the source remains quiescent rather than
  reclaiming the old Session early.

No queue-full path releases or transfers a buffer before successful queue
publication. Generic packet handoff uses the same failure-atomic rule: only a
successful handoff enqueue transfers buffer ownership, and every failure
leaves the buffer with the caller for retry or typed drop accounting.

### Unsupported application cases

The first implementation migrates only bare transport Sessions with no
application facts. It rejects, without partial target publication:

- `AppSession` or `SessionAppContext` values containing worker-local state
  that cannot be transferred or recreated by an owner-side migration
  callback;
- immutable application Session handles or RX queues that cannot be rebound;
- upper/lower Session relationships, external application attachments, or
  FIFO/segment ownership relationships for which the corresponding app
  worker cannot acknowledge the new handle; and
- any Session App without the migration callback required to accept the new
  owner.

QUIC's private Quinn connection context is unsupported by #219 and remains
owned by its original worker until #238 supplies the corresponding app
migration. A typed unsupported-application rejection cancels the route claim
and sends the pending datagram through generic handoff to the old owner. If
that handoff cannot be published, the target retains ownership through the
failed enqueue and performs a terminal typed drop; it never mutates the old
worker directly.

### Publication and cleanup order

The complete connected-UDP sequence is:

1. The target worker finds a foreign-owner connected UDP endpoint and retains
   ownership of the received buffer.
2. The target verifies `Opened` plus the expected old handle and claims the
   route as `Migrating`.
3. The target publishes a migration request to the source queue. A full
   queue returns the request and its buffer to the target.
4. The source processes the request through its own `&mut SessionWorker` and
   `&mut UdpWorker` owner paths, verifies the old generation, and clones or
   packages the Session and UDP connection state.
5. The source publishes a migration-state record to the target queue. Until
   that push succeeds, the source retains the migration-state record, pending
   buffer, old Session, and old transport.
6. The target accepts the migration-state record and installs its own Session and UDP
   connection. A successful queue pop transfers the migration-state record to the
   target; installation failure drops the new clone, cancels the route, and
   either hands the pending datagram back to the old owner or performs the
   caller-owned typed drop if that handoff cannot be published.
7. The target publishes the new endpoint handle only by compare-current
   replacement of the claimed old handle, and changes the shared route from
   `Migrating` to `Ready`. This is the publication commit. The target Session
   itself remains migration-blocked until application acceptance.
8. The target accepts the bare transport migration, then processes the pending
   datagram using its own Session/UDP state. The buffer is released, handed
   onward, or typed-dropped only by the target after it owns the accepted
   migration-state record.
9. The target publishes a completion record containing both the old and new
   `SessionHandle` values. The old owner is not reclaimed until this record is
   accepted by its queue.
10. The old owner invokes the migration callback with `{ old_handle,
    new_handle }`, where `old_handle` identifies the source Session and
    `new_handle` identifies the published target Session. It then removes the
    old endpoint/Session route and cleans the old UDP transport and Session
    through owner-local APIs. The target's acceptance operation clears its
    migration block and reschedules any transport I/O that was already
    pending, matching `session_migrate_accept` for the Session facts Hammer
    represents.

Any failure before step 7 leaves the old Session as the only published owner
and must cancel the `Migrating` claim. A typed rejection may route the pending
packet back through generic handoff; a queue-full or failed handoff keeps the
packet with the current caller until its typed drop. Any failure after step 7
preserves the new published owner; old-owner cleanup is retried from the
completion queue and never performed by the target through foreign access.

## Implementation staging decisions

### Rejected datagram-prefixed migration mechanism

The proposed public `DatagramSessionMigration` value and
`prepare_datagram_migration / install_datagram_migration /
abort_datagram_migration / accept_datagram_migration` surface are withdrawn.
They incorrectly propagated the datagram-specific trigger into the generic
Session migration mechanism and exposed Rust implementation staging as domain
operations.

VPP's ownership split is narrower:

- `session_dgram_connect_notify` is the only datagram-specific Session entry
  point. UDP calls it after cloning the UDP transport connection.
- `session_program_thread_migration` is generic Session scheduling. Its request
  contains only `{ old_handle, new_handle }`.
- `session_switch_pool` is generic old-owner Session/App notification and
  cleanup dispatch.
- `session_migrate_accept` is the generic Session completion point used by a
  Session App or the external Application path.

Hammer retains those semantic names and scopes while keeping the bounded
cross-worker records private to the Session Runtime. The implemented seams are:

```rust
SessionWorker::program_thread_migration(
    runtime: &DataPlaneRuntime,
    target_worker: DataWorkerId,
    old_handle: SessionHandle,
    transport: SessionTransportId,
    local: SocketAddr,
    remote: SocketAddr,
    dgram: SessionDgramArgs,
) -> SessionMigrateResult

SessionWorker::migration_snapshot(session: SessionId)
    -> Option<SessionMigrationState>
SessionWorker::install_migrated_session(
    state: SessionMigrationState,
    transport_index: Index,
) -> RuntimeResult<(SessionId, SessionHandle)>
SessionWorker::accept_migrated_session(session: SessionId)
    -> RuntimeResult<()>
```

`SessionSwitchPoolArgs`, `SessionSwitchPoolReply`, and
`SessionSwitchPoolCompletion` are private transport-migration records. Unlike
VPP's generic `session_switch_pool_args_t`, the request/reply records also carry
the endpoint identity and the pending datagram because Hammer's bounded
asynchronous queue must transfer the first packet without a foreign-worker
reference. They do not carry QUIC context or Runtime wake state. The source
creates the migration snapshot through its owner-local Session APIs; the target
installs shared FIFO references, publishes the endpoint by compare-current
replacement, accepts the bare transport Session, and hands the pending packet
to the target Session. Completion invokes the old/new-handle App callback when
supported and then performs owner-local cleanup. The current #219 policy rejects
nonzero Session App contexts, external AppSession state, and Session relations,
so the callback remains pre-wired for a later safe rebind path.

### Standard Rust clone decision

Hammer will use the standard `Clone` trait. It will not add a `clone_safe`
trait, method, free function, callback, carrier, wrapper, or parallel clone
protocol. VPP's `session_clone_safe` and `udp_connection_clone_safe` names are
C implementation names; reproducing those names in Rust would confuse the
lifetime protocol with the value-copy operation.

- `UdpConnection` already implements `Clone` and `Copy`. The migration path
  uses `let mut new_connection = old_connection.clone();`, installs it in the
  arrival worker's UDP connection pool, and then rewrites the target-worker
  identity, new pool index/Session identity, and connection-local migration
  flags required by the UDP owner.
- `SessionEntry<Index>` will implement `Clone`. The Session path uses
  `let mut new_session = old_session.clone();`, inserts it in the arrival
  worker's Session pool, and then rewrites the target transport index and
  Session lifecycle facts to `READY + IS_MIGRATING + !RX_READY` semantics.
- Cloning a `SessionEntry` follows ordinary field semantics: `Arc<Fifo>` keeps
  the same FIFO allocations alive by increasing their reference counts,
  `String` and other owned values clone normally, and `Copy` identity/state
  fields are copied by value. There is no raw byte copy, `ptr::read`, manually
  incremented `Arc`, or second immutable migration-facts record.
- `Clone::clone` itself is safe Rust and contains no migration policy. A
  Session-owned claim/lifetime protocol must separately ensure that the exact
  old generation remains live and is not mutated, removed, or dropped while
  the source reference is used. A close that wins before the claim prevents
  migration; a close after the claim is completed by `switch_pool`, matching
  VPP's close-during-migration branch.
- If the final source-acquisition design needs `unsafe`, the unsafe block may
  only establish a temporary source reference after the Session claim and
  aliasing protocol have been proven. The actual copy remains
  `old_session.clone()` or `old_connection.clone()`. Stable `Pool` addresses
  alone are not lifetime or aliasing proof, because the old worker may hold an
  exclusive `&mut SessionWorker` or `&mut UdpWorker`.

The current `ThreadOwned` API intentionally prevents foreign worker access.
The source-acquisition/old-owner quiescence protocol is implemented by the
Session-owned bounded request/reply/completion lanes: the source creates the
snapshot through its own mutable worker APIs, and the target receives only
owned migration records. No generic foreign accessor is added to `ThreadOwned`,
no raw pointer into a worker pool is cached in a lookup or another module, and
no duplicate immutable Session model is published merely to make cloning
convenient.

No migration business logic may be added to a basic data structure:

- `hammer_infra::Pool` remains a fixed-capacity generation-bearing owner of
  generic values. It gains no migration phase, peeker, Session identity, UDP
  identity, callback, or foreign-access operation.
- `hammer_infra::thread_owned::ThreadOwned` remains owner-thread-only. It gains
  no unchecked getter, shared snapshot, migration exception, lock, or weaker
  `Send`/`Sync` contract.
- `hammer-runtime` owns only the approved worker/node interrupt scheduling
  primitive. It stores no migration state or transport/App fact.
- Session migration state and its source-lifetime proof live only in
  `hammer-service::session`. UDP retains only its ordinary worker-local
  connection pool, standard `Clone` on `UdpConnection`, and the
  connection-local migrated cleanup marker already required by VPP semantics.
- If endpoint publication later proves that `Bihash` lacks one required
  operation, the only admissible infrastructure addition is a generic
  compare-old-value replacement/removal primitive. That possible primitive is
  a separate approval decision and may not contain Session, transport, worker,
  or migration concepts.

The standard `Clone` decision is confirmed. The public Session migration
surface is the existing `program_thread_migration`, snapshot/install/accept,
completion, and owner-cleanup operations described above. The implementation
keeps the source lifetime proof in Session-owned bounded lanes, passes old/new
handles to the pre-wired migration callback, and deliberately defers external
Application and QUIC context rebinding to later issues.
