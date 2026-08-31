# VPP Synchronization Evidence Map

Use these vendored sources as the semantic reference before changing Hammer
synchronization. Read the callers around each declaration.

## Worker barrier

- `third_party/vpp/src/vlib/threads.h:64-120`: barrier state and worker
  acknowledgement contract.
- `third_party/vpp/src/vlib/threads.c:1280-1450`: sync/release recursion,
  worker wait, and deadlock handling.

## Locks, atomics, and fences

- `third_party/vpp/src/vppinfra/lock.h:80-220`: acquire/relaxed spinlock and
  reader-preferring rwlock behavior.
- `third_party/vpp/src/vppinfra/atomics.h:1-80`: acquire/release atomics and
  fence helpers.
- `third_party/vpp/src/vppinfra/clib.h:145-170`: compiler, store, and full
  memory barriers; these do not transfer ownership.

## Publication and ownership

- `third_party/vpp/src/vlib/node.c:150-210,680-725`: graph mutation under the
  worker barrier.
- `third_party/vpp/src/vnet/adj/adj.c:62-108,265-310`: barrier-protected pool
  growth and publication.
- `third_party/vpp/src/vppinfra/bihash_template.h:250-300,390-445`:
  lock/publication ordering for shared buckets.

## Hammer counterparts

- `crates/hammer-runtime/src/barrier.rs`: `WorkerBarrier` acknowledgement
  boundary and deadlock behavior.
- `crates/hammer-runtime/src/sync.rs` and
  `crates/hammer-infra/src/sync.rs`: project-owned spin/rw locks and fences.
- `crates/hammer-infra/src/thread_owned.rs`: indexed worker ownership without
  `thread_local!` or a shared lock.
- `crates/hammer-runtime/src/global_main/control.rs`: main-thread and barrier
  preconditions for control-plane mutation.

For every state, record owner, readers/writers, publication release/acquire,
reclamation proof, and queue/allocation/cancellation recovery. Treat relaxed
atomics as scalar accounting only; they do not publish object fields.
