# Vendored VPP Source Map

This map is the minimum evidence set for IP/FIB and synchronization changes.
Search these files locally before using any external source.

## IP Ownership and Initialization

- `third_party/vpp/src/vnet/ip/ip.h:36-115`: `ip_main_t` protocol and port
  registries plus accessors.
- `third_party/vpp/src/vnet/ip/ip_init.c:13-82`: `ip_main_init` initializes
  those registries; it does not create lookup graph nodes.
- `third_party/vpp/src/vnet/ip/lookup.h:104-146`: `ip_lookup_main_t` local-next
  mapping and `ip_lookup_set_buffer_fib_index`.
- `third_party/vpp/src/vnet/ip/ip4.h:75-139`: `ip4_main_t`, FIB pool, and
  `fib_index_by_sw_if_index`.
- `third_party/vpp/src/vnet/ip/ip6.h:83-120`: `ip6_main_t`, FIB pool, and
  `fib_index_by_sw_if_index`.
- `third_party/vpp/src/vnet/ip/ip4_forward.c:66-78,1075-1140`: static
  `ip4-lookup` node and init/default FIB creation.
- `third_party/vpp/src/vnet/ip/ip6_forward.c:698-718,2755-2799`: static
  `ip6-lookup` node and init/default FIB creation.

## Forwarding and DPO Order

- `third_party/vpp/src/vnet/ip/ip4_forward.c:115-170`: load-balance node uses
  the existing flow hash or computes it from the selected load-balance config;
  nested levels shift the hash to avoid polarization.
- `third_party/vpp/src/vnet/ip/ip4_forward.c:383-427`: destination lookup,
  selected load-balance config, flow hash, bucket, and DPO next/index order.
- `third_party/vpp/src/vnet/ip/ip6_forward.c:730-820`: IPv6 equivalent,
  including per-packet FIB selection and DPO next/index publication.
- `third_party/vpp/src/vnet/dpo/load_balance.c:229-263`: default flow-hash
  configuration and power-of-two bucket mask invariant.
- `third_party/vpp/src/vnet/dpo/lookup_dpo.c:392-415`: lookup DPO computes
  hash after LPM and does not reuse one hash blindly at every recursion level.
- `third_party/vpp/src/vnet/ip/ip4_inlines.h:20-76`: IPv4 hash inputs and
  router-id mixing.
- `third_party/vpp/src/vnet/ip/ip6_inlines.h:35-104`: IPv6 hash inputs,
  flow-label and GTPv1 TEID handling.

## Synchronization

- `third_party/vpp/src/vlib/threads.h:64-120` and
  `third_party/vpp/src/vlib/threads.c:1280-1450`: worker barrier state,
  recursive sync/release, worker acknowledgement, and deadlock handling.
- `third_party/vpp/src/vppinfra/lock.h:80-220`: acquire/relaxed spinlock and
  reader-preferring rwlock semantics.
- `third_party/vpp/src/vppinfra/atomics.h:1-80`: acquire/release atomic
  helpers and fence semantics.
- `third_party/vpp/src/vppinfra/clib.h:145-170`: compiler, full-memory, and
  store barriers; these are not ownership mechanisms.
- `third_party/vpp/src/vnet/adj/adj.c:62-108,265-310`: barrier before pool or
  counter expansion and release after publication.
- `third_party/vpp/src/vppinfra/bihash_template.h:250-300,390-445`: lock and
  acquire/release publication for shared hash buckets.
- `third_party/vpp/src/vlib/node.c:150-210,680-725`: graph mutation guarded by
  worker barrier.

## Hammer Counterparts

- `crates/hammer-runtime/src/barrier.rs`: `WorkerBarrier` and its acknowledgement
  boundary; timeout aborts on a worker deadlock.
- `crates/hammer-runtime/src/sync.rs` and
  `crates/hammer-infra/src/sync.rs`: cache-line-isolated spin/rw locks and
  explicit fence helpers.
- `crates/hammer-infra/src/thread_owned.rs`: indexed worker ownership without
  `thread_local!`; it is not a shared lock.
- `crates/hammer-runtime/src/global_main/control.rs`: existing control-plane
  checks for GlobalMain and worker-barrier requirements.
- `crates/hammer-plugins/ip/src/lookup/mod.rs:149-473`: current Hammer FIB
  contribution owner and the architectural mismatch that must be resolved in
  a design issue before broad API migration.
