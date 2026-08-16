# Hammer Stats Segment follows VPP's vlib stats segment with a single Main Thread owner

Status: accepted

Hammer exposes named measurements through `hammer-stats`, a stats segment modeled on VPP's vlib stats segment (`third_party/vpp/src/vlib/stats/`). `StatsMain` installs a fixed-capacity, page-reserved shared mapping on the Main Thread and owns its structure for the engine's lifetime; domain owners publish values through `Counter`/`Gauge` handles, and external tools read the directory and live values through the Binary API without touching the data plane.

The segment keeps the VPP layout — header first, then directory, then per-metric value blocks — while replacing VPP's lock-based structural protocol with a single-owner atomic protocol and retiring entries by generation instead of raw slot reuse. Every structural decision below is grounded in the vendored VPP sources.

## Layout: header first, then directory, then value blocks

VPP places the shared header first in the shared memory segment and carries the directory pointer in it (`shared.h:42-52`, `vlib_stats_shared_header_t`: version, base, epoch, in_progress, directory_vector). Hammer's `StatsHeader` occupies the first page the same way, followed by a directory of fixed-size slots and per-metric descriptor and value blocks. Unlike VPP, whose segment is a growing mapping managed by `vlib_stats_main_t`, Hammer's mapping has a fixed capacity decided at install: `StatsConfig` carries the capacity, and install fails with a typed `StatsError` (`CapacityTooSmall`) when it is below the minimum (hammer-service/stats.rs). The fixed capacity keeps every slot offset stable for the segment's lifetime and lets the directory be rebuilt from the header alone.

VPP entries pack a type tag, a union (`index1`/`index2`, `index`, `value`, `data`, or `string_vector`), and a 128-byte name into a 144-byte `vlib_stats_entry_t` (`shared.h:21-40`). Hammer keeps the same inline 128-byte NUL-terminated name field (`ENTRY_NAME_LEN = 128`, directory.rs) but splits VPP's union into explicit `link` and `offset` fields so the directory remains relocatable: a slot either links to another slot or addresses its descriptor and value blocks at mapping-relative `Offset`s. All value addressing inside the segment is mapping-relative; a process that maps the segment elsewhere adds its own base, exactly the role VPP's `base` pointer plays for `stat_client.c`.

## The process-local name hash rebuilds from the directory

VPP keeps `directory_vector_by_name`, a hash from entry name to directory index, inside `vlib_stats_main_t` (`stats.c:78, 123, 196, 570, 578`): it is per-process and not part of the shared segment. Hammer mirrors this with `StatsMain.names: HashMap<Box<str>, EntryId>`, rebuilt from the directory at install. The hash is lookup acceleration, never an ownership record; the directory remains the source of truth.

## Metric values live in the segment and are owned by their domains

A `Counter`/`Gauge` handle holds its `EntryId` and a direct `value_offset` into the segment, so the hot update path is a generation-checked store with no directory re-lookup. Value records are cache-line-aligned atomics inside the segment (stats.rs handles; directory.rs `value_offset`). `PrometheusType`, `fq_name`, `help`, and `const_labels` are stored per metric, but Hammer publishes definitions only: there is no exporter and no scraping server, and no definition-derived state to keep warm. The Prometheus descriptor is a stable, externally readable attribute of the entry, not a live collection path.

## Retirement uses generations, not raw slot reuse

VPP reuses directory slots on removal and hands clients raw indices, so a stale client reference can silently alias a newer metric. Hammer's `EntryId` pairs a slot index with a per-slot generation; `StatsError` reports `NotFound` and `StaleEntry` with the full id, and every handle and IPC path revalidates the generation. This is an intentional difference from VPP, at the cost of one u64 per slot; it is what makes the checked IPC wire (`hammer-ipc::stats`) safe against concurrent list/dump races.

## One structural owner: the Main Thread, via ThreadOwned

VPP's segment is written by every thread that updates stats; structural changes take a `clib_spinlock` plus an `in_progress`/epoch protocol (`stats.c:12-54`). Hammer has exactly one structural writer: the Main Thread installs `StatsMain` through `ThreadOwned`, and every directory, descriptor, and retirement mutation runs there, enforced by `with_mut`. Because there is a single owner, Hammer documents omitting VPP's structural spinlock entirely: the remaining readers coordinate with the same `in_progress`/epoch bracket VPP uses, implemented with atomics instead of a lock (stats.rs list/dump). The orderings recreate the two boundaries the omitted spinlock supplies. A read snapshots the epoch and the marker with acquire loads, copies the directory or values while `in_progress == 0`, and rechecks both after an acquire fence; a structural write marks `in_progress` with a seq_cst store — the begin boundary, standing in for VPP's spinlock-ordered plain `in_progress = 1` (stats.c:26-27) so no structural write can become visible before the marker — performs its prevalidated infallible writes, bumps the epoch, and clears `in_progress` with a release store, the analogue of VPP's `__atomic_store_n (&in_progress, 0, __ATOMIC_RELEASE)` (stats.c:49), so the clear publishes the structural writes and the bumped epoch to a reader whose re-check acquires it. Value stores from domain handles are per-record atomics and never take the bracket.

## Collection is a Main Thread Process Node

VPP's `stat_segment_collector_process` sets `/sys/boottime` once, then loops: run every registered collector, bump `STAT_COUNTER_HEARTBEAT`, and suspend for the update interval (`collector.c:132-181`; system scalars in `stats.h:22-24`). Hammer's `stats_collector` Process Node follows the same shape on the main-thread LocalSet: the capability installs on the Main Thread, the first pass records boottime, and each pass runs the registered collectors and bumps the heartbeat. `update_interval` comes from `StatsConfig`. Collector errors are logged and the pass continues; only ThreadOwned failures terminate the node.

## list and dump are transient, non-collecting reads

`hammerctl stats list` and `stats dump` are thin clients of the typed Binary API wire (`hammer-ipc::stats`). The daemon-side methods copy entries and values from the segment at the moment of the call: they do not trigger collection, do not scan domain-owner state, do not create a snapshot, and do not replace any persistent data. They are registered with the `mp_safe` marker and therefore never fetch the worker barrier nor finish deferred graph updates (ADR 0039), matching VPP's `is_mp_safe` dispatch (`msg_handler_internal` takes the barrier only when `!m->is_mp_safe`, api_shared.c:545, 564). The CLI prints a stable, tab-separated, headerless record stream and formats only, keeping the tool stateless between invocations.

## Intentional differences from VPP

Fixed-capacity mapping instead of VPP's growing segment; single-owner atomic `in_progress`/epoch protocol instead of the structural spinlock; generation-based retirement instead of raw slot reuse; typed prost wire instead of raw segment pointers for external reads; Prometheus attributes as definition-only metadata instead of a built-in exporter plugin. Each difference exists because Hammer's runtime has exactly one structural owner where VPP has many threads, and every one is documented with its VPP counterpart above.
