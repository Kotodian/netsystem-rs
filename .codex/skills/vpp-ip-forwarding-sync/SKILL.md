---
name: vpp-ip-forwarding-sync
description: Align Hammer IP/FIB lookup, node registration, and synchronization design with vendored VPP ownership. Use when changing IpMain, IP lookup or rewrite nodes, FIB tables, interface-to-FIB selection, DPO/load-balance lookup, WorkerBarrier, locks, atomics, or publication of worker-visible forwarding state.
---

# VPP IP/FIB and Synchronization Alignment

Use this skill before changing the IP plugin or its worker-visible state. The
goal is semantic alignment with VPP, not a 1:1 C-to-Rust rename. Start with
ownership and execution order; only then choose an API or primitive.

## VPP Ownership Map

The following objects are distinct in VPP and must not be collapsed into one
Hammer "control plane" object:

- `ip_main_t` (`third_party/vpp/src/vnet/ip/ip.h:36-98`) owns IP protocol and
  TCP/UDP port registries. It does not own FIBs or graph-node registration.
- `ip_lookup_main_t` (`third_party/vpp/src/vnet/ip/lookup.h:104-129`) owns the
  protocol-to-local-next tables used by local delivery. It does not own a FIB
  pool or an application-created node object.
- `ip4_main_t` and `ip6_main_t` (`third_party/vpp/src/vnet/ip/ip4.h:75-134`,
  `ip6.h:83-120`) own FIB pools and the `fib_index_by_sw_if_index` mapping.
  The mapping is not the RX interface index itself.
- `ip4_lookup_node` and `ip6_lookup_node` are statically registered VLIB
  nodes (`ip4_forward.c:66-78`, `ip6_forward.c:698-718`). Their init functions
  initialize the owning main structures and default FIB, but a main structure
  does not register itself as a node.
- `ip_lookup_set_buffer_fib_index` (`third_party/vpp/src/vnet/ip/lookup.h:134-144`)
  first maps RX `sw_if_index` through the per-family table and then applies a
  TX override. Never use `sw_if_index` as a FIB index without an explicit map.

In the current Hammer checkout, `IpMain` at
`crates/hammer-plugins/ip/src/lookup/mod.rs:149-473` combines FIB source
contributions and a single `FibTableHandle`. That is a current implementation
fact, not proof that it is the VPP `ip_main_t` object. `IpLookupControlPlane`
is also a Hammer-specific aggregate. Do not preserve or extend a target that
merely removes `control_handle` while retaining the same aggregate and an
optional embedded barrier; that target still violates the VPP ownership map.

## Correct Design Constraints

For an issue or implementation plan, state the existing Hammer owner first and
map it to the VPP owner explicitly:

1. Keep protocol/port registration in the IP plugin's protocol owner.
2. Keep FIB source contributions and table storage in the forwarding/FIB owner.
3. Keep node registration in the existing graph-registration callback. The FIB
   owner must not call `runtime.nodes()` as a side effect of being a data
   object.
4. Treat interface-to-FIB selection as a separate mapping problem. If Hammer
   still has one table, say so and preserve that contract; do not invent a
   `FibTableIndex`, central registry, or use `sw_if_index` as a table id.
5. Preserve `NetworkOpaque` as the packet ABI overlay. It carries RX/TX
   interface facts; it is not a FIB registry and must not gain a fake wrapper.
6. Do not add a new abstraction merely to make the names resemble VPP. Any
   proposed new public type or method needs an owner, consumers, and an
   approval record.

## VPP Lookup Order and Hashing

VPP's forwarding path performs table selection and destination LPM before
selecting a load-balance bucket. See `ip4_forward.c:383-415`,
`ip6_forward.c:760-805`, and `dpo/lookup_dpo.c:392-415`.

The flow hash is computed with the selected load-balance's configuration, and
the VPP inputs are read from the packet (`ip4_inlines.h:20-76`,
`ip6_inlines.h:35-104`). Those inputs include addresses, ports, protocol,
reverse/symmetric flags, IPv6 flow label, GTPv1 TEID, and router id. In Hammer,
IP input runs before the later transport metadata validation in `ip/local.rs`.
Do not add `source_port`/`flow_label` fields to `ParsedIpPacket` or change the
hash API until the packet-byte access point, fragment/extension-header rules,
and ownership of the computed hash are specified. A hash calculated before LPM
and reused for every nested load-balance is not VPP semantics.

## Synchronization and Publication

Use the VPP synchronization counterparts listed in
`references/vpp-ip-sync-map.md` and record these facts for every changed state:

- owner (worker, GlobalMain/control thread, barrier-owned value, or shared
  infrastructure);
- all readers and writers, including cleanup and RPC paths;
- publication event and matching acquire/release operation;
- lifetime/reclamation proof, which is separate from visibility;
- behavior on queue full, stale generation, allocation failure, cancellation,
  and worker exit.

Choose primitives in this order:

1. Worker-owned state with `&T`/`&mut T`.
2. Existing `WorkerBarrier` for main-thread mutation of worker-visible graph,
   topology, registry, or FIB snapshots. Mutate directly during the
   acknowledgement phase; do not wrap the barrier-owned value in a second
   mutex/lock or store an optional barrier in each owner object.
3. Atomics for independent scalar counters, flags, sequence numbers, and queue
   indices. Relaxed ordering is not publication; object publication requires a
   release/acquire pair.
4. `hammer_runtime::sync::SpinLock` only for short bounded non-blocking work;
   `RwLock` only for short read-dominant state with accepted reader preference.
5. Bounded handoff queues or owner-worker RPC when ownership moves. A foreign
   worker must not mutate or free a worker-local pool entry directly.

Never use `thread_local!`, a packet-hot-path mutex, an atomic pointer as an
ownership model, or a lock around `WorkerBarrier` to make an invalid access look
safe. Compiler/hardware fences are allowed only with a documented low-level
publication/device protocol and named matching operation.

## Review Checklist

Reject a design when it:

- treats `IpMain` as a VPP node registrar;
- equates `sw_if_index` with `fib_index`;
- stores a per-object optional barrier or a second completion protocol around
  the runtime barrier;
- calculates flow hash before the selected LPM/load-balance configuration;
- moves transport parsing into IP input solely to satisfy a hash API;
- adds a generic index wrapper/registry without a real owner and consumer;
- publishes worker-visible FIB/graph state without a barrier or release/acquire
  proof; or
- frees worker-owned state from a foreign worker.

Use focused behavior tests for node registration, interface-to-table policy,
post-LPM load-balance selection, barrier acknowledgement, publication
visibility, and ownership-preserving cleanup. Do not use source-text matching
as a behavioral test.
