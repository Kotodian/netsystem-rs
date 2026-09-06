# DPO framework alignment review

## Scope and verdict

Verdict: **Needs changes**. This is a static review of the current worktree,
including the uncommitted operation-registration draft, not a claim that the
draft compiles or that a complete implementation exists.

The review below records the baseline. The subsequent user instruction approves
fixing every finding, committing by owning module, pushing, and cleaning the
workspace target directory after verification. Findings are not resolved merely
because an implementation edit exists.

## Implementation ledger

Active objective correction: delivery now targets ICMP alignment against ADR
and vendored VPP, with VPP-derived tests, module commits, push and target
cleanup. Other DPO findings remain recorded but are not implementation scope
unless required by that ICMP chain. ADR/CONTEXT now place error generation in
IP and only local input/type dispatch/echo/received PMTU parsing in ICMP.

ICMP source-selection investigation: `vnet/ip/ip_sas.c:64-145` selects the
longest common-prefix source on the offending interface, follows unnumbered
address ownership and uses the interface link-local source for IPv6 link-local
destinations. It is not "first matching family in a snapshot". Current
`InterfaceMain::interface_addresses` allocates a Vec. The original
`add_address` appended before validating the interface; `remove_address`
shifted the shared array without repairing `SwInterface.addresses`.
The user approved `interface_address(index) -> Option<IpNet>` and correction
of the address index lifecycle. Commit `78415978` replaces the address Vec
with the existing Pool, validates the software interface before insertion,
removes the exact interface-list entry with `position`/`remove`, and reclaims
only the removed interface's addresses. Surviving indices remain stable;
released slots can be reused without generation or a reference wrapper.
Independent review found no new blocker in this bounded scope; formatting,
diff checks and `cargo test -p hammer-service --lib --tests` passed (5 tests).
The new scenario proves storage lifetime, not route or ICMP graph behavior.
No address-source cache or per-packet Vec fallback is approved; the existing
InterfaceMain barrier/UnsafeCell contract has not been redesigned by this fix.

ICMP verification anchors: `test_ip4.py::TestICMPEcho::test_icmp_echo` checks
swapped addresses, echo reply type, ID, sequence and unchanged payload;
`test_ip6.py::TestICMPv6Echo::test_icmpv6_echo` checks both global and link-local
destinations. Error generation additionally must exercise the actual graph's
new/original buffer disposition and 576/1280 truncation, not merely a builder
return value. VPP's suppression primitive is `vnet/util/throttle.h`; its
per-worker bitmap is 512 bits, with error-node periods 1e-5 (IPv4) and 1e-3
(IPv6). No matching Hammer primitive was found. Its proposed owner is
`hammer-service::net`, corresponding to VPP's `vnet/util`, not infra;
IP supplies address keys and periods, while each worker owns its mutable
suppression state. The user subsequently approved this service-owned
interface. The current implementation adds only `net::throttle::Throttle`
with `Bitmap`, seed, last reset and period; `new(Duration)`, `seed(Duration)`
and `check(u64, u64)` implement construction, per-frame interval refresh and
per-packet approximate suppression. It reuses infra's word hash: bucket
collisions need not be bit-identical to VPP's `clib_xxhash`, but finite bitmap,
strict interval expiration and repeated-key suppression semantics match.
The primitive has no IP fields, locks, worker registry or global state.
Two tests cover interval/worker isolation and collision/reset behavior;
these are derived from `throttle.h` behavior, not ICMP packet test substitutes.
IP error-node ownership, initialization and consumption are still outstanding.

Delivery checkpoint, 2026-09-06: the user explicitly requested committing
the current non-ICMP changes before the ICMP migration. The current batch
passed `cargo fmt --all -- --check`, `git diff --check`, and
`cargo test -p hammer-infra -p hammer-component-macros -p hammer-runtime
-p hammer-service -p hammer-plugin-ip --lib --tests` (10 tests passed).
Module commits are `bbfe9365` (infra), `35b2ca62` (macros), `acc8fcb8`
(runtime), `0cdd71e2` (service), and `fe3b536b` (IP). Earlier statements below
about tests or commits not having occurred describe the historical checkpoints.
This delivery does not close the remaining production-integration findings.
ICMP-error migration is deferred: the latest explicit decision assigns error
generation to IP and only local ICMP handling to ICMP. The proposed
`IpErrorRequest` boundary below is superseded and is not implementation authority.

O07 producer/consumer boundary investigation (project facts): IP input selects
`icmp-error` but writes only its node-error/cursor facts, not an ICMP request.
`IcmpErrorOpaque` is private in the ICMP plugin; UDP writes a duplicate packed
layout through `port_unreachable_metadata`, and TCP has an unused duplicate
declaration. IP lookup also uses the start of `SecondaryOpaque` for forwarding
metadata. Thus ADR's instruction to write the "existing packet error metadata"
does not identify a legal shared owner-defined API. Reverse-depending on ICMP,
copying its private bit packing into null/PMTU, or overlaying a Rust enum onto
arbitrary forwarding bytes would not close O07 safely.

Proposed boundary, awaiting explicit new-API approval: an IP-owned
`IpErrorRequest` with `TimeExceeded`, `DestinationUnreachable`,
`AdministrativelyProhibited`, `PortUnreachable`, and `PacketTooBig { mtu: u32 }`.
It describes the requested IP error, not ICMP wire fields, a DPO class or a
family selector. Its concrete `write(self, &mut Buffer)` and
`take(&mut Buffer) -> Option<Self>` operations own packet metadata encoding and
clearing; no borrowed-access callback or reference wrapper is involved. ICMP
reads this through its existing dependency on IP and maps the request plus the
packet's IP version to wire type/code/data. IP input/null/PMTU and UDP migrate
together; the old ICMP/UDP/TCP private overlay declarations and UDP bit-packing
helper are removed. ICMP's wire builder remains ICMP-owned. Packet metadata
placement/initialization must be checked against lookup/reassembly/transport
stage ownership before implementation. No implementation authority is inferred
from this proposed row; it records the missing boundary needed for the report.

Bucket storage boundary correction (explicit user request): removed the
DPO-local allocation, release and layout helpers. Both LB and replicate now
use infra's `heap_boxed::allocate` / `deallocate` with cache-line base alignment.
These new Main-Heap-only entry points reuse private `allocate_in`/`deallocate_in`.
The attempted direct cross-crate `Heap` import failed compilation and was
removed: `Heap`, `Slice`, and alternative-heap operations remain private.
Ring/bihash callers retain their existing representation. No new allocator,
owning wrapper or DPO business primitive is added to infra. DPO count/pointer layout and class-child
retirement order are unchanged. The existing 4/8/1/8/4 owner scenario remains
the behavioral gate; no allocator-getter test is substituted for it.
`cargo check -p hammer-infra -p hammer-service -p hammer-plugin-ip --all-targets
--message-format=short` passes after the visibility correction. Independent
read-only review found no new storage/provenance/layout blocker in this slice.
`git diff --check` passes; tests remain reserved for the final pre-commit gate.

uRPF continuation (explicit user approval): added the FIB-owned immutable
`FibUrpfList` pool, concrete create/lock/unlock and standard `Ref` queries,
path-list bake/Drop and LB association/retirement. Bucket replacement acquires
the retained list before displacing the old LB. Association clearing and bucket
replacement preflight mutable list access before changing graph/owning fields.
User-rejected callback borrow APIs `with_urpf_list` and `with_load_balance`
were deleted, not renamed; worker queries expose only concrete scalar facts.
IP local now consumes source lookup/list size with the inspected IPv4 broadcast
and IPv6 ICMP/link-local exceptions. The existing FIB scenario covers shared
lists, rebake, per-LB retention, membership and final withdrawal. Service/IP
all-target compilation passed before the latest borrow-preflight adjustment;
tests have not run. Production path resolution/back-walk, mutable IP table
publication and live local-node execution still lack closure; these changes
do not prove full O01/F07/F14 completion. Independent read-only review returned
**Needs changes**: all bake/association callers are inside the IP FIB test,
while local consumes the list unconditionally for non-exempt packets; prefix
loose-uRPF exemption is also absent from production entry construction. Borrow
preflight order was reviewed without a new finding in that bounded scope.

Source correction after reading `vnet/fib/fib_path.c:2285-2362`: uRPF is
**not** a projection of only forwarding-resolved paths. VPP includes unresolved
attached paths and non-looped recursive paths with a via-entry. Receive and
interface-RX paths do not automatically contribute their interface. The old
resolved-only Rust comment and ADR/glossary definition were incorrect and are
replaced. `FibPathList::bake_urpf` now leaves no list under `NO_URPF`, matching
the allocation/resolve sequence in `fib_path_list.c:535-585`, instead of
allocating an empty pool element. It releases a previous owned reference
before clearing its index, so a rejected control-plane borrow does not lose
the ownership record. No additional public type/API is introduced. Independent
read-only review found no new blocker in this correction; the production and
prefix-exemption blockers remain. ADR's review table, PMTU inventory and final
decision list also remove obsolete prohibitions on the explicitly approved
class unlock dispatch. This corrects contract contradictions, not proof of
PMTU lifecycle implementation.

The remaining uRPF migration closure is concrete and inseparable from F07:

| Requirement / evidence class | Current symbol/caller | Required owner change | Completion evidence |
| --- | --- | --- | --- |
| Explicit approval: production, consumption and reclamation together | `FibPathList::bake_urpf`; only calls in IP `fib.rs` tests | Actual path-owner contribution and path-state rebuild call bake; never infer interfaces from normalized LB buckets | Production path calls plus route/neighbor/state-change execution |
| Vendored fact: resolved and unresolved path contributions differ by path role | `FibPath` stores configuration; production contribution is absent | Attached paths contribute interfaces; non-looped recursive paths combine via-entry lists; special paths query their owner; receive/RX do not synthesize contributions | Corresponding FIB scenarios, including incomplete adjacency, not a hand-built list |
| Vendored fact: entry association and exemption are entry-local | `set_load_balance_urpf`; only test calls | Source forwarding construction installs the path-list association; exemption with an empty set creates a separate local0 list and drops the construction reference after LB acquisition | `fib_test.c:3974-3996`: exempt prefix is drop with local0 while its default cover remains empty; withdrawal reclaims references |
| Issue requirement: dispatcher-owned publication | `Ip4Main`/`Ip6Main` immutable empty tables; no route Binary API handler | Existing ADR route API contract invokes the IP path/FIB owner under dispatcher barrier; backend LPM is updated only after valid forwarding construction | Actual Binary API route request visible to Data Workers, then replacement and withdrawal |
| Project fact: local is already a consumer | `local.rs` source LPM and list-size rejection | Retain source validation; complete producers rather than bypassing the check | Actual local packets after route publication, IPv4 spoof/broadcast and IPv6 exceptions |

The existing test's manually supplied hardware-interface list is evidence only
for the approved list sharing/lifetime seam. Its LB uses interface-RX DPOs;
that does **not** make it evidence that interface-RX paths contribute uRPF.
No test, commit, push or target cleanup is authorized as completed by these
static checks. F01-F14/O01-O07 remain in the full delivery scope.

O01 normalization draft: `LoadBalancePath { dpo, path_index, weight }` is now
the concrete LB owner's input to `LoadBalanceDpo::new` and
`NetMain::update_load_balance`, replacing pre-expanded `&[DpoId]` arguments.
No registry operation or generated CRUD is added. Construction performs path
ordering, all-zero fallback, power-of-two normalization and sticky live-path
redistribution; an empty set becomes a drop bucket. Update stacks the complete
replacement before acquiring children and swapping the pool value. All current
callers migrate together. The existing IP FIB scenario adds vendored
`fib_test_sticky` equal/unequal-cost bucket sequences, failure/recovery and empty
path replacement. These are resolved-path consumer tests, not adjacency/BFD
back-walk execution evidence. Actual vendored tolerance is **0.1**
(`load_balance.c:23`), not the 1% stated by its comment or the earlier handoff.
Service/IP all-target compilation passes; no tests run. Independent review
found and corrected an empty-prefix bug at the bucket cap: normalization now
rejects an empty retained prefix with existing `InvalidBucketCount` before
graph/reference changes, rather than assigning all traffic to a zero-weight
or tiny-weight path. The correction and route/bucket/pool preservation
assertions passed a second read-only review. This bounded review is not
execution evidence. Map sharing/translation/path-state propagation, uRPF lifetime and
to/via counters remain open, so O01 is not complete.

Storage-lifetime continuation: the existing IP FIB source-lifetime scenario
now installs a route over eight distinct, interface-owned RX DPOs and replaces
its load-balance buckets through 4/8/1/8/4 counts. It checks the selected root,
each ordered bucket, retention of the four remaining children after releasing
construction references, and both owner pool baselines after route withdrawal.
This extends the route/bucket/withdrawal scenario in vendored
`src/plugins/unittest/fib_test.c:774-834`; it does not claim that preselected
Hammer buckets implement VPP path normalization. `cargo check -p
hammer-plugin-ip --all-targets --message-format=short` passes. Test execution,
replicate storage transitions, weighted paths/maps/uRPF, and live graph
execution remain open; O01/O02/F14 are not marked resolved by this check.

Registration correction: the builtin distinction and range error are removed.
One `DpoMain::register(Option<DpoType>, ...) -> Result<DpoType, DpoError>`
implements allocation/binding validation and installation. `NetMain::register_dpo`
supplies its publication/borrow boundary, and the derive calls it with `None`.
Owners with an assigned class key pass `Some(class)` through the same path.
The old `register_new_type`, `register_builtin`, and duplicate NetMain entry
are deleted, not compatibility aliases. All caller/macro migrations compile
under the macros/service/IP all-target check. The historical findings below
still describe the baseline, not an instruction to reintroduce those APIs.

Continuation correction: removed all four per-class publish/retire-root APIs
and the two owners' `publish_root`/`withdraw_root` helpers. Existing callers
use the approved class-dispatched lock/unlock within the publication scope.
No renamed root API is retained. Interface-RX now has interface-owned pool/DB
reuse and final-reference retirement plus IP graph-registration adapters; RX
interface counters and packet execution evidence remain open. Its FIB test
uses the route/bucket/withdrawal sequence from
`third_party/vpp/src/plugins/unittest/fib_test.c:9018-9054,9152-9170`, without
adding the source test's MPLS business. Protocol-error assertions were removed
from the VPP lifecycle scenario. `cargo check -p hammer-service -p
hammer-plugin-ip --all-targets --message-format=short` passes; no tests have
been executed. Independent review identified four additional defects:
registration-operation replacement, post-commit common-slot validation,
`u16::MAX` edge-cache collision, and update protocol mismatch. All four are now
corrected in the draft and independently re-reviewed without a new blocking
finding in that bounded diff; execution evidence is still pending. Registration
freezes owner operations on its first installation and later initializers only
append protocol nodes. Runtime's existing batch edge operation validates
shared-slot ranges before requesting refork and restores both next/pending-name
tables on rejection. DPO cache entries use `Option<u16>`. The runtime/service/IP
all-target compile gate passes after these migrations. Constructor/getter and
fake-node tests were removed from `net/dpo.rs`; those were not VPP behavioral
evidence. The full F01-F14/O01-O07 scope remains open until its integration and
execution gates are met.

| Scope | Approved change / consumers | Current state | Verification gate |
| --- | --- | --- | --- |
| Service registry, F03-F06/F09/F13 | One full operation registration contract for builtin/dynamic classes; distinguish missing protocol slots and resolver-only classes; both stack entries use owner resolution; owner diagnostics; no class-record wrapper | Implemented draft compiles; omitted operations preserve earlier protocol initialization; LB/replicate formatting and memory operations added; no execution evidence yet | Registration/graph/owner behavioral tests, service compile/lint |
| Owner storage, O02 | LB/replicate use either four inline buckets or one complete aligned out-of-line array; remove unused replacement methods; constructors/accessors/Drop migrate together | Implementation edited, not verified | Boundary-size access/replacement/lifetime tests and layout assertions |
| Publication and ownership, F01/F02/F07/F10-F12 | Enforce reader lifetime and publication scope; complete root/child mutation transaction and absence contracts | Guards, Copy-only worker selection and multi-bucket graph transactions compile. FIB projection now transfers one source-owned reference; replacement, withdrawal, duplicate-source count and Drop account for it; owning Clone and backend mutation bypass removed. Execution/live-worker/source-behavior integration remains open | Borrow-safety, live barrier, rollback and root-retirement evidence |
| Macro registration, F04/F05/F09/F14 | Extend existing derive for full owner operations; no CRUD/object store generation | Compiles with all six optional operations plus paired lock/unlock; resolver-only classes accepted; generated method calls NetMain publication API; concrete integration still open | Actual derived owner installation, operation dispatch and negative compilation cases |
| Concrete owners, O01/O03-O07 | Complete each report-defined owner lifecycle and graph consumer at its existing owning layer | Interface-TX resolver now installed by actual interface-output graph init; unused TX wrapper removed and default output initialization avoids mutating existing pool borrows. RX and other concrete-owner closures remain open | Owner/FIB/interface/plugin integration and VPP scenario comparisons |
| Docs and delivery | Update ADR/inventory/issue contradictions; per-module commits in dependency order; rebase and push; clean only verified project target | Open | Final reviewed commit gates, remote commit check, target cleanup report |

No scope in this ledger is removed from the objective. New wrapper types,
business-in-framework changes, and secondary synchronization mechanisms remain
forbidden. Standard callback signatures and existing owner methods are the
approved migration surface.

Current verification: `cargo check -p hammer-runtime -p hammer-service
-p hammer-component-macros -p hammer-plugin-ip --all-targets
--message-format=short` passed with existing warnings. This is compilation,
not test execution. `dpo_lifetime.rs` now covers missing mutation targets,
multi-bucket graph rejection without cache/topology changes, default interpose
retention, scalar query defaults and pool-memory reporting. These scenarios
have not run. No implementation commit, push or target cleanup has occurred.
Runtime's existing `GraphNodeInitialization` source now accepts the concrete
boundary error; service/plugin node initializers translate at that boundary
instead of using `RuntimeError::Subsystem`.

FIB continuation: `cargo check -p hammer-service -p hammer-plugin-ip
--all-targets --message-format=short` passed. The former fake-index IP FIB
tests are replaced by one real pool/root scenario spanning IPv4 default,
host and cover selection, source precedence, repeated source references,
failed publication/source preservation, IPv6 withdrawal and table destruction.
`FibError<B::Error>` preserves the concrete backend source. No test execution
or delivery gate has yet occurred. FIB source behavior/delegate integration and
the concrete owner rows remain part of the open objective.

Interface-TX continuation: `interface_tx_stack_uses_the_interface_output_node`
compiles against the real graph initializer, pre-existing local0, a newly
created hardware/software interface, and interface deletion. It has not run.
The VPP output-node reference was confirmed in `vnet/adj/rewrite.c:57-61`;
it is not the hardware `tx_node_index`. `InterfaceMain` owns the one-time
default node and an explicit per-interface output remains optional. The broader
interface publication/driver and RX lifecycle work is not claimed complete.

Evidence baseline: HEAD `4c2a9935`; the interrupted draft changes
`crates/hammer-service/src/net/{dpo.rs,mod.rs}` and
`crates/hammer-component-macros/src/lib.rs`. Other pre-existing dirty IP/FIB
files are preserved. Line numbers below refer to this worktree.

The review covers the complete public framework contract in vendored
`src/vnet/dpo/dpo.h`, its implementation in `dpo.c`, all DPO-directory VFT
registrations, and the adjacency/PMTU operation registrations outside that
directory. Concrete implementations are inspected where they demonstrate an
operation, object-lifetime, graph, or storage requirement. This is not a
line-by-line audit of every protocol's packet-processing implementation.

## Complete framework contract cross-check

This matrix covers every operation family exposed by `dpo.h`, including the
eight VFT members. A family being present does not mean its owner call chain
is complete. The finding IDs below are the migration closure, not separate
requests to implement individual helpers.

| VPP contract | Current result | Findings / disposition |
| --- | --- | --- |
| `dpo_id_t`, `DPO_INVALID`, `dpo_id_is_valid`, `dpo_cmp` | Compact identity, invalid predicate and equality agree | Keep Copy identity; do not conflate it with an owning reference |
| `dpo_set` and adjacency subtype selection | Fixed constructors create identities; subtype owner absent | F07, O04; move subtype choice to the owning adjacency implementation, not a generic IP switch |
| `dpo_copy`, `dpo_reset` | Copy is non-owning; only two bucket owners implement dependency accounting | F01, F07, F10; preserve semantic replacement/withdrawal, not C API names |
| `dpo_lock` / `dv_lock` | Dynamic callbacks and two concrete owners exist | F04, F05, F07, F12 |
| `dpo_unlock` / `dv_unlock` | Dynamic callbacks and two concrete owners exist | F01, F04, F07, O01; associated FIB/map/interface dependencies remain incomplete |
| `dpo_register`, `dpo_register_new_type` | Different registration capabilities and ambiguous empty slots | F03, F04, F05, F13 |
| `dv_get_next_node` and default node resolver | Draft only; resolver-only class identity fails; one stack path bypasses it | F02, F05, F06, O05 |
| `dpo_stack`, `dpo_get_next_node_by_type_and_proto` | Edge generation/cache exists; reference contract and error transaction incomplete | F02, F07, F11, F14 |
| `dpo_stack_from_node` | Has explicit-node fast-path mechanics, but caller substitutes parent resolution | F06, F07, F14 |
| `dpo_get_mtu` / `dv_get_mtu` | Draft scalar default matches; no concrete owner installed | F04, O01, O04, O07, F14 |
| `dpo_get_urpf` / `dv_get_urpf` | Draft scalar default matches; no concrete owner installed | F04, O04, O07, F14; LB's baked list is not this interface-valued operation |
| `dpo_mk_interpose` / `dv_mk_interpose` | Draft default retains original; custom owner and callers absent | F08, O07, F14 |
| `format_dpo_id/type/proto` / `dv_format` | Numeric Debug only | F09 |
| `dpo_memory_show` / `dv_mem_show` | Missing | F09 |
| `dpo_is_adj` | Intentionally removed generic class predicate; actual subtype owner not implemented | O04, not a request to restore the rejected helper |
| `vnet_link_to_dpo_proto`, `dpo_proto_to_link` | No demonstrated generic Hammer consumer needing those C conversion tables | Consumer-owned conversion only; do not add excluded protocol businesses |
| `dpo_pool_barrier_sync/release` | Existing runtime macro used for two pools | F01, F02, F14; enforce Rust borrow lifetime as well as worker visibility |
| `dpo_module_init` | Drop and IP LB registrations found; several advertised classes are not installed | O03-O07; retain plugin init ownership rather than importing all VPP businesses into service |

Registration search also covered consumers outside `vnet/dpo`: adjacency,
PMTU, MFIB, BIER, ILA and the VPP LB plugin. They use the same operation
families; they do not justify another generic object store or a central
business enum. Multiple classes sharing one concrete pool/operations set is
a required registration use case, demonstrated by lookup and label owners.

## Framework findings

All paths in the tables are repository-relative; VPP paths start at
`third_party/vpp/src/`.

| ID / severity | Hammer evidence | VPP evidence and gap | Impact / required action |
| --- | --- | --- | --- |
| F01 Blocking | `net/mod.rs:803,808,136` return references from `&self`; mutations also take `&self`, via `UnsafeCell` at `:84,91,104` | `vnet/dpo/dpo.h` pool-barrier macros protect worker access, not Rust references retained on the control thread | Safe Rust can retain `net.load_balance(index)` across `retire_load_balance_root` or replacement and then use it. A barrier does not end that borrow. Enforce the publication/read lifetime through the actual owning access boundary; do not add a pointer wrapper or another lock. The same concern applies to retaining `&DpoMain` across registration. |
| F02 Blocking | `net/mod.rs:84` constructs `&mut DpoMain`; `create_*_inner` and updates call `stack` through it; `dpo.rs:528` may acquire a barrier only later | `vnet/dpo/dpo.c:404` mutates graph/cache under its synchronization contract | An exclusive Rust reference is created before worker quiescence on call paths that rely on stack's internal barrier. Public readers are not constrained to avoid it. Define and enforce registry reader/writer scopes before creating the exclusive borrow. The draft instance callback also runs while this exclusive registry borrow exists, so callbacks must not re-enter registry access. |
| F03 Blocking | `dpo.rs:node_slot_mut`, `nodes`, `identity`, `register_builtin` | `vnet/dpo/dpo.c:320` distinguishes class registration, node arrays and custom instance resolution | Vector growth fills earlier protocol slots with empty vectors. Registering only IP6 makes `nodes(class, IP4)` return `Some(&[])`; `identity` accepts it and a later IP4 builtin registration reports a duplicate. Distinguish absent bindings from registered bindings without introducing a class-record wrapper. |
| F04 Blocking | `dpo.rs:428`; `net/mod.rs:193` | VPP `dpo_register` accepts the same full VFT used by `dpo_register_new_type`; adjacency and LB install MTU/uRPF operations through it | Builtin registration accepts only nodes, and NetMain hardcodes lock/unlock for LB/replicate. The new four-operation draft only extends dynamic registration. Builtin/interface/adjacency owners cannot install their full operations. Use one coherent registration contract for both allocation paths. |
| F05 Blocking | `dpo.rs:397`; `net/mod.rs:222,248` validate via `nodes(...).is_some()` | `vnet/dpo/interface_tx_dpo.c:51,77` registers an instance resolver with a NULL static node table | A legitimate resolver-only class has no usable identity/reference path unless it supplies artificial empty protocol bindings. Class existence must not be inferred from a static node table. |
| F06 Blocking | `dpo.rs:458` requires caller-supplied `parent_nodes`; draft `stack` alone uses `next_nodes` | `vnet/dpo/dpo.c:541` calls the parent's registered instance resolver itself | The two stack entry points do not share parent resolution semantics. `stack_from_node` can be passed unrelated parent nodes. Resolve through the registered operation in both paths; retain the VPP last-edge behavior for this path rather than incorrectly imposing the sibling-edge assertion from class stacking. |
| F07 Blocking | `dpo.rs:528,599` return a non-owning identity; LB/replicate acquire children separately in `net/mod.rs` | `vnet/dpo/dpo.c:179,225,255,496` covers replacement, reset, retain-new/release-old, and stacking | The Rust identity-only split is allowed, but the ownership closure is demonstrated only for LB/replicate buckets. Foreign owning fields, restacking and withdrawal are not integrated. Complete owner call chains, including same-object replacement, before claiming the semantics of VPP set/reset/stack are preserved. Do not add `dpo_copy` or a reference-wrapper type. |
| F08 Blocking | Draft `net/mod.rs:176`, `dpo.rs:interposes`; no production caller | `vnet/dpo/dpo.c:312,351` default interpose retains the original; custom callbacks own creation/stacking | Draft default retention is present but custom interpose, replacement/withdrawal and failure behavior are not demonstrated. Its fixed `Result<DpoId,DpoError>` also needs a concrete account of plugin-owned failures without generic boxing or moving business variants into service. Define whether returned identity owns one reference and verify default/custom/invalid paths. This is an unfinished contract, not a proved runtime failure. |
| F09 Blocking | `dpo.rs:218`; macro registration; only derived Debug for identities | `vnet/dpo/dpo.h:410` includes `dv_format` and `dv_mem_show`; `dpo.c:141,613` consumes them | Per-class object formatting and aggregate memory reporting are absent, including the draft. Numeric Debug does not inspect the owner object or report pool usage. Supply owner operations and diagnostic consumers without copying C varargs or embedding business payloads. |
| F10 Blocking | `dpo.rs:192`; `net/mod.rs:109` and mutation/root paths; `dpo.rs:From<DpoError>` | `vppinfra/pool.h:pool_elt_at_index` asserts occupied; `vnet/dpo/load_balance.h` separately offers checked lookup; lock/unlock callbacks are void | `ObjectMissing` remains in ordinary absence and already-owned internal paths; generic `RuntimeError::Subsystem` remains the escape route. Complete definition/caller/test migration: normal lookup absence is Option, owned-slot violations are local invariants, actionable external rejection belongs to its API owner. Do not replace every occurrence with expect or silently swallow failure. |
| F11 Blocking | `net/mod.rs:create_load_balance_inner`, `update_load_balance_inner`, replicate equivalents loop over independent `stack` calls | VPP's graph operations are assert/infallible at those points; Hammer exposes typed graph failures and AGENTS requires failure atomicity | An early bucket can publish a graph edge/cache entry before a later bucket returns a node/graph error. The target pool may remain unchanged while graph state does not. Validate the whole transaction or explicitly restore the whole mutation scope. Existing invalid-index test fails before this point and does not cover it. |
| F12 Blocking | `net/mod.rs:109` special-cases only LOAD_BALANCE and REPLICATE; builtin lifecycle selection at `:193` | VPP class owners validate their own pool in concrete lock operations; the framework dispatches by registered class | Validation and lifecycle installation remain coupled to two owner implementations. No common object-store validator is needed: require the appropriate owner contract and keep recoverable external validation at that owner. Merely adding every new DPO to these matches would repeat the defect. |
| F13 Contract mismatch | `dpo.rs:38-56,240`, registration uses numeric `<30`; test asserts first key 30 | VPP's enum reserves values for its actual linked-in businesses; it is not a Hammer wire/ABI contract | Hammer reserves absent VPP business slots and claims numeric interoperability without a demonstrated consumer. `DYNAMIC_TYPE_START` is the removed boundary concept under another name. Allocate based on Hammer's approved registration/identity contract, not unimplemented VPP businesses. Capacity behavior must be explicit; current checked increment cannot allocate key 255. Do not copy C wraparound. |
| F14 Blocking evidence gap | `dpo_lifetime.rs:11`; `dpo.rs:tests`; no independent-owner macro-operation integration | VPP `fib_test.c` validates resulting buckets, references and pool reclamation through FIB operations, not just constructors | Existing lifetime test deliberately binds both classes to a terminal node and starts no workers. It cannot prove sibling stack execution, instance next resolution, dynamic owner macro registration, MTU/uRPF, interpose, DSO callbacks, live barrier/refork, or graph rollback. The test named `class_registration_and_stack_are_monotonic` never calls stack. Add focused behavioral coverage at these boundaries, not one test per getter. |

## Owner integration gaps, not reasons to add business to the framework

These are real incompleteness in the present DPO surface. They belong to their
concrete owner; protocol-independent layouts alone do not implement them.

| ID / severity | Hammer evidence | VPP evidence | Missing behavior / owner |
| --- | --- | --- | --- |
| O01 Blocking | `net/dpo.rs:730,867,923`; `net/mod.rs:193,549` | `vnet/dpo/load_balance.c:368,399,654,897,953`; `load_balance_map.c:428,465,504` | LB has inert `USES_MAP`, `STICKY`, `map_index`, `urpf_index`; bucket selection does not consult a map. No normalization/map lifecycle, baked uRPF-list retention/release, per-LB to/via counters, or child minimum-MTU operation is wired. Keep this in the concrete LB/FIB owners, not generic DpoMain. |
| O02 Non-blocking layout divergence | `net/dpo.rs:bucket_storage`, `select_bucket`, `overflow_slice` | `vnet/dpo/load_balance.h:240`; `load_balance.c:73`; `replicate_dpo.c:66` | Hammer stores first four buckets inline even for large objects, then only the tail out of line. VPP switches to one contiguous full bucket array above the inline threshold. Both Hammer objects are 64-byte aligned, but their access/layout model is not identical. Record and measure the split-array choice or align the storage behavior; do not claim size/alignment alone proves equivalent hot-path behavior. |
| O03 Blocking | `net/dpo.rs:665` is only a layout; DpoId lookup constructor chooses one class | `vnet/dpo/lookup_dpo.c:73,127,232,1449` | Missing concrete lookup pool lifecycle, FIB/MFIB reference retention and the runtime class keys for source/destination/interface-selected/multicast lookup nodes. Per-packet loop limit belongs to those concrete nodes, not generic DpoId. Protocol table selection remains with the producer implementation. |
| O04 Blocking | `net/dpo.rs:640,675`; repository search finds definitions/re-exports but no owner construction/use | `vnet/dpo/receive_dpo.c:26,60,88`; `vnet/adj/adj.h:224,264,304`; adjacency VFTs | Receive lacks actual pool/reference/source-selection integration. Adjacency lacks concrete subtype projection, FIB child/back-walk relationship, delegates, midchain fixup/restack/unstack and target tracking. These must be supplied by the concrete adjacency/receive owners; no IP address union or universal delegate payload in the registry. |
| O05 Blocking | `net/dpo.rs:685,692`; no instance resolver caller/registration found for interface TX | `vnet/dpo/interface_rx_dpo.c:19,56,75,229`; `interface_tx_dpo.c:51,77` | RX lacks per-interface/protocol object reuse, release and RX-node use; TX lacks actual sw_if_index-to-output-node resolution. TX is stateless in VPP: its extra one-field Hammer object is not required to achieve that contract. Interface owner must connect the already-existing interface state. |
| O06 Blocking | `net/dpo.rs:706`; `net/mod.rs:create_replicate,update_replicate`; registrations found only in the lifetime test | `vnet/dpo/replicate_dpo.c:311,527,728` | Replicate has storage/reference operations but no production registration/node execution found, no local-bucket exclusion duplication behavior, and no demonstrated clone/enqueue/allocation-failure/counter path. Implement in the concrete replication owner and graph adapters. |
| O07 Blocking integration gap | `hammer-plugins/net/ip/src/lib.rs:14,20` derives DpoClass but has no calls to generated registration; graph inventory lacks these DPO nodes | `vnet/dpo/punt_dpo.c`; `vnet/dpo/ip_null_dpo.c`; `vnet/ip/ip_path_mtu.c:413,835` | PUNT has an identity but no registered DPO path found. IP-null/PMTU are declarations, not installed owners; PMTU still derives Copy despite containing owning dependency facts and has no registered lifetime/MTU/uRPF/interpose callbacks. IP implementations stay in the IP plugin. Their absence prevents using them as proof that the generic extension point works. |

### FIB root ownership closure

F07 is not merely a missing test: `hammer-service/src/net/fib.rs:292` stores
source and selected forwarding identities, and `remove_route` removes or
replaces them. `hammer-plugins/net/ip/src/fib.rs` copies those identities into
its backend and returns `entry.forwarding` from projection. Neither path
acquires/releases DPO references; searches find root retirement callers only
in the lifetime test. Thus production route/source retention, winner change,
withdrawal, table destruction and rollback have no demonstrated ownership
closure. They cannot be repaired by making `DpoId::Copy` count references.

## Documentation and verification gaps

- ADR-0005 at its DPO-operation paragraphs and inventory still says MTU/uRPF/
  interpose are not service-dispatched operations. The just-approved extension
  and interrupted draft supersede that statement, but the document has not
  been migrated. Issue #291 still forbids manual unlock despite the later
  explicit approval of class lock/unlock. These are conflicting contracts,
  not implementation proof.
- The interrupted draft has no compile/test evidence. Its four new callback
  slots cannot be marked implemented end-to-end. No test command was run in
  this review, as requested by the repository final-precommit test rule.
- Callback panic containment and live-worker borrow scopes still require
  lifecycle evidence. Callback code unmapping is **not** a current finding:
  `hammer-runtime/src/plugin_loader.rs:38` pins images for process lifetime,
  and `plugin.rs:282` explicitly forbids active unload. Do not invent an
  unload protocol or report an unverified unload crash.
- Unused `replace_buckets` methods and constructor-only assertions are cleanup
  candidates, not additional VPP features to implement. `DpoMain: Clone` also
  needs a justified consumer because copying class allocation/edge authority
  is not how VPP's one process-wide DPO registry works.

## Explicit non-findings

- Eight-byte Copy DpoId, equality/hash on class/index, invalid identity fields,
  drop index=protocol and punt index=1 match the inspected VPP semantics.
- `DpoProto` is a graph/link discriminator. VPP itself has a closed protocol
  set; removing that fact or turning it into IP wire protocol dispatch is not
  required by this audit. Link/wire conversions belong at concrete consumers;
  no unused business conversion enum should be added to net.
- Class/protocol edge caching in class stacking follows VPP. Do not add an
  instance-index cache merely because TX resolution is instance-dependent.
- `stack_from_node` selecting the last returned edge is VPP behavior, unlike
  the common-slot assertion used for class sibling stacking.
- MTU default `0xffff` and uRPF default `~0`, including invalid identities, are
  correct in the draft. Ordinary missing-object lookup remains a separate
  Option contract.
- Pool rather than Vec for owner objects, 64-byte LB/replicate objects, four
  inline buckets and thin overflow pointer are already present. Those facts
  do not prove the omitted lifecycle or complete storage behavior.
- Barrier on every Hammer pool insertion can be justified by Pool's occupancy
  reads. VPP's growth-only barrier does not authorize racing Rust metadata.
- Removing a pool value before recursively destroying it is an intentional
  Rust aliasing adaptation; restoring C destruction ordering blindly would
  not fix F01/F02.
- MPLS/LISP/BIER/PW, classify, proxy and DVR packet policy is not missing generic
  DPO infrastructure. Their VFT registrations were checked for required
  framework capabilities; their business implementations are not proposed
  additions to service. No DpoObjectStore, DpoRef, generation, central business
  enum, C-style dpo_copy or per-class generated CRUD is warranted.

## Evidence and actions taken

Read-only source inspection: complete framework `dpo.c/.h`; DPO-directory VFT
inventory; concrete LB/map/replicate/lookup/receive/interface/drop/punt paths;
adjacency and PMTU registrations; Hammer registry/owner/macro and test paths;
runtime graph-edge publication; ADR-0005; GitHub issue #291.

Used `rg`, `sed`, `cat`, `git diff/status`, and `gh issue view`. No compilation,
runtime test, local lab, issue mutation, commit or push was performed for this
review. Only this review artifact was added after the user changed the task to
review. The interrupted Rust draft was neither completed nor reverted.

The source-backed findings above are current-project facts. Suggested owner
fixes are recommendations, not permission to add new APIs. Unverified runtime
properties are explicitly labeled as evidence gaps rather than observed bugs.
