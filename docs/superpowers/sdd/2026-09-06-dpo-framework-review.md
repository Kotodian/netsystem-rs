# DPO framework alignment review

## Final ICMP and Feature Arc review checkpoint

Verdict: **Needs changes**. This checkpoint records the current uncommitted
IP/ICMP, local, punt, buffer and interface Feature Arc migration. It supersedes
earlier checkpoint statements for those paths; the older DPO findings below
remain a historical baseline, not a newly verified list of open defects.
This review does not authorize new types or APIs. Implementation and delivery
remain incomplete; no finding is closed merely by adding a node declaration.

### Findings and implementation status

IC02 implementation update: service `PuntNode` now uses ordinary graph-node
initialization only. IP's existing concrete punt nodes use explicit graph init
callbacks to register PUNT/IP4 and PUNT/IP6 with their own node identities and
preserve their named next to the generic terminal. No new public type/API was
introduced. The existing IP FIB lifecycle test now invokes those production
registration entries, stacks each protocol's PUNT DPO, resolves the resulting
graph edge and checks the concrete punt node and its terminal edge. This is
required graph-dispatch proof for issue #291 and IC02, not a constructor test.
`cargo check -p hammer-plugin-ip --all-targets --message-format=short` passed
after this change; tests have not run. IC02 therefore remains pending the final
behavior-test gate, rather than a current known wrong registration.

Changed paths for this slice: service `src/data_plane.rs`, IP `src/punt.rs`,
IP `src/fib.rs` (existing test only), and this ledger. The remaining findings
are not resolved by this registration change. Work continues in the current
thread without sub-agents, as explicitly requested by the user.

IC03/IC04/IC05 implementation update: IP `local.rs` now checks the declared
packet length against the first segment plus chain length, validates UDP length
before local features, releases its first-buffer borrow, and feeds chain
segments directly to the existing `InternetChecksum` implementation. The
checksum stops at the declared IP transport length, carries odd bytes between
segments, and uses no collected payload. IPv6 source lookup now honors packet
FIB plus its explicit override. A regression case uses the vendored IPv4 echo
message split at an odd buffer boundary, checks corruption in the tail and
verifies chain reclamation. The test's configuration import was corrected to
the existing `hammer_runtime::DataPlaneBufferConfig` export. The command
`cargo check -p hammer-plugin-ip -p hammer-plugin-icmp --all-targets --message-format=short`
now passes, including the test targets; no tests have executed.
Concrete per-family validation/errors, offload/translated
facts and receive-object integration remain open; these changes do not close
IC04/IC05 or prove complete local graph behavior.

Local dispatch update: `local.rs` now selects the local-next entry using the
base IPv4/IPv6 header protocol, before parsing transport or IPv6 extensions.
The end-of-arc branch returns from that dispatch portion without calling
`ip_header`, transport validation, source lookup or Feature Arc start. IPv4
fragment classification still selects reassembly, matching
`ip4_forward.c:1680-1694,1797-1818`; IPv6 uses `ip->protocol` as in
`ip6_forward.c:1591`, not the effective upper-layer protocol. No new helper or
public API was introduced. Full graph behavior tests remain pending.

IC08 FIB update: the IPv6 echo response to a request from a non-link-local
source to a link-local destination now clears both packet FIB selection facts.
The existing `lookup_index` then maps the preserved RX interface to its IPv6
FIB. This implements `ping.c:652-664` without interpreting TX interface as a
FIB index or adding a cross-plugin API. Other replies retain their packet FIB
facts. Host TTL policy, IPv4 fragment-ID generation and locally-originated
metadata remain incomplete. Compilation of IP/ICMP all targets and
`git diff --check` passed after this change; packet tests remain pending the
final pre-commit gate.

ICMP echo message tests now reproduce the ID `0xB`, sequence `5` and eighteen
`0x0a` payload bytes from `test_ip4.py::TestICMPEcho.test_icmp_echo` and
`test_ip6.py::TestICMPv6Echo.test_icmpv6_echo`. IPv6 covers global and link-local
destinations. Both tests call the real in-place writer with either all bytes
or only the IP/ICMP headers available, then check swapped addresses, reply
type/code, preserved ID/sequence/payload and complete checksums. They do not
claim Ethernet rewrite, graph dispatch, FIB selection, host TTL policy,
random IPv4 ID or origin-flag coverage. These are private protocol tests, with
no exported test API or additional dependency. `cargo check -p hammer-plugin-icmp
--all-targets --message-format=short` passed; tests have not executed.

ICMPv6 input correction: `icmp6.c:165-194` selects punt for all classified
type/code/hop-limit/length errors. Hammer's IPv6 input default error next now
selects `ip6-punt`, retaining its typed error assignment; IPv4 and echo nexts
are unchanged. The new private input test installs the actual runtime,
service, IP and ICMP registration images and invokes the input path under its
node context. Cases cover registered echo, invalid echo code, neighbor hop
limit, length-error precedence and unknown type; assertions resolve the next
through the real graph and compare the buffer's preinstalled error index.
`cargo check -p hammer-plugin-icmp --all-targets --message-format=short` passed.
The test is not executed yet: neither successful runtime initialization nor
end-to-end packet processing is claimed from compilation alone.

Feature-chain termination update: the compiler no longer emits an outgoing
edge/config next for the final occurrence when its node already equals the
selected end node. This follows `vnet/config.c:84-89` and removes the previous
end-node self edge. The end-node identity remains in the configuration sharing
key. `crates/hammer-service/tests/interface_features.rs` uses existing concrete
graph nodes and the public interface configuration methods to check implicit
and explicitly enabled termination, resolved next edges, no terminal self edge,
disable-to-default behavior and buffer reclamation. It is a configuration
compiler regression, not a claim of complete packet graph execution.
`cargo check -p hammer-service --test interface_features --message-format=short`
and `git diff --check` passed; the test has not executed.

IC06 replacement was approved and implemented on 2026-09-06. The const-generic
`next_feature_with_config::<N>` returns `([u32; N], u16)`, copying configuration
words into a fixed owned array without allocation, a new type or a callback.
ADR-0006 records the decision. The executed `interface_features` test checks
two-word and zero-word cursor advancement, terminal behavior, and owned words
remaining valid after configuration removal and reuse. The callback reentry
defect is resolved; this does not resolve the independent handoff defect IC01.

IC09 implementation update: `icmp_error.rs` now selects destination-unreachable,
time-exceeded, parameter-problem and IPv6 packet-too-big sent counters according
to the type mappings in VPP `icmp4.c:200-214` and `icmp6.c:238-254`. Counter
selection does not introduce an extra wire-code policy. At the user's explicit
request, the newly added trace macro call and manual trace-handle transfer have
been deleted. Trace handling is deferred to separate work; existing trace
infrastructure is unchanged. Locally-originated metadata and full graph counter
test evidence remain open. No tests have run for this batch.

IC01 migration constraint: `NodeRuntime::node_for_handle` uses a registration
map, so `NodeHandle::new(resolved_node.slot())` is not a valid replacement for
the current continuation mechanism. TCP is the current `Some(continuation)`
caller; reassembly and UDP migration use direct targets. Keep the feature
cursor untouched in the eventual fix without pretending node IDs and registered
handles are interchangeable. No handoff implementation change has been made
at this checkpoint.

IC07 corrected decision after the user's explicit VPP-alignment instruction:
`hammer-service/src/binary_api.rs:392` gives `BinaryApiMain` only socket-listener,
path and frame-limit state. Its request dispatch at lines 570-612 executes on
the main/control path and owns the worker barrier. There is no existing worker
submission method to reuse. ADR-0005's ICMP/PMTU paragraph incorrectly assumes
that such an ingress already exists. Source call-site inspection found
`ip_path_mtu_update` in VPP's `vnet/ip/ip_api.c:2077` and control maintenance in
`ip_path_mtu.c:751`, not in ICMP input. The user reiterated that the design must
align with VPP rather than add this extension. ADR-0005 now removes the mistaken
worker-event requirement. The ICMP PMTU node/image entry, its chain collector
and the two IP ICMP-byte parsers have been deleted. PMTU messages remain on
ordinary ICMP type dispatch; no worker ingress is proposed. IP FIB-linked PMTU
state, DPO behavior and control-plane API are still required and are not proved
complete by this deletion. No tests have run for the deletion batch.

| ID | Owner and current evidence | Contract, impact and required correction |
| --- | --- | --- |
| IC01 | Runtime: `crates/hammer-runtime/src/data_plane/handoff.rs:89` writes a resolved node slot to `current_config_index`; service `interface/feature.rs:337` reads that field as a shared configuration heap index. | A handoff with a continuation inside an active feature chain corrupts its cursor, allowing a wrong next or out-of-bounds access. Preserve feature configuration across handoff using the existing handoff ownership contract. ADR-0006's paragraph allowing this reinterpretation also needs correction; renaming the field does not separate the two simultaneous facts. |
| IC02: implemented, test gate pending | Service `PuntNode` no longer registers a DPO; IP `punt.rs` registers each concrete protocol binding in its node init. | VPP `src/vnet/dpo/punt_dpo.c:65` binds `ip4-punt` and `ip6-punt`. The former bypass is removed in source; the added graph-stacking regression checks await execution. Generic terminal disposition remains separate. |
| IC03: implemented, test gate pending | IP `local.rs` validates declared length against the chain and feeds segments to `InternetChecksum`; the odd-boundary ICMP checksum test compiles. | VPP `src/vnet/ip/ip6_forward.c:1058` onward computes over buffer chains. The implementation no longer requires the entire transport payload in the first buffer, but executable chain and full local graph evidence remain pending. |
| IC04: partially implemented | IP `local.rs` now validates UDP length/checksum before local features, but still lacks translated/offload/computed facts and concrete family-specific paths/errors. | ADR-0006 and VPP `ip4_forward.c`/`ip6_forward.c` require those independent validation branches. The existing shared IPv4/IPv6 function remains incomplete. |
| IC05: partially implemented | IPv6 source lookup now uses packet FIB plus explicit override. Local features still use raw RX interface; generic `ReceiveDpo<A>` has no connected instance lookup in this path. | ADR-0006 and VPP `ip6_forward.c:1574-1586` require effective receive-interface facts. Connect receive-object production/consumption without moving concrete address policy into service. |
| IC06: resolved and tested | Service `next_feature_with_config::<N>` returns owned words and next without calling user code. | Approved replacement removes the borrowed-slice callback; the configuration lifecycle regression passes. IC01 remains independent. |
| IC07: non-native extension removed | ICMP PMTU node, graph declaration and chain collector deleted; IP ICMP-byte cache parsers deleted. | Latest user instruction requires native VPP behavior. ADR-0005 now specifies IP control-plane PMTU updates, not automatic ICMP worker submission. Ordinary type dispatch/punt remains; full IP PMTU control/DPO behavior still requires its own completion evidence. |
| IC08 | ICMP: `src/protocol.rs:85,92` fixes TTL/hop-limit at 64 and preserves the request's IPv4 fragment ID; echo processing lacks the complete origin/FIB handling. | VPP `src/plugins/ping/ping.c:303,449-462,650-665` uses host configuration, a new IPv4 fragment ID, locally-originated state and the IPv6 link-local/global reply FIB adjustment. Complete these owner-local semantics rather than treating address/type swaps as full echo alignment. |
| IC09: partially implemented; trace deferred | IP `icmp_error.rs` separates sent-type counters. Locally-originated state and graph counter evidence remain missing. | VPP `src/vnet/ip/icmp4.c:286-294` and the corresponding IPv6 path mark origin. Complete non-trace metadata/counter behavior. The user explicitly deferred trace: the newly added macro and handle-transfer code are deleted and must not be restored as part of this work. |

### Headroom correction: withdrawn finding

The earlier claim that user-configured headroom must cover an IP/ICMP header,
or that zero/small configured headroom proves a prepend panic, is withdrawn.
Headroom is user-owned buffer policy, not an IP or ICMP configuration requirement.
No protocol-specific minimum, allocator or automatic headroom adjustment is
requested by this review.

VPP `src/vlib/buffer.h:178-187` contains three distinct declarations:
`CLIB_ALIGN_MARK (headroom, 64)` is an alignment marker, not writable packet
storage; `pre_data[VLIB_BUFFER_PRE_DATA_SIZE]` is the reserved storage directly
before `data[]`; `data[]` is the packet data origin. Backward header insertion
uses the valid packet-storage range, including `pre_data`, with `current_data`
locating the current packet relative to `data[0]`. It does not write into the
alignment marker or buffer metadata.

Hammer `buffer/mod.rs:53` defines 128 bytes of pre-data, and
`buffer/header.rs:78-79` places the data origin after that reserve. The existing
method named `available_headroom` at lines 497-500 measures writable prefix
capacity from the end of the buffer header to the current packet start. Its
name must not be used to equate that capacity with the user's headroom setting.
`reset_empty` applies the configured initial data offset; `pool.rs:123-140`
preserves the source offset in the independent response buffer. These are
separate layout facts, not authorization for a protocol to change or require
the user's reserved headroom.

VPP `icmp4.c:274-296` copies the current first buffer and advances backward to
prepend response headers. The Hammer comparison must follow the corresponding
`pre_data`/`current_data` bounds and response layout. User-defined headroom is
not an ICMP-owned resource or an ICMP configuration prerequisite.

An exhausted prepend range must be demonstrated through an actual reachable
buffer state before reporting a failure-path defect. This checkpoint makes no
such claim and does not change buffer policy or classify that condition anew.

### Verification and delivery status

Current test checkpoint (2026-09-06; supersedes historical test-pending notes
above and below): the previous workspace run failed only the ICMP stats fixture
and interface-address main-thread fixture. Both fixtures are corrected without
weakening production checks. At the user's direction, the rerun is limited to:

- `cargo test -p hammer-plugin-icmp --lib --message-format=short`: 3 passed.
- `cargo test -p hammer-service --test interface_address_lifetime --test interface_features --test dpo_lifetime --test packet_throttle --message-format=short`: 5 passed.
- `cargo test -p hammer-core --test buffer_allocation -p hammer-plugin-ip --lib --message-format=short`: 3 passed.

All 11 selected tests pass. The IP FIB test verifies concrete punt stacking;
the chain test verifies odd-boundary checksum and corruption, not full local
graph coverage. Formatting was applied only to changed Rust files, and
`git diff --check` passes. No host TUN/TCP lab or further workspace test was run.
Trace remains deferred. Green selected tests do not close IC01, IC04, IC05,
IC08, IC09 or the remaining PMTU integration gap. Delivery must remain a draft
until those acceptance gaps are resolved; issue #291 must not be marked complete.

Historical review checkpoint:

- Current review commands: source inspection with `rg`/`sed`/`nl`,
  `git status --short`, `git diff --check`, and
  `cargo fmt --all -- --check`.
- `git diff --check` passed. Formatting check failed in the new macro,
  interface feature and IP node changes; it did not modify files.
- The preceding checkpoint records a successful multi-crate `cargo check
  --all-targets`; this review did not rerun compilation and does not treat
  compilation as packet-graph behavior evidence.
- No test command was run during this review. Current migration lacks verified
  graph tests for feature order/config/end/handoff, DPO punt traversal, receive
  interface selection, chained local validation and ICMP response behavior.
  The core allocation test alone cannot establish these contracts.
- Test cases must derive from vendored packet behavior, including
  `test/test_ip4.py` ICMP echo and TTL/MTU cases and `test/test_ip6.py` echo and
  hop-limit cases. Exercise real nodes, packet contents, next arcs and owner
  state, not source-text assertions or constructor-only next-slot assertions.
  The concrete echo baseline is `test_ip4.py:534`,
  `TestICMPEcho.test_icmp_echo`: request ID `0xB`, sequence `5`, eighteen
  `0x0a` payload bytes; verify swapped addresses, reply type and preserved
  ID/sequence/payload. `test_ip6.py:1580`,
  `TestICMPv6Echo.test_icmpv6_echo`, applies the same message checks to both
  global and link-local destinations with a global source. Neither reference
  requires creating a local host TUN interface for Hammer's graph tests.
- Run tests only at the repository's final pre-commit gate after implementation,
  review and formatting are ready. Module commits, push and target cleanup have
  not been completed. Do not claim full alignment or close the issue yet.

## Scope and verdict

Verdict: **Needs changes**. This is a static review of the current worktree,
including the uncommitted operation-registration draft, not a claim that the
draft compiles or that a complete implementation exists.

The review below records the baseline. The subsequent user instruction approves
fixing every finding, committing by owning module, pushing, and cleaning the
workspace target directory after verification. Findings are not resolved merely
because an implementation edit exists.

## Implementation ledger

Current uncommitted ICMP migration checkpoint:

- IP owns `protocol::icmp::IcmpErrorMetadata` and concrete
  `ip4-icmp-error`/`ip6-icmp-error` nodes. IP input and UDP unknown-port paths
  write the shared metadata and select those concrete nodes; duplicate
  TCP/UDP overlays and ICMP's error node/source snapshots/builders are removed.
- IP error nodes attempt to use new response buffers, quote only the original
  first buffer, bound output at 576/1280, and consume originals through drop.
  The arena lock reentry is corrected: `generate_error` drops the source borrow
  before calling core's `alloc_index_from`, which allocates and transfers the
  current segment under one arena write guard. It then truncates and prepends
  directly in the response buffer. Copy assignment preserves both opaque
  regions, including the FIB override; only the error request and response
  cursor are updated by IP. This is not yet executable graph proof.
  VPP `buffer_funcs.h:1257` allocates a
  distinct first buffer and copies both opaque regions and current bytes;
  `icmp4.c:274` and `icmp6.c:315` consume that operation.
  Existing `Ip4Main`/`Ip6Main` hold indexed `ThreadOwned<Throttle>` slots;
  worker init installs them and owner-worker exit clears them. No new Main,
  lock or pointer-publication mechanism is added.
- Explicit user correction: no address stale flag or generation mechanism is
  added. Current interface address membership is authoritative.
- Explicit user approval: `write_ipv4_push_header` gains the final
  `dont_fragment: bool` argument. TCP/UDP pass true; ICMP errors pass false.
  The existing writer computes the checksum with the selected flags. IPv6
  errors reuse `write_ipv6_push_header`; protocol numbers use `IpProtocol`.
- ICMP input tables now live directly in `IcmpMain`. The legacy control-plane,
  snapshot, handle and mutex-runtime-registry surfaces and the ICMP crate's
  `arc-swap` dependency are removed. Type registration requires the existing
  main-thread publication scope; packet reads copy one table entry.
- Echo mutates the original first buffer's headers in place, incrementally
  updates the ICMP type/checksum and preserves the chain. The generated-packet
  Vec and echo chain aggregation are removed. Echo registers into the type
  table and uses concrete IPv4/IPv6 lookup nexts. Node error descriptors are
  installed for input and echo.
- Echo now resolves its existing NetMain dependency before borrowing or
  rewriting the packet, so failure to obtain that dependency cannot leave a
  modified reply. This is a source-inspected ordering fix, not test evidence.
- Registration non-finding: `NodeRuntimeInner::materialize_node_errors` and
  `validate_node_error_batch` both skip already-installed tables
  (`hammer-runtime/src/node.rs:629-693`). The later empty NodeEntry descriptor
  slice therefore does not overwrite descriptors installed by the ICMP node
  callbacks. No registration API change is warranted for this concern.
- Explicit user correction: the proposed `copy_no_chain` API is withdrawn and
  not approved. BufferChain already expresses chain traversal; the missing
  capability is safe access to distinct source/destination packet storage,
  not another chain abstraction. Index and header Copy semantics do not
  allocate or duplicate the packet bytes stored outside the header. The user's
  subsequent confirmation approves owner-local independent segment allocation:
  core now exposes `alloc_index_from(Index) -> DataPlaneResult<Index>`, called
  through existing `runtime.buffers()`. No runtime wrapper, generation,
  intermediate payload storage or guard bypass is introduced. The new core
  lifecycle scenario covers independent mutation, source-chain survival,
  opaque/offset preservation, allocation exhaustion and final reclamation;
  it has not yet run and is not a substitute for ICMP graph tests.
- Earlier checks: focused `cargo check` for IP/ICMP/UDP/TCP, all targets, passed
  before the independent-buffer allocation change.
  The core allocation and IP consumer migration subsequently passed
  `cargo check -p hammer-core -p hammer-plugin-ip -p hammer-plugin-icmp
  -p hammer-plugin-udp -p hammer-plugin-tcp --all-targets --message-format=short`.
  Independent read-only review found no blocker in that bounded migration;
  its trace/error exclusion coverage note was addressed by setting nondefault
  source values in the same lifecycle scenario. Tests remain unexecuted.
  No tests or commits have run for this migration. These checks are not packet,
  DSO, publication, or complete VPP-alignment proof.
- Remaining integration work includes IP local's old ArcSwap/runtime registry,
  trace/origin/FIB metadata equivalence, IPv4 echo fragment-ID/host policy,
  IPv6 link-local forwarding/source-selection details, real punt behavior,
  PMTU event publication and VPP-derived graph tests. The current PMTU consumer
  still aggregates a chain and directly calls the old IP cache parser; it is
  not complete. The service Binary API currently accepts socket requests only;
  no worker-to-dispatch request ingress was found. Runtime remote-local queues
  are main-to-worker and mutex-backed, not a substitute for this missing seam.
- Complete-path audit correction: IP local's static `icmp-input` next and
  ICMP-specific default dispatch are removed. Existing explicit registration
  is the only local ICMP connection; the IP graph no longer requires that
  foreign node by name. This fixes one dependency edge, not full IP-only graph
  execution. Default punt still aliases drop and needs its real IP behavior.
  Both registration entry points still mutate one shared table, contrary to
  ADR-0006, and local requires all transport bytes in the first buffer. Thus
  the in-place echo chain behavior cannot yet be reached for valid chained
  input. Resolve these together with the approved concrete local nodes/tables
  and removal of the snapshot/runtime registry, not by extending the shared
  local model with another protocol helper.
- Trace audit: VPP `trace_funcs.h:168` assigns the existing trace flag/handle
  to the response; Hammer's trace state finalizes a handle when its buffer is
  released. New-buffer allocation deliberately does not inherit that handle.
  The ICMP path therefore still needs a verified trace-lifetime solution;
  `try_mark_trace` is not equivalent because it consumes an independent node
  quota. Do not simply copy the numeric handle and assume shared lifetime.
- PMTU audit: vendored `ip_path_mtu_update` callers found in `ip_api.c`,
  `ip_path_mtu.c` replacement handling and API test clients, not ICMP input.
  Automatic ICMP-to-typed-PMTU publication remains an ADR extension, not proof
  of an existing VPP receive path. The proposed generic Binary API worker
  ingress is withdrawn, not approved by the overall alignment instruction.

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
