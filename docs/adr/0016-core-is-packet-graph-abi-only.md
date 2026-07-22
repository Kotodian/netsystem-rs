# Core is packet-graph ABI only

Status: accepted

## Context

`hammer-core` accumulated process configuration, service lookup, metrics,
logging, forwarding, and protocol implementations after it became the shared
location for packet graph primitives. Those subjects have different lifecycle
and plugin owners. Keeping them in Core makes every DSO depend on a broad
control-plane crate and lets protocol implementations cross plugin boundaries
through common imports.

The dynamic-plugin work recorded in issue #95 previously allowed plugin
registration and lifecycle inventory records to move into Core. Issue #114
defines the later, narrower Core boundary: only packet-graph ABI may remain in
Core. The current runtime registration authority already implements that
direction.

## Decision

`hammer-core` is limited to the cross-DSO packet graph ABI:

- graph identity and copyable local-next facts;
- buffer, index, packet opaque, cursor, chain, and frame ownership facts;
- layout constants and failures intrinsic to those facts.

Core must not own configuration parsing, process service lookup, logging,
metrics, generic data-structure compatibility exports, network-business
models, forwarding policy, protocol implementations, or lifecycle records.

Ownership outside Core is explicit:

| Owner | May own | Must not own |
| --- | --- | --- |
| `hammer-infra` | Generic containers, allocation, map, mtrie, queue, and memory primitives | Packet graph, protocol, session, or plugin concepts |
| `hammer-runtime` | Graph execution, registration images, worker/process lifecycle, runtime registry, metrics, logging, FileMain, and barriers | Protocol implementation or session-policy ownership |
| `hammer-service` | Protocol-neutral session, interface, and device contracts | A replacement common protocol or Core facade |
| Protocol/device plugins | Their wire/state/config/dispatch/error/counter/metric implementations and declared upstream plugin dependencies | Reverse dependencies into a lower protocol layer, dependency cycles, or generic runtime scheduling |
| `hammer` | Bootstrap, process composition, and presentation | Plugin implementation state |

Issue #114 supersedes the Core-placement portion of issue #95. Registration
images, lifecycle inventories, and plugin metadata remain Runtime-owned unless
an approved owner-neutral ABI is required by independently loaded code. No new
registration, service-lease, compatibility, or carrier interface is introduced
by this decision.

Plugin dependencies express the real protocol stack and are directed: `tcp`
and `tun` depend on `ip`; a future `tap` plugin will depend on `ethernet`.
`load_after` declares activation order and dependency closure. A Cargo
dependency is added only when the dependent plugin actually imports an
upstream plugin type; metadata alone is not a reason to create an unused Rust
link edge. Lower layers must not depend back on their consumers, and cycles are
invalid.

## Migration Rules

Each vertical slice removes a Core owner rather than leaving a re-export. It
must compile without compatibility aliases and preserve behavior through the
owner's executable tests. Source-text checks can report remaining migration
work but cannot establish graph, DSO, or runtime behavior.

The initial slice moves `RuntimeRegistry`, metrics registry/recorder, generic
network values, and data-structure re-exports to their actual owners. Later
slices move runtime configuration and logging, then forwarding and protocol
implementations with their plugins.

## Verification

Every slice runs its focused owner tests and `cargo check --workspace`; the
final migration additionally requires real plugin-image loading, empty-runtime,
additive-plugin, rollback, counter attribution, and metrics export coverage.
