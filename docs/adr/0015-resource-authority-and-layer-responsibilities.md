# Resource authority and layer responsibilities

Status: accepted

## Context

Hammer has several resource paths whose semantic owner differs from the Rust
value that closes a descriptor or mapping. Without a common vocabulary, code
can mistake observation for ownership, move generic readiness machinery into a
protocol plugin, or replay worker initialization over already-live resources.

VPP is the semantic reference. In particular:

- `vlib/file.c` owns `clib_file_t` registration and dispatch.
- `vnet/session/application.c:429-484` makes an app message-queue readiness
  callback mark pending work and schedule an input node; the node drains work.
- `vnet/session/session_node.c:2033-2164` makes `session_queue_node` update
  transport time and consume session work.
- `vnet/session/transport.c:317-339` registers protocol behavior while
  transport workers retain protocol-private connection and timer state.

Hammer follows those ownership semantics without copying VPP's C structures or
function-table interfaces.

## Vocabulary

- **Resource Authority** decides whether a resource exists and defines its
  invariants. There is exactly one authority for each resource class.
- **Handle Owner** holds the RAII value that releases one concrete fd, mapping,
  allocation, or registration. A duplicated fd has a different Handle Owner
  even when it refers to the same kernel object.
- **Registrar** attaches an already-created resource to another authority. A
  Registrar does not acquire the resource's business behavior.
- **Work Consumer** performs the work signaled by readiness or an event.
  Observation and consumption are separate roles.
- **Failure Translator** maps an owner-local typed failure at a crate seam. It
  may add context but must preserve category and source.

Holding a `RawFd`, `NodeId`, `Index`, queue offset, or Transport Index is
observation or routing, not ownership.

## Responsibility matrix

| Resource class | Creation authority | Handle / lifetime owner | Registration / observation | Work consumer | Destruction | Failure translator |
| --- | --- | --- | --- | --- | --- | --- |
| Ordinary Rust and third-party allocation | process bootstrap selects capacity; `hammer-infra` initializes Main Heap | allocating standard type, backed by the one process Main Heap | any crate may allocate after `READY` | allocating module | value `Drop`; Main Heap authority lasts for the process | `hammer-infra` for initialization/exhaustion facts; process entry for startup presentation |
| SVM mapping and region allocation | `hammer-infra::segment` / `svm_region` | each creating or attaching SVM mapping; borrowed attach fds require an explicit outer owner | attach/session code may exchange a borrowed fd and offsets | FIFO and Message Queue modules operating on offsets | mapping owner unmaps; owning mapping closes its backing fd | infra mapping error, translated once by attach/session owner |
| Physmem mapping and Buffer Arena | `hammer-infra::PhysmemMap`; `hammer-core` selects Buffer Arena policy | `PhysmemMap` inside the Buffer Arena | Graph Runtime and packet modules observe Buffer/Index through `hammer-core` | Data-Plane Buffer and Frame owners | Buffer Arena drops the map; `PhysmemMap` closes backing fd | infra physmem error to core buffer initialization error |
| Local Session Message Queue signal | Session Message Queue | queue atomic state; no fd | Session Runtime observes queue state | `session-queue` Graph Node | queue drop | queue/session owner |
| SVM Session Message Queue signal endpoints | Session Message Queue | queue owns original read/write endpoints; every readiness/async duplicate has its own owner | Session Runtime may register a duplicated read endpoint in the same worker's `FileMain`; app adapter may duplicate for `AsyncFd` | `session-queue` drains dataplane events; app adapter drains app-visible events | each duplicate closes independently; queue closes originals | queue construction error to Session Runtime or app attach error |
| File record and registered descriptor | descriptor's domain owner creates/opens; `hammer-runtime` creates File record | `File` owns the registered descriptor; `FileMain` owns File lifetime and backend interest | platform poller observes only live generation-bearing File records | callback marks/schedules existing work; Graph Node or domain module performs I/O | `FileMain::delete`, unhandled error, or worker teardown removes interest before File drop | runtime File/poller error; caller translates only at its layer seam |
| Device descriptor and queue | device plugin | after registration, worker `File` owns the fd; plugin worker state retains File Index and device facts | device plugin registers with same-worker FileMain and DeviceMain | device Graph Nodes perform packet `readv`/`writev` | device owner removes File before retiring worker state; worker teardown drops both | device plugin translates platform errors; runtime translates readiness errors |
| Graph identity and scheduling | main-thread Graph Runtime | Graph Runtime owns topology, pending frames, states, and scheduling queues | service/plugin code may declare nodes, bind worker-local data, and request scheduling by existing identity | Graph Runtime dispatches; Graph Node consumes Pending Frame | Graph Runtime drains/replaces graph under its lifecycle/barrier contract | runtime graph error; node owner records packet/domain error |
| Session FIFO and Session Message Queue content | Session Runtime / app-session attach owner | session objects own FIFO bytes and queue references; mapping owns backing storage | app and transport cross only approved session interfaces | Session Runtime consumes app-to-session events and prepares TX/RX work | Session Lifecycle coordinates app and transport deletion before storage release | session owner, preserving lower mapping/queue source |
| Session Event | producing app/session owner | queue slot until dequeue; then the consumer's local value | IO events carry session index; control events carry Session Handle | Session Runtime or app adapter according to queue direction | dequeue releases queue slot; stale/unmapped events are dropped | session/app owner; transport never reinterprets event identity |
| Transport connection, lookup, recovery, and timers | protocol transport worker | protocol worker-local pools and timer modules | Session stores only opaque Transport Index and dispatch key | registered transport dispatch and protocol Graph Nodes | protocol state machine deletes transport object; Session Lifecycle observes deletion | protocol plugin, translated to session/runtime only at typed transport calls |
| Plugin image and worker initialization | main-thread `PluginMain` activates images; Graph Runtime publishes topology | activated image lasts for process; each module owns resources installed by its init record | runtime orders lifecycle records; worker init binds only current-worker state | owning module after initialization | no plugin unload; worker resources follow their owner/worker lifetime | plugin loader/runtime for activation; module for owner-local initialization |

## Layer contracts

### `hammer-infra`

May create and release generic allocation, mapping, queue, and memory
primitives. It must not know FileMain, Graph Nodes, Session Events, TCP, or
plugin lifecycle. SVM and Physmem backing descriptors never register with
FileMain.

### `hammer-core`

The narrow packet-graph ABI contract in ADR-0016 supersedes this ADR's earlier
Core ownership list. Core may define only data-plane identity and Buffer/Frame
ownership facts plus errors intrinsic to those primitives. It must not own
configuration, process services, logging, metrics, protocol wire/state, or
forwarding policy; it must not poll descriptors, schedule nodes, own session
queues, or own transport workers.

### `hammer-runtime`

Owns Graph Runtime, FileMain, barriers, worker/main-loop lifecycle, plugin image
activation, and lifecycle ordering. Platform readiness adapters may only add,
modify, delete, and collect interest for existing File records. Runtime must not
interpret Session Events or protocol state.

### `hammer-service`

Owns transport-neutral device registries, interfaces, Session Runtime, Session
Lifecycle, FIFO access, session node scheduling policy, and session event
consumption. It may register owner-created resources through runtime interfaces.
It must not own TCP connections/timers or create protocol-specific runtime APIs.

### Device and protocol plugins

A device plugin may open its device, publish generic queue facts, register the
descriptor in the same worker's FileMain, and perform device I/O in its Graph
Nodes. A protocol plugin may create protocol-private worker state and bind it to
the approved Session Runtime/Graph interfaces. It must not implement generic
session readiness, File polling, graph scheduling policy, or app/session queue
consumption.

### `hammer-app`, daemon, IPC, and CLI

App adapters own their process-local attach and async descriptor duplicates and
use the app/session seam. Daemon and IPC surfaces translate control failures and
present state; they do not acquire data-plane resource ownership. CLI owns only
its client connection and presentation values.

## Runtime observability contract

Runtime statistics follow VPP's `show runtime`, `show trace`, and `show files`
ownership model:

- `hammer-runtime` owns worker-local Node and File counters, bounded Packet
  Trace collection, and publication of a consistent worker snapshot before a
  Data Worker acknowledges the existing barrier.
- Runtime snapshots use the existing VPP-style worker barrier without a second
  timeout, generation, or diagnostic synchronization protocol.
- The daemon may request and format runtime snapshots through the Runtime
  Engine. It must not inspect worker-local state directly, add packet-path
  logging, or introduce device/protocol-specific runtime observation types.
- CI owns host evidence outside the process: interface and route state, process
  and descriptor state, packet capture, and daemon logs. Host setup and capture
  do not move descriptor or packet-I/O ownership out of the TUN plugin.

This split keeps counters and trace state with the Data Worker that produces
them, while preserving host evidence when the process cannot publish a barrier
snapshot.

## File callback contract

A callback may validate its File identity, update callback-private readiness
facts, or schedule/mark already-existing worker-local work. It must return
without draining Session Events, advancing transport time, changing protocol
state, or reading/writing packet payload. The scheduled Graph Node or domain
module is the Work Consumer.

Signal-byte draining is owned with queue consumption. Generic FileMain tests may
read a test descriptor to prove repeated readiness, but that is not production
domain behavior.

## Worker-init contract

Worker init runs after the worker has its published graph and existing runtime
authorities. It may:

- construct protocol/device-private state for the current worker;
- bind worker-local node runtime data through `Engine`;
- ask an owning service module to compile generic worker-local state;
- register an owner-created descriptor with that worker's existing FileMain.

It must not change main graph identity, mutate another worker, create a second
runtime authority, or replay over a live resource without a defined owner-local
replacement transition. Additive graph publication runs only newly required
initialization unless an approved transition first cleans up the old resources.

## Current verification

- `hammer-runtime/tests/file_readiness.rs` proves generation checks, callback
  dispatch, backend removal, and descriptor close behavior.
- `hammer-service/src/session/node/tests.rs` proves queue signal readiness only
  schedules `session-queue`; transport time and Close handling occur after node
  dispatch.
- `hammer-runtime/tests/session_msg_queue.rs` proves queue-owned signal endpoint
  lifetime and independent File readiness duplicate lifetime.
- `hammer-service/tests/device_queue_affinity.rs` and
  `device_output_runtime.rs` prove worker-local device queue publication.
- ADR-0004, ADR-0008, ADR-0010, ADR-0012, and ADR-0014 remain authoritative for
  graph, transport, event identity, queue concurrency, and Main Heap details.

## Confirmed remediation order

1. #113 fixes concrete SCM_RIGHTS/SVM/async descriptor lifetime gaps; it is
   independent and may proceed immediately.
2. #111 fixes additive graph publication replay over established worker-owned
   resources.
3. #112, blocked by #111, moves generic SVM Session Message Queue readiness out
   of the TCP plugin and gives Session Runtime the install/remove lifecycle.
4. #108 then migrates remaining stringly errors after the owning seams are
   stable, preserving OS sources and owner-local categories.

No new public type or API is approved by this ADR. Each remediation must reuse
existing surfaces or obtain explicit approval under the repository's interface
rules.
