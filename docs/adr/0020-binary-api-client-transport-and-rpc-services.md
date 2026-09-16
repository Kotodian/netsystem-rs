# ADR-0020: External Client Repository, Binary API Transport, and RPC Services

- Status: accepted
- Date: 2026-09-16
- Supersedes: ADR-0018 section 4, `Client surface`
- Amends: ADR-0017 client ownership where it places the client and server
  `ApiMain` in the same ownership path
- Related: ADR-0016, ADR-0017, ADR-0019

## 1. Context

The previous draft of this ADR was wrong in two ways:

1. It treated `hammer-infra`'s allocator as if it were the allocation policy
   for every Hammer process. Hammer does not have a rule that every process and
   every allocation must use the Main Heap. The existing daemon initializes
   `hammer-infra` and uses its allocator where the daemon has chosen to do so.
   That is a daemon decision, not a client requirement.
2. It invented public transport and allocator types to work around a linking
   problem. Those types are not in VPP and are not needed in Hammer.

The actual boundary is narrower and concrete:

- An external process may depend on the Binary API client and the stats client.
- Those dependency graphs must not contain `hammer-infra`.
- `hammer-infra` currently contains the process-wide declaration at
  `crates/hammer-infra/src/mem/mod.rs:1966-1967`. Linking that crate into an
  external binary brings that declaration with it.
- The correct fix is dependency isolation and relocation of existing
  allocator-free code. It is not a new allocator, a feature flag, a fake
  `ApplicationAllocator`, or a requirement that the external process call
  `MainHeapConfig::initialize()`.

The client is the connection and protocol transport boundary. It is not the
owner of VPE, interface, plugin, or application business operations. The RPC
service layer owns those operations. `VpeService::show_version()` is the first
concrete service method.

`third_party/vpp/src/vlibmemory/memory_client.c` is not a source for this
design. That file contains the legacy create-v1, receive-thread, and `setjmp`
path. The V2 client path in `vapi.c` and the V2 server handler in
`memory_api.c` are the design sources.

## 2. VPP Evidence

The following source facts define the boundary. The paths are relative to
`third_party/vpp/`.

| Concern | Source evidence | Consequence for Hammer |
| --- | --- | --- |
| V2 registration | `src/vlibmemory/memclnt.api:239-254` defines `memclnt_create_v2` with `context`, `ctx_quota`, `input_queue`, `name`, `api_versions`, and `keepalive`. The reply contains `context`, `response`, `handle`, `index`, and `message_table`. | The client owns connection registration, its input queue, and the imported message table. No business message is part of this handshake. |
| V2 server ownership | `src/vlibmemory/memory_api.c:209-287` creates a server-side registration, records the client queue, serializes the message table, and sends `memclnt_create_v2_reply`. | The server `ApiMain` remains server-owned. The client must not use `ApiMain::current()` or the server registration pool as its connection state. |
| Transport-only common API | `src/vpp-api/vapi/vapi.h:23-33` states that the common VAPI declarations are "only the transport layer"; generated higher-level APIs are separate. | `Client` owns connection, send, receive, request correlation, message-ID translation, and lifecycle. It does not own VPE methods. |
| Opaque connection context | `src/vpp-api/vapi/vapi.c:72-96` shows `vapi_ctx_s` containing connection state, request slots, message-ID maps, the input queue, and the selected transport fields. | A single Hammer `Client` value is the connection context. A public transport trait or transport wrapper is not required. |
| Message allocation and release | `src/vpp-api/vapi/vapi.c:223-275` selects SHM allocation with `vl_msg_api_alloc_as_if_client_or_null` and socket allocation with `vec_validate_init_empty`, then releases through the matching path. | Ordinary client Rust values and shared API messages have different allocation domains. The client must keep those domains paired correctly. |
| V2 connection path | `src/vpp-api/vapi/vapi.c:646-720` creates the client queue, sends `memclnt_create_v2`, receives its reply, and imports the message table. | Hammer's SHM client follows this sequence. It does not require a server-side `ApiMain` object in the external process. |
| Backend selection | `src/vpp-api/vapi/vapi.c:959-1084` makes `vapi_connect_ex(..., use_uds)` choose the SHM or UDS path and publish connection state. | SHM is the implemented backend now. Socket is a future backend of the same connection abstraction, not a public stub. |
| Generic send and receive | `src/vpp-api/vapi/vapi.c:1420-1620` selects SHM or socket internally, handles keepalives in the receive path, and keeps transport operations separate from message business. | `Client` owns generic send/receive and keepalive handling. RPC services call it rather than calling raw queue operations. |
| Generated operation path | `src/vpp-api/vapi/vapi_c_gen.py:669-739` generates operations which allocate a request, assign a context, call `vapi_send`, and then dispatch the typed reply. | The typed operation belongs to a service layer. The client only provides the request/reply mechanism. |
| Language-neutral API declaration | `src/tools/vppapigen/VPPAPI.rst:4-13` defines one API language for the RPC interface and states that the compiler emits JSON or C. | Binary API declarations are a shared protocol artifact; a Rust crate layout is not the protocol contract. |
| Per-language binding generation | `src/tools/vppapigen/generate_go.py:120-149` consumes generated JSON definitions with GoVPP's `binapi-generator`; `src/cmake/api.cmake:106-173` generates C and C++ VAPI headers from the same JSON input. | `netsystem-client` consumes a versioned schema and owns language-specific bindings and RPC services. Bindings do not redefine message identity or server behavior. |
| VPE ownership | `src/vpp/api/vpe.api:56-81` declares `show_version` and its reply; `src/vpp/api/api.c:93-112` implements the server-side handler. | `VpeService`, not `Client`, owns `show_version()`, its request construction, reply decoding, and `retval` interpretation. |
| Independent stats client | `src/vpp-api/client/stat_client.c:42-137` connects a Unix socket, receives an fd with `SCM_RIGHTS`, opens the segment read-only with `mmap(PROT_READ)`, and decodes the stats directory separately from the Binary API transport. | The Hammer stats client is not a Binary API service and must not depend on the Binary API client or the server Binary API owner. |

GoVPP is a useful cross-check for the service boundary, not a Rust API
template. Its `api.Connection` exposes only generic `NewStream`, `Invoke`,
`WatchEvent`, and `CheckCompatibility` operations. Generated services such as
`vpe.RPCService` own `ShowVersion`, call `Connection.Invoke`, and translate a
non-zero `retval` after the transport call succeeds. Hammer keeps that
relationship without introducing a dynamic RPC registry.

## 3. Decisions

### 3.1 Standalone language-neutral client repository

All client implementations live in a separate Git repository at this path:

```text
netsystem-rs/
    .gitignore
    Cargo.toml
    crates/
    netsystem-client/
        .git/
        README.md
        schema/
        rust/
            Cargo.toml
            Cargo.lock
            crates/
        go/
        python/
```

`netsystem-client/` is an independent repository, not a submodule:

- The parent repository must contain no `.gitmodules` entry for it and no
  gitlink at `netsystem-client`.
- The parent `.gitignore` must contain `/netsystem-client/`. The parent
  repository therefore does not track the child repository, its files,
  branches, tags, or lockfile.
- The parent Cargo workspace must exclude `netsystem-client` and must not list
  client crates as members. The parent has no client crate to move.
- The child repository has its own formatting configuration, CI, tags, and
  release process. Its Rust binding has its own Cargo workspace and lockfile
  under `rust/`; the repository root is not a Rust workspace.
- All client development commands run from `netsystem-client/`; a parent
  `cargo build --workspace` does not build or test the client repository.

The client repository is independent of the parent repository's Git history.
It does not use a submodule, subtree, or parent gitlink. Cross-repository
integration is an explicit checkout or dependency at a pinned revision, not a
Git workspace relationship.

The server repository remains the home of the canonical protocol declarations,
the allocator-free shared leaf packages, and the server API implementation.
For each release it publishes a versioned language-neutral schema, equivalent
in role to VPP's generated `.api.json` definitions. The client repository
vendors that schema snapshot and generates or writes each language binding from
it. The schema contains message names, CRCs, field layouts, service
relationships, and protocol versions; it does not contain Rust types or
client-side error policy.

The Rust binding may consume shared packages through an immutable versioned
registry or Git revision. A committed client manifest must not contain a
relative path dependency into `../netsystem-rs/crates/**` or any other parent
checkout path. Other language bindings decode the same schema without linking
Hammer Rust crates.

For local cross-repository work, a developer may use an uncommitted Cargo
patch or local configuration override pointing at the parent checkout. That
override is a development convenience, not part of the repository contract.
The client CI builds from its own repository using pinned package versions or
revisions, and the compatibility job selects the server revision it is
testing against.

### 3.2 Language bindings and Rust packages

Each language binding owns its public packaging and service tree. The repository
must not require another language to depend on Rust crates, generated Rust
types, or Cargo package names.

The initial Rust binding may use these internal packages:

- `hammer-binary-api-client` owns the Binary API connection abstraction and
  the SHM backend implementation.
- `hammer-binary-api-rpc` owns client-side RPC services. Its first service is
  `VpeService`.
- `hammer-stats-client` owns stats socket handoff, read-only mapping, directory
  traversal, and decoded stats values.

These are Rust implementation packages, not the repository contract. Go,
Python, C, or another binding may organize its client and services differently
while preserving the same connection and RPC semantics.

The client and stats client are not features of one another, do not share a
connection object, and do not re-export a server `ApiMain` or `StatsMain`.

The shared protocol and shared-memory declarations move to allocator-free leaf
crates. The initial migration should use these ownership layers:

```text
hammer-shmem
    existing SvmQueue, SvmRegion, MemHeap, and their existing config/error types

hammer-binary-api-protocol
    existing Api, Service, Message, MessageId, codec, message declarations,
    MsgBuf/ShmemHeader shared Binary API layouts

hammer-stats-protocol
    existing SharedHeader, DirectoryEntry, DirectoryType, metric and decoded
    value types
```

`hammer-shmem` and the protocol crates contain moved code, not new
transport-style abstractions. They must not contain a global allocator, a
`MainHeapConfig`, `MemMain`, or a dependency on `hammer-infra`.

`MemHeap` in this split is an instance used for a mapped region, not the
process-wide Rust allocator. Moving the existing region heap support does not
select it as `#[global_allocator]`, and it does not make the external client
initialize or replace `MemMain`.

Server-only checks and ownership stay in the server crate. In particular,
`hammer_runtime::thread_main::ensure_main_thread`, `ApiMain` registration and
dispatch, the server client pool, dead-client scanning, and handler execution
must not move into an allocator-free leaf crate. The existing allocation role
logic may move, but the server call site remains responsible for enforcing
that server rings run on the main thread.

`hammer-infra` may depend on `hammer-shmem` and re-export the moved primitives
for existing server users. That direction is valid because `hammer-infra` is
the upper server crate. The reverse dependency is forbidden.

### 3.3 Client is one concrete abstraction, not a public transport hierarchy

The public client is a single value:

```rust
pub struct Client {
    // Private connection state: mapped API region, input queue, request
    // correlation, message-ID maps, keepalive state, and backend selection.
}
```

The public methods are:

```rust
impl Client {
    pub async fn connect(
        name: &str,
        api_segment: &std::path::Path,
        response_queue_size: std::num::NonZeroU32,
        max_outstanding_requests: std::num::NonZeroUsize,
        handle_keepalives: bool,
    ) -> Result<Self, Error>;

    pub fn client_index(&self) -> Option<u32>;

    pub async fn invoke<Request, Reply>(&mut self, request: Request) -> Result<Reply, Error>
    where
        Request: hammer_binary_api_protocol::Api,
        Reply: hammer_binary_api_protocol::Api;

    pub async fn disconnect(&mut self) -> Result<(), Error>;
}
```

`invoke` validates the existing `Request::SERVICE` metadata before publishing
the request. The declared service reply must equal `Reply::NAME`. The method
sets the request header from the connection, allocates the request in the API
region, assigns a private context, sends it through the selected backend,
waits for the matching reply, decodes it, and returns the owned reply. It does
not interpret a business `retval`; the owning RPC service does that.

The following types are explicitly not part of this API:

```text
ClientTransport
TransportMessage
ShmemTransport
SocketTransport
SharedMapping
SharedQueue
SharedRegionAllocator
SharedMessage
EventRegistration
EventSubscription
MainHeapAllocator
```

They are not public, not private, and not placeholders. A private enum or
private fields may be used inside `Client` later to hold an implementation
backend, but callers never name a backend type, trait, or wrapper.

This is what "the client is an abstraction" means here. VPP's `vapi_ctx_t` is
also an opaque connection context whose implementation selects SHM or a socket
internally. The abstraction is the connection behavior, not a Rust trait
hierarchy.

### 3.4 SHM ownership is split between server and external client

The server side owns:

- the API segment backing file and root region;
- the server input queue and server-side registrations;
- the serialized server message table;
- server-side message allocation rings and Data Heap;
- server handlers and message-ID registration.

The external client side owns:

- its process-local mapped view of the API segment;
- its client queue allocated in the shared API region;
- its `memclnt_create_v2` handshake and returned client index;
- its copy of the server message table;
- its outstanding request table and context counter;
- its decoded replies and other ordinary Rust state.

The external client does not own or call the server's `ApiMain`, its
registration pool, its handler table, or its Main Heap lifecycle. It uses the
moved shared-memory types to map and queue, but it initializes only its own
connection state. This is the "external self initialization" boundary: the
client initializes a connection and its queue, not the process global
allocator.

The daemon may construct the server side with its existing `hammer-infra`
allocation policy. This ADR does not change whether, when, or how the daemon
uses the Main Heap.

### 3.5 Socket support is deferred without a stub

Socket support is not implemented in this ADR. There is no
`SocketTransport`, no `Client::connect_socket`, no `Unsupported` error variant,
and no feature flag that advertises a backend which cannot work.

When socket support is implemented, it is added behind the same `Client`
methods. The RPC service signatures remain unchanged. A future socket backend
may own socket framing and local message buffers internally; it must not make
the public client return a transport-specific type.

## 4. RPC Service Boundary

### 4.1 `VpeService`

`hammer-binary-api-rpc` owns the client-side VPE service:

```rust
pub struct VpeService<'a> {
    client: &'a mut hammer_binary_api_client::Client,
}

impl<'a> VpeService<'a> {
    pub fn new(client: &'a mut hammer_binary_api_client::Client) -> Self;

    pub async fn show_version(&mut self) -> Result<ShowVersionReply, VpeError>;
}
```

The service is concrete. There is no generic `RpcService` trait, service
registry, callback map, or dynamic dispatch requirement. A future interface,
plugin, or application service is another concrete service type in the crate
that owns its protocol.

`show_version()` owns all of the following:

1. constructing the `ShowVersion` request;
2. calling `Client::invoke::<ShowVersion, ShowVersionReply>`;
3. validating the returned reply type;
4. converting a non-zero VPE `retval` into `VpeError::Rejected`;
5. returning the owned reply fields only on success.

The error split is deliberate:

```rust
pub enum VpeError {
    Client(hammer_binary_api_client::Error),
    Rejected { retval: i32 },
}
```

A connection, context, timeout, allocation, queue, or codec failure remains a
client error. A successfully received VPE reply with a non-zero `retval` is a
VPE service error. The client must not convert between those categories.

### 4.2 Protocol declarations

`ShowVersion` and `ShowVersionReply` are protocol declarations and must be
shared with the server. They live in an allocator-free protocol crate, not in
the client crate and not in the stats client. Their codec and CRC generation
are protocol facts, not business operations.

The service operation and `retval` policy live only in
`hammer-binary-api-rpc`. The server handler and VPE build metadata remain
server-owned.

Business methods must never be added to `Client`. In particular:

- `Client` has no `show_version`, `control_ping` business wrapper, interface
  method, or plugin method;
- `Client` does not install server handlers;
- `Client` does not expose the server registration pool;
- RPC services do not implement transport queues or region allocation.

## 5. Stats Client Boundary

`hammer-stats-client` is independent of the Binary API client:

```rust
pub struct StatsClient {
    // Private read-only mapped stats segment.
}

impl StatsClient {
    pub fn connect(socket_path: impl AsRef<std::path::Path>) -> Result<Self, Error>;
    pub fn list(&self) -> Result<Vec<String>, Error>;
    pub fn read(&self, name: &str) -> Result<MetricValue, Error>;
}
```

The stats client owns:

- the Unix socket connection;
- `SCM_RIGHTS` descriptor handoff;
- `fstat` and read-only `mmap` of the received descriptor;
- validation of the shared stats header and directory;
- relocation and decoding of `MetricValue` using the shared layout;
- ordinary `Vec` and `String` allocations decoded into the embedding
  process's allocator.

The stats client does not own:

- `StatsMain`, the server socket listener, or the server segment;
- the server-created writable Data Heap;
- the Binary API region, queues, or message table;
- a global allocator or `MainHeapConfig`.

The shared stats protocol types move out of server `hammer-stats` into
`hammer-stats-protocol`:

```text
SharedHeader
DirectoryEntry
DirectoryType
DirectoryIndex
Counter
ScalarBits
Gauge
MetricValue and related decoded metric values
```

The move preserves the existing layout and methods. It does not create a new
`StatsMapping`, `StatsDirectory`, or `StatsValue` hierarchy. The server
`hammer-stats` crate depends on `hammer-stats-protocol` and `hammer-infra`.
The external `hammer-stats-client` crate depends on `hammer-stats-protocol`
and ordinary OS dependencies only. It must not depend on `hammer-stats`, the
Binary API client, or `hammer-infra`.

## 6. Allocator Isolation

### 6.1 The problem is link reachability

Rust's `#[global_allocator]` is effective for the final linked program when the
crate containing it is linked. A dependency edge is therefore not harmless
merely because the external client never calls `MainHeapConfig::initialize()`.

This is rejected:

```text
external application
    -> hammer-binary-api-client
        -> hammer-infra
            -> #[global_allocator] MEM_MAIN
```

This is required:

```text
external application
    -> hammer-binary-api-client
        -> hammer-binary-api-protocol  -> no hammer-infra
        -> hammer-shmem                -> no hammer-infra

external application
    -> hammer-stats-client
        -> hammer-stats-protocol      -> no hammer-infra
```

The external application keeps whatever allocator it already owns. It does not
declare an allocator for the client, and the client does not declare one for
the application.

### 6.2 Existing `hammer-infra` behavior is not changed

`crates/hammer-infra/src/mem/mod.rs` keeps its current `MEM_MAIN` declaration
and `MainHeapConfig` behavior. This ADR does not:

- move the global allocator to the daemon executable;
- add `MainHeapAllocator`;
- change daemon Main Heap initialization;
- state that all Hammer allocations must use the Main Heap;
- require external applications to initialize a Hammer heap.

Those changes would be a separate allocation-policy ADR. They are unnecessary
for the client boundary and are out of scope here.

### 6.3 Allocation domains

| Allocation | Allocator or owner | Release |
| --- | --- | --- |
| Client maps, request tables, decoded replies, diagnostic strings | The embedding process's existing allocator | Ordinary Rust drop |
| `Client::connect` queue and shared header | The shared API region | Client disconnect and region cleanup |
| API request and reply payloads | The shared API region allocation contract | Matching region release after send or receive |
| Server `ApiMain`, handlers, registration pools | Existing daemon ownership and its chosen allocation policy | Existing server lifecycle |
| Stats client decoded vectors and strings | The embedding process's existing allocator | Ordinary Rust drop |
| Stats directory and payload bytes | The server stats segment, mapped read-only | `StatsClient` drop or unmap |

The client does not allocate shared API messages through its process global
allocator, and it does not assume that the server's Main Heap is available in
the client process. Conversely, the embedding process's allocator is not used
to free a shared API region allocation.

### 6.4 Verification rule

The boundary is checked from an actual external consumer in each implemented
language. For the Rust binding, the check uses a real crate outside this
repository:

1. The consumer has its own `#[global_allocator]` or uses the platform
   allocator.
2. It depends directly on `hammer-binary-api-client`,
   `hammer-binary-api-rpc`, and/or `hammer-stats-client`.
3. Its normal dependency tree contains no `hammer-infra` and, for the stats
   client, no server `hammer-stats`.
4. It can build and run a V2 SHM connection without calling
   `MainHeapConfig::initialize()`.

A source-text search for forbidden imports is not a replacement for this
check. The relevant proof is the compiled dependency graph and the external
binary's behavior at link time. Other language bindings must prove the same
property through their package graph and must not link the server executable or
server-only packages.

## 7. Required Dependency Direction

```text
hammer-shmem
    -> libc, posix-sync, and existing low-level dependencies only

hammer-binary-api-protocol
    -> hammer-shmem
    -> codec/serde/macro dependencies only

hammer-binary-api-client
    -> hammer-binary-api-protocol
    -> hammer-shmem
    -> tokio, thiserror, tracing
    -X-> hammer-infra
    -X-> hammer-ipc server ownership
    -X-> hammer-stats
    -X-> hammer-runtime

hammer-binary-api-rpc
    -> hammer-binary-api-client
    -> hammer-binary-api-protocol

hammer-stats-protocol
    -> no hammer-infra

hammer-stats-client
    -> hammer-stats-protocol
    -> memmap2, socket2, libc
    -X-> hammer-infra
    -X-> hammer-stats server ownership
    -X-> hammer-binary-api-client

hammer-infra
    -> hammer-shmem
    -> existing daemon and dataplane dependencies

hammer-stats
    -> hammer-stats-protocol
    -> hammer-infra

hammer-ipc server API
    -> hammer-binary-api-protocol
    -> hammer-shmem
    -> hammer-infra, hammer-runtime as required by the server
```

The direction of every client arrow is one-way. The protocol and shared-memory
leaf crates never depend on a client binding, RPC service, server handler, the
stats server, or `hammer-infra`.

This dependency graph is split across two repositories without changing the
arrow direction. The parent repository owns the canonical schema, shared leaf
packages, and server. The `netsystem-client` repository owns every external
client implementation. Its `rust/` workspace consumes the Rust leaf packages
at pinned versions; other language directories consume the versioned schema
and own their binding implementations. The parent does not depend on
`netsystem-client`, and the client repository does not use a parent-relative
path dependency in its committed manifests.

## 8. Migration

1. Create `netsystem-client` as an independent Git repository under the parent
   checkout. Add `/netsystem-client/` to the parent `.gitignore`, add it to the
   parent workspace `exclude`, and ensure no `.gitmodules` entry or gitlink is
   created.
2. Remove every parent client crate, CLI/client binary, client export, and
   client-driven test. Keep only the server-side socket envelope, server
   registration code, and protocol declarations that both sides consume.
3. Publish a versioned language-neutral schema from the server build and make
   it consumable by the child repository through an immutable artifact. The
   committed client repository must not use a parent-relative path dependency.
4. Put the Rust binding in `netsystem-client/rust/`; it owns that Cargo
   workspace and lockfile. The repository root is not a Rust workspace and no
   other language is required to consume Rust packages.
5. Extract the existing allocator-free shared-memory primitives from
   `hammer-infra::svm` and the existing shared Binary API message layout from
   `hammer-ipc::binary_api`, preserving their names and behavior. Do not add a
   second protocol implementation in the server.
6. Extract the existing Binary API definitions, codec, message inventory, and
   message-table decoding into `hammer-binary-api-protocol`. The server uses
   the same declarations, and the Rust binding consumes published packages.
7. Implement `hammer-binary-api-client` in the Rust binding so its connection
   state is owned by `Client`, not by `hammer_infra::ApiMain`. Remove the
   `ApiMain::current()` dependency and remove `ShowVersion` from the client
   crate.
8. Move `show_version` operation and error handling to
   `hammer-binary-api-rpc::VpeService`.
9. Extract the existing stats layout and value types into
   `hammer-stats-protocol`; retarget server `hammer-stats` and
   `hammer-stats-client` to that crate.
10. Remove old `hammer-ipc` client/stats exports that would preserve the
   dependency path back to `hammer-infra`. Compatibility re-exports must not
   silently reintroduce the allocator or server ownership.
11. Leave socket support unimplemented. Do not add a placeholder constructor or
   unsupported runtime branch.

The client implementations that currently import `ApiMain` and `ShowVersion`
are removed from the parent repository. They are migration inputs only if a
future client task deliberately reuses allocator-free transport logic.

## 9. Acceptance Criteria

- The parent repository contains no Binary API, stats, RPC, or CLI client
  implementation, no client export, and no client-driven test.
- The parent has no gitlink or `.gitmodules` entry at `netsystem-client`, and
  its `.gitignore` and Cargo workspace exclude the sibling repository.
- The child repository is language-neutral; its Rust binding has its own
  workspace and lockfile under `rust/`, and other bindings do not depend on it.
- The child repository builds and tests without a committed path dependency on
  the parent checkout.
- Every binding consumes the same versioned schema and does not depend on
  server `hammer-infra`, server `hammer-ipc`, or server stats ownership.
- An external crate can depend directly on `hammer-binary-api-client` and
  `hammer-binary-api-rpc` while retaining its own global allocator.
- An external crate can depend directly on `hammer-stats-client` without
  linking `hammer-stats` or `hammer-infra`.
- `Client` exposes only connection lifecycle and generic typed invocation; it
  has no VPE, interface, plugin, or application method.
- `VpeService::show_version()` constructs the request, invokes the generic
  client, decodes `ShowVersionReply`, and returns a typed error for a non-zero
  `retval`.
- A real V2 SHM connection performs `memclnt_create_v2`, imports the message
  table, and completes a request/reply cycle.
- Shared API messages are released through their originating API region, while
  ordinary client values use the embedding process's allocator.
- The stats client receives the segment descriptor with `SCM_RIGHTS`, maps it
  read-only, and decodes the directory without using the Binary API.
- Socket support has no public type, feature, constructor, or unsupported
  error path in this change.

## 10. Rejected Designs

1. A `ClientTransport` trait, `TransportMessage`, `ShmemTransport`, or
   `SocketTransport` public hierarchy. VPP's SHM/socket choice is internal to
   one opaque connection context, and Hammer has no need for a second public
   abstraction.
2. A `hammer-infra` global allocator feature flag or a new
   `MainHeapAllocator`. Cargo features are additive, and changing the
   allocator policy does not solve the external client dependency graph.
3. Treating the Main Heap as the allocator of every Hammer process. The daemon
   may use it; the external client does not inherit that choice.
4. Keeping `show_version()` on `Client`. It belongs to `VpeService`, which owns
   the operation and `retval` semantics.
5. A generic `RpcService` trait or dynamic service registry. Concrete service
   types over one static `Client` are sufficient and preserve ownership.
6. A stats client built on the Binary API client or on server `hammer-stats`.
   Stats has its own socket and read-only mapping protocol.
7. A socket stub that returns unsupported. The backend is deferred until its
   framing and lifecycle contract is designed.
8. Reusing `memory_client.c` as a template. Its create-v1, receive thread, and
   `setjmp` behavior are outside the V2 design.

## 11. Consequences

Each language binding exposes one transport-facing client type that can connect
to the V2 SHM server without exposing `ApiMain` or any allocator policy. RPC
services own typed protocol operations, so adding a new service does not add a
method or message-specific field to the client.

The external dependency graph contains only allocator-free protocol and
shared-memory leaves. An external process therefore keeps its own global
allocator and does not initialize or replace Hammer's Main Heap. The daemon's
existing allocation behavior remains unchanged.

The cost is a real repository, schema, and code-generation boundary. That is
acceptable because it removes the link-time allocator hazard, keeps all client
code out of the server repository, and lets language bindings evolve without
turning one Rust crate layout into the protocol contract. It does not require
the generic transport, allocator, event, or message-wrapper types proposed by
the rejected draft.
