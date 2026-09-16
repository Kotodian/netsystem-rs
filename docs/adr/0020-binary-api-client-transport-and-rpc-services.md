# ADR-0020: External Client Repository, Binary API Transport, and RPC Services

- Status: accepted
- Date: 2026-09-16
- Supersedes: ADR-0018 section 4, `Client surface`
- Amends: ADR-0017 client ownership where it places the client and server
  `ApiMain` in the same ownership path
- Related: ADR-0016, ADR-0017, ADR-0019

## 1. Context

The previous draft of this ADR was wrong in three ways:

1. It treated `hammer-infra`'s allocator as if it were the allocation policy
   for every Hammer process. Hammer does not have a rule that every process and
   every allocation must use the Main Heap. The existing daemon initializes
   `hammer-infra` and uses its allocator where the daemon has chosen to do so.
   That is a daemon decision, not a client requirement.
2. It invented public transport and allocator types to work around a linking
   problem. Those types are not in VPP and are not needed in Hammer.
3. It treated the Rust binding as the client repository contract. Rust types,
   Cargo crates, traits, and `#[global_allocator]` are language runtime
   objects; they cannot define a protocol that Go, Python, C, C++, or Lua must
   consume.

The actual boundary is narrower and concrete:

- An external process may depend on a language binding for the Binary API
  client and the stats client.
- No binding may be required to depend on Rust crates or a Rust runtime
  allocator, and no client dependency graph may contain `hammer-infra`.
- `hammer-infra` currently contains the process-wide declaration at
  `crates/hammer-infra/src/mem/mod.rs:1966-1967`. Linking that crate into an
  external binary brings that declaration with it.
- The correct fix is a versioned language-neutral schema plus an independently
  owned client SHM implementation with an explicit region-local heap instance.
  The shared-memory transport core is an internal cross-language boundary, not
  a public Rust type hierarchy. It is not a new process allocator, a feature
  flag, a fake `ApplicationAllocator`, or a requirement that the external
  process call `MainHeapConfig::initialize()`.

The client core may include a private allocator implementation, including a
dlmalloc/mspace-compatible allocator, when the shared region's published ABI
requires it. That implementation belongs to the connection context and
operates only on the mapped region's Data Heap. It must not become the host
process allocator, install allocator hooks, export `malloc`/`free`, call
`clib_mem_init`/`vac_mem_init`, or declare Rust's `#[global_allocator]`.

Shared memory does not require the client to be written in the same language
as the server. The operating system shares bytes and process-shared locks, not
Rust values. What must be shared is a stable ABI: field layout, alignment,
endianness, queue representation, ring representation, allocator metadata,
lock protocol, and ownership rules. A common C ABI implementation is one way
to share those rules; a language binding may also implement the same published
layout natively. Rust types and Rust trait objects cannot be that ABI.

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
| Message allocation and release | `src/vpp-api/vapi/vapi.c:223-275` selects SHM allocation with `vl_msg_api_alloc_as_if_client_or_null` and socket allocation with `vec_validate_init_empty`, then releases through the matching path. | Binding-local values and shared API messages have different allocation domains. The client must keep those domains paired correctly. |
| Client queue allocation | `src/vpp-api/vapi/vapi.c:669-672` calls `svm_queue_alloc_and_init`; `src/svm/queue.c:61-72` allocates that queue with `clib_mem_alloc_aligned`; `src/vlibapi/api_shared.c:969-981` selects the mapped API region's Data Heap. | The client creates and owns its input queue in the shared API region. The server does not pre-create queue storage and the client does not claim a server-owned queue slot. |
| Message ring allocation | `src/vlibapi/memory_shared.c:77-147` allocates requests and replies from `vl_rings` or `client_rings`, marks the selected slot with `msgbuf_t::q`, and garbage-collects abandoned slots after 10 seconds. `src/vlibapi/memory_shared.c:150-171` falls back to shared-heap allocation when no ring slot fits. | VPP-style message rings are required. A ring-only rule that removes the shared-heap fallback is not VPP behavior. |
| V2 connection path | `src/vpp-api/vapi/vapi.c:646-720` creates the client queue, sends `memclnt_create_v2`, receives its reply, and imports the message table. | Hammer's SHM client follows this sequence. It does not require a server-side `ApiMain` object in the external process. |
| Backend selection | `src/vpp-api/vapi/vapi.c:959-1084` makes `vapi_connect_ex(..., use_uds)` choose the SHM or UDS path and publish connection state. | SHM is the implemented backend now. Socket is a future backend of the same connection abstraction, not a public stub. |
| Generic send and receive | `src/vpp-api/vapi/vapi.c:1420-1620` selects SHM or socket internally, handles keepalives in the receive path, and keeps transport operations separate from message business. | `Client` owns generic send/receive and keepalive handling. RPC services call it rather than calling raw queue operations. |
| Generated operation path | `src/vpp-api/vapi/vapi_c_gen.py:669-739` generates operations which allocate a request, assign a context, call `vapi_send`, and then dispatch the typed reply. | The typed operation belongs to a service layer. The client only provides the request/reply mechanism. |
| Language-neutral API declaration | `src/tools/vppapigen/VPPAPI.rst:4-13` defines one API language for the RPC interface and states that the compiler emits JSON or C. | Binary API declarations are a shared protocol artifact; a Rust crate layout is not the protocol contract. |
| Per-language binding generation | `src/tools/vppapigen/generate_go.py:120-149` consumes generated JSON definitions with GoVPP's `binapi-generator`; `src/cmake/api.cmake:106-173` generates C and C++ VAPI headers from the same JSON input. | `netsystem-client` consumes a versioned schema and owns language-specific bindings and RPC services. Bindings do not redefine message identity or server behavior. |
| Go binary API transport | GoVPP's `adapter` tree contains `socketclient` for the Binary API and a separate `statsclient` for shared-memory stats. Its README describes the Binary API adapter as a "Pure Go implementation of VPP binary API protocol (socketclient)". | Go does not reimplement the VPP shared-memory Binary API transport. The language-neutral Binary API transport over sockets is implementable per language. |
| Python binary API transport | The vendored and current upstream `vpp_papi` package contains `vpp_transport_socket.py`; there is no `vpp_transport_shmem.py` Binary API transport. | Python follows the same split: native Binary API socket transport and separate stats shared-memory decoding. |
| Rust shared-memory transport | The `vpp-api-transport` crate's `shmem` backend links `libvppapiclient.so`; its build script requires `libvppapiclient` and bindgen generates the `vac_*` ABI. | A real Rust shared-memory client delegates allocator, queue, and connection mechanics to the C client library instead of reimplementing VPP's region heap in Rust. Rust is therefore a consumer of the cross-language boundary, not the boundary itself. |
| Lua shared-memory transport | `src/vpp-api/lua/vpp-lapi.lua` declares and calls the same `vac_connect`, `vac_read`, `vac_write`, and `vac_free` ABI. | Multiple non-C languages converge on the C ABI for shared-memory Binary API access. |
| C shared-memory client core | `src/vpp-api/CMakeLists.txt:18-24` builds `libvppapiclient.so` from `client.c` and links `vppinfra`, `vlibmemoryclient`, and pthread. `vac_mem_init()` initializes the client process heap used by that shared-memory path. | The reusable cross-language SHM boundary is a C ABI transport core, not a per-language copy of the region heap and queue implementation. Hammer keeps the C ABI structure and may reuse the region allocator internally, but it does not copy `vac_mem_init` or install a process-global heap. |
| VPE ownership | `src/vpp/api/vpe.api:56-81` declares `show_version` and its reply; `src/vpp/api/api.c:93-112` implements the server-side handler. | `VpeService`, not `Client`, owns `show_version()`, its request construction, reply decoding, and `retval` interpretation. |
| Independent stats client | `src/vpp-api/client/stat_client.c:42-137` connects a Unix socket, receives an fd with `SCM_RIGHTS`, opens the segment read-only with `mmap(PROT_READ)`, and decodes the stats directory separately from the Binary API transport. | The Hammer stats client is not a Binary API service and must not depend on the Binary API client or the server Binary API owner. |

GoVPP is a useful cross-check for the service boundary, not a Rust API
template. Its `api.Connection` exposes only generic `NewStream`, `Invoke`,
`WatchEvent`, and `CheckCompatibility` operations. Generated services such as
`vpe.RPCService` own `ShowVersion`, call `Connection.Invoke`, and translate a
non-zero `retval` after the transport call succeeds. Hammer keeps that
relationship without introducing a dynamic RPC registry.

The cross-language transport comparison is:

| Client surface | Official or established implementation | Binary API transport | Consequence |
| --- | --- | --- | --- |
| C and C++ | `libvppapiclient.so` plus generated C API headers | Shared memory and the C `vac_*` ABI | The C ABI is the common implementation boundary for native clients that do not implement the shared-memory protocol themselves. |
| Lua | `vpp-lapi.lua` loads `libvppapiclient.so` through `ffi.load` and calls `vac_connect`, `vac_read`, `vac_write`, and `vac_free` | Shared memory through the same C ABI | A non-C language can use shared memory without reimplementing the region heap. |
| Rust | `vpp-api-transport` 0.1.5/0.1.6 | `shmem` links `libvppapiclient.so`; `afunix` is a native Unix-socket implementation | Rust resolves the shared-memory path through C FFI. This is direct evidence that shared memory is not language-bound. |
| Go | GoVPP `adapter/socketclient` and `adapter/statsclient` | Binary API is pure-Go Unix socket; stats use a separate read-only shared-memory client | Go does not use VPP shared memory for the Binary API, but that is an upstream implementation choice, not a language restriction. |
| Python | `vpp_papi/vpp_transport_socket.py` and `vpp_papi/vpp_stats.py` | Binary API is Unix socket; stats use a separate read-only `SCM_RIGHTS` plus `mmap` client | Python likewise proves the socket and stats split, not a shared-memory language restriction. |

The conclusions are independent of language:

1. Shared memory is a byte layout and synchronization contract, not a Rust
   object graph.
2. A cross-language client needs one canonical schema and either one common
   ABI implementation or multiple implementations of the same published
   layout. It must never require another binding to link Rust crates.
3. VPP chooses a common C ABI for shared-memory clients; Go and Python use the
   Unix socket for the Binary API while reading stats shared memory directly.
   Hammer's first implementation may use the common core for SHM and leave
   socket transport native to each binding later.

For reproducibility, the external check was made against GoVPP commit
`b7ff1d40eaecc169f592c7f65f0b28b4f8b09066`, VPP commit
`b5d2e1b02f2be41ad3e1dbc1e2cc275fe0a494f2`, and
`vpp-api-transport` commit `ed9ed694c8e9b9ee38261f1818acc919c88c98a4`.
The vendored VPP checkout is `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`.
The important point is not that every upstream project uses SHM; it is that
the clients which do use SHM do not all share the server's implementation
language.

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
        core/
            CMakeLists.txt
            include/
            src/
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

The server repository remains the home of the canonical protocol declarations
and the server API implementation.
For each release it publishes a versioned language-neutral schema, equivalent
in role to VPP's generated `.api.json` definitions. The client repository
vendors that schema snapshot and generates or writes each language binding from
it. The schema contains message names, CRCs, field layouts, service
relationships, and protocol versions; it does not contain Rust types or
client-side error policy.

Each language binding consumes the versioned schema and owns its language
protocol, client, and RPC packages. The shared-memory transport core is a
separate internal C ABI package under `core/`; it is not the Rust binding's
private crate. A committed client manifest must not contain a relative path dependency into
`../netsystem-rs/crates/**` or any other parent checkout path. Other language
bindings decode the same schema without linking Hammer Rust crates.

For local cross-repository work, a developer may use an uncommitted Cargo
patch or local configuration override pointing at the parent checkout. That
override is a development convenience, not part of the repository contract.
The client CI builds from its own repository using pinned package versions or
revisions, and the compatibility job selects the server revision it is
testing against.

### 3.2 Language bindings and internal transport core

Each language binding owns its public packaging and service tree. The repository
must not require another language to depend on Rust crates, generated Rust
types, or Cargo package names.

The repository has three separate contracts:

- The versioned schema owns message identity, field layout, CRCs, service
  relationships, and protocol versions.
- `hammer-client-core` owns SHM mapping, SVM queues, the region-local heap,
  message rings, the V2 handshake, request correlation, and the opaque
  connection context behind an internal C ABI. It contains no typed protocol
  codec, RPC service, or business operation. Connection state is per opaque
  handle; there is no process-global current client, active heap, or last
  error slot.
- Each language binding owns its public `Client`, protocol codec, and RPC
  services. The Rust binding's packages are illustrative of one binding, not
  the repository contract.

This is not a public transport hierarchy. `hammer-client-core` is an internal
implementation boundary used to avoid reimplementing the shared-memory heap
and queue protocol independently in every language. C, C++, Lua, and the Rust
`vpp-api-transport` ecosystem follow the same pattern around
`libvppapiclient.so`. A language binding does not expose the C ABI, and callers
still use their language's concrete `Client` value. The C ABI is an
implementation detail even though it is stable enough for bindings to link.

Native socket transport may be implemented per language when it is added,
following GoVPP and `vpp_papi`. The SHM backend remains in
`hammer-client-core` because it is the backend that requires the shared-region
allocator and process-shared synchronization ABI.

The client and stats client are not features of one another, do not share a
connection object, and do not re-export a server `ApiMain` or `StatsMain`.

The `netsystem-client` repository owns all client implementation packages. The
server does not depend on these packages and does not share their source tree.
The initial ownership layers are:

```text
core/
    hammer-client-core: C ABI, SHM mapping, SVM queue, region heap, message
    rings, V2 handshake, and request correlation; no business operations

rust/crates/hammer-binary-api-protocol
    generated Api, Message, MessageId, codec, and V2 handshake types

rust/crates/hammer-binary-api-client
    safe Rust connection abstraction over hammer-client-core

rust/crates/hammer-binary-api-rpc
    typed RPC services such as VpeService

rust/crates/hammer-stats-protocol
rust/crates/hammer-stats-client
    stats-specific protocol and read-only client
```

A `hammer-shmem` crate is not the cross-language contract and must not be
introduced as a second implementation of the same heap, queue, and message
ring protocol. There is no public Rust `ClientTransport`, `TransportMessage`,
`ShmemTransport`, or `SocketTransport` hierarchy.

No client package contains a process-wide `#[global_allocator]`, a
`MainHeapConfig`, `MemMain`, or a dependency on `hammer-infra`.

No client package exposes or accepts process-allocator configuration. The final
executable's allocator choice is outside the client API and is never changed,
initialized, or replaced by the client library. In particular, do not port a
`vac_mem_init`-style process-heap initializer into Hammer.

The region heap in `hammer-client-core` is an explicit instance attached to a
mapped region, not the process-wide Rust allocator. VPP reaches the same
boundary by pushing `rp->data_heap` before calling `clib_mem_alloc_aligned`
for the client queue and by wrapping that behavior in `libvppapiclient`.

The client core may compile a private dlmalloc/mspace-compatible allocator, or
an equivalent implementation of the published shared-region ABI, directly
inside `hammer-client-core`. It may not depend on `hammer-infra::MemHeap` as a
Rust crate, because that would link the server allocator and Rust runtime into
every binding.

The private region allocator must satisfy all of the following:

- every allocation and free receives the explicit mapped-region heap context;
- the implementation is not registered as `#[global_allocator]`;
- it does not define or interpose process-wide `malloc`, `free`, `realloc`,
  `calloc`, `new`, or `delete`;
- it does not install constructor-time allocator hooks;
- it does not call `vac_mem_init`, `clib_mem_init`, or any equivalent routine
  that publishes a process-global heap;
- its symbols are private to the core library or namespaced to avoid collision
  with the embedding process's allocator.

The published shared-region ABI still defines the exact allocation contract.
The core may vendor the allocator source it needs to implement that contract,
but the allocator remains a private implementation detail of the core.

Server-only checks and ownership stay in the server crate. In particular,
`hammer_runtime::thread_main::ensure_main_thread`, `ApiMain` registration and
dispatch, the server client pool, dead-client scanning, and handler execution
must not move into the client-owned SHM or protocol crates. The existing
allocation role logic may remain server-side, and the server call site remains
responsible for enforcing that server rings run on the main thread.

`hammer-infra` keeps its server-owned memory, region, and queue implementation.
It must not depend on `hammer-client-core`, and `hammer-client-core` must not
depend on `hammer-infra` or any server-only crate.

### 3.3 Client is one concrete abstraction, not a public transport hierarchy

Every language binding exposes one concrete connection value. The Rust
binding's shape is shown below only to make the lifetime and ownership rules
concrete; Go, Python, C, and C++ use their own idiomatic types with the same
operations:

```text
connect(connection configuration) -> Client
invoke(typed request) -> typed reply
disconnect(Client)
```

The Rust binding's public client is a single value:

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

The following types are explicitly not part of any binding's public API:

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

They are not public, not placeholders, and not a cross-language ABI. The
internal `hammer-client-core` C ABI exists below the binding; callers never
name its handle, mapping, queue, allocator, or backend operations directly.

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
- its decoded replies and other binding-local state.

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

The Rust binding's `hammer-binary-api-rpc` package owns the client-side VPE
service. Other bindings expose the same operation through their own service
types:

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
compatible with the server. They live in the client-owned protocol crate, not
in the client or stats client. Their codec and CRC generation are protocol
facts, not business operations.

The service operation and `retval` policy live only in the RPC service package
for that language. The server handler and VPE build metadata remain
server-owned.

Business methods must never be added to `Client`. In particular:

- `Client` has no `show_version`, `control_ping` business wrapper, interface
  method, or plugin method;
- `Client` does not install server handlers;
- `Client` does not expose the server registration pool;
- RPC services do not implement transport queues or region allocation.

## 5. Stats Client Boundary

The stats client is independent of the Binary API client in every language.
The Rust binding shown here follows the GoVPP and `vpp_papi` split: the stats
path uses its own read-only shared-memory protocol rather than the Binary API
connection:

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
        -> hammer-client-core          -> no hammer-infra
                                          no process-global allocator

external application
    -> hammer-stats-client
        -> hammer-stats-protocol      -> no hammer-infra
```

The external application keeps whatever allocator it already owns. It does not
declare or configure an allocator for the client, and the client does not
declare or configure one for the application.

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
| Binding-local maps, request tables, decoded replies, diagnostic strings | The embedding language runtime's existing allocator | Binding-native cleanup |
| Connection setup queue and shared header | The shared API region | Client disconnect and region cleanup |
| API request and reply payloads | The shared API region allocation contract | Matching region release after send or receive |
| Server `ApiMain`, handlers, registration pools | Existing daemon ownership and its chosen allocation policy | Existing server lifecycle |
| Stats client decoded vectors and strings | The embedding language runtime's existing allocator | Binding-native cleanup |
| Stats directory and payload bytes | The server stats segment, mapped read-only | `StatsClient` drop or unmap |

The client does not allocate shared API messages through its process global
allocator, and it does not assume that the server's Main Heap is available in
the client process. Conversely, the embedding process's allocator is not used
to free a shared API region allocation.

### 6.4 Verification rule

The boundary is checked from an actual external consumer in each implemented
language. For the Rust binding, the check uses a real crate outside this
repository:

1. The consumer uses the platform allocator or whatever process allocator its
   final executable already chose. The client packages and the core library do
   not replace it, install one from a constructor, or request allocator
   configuration.
2. It depends directly on `hammer-binary-api-client`,
   `hammer-binary-api-rpc`, and/or `hammer-stats-client`.
3. Its normal dependency tree contains no `hammer-infra` and, for the stats
   client, no server `hammer-stats`.
4. It can build and run a V2 SHM connection without calling
   `MainHeapConfig::initialize()` or any `vac_mem_init` equivalent.

A source-text search for forbidden imports is not a replacement for this
check. The relevant proof is the compiled dependency graph and the external
binary's behavior at link time. Other language bindings must prove the same
property through their package graph and must not link the server executable or
server-only packages.

## 7. Required Dependency Direction

The client repository has one internal C ABI core and independent language
bindings:

```text
hammer-client-core
    -> libc, OS mapping, and process-shared synchronization dependencies only
    -X-> hammer-infra
    -X-> process-global allocator
    -X-> server executable or server-only crate

hammer-binary-api-protocol
    -> generated schema and language codec dependencies only
    -X-> hammer-client-core

hammer-binary-api-client
    -> hammer-binary-api-protocol
    -> hammer-client-core C ABI
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
```

The exact package manager and language-runtime dependencies for non-Rust
bindings are owned by those bindings. Their only shared implementation
dependency is the internal `hammer-client-core` C ABI and the versioned schema.

The parent repository keeps its server-owned dependency graph:

```text
hammer-infra
    -> existing daemon and dataplane dependencies only
    -X-> hammer-client-core

hammer-stats
    -> existing server stats dependencies

hammer-ipc server API
    -> hammer-infra, hammer-runtime as required by the server
```

The direction of every client arrow is one-way. `hammer-client-core` never
depends on a language binding, RPC service, server handler, stats server, or
`hammer-infra`. SHM bindings use the core. A native per-language SHM
implementation would require a separate ADR with its own compatibility proof;
it must never invent a second schema. Future socket bindings may be native
because the socket protocol does not require the shared-region allocator.

The parent repository owns the canonical schema and server implementation. The
`netsystem-client` repository owns `hammer-client-core`, every language
binding, and the published schema snapshot. Both sides implement the same
published ABI independently. The parent does not depend on `netsystem-client`,
and the client repository does not use a parent-relative path dependency in
its committed manifests.

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
5. Implement `netsystem-client/core/` as the internal C ABI transport core. It
   owns mapping, queues, message rings, the V2 handshake, request correlation,
   and an explicit region-local heap context. It must not link or re-export
   `hammer-infra`, install a constructor allocator, expose allocator setup, or
   route ordinary host allocations through the region heap.
6. The core may vendor or compile a private dlmalloc/mspace-compatible
   allocator to implement the published shared-region ABI. Keep its symbols
   private to the core and verify that the core allocates, publishes, and frees
   client queue and message storage without changing the embedding process's
   allocator.
7. Generate or write the client-owned `hammer-binary-api-protocol` from the
   published schema. It owns the codec, message inventory, V2 handshake types,
   and message-table decoding used by the Rust binding.
8. Implement `hammer-binary-api-client` in the Rust binding as the safe
   language wrapper over the core C ABI. Its connection state must not depend
   on `hammer_infra::ApiMain`; remove the `ApiMain::current()` dependency and
   remove `ShowVersion` from the client crate.
9. Move `show_version` operation and error handling to
   `hammer-binary-api-rpc::VpeService`.
10. Write the client-owned `hammer-stats-protocol` from the published stats
   segment ABI; the server and stats client validate the same published layout
   without sharing Rust source.
11. Remove old `hammer-ipc` client/stats exports that would preserve the
   dependency path back to `hammer-infra`. Compatibility re-exports must not
   silently reintroduce the allocator or server ownership.
12. Leave socket support unimplemented. Do not add a placeholder constructor or
   unsupported runtime branch.
13. Do not scaffold empty Go, Python, or C++ binding directories. Add a binding
    when its package, generated types, and tests are ready.

The client implementations that currently import `ApiMain` and `ShowVersion`
are removed from the parent repository. They are migration inputs only for
behavioral reference; the child repository owns its implementation.

## 9. Acceptance Criteria

- The parent repository contains no Binary API, stats, RPC, or CLI client
  implementation, no client export, and no client-driven test.
- The parent has no gitlink or `.gitmodules` entry at `netsystem-client`, and
  its `.gitignore` and Cargo workspace exclude the sibling repository.
- The child repository is language-neutral; its Rust binding has its own
  workspace and lockfile under `rust/`, and other bindings do not depend on it.
- `hammer-client-core` exposes one opaque C ABI. No Rust collection, trait,
  `Result`, panic, or allocator type crosses that ABI.
- The core and every binding can be loaded by an external process without
  replacing, declaring, or initializing the process allocator.
- The core does not link `hammer-infra`. If it carries a private
  region-allocator implementation, that allocator is not exported, interposed,
  installed by a constructor, or selected as the process global allocator.
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
9. A server-preallocated pool of client input queues that the client claims.
   VPP's V2 client creates its own queue in the mapped API region and sends
   that queue address in `memclnt_create_v2`; it does not claim a queue slot
   created by the server.
10. A ring-only shared-message path with no shared-heap fallback. VPP tries
    the size-selected message ring and falls back to shared-memory allocation
    when the ring is unavailable or the message is too large.
11. Pulling all of server `hammer-infra` into the client to obtain its region
    allocator. The client repository owns an independent region-local heap
    implementation without a process-wide `#[global_allocator]` declaration.
12. Requiring an application to declare an allocator for the client or exposing
    allocator setup in the client API. A client library must remain passive
    with respect to the final executable's process allocator.
13. Claiming that shared memory is only usable by the server's implementation
    language. Shared memory is a byte-layout and synchronization contract.
    VPP's Lua and third-party Rust SHM clients both cross a C ABI, and the Go
    and Python socket decisions are implementation choices rather than proof
    of a language restriction.
14. Making a Rust crate, Rust trait, Rust `Client`, or Cargo package the
    cross-language transport contract. Only the schema and an ABI with
    language-neutral data representations can cross binding boundaries.
15. Reimplementing the region heap, queue, ring, and V2 handshake independently
    in every language without a published shared-region contract. That creates
    multiple incompatible definitions of the same memory protocol.
16. Exposing the `hammer-client-core` C ABI as the public user API. Each
    language binding owns its idiomatic `Client` and RPC services; the core is
    an internal implementation boundary.
17. Porting a `vac_mem_init`-style process-heap initializer or any constructor
    that replaces the host allocator. The core may own region-local allocation
    state, but it must remain passive with respect to the final executable's
    allocator.
18. Treating an internal region allocator as a reason to replace or intercept
    the host process allocator. A private dlmalloc/mspace-compatible allocator
    is allowed inside the core only when it is context-owned, symbol-isolated,
    and used exclusively for shared-region allocations.

## 11. Consequences

Each language binding exposes one transport-facing client type that can connect
to the V2 server without exposing `ApiMain`, the core C handle, or any
allocator policy. RPC services own typed protocol operations, so adding a new
service does not add a method or message-specific field to the client.

The external dependency graph contains one client-owned C ABI core, generated
language protocol packages, and language-specific client and RPC packages, but
no `hammer-infra` or server process allocator. An external process therefore
keeps its own global allocator and does not initialize or replace Hammer's Main
Heap. The daemon's existing allocation behavior remains unchanged.

The core owns any private region allocator implementation required by the V2
shared-region ABI. That allocator is linked into the core and receives the
mapped region's heap context explicitly; it never becomes the embedding
runtime's allocator and never appears in a language binding's public API.

The cost is a real repository, schema, and code-generation boundary. That is
acceptable because it removes the link-time allocator hazard, keeps all client
code out of the server repository, and lets language bindings evolve without
turning one language's crate layout into the protocol contract. It also keeps
the shared-memory complexity in one internal implementation instead of
requiring every binding to reproduce the heap, queue, ring, and locking ABI.
It does not require the generic public transport, allocator, event, or
message-wrapper types proposed by the rejected draft.
