# VPP Clone Refactor — Phase D: IPC Daemon + Lifecycle Deletion

## Summary

Phase D completes the VPP clone refactor by:
1. Deleting all iOS NetworkExtension legacy: hammer-control crate, Lifecycle trait, LogWriter/Factory/DiscardWriter, EventRegistry, RuntimeService/ServiceState, ControlCommand variants
2. Creating a VPP-style IPC daemon with async `clnt_loop` process node equivalence
3. Rewriting hammer/src/main.rs with tokio current_thread runtime
4. Making hammerctl async

## Global Constraints

- **All existing transport/session tests must continue passing** after every change (`cargo test -p hammer-runtime`, `cargo test -p hammer-core`, etc.)
- **`cargo clippy --workspace --all-targets` must be clean** (no new warnings; pre-existing warning in session/runtime.rs:987 is allowed)
- **`cargo fmt --all`** must be clean
- VPP architecture: async `tokio::spawn` for process node equivalence (NOT setjmp/longjmp)
- Session runtime owns node scheduling; congestion control must not schedule nodes
- Thread-local `Engine` per vlib_get_main convention
- No `Arc<dyn Trait>`, no trait object, no `Box<dyn Future>` for IPC handlers
- All types are concrete; handler signature is `fn(&mut Engine, &[u8]) -> Vec<u8>`
- Frame format: `[4-byte BE u32 length][bincode payload]`
- Daemon tokio: `#[tokio::main(flavor="current_thread")]` single reactor
- Handler dispatch: `#[ipc_handler]` → linkme `IPC_HANDLERS` slice → name string lookup
- Engine access: thread-local `Engine::with_current(|e| ...)`
- `hammerctl` also `#[tokio::main(flavor="current_thread")]`

## Task D1 — Delete hammer-control + Lifecycle + LogWriter + EventRegistry + RuntimeService

### Scope
Delete these files/crates entirely:
1. `crates/hammer-control/` — entire crate directory (all files)
2. `crates/hammer-core/src/lifecycle.rs` — Lifecycle trait, StartStage, LifecycleService, ALL_STAGES, LIFECYCLE_ORDER
3. `crates/hammer-core/src/log/factory.rs` — LogWriter trait, Factory, DiscardWriter (keep `mod.rs` minimal)
4. `crates/hammer-adapter/src/lifecycle.rs` — if it re-exports from hammer_core::lifecycle

### Cross-crate references to delete/update

**hammer-core:**
- `lifecycle.rs`: delete entire file. Remove `pub mod lifecycle;` from `lib.rs`
- `log/factory.rs`: delete entire file (LogWriter, Factory, DiscardWriter). Remove `pub use factory::{...};` from `log/mod.rs`

**hammer-adapter:**
- `src/lib.rs`: remove `pub use hammer_core::lifecycle::*`
- `src/network.rs:1`: remove `use hammer_core::lifecycle::Lifecycle`
- `src/connection.rs:3`: remove `use hammer_core::lifecycle::Lifecycle`
- `src/service.rs:4`: remove `use hammer_core::lifecycle::{Lifecycle, LifecycleService}`
- `src/certificate.rs:4`: remove `use hammer_core::lifecycle::{Lifecycle, LifecycleService}`

**hammer-runtime:**
- `src/control_thread.rs:12`: remove `use hammer_core::log::{Level, LogWriter}`
- `src/control_thread.rs:888`: remove test `use hammer_core::log::Level`
- `src/component_registry.rs:39`: remove test use of LogWriter/Factory/DiscardWriter/Logger
- `src/spawn.rs:1349`: remove test use of LogWriter/etc
- `Cargo.toml:38`: remove `hammer-control = { path = "../hammer-control" }`

**hammer-service:**
- `src/service.rs`: remove all imports from `hammer_control::*` and `hammer_core::log::*` and `hammer_core::lifecycle::*`
- `src/event_subscribers.rs:4`: remove `use hammer_core::log::Logger`
- `Cargo.toml:19`: remove `hammer-control = { path = "../hammer-control" }`

**hammer (main crate):**
- `Cargo.toml:15`: remove `hammer-control = { path = "../hammer-control" }`
- Any `use hammer_control` in `src/lib.rs` or `src/main.rs`

**hammer-service service.rs specific deletions:**
- Delete `RuntimeService`, `ServiceInner`, `ServiceState`
- Delete `build_standard_event_subscribers`, `EventSubscriberBuilder`
- Delete `publish_event`, `subscribe_event`
- Keep `RuntimeTcpListenerControlState` and any TCP-related types

**ControlEvent/Command cleanup in hammer-runtime:**
- Remove `ControlCommand::Flush`, `ControlCommand::RegisterEventSubscriber`, `ControlCommand::CancelEventSubscription`
- Remove EventRegistry struct (entire file if it exists as a dedicated file)
- Clean up ControlThread fields that referenced EventRegistry, LogWriter, etc.

**ControlThread cleanup:**
- Remove `flush_timeout` method
- Remove `publish_event`, `subscribe_event` methods
- Remove event-related fields from control thread config/state

**Cargo.toml workspace:**
- `Cargo.toml:8`: remove `"crates/hammer-control"` from workspace members

### Test safety
- `cargo test -p hammer-runtime` must pass
- `cargo test -p hammer-core` must pass
- `cargo test -p hammer` must pass  
- Try `cargo test --workspace` — any failures are blockers

### Files
No new files. Pure deletion + reference cleanup.

## Task D2 — hammer-ipc Async Frame + Handler Slice

### Scope
1. `crates/hammer-ipc/src/frame.rs` — replace sync frame functions with async versions
2. New `crates/hammer-ipc/src/handler.rs` — IpcHandler struct, IpcHandlerFn, linkme IPC_HANDLERS slice, dispatch fn
3. `crates/hammer-ipc/src/lib.rs` — add `pub mod handler;`
4. Delete `crates/hammer-ipc/src/server.rs` entirely

### frame.rs
Sync functions to keep (still valid for synchronous callers):
- `read_frame(stream: &mut impl Read, buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>>`
- `write_frame(stream: &mut impl Write, data: &[u8]) -> Result<()>`

Async functions to add:
- `pub async fn async_read_frame(stream: &mut (impl AsyncRead + Unpin), buf: &mut [u8]) -> Result<Option<Vec<u8>>>`
  - Read 4-byte BE length
  - If length == 0, return None (peer closed)
  - Read `length` bytes into buffer
  - Return the data
- `pub async fn async_write_frame(stream: &mut (impl AsyncWrite + Unpin), data: &[u8]) -> Result<()>`
  - Write 4-byte BE length as u32
  - Write data
  - Flush

`use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};`

Errors: use `crate::IpcError`

### handler.rs
```rust
use linkme::distributed_slice;

/// Handler type: synchronous fn called from reactor thread
pub type IpcHandlerFn = fn(&mut ..., &[u8]) -> Vec<u8>;

pub struct IpcHandler {
    pub name: &'static str,
    pub handler: IpcHandlerFn,
}

/// Linkme distributed slice — registered by #[ipc_handler] macro
#[distributed_slice]
pub static IPC_HANDLERS: [IpcHandler] = [..];

/// Dispatch a request by name
pub fn dispatch_handler(name: &str, engine: &mut ..., request: &[u8]) -> Option<Vec<u8>> {
    IPC_HANDLERS
        .iter()
        .find(|h| h.name == name)
        .map(|h| (h.handler)(engine, request))
}
```

Note: The `&mut ...` for engine is placeholder — will be `&mut crate::Engine` after D5. For now use `&mut ()` or a type parameter. Let's use `&mut hammer_runtime::engine::Engine` directly as it already exists.

Add to `Cargo.toml`: `linkme` dependency, `hammer-runtime` dependency

### server.rs — DELETE
Entire file is deleted. The TCP accept/connection loop moves to hammer's main.rs/ipc_loop.rs in D6/D7.

### Tests
- Test async_read_frame / async_write_frame roundtrip
- Test dispatch_handler with mock handlers

## Task D3 — #[ipc_handler] Proc Macro

### Scope
Add to `crates/hammer-component-macros/src/lib.rs`:
```rust
#[proc_macro_attribute]
pub fn ipc_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse: #[ipc_handler(name = "some_name")]
    // Expects: a function with signature fn(&mut Engine, &[u8]) -> Vec<u8>
    // Generates: #[distributed_slice(IPC_HANDLERS)] registration
    let input = parse_macro_input!(item as ItemFn);
    let attr_args = parse_macro_input!(attr as AttributeArgs);
    
    // Extract `name = "..."` from attr
    let name = /* parse name string */;
    
    let fn_name = &input.sig.ident;
    let vis = &input.vis;
    let block = &input.block;
    let sig = &input.sig;
    
    TokenStream::from(quote! {
        #vis #sig {
            #block
        }
        #[::linkme::distributed_slice(crate::IPC_HANDLERS)]
        static #fn_name: crate::IpcHandler = crate::IpcHandler {
            name: #name,
            handler: #fn_name,
        };
    })
}
```

Wait — the `#[distributed_slice]` must reference the slice from hammer-ipc, but the proc macro only sees token streams. The user writes:
```rust
#[hammer_component_macros::ipc_handler(name = "foo")]
fn handle_foo(engine: &mut Engine, request: &[u8]) -> Vec<u8> { ... }
```

The macro expands to register the handler onto `hammer_ipc::IPC_HANDLERS`. So the generated code uses `::hammer_ipc::IPC_HANDLERS`.

Let's make the registration path configurable or just hardcode `::hammer_ipc::IPC_HANDLERS`.

Dependency needed: `linkme` in hammer-component-macros (or it just generates code that references linkme).

The macro crate needs to add `linkme` to its dependencies (for `distributed_slice` macro). Actually linkme's `distributed_slice` attribute is a proc macro too — the generated code just needs linkme as a dependency of the calling crate. The proc macro crate itself doesn't need linkme.

Add `proc-macro-crate` for finding hammer_ipc, or just hardcode the path.

### Files
- `crates/hammer-component-macros/src/lib.rs`

## Task D4 — ControlThread Reorganization

### Scope
Trim `crates/hammer-runtime/src/control_thread.rs` (and related):

1. Delete `EventRegistry` — entire struct and all references
2. Delete `ControlCommand::Flush`, `ControlCommand::RegisterEventSubscriber`, `ControlCommand::CancelEventSubscription`
3. Remove `LogWriter`-related state from ControlThread struct fields
4. Remove `publish_event`, `subscribe_event`, `flush_timeout` methods
5. Clean up any references to LogWriter in `control_thread/timer.rs`

The remaining ControlCommand variants should be: `Pause`, `Wake`, `Shutdown`, `UpdateNetworkSettings`, `PauseConnection`, and any VPP-relevant commands.

### Files
- `crates/hammer-runtime/src/control_thread.rs`
- `crates/hammer-runtime/src/control_thread/timer.rs`
- `crates/hammer-runtime/src/control_thread/event.rs` (delete entire file if EventRegistry lives there)

### Tests
- Any tests referencing deleted commands/methods must be updated or removed
- The control thread tests must still compile and pass

## Task D5 — EnginePool Extension

### Scope
Extend `crates/hammer-runtime/src/engine.rs`:

**EnginePool changes:**
- Add fields: `ipc_listener: Option<TcpListener>`, `workers_started: bool`
- Add `from_config(config: &HammerConfig) -> Result<Self>` constructor that:
  - Reads IPC socket path from config
  - Binds `TcpListener` 
  - Configures workers
- Add `main_loop_enter(&mut self, engine: &mut Engine)` — runs `start_workers`, sets thread-local Engine
- Add `main_loop_exit(&mut self, engine: &mut Engine)` — sets shutdown flag
- Add `close(&mut self)` — closes IPC listener
- Add `into_ipc_listener(self) -> Option<TcpListener>` for moving listener to main.rs

**Thread-local Engine:**
```rust
use std::cell::RefCell;

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

impl Engine {
    pub fn install_current(&mut self) {
        CURRENT_ENGINE.with(|cell| {
            *cell.borrow_mut() = Some(self as *mut Engine);
        });
    }
    
    pub fn with_current<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut Engine) -> R,
    {
        CURRENT_ENGINE.with(|cell| {
            let ptr = *cell.borrow();
            ptr.map(|p| {
                // Safety: Engine is !Send and we only access from the owning thread
                let engine = unsafe { &mut *p };
                f(engine)
            })
        })
    }
    
    pub fn uninstall_current() {
        CURRENT_ENGINE.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}
```

Note: `Engine::uninstall_current` should be a static fn.

### Files
- `crates/hammer-runtime/src/engine.rs`

## Task D6 — hammer/src/ipc_loop.rs

### Scope
New file: `crates/hammer/src/ipc_loop.rs`

```rust
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// clnt_loop — async process node equivalent of VPP vl_api_clnt_process
/// Spawned by main.rs, handles IPC accept + per-connection dispatch
pub async fn clnt_loop(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(conn_loop(stream));
            }
            Err(e) => {
                tracing::error!("IPC accept error: {e}");
                // Continue listening
            }
        }
    }
}

/// Per-connection handler loop
async fn conn_loop(stream: tokio::net::TcpStream) {
    let (mut reader, mut writer) = stream.into_split();
    let mut buf = vec![0u8; 65536];
    
    loop {
        match hammer_ipc::frame::async_read_frame(&mut reader, &mut buf).await {
            Ok(Some(request)) => {
                // Dispatch handler synchronously on reactor thread
                let response = hammer_ipc::handler::dispatch_handler(
                    /* extract name from request somehow? */
                    &mut (), // engine placeholder — will be real engine
                    &request,
                );
                
                if let Some(response) = response {
                    if let Err(e) = hammer_ipc::frame::async_write_frame(&mut writer, &response).await {
                        tracing::error!("IPC write error: {e}");
                        break;
                    }
                }
            }
            Ok(None) => {
                tracing::debug!("IPC client disconnected");
                break;
            }
            Err(e) => {
                tracing::error!("IPC read error: {e}");
                break;
            }
        }
    }
}
```

Wait — the request protocol needs a name prefix. Let me think about the protocol.

Current protocol (from existing code): `[4-byte length][bincode-encoded serialized request]`.

We need to include the handler name. Options:
1. Request starts with a 2-byte name length + name string + payload
2. Use an enum serialized via bincode that includes variant name
3. Use a wrapper struct: `IpcRequest { name: String, payload: Vec<u8> }`

Option 3 is cleanest since we're already using bincode. Let me check the existing frame protocol.

Looking at the existing `crates/hammer-ipc/src/frame.rs`, the protocol is:
- 4 bytes: BE u32 length of entire payload
- Payload: bincode-serialized Request enum

For the new design, let's use:
- 4 bytes: BE u32 length
- Payload: bincode-serialized `IpcRequest { name: String, payload: Vec<u8> }`

New types in `hammer-ipc/src/lib.rs` or `handler.rs`:
```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct IpcRequest {
    pub name: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IpcResponse {
    pub payload: Vec<u8>,
}
```

The dispatch function takes the request payload (the handler-specific data) and returns response payload.

Actually, looking at the plan more carefully: the handler signature is `fn(&mut Engine, &[u8]) -> Vec<u8>`. The `&[u8]` is the handler-specific payload (not including the name). The response `Vec<u8>` is the handler-specific response data. The caller (conn_loop) wraps/unwraps the name.

So the flow:
1. conn_loop reads frame → deserializes IpcRequest { name, payload }
2. Looks up handler by name via IPC_HANDLERS
3. Calls handler(engine, &payload)
4. Wraps result in IpcResponse { payload }
5. Serializes and writes frame

Let me simplify the protocol:
- Frame: [4-byte BE len][bincode(IpcRequest)]
- Response: [4-byte BE len][bincode(IpcResponse)]

```rust
#[derive(Serialize, Deserialize)]
pub struct IpcRequest {
    pub name: String,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct IpcResponse {
    pub payload: Vec<u8>,
}
```

These go in `hammer-ipc/src/handler.rs`.

### Files
- Create: `crates/hammer/src/ipc_loop.rs`

## Task D7 — hammer/src/main.rs Rewrite + ipc_handlers.rs

### Scope
Rewrite `crates/hammer/src/main.rs` and create `crates/hammer/src/ipc_handlers.rs`.

**main.rs:**
```rust
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::ShutdownFlag;

fn main() {
    // Parse CLI args — just config path for now
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <config.toml>", args[0]);
        std::process::exit(1);
    }
    let config_path = &args[1];
    
    // Read and parse config
    let config_content = std::fs::read_to_string(config_path)
        .expect("Failed to read config");
    let config: HammerConfig = toml::de::from_str(&config_content)
        .expect("Failed to parse config");
    
    // Build engine pool
    let mut engine_pool = EnginePool::from_config(&config)
        .expect("Failed to build engine pool");
    
    // Create engine and install thread-local
    let mut engine = Engine::new(config.clone());
    engine.install_current();
    
    // Enter main loop
    engine_pool.main_loop_enter(&mut engine);
    
    // Extract IPC listener
    let listener = engine_pool.into_ipc_listener()
        .expect("IPC listener not configured");
    
    // Start the tokio current_thread runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("Failed to build tokio runtime");
    
    rt.block_on(async {
        // Spawn IPC client loop
        let clnt_handle = tokio::spawn(ipc_loop::clnt_loop(listener));
        
        // ... signal handling, etc.
        
        clnt_handle.await.expect("IPC client loop panicked");
    });
    
    // Exit main loop
    engine_pool.main_loop_exit(&mut engine);
    engine_pool.close();
    engine.uninstall_current();
}
```

Wait, this needs more thought. The `main_loop_enter` calls `engine_main_loop(engine)` which is blocking (it has the 9-step vlib loop). But we also need tokio running for IPC.

Re-reading Phase C: `engine_main_loop` is the blocking 9-step loop that runs on each worker thread. The main thread also runs `engine_main_loop`. The tokio reactor is step #4 inside the main loop.

But for Phase D, the VPP design for IPC is different — we need tokio to run concurrently for IPC accept/handling.

Looking at the plan more carefully:

> D6 — hammer/src/main.rs rewrite: tokio current_thread runtime, from_config, block_on join(clnt_loop, control_thread, signal_watcher), main_loop_exit, close socket

So the main thread runs tokio::block_on with:
- clnt_loop (IPC accept + dispatch)
- control_thread (if needed — the plan says "ControlThread" but maybe it's the old one)
- signal_watcher (shutdown signals)

The `engine_main_loop` runs on worker threads (started via `start_workers`), not on the main thread. The main thread runs the tokio reactor for IPC.

Wait, but step #4 of engine_main_loop IS the tokio reactor. Let me re-check the Phase C architecture.

From Phase C:
> C4: engine_main_loop with vlib 9-step order, tokio reactor as step #4

Each worker thread (and maybe the main thread) runs engine_main_loop, which includes tokio::block_on(tokio::time::sleep(Duration::from_millis(1))) as its reactor step. But that's a very short sleep, not a full event loop.

For Phase D IPC, we need a dedicated tokio runtime that can handle TCP connections. The plan says:

> Daemon tokio: `#[tokio::main(flavor="current_thread")]` single reactor. IPC accept + conn_loop run via reactor; handler sync execution inline (no spawn_blocking — handlers are fast control-plane ops).

So the main.rs creates a tokio runtime (current_thread, enable_io), and runs:
1. clnt_loop (IPC accept, spawns conn_loop per connection)
2. signal handling

The workers don't need the tokio runtime — their engine_main_loop has its own polling step.

Let me re-examine the architecture:
- Main thread: tokio current_thread runtime for IPC
- Worker threads: OS threads running engine_main_loop with their own short polling

This means the main thread does NOT run engine_main_loop. The workers do.

But wait — does the main thread run the control thread? Looking at the original architecture, ControlThread was the main thread's command listener. After deletions (D4), ControlThread is trimmed but still exists as a command channel.

Let me revise the main.rs design:

```rust
fn main() {
    // Parse config, build EnginePool, create Engine
    let mut engine = Engine::new(config.clone());
    engine.install_current();
    
    let mut pool = EnginePool::from_config(&config).expect("...");
    
    // Start workers (these run engine_main_loop on their own threads)
    pool.main_loop_enter(&mut engine);
    
    // Extract IPC listener
    let listener = pool.take_ipc_listener();
    
    // Run tokio reactor for IPC
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("...");
    
    rt.block_on(async {
        let clnt = tokio::spawn(ipc_loop::clnt_loop(listener));
        // Wait for shutdown signal
        // clnt.await
    });
    
    // Shutdown
    pool.main_loop_exit(&mut engine);
    pool.close();
    engine.uninstall_current();
}
```

This is cleaner. `main_loop_enter` starts workers. The main thread runs IPC. `main_loop_exit` signals workers to stop.

**ipc_handlers.rs:**
9 handler functions:
```rust
use hammer_component_macros::ipc_handler;

#[ipc_handler(name = "ping")]
fn handle_ping(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
    // ... respond to ping
}

// Initially 7 stubs, 2 real (ping, status)
```

The handlers need to be annotated with `#[ipc_handler]`, but `#[ipc_handler]` generates `#[distributed_slice(IPC_HANDLERS)]`. The IPC_HANDLERS slice is in hammer-ipc. So these handlers must be in a crate that depends on hammer-ipc.

The hammer crate depends on hammer-ipc already (via hammer-runtime). So the handlers go in hammer/src/ipc_handlers.rs which is part of the hammer crate.

But the `#[distributed_slice]` registration inserts a `#[linkme::distributed_slice(::hammer_ipc::IPC_HANDLERS)]` — this works as long as the hammer crate has `linkme` as a dependency and `hammer_ipc` is linked.

For the `#[ipc_handler]` macro, it generates:
```rust
#[::linkme::distributed_slice(::hammer_ipc::IPC_HANDLERS)]
static HANDLE_FOO: ::hammer_ipc::IpcHandler = ::hammer_ipc::IpcHandler { ... };
```

The `::hammer_ipc::` path must resolve. The hammer crate depends on hammer-ipc (directly or transitively), so it should work.

### Files
- Rewrite: `crates/hammer/src/main.rs`
- Create: `crates/hammer/src/ipc_handlers.rs`
- Add to `hammer/src/lib.rs`: `mod ipc_loop; mod ipc_handlers;`

## Task D8 — hammerctl Async

### Scope
Convert `crates/hammerctl/src/main.rs` (or wherever hammerctl lives) to async:

```rust
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Parse args
    // Connect to IPC socket
    // Send request, await response
    // Print response
}
```

Uses `hammer_ipc::frame::async_write_frame` and `hammer_ipc::frame::async_read_frame`.

### Files
- `crates/hammerctl/src/main.rs` (check exact path)

## Task D9 — fmt + clippy + test

### Scope
Final cleanup:
- `cargo fmt --all`
- `cargo clippy --workspace --all-targets`
- Fix any issues
- `cargo test --workspace`
- Ensure all pass

### Files
None — verification only.
