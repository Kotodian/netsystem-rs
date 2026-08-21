# Binary API Dispatch is Serial and Barriered

Status: accepted

Hammer dispatches Binary API methods serially on the Main Thread. After a complete frame is read and its method registration is resolved, the binary-api ProcessNode acquires `WorkerBarrier`, invokes exactly one synchronous `BinaryApiMethodEntry`, constructs its reply, and releases the barrier. A method cannot execute concurrently with another method, spawn parallel method work, or retain the barrier across an await boundary.

This follows VPP's Session Socket API control path: `sapi_sock_read_ready` reads the message, calls `vlib_worker_thread_barrier_sync`, dispatches the selected application or certificate mutation handler, and then calls `vlib_worker_thread_barrier_release`. A single dispatch boundary makes every plugin-visible control mutation use the same publication protocol and avoids method-specific barrier metadata, handler locks, and nested completion mechanisms.

## Ownership: FileMain owns fd readiness, the ProcessNode owns dispatch

A registered `binary-api` ProcessNode serves the socket, mirroring `vl_api_clnt_node`. The capability (`BinaryApiMain`) binds the listener and owns the `FileMain` until the node starts; the node task then owns the `AsyncFileMain` (the FileMain's async adapter) for its lifetime. Readiness for the listener and every client is therefore FileMain-owned and processed on the main thread, and the ProcessNode runs on the main-thread LocalSet, so readiness callbacks signal the node's bounded event queue on the same thread: no cross-thread handoff and no locks. The ProcessNode is the only owner of the FileMain after start; node shutdown aborts the task, dropping the FileMain and closing the listener and all client descriptors, after which new connections are refused.

## Readiness loop: poll once, drain all signalled batches

The node polls the FileMain once per main-loop turn and then drains every signalled event batch (accept, client read, client write) before polling again, mirroring `vl_api_clnt_node`, which consumes the events its readiness callbacks signal. kqueue readiness is level-triggered, so an undrained event would re-dispatch indefinitely; the drain is therefore mandatory, not an optimization. Accept and read failures are handled per connection: a connection that cannot be served (for example a peer that closed before accept, which macOS rejects with EINVAL at the SO_NOSIGPIPE setsockopt) is dropped with a warning while the node and the remaining connections keep running.

## Barrier scope: method call and reply construction only

The worker barrier covers exactly the synchronous method invocation and reply construction (`dispatch_barriered`). Frame assembly, parse-time validation, and reply flush are I/O and happen outside the barrier: the reply is encoded into the client's bounded output buffer after the barrier is released and is flushed under TCP backpressure on later write readiness. A method never holds the barrier across an await point. If a barrier is already pending, dispatch proceeds unlocked, matching VPP's `msg_handler_internal` skip-while-pending behavior.

The daemon's current-thread Tokio runtime may interleave connection I/O only at await points outside method dispatch. It does not make plugin methods parallel. Plugin-owned Main Thread operations reached locally through the ABI must use the same serial control authority when they publish worker-visible state.

## Mp-safe methods dispatch without the worker barrier

The barrier is not universal: an entry carries VPP's `is_mp_safe` flag (`vl_msg_api_msg_config_t.is_mp_safe`, api_common.h:122), copied to the registered entry exactly as `vl_msg_api_registration` copies the config bit to the message (api_shared.c:754). `BinaryApiMethodEntry::new` builds a legacy barriered entry with `is_mp_safe = false`; the const `mp_safe()` builder marks an entry mp-safe. The `#[binary_api]` attribute takes a bare `mp_safe` marker (`#[binary_api(name = "method.name", mp_safe)]`); a valued or duplicated marker is a compile-time error, and the absent marker keeps the legacy constructor, so existing registrations remain backward compatible.

`dispatch` resolves the method entry exactly once, before any barrier decision. A successfully resolved mp-safe entry invokes directly on the serial Main Thread: it never fetches the worker barrier, never runs the deferred graph-update finish, and therefore cannot mutate or publish worker-visible state. Every resolution failure (missing, duplicate, internal, Main Thread unavailable) and every default (non-mp-safe) method keeps the legacy barriered/pending branches and the same deferred graph-update finish as before, so the flag changes nothing for existing methods. This is exactly VPP's `msg_handler_internal`, which takes the worker barrier only when `!m->is_mp_safe` — `vl_msg_api_barrier_sync` before the handler and `vl_msg_api_barrier_release` after it (api_shared.c:545, 564).
