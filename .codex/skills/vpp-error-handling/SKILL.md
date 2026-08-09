---
name: vpp-error-handling
description: Classify, implement, and review Hammer error handling using the VPP node-error, typed Result, failure-atomicity, and panic-boundary model. Use when adding or changing packet/node error counters, control-plane Result types, config or lifecycle validation, crate or plugin translation seams, cleanup paths, async/worker failure categories, or tests that assert error contracts.
---

# VPP Error Handling

## Gate

Treat error-path changes as a contract change, not a local code detail. Read root `AGENTS.md`, `CONTEXT.md`, and the relevant `docs/adr/` before editing. Search `third_party/vpp/` first and use VPP as the semantic and ownership reference, not a naming or API template.

## Classify First

Before choosing `Result`, a next arc, or a panic, classify the failure:

- Expected per-packet failure: the node or protocol owns a typed error code, a counter, and a next decision. Do not allocate, format, or return a control-plane `Result`.
- Recoverable control-plane failure: the owner that can act on it returns `Result<T, E>` with a concrete typed variant carrying diagnostic identities and the `#[source]` chain.
- Programmer bug or impossible state: assert or panic locally with the violated invariant and identities. Contain it at the worker/plugin/FFI boundary and do not continue over corrupted state.
- Boundary failure: distinguish cancellation, timeout, channel closure, panic, worker exit, and the inner typed error instead of collapsing them into one message variant.

## Ownership

Use the error authority that owns the failed operation:

- `hammer-infra` owns generic allocation, mapping, queue, and memory failures.
- `hammer-core` owns `DataPlaneError`, `BufferInvariant`, and failures intrinsic to packet-graph ABI values.
- `hammer-runtime` owns graph execution, FileMain, barriers, worker lifecycle, plugin loading, and runtime lifecycle failures.
- `hammer-service` owns interface, device, and session failures.
- Each plugin owns its protocol or device failures.

Do not move a business error downward just to reuse a type or avoid a dependency. Translate at most once at a real crate, process, or DSO seam, preserving category and source chain.

## Implementation Rules

- Name each recoverable variant after one actionable failure category. Add fields for identities needed to diagnose or retry.
- Do not introduce catch-all variants such as `Invariant`, `Internal(String)`, `Other`, `Message`, or generic `Subsystem`.
- Use `Option<T>` only when absence is an ordinary successful outcome.
- Preserve typed category and `#[source]` across seams. Do not use `to_string()`, `format!`, message matching, or repeated wrapper enums as propagation.
- Validate every participant before mutating shared graph, topology, registry, session, or plugin state. After the first mutation, either complete infallibly or roll back all sibling state before returning `Err`.
- Do not use `tracing`, `println!`, or log-only arms to handle an error. Logging is observability, not error handling.
- Do not discard errors with `let _`, `.ok()`, wildcard arms, `expect`, `panic!`, or `assert!` on a recoverable cleanup path. Cleanup failure must flow through a typed `Result` and must not replace the primary error.
- Keep packet hot paths worker-owned and lock-free. Record node errors through the runtime counter and `BufferNodeError` path; do not allocate an error object per packet.
- Do not unwind across an FFI or plugin ABI boundary. Contain panics at the owning runtime boundary and terminate that execution scope according to its lifecycle.
- Add new public error types or APIs only with the approval required by `AGENTS.md`. State the owner, recovery action, boundary consumers, and why existing types are insufficient.

## VPP Cleanup Sequence

VPP does not use a generic guard. After a failed mutation it runs an
explicit owner-local cleanup sequence, such as `session_lookup_del_half_open`,
`session_free`, `segment_manager_dealloc_fifos`, and then notifies the App with
the concrete error. Mirror that shape: call the concrete cleanup operations
owned by the same layer; do not invent a cleanup helper.

After a primary error, do not replace it with a log line or a successful
`Ok(())` from cleanup:

```rust
if let Err(primary) = operation() {
    return match cleanup() {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(OwnerError::OperationWithCleanup {
            operation: primary,
            cleanup,
        }),
    };
}
```

When both errors are recoverable, carry both in the owner-local error type with
structured fields. The primary operation remains the category that triggered
the failure; cleanup is a distinct source, never a display-only suffix.

For packet-path failures, cleanup or drop decisions use typed node errors and
next arcs. They do not log a control-plane failure and continue processing.

## Workflow

1. Read `CONTEXT.md`, `AGENTS.md`, and relevant ADRs.
2. Find the closest VPP counterpart in `third_party/vpp/`. Useful starting points are `vlib/error.c`, `vnet/tcp/tcp_input.c`, `vnet/tcp/tcp_output.c`, and `vnet/session/session_node.c`.
3. Classify the failure and the owner.
4. Change the smallest owner-local surface and ensure failure atomicity.
5. Add tests that match concrete variants and fields, verify source chains, and prove cleanup or retry behavior.
6. Run `cargo fmt --all -- --check`, focused `cargo test -p <crate>`, and `git diff --check`.

## Review Checklist

When reviewing an error diff, check the following:

- Packet-path failures use node error codes and next arcs, not `RuntimeResult`.
- New `Result` variants are typed and actionable; no catch-all or string-only errors.
- Cross-crate conversions preserve `#[source]` and translate once.
- Shared state is failure-atomic or uses an owner-scoped cleanup sequence.
- Boundary failures distinguish cancellation, timeout, closure, panic, exit, and inner error.
- Tests do not assert display strings and do not read source files to prove behavior.

Search the diff and touched files for legacy patterns before accepting it. See `references/hammer-error-contract.md` for exact project paths and examples.

Also search touched files for:

```sh
rg -n "tracing::(error|warn|debug|info)|let _ =|\.ok\(\)" <touched files>
```

Every hit on a recoverable or cleanup path is a blocking finding unless the
dropped failure is the documented domain semantics of that exact operation.

## Legacy Surfaces

The repo already has legacy error surfaces that `AGENTS.md` forbids extending. Do not add new uses; when a change touches their owning seam, plan a typed replacement or the approved cleanup:

- `RuntimeError::Subsystem` and `RuntimeError::subsystem`.
- String-only `RuntimeError::ConfigParse`, `ConfigValidation`, and `Lifecycle`.
- `TcpError::Dispatch` used for multiple timer, probe, and retransmit categories.
- Source-text or display-string-only error assertions in older tests.

Read `references/hammer-error-contract.md` for the current owner map, concrete code locations, VPP evidence, and review commands.
