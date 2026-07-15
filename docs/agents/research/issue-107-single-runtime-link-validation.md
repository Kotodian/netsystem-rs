# Issue 107 validation: the single Rust-runtime authority dylib is not linkable

Date: 2026-07-15

## Result

**Blocked by stable rustc dependency-format semantics.** The only authorized topology cannot produce a second Rust final artifact that consumes a `hammer-infra` dylib which statically contains `std`, `core`, `alloc`, the panic runtime, and their transitive runtime crates.

No allocator implementation, interposition mechanism, forwarding shim, alternate authority, or primary-worktree build change was selected from this result.

## Environment

- `rustc 1.96.0 (ac68faa20 2026-05-25) (Homebrew)`
- `cargo 1.96.0 (30a34c682 2026-05-25) (Homebrew)`
- Host: `aarch64-apple-darwin`
- Primary worktree: dirty and preserved
- Prototype: disposable copy under `/tmp`

## Prototype topology

The disposable copy applied only the topology authorized by issues 104, 106, and 107:

1. Removed workspace-wide `-C prefer-dynamic`, retaining `-C rpath`.
2. Made `hammer-infra`, `hammer-core`, `hammer-runtime`, and `hammer-service` available as `dylib` only.
3. Built `hammer-infra` so it statically contained the Rust runtime and allocator authority.
4. Attempted to build `hammer-core` as the next Rust dylib consumer.

`hammer-infra` built successfully. `hammer-core` failed before linking with rustc's dependency-format error:

```text
error: cannot satisfy dependencies so `std` only shows up once
  = help: having upstream crates all available in one format will likely make this go away
  = note: `hammer_infra` was unavailable as a static crate, preventing fully static linking
```

Rustc reported the same conflict for `core`, `alloc`, `compiler_builtins`, `libc`, unwind/panic support, and the other runtime crates already included by `hammer-infra`.

Adding `-C prefer-dynamic` only to the `hammer-core` final link produced the same error. It cannot turn the runtime already included in `hammer-infra` into the consumer's supported sysroot dependency format.

## Minimal reproduction

To separate the result from Hammer's crate graph, the prototype compiled a minimal consumer against the produced `libhammer_infra.dylib`:

```rust
fn main() {
    println!("{}", hammer_infra::main_heap::capacity());
}
```

Both the default link and `-C prefer-dynamic` failed with the same `std`/`core`/`alloc` single-copy errors.

A `#![no_std]` Rust dylib consumer also failed with the same dependency-format errors. An `rlib` consumer can record the dependency because it is not a final link, but the failure returns when producing the next dylib, cdylib, executable, or test binary.

## Conclusion

Stable Rust can consume the current Hammer Rust dylib graph when the dylibs share the toolchain's dynamic `libstd`. Issue 106 already proved that this separate dynamic `libstd` owns a System-backed allocator shim on macOS, so it does not meet the process-global Main Heap requirement.

Conversely, a `hammer-infra` dylib that statically owns `std` and the allocator shim cannot be consumed by the higher Rust final images required by Hammer. The failure occurs in rustc's dependency-format selection before platform loader behavior, so the authorized Linux/macOS topology is invalidated at the common Rust link layer.

Issue 107 therefore has no successful implementation result. Under the explicit no-alternate-architecture constraint, this is a blocker rather than permission to introduce per-image forwarding allocators, native interposition, a custom `libstd`, or a different authority.
