# Ordinary allocation uses one fixed process-global Main Heap

Status: accepted

## Context

Hammer executables, shared Hammer Rust libraries, dynamic `libstd`, and late-loaded plugin DSOs are separate link images. A Rust `#[global_allocator]` embedded independently in each image cannot establish one allocator authority, while statically embedding Rust runtime crates in the shared `hammer-infra` dylib is rejected by stable rustc because the final link would contain duplicate runtime crates.

Issue #104 also retires allocator-specific ordinary collections. Standard `String`, `Vec`, `PathBuf`, `Box`, `Arc`, serde/TOML values, and third-party allocations therefore need one process allocation seam without changing the explicit Buffer Arena and SVM allocation domains.

## Decision

The shared `hammer-infra` dylib owns one fixed-capacity mimalloc arena and the process allocation ABI:

- Before Main Heap `READY`, Rust bootstrap allocation uses `std::alloc::System`. Native malloc-family calls are routed to the original operating-system allocator: malloc zones on macOS and glibc `__libc_*` entry points on Linux.
- Process entry points load only `BootstrapConfig`, whose result contains `memory.main_heap_size` and no owned paths or parsed TOML state. The bootstrap `PathBuf`, include lists, TOML documents, parser state, and all other bootstrap temporaries are dropped before `READY` is published.
- `hammer-infra::main_heap::init` reserves the configured arena through `libmimalloc-sys`, disables further mimalloc OS allocation, publishes the arena provenance, and moves the lifecycle one way to `READY`.
- After `READY`, ordinary Rust and native malloc-family allocation calls use only mimalloc (`mi_malloc*`, `mi_calloc`, `mi_realloc*`, and `mi_free`) inside that reserved arena. Exhaustion returns allocation failure; it never falls back to `System` or another mapping.
- Bootstrap pointers that are freed after `READY` retain system provenance. Reallocation migrates them into the Main Heap only after a successful mimalloc allocation and then releases the original pointer through its operating-system allocator.
- After `READY`, the daemon reconstructs the config path and loads the complete `Config` exactly once. Repeating `main_heap::init` only verifies that the final configured capacity matches the published capacity.

Hammer implements only `GlobalAlloc` routing, native malloc ABI interposition, arena lifecycle, and provenance decisions. It does not implement bins, slabs, free lists, page allocation policy, allocator metadata, or any other allocation algorithm. Those mechanics remain owned by mimalloc and the operating-system allocator.

## Allocation-domain exceptions

- Buffer headers and packet payload remain inside the Buffer Arena `PhysmemMap`. Buffer control metadata may use the Main Heap. Warmed buffer allocation/free must not perform an ordinary heap allocation, and the mapped domain never falls back to the Main Heap.
- SVM payload remains inside its owning SVM mapping and is allocated/freed by that mapping's region allocator. SVM control metadata may use the Main Heap. Attached or exhausted SVM mappings never fall back to the Main Heap.

## Link and layer contract

- `hammer-infra` alone owns the mimalloc arena state and native allocator symbols.
- Executables, shared Hammer libraries, and plugin DSOs dynamically depend on the same `hammer-infra` image and must not embed another mimalloc authority.
- macOS uses `__DATA,__interpose` records for the malloc family, including typed malloc entry points. Linux exports the malloc family from `hammer-infra`, bypasses it through glibc `__libc_*` calls before `READY`, and resolves the original `malloc_usable_size` through `RTLD_NEXT` for bootstrap-pointer migration.
- No plugin-specific, transport-specific, or public diagnostic allocator interface is introduced. Cross-image validation uses real loading, behavior, and image dependency/symbol inspection.
- Ordinary storage uses standard collections through the Main Heap. Packet-path exact lookup uses the owning dataplane primitive, such as Bihash or mtrie; the removed `hammer-infra::Vec`, `vec!`, `FlatHashTable`, and `FlatHashKey` surfaces are not replaced by another facade.

## Consequences

The configured Main Heap is a hard process memory budget. Startup fails if the arena cannot be reserved, and post-`READY` allocation failure is terminal for infallible Rust allocation callers. This is intentional: capacity changes require a process restart, not runtime expansion or allocator switching.

The design adapts VPP's fixed main-heap authority and explicit mapped-domain ownership, but uses mimalloc rather than porting VPP's allocator implementation.

## Verification

Linux and macOS CI run the same repository targets used locally. The dedicated
`release-perf` profile keeps optimized code and the supported dynamic-link
topology while disabling LTO for integration-test and benchmark final links.

```text
make verify-allocation-contract
make verify-dataplane-performance
```

The real plugin example loads tun, ip, tcp, and udp DSOs, verifies their shared `hammer-infra` dependency, rejects embedded mimalloc authorities, exercises dynamic-libstd allocation after `READY`, and completes a live graph update.
