# Issue 106 research: one ordinary-allocation authority across Rust dynamic images

Date: 2026-07-15
Scope: research only; no allocator implementation or tests were performed.

## Decision

The only direction authorized for validation is:

> Keep `hammer-infra` as the authority approved by issue 104, but make its final Rust `dylib` the one link image that contains `std`, the panic/runtime support, the allocator shim, and the single mimalloc authority. Every other shared Hammer library, executable, and plugin must dynamically reuse that image rather than loading a separate dynamic `libstd` or embedding another Rust runtime.

This direction is **Needs validation**, not yet an implementation decision. The only next experiment is an isolated build/link prototype of this topology. If that prototype fails, stop and report the blocker. Failure does not authorize a different allocator architecture or authority.

## Findings

### 1. The allocator shim belongs to a final Rust link image

- **External fact.** `alloc` calls compiler-known `__rust_alloc`, `__rust_dealloc`, `__rust_realloc`, and `__rust_alloc_zeroed` symbols. Rustc generates those functions as thin veneers either to the selected `#[global_allocator]` implementation or to `std`'s `__rdl_*` defaults. Sources: [`library/alloc/src/alloc.rs`](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/library/alloc/src/alloc.rs#L10-L39), [`library/std/src/alloc.rs`](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/library/std/src/alloc.rs#L438-L487), and rustc's [LLVM allocator shim generator](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/compiler/rustc_codegen_llvm/src/allocator.rs).
- **External fact.** If a final artifact dynamically links a Rust crate that already supplies an allocator shim, rustc does not generate another shim for that artifact. Source: [`allocator_kind_for_codegen`](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/compiler/rustc_codegen_ssa/src/base.rs#L627-L658).
- **External fact.** Rust marks the `__rdl_*` implementation symbols as internal implementation details that should not be a DLL API. Source: [rustc monomorphization visibility rules](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/compiler/rustc_monomorphize/src/partitioning.rs#L896-L929).
- **Project evidence.** The current macOS build contains two allocator shims. `libhammer_infra.dylib::__rust_alloc` calls `HammerMainHeap`; dynamic `libstd-cc9b12538d143830.dylib::__rust_alloc` branches directly to its own `__rdl_alloc`, which calls `System`. The executable and plugin imports bind to `libhammer_infra`, but allocation code already compiled inside `libstd` stays inside `libstd`'s shim.
- **Project evidence.** The prior LLDB run observed exactly that split: Hammer-instantiated allocation entered the fixed arena, while a post-init standard `String` allocation executing in dynamic `libstd` used a System mapping.

Consequence: a shared allocator state object and a process-wide allocator call path are different properties. `#[global_allocator]` in a dependency does not rewrite allocation calls already bound inside a separate dynamic `libstd` image.

### 2. Stable Rust has no supported override seam for a separate dynamic `libstd`

- **External fact.** The Rust standard-library contract describes one global allocator for a linked program, and `cdylib`/`staticlib` outputs use `System` by default when no allocator is selected. It does not expose a stable function-pointer or runtime setter that can retarget allocator shims in already-built Rust DSOs. Source: [`std::alloc`](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/library/std/src/alloc.rs#L1-L56).
- **External fact.** Rust issue [“global_allocator does not work with -C prefer-dynamic”](https://github.com/rust-lang/rust/issues/100781) remains open. Rust maintainers explicitly call out the possibility that dynamic `libstd` uses `System` while another image uses the custom allocator, with ELF preemption differing from Mach-O two-level namespaces.
- **External fact.** The related regression report [“Regression in global_allocator when using prefer-dynamic”](https://github.com/rust-lang/rust/issues/114518) documents mixed-allocator crashes and notes that `libstd` can resolve `__rust_alloc` to its own System allocator.
- **Inference.** Therefore, while dynamic `libstd` remains a separate image, stable Rust does not provide a documented, cross-platform, process-wide allocator seam that meets issue 104.

### 3. Rust linkage can remove the extra allocator boundary

- **External fact.** A Rust `dylib` may contain statically linked upstream crates, and rustc tracks those dependencies so every crate appears exactly once in a final artifact. Source: Rust Reference [Linkage](https://github.com/rust-lang/reference/blob/afdc77bab886d4455c11247cdd32391bfab636ae/src/linkage.md) and rustc's [`dependency_format`](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/compiler/rustc_metadata/src/dependency_format.rs#L1-L50).
- **External fact.** Without `-C prefer-dynamic`, rustc first tries to link dependencies statically into `dylib` and `cdylib` outputs. If a required dependency is only available dynamically, it falls back to a mixed graph and records crates already contained by that dylib as `IncludedFromDylib`. Source: [`calculate_type`](https://github.com/rust-lang/rust/blob/ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96/compiler/rustc_metadata/src/dependency_format.rs#L82-L215).
- **Project evidence.** This repository globally enables `-C prefer-dynamic`, causing every current final image to load dynamic `libstd`.
- **Inference.** A disposable build in which `hammer-infra` is the only available Rust authority dylib, statically contains `std`, and every higher Hammer image is forced to use its dylib rather than its rlib should produce one allocator shim and one Rust runtime across the process.
- **Needs validation.** Rustc has no stable per-dependency “prefer this crate dynamically” switch; its own source calls this out. The exact Cargo artifact layout needed to force the intended graph—likely removing rlib alternatives from the shared Hammer libraries in the prototype—must be proven on both platforms before editing the real build.

This is the sole authorized direction because it preserves the approved owner (`hammer-infra`) and the existing Rust `GlobalAlloc` bootstrap/provenance code instead of adding a new process ABI.

### 4. Linux native interposition options are rejected

- **External fact.** ELF `dlopen` resolves a newly loaded object's references first from the main program and its startup dependencies, then previously loaded `RTLD_GLOBAL` objects, then the new object and its dependencies. Executable symbols must be in the dynamic symbol table, commonly via `--export-dynamic`. Source: Linux [`dlopen(3)`](https://man7.org/linux/man-pages/man3/dlopen.3.html).
- **External fact.** GNU ld `--wrap` rewrites only undefined references processed by that link. It does not rewrite translation-unit-internal references and cannot retrofit a dynamic `libstd` or a later plugin DSO. Source: GNU ld [`--wrap`](https://sourceware.org/binutils/docs/ld/Options.html#index-_002d_002dwrap_003dsymbol).
- **External fact.** `-Bsymbolic` can bind a shared library's internal calls to itself, defeating preemption; `-z interpose` changes loader search order for one ELF shared object. Sources: GNU ld [`-Bsymbolic`](https://sourceware.org/binutils/docs/ld/Options.html#index-_002dBsymbolic) and [`-z interpose`](https://sourceware.org/binutils/docs/ld/Options.html).
- **External fact.** Mimalloc recommends `LD_PRELOAD` for dynamic ELF override or placing `mimalloc.o` first for a static final link. Source: mimalloc v3.3.2 [override documentation](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/readme.md#overriding-standard-malloc).
- **Disposition: rejected.** Issue 104 does not approve an ELF/glibc allocator ABI. `--wrap` lacks DSO coverage; symbol preemption depends on visibility and binding details; stock mimalloc override changes bootstrap allocation behavior. No Linux interposition prototype or ticket should be created.

### 5. macOS native interposition and malloc zones are rejected

- **External fact.** Mach-O uses a two-level namespace by default: each reference records the dylib that supplied it, so a same-named symbol in another image does not ordinarily replace it. Source: the macOS `ld(1)` two-level namespace documentation.
- **External fact.** Apple's supported interpose declaration is a tuple in `__DATA,__interpose`. Current dyld builds interpose tables only from dylibs loaded at process launch, and platform security may disallow interposing. Sources: Apple's [`dyld-interposing.h`](https://github.com/apple-oss-distributions/dyld/blob/fd8d0c4d52320ebf64db34f3cb280310d905c5ae/include/mach-o/dyld-interposing.h) and [`DyldRuntimeState.cpp`](https://github.com/apple-oss-distributions/dyld/blob/fd8d0c4d52320ebf64db34f3cb280310d905c5ae/dyld/DyldRuntimeState.cpp#L1159-L1333).
- **External fact.** Apple malloc zones support registering a custom zone and identifying an allocation's owning zone, but the public `malloc_zone_register` API does not expose the internal `make_default` operation. Sources: Apple's [`malloc.h`](https://github.com/apple-oss-distributions/libmalloc/blob/c49dafa25f1efe8607701ae6014a663ad2ee437f/include/malloc/malloc.h#L388-L507) and [`malloc_zone_malloc(3)`](https://github.com/apple-oss-distributions/libmalloc/blob/c49dafa25f1efe8607701ae6014a663ad2ee437f/man/malloc_zone_malloc.3).
- **External fact.** Mimalloc's dynamic macOS override is documented through `DYLD_INSERT_LIBRARIES`. Its static build falls back to malloc-zone registration unless compiled as a shared mimalloc library with interpose exports. Sources: mimalloc [override documentation](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/readme.md#dynamic-override-on-macos), [`alloc-override.c`](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/src/alloc-override.c), and [`alloc-override-zone.c`](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/src/prim/osx/alloc-override-zone.c).
- **Disposition: rejected.** Environment injection is forbidden, launch interposition adds a new native authority seam and security constraints, and zone-order manipulation is not the approved Hammer allocator boundary. No macOS interposition prototype or ticket should be created.

### 6. Bootstrap and provenance are hard acceptance gates

Any accepted topology must preserve these behaviors already encoded in `main_heap.rs`:

- **Project evidence.** Before `READY`, allocation, zeroed allocation, and realloc use `System`.
- **Project evidence.** After `READY`, new ordinary allocations use mimalloc; no System fallback is allowed.
- **Project evidence.** Free is routed by address provenance. A bootstrap pointer remains a System pointer even if freed after initialization.
- **Project evidence.** Realloc of a bootstrap pointer after initialization allocates from Main Heap, copies the prefix, and frees the original through `System`.
- **Project evidence.** Main Heap is reserved before plugins or worker threads, then OS allocation is disabled for the sole mimalloc authority.

Required gates for the linkage prototype and later implementation:

1. No mimalloc allocation before explicit Main Heap initialization.
2. Exactly one mimalloc state image and one reserved arena.
3. Every successful post-init ordinary allocation lies inside that arena.
4. Exhaustion returns allocation failure/abort according to Rust allocation handling; it never creates another mapping.
5. Bootstrap pointers can be freed and reallocated after initialization without cross-allocator free.
6. Buffer Arena and SVM addresses and allocators remain unchanged.

### 7. Mimalloc arenas fit only when there is one mimalloc image

- **External fact.** The locked `libmimalloc-sys 0.1.49` vendors mimalloc v3.3.2 commit [`30b2d9d`](https://github.com/microsoft/mimalloc/tree/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227).
- **External fact.** `mi_reserve_os_memory_ex` reserves an arena; `mi_option_disallow_os_alloc`/legacy `mi_option_limit_os_alloc` prevents new OS allocation and allows only programmatically reserved arenas. Sources: [`mimalloc.h`](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/include/mimalloc.h#L330-L341), [`mimalloc.h` options](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/include/mimalloc.h#L445-L467), and [`arena.c`](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/src/arena.c#L506-L550).
- **External fact.** Mimalloc options are stored in one static `mi_options` array, and arena calls operate on that mimalloc image's current sub-process state. They are process-wide only if the process contains one mimalloc link image. Source: [`options.c`](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/src/options.c#L111-L152).
- **External fact.** Mimalloc v3 first-class heaps can allocate from any thread, and `mi_heap_new_in_arena` restricts a heap to one arena. Sources: v3.3.2 [README](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/readme.md) and [`heap.c`](https://github.com/microsoft/mimalloc/blob/30b2d9d89099bee08e9f67a1ffb3e12e7ba45227/src/heap.c#L90-L135).
- **Inference.** The existing non-exclusive reserved arena plus OS-allocation limit is sufficient only if no separate mimalloc image and no pre-init mimalloc pages exist. The recommended single-image Rust linkage preserves exactly that condition.

## Candidate matrix

| Candidate | Linux | macOS | Stable Rust | Dynamic-`libstd` coverage | Late-plugin coverage | Bootstrap safety | Fixed-capacity enforcement | ABI/link risks | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| Current dependency `#[global_allocator]` with separate dynamic `libstd` | ELF behavior can differ by preemption | Proven split under two-level namespace | Attribute is stable; topology is not a supported process seam | No | Partial: plugin-local code can bind authority, `libstd` cannot | Current wrapper is safe | Fails because `libstd` allocates outside arena | Mixed alloc/free; platform divergence | Reject |
| `hammer-infra` is the sole Rust runtime + allocator-shim dylib; no separate dynamic `libstd` | Expected yes | Expected yes | Yes | Removes the separate image | Expected yes | Preserves current code | Preserves current arena/limit design | Cargo/rustc dependency selection must be proven | **Recommend; validate only this** |
| Interpose Rust `__rust_*`/`__rdl_*` symbols | ELF-only possibility | Two-level namespace prevents ordinary override | No: compiler-internal ABI | Fragile | Fragile | Possible only with custom shims | Possible only with custom shims | Toolchain-mangled hidden/internal symbols | Reject |
| GNU `--wrap` / Mach-O linker alias | Final-link references only | Alias does not retarget dylib bindings | N/A | No | No | N/A | N/A | Does not process already-linked/later DSOs | Reject |
| Stock mimalloc malloc override | Requires preload or link-order/preemption assumptions | Requires insertion or zone behavior | C ABI, not Rust allocator support | C calls only | Platform-dependent | No: mimalloc becomes active before explicit init | Cannot prove all post-init blocks came only from reserved arena | Foreign-pointer and startup-order risks | Reject |
| Custom process `malloc` interposer routing System then mimalloc | Technically possible with ELF/glibc-specific machinery | Technically possible with launch interpose | C ABI | Potentially | Potentially | Would require a new provenance wrapper | Would require a new dedicated arena path | New native authority ABI; security/loader coupling | Reject; no fallback |
| Per-plugin forwarding allocator with separately linked Rust runtimes | Possible | Possible | Yes per image | No shared runtime | Yes per plugin | Custom per image | Authority may forward, runtime still duplicated | Panic/TLS/Rust-runtime duplication | Reject |

## Sole next validation experiment

Classification: **Needs validation**.

Use a disposable copy; do not edit or clean the dirty primary worktree.

1. Disable the workspace-wide `-C prefer-dynamic` in the disposable copy.
2. Make the shared Hammer Rust libraries available to final links only in the dylib form required to force the dependency chain through `hammer-infra`; do not add a new crate, type, or allocator API.
3. Build, but do not run the test suite.
4. On macOS and Linux, inspect the executable, hammerctl, `hammer-infra`, `hammer-core`, `hammer-runtime`, `hammer-service`, and all four plugins.
5. The prototype succeeds only if:
   - no final artifact has a standalone dynamic `libstd` dependency;
   - `hammer-infra` alone contains `std`, the panic runtime, allocator shim definitions, Main Heap state, and mimalloc symbols;
   - every higher Hammer dylib and plugin imports its allocator shim from `hammer-infra`;
   - no artifact embeds another `hammer-infra`, mimalloc, `std`, or panic-runtime copy;
   - the real late-plugin example observes Main Heap addresses for host and plugin `String`, `Vec`, `PathBuf`, `Arc`, and serde/TOML allocations;
   - bootstrap free/realloc, exhaustion, Buffer Arena, and SVM invariants remain observable.

If any of those conditions fails, stop and report which rustc dependency-format or symbol-binding fact invalidated the direction. Do not select another allocator architecture without explicit user direction.

## Cross-platform artifact validation matrix

| Artifact | Dependency check | Definition/import check | Runtime/provenance check |
|---|---|---|---|
| `hammer` | Loads shared Hammer dylibs; no standalone dynamic `libstd` | Imports allocator shim from `hammer-infra`; no mimalloc definition | Host ordinary allocations inside Main Heap after init |
| `hammerctl` | Same runtime carrier; no standalone dynamic `libstd` | Same allocator import | Same bootstrap/post-init behavior |
| `hammer-infra` | Owns statically included Rust runtime; no dynamic `libstd` | Sole `__rust_*` shim, Main Heap state, and `mi_*` authority | Shim disassembles to `HammerMainHeap`, not `__rdl_alloc` |
| `hammer-core` | Dynamically depends on `hammer-infra`; no dynamic `libstd` | No allocator or mimalloc definition | Standard allocations reach authority |
| `hammer-runtime` | Dynamically depends on lower shared images | No independent runtime/allocator | Threads preserve same authority id |
| `hammer-service` | Dynamically depends on lower shared images | No independent runtime/allocator | Service/control allocations stay in Main Heap |
| tun plugin | Late-loaded; depends on shared Hammer images; no dynamic `libstd` | Allocator import resolves to `hammer-infra` | Plugin `String`/`Vec` allocation inside Main Heap |
| IP plugin | Same | Same; no mimalloc | Same plus registration lifetime |
| TCP plugin | Same | Same; no mimalloc | Same under worker/thread activity |
| UDP plugin | Same | Same; no mimalloc | Same |

Platform tools:

- Linux: `readelf -d`, `readelf -Ws`, `objdump -dr`, loader binding diagnostics, `/proc/<pid>/maps`.
- macOS: `otool -L`, `nm -m`, `otool -Iv`/dyld bind information, LLDB disassembly/breakpoints, `vmmap`.
- Both: the real `dlopen` example and observable address/provenance assertions; no source-text architecture tests.
