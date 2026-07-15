## Destination

Choose and document one deployable Linux/macOS process-level Ordinary Allocation Authority that covers dynamic `libstd`, shared Hammer Rust `dylib`s, and late-loaded plugin `cdylib`s while preserving bootstrap System storage, one fixed-capacity Main Heap, Buffer Arena ownership, and SVM ownership.

## Notes

- Issue #104 is authoritative for the approved architecture and acceptance criteria.
- There is no alternate allocator architecture in this map. Validate only the single `hammer-infra` Rust-runtime/allocator image; a failed validation is a blocker.
- Consult `rust-router`, `m10-performance`, and `unsafe-checker` while resolving allocator/linkage tickets.
- Research must use primary sources and distinguish project evidence, external fact, inference, and needs validation.
- Planning only: do not implement the allocator solution until the authority decision is approved.

## Decisions so far

- [Establish supported process-wide allocator seams across Rust dylibs](https://github.com/Kotodian/hammer-ios-rs/issues/106) — The only authorized validation path makes `hammer-infra` the sole Rust runtime, allocator-shim, and mimalloc dylib; failure is a blocker.
- [Validate the single hammer-infra Rust-runtime allocator topology](https://github.com/Kotodian/hammer-ios-rs/issues/107) — Stable rustc cannot link a Rust consumer against a `hammer-infra` dylib that statically contains the Rust runtime, so the only authorized topology is blocked.

## Not yet specified

## Out of scope

- Replacing Buffer Arena PhysmemMap storage or SVM mmap-backed payload storage.
- Per-plugin heaps, allocator switching at runtime, or activated-plugin unload.
- Resuming ordinary-collection migration before the process allocation seam is proven.
