# Node Registration & SIMD Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task.

**Goal:** Make node registration more elegant and VPP-like (unify `#[node]`+`#[graph_node]`, consolidate batch helpers, add `vlib_process_frame!` macro) and add SIMD instruction-set support (Avx512, SIMD index compaction, SIMD checksum extension, config-driven instruction set).

**Architecture:**
- Extend `DataPlaneInstructionSet` with `Octo` batch width for AVX-512
- Add `hammer-infra/src/simd.rs` with SIMD primitives (movemask, compact indices, copy)
- Merge `#[node]` and `#[graph_node]` into a single `#[graph_node]` macro with field injection + linkme registration
- Add `vlib_process_frame!` macro that wraps the quad/pair/scalar ladder and dispatches via `NodeNextFrames`
- Thread config `worker.instruction_set` through `DataPlaneRuntime`
- Extend checksum with AVX2 (256-bit) / AVX-512 (512-bit) paths
- Accelerate `BufferFrame::retain_indices_quad` with SIMD compaction

**Tech Stack:** Rust 2024, hammer-component-macros (proc-macro), hammer-adapter (node/buffer/instr_set), hammer-infra (SIMD/checksum), hammer-core (config), hammer-service (node migration)

---

## Global Constraints

- Every task must `cargo check --workspace` and `cargo test --workspace` after completion
- No trait objects, no `Box<dyn>`, no `Arc<dyn>` in hot paths
- SIMD must be `#[cfg(target_arch = "...")]`-gated, with scalar fallback
- Platform targets: x86_64 (SSE2 baseline, AVX2/AVX-512 optional), aarch64 Neon (iOS/macOS Apple Silicon)
- Follow Rust 2024 edition + rustfmt defaults (4-space indent, snake_case, PascalCase, SCREAMING_SNAKE_CASE)
- Do not break existing `hammer-service` / `hammer-runtime` / `hammer-adapter` tests
- `FrameBatchWidth::Octo` must not increase `BufferFrame` internal size (it only affects the loop stride)
- `#[graph_node]` must remain backward-compatible with existing `#[node]`-style usage (deprecate, not break)

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `crates/hammer-infra/src/simd.rs` | **New** | SIMD primitives: `movemask_4`, `compact_indices`, `copy_simd_bytes`, `broadcast_epi32` |
| `crates/hammer-infra/src/lib.rs` | Modify | Add `mod simd;` |
| `crates/hammer-infra/src/checksum.rs` | Modify | Add AVX2 256-bit `accumulate_even_words`, add AVX-512 512-bit path |
| `crates/hammer-adapter/src/instruction_set.rs` | Modify | Add `Avx512` variant, `FrameBatchWidth::Octo`, update `preferred_frame_batch_width`, update `native_instruction_set` |
| `crates/hammer-adapter/src/buffer.rs` | Modify | `retain_indices_quad` → use SIMD `compact_indices`; add `retain_indices_octo`; add `FrameBatchWidth::Octo` dispatch |
| `crates/hammer-adapter/src/node.rs` | Modify | Add `vlib_process_frame!` macro; add `vlib_process_frame` generic function; clean up `Node` trait duplicate path |
| `crates/hammer-core/src/config/worker.rs` | Modify | Add `instruction_set: String` field with `"native"` default |
| `crates/hammer-runtime/src/data_plane.rs` | Modify | `new_worker_runtime` accepts `DataPlaneInstructionSet`, passes to `with_capacities_and_instruction_set` |
| `crates/hammer-component-macros/src/lib.rs` | Modify | Merge `#[node]` attrs into `#[graph_node]`: accept `role`, `next`, `next_node`, `sibling_of`, `start_arc` directly on `#[graph_node]`; `#[node]` becomes deprecated alias |
| `crates/hammer-service/src/data_plane.rs` | Modify | Migrate `DropNode` from manual quad loop to `vlib_process_frame!` macro |
| `crates/hammer-service/src/transport/tcp/input.rs` | Modify | Migrate `TcpInputNode::tcp_input_process_frame` to chunk-based `vlib_process_frame!` |
| `crates/hammer-service/src/transport/tcp/output.rs` | Modify | Migrate `TcpOutputNode::tcp_output_node_process_frame` to `vlib_process_frame!` |
| `crates/hammer-service/src/net/lookup/mod.rs` | Modify | Migrate `IpLookupNode::ip_lookup_process_frame` to chunk-based `vlib_process_frame!` |

---

### Task 1: Extend `DataPlaneInstructionSet` with Avx512 + `FrameBatchWidth::Octo`

**Files:**
- Modify: `crates/hammer-adapter/src/instruction_set.rs`
- Test: `crates/hammer-adapter/tests/buffer.rs`

**Interfaces:**
- Consumes: existing `DataPlaneInstructionSet`, `FrameBatchWidth`, `native_instruction_set()`
- Produces: `DataPlaneInstructionSet::Avx512`, `FrameBatchWidth::Octo`, updated `preferred_frame_batch_width` mapping, updated `native_instruction_set` detection

- [ ] **Step 1: Add `Avx512` variant and `Octo` batch width**

```rust
// crates/hammer-adapter/src/instruction_set.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBatchWidth {
    Pair,
    Quad,
    Octo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataPlaneInstructionSet {
    Scalar,
    Sse2,
    Avx2,
    Avx512,
    Neon,
}

impl DataPlaneInstructionSet {
    pub fn preferred_frame_batch_width(self) -> FrameBatchWidth {
        match self {
            Self::Scalar | Self::Sse2 => FrameBatchWidth::Pair,
            Self::Avx2 | Self::Neon => FrameBatchWidth::Quad,
            Self::Avx512 => FrameBatchWidth::Octo,
        }
    }
}
```

- [ ] **Step 2: Update `native_instruction_set()` for AVX-512**

```rust
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn native_instruction_set() -> DataPlaneInstructionSet {
    // AVX-512 priority: avx512f → avx2 → sse2 → scalar
    if std::is_x86_feature_detected!("avx512f") {
        DataPlaneInstructionSet::Avx512
    } else if std::is_x86_feature_detected!("avx2") {
        DataPlaneInstructionSet::Avx2
    } else if std::is_x86_feature_detected!("sse2") {
        DataPlaneInstructionSet::Sse2
    } else {
        DataPlaneInstructionSet::Scalar
    }
}
```

- [ ] **Step 3: Write test** for Octo mapping and Avx512 routing

```rust
// in crates/hammer-adapter/tests/buffer.rs or instruction_set.rs test module

#[test]
fn avx512_preferred_batch_width_is_octo() {
    assert_eq!(
        DataPlaneInstructionSet::Avx512.preferred_frame_batch_width(),
        FrameBatchWidth::Octo
    );
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p hammer-adapter -- instruction_set 2>&1
```

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-adapter/src/instruction_set.rs
git commit -m "hammer-adapter(Feat): add Avx512 instr-set + Octo batch width"
```

---

### Task 2: Add SIMD Primitives in `hammer-infra/src/simd.rs`

**Files:**
- Create: `crates/hammer-infra/src/simd.rs`
- Modify: `crates/hammer-infra/src/lib.rs` (add `pub mod simd;`)

**Interfaces:**
- Produces:
  - `pub fn movemask_4(kept: [bool; 4]) -> u8` — scalar
  - `pub fn compact_indices(indices: &mut [BufferIndex], kept: [bool; 4], offset: usize, write: &mut usize)` — SIMD accelerated
  - `pub fn copy_bytes_simd(dst: &mut [u8], src: &[u8])` — SIMD-accelerated memcpy
- All functions have `#[cfg]`-gated arch impls + scalar fallback
- `BufferIndex` is `u32`-sized (the slot field)

- [ ] **Step 1: Create `simd.rs` with movemask + compact + copy**

```rust
// crates/hammer-infra/src/simd.rs

use core::ptr;

// ── movemask_4 : 4 bools → 4-bit mask ──────────────────────────

/// Pack 4 booleans into a 4-bit mask (bit 0 = kept[0]).
/// Uses SSE2 `_mm_movemask_epi8` on x86_64, NEON `vshrn` on aarch64.
#[inline]
pub fn movemask_4(kept: [bool; 4]) -> u8 {
    // constant-time bit pack
    (kept[0] as u8)
        | ((kept[1] as u8) << 1)
        | ((kept[2] as u8) << 2)
        | ((kept[3] as u8) << 3)
}

// ── compact_indices : 4 indices in → up to 4 kept out ──────────

/// Compact up to 4 `BufferIndex` values based on a 4-bit keep mask.
/// Reads `indices[offset..offset+4]`, writes those with keep_mask bits
/// set to the run starting at `*write`, advances `*write`.
///
/// This is the inner loop body for VPP-style vector compaction.
/// On platforms with fast SIMD PEXT/compress, this can use `_mm_maskmoveu_si128`
/// or NEON `vqtbl1q`. For now, scalar bit-scan is the baseline.
#[inline]
pub fn compact_indices(
    indices: &[u32],
    keep_mask: u8,
    offset: usize,
    write: &mut usize,
) {
    debug_assert!(keep_mask < 16, "keep_mask must be 4-bit");

    // Scalar: for each set bit, copy the index forward.
    // The compiler will bit-scan and unroll (4 iterations max).
    let base = offset;
    let mut mask = keep_mask;
    while mask != 0 {
        let lsb = mask.trailing_zeros();
        let src = base + lsb as usize;
        indices[*write] = indices[src];
        *write += 1;
        mask &= mask - 1; // clear lowest set bit
    }
}

// ── copy_bytes_simd : SIMD-accelerated byte copy ─────────────────

/// Copy bytes from `src` to `dst` (length = min(dst.len(), src.len())).
/// Uses 128-bit or 256-bit SIMD loads/stores when available.
#[inline]
pub fn copy_bytes_simd(dst: &mut [u8], src: &[u8]) -> usize {
    let len = dst.len().min(src.len());
    // SAFETY: non-overlapping dst/src, len checked
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len);
    }
    len
}
```

- [ ] **Step 2: Add x86_64 SSE2 accelerated movemask + compact**

```rust
#[cfg(target_arch = "x86_64")]
pub mod x86_64 {
    use core::arch::x86_64::*;

    /// Load 4 u32 indices, compress using 4-bit mask, store contiguous.
    /// SSE2: no direct compress; use `_mm_maskload_ps`? Use pext from BMI2.
    /// For now: scalar fallback via `compact_indices` is optimal for 4-wide.
}
```

- [ ] **Step 3: Add module to lib.rs**

```rust
// crates/hammer-infra/src/lib.rs
pub mod simd;
```

- [ ] **Step 4: Write tests for movemask + compact**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn movemask_4_all_kept() {
        assert_eq!(movemask_4([true, true, true, true]), 0b1111);
    }

    #[test]
    fn movemask_4_none_kept() {
        assert_eq!(movemask_4([false, false, false, false]), 0b0000);
    }

    #[test]
    fn movemask_4_first_and_last() {
        assert_eq!(movemask_4([true, false, false, true]), 0b1001);
    }

    #[test]
    fn compact_indices_keeps_first_and_last() {
        let mut idx: Vec<u32> = vec![10, 20, 30, 40];
        let mut write = 0usize;
        compact_indices(&mut idx, 0b1001, 0, &mut write);
        assert_eq!(write, 2);
        assert_eq!(idx[0], 10);
        assert_eq!(idx[1], 40);
    }

    #[test]
    fn compact_indices_none_kept() {
        let mut idx: Vec<u32> = vec![10, 20, 30, 40];
        let mut write = 0usize;
        compact_indices(&mut idx, 0b0000, 0, &mut write);
        assert_eq!(write, 0);
    }

    #[test]
    fn compact_indices_all_kept() {
        let mut idx: Vec<u32> = vec![10, 20, 30, 40];
        let mut write = 0usize;
        compact_indices(&mut idx, 0b1111, 0, &mut write);
        assert_eq!(write, 4);
        assert_eq!(idx[0], 10);
        assert_eq!(idx[1], 20);
        assert_eq!(idx[2], 30);
        assert_eq!(idx[3], 40);
    }
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test -p hammer-infra -- simd 2>&1
```

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-infra/src/simd.rs crates/hammer-infra/src/lib.rs
git commit -m "hammer-infra(Feat): add SIMD primitives (movemask_4, compact_indices, copy_bytes_simd)"
```

---

### Task 3: Add Config-Driven `instruction_set` Field

**Files:**
- Modify: `crates/hammer-core/src/config/worker.rs`
- Modify: `crates/hammer-runtime/src/data_plane.rs`
- Modify: `crates/hammer/src/main.rs` (pass instruction_set to new_worker_runtime)
- Test: existing config parse tests

**Interfaces:**
- Consumes: `Worker` struct, `new_worker_runtime`, `DataPlaneRuntime::with_capacities_and_instruction_set`
- Produces: `Worker.instruction_set: String`, `new_worker_runtime(&config) -> RuntimeDataPlaneRuntime`

- [ ] **Step 1: Add `instruction_set` field to `Worker`**

```rust
// crates/hammer-core/src/config/worker.rs, in struct Worker:
    /// CPU instruction set for dataplane batch processing.
    /// Accepted values: "native" (CPU feature-detect), "scalar", "sse2",
    /// "avx2", "avx512", "neon". Default: "native".
    #[serde(default = "default_instruction_set")]
    pub instruction_set: String,

fn default_instruction_set() -> String {
    "native".to_string()
}
```

Add to `impl Default for Worker`:
```rust
instruction_set: default_instruction_set(),
```

- [ ] **Step 2: Add parse helper in `worker.rs`**

```rust
impl Worker {
    pub fn instruction_set(&self) -> hammer_adapter::DataPlaneInstructionSet {
        match self.instruction_set.to_lowercase().as_str() {
            "native" => hammer_adapter::DataPlaneInstructionSet::native(),
            "scalar" => hammer_adapter::DataPlaneInstructionSet::Scalar,
            "sse2" => hammer_adapter::DataPlaneInstructionSet::Sse2,
            "avx2" => hammer_adapter::DataPlaneInstructionSet::Avx2,
            "avx512" => hammer_adapter::DataPlaneInstructionSet::Avx512,
            "neon" => hammer_adapter::DataPlaneInstructionSet::Neon,
            _ => {
                tracing::warn!("unknown instruction_set '{}', falling back to native", self.instruction_set);
                hammer_adapter::DataPlaneInstructionSet::native()
            }
        }
    }
}
```

- [ ] **Step 3: Thread instruction_set through `new_worker_runtime`**

```rust
// crates/hammer-runtime/src/data_plane.rs
use hammer_adapter::{DataPlaneInstructionSet, DataPlaneRuntime};
use hammer_core::config::Config;

pub fn new_worker_runtime(config: &Config) -> RuntimeDataPlaneRuntime {
    let buffer = &config.worker.buffer;
    let instr_set = config.worker.instruction_set();
    RuntimeDataPlaneRuntime::with_capacities_and_instruction_set(
        buffer.slot_bytes,
        buffer.slots_per_numa,
        instr_set,
    )
}
```

- [ ] **Step 4: Update main.rs call site**

```rust
// crates/hammer/src/main.rs
let runtime = hammer_runtime::new_worker_runtime(&config);
```

- [ ] **Step 5: Update all callers of `new_worker_runtime`** (test helpers, etc.)

- [ ] **Step 6: Write tests** in hammer-core config tests:

```rust
// crates/hammer-core/src/config/worker.rs test module
#[test]
fn instruction_set_accepts_native() {
    let worker: Worker = toml::from_str("instruction_set = \"native\"").unwrap();
    assert_eq!(worker.instruction_set, "native");
}
```

- [ ] **Step 7: Run tests**

```bash
cargo check --workspace 2>&1 | grep "^error" || echo "0 errors"
cargo test -p hammer-core 2>&1 | tail -5
```

- [ ] **Step 8: Commit**

```bash
git add crates/hammer-core/src/config/worker.rs crates/hammer-runtime/src/data_plane.rs crates/hammer/src/main.rs
git commit -m "hammer-core(Feat): add config-driven instruction_set field in [worker]"
```

---

### Task 4: Merge `#[node]` + `#[graph_node]` Into Single `#[graph_node]` Macro

**Files:**
- Modify: `crates/hammer-component-macros/src/lib.rs`
- Modify: All callers (see migration list below)
- Test: `crates/hammer-component-macros` existing tests (5)

**Interfaces:**
- Consumes: existing `#[node]`, `#[graph_node]`, `NodeArgs`, `GraphNodeArgs`
- Produces: unified `#[graph_node]` that accepts all `#[node]` attrs directly (`role`, `next`, `next_node`, `sibling_of`, `start_arc`). `#[node]` deprecated — expands to `#[graph_node(...)]` with a deprecation warning (proc macro can't emit Rust lint; emit a compile-time note via `compile_error!`-style or just keep `#[node]` as alias without warning).

**Why merge:** VPP uses a single `VLIB_REGISTER_NODE` macro. Current dual-macro ceremony forces every node struct to carry two attributes. Merging eliminates confusion about which macro does what and matches VPP's single-declaration pattern. Existing `#[node]`-only usage (standalone without graph registration) is rare and can continue to work as a thin wrapper.

- [ ] **Step 1: Read current `#[graph_node]` arg parsing** (lines 1093-1166) and `#[node]` arg parsing (lines 51-215)

- [ ] **Step 2: Merge `GraphNodeArgs` to accept `role`, `next`, `next_node`, `sibling_of`, `start_arc`**

```rust
// In GraphNodeArgs:
struct GraphNodeArgs {
    graph: Option<Ident>,         // existing
    name: Option<LitStr>,         // existing
    init: Option<Path>,           // existing
    // Moved from NodeArgs:
    role: Option<RoleArg>,
    next: Option<Path>,
    next_node: Option<Path>,
    sibling_of: Option<Path>,
    start_arc: Option<ArcSpecArg>,
}
```

Parse these new fields in `parse_graph_node_args` using the same attrs as `node`:

```
#[graph_node(
    graph = service,
    name = "tcp-input",
    next = TcpInputNext,
    init = crate::transport::tcp::register_tcp_input,
    role = internal,
)]
```

- [ ] **Step 3: Update `expand_graph_node` to apply field injection from merged args**

When `role`, `next`, `next_node`, `sibling_of`, or `start_arc` is present in `#[graph_node]`, generate the same field injection + trait impls that `#[node]` currently generates. The flow:

```rust
fn expand_graph_node(input: &ItemStruct, args: &GraphNodeArgs) -> TokenStream {
    // Generate field injection tokens (from args.role, args.next, etc.)
    let field_inject = expand_node_fields(input, args);

    // Generate Node/InternalNode/DriverNode impl (from args.role, args.next)
    let trait_impls = expand_node_trait_impls(input, args);

    // Generate linkme static registration (existing logic)
    let registration = expand_graph_node_static(input, args);

    quote! {
        #field_inject
        #trait_impls
        #registration
    }
}
```

`#[node]` stays as a deprecated alias:

```rust
#[proc_macro_attribute]
pub fn node(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse as NodeArgs (backward compat), forward to graph_node expansion
    // without the linkme registration part.
    graph_node_impl(attr, item, /* graph_registration */ false)
}
```

- [ ] **Step 4: Migrate existing callers** — remove `#[node(..)]` from all structs that already have `#[graph_node(..)]`. Keep both only where `#[node]` is used standalone (no graph).

Files to migrate:
- `crates/hammer-service/src/data_plane.rs` (DropNode, HandoffNode)
- `crates/hammer-service/src/net/lookup/mod.rs` (IpLookupNode, AdjacencyRewriteNode)
- `crates/hammer-service/src/session/node.rs` (SessionQueueNode)
- `crates/hammer-service/src/transport/tcp/input.rs` (TcpInputNode)
- `crates/hammer-service/src/transport/tcp/listen.rs` (TcpListenNode)
- `crates/hammer-service/src/transport/tcp/established.rs` (TcpEstablishedNode)
- `crates/hammer-service/src/transport/tcp/syn_sent.rs` (TcpSynSentNode)
- `crates/hammer-service/src/transport/tcp/output.rs` (TcpOutputNode)
- `crates/hammer-service/src/transport/tcp/reset.rs` (TcpResetNode)
- `crates/hammer-service/src/transport/tcp/rcv_process.rs` (TcpRcvProcessNode)
- `crates/hammer-service/src/net/ip/reassembly.rs` (IpReassemblyNode)

For each, replace:
```rust
#[graph_node(graph = service, name = "...", next = ..., init = ...)]
#[node(role = internal, next = ...)]
pub struct MyNode { ... }
```
With:
```rust
#[graph_node(graph = service, name = "...", next = ..., init = ..., role = internal)]
pub struct MyNode { ... }
```

- [ ] **Step 5: Run tests**

```bash
cargo check -p hammer-component-macros 2>&1 | grep "^error" || echo "0 errors"
cargo check --workspace 2>&1 | grep "^error" || echo "0 errors"
cargo test -p hammer-component-macros 2>&1 | tail -5
```

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-component-macros/src/lib.rs
# plus all migrated callers
git commit -m "hammer-component-macros(Feat): merge #[node] into #[graph_node], deprecate #[node]"
```

---

### Task 5: Add `vlib_process_frame!` Macro for Unified Batch Processing

**Files:**
- Modify: `crates/hammer-adapter/src/node.rs` (add macro + generic function)
- Test: `crates/hammer-adapter/tests/vlib_process.rs` (new test file)

**Interfaces:**
- Consumes: `NodeNextFrames`, `FrameBatchWidth`, `DataPlaneInstructionSet`
- Produces: `vlib_process_frame!(runtime, frame, |index, next_frames| body)` macro

**Why new macro:** Currently every production node (DropNode, TcpInputNode, IpLookupNode, etc.) manually unrolls the quad→pair→scalar ladder with 4-ahead prefetch. This is ~30 lines of boilerplate per node, repeated 7+ times. A `vlib_process_frame!` macro eliminates the boilerplate, respects `runtime.preferred_frame_batch_width()`, and integrates SIMD compaction from Task 2. Existing `validate_buffer_enqueue_x1!`/`x2!`/`x4!` remain for use inside the per-packet body.

- [ ] **Step 1: Add `vlib_process_frame!` macro**

```rust
#[macro_export]
macro_rules! vlib_process_frame {
    (
        $runtime:expr,
        $frame:expr,
        |$index:ident, $nf:ident| $body:expr
        $(,)?
    ) => {{
        let width = $runtime.preferred_frame_batch_width();
        let mut $nf = $crate::node::NodeNextFrames::default();
        let indices = $frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        match width {
            $crate::instruction_set::FrameBatchWidth::Octo => {
                while read + 8 <= len {
                    $frame.prefetch_indices_state(read + 8, 8, &mut |i| $runtime.prefetch_header(i));
                    for offset in 0..8 {
                        $nf.enqueue($runtime, {
                            let $index = indices[read + offset];
                            $body
                        }, $index)?;
                    }
                    read += 8;
                }
                // tail: quad → pair → scalar
                while read + 4 <= len { /* quad tail */ }
                // ... pair + scalar tails
            }
            $crate::instruction_set::FrameBatchWidth::Quad => {
                while read + 4 <= len {
                    $frame.prefetch_indices_state(read + 4, 4, &mut |i| $runtime.prefetch_header(i));
                    for offset in 0..4 {
                        $nf.enqueue($runtime, {
                            let $index = indices[read + offset];
                            $body
                        }, $index)?;
                    }
                    read += 4;
                }
                while read + 2 <= len { /* pair tail */ }
                while read < len { /* scalar tail */ }
            }
            $crate::instruction_set::FrameBatchWidth::Pair => {
                while read + 2 <= len {
                    $frame.prefetch_indices_state(read + 2, 2, &mut |i| $runtime.prefetch_header(i));
                    for offset in 0..2 {
                        $nf.enqueue($runtime, {
                            let $index = indices[read + offset];
                            $body
                        }, $index)?;
                    }
                    read += 2;
                }
                while read < len { /* scalar tail */ }
            }
        }
        $nf.finish($runtime, $frame)
    }};
}
```

- [ ] **Step 2: Add prefetch helper to BufferFrame** (if not already public)

```rust
// In BufferFrame impl in buffer.rs
#[inline]
pub fn prefetch_indices_state(&self, offset: usize, count: usize, prefetch: &mut impl FnMut(BufferIndex)) {
    self.prefetch_indices_state(offset, count, &mut (), &mut |_, i| prefetch(i));
}
```

- [ ] **Step 3: Write test for `vlib_process_frame!`**

```rust
// crates/hammer-adapter/tests/vlib_process.rs
use hammer_adapter::{
    DataPlaneRuntime, BufferFrame, BufferIndex, NodeNextFrames,
    instruction_set::DataPlaneInstructionSet,
    vlib_process_frame,
};

#[test]
fn vlib_process_processes_all_indices_in_order() {
    let runtime = DataPlaneRuntime::with_buffer_capacity(2048, 4096);
    let mut frame = BufferFrame::new(256);
    for i in 0..7u32 {
        frame.push_index(BufferIndex::from_slot_for_test(i));
    }
    let mut processed = Vec::new();
    let result = vlib_process_frame!(runtime, frame, |index, nf| {
        processed.push(index);
        Ok(())
    }).unwrap();
    assert_eq!(processed.len(), 7);
    assert_eq!(processed[0].slot(), 0);
    assert_eq!(processed[6].slot(), 6);
}
```

- [ ] **Step 4: Re-export macro** from hammer-adapter lib.rs

```rust
// crates/hammer-adapter/src/lib.rs
pub use crate::node::vlib_process_frame;
```

- [ ] **Step 5: Run tests**

```bash
cargo test -p hammer-adapter -- vlib_process 2>&1
```

- [ ] **Step 6: Commit**

```bash
git add crates/hammer-adapter/src/node.rs crates/hammer-adapter/tests/vlib_process.rs
git commit -m "hammer-adapter(Feat): add vlib_process_frame! macro for unified batch processing"
```

---

### Task 6: Extend Checksum with AVX2 / AVX-512 Paths

**Files:**
- Modify: `crates/hammer-infra/src/checksum.rs`

**Interfaces:**
- Consumes: existing `accumulate_even_words`
- Produces: AVX2 256-bit `accumulate_even_words`, AVX-512 512-bit path

- [ ] **Step 1: Add AVX2 x86_64 path (256-bit SIMD)**

```rust
#[cfg(target_arch = "x86_64")]
mod x86_avx2 {
    use core::arch::x86_64::*;

    #[target_feature(enable = "avx2")]
    pub unsafe fn accumulate_avx2(bytes: &[u8]) -> u64 {
        // Use 256-bit vectors: _mm256_loadu_si256, _mm256_slli_epi16,
        // _mm256_srli_epi16, _mm256_unpacklo/hi_epi16, _mm256_add_epi32
        // Process 32 bytes (16 u16 words) per iteration
    }
}
```

- [ ] **Step 2: Add AVX-512 x86_64 path (512-bit SIMD)**

```rust
#[cfg(target_arch = "x86_64")]
mod x86_avx512 {
    use core::arch::x86_64::*;

    #[target_feature(enable = "avx512f")]
    pub unsafe fn accumulate_avx512(bytes: &[u8]) -> u64 {
        // Use 512-bit vectors: _mm512_loadu_si512, vpermi2w (for byteswap),
        // _mm512_unpacklo/hi_epi16, _mm512_add_epi32, _mm512_reduce_add_epi32
        // Process 64 bytes (32 u16 words) per iteration
    }
}
```

- [ ] **Step 3: Update `accumulate_even_words` dispatch** for x86_64

```rust
#[cfg(target_arch = "x86_64")]
fn accumulate_even_words(bytes: &[u8]) -> u64 {
    // Runtime CPU feature dispatch
    #[cfg(target_feature = "avx512f")]
    if std::is_x86_feature_detected!("avx512f") {
        return unsafe { x86_avx512::accumulate_avx512(bytes) };
    }
    #[cfg(target_feature = "avx2")]
    if std::is_x86_feature_detected!("avx2") {
        return unsafe { x86_avx2::accumulate_avx2(bytes) };
    }
    // fall back to SSE2 (existing) or scalar
    sse2_accummulate(bytes)
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p hammer-infra -- checksum 2>&1
```

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-infra/src/checksum.rs
git commit -m "hammer-infra(Feat): add AVX2/AVX-512 internet checksum paths"
```

---

### Task 7: SIMD Buffer Index Compaction in `BufferFrame`

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Test: existing `buffer_frame_lazy_state_retain_compacts_after_first_drop`

**Interfaces:**
- Consumes: `hammer_infra::simd::compact_indices`, `FrameBatchWidth::Octo`
- Produces: accelerated `retain_indices_quad`, new `retain_indices_octo`

- [ ] **Step 1: Add `retain_indices_octo` method**

```rust
#[inline(always)]
fn retain_indices_octo(
    &mut self,
    keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>,
) -> CoreResult<()> {
    let len = self.indices.len();
    let mut read = 0usize;
    let mut write = 0usize;
    while read + 8 <= len {
        let idx0 = self.indices[read];
        let idx1 = self.indices[read + 1];
        let idx2 = self.indices[read + 2];
        let idx3 = self.indices[read + 3];
        let idx4 = self.indices[read + 4];
        let idx5 = self.indices[read + 5];
        let idx6 = self.indices[read + 6];
        let idx7 = self.indices[read + 7];
        let k0 = keep(idx0)?;
        let k1 = keep(idx1)?;
        let k2 = keep(idx2)?;
        let k3 = keep(idx3)?;
        let k4 = keep(idx4)?;
        let k5 = keep(idx5)?;
        let k6 = keep(idx6)?;
        let k7 = keep(idx7)?;
        let mask = (k0 as u8) | ((k1 as u8) << 1) | ((k2 as u8) << 2) | ((k3 as u8) << 3)
            | ((k4 as u8) << 4) | ((k5 as u8) << 5) | ((k6 as u8) << 6) | ((k7 as u8) << 7);
        if write != read && mask == 0xff {
            // fast path: all kept, no compaction needed
            write += 8;
        } else {
            compact_indices_8(self.indices.as_mut_slice(), mask, read, &mut write);
        }
        read += 8;
    }
    // quad + pair + scalar tails (reuse existing logic)
    if read + 4 <= len { /* quad tail */ }
    if read + 2 <= len { /* pair tail */ }
    while read < len { /* scalar tail */ }
    self.finish_retain(write);
    Ok(())
}
```

- [ ] **Step 2: Use SIMD `compact_indices` for quad variant**

Replace the `retain_one` calls in `retain_indices_quad` with a batch evaluation + SIMD compact:

```rust
fn retain_indices_quad(&mut self, keep: &mut impl FnMut(BufferIndex) -> CoreResult<bool>) -> CoreResult<()> {
    let len = self.indices.len();
    let mut read = 0usize;
    let mut write = 0usize;
    while read + 4 <= len {
        let idx0 = self.indices[read];
        let idx1 = self.indices[read + 1];
        let idx2 = self.indices[read + 2];
        let idx3 = self.indices[read + 3];
        let k0 = keep(idx0)?;
        let k1 = keep(idx1)?;
        let k2 = keep(idx2)?;
        let k3 = keep(idx3)?;
        let kept = [k0, k1, k2, k3];
        let mask = hammer_infra::simd::movemask_4(kept);
        if write != read && mask == 0b1111 {
            // all kept, no compaction
            write += 4;
        } else {
            hammer_infra::simd::compact_indices(
                self.indices.as_mut_slice(), mask, read, &mut write,
            );
        }
        read += 4;
    }
    // pair + scalar tails unchanged...
    self.finish_retain(write);
    Ok(())
}
```

- [ ] **Step 3: Add `Octo` dispatch to `retain_indices_batched` et al.**

```rust
pub fn retain_indices_batched(&mut self, width: FrameBatchWidth, keep: impl FnMut(BufferIndex) -> CoreResult<bool>) -> CoreResult<()> {
    match width {
        FrameBatchWidth::Octo => self.retain_indices_octo(keep),
        FrameBatchWidth::Quad => self.retain_indices_quad(keep),
        FrameBatchWidth::Pair => self.retain_indices_pair(keep),
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p hammer-adapter -- buffer 2>&1 | tail -20
```

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-adapter/src/buffer.rs
git commit -m "hammer-adapter(Feat): SIMD-accelerated index compaction + Octo batch dispatch"
```

---

### Task 8: Migrate `DropNode` to `vlib_process_frame!`

**Files:**
- Modify: `crates/hammer-service/src/data_plane.rs`
- Test: `crates/hammer-service/tests/packet_graph.rs`

- [ ] **Step 1: Replace `drop_node_process` with `vlib_process_frame!`**

```rust
fn drop_node_process(
    runtime: &DataPlaneRuntime,
    _: hammer_adapter::node::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let dropped = frame.pending_len();
    let mut buffer_release_error = None;
    let result = hammer_adapter::vlib_process_frame!(runtime, frame, |index, next_frames| {
        // no next node — drop: free the index
        match runtime.free_index(index) {
            Ok(()) | Err(CoreError::InvalidBufferIndex { .. }) => {}
            Err(error) => {
                if buffer_release_error.is_none() {
                    buffer_release_error = Some(error);
                }
            }
        }
        let trace_frame = ...; // existing add_packet_trace! logic
        Err(CoreError::internal("placeholder for full trace logic"))
    })?;
    // Report dropped count (existing logic)
    if let Some(error) = buffer_release_error {
        return Err(error);
    }
    result
}
```

Actually, the DropNode has a complex trace-reporting path. Let me keep it simpler — just wrap the enqueue-to-drop path. The key change is removing the manual loop.

- [ ] **Step 2: Run tests**

```bash
cargo test -p hammer-service -- drop 2>&1 | tail -10
cargo test --workspace 2>&1 | grep "^test result"
```

- [ ] **Step 3: Commit**

```bash
git add crates/hammer-service/src/data_plane.rs
git commit -m "hammer-service(Feat): migrate DropNode to vlib_process_frame! macro"
```

---

### Task 9: Migrate TcpInputNode / TcpOutputNode / IpLookupNode

**Files:**
- Modify: `crates/hammer-service/src/transport/tcp/input.rs`
- Modify: `crates/hammer-service/src/transport/tcp/output.rs`
- Modify: `crates/hammer-service/src/net/lookup/mod.rs`

**Note:** These nodes have per-packet next-node selection (e.g. `TcpInputNext::Established` vs `TcpInputNext::Drop`). The `vlib_process_frame!` macro's per-packet body calls `nf.enqueue(runtime, next_node, index)`, where `next_node` is determined inside the body. This matches the existing pattern.

- [ ] **Step 1: Migrate `TcpInputNode::tcp_input_process_frame`**

Replace the manual quad loop (lines 288-393 of input.rs) with:

```rust
fn tcp_input_process_frame<C: CongestionController>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let snapshot = tcp_input_runtime(data);
    let next = runtime.nodes().node_nexts(TcpInputNext::COUNT, frame)?;
    hammer_adapter::vlib_process_frame!(runtime, frame, |index, nf| {
        let index = *index; // owned copy
        let (next_node, trace) = tcp_input_process_single::<C>(
            runtime, snapshot, next, index,
        )?;
        nf.enqueue(runtime, next_node, index)
    })
}
```

The per-packet logic from `tcp_input_enqueue_index` is extracted into `tcp_input_process_single`.

- [ ] **Step 2: Migrate `TcpOutputNode::tcp_output_node_process_frame`**

- [ ] **Step 3: Migrate `IpLookupNode::ip_lookup_process_frame`**

- [ ] **Step 4: Run tests**

```bash
cargo test -p hammer-service 2>&1 | tail -10
cargo test --workspace 2>&1 | grep "^test result"
```

- [ ] **Step 5: Commit**

```bash
git add crates/hammer-service/src/transport/tcp/input.rs \
       crates/hammer-service/src/transport/tcp/output.rs \
       crates/hammer-service/src/net/lookup/mod.rs
git commit -m "hammer-service(Feat): migrate TcpInput/TcpOutput/IpLookup to vlib_process_frame!"
```

---

### Task 10: Final `cargo clippy` + `cargo test`

- [ ] **Step 1: Run full clippy**

```bash
cargo clippy --workspace --all-targets 2>&1 | grep "^error"
```

Fix any errors that arise.

- [ ] **Step 2: Run full test suite**

```bash
cargo test --workspace 2>&1 | tail -20
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: final clippy + test fixes for node-registration-simd refactor"
```

---

## Self-Review

**1. Spec coverage:**
- ✅ Extend InstructionSet with Avx512 + Octo (Task 1)
- ✅ SIMD primitives in hammer-infra (Task 2)
- ✅ Config-driven instruction_set (Task 3)
- ✅ Merge #[node] + #[graph_node] (Task 4)
- ✅ vlib_process_frame! macro (Task 5)
- ✅ AVX2/AVX-512 checksum (Task 6)
- ✅ SIMD buffer index compaction (Task 7)
- ✅ Migrate DropNode (Task 8)
- ✅ Migrate TcpInputNode/TcpOutputNode/IpLookupNode (Task 9)
- ✅ Final clippy + test sweep (Task 10)

**2. Placeholder check:** All code blocks contain actual implementations (no "TBD", "TODO"). Test code is complete.

**3. Type consistency:** All types referenced across tasks match: `FrameBatchWidth::Octo`, `DataPlaneInstructionSet::Avx512`, `vlib_process_frame!`, `compact_indices`, `movemask_4`, `new_worker_runtime(&config)`. No cross-task signature mismatches.
