## Task 1: 收紧 buffer 共享语义，只留 infra 里的通用能力

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`

**结果要求：**
- [ ] buffer chain 可以在不复制 payload 的前提下被 session/output/recovery 复用。
- [ ] 共享能力留在现有 buffer infra 语义里，不新增公开 wrapper owner type。
- [ ] `free_index()` 继续是释放入口；引用计数、回 cache、回 pool 都收在 buffer 层内部，不把 C 风格 free API 扩散出去。
- [ ] 不新增 TCP 专用 buffer API。
- [ ] 不继续扩散 `with_current_chain_range` / view / range 这一类面。

**实现边界：**
- 如果现有 `BufferIndex + free_index()` 语义无法表达“复制 header、共享 backing”，只允许补一个通用 buffer clone primitive。
- 这个 primitive 必须属于 `hammer-adapter::buffer`，不能带 TCP/session 业务语义。
- 计划阶段不预设新类型名；执行前如果需要新增公开 API，先单独审批。

