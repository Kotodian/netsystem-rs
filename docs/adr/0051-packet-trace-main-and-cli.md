# ADR-0051: per-main Packet Trace、trace_buffer 与 CLI

Status: implemented; static VPP/ADR review completed, build and tests not run

## 决定与源码依据

`TraceMain` 是 **每个 `DataPlaneMain` 的字段**，对应 VPP
`vlib_main_t.trace_main`；不定义 `TraceWorker`、全局 Trace 控制器、
trace 状态的 `Vec<UnsafeCell<...>>`、epoch 或完成队列。`trace add`
CLI 只配置源 Node 的捕获额度；真正的 `trace_buffer` 在源 Node 收包时
创建本线程的 trace pool 项、标记 Buffer/chain、传播 Next Frame
trace 位并返回是否成功；**额度由源 Node 管**。
`add_trace` 为已标记的 Buffer 追加带 Node/时间的记录。Buffer free 不
清除记录；`show trace` 查看所有 main，`clear trace` 才释放 pool。
本期不考虑 classifier、capture、post-mortem filter，不创建 filter CLI。

| ID | vendored VPP 来源 | 直接事实 |
| --- | --- | --- |
| V1 | `third_party/vpp/src/vlib/main.h:70-105,137`；`vlib/trace.h:13-35,62-93` | 每个 `vlib_main_t` 持有 `trace_main`；它有 trace-buffer pool、Node count/limit、enable、verbose。 |
| V2 | `third_party/vpp/src/vlib/trace_funcs.h:117-165` | `vlib_trace_buffer(vm,r,next,b,follow_chain)` 先检查 enable；不扣额度；设 Next Frame trace 位；pool_get 空记录；标记首 Buffer 或整个 chain；成功返回 1。filter/callback 分支本期删除。 |
| V3 | `third_party/vpp/src/vlib/trace_funcs.h:176-198`；`src/plugins/tap/rx_node.c:330-351` | 源 Node 用普通 `if (n_trace > 0)` 进入捕获循环；成功标记后扣额度，批次末写回；`vlib_add_trace` 返回的空间由 caller 逐字段写入，没有临时 payload 或 memcpy。 |
| V4 | `third_party/vpp/src/vlib/trace_funcs.h:22-89`；`vlib/trace.c:590-595`；`src/vnet/tcp/tcp_input.c:1289-1300` | 未追踪/trace 关闭/旧 pool index 时不追加；正常追加 header 与按 `sizeof(header)` 向上对齐的 payload；header 的 `n_data` 数的是 16 字节单位。TCP caller 也直接填返回的 trace 记录。 |
| V5 | `third_party/vpp/src/vlib/buffer.h:386-415`；`vlib/handoff_trace.c:88-105` | Buffer 的 trace handle 编码 thread/index；跨 Worker 时在接收线程另建 trace，并记录前一 thread/index。 |
| V6 | `third_party/vpp/src/vlib/main.c:450-455,1024-1059`；`vlib/trace_funcs.h:100-105` | 源 Node 的 `trace_buffer` 给目标 Next Frame 设 trace 位；中间 Node 出队时 runtime trace 位传给后续 Next Frame，目标 dispatch 再接收该位。它仅表示本帧可能含 traced Buffer。 |
| V7 | `third_party/vpp/src/vlib/trace.c:93-169,273-355,372-466,570-582,740-800` | clear 先禁用所有 main，再清额度与 pool；show 逐 main 排序、默认最多各 50 包；trace add 给各 main 增加 Node 额度，0 清额度；显示时使用 Node formatter 与时间格式。 |
| V8 | `third_party/vpp/src/vlib/threads.h:148-166`；`vlib/threads.c:599-604,766-767`；`vlib/cli.c:593-614` | VPP 有 main 列表；`foreach_vlib_main()` 要求 Worker 停在 barrier；非 MP-safe CLI 的回调在 barrier 内同步执行。 |
| V9 | `third_party/vpp/src/vnet/ip/ip4_forward.c:1220-1289`；`src/vnet/tcp/tcp_input.c:1280-1303`；`src/vnet/ip/ip_punt_drop.h:64-68,159-169`；`src/vlib/drop.c:93-155` | IP/TCP 中间 Node 和 Drop 终端 Node 先检查 Buffer 的 `VLIB_BUFFER_IS_TRACED`，仅为已追踪的包调用 `vlib_add_trace`；它们不调用 `vlib_trace_buffer`，不再分配 handle 或消耗源 Node 额度。 |
| V10 | `third_party/vpp/src/plugins/tap/rx_node.c:330-351`；`src/vnet/ip/ip4_forward.c:224-234,1716-1720`；`src/vnet/ip/ip4_input.c:109-115`；`src/vnet/tcp/tcp_input.c:2888-2891`；`src/vnet/tcp/tcp_output.c:2226-2252,2305-2316` | trace 时机依 Node 而定：TUN RX/lookup 与 TCP input 在批次末，IP input/local 与 TCP output 在入口；不能统一前置或后置。TCP4/6 output 在压入 IP header 前保存 TCP header 与连接快照；receive/output 使用各自布局，均不保存旧设计的 `next` 字段。 |
| H1 | `crates/hammer-runtime/src/trace.rs:11-19,115-151,324-504`；`src/data_plane/trace.rs:43-83` | 旧代码用全局 Arc/Mutex、epoch、SegQueue、serde/bincode；`try_mark_trace` 无生产调用者。 |
| H2 | `crates/hammer-core/src/buffer/pool.rs:321-383`；`crates/hammer-plugins/net/ip/src/reassembly.rs:402-411` | Buffer free 和 reassembly 过期处理错误地 finalize trace。 |
| H3 | `docs/adr/0049-cli-main-and-unix-process.md:71-146,270-310`；`crates/hammer-runtime/src/cli.rs:71-98`；`crates/hammer-component-macros/src/lib.rs:2482-2535` | CLI 已规定宏注册、拥有型 `Args: FromStr`、handler 返回 `Display` 值和 Unix Process 输出；dispatch 只在同步 `start` 阶段持 barrier，现有 async handler 的 body 在释放后才被 poll。 |

## 类型、所有权与布局

VPP 的 `trace_buffer_pool` 是 **pool，pool 元素才是可增长的 trace
vector**（`trace.h:65`、`trace_funcs.h:69-79`）。Rust 对应现有
`hammer_infra::pool::Pool<Vec<TraceHeader>>`：这个 `Vec` 仅是每个包
的连续 trace 存储，**不是**额外的 per-worker 状态表，也不是
`Vec<u128>` 代替 VPP header。`TraceHeader` 是 16 字节单位；
header 之后的定长 payload 占若干个 header 单位，padding 清零。
解析按 `1 + n_data` 跳到下一条，`n_data` 不是字节数或 u64 字数。
runtime 把新增的、已清零的 payload 空间借成 Node 的定长 `&mut T`；
Node 直接填字段，formatter 只按同一 `T` 的布局读，不经临时对象、
`copy_from_slice` 或序列化。VPP 的 `n_data_bytes` 允许变长记录；本 ADR
只设计现有 caller 所需的定长 Node 记录，这是明确的范围限制。

这里的“无中间复制”不等于 trace 完全不复制包内容。VPP 在
`vlib/trace.c:50-82`、`ip4_forward.c:1235-1289` 把 Buffer 字节复制**一次**
到保留的 trace record，否则 Buffer 后续修改或释放会破坏历史记录。
TUN 的 virtio header 也必须从原 Buffer 直接写入 `TunInputTrace.hdr`；
不得先复制到 `trace_headers` 数组再复制进 record。
VPP `vlib/trace.c:29-82` 的 `vlib_trace_frame_buffers_only` 是通用 x2
扫描加单项尾循环，预取后两项的 Buffer header；Rust 对应
`DataPlaneMain::trace_frame_buffers_only::<T>(&NodeRuntime, &[u32])`，
用 `size_of::<T>()` 代替调用者传 `sizeof(record)`，由 Node 的 formatter
解读同一个 `T`。Rust slice 长度已表示 `n_buffers`；VPP 源码中的
`next_buffer_stride` 参数在该函数内并未使用，不在 Rust API 增加空参数。

```rust
// hammer-runtime::trace; VPP: vlib/trace.h:13-35,62-93;
// vlib/trace_funcs.h:69-89.
#[repr(C, align(16))]
#[derive(Clone, Copy, zerocopy::IntoBytes, zerocopy::FromBytes, zerocopy::Immutable)]
struct TraceHeader {
    time: u64,
    node_index: u32,
    n_data: u32, // 16-byte units following this header
}

const TRACE_THREAD_SHIFT: u32 = 24;
const TRACE_THREAD_LIMIT: u32 = 0xff;
const TRACE_INDEX_LIMIT: u32 = 0x00ff_ffff;
const _: () = {
    assert!(std::mem::size_of::<TraceHeader>() == 16);
    assert!(std::mem::align_of::<TraceHeader>() == 16);
};

#[derive(Clone, Copy)]
struct TraceNode {
    count: u32,
    limit: u32,
}

// VPP: vlib/trace.h:95-105; vlib/trace.c:740-800. Only thread 0's value
// controls display; clear trace does not reset it.
#[derive(Clone, Copy)]
enum TraceTimestampFormat { Relative, Unix, Datetime }

impl std::fmt::Display for TraceTimestampFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("trace timestamp format: ")?;
        match self {
            Self::Relative => f.write_str("relative"),
            Self::Unix => f.write_str("unix"),
            Self::Datetime => f.write_str("datetime"),
        }?;
        f.write_str("\n")
    }
}

struct TraceMain {
    trace_buffer_pool: hammer_infra::pool::Pool<Vec<TraceHeader>>,
    nodes: Vec<TraceNode>, // indexed by the existing NodeId; VPP tm->nodes
    trace_enable: bool,
    verbose: bool,
    timestamp_format: TraceTimestampFormat, // thread 0 display setting
}

// Existing DataPlaneMain owns this value directly, not a handle to a global
// trace service. VPP: vlib/main.h, vlib/trace.h:62-93.
struct DataPlaneMain {
    // existing fields ...
    trace_main: TraceMain,
    handoff_trace_node: NodeId,
    main_loop_start_ticks: u64,
    seconds_per_cpu_tick: f64,
    cpu_reference_ticks: u64,
    unix_reference_seconds: f64,
}

impl Default for TraceMain {
    fn default() -> Self {
        Self {
            trace_buffer_pool: hammer_infra::pool::Pool::new(),
            nodes: Vec::new(),
            trace_enable: false,
            verbose: false,
            timestamp_format: TraceTimestampFormat::Relative,
        }
    }
}

impl TraceMain {
    fn add_count(&mut self, node: NodeId, count: u32, verbose: bool) {
        // The CLI has checked every main's limit before mutating any main.
        let index = node.slot() as usize;
        if self.nodes.len() <= index {
            self.nodes.resize(index + 1, TraceNode { count: 0, limit: 0 });
        }
        let trace_node = &mut self.nodes[index];
        if count == 0 {
            trace_node.count = 0;
            trace_node.limit = 0;
        } else {
            trace_node.limit = trace_node.limit.checked_add(count)
                .expect("trace add prevalidated the limit");
        }
        self.verbose = verbose;
        self.trace_enable = true;
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.trace_buffer_pool.clear();
    }
}

// Existing hammer-infra::Pool method; VPP: vlib/trace.c:93-115 uses
// vec_free per occupied trace record, then pool_free for the pool.
impl<T> Pool<T> {
    pub fn clear(&mut self) {
        for position in 0..self.vector.len() {
            let index = position as u32;
            if self.contains_key(index) {
                drop(self.remove(index));
            }
        }
        self.opaque = 0;
        if let Some(max_elts) = self.max_elts {
            self.free_indices.clear();
            self.free_indices.extend((0..max_elts).rev());
        } else {
            self.vector.clear();
            self.free_bitmap.clear_all();
            self.free_indices.clear();
        }
    }
}
```

`clear trace` 在 barrier 内先遍历所有 main 设置 `trace_enable = false`，
再遍历所有 main 调用 `TraceMain::clear()`；不能对每个 main 逐个执行
“禁用并清理”。`Pool::clear(&mut self)` 是 `hammer-infra` 的通用原地
清空操作：恰好 drop 每个占用的元素，重置占用索引和 `opaque`，使动态 pool 的
下一次插入从索引 0 开始；固定容量 pool 则恢复全部空闲槽位。
`Pool` 值本身不被替换，也不新建 pool。此处元素是
`Vec<TraceHeader>`，drop 元素即释放每条记录。与 VPP `pool_free`
释放 pool 后备容量不同，Rust `clear` 可保留容量供下一次捕获复用；
trace 记录和旧索引的失效语义保持一致。VPP `trace.c:93-115`。

`Pool<Vec<TraceHeader>>` 的 pool slot 只属于捕获线程，记录从第一次
`trace_buffer` 到 `clear trace` 存活，不由 Buffer refcount 管理。
`TraceHeader` 要有 `size_of == align_of == 16` 的编译期断言；
`TraceMain` 自身随现有 64 字节对齐的 `DataPlaneMain` 分配，**不再
额外放 cacheline mark**。`Vec` 的首元素与后续 record 均保持
16 字节对齐。Worker clone 必须用 `TraceMain::default()` 得到空的
本线程状态，不能复制 thread 0 的 pool 或额度。trace 默认关闭；
`trace add` 才开启。VPP `trace.c:372-405`。

Buffer 已有 `TRACED` flag 与 `trace_handle: u32`，继续复用；handle
内部按 VPP 的 thread/index 位宽编码，不增加 `TraceHandle` 包装或
Buffer refcount。配置 Worker 数超出 thread 位宽时，
启动校验返回具体 `RuntimeError::TraceThreadCapacity { count }`；
pool index 达到 handle 可表达上限时不 wrap/错写别的 trace。
VPP 的 `ASSERT` 仍作为内部容量不变量参考，Rust 对外是启动错误，
捕获热路径返回 `false`（不丢包）。VPP `buffer.h:386-415`。

## trace_buffer：完整状态转移

`NodeRuntime` 当前只有 `words/cached_next_index/flags`
（`crates/hammer-runtime/src/node.rs:115-131`），没有 VPP 的
`node_index`（`third_party/vpp/src/vlib/node.h:483-510`）。为让下列
函数保持 VPP 的参数形状，注册时给现有 `NodeRuntime` 填入 NodeId，
refork/clone 保留该字段；不再靠 ambient `current_node` 反查。
未注册的 NodeRuntime 调用 trace 是 owner 不变量错误。

```rust
// Existing NodeRuntime gains only its own graph identity.
// VPP: vlib/node.h:483-510; vlib/trace_funcs.h:72-76,176-198.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRuntime {
    words: [u64; 4],
    cached_next_index: u32,
    flags: u16,
    node_index: NodeId, // overwritten by NodeMain at registration
}

impl NodeRuntime {
    #[inline(always)]
    pub const fn empty() -> Self { Self::from_words([0; 4]) }

    #[inline(always)]
    pub const fn from_words(words: [u64; 4]) -> Self {
        Self {
            words,
            cached_next_index: 0,
            flags: 0,
            node_index: NodeId::new(0),
        }
    }

    #[inline(always)]
    pub fn node_index(&self) -> NodeId { self.node_index }
}

impl Default for NodeRuntime {
    fn default() -> Self { Self::empty() }
}
```

```rust
// hammer-runtime::NodeMain; VPP: vlib/trace_funcs.h:100-105.
impl NodeMain {
    #[inline(always)]
    pub(crate) fn trace_next_frame(&mut self, node: NodeId, next: u16) {
        let index = self.prepare_next_frame(node, u32::from(next));
        self.next_frames[index].flags |= 1 << 5; // existing Frame trace bit
    }
}

// hammer-runtime::DataPlaneMain; VPP: vlib/trace_funcs.h:22-76,100-198.
impl DataPlaneMain {
    #[inline(always)]
    pub fn trace_count(&self, node: &NodeRuntime) -> u32 {
        let Some(trace_node) = self.trace_main.nodes.get(node.node_index().slot() as usize)
        else {
            return 0;
        };
        assert!(trace_node.count <= trace_node.limit);
        trace_node.limit - trace_node.count
    }

    #[inline(always)]
    pub fn set_trace_count(&mut self, node: &NodeRuntime, remaining: u32) {
        let trace_node = self.trace_main.nodes
            .get_mut(node.node_index().slot() as usize)
            .expect("source Node has an active trace quota");
        assert!(remaining <= trace_node.limit);
        trace_node.count = trace_node.limit - remaining;
    }

    #[inline(always)]
    pub fn trace_buffer(
        &mut self,
        node: &NodeRuntime,
        next_index: u16,
        buffer_index: u32,
        follow_chain: bool,
    ) -> bool {
        if unlikely(!self.trace_main.trace_enable) {
            return false;
        }
        if unlikely(self.trace_main.trace_buffer_pool.len() >= TRACE_INDEX_LIMIT as usize) {
            return false;
        }
        assert!(self.thread_index() < TRACE_THREAD_LIMIT);
        self.nodes.trace_next_frame(node.node_index(), next_index);
        // One pool slot owns one packet's initially empty trace vector.
        // Vec::new has no backing allocation; add_trace grows this record.
        let index = self.trace_main.trace_buffer_pool.insert(Vec::new());
        assert!(index < TRACE_INDEX_LIMIT);
        let handle = (self.thread_index() << TRACE_THREAD_SHIFT) | index;
        let mut current = Some(buffer_index);
        while let Some(index) = current {
            current = if follow_chain {
                self.buffer(index).next_buffer_slot()
            } else {
                None
            };
            self.buffer_mut(index).set_trace_handle(handle);
        }
        true
    }

    #[inline(always)]
    pub fn add_trace<T>(
        &mut self,
        node: &NodeRuntime,
        buffer_index: u32,
    ) -> Option<&mut T>
    where
        T: zerocopy::KnownLayout
            + zerocopy::FromBytes
            + zerocopy::IntoBytes
            + zerocopy::Immutable,
    {
        const {
            assert!(std::mem::size_of::<T>() > 0);
            assert!(std::mem::align_of::<T>() <= 16);
        }
        let handle = self.buffer(buffer_index).trace_handle();
        if unlikely(handle.is_none()) {
            return None;
        }
        let handle = handle.expect("checked traced Buffer");
        if unlikely(!self.trace_main.trace_enable) {
            return None;
        }
        let previous_thread = handle >> TRACE_THREAD_SHIFT;
        if unlikely(previous_thread != self.thread_index()) {
            let previous_index = handle & TRACE_INDEX_LIMIT;
            let handoff_node = self.nodes.node_runtime_data(self.handoff_trace_node)
                .expect("handoff-trace registered before graph freeze");
            if !self.trace_buffer(&handoff_node, 0, buffer_index, true) {
                return None;
            }
            let handoff = self.add_trace::<HandoffTrace>(&handoff_node, buffer_index)
                .expect("new local handoff trace has a pool slot");
            handoff.prev_thread = previous_thread;
            handoff.prev_trace_index = previous_index;
        }
        let handle = self.buffer(buffer_index).trace_handle()?;
        let pool_index = handle & TRACE_INDEX_LIMIT;
        if unlikely(!self.trace_main.trace_buffer_pool.contains_key(pool_index)) {
            return None;
        }
        let dispatch_time = self.last_time_stamp;
        let trace = self.trace_main.trace_buffer_pool.get_mut(pool_index)
            .expect("checked occupied trace index");
        let words = std::mem::size_of::<T>().div_ceil(16);
        let start = trace.len();
        let end = start.checked_add(1 + words).expect("trace length fits usize");
        trace.resize(end, TraceHeader { time: 0, node_index: 0, n_data: 0 });
        trace[start] = TraceHeader {
            time: dispatch_time,
            node_index: node.node_index().slot(),
            n_data: u32::try_from(words).expect("trace payload count fits u32"),
        };
        let bytes = trace[start + 1..end].as_mut_bytes();
        let (payload, _) = T::mut_from_prefix(bytes)
            .expect("zeroed trace payload has T's size and alignment");
        Some(payload)
    }
}

// Source Node starts a trace; VPP: plugins/tap/rx_node.c:330-351.
let mut remaining = runtime.trace_count(node);
if remaining > 0 {
    for (buffer_index, next_index, hw_if_index, packet_length) in received {
        if remaining == 0 { break; }
        if runtime.trace_buffer(node, next_index, buffer_index, false) {
            let trace = runtime.add_trace::<TunInputTrace>(node, buffer_index)
                .expect("newly marked buffer has a local trace slot");
            trace.next_index = next_index;
            trace.hw_if_index = hw_if_index;
            trace.len = packet_length;
            remaining -= 1;
        }
    }
    runtime.set_trace_count(node, remaining);
}
```

`trace_buffer` 的顺序与 VPP 一样；只有 enable/容量不足是普通
`false`，无 packet error/日志。`next_index` 不属于当前 Node、chain
断裂、thread/index 不变量违背则断言，不能当成可恢复丢包。
`Pool::insert` 和 `Vec` 扩张由 Main Heap 承担，固定 Heap 耗尽沿用
进程内存语义；不造 trace 专属分配错误。首次 `trace_buffer` 可以替换
一个已经标记的 Buffer handle，和 VPP 一致；调用者决定是否捕获。
TUN RX 必须实际调用它；当前 `try_mark_trace` 无生产调用点，光有
`[trace]` 配置不会产生记录。VPP `trace_funcs.h:117-165`。

`add_trace<T>` 不按 Buffer free 结束记录。为了避免 VPP placeholder 的
“未追踪也能写” C 语义，Rust 返回 `Option<&mut T>`：None 是正常
未追加，并不代表错误。`T` 由 Node owner 定义为无 padding、任意零位
模式有效且对齐不超过 16 字节的定长记录；runtime 先初始化整段存储，
再交出唯一可变借用，Node 直接写字段。对刚由 `trace_buffer` 成功标记
的 Buffer，`None` 违反本地创建不变量，应断言，不应静默跳过。
对于已标记 Buffer，检查 handle 的 thread；跨 Worker 时按下列代码
在接收 main 的 pool 新开记录，写先前 thread/index 的 handoff payload，
然后重新绑定 Buffer/chain。runtime 在构造 main 的 NodeMain 时注册
`handoff-trace` 一次，缓存返回的 NodeId；Worker 从 main 复制同一图和
NodeId，不在每包按名字查找，也不把 Node 放在插件或 service。
VPP `handoff_trace.c:12-28,50-105`。

```rust
// hammer-runtime::trace; VPP: vlib/handoff_trace.c:12-28,50-105.
#[repr(C)]
#[derive(zerocopy::KnownLayout, zerocopy::FromBytes,
         zerocopy::IntoBytes, zerocopy::Immutable)]
struct HandoffTrace {
    prev_thread: u32,
    prev_trace_index: u32,
}

#[derive(Clone, Copy)]
#[repr(u16)]
enum HandoffTraceError { UnexpectedDispatch }

impl NodeErrorCode for HandoffTraceError {
    fn local_code(self) -> u16 { self as u16 }
}

const HANDOFF_TRACE_ERRORS: [NodeErrorDescriptor; 1] = [
    NodeErrorDescriptor::new(
        "unexpected-dispatch", NodeErrorSeverity::Warn,
        "Packets sent to the handoff trace node",
    ),
];

struct HandoffTraceNode;

impl InternalNode for HandoffTraceNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next("handoff-trace", 1))
    }
}

impl Node for HandoffTraceNode {
    fn process(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame)
        -> usize
    {
        let count = frame.len();
        runtime.buffer_free(frame.vector_args());
        runtime.record_current_node_error_count(
            HandoffTraceError::UnexpectedDispatch, count as u64,
        ).expect("handoff-trace error column was registered");
        count
    }

    fn trace_supported(&self) -> bool { true }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_handoff_trace)
    }
}

fn format_handoff_trace(bytes: &[u8]) -> String {
    let (trace, _) = HandoffTrace::ref_from_prefix(bytes)
        .expect("handoff trace record has its registered layout");
    format!("HANDED-OFF: from thread {} trace index {}",
            trace.prev_thread, trace.prev_trace_index)
}
```

```rust
// In DataPlaneMain construction, before graph freeze and worker clone.
// VPP: vlib/handoff_trace.c:68-85,93-104; vlib/trace_funcs.h:49-76.
let nodes = NodeMain::default();
let handoff_trace_node = nodes.try_register_internal_with_next_names(
    HandoffTraceNode, &["drop"],
)?;
let main = DataPlaneMain {
    nodes,
    handoff_trace_node, // direct NodeId; worker construction copies it
    // existing fields ...
};
main.register_node_errors(handoff_trace_node, &HANDOFF_TRACE_ERRORS)?;
```

`handoff-trace` 的 next 0 指向 Hammer 现有的 `drop` Node（VPP 的名字是
`error-drop`），仅供 `trace_buffer` 设置 Frame trace 位，
不是常规数据路径；若误收到 Frame，按 VPP 释放 Buffer 并计
`UnexpectedDispatch` Node error。旧线程 pool 原样保留。
`add_trace<T>` 调用者不传第二个 next；接收 Worker 的 handoff 入口与
直接目标 Frame 必须保留 trace bit 提示。VPP
`vlib/handoff.c:178-180,389-406` 在生产者标记对应队列范围，在接收者
创建直接 Frame 时设置 `VLIB_NODE_FLAG_TRACE`。ADR-0052 的新 handoff
queue 以 `trace_stop` 记录源 Node trace 位覆盖到的已接受 slot 序号；
接收者在取得 slot 后比较水位并给直接 Frame 设置 trace 位，**不扫描
slot 内的 Buffer，也不在 dequeue 创建 handoff trace 记录**。旧的
`HandoffSlot` 扫描仅描述当前待删除实现，不属于新设计。此位只让目标
Node 进入逐包检查，不在跨线程时复制记录或改写 Buffer handle；目标
Node 第一次 `add_trace` 仍创建本 Worker 的 handoff 记录。
clear 后在途 Buffer 的旧 index 无效时
返回 None；若新 pool 恰好重用该 index，VPP 明确允许极低概率混入
（`trace_funcs.h:53-66`）。

### Buffer copy 与共享 tail

VPP `vlib_buffer_clone_at_offset` 为**新分配的 head**复制 source 的
trace flag/handle（`vlib/buffer_funcs.h:1387-1412`）；
`vlib_buffer_attach_clone` 只把现有 tail 接到现有 head，并不把 tail 的
trace handle 复制到 head（同文件 `1494-1517`）。Hammer 当前只有后者的
`buffer_attach_clone` 和对应 VPP `copy_no_chain` 的单 Buffer 复制，
没有完整的 `vlib_buffer_clone_at_offset` 操作。因此本 ADR 不借 trace
改动新造完整 clone API，也不修改 `attach_clone` 的头部标记。

```rust
// Existing attach_clone contract; VPP: vlib/buffer_funcs.h:1494-1517.
runtime.buffer_mut(tail).set_trace_handle(tail_handle);
assert_eq!(runtime.buffer(head).trace_handle(), None);
runtime.buffer_attach_clone(head, tail);
assert_eq!(runtime.buffer(head).trace_handle(), None);
assert_eq!(runtime.buffer(tail).trace_handle(), Some(tail_handle));

// Only an actual full-copy/clone operation creating a new head does this;
// it is not part of buffer_attach_clone or copy_no_chain.
// VPP: vlib/buffer_funcs.h:1219-1250,1387-1412.
if let Some(handle) = source.trace_handle() {
    new_head.set_trace_handle(handle);
}
```

### 已追踪包经过中间或终端 Node

中间 Node **不启动 trace**：不调用 `trace_count`、`trace_buffer`、
`set_trace_count`，不改变 Buffer 的 trace handle，也不减少入口 Node
的额度。它用 `unlikely(TRACED)` 检查当前 Buffer；若已标记，调用
`add_trace::<T>` 把**本 Node** 的记录追加到该 handle 对应的 pool 项，
直接写入返回的记录。`None` 表示 clear 后旧槽、trace 已关闭等正常
未追加情况；与刚由源 Node 成功标记后的内部断言不同，中间 Node
直接跳过本次记录，不影响包的后续处理。VPP
`ip4_forward.c:1230-1289`、`tcp_input.c:1280-1303`、`drop.c:93-155`。

```rust
// Middle-Node pattern with VPP's actual punt-redirect trace fields.
// VPP: vnet/ip/ip_punt_drop.h:64-68,159-169. The concrete record belongs
// to the IP plugin, not to runtime. This VPP caller uses PREDICT_FALSE.
#[repr(C)]
#[derive(zerocopy::KnownLayout, zerocopy::FromBytes,
         zerocopy::IntoBytes, zerocopy::Immutable)]
struct IpPuntRedirectTrace { rrxi: u32, next: u32 }

if unlikely(buffer_flags.contains(BufferFlags::TRACED)) {
    if let Some(trace) = runtime.add_trace::<IpPuntRedirectTrace>(node, buffer_index) {
        trace.rrxi = redirect_index;
        trace.next = next_index;
    }
}
// Continue normal packet processing and enqueue; no trace_buffer/count here.

// Terminal Drop Node follows the same append-only rule.
// VPP: vlib/drop.c:93-155. It records the existing packet error, not a new
// source trace or a new trace handle. DropTrace here is the proposed fixed
// trace layout, replacing the current serde `dropped` count record.
if unlikely(buffer_flags.contains(BufferFlags::TRACED)) {
    if let Some(trace) = runtime.add_trace::<DropTrace>(node, buffer_index) {
        trace.error = packet_error;
    }
}
```

`trace_buffer` 只在源 Node 给选中的 Next Frame 设 trace 位。VPP
`main.c:450-455` 在中间 Node enqueue 时把当前 runtime 的 trace 位
传到后续 Next Frame，`main.c:1053-1056` 在目标 dispatch 时再接收；
Hammer 的 NodeMain/Next Frame 同样负责传播，不要求每个中间 Node
重复调用 `trace_buffer`。这个位只是 Frame 级提示，逐包是否追加
仍以 Buffer 的 `TRACED` flag 为准。同一 Node 的 formatter 解读它
自己追加的定长布局；只保留逐包 `unlikely(TRACED)`，不搬用旧
`add_packet_trace!` 的全局策略、序列化或错误路径。VPP 某些 Node 会把
TCP header/connection 或包的
前几个字节复制到**已经分配的 trace 记录本身**，这是被追踪内容的
快照（`tcp_input.c:1256-1268`、`ip4_forward.c:1237-1255`），不同于
先构造临时 trace 对象、再复制整个对象到记录空间；是否需要快照由
该 Node 的 VPP 对应实现决定。

## CLI 如何访问各个 main

VPP `foreach_vlib_main()` 在 barrier 内通过 `vlib_mains` 访问各
`vlib_main_t`（`threads.h:148-166`、`threads.c:599-604,766-767`）。
Hammer 的 `WorkerThread` 同时描述 thread 0、Data Worker 与不克隆
数据结构的辅助线程；把 `Option<Box<DataPlaneMain>>` 放进每个描述符
会把角色差异伪装成运行期可选状态。因此 `WorkerThread` 只保留线程
描述。`ThreadMain` 启动前一次性安装稠密的
`Vec<UnsafeCell<Box<DataPlaneMain>>>`，只对应 Data Worker，槽位顺序
为 thread index 减一；每槽的 main 必然存在，Box 地址不变，安装后
Vec 结构不再修改。thread 0 仍由主循环持有其 main，辅助线程不在这
个列表中。这个列表是 VPP `vlib_mains` 可遍历性的 Hammer 等价，
不是 trace 专属状态表；`TraceMain` 仍只属于各 `DataPlaneMain`。
Worker 每轮只在 barrier 检查之间借用自己的槽位，主线程 CLI 仅在
barrier 内借用这些槽位。不公开裸指针或跨线程自由借用。

现有 `data_plane_main_loop(&mut DataPlaneMain, ...)` 的长生命周期
`&mut` **必须拆到每轮 barrier 检查之间**，否则即使 Worker 停住，
主线程另造 `&mut` 也违反 Rust 独占借用契约。内部受控
每槽 `UnsafeCell<Box<DataPlaneMain>>` 的安全证明为：启动前安装一次，
列表不再重排；不同 Worker 只借用不同槽位，不构造整张 Vec 的
并发 `&mut`；Worker 正常期独占短借用，进 barrier 前释放，main 仅在
barrier 持有期间借用。退出后进程全局 `ThreadMain` 保留各 Box 至进程
结束；不在锁或 barrier 内 `.await`。这是一处 runtime 生命周期迁移，
不是 trace 热路径同步。若不能完成该证明，CLI 不得偷用 `&mut` 或
快照伪装 VPP 行为。

```rust
// hammer-runtime::ThreadMain; VPP: vlib/threads.h:148-166;
// vlib/threads.c:599-604,766-767; vlib/cli.c:593-614.
struct ThreadMain {
    worker_threads: Vec<WorkerThread>,
    worker_mains: UnsafeCell<Vec<UnsafeCell<Box<DataPlaneMain>>>>,
    // existing thread administration fields
}

impl ThreadMain {
    pub(crate) fn install_worker_mains(
        &self,
        mains: Vec<UnsafeCell<Box<DataPlaneMain>>>,
    ) {
        ensure_main_thread().expect("Data Worker mains install on thread zero");
        assert_eq!(mains.len(), self.worker_count as usize);
        let installed = unsafe { &mut *self.worker_mains.get() };
        assert!(installed.is_empty(), "Data Worker mains install once");
        *installed = mains;
    }

    /// Safety: only the owning Worker borrows its slot between barrier checks.
    pub(crate) unsafe fn worker_main_on_worker(
        &self, worker: &WorkerThread,
    ) -> &mut DataPlaneMain {
        assert!(worker.is_current());
        let mains = unsafe { &*self.worker_mains.get() };
        let main = mains.get(worker.thread_index() as usize - 1)
            .expect("Data Worker main installed before launch");
        unsafe { &mut **main.get() }
    }

    /// Safety: thread 0 holds WorkerBarrier and Worker borrows have ended.
    pub(crate) unsafe fn worker_main_at_barrier(
        &self, worker: &WorkerThread,
    ) -> &mut DataPlaneMain {
        ensure_main_thread().expect("trace CLI runs on thread zero");
        assert!(barrier::global().is_some_and(|barrier| barrier.is_pending()));
        let mains = unsafe { &*self.worker_mains.get() };
        let main = mains.get(worker.thread_index() as usize - 1)
            .expect("Data Worker main installed before barrier CLI");
        unsafe { &mut **main.get() }
    }

    pub(crate) fn data_workers(&self) -> impl Iterator<Item = &WorkerThread> {
        self.worker_threads.iter().skip(1)
            .take(self.worker_count as usize)
    }
}
```

Worker main-loop 每轮先结束上一轮的 `&mut DataPlaneMain`，再检查
barrier；检查后通过 `worker_main_on_worker` 重新借用，完成现有 File、
Node、timer 固定调度步骤。`worker_main_at_barrier` 仅供 runtime CLI
的同步非 MP-safe handler 使用，不导出到插件。无 DataPlaneMain 的线程
不在 `data_workers()` 中。

CLI 命令为 `trace add <node> <count> [verbose]`、
`show trace [max N]`、`clear trace`、
`set trace timestamp-format <relative|unix|datetime>`、
`show trace timestamp-format`。VPP `cli_add_trace_buffer` 以
`"%U %d"` 解析 Node 和 count，CLI 的 count 不是可省略的；50 只用于
`show trace` 默认上限及 VPP 内部 `trace_update_capture_options(~0)`。
CLI 的 `trace add` 只更新各 main 的额度和 enable，不为任何 Buffer
立即创建 trace 记录；第一次记录由源 Node 后续调用 `trace_buffer`
时创建。
`add 0` 把目标 Node count/limit 置零但保留已有记录；`clear` 先禁用所有
main，再原地清空各自的 Node 表和 pool。`trace add` 先在 thread 0 的图中验证
Node 存在且 `trace_supported`，再对所有 main 做溢出检查，最后
统一更新，保证非法命令无部分修改。VPP `trace.c:273-355,372-466`。

沿用 ADR-0049 的 `#[cli_command(path, args, mp_safe)]`、拥有型
`Args: FromStr<Err = CliError>`、有输出值的 `Display` 格式化、注册 image、
`CliMain::dispatch` 和 Unix CLI Process/AsyncFileMain/BufWriter。
**不手写 CLI 注册项或命令适配函数。** 现有宏只
接受 `async fn(Args)`，其 body 在 barrier 后才运行，不可直接读写
`DataPlaneMain`。因此对同一个宏增加一种明确的**同步** handler 形式
`fn(&mut DataPlaneMain, Args) -> Result<R, CliError>`：有输出时
`R: Display`，无输出时 `R = ()`；
原有异步 `async fn(Args)` 形式原封不动。宏对同步形式生成同一个
现有注册契约的私有适配入口；它只是宏展开细节，不是 trace API、
CLI 命令或需要设计命名的新函数。适配入口同步解析 Args、在
`CliMain::dispatch` 给出的 `&mut DataPlaneMain` 上完整执行 handler、
对有输出的 `R` 调用 `Display`，对 `()` 生成零字节输出，然后仅把
最终 `String` 放进 ready task。无需为无输出命令定义假的字符串结果、
空输出包装类型或另一个 CLI 回调。所有 trace 命令 `mp_safe=false`，
所以解析后的 main/Worker 访问和格式化都在 barrier 内完成；ready
task 不持有 main、pool 引用或 barrier guard。此同步 main-aware
形式是 ADR-0049 “handler 只接收 Args”规则针对必须访问主线程
`DataPlaneMain` 的窄修订，不改变命令目录、输出路径或 Future 调度。
VPP 对应命令没有设置 `is_mp_safe`，`vlib/cli.c:593-614` 在调用其
同步回调期间持有 Worker barrier；Rust 异步命令的 body 无法等价地
包进这个临界区。

```rust
// hammer-runtime::trace CLI; VPP: vlib/trace.c:273-355,408-466,570-582,
// 740-800; vlib/cli.c:593-614. Each Args::from_str consumes the entire
// command suffix and rejects unknown or duplicate words.
struct TraceAddArgs {
    node: String,
    count: u32,
    verbose: bool,
}
impl FromStr for TraceAddArgs {
    type Err = CliError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut words = input.split_whitespace();
        let node = words.next().ok_or_else(|| CliError::InvalidArgument {
            argument: input.to_owned(),
        })?.to_owned();
        let count = words.next().ok_or_else(|| CliError::InvalidArgument {
            argument: input.to_owned(),
        })?.parse().map_err(|_| CliError::InvalidArgument {
            argument: input.to_owned(),
        })?;
        let verbose = match words.next() {
            None => false,
            Some("verbose") => true,
            Some(_) => return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            }),
        };
        if words.next().is_some() {
            return Err(CliError::InvalidArgument { argument: input.to_owned() });
        }
        Ok(Self { node, count, verbose })
    }
}

struct ShowTraceArgs { max: u32 }
impl FromStr for ShowTraceArgs {
    type Err = CliError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut words = input.split_whitespace();
        let Some(word) = words.next() else { return Ok(Self { max: 50 }); };
        if word != "max" {
            return Err(CliError::InvalidArgument { argument: input.to_owned() });
        }
        let max = words.next().ok_or_else(|| CliError::InvalidArgument {
            argument: input.to_owned(),
        })?.parse().map_err(|_| CliError::InvalidArgument {
            argument: input.to_owned(),
        })?;
        if words.next().is_some() {
            return Err(CliError::InvalidArgument { argument: input.to_owned() });
        }
        Ok(Self { max })
    }
}

struct ClearTraceArgs;
impl FromStr for ClearTraceArgs {
    type Err = CliError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::InvalidArgument { argument: input.to_owned() });
        }
        Ok(Self)
    }
}

struct TraceTimestampArgs { format: TraceTimestampFormat }
impl FromStr for TraceTimestampArgs {
    type Err = CliError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let format = match input.trim() {
            "relative" => TraceTimestampFormat::Relative,
            "unix" => TraceTimestampFormat::Unix,
            "datetime" => TraceTimestampFormat::Datetime,
            _ => return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            }),
        };
        Ok(Self { format })
    }
}

struct ShowTraceTimestampArgs;
impl FromStr for ShowTraceTimestampArgs {
    type Err = CliError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::InvalidArgument { argument: input.to_owned() });
        }
        Ok(Self)
    }
}

#[cli_command(path = "trace add", args = TraceAddArgs, mp_safe = false)]
fn trace_add(main: &mut DataPlaneMain, args: TraceAddArgs)
    -> Result<(), CliError>
{
    let node = main.node_by_name(&args.node).ok_or_else(||
        CliError::TraceNodeMissing { name: args.node.clone() }
    )?;
    if !main.nodes.node_trace_supported(node)
        .expect("named Node is registered")
    {
        return Err(CliError::TraceUnsupported { node: args.node });
    }

    // VPP trace_update_capture_options first visits every main. The Rust
    // overflow preflight makes that same publication failure-atomic.
    let workers = ThreadMain::global();
    for owner in std::iter::once(&*main).chain(workers.data_workers().map(|worker| {
        // SAFETY: this non-MP-safe CLI is inside WorkerBarrier; the Worker
        // ended its previous DataPlaneMain borrow before acknowledging it.
        unsafe { worker.main_at_barrier() as &DataPlaneMain }
    })) {
        let limit = owner.trace_main.nodes.get(node.slot() as usize)
            .map_or(0, |trace_node| trace_node.limit);
        if args.count != 0 && limit.checked_add(args.count).is_none() {
            return Err(CliError::TraceLimitOverflow { node: args.node });
        }
    }

    main.trace_main.add_count(node, args.count, args.verbose);
    for worker in workers.data_workers() {
        // SAFETY: the same barrier is still held and each Worker main is
        // borrowed for only this call, never concurrently with its Worker.
        unsafe { worker.main_at_barrier() }
            .trace_main.add_count(node, args.count, args.verbose);
    }
    Ok(())
}

// VPP: vlib/trace.c:118-169; vppinfra/std-formats.c:152-212;
// vppinfra/unix-formats.c:259-344. This is the Rust format_vlib_trace.
fn format_trace_buffer(
    owner: &DataPlaneMain,
    trace: &[TraceHeader],
    timestamp_format: TraceTimestampFormat,
    output: &mut String,
) {
    use std::fmt::Write;
    use zerocopy::IntoBytes;

    let mut offset = 0;
    let mut previous_node = None;
    while offset < trace.len() {
        let header = trace[offset];
        let end = offset.checked_add(1)
            .and_then(|start| start.checked_add(header.n_data as usize))
            .expect("trace header length fits its vector");
        assert!(end <= trace.len(), "trace header stays within its vector");
        let node = NodeId::new(header.node_index);
        let name = owner.nodes.node_name(node)
            .expect("trace Node remains registered")
            .expect("trace Node has a name");
        if previous_node != Some(node) {
            match timestamp_format {
                TraceTimestampFormat::Relative => {
                    let seconds = header.time.checked_sub(owner.main_loop_start_ticks)
                        .expect("trace follows main-loop start") as f64
                        * owner.seconds_per_cpu_tick;
                    let whole = seconds.trunc() as u64;
                    let microseconds = (seconds.fract() * 1_000_000.0).trunc() as u32;
                    writeln!(output, "\n{:02}:{:02}:{:02}:{:06}: {}",
                        whole / 3_600, (whole / 60) % 60, whole % 60,
                        microseconds, name).expect("write to String");
                }
                TraceTimestampFormat::Unix | TraceTimestampFormat::Datetime => {
                    let seconds = owner.unix_reference_seconds
                        + header.time.checked_sub(owner.cpu_reference_ticks)
                            .expect("trace follows clock reference") as f64
                            * owner.seconds_per_cpu_tick;
                    if matches!(timestamp_format, TraceTimestampFormat::Unix) {
                        writeln!(output, "\n{seconds:.6}: {name}")
                            .expect("write to String");
                    } else {
                        let seconds_whole = seconds.trunc() as libc::time_t;
                        let microseconds = (seconds.fract() * 1_000_000.0).trunc() as u32;
                        let mut calendar = std::mem::MaybeUninit::<libc::tm>::uninit();
                        // SAFETY: seconds_whole and calendar are valid C objects;
                        // localtime_r writes the latter before assume_init.
                        let calendar = unsafe {
                            assert!(!libc::localtime_r(
                                &seconds_whole, calendar.as_mut_ptr()
                            ).is_null(), "trace timestamp is representable");
                            calendar.assume_init()
                        };
                        writeln!(output,
                            "\n{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}: {}",
                            calendar.tm_year + 1900, calendar.tm_mon + 1,
                            calendar.tm_mday, calendar.tm_hour, calendar.tm_min,
                            calendar.tm_sec, microseconds, name,
                        ).expect("write to String");
                    }
                }
            }
        }
        previous_node = Some(node);

        let payload = trace[offset + 1..end].as_bytes();
        if let Some(formatter) = owner.nodes.node_trace_formatter(node)
            .expect("trace Node remains registered")
        {
            writeln!(output, "  {}", formatter(payload)).expect("write to String");
        } else {
            output.push_str("  ");
            for byte in payload {
                write!(output, "{byte:02x}").expect("write to String");
            }
            output.push('\n');
        }
        offset = end;
    }
}

#[cli_command(path = "show trace", args = ShowTraceArgs, mp_safe = false)]
fn show_trace(main: &mut DataPlaneMain, args: ShowTraceArgs)
    -> Result<String, CliError>
{
    use std::fmt::Write;

    let mut output = String::new();
    let timestamp_format = main.trace_main.timestamp_format;
    let workers = ThreadMain::global();
    let mains = std::iter::once((0, "main", &*main))
        .chain(workers.data_workers().map(|worker| {
            // SAFETY: this non-MP-safe CLI holds WorkerBarrier for the
            // complete collection, sorting and formatting operation.
            (worker.thread_index(), worker.name(),
             unsafe { worker.main_at_barrier() as &DataPlaneMain })
        }));
    for (thread_index, name, owner) in mains {
        writeln!(output,
            "------------------- Start of thread {thread_index} {name} -------------------"
        ).expect("write to String");

        // Borrow only record slices. No trace payload is copied for sorting.
        let mut traces: Vec<&[TraceHeader]> = owner.trace_main.trace_buffer_pool
            .iter()
            .filter_map(|(_, record)| (!record.is_empty()).then_some(record.as_slice()))
            .collect();
        if traces.is_empty() {
            output.push_str("No packets in trace buffer\n");
            continue;
        }
        traces.sort_unstable_by_key(|record| record[0].time);
        for (index, record) in traces.iter().take(args.max as usize).enumerate() {
            writeln!(output, "Packet {}", index + 1).expect("write to String");
            format_trace_buffer(owner, record, timestamp_format, &mut output);
            output.push_str("\n\n");
        }
        if traces.len() > args.max as usize {
            writeln!(output, "Limiting display to {} packets. To display more specify max.",
                args.max).expect("write to String");
        }
    }
    Ok(output)
}

#[cli_command(path = "clear trace", args = ClearTraceArgs, mp_safe = false)]
fn clear_trace(main: &mut DataPlaneMain, _: ClearTraceArgs)
    -> Result<(), CliError>
{
    let workers = ThreadMain::global();
    main.trace_main.trace_enable = false;
    for worker in workers.data_workers() {
        // SAFETY: the non-MP-safe CLI holds WorkerBarrier throughout both
        // passes; no Worker can create a new trace between them.
        unsafe { worker.main_at_barrier() }.trace_main.trace_enable = false;
    }
    main.trace_main.clear();
    for worker in workers.data_workers() {
        unsafe { worker.main_at_barrier() }.trace_main.clear();
    }
    Ok(())
}

#[cli_command(path = "set trace timestamp-format", args = TraceTimestampArgs,
              mp_safe = false)]
fn set_trace_timestamp_format(main: &mut DataPlaneMain, args: TraceTimestampArgs)
    -> Result<(), CliError>
{
    main.trace_main.timestamp_format = args.format;
    Ok(())
}

#[cli_command(path = "show trace timestamp-format", args = ShowTraceTimestampArgs,
              mp_safe = false)]
fn show_trace_timestamp_format(main: &mut DataPlaneMain, _: ShowTraceTimestampArgs)
    -> Result<TraceTimestampFormat, CliError>
{
    Ok(main.trace_main.timestamp_format)
}
```

`show trace` 保留 `Result<String, CliError>`：它在 barrier 内访问多个
worker 的 pool、按时间排序并格式化完整记录，barrier 释放后 Unix CLI
Process 只能持有这份拥有型文本；单独再造一个只包住 `String` 的
`TraceOutput` 没有领域意义。其他命令返回 `()` 或已有时间格式枚举，
由同一个宏完成空输出或 `Display` 转换。VPP `trace.c:273-355,408-466,
570-582,776-800` 的 add/clear/set 无成功文本，show 才输出内容。

VPP 把单个时间展示设置存在 `vlib_trace_filter_main`
（`vlib/trace.c:13-23,740-800`）。Hammer 的 thread-0 `TraceMain`
持有它，是本期不引入 filter main 的显式所有权差异；Worker 的同名
字段不参与展示，也不需要复制或同步格式设置。

`TraceMain` 不需要自己的 `init/global`：随 `DataPlaneMain::new` 和
`new_worker` 默认构造，正是 VPP 的 per-main 字段生命周期。
`clear trace` 不重置 thread 0 的时间展示设置，和 VPP 的独立
`vlib_trace_filter_main.timestamp_format` 生命周期一致；其余 Worker
字段仅保持默认值，从不参与显示策略。
show 读取 Node formatter 时使用既有 Node 注册元数据；不创建另一份
formatter 表。Node 需要单独 `trace_supported` 注册事实，对齐 VPP
`VLIB_NODE_FLAG_TRACE_SUPPORTED`（`trace.c:445-454`）；它不等于
`node_trace_formatter().is_some()`。格式缺省可显示原始 payload，
不阻止 trace add。这里的原始十六进制后备是明确的 Rust 差异：
VPP `format_vlib_trace` 会把无 formatter 的 payload 传给
`node->format_buffer`（`trace.c:159-162`）；Hammer 不能把任意 trace
payload 伪装成 `Buffer`，所以只显示这段已初始化的字节。

```rust
// hammer-runtime::Node and NodeDescriptor; VPP: vlib/trace.c:445-454,
// vlib/trace.c:118-169; vlib/node.h:483-510. Both static and dynamic
// registration carry this fact; a formatter does not imply source support.
trait Node {
    fn trace_supported(&self) -> bool { false }
    fn node_trace_formatter(&self) -> Option<TraceFormatter> { None }

    fn node_descriptor(&self) -> RuntimeResult<NodeDescriptor<'_>>
    where Self: Sized,
    {
        Ok(NodeDescriptor::new(
            Self::process,
            self.node_runtime_data()?,
            Node::node_registration(self),
            self.node_initial_nexts(),
            self.node_trace_formatter(),
            self.trace_supported(),
        ))
    }
}

struct NodeDescriptor<'a> {
    process: NodeProcessFn,
    runtime_data: NodeRuntime,
    registration: Option<NodeRegistration>,
    initial_nexts: &'a [NodeId],
    trace_formatter: Option<TraceFormatter>,
    trace_supported: bool,
    frame_args_size: (u16, u16, u16),
}

impl<'a> NodeDescriptor<'a> {
    pub fn new(
        process: NodeProcessFn,
        runtime_data: NodeRuntime,
        registration: Option<NodeRegistration>,
        initial_nexts: &'a [NodeId],
        trace_formatter: Option<TraceFormatter>,
        trace_supported: bool,
    ) -> Self {
        Self {
            process, runtime_data, registration, initial_nexts,
            trace_formatter, trace_supported, frame_args_size: (0, 4, 0),
        }
    }
}

// Existing NodeRuntimeSlot already owns the registered Node's process and
// runtime data; keep the source-trace bit there, not in a parallel table.
struct NodeRuntimeSlot {
    kind: NodeKind,
    process: NodeFunction,
    declared_process: NodeFunction,
    frame_args_size: (u16, u16, u16),
    runtime_data: Option<NodeRuntime>,
    trace_supported: bool,
}

impl NodeRuntimeInner {
    fn push_node_slot(&mut self, mut slot: NodeRuntimeSlot) -> NodeId {
        let id = NodeId::new(u32::try_from(self.nodes.len())
            .expect("node index fits u32"));
        if let Some(runtime_data) = slot.runtime_data.as_mut() {
            runtime_data.node_index = id;
        }
        self.nodes.push(slot);
        self.node_states.push(NodeState::Polling);
        self.interrupt_pending.push(false);
        self.input_main_loops_per_call.push(0);
        self.error_columns.push(None);
        self.node_names.push(None);
        self.node_trace_formatters.push(None);
        self.next_nodes.push(Vec::new());
        self.pending_next_names.push(Vec::new());
        self.sibling_owners.push(None);
        self.siblings.push(Vec::new());
        id
    }

    fn push_function_node(
        &mut self,
        kind: NodeKind,
        process: NodeProcessFn,
        runtime_data: NodeRuntime,
        trace_supported: bool,
    ) -> NodeId {
        self.push_node_slot(NodeRuntimeSlot {
            kind,
            process,
            declared_process: process,
            frame_args_size: (0, 4, 0),
            runtime_data: Some(runtime_data),
            trace_supported,
        })
    }
}

impl NodeMain {
    pub fn node_trace_supported(&self, node: NodeId) -> RuntimeResult<bool> {
        let inner = self.inner.borrow();
        inner.validate_node(node)?;
        Ok(inner.nodes[node.slot() as usize].trace_supported)
    }

    // The named-next path bypasses NodeDescriptor, so pass the same fact
    // directly. VPP: vlib/node.c registration; vlib/trace.c:445-454.
    pub fn try_register_internal_with_next_names<N>(
        &self,
        node: N,
        next_names: &[&'static str],
    ) -> RuntimeResult<NodeId>
    where N: InternalNode + Node,
    {
        self.ensure_topology_owner()?;
        let mut inner = self.inner.borrow_mut();
        inner.register_function_declared(
            NodeKind::Internal,
            N::process,
            node.node_runtime_data()?,
            InternalNode::node_registration(&node),
            &[],
            node.node_trace_formatter(),
            node.trace_supported(),
            None,
            Some(next_names),
        )
    }
}
```

`NodeRuntimeInner::register_function_declared` 接收上述 `trace_supported`
参数，在 `None`、`Next`、`Sibling` 三个既有分支中都传给
`push_function_node`。`NodeMain::register_descriptor`、
`try_register_descriptor`，以及 driver、pre-input、
internal 的直接注册和具名 next 注册路径，均从各自的
`NodeDescriptor::trace_supported` 或 `node.trace_supported()` 传递同一
布尔值；未声明源追踪能力的 process/内建节点默认 false。
`push_node_slot` 一次赋予 `NodeRuntime` 的 NodeId，clone/refork 复制该
字段，不在包路径查名字。CLI 只查询这个注册事实，不以 formatter 推断。

## 时间、inline、predict、错误

`TraceHeader.time` 取本线程当前 Node dispatch 的 CPU counter，
对应 `vlib_add_trace_inline` 的 `vm->cpu_time_last_node_dispatch`
（`trace_funcs.h:72-76`）；Hammer 已有
`DataPlaneMain::last_time_stamp`。relative 的减数**不是**这一个会在
每次 dispatch 后更新的值，而是 main-loop 启动时固定的
`main_loop_start_ticks`。thread 0 构造/启动 main-loop 时记录一次，
Worker clone 复制该基准，不在每轮循环覆盖；对应 VPP
`main.c:1461-1468`、`trace.c:150-153`。

现有 `hammer_infra::time::cpu_time_now` 只有 ticks、没有校准。
`show trace` 使用下列通用 CPU counter 频率方法和主线程初始化方法，
按 VPP `vppinfra/time.c:110-202` 的架构频率优先、估算/2 GHz 后备顺序；
不把 ticks 猜成纳秒。这里用 Rust `Instant` 做 VPP 估算分支，不读
Linux `sysfs`/`proc` 的 CPU MHz：后者不是恒定 TSC 频率的可靠证明，
这是需由时间换算测试覆盖的显式平台差异。
这些事实放在 **当前 `DataPlaneMain`**，不是新的 trace worker。
CLI 在 barrier 内读 owner main 的 clock facts，绝不把 ticks 当纳秒。
thread 0 不跑 packet dataplane，仍在 show 中保留 thread 0 编号。

```rust
// hammer-infra::time; VPP: vppinfra/time.c:110-202.
// This generic frequency method is used at initialization, never per packet.
pub fn cpu_clock_frequency() -> f64 {
    #[cfg(target_arch = "aarch64")]
    {
        let frequency: u64;
        // SAFETY: cntfrq_el0 is a read-only architectural counter-frequency register.
        unsafe {
            core::arch::asm!("mrs {}, cntfrq_el0", out(reg) frequency,
                options(nomem, nostack, preserves_flags));
        }
        if frequency != 0 { return frequency as f64; }
    }
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: CPUID only queries supported processor information.
        let max_leaf = unsafe { core::arch::x86_64::__cpuid(0) }.eax;
        if max_leaf >= 0x15 {
            let ratio = unsafe { core::arch::x86_64::__cpuid(0x15) };
            if ratio.eax != 0 && ratio.ebx != 0 && ratio.ecx != 0 {
                return ratio.ecx as f64 * ratio.ebx as f64 / ratio.eax as f64;
            }
        }
        if max_leaf >= 0x16 {
            let base = unsafe { core::arch::x86_64::__cpuid(0x16) }.eax & 0xffff;
            if base != 0 { return base as f64 * 1_000_000.0; }
        }
    }

    let start = std::time::Instant::now();
    let start_ticks = cpu_time_now();
    while start.elapsed() < std::time::Duration::from_millis(1) {
        std::hint::spin_loop();
    }
    let elapsed = start.elapsed().as_secs_f64();
    let ticks = cpu_time_now().wrapping_sub(start_ticks);
    if elapsed > 0.0 && ticks != 0 {
        ticks as f64 / elapsed
    } else {
        2_000_000_000.0
    }
}

// hammer-runtime::DataPlaneMain; VPP: vlib/main.c:1461-1468;
// vlib/trace.c:135-153; vppinfra/time.c:173-202.
impl DataPlaneMain {
    fn initialize_trace_clock(&mut self) {
        self.seconds_per_cpu_tick =
            1.0 / hammer_infra::time::cpu_clock_frequency();
        self.unix_reference_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_secs_f64();
        self.cpu_reference_ticks = hammer_infra::time::cpu_time_now();
    }

    fn start_main_loop_trace_clock(&mut self) {
        self.main_loop_start_ticks = hammer_infra::time::cpu_time_now();
    }
}

// At worker construction, copy the owner's fixed display reference. Only
// last_time_stamp gets a fresh worker-local counter before dispatch.
worker.main_loop_start_ticks = main.main_loop_start_ticks;
worker.seconds_per_cpu_tick = main.seconds_per_cpu_tick;
worker.cpu_reference_ticks = main.cpu_reference_ticks;
worker.unix_reference_seconds = main.unix_reference_seconds;

// Inside show_trace, with the owner DataPlaneMain of this TraceHeader:
let relative_seconds = header.time
    .checked_sub(owner.main_loop_start_ticks)
    .expect("trace timestamp follows main-loop start") as f64
    * owner.seconds_per_cpu_tick;
let unix_seconds = owner.unix_reference_seconds
    + header.time.checked_sub(owner.cpu_reference_ticks)
        .expect("trace timestamp follows clock reference") as f64
        * owner.seconds_per_cpu_tick;
// Relative uses relative_seconds; Unix and Datetime format unix_seconds.
```

| 路径 | Rust 设计 | VPP 来源 |
| --- | --- | --- |
| `trace_count`、`set_trace_count`、`trace_buffer`、`add_trace<T>` 的热路径边界 | 对应 VPP `always_inline` 用 `#[inline(always)]`；较大 handoff/扩容实现仍在独立冷函数中，避免把整段控制逻辑复制进每个 Node | `trace_funcs.h:22-89,117-198` |
| `trace_buffer` 的 trace-disabled 分支、`add_trace<T>` 的 untraced/disabled/cross-thread/stale 分支 | 只对 VPP 原有 `PREDICT_FALSE` 条件复用 `hammer_infra::hint::unlikely`；`unlikely` 在 Hammer 只标记 cold path，不是零成本分支消除 | `trace_funcs.h:32-66,126-127`；`crates/hammer-infra/src/hint.rs:1-7` |
| Node 源额度与后续 trace | TUN 源 Node 用普通 `if remaining > 0`；中间/终端 Node 在逐包 `TRACED` 检查上用 `unlikely`，进入后直接追加记录并写字段；不对额度判断加 hint | `plugins/tap/rx_node.c:330-351`；`vnet/ip/ip_punt_drop.h:159-169`；`vnet/tcp/tcp_input.c:1289-1300`；`vlib/drop.c:93-155` |
| CLI/show/formatter/clear | 无热路径 inline 或 predict | `trace.c:93-169,273-466` |

`PREDICT_FALSE` 在 VPP punt-redirect 的逐包 trace 检查中存在，但
TCP input 和 Drop 的相同检查是普通 `if`。Hammer 统一在中间/终端
Node 的逐包追加分支使用 `unlikely(TRACED)`，这是明确的提示策略差异，
不能标成 VPP 所有 caller 都有的逐行语义；`trace add` 默认关闭时
绝大多数包未标记。源 Node 的 `n_trace > 0` 按 VPP TUN caller 保持
普通 `if`，不加 `unlikely`。
`#[inline(always)]` 只标与 VPP 同级的短 hot entry；若 handoff、pool
增长需要拆冷实现，不改变 caller 的直接记录借用或字段写入。

CLI 的无效 Node、Node 不支持源 trace、额度溢出，是可恢复的
`CliError::{TraceNodeMissing { name }, TraceUnsupported { node },
TraceLimitOverflow { node }}`；语法错误复用 `CliError::InvalidArgument`。
启动 Worker 数无法编码进 handle 用具体
`RuntimeError::TraceThreadCapacity { count }`；时钟频率取得为零时按
VPP `vppinfra/time.c:173-202` 先估计，再用明确的 2 GHz 后备，
所以不虚构 clock calibration 错误类别。实施前这些公开变体和
infra 时钟 API 须单独获批准。

```rust
// Extend the existing owner-local enums; these are the proposed additions,
// not new error enums. VPP: vlib/trace.c:408-466; vlib/buffer.h:386-396;
// vppinfra/time.c:173-202.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("trace Node not found: {name}")]
    TraceNodeMissing { name: String },
    #[error("Node does not support source tracing: {node}")]
    TraceUnsupported { node: String },
    #[error("trace limit overflows for Node: {node}")]
    TraceLimitOverflow { node: String },
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("trace handle cannot encode {count} threads")]
    TraceThreadCapacity { count: usize },
}
```

packet 路径的 trace 关闭、旧槽或容量尽是正常未捕获，返回
`false`/`None`，不产生 node error、`Result`、日志或整数错误码。
错误 Node/chain、header 跨界、错误线程写自己的 pool 是程序错误，
在 owner 内断言；Main Heap 耗尽沿用进程固定容量语义。

## 删除与 caller 迁移

| Owner | 同一批删除/替换 |
| --- | --- |
| runtime trace/config/lib | 删除旧 `TraceControlPlane/Handle/RecordSink`、`TracePolicy/InputPolicy`、`TraceRecord/Entry`、`DataPlaneTrace`、epoch/ring/SegQueue、`PacketTrace` serde、`format_packet_trace!`、`add_packet_trace!`、旧 `[trace]` TOML 与 config hook、`PacketTraceSerialization`。`TraceMain` 成为每个 main 的字段。 |
| runtime worker/main loop | worker main 改为启动前稳定存储、短借用执行；barrier 内可访问各 main 的 trace。删除 `worker_parts/new_worker` trace handle 克隆与 `set_trace_control/trace_control/try_mark_trace/should_trace_packet`。 |
| runtime Node/graph | `NodeRuntime` 注册时获得 NodeId；Node descriptor 保存仅源 Node 使用的 `trace_supported`；runtime 在 graph freeze 前注册 `handoff-trace`（next 0 为现有 `drop`）并缓存 NodeId；`trace_buffer` 设置源 Next Frame trace 位，NodeMain 在中间 Node enqueue/目标 dispatch 时传播它，并核对跨 Worker direct Frame 路径。 |
| core Buffer | 保留 flag/handle；删 `free_buffers` 的 `release_trace` 回调与 `take_trace_handle` 的 trace 完成语义。Buffer free 和 chain 不删除 pool 记录；现有 `attach_clone` 只共享 tail，不复制标记到 head。 |
| service/插件 | 只有 `tun-input` 等入口源 Node 调用 `trace_count/trace_buffer/set_trace_count` 启动捕获。IP input/local/reassembly、ICMP、TCP input/output 和 Drop 等中间/终端 Node 的旧 `add_packet_trace!`/`format_packet_trace!` 调用迁移为 `TRACED` 判断、`add_trace::<T>` 和各 Node 自己的定长 formatter；现有 `drop` 只追加错误记录，绝不启动捕获。不把 IP/TCP trace 类型搬入 runtime；IP reassembly Process 不再调用 `TraceControlHandle::finalize`。 |
| 文档/范围 | ADR-0007/0009/0049 与 `CONTEXT.md` 中旧 trace 生命周期描述更新。Binary API 的 `traced` 元数据和日志级别 Trace 属于别的系统，不删除。 |

现有插件 trace 结构里有 `Option`/Rust enum，不可直接借作任意零位
模式有效的 trace 记录；各 owner 要改成满足
`KnownLayout + FromBytes + IntoBytes + Immutable` 的无 padding 定长
payload，保留领域 enum 的展示含义。Node 通过
`add_trace::<T>` 直接填获得的 `&mut T`，不再构造临时 `T`、复制到
字节切片或走 serde/bincode。
`TraceHeader` 的 16 字节单位不允许在一个 Node 混入长度不明的
bincode blob。无适配旧 API 的 re-export。全仓搜索 definitions、
imports、宏调用、测试、配置注册和文档以确认旧链路彻底消失。

TCP 的接收和输出使用两个定长记录布局。VPP 的
`tcp_rx_trace_t`/`tcp_tx_trace_t` 都保存 TCP header 与当时的
`tcp_connection_t`（`tcp_input.c:1213-1268,2410-2428`、
`tcp_output.c:50-69,2226-2252`）。Rust 不按字节复制含定时器和私有
状态的整个 `TcpConnection`，只记录展示所需的连接事实：owner worker、
连接索引、状态、IP family、本地/远端地址与端口。`tcp-input` 成功
lookup 后把真实 listener connection index 及来源写入 Buffer opaque；
`tcp-listen` 在创建 child 前按 `Listener` 来源读 listener，TIME-WAIT
转入 listen 时按 `Session` 来源读原连接。无 listener 或连接已删除是
原有 packet error，不合成 id。输出 Node 在压入 IP header 前读取同样
事实；无连接的 stateless RST 只记录 TCP header。两类记录都直接填
`add_trace` 借出的空间，不 Clone 连接。VPP
`tcp_input.c:2820-2824,2762-2768,2418-2426`。

## 新 API 的实施批准点

| 拟议项 | 现有接口为什么不足 | 最终结果与边界 |
| --- | --- | --- |
| 每 `DataPlaneMain` 的 `TraceMain` 与 `trace_buffer/add_trace<T>/trace_count/set_trace_count` | 旧全局控制器不能给源 Node 提供 VPP per-main pool/count，也错误地在 Buffer free 时完成记录 | 只在当前执行 main 上写；`add_trace<T>` 直接借出已清零的定长记录；CLI 只在 barrier 内访问别的 main；无旧适配层。 |
| `hammer-infra::Pool::clear(&mut self)` | 原有 Pool 只有单项 `remove`，不能靠替换 `Pool::new()` 表达 trace clear | infra 已补通用原地清空：恰好 drop 占用元素并重置索引和 `opaque`，保留 Pool 对象及可复用容量；固定容量 pool 仍保持固定容量。 |
| `NodeRuntime::node_index`、Node 的 `trace_supported` | 现有 NodeRuntime 没有自身身份，formatter 存在性也不等于源追踪能力 | NodeMain 注册时一次赋值，所有注册路径携带；不添加第二种 Node 身份。 |
| WorkerThread 稳定保存 main、短借用循环与 barrier 内只借用 trace | 现有线程闭包 move `Box<DataPlaneMain>`，主线程无法实现 VPP `foreach_vlib_main` | 不增加 trace worker 表，不公开裸指针；仅受控内部 `UnsafeCell`，有明确借用交接。 |
| `hammer-infra::time` CPU counter 频率校准与具体 CLI/runtime 错误变体 | 现有 `cpu_time_now` 返回 ticks，不能正确展示秒；现有错误不能区分不支持的 Node 与句法错误 | 频率能力通用且仅在初始化/展示用；错误限于可恢复 CLI/启动边界，不进入 packet 热路径。 |
| `#[cli_command]` 的同步 `fn(&mut DataPlaneMain, Args)` 形式 | 现有 async-only 宏在 barrier 释放后才 poll handler，不能安全访问每个 main 的 trace pool；现有 `R: Display` 不能表达 VPP add/clear/set 的无输出成功 | 仍生成现有 `CliCommandRegistration` 和 `CliCommandFn`；有输出的 `R: Display`，无输出的 `R = ()` 生成零字节文本；解析、执行和格式化在 `dispatch` 的 barrier 内，ready task 只持有 String；ADR-0049 的 async Args-only 形式不变。 |

以上是 ADR 的设计提案，不是暗中扩大已批准的生产 API；实施前按仓库
规则逐项确认。CLI 命令只经现有宏注册到 `CliCommandRegistration`。

## 设计审查与验证矩阵

| 项 | 当前 Hammer | 目标与依据 |
| --- | --- | --- |
| owner/同步 | 全局 Arc/Mutex + 完成队列 | 每 `DataPlaneMain` 一个 TraceMain；CLI barrier 内逐 main；V1/V8。 |
| 创建/额度 | 配置有 quota，但 `try_mark_trace` 没有生产 caller | 只有源 Node 按 V2/V3 取 count、标记成功才扣；中间/终端 Node 不调用 `trace_buffer`，V9。 |
| 记录/释放 | bincode + Buffer free finalize | 源和中间/终端 Node 各自借用 `&mut T` 直接追加本 Node 记录；仅 clear 释放，V3/V4/V7/V9。 |
| 传播 | 无跨 Worker trace 记录 | 保留 Next Frame bit，目标线程重建 trace 并标源 handle；V5/V6。 |
| CLI | async handler 会在 barrier 之后执行 | 仍用 ADR-0049 的命令宏/注册目录；同步 main-aware 形式由宏私有适配，`dispatch` 在 barrier 内完成 Args 解析、trace 操作和 Display 格式化；ready task 只持有 String；V7/V8/H3。 |

| 测试（实施后执行） | 必须观察到的行为 |
| --- | --- |
| `trace add tun-input 2`，送三包 | 每线程只新增两条，后续 IP/TCP/Drop 节点追加到对应包；`trace add ... 0` 不清旧记录。 |
| 已标记包经过中间 Node 和 Drop | 各 Node 只追加自身记录，handle 和源额度不变；未标记包不产生记录；trace 关闭或旧槽时跳过追加但继续包处理。 |
| Trace 关闭时的中间/终端 Node 吞吐 | release profile 验证 `unlikely(TRACED)` 冷分支不改变未追踪包的处理结果，并记录相对普通 `if` 的热路径代价；这是 Hammer 相对 VPP TCP/Drop caller 的显式提示差异。 |
| TUN/TCP trace record 与未追踪流量 | Node 直接写所借 `&mut T`，字段与包事实一致；未标记包不分配记录；没有中间 payload 拷贝或旧宏调用。 |
| `trace add tun-input`、多余或非法 Args | 缺少 count、未知 token、非法 max/时间格式均返回具体 `CliError`；错误发生在任何 main 的 trace 状态改变之前。 |
| `show trace` / `max N` / timestamp-format | 逐 main 排序、各限 N、不消费 pool；tick 到时间的换算正确。 |
| CLI 成功输出 | add/clear/set 返回 `()`，连接只发原有 NUL 完成标记；show trace 返回拥有型 String，show timestamp-format 通过枚举 `Display` 输出 VPP 的格式行。 |
| `set trace timestamp-format` 后 `clear trace` | thread-0 时间展示设置仍保持，Worker 无展示格式同步；show 命令仍通过原 CLI 宏/Process 输出。 |
| `clear trace` 后仍在途的 Buffer | 无越界/悬空写，池已清，只有重新 `trace add` 后才捕获新包。 |
| chain、attach_clone、handoff、reassembly expiry、free | `follow_chain` 标记 chain 各段；`attach_clone` 保持 head/tail 原有标记且不把 tail handle 复制到 head；跨 Worker 新记录保留来源；任何 Buffer 释放都不删除已有 trace。 |
| 非法/不支持 Node、额度溢出 | 具体 CLI 错误；各 main 的 count/limit/pool 未部分修改。 |
| 编译期 layout、全仓删除审计、CLI 同步测试 | `TraceHeader` 16/16、各 `T` 的对齐和布局满足借用条件；没有旧控制器/config/serde caller；宏仍走原命令目录/Unix Process；Worker 停在 barrier 且已释放长生命周期 `&mut` 才被 CLI 借用。 |

**实施审查结论：** 上述 `NodeRuntime` 身份、`trace_supported`、
Worker-main 借用边界、infra 时钟、源捕获、跨 Worker trace 位与 TCP
连接快照均已接线。集中静态核对和修正记录见
`docs/reviews/issue-374-adr-0051.md`。按本次要求未运行编译、测试或
CI；本 ADR 不声称运行时验证已完成。
