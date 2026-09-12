# ADR-0011：SVM 队列、FIFO、SSVM 所有权与普通函数多架构选择

- 日期：2026-09-11
- 修正日期：2026-09-12
- 状态：Proposed；仅设计，尚未批准本次修正规范，未修改生产代码
- Hammer 基线：`418aa0953d929f4175370ce1873066f4ff193f38`
- VPP 基线：`third_party/vpp`，提交
  `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`
- 替代关系：ADR-0012 完整拥有 `svm_region`、PVT/Data Heap 和 active heap
  设计；本 ADR 不再定义 region allocator、region layout 或 subregion
  address assignment

本文是 ADR-0011 的一致性修正版。旧版关于 offset region heap、different-VA
region attach、`SvmRegionHeap`、`SvmHashMap` 和独立 monotonic subregion identity
的规范已删除，而不是继续作为“历史设计”留在可实施段落中。它们均为当前代码中
待迁移的错误基线，不是可选实现。

## 1. 范围与结论

本文只决定以下内容：

1. `SsvmPrivate` 映射层与 `SvmRegion`、FIFO segment 的关系；
2. VPP `svm_queue_t` 对应的 runtime-sized byte queue；
3. VPP `svm_msg_q_t` 对应的 descriptor queue 和 inline data rings；
4. `svm_fifo_t` 的 shared/private split、SPSC ownership、chunk、OOO 和通知；
5. `fifo_segment_t` 的 slice、allocation、migration、MQ 和 memory pressure；
6. 两个 FIFO copy function 的普通函数 multiarch selection。

本文不把 Session event、TCP、应用策略或 Binary API schema 下移到
`hammer-infra`，也不把任何 FIFO segment 解释成 `SvmRegion`。

最终决策如下：

- **D1 Region authority。** `SvmRegion` 直接拥有 fixed-VA mapping、mandatory
  PVT `MemHeap`、optional Data `MemHeap`、membership、root bitmap 和
  `SvmMainRegion`。它不是 `SsvmPrivate` payload。所有细节以 ADR-0012 为准。
- **D2 SSVM mapping。** 通用 `SsvmPrivate` 对应 `ssvm_private_t`，拥有
  SHM/MEMFD/PRIVATE backing、mapping、shared header、ready 和 generic shared
  heap。creator 记录实际 `ssvm_va`，client probe page 0 后默认在同一 VA 完整
  attach。只有 payload protocol 明确证明全部共享引用都是 relative offset 时，
  才能把 `ssvm_va` 发布为 0。
- **D3 FIFO segment exception。** `SvmFifoSegment` 是 `SsvmPrivate` payload，
  不使用 `SvmRegion` PVT/Data Heap，也不从 SSVM generic heap 分配 FIFO storage。
  它通过 owner-private map-only path 保留 two-page allocation overhead，从第二页之后
  直接管理 storage，发布 FIFO-segment-header-relative offset，并把 `ssvm_va` 置 0，
  所以 client 可以 arbitrary-VA attach。该 path 不向任何参与者暴露可用 generic heap。
- **D4 Fixed-element queue。** `SvmQueue` 是 runtime `nels`/`elsize` byte queue，
  queue level 不使用 `T`。元素 schema 属于调用方协议。
- **D5 Message queue。** `SvmMsgQ` 是 8-byte descriptor queue 加可变数量的
  ordered inline data rings。保留显式
  `alloc_msg -> add -> sub -> free_msg`；不增加 reservation、cancel 或 Drop
  rollback。
- **D6 FIFO。** `svm::fifo::Fifo` 的 shared header/chunks 位于共享 mapping；
  OOO trees/pool、chunk lookup、active links 和 worker ownership 位于每个进程的
  private FIFO。生产者和消费者各自只写本方 index，读取对方 index 时 acquire。
- **D7 FIFO segment。** segment 使用 monotonic CAS byte allocator 和 per-slice
  freelists，拥有 create/attach/delete/cleanup、FIFO direction、migration、MQ、
  preallocation、capacity accounting 和 pressure policy。只有 RX FIFO 进入
  private active list。
- **D8 Multiarch。** `svm_fifo_copy_to_chunk` 和
  `svm_fifo_copy_from_chunk` 通过 infra 普通函数选择机制在进程启动后选定实现；
  它不使用 Graph Node registration，也不改变 FIFO ownership。

## 2. VPP 证据账本

以下路径均相对 `third_party/vpp/src/`。

| ID | 来源 | 已核实事实 | 约束 |
| --- | --- | --- | --- |
| V1 | `svm/ssvm.h:46-87` | shared header 含 heap、VA、size、pid、name、opaque、ready、backend；`ssvm_private_t` 持进程私有 mapping/backing 状态 | shared 与 private state 必须分开 |
| V2 | `svm/ssvm.c:70-106`、`:209-277`、`:357-410` | SHM、MEMFD、PRIVATE server 都创建 shared locked heap，并把实际 mapping address 写入 `ssvm_va` | 通用 SSVM 不是默认 offset-only mapping |
| V3 | `svm/ssvm.c:111-178`、`:280-340` | client probe page 0并读取 `ssvm_va`/size；SHM 无条件 fixed remap且等待 ready，MEMFD 仅对非零 VA fixed remap且不等待 | attach、地址选择与 ready 是独立约束 |
| V4 | `svm/fifo_segment.c:285-330` | FIFO segment 从第二个 page 后直接布置 header/allocator，发布 header offset，最后把 `ssvm_va` 清零 | zero VA 是 FIFO owner 的 arbitrary-address 意图；backend 差异见 V18 |
| V5 | `svm/queue.h/c` | `svm_queue_t` 使用 runtime `maxsize`/`elsize` 和 inline bytes；shared robust mutex/condvar 保护 add/sub/wait；producer/consumer eventfd 数值位于 shared queue，但各自只在所属进程有意义 | queue 不应按 Rust element type 参数化；两个 fd slot 不能冒充跨进程 descriptor identity |
| V6 | `svm/message_queue.h:18-87` | shared descriptor queue/ring state与private ring pointers/eventfd/spinlock分开；descriptor 是两个 `u32` 的 8-byte union | MQ 不是旧 MultiRing layout |
| V7 | `svm/message_queue.c:62-117` | `svm_msg_q_init` 把每个 ring header 和 `nitems * elsize` bytes 连续排在 queue allocation 内 | data ring 是 inline shared storage |
| V8 | `svm/message_queue.c:103-110` 及全树调用点 | `ring_cfg.data` 只在 size calculation 中被检查，init/attach/msg_data 从不绑定它；所有在树调用者都传 0 | 不得把 `data` 字段解释成可用的 caller-owned ring contract |
| V9 | `svm/message_queue.c:210-307`、`:334-467` | ring tail 分配、descriptor add/sub、ring head 回收互相独立；回收必须按 ring head 顺序 | 保留 explicit four-stage lifecycle |
| V10 | `svm/fifo_types.h:29-195` | chunk/shared FIFO/signals 位于共享存储；`svm_fifo_t`、OOO、active list、session fields 位于 private storage | Rust 不能把整个 FIFO struct 放进 shared mapping |
| V11 | `svm/svm_fifo.h:68-111`、`:466-620` | owner index relaxed，foreign index acquire；通用检查两边 acquire | 需要 role-specific max/read/write APIs |
| V12 | `svm/svm_fifo.h:219-457` | 完整操作含 fill/provision、enqueue variants、peek/drop、arbitrary segment list、clone 和 diagnostics | 两个 slice 返回值不构成 parity |
| V13 | `svm/svm_fifo.h:629-883` | OOO newest/reset；event flag；immediate/full/empty dequeue notification；threshold；最多 7 subscribers | 通知不是单个 bool |
| V14 | `svm/fifo_segment.c:127-210`、`:482-919` | byte range CAS allocation；chunk freelist head 使用 48-bit offset + 16-bit tag；按 size class batch/reuse | plain offset CAS 不足以声称等价 |
| V15 | `svm/fifo_segment.c:938-1077` | duplicate、server/client free 和 worker-slice attach/detach 是不同操作；slice attach 取得 replacement resources，但没有 allocation-failure branch | Hammer 必须保留角色差异并补上 failure-atomic error path |
| V16 | `svm/fifo_segment.c:1109-1529` | segment 提供 MQ alloc/attach/discover/offset、header/chunk/pair preallocation、容量统计和 pressure status | 这些能力属于 FIFO segment owner |
| V17 | `svm/svm_fifo.c:17-100`、`svm/CMakeLists.txt` | 只有两个 FIFO chunk copy functions 使用 `CLIB_MARCH_FN` | multiarch 范围必须保持具体 |
| V18 | `svm/ssvm.c:161-168`、`:316-328` 与 `svm/fifo_segment.c:320-330` | FIFO init 把 `ssvm_va` 清零；MEMFD client 仅在 VA 非零时加 `MAP_FIXED`，但 SHM client 无条件 `MAP_FIXED` | vendored SHM FIFO attach 与 zero-VA contract 冲突；Hammer 必须显式决定而不能声称该分支已由 VPP 证明 |
| V19 | `svm/fifo_segment.c:873-918`、`:948-1006`、`:1042-1077` | RX/TX allocation 都增减 shared active count，只有 RX 进入 private active list；slice attach 在 private copy 后分配 replacement header/chunks，且没有 allocation-failure branch | direction 的 count/list 语义必须分开；Hammer failure atomicity 是安全增强，不是 VPP 已有错误路径 |
| V20 | `svm/fifo_segment.c:1414-1529`、全树 `FIFO_SEGMENT_F_MEM_LIMIT` 搜索及 `plugins/unittest/segment_manager_test.c:334-353` | status calculation 在 usage 低于 high watermark 时清除 `MEM_LIMIT`，但 vendored tree 没有置位调用；预期 `NoMemory` 的测试被注释 | 自动置位不是已核实 VPP 行为；Hammer 若补全该 transition，必须记录为明确策略 |
| V21 | `svm/queue.c:87-100`、`svm/message_queue.c:176-207` | robust mutex owner death只调用 `pthread_mutex_consistent`，没有验证受保护的 queue indices/occupancy | Hammer 必须按仓库错误规则验证 exact state或retire queue，不能无条件继续 |

VPP 测试来源是
`plugins/unittest/svm_fifo_test.c`、
`plugins/unittest/segment_manager_test.c` 和
`plugins/unittest/session_test.c`。这些测试是行为来源，不等于 Hammer 已通过。

## 3. 当前 Hammer 基线与语义差距

本表记录当前代码，不是目标状态。

| 当前位置 | 已有内容 | 仍不符合本文的部分 |
| --- | --- | --- |
| `svm/region.rs`、`region_heap.rs`、`hash_map.rs` | offset region、`SvmRegionHeap`、`SvmHashMap` 已实现 | 整体由 ADR-0012 替换并待删除 |
| `svm/ssvm.rs` | backend、mapping、probe attach、ready、typed FIFO header offset | 默认把 `ssvm_va` 写 0；没有 generic shared heap；payload 紧跟自定义 header；因此不能声称 generic SSVM parity |
| `svm/queue.rs` | runtime byte element、shared mutex/condvar、add/add2/sub/wait | 仍需跨进程、owner-death、eventfd 与 ABI integration 证明 |
| `svm/msg_queue.rs` | descriptor、four-stage operations、wait/eventfd 基本面 | `SvmMsgQRingConfig<'data>` 把 ring bytes 放在 caller slice，和 V7/V8 相反；生命周期与并发 guard 仍需重做 |
| `svm/fifo.rs` | shared/private 字段、chunk、部分 OOO/notification/multiarch operations | `unsafe impl Sync`、大量 `&self` mutation、`Read/Write for &Fifo`、两个 segment 上限、Session IDs 下沉 infra、通知和 chunk APIs 不完整 |
| `svm/fifo_segment.rs` | shared header/slices、byte CAS、basic allocate/free/attach、部分 preallocation/statistics | private storage 是 `Vec<Vec<Option<Fifo>>>`；没有 direction/active RX list、完整 main-owned lifecycle、migration、MQ、flags/watermarks/pressure；freelist 没有完整 tagged-head proof |
| `multi_ring_msg_queue.rs` 及 runtime callers | 旧 MP/SP queue 仍被 Session runtime 使用 | 完整调用闭包迁移后删除，不保留 alias |

因此 `Fifo`、`SvmFifoSegment`、`SsvmPrivate` 和 `SvmMsgQ` 当前都不能标记为
“完全符合 VPP”。已有单元测试只证明其覆盖到的行为，不能替代本表中的缺口。

### 3.1 缺失或必须重做的 API inventory

下表以最终语义为准；“重做”表示当前有同名或相近入口，但 ownership、layout 或失败行为
不满足契约，不能按“API 已存在”关闭差距。

| Owner | 缺失或必须重做的 surface | 当前差距 |
| --- | --- | --- |
| `SsvmPrivate` | generic shared `MemHeap` create/attach、nonzero same-VA attach、zero-VA map-only attach、owner-private FIFO map-only creation | 当前所有 server path 都没有 generic heap，且默认发布 zero VA |
| `SvmQueue` | byte `add/add2/sub/sub2`、guard-owned no-lock operations、wait/timed-wait、producer/consumer eventfd、owner-death validation | 基本入口已有；仍缺跨进程、eventfd role ownership和owner-death后的行为证明 |
| `SvmMsgQ` | inline-data `layout/size/init/attach/cleanup`、producer guard 内的 alloc/write/add、`sub/sub_batch/free_msg`、wait/timed-wait、eventfd install/allocate | 当前 ring data 来自 caller slice，producer lock 没有覆盖完整 three-stage publication |
| `Fifo` | role-specific max enqueue/dequeue 与 empty/full、`fill_chunk_list`、`provision_chunks`、arbitrary-count `segments`、max read/write chunk、newest OOO/reset、完整 dequeue notification/subscribers、single-chunk clone | 当前只有 generic capacity、two-slice read、部分 OOO/notification；多数 mutator 错用 `&self` |
| `SvmFifoSegmentMain` | `init/create/attach/lookup/index/delete` 和 segment base/size query | 当前 Main 只有 local add/get，create/attach/delete 生命周期在 segment 外没有闭合 |
| `SvmFifoSegment` FIFO/chunk | directional allocate、offset attach、duplicate、server free、client free、slice migrate、FIFO offset；chunk allocate/collect/offset | 当前 allocate 没有 direction；attach/detach/free 没有完整 VPP role 与 migration contract |
| `SvmFifoSegment` MQ/preallocation | MQ allocate/attach/discover/offset；FIFO header、每个 chunk class、RX/TX pair preallocation；aligned reserved-byte allocation | 当前没有 MQ/reserved allocation，preallocation surface 只覆盖部分行为 |
| `SvmFifoSegment` accounting | segment allocatable size、new/cached/available/freelist bytes、active/free FIFO counts、per-class free chunk counts、flags、watermarks、usage/status | 当前只有部分 free/cached/available counters，没有 active count、flags 或 pressure state machine |
| `simd` | `march_fn!` 和两个 FIFO copy registrations | scalar/variant functions 已有，普通函数 registration/selection 未闭合 |

## 4. ABI 与映射边界

“SVM 一律用 offset”与“SSVM 一律 same VA”都不成立。每个 owner 的 ABI 如下：

| Owner | shared references | attach VA | allocator |
| --- | --- | --- | --- |
| `SvmRegion` | raw pointers、ordinary collection pointers、`*mut MemHeap` | 必须与 creator 相同 | mandatory PVT `MemHeap`，optional Data `MemHeap` |
| generic `SsvmPrivate` payload | header/shared heap 中含 raw pointers | 默认与 creator 相同 | SSVM generic locked `MemHeap` |
| `SvmFifoSegment` payload | FIFO-header-relative offsets；private process rebuilds pointers | 可以不同；owner 显式发布 `ssvm_va = 0` | monotonic segment byte allocator + freelists；不使用 region heap或generic heap |
| `SvmQueue` | inline bytes/indices和两个role-specific fd数值，无进程本地 pointer | 由 enclosing payload contract 决定 | 不拥有 payload allocator |
| `SvmMsgQ` | inline descriptor queue、ring headers和ring bytes；private attach state重建 pointers | 由 enclosing FIFO segment offset contract 决定 | enclosing FIFO segment allocation |

共享 layout 只支持相同 architecture、endianness、atomic width 和已批准的 layout
version。Hammer 不承诺与 VPP C struct 二进制互通，但每项有意 layout 差异必须单独
记录；不得以“Rust ABI”作为改变状态机或 ownership 的理由。

创建发布顺序是：mapping -> allocator/header/storage 初始化 -> protocol identity
发布 -> ready release。attach acquire ready 后才读取 payload。失败按逆序释放尚未
发布的资源，不得让 peer 看见半初始化 header。

## 5. `SsvmPrivate` 规范

### 5.1 Owner 与 shared header

`SsvmPrivate` 是进程私有 owner，保存 mapping base/size、backend、server role、pid、
name copy、fd 或 attach timeout。它不包含 `SvmRegion`，也不替 region 管 membership、
root、PVT/Data Heap 或 subregion。

通用 shared header 至少表达：actual `ssvm_va`、mapping size、server/client pid、
backend、ready、shared `MemHeap` pointer 和 payload publication slot。Hammer 可保留
magic/layout version 与 typed FIFO header offset 作为明确扩展。VPP 中未被调用的
recursive header spinlock 不要求为了字段相似而移植；shared heap 和同 VA attach
具有实际调用语义，不能再删除。

generic server 对 SHM/MEMFD/PRIVATE 创建 locked shared `MemHeap`，并在该 active heap 中
分配 generic payload metadata。attach 只复用 creator 的 heap，不重建。PRIVATE
backend 没有 client attach。

### 5.2 Mapping 与 ready

- server 的 `requested_va == 0` 只表示让 OS 选择 creator address；初始化完成后
  shared `ssvm_va` 记录返回的实际非零地址；
- SHM/MEMFD client 先只映射 page 0，读取 size、backend 和 `ssvm_va`，解除 probe，
  再映射完整 segment；
- generic SSVM 的非零 `ssvm_va` 要求 same-VA attach；目标 range 已占用必须返回
  typed error，不能覆盖未知 mapping；
- owner 明确发布的零 `ssvm_va` 表示 non-fixed attach；Hammer 对 SHM/MEMFD 使用同一
  分支，不复制 VPP SHM client 无条件 `MAP_FIXED` 到地址 0 的矛盾；
- SHM client 等待 ready；MEMFD attach 本身不等待 ready，payload owner 决定何时
  调 `wait_ready`；
- `delete` 按 backend 分发，server/client 只释放各自拥有的 mapping/fd/name；
  SHM backing name 只由 lifecycle owner unlink。

### 5.3 FIFO segment 特例

VPP 的 generic server init 先建立 shared heap，`fifo_segment_init` 随后保留 two-page
overhead，从第二页之后直接建立 header 与 byte allocator，并留下“remove ssvm heap
entirely”的 TODO。该 heap 与直接管理的 storage range 重叠但之后不再用于 FIFO storage。

Hammer 把这项实现残留收窄为 owner-private map-only path：保留 two-page layout，
不创建或发布可用 generic heap。它把 header 相对 SSVM base 的 offset 发布到 typed slot，
再把 `ssvm_va` 改为 0，最后发布 ready。client arbitrary-VA attach 后只允许调用
FIFO segment 的 offset translation；不存在可读取或重建的 generic heap pointer。

这一特例不能推广到 `SvmRegion`、generic SSVM 或未来任意 payload。新增 offset
payload 必须逐项证明 shared state 中没有 raw pointer，并由自己的 owner 明确选择
`ssvm_va = 0`。

## 6. `SvmQueue` 与 `SvmMsgQ`

### 6.1 `SvmQueue`

`SvmQueue` 的配置是 `nels`、`elsize` 和 consumer identity；共享 storage 是 header
后连续 `nels * elsize` bytes。公开操作接收精确长度的 `&[u8]`/`&mut [u8]`：

- `add`、`add2`、对应的 guard-owned no-lock variants；
- `sub`、nonblocking `sub2`；
- empty/full/length；
- lock/try-lock RAII guard；
- wait、timed wait、producer/consumer signal 与 eventfd installation。

`add2` 容量不足必须零提交。wait 在循环中重查 predicate。eventfd 改变通知路径，
不删除 fixed queue 的 shared mutex。VPP 把 `producer_evtfd`/`consumer_evtfd` 数值放在
shared queue 中，但每个 slot 只由对应 role 在自己的进程安装和解释；Rust owner保留
`OwnedFd`，shared slot只发布该进程的 raw number，不能把它复制给另一进程直接使用。
Data Worker packet loop 不做 condvar wait、poll或未知时长的 shared-lock contention。

### 6.2 `SvmMsgQ` layout

shared allocation 依次包含：

```text
SvmMsgQShared
descriptor queue header + q_nitems * 8-byte descriptor
ring[0] header + ring[0].nitems * ring[0].elsize bytes
...
ring[n-1] header + ring[n-1].nitems * ring[n-1].elsize bytes
```

private `SvmMsgQ` attach state保存 descriptor queue pointer、每个 ring 的 shared
pointer、eventfd 和 process-local producer lock。`SvmMsgQRingConfig` 只包含
`nitems`/`elsize`；删除 `'data` lifetime 和 `&mut [u8] data`。VPP 的 dead
`ring_cfg.data` 字段不进入 Hammer API。

`alloc_msg(nbytes)` 按配置顺序选择第一个 `elsize >= nbytes` 且未满的 ring；
`alloc_msg_w_ring` 使用指定 ring。两者推进 ring tail/cursize但不发布 descriptor。
`msg_data` 只借用 descriptor 对应的 inline slot。`add` 才复制 descriptor 并 release
发布 descriptor queue tail；`sub` acquire 取得 descriptor但不回收 slot；
`free_msg` 必须等于该 ring 当前 head，随后推进 head 并减少 cursize。

并发生产者必须让 alloc、slot write 和 add 处于同一个 producer lock scope。
Rust 用 owner-local RAII producer guard表达这个 scope；guard Drop 只解锁，不取消
已经分配的 ring slot。不存在 `MsgReservation`、`cancel` 或隐式回收。单 consumer
通过 `&mut SvmMsgQ` 执行 sub/read/free，不添加 consumer lock。

无 eventfd 时使用 descriptor queue 的 process-shared robust mutex/condvar；有
eventfd 时 producer coordination 使用 process-local spinlock，fd 只存在 private
handle。eventfd number 不进入 shared layout。queue 从 empty 变 non-empty 时通知
consumer；descriptor queue 或 data ring从 full 变 non-full时通知 producer。

## 7. `Fifo` 规范

### 7.1 Shared/private split

shared mapping 中只有：

- `SvmFifoShared`：start/end/head/tail chunk offsets、size、min allocation、slice、
  freelist next、signals、head/tail positions；
- `SvmFifoChunk`：normalized start、length、next offset、chunk lookup indices、bytes；
- FIFO segment shared slices与 allocation counters。

process-private `Fifo` 保存 shared/header pointers、enqueue/dequeue chunk lookup、OOO
trees、OOO segment `Pool`、newest/list state、local reference/lifecycle state和 active
list links。它由当前 Data Worker直接拥有，不能 `Sync`，也不能通过 `&self` 修改
private或shared FIFO state。删除 `Read/Write/BufRead for &Fifo`；producer/consumer
domain operation 均要求真实 owner 的 `&mut self`。

VPP private `svm_fifo_t` 内的 app/session/thread/segment-manager identities 是 VPP
Session coupling。Hammer 将这些 identities 留在 `hammer-service` 的 Session owner，
不在 `hammer-infra::svm::fifo::Fifo` 复制 `app_session_index`、`vpp_sh` 等字段。这是
保持 crate dependency direction 的有意差异，不影响 FIFO storage/state machine。

### 7.2 Index ordering

- producer relaxed 读写自己拥有的 tail；读取 consumer head 使用 Acquire；发布新
  tail 使用 Release；
- consumer relaxed 读写自己拥有的 head；读取 producer tail 使用 Acquire；发布新
  head 使用 Release；
- role-independent diagnostics 对 head/tail 都使用 Acquire；
- chunk bytes和links必须在 tail Release 前完成；consumer 释放 chunk/header前先
  head Release并结束所有借用。

API 必须区分 `max_dequeue_cons`、`max_dequeue_prod`、`max_dequeue` 和
`max_enqueue_prod`、`max_enqueue`，以及 owner-specific `is_empty`/`is_full`。

### 7.3 Chunk 与 I/O API

必须提供或保留以下语义：

- `fill_chunk_list`：为 advertised FIFO size 准备完整 chunk coverage；
- `provision_chunks`：按 caller 提供的输出容量返回任意数量 writable segments，
  允许只 provision 部分请求；
- `enqueue`、`enqueue_with_offset`、`enqueue_segments`、`overwrite_head`；
- `dequeue`、`peek`、`dequeue_drop`、`dequeue_drop_all`；
- `segments`：填充 caller-provided segment slice，不限制为两个 borrowed slices；
- `max_read_chunk`、`max_write_chunk`；
- `init_pointers`、single/default-chunk-only clone、`n_chunks`、`is_sane`。

公开 safe API 不得发布未初始化 bytes。VPP `enqueue_nocopy` 的 Rust 对应只能是
owner-internal commit，或由能证明 destination 已完整初始化的写入借用调用；不能让
任意 safe caller仅凭长度推进 tail。任何协议层只能直接写相邻 destination FIFO，
commit 成功后再 consume source；失败时两边可见位置不变。

### 7.4 OOO

OOO state完全属于 producer private FIFO。`enqueue_with_offset` 使用 wrapping
sequence comparison，合并相邻/重叠 holes，并只在从 tail 开始的洞被连续填满时
推进 visible tail。API 包含 first/count/has、newest 和 newest reset。OOM、越界或
chunk provision 失败不能发布部分 OOO range。

### 7.5 Notification

完整通知状态包含：

- event flag 的 set/unset；
- dequeue immediate notification；
- 从 full 到 non-full transition notification；
- 到 empty transition notification；
- `has_deq_ntf` suppression/reset；
- dequeue threshold；
- 最多 7 个 subscriber identities及 add/delete/count。

`needs_deq_ntf(last_dequeued)` 必须按 flags、threshold、前后 occupancy 和
`has_deq_ntf` 计算，不能把全部模式折叠成 `want == 1`。通知只产生 owner-defined
event fact，不把 Session scheduler下移到 infra。

## 8. `SvmFifoSegment` 规范

### 8.1 Layout 与 allocation

shared segment header包含 cached/active/reserved bytes、max FIFO size、slice count、
first-allocation percentage、MQ count、monotonic byte index/range和 fixed shared
slices。每个 shared slice包含 11 个 4 KiB 到 4 MiB chunk classes、FIFO header
freelist、cached chunk bytes、virtual bytes和per-class counts。

byte allocator只用 CAS 授予不重叠的新 range；它不承担 free。回收的 chunks进入
size-class freelist，FIFO shared headers进入本 slice header freelist。private slice
使用现有 `Pool<Fifo>` 或等价 fixed owner-local pool，并维护 virtual bytes与 active
RX FIFO list；禁止 `Vec<Vec<Option<Fifo>>>` 作为最终 ownership model。

VPP 的 chunk stack把 48-bit segment-relative offset与 16-bit generation tag放在同一
atomic head。该 tag 只降低 ABA 概率，注释本身不是 Rust memory-safety proof。实现前
必须提交并批准以下二选一证明：

1. 证明 producer/consumer/slice lifecycle使被 pop node 在所有潜在 reader结束前
   不会重新入栈，tag wrap不影响有效 pointer读取；或
2. 选择已有、可证明 reclamation protocol，同时保持 segment-relative identity、
   lock-free hot path和VPP可观察语义。

在该证明获批并通过 loom/model与跨进程 stress前，chunk freelist parity是 blocker，
不能仅把 plain offset改成 tagged integer后宣称完成。所有可入栈 chunk offset还必须
严格小于 `2^48`，create/attach在发布前验证 segment range满足该表示上限。

### 8.2 Lifecycle 与 direction

`SvmFifoSegmentMain` 以 `Pool<SvmFifoSegment>` 拥有 process-local segments，并提供
main init、create、attach、lookup、delete。segment 自身提供 cleanup；delete先
cleanup private FIFOs/MQs，再按 SSVM backend释放 mapping。

FIFO allocation必须接收 `FifoDirection::{Rx, Tx}`。两种方向都建立 shared header、
chunks和private FIFO，并都增加 shared active count；只有 RX 进入 private active list。
server free对两种方向都减少 shared active count，并回收 private FIFO、shared header和
chunks；client-only free只释放 client private FIFO，不回收server shared storage。按
shared-header offset attach和duplicate是独立操作，不能合并为一个“reconstruct then
always free”入口。

### 8.3 Worker slice migration

VPP 的 attach path先复制 private FIFO，再取得 replacement shared header/chunks，最后
写 `slice_index`；这些 allocation 没有失败分支。Hammer 不能把这个前置条件改写成可恢复
错误后继续半迁移。migration只能在 Session owner先停用旧 FIFO I/O 后开始，目标 slice
必须先预留与旧 FIFO 等价的 private capacity、replacement header/chunks并建立新 private
lookup，再一次性发布 slice/owner transition，最后释放旧 private state。任何预留失败都
保持旧 owner、旧 slice和shared header不变。这是仓库 failure-atomicity 规则要求的安全增强。

跨 worker请求走既有 event/handoff path；不得让一个 worker按任意 index取得另一个
worker private slice的 `&mut`，不得新增 TLS container或锁住整个 segment热路径。

### 8.4 MQ、preallocation 与 pressure

segment必须提供：

- MQ allocate、attach-at-offset、MQ-only segment discovery、MQ offset query；
- FIFO header、chunk size class和RX/TX pair preallocation；
- allocated size、new free bytes、cached bytes、available bytes、free-list bytes；
- active/free FIFO count和per-size free chunk count；
- usage percentage和 `NoPressure`/`LowPressure`/`HighPressure`/`NoMemory`；
- preallocated/will-delete/memory-limit/custom-use flags；
- low/high watermarks；Hammer 在 segment-capacity exhaustion 时设置 memory-limit flag，
  该 flag保持sticky，只有usage降到high watermark以下时由status calculation自动清除。

最后一条是 Hammer 补全的明确策略：vendored VPP 已实现 sticky/clear status calculation，
但当前树没有任何 `MEM_LIMIT` 置位调用，且对应 `NoMemory` 断言被注释。实现不得把该
Hammer transition误报成上游已覆盖行为。

flags、low/high watermarks和memory-limit hysteresis属于每个进程的 private
`SvmFifoSegment` state；usage 所读取的 reserved/cached/free counters位于 shared header。
attach 不能把一个进程的 pressure flag当作跨进程发布状态。

segment还提供aligned reserved-byte allocation；该分配和MQ storage都来自monotonic
allocator并计入reserved bytes。attach用
FIFO-segment-header-relative offset重建 private `SvmMsgQ`；eventfds由外层 fd transfer
传递，不存共享数值。

## 9. 同步、层隔离与错误

| State | owner / synchronization | 禁止项 |
| --- | --- | --- |
| Region metadata/heaps | ADR-0012 region lock + active PVT/Data heap | offset region、different-VA attach、fallback Main Heap |
| generic SSVM heap | shared locked `MemHeap`，same-VA participants | FIFO segment client解引用该heap、重建heap |
| fixed queue | shared robust mutex/condvar；role-specific shared fd slots；RAII guard | 把一个进程的raw fd当成peer identity、Data Worker blocking wait、manual unlock泄漏 |
| MQ no-eventfd | shared robust mutex/condvar | 第二套payload publication state |
| MQ eventfd | process-local producer spinlock + private fd | 把fd或lock写入shared ABI |
| FIFO positions | SPSC release/acquire，private owner `&mut Fifo` | `unsafe impl Sync`、`&self` mutation、跨worker borrow |
| segment byte index | CAS range grant | 当成object initialization publication |
| segment freelists | per-slice protocol；tag/reclamation proof required | 仅凭16-bit tag宣称Rust安全 |

### 9.1 有意差异

| ID | Hammer decision | VPP 差异与理由 | 验证 |
| --- | --- | --- | --- |
| I1 | zero `ssvm_va` 对 SHM/MEMFD 都表示 non-fixed attach | 修正 V18 的 backend 矛盾；nonzero 才允许 fixed mapping | 两个 backend 独立 exec different-VA attach |
| I2 | FIFO map-only path 不创建或发布 generic heap | VPP 留有未使用且与 direct allocator range 重叠的 heap和删除 TODO；Hammer 不暴露失效 raw pointer | layout probe；FIFO attach无法取得heap |
| I3 | Session/thread/segment-manager identities不进入 infra `Fifo` | VPP private FIFO 与 Session 耦合；Hammer crate dependency direction要求Session持有 | compile/API boundary与Session lookup integration |
| I4 | FIFO mutation要求 owner `&mut`；safe no-copy只能通过初始化写入借用commit | C pointer API不能证明 Rust aliasing或initialized-byte invariant | compile-fail ownership；error位置不变 |
| I5 | slice migration在所有可失败预留后一次发布 | VPP 对 replacement allocation没有失败分支；Hammer control-plane error必须failure-atomic | private/header/chunk exhaustion injection |
| I6 | segment-capacity exhaustion显式设置memory-limit flag | VPP 有sticky/clear reader但当前树没有writer；Hammer闭合 `NoMemory` transition | pressure hysteresis test |
| I7 | robust owner death后验证queue state，否则retire identity | VPP 无条件标记consistent；Hammer不能在未知共享状态上继续 | owner-death subprocess与corruption cases |
| I8 | chunk freelist只有在48-bit+16-bit方案得到lifetime/tag-wrap证明后才照搬，否则选已证明的reclamation protocol | VPP tag不是Rust memory-safety proof | model checking、multi-process stress、tag-wrap strategy |
| I9 | 不移植 generic SSVM recursive header spinlock | vendored tree对该lock无调用者；真实同步由payload owner提供 | scoped symbol/call-site audit与owner tests |

Expected full/empty、capacity exhaustion、invalid offset/layout、timeout、owner death和
OS mapping错误属于 owner-local typed `Result`。Corrupted shared links、double free、
wrong ring-head release、违反 owner role和不可能的 internal state是 assertion/abort
边界，不能恢复后继续访问未知内存。

Fallible operations必须 failure-atomic：`add2` 不部分入队；FIFO write在完整 copy或
destination-write reservation commit前不推进tail；segment FIFO allocation失败回收本次取得的
header/chunks；migration失败保留old owner；ready之前失败不发布payload。通知发生在
state commit之后，通知I/O失败必须表达“操作已提交”，避免调用方重复提交。

`hammer-infra` 只拥有 generic mapping、allocation和queue/FIFO state。Session拥有
IO/CTRL event meanings、Session identities、scheduling和worker handoff。TCP拥有
sequence/ACK/recovery/timers。应用拥有业务协议。依赖方向不可反转。

## 10. API 与删除批准清单

下表是后续实现需要的公开或 crate-visible surface。它不表示已批准生产代码。

| ID | 新增或修改 | Owner | 原因 |
| --- | --- | --- | --- |
| A1 | `SsvmPrivate` generic shared `MemHeap`、same-VA default、zero-VA non-fixed attach、FIFO owner-private map-only creation/publication | `svm::ssvm` / `svm::fifo_segment` | 修正当前默认 arbitrary-VA 和无heap布局，并隔离 FIFO offset ABI |
| A2 | `SvmQueueConfig { nels, elsize, consumer_pid }` 及 byte add/add2/sub/wait API | `svm::queue` | VPP queue element size是runtime ABI，不是Rust泛型 |
| A3 | `SvmMsgQRingConfig { nitems, elsize }`、inline ring layout、`SvmMsgQProducerGuard`、alloc/write/add/sub/sub-batch/free/cleanup/wait/eventfd API | `svm::msg_queue` | 删除错误caller-owned data contract并闭合MP lock scope |
| A4 | `Fifo` owner-mutating full API：role-specific capacity/empty/full、fill/provision、arbitrary segments、max chunk、OOO newest/reset、notification/subscribers、clone | `svm::fifo` | 当前只覆盖部分VPP behavior |
| A5 | `FifoDirection`、Main-owned segment lifecycle/query、directional alloc/free/duplicate/migrate、FIFO/chunk offset operations、MQ、preallocate、reserved allocation、accounting和pressure API | `svm::fifo_segment` | 当前segment lifecycle与owner surface不完整；细项见3.1 |
| A6 | `FifoSegmentFlags`、`FifoSegmentMemoryStatus` | `svm::fifo_segment` | flags/watermarks/pressure是segment domain state |
| A7 | `march_fn!` 及两个具体 FIFO copy registrations | `hammer-infra::simd` / `svm::fifo` | 对应唯一两个 `CLIB_MARCH_FN` uses |
| A8 | owner-local typed SVM errors和post-commit notification errors | 各SVM owner | 保持恢复动作、source chain和failure atomicity |

删除或迁移：

| 当前 surface | 处理 | 完成证明 |
| --- | --- | --- |
| `SvmRegionHeap`、`SvmHashMap`、offset `SvmRegion` layout/tests | 按ADR-0012删除并替换 | old symbols归零；same-VA region behavior tests通过 |
| `SsvmPrivate` 默认 `ssvm_va = 0`、无shared heap、紧邻header payload layout | 改成generic SSVM contract；FIFO segment显式选择map-only例外 | generic same-VA与SHM/MEMFD FIFO different-VA测试分别通过 |
| `SvmMsgQRingConfig<'data>::data` 和 MQ caller-owned ring pointers | 删除 | inline layout/attach/roundtrip通过；`'data` API归零 |
| `unsafe impl Sync for Fifo`、`Read/Write/BufRead for &Fifo`、生产修改的`&self`方法 | 删除或改真实`&mut` owner operation | compile-time ownership和SPSC behavior通过 |
| infra FIFO 的 Session union/indices和segment-manager identities | 上移到Session owner | infra不依赖或命名Session；integration仍能定位FIFO |
| FIFO `segments() -> (&[u8], &[u8])` | 替换为caller-capacity arbitrary segment API | 三个以上chunks零拷贝读取通过 |
| `Vec<Vec<Option<Fifo>>>` private slices、segment-wide `Sync` | 改owner-local Pool/slice直接借用 | worker不能借外slice；migration tests通过 |
| plain-offset chunk freelist | 替换为获批且证明过的reclamation protocol | model/stress/ABA-wrap proof通过 |
| `MultiRingMsgQueue`、mode tags、claim/reservation/cancel、runtime callers | 完整迁移到`SvmMsgQ`后删除 | old symbols/imports/layout tags归零；Session tests通过 |

不新增 `SvmRegionHeap`、`OffsetHeap`、`MessageReservation`、generic queue backend、
thread-local worker container或 closure-mediated state access。

## 11. 验证矩阵

实现、review和formatting都完成后，才运行最终pre-commit gate。必需行为包括：

| Area | Test | 通过条件 |
| --- | --- | --- |
| generic SSVM | independent exec creator/attacher；occupied target VA | actual `ssvm_va`非零；same-VA attach；heap pointer可用；占用时typed失败且不覆盖mapping |
| FIFO segment mapping | SHM/MEMFD independent exec at different VAs | zero VA走non-fixed map；header/chunk/MQ offset解析一致；无generic heap pointer；ready前不可用 |
| fixed queue | capacities 1/3/5、add2、sub2、wait/timedwait、eventfd、owner death | 顺序无丢失；add2零部分提交；predicate重查；每个role只使用自己安装的fd；source chain保留 |
| MQ layout | 复现`session_test_mq_basic`的16 descriptors与两个8-slot rings | inline data、first-fit ring选择、跨ring descriptor顺序、ordered free、最终占用为0 |
| MQ concurrency | multiple producers和single consumer；condvar/eventfd各一套 | alloc/write/add在同一guard；无重复descriptor；full transition通知正确 |
| FIFO basic/chunks | non-power-of-two size、3+ chunks、fill/provision、grow/shrink、wrap | bytes一致；arbitrary segment count；max read/write chunk正确；失败不改变visible positions |
| FIFO OOO | VPP fifo1/2/3/5/6/7/large scenarios | wrapping compare、adjacent/overlap merge、newest/reset、hole fill后才推进tail |
| FIFO notifications | immediate/full/empty、threshold、7 subscribers | `want`/`has` transition和suppression与VPP一致 |
| FIFO ownership | compile-fail/API checks | 无`Sync`共享修改；无`Read/Write for &Fifo`；owner `&mut`不跨`.await` |
| segment allocation | exhaust/reuse、preallocate、all size classes、direction | RX/TX都计入shared active count；只有RX进入private active list；client free不回收server storage |
| segment migration | destination private/header/chunk exhaustion与成功handoff | 每种失败old owner/shared header不变；成功后只有new owner active；无提前回收 |
| segment pressure | low/high/no-memory和sticky limit hysteresis | watermarks边界精确；segment-capacity exhaustion按Hammer策略进入NoMemory；usage低于high后自动清除sticky flag |
| freelist safety | model checking + multi-process stress + tag wrap strategy | 无ABA use-after-free、lost node、duplicate allocation |
| multiarch | forced baseline与所有host-supported variants | 两个copy入口在0/1、alignment、vector边界和chunk边界结果一致；unsupported永不执行 |
| Session migration | runtime/service/app integration | IO/CTRL ring选择、fd handoff、close/reset和backpressure不再引用legacy queue |

测试命令在完整实施候选上至少包括：

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test -p hammer-infra
cargo test -p hammer-runtime
cargo test -p hammer-service
cargo test -p hammer-app
cargo test --workspace
git diff --check
```

不得用 source-text assertions代替compile/runtime proof。跨进程测试使用显式
socket/pipe phase handshake，不用sleep猜执行顺序。本地不运行TUN/TCP lab；该部分
仍由CI workflow负责。

## 12. vlibapi / vlibmemory 边界

未来 Binary API shared-memory path不能把三个协议混成一个：

- `SvmQueue` 传输runtime-sized queue elements；VPP API queue元素虽常是pointer，
  Hammer跨地址协议必须使用所属API owner验证过的identity；
- Binary API allocation rings决定message size class和独立slot回收；它们不是
  `SvmMsgQ` ordered data rings；
- Session `SvmMsgQ` 传输Session event descriptors；event schema和调度留在Session；
- API region allocation使用ADR-0012 fixed-VA PVT/Data Heap，不再承诺different-VA
  region；
- FIFO segment仍是offset payload，可different-VA attach；这个例外不改变API region
  contract。

完成 queue或MQ单元测试不足以声称vlibapi/vlibmemory已迁移。还需验证allocation
ring的乱序释放、sender failure ownership、receiver cleanup、client detach、in-flight
message和SCM_RIGHTS lifecycle。

## 13. 撤销记录与审查结论

2026-09-12 一致性审查撤销并删除了旧 ADR-0011 的以下规范：

- shared region一律relative offset；
- `SvmRegion` different-VA attach；
- `SvmRegion` over `SsvmPrivate`；
- `SvmRegionHeap`/`SvmHashMap`；
- monotonic independent subregion identity；
- `SvmRegion::ssvm()`；
- 把 FIFO segment 的 `ssvm_va = 0` 推广成所有 SSVM 默认；
- MQ caller-owned ring data和reservation/cancel lifecycle。

Region replacement只在ADR-0012中定义：PVT heap、optional Data Heap、active heap、
same-VA raw-pointer ABI、root bitmap和subregion pool index。

审查结论：**Needs decisions / not yet aligned in code**。`SvmQueue` 和 `SvmMsgQ` 已有
部分基础实现；generic SSVM、MQ storage contract、`Fifo` ownership/API、FIFO segment
lifecycle/pressure/migration和legacy Session queue仍有明确缺口。实施前必须批准
A1-A8，并在8.1节的两个freelist方案中选定一个可证明方案。完成第10节迁移并通过
第11节验证前，不得标记SVM FIFO/FIFO segment或整体SVM为VPP-aligned。
