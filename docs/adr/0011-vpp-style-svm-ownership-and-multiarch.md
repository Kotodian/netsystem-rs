# ADR-0011：SVM 所有权、消息队列、同步与普通函数多架构选择

- 日期：2026-09-11
- 状态：Proposed；本轮仅修改 ADR/CONTEXT。2026-09-12 更新：SVM owner 已移入
  `crates/hammer-infra/src/svm/`，见 §12.1 模块落点；A11 `SvmRegionHeap` 已按 §12.4
  落地并带测试（`crates/hammer-infra/tests/svm_region_heap.rs`，见 §12.11）。
  Linux 优先，macOS/iOS 不在本轮设计验证范围；A12/A13 及其余生产代码尚未迁移。
- 请求：完整核对 VPP `src/svm`，删除 Hammer `MultiRingQueue`，Rust 支持对应的 `CLIB_MARCH_FN`，更新 ADR 和 CONTEXT。
- Hammer 基线：`418aa0953d929f4175370ce1873066f4ff193f38`。
- VPP 参考：官方 FDio/vpp `629fe2764bd997189fedd2d98cbe8dc9189c1ec3`。

## 1. 证据边界与批准状态

用户指定的 `/Users/linqiankai/rust/src/github.com/Kotodian/hammer-ios-rs/third_party/vpp/src/svm`
在本环境不存在；工作区初始也没有 `third_party/vpp`。因此从官方仓库取得
上述提交，置于仓库已忽略的 `third_party/vpp/`，作为可复核参考。不能声称
它与用户机器上的版本相同。以下 VPP 路径均相对此目录；官方永久链接为
[本次 SVM 参考源码](https://github.com/FDio/vpp/tree/629fe2764bd997189fedd2d98cbe8dc9189c1ec3/src/svm)。

事实类别：`H` 是当前 Hammer 代码，`V` 是实际读取的 VPP 源码，`R` 是用户
要求或仓库约束，`D` 是本 ADR 提议，不能把 D 当成已经实现或获批的事实。

用户本轮明确要求修改 ADR，不实施生产代码。本文具体化 Linux 优先的替代
设计及删除清单；“删除”指后续实施目标，不表示旧文件已经删除。第 8 节是
拟议类型和方法，不是生产 API。后续实施仍按 AGENTS.md 的接口批准规则执行，
本轮文档工作不以再次批准为前提。

## 2. 完整目录范围

“SVM”不是一个 mmap 类型，也不是一条消息队列。目录中的层次必须分清：

| VPP 文件 | 类型、函数族及职责 | Hammer 目标归属 |
| --- | --- | --- |
| `ssvm.h/c` | `ssvm_private_t`、`ssvm_shared_header_t`；server/client init、delete，SHM/MEMFD/PRIVATE backend，ready 和映射生命周期 | infra `svm::segment`：映射与 backing resource |
| `svm_common.h`、`svm.h/c` | `svm_region_t`、`svm_main_region_t`、`svm_subregion_t`、`svm_map_region_args_t`；根/子 region、find-or-create、成员扫描、map/unmap、metadata/data heap、user root | infra `svm::region`：region authority |
| `fifo_types.h` | `svm_fifo_shared_t`、`svm_fifo_t`、chunk、OOO、signals；共享/私有 FIFO 和 segment slice 状态 | infra `svm::fifo`、`svm::fifo_segment` |
| `fifo_segment.h/c` | `fifo_segment_t`、`fifo_segment_main_t`、shared/private slice；create/attach/delete、FIFO/chunk 分配回收、迁移、MQ 存储、预分配及压力统计 | infra FIFO segment；Session 保留应用策略 |
| `svm_fifo.h/c` | enqueue/offset/nocopy/segments、dequeue/peek/drop、chunk provision、OOO、通知、clone、diagnostics；两个多架构复制函数 | infra 字节 FIFO |
| `queue.h/c` | `svm_queue_t`、wait 条件；固定尺寸元素的 add/add2/sub/batch-related raw operations、锁、通知和等待 | infra `svm::queue`；与 MQ data ring 分离 |
| `message_queue.h/c` | `svm_msg_q_t`、shared queue/ring、8-byte message descriptor、config；alloc/attach/cleanup、reserve/add/sub/free、wait/eventfd | infra `svm::msg_queue` |
| `svmdb.h/c` | 独立 `svmdb` 库：client、shared header、string/vector namespaces、values、notification registrations；map/unmap、get/set/unset、serialize/unserialize | 需单独交付的 SVM database，不能塞进 Session |
| `svmtool.c`、`svmdbtool.c` | region/database 诊断命令 | owner diagnostics；CLI glue 不拥有共享状态 |
| `svm_test.c`、`persist.c` | region 测试、持久共享对象示例；`persist.c` 明确标记 test/demo | 行为参考，不能注册为 daemon 生产业务 |
| `dir.dox`、`CMakeLists.txt` | 目录说明、库/工具构建和 multiarch 来源 | 文档与 Cargo 编译组织 |

本设计的可实施核心是六个 SVM owner 与多架构函数支持。Database/工具也列入
完整性账本；它们是独立交付范围，尚未设计新的公开接口，不能在核心完成后
宣称整个目录的功能已经全部移植。`CMakeLists.txt` 仅把 `svm_fifo.c` 放入
`MULTIARCH_SOURCES`；不应把整个 SVM 库全部做多架构注册。

## 3. 证据账本与当前差距

| ID | 来源及符号 | 已核实事实 | 设计影响 |
| --- | --- | --- | --- |
| V1 | `ssvm.h:42`、`ssvm.c` 的 `ssvm_server_init_*` / `ssvm_client_init_*` / `ssvm_delete_*` | 三种 backend；共享 header 与进程私有映射信息不同；private client attach 不成立 | D1：映射 owner 与 region 分开 |
| V2 | `svm_common.h` 的 `svm_region_t` / `svm_main_region_t`；`svm.c:434`、`:879`、`:1010` | region 初始化共享 mutex/condvar，维护两个 heap、成员和命名子区；退出有 server/client 区分 | D2：现有 mmap owner 不能冒充 region |
| V3 | `queue.c:21`、`:87`、`:112`；`vlibmemory/memory_client.c:179` | 固定元素队列由 Binary API 客户端创建；共享 robust mutex/condvar；eventfd 可替换通知 | D3：独立 SvmQueue，不复用 Session 消息模型 |
| V4 | `message_queue.h:19`、`:32`、`:39`、`:63`；`message_queue.c:134` | shared queue/rings 与私有 attach 状态分离；evtfd 和多生产者 spinlock 在进程私有对象 | D4：一份共享布局，删除 MP/SP 两种存储语义 |
| V5 | `message_queue.c:210`、`:254`、`:282`、`:334`、`:390`、`:411` | ring tail 分配、ring head 按序释放、cursize 计数；descriptor 与 payload slot 各自占用；按配置顺序选择足够大的非满 ring | D4：保留多 data ring，删除任意空闲槽栈 |
| V6 | `message_queue.h:361`、`:380`；`message_queue.c:177`、`:489`、`:531`、`:589`；`application.c::app_rx_mqs_alloc`；`segment_manager.c::segment_manager_alloc_queue` | app RX MQ 创建 eventfd；segment manager 按配置选择；无 eventfd 走共享 mutex，有 eventfd 走本地 spinlock；通知条件分别覆盖空/满转换 | D5：按真实创建/调用链选择具体 Rust 同步能力 |
| V7 | `vnet/session/application_worker.c:970`；`session_node.c:1974`；`vcl/vppcom.c` 的 `svm_msg_q_sub_raw_batch` 调用 | Session 选择 IO/CTRL ring，消费事件后释放 slot；VCL 可批量取 descriptor | D6：事件类型和调度仍属 Session |
| V8 | `fifo_types.h`；`fifo_segment.c:15`、`:190`、`:874`、`:949`、`:1015`、`:1042` | 全局字节区间由 CAS 分配；chunk free list 使用带 tag 的 offset；FIFO header list 与 private slice 有不同同步；迁移要求停用旧 FIFO | D7：不能把所有 free list 都当成 MQ ring 或都加同一把锁 |
| V9 | `svm_fifo.h:74`、`:87`；`svm_fifo.c:833`、`:892`、`:1092` | SPSC 角色所有权，外方 index acquire，本方提交 release；OOO 与 chunk lookup 为私有状态 | D8：角色借用、chunk storage、发布顺序 |
| V10 | `svm_fifo.c:17`、`:48`、`:81`；`vppinfra/cpu.h:554`；`svm/CMakeLists.txt` | 两个复制函数各有 baseline/ISA 实现，constructor 按 CPU priority 选择函数指针；普通入口调用 selected function | D9：普通函数选择机制，非 Graph Node 注册 |
| V11 | `svmdb.c:59`、`:127`、`:182`、`:417`；`persist.c:6`；`svm/CMakeLists.txt` | database 单独成库，锁 region、发布数据库 root、支持通知及字符串持久化；persist 是示例 | 不得遗漏，也不得冒充当前已有功能 |
| H1 | `crates/hammer-infra/src/multi_ring_msg_queue.rs` 的 `MultiRingMsgQueue<P>`、`QueueHeader`、`RingHeader`、`ProducerGuard`、`RingMsg` | MP 使用 shared spin word 与 free-slot stack；SP 使用 claim、独立 cursor；power-of-two descriptor 容量；两种回收行为 | 删除整套旧布局与模式标签 |
| H2 | 同文件 `MsgSlot` / `ProducerReservation::publish` / `sub` | MP slot 不带 guard lifetime；publish 借用 `&mut self` 可被再次调用；MP consumer 仅接收 `&self` | 替代 API 必须约束事务生命周期和唯一消费者，不能保留旧安全漏洞 |
| H3 | `svm/region.rs` 的 `SvmRegionInner`；`segment.rs` 的 `Segment` | 映射内存由创建方 Talc 分配；attached mapping 无 allocator；`Segment::local` 仍创建映射，只隐藏可共享属性 | 不是 VPP region，也没有真正 private backend 选择 |
| H4 | `fifo.rs` 的 `Fifo`、`FifoHeader`、`OooBookkeeping`、`peek_segments` | FIFO 自带 chunk 布局、私有 OOO；没有 FIFO segment slices；共享引用能进入修改路径；存在 closure-mediated borrow | 迁移到 FIFO segment owner，删除闭包访问，收紧借用 |
| H5 | runtime `app/session_msg_queue.rs`、service `session/{application,runtime,control}.rs`、app `attach.rs` | SessionMsgQueue 包装旧 infra queue；生产者模式贯穿 app/session bootstrap；pipe/AtomicBool 通知在 runtime | 必须迁移整个调用闭包，不能只改 infra 名称 |
| H6 | infra `simd.rs`、`checksum.rs`；component macros `lib.rs:605` 的 `node_function` | 已有 CPU feature detection、专用 ISA 复制及 Graph Node variants；没有通用普通函数选择宏 | 复用检测/复制，不能让 infra 依赖 graph/runtime |
| H7 | infra `sync.rs`；runtime `sync.rs:15` | 当前 SpinLock 实现在 infra，runtime 重导出；与 AGENTS 对泛型锁归属的文字存在既有差距 | 复用已有实现，本次不复制锁、不偷偷重排 crate 依赖 |
| H8 | service `binary_api.rs`；ipc `binary_api.rs` | 当前 Binary API 是 Unix socket/protobuf 路径；CONTEXT 中的完整 API shared-memory 术语不是实现证据 | SvmQueue 可独立验证，不凭空宣称已接入 Binary API |

## 4. 三方语义比较与决策

| 维度 | 当前 Hammer | 提议 Hammer | VPP 依据 |
| --- | --- | --- | --- |
| 映射 / region | 两种名字围绕同一个 mmap+Talc owner | D1/D2：SvmSegment 管资源；SvmRegion 管 metadata/data allocation、成员与 root；命名根另有 SvmRegionMain | V1/V2 |
| MQ 数据结构 | MP 空闲槽栈，SP cursor，各自 mode tag | D4：单一 descriptor FIFO + 可变数量的有序 data rings；正整数容量不强制二次幂 | V4/V5 |
| FIFO 分配 | 每个 FIFO 预铺固定 chunk 存储 | D7：segment slices、分级 chunk 复用、按需 provision、压力与回收统计 | V8 |
| 发布 | 部分 index 已 release/acquire，但共享引用掩盖写者权限 | D5/D8：角色所有权 + 显式发布/回收边界；OOO 只属 producer | V6/V9 |
| 等待 | runtime pipe 与共享进程内 AtomicBool 通知 | D5：infra 队列等待后端；runtime 只集成就绪事件与调度 | V6/V7 |
| ISA | Graph Node 专用；部分复制固定 ISA | D9：普通函数按支持的 CPU features 选定实现，复制入口实际使用选择结果 | V10 |
| ABI | Hammer offsets + 自定义 queue layouts | D10：继续相对 offset，版本化且拒绝旧布局；不承诺 C 二进制互通 | V1/V4/V8 + R |
| 生命周期 | 本地 Arc 管映射引用，不能代表远端进程退出 | D11：peer detach/worker handoff 后才回收；本地 Drop 不销毁仍被外部使用的共享对象 | V1/V2/V7/V8 |

### D1/D2：映射、region 与分配 authority

将现有 `SvmRegion` 的 fd/mmap 部分迁入 `SvmSegment`，合并现有 `Segment`
重复资源路径。`SvmSegment` 拥有 mapping、backing fd、size、backend 和本地
生命周期；不把 region allocator 自动绑定到每个映射。

真正的 `SvmRegion` 拥有 region metadata、data allocation authority、成员与
published root；`SvmRegionMain` 管命名根和子区。初始化在对象最终共享地址
完成 mutex/condvar 与 metadata 设置，最后 release 发布 ready/version；attach
先 acquire 再验证版本、偏移、容量与对齐。不能在 attached mapping 重建 Talc
并覆盖创建方 heap。跨进程通用 region 分配需要共享的 offset allocator 元数据，
不能使用带进程私有裸指针的 Talc 状态伪装为共享 heap。

SSVM 的 SHM、MEMFD、PRIVATE backend 是真实资源模式。Rust private 模式不
导出 fd；共享 region 与 Buffer Arena 继续是 Main Heap 的例外。普通 Vec/Pool
控制元数据仍遵循现有全局 Main Heap。VPP 的 push/pop 当前 heap 不移植为
thread-local heap 切换，改为明确持有 allocator owner 并直接调用。

### D3/D4：删除旧队列，建立两个不同的 SVM 队列

`SvmQueue<T>` 交换固定尺寸元素；`SvmMsgQueue` 交换 8-byte `(ring_index,
element_index)` descriptor，payload 保留在 data rings。MQ 的共享 descriptor
header 与 SvmQueue 不必有相同 Rust 类型：此 VPP 版本也把两者分开定义。

MQ ring 分配推进 tail，消费完成推进该 ring 的 head；两者 modulo `nitems`。
每个 ring 的 occupancy 包含已分配、尚未回收的 slot，不能由 descriptor queue
长度推导。跨 ring 保持 descriptor 入队顺序，每个 ring 按序回收；consumer 取出
descriptor 并不立即释放 payload。保留多个 ring，删除的是 Hammer 自定义
MultiRing 结构及任意 slot 回收策略。

共享布局不再含 `mode` / `claim` / `q_mask` / `free_top` / `free_slots`。
单生产者与多生产者是访问权限与锁调用条件，不是两种共享对象。Linux 设计统一以共享 mutex 串行化生产与队列状态修改，不保留依赖 eventfd
切换的私有 producer spinlock。消费者持有同一把队列锁直至释放 slot，保证每个 ring 按序
释放；所有 attach 使用同一份共享锁对象。这是相对 VPP eventfd MQ 路径的
明确差异，不能把私有 spinlock 误当成跨进程互斥。

非阻塞 reserve 先检查 descriptor capacity 与目标 ring capacity，再返回借用
producer owner 的 reservation。它独占尚未提交的 slot，提供直接 `&mut T`；
`commit(self)` 消耗 reservation，只允许一次发布。取消未发布 reservation 时
恢复 ring tail/accounting，不产生通知或 descriptor。消费消息的借用覆盖 payload
使用到按序 free，批量 API 必须保持每个 ring 的释放顺序。guard/reservation 是
锁定/事务职责，不是缓存别处指针的 observation wrapper。

旧 `MsgSlot` 没有 guard lifetime，能够越过锁与队列生命周期；旧 SP
`publish(&mut self)` 也未禁止重复发布。这两项是需要消除的 Rust API 风险，
不是应该忠实移植的语义。

### D5：同步原语与等待

| 状态 | Owner / readers / writers | 原语及发布边界 | 回收与失败 |
| --- | --- | --- | --- |
| region metadata / allocator | 加入同一 region 的控制线程和进程 | Rust process-shared mutex 的 RAII guard；VPP 参考为 PTHREAD_PROCESS_SHARED，具体平台字段留在同步实现内部 | 最后成员退出且无借用/等待者才销毁；初始化失败不能发布 ready |
| Linux MQ 状态 | 同一映射的各生产者及消费者 | posix_sync 的 process-shared robust mutex 与 shared condvar；guard 保护 descriptor、payload 使用与 ring accounting | full 保持可见位置；owner death 关闭旧队列，生命周期 owner 重建，见 8.4 |
| Linux MQ 消费事务 | 同一队列的各消费者 | 与生产事务使用同一把 robust mutex；借用 payload 期间保留 crate guard | 只在短读取/解码中持有，释放消息后再调用 handler；不另加消费者锁 |
| descriptor / payload | producer 发布；consumer 读取并释放 | Linux MQ 由 state mutex unlock/lock 发布和回收；ring occupancy 包含消费中 slot，不能以 descriptor 出队代替回收 | 不额外叠加原子发布协议；满时不覆盖，通知不替代互斥 |
| FIFO head/tail | 一 producer，一 consumer | 写 bytes/chunk links → tail Release → consumer Acquire；读完成 → head Release → producer Acquire | OOO 由 producer 本地维护，未填洞不能推进可读 tail |
| segment byte allocator | 多 slice 申请不重叠区间 | CAS 仅授予字节区间所有权，不发布初始化后的对象 | 初始化后另行发布；容量耗尽保持 allocator 有效 |
| segment chunk free lists | chunk 交还者与分配者 | 对照 VPP offset+tag CAS；Rust 必须证明节点读取、tag wrap、在途读者及重用安全 | 不能把 16-bit tag 的“概率低”当 Rust 安全证明；未证明前该 slice 不可交付 |
| FIFO private slice / OOO | 当前 Data Worker | 启动前固定 slice 数；执行 worker 直接 `&mut` 借用自己的 entry | 外 worker 通过现有事件/handoff 请求；无 TLS、无全局新锁 |
| graph/file/worker 可见登记 | main/control 与 Data Workers | 现有 WorkerBarrier acknowledgement 下修改 | Barrier 不包含外部 app 进程；不能以它证明 SVM peer 已退出 |

Linux MQ 的 descriptor/head/tail/count 与 ring accounting 都受同一个 state
mutex 保护，payload 在生产事务中写入、commit 后通过 unlock/lock 发布。
消费事务释放 slot 后，下一次获得 state mutex 的生产者才能重用。无需为了
模仿 VPP 的 relaxed cursize 再加一套原子计数协议。

Condvar 与 state mutex 配对；检查 predicate、释放锁并睡眠是同一等待协议。
同一个 condvar 服务空/满等待者时采用 notify_all，醒来循环重查。eventfd 为
本地 OwnedFd，经现有 fd 传递路径共享内核对象，不能将 fd 数值写入共享 ABI
供另一个进程直接使用。启用 eventfd 不改变本轮 Linux MQ 的共享 mutex；这与
VPP eventfd MQ 的私有 spinlock 路径不同，8.4 节保留源行为对照。

等待容量和获取锁是两件事：reserve/dequeue 使用 lock，容量不足立即返回，
但不承诺锁竞争时整个调用非阻塞。显式 wait 仅供控制线程使用；Data Worker
不调用 condvar wait/poll，也不能持 guard 进入调度、barrier、I/O 或 await。
若后续要求 Data Worker 在外部进程停顿时仍具备有界延迟，必须另行解决这项
共享锁约束，不能把 lock 接口标记为无锁或 wait-free。

Linux 队列改选 posix-sync 0.1.0：固定使用 Robust、MutexSharing::Shared 和
CondvarSharing::Shared。rustix-futex-sync 不满足 owner-death 要求，不再作为
选定依赖。正常加锁、条件等待重新加锁都处理 owner death。队列内容不能证明
一致时，不调用 consistent；旧队列进入不可恢复状态，由生命周期 owner 建立
新队列并重新交换身份，8.4 节明确全过程。SVM region 初始化在 VPP svm.c 中
只设 process-shared，不把 queue.c 的 robust 设置误称为所有 VPP region 锁的
属性；Rust 共享 allocator 同样采用 robust，并在异常时隔离所属 region。

不新增 ProcessMutex、ProcessCondvar、同步 trait、GAT 或后端类型参数。
具体锁与 guard 直接来自外部 crate；内容 T 不改变同步协议。

### D6/D7/D8：Session、FIFO segment 与字节操作

Session 仍负责 IO/CTRL ring 的含义、消息编码、目标 Session 和节点调度。
SVM 只知道 ring index、slot capacity、FIFO bytes、offset、slice 及同步。
不向 infra 下移 TCP、TLS、HTTP、SessionEvt 或应用 attach 策略。

`SvmFifoSegment` 拥有 chunk/header 存储、共享 slice freelists、私有 FIFO
对象、MQ storage 及预分配/压力统计。普通 RX/TX 分配、client attach、client
detach 和最终 FIFO free 是不同操作；attached client 不能回收 server 的共享
header。迁移先由 Session 停止旧 owner 的 I/O，再完成目标 slice 的资源准备
和所有权交接，最后回收旧私有状态。失败时旧 owner 仍有效，禁止先改 slice
index 后返回资源不足。

FIFO 私有状态借用必须收紧，删除 `peek_segments(..., FnOnce)`；直接返回
与 owner 同寿命的 slice/iterator。不能以 `Read for &Fifo`、`Write for &Fifo`
或无条件 `Sync` 暴露多写者/多消费者。使用真实 owner 的 `&mut`；跨进程映射
建立 producer/consumer 权限属于 attach 契约。新借用设计需要连同 SDK async
方法检查，不能跨 `.await` 持有同步 guard 或 worker-local 协议借用。

OOO 使用现有 infra Pool/RbTree，存储在 producer owner 内；不引入 observer
atomics/locks。有效 sequence 窗口和 wrapping 比较需要边界测试。Chunk provision
可能产生任意数量的片段，不能把当前“两个 slice”接口宣称为完整 chunk chain
API。保留写 reservation 的事务语义：目标 commit 先于源 consume，错误时两个
FIFO 可见位置不动。TX retention 与 ACK 回收仍属于 Session，TCP 不复制恢复载荷。

### D9：Rust 普通函数的 CLIB_MARCH_FN 对应能力

已核实完整 `src/svm` 的调用定义只有：

1. `svm_fifo.c:17`：`svm_fifo_copy_to_chunk`，把 source bytes 跨 chunk 写入 FIFO。
2. `svm_fifo.c:48`：`svm_fifo_copy_from_chunk`，从 FIFO chunk chain 复制到 destination。

普通入口在同文件 `:84` / `:92`，通过 `CLIB_MARCH_FN_SELECT` 调用。
`vppinfra/cpu.h:554` 定义 constructor 选择；baseline priority 为 0，受支持
架构提供更高 priority。不要把 `CLIB_MARCH_FN_REGISTRATION`、
`VLIB_NODE_FN` 和这套普通函数 selected pointer 机制混为一谈。

全 `src` 范围的同名宏定义使用还包括 `plugins/npol/npol_match.c:690`、
`plugins/memif/node.c:1052`、`plugins/memif/device.c:399`；这些是范围核对，
本次不扩张到对应插件。检查命令：

```bash
rg -n 'CLIB_MARCH_FN|CLIB_MARCH_FN_SELECT' third_party/vpp/src/svm
rg -n 'CLIB_MARCH_FN\s*\(' third_party/vpp/src --glob '*.[ch]'
```

提议在 infra `simd` 中新增 `march_fn!` 声明宏。它生成一个普通具体签名入口、
私有 baseline/ISA 实现及一个相同签名的 selected 函数指针。避免新增 proc-macro
依赖，维持 infra external-only 依赖规则。已有 `node_function` 专属 Graph Node，
不能直接用于 FIFO，也不向普通函数注入 DataPlaneMain/Frame。

宏输入列出函数体、baseline/ISA 的 feature 集和 priority；使用编译期 `cfg` 排除
外架构实现，使用 `#[target_feature]` 编译各实现，运行时检查全部必需 feature，
按 priority 选优。x86 的 AVX2/AVX512 依赖 feature detection 的 OS 状态支持；
AArch64 基线与 NEON 选择分别声明，其他 target 必有 baseline。不能按向量宽度
推断任意 ISA 已受支持，也不能只编译不同名字但全部调用相同 scalar 热路径。

每个普通函数的选择状态属于定义该函数的进程映像，具体函数指针经 OnceLock
发布，启动预热后不变；standalone SDK 使用时也必须可安全初始化。CPU 检测只
在初始化发生，payload 热路径不分配、不注册节点、不反复检测 CPU、不跨 DSO
查找全局 registry。目标 CPU 支持集合必须覆盖该进程允许运行的全部 CPU；不能
把某个初始化 core 的额外特性无条件用于异构 core。

两个 FIFO chunk 复制入口必须实际使用该机制，并复用现有平台复制内核或由
专用 target 编译的内核。它只选择机器码，不改变 FIFO ownership、不产生中间
payload Vec、不将协议状态类型擦除。这个具体函数指针调用是用户要求的运行时
ISA 选择，需要在批准时明确为该用途的例外；不由此放开任意 dyn trait dispatch。
不得把 selected 指针写进共享 region。宏不支持的 generic/async/签名形式应在
编译期拒绝，不生成假的通用支持。

### D10/D11：ABI、发布与销毁

对齐的是职责、状态转换和同步语义，保留 Rust offset ABI；VPP SSVM 的固定
virtual address 和共享裸指针不照搬。pthread ABI 是平台相关的，跨平台不承诺
同一共享映射布局。布局变更必须更新 attach/bootstrap 版本且拒绝旧 queue/FIFO，
不能解释旧 mode tag 或留下旧 `MultiRingMsgQueue` alias。

创建过程为 mapping → allocator/header/rings 初始化 → ready 发布 → fd/offset
交付。失败逆序清理，保留 primary error 与 cleanup error。退出顺序为停止
enqueue/调度来源 → 处理在途 descriptor/借用 → detach peer / 完成 owner handoff →
注销 fd readiness → 销毁共享同步对象（仅最终 owner）→ unmap/close。Arc 的本地
引用计数不代表远端消费者已经不再访问，WorkerBarrier 也不提供此证明。

## 5. 层隔离契约

| 层 | 允许调用 | 禁止调用/存储 | 验证 |
| --- | --- | --- | --- |
| SvmSegment / SvmRegion | libc backing/mapping；已有 infra allocator/primitives | runtime/service/plugin 状态、TLS 当前 heap 切换 | infra 独立编译；不同地址 subprocess attach、资源回收 |
| SvmFifoSegment / SvmFifo | region storage、Pool/RbTree、直接 slice/reservation | SessionEvt、TCP header、Graph Node scheduling、跨 worker 私有状态 | FIFO/segment integration；借用编译失败用例；边界失败原子性 |
| SvmQueue / SvmMsgQueue | 通用元素、descriptor、slot、队列等待和通知 | IO/CTRL 含义、应用业务、通用 erased registry | 通用消息 roundtrip、并发/跨进程、取消/唤醒测试 |
| runtime Session MQ 接入 | infra queue、已有 FileMain readiness | 第二套 slot allocator、重复 queue signaling 状态机 | runtime 事件队列集成测试 |
| service Session / hammer-app | owner-defined queue API、消息编码、attach/handoff | 新的共享协议状态锁、业务逻辑进入 SDK | App/Session bootstrap、控制消息与关闭流程 |
| march_fn | 同签名机器码变体、CPU feature detection | Frame、DataPlaneMain、共享函数指针、plugin registry | infra-only compile、选择和两条复制入口行为测试 |

## 6. 错误契约

这些是基础设施或生命周期失败，不是每包分配的控制面错误。

| Owner | 类别 / 事实字段 | 可行动调用者 | 恢复与原子性 |
| --- | --- | --- | --- |
| SvmSegment | backing create/size/map/attach、size/alignment、backend unsupported、原始 io source | region/session bootstrap、SDK attach | 拒绝 attach，关闭本次 fd；已发布映射不改变 |
| SvmRegion | version/root/offset 验证、成员状态、容量、共享同步初始化、成员退出 | RegionMain / 生命周期控制方 | 回滚成员登记；不能假装普通 absence |
| SvmQueue / SvmMsgQueue | queue full、ring full `{ring}`、element size `{requested, capacity}`、timeout、无效 descriptor、signal io error | Session 重试/背压、SDK wait、队列生命周期 owner | reserve 失败不推进；发布后 signal 失败不能报告成“消息未发送”诱发重复提交 |
| SvmFifo / segment | invalid capacity、segment exhausted、reservation too long、非法 offset/attachment、migration allocation | Session/TCP owner、协议链 | chunk 初始化失败不发布，目标 reservation 失败不消费源 |

复用现有 `FifoError` 与 `SegmentAllocationError` 中仍匹配的事实，不保留旧 MQ
`InvalidConfig` 吞掉所有 allocation/OS 原因的路径。迁移时旧 `ModeMismatch`
与 `ProducerClaimed` 随旧布局删除；外部旧 ABI 返回版本/布局错误。
具体新增错误见第 8 节，source 只在真正 seam 翻译一次。

reserve/commit 分离保证消息是否已经发布可由调用顺序确定。signal/wait 返回
单独结果；不能把发布后的 signal failure 转成 `Full`，也不能对已发布消息回滚。
格式字符串不是错误类别。pthread 函数返回的 errno 数值需要直接转换为 source，
不能误读 `last_os_error()`。owner-local cleanup 必须显式释放全部已创建对象，
并在主错误中保留次级 cleanup 事实；不新增通用 boxed error helper。

## 7. 迁移闭包与删除账本

| 请求 | 当前定义/调用者 | 替换或删除 | 证明 |
| --- | --- | --- | --- |
| 删除 MultiRing | infra `multi_ring_msg_queue.rs`、`lib.rs` export | 删除整个文件及 MultiRingMsgQueue/Cfg/Error、ProducerMode、MP/SP tags、RingView/ProducerRing、SlotFree | 编译全部调用者；旧定义/import 搜索为零 |
| 同一共享 MQ 模型 | runtime `app/{mod,session_msg_queue,layout,session}.rs` | SessionMsgQueue 接入新的 descriptor/ring API；删除存储 mode 泛型，移交通用 fd signaling | runtime 消息顺序/背压/slot 回收/borrow contract |
| App/Session 迁移 | service `session/{app,application,runtime,control}.rs`；app `attach.rs`；其他经全仓搜索发现的调用者 | bootstrap 使用新版布局；producer/consumer 权限、控制消息及 reply 迁移 | 多进程 attach、IO/CTRL、listen/connect/accept/close/reset 相关原有路径回归 |
| region/mapping 分离 | infra `segment.rs`、`svm/region.rs`；stats `segment.rs`、`metric.rs`、`lib.rs`；所有 Segment imports | 明确 mapping 与 allocator owner，更新 raw allocation ownership；stats 不被迫创建 FIFO segment | stats allocation/drop 与共享 bootstrap 回归 |
| FIFO segment | infra `fifo.rs`；runtime app session；service session allocation/reclaim；TCP/协议 FIFO 消费者 | 移交 chunk/header 分配；直接借用替代闭包访问；同步更新 OOO、retention 和 reclaim | FIFO/OOO、协议失败原子性、TX/ACK retention 回归 |
| 普通函数 multiarch | infra `simd.rs`；FIFO copy paths | 新增宏及两条复制入口选择；复用已有 ISA 内核 | 真实复制行为、选择合法性、baseline 和 ISA 一致 |
| 文档和依赖 | CONTEXT、此 ADR、各 owner docs、Cargo manifests | 无兼容别名；新增测试依赖按现有布局组织；第三方源码不提交 | diff review、Cargo 依赖检查、旧术语删除审计 |

不能只迁移默认 MP 路径而留下 `<SingleProducer>` 控制队列，也不能把 generic
mode 删掉后保留原 free-list 布局。变更共享 layout 时必须覆盖从 creator 到
attached client 的全部 constructor、offset、对齐与版本验证。

## 8. 新类型/API 批准清单

以下均为提议，批准必须覆盖对应行为和调用闭包，而不是只批准名字。
内部 wire/header 类型保持私有；不引入 View、TLS 或闭包借用。

| ID | 新增/替代类型与 API | Owner / consumer | 为什么已有接口不足 |
| --- | --- | --- | --- |
| A1 | `SvmSegment`、`SvmSegmentConfig`、`SegmentBackend::{Private,Shm,Memfd}`；create/attach、size/backend、显式 close | infra；region/FIFO segment/stats/bootstrap | Segment 把资源与创建方 allocator 绑死，没有完整 backend/ready 生命周期 |
| A2 | 重定义 `SvmRegion`；`SvmRegionMain`、`SvmRegionConfig`；find-or-create、加入/离开、root、allocate/free | infra；命名 SVM 使用方 | 当前同名类型只是映射；缺失共享 metadata allocator、成员与命名根 |
| A3 | `SvmFifoSegment`、`SvmFifoSegmentConfig`、`SvmFifoSegmentMain`；create/attach/delete、slice FIFO/chunk 分配回收、迁移、preallocate、pressure 查询 | infra；Session | 没有该分配 owner；FIFO 各自分配无法提供 slice 复用和生命周期 |
| A4 | `SvmFifo` 替代 Fifo；私有 shared header/chunk/OOO；迁移现有 enqueue/peek/drop/OOO/notification，新增 chunk provision/segment borrow、subscriber 和迁移操作 | infra；Session/协议/SDK | 缺少完整 chunk 模型；共享引用和 closure 接口不满足所有权约束 |
| A5 | `SvmQueue<T>`、`SvmQueueConfig`；typed 元素/批次 enqueue/dequeue、双元素原子 enqueue、wait、attach；使用 zerocopy 0.8 直接依赖约束字节表示 | infra；通用 SVM 调用者 | FifoQueue 是进程内容器，MultiRing 是不同协议；Copy 不保证共享字节安全；当前 Binary API 未接入 |
| A6 | `SvmMsgQueue`、`SvmMsgQueueConfig`、MsgRingConfig/MsgDescriptor；`reserve<T>`、`dequeue<T>`、init/attach；ring 数量与容量是运行时配置 | infra；runtime Session MQ | 必须替换旧双布局/任意空闲槽模型；descriptor 只表示身份，不授予裸内存访问 |
| A7 | `MsgReservation<'queue, 'segment, T>`；`value_mut` 直接借用 T，commit 消耗 self 并完成通知；Message<T> 持有消费事务，payload 直接借用，Drop 按序回收 | infra；SessionProducer/SessionControlItem 迁移 | 旧 capability 缓存 pointers、MP slot 无 lifetime；不新增纯借用 Producer/Consumer wrapper，见 8.3 |
| A8 | 直接使用外部 crate 的同步原语和 RAII guard；Linux 队列设计选用 posix-sync 的共享 robust mutex/condvar，恢复契约见 8.4；不新增同步泛型 API | 队列维护自己的同步协议；通用原语遵循现有归属规则 | eventfd MQ 与固定元素队列的锁语义不同，不能抽象成可任意组合的后端 |
| A9 | `march_fn!`；具体函数签名的 baseline/ISA 变体与一次 CPU 选择 | infra `simd` 和 FIFO | node_function 引入 graph 类型，不能用于 infra 普通函数；需要具体函数指针 ISA dispatch 的限定许可 |
| A10 | `SvmSegmentError` / `SvmRegionError` / `SvmQueueError` / `SvmMsgQueueError`；现有 FifoError/SegmentAllocationError 的必要迁移 | 各 infra owner；bootstrap、Session、SDK | 现有 InvalidConfig/Option 丢掉 OS、owner-death、版本和容量事实；按第 6 节限定类别，禁止万能 error |
| A11 | `SvmRegionHeap`；offset 化块分配/释放/`reallocate`/`holds`/统计；无 `Result`，耗尽与损坏按 `SvmRegionHeapViolation` 终止 | infra region；root region 名字表、成员表、用户上下文 | 现有 bump 分配器不回收，Talc 含进程私有指针不能跨进程；§12.4 |
| A12 | `SvmHashMap<V>` 及其 `SvmEntry`/`SvmIter`/`SvmKeys`/`SvmValues`/`SvmValuesMut` 句柄；键是 arena 内字节串，表自己持有键字节 | infra region（名字注册表）；后续通用 SVM 调用者 | 标准 `HashMap` 只能从进程全局堆分配、内部是绝对指针、`RandomState` 每进程随机种子；`Bihash` 无字节键且槽位为指针；§12.5 |
| A13 | `SvmRegionHeader`/`SvmRegion`/`SvmRegionConfig`/`SvmRegionState`/`SvmRegionMain`/`SvmRegionError`/`RegionLock`/`RegionMembership` 的具体化；成员表、锁序与子区序号 | infra region；命名 SVM 使用方、daemon 注册 owner、SDK | A2 只有粗粒度描述，缺少字段映射、三段布局、锁恢复与子区所有权；§12.2–§12.8 |

A1–A13 不自动批准 database/CLI 的全新产品接口，也不自动批准改变所有 runtime
同步原语归属。Database、工具 parity 需后续具体 owner/API 设计；这项缺口保留在
完整性账本，不作为删除旧 MQ 的前置依赖。

## 8.1 泛型范围：只描述存储的内容

用户明确要求只对存储内容泛型化。本设计的 `T` 表示实际存入队列或 ring slot
的值。锁、通知、映射后端、生产者模式和 ring 数量均不作为类型参数；ring
数量和容量继续是运行时配置。本文删除此前扩张出的同步 trait、GAT、同步
后端类型和 const ring 参数，相关接口不再属于提议清单。

以下代码省略方法体，表达字段、类型约束、借用和方法签名，不是已实现代码。

| 存储对象 | Rust 表达 | T 的含义 |
| --- | --- | --- |
| 固定元素队列 | `SvmQueue<T>` | 每个元素都是 T，尺寸和对齐由 T 决定 |
| 异构消息队列 | `SvmMsgQueue`，`reserve<T>` / `dequeue<T>` | 当前 ring slot 内的具体值；不同 rings 可以存不同内容 |
| 未发布的消息 | `MsgReservation<'queue, 'segment, T>` | 该事务独占的、尚未发布的 T |
| byte FIFO | `SvmFifo` | 内容固定为 u8，保留字节位置和 OOO 语义 |
| 映射 / FIFO segment | `SvmSegment` / `SvmFifoSegment` | 仍是存储 owner，不引入同步或后端泛型 |

MQ 不强制所有 data rings 同一种 T。泛型放在访问当前 slot 的方法和实际写入
事务上；不另造一个只缓存 ring pointer 的 `Ring<T>` 借用 wrapper。

### 8.2 固定元素队列 SvmQueue<T>

```rust
// hammer_infra::svm::queue
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub struct SvmQueueConfig {
    pub capacity: u32,
}

pub struct SvmQueue<T> {
    segment: Arc<SvmSegment>,
    header_offset: u64,
    producer_event: Option<OwnedFd>,
    consumer_event: Option<OwnedFd>,
    element: PhantomData<T>,
}

impl<T> SvmQueue<T>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn layout(config: &SvmQueueConfig) -> Result<Layout, SvmQueueError>;

    pub unsafe fn init_at(
        segment: Arc<SvmSegment>,
        offset: u64,
        config: &SvmQueueConfig,
    ) -> Result<Self, SvmQueueError>;

    pub unsafe fn attach(
        segment: Arc<SvmSegment>,
        offset: u64,
    ) -> Result<Self, SvmQueueError>;

    pub fn enqueue(&self, value: T) -> Result<(), SvmQueueError>;
    pub fn enqueue_pair(&self, first: T, second: T)
        -> Result<(), SvmQueueError>;
    pub fn dequeue(&self) -> Result<Option<T>, SvmQueueError>;
    pub fn dequeue_batch(&self, destination: &mut [T])
        -> Result<usize, SvmQueueError>;
    pub fn enqueue_wait(&self, value: T) -> Result<(), SvmQueueError>;
    pub fn dequeue_wait(&self) -> Result<T, SvmQueueError>;
    pub fn dequeue_until(&self, deadline: Instant) -> Result<T, SvmQueueError>;
    pub fn len(&self) -> Result<usize, SvmQueueError>;
    pub fn is_empty(&self) -> Result<bool, SvmQueueError>;
}
```

enqueue/enqueue_pair/dequeue/dequeue_batch 均通过 lock 取共享 mutex，容量不足
或为空时立即返回，不在这些方法内部等待容量。enqueue_pair 要么同时发布两个
元素，要么一个也不发布。显式 *_wait / *_until 在相同 mutex 下循环检查条件；
超时使用固定截止时间，spurious wakeup 不重置总等待时间。这里不提供 try_lock，
也不承诺普通方法在锁竞争时立即返回。固定元素的复制与出队在 guard 内完成，
无需向 Binary API 暴露共享 header 或手工 lock/unlock/raw 方法。

`size_of::<T>()` / `align_of::<T>()` 替代调用方手填 elsize 与强制指针转换。
capacity 仍是配置。T 为 ZST、布局溢出、capacity 为 0 在初始化时拒绝。

Copy 不足以保证跨进程字节有效性，因此提议复用 zerocopy 的既有约束，不新增
一套通用消息 trait。`IntoBytes` 排除未初始化 padding；`FromBytes` 保证所读取
位模式有效；引用、进程指针、String、Vec 不允许作为共享 T。现有 lockfile
已有 zerocopy 0.8，采用时仍需 infra 的直接依赖声明。相关接口依据：
[IntoBytes](https://docs.rs/zerocopy/latest/zerocopy/derive.IntoBytes.html)、
[FromBytes](https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html)。

attach 必须验证版本、元素大小、对齐和调用方约定的 schema。相同 size 不能
证明是同一种业务消息；不能将 Rust TypeId 当成跨进程 schema。协议有效性
由内容 owner 验证，infra 的约束只解决安全字节表示。

### 8.3 消息队列：统一布局，reserve<T> / dequeue<T>

```rust
// hammer_infra::svm::msg_queue；设计签名，省略方法体。
use posix_sync::condvar::{BorrowedCondvar, RawCondvarAlloc};
use posix_sync::mutex::{BorrowedMutex, RawMutexAlloc};
use posix_sync::mutex::guards::StandardGuard;
use posix_sync::mutex::robustness_markers::Robust;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(Clone, Copy)]
#[repr(C)]
pub struct MsgDescriptor {
    ring_index: u32,
    element_index: u32,
}

pub struct MsgRingConfig {
    pub capacity: u32,
    pub element_size: usize,
    pub element_alignment: usize,
}

impl MsgRingConfig {
    pub fn new<T>(capacity: u32) -> Self
    where T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable;
}

pub struct SvmMsgQueueConfig<'a> {
    pub capacity: u32,
    pub rings: &'a [MsgRingConfig],
}

// 位于共享映射；descriptor 数量和各 ring 容量彼此独立。
#[repr(C)]
struct QueueState {
    head: u32,
    tail: u32,
    len: u32,
}

#[repr(C, align(64))]
struct QueueHeader {
    ready: AtomicU32,
    version: u32,
    capacity: u32,
    ring_count: u32,
    descriptors_offset: u64,
    rings_offset: u64,
    // crate 自己的原位存储类型；不自行定义 pthread 存储或锁封装。
    mutex: RawMutexAlloc,
    condvar: RawCondvarAlloc,
    state: UnsafeCell<QueueState>,
    // 独立终止标志，可在无法获得锁时观察；不是第二套 payload 发布协议。
    failed: AtomicBool,
}

#[repr(C)]
struct RingState {
    head: u32,
    tail: u32,
    occupied: u32,
}

#[repr(C)]
struct RingHeader {
    capacity: u32,
    element_size: u32,
    element_alignment: u32,
    elements_offset: u64,
    // 与 descriptor 存储一样，仅在 QueueHeader.mutex guard 下修改。
    state: UnsafeCell<RingState>,
}

pub struct SvmMsgQueue<'segment> {
    segment: &'segment SvmSegment,
    header_offset: u64,
    mutex: BorrowedMutex<'segment, Robust>,
    condvar: BorrowedCondvar<'segment>,
    event: Option<OwnedFd>,
}

// 占有未发布 slot 与队列状态锁；取消不推进共享 tail/count。
pub struct MsgReservation<'queue, 'segment, T> {
    queue: &'queue SvmMsgQueue<'segment>,
    guard: Option<StandardGuard<'queue>>,
    descriptor: MsgDescriptor,
    element: PhantomData<T>,
}

// 消费事务负责 slot 回收；不是缓存指针的纯借用对象。
pub struct Message<'queue, 'segment, T> {
    queue: &'queue SvmMsgQueue<'segment>,
    guard: Option<StandardGuard<'queue>>,
    descriptor: MsgDescriptor,
    element: PhantomData<T>,
}

impl<'segment> SvmMsgQueue<'segment> {
    pub fn layout(config: &SvmMsgQueueConfig<'_>)
        -> Result<Layout, SvmMsgQueueError>;
    pub unsafe fn init_at(
        segment: &'segment SvmSegment, offset: u64,
        config: &SvmMsgQueueConfig<'_>,
    ) -> Result<Self, SvmMsgQueueError>;
    pub unsafe fn attach(segment: &'segment SvmSegment, offset: u64)
        -> Result<Self, SvmMsgQueueError>;

    pub fn reserve<T>(&self, ring: u32)
        -> Result<MsgReservation<'_, 'segment, T>, SvmMsgQueueError>
    where T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable;

    pub fn dequeue<T>(&self)
        -> Result<Option<Message<'_, 'segment, T>>, SvmMsgQueueError>
    where T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable;

    pub fn wait_nonempty(&self) -> Result<(), SvmMsgQueueError>;
    pub fn wait_nonempty_until(&self, deadline: Instant)
        -> Result<(), SvmMsgQueueError>;
    pub fn wait_space(&self, ring: u32) -> Result<(), SvmMsgQueueError>;
    pub fn len(&self) -> Result<usize, SvmMsgQueueError>;
    pub fn is_empty(&self) -> Result<bool, SvmMsgQueueError>;
}

impl<T> MsgReservation<'_, '_, T>
where T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn value_mut(&mut self) -> &mut T;
    pub fn commit(self) -> Result<(), SvmMsgQueueError>;
}

impl<T> Message<'_, '_, T>
where T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn ring_index(&self) -> u32;
    pub fn value(&self) -> &T;
    pub fn release(self) -> Result<(), SvmMsgQueueError>;
}
```

这里只泛型化内容 T。MsgReservation 与 Message 实现 Drop，分别拥有取消
未发布事务和按序回收 slot 的职责；没有 ProducerMode、SingleProducer、
MultiProducer、SessionProducer 或同步泛型。两个事务都不能 Clone，guard 不可
手动解锁。普通原语直接使用 crate 类型。guard 的 Option 仅用于 commit/release 在实现 Drop
的事务中取出并释放 guard，防止重复回收；不代表第二种锁或同步后端。

共享存储为 header、descriptor FIFO、ring headers、各 ring slots。一份布局
处理所有生产者数量；capacity 可为任意正整数，head/tail 按容量取模，完整
capacity 可用，不依赖 q_mask 或空槽哨兵。occupied 包含已发布但未释放的消息，
descriptor 出队只减少 QueueState.len，Message 释放才减少 ring occupied。

reserve 持有 state guard，检查 queue/ring capacity 与完整 T 布局后，把目标
slot 全字节初始化再借出 &mut T。commit(self) 一次性推进 ring tail/occupied
和 descriptor tail/len，释放 guard 后通知；Drop 取消不产生 descriptor 或通知。
所有 size/offset/stride 计算 checked，拒绝 ZST、溢出、错位和错误 schema。
相同尺寸不同消息的语义判别仍归 Session，infra 不使用 TypeId 作为共享 schema。

dequeue<T> 获取同一把 robust mutex，验证队首 descriptor 和 T 布局后才改变
head/len。返回的消费事务保留这份 guard；release 或 Drop 在持锁期间按序归还
slot，然后释放 guard。不再添加 空值锁或第二把消费者锁。descriptor 出队
和 slot 释放仍是不同计数变化，但本版本不允许生产者在消费借用期间并行修改
其他 slots；这是为保持单锁和明确借用边界作出的并发度取舍。

SvmMsgQueue 的 segment 生命周期是真实映射借用；BorrowedMutex/Condvar
存放在进程私有队列对象内，共享内存中只放 crate 的 Raw*Alloc。生命周期参数
不表示新的同步/生产者模式。映射必须由外层 region/应用内存 owner 持有，再
构造使用它的队列；不得把临时 BorrowedMutex.lock() 的 guard 延长为队列生命周期，
不得伪造 'static，也不得在同一个可移动对象中自引用其 segment 字段。相应的
Session/SDK 安装接口必须保留这条外层 owner → queue → transaction 的借用链。

持有消费事务时只能短暂读取/解码，不重入 dequeue、reserve、wait，不执行
Session handler、I/O、await 或 barrier。SessionControlItem 应先解码为已有具体
控制消息值并结束消费事务，再调用控制处理逻辑。生产事务同样只初始化内容并
提交，不在其中执行调用者回调。mem::forget 可以使队列失去进展，但不能使仍
被借用的 slot 提前重用。

SessionMsgQueue 本身不再有模式参数。IO 及 worker CTRL 采用 [u8; 24]，应用
CTRL 采用 [u8; SESSION_CTRL_MSG_MAX_SIZE]；SessionControlPayload 继续负责
定长编码。删除 claim_producer 与 SessionProducer，SDK/daemon 直接拥有各自
的 SessionMsgQueue。不同 rings 的内容类型可以不同，存储中不放业务消息类型。

condvar 通知覆盖发布、descriptor 空位和 ring 空位；等待方重查 predicate。
eventfd 通知失败不能回滚已经发布/释放的消息；commit/release 的错误必须标明
操作已经完成。Drop 不能返回错误，因此 fd 通知错误必须留在队列 owner 的明确
可查询通知状态中，由 runtime 处理；不能仅写日志后丢失。该状态及 fd 接入接口
在实施前需补齐，不能宣称本节已提供完整 eventfd 实现。

### 8.4 同步保持具体实现，不参与内容泛型

| 已核对的 VPP 路径 | 实际语义 | Rust 约束 |
| --- | --- | --- |
| `application.c::app_rx_mqs_alloc` | 每条 app RX MQ 创建 eventfd | 私有 owner 持有 fd 和 producer lock |
| `segment_manager.c::segment_manager_alloc_queue` | 由 use_mq_eventfd 决定是否创建 eventfd | 配置保持原有选择，不产生类型参数 |
| `message_queue.h::svm_msg_q_lock/try_lock/unlock` | eventfd MQ 用私有 spinlock；无 eventfd 用共享 mutex | 复用具体原语，不新增同步 trait 或 GAT |
| `queue.c::svm_queue_lock` / `svm_queue_send_signal_inline` | eventfd 只替换通知，共享 mutex 保留 | 不套用 MQ 的锁选择规则 |
| `svm.h::svm_mem_alloc/free` | region 分配使用共享 mutex | region 分配锁不因队列内容 T 改变 |

以上表格描述 VPP 源行为。本轮 Linux MQ 设计统一使用共享 mutex，
eventfd 只改变通知，不采用 VPP 的私有 producer spinlock 分支。SVM 领域类型不直接暴露原始
pthread 字段，也不为此定义一个可插拔同步框架。具体平台初始化/guard 能力通过外部 crate 选型落实；内容泛型不授权更改同步原语归属、依赖图或增添新后端。

mutex 与 condvar 仍属于共享队列头。上面的公开 owner 只保存映射和
header_offset，不能把没有列出共享 header 误写成没有这两个同步字段。
VPP 的依据是 `queue.h:16`、`message_queue.h:21`，以及两份 `.c` 初始化时的
`pthread_mutexattr_setpshared` / `pthread_condattr_setpshared`。

同步原语直接复用外部 crate，不定义 Hammer 自有 mutex/condvar 类型，
不增加同步 trait 或队列同步类型参数。此处不再用未实现的类型名占位。

锁接口按用户要求只保留 lock，由 guard 释放；不要求 crate 提供 try_lock。
队列 enqueue/reserve 的容量检查与锁获取是不同操作，不能因此宣称整个
调用无等待；这也不是给公开队列增加同步类型参数的理由。

**Linux 选型：posix-sync 0.1.0，共享 robust mutex + condvar**

文档确认了完整组合：BorrowedMutex<Robust> 支持共享映射中的原位初始化、
attach、阻塞 lock 和 RAII guard；BorrowedCondvar 支持 shared、Monotonic、
wait/wait_for、notify_one/notify_all。两种锁获取入口都能识别 owner death。
Robust 是第三方 crate 的固定配置，本项目不把它暴露为队列类型参数。内容 T
继续只描述 slot/固定队列元素，crate 的 mutex 无需也不提供 Mutex<T>。

```toml
# 仅为 ADR 的拟议依赖；本轮不修改 Cargo.toml。
[target.'cfg(target_os = "linux")'.dependencies]
posix-sync = "=0.1.0"
```

```rust
use posix_sync::mutex::{
    BorrowedMutex, MutexBuilder, MutexSharing, RawMutexAlloc,
    guards::{RobustGuardContainer, StandardGuard},
    robustness_markers::Robust,
};
use posix_sync::condvar::{
    BorrowedCondvar, CondvarBuilder, CondvarSharing, CondvarClock, RawCondvarAlloc,
};

// 创建方：mutex_storage / condvar_storage 指向最终映射中相应 Raw*Alloc 空间。
// segment 是外层 owner；初始化完成前不得发布共享 ready。
let mutex = unsafe {
    MutexBuilder::<Robust>::new()
        .with_sharing(MutexSharing::Shared)
        .build_borrowed(mutex_storage, segment)
};
let condvar = unsafe {
    CondvarBuilder::new()
        .with_sharing(CondvarSharing::Shared)
        .with_clock(CondvarClock::Monotonic)
        .build_borrowed(condvar_storage, segment)
};

// attach 方：先 Acquire ready 并校验版本/偏移/对齐；不能再次 build。
let mutex: BorrowedMutex<'_, Robust> = unsafe {
    BorrowedMutex::from_raw(mutex_storage, segment)
};
let condvar = unsafe {
    BorrowedCondvar::from_raw(condvar_storage, segment, CondvarClock::Monotonic)
};
```

RawMutexAlloc / RawCondvarAlloc 是 crate 提供的实际同步存储，不是 Hammer 新增
类型。按其 SIZE/ALIGN 安排空间，构造后禁止移动，不能将 BorrowedMutex 指针
值写进共享 ABI。RawMutexAlloc 的 !Unpin 约束不免除调用者对 mmap 的生命周期
责任。所有进程/线程的借用和等待结束后，最后资源 owner 才能 destroy；普通
attached queue Drop 只结束本地借用，不销毁共享同步对象。

**owner death 的确定处理策略：关闭损坏实例，重建队列**

本 ADR 不宣称多字段 queue/ring 更新可自动修复，也不增加通用事务日志。
遇到 EOWNERDEAD 时，接管锁的线程已持锁，但此前操作是否提交不可判定。
决定停止该实例，而不是通过 consistent 把未知内容宣布为有效：

1. 共享 header 的 failed 原子标志置为 true（Release）；所有队列入口 Acquire
   检查，获得 guard 后再次检查。在这一故障分支不读取 descriptor/payload、不
   改 head/tail/count，不把异常队列清空后重新使用。
2. 丢弃未 make_consistent 的 IndeterminateGuard。POSIX 将 mutex 留在
   ENOTRECOVERABLE 状态。初次检测者返回 OwnerDied；后续 lock 的
   MutexLockError::NotRecoverable 返回 NotRecoverable，已有 failed 标志返回
   Closed。不能把错误当成 empty/full，也不能返回一个看似有效的内容引用。
3. queue 生命周期 owner 停止该实例的发送/消费，通过现有控制连接撤销旧队列
   身份。等待所有存活参与者退出旧实例后，用新存储、版本化身份和新的同步对象
   重建；禁止在仍有 waiter/借用的旧地址覆盖初始化。Binary API 的损坏共享
   allocator 需要隔离整个所属 region，不能只重置队列计数后复用其 heap。
4. 原操作的交付状态报告为不确定；控制/业务 owner 按现有消息身份处理重复与
   重试。SVM 不自动重发未收到确认的消息，不承诺跨进程崩溃的 exactly-once。

这就是本轮选定的 robust 故障恢复契约：内核帮助接管死 owner 的锁、程序明确
隔离不一致状态并重新建连。不是不处理 owner death，也不是无条件 consistent
继续运行。VPP queue.c 调用 consistent 后继续的行为被此明确失败路径替代。

```rust
// 队列私有方法；使用 crate guard，不定义新同步包装类型。
fn lock(&self) -> Result<StandardGuard<'_>, SvmMsgQueueError> {
    if self.header().failed.load(Ordering::Acquire) {
        return Err(SvmMsgQueueError::Closed);
    }
    match unsafe { self.mutex.lock() } {
        Ok(RobustGuardContainer::Standard(guard)) => {
            if self.header().failed.load(Ordering::Acquire) {
                drop(guard);
                return Err(SvmMsgQueueError::Closed);
            }
            Ok(guard)
        }
        Ok(RobustGuardContainer::Indeterminate(guard)) => {
            self.header().failed.store(true, Ordering::Release);
            drop(guard); // 不 consistent：后续 pthread lock 返回 ENOTRECOVERABLE。
            Err(SvmMsgQueueError::OwnerDied)
        }
        Err(source @ posix_sync::mutex::MutexLockError::NotRecoverable) => {
            self.header().failed.store(true, Ordering::Release);
            Err(SvmMsgQueueError::NotRecoverable { source })
        }
        Err(source) => Err(SvmMsgQueueError::Lock { source }),
    }
}
```

len/is_empty 同样返回 Result，不能把锁错误或 failed 状态伪装成空队列。

header() 是队列私有的直接借用，校验过的 header 内存由 segment 保持；只要
状态未知，除 failed 这一独立标量以外都不读取。上面使用第三方现有错误作为
source；不复制一个 MutexLockError，也不通过字符串判断 errno。

**条件变量重新加锁必须走同样的恢复分支**

BorrowedCondvar::wait/wait_for 接受 &mut guard，不消耗 guard。返回
CondvarWaitError::OwnerDead 时 mutex 已被重新获取；此时设置 failed、保持
数据未读、让 guard 未标记 consistent 地释放，然后返回 OwnerDiedDuringWait
并保留 source。不能写成 wait(...)? 后继续使用 payload。NotRecoverable 时
不认为持有有效锁，不访问受保护状态，由 crate guard 的等待状态处理其释放；
这一路径必须列入实施时的实际行为验证。

crate 提供 IndeterminateGuard::make_consistent() 和 MutexGuard::mark_consistent()
供已经完成数据修复的调用方使用；本设计的故障关闭路径故意不调用它们。
RobustGuardContainer 的 Indeterminate 来自 lock；condvar wait 的 OwnerDead
通过错误返回，不能假设 wait 会把 StandardGuard 自动变成 Indeterminate 变体。

owner 死亡不会自动为 condvar 产生业务通知。无限等待接口在控制线程内部使用
Monotonic wait_for 的有限检查周期（设计默认 100 ms），每轮重新加锁并检查
failed、predicate 和截止时间。内部周期超时不等于调用者超时；只有用户截止
时间到达才返回 Timeout。已在 mutex.lock 阻塞者由 robust mutex owner-death
协议接管；不改成 try_lock。活 owner 永久停顿和在途 syscall 的调度延迟不由
robust 解决，100 ms 也不是端到端实时上界。

初始化限制：0.1.0 builder 文档签名不返回 Result，不能在 ADR 中虚构可传播的
初始化错误。原位初始化在 bootstrap 未发布阶段完成，初始化异常不得发布 ready；
由 bootstrap 边界结束本次构建并回收尚未交给 peer 的映射。若以后要求逐条
pthread 初始化 errno 可恢复，须单独调整依赖能力，不能伪称当前 builder 支持。

官方文档依据（只读在线文档，未下载 crate 源码或编译）：
[mutex 与 robust 示例](https://docs.rs/posix-sync/latest/posix_sync/mutex/index.html)、
[MutexBuilder](https://docs.rs/posix-sync/latest/posix_sync/mutex/builders/struct.MutexBuilder.html)、
[BorrowedMutex](https://docs.rs/posix-sync/latest/posix_sync/mutex/struct.BorrowedMutex.html)、
[BorrowedCondvar](https://docs.rs/posix-sync/latest/posix_sync/condvar/struct.BorrowedCondvar.html)、
[CondvarBuilder](https://docs.rs/posix-sync/latest/posix_sync/condvar/builders/struct.CondvarBuilder.html)、
[RobustGuardContainer](https://docs.rs/posix-sync/latest/posix_sync/mutex/guards/enum.RobustGuardContainer.html)、
[IndeterminateGuard](https://docs.rs/posix-sync/latest/posix_sync/mutex/guards/struct.IndeterminateGuard.html)、
[CondvarWaitError](https://docs.rs/posix-sync/latest/posix_sync/condvar/enum.CondvarWaitError.html)、
[MutexGuard::mark_consistent](https://docs.rs/posix-sync/latest/posix_sync/mutex/guards/trait.MutexGuard.html)。

rustix-futex-sync::shm 虽支持共享 mutex/condvar，但缺少上述 robust 能力，退出
本 ADR 选型。[其共享同步文档](https://docs.rs/rustix-futex-sync/latest/rustix_futex_sync/shm/index.html)
不能作为已支持 owner-death 的证据。

**parking_lot 0.12.5 文档核对（2026-09-11）**

该 crate 同时提供 Mutex<T> 和 Condvar，lock 返回 RAII guard，wait 接受
&mut MutexGuard，符合所需的调用形式。



但其同步范围是同一进程中的线程。parking_lot_core 文档说明，线程等待队列
由 parking lot 按锁地址管理；这不是共享 segment 中可供独立 app 进程 attach
的等待状态。将 Mutex/Condvar 放入 mmap 不会把 parking lot 的线程队列变成
跨进程共享队列。因此不将 parking_lot 选为 SVM 共享 mutex/condvar 的实现。
这个适配判断与是否提供 try_lock 无关。

依据：[Mutex](https://docs.rs/parking_lot/latest/parking_lot/type.Mutex.html)、
[Condvar](https://docs.rs/parking_lot/latest/parking_lot/struct.Condvar.html)、
[parking_lot_core 的等待队列设计](https://docs.rs/parking_lot_core/latest/parking_lot_core/)。

**process-sync 0.2.2 文档核对（2026-09-11）**

该 crate 确实同时提供 `process_sync::SharedMutex` 和
`process_sync::SharedCondvar`，支持基础跨进程互斥与等待/通知，但公开 API
不足以直接承担本 ADR 的完整 SVM 同步契约：

| 需求 | 文档中的接口与结论 |
| --- | --- |
| 现有共享 segment 的原位初始化、独立 app attach | mutex/condvar 只有 new 构造；没有指定地址初始化、from_raw 或 fd attach API。SharedMemoryObject 文档描述 fork/clone 继承 MAP_SHARED 映射，不能作为独立 app attach 的接口证据 |
| 超时等待 | SharedCondvar 只列 wait、notify_one、notify_all，没有 timed wait |
| Rust 持锁生命周期 | lock(&mut self) 返回 Result<()>，需要手动 unlock；两个同步类型均为 !Send、!Sync，没有可直接复用的 RAII guard |
| owner death | 没有公开 robust 配置或 consistent 恢复接口；不能从基础 pthread 封装推断已满足 VPP robust mutex 契约 |

以上是公开文档/API 的适配结论，不是源码审计或运行验证。可用于文档演示的
父子进程基础同步，不能原样选作 Hammer 独立 app/daemon 共享队列的同步依赖；
也不通过新增本地同步包装类型绕过这些接口缺口。

文档依据：
[SharedMutex](https://docs.rs/process-sync/latest/process_sync/struct.SharedMutex.html)、
[SharedCondvar](https://docs.rs/process-sync/latest/process_sync/struct.SharedCondvar.html)、
[SharedMemoryObject](https://docs.rs/process-sync/latest/process_sync/struct.SharedMemoryObject.html)。

iceoryx2-bb-posix 0.9.3 的公开模块中未找到配套 condvar，不列为本次方案。
[模块目录](https://docs.rs/iceoryx2-bb-posix/latest/iceoryx2_bb_posix/)。

操作契约：lock 返回 RAII guard；wait 持有同一 mutex 的 guard，原子释放锁并
等待，返回前重新获取锁；调用方循环检查队列 predicate。condvar 本身不保护
队列内容。std::sync::Mutex / Condvar 可用于进程内阻塞状态；不能将这一组
进程内字段替换到上述跨进程共享布局中而声称语义已对齐。

VPP MQ 选择 eventfd 时使用私有 producer spinlock；VPP 固定元素队列仍使用
共享 mutex。本轮 Linux MQ 明确选择统一的共享 mutex/condvar 协议，不能将
这一 Rust 决策写成 VPP 原有行为，也不以新增后端泛型隐藏差异。

### 8.5 Mapping、region 与 FIFO segment 不增加泛型参数

SvmSegment、SvmRegion、SvmRegionMain、SvmFifoSegment 与 SvmFifoSegmentMain
继续表达实际存储/分配 owner。root、chunk、members 等共享位置仍使用 offset；
OS fd 留在私有 owner。内容类型 T 不进入 backend、slice 数、worker、TCP 或
通知选择。Region 的共享分配锁和映射生命周期仍按前面的 VPP 证据单独设计。

### 8.6 CLIB_MARCH_FN 与内容泛型是两个独立要求

保留用户最初要求的普通函数多架构选择。它按 CPU 支持选择具体机器码，
不因此新增 ByteCopy trait、算法类型参数或 generic FIFO。两个入口仍是
copy_to_chunk / copy_from_chunk；第 8.8 节给出宏与具体签名。

### 8.7 FIFO segment 与 FIFO 私有状态

```rust
// hammer_infra::svm::fifo_segment
const CHUNK_SIZE_CLASSES: usize = 11;

#[repr(C, align(64))]
struct FifoSegmentSlice {
    free_chunks: [AtomicU64; CHUNK_SIZE_CLASSES],
    free_fifos: u64,
    free_chunk_bytes: AtomicU64,
    virtual_bytes: u64,
    chunk_counts: [u32; CHUNK_SIZE_CLASSES],
}

struct FifoSlice {
    fifos: Pool<SvmFifo>,
    virtual_bytes: u64,
    active_fifos: Vec<u32>,
}

pub struct SvmFifoSegment {
    segment: Arc<SvmSegment>,
    header_offset: u64,
    slices: Vec<FifoSlice>,
    message_queue_offsets: Vec<u64>, // 已分配队列身份；使用者借用外层映射后 attach。
}

pub struct SvmFifoSegmentMain {
    segments: Pool<SvmFifoSegment>,
    attach_timeout: Duration,
}

impl SvmFifoSegment {
    // slices 在启动前固定；调用者只能操作当前 worker 的 slice。
    pub fn allocate_fifo(&mut self, slice: u32, capacity: u32)
        -> Result<u32, FifoError>;
    pub fn fifo(&self, slice: u32, index: u32) -> Option<&SvmFifo>;
    pub fn fifo_mut(&mut self, slice: u32, index: u32) -> Option<&mut SvmFifo>;
    pub fn free_fifo(&mut self, slice: u32, index: u32) -> Result<(), FifoError>;
    pub fn preallocate_chunks(&mut self, slice: u32, size: u32, count: u32)
        -> Result<(), FifoError>;
    pub fn available_bytes(&self) -> usize;
    pub fn cached_bytes(&self) -> usize;
}
```

上面的 `&mut SvmFifoSegment` 是独占的配置/单 worker 接入，不是多个 worker
从 shared Main 获取整个 segment 的可变引用的许可。多 worker 的最终安装接口
必须把启动前已存在的 slice 私有值直接交给其 worker，或从真实借用的 owner
分割现有 slice；禁止 `&self -> &mut FifoSlice`，禁止按任意 thread index 安全
地借到外 worker。A3 的并行 worker 安装和迁移签名在所有权拆分后才能定稿。

```rust
// hammer_infra::svm_fifo
#[repr(C, align(64))]
struct FifoPosition {
    chunk_offset: AtomicU64,
    byte: AtomicU32,
}

#[repr(C, align(64))]
struct FifoHeader {
    start_chunk: AtomicU64,
    end_chunk: AtomicU64,
    size: u32,
    min_alloc: u32,
    slice_index: u32,
    has_event: AtomicU32,
    want_dequeue_notification: AtomicU32,
    has_dequeue_notification: AtomicU32,
    dequeue_threshold: AtomicU32,
    consumer: FifoPosition,
    producer: FifoPosition,
}

pub struct SvmFifo {
    segment: Arc<SvmSegment>,
    header_offset: u64,
    segment_header_offset: u64,
    enqueue_lookup: RbTree<u32, u64>,
    dequeue_lookup: RbTree<u32, u64>,
    ooo: Pool<OooSegment>,
}

// 迁移现有事务类型；不新增 payload 缓存。
pub struct FifoWriteReservation<'a> {
    fifo: &'a mut SvmFifo,
    start_tail: u32,
    reserved_len: usize,
    committed: bool,
}

impl SvmFifo {
    pub fn max_enqueue(&self) -> usize;
    pub fn max_dequeue(&self) -> usize;
    pub fn enqueue(&mut self, source: &[u8]) -> Result<usize, FifoError>;
    pub fn enqueue_ooo(&mut self, offset: u32, source: &[u8])
        -> Result<OooResult, FifoError>;
    pub fn peek(&mut self, offset: usize, destination: &mut [u8])
        -> Result<usize, FifoError>;
    pub fn readable_segment(&self) -> Option<&[u8]>;
    pub fn dequeue_drop(&mut self, bytes: usize) -> Result<usize, FifoError>;
    pub fn reserve_write(&mut self, bytes: usize)
        -> Result<FifoWriteReservation<'_>, FifoError>;
    pub fn set_event(&self) -> bool;
    pub fn unset_event(&self);
}

impl FifoWriteReservation<'_> {
    // 每次借出一个实际 chunk 的目标区域，不假定 chain 只有两个片段。
    pub fn segment_mut(&mut self, offset: usize) -> Option<&mut [u8]>;
    pub fn reserved_len(&self) -> usize;
    pub fn commit(self, initialized: usize) -> Result<usize, FifoError>;
}
```

FIFO 的 attach 同样需要证明 producer/consumer 权限；`&mut` 只能防止本地
同一 owner 的重入，不能单凭它保证跨 mapping 的唯一角色。FifoPosition 是
自身持有 index 的共享 cacheline，不是包裹外部借用的 View。完整 subscriber、
OOO list linkage、迁移 rollback 字段仍须由对应 VPP 函数的具体 slice 设计补齐；
这些块明确展示核心状态，不作为所有 VPP 字段已映射的声明。

### 8.8 普通函数 multiarch：具体函数签名

```rust
// 提议宏语法。宏生成同一函数体的各 target-feature 实现与选择入口。
march_fn! {
    variants {
        baseline;
        x86_64("avx2", priority = 20);
        x86_64("avx512f,avx512bw", priority = 30);
        aarch64("neon", priority = 10);
    }

    unsafe fn copy_to_chunk(
        fifo: &mut SvmFifo,
        chunk_offset: u64,
        position: u32,
        source: &[u8],
    ) -> u64 {
        // 同一 chunk 遍历函数体，由宏复制到各 ISA entry。
        // 在每个 entry 内编译对应字节复制指令；此处省略实现。
    }
}
```

copy_from_chunk 使用相同声明方式，最后一个参数为 `destination: &mut [u8]`。
返回最后处理位置对应的 chunk offset。各具体 entry 根据 feature 集编译，普通
入口通过本进程的一次性选择调用受支持实现：

```rust
static SELECTED: OnceLock<
    unsafe fn(&mut SvmFifo, u64, u32, &[u8]) -> u64,
> = OnceLock::new();
```

不把所选函数写入共享内存；不新增算法 trait、generic worker 或 graph
registration。实际复制必须走选择后的 ISA 内核，不能只生成不同名字。

### 8.9 错误的具体签名示例

```rust
// hammer_infra::svm::msg_queue：owner-local，保留结构化事实。
#[derive(Debug, thiserror::Error)]
pub enum SvmMsgQueueError {
    #[error("message descriptor queue is full")]
    QueueFull,
    #[error("message ring {ring} is full")]
    RingFull { ring: u32 },
    #[error("message ring {ring} does not exist")]
    InvalidRing { ring: u32 },
    #[error("no configured ring can hold {requested} bytes")]
    MessageTooLarge { requested: usize },
    #[error("message queue mutex owner died; delivery is indeterminate")]
    OwnerDied,
    #[error("message queue owner died while reacquiring after condition wait")]
    OwnerDiedDuringWait { #[source] source: posix_sync::condvar::CondvarWaitError },
    #[error("message queue mutex is not recoverable")]
    NotRecoverable { #[source] source: posix_sync::mutex::MutexLockError },
    #[error("message queue mutex could not be reacquired after condition wait")]
    NotRecoverableDuringWait { #[source] source: posix_sync::condvar::CondvarWaitError },
    #[error("message queue is closed")]
    Closed,
    #[error("message queue lock failed")]
    Lock { #[source] source: posix_sync::mutex::MutexLockError },
    #[error("message queue wait timed out")]
    Timeout,
    #[error("message queue layout version {actual} differs from {expected}")]
    VersionMismatch { expected: u32, actual: u32 },
    #[error("message was published but notification failed")]
    PublishedSignal { #[source] source: std::io::Error },
    #[error("descriptor was dequeued but producer notification failed")]
    DequeuedSignal { #[source] source: std::io::Error },
    #[error("message slot was released but producer notification failed")]
    ReleasedSignal { #[source] source: std::io::Error },
    #[error("message queue wait failed")]
    Wait { #[source] source: posix_sync::condvar::CondvarWaitError },
}
```

PublishedSignal/DequeuedSignal/ReleasedSignal 的类别直接记录操作已经完成，
调用者据此重试通知或关闭连接，不能重试数据操作。布局/初始化/同步 acquire/
cleanup 的其余错误随 create/attach 事务定稿。owner death 与 Rust panic 不同；
panic unwind 会释放 RAII guard，不能期待内核随后报告 EOWNERDEAD。任何可能
部分修改状态的 unwind 路径必须由所属队列事务先设置 failed，再释放 guard。

## 9. 验证矩阵与执行顺序

基线没有发现 infra 的 crate-local FIFO/MQ 测试文件，不能写“现有测试已覆盖”。
VPP 的有效参考包括 `src/plugins/unittest/svm_fifo_test.c`，以及
`src/plugins/unittest/session_test.c` 中的 MQ basic/speed 测试。
`src/svm/svm_test.c` 是未列入当前 CMake 测试目标的旧示例，见 9.1。
robust owner-death 与 Rust 借用/多架构专项用例是补充设计，不冒充上游已有测试。

| 测试 | 级别与布置 | 必须验证 | 决策 |
| --- | --- | --- | --- |
| segment attach | 两个独立进程，不同映射地址，真实 fd 传递 | offset 可用、ready 前不可读、版本拒绝、fd 生命周期、失败不泄漏 | D1/D10/D11 |
| region membership | 独立进程 create/find/leave + 分配失败注入 | root/成员一致，最后退出后回收；attached allocator 不覆盖共享状态 | D2 |
| fixed element queue | typed T，真实跨进程队列与具体等待路径 | 顺序、add2 容量不足不部分插入、full/empty、timeout、spurious wakeup | D3/D5 |
| MQ ring lifecycle | 多种 elsize、非二次幂容量、descriptor capacity 与 ring capacity 不同 | 8-byte descriptor、按序 free、ring 满与 queue 满分别背压、reserve 取消不泄漏 | D4 |
| MQ ownership | 编译失败用例 + 并发 producer/subprocess consumer | reservation 不越过 owner；commit 不能二次调用；consumer 无同时借用；无丢失/重复 | D4/D5 |
| MQ notifications | eventfd 与条件等待路径分别运行；生产/消费竞争窗口 | 空→非空、两种满→非满通知；清信号重检查；已发布后 signal failure 不重发消息 | D5/D11 |
| robust owner death | 独立 child 在 reserve/commit/dequeue/release 各持锁窗口退出；父进程有外部超时保护 | OwnerDied 后不读 payload，不 consistent；后续 Closed/NotRecoverable；旧身份作废、新队列 roundtrip | D5/8.4 |
| robust condvar reacquire | waiter 已释放锁睡眠，另一进程取得锁后退出 | wait OwnerDead 分类及无非法 unlock；无 notify 也通过周期检查发现；NotRecoverable 不访问数据 | D5/8.4 |
| FIFO bytes / OOO | 独立 owner，跨多个 chunk、sequence wrap、洞填充 | 发布/回收顺序；OOO 合并；source/destination 位置 failure-atomic | D7/D8 |
| FIFO allocation / migration | slice 耗尽、chunk 回收、迁移分配失败、peer detach | 不错 slice 回收、旧 owner 失败仍有效、无提前 unmap | D7/D11 |
| multiarch selection | baseline 强制路径及当前 CPU 支持的每个 ISA 路径 | unsupported 永不执行；priority 正确；copy-to/from 在长度/对齐/chunk 边界结果一致 | D9 |
| multiarch linking | infra standalone + SDK/process image，适当的真实 DSO loading | 普通函数无需 runtime；选择只初始化一次；没有共享函数指针 | D9 |
| platform layout | Rust size/alignment/offset assertions；必要时编译平台 primitive probe | 共享同步状态在最终地址初始化、shared header 对齐、同步 ABI/旧布局拒绝 | D10 |
| content types | compile-pass/compile-fail + typed slot 行为测试 | 指针/Vec/非法表示不能成为 T；错误尺寸/对齐拒绝；内容借用阻止提前回收；不同 ring 的内容类型可不同；commit 不能重复调用 | D3/D4 |
| Session / SDK / stats | 各 crate integration | app attach/control events、TX retention/ACK release、close/reset、stats storage 不回归 | D6/D8/D11 |

步骤：批准 A1–A10 → infra ownership/storage 完整迁移 → runtime/service/app/stats
调用闭包 → 实现审查与 formatting → 最终 pre-commit gate → 测试通过立即提交。
涉及多 owner 的 commits 按依赖顺序组织。测试失败保持未提交，修复并完成 review
后重新运行同一 gate。不做本地 TUN/daemon/lab；这些只由 CI 执行。

最终候选 gate（本次设计阶段未运行）：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
git diff --check
```

全工作区测试应包含上表新增的行为测试；不能只跑某个 queue unit suite 就声称
跨 crate 迁移完成。`rg` 只用于删除/依赖文字审计，不写读取 `.rs` 的架构断言测试。
每个完成项需同时有编译/行为/回收证据。

### 9.1 VPP 测试来源与逐项迁移

本节基于第 1 节的固定 VPP 提交。主要参考文件是
[src/plugins/unittest/svm_fifo_test.c](https://github.com/FDio/vpp/blob/629fe2764bd997189fedd2d98cbe8dc9189c1ec3/src/plugins/unittest/svm_fifo_test.c)
与
[src/plugins/unittest/session_test.c](https://github.com/FDio/vpp/blob/629fe2764bd997189fedd2d98cbe8dc9189c1ec3/src/plugins/unittest/session_test.c)。
下表的 Rust 文件均为**拟新增测试**，本轮不创建测试代码、不运行 VPP 或 Hammer。

| VPP 测试及位置 | 已核对的测试行为 | Rust 文件与必须保留的断言 |
| --- | --- | --- |
| session_test.c:2107，session_test_mq_basic | descriptor 容量 16；两个容量 8、元素尺寸 8/16 的 ring；分配/释放、满小 ring 后选择大 ring、跨 ring 入队顺序、payload 123 和 0..11、最终占用清零 | hammer-infra/tests/svm_msg_queue.rs：typed 8/16-byte 内容；发布 13 条后的占用分别为 8/5，消费顺序与内容逐条一致，回收后两 ring 可再次用满；见下文显式选 ring 的差异 |
| session_test.c:1977，session_test_mq_speed；:1875，wait_for_event | fork 收发，分别使用 condvar 和 eventfd/epoll；读 fd 后重新检查 queue predicate；打印事件速率 | hammer-infra/tests/svm_msg_queue_process.rs：独立 exec 进程与真实 fd 传递，分别验证等待和通知；正确性验证序列无遗漏/重复，吞吐只作独立 benchmark，不作为功能通过证据 |
| svm_fifo_test.c:217，sfifo_test_fifo1；:479，fifo2；:869，fifo5；:1005，fifo6 | OOO 间隙、相邻与重叠区间合并、已排序/未排序输入；补洞推进可读边界 | hammer-infra/tests/svm_fifo_ooo.rs：最终字节一致，补洞前不暴露后方 bytes；保留 fifo2 的 [4,3000) 合并为一段以及前 4 bytes 补齐后可读 3000 的具体断言 |
| svm_fifo_test.c:565，sfifo_test_fifo3；:2858 起的配置组 | 可重复随机分段、overlap、in-seq-all、初始偏移、drop、顺序输入 | 同上：保留 seed=123、nsegs=10、initial-offset=3917 及上游五种组合；失败输出 seed、操作序列和首次不一致字节，能确定性重放 |
| svm_fifo_test.c:1093，sfifo_test_fifo7；:1174，fifo_large | 非二次幂容量 101、接近 u32 边界的初始位置、反向单字节 OOO、重复补洞/消费；大 FIFO 两半乱序输入 | svm_fifo_ooo.rs：100 次循环，合并前后 OOO 数量/可读量和逐字节数据；分别覆盖物理环绕与逻辑序号回绕，不把二者混成一个条件 |
| svm_fifo_test.c:1374，sfifo_test_fifo_grow；:1761，fifo_shrink | 增长/缩小、chunk 分配、不同 enqueue/dequeue 步长以及 sanity 检查 | hammer-infra/tests/svm_fifo_chunks.rs：跨 chunk 数据保持、容量变化后的可写量、空/满、分配失败后旧 FIFO 仍可读写；Rust 按本身布局计算容量，不抄 C struct 大小 |
| svm_fifo_test.c:1915，sfifo_test_fifo_indirect | 4096-byte 初始 chunk，扩展到 4 MiB；nocopy 发布跨 chunk，max_read/write_chunk 和 head/tail 所在 chunk 变化 | svm_fifo_chunks.rs：通过真实写 reservation 初始化 bytes 后 commit；跨 chunk 借用长度和读写结果正确，未初始化字节不得像 C 测试那样直接 nocopy 发布 |
| svm_fifo_test.c:2033，sfifo_test_fifo_make_rcv_wnd_zero | 4096 容量写入 3000，剩余 1096；缩至占用量后 max_enqueue 为 0 | svm_fifo_chunks.rs：精确验证 1096→0、数据仍完整、消费后容量释放；TCP advertised window 的行为另放 TCP owner 测试 |
| svm_fifo_test.c:2096，segment_hello_world；:2145，segment_fifo_grow；:2379，segment_over_max_chunk | segment 内 FIFO 填满/排空/再次填满、扩大、超过最大单 chunk 的多 chunk 数据读回 | hammer-infra/tests/svm_fifo_segment.rs：往返数据、重复使用、跨 chunk、FIFO/header/chunk 回收；额外保留不同 slice 的隔离断言 |
| svm_fifo_test.c:2546，segment_mempig；:2615，segment_prealloc | 分配至耗尽、释放后再分配；预分配 50 个 4096-byte chunks 与 50 个 headers；缓存数/字节数；空间不足的负向分配 | svm_fifo_segment.rs：可恢复的容量错误、缓存/accounting 与实际使用一致、已分配 FIFO 不被失败破坏、回收后可再次分配；不抄上游 overhead=8192+384 魔数 |
| svm_fifo_test.c:2440/:2490，segment_slave / segment_master_slave | 子进程 attach，共享 FIFO 的 Hello world 传递 | hammer-infra/tests/svm_segment_process.rs：独立 exec、不同虚拟地址映射、ready/ABI 验证、实际 payload、fd 关闭及退出清理；父进程检查 child exit status，不能仅检查共享计数 |

上游 segment_master_slave 被默认全量 segment 测试注释跳过（:2795，原因是慢），
不能声称运行上游 all 就覆盖了它。Rust 将独立进程套件放在明确的 Linux CI job，
必须显式执行，失败不得被 benchmark 或 TUN job 的结果掩盖。

src/svm/svm_test.c 只保留一个 root/subregion 创建、写入 0xdeadbeef、unmap 的
旧示例；其 region 函数实参与当前 svm_common.h 的声明不一致，且当前
src/svm/CMakeLists.txt 没有将它列为测试目标。只提取生命周期意图，不能把它当作
当前可运行的 SVM 回归套件。src/vppinfra/test_fifo.c 是另一种容器的测试，也不能
作为 svm_fifo / svm_msg_q 已覆盖的证据。

### 9.2 MQ 语义差异必须反映到测试

VPP mq_basic 的 alloc_msg 按内容字节数自动寻找可容纳的非满 ring。本 ADR 的
reserve<T>(ring) 显式指定 ring 且要求完整 T 布局匹配，不把 7-byte 内容自动换到
16-byte ring。Rust 对应测试先发布并消费一个小 ring 消息以产生 cursor 环绕起点，
再向大 ring 发布 123，向小 ring 发布 0..7，向大 ring 发布 8..11；随后断言顺序
为 123、0..11，并验证重新使用。另断言小 ring 满时 reserve 返回 RingFull，
不会暗中改用另一 ring。未提交取消不推进 tail，与 VPP alloc 后再 free 的位置
变化不同，必须单独测试，不能照抄其 element_index 期望值。

Rust 额外覆盖 descriptor/ring 容量组合 (3;5,2)、(5;2,3)、容量 1 和零容量拒绝，
区分 QueueFull 与 RingFull。每轮验证取消、commit 消耗事务、错误类型/对齐不出队、
同 ring 按序回收、不同内容 ring 往返。消费 guard 持有期间不能调用需要再次
lock 的 len/dequeue；中间 accounting 的检查放在 infra 内部、已有 guard 下的
单元测试中，外部集成测试通过释放后可重用的容量验证，不新增生产观察包装类型。

以下用例是从 SVM 实现和 Rust 设计推导的新增要求，不伪称 VPP 已有同名测试：

| Rust 测试文件 | 来源 / 场景 | 可观察通过条件 |
| --- | --- | --- |
| hammer-infra/tests/svm_queue.rs | queue.c 的 add/add2/sub/wait；内容 T、full/empty、配对插入、截止时间 | add2 容量不足完全不入队；两元素顺序保持；空队列 Option 与超时错误不同；非二次幂容量完整可用 |
| hammer-infra/tests/svm_sync_process.rs | queue.c robust 初始化/lock；本 ADR posix-sync 的 wait 重新加锁 | kill 持锁子进程后返回 OwnerDied/Closed/NotRecoverable 而非死等；condvar 返回 OwnerDead 的路径保留 source；不读半提交数据、不重复 unlock；新队列成功往返 |
| 同上 | waiter 进入条件等待后 producer 持锁退出，没有 notify | 通过有限周期重新获取 robust mutex 检出死亡；外部 deadline 只防止测试卡死，不能用 sleep 猜测线程先后顺序 |
| hammer-infra/tests/svm_segment_process.rs | ssvm.c / svm.c 的 ready、attach、成员退出；Rust offset ABI | 不同地址 attach 成功，未 ready/旧 ABI/越界/错位拒绝；失败不发布 root，不释放仍在使用的映射 |
| hammer-infra/tests/svm_multiarch.rs | svm_fifo.c 的 copy_to_chunk / copy_from_chunk 及 MULTIARCH_SOURCES | baseline 与本机支持 ISA 的真实复制结果一致；长度 0/1、向量边界 ±1、不对齐、chunk 边界；不执行不支持 ISA，输出实际选中的实现身份 |
| infra 编译失败用例（crate-local） | Rust 借用与内容表示契约 | 不能重复 commit、越过映射寿命使用 guard、提前释放仍被借用的内容、将 Vec/引用/指针当共享 T；用 Rust 编译结果证明，禁止 source-text assertion |
| runtime/tests/session_msg_queue.rs 与 app/tests/session_control.rs | Session/SDK 的实际迁移闭包 | IO/CTRL 编码及环选择、处理前释放消息 guard、满队列重试、关闭与重建、独立 app attach；不得依赖旧 SingleProducer 类型或 claim 路径 |

同步测试以 socket/pipe 的阶段握手确认“已持锁”“进入等待前”等状态；再执行
目标动作，并用有界等待、child kill/wait 回收兜底。需要精确命中 commit/release
中间窗口时使用仅测试构建可见的内部阶段控制，不能向生产 API 注入回调或在
共享 ABI 中永久增加测试字段。真实 pthread/futex 跨进程行为必须由独立进程
验证；线程测试或模型检查只能补充，不能替代。每个队列测试还要核对精确消息数、
每个生产者的顺序及有效 payload，而不是只断言最终 len=0。

### 9.3 测试落点与实施验收顺序

基础设施测试放在 crates/hammer-infra/tests/，runtime 与 SDK 接入测试各归其
crate；内部私有状态测试邻近所属模块。上文表格省略了路径共同前缀 crates/。
Linux CI 分开执行编译/lint、FIFO/队列确定性测试、跨进程同步与生命周期、
Session/SDK 集成、性能测量。进程测试无需 TUN，不启动 Hammer daemon；真正
TUN/TCP lab 仍仅由专门 CI workflow 运行。

实现完成并完成 review/formatting 后，最终提交前 gate 才运行测试。用例落地后
应至少覆盖以下命令对应的测试目标（本轮文档提交不执行这些命令）：

```bash
cargo test -p hammer-infra --test svm_msg_queue --test svm_queue
cargo test -p hammer-infra --test svm_fifo_ooo --test svm_fifo_chunks --test svm_fifo_segment
cargo test -p hammer-infra --test svm_segment_process --test svm_msg_queue_process --test svm_sync_process
cargo test -p hammer-infra --test svm_multiarch
cargo test -p hammer-runtime --test session_msg_queue
cargo test -p hammer-app --test session_control
```

这些是拟新增目标，不宣称目前存在或已通过。最后按第 9 节全工作区 gate 检查
跨 crate 影响；全部通过立即提交，不在提交后重复测试。本轮只核对 VPP 源码、
补充测试设计和执行 git diff --check；未获得任何生产行为测试通过证据。

## 10. 后续 vlibapi / vlibmemory 的 SVM 适配核对

核对范围是 vendored src/vlibapi 与 src/vlibmemory 的调用者及其 SVM 实现，
不是只查结构定义。结论是：**统一 MQ 不阻碍后续分层；仅完成 MQ 仍不足以
宣称 Binary API 共享内存路径已经可迁移或验证通过。**

| VPP 证据 | 实际需求 | Rust 设计边界与验收 |
| --- | --- | --- |
| vlibmemory/memory_client.c::vl_client_connect_to_vlib（svm_queue_alloc_and_init / svm_queue_sub）；vlibapi/api_shared.c::vl_msg_api_queue_handler | 客户端输入队列固定存 sizeof(uword) 的消息地址，支持阻塞接收 | 使用独立 SvmQueue<T>，T 是协议定义的 offset/消息身份值；不使用 Session IO/CTRL 或 SvmMsgQueue data rings |
| vlibapi/memory_shared.c::vl_msg_api_send_shmem / vl_mem_api_can_send / vl_msg_api_send_shmem_nolock；vlibmemory/memory_api.c::void_mem_api_handle_msg_i | 满队列背压、发送所有权转移、主线程消费；部分 C 调用直接使用 raw 队列操作 | Rust 的 enqueue/dequeue 由 owner 内部加锁；不要求调用方访问 header，也不复制裸指针或暴露手工解锁 |
| svm/queue.c::svm_queue_add2 / svm_queue_sub | 两元素原子插入、空/满判断、WAIT/NOWAIT/TIMEDWAIT | SvmQueue<T> 有 enqueue_pair、显式阻塞/截止时间接收；按用户要求只用 lock，NOWAIT 的锁忙即返行为是有意不保留的差异 |
| vlibapi/memory_shared.c::vl_msg_api_alloc_internal / vl_msg_api_free_w_region | API allocation rings 根据消息大小选择池；slot 可在消费者处理完成后独立释放；miss 转 region heap | 这是 Binary API 消息分配策略，不是 SvmMsgQueue 按序释放的 data ring。分配器归未来 Binary API owner，使用 infra 的共享 region 分配能力，不能把消息回收强制改成 MQ 顺序 |
| vlibmemory/socket_api.c 的 sock_init_shm handler；socket_client.c 的 sock_init_shm_reply handler | memfd 建立后在其中初始化 region、API root/rings，ready 后传 fd 并 attach | SvmSegment 管 memfd/mmap，SvmRegion 管共享 allocator/root；Unix socket 层管 SCM_RIGHTS 和注册。不同地址映射使用 offset，不依赖 C 的固定 VA |
| vlibapi/memory_shared.c::vl_map_shmem / vl_unmap_shmem_internal；svm.c::svm_region_find_or_create / svm_region_unmap_client | 命名 region、成员和 server/client 不同的退出职责 | 通用 SvmRegion/SvmRegionMain 提供能力，API 层持有自己的注册和消息身份；MQ 不持有客户端注册表 |
| vlibmemory/memory_api.c::memclnt_queue_callback | 检查输入队列是否有消息，再发 VLIB process event | 调度留在 runtime/API owner。Rust 不借用 C 的 volatile cursize 指针，不在 SVM 层注册 graph node；len/is_empty 只提供检查结果 |
| svm/queue.c::svm_queue_init / svm_queue_lock；svm.c 的 region 初始化 | 固定队列设 robust + process-shared；region 初始化只设 process-shared | posix-sync 采用 Shared + Robust；owner death 关闭旧实例并重建；不得把所有 VPP region 锁描述为 robust |

SVM 层只拥有通用共享存储、队列、分配、等待与回收协议，不包含 API 消息 ID、
处理函数表、客户端注册、版本协商或 Session 控制消息。API owner 可以调用
SvmQueue 的 typed 接口和 SvmRegion 分配接口；SVM 不反向依赖 API/runtime。
同一个 segment 中可以同时放 API queues 与 API allocation rings，但两者的
生命周期、回收顺序和容量统计必须独立。

共享 region allocator 是实际前置能力：当前 Hammer attached SvmRegion 没有
allocator，不能让各进程重新初始化带本地指针的 Talc。未来 API client 与 daemon
必须能通过同一套 offset allocator 元数据分配/释放，或在具体设计中明确改为
由拥有者处理回收请求；后者不在本轮擅自选定。存储耗尽返回所属层错误，不能
回落进程 Main Heap 后将私有地址发给 peer。

API 消息身份必须能在当前映射中检查范围、所属 region 和有效生命周期；具体
协议字段由 API owner 定稿，不从裸 u64 的可存储性推断身份有效。VPP allocation
ring 按时间强行回收旧 slot 的行为不能直接移植为 Rust 内存安全保证：需要证明
原 owner 已放弃访问。队列满时尚未发生所有权转移，消息仍归发送方；成功入队
后发送方不得再访问，通知失败不得触发重复发送。

后续验收必须包含：独立 exec 进程在不同地址 attach；固定元素 queue 的顺序、
满/空、双元素插入失败原子性、condvar 与绝对截止时间；API allocation slots
乱序释放；共享 heap miss/耗尽；发送失败仍由发送方回收；接收处理后回收；
client detach 与在途消息；持锁 peer 退出。当前没有执行这些行为测试，不能把
此次源码和文档核对写成“后续使用 SVM 已确认没有问题”。

## 11. 设计审查结论与未决证据

结论：`Design only / Linux first`。统一 MQ 方案已写入设计；当前实现尚未迁移，
未来 Binary API 的完整接入也尚未验证。

- 已决（Linux robust）：选用 posix-sync 的共享 robust mutex/condvar；按 8.4
  隔离故障实例并重建。实施必须通过 lock 与 condvar reacquire 的 owner-death
  测试，文档选型不冒充行为验证。
- Blocking（完整 Binary API 接入）：共享 region allocator、API 消息分配/回收、
  peer detach 与消息身份校验须由相应 owner 定稿并通过第 10 节验收。其中共享
  region allocator 已在 §12 给出具体类型、方法、布局与验收矩阵，但实施与验收
  仍未完成。
- Blocking（MQ 实施完成前）：eventfd 通知错误留存/处理接口、现有 Session
  消费事务的释放时机，以及跨进程互斥/唤醒测试必须补齐。
- Blocking（完整 FIFO parity）：chunk reclamation 与多 worker slice 安装需要
  所有权证明；不能以本轮 MQ 设计代替 FIFO segment 的验收。
- Non-blocking（本轮 ADR）：macOS/iOS 暂不设计；database/工具另有 API 范围。

本轮交付是 Linux MQ 的具体类型、方法、同步与删除设计，以及 Binary API
适配核对。未修改生产代码、依赖和测试，未删除现有 MultiRingMsgQueue，未声称
完成整体 SVM 重构。后续实施使用第 7/8 节清单及第 9/10 节验证矩阵。

## 12. `svm_region` 重新设计：offset heap、名字表与独立子区

本节把 §8 的 A2 细化为可实施的具体类型与方法，并给出字段映射、布局、
删除账本与验证矩阵。事实类别沿用第 1 节：`V` 是 vendored VPP 源码，`H` 是
当前 Hammer，`R` 是仓库/用户约束，`D` 是本节提议。已经落地的是 12.1 的模块
落点（纯文件移动与导入路径更新，无行为变化）；12.4/12.5/12.6/12.7 的类型与
方法仍未实现，本轮不修改任何行为、依赖或测试。

### 12.1 证据与决策

| ID | VPP 来源 | 已核实事实 | 本节决策 |
| --- | --- | --- | --- |
| V11 | `svm_common.h:28-56` | `svm_region_t` 全集含 `version/mutex/condvar/mutex_owner_pid/mutex_owner_tag/flags/virtual_base/virtual_size/region_heap/data_base/data_heap/user_ctx/bitmap_size/bitmap/region_name/backing_file/filenames/client_pids`，并注明 `region_heap` 之后才是 data 段 | 保留跨进程有意义的字段；删除绝对 VA、文件 backing 与 bitmap，见 12.2 |
| V12 | `svm.c:90-104`、`:110-113`、`:694-732` | `mutex_owner_pid/tag` 只在 `#ifdef MUTEX_DEBUG` 下由 `region_lock`/`region_unlock` 写；attach 侧发现 holder 已死后直接 `pthread_mutex_init` 重建 region 锁 | 保留两个字段；**不重建 mutex**：robust guard 返回 `Indeterminate` 时把 region 标记 Failed，后续访问返回 `OwnerDied`，沿用 §8.4 的恢复契约 |
| V13 | `svm.c:474-476`、`:498-512` | pvt heap 建在 `baseva+MMAP_PAGESIZE`（默认 128K `SVM_PVT_MHEAP_SIZE`），`data_base` 按页对齐到它之后 | 三段布局：固定 header + metadata heap + data 段，见 12.3 |
| V14 | `svm.c:801-813` | root region 的 `svm_main_region_t` 由 pvt heap 分配，并覆写 `rp->data_base` 指向它（`:812`） | root 的 `SvmRegionMain` 在 metadata heap 内，由 `data_base_offset` 指向，见 12.6 |
| V15 | `svm.c:786-800`、`:910`、`:965-969`、`:1090-1106` | 名字表 `hash_create_string`；key 是堆内名字字符串地址，value 是 subregion pool 下标；删除时先 `hash_unset_mem` 再 `vec_free` 名字 | 名字表改为 `SvmHashMap<u64>`（键字节由表持有），值是单调不复用的子区序号 |
| V16 | `svm_common.h:92`、`svm.c:262-266`、`:909-975` | root region 保留 64MB 保留 VA，用 bitmap 按页切分，子区 mmap 在 `baseva + index*MMAP_PAGESIZE`，因此依赖固定 VA | 不做 VA 切分：每个子区一个独立 `SvmSegment`，fd 经 socket + `SCM_RIGHTS` 传递；删除 `bitmap/bitmap_size` 与 `SVM_FLAGS_FILE` |
| V17 | `svm.c:483`、`:1215-1229`、`:1258-1275` | 成员表是每个 region 私有堆内的 `client_pids` vec；回收只用 `kill(pid, 0)` 探测，不记录进程启动时间 | 成员数组与计数放在 region 自己的 metadata heap；回收语义与 VPP 相同 |
| V18 | `ssvm.h:47-70`、`svm.c:326-339` | `ssvm_shared_header_t.heap` 仍在，但 `ssvm.h` 自己带 `/* TODO remove ssvm heap entirely */`；`data_heap` 只在 `SVM_FLAGS_MHEAP` 时创建 | segment 不携带 allocator；region heap 归 region owner；segment 只提供 payload 定位 |
| V19 | `mem.h:143-158`、`mem_dlmalloc.c:363-383`、`:418-423`、`:519-536` | `clib_mem_heap_alloc` 失败即 `os_out_of_memory()` 终止；`clib_mem_heap_free` 以 ASSERT 校验对象属于该堆后 `mspace_free` | `SvmRegionHeap` 不返回 `Result`；耗尽/双释放/块损坏以携带事实的违规载荷终止，见 12.4 |
| V20 | `vlibapi/api_common.h:356`、`svm.c:409-428`、`:568`、`:625`、`:856-869`、`memory_api.c:1140-1160` | `root_path` 只用于拼 `/dev/shm/<root>-<name>` 文件名，`uid`/`gid` 只用于 `fchown`，扫描与清理按同一路径进行 | 不移植 `root_path`/`uid`/`gid`：Hammer 的 rendezvous 是 socket + `SCM_RIGHTS` 传 fd，没有路径解析与 uid 复核，见 12.2 |
| V21 | `svm_common.h:46`、`:67-68`、`svm.c:262-322`、`:347-404` | `backing_file`/`backing_mmap_size` 只在 `SVM_FLAGS_FILE` 下使用：把 region 的 data 段以 `MAP_SHARED|MAP_FIXED` 映射到普通文件，使 data 内容跨 daemon 重启保留；`backing_mmap_size` 只在 backing 不是普通文件时决定映射长度；`svm_region_t.backing_file`（`svm_common.h:46`）在 `svm.c:320` 写入后全树无人读取 | 不移植 `backing_file`/`backing_mmap_size`/`SVM_FLAGS_FILE`：它是 segment backend 选择与持久化需求，不是 region 语义，见 12.2 |

模块落点：SVM owner 已独立成 `crates/hammer-infra/src/svm.rs` +
`crates/hammer-infra/src/svm/` 子树，文件划分对应 VPP `src/svm` 的职责：

```text
svm.rs              子树文档与子模块声明
svm/segment.rs      SvmSegment：映射、backing、ready
svm/region.rs       SvmRegion、SvmRegionConfig、SvmRegionMain：region authority
svm/queue.rs        SvmQueue<T>
svm/msg_queue.rs    SvmMsgQueue
svm/fifo.rs         SVM 字节 FIFO
svm/fifo_segment.rs SvmFifoSegment：FIFO 存储 owner
```

公开路径相应变为 `hammer_infra::svm::{region,segment,queue,msg_queue,fifo,fifo_segment}`；
`hammer_infra::page_size` 保留为 crate 根的重导出。新的 `svm/region_heap.rs`、
`svm/hash_map.rs` 按 12.4/12.5 落在这棵子树内。留在 crate 根部的 `segment.rs`
（旧 `Segment`）与 `multi_ring_msg_queue.rs` 是 §7 已登记的删除目标，不搬进子树。

### 12.2 `svm_region_t` 字段映射

| VPP `svm_region_t` | Hammer `SvmRegionHeader` | 说明 |
| --- | --- | --- |
| `volatile uword version` | `version: AtomicU64` | 唯一 ready 标志；初始化最后 release 写（`svm.c:513-517`），attach 先 acquire 读 |
| `pthread_mutex_t mutex` | `mutex: BorrowedMutex` | process-shared robust，posix-sync，见 §8.4 |
| `pthread_cond_t condvar` | `condvar: BorrowedCondvar` | VPP 建了 process-shared condvar，但 `svm.c` 从不 wait/notify；保留字段以免将来改变共享布局 |
| `int mutex_owner_pid` | `mutex_owner_pid: AtomicI32` | 与 `region_lock` 同步写（`svm.c:93-96`） |
| `int mutex_owner_tag` | `mutex_owner_tag: AtomicI32` | 取值来自 `RegionLockTag`，不是任意整数 |
| `uword flags` | `flags: AtomicU64` | 只保留 `REGION_FLAG_DATA_HEAP`（VPP `SVM_FLAGS_MHEAP`）与 `REGION_FLAG_SUBDIVIDED`（VPP `SVM_FLAGS_NODATA`） |
| `uword virtual_base` | 删除 | 跨进程绝对 VA 无意义；header 自身即 offset 0，`SvmSegment` 负责映射地址 |
| `uword virtual_size` | `virtual_size: u64` | attach 校验与 `layout()` 用 |
| `void *region_heap` | 删除 | heap 起点由 header 尺寸与 64 字节对齐算出，固定布局，见 12.3 |
| `void *data_base` | `data_base_offset: u64` | `SUBDIVIDED` 时指向 `SvmRegionMain`（对齐 `svm.c:812`），否则指向 data 段起点 |
| `void *data_heap` | `data_heap: SvmRegionHeap` | 仅 `REGION_FLAG_DATA_HEAP` 时有效；header 内嵌，不使用绝对地址 |
| `volatile void *user_ctx` | `user_ctx_offset: u64` | 0 = 未设置；用户上下文保存在同一 region 的堆内 |
| `uword bitmap_size`、`uword *bitmap` | 删除 | 不再切分保留 VA，见 12.6 |
| `char *region_name` | 删除 | 名字的唯一权威是 root region 的名字表；region 自身不重复保存名字 |
| `char *backing_file`、`char **filenames` | 删除 | 文件 backing 与 `SVM_FLAGS_FILE` 不移植；backing 归 `SvmSegment` |
| `uword *client_pids` | `client_pids_offset: u64`、`client_count: u64`、`client_capacity: u64` | 成员数组在 metadata heap；VPP vec 的 append/delete 语义保留，增长用 offset 块重分配 |

不移植的开关：`SVM_FLAGS_FILE`、`SVM_FLAGS_NEED_DATA_INIT`、`SVM_OVERLAY_REGION_BASEVA/SIZE/BASENAME`，
以及 `svm_main_region_t` 的 `root_path`/`uid`/`gid`（`svm_common.h:110-116`）。前三个的原因
是没有文件 backing、ready 由 `version` 单点表达、不支持 overlay 布局；`root_path`/`uid`/`gid`
的原因见下面两段。

chroot/`root_path`/`uid`/`gid` 与 region 语义无关，它们服务的是"按文件路径
rendezvous + 跨 uid 放行"：`root_path` 只用于拼 shm 文件名
（`shm_name_from_svm_map_region_args`，`svm.c:409-428`），`uid`/`gid` 只做一次
`fchown (svm_fd, uid, gid)`（`svm.c:568`、`:625`）让不同 uid 的客户端能打开该
文件，清理与扫描同样按 `/dev/shm/<root>-*` 路径进行（`memory_api.c:1140-1160`、
`svm.c:1237-1252`）；`svm_region_init_chroot_uid_gid` 就是这两件事的组合
（`svm.c:856-869`）。Hammer 的 rendezvous 是 socket + `SCM_RIGHTS` 传 fd：
`SvmSegment` 的 `shm` 后端 open 后立即 `shm_unlink`（`svm_segment.rs:260-276`），
`memfd` 后端无路径，`SvmSegmentConfig.name` 只存在于创建进程的私有 config，
权限在 `connect()` 时判定一次而不是按 uid/gid 复核。因此 `root_path` 在本设计
中没有消费者；把它放进共享区反而有害——chroot 内的绝对路径对另一 mount
namespace 的进程无意义。

`backing_file`/`backing_mmap_size`/`SVM_FLAGS_FILE` 同样不属于 region 语义：
它们只在 `SVM_FLAGS_FILE` 下把 region 的 data 段映射到普通文件，使 data 内容
跨 daemon 重启保留（`svm.c:262-322`、`:347-404`），代价是必须 `MAP_FIXED` 到
同一地址，因为 data heap 内部保存的是绝对指针；使用者只是 `persist.c` 这个
test/demo，而 `svm_region_t.backing_file`（`svm_common.h:46`）在 `svm.c:320`
写入后全树无人读取，本身就是死字段。Hammer 的 region 共享位置只有 offset，
不依赖固定 VA，也没有跨重启持久化需求；将来若需要，那是 `SvmSegment` 的
backend 选项，`SvmRegion::attach` 只需把旧文件内容当不可信输入并先做
magic/version/flags/size/heap 边界校验。

### 12.3 三段布局

```text
SvmSegment payload（offset 0 = region 起点）
  [0, sizeof(SvmRegionHeader))           固定元数据：version/mutex/condvar/
                                         flags/两个 heap/成员表头
  align_up(sizeof(header), 64)           SvmRegionHeap（metadata heap）起点
  data_base_offset ─┬─ SUBDIVIDED（root）：SvmRegionMain（名字表 + 子区序号）
                    └─ 普通 region：data 段起点；data_heap_offset != 0 时为 data heap 起点
```

- 共享位置一律用 payload 内 offset；map 由 `SvmRegion` 在 attach 后建立，不进入共享区。
- 没有固定 VA、没有页粒度切分、没有 64MB 保留区间；子区是独立 segment（V16）。
- header 不得放进 heap：`region_heap` 字段因此删除，heap 的 `heap_start` 由 header 尺寸算出。
- 两个 `SvmRegionHeap` 都是 header 的字段，范围在 `create` 时切定、之后不变
  （`header_end = align_up (sizeof (header), 64)`）：
  `SUBDIVIDED`（root）只有 metadata heap，覆盖 `[header_end, payload_len)`，
  `data_base_offset` 指向由它分配的 `SvmRegionMain`（对齐 `svm.c:812`）；
  普通 region 的 metadata heap 覆盖 `[header_end, data_base_offset)`，
  带 `REGION_FLAG_DATA_HEAP` 时 data heap 覆盖 `[data_base_offset, payload_len)`，
  不带标志时 `data_base_offset` 之后是调用方自管的数据段。
- 发布顺序：初始化 mutex/condvar/heap/成员/名字表，最后 release 写 `version`；attach 先
  acquire 读 `version`，再校验 magic、size、flags、offset 边界与对齐，任何失败都不发布。
- `SvmRegionHeader` 含 `#[repr(C, align(64))]`；所有字段宽度固定，跨 32/64 位进程不互通
  （沿用 D10：不承诺 C 二进制兼容，只承诺同架构、同版本 Hammer 进程互通）。

### 12.4 `SvmRegionHeap`

`DLmalloc` 的 offset 化等价物：块头保留相邻块合并所需的 size/flag，空闲块按大小链入
定长 bin，用户区前的 8 字节前缀回指块头，从而让 `deallocate` 在没有进程私有指针的
情况下完成校验与合并。已落地在 `hammer_infra::svm::region_heap`（测试
`crates/hammer-infra/tests/svm_region_heap.rs`）。

```rust
// hammer_infra::svm::region_heap
pub const SVM_REGION_HEAP_BINS: usize = 64;

const FLAG_PREVIOUS_IN_USE: u64 = 1 << 0; // 相邻前一块在使用，同 dlmalloc PREV_INUSE
const FLAG_CURRENT_IN_USE: u64 = 1 << 1;
const BLOCK_HEADER_SIZE: u64 = 16; // [previous_size][size_and_flags]
const USER_PREFIX_SIZE: u64 = 8;   // 用户区前 8 字节：本块块头 offset
const MIN_BLOCK_SIZE: u64 = 40;    // 块头 + 前缀 + 两个 bin 链指针

// 块布局：
//   [block + 0,  block + 16)   previous_size / size_and_flags
//   [block + 16, block + 24)   back-pointer：块头 offset，deallocate 恢复块头的唯一依据
//   [block + 24, block + size) 用户数据；块空闲时尾部 16 字节是 next/previous bin 链

#[repr(C)]
pub struct SvmRegionHeap {
    free_bins: [u64; SVM_REGION_HEAP_BINS], // 各 bin 空闲链头 offset；0 = 空
    heap_start: u64,                        // 管理区起点（含本 header）
    heap_end: u64,
    free_bytes: u64,
    used_bytes: u64,
    peak_used_bytes: u64,
}

impl SvmRegionHeap {
    pub const fn new() -> Self;
    pub fn initialize(&mut self, arena: &mut [u8], heap_start: u64, heap_end: u64);
    pub fn allocate(&mut self, arena: &mut [u8], layout: Layout) -> u64;
    pub fn reallocate(
        &mut self,
        arena: &mut [u8],
        offset: u64,
        layout: Layout,
        new_size: usize,
    ) -> u64;
    pub fn deallocate(&mut self, arena: &mut [u8], offset: u64, layout: Layout);
    pub fn holds(&self, arena: &[u8], offset: u64, layout: Layout) -> bool;
    pub fn free_bytes(&self) -> u64;
    pub fn used_bytes(&self) -> u64;
    pub fn peak_used_bytes(&self) -> u64;
    pub fn bytes_at<'a>(&self, arena: &'a [u8], offset: u64, length: u64) -> &'a [u8];
    pub fn bytes_at_mut<'a>(
        &mut self,
        arena: &'a mut [u8],
        offset: u64,
        length: u64,
    ) -> &'a mut [u8];
}
```

每个方法都接收当前映射的字节视图 `arena`：descriptor 只保存 offset 与计数，不保存
指向数据的进程私有指针，因此既不引入 wrapper，也不要求 heap 与它管理的字节同处一个
Rust 值；调用方每次调用借一次映射即可。

bin 链放在空闲块的尾部而不是 dlmalloc 的用户区头部：用户区前的 back-pointer 是 offset
heap 里从用户 offset 找回块头的唯一依据，若被链指针覆盖，重复释放会被误报成
`NotAllocated`。放在尾部后，双释放能准确报出 `DoubleFree`。

生命周期与所有权：

- **归属**：heap 归它所在的 region。共享侧只有两个 `SvmRegionHeader` 字段（metadata heap
  与 data heap），它们不在自己管理的 arena 内，否则无法完成第一次分配。进程侧
  `SvmRegion` 只是句柄，不缓存 `*mut SvmRegionHeap`，每次按 `header_offset` 在当前映射
  现取 `&SvmRegionHeap`/`&mut SvmRegionHeap`；`SvmHashMap` 等使用者每次调用借一次；
  `SvmSegment` 不持有、也不知道 heap（V18）。
- **无析构**：不实现 `Drop`，没有 `destroy`，也没有“把 heap 拷出共享区”的按值持有方式；
  把 descriptor 拷出后它与真实空闲链立刻脱节。删除 region 只有一条路径：先从 root
  名字表移除名字（客户端不再可能找到），再由 daemon 关闭对应 `SvmSegment`，映射消失
  即 heap 字节消失。
- **初始化只发生一次**：只有 `SvmRegion::create` 在 `state == Uninitialized`、持有 region
  锁、且在 release 发布 `version` 之前调用 `initialize`；shm/memfd 初始为零页，因此初始
  状态确定——`[heap_start, heap_end)` 一个整块空闲。`attach` 只校验，**绝不初始化或
  修复**，否则两个进程都会自认为 owner（即 VPP attach 侧重建 Talc 的错误翻版）。
- **范围在 create 时切定**，堆范围本身不增长；`SvmRegionHeap` 内的内容（名字表桶、键
  字节、成员数组、`user_ctx`、`SvmRegionMain`）按需 allocate/reallocate。
- **进程死亡不改变所有权**：某个客户端进程退出只回收它自己的映射；heap 仍在其他映射
  中有效，直到该 region 的最后一个成员 unmap，或 daemon 拆除该 segment。owner death
  后 region 置 `Failed`，heap 不再使用也不就地修复，随 segment 拆除一起消失。
- **借用纪律**：`&mut SvmRegionHeap` 只允许在 `RegionLock` 内取得；不得跨锁释放保留，
  也不得同时持有两个 heap 的可变借用去跨界分配。

契约：

- **没有 `Result`，没有 error 类型。** 空间不足、offset 落在 heap 外、对齐不匹配、
  双释放、bin 链或块大小损坏，一律构造 `SvmRegionHeapViolation` 并把结构化事实
  写到 stderr 后 `std::process::abort()`（对齐 V19）。共享堆一旦损坏，其他进程可能
  正在使用，unwind 会让同一 region 处于不可判定状态。
- 终止载荷是单一枚举，不另造只被嵌套的 wrapper，每个变体只陈述一类可诊断事实：
  `NotInitialized { heap_start, heap_end, arena_len }`、`InvalidRange { .. }`、
  `Exhausted { requested, alignment, free_bytes }`、`LayoutOverflow { size, alignment }`、
  `ZeroSizeReallocation { offset }`、`Misaligned { offset, alignment }`、
  `OutOfRange { offset, length, heap_end, arena_len }`、`NotAllocated { offset }`、
  `DoubleFree { offset }`、`BlockCorruption { offset, declared_size, previous_size, heap_end }`、
  `BinCorruption { bin, offset }`。
- 对齐：`layout.align()` 向上取到 8 字节档；每个块（含空闲块与切分出的尾块）都不小于
  `MIN_BLOCK_SIZE`，因此释放后一定能放下块头、前缀与 bin 链。用户 offset 满足
  `offset % align == 0`，且等于由块头与 `align` 反推的位置；`deallocate`/`holds` 都按
  这一条校验，所以按分配时的 `layout` 调用才能通过，内部 offset 一律被拒。
- `reallocate` 遵循 dlmalloc：原地扩缩可行则原地完成，否则新分配、拷贝 `min(old, new)`、
  释放旧块；收缩只在前后两半都不小于 `MIN_BLOCK_SIZE` 时切分；`new_size == 0` 视为非法
  （调用方应 `deallocate`）。
- heap 不自带锁：跨进程互斥由 region 的 `RegionLock` 承担（VPP 建的是 `is_locked=1`
  的 mspace，Hammer 用 region mutex 代替，因为 offset heap 里放不下进程私有的锁状态）。
- `free_bytes`/`used_bytes`/`peak_used_bytes` 是单调可核对的自有统计，不外推为
  “映射真实占用”。

### 12.5 `SvmHashMap<V>`

与 `std::collections::HashMap` 语义一一对应的共享表：相同的替换/删除/计数/容量/迭代
语义，唯一的差别是底层 allocator 换成 offset arena，且键是 arena 内字节串。

```rust
// hammer_infra::svm::hash_map
#[repr(u8)]
enum SvmSlotState { Empty = 0, Occupied = 1, Vacant = 2 }

#[repr(C)]
struct SvmSlot<V> {
    state: SvmSlotState,
    _align: [u8; 7],
    name_offset: u64,   // 键字节块在 arena 内的 offset；0 = 无
    name_len: u64,
    value: V,
}

#[repr(C)]
pub struct SvmHashMap<V> {
    buckets_offset: u64,   // SvmSlot<V>[bucket_capacity] 的 offset；0 = 未建表
    bucket_capacity: u64,  // 2 的幂，最小 64（VPP hash.c:646-650）
    occupied: u64,
    vacant: u64,
    _value: PhantomData<V>,
}
```

```rust
impl<V: IntoBytes + FromBytes + Immutable + KnownLayout> SvmHashMap<V> {
    pub const fn new() -> Self;
    pub fn with_capacity(arena: &mut SvmRegionHeap, capacity: usize) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn capacity(&self) -> usize; // 不再扩容可容纳的元素数 = bucket_capacity * 3 / 4

    pub fn contains_key(&self, arena: &SvmRegionHeap, name: &str) -> bool;
    pub fn get<'a>(&self, arena: &'a SvmRegionHeap, name: &str) -> Option<&'a V>;
    pub fn get_mut<'a>(&mut self, arena: &'a mut SvmRegionHeap, name: &str) -> Option<&'a mut V>;
    pub fn get_key_value<'a>(&self, arena: &'a SvmRegionHeap, name: &str) -> Option<(&'a str, &'a V)>;
    pub fn insert(&mut self, arena: &mut SvmRegionHeap, name: &str, value: V) -> Option<V>;
    pub fn remove(&mut self, arena: &mut SvmRegionHeap, name: &str) -> Option<V>;
    pub fn entry<'a>(&'a mut self, arena: &'a mut SvmRegionHeap, name: &str) -> SvmEntry<'a, V>;
    pub fn clear(&mut self, arena: &mut SvmRegionHeap);
    pub fn reserve(&mut self, arena: &mut SvmRegionHeap, additional: usize);
    pub fn shrink_to_fit(&mut self, arena: &mut SvmRegionHeap);

    pub fn iter<'a>(&self, arena: &'a SvmRegionHeap) -> SvmIter<'a, V>;
    pub fn keys<'a>(&self, arena: &'a SvmRegionHeap) -> SvmKeys<'a>;
    pub fn values<'a>(&self, arena: &'a SvmRegionHeap) -> SvmValues<'a, V>;
    pub fn values_mut<'a>(&mut self, arena: &'a mut SvmRegionHeap) -> SvmValuesMut<'a, V>;

    fn slots<'a>(&self, arena: &'a SvmRegionHeap) -> &'a [SvmSlot<V>];
    fn slots_mut<'a>(&mut self, arena: &'a mut SvmRegionHeap) -> &'a mut [SvmSlot<V>];
    fn find_occupied(&self, arena: &SvmRegionHeap, name: &str) -> Option<u64>;
    fn find_insert_slot(&self, arena: &SvmRegionHeap, name: &str) -> u64;
    fn grow(&mut self, arena: &mut SvmRegionHeap, capacity: u64);
    fn release_key(&mut self, arena: &mut SvmRegionHeap, slot: u64);
}

impl<'a, V> SvmEntry<'a, V> {
    pub fn key(&self) -> &str;
    pub fn or_insert(self, value: V) -> &'a mut V;
}
impl<'a, V> SvmOccupiedEntry<'a, V> {
    pub fn key(&self) -> &str;
    pub fn get(&self) -> &V;
    pub fn get_mut(&mut self) -> &mut V;
    pub fn into_mut(self) -> &'a mut V;
    pub fn insert(&mut self, value: V) -> V;
    pub fn remove(self) -> V;
}
impl<'a, V> SvmVacantEntry<'a, V> {
    pub fn key(&self) -> &str;
    pub fn insert(self, value: V) -> &'a mut V;
}

fn hash_name(name: &str) -> u64;                       // DefaultHasher::new() + 字节 + 长度
fn probe_start(hash: u64, bucket_capacity: u64) -> u64; // hash & (bucket_capacity - 1)
fn name_at<'a>(arena: &'a SvmRegionHeap, offset: u64, len: u64) -> &'a str;
```

与 `HashMap` 一致的语义：

- `insert` 命中已有键 → 替换值、返回旧值、`len` 不变；未命中 → 新增、返回 `None`。
- `get`/`get_mut`/`contains_key` 只按键内容（长度 + 字节）匹配；`len` = `occupied`；
  `capacity` 是"不再扩容可容纳的元素数"；`clear` 后 `len == 0`，桶数组不释放。
- 探测：线性探测，遇 `Empty` 终止；`Vacant` 不终止查找，但插入优先复用第一个 `Vacant`。
- 扩容 3/4、收缩 1/4 且 `len > 32`，最小 64 桶（对齐 VPP `hash.c:499-509`、`:628-637`）。
- 迭代顺序不保证，与 std 相同。
- 空间不足走 `SvmRegionHeapViolation` 终止（V19）；容量参数导致的 `usize` 溢出是调用方
  bug，panic 并带请求容量。

与 `HashMap` 的差异，全部有明确原因：

| 差异 | 原因 |
| --- | --- |
| 每个方法多一个 `arena: &SvmRegionHeap` 参数 | 表在共享区，自己不能持有本进程 allocator 句柄 |
| 哈希用固定种子 `DefaultHasher`，不用 `RandomState` | `RandomState` 每进程随机种子，会让另一进程查不到同名项；`DefaultHasher::new()` 跨进程结果一致（同一二进制） |
| 键是 `&str`，表在 arena 内持有键字节 | `String`/`Box<str>` 在共享区不可移植；键由表持有后，`remove`/`clear` 能一次性释放，不会悬空（VPP 用 `hash_unset_mem_free` 达到同样所有权，`hash.h:256-270`） |
| 没有 `remove_entry`、`drain` | std 语义要求把 `K` 交还调用方；键字节在 arena 内，交出借用后立刻释放即悬空。用 `keys()` + `remove()` 等价替代 |
| 没有 `retain`、`and_modify`、`or_insert_with`、`or_default` | AGENTS 禁止闭包中介访问既有状态；需要值时用 `entry` + `or_insert` |
| 没有 `Extend`、`FromIterator`、`Index`、`Clone`、`Debug`、`PartialEq` | 都需要 arena 参数或对共享表无意义；`Clone` 复制跨进程表本身是错的 |
| 表不实现 `Drop` | 表住在共享区，任何一个进程的析构都会破坏其他进程；释放必须显式 `clear(arena)`。`SvmVacantEntry` 是进程内句柄，未插入时由它的 `Drop` 释放已分配的键块 |
| 值类型约束 `IntoBytes + FromBytes + Immutable + KnownLayout` | 与 A5 的 `SvmQueue<T>` 同一套共享内容约束 |

### 12.6 子区与名字注册表

```rust
#[repr(C, align(64))]
pub struct SvmRegionMain {
    subregions: SvmHashMap<u64>,      // 名字 → 子区序号
    next_subregion_id: AtomicU64,     // 单调、不复用；0 保留表示无效
}

impl SvmRegionMain {
    pub fn find_or_create(&mut self, arena: &mut SvmRegionHeap, name: &str) -> (u64, bool);
    pub fn subregion_id(&self, arena: &SvmRegionHeap, name: &str) -> Option<u64>;
    pub fn remove(&mut self, arena: &mut SvmRegionHeap, name: &str) -> Option<u64>;
    pub fn subregion_count(&self) -> u64;
    pub fn subregion_names<'a>(&self, arena: &'a SvmRegionHeap) -> SvmKeys<'a>;
    pub fn next_subregion_id(&self) -> u64;
}
```

- 位置：`SvmRegionMain` 在 root region 的 metadata heap，`SvmRegionHeader::data_base_offset`
  指向它（对齐 `svm.c:812`）。
- VPP 的 `svm_subregion_t` 池消失：它只承担 (a) 持有名字字符串、(b) 作为 hash value、
  (c) 枚举名字（`svmtool.c:64-65`、`:318-319`）。`SvmHashMap` 自己持有键字节、值是
  子区序号、`subregion_names()` 提供枚举，三件事都已覆盖。因此先前草稿中的
  `SvmSubregion`、`SvmSubregionTable`、`SvmRegionNameHash`、`RegionName` 都**不引入**。
- 值 = 单调不复用的子区序号。VPP 用 pool 下标，`pool_put` 后可被 `pool_get` 复用
  （`svm.c:1103-1105`）；不复用可保证 stale 句柄不会静默指向另一个子区。
- `find_or_create` 是唯一会改共享状态的入口，且只有一次插入；调用方（daemon 侧注册
  owner）创建/attach 子区 `SvmSegment` 失败时必须调用 `remove` 回滚名字。顺序契约：
  先登记名字再让段对客户端可见，撤销时先把段从客户端可见集合移除再删名字，任何时刻
  都不得出现"名字可见但没有段"的持续状态。
- 子区与 root 的段关系、fd 传递（socket + `SCM_RIGHTS`）由 SDK/daemon 的注册 owner 执行；
  `svm_region` 只提供上面的原子单步和子区自己的 `SvmRegion` 视图。

### 12.7 `SvmRegion`、锁与成员

```rust
pub struct SvmRegionConfig {
    pub size: u64,
    pub flags: u64,
}

pub struct SvmRegion {
    segment: Arc<SvmSegment>,
    header_offset: u64,
}

#[repr(u32)]
pub enum SvmRegionState { Uninitialized = 0, Ready = 1, Failed = 2 }

impl SvmRegion {
    pub fn layout(config: &SvmRegionConfig) -> Result<Layout, SvmRegionError>;
    pub fn create(segment: Arc<SvmSegment>, config: &SvmRegionConfig) -> Result<Self, SvmRegionError>;
    pub fn attach(segment: Arc<SvmSegment>) -> Result<Self, SvmRegionError>;

    pub fn segment(&self) -> &Arc<SvmSegment>;
    pub fn header_offset(&self) -> u64;
    pub fn virtual_size(&self) -> Result<u64, SvmRegionError>;
    pub fn flags(&self) -> Result<u64, SvmRegionError>;
    pub fn state(&self) -> Result<SvmRegionState, SvmRegionError>;

    pub fn allocate(&self, layout: Layout) -> Result<u64, SvmRegionError>;
    pub fn reallocate(&self, offset: u64, layout: Layout, new_size: usize) -> Result<u64, SvmRegionError>;
    pub fn deallocate(&self, offset: u64, layout: Layout) -> Result<(), SvmRegionError>;
    pub fn free_bytes(&self) -> Result<u64, SvmRegionError>;
    pub fn used_bytes(&self) -> Result<u64, SvmRegionError>;

    pub fn publish_root(&self, offset: u64) -> Result<(), SvmRegionError>;
    pub fn root(&self) -> Result<Option<u64>, SvmRegionError>;
    pub fn main(&self) -> Result<&SvmRegionMain, SvmRegionError>;   // 仅 SUBDIVIDED

    pub fn join(&self) -> Result<RegionMembership<'_>, SvmRegionError>;
    pub fn member_count(&self) -> Result<u64, SvmRegionError>;
    pub fn client_pids(&self) -> Result<&[i32], SvmRegionError>;
    pub fn remove_exited_members(&self) -> Result<usize, SvmRegionError>;

    fn lock(&self) -> Result<RegionLock<'_>, SvmRegionError>;
}

pub struct RegionLock<'a> { /* posix-sync robust guard */ }
pub struct RegionMembership<'a> { /* RAII：Drop 时从 client_pids 移除本 pid */ }
```

- `lock()` 是私有入口：robust guard 返回 `Indeterminate` 时把 `state` 置 `Failed` 并返回
  `SvmRegionError::OwnerDied`，**不重建 mutex**（V12）。
- `RegionLockTag` 是枚举（`RegionLockTag::{Init, Attach, Unmap, Scan}`），写入
  `mutex_owner_tag`，与 VPP 的 `tag` 参数一一对应（`svm.c:471`、`:545`、`:1163`、`:1240`）。
- 锁序：root → subregion，永不反向（`svm.c:37-39`、`:1280-1295`）。跨进程死锁检查依赖
  这个全局顺序，debug 构建断言反向获取。
- 成员回收只用 pid 探测（`kill(pid, 0)`），不记录进程启动时间，与 V17 相同；不引入 pid
  复用防护的额外字段。
- `SvmRegion` 不读 `SvmSegment::ready`、不碰 fd、不调用 `munmap`；这些属于 `SvmSegment`。
  `SvmSegment` 需要新增 `payload_offset()`/`payload_len()`（A1 的补充，见 12.12）。

### 12.8 错误契约

`SvmRegionError`（`hammer-infra`，owner 是 region）：

| 变体 | 事实字段 | 可行动调用者 | 恢复与原子性 |
| --- | --- | --- | --- |
| `InvalidMagic` | `found: u64` | attach | 拒绝 attach，关闭本次 fd，不发布任何状态 |
| `UnsupportedVersion` | `found: u64`, `expected: u64` | attach | 同上；不尝试降级 |
| `NotReady` | `state: SvmRegionState` | attach/访问 | 等待并重试，或按调用方策略放弃 |
| `RegionFailed` | `mutex_owner_pid: i32` | 任何访问 | 该 region 不再可用；调用方重建新 region，不复用本映射 |
| `OwnerDied` | `pid: i32` | 持锁方 | 明确区分于"锁忙"；不允许假装成功 |
| `InvalidBounds` | `offset: u64`, `length: u64`, `size: u64` | 任何 offset 使用方 | 拒绝操作，不部分写入 |
| `Misaligned` | `offset: u64`, `alignment: usize` | 任何 offset 使用方 | 同上 |
| `LayoutMismatch` | `declared: u64`, `expected: u64` | attach | 拒绝 attach（版本相同但布局不同） |
| `InvalidRegionName` | `length: u64` | 名字表调用方 | 拒绝插入；空名/超长名不进入共享状态 |
| `InvalidRoot` | `offset: u64` | 任何 root 使用方 | 拒绝；不把非法 offset 当成"没有 root" |
| `MemberProbeUnavailable` | `pid: i32`, `source` | 成员回收方 | 保留条目，稍后重试；不得当成成员已退出 |
| `UnsupportedOperation` | `operation: RegionOperation` | 调用方 | 例如对 `SUBDIVIDED` region 请求 data heap |
| `Lock` | `source` | 持锁方 | 原始 errno 保留为 `#[source]`，不转成字符串 |
| `Segment` | `source: SvmSegmentError` | 创建/attach | 语义未改变，用 `#[from]`；其余显式构造 |

不提供万能变体（`Internal`/`Other`/`Message`/`Subsystem`）。堆耗尽与堆损坏不进这个
枚举，按 12.4 终止。

### 12.9 层隔离契约

| 层 | 允许 | 禁止 | 验证 |
| --- | --- | --- | --- |
| `SvmSegment` | libc backing/mapping、fd、backend、ready、payload offset 定位 | allocator、region 语义、名字 | infra 独立编译；不同地址 attach |
| `SvmRegionHeap` | 只操作自己 `[heap_start, heap_end)` 的 offset；失败终止 | 读 segment ready、碰 fd、访问 heap 外 | 子进程断言终止；块合并统计 |
| `SvmHashMap` | 通过传入的 `&SvmRegionHeap` 分配/释放/借用；键字节由自己持有 | 持有进程 allocator 句柄、内部锁、`Drop` | 跨进程读写同一张表 |
| `SvmRegion` | heap、成员、root、锁 | fd/munmap/segment ready、runtime/service/plugin 状态 | infra 独立编译；并发与 owner-death 测试 |
| `SvmRegionMain` | root region 的名字表与子区序号 | 子区段创建、fd 传递、SDK 策略 | 多进程 find_or_create/remove |

不新增 `SvmHashMap` 之外的通用泛型容器；`Bihash`（指针槽、`Arc<Heap>`、hazard slot、
无字节键，`bihash/key.rs:11-61`）与 `Pool`/`Bitmap`/`RbTree`（`Vec` 支撑）都不能放进
共享区，这一点由本节的布局约束保证，不再另立“共享容器 trait”。

### 12.10 删除账本

| 现状 | 处理 | 证明 |
| --- | --- | --- |
| `svm/region.rs` 的 `SvmRegionInner`（Talc + `RawSpinlock`）、`SvmRegionConfig::data_offset` | 删除，替换为 12.3/12.7 的 header + offset heap | 旧符号搜索为零；跨进程 allocator 测试 |
| `svm/region.rs` 的 bump `next_offset`/`free()`（`free` 不回收）、attach 侧 `alloc_layout` panic | 删除，由 `SvmRegionHeap` 承担分配与回收 | 释放后可复用；双释放终止 |
| `svm/region.rs` 自带 `memfd_create`/`mmap` 路径 | 删除，全部经 `SvmSegment` | region 不再直接持有 fd |
| `svm/region.rs` 的 `RegionMetadata`（magic/version/ready/next_offset/end_offset/root_offset/members） | 删除，字段按 12.2 映射到 `SvmRegionHeader` | 字段映射表逐项核对 |
| `heap.rs:49-52` 的 `HeapError::AttachedSvmRegion` 拒绝路径 | 保留到 `Segment` 删除完成；attached region 不再回到 Main Heap | 迁移后删除该变体 |
| `segment.rs::Segment` 旧包装 | 只登记删除，本轮不动 | 迁移清单 |
| `SvmRegionMain` 的 `root_path`/`uid`/`gid`、`bitmap`/`bitmap_size`、`filenames`/`backing_file`/`backing_mmap_size`、`SVM_FLAGS_FILE`/`NEED_DATA_INIT`/`OVERLAY_*` | 不移植 | 12.2 的删除说明 |

### 12.11 验证矩阵

| 行为 | 方式 | 通过条件 |
| --- | --- | --- |
| 跨进程 attach（不同 VA） | 独立 exec 进程经 `SCM_RIGHTS` 拿 fd 后 attach | version/magic/flags/size 校验通过；共享位置的 offset 在两进程都指向同一逻辑对象 |
| 名字表跨进程可见 | 进程 A `find_or_create`，进程 B 查同一名字 | B 得到相同子区序号；两个 exec 进程对同一名字得到同一 `hash_name` |
| 名字表语义 | 插入/替换/删除/墓碑复用/3-4 扩容/1-4 收缩/`clear` | `len`、`capacity`、返回值、迭代集合与 std `HashMap` 行为一致 |
| heap 模块（已实现，`crates/hammer-infra/tests/svm_region_heap.rs`） | 对齐与清零、块头计费、释放后同 layout 复用、双向相邻合并、500 块占满再逐块释放、grow 拷贝/原地 shrink、尾块不足最小块时不切分、0 字节请求、`holds` 拒绝内部与越界与更强对齐、未初始化查询、3 个种子 × 2000 步混合 allocate/reallocate/deallocate 序列 | 每一步 `free_bytes + used_bytes == heap_end - heap_start`；活跃块互不重叠且 `holds` 成立；序列结束后整堆可被一个块重新分配 |
| heap 违规（已实现，同文件子进程用例） | 双释放、耗尽、未对齐释放、非起始 offset 释放、段外读取、越过块的读取、非法范围初始化、0 长度 `reallocate`、超大 `reallocate`、被写坏的块头 | 子进程 abort（`status.code() == None`），stderr 携带对应变体的结构化事实 |
| 子区 | `find_or_create` 幂等；`remove` 后名字不可见；序号不复用 | 段创建失败时名字被回滚，无"名字可见但无段"状态 |
| 成员 | 多进程 join/离开；`remove_exited_members` 回收 dead pid | 条目数与真实成员一致；探测失败不误删 |
| owner death | 持锁进程被 `SIGKILL` 后另一进程访问 | 返回 `OwnerDied` 并把 region 置 `Failed`；不重建 mutex、不假装成功 |
| 锁序 | root/subregion 双层获取；debug 断言反向获取 | 无死锁；反向顺序被断言捕获 |
| 版本/布局 | 篡改 `version`/magic/size/flags 后 attach | 分别返回 `UnsupportedVersion`/`InvalidMagic`/`LayoutMismatch`/`InvalidBounds` |

### 12.12 批准请求与未决项

- A11 `SvmRegionHeap`、A12 `SvmHashMap<V>` 及其句柄类型、A13 `SvmRegionMain`/`SvmRegion`/
  `SvmRegionConfig`/`SvmRegionHeader`/`SvmRegionState`/`SvmRegionError`/`RegionLock`/
  `RegionMembership` 的具体化，取代 A2 的粗粒度描述。批准必须覆盖本节的行为契约。
- A1 补充：`SvmSegment` 新增 `payload_offset()`/`payload_len()`；`SvmSegment` 仍不携带
  allocator（V18）。
- `SvmHashMap` 的已撤回提案：不在 `hammer-infra` 新增 `hash_bytes`，改用固定种子
  `DefaultHasher`；若后续要求仓库自有哈希函数，它需要单独批准并补进本节。
- 已完成：A11 `SvmRegionHeap` 的具体化与测试（`crates/hammer-infra/src/svm/region_heap.rs`、
  `crates/hammer-infra/tests/svm_region_heap.rs`）。A12/A13 仍未实现。
- 未决：`SvmRegionHeap` 是否需要 `peak` 之外的压力统计（例如最大连续空闲块）、
  以及子区序号的持久化语义（重启后是否必须保持）。这两项不影响本节的类型与
  方法形状，实施前定稿即可。
