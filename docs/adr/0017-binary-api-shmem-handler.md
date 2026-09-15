# ADR-0017：VAPI SHM create-v2 客户端与服务器协议边界

- 日期：2026-09-15
- 状态：修订中的设计与实施记录；最终集成验证尚未完成。
- 范围：ordinary SVM、create-v2、delete、keepalive、control_ping；socket 只修正 File/Process 职责。
- 复用：ADR-0001 异步 Process、ADR-0015 Api/codec、ADR-0016 SVM/MsgBuf。

## 1. 决策

客户端依据是 `vpp-api/vapi/vapi.c` 的 SHM create-v2 路径。
“v2”指 `memclnt_create_v2` 握手，不是对 VAPI 整个库的版本命名。
`vlibmemory/memory_client.c` 的旧 create、RX pthread 和 setjmp 不移植。
VAPI 的 context 由调用方持有，Hammer 对应一个实际的 `memory_client::Client` 值；
应用用同一线程上的 async 方法驱动它。没有额外接收线程，没有客户端全局 handler 安装。

`ApiMain` 只保存消息注册、映射、服务器 registration pool 和共享消息表。
它不实现 control_ping、create/delete/keepalive 消息的接收 handler，不保存客户端 outstanding
requests、连接标记、回调或 Process 等待状态。线程身份复用 runtime 的已有主线程事实，
不增加 `owner_thread` 或客户端连接 TLS；保留 `my_api_main` 作为当前 ApiMain 的选择器。
选择器不拥有服务器状态，daemon 不为 Data Worker 创建 ApiMain 副本。
registration 查找、回收和死客户端扫描属于 ApiMain 自身的状态维护，使用 owner 方法。

服务器调用链保持 `dispatch_message::<T>` → safe `handler<T: Api>(t: T)` → safe
具体 `fn(Message)`。具体 handler 在协议模块内直接完成验证、mutation、回复编码和发送。
不得由具体 handler 再转发给 `ApiMain` 的同名业务方法，也不保留 `send_registration_reply`。

Process 保留原 Tokio task、JoinHandle、事件 mpsc、Process factory 和运行时生命周期。
VPP 的 longjmp 协程挂起/恢复由 async Future 的 Pending/Waker 表达。
`SocketMain` 持有 socket 状态，File callback 做 I/O；Process 只处理完整消息和延后删除。
这修订 ADR-0001 中“连接 Pool 由 Process 持有”的表达，保留其主 OS thread、合作调度和 worker 隔离。

## 2. VPP 源码依据：定义必须连到调用和释放

以下链接均指向 vendored 源码。行号用于定位，符号和调用关系是判断依据。

| 编号 | 定义、调用点 | 核对结果 |
| --- | --- | --- |
| V1 | [`vlib/node.h:612`](../../third_party/vpp/src/vlib/node.h#L612)，`vlib_process_t`；[`vlib/node.h:750`](../../third_party/vpp/src/vlib/node.h#L750)，`vlib_node_main_t` | Process 自身含 node runtime、运行状态、挂起和事件状态；Node Main 持有 processes、restore current/next、current process index 和下一就绪时间。 |
| V2 | [`vlib/node.c:487`](../../third_party/vpp/src/vlib/node.c#L487)，`vlib_register_node` 的 process 分支；[`vlib/main.c:1227`](../../third_party/vpp/src/vlib/main.c#L1227)，`dispatch_process`；`:1306`，`dispatch_suspended_process`；`:1479`、`:1614` 的主循环调用 | 注册为 Process 后由 graph runtime 分配和执行；启动与恢复都设置 current process，运行后恢复；计时器和事件恢复由主循环处理，不由 API Main 执行。 |
| V3 | [`vlib/node_funcs.h:813`](../../third_party/vpp/src/vlib/node_funcs.h#L813)，`vlib_process_wait_for_event`；`:987`，`vlib_process_signal_event_helper` | 等待前检查已有事件；事件写入 Process 的 pending data；运行中的 Process 不重复入恢复队列；事件唤醒 event-or-clock 等待时撤销相应计时器。 |
| V4 | [`vlibmemory/memclnt_api.c:307`](../../third_party/vpp/src/vlibmemory/memclnt_api.c#L307)，`vl_api_clnt_process`；`:483`，`vl_api_clnt_node` | `api-rx-from-ring` 是 `VLIB_NODE_TYPE_PROCESS`。函数局部变量保存 drain 起点、sleep、scan deadline、private rotor、event data；这些变量不在 `api_main_t` 中。 |
| V5 | [`vlibmemory/memory_api.c:30`](../../third_party/vpp/src/vlibmemory/memory_api.c#L30)，`memclnt_queue_callback`；`:529` 安装 callback；[`vlib/main.c:1585`](../../third_party/vpp/src/vlib/main.c#L1585) 调用 | 主循环观察共享队列占用量，再 signal API Process。共享队列入队本身不等于直接唤醒 Process，也不等于跨进程 fd 已经传递。 |
| V6 | [`vlibapi/api_common.h:245`](../../third_party/vpp/src/vlibapi/api_common.h#L245)，`api_main_t`；`:37`，`vl_api_registration_t` | API Main 持有 msg data、name/CRC、region、client pointer pool、serialized table；registration 持有队列、region/header、name、liveness。没有 API Process 调度器。 |
| V7 | [`vlibmemory/memory_api.c:754`](../../third_party/vpp/src/vlibmemory/memory_api.c#L754)，`vl_mem_api_handler_with_vm_node`；`:883`，`void_mem_api_handle_msg_i`；`:905`，`vl_mem_api_handle_msg_main` | 从队列取出一个地址，按 ID 找 handler，按 MP-safe 包围调用，之后按 bounce 决定是否 free。private 路径临时选择并恢复 region/header。 |
| V8 | [`vlibapi/api_shared.c:483`](../../third_party/vpp/src/vlibapi/api_shared.c#L483)，`msg_handler_internal`；`:715`，`vl_msg_api_config` | 通用入口检查计算长度、按策略执行、按 free_it 释放；注册零 ID、重复 handler、name/CRC 各有不同语义。不能把这个入口的长度检查说成 V7 的实现。 |
| V9 | [`vlibmemory/memory_api.c:127`](../../third_party/vpp/src/vlibmemory/memory_api.c#L127)，create；`:209`，create-v2；`:100`，internal create | `am->vl_clients` 是进程私有的指针池。registration 在选定共享 Data Heap 单独分配；返回其共享地址和独立的 index。主区域表缓存复用，private 区域另行序列化。 |
| V10 | [`vlibmemory/memory_api.h:30`](../../third_party/vpp/src/vlibmemory/memory_api.h#L30)，`vl_msg_api_handle_*`；[`vlibmemory/memory_api.c:965`](../../third_party/vpp/src/vlibmemory/memory_api.c#L965)，index lookup | index 上 24 位为池索引，下 8 位为 restart epoch；构造断言 `index < 0x00ffffff`。lookup 验证池占用和 registration 所属 header 的 epoch。不是每次复用递增的 generation。 |
| V11 | [`vlibmemory/memory_api.c:289`](../../third_party/vpp/src/vlibmemory/memory_api.c#L289)，reapers；`:310`，delete | 先调用 reaper 再验 epoch；`do_cleanup=false` 先回复再删除，true 不回复并释放队列；ordinary delete 手工 free 请求；private delete 删除索引和 region 记录后连同 header page unmap，不再碰请求。 |
| V12 | [`vlibmemory/memory_api.c:469`](../../third_party/vpp/src/vlibmemory/memory_api.c#L469)，`vl_mem_api_init` | 安装五个 handler，create/create-v2/delete 默认非 MP-safe，keepalive 两方向标 MP-safe，delete bounce，五个 handler 不 trace/replay。 |
| V13 | [`vlibmemory/memory_api.c:427`](../../third_party/vpp/src/vlibmemory/memory_api.c#L427)，keepalive handlers；`:555`，`send_memclnt_keepalive`；`:600`、`:643`，dead scan | head 变化刷新活性；否则用 client allocation 发 probe，尝试前增加 unanswered；满队列是预期行为；两次未响应后检查 PID；先收集死亡索引，后回收。server 自发 keepalive 的回复进 server input queue。 |
| V14 | [`vlibapi/memory_shared.c:37`](../../third_party/vpp/src/vlibapi/memory_shared.c#L37)，allocator；`:740`，`vl_msg_api_send_shmem`；[`svm/queue.c:250`](../../third_party/vpp/src/svm/queue.c#L250)，add；`:424`，sub2 | 环先分配、Data Heap fallback；普通 send 使用 waiting add，probe 使用 nowait。sub2 不等数据，但会获取队列 mutex；不能称为无锁或绝不会阻塞。 |
| V15 | [`vlibmemory/memory_client.c:110`](../../third_party/vpp/src/vlibmemory/memory_client.c#L110)，create reply；`:145`，connect；`:235`，delete reply；`:250`，disconnect | client 队列分配在共享堆；保存 registration 地址、index 和 table；disconnect 回传地址和 index；普通 delete reply 由 client 接收后释放自己的队列。 |
| V16 | [`vlibapi/api_shared.c:187`](../../third_party/vpp/src/vlibapi/api_shared.c#L187)，`vl_api_serialize_message_table`；V9/V15 调用 | 表编码是 count、compact integer ID、cstring 编码；与 socket table records 不同。VPP 返回的是带 VPP vec 元数据的共享对象，不能声称 Hammer Vec 自动兼容。 |
| V17 | [`vlibmemory/memclnt_api.c:326`](../../third_party/vpp/src/vlibmemory/memclnt_api.c#L326)；[`vlib/init.c`](../../third_party/vpp/src/vlib/init.c)，`vlib_call_init_exit_functions` | 底层 socket/API 初始化失败退出本 Process；API-init hook 错误报告后继续；call-once/sorting 归全局 init 机制。两类初始化失败不能合并。 |
| V18 | [`vlibmemory/memclnt.api`](../../third_party/vpp/src/vlibmemory/memclnt.api)，create/delete/keepalive/create-v2；[`memclnt.api.json`](../../third_party/vpp/src/vpp-api/python/vpp_papi/data/memclnt.api.json) | 消息字段、autoreply、声明顺序与版本依据；不能按这五个 handler 的排列重新编号。 |
| V20 | [`vlib/main.c:1171`](../../third_party/vpp/src/vlib/main.c#L1171)，`vlib_process_startup`；`:1197`，`vlib_process_resume`；[`vlib/node_funcs.h:608`](../../third_party/vpp/src/vlib/node_funcs.h#L608)，suspend；`:913`，event-or-clock；[`vlib/node.h:619`](../../third_party/vpp/src/vlib/node.h#L619)，return/resume longjmp | 启动切换 coroutine stack，suspend 保存 resume_longjmp 并返回调度器，resume 从等待点继续。Rust 对应具体 Future 的 Pending/Waker/再次 poll，不能据 graph 函数 ABI 删除 async task。 |

已沿 V1→V2→V3→V4 核对调度；V4→V7→V9/V11/V13→V14 核对服务器；
C1→V14→V7→C3/C7 核对 v2 客户端往返；V17→V12→V8/V16 核对安装与消息表。
private 路径用于确认释放边界，不作为 ordinary mapping 已经具备 memfd 能力的证据。

### 客户端 v2 的源码证据

**“v2”在本文只指 `memclnt_create_v2` 握手，不是把整个客户端库按 v1/v2 命名。**
两套库在 vendored 树中并存；本设计选择 VAPI SHM。旧客户端的 `setjmp` 是 RX pthread
退出跳转，不是 VAPI 接收机制，也不能拿它解释 daemon Process 的 stackful coroutine。

| 编号 | 本地源码 | 直接核对的行为 |
| --- | --- | --- |
| C1 | [`vapi.c:571`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L571)，v2 reply；[`vapi.c:647`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L647)，connect；[`vapi.c:679`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L679) | SHM queue 分配、发送 CREATE_V2、等待回复、保存 index、导入 name/CRC；不保存 reply 的 handle。 |
| C2 | [`vapi.c:626`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L626)，delete reply；[`vapi.c:802`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L802)，send disconnect；[`vapi.c:1258`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L1258)，vapi_shm_disconnect；`:1369`，public disconnect | 请求只填 index/do_cleanup，handle=0；public disconnect 的 SHM 路径直接等待 delete reply，排空其它消息，处理 queue/index 后清理表并 unmap。`:836` 的 vapi_shm_client_disconnect 是另一入口，不能把它调用 vl_msg_api_handler 的行为当成 public 路径。 |
| C3 | [`vapi.c:1510`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L1510)，SHM recv；[`vapi.c:1797`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L1797)，dispatch | 取共享消息及 prefix length；server ID 映射到本地 metadata，换序、验证长度，按 context 区分 response/event，callback 后 free。不是以旧 queue_handler 作为正常客户端入口。 |
| C4 | [`vapi.c:1574`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L1574)，recv | handle_keepalives 时拦截 probe，以当前 client index 作 reply context，retval=0，发送、free 后继续 recv；EAGAIN 会重试。 |
| C5 | [`vapi.c:72`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L72)，ctx；[`vapi.h:22`](../../third_party/vpp/src/vpp-api/vapi/vapi.h#L22)；[`vapi_doc.rst`](../../third_party/vpp/src/vpp-api/vapi/vapi_doc.rst) | ctx 保存 connected、requests、context_counter、ID 映射、queue 和 keepalive policy；没有 RX pthread/jmp_buf 或七状态 enum。调用方驱动 blocking/nonblocking send/recv/dispatch；异步等待是 Hammer 适配。 |
| C6 | [`vapi.c:99`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L99)，context；`:131`，store_request；[`vapi_c_gen.py:665`](../../third_party/vpp/src/vpp-api/vapi/vapi_c_gen.py#L665)，生成请求；[`vapi.c:1637`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L1637)，response | context 低31位计数、高位置1；发送提交后登记有界 pending 请求。REG 一次回复完成；DUMP 以 control_ping_reply 结束；STREAM 有 details 与最终回复。找到后面的 context 时，前面未答请求收到 ENORESP。 |
| C7 | [`vapi.c:945`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L945)，lookup；[`vapi.c:960`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L960)，connect_ex；[`vapi.c:1061`](../../third_party/vpp/src/vpp-api/vapi/vapi.c#L1061) | 校验容量、映射、v2 握手、name/CRC 绑定；control_ping 和 reply 均可用才 connected=true。SHM connect_ex 开启 server keepalive，handle_keepalives 是客户端是否自动回答的另一项政策。 |
| C8 | [`memory_client.c:189`](../../third_party/vpp/src/vlibmemory/memory_client.c#L189)；[`memory_client.c:50`](../../third_party/vpp/src/vlibmemory/memory_client.c#L50) | 旧客户端发送 CREATE；setjmp/longjmp 属于其 RX pthread 的退出。只作排除依据，不移植其状态字段、handle 回传或 no-rx-pthread 接口族。 |
| C9 | [`memclnt.api:219`](../../third_party/vpp/src/vlibmemory/memclnt.api#L219)；[`memclnt_api.c:128`](../../third_party/vpp/src/vlibmemory/memclnt_api.c#L128)；[`api_helper_macros.h:63`](../../third_party/vpp/src/vlibapi/api_helper_macros.h#L63)；[`memclnt_api.c:219`](../../third_party/vpp/src/vlibmemory/memclnt_api.c#L219) | control_ping 查 registration 后回复 context、retval=0、client_index、vpe_pid；无 registration 则不回复。ping 与 reply 均 MP-safe；VPP trace=1/replay=0。 |
| C10 | [`vapi_c_test.c:838`](../../third_party/vpp/src/vpp-api/vapi/vapi_c_test.c#L838)；[`vapi_c_test.c:880`](../../third_party/vpp/src/vpp-api/vapi/vapi_c_test.c#L880) | 实际 nonblocking send/dispatch、缺失 response 的回调错误；不能仅验证握手就声称移植了请求关联。 |

本次设计 VAPI SHM 的连接、消息发现、REG/DUMP/STREAM 请求与事件分发，control_ping 是统一路径的
验证消息。旧 ping 专用原型不是目标客户端。Rust 复用 Api/codec/Service；客户端生成分支与服务器
执行表职责不同，不能因为已有 server registry 就省掉客户端本地 ID 和回调元数据。见第 8、9 节。


### 初始化与生成代码的补充依据

| 位置 | 实际行为 | Hammer 决策 |
| --- | --- | --- |
| `third_party/vpp/src/tools/vppapigen/vppapigen_c.py:1730` | 普通 API 模块生成 `setup_message_id_table`，申请模块 ID 段、添加 name/CRC、配置请求与回复；回复的 server handler 为零 | 复用 Api 派生元数据和原 ApiMsgConfig；不维护第二份 handler/CRC 列表 |
| `third_party/vpp/src/vlibmemory/memory_api.c:480` | `vl_mem_api_init` 先 map，再安装固定 memclnt 槽和名称/CRC | 内置 bootstrap ID 不经过动态分段或紧凑重排 |
| `third_party/vpp/src/vlibmemory/memclnt_api.c:182`、`:307` | Process 初始化 control 消息，随后调用 API-init hooks，再开始接收 | 所有 hook 必须在消费 create-v2 之前完成 |
| `third_party/vpp/src/vlibmemory/memory_api.c:208` | create-v2 首次需要消息表时序列化，primary region 复用快照；回复使用 `vl_msg_api_alloc` | handler 按需调用共享表存储操作，服务器使用同一个 `ApiMain::alloc` |
| `third_party/vpp/src/vpp-api/vapi/vapi.c:574`、`:647` | client reply handler 接收显式 ctx、将共享表导入 ctx；create 请求使用 client allocator | Client::connect 消费局部 reply 后设置索引和本地表；不写 ApiMain 的服务器消息注册表 |
| `third_party/vpp/src/vlibmemory/socket_api.c:248`、`:438` | read callback 收集完整消息并 signal Process；accepted connection 保存 File index | callback I/O 与 Process 消息分派分开，保留实际 File index |

本次生成器覆盖内置固定 ID 表和普通 API owner 的动态 range 表：固定表由完整
`memclnt.api` 顺序产生，range 表由 `ApiMain::get_msg_ids` 分配连续段。它仍然
不是 C VPP 二进制互操作；同构建 Rust SVM 布局和 trusted shared-memory 合同保持不变。

## 3. 现有结构与职责隔离

| Owner | 保存的实际值 | 允许调用 | 禁止依赖 |
| --- | --- | --- | --- |
| hammer-infra | Pool、SvmQueue、SvmRegion、Data Heap | 分配、映射、队列锁和 Add/Sub | memclnt、Client、control_ping |
| hammer-runtime | Process task、event receiver、FileMain/AsyncFileMain、barrier、主线程身份 | 调度 future、File readiness、生命周期 | ApiMain/SocketMain/Client 的字段、业务 handler |
| hammer-ipc::binary_api::ApiMain | 消息身份、注册表、映射、server registrations、序列化表 | 注册查找/回收、dead_client_scan、映射、存储 | socket I/O、具体消息 handler、客户端请求完成、Process 调度 |
| hammer-ipc::binary_api::memclnt / control | 消息定义与具体 server handler | 调用 ApiMain 的 registration 操作，构造具体回复，调用 MsgBuf/SvmQueue | runtime 新业务字段、客户端回调状态 |
| hammer-ipc::binary_api::memory_client::Client | 连接、响应队列、已发现 ID、pending 请求和应用 callback | ApiMain 的映射/分配入口，codec、SvmQueue、Tokio 等待 | 安装 server handler、修改 ApiMain 客户端字段、worker barrier |
| hammer-service::binary_api::SocketMain | listener File index、连接 Pool、完整消息 Pool | File 注册/删除、callback I/O、Process signal、socket 消息 dispatch | SHM registration、Client outstanding requests |

复用普通 Vec/HashMap、原 Pool 和 SVM allocation。共享 registration 是 SVM 分配的对象；
本地 Pool 只存指针，不将会扩容的 Pool 自身放入共享内存。
所有普通集合使用 Main Heap；共享表及 registration/queue 使用所属 region Data Heap。
Data Heap activation 必须在 `.await`、业务回调和本地集合扩容之前结束。

## 4. 服务端初始化与宏生成的 setup

内置 ID 由宏根据 `memclnt.api` 的完整声明顺序生成一次。各初始化阶段只选择
本阶段实际安装的消息并声明 MP-safe、trace、replay 策略，不逐项抄写数字：

```rust
// memclnt 模块：server mapping 完成后调用
hammer_component_macros::api_message_table! {
    pub fn setup_message_id_table;
    ids super;
    MemclntDelete => memclnt_delete_handler { is_mp_safe: false, traced: false, replay: false };
    MemclntDeleteReply { is_mp_safe: false, traced: false, replay: false };
    MemclntKeepalive { is_mp_safe: true, traced: false, replay: false };
    MemclntKeepaliveReply => memclnt_keepalive_reply_handler { is_mp_safe: true, traced: false, replay: false };
    MemclntCreateV2 => memclnt_create_v2_handler { is_mp_safe: false, traced: false, replay: false };
    MemclntCreateV2Reply { is_mp_safe: false, traced: false, replay: false };
}

// control 模块：Process 初始化、API-init hooks 之前调用
hammer_component_macros::api_message_table! {
    pub fn setup_message_id_table;
    ids super;
    ControlPing => control_ping_handler { is_mp_safe: true, traced: true, replay: false };
    ControlPingReply { is_mp_safe: true, traced: true, replay: false };
}
```

[新增、按用户要求] `hammer-component-macros::api_message_table!` 生成上面命名的函数：

```rust
pub fn setup_message_id_table(api: &ApiMain);
```

宏的 order 模式从 1 开始按完整 `memclnt.api` 顺序生成消息 ID 常量和
`MEMCLNT_LAST`；setup 模式从该组常量按消息类型名选择 ID。展开逐项使用
`ApiMsgConfig::new::<Message>(id)`、三个声明策略、`api.msg_config(config)`
和 `api.add_msg_name_crc(Message::NAME_CRC, id)`。hookup 条目的
`Message => concrete_handler` 决定该端是否有 executable dispatch；没有 `=>` 的回复
只安装身份和策略，不伪造入口。宏拒绝重复/零/超出 u16 的 ID、重复策略和遗漏策略。
不扫描相邻 Rust 文件，不另建全局注册 inventory，不手抄 ID 或 NAME_CRC 字符串。

ID 3/4、21/22、23/24、25/26 来自内置 memclnt 消息次序；旧 create 等不支持的消息保留空洞。
客户端建连 bootstrap 用固定 create-v2/reply ID；连通后 keepalive 和普通请求使用已发现的数值 ID。
delete 保留 memclnt 固定 ABI，与 VAPI SHM public disconnect 一致。

目标顺序：

1. daemon 生命周期在 Main Heap 就绪后创建并安装 ApiMain；原配置 setter 仍在 install 前执行。
2. 显式启用 memory 时，由该生命周期持有 root SvmRegion，调用既有 map_shared_region 完成 server map。
3. server map 完成后调用 memclnt::setup_message_id_table；Process factory 调用 control::setup_message_id_table，
   然后执行原 run_api_init(main)。API hook 失败仍按 Hammer 原 RuntimeError 路径停止 task。
4. Process 开始收消息。首个 create-v2 handler 调用 `serialize_shared_message_table`，后续 primary client 复用快照。
5. Client::connect 收到 create-v2 reply 后只导入自己的消息表，再发布 connected。

VPP 的 mapping init 和 Process init 在这里保持分阶段调用。既有 ApiMain::map_shared_region
仅在 is_server=true 的成功路径设置 primary region 并调用 memclnt::setup_message_id_table(self)；
client mapping 路径不执行注册。Process factory 只装 control 表，不重复安装 memclnt 表。
两张表分别由同一个宏生成，所有普通 API-init hooks 完成之后才开始接收 create-v2。
共享表快照在首次 create-v2 内按需生成。客户端没有 publish_client_message_table，也不调用服务端 setup。

默认 ApiMain 已在既有 early configure 安装，动态起始 ID 为 29，保留 memclnt 的全部 1..=28。
memory 配置和 server mapping 已接入 service init；本次工作仍以真实 SHM
`control_ping`/`show_version` 往返作为行为门禁。不得用 socket-only 启动成功
替代 memory 初始化验证。当前工作树在序列化表发布后禁止改消息 ID/handler/name-CRC，这是 Hammer 当前启动期加载策略，
不是 VPP 允许替换 handler 的完整语义。若允许运行中加载，必须设计旧共享快照寿命与新连接发现，
不能绕过断言留下两份不一致的表。API-init 的 call-once 仍只由 GlobalMain 持有。

### 4.1 api_init_function_registrations：声明端、收集端与执行端

这个现有字段必须复用，不能由 ApiMain 新建 API-init inventory，也不能以客户端 MessageId 表替代。
执行端和声明端均已接入：`binary_api_clnt` 调 `run_api_init(main)`，后者直接遍历
GlobalMain 的表；service image 将 `vpe_api_hookup` 放入 `api_init_functions`，由
hookup 调用 range 宏并安装 VPE 消息。bootstrap 仍在遍历前直接安装，因此一次
`run_api_init` 不会重复安装 control 表。

```text
普通 API owner 的 InitFunction 静态声明
  → 所属 RegistrationImage.api_init_functions
      内建 owner：既有 __declare_registration_image 的 api_init_functions 项
      插件 owner：既有 plugin 宏的 api_init_functions 参数
  → daemon 将内建 image 交给 PluginMain，插件 image 随加载保存
  → PluginMain::register_global_declarations
  → GlobalMain::register_api_init → api_init_function_registrations
      分配已有 callback_index，按同一声明指针去重
  → API Process 完成 bootstrap 后调用 run_api_init(main)
  → 既有 dispatch_init 排序、mark_init_function_called、调用 InitFunction.func
  → owner 的 API hookup → 宏生成的 setup_message_id_table
  → ApiMain 的消息范围/handler/name-CRC 安装
  → 才能消费 create-v2 并发布完整消息表
```

复用现有 `#[init_function]` 即可生成 InitFunction 及适配入口；**阶段由 image 中放入哪个列表
决定**，该属性本身不把声明自动装进 ordinary init。无需为此再造同义 api_init_function 宏。
未来普通 interface API owner 的接法如下（展示接入合同，不表示该模块现在已有这些消息）：

```rust
#[hammer_component_macros::init_function(name = "interface_api_hookup")]
fn interface_api_hookup() -> hammer_runtime::RuntimeResult<()> {
    let api = ApiMain::current();
    setup_message_id_table(api); // 该 API owner 的生成代码
    Ok(())
}
// 所属 image 的既有字段：
// api_init_functions = [interface_api::__INIT_FN_INTERFACE_API_HOOKUP];
// 不同时加入 init_functions，也不由 Process 手动逐一调用普通模块的 setup。
```

`runs_before/runs_after` 只引用同一个 API-init 阶段的声明；bootstrap 先于整个链是 Process 的
控制流事实，不伪造一个跨阶段排序依赖。当前 runtime 对未解析依赖/循环返回 InitError，hook
调用前标记 call-once，返回错误停止链；ADR-0017 的 Process 使用 `?` 停止启动。这与 VPP
API-init 错误报告后继续的行为存在明确差异，不得沿用 ADR-0015 的目标描述声称已实现后者。

VPP 也有两个不同安装阶段：`memory_api.c:480` 的 map+memclnt 固定表、
`memclnt_api.c:182,333` 的 vlib_api_init/control_ping 在表遍历前直接执行；
`memclnt_api.c:342` 才调用 `api_init_function_registrations`。普通 hookup 例如
`interface_api.c:1836` 的 VLIB_API_INIT_FUNCTION(interface_api_hookup)，同文件中的
setup_message_id_table 安装该模块动态范围。`memclnt_api.c:740` 的 rpc_api_hookup 也走此链，
所以不能按文件目录判定所有 memclnt_api 内容都是 bootstrap。

客户端 name/CRC → 双向 ID 映射则对应 VAPI 自己的 metadata/连接发现，不调用 server API-init，
不访问 GlobalMain，不因此在客户端安装服务器 handler。客户端生成仍复用 Api/Service 静态定义。

验证要执行非空 owner hookup，观察其只调用一次、排序成立、分配的范围和 NAME_CRC 出现在
首次 create-v2 返回的表里；DSO 情形还要验证声明被实际 image 导出/收集。只检查向量存在或
run_api_init 符号被调用均不算通过。当前尚无普通 API 模块完成这项行为验证。

## 5. Rust 类型与 safe 分派

### 5.1 Api 和消息表

[修改] `Api` trait 只描述消息身份、服务关系和生成的 codec，不保存业务 handler。
`api_message_table!` 的 hookup 条目用 `Message => concrete_handler` 绑定具体的
safe `fn(Message)`；未写 `=>` 的回复仍安装 name/CRC 和策略，但没有 dispatch。
`#[derive(Api)]` 从同一份字段声明生成编码，不把 `repr(C)` 或 Rust struct 内存布局当成消息布局。

```rust
pub struct ApiMsgConfig {
    pub id: u16,
    pub name: &'static str,
    pub crc: u32,
    pub dispatch: Option<fn(&[u8], bool) -> Result<(), codec::Error>>,
    pub is_mp_safe: bool,
    pub traced: bool,
    pub replay: bool,
}

pub struct ApiMsgData {
    pub name: Option<&'static str>,
    pub dispatch: Option<fn(&[u8], bool) -> Result<(), codec::Error>>,
    pub is_mp_safe: bool,
    pub trace_enable: bool,
    pub replay_allowed: bool,
}

impl ApiMsgConfig {
    pub fn new<T: Api>(id: u16) -> Self;
}

// 泛型入口只消费 owned T；具体 safe fn 由 hookup 条目传入，MP-safe 由表配置决定。
#[inline(always)]
fn handler<T: Api>(message: T, function: fn(T)) {
    function(message);
}
```

server `receive()` 的完整路径：

1. 在 runtime 主线程从 primary queue `sub2` 取出地址，区分 Empty、提交前错误、提交后通知错误。
2. `MsgBuf::from_address` 在短期 unsafe 内验证 prefix 对齐、prefix range、完整 payload range。
3. 借用 initialized payload 读取大端 u16 ID；`ApiMain::get_msg_data` 复制 ApiMsgData，结束表借用。
4. 单态化 dispatch 解码 owned T，检查尾部无多余字节；此时尚未进入 barrier。
5. MP-safe 消息直接执行 `handler::<T>(message)`；其余通过既有 worker barrier 宏同步执行。
6. 具体 safe handler 按协议验证 registration/queue，执行 mutation，构造具体回复。
7. dispatcher 在正常/拒绝路径释放普通请求；panic 时先释放再恢复 unwind，由 runtime task 边界处理。

codec 不持有 barrier；handler 不 await；不得持有 pool entry 的可变借用进入其它业务 dispatch。
SHM 请求未通过地址/消息验证不可以被当作任意 Rust 引用。
VPP 对 trusted shared-memory 地址的假设不等于对恶意共享内存写者的 Rust 安全防护，见第 11 节。

### 5.2 ApiMain 的实际字段

[复用] 注册、配置和映射字段；[新增] server registration pool、serialized table；
[删除] 客户端连接/请求/回调字段和冗余线程身份。

```rust
pub struct ApiMain {
    msg_data: RefCell<Vec<ApiMsgData>>,
    msg_id_by_name: RefCell<HashMap<&'static str, u16>>,
    msg_index_by_name_and_crc: RefCell<HashMap<std::string::String, u16>>,
    first_available_msg_id: Cell<u16>,
    msg_ranges: RefCell<Vec<ApiMsgRange>>,
    msg_range_by_name: RefCell<HashMap<std::string::String, usize>>,
    api_version_list: RefCell<Vec<ApiVersion>>,
    pub(super) clients: UnsafeCell<Pool<NonNull<ApiRegistration>>>,
    pub(super) serialized_message_table: UnsafeCell<Option<NonNull<Vec<u8>>>>,
    pub(super) rp: UnsafeCell<Option<NonNull<SvmRegion>>>,
    pub(super) primary_rp: UnsafeCell<Option<NonNull<SvmRegion>>>,
    pub(super) private_rps: UnsafeCell<Vec<NonNull<SvmRegion>>>,
    pub(super) mapped_shmem_regions: UnsafeCell<Vec<Box<SvmRegion>>>,
    pub(super) shmem_header: UnsafeCell<Option<NonNull<ShmemHeader>>>,
    pub(super) process_pid: AtomicI32,
    pub(super) ring_misses: AtomicU32,
    pub(super) input_queue_length: u32,
    api_uid: i32,
    api_gid: i32,
    global_base_va: u64,
    global_size: u64,
    pub(super) api_size: u64,
    global_pvt_heap_size: u64,
    pub(super) api_pvt_heap_size: u64,
    pub(super) api_region_name: String,
}
```

注册表通过已有方法访问；post-install 的消息配置/查询在任何 RefCell 借用前检查 runtime 主线程。
`get_msg_data` 返回复制值；table/range/version 返回实际 `Ref`，不伪造安装后的 `&mut ApiMain`。
API-init 在 OnceLock 安装之后运行，所以原要求 `&mut self` 的范围和版本接口必须联动修改：

```rust
impl ApiMain {
    pub fn get_msg_ids(&self, name: &str, count: u16) -> Result<u16, api::Error>;
    pub fn message_range(&self, name: &str) -> Option<Ref<'_, ApiMsgRange>>;
    pub fn add_version(&self, version: ApiVersion);
    pub fn versions(&self) -> Ref<'_, [ApiVersion]>;
    pub fn install(self);
    #[inline(always)]
    pub fn current() -> &'static Self;
    #[inline(always)]
    pub unsafe fn set_main(main: &'static Self);
}
```

| 对象/入口 | 何时使用 | 原因与依据 |
| --- | --- | --- |
| 私有 api_global_main: OnceLock<ApiMain> | 安装 daemon/普通客户端进程的默认实例 | api_shared.c:23-30；实例与 selector 不是两份 Main。 |
| my_api_main | 一个 OS thread 当前所选 Main；未显式选择时用默认实例 | api_common.h:382-401；不保存注册表或 Client。 |
| ApiMain::current() | 无 owner 参数的同步 handler、Process factory、Client::new | VPP vlibapi_get_main；调用入口取得真实 owner。 |
| ApiMain::set_main(main) | 客户端线程选择独立 API context，如未来 VCL worker | vcl_bapi.c:499-513；不用于每次 Process poll。 |
| &self / Client.api | 映射、alloc/free、注册维护、setup、表操作、异步 Client 收发 | 已知 owner 就使用该值；不能在嵌套调用或 await 后偷换 region。 |

主线程服务器必须保持 daemon Main 的选择；TLS 不导致每线程复制服务器，也不调度 Process。
Client::new 捕获选中 Main 后可在其它 Main 被选中时继续使用自身 owner；unsafe set_main 仍要求
遵守所选实例的线程访问合同。TLS 选择与同一 ApiMain 内的 rp/header 更换是不同操作：后者只有
旧区域全部用户结束后才允许。`is_mapped` 仅查询映射事实，不代表 handlers/setup/task 就绪。

注册表 RefCell 的全部访问先检查 runtime 主线程，版本/范围也不能例外。非主线程客户端不调用
服务器 registry 方法，其 ID 表属于 Client。unsafe Send/Sync 不许可任意并发修改映射或 server pool。
不新增 public global getter 绕过 selector；`serialize_shared_message_table` 留在 ApiMain 存储实现中。

消息分配的六个入口从 ShmemHeader 移到 ApiMain，普通 free 从 MsgBuf 移到 ApiMain：

```rust
impl ApiMain {
    pub unsafe fn alloc(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_zeroed(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_or_null(&self, payload_len: usize) -> Option<MsgBuf>;
    pub unsafe fn alloc_as_client(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_zeroed_as_client(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_as_client_or_null(&self, payload_len: usize) -> Option<MsgBuf>;
    pub unsafe fn free(&self, message: MsgBuf);
}
```

原因不是重命名：ShmemHeader 只拥有 ring 存储，不能用自己的 ring 配上 TLS 中另一 Main 的 fallback
heap。由一个 ApiMain 选择 header/rp/PID/counter，free 也验证消息来自同一 region。调用者必须保证
区域存活和消息独占，Add 提交后 producer 不再 free。重启已持锁路径仍复用 MsgBuf::free_nolock。

## 6. 消息声明与 server handler

[新增] 八种消息；keepalive reply 由 autoreply 自动生成。以下列出全部协议字段；
handler 在上面的 hookup 表绑定，`#[derive(Api)]` 生成固定数组 codec，不能编码长度前缀。

```rust
#[derive(Clone, Copy, Debug, Api)]
#[api(name = "memclnt_delete", returns = MemclntDeleteReply)]
pub struct MemclntDelete {
    pub id: u16,
    pub index: u32,
    pub handle: u64,
    pub do_cleanup: bool,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "memclnt_delete_reply")]
pub struct MemclntDeleteReply {
    pub id: u16,
    pub response: i32,
    pub handle: u64,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "memclnt_keepalive", autoreply = MemclntKeepaliveReply)]
pub struct MemclntKeepalive {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "memclnt_create_v2", returns = MemclntCreateV2Reply)]
pub struct MemclntCreateV2 {
    pub id: u16,
    pub context: u32,
    pub ctx_quota: i32,
    pub input_queue: u64,
    #[api(string)]
    pub name: [u8; 64],
    pub api_versions: [u32; 8],
    pub keepalive: bool,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "memclnt_create_v2_reply")]
pub struct MemclntCreateV2Reply {
    pub id: u16,
    pub context: u32,
    pub response: i32,
    pub handle: u64,
    pub index: u32,
    pub message_table: u64,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "control_ping", returns = ControlPingReply)]
pub struct ControlPing {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "control_ping_reply")]
pub struct ControlPingReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub client_index: u32,
    pub vpe_pid: u32,
}

// [宏生成] MemclntKeepalive 的 autoreply，不重复手写定义。
pub struct MemclntKeepaliveReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
}
```

| 请求/回复 | 字段编码长度 | Server handler | MP-safe |
| --- | --- | --- | --- |
| MemclntDelete / Reply | 15 / 14 | memclnt_delete_handler / 无 | 否 |
| MemclntKeepalive / Reply | 10 / 10 | 无 / memclnt_keepalive_reply_handler | 是 |
| ControlPing / Reply | 10 / 18 | control_ping_handler / 无 | 是 |
| MemclntCreateV2 / Reply | 115 / 30 | memclnt_create_v2_handler / 无 | 否 |

这里的 keepalive request 是 server → client。当前 ordinary 外部客户端模式不安装 server-side
keepalive request handler；并不宣称覆盖 VPP 内部客户端可能使用的双向 keepalive 入口。

具体入口在 memclnt/control 消息模块，不属于 `impl ApiMain`：

```rust
fn memclnt_create_v2_handler(request: MemclntCreateV2);
fn memclnt_delete_handler(request: MemclntDelete);
fn memclnt_keepalive_reply_handler(reply: MemclntKeepaliveReply);
fn control_ping_handler(request: ControlPing);

pub fn receive() -> Result<bool, SvmQueueError>;
// ApiMain 自身的 registration 操作，不接收任何协议 request。
impl ApiMain {
    pub(super) fn registration(&self, client_index: u32) -> Option<NonNull<ApiRegistration>>;
    fn remove_registration(&self, slot: u32, do_cleanup: bool) -> Result<(), SvmRegionError>;
    pub fn dead_client_scan(&self, now: Instant);
}
```

control_ping 的业务实现在这个函数本身：

```rust
fn control_ping_handler(request: ControlPing) {
    let api = ApiMain::current();
    let Some(registration) = api.registration(request.client_index) else {
        return;
    };
    let queue = unsafe { registration.as_ref().input_queue.as_ref() };
    let reply = ControlPingReply {
        id: 24,
        context: request.context,
        retval: 0,
        client_index: request.client_index,
        vpe_pid: std::process::id(),
    };
    let header = unsafe { api.shmem_header() };
    let mut message = unsafe { header.alloc(18) };
    let written = unsafe { message.encode(&reply) }
        .expect("owned protocol message encodes at its declared length");
    assert_eq!(written, 18, "message length agrees with API declaration");
    let address = usize::from(&message).to_ne_bytes();
    let sent = queue.add(&address, SvmQueueConditionalWait::Nowait);
    if sent
        .as_ref()
        .err()
        .is_some_and(|source| !is_committed(source, SvmQueueOperation::Add))
    {
        unsafe { message.free() };
    }
    if let Err(source) = sent {
        tracing::error!(
            ?source,
            client_index = request.client_index,
            "control ping reply queue"
        );
    }
}
```

服务器普通回复用 `ApiMain::alloc`；客户端请求用其映射 owner 的 alloc_as_client。
keepalive probe 是明确的例外：VPP send_memclnt_keepalive 使用 alloc_as_if_client_w_reg，
所以 ApiMain::dead_client_scan 用 registration 所属 header 的 alloc_as_client。它们选择不同 ring，不得为了复用一个“发送 helper”统一成 client allocator。
构造具体回复、选择 ID/context/retval 和处理队列错误都是协议 handler 的责任。

## 7. 服务器 registration 生命周期

```rust
pub(super) struct ApiRegistration {
    pub pool_index: u32,
    pub name: [u8; 64],
    pub input_queue: NonNull<SvmQueue>,
    pub region: NonNull<SvmRegion>,
    pub shmem_header: NonNull<ShmemHeader>,
    pub last_heard: Instant,
    pub last_queue_head: u32,
    pub unanswered_keepalives: u32,
    pub keepalive_enabled: bool,
    pub is_being_removed: bool,
    pub cleanup_queue: bool,
}
```

registration 对象在 SVM，ApiMain 的原 Pool 保存 NonNull。client index 是 24-bit slot +
8-bit application-restarts epoch，handle 是回显用共享地址；handle 不作为客户端随意解引用的许可。
ApiMain::registration 先校验 epoch，再查 Pool，并拒绝 being_removed。
Pool 没有 generation 能力，本设计不得假设有。

create-v2：验证队列地址非空、64 字节对齐、对象及 ring extent 位于映射、element_size 等于地址宽度。
随后懒生成共享表、分配 registration、安装 Pool entry、构造 reply。
回复 Add 未提交则 free 回复并回滚 registration；Add 已提交则保留 registration，不能重发创建回复。
name、keepalive、region/header、pool index、consumer liveness 都由具体 handler 初始化。
ctx_quota/api_versions 在 VPP 此分支未用于额外容量状态机，Hammer 不发明 ClientCapacity 错误族。

普通 delete：回复提交之后标记 registration 并移除；client 收到回复再释放自己的队列。
`do_cleanup = true`：server 不回复，接管队列 drain/销毁与 registration 清理；client 在 Add 提交后
不再访问队列。清理前失败需保存 being_removed/cleanup_queue，以便扫描重试。
private segment unmap、reaper callback 和 bounce request 的特殊释放另需实现；当前只处理 ordinary region，
不能照搬 VPP 删除 private segment 后的消息 free 次序。

keepalive：独立十秒扫描，先检查 consumer head 的推进；有推进则重置 last_heard 和 unanswered。
无推进时发送 probe；达到未回复阈值再探测 consumer PID。ESRCH 才表示已退出，EPERM 不等于死亡。
原 SvmQueue/region source 保留；不得把每种锁名做成新的协议 Error variant。

## 8. Client 的完整目标：VAPI context，不是 ping 专用收发器

本节替换旧 ping 专用 Client。以下类型与方法已进入工作树；行为验证状态单列于第 13 节。
现有 `VecDeque<(u32, Sender<ControlPingReply>)>`、两个消息专用 event 字段以及每次按字符串查询
ping ID 的路径均列入替换范围。删除握手缓存只解决了字段归属，未解决客户端架构。

### 8.1 两种 context 的理由和边界

`api_global_main` 是默认 ApiMain 实例；`my_api_main` 是选中实例的 TLS 指针。
daemon 的主线程保持默认选择，所有 server registrations 仍在同一个 Main，Data Worker 不复制它。
后续 VCL worker 由自己的生命周期持有 ApiMain，再选择它；参考 `vcl_bapi.c:499-513`，这一步
不需要把服务器变成每线程服务。VCL 在 VPP 当前使用旧 memory-client 路径，不能声称它就是 VAPI。
这里保留的是底层 ApiMain 的选择能力；未来 VCL 的连接模型仍须由 VCL 单独设计。

VAPI `vapi_ctx_s` 则是**显式连接**：同一 API 映射可以有多个响应队列、client index 和请求环。
它们属于不同 Client。TLS 不选择 Client，server handler 也不能通过 ApiMain 找到应用的 pending 请求。

Hammer 新增 `Client.api: &'static ApiMain` 直接引用构造时选择的实际 owner。原因是 C 的
`vapi_shm_send` 同步使用 TLS，而 Rust `recv/connect/disconnect` 会挂起：恢复时再次查询 TLS
可能拿到另一映射。捕获实际 owner 后，分配、接收验证和释放始终走 `self.api`，不受后来 TLS
选择影响。不新增缓存 region/header 地址的借用包装，也不在 runtime 安装 task-local API selector。

`set_main` 只更换选择，不映射、不释放、不迁移连接；另一个 Main 的选择并不授权修改旧 Main 的
`rp/shmem_header`。已借出的 region、队列、消息和 Client 仍要求原映射存活。
当前 setter 要求 `&'static ApiMain`，只支持寿命覆盖进程的 context 实例；后续 VCL 若要销毁
ApiMain 对象本身，必须先另行设计有真实生命周期的借用接口，不能从 TLS 裸指针伪造 `'static`。

### 8.2 消息定义复用与生成边界

VAPI 的本地 metadata ID 与服务器 ID 是两个空间：本地 ID 标识编译进 SDK 的消息，服务器 ID
由 name/CRC 发现，不能把 `23/24` 当成正常业务分发规则。服务器的 `ApiMsgData` 含 barrier 和
server handler 策略，不适合拿来充当客户端 descriptor；应从现有 `Api` 常量、`Service` 和
Rust 字段声明生成客户端匹配代码，不抄第二份 CRC/字段表。

[新增/生成] 客户端声明集合生成 `MessageId`、owned `Message` 和具体请求方法。以下八项是
当前集合的完整展开；增加业务 API 时扩展声明集合，由宏产生相应变体和匹配分支，不改 transport
循环，也不在运行时创建动态值树。宏不扫描邻近源码、不导入 `.api` 文件、不调用 vppapigen。
本地枚举序号只用于当前构建的数组下标，不进入共享消息。

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum MessageId {
    MemclntCreateV2, MemclntCreateV2Reply,
    MemclntDelete, MemclntDeleteReply,
    MemclntKeepalive, MemclntKeepaliveReply,
    ControlPing, ControlPingReply,
}

pub enum Message {
    MemclntCreateV2(MemclntCreateV2),
    MemclntCreateV2Reply(MemclntCreateV2Reply),
    MemclntDelete(MemclntDelete),
    MemclntDeleteReply(MemclntDeleteReply),
    MemclntKeepalive(MemclntKeepalive),
    MemclntKeepaliveReply(MemclntKeepaliveReply),
    ControlPing(ControlPing),
    ControlPingReply(ControlPingReply),
}

impl MessageId {
    pub const ALL: &'static [Self]; // 生成的完整声明顺序
    #[inline]
    pub const fn name_crc(self) -> &'static str; // T::NAME_CRC
    fn decode(self, payload: &[u8]) -> Result<Message, codec::Error>;
}
impl Message {
    #[inline]
    pub fn id(&self) -> MessageId;
    #[inline]
    pub fn context(&self) -> Option<u32>; // 按实际声明，无 context 的 delete 不伪造值
}
```

`decode` 的生成分支只调用现有 `codec::Deserializer`、具体 `Deserialize`，检查剩余字节为零后
返回对应 owned 变体。enum 拥有不同协议消息，不是观察共享 buffer 的 wrapper。
客户端应用的二进制确定消息集合；动态服务器插件不要求客户端动态装载 Rust 类型，未编译进来的
ID 被明确拒绝。服务端 `fn<T: Api>(t: T)` 和具体 safe `fn(MessageType)` 完全保留。

### 8.3 请求、回调和 Client 字段

[新增] `RequestMode` 是协议完成条件，**不是连接生命周期 state enum**。
保留 VAPI 的 STREAM 两槽表达：details 槽与 terminal reply 槽共享 context，分别完成各自 callback。
这使容量检查也与 `vapi_c_gen.py:665-713` 的一槽/两槽规则一致。

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestMode { Regular, Dump, Stream }

struct Request<C> {
    context: u32,
    response_id: MessageId,
    mode: RequestMode,
    callback: fn(&mut C, u32, bool, Result<Option<Message>, Error>),
}

pub struct Client<C> {
    api: &'static ApiMain,
    connected: bool,
    input_queue: Option<NonNull<SvmQueue>>,
    client_index: Option<u32>,
    create_pending: bool, // 已提交 create，尚未收到身份；取消后续收，禁止重发
    delete_pending: bool, // 已提交 delete，尚未收到确认；取消后续收，禁止重发
    handle_keepalives: bool,
    requests: VecDeque<Request<C>>,
    max_outstanding_requests: usize,
    context_counter: u32,
    message_table: HashMap<String, u16>,
    server_message_ids: Vec<Option<u16>>,       // 本地 MessageId -> server ID
    local_message_ids: Vec<Option<MessageId>>, // server ID -> 本地 MessageId
    generic_callback: Option<fn(&mut C, Message)>,
    event_callbacks: Vec<Option<fn(&mut C, Message)>>,
}
```

`C` 是应用已有的具体回调状态，在 `dispatch(&mut C, ...)` 中直接借入；Client 不再造一个
应用全局，不拥有或擦除应用指针。请求回调接收 API context，使应用能在自己的状态中定位请求；
不同请求可登记不同 safe 函数。STREAM 的 details 与 terminal 分别登记函数。C 的泛型只用于单态化应用回调，不参数化 runtime/worker。
`Result<Option<Message>, Error>` 的三个含义固定：`Ok(Some(message))` 是数据，
`Ok(None)` 是 DUMP 的正常完成，`Err(NoResponse { context })` 是有序请求缺失。
`bool` 只表示该 callback 是否最终调用，与 VAPI `is_last` 对应。

| VAPI ctx 字段 | Hammer 对应及为什么 |
| --- | --- |
| requests_size/start/count/requests | VecDeque 的环位置和数量 + 显式容量策略；元素是 Request<C>，不绑定 ControlPingReply。提前 reserve，已提交后 push 不再分配。 |
| response_id/type/callback/callback_ctx | Request 的预期本地 ID、完成模式和 safe callback；应用通过传入的 C 与 context 持有请求状态，去掉 void* 擦除。 |
| context_counter | 低 31 位计数、高位置 1；回绕时跳过仍在 pending 的 context。 |
| generic_cb/event_cbs | generic_callback 与按本地 ID 索引的 event_callbacks；具体事件优先，未设置才用通用事件入口。 |
| 两个 ID 数组、vl_msg_id_max | server_message_ids 与 local_message_ids；最大合法范围由后者 len 表达，不再存一个可失配的 max。所有空洞显式初始化为 None。 |
| msg_index_by_name_and_crc | message_table 只用于发现和诊断，普通 send/dispatch 使用数值映射。 |
| connected/handle_keepalives/queue/index | 连接、策略、该连接独占的响应队列和服务器身份；没有握手 message 缓存。 |
| mode | try 请求入口在满队列时立即返回；async dispatch 等待就绪并让出主线程。两种行为由方法明确表达，不存一个会让同步调用阻塞 OS thread 的 C blocking 开关。 |
| requests_mutex | Client 的 !Send/!Sync 与 &mut 提交/dispatch 保证请求环单执行者；不保留 C 的多生产者线程能力，不添无效 Mutex。 |
| use_uds/client_socket | 当前明确只移植 SHM，socket server 的 AsyncFileMain 接入不是客户端 UDS transport；无相应连接就不保存 socket 字段。 |
| time | Instant deadline 属于一次等待；进程时钟无须放进每个 Client。 |

不是把 mode、mutex、socket 随意删掉：其行为要么用 Rust 的调用/借用合同替代，要么明确在 SHM
范围外；请求模式、预期 ID、双向映射和事件优先级则是 SHM 已需要的语义，不能推迟成“以后扩展”。

### 8.4 最终方法及生成请求入口

```rust
impl<C> Client<C> {
    pub fn new() -> Self; // 只建立空连接并捕获 current Main，不 map/注册 server handlers
    #[inline]
    pub fn is_connected(&self) -> bool;
    #[inline]
    pub fn client_index(&self) -> Option<u32>;
    #[inline]
    pub fn request_count(&self) -> usize;
    #[inline]
    pub fn message_id(&self, id: MessageId) -> Option<u16>;
    #[inline]
    pub fn set_event_callback(&mut self, id: MessageId,
        callback: Option<fn(&mut C, Message)>);
    #[inline]
    pub fn set_generic_callback(&mut self, callback: Option<fn(&mut C, Message)>);

    pub async fn connect(&mut self, name: &str,
        response_queue_size: NonZeroU32, max_outstanding_requests: NonZeroUsize,
        handle_keepalives: bool) -> Result<(), Error>;
    // [生成] REG 示例。提交成功返回 context；回复由 dispatch 驱动。
    pub fn control_ping(&mut self,
        callback: fn(&mut C, u32, bool, Result<Option<Message>, Error>))
        -> Result<u32, Error>;
    pub async fn dispatch_one(&mut self, callbacks: &mut C, timeout: Duration)
        -> Result<bool, Error>;
    pub async fn dispatch(&mut self, callbacks: &mut C, timeout: Duration)
        -> Result<(), Error>;
    pub fn send_disconnect(&mut self, do_cleanup: bool) -> Result<(), Error>;
    pub async fn disconnect(&mut self, callbacks: &mut C) -> Result<(), Error>;

    // transport 实际编码/提交操作；不向应用开放绕过 pending 的请求入口。
    fn allocate<T: Api>(&self, request: &T) -> Result<MsgBuf, Error>;
    fn try_send<T: Api>(&self, request: &T) -> Result<(), Error>;
    async fn recv(&mut self, deadline: Instant) -> Result<Option<MsgBuf>, Error>;
    fn dispatch_response(&mut self, callbacks: &mut C, message: Message) -> Result<(), Error>;
    fn release_input_queue(&mut self) -> Result<(), Error>;
}
```

`api_client_messages!` 的消息 Path 列表生成 MessageId/Message、Api 元数据匹配、完整解码和
From<具体消息>；其后的 constructor 函数生成同名 Client 请求方法。普通请求的 constructor
参数为具体请求字段或 owned 请求，返回该 Api 类型；STREAM constructor 用 `#[api(stream)]`
声明额外 details callback 参数。协议回复和完成模式仍只从 `Api::SERVICE` 取得，声明的 callback
个数必须与 stream_message 一致。当前实际请求声明是：

```rust
hammer_component_macros::api_client_messages! {
    [MemclntCreateV2, MemclntCreateV2Reply, MemclntDelete, MemclntDeleteReply,
     MemclntKeepalive, MemclntKeepaliveReply, ControlPing, ControlPingReply]
    fn control_ping() -> ControlPing {
        ControlPing { id: 0, client_index: 0, context: 0 }
    }
}
```

复用 codec Serializer 的计数模式 `serialized_len` 得到分配长度，不分配临时 payload，
不复制 Rust 布局大小；写入仍由同一个 serializer 完成。Api derive 只按实际头字段生成
`context()` 和 `set_request_header()`，delete 不伪造 context。上述新增方法归 IPC codec/definition；
现有 codec 只有写入后长度，不能在共享存储分配前计算变长消息空间，故需要计数入口。

请求提交读取 `Api::SERVICE`，完成服务器 ID 填写、client_index/context、
编码长度和预期回复规则。`control_ping` 是普通 REG 的生成结果，不是 transport 中的特殊 if。
无参 ping 之外的请求方法接收该具体 owned 请求；不能让应用填任意 server ID 或任意 payload_len。
DUMP 生成器必须检查两个消息都可用，并复用 **现有 `SvmQueue::add2`** 一次提交 dump 与同 context
的 control_ping；不能连续两次 add 假装事务。STREAM 只发送一条请求，但先保证请求环有两个空位。
这正是控制消息测试必须牵连的生成器、消息身份和队列提交改动。

### 8.5 init、connect、disconnect 的实际语义

`new` 对应创建空 VAPI context。现有 ApiMain map/unmap 是底层映射生命周期，server init 的
setup 与 Client 的 name/CRC 发现是两件事。当前接口 `connect` 对应 **已映射区域上的握手**，
并不等于 `vapi_connect_ex` 的 map+握手复合入口；必须在调用文档和验证中保留这个区别。
应用生命周期持有 root SvmRegion，调用既有 `ApiMain::map_shared_region(..., false)`，随后连接；
全部连接退出后才调用同一 owner 的 unmap。后续若提供复合入口，必须拥有 root/backing 生命周期，
不能只写一个 init helper 然后遗漏清理责任。当前不得宣称该部分已完整移植为 VAPI 顶层 SDK。

connect 的步骤是：

1. 检查未连接、无遗留队列；为请求环和客户端映射表预分配进程 Main Heap 存储。
2. 在 `self.api` 的 Data Heap 建立响应 SvmQueue，结束 heap activation/lock。
3. 局部构造 create-v2，ctx_quota=0、keepalive=true，与 VAPI connect_ex 对齐；
   `handle_keepalives` 只决定客户端是否自动回答，不能混成 server 是否发送 probe。
4. 提交一次 create，异步等最多十秒；不在 Client 保存 request/reply，也不在超时后重发 create。
5. 收到成功回复保存 index，将共享表复制/解码到该 Client 的本地 message_table，然后释放回复。
6. 对所有 MessageId 的 NAME_CRC 建立双向映射；缺失是 None，不把 local ID 当 server ID。
   ping 两方向必须可用；自动 keepalive 还须 probe/reply 两方向均可用，后者比 C 多检查 reply
   可用性，避免向不支持的 ID 发送。全部验证成功才设置 connected=true。

握手拒绝且 peer 不再持有 queue 时释放队列。成功注册后的表导入失败走 delete 清理；已提交
create 的超时/取消并不能证明 peer 未接收，不得提前释放响应队列。

正常 disconnect 发送一次 handle=0、do_cleanup=false 的 delete，等待最多两秒，排空其它消息；
收到 delete reply 后释放 queue/本地映射并以 NoResponse 完成剩余 pending。do_cleanup=true 的 Add
提交后，queue 清理责任转给 server，Client 立即清除访问权；提交前失败保留访问权。
映射是 ApiMain 所有，不是该 Client 私有；关闭一个连接不能 unmap 其它连接正在使用的区域。
VAPI public disconnect 会 unmap，Hammer 的显式共享 owner 是有意差异，不能机械复制该调用。

等待取消只终止等待；已提交请求仍占 pending，后续 dispatch 继续消费。握手 timeout、无法确认
server 已停止使用 queue 的 disconnect timeout 必须保留该 Client 和映射。重复 connect 只续收
已提交 create 的回复（参数仍以原提交为准），disconnect 可先完成这次握手再删除；重复 disconnect
只续收已提交 delete 的确认，禁止重发。create_pending/delete_pending 仅记录这两项跨 await
的实际协议义务，不缓存请求/回复，不枚举虚构连接 state。确认 delete 后先清除 index，再清理队列，
使清理故障后的重试不会再次删除 registration。do_cleanup=true 转交队列之后，仍调用 disconnect
完成剩余 pending callback；这之前不允许 reconnect。peer 永久失联后的区域回收仍归进程/映射
生命周期，不能以 Drop 自动释放仍可能被 peer 使用的队列。

## 9. 分发、完成和 control_ping 验证路径

一次 `dispatch_one`：从本连接队列取得 MsgBuf → 验证 prefix/长度 → server ID 经
local_message_ids 查本地 MessageId → 按生成分支解码 owned Message 并检查完整消费 → 释放
MsgBuf → 判断实际 context 和请求环/事件规则。共享借用结束后才进入应用 callback；callback
panic 由应用执行边界处理，不能因 panic 遗漏 free。未知或不支持 ID 释放后返回明确拒绝，不能
像现有原型那样悄悄忽略并返回“已成功分发”。

`recv` 拦截已发现 ID 的 keepalive；自动回答用 client index 作 context，使用 self.api 的 client
allocator 和已发现 reply ID。Add 的 QueueFull/LockBusy 后异步重试，提交后的通知错误只报告，
不重复发；重试受本次 deadline 限制，不能让 ping/退出超时被 keepalive 无限拖延。没有自动处理
时，probe 走与其它事件相同的 event_callbacks/generic_callback 路径。

| 回复情形 | 请求环和 callback |
| --- | --- |
| 无 context 或高位为零 | 按本地 ID 调具体事件 callback；缺省调 generic；两者均无才按未订阅事件丢弃。 |
| 高位为 1、context 不在环中 | 丢弃迟到/未知关联回复，不移除任何 pending；不是事件。 |
| 匹配后面的 context | 先完成更早请求为 NoResponse，STREAM 的两槽分别通知；只在存在匹配后执行，未知 context 不清空环。 |
| Regular | 预期回复 ID 才调用最终 callback 并弹出；不能用同 context 的任意消息完成请求。 |
| Dump | details ID 调 callback(is_last=false) 并保留；同 context 的 ControlPingReply 调 Ok(None)/is_last=true 并弹出。 |
| Stream | details ID 调当前槽 callback(false)；terminal reply 必须匹配下一 Regular 槽的同 context/ID，移除 details 槽，再完成 terminal 槽。 |
| 已知 context、错误回复 ID | 不拿错误类型调用 callback、不移除该请求，返回具体 UnexpectedResponse；可由后续正确回复完成。 |

最后一项是 Rust 相对 VAPI C 的主动加强：C 的 REG 路径没有同等严格的 response_id 检查，
不能把它的未校验 payload 转成 Rust 的另一具体类型。

```text
应用 map 当前 ApiMain → Client::new（捕获该 owner）→ connect().await
  → create-v2 → 导入所有 NAME_CRC → 建立双向 ID 表
  → control_ping(callback) → 一槽容量检查 → 分配 context
      → 查数值 server ID → client allocator/codec → Add
      → Add 提交后登记 Request { response_id: ControlPingReply, mode: Regular }
  → dispatch(&mut application, timeout).await
      → 通用 recv/keepalive → 数值 ID 映射 → owned 解码/free
      → 通用 response 匹配 → callback 得到具体 ping 变体
  → disconnect(&mut application).await → 最后一个 Client 结束后应用 unmap
```

Add 提交和登记 pending 之间不 await，不调用应用 callback；请求环已预留容量，所以登记不分配。
提交前错误释放编码消息且不登记请求；提交后通知失败仍登记请求，不以同一个错误驱动应用重发。
`control_ping` 的回调检查 context、retval、client_index、server PID。还必须用相同生成/dispatch
路径验证 DUMP 的 ping 终止和 STREAM 两槽容量，不能加两个无效枚举分支就宣称覆盖了 VAPI。

### 9.1 必须联动的改动和实施状态

| Owner | 最终改动 | 当前状态 |
| --- | --- | --- |
| api.rs | 保留默认 Main + TLS selector；无 per-thread server pool，已有 &self 方法不重查 TLS | TLS 已恢复；server 仍受 runtime 主线程检查 |
| memory_shared.rs | 六个 alloc 入口和普通 free 归实际 ApiMain，ring/header/heap 使用同一 owner | 已迁移，不能再引用 ShmemHeader::alloc 或 MsgBuf::free |
| memory_client.rs | 捕获 api；用一般 Request/C 回调、双向 ID 表、通用分发替换 ping oneshot 模型 | 已替换 oneshot；请求环、双向映射、数字分发、事件回调和可续收握手均已实施 |
| definition / component macros | 从已有 Api/Service 生成本地 MessageId、owned Message、REG/DUMP/STREAM 请求；保留 server setup 宏 | 固定表宏与客户端 inventory/请求宏均已实施，复用 Api/Service 和 codec |
| SvmQueue | DUMP 的 request+ping 原子入队 | 复用现有 add2，不新增 queue wrapper |
| service/runtime | Main 安装 → map → setup/API hooks → async Process；TLS 不进通用调度器 | 默认安装已接线，完整 mapping/退出接线未完成 |
| dylib/rlib 构建 | 各使用方须解析到同一个 IPC global/TLS 定义，不能每个 DSO 静态复制一份 | IPC 已声明 dylib/rlib；真实跨 DSO 身份验证待做 |

本节新增类型/API 的必要性：Request 表达异构在途请求；RequestMode 表达协议完成规则；生成的
MessageId/Message 表达编译时消息全集；Client<C> 用具体应用借用替代 C void*。现有服务器
ApiMsgData 和 ping 专用 oneshot 均不能承担这些职责。它们属于本次客户端迁移，编译与行为证据按第 13 节记录。新错误只在客户端边界扩展：RequestsFull { capacity: usize } 由应用 dispatch
后重试；MessageUnavailable { id: MessageId } 由调用方停止不兼容请求；UnknownMessageId { id: u16 }
拒绝不支持的入站消息；UnexpectedResponse { context: u32, expected: MessageId, received: MessageId }
保留 pending 等待正确回复。沿用已有 Queue/Codec/Region source，不按内部 helper 再建错误族。


## 10. Process 与 AsyncFileMain

### 10.1 保留异步 task

[复用] `Process<S,F>`、`RunningProcess`、`ProcessStartFn`、`process_start`、
`__register_process`、独立 task JoinHandle 和事件 mpsc 投递路径。Process 由现有主线程
Tokio runtime poll；ApiMain 不增加 Process scheduler、deadline 或 resume 方法。

```rust
#[hammer_component_macros::process_node(name = "binary-api")]
fn binary_api_clnt(main: &mut DataPlaneMain)
    -> impl Future<Output = RuntimeResult<()>> + Send + 'static;
```

factory 同步执行 setup、run_api_init 和 process_events，future 只带 receiver 与运行局部变量，
不捕获 `&mut DataPlaneMain` 跨 await。future 每轮：

1. 首先检查 prequeued SHM 消息，最多 drain 约 10 µs，不依赖 socket event 才开始接收。
2. 检查独立十秒 scan deadline；到期在同步 barrier scope 内执行 api.dead_client_scan(now)。
3. `yield_now().await` 让其它主线程 task 得到执行机会。
4. select 原 events.recv 和最近的 queue-check/scan deadline；400 µs queue-check 是当前 Hammer
   没有跨进程 readiness 通知时的兜底轮询选择，不是 VPP 固定参数。
5. 完整 socket message token 调 SocketMain::process_message；remove token 调 close。

Empty 结束本轮 drain；Sub 已提交的通知错误报告后继续轮询，不再次出队；无法恢复的队列错误
停用 memory 接收，由 future 保留该启用事实。task abort 后必须 join 再执行 domain cleanup，
同步 handler 中不会在持有 barrier/queue lock 时 await。runtime 生命周期负责区分取消、panic、
inner operation error；此次不另造 RuntimeError 的字符串包装。

### 10.2 File callback 与 socket owner

[修改/迁移] 删除 `BinaryApiConnections` 和原 `process_event`。目标实际 owner：

```rust
pub struct SocketMain {
    listener: u32,
    socket_path: PathBuf,
    socket_device: u64,
    socket_inode: u64,
    max_frame_bytes: usize,
    clients: UnsafeCell<Pool<BinaryApiConnection>>,
    process_messages: UnsafeCell<Pool<(u32, Vec<u8>)>>,
}

impl SocketMain {
    pub fn bind(path: impl AsRef<Path>, max_frame_bytes: usize)
        -> Result<Self, BinaryApiServerError>;
    fn accept_ready(&self, fd: RawFd) -> RuntimeResult<()>;
    fn read_ready(&self, graph: &mut NodeMain, index: u32, fd: RawFd) -> RuntimeResult<()>;
    fn write_ready(&self, graph: &mut NodeMain, index: u32, fd: RawFd) -> RuntimeResult<bool>;
    fn process_message(&self, index: u32) -> RuntimeResult<()>;
    fn close(&self, index: u32) -> RuntimeResult<()>;
    fn shutdown(&self) -> RuntimeResult<()>;
}
```

callback 的 File 已由 FileMain 借出，不能通过 file_index 再借同一个
File 调 FileMain::accept/read_some/write_some。callback 使用传入 File 的 descriptor 做 accept/read/write；
accepted socket 注册新 File，保存 add 返回的真实 file_index。

```text
FileMain listener registration → listener_file_index
  → main[0] 既有 AsyncFileMain 等待 readiness
  → FileMain 调 listener callback(&mut File)
      → accept → connection Pool.insert
      → FileMain.add(新 File) → accepted_file_index → connection.file_index
  → read callback(&mut File)
      → read / framing → process_messages.insert((connection_index, complete_frame))
      → signal Process(complete_message_index)
  → Process 消费完整消息 token
      → 从 process_messages 取走 owned frame，结束 Pool 借用
      → dispatch → 输出放回 connection，启用 write interest
  → write callback(&mut File)
      → send / flush → 修改该 File 的 write-enabled 标记
```

| Index | 来源与用途 |
| --- | --- |
| listener/accepted file_index | FileMain Pool 返回值，用于注册后 interest 更新和删除 |
| connection_index | SocketMain.clients Pool slot，存于 File.private_data |
| complete_message_index | SocketMain.process_messages Pool slot，唯一进入消息 Process event 的 token |

FileMain 在 callback 借用结束后对比 write interest 并更新 poller，避免 callback 重借 File。
close 先删除 File，再释放可复用 connection slot；尚未消费的消息 token 的 Pool slot 不提前复用，
标记连接已移除直至 token 被消费。不能用只有 pool index 的 token 假装具有 generation 校验。

静态存储是 `OnceLock<SocketMain>`，其 clients/process_messages UnsafeCell 私有，所有入口只在
runtime 主线程执行。没有 Arc → &mut 转换。退出 hook 显式 shutdown，不依赖 static Drop。

socket 仍使用现有长度前缀/protobuf envelope；本次 File/Process 修正并未迁移为 VPP socket 协议，
也未删除原 runtime BinaryApiMethod 表。必须单独验证 socket 回归，不能用 control_ping SHM 测试代替。

## 11. 消息存储与错误合同

### 11.1 Add/Sub 提交点

| 操作结果 | 消息 owner | 必须处理 |
| --- | --- | --- |
| Add 成功 | 消费者 | producer 不再 free/read/retry |
| Add 提交前 QueueFull/LockBusy | producer | free 当前编码，业务可以稍后重新尝试 |
| Add 提交后的 SignalAfterCommit/EventSignalAfterCommit | 消费者 | 保留原 source、记录已提交，不重发、不 free |
| Sub Empty/LockBusy | queue | 返回无消息/稍后再试 |
| Sub 已提交但通知失败 | 当前接收者 | 仍 dispatch/free 已取出的地址，再报告原 source |

每次 codec/queue source 只在真实 client API 边界翻译一次，不创建 ReplyAllocation、
RegistrationRegionLock、ClientQueueAddressInvalid 等按内部步骤拆出的重复 Error 族。
server 业务失败由实际 handler 决定是否回复、回滚或放弃无法回复的请求。

客户端原型已有 Error 字段（目标补充的四类错误及恢复动作见 9.1，不按内部步骤增加包装）：

```rust
pub enum Error {
    #[error("memory client already connected")]
    AlreadyConnected,
    #[error("memory API region is not mapped")]
    NotMapped,
    #[error("memory client is not connected")]
    NotConnected,
    #[error("create-v2 reply did not arrive for context {context}")]
    ConnectTimeout { context: u32 },
    #[error("delete reply did not arrive for client index {client_index}")]
    DisconnectTimeout { client_index: u32 },
    #[error("create-v2 rejected with response {response}")]
    CreateRejected { response: i32 },
    #[error("required API message `{name_crc}` is unavailable")]
    IncompatibleMessage { name_crc: &'static str },
    #[error("API response did not arrive for context {context}")]
    ResponseTimeout { context: u32 },
    #[error("API request lost its response for context {context}")]
    NoResponse { context: u32 },
    #[error("memory client queue operation: {source}")]
    Queue {
        #[source]
        source: SvmQueueError,
    },
    #[error("memory client codec: {source}")]
    Codec {
        #[source]
        source: codec::Error,
    },
    #[error("memory client region: {source}")]
    Region {
        #[source]
        source: SvmRegionError,
    },
}
```

AlreadyConnected/NotConnected 由应用修正调用顺序；CreateRejected 保留 response；
IncompatibleMessage 由应用处理协议不兼容；Timeout 是本次等待结束，不转移资源所有权；
NoResponse 对应有序 pending 请求的缺失回复。Queue/Codec/Region 保留原 source 分类供边界调用方处理。
未实现重启检测时不发布永远不会返回的 ServerRestarted variant。

### 11.2 SVM 安全与清理限制

ordinary region 是同构建、同虚拟地址的可信共享内存协议，不是恶意 peer 的隔离边界。
当前 shared table 使用 SVM 上的 Rust Vec 对象；其对象与数据 extent 检查不能证明 hostile peer
没有并发篡改 header，也不能宣称任意伪造的 Rust Vec 都安全。
同样，安全 handler 内的 unsafe queue attach 依赖这个信任和独占消息转移合同。

所有普通请求/响应只 free 一次。Data Heap 分配必须在同一所属堆释放，共享 Vec 的对象与
backing allocation 都要清理。本地 requests/message_table/Pool 不在共享堆扩容。
完整 server shutdown（registrations、serialized table、root/subregion）与重启/遗留队列回收
尚未验证；不能把 drop(Client) 或 daemon exit 当作已完成该协议。

## 12. 内联与删除清单

| 位置 | 标注 | 依据 |
| --- | --- | --- |
| ApiMain::current/set_main、handler<T> | inline(always) | 短 selector 和 safe 单态入口，不含 await、I/O 或循环 |
| ApiMain::is_mapped/get_msg_data/message_table | inline | 小查询；主线程检查和借用规则仍必须成立 |
| Client 的标量/ID getter、callback setter，生成的 Message ID 查询 | inline | 同一个 Client 值的直接字段访问 |
| MsgBuf 的长度/字节查询 | 保留原 inline | 短存储访问；不改变 unsafe 前提 |
| setup、connect/disconnect、dispatch、具体 handler、scan、socket callback | 无强制内联 | 含循环、系统调用、编码或冷错误分支 |

[删除] ApiMain 的业务方法、客户端状态、owner_thread；client publish/import/send/decode/finish 中转层；
手写 install_handlers 双表、scan_clients 及五个握手/keepalive 消息缓存字段；旧 BinaryApiConnections::process_event 和 readiness event → Process I/O 链。
[复用] 原 Api 元数据、codec、SVM/Pool、Process task、主线程 runtime 身份、FileMain、AsyncFileMain、barrier。
[新增必要接口] server macro 生成 setup；客户端生成的消息集合、Request 和回调接口详见 8/9 节。
原 MsgBuf::decode 增加完整 payload 消费校验；普通 alloc/free 迁移到实际 ApiMain。

## 13. control_ping 验证与当前完成边界

本节是可执行验证要求，未运行的项目不能标成通过。源码搜索只用于清理检查，不是行为测试。

| 验证 | 需要观察的行为 |
| --- | --- |
| 宏与编译 | 真实展开八个槽；safe fn 类型成立；CRC 来自 Api；非法 ID/策略在编译时拒绝 |
| 初始化 | map → bootstrap setup → 非空 image API-init 链 → first create；观察真实 hook 排序/call-once 和动态范围；client 不安装 server 表；快照包含 hooks 注册结果 |
| 双进程 connect/ping/delete | 只发送 create-v2；导入 CRC；ping 返回 context/index/PID；delete 后资源 owner 正确 |
| 协议布局 | create-v2 115、reply 30、ping 10、reply 18；reject 截断和尾部多余字节 |
| 真实 registry dispatch | MP-safe ping 无 barrier；create/delete 经 barrier；未知客户端不能改变 registration |
| 有界队列 | Add/Sub 提交前与提交后错误分别验证，不能重复发送/free 或遗失已出队消息 |
| pending | 一槽 REG、两槽 STREAM、DUMP 的 add2 + ping 终止、满环；错误 ID 不弹出，未知 context 不清环；NoResponse、取消/timeout |
| 客户端发现/回调 | server ID 与本地 ID 不同、稀疏 ID、缺失 CRC；普通消息走数字映射；具体事件优先于 generic；callback 得到 owned 值 |
| TLS/owner | 默认 daemon Main 不复制；两个不同区域 Main、两个 Client 交替恢复，TLS 改选不改变已绑定 Client 的分配/释放 owner；客户端线程不碰 server registry |
| DSO 身份 | 真正构建并加载两个 IPC 使用方，检验 default Main 与 TLS selector 都解析到同一个 owner，不能只 cargo check |
| keepalive | 自动回复开关、queue-head 推进、十秒 deadline 不被 ping/socket 流量重置 |
| Process | 无 socket event 仍能 ping；drain 预算后其它 task 运行；runtime abort/join 后再释放映射 |
| socket 回归 | 真实 callback accept/read/write；保留 file_index；分片/粘包、backpressure、延迟 token、slot 复用 |
| 清理 | create 拒绝、表导入错误、delete 两种模式、通知错误、shutdown、重启；保留主错误与次要 source |

清理检查：

```bash
rg -n 'owner_thread|publish_client_message_table|send_registration_reply|reply_control_ping|create_registration|delete_registration|note_keepalive' crates/hammer-ipc/src/binary_api
rg -n 'BinaryApiConnections|process_event|read_some|write_some' crates/hammer-service/src/binary_api.rs
rg -n 'ApiMain|SocketMain|ApiRegistration|ControlPing' crates/hammer-runtime/src
```

第一组不得残留旧业务入口和冗余 owner 字段；第二组只允许原 process_events 订阅符号，
不能存在旧 readiness dispatcher；第三组不应引入 Binary API owner 类型。

当前工作树已完成第 8/9 节的通用客户端迁移：请求环、双向 ID 表、inventory/请求生成、
owned 解码后 free、REG/DUMP/STREAM 完成规则、事件优先级、create/delete 续收和连接清理。
生产代码和 control_ping/show_version 测试均为 control bootstrap → run_api_init；不存在
`binary_api_hookup` 转移 bootstrap 的声明。service image 的非空 API-init 表安装
VPE range，control_ping 仍不移入普通 hookup。

验证范围（2026-09-15）：IPC integration 使用实际 SHM 队列与 runtime Process
执行 create-v2 → control_ping → delete；owner 内测试用同一队列验证满环、超时续收、错误 ID、
未知 context、NoResponse、事件优先级与重连，不开放私有 registration 给测试 crate。
当前 inventory 没有 DUMP/STREAM 业务消息，不能把 REG ping 测试称作两种流式服务的行为证明。
双进程、真实 DSO 身份、daemon 完整 mapping/退出、失联重启和 socket 回归不属于这组证据。
这些边界不改变 SHM 客户端代码迁移必须在测试前完成的要求。

提交前门禁是目标 ADR 的真实 `show_version` SHM 往返；其编译和运行结果记录在 PR。
不把 socket-only 或独立 `cargo check` 结果当成 memory API 行为证明。

首轮门禁的 IPC 失败已由 GDB 栈定位：测试配置 count=0 被 runtime 拒绝，随后 libtest 释放
Main Heap 切换前的 System 捕获缓冲区触发 abort。测试改用合法的一名 Data Worker，并复用
infra SVM 测试的子进程隔离和显式退出；测试函数内先完成 client、Process、region 的实际清理，
再退出以避开父测试框架的分配跨 heap 生命周期。没有放宽 allocator 的来源校验。

目标测试必须在具备 runtime FileMain 所需内核能力的环境运行；环境失败不能被记录为
协议通过，也不能替换 Process 或放宽 runtime 能力检查。
