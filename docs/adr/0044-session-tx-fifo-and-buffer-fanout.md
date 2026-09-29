# ADR-0044: Session TX FIFO、Buffer 链、输出扇出与 TCP RX OOO

- 日期：2026-09-28
- 状态：实施中；源码审查与未完成项见 `docs/reviews/issue-364-adr-0044.md`
- 前置：ADR-0037、ADR-0038、ADR-0039、ADR-0042、ADR-0043
- 取代：上述 ADR 中关于 `TxMode`/`TransportTxMode`、单包 TX、TX 参数快照、
  `SessionTxPacket` 和 TX output next 的设计；并更正 TCP RX 对 FIFO OOO segment
  的使用；其他非 TX 部分不变
- 范围：service 的 Session FIFO 与 queue 调度、plugin-session 的 IP Session 类型和
  output 注册、具体 transport 的 `Transport` trait 实现，以及 TCP 接收对 Session RX FIFO
  OOO segment 的使用

## 源码依据与事实

以下行号均指本仓库 `third_party/vpp/`。VPP 是行为和所有权依据，Hammer 不引入 VFT。

| VPP 源码 | 必须保留的语义 |
|---|---|
| `src/vnet/session/transport_types.h:14-23`；`src/vnet/session/session.c:1819-1847`；`src/vnet/session/session_types.h:293-315` | VPP **确有** `TRANSPORT_TX_PEEK/DEQUEUE/INTERNAL/DGRAM`，但它们在 transport 注册时选择 `session_tx_fns[session_type]`；一个 transport protocol 对应两个 IP family 的 Session type，而不是注册两次产生两个 protocol id。DGRAM 选择 dequeue 函数，函数内部才有 datagram 分支。 |
| `src/vnet/session/session.h:22-43,82-170,210-225` | 每 worker 有 cacheline 分隔的 TX context、可重用 buffer 向量、pending buffer/next 平行向量、可选 DMA ring；`wrk->vm` 指向所属 dataplane main；Main 按 Session 类型保存 TX 入口和 output next。 |
| `src/vnet/session/session_node.c:923-973,1858-1910,2033-2161` | queue 以 `128` packet frame 预算处理 control、新 IO、旧 IO；TX/FLUSH 由 Session 类型分发；pending packet 在一次 queue trace 后集中 flush；本轮开始时已 pending 的 buffer 先占预算。 |
| `src/vnet/session/session_node.c:976-1071` | 普通路径从 FIFO `peek` 到目标 Data Plane Buffer；仅启用 DMA 时借 FIFO segments 安排传输。首 buffer 与链尾分别处理。 |
| `src/vnet/session/session_node.c:1073-1237` | 首 buffer 预留 `TRANSPORT_MAX_HDRS_LEN`；剩余 payload 链入后续 buffer，设置 `NEXT_PRESENT`、`next_buffer`、`TOTAL_LENGTH_VALID` 与 tail 长度。Peek 只推进本轮 `tx_offset`，dequeue 消费 FIFO；datagram 则按 header 的 `data_offset` 更新并在完整消费后丢弃整个 record。 |
| `src/vnet/session/session_node.c:1239-1435` | `session_tx_not_ready`、connection/listener 选择、MSS/发送窗口/可读长度、datagram 完整性/GSO、每段和每首 buffer 容量、全部 buffer 数量在分配前计算。 |
| `src/vnet/session/session_node.c:1437-1468` | 发送完成后先清 FIFO event，再重新检查可读字节并有条件重设 event；无可读字节才 deschedule。普通和 DMA pending buffer 分流。 |
| `src/vnet/session/session_node.c:1472-1677` | `TX_FLUSH` 先调用 transport `flush_data`；custom TX 先使用本轮 burst；再取得即时 send params、应用 pacer、一次分配整批 buffer、成批填充、一次调用 `push_header`、再放入 pending 向量。`n_left >= 4` 循环每轮预取后处理 **两个** packet，其后处理尾数；不能误写成每轮四包。 |
| `src/vnet/session/session_node.c:1680-1747` | peek/dequeue 是同一 FIFO algorithm 的两个入口；internal 是独立 custom TX 路径，具有自己的 burst、重排、FIFO event 与 dequeue 通知规则。 |
| `src/vnet/session/session_node.c:1965-1973,2132-2159`；`src/vnet/session/session.h:1093-1115` | worker 的 pending buffer/next 成对扇出，随后重用向量；transport 直接提交的 control packet 也进入同一 pending 列。DMA 完成前不得扇出未写完的 payload。 |
| `src/vnet/session/session.c:108-141,172-202,657-680,730-749,2119-2194` | TX event、custom TX 优先级、`session_tx_fifo_peek_bytes`/`session_tx_fifo_dequeue_drop`、应用 dequeue 通知及 DMA completion 是不同操作；DMA 配置失败时该 worker 走非 DMA 路径。 |
| `src/vnet/session/transport.h:21-55,75-90,204-208,330-369`；`src/vnet/tcp/tcp.c:1171-1196,1372-1380,1402-1431` | transport 实时给出 `snd_space/tx_offset/snd_mss/flags`，TX_FLUSH 通知 TCP 的 PSH 逻辑，pacer 在 Session TX 前后由 connection 更新。 |
| `src/vnet/tcp/tcp.c:1128-1196`；`src/vnet/tcp/tcp_output.c:1804-2065,2070-2186` | TCP 普通 FIFO TX 在 recovery 中停止；custom TX 除重传和 ACK 外，还会按 recovery/PRR 预算直接从同一 Session TX FIFO packetize 未发送的新数据。其 packet 计数用于 frame 预算，ACK buffer 分配失败不改变 `tcp_send_acks` 的计数。 |
| `src/vnet/tcp/tcp_output.c:884-1035`；`src/vnet/tcp/tcp.c:1590-1630,1743-1749` | TCP `push_header` 对整个 packet 批次更新 sequence、拥塞和重传 timer；TCP 注册 4/6 两个 output edge，worker 缓存对应 next，以便 ACK、重传和 timer control packet 进入同一 Session pending 列。 |
| `src/vnet/tcp/tcp.c:789-799,1703-1707`；`src/vnet/tcp/tcp_rack.c:174-485,506-805`；`src/vnet/tcp/tcp_bt.c:1710-1742`；`src/vnet/tcp/tcp_output.c:2125-2148` | VPP 的 RACK 仅在启用并协商 SACK 后使用 byte tracker；按最新交付传输的发送时序、RTT 与重排窗口判丢，REO timer 安排重传，不直接完成一个样本。VPP 默认关闭；Hammer 按产品要求默认开启，但仍以 SACK 协商为门槛。 |
| `src/vnet/session/transport.h:81-88,183-191`；`src/vnet/session/session_node.c:1496-1504,1884-1893` | `flush_data` 只在 TX_FLUSH 分支、普通 TX 参数计算之前调用；RX IO event 调用 transport 的 `app_rx_evt`，它不是 AppWorker RX callback。未提供 RX callback 的 transport 在 VPP 是成功的 no-op。 |
| `src/vnet/session/session.h:917-975`；`src/vnet/tcp/tcp.c:1372-1401` | TCP flush 从 Session TX FIFO 的 consumer 可读量确定 `psh_seq`；App RX 只在曾发送零窗口时检查 RX FIFO 空间，阈值为 `clamp(size >> 3, 4 KiB, 128 KiB)`，不足时请求 dequeue 通知，否则发送 ACK。 |
| `src/vnet/tcp/tcp_output.c:917-930,1037-1058`；`src/vnet/tcp/tcp_types.h:115-125,490-499` | PSH 仅写入覆盖 `psh_seq` 的 TCP 段；独立 ACK 使用 Session 的 pending buffer/next，不构造应用通知或新的 Session TX event。ACK buffer 分配失败时 VPP 更新接收窗口并计 TCP worker `no_buffer`，不是 Session Queue node error。 |
| `src/vnet/tcp/tcp_input.c:494-545,885-895,1407-1412,2365-2370`；`src/vnet/session/session.c:738-749` | ACK 不逐包直接 drop Session TX FIFO：TCP worker 先合并 `burst_acked` 到 `pending_deq_acked`，输入 burst 结束后每 connection 只 drop 一次，触发应用 dequeue 通知，再重排、更新重传 timer 和 pacer。 |
| `src/vnet/tcp/tcp_input.c:2726-2772,3006-3235` | TCP input 从当前连接的 `state` 与过滤后的 FIN/SYN/RST/ACK 查 dispatch 表；表项同时给出 next 与 enum 错误，未列出的组合为 `DISPATCH`/drop。跨 worker 报文先交给目标 TCP input 再查表，不在 tuple lookup 中缓存 next。Hammer 的无连接 pending ACK 仍须交给现有 listen 握手路径，因为它没有可查的 TCP connection state。 |
| `src/vnet/tcp/tcp_inlines.h:282-377`；`src/vnet/tcp/tcp_input.c:2896-2960` | `tcp4/6-input-nolookup` 与普通 input 共用解析、批处理及状态分发表；区别仅是从 Buffer 预选的 TCP connection index 直接取本 worker 的连接，不做 tuple/session lookup。Hammer 的预选索引只存 TCP secondary opaque，不扩展通用 IP opaque。 |
| `src/vnet/tcp/tcp_input.c:977-1101,1147-1210,1361-1412` | TCP RX 先按 `rcv_nxt` 区分旧包、重叠、未来和按序数据。未来字节按相对 `rcv_nxt` 的 offset 写入 **Session RX FIFO 自身的 OOO segment**，不推进 `rcv_nxt`、不发应用 RX event；按序写入时 FIFO 收集相邻 OOO segment，返回值含补洞后连续可读字节。乱序发 DUPACK；FIN 仅在 `seq_end == rcv_nxt` 时接受。 |
| `src/vnet/tcp/tcp_input.c:1361-1412,2260-2370` | established 与 receive-process 都先处理 ACK、再将 payload 交给 Session，最后按**原 packet 的** `seq_end == rcv_nxt` 判断 FIN；`FIN_WAIT_1` 有 `FINPNDG` 时不能直接进入 `CLOSING`，`FIN_WAIT_2` 有未读 RX 数据时先 flush RX 通知再通知 transport closed。input frame 尾部依次 flush RX、处理 postponed TX dequeue、处理 pending disconnect/reset，随后释放原 Buffer。 |
| `src/vnet/tcp/tcp_sack.h:127-147`；`src/vnet/tcp/tcp_input.c:494-545,875-892` | `ac.bytes_acked` 是 ACK 反馈分类的结果，只有非零时把 connection 首次排入 `pending_deq_acked` 并累加 `burst_acked`；不是每 packet 在 service 直接 drop FIFO，也不能把未获 ACK 处理接受的序号差误当成 dequeue 字节。 |
| `src/vnet/session/session.h:685-829`；`src/vnet/session/session.c:690-715` | `session_enqueue_stream_connection` 的 `queue_event=1` 仅用于按序路径，Session worker 合并同一 burst 的 RX 通知并在 TCP input node 末尾 flush；OOO 路径以 `queue_event=0` 返回。多 Buffer 链按同一逻辑 offset 继续写入 FIFO。 |
| `src/svm/svm_fifo.c:171-340,593-726,833-930`；`src/svm/fifo_types.h:105-106`；`src/vnet/session/transport.c:1205-1210` | FIFO 的 `ooo_segment_add` 负责相邻/重叠区间合并，`svm_fifo_enqueue` 补洞并收集 OOO segment。TCP 创建 Session FIFO 后初始化 RX 的 OOO enqueue **chunk lookup**、TX 的 OOO dequeue **chunk lookup**；两者不是 segment 索引。TCP SACK 查询 FIFO **合并后的 newest OOO segment**，不是刚收到的 packet 区间。 |
| `src/vnet/tcp/tcp_error.def:22-26,51`；`src/vnet/tcp/tcp_input.c:1001-1101,1160-1210` | `ENQUEUED`、`ENQUEUED_OOO`、`FIFO_FULL`、`PARTIALLY_ENQUEUED`、`SEGMENT_OLD`、`ZERO_RWND` 是 TCP input node 的逐包分类/计数，不是 service `SessionError` 或数值 retval。 |
| `src/vnet/session/session_types.h:495-517`；`src/vnet/session/session_node.c:1316-1383` | datagram 前缀为长度和偏移，其后是具体连接 metadata 与 GSO 大小；`ip46_address_t` 是 IP 实例，不属于通用 service。 |
| `src/svm/svm_fifo.h:70-108,467-475,690-718` | consumer 自己的 head 可 relaxed 读，producer 的 tail 必须 acquire 读；`set_event` 是 release 交换，`unset_event` 是 acquire 交换，随后必须重检 FIFO 再决定重排。 |

现状核对：`crates/hammer-service/src/session/core.rs` 的 `tx_fifo_peek_and_send` 每次只分配一个
buffer，预留 `60` 字节，按一个 MSS 拷贝并直接返回；没有 VPP 的整批 buffer 预算、链尾、
`n_left >= 4` 循环、pacer/custom TX/通知完整时序。`SessionTxContext.tx_mode`、
`SessionMain.session_tx_modes` 和 `TransportOptions.tx_mode` 是把 VPP 注册选择误做成了运行时状态。
当前 `SessionDmaTransfer` 只有两个标量，VPP 对应的是两条 pending 向量。TCP 注册 output next
为 `u32::MAX`，TX callback 自己按 IP family 选择 `tco_next_node`；目标应是注册期按 Session 类型
建立 queue 的 output edge，不让 service 解释 IP。TCP 的 ACK 路径目前直接调用 FIFO
`drop_dequeue`，漏掉 VPP `session_tx_fifo_dequeue_drop` 所执行的应用 dequeue 通知。
当前 service 还把 `Session.session_type` 直接当 transport protocol，并由每次
`register_transport_type` 分配一个新 protocol id；若直接按 IPv4/IPv6 调用两次，
同一个 TCP 会变成两个 protocol，RX/control/time 与 TX 注册无法对应。
RX 并非完全没有 OOO：现有 `established.rs` 会以 offset 调用
`SessionWorker::enqueue_rx`，`hammer-infra::svm::Fifo` 也已有 `OooSegment`、
`enqueue_ooo` 和 `OooResult`。但目前 `enqueue_ooo` 把**本次写入**的 offset/length
放进 `OooResult`，且 `collect_ooo_segments`/`replace_ooo_segments` 每次用临时
`Vec` 重建 OOO pool/tree；service 又把链上各 chunk 的最小起点和最大终点当作一个
newest 区间；两者都不是 VPP FIFO 合并后的 newest OOO segment。
`rcv_process.rs`、`syn_sent.rs` 的 payload 分支还把 `enqueue_rx` 的 offset 固定为零，
未共同使用 established 路径的序号裁剪/OOO 判定；
`connection.rs::receive_open_reply` 对带 data 的 SYN-ACK 甚至在 RX FIFO 入队前
按 packet 长度推进 `rcv_nxt`，可能确认尚未收进 FIFO 的字节。
以上均是迁移清单，**本 ADR 不改代码**。

## 分层决定

`hammer-service::session` 拥有 queue 的 128 packet 预算、Session state、FIFO、TX context、
buffer 链、pending/next 向量、FIFO event 重排与 dequeue 通知。它不定义 IP endpoint、TCP
sequence、TCP header、SACK 或具体 transport connection。FIFO OOO 区间由
`hammer-infra::svm::Fifo` 管理；service 只按 transport 给的相对 offset 将 Buffer 链
写入 RX FIFO，并将**实际 FIFO 结果**交还 transport，不建立第二份乱序重组表。
`hammer-service::transport::Transport`
仍是 trait；`send_params`、`flush_data`、`custom_tx` 和批量 `push_header` 由具体插件实现。

`hammer-plugin-session` 为 TCP 的 IPv4/IPv6 等具体 Session 类型配置 output edge，并把每种
Session 类型的**单态化 TX 入口**注册给 service。同一 TCP protocol id 只分配一次，
plugin-session 为两个 IP family 给出不同的 opaque Session type；service 的 Session
分别保存 `session_type`（TX 函数/output next 索引）和 `transport_protocol`
（RX/control/time 入口索引），不解释 IP family。注册点选择 peek、dequeue、datagram 或
internal 的实际入口，不保存 `TransportTxMode`、其别名或改名后的等价运行时 mode。
`IpSessionFamily`、datagram 的 IP metadata 及 IP 地址处理只在 plugin-session。
TCP/UDP 依赖 plugin-session，TCP 以 peek 入口接到它实现的 `Transport`；未来 UDP 以
datagram 入口接入。两者的 `push_header`、`custom_tx` 仍由各自插件拥有。现有获批的按
protocol 单态化函数入口仅是 service 跨插件分发的边界，不是 transport VFT，也不在
每个 packet 上查找 `dyn` 对象。

### TCP 与 Session 接线偏差清单

下表只核对 TCP 数据收发及其直接依赖的 Session 生命周期/通知；IP lookup、
TCP options codec 和应用协议本身不在本 ADR 重做。路径均为本仓库现有源码，
VPP 路径均以 `third_party/vpp/src/` 为根。

| 当前路径与偏差 | VPP 依据 | 本 ADR 的目标 |
|---|---|---|
| `service/session/core.rs::SessionFlags` 无 custom-TX 位；`enqueue_ready` 直接加 TX event | `vnet/session/session.c:172-200`、`session_node.c:1496-1532` | Session 持有 custom-TX 标志和 FIFO event 去重；ACK/重传使用同一队列，优先级分别入 new/old，不造第二套发送队列 |
| `tcp/established.rs` 即时构造 ACK，`tcp/connection.rs::ready_segment` 自行选控制包 | `vnet/tcp/tcp_output.c:1058-1089,2070-2186` | RX 仅 program ACK/dupACK；TCP custom TX 在 Session queue 预算下生成实际 ACK Buffer |
| `tcp/established.rs` 的 OOO SACK 取 `OooResult` 原 packet 区间 | `vnet/tcp/tcp_input.c:1051-1101`、`svm/svm_fifo.c:171-299` | SACK 只读取 FIFO 合并后的 newest OOO segment；无新 segment 不虚构范围 |
| `tcp/rcv_process.rs`、`tcp/syn_sent.rs` 固定 offset 零；`receive_open_reply` 在 enqueue 前推进 `rcv_nxt` | `vnet/tcp/tcp_input.c:1147-1210,1897-1907,2260-2271` | 三个 data-bearing input 分支复用旧/重叠/未来/按序判断，仅按 FIFO 实际可读推进 `rcv_nxt` |
| `infra/svm/fifo.rs` 用临时 `Vec` 重建 OOO 区间、把 segment 索引误写进 chunk lookup、返回未合并写入范围；共享引用下通过非 `UnsafeCell` 字段写 head/newest | `svm/svm_fifo.c:171-340,593-726,833-930`、`svm/fifo_types.h:105-106` | 原位更新已有 `Pool<OooSegment>`/list，无 packet-path `Vec`；chunk lookup 仅定位 FIFO chunk；两个可变 list 索引纳入 producer-owned `UnsafeCell`，返回 newest 合并区间 |
| `service/session/core.rs::enqueue_rx` 把链 chunk 的 min/max 拼作一个 segment | `vnet/session/session.h:685-829`、`vnet/tcp/tcp_input.c:1081-1096` | 同一 packet 的链偏移连续推进，以最后一次 FIFO OOO 插入的 newest 标记为准；仅按序排应用 RX event |
| `tcp/established.rs`、`rcv_process.rs`、`syn_sent.rs` 每个 ACK 直接 `drop_dequeue`/`enqueue_ready` | `vnet/tcp/tcp_sack.h:127-147`、`vnet/tcp/tcp_input.c:494-545,875-892,1407-1412,2365-2370`、`vnet/session/session.c:738-749` | ACK 反馈给出 `bytes_acked`，worker 在 burst 内合并；末尾每 connection 一次 Session TX FIFO drop、应用 dequeue 通知、reschedule/timer/pacer 更新。service 不重新推算 TCP ACK 字节数 |
| `service/session/core.rs::tx_fifo_peek_and_send` 一次只发送一个 Buffer，预留 60 字节 | `vnet/session/session_node.c:976-1677` | 一个 event 按 frame 预算生成多个 MSS/Buffer chain，首 Buffer 预留 140 字节，一次批量 `push_header` |
| `service/session/core.rs::SessionIoDispatch` 合并 RX/TX 并返回 `(usize,bool)`；Session 类型等于 protocol | `vnet/session/session.c:1819-1921`、`session_node.c:1858-1910` | protocol 只注册 RX/control/time；IPv4/IPv6 Session type 各注册 TX/next，保留真实 TX outcome |
| `tcp/lib.rs::send_params` 调用旧 `tx_payload_budget`，混入 pacing/Nagle/intent；缺 VPP Limited Transmit 和真实 `snd_mss` | `vnet/tcp/tcp.c:1100-1196`、`vnet/session/session_node.c:1530-1588` | TCP 给出即时 MSS/CC/peer window/offset；dupACK/SACK 尚未进入 recovery 时按 VPP 限额 Limited Transmit，通用 pacer 仅由 Session TX 扣账 |
| `tcp/lib.rs::flush_data` 空、`connection.rs::tx_segment` 默认 PSH | `vnet/tcp/tcp.c:1372-1380`、`tcp_output.c:300-329,917-930` | TX_FLUSH 保存最后待 push 字节；仅覆盖该序号的 payload segment 置 PSH |
| `tcp/lib.rs::custom_tx` 恒返回零，timer/ACK 绕到即时控制包 | `vnet/tcp/tcp_output.c:1123-1270,1672-1708,1804-2186` | recovery 重传和受 PRR 允许的新数据从 Session TX FIFO 读取；ACK/dupACK 按剩余 burst 预算生成 |
| `tcp/lib.rs::push_header` 忽略 `available_bytes`，逐 Buffer 写并用 `BufferMain::global` | `vnet/tcp/tcp_output.c:884-1035` | 使用当前 runtime 对整批首 Buffer 写 header、按链长度推进序号、更新 cwnd-limited/RTT/timer；无全局第二借用 |
| `tcp/lib.rs::tcp_session_io` 的 RX 在检查 zero-window 前就改 `rcv_wnd`，还 clone 整个连接 | `vnet/tcp/tcp.c:1382-1401`、`tcp_output.c:1037-1058` | 无零窗口历史直接返回；不足阈值请求 FIFO dequeue 通知；满足时原位构造 ACK 并加入 Session pending 列 |
| `tcp/output.rs` 注册两个 output 后手动禁用 `session-queue`；`tcp/lib.rs::bind_worker_graph` 再按名称找 next | `vnet/tcp/tcp.c:1590-1630,1743-1749`、`vnet/session/session.c:2196-2270` | output init 编译 next 并发布两种 Session type；TCP worker 缓存已有 next；queue enable/disable 只归 Session 生命周期 |
| `tcp/worker.rs` 的 pending disconnect/reset 队列未被 input 消费，只有部分路径直接通知 service | `vnet/tcp/tcp_input.c:932-975,1407-1413`、`vnet/session/session.c:965-980,1131-1185` | input burst 末尾按 VPP 顺序排空：先 RX flush/dequeue，再 closing/reset/closed 通知；`Accepting` 延迟由 service 处理 |
| `tcp/lib.rs::update_time` 只推进 wheel，`tcp_session_update_time` 另处理 pending timer；`pending_cleanups` 未被消费 | `vnet/tcp/tcp.c:1293-1369`、`vnet/session/session_node.c:2033-2050` | 保持已有 `Transport::update_time(now,worker)` 签名和一次订阅：trait 更新 clock/收集 wheel token，同一 subscriber 再排空到期 cleanup、最后 bounded 派发 timer；cleanup 取消的 token 在派发时跳过，不在 service 执行 TCP 决策或二次推进 wheel |

这里的“全部”是上述 TCP↔Session 数据路径接点，不把纯 TCP 校验、IP routing 或
application callbacks 拉进 service。实际发送由 `Transport` 和 Session queue 完成。

现有 `SessionIoDispatch` 对 TX/RX 合并返回 `(packets, pending)`，不足以表示 VPP 的
head-old、tail-old、deschedule 和 no-buffer 四种处置。目标把 TX 入口与既有 RX 入口
分开；TX 入口由 service 的事件调度持有 event index，具体 transport callback 只取得
本 worker 的 Session/connection 并调用 service 的批量 FIFO 方法。`SessionTxPacket`、
per-Session send-params snapshot、每次 TX 的 `tx_mode` 和 transport 私有 payload copy
均不进入目标路径。

## 类型和注册

以下 Rust 块是**目标签名/执行契约**，不是当前实现；引用标在类型、方法旁。
复用已有 `CacheLineAlignMark`、`SvmFifo`、`Pool`、`BufferIndex`、`SessionHandle`、
`TransportSendParams`、`SessionEventType` 和 `Transport`。不新增 TX transport VFT。

```rust
// VPP session.h:22-43. Worker-owned scratch; no transport pointer or IP metadata.
#[repr(C)]
pub struct SessionTxContext {
    cacheline0: CacheLineAlignMark,
    session: Option<SessionHandle>,
    send_params: TransportSendParams,
    max_dequeue: u32,
    left_to_send: u32,
    max_len_to_send: u32,
    dequeue_per_first_buffer: u16,
    dequeue_per_buffer: u16,
    segments_per_event: u16,
    buffers_needed: u16,
    buffers_per_segment: u8,
    cacheline1: CacheLineAlignMark,
    tx_buffers: Vec<u32>,
    transport_pending_buffers: Vec<u32>,
}

// VPP session.h:59-69,139-155; session.c:2119-2134.
pub struct SessionDmaTransfer {
    pending_tx_buffers: Vec<u32>,
    pending_tx_nexts: Vec<u16>,
}

// These are TX outcomes, not SessionError values or numeric retval.
// VPP session_node.c:943-949,1472-1677.
pub enum SessionTxOutcome {
    Ok,
    NoData,
    NoBuffers,
}

// VPP session_node.c:1239-1273 returns three readiness decisions.
// Rust represents the decisions, never VPP's numeric return values.
enum SessionTxReadiness {
    Ready,
    Defer,
    Discard,
}

// VPP session_types.h:207-225; session.c:172-202.
// One added field in the existing SessionFlags, not a second flags type.
pub struct SessionFlags {
    // ... existing flags
    pub custom_tx: bool,
}

// VPP session_types.h:240-247,293-315. Existing Session fields only:
// IP family is encoded by plugin-session in the opaque session_type;
// service stores the protocol separately instead of decoding IP bits.
pub struct Session {
    // ... existing fields
    session_type: u8,
    transport_protocol: u8,
}

impl SessionWorker {
    // VPP session_types.h:293-315; session_node.c:1870-1890.
    // plugin-session supplies both identities when allocating an IP Session.
    pub fn allocate(
        &mut self, state: SessionState, session_type: u8,
        transport_protocol: u8, opaque: u32,
    ) -> SessionHandle;
}

impl Session {
    // VPP session_types.h:311-315. No service-side IP bit decoding.
    #[inline(always)]
    pub const fn transport_protocol(&self) -> u8;
}

// VPP session.h:82-170. Existing fixed per-worker record in SessionMain;
// only its TX fields are shown. No second worker state is introduced.
#[repr(C)]
pub struct SessionWorker {
    cacheline0: CacheLineAlignMark,
    // ... sessions, event queue, time, timerfd and event lists
    tx_context: SessionTxContext,
    pending_tx_buffers: Vec<u32>,
    pending_tx_nexts: Vec<u16>,
    dma_enabled: bool,
    dma_transfers: Vec<SessionDmaTransfer>,
    dma_head: u16,
    dma_tail: u16,
    // ... other existing worker state
}
```

`SessionTxContext` 的第一组是每次事件覆盖的计算值，第二组是复用的 buffer scratch；两个
cacheline mark 对齐 `session_tx_context_t` 的分组。`session` 在 TX event 开始时从
event 设为 `Some(handle)`，结束时清空；它对应 VPP 的 `ctx->s`，使下列 helper
无需各传一份 Session/FIFO/offset。具体 `tc` 仍由单态化 transport 入口借用，
不把 plugin-owned connection 引用、不安全裸指针或假 `'static` 生命周期放进
service。`SessionDmaTransfer` 的两个
向量只在 DMA 真正启用时按 worker 初始化；普通路径无需分配 1024 份空向量。

```rust
// VPP session.c:2019-2090,2119-2194; session.h:22-43,82-170.
impl Default for SessionTxContext {
    // session begins None; counters are zero; scratch Vecs begin empty.
    fn default() -> Self;
}

impl SessionWorker {
    // Existing worker constructor: builds the TX context and paired pending
    // Vecs before workers start; DMA transfer slots only when DMA is active.
    fn new(worker_index: u32, config: SessionConfig) -> Self;
}

impl SessionMain {
    // Existing global lifecycle; no second TX Main or per-event initializer.
    pub fn init(config: SessionConfig, worker_mq_segment: SvmFifoSegment)
        -> Result<(), SessionError>;

}
```

`SessionMain.workers` 仍存现有 `SessionWorker`，不新增另一个 worker 记录。
VPP `wrk->vm` 是 worker 生命周期内的指针；Hammer 的 graph node 同时独占借用
`&mut DataPlaneMain`，故这里保留显式 runtime 参数，不把节点临时借用存入全局
worker，也不为传参再引入类型。一个 node dispatch 内只取得一次 Session worker，
将这两个独立 owner 的借用传入 TX/transport 方法。来源：VPP
`session.h:82-170,341-354`、`session_node.c:2033-2044`；
Hammer `session/core.rs:248-308,2496-2512`、`session/node.rs:176-207`。

```rust
// VPP session.c:1819-1847; session_node.c:1858-1910.
// Existing SessionIoDispatch is narrowed to RX; TX uses one monomorphized
// entry per session_type. Output next is a registered graph edge, not MAX.
// The return is VPP's TX status; packets is the sole packet count output.
pub type SessionTxDispatch = fn(
    worker: &mut SessionWorker,
    runtime: &mut DataPlaneMain,
    node: &mut NodeRuntime,
    event_index: u32,
    packets: &mut usize,
) -> SessionTxOutcome;

impl SessionMain {
    // VPP session.c:1883-1921. Allocate one protocol id during tcp_init.
    pub fn register_transport_protocol(&self) -> Result<u8, SessionError>;

    // VPP session.c:1827-1847. Called by each TCP output graph init after
    // SessionQueueNode::compile_output_next returns the existing next slot.
    // Startup publication, or WorkerBarrier if graph changes while running.
    pub fn register_transport(
        &self, session_type: u8, output_next: SessionQueueNext,
        tx: SessionTxDispatch,
    ) -> Result<(), SessionQueueError>;
}

impl IpSessionMain {
    // VPP tcp.c:1743-1749; session.c:1827-1847. Map protocol + family to
    // the opaque Session type, then publish the already compiled output next.
    pub fn register_transport(
        &self, protocol: u8, family: IpSessionFamily,
        output_next: SessionQueueNext, tx: SessionTxDispatch,
    ) -> Result<(), SessionError>;
}
```

上述 `IpSessionMain` 是 IP 实例的编排签名，不让 service 接触 `IpSessionFamily`。它不重新
注册 Graph Node：`init_tcp` 先通过 service 分配**一个** transport protocol id、
构造 TCP Main 并注册 RX/control/time 入口。现有 TCP4/TCP6 output graph init
已经调用 `SessionQueueNode::compile_output_next`；各自拿到编译好的
`SessionQueueNext` 后直接调用 `IpSessionMain::register_transport` 发布该 family 的
Session type/TX 入口，无新的 bind init 或 main-loop-enter 回调。
现有 `tcp_worker_init` 仍在 graph materialization 后把同一对
`SessionQueueNext` 写入 `TcpWorker::tco_next_node`，给 ACK、重传、timer control
packet 使用；普通 FIFO TX 使用 service 按 Session type 注册的同一个 next。
依据：Hammer `tcp/src/output.rs:43-74`、`tcp/src/lib.rs:988-1026`；
VPP `tcp.c:1743-1749`、`session.c:1827-1847`。service 在 worker 启动前
（或运行期 barrier 内）发布两项 TX/next 映射；
已注册 protocol 的 RX/control/time
入口仍只占一个槽。TCP worker 的 `tco_next_node` 缓存这两个已注册 next，供
ACK、重传和 timer control packet 使用，普通 FIFO TX 只从 Session type 表取 next。
`SessionWorker::allocate`、listening/half-open 构造以及 plugin-session 的
accept/connect 路径必须一同写入两种身份；不能只改 `transport_protocol()` getter
而让现有 `session_type == protocol` 的值继续流进 TX 表。
`init_tcp` 分配 protocol 前验证容量；两个 output graph init 各自验证已编译 next，
worker init 验证两项已齐。后续重复映射是注册者 bug，
不是需要在热路径回滚的错误。`SessionTxDispatch` 利用已获批的单态化入口，替换当前
`SessionIoDispatch` 的 TX 分支，不新增每包模式分派或函数指针表套函数指针表。

## Transport 与 TCP 契约

现有 `Transport` 是 service 定义、具体插件实现的 trait，保持这个边界。下面只列
Session IO 涉及的目标签名；listen/connect/close 不重写。显式 `runtime`
对应 VPP `wrk->vm`，`connection_index` 解析插件持有的 connection；
`SessionWorker` 是 service 正在持有的**同一个** owner borrow。
VPP 的 `tcp_update_burst_snd_vars` 经 connection 回查 RX/TX FIFO，
`tcp_push_one_header` 也验证 TX FIFO 可读量；Rust 不能在此时从全局 Main
再次借用该 worker，因此需要 FIFO 和 Buffer 的方法接收当前 worker 与 runtime，
不从全局 Main 再取 worker。
不另拆 Session FIFO、PSH 参数或
AppWorker。来源：
`transport.h:75-90,183-224,307-369`、`session_node.c:1472-1677,1884-1893`、
`tcp_output.c:300-329,965-978`、`tcp.h:365-367`。

```rust
// VPP transport.h:87; session_node.c:1507-1516,1696-1724.
// C passes either transport_connection_t* or session_t* through void*.
// This enum is the single Rust custom-TX target, selected by the registered
// Session TX entry; it is not a TX mode or a per-packet allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportTxTarget {
    Connection(u32),
    Session(SessionHandle),
}

pub trait Transport<E> {
    type Connection;

    // VPP transport.h:81-85; tcp.c:1171-1196. Fill the caller's current
    // TX parameters after any flush/custom TX work.
    fn send_params(
        &self, runtime: &DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        params: &mut TransportSendParams,
    );

    // VPP transport.h:86; session_node.c:1496-1504; tcp.c:1372-1380.
    // Optional transport hook; absent VPP callback means successful no-op.
    fn flush_data(&self, _: &SessionWorker, _: u32) {}

    // VPP transport.h:88,183-191; session_node.c:1884-1893;
    // tcp.c:1382-1401. A transport without RX behavior implements no-op.
    fn app_rx_event(&self, _: &mut DataPlaneMain, _: &mut SessionWorker, _: u32) {}

    // VPP transport.h:81-88; tcp_output.c:980-1035,2154-2186.
    fn push_header(
        &self, runtime: &mut DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        buffers: &[u32], available_bytes: u32,
    );
    fn custom_tx(
        &self, runtime: &mut DataPlaneMain, worker: &mut SessionWorker,
        target: TransportTxTarget,
        params: &mut TransportSendParams,
    ) -> usize;

    // VPP transport.h:210-224,307-369. Current Transport::connection gives
    // only &Connection, so service cannot mutate these owner-worker facts.
    fn is_descheduled(&self, runtime: &DataPlaneMain, connection_index: u32) -> bool;
    fn deschedule(&self, runtime: &DataPlaneMain, connection_index: u32);
    fn clear_descheduled(&self, runtime: &DataPlaneMain, connection_index: u32);
    fn is_tx_paced(&self, runtime: &DataPlaneMain, connection_index: u32) -> bool;
    fn tx_pacer_burst(&self, runtime: &DataPlaneMain, connection_index: u32) -> u32;
    fn tx_pacer_update_bytes(&self, runtime: &DataPlaneMain, connection_index: u32, bytes: u32);
    fn tx_pacer_reset_bucket(&self, runtime: &DataPlaneMain, connection_index: u32, bucket: u32);
}
```

`custom_tx` 仍只有一个 trait 方法。VPP 用一个 `void *` 槽：普通 FIFO TX
传 `transport_connection_t *`，internal TX 传 `session_t *`。Rust 不把不同
对象重新擦成裸指针；注册为 peek 的 TCP 只接受 `Connection`，注册为 internal 的
transport 只接受 `Session`，错误 variant 是注册者内部不变量，不能当作可恢复
Session error。`TransportTxTarget` 只在 custom TX 调用点构造，不进入 Session
record 或 TX context。`TransportSendParams` 复用现有 service 类型，由 `send_params`
填写调用方本轮持有的 `&mut TransportSendParams`，不保留跨事件快照；没有新 callback
包装结构、VFT、数字 retval 或 `TxMode`。`send_params`、`push_header` 和
`custom_tx` 也必须接通：当前 TCP `custom_tx` 恒返回零，`push_header` 忽略
`available_bytes`，并不符合 VPP 的 retransmit/ACK、cwnd-limited 和 timer 路径。
TCP 的 `send_params` 先执行 `tcp_update_burst_snd_vars`（包括待发送 PSH 边界、
TCP options、有效 MSS 与本地接收窗口），再把拥塞窗口与对端公告窗口共同允许的发送空间、
`snd_nxt - snd_una` 的 peek offset、MSS 和零发送空间时的 deschedule 标志写入
本轮 `params`，保留调用方预先设置的 burst 上限；来源：
`tcp.c:1128-1196`、`tcp_output.c:300-329`。recovery/Closed 时普通
`snd_space=0`，新数据若可在 recovery 中发送，也由 TCP custom TX 按拥塞/PRR
预算从同一 Session TX FIFO 的 `snd_nxt - snd_una` offset 直接填最终 Buffer，
并与重传共用本轮预算；不能让普通 Session FIFO TX 再发送一遍。
custom TX 只在借用 FIFO 可读片段并把 payload 写入最终 Buffer 的短作用域内持有
Session 借用；该借用结束后才调用 `SessionWorker::add_pending_tx_buffer`，不将
Session FIFO 借用与 `&mut SessionWorker` 的 pending 列修改重叠。
当前 `TcpMain::send_params` 复用的 `TcpConnection::tx_payload_budget`
（`tcp/src/lib.rs:503-541`、`connection.rs:1428-1486`）同时执行了
`pacing_ready`/`next_send_delay`、Nagle 和 recovery 门控；这是旧 packetized
发送决策，不能搬入新 `send_params`。目标发送空间先按 VPP 的
`tcp_snd_space_inline` 与对端窗口计算，随后由 service Session TX **一次**
应用 `TransportConnection.pacer`；TCP recovery 的 pacing 只在其 custom TX
分支扣账。普通发送不得再经 `pacing_ready`、`tx_intent_sequence` 或第二套
逐包预算筛选。现有 Nagle 配置可留作旧接口迁移信息，但不能悄悄改变此条
VPP 对齐路径；若保留 Nagle 行为，须另行决定其独立语义，不能伪称来自
`tcp_session_send_params`。来源：`tcp.c:1100-1196`、
`session_node.c:1553-1570`、`tcp_output.c:1818-1826,1989-1997`。
TCP `custom_tx` 先处理 recovery 中待重传和允许的新数据，再按剩余预算处理待发
ACK/dupACK；预算耗尽时重新编排 ACK。返回值是 VPP 用于 frame 预算的 packet
**计数**，不是可靠的已分配 Buffer 数：`tcp_send_acks` 在 ACK buffer 分配失败时
仍可能计入尝试次数，真正的 output 只由 pending buffer 列决定。
来源：`tcp.c:1128-1164`、`tcp_output.c:1804-2065,2070-2186`。
`push_header` 遍历已经填好的**整个** buffer
批次，仅在首 buffer headroom 内写 TCP header/options，依据各 buffer 链的实际
payload 长度递增 `snd_nxt`；它用 `available_bytes` 计算 cwnd-limited 事实，
随后按 VPP 启动 RTT 采样及重传 timer，不复制 payload 或逐包调用 Session。
VPP 的 `tcp_session_push_header` 返回常量零，Session 调用方不读取该值；Rust
trait 因而返回 `()`，不把零伪装成错误或 packet 计数。
VPP 的 `vlib_buffer_t **` 批次数组仅被遍历，Hammer 用 `&[u32]` 传首 Buffer
index；TCP 经 `runtime` 修改实际 Buffer，不能借口 VPP 指针可写而要求
`&mut [u32]`，否则会与同时借入的 `&SessionWorker` 冲突。
来源：`session_node.c:1652-1655`、`tcp_output.c:884-1035`。

上述 deschedule/pacer 操作只修改具体插件 owner worker 中已有的
`TransportConnection.flags` 和通用 `Pacer`：`deschedule` 置位，
`clear_descheduled` 清位且在 pacing 启用时把 bucket 重置为零；burst 查询、
发送后扣字节、无可发 FIFO 数据时重置 bucket，依次对应
`transport.h:210-224,307-369` 与 `session_node.c:1530-1588`。当前
`Transport::connection` 只给不可变引用，不能用它假装完成这些写入；直接的
trait 方法由 transport owner 在本 worker 执行，不引入新的 flags snapshot 或锁。
现有 `TransportConnection::clear_descheduled(now)` 的 `now` 参数在目标中移除：
VPP 清标志只重置 pacer bucket，pacer 时间由本轮 transport time subscriber 更新，
不能在 ACK 收尾凭一个局部时间戳额外推进 pacer。

```rust
// VPP tcp_types.h:115-125,460-465,490-499;
// tcp.c:1372-1380; tcp_input.c:494-545.
// Existing TcpConnectionCacheline0 in connection.rs: only these fields are
// added beside its current hot fields; its cacheline mark stays in place.
#[repr(C)]
pub struct TcpConnectionCacheline0 {
    cacheline0: CacheLineAlignMark,
    // ... existing sequence, window, timer, and flag fields unchanged
    pub(crate) psh_pending: bool,
    pub(crate) psh_sequence: TcpSeq,
    pub(crate) send_mss: u32,
    pub(crate) limited_transmit: TcpSeq,
    pub(crate) cwnd_limited_sequence: TcpSeq,
    pub(crate) deq_pending: bool,
    pub(crate) burst_acked: u32,
    pub(crate) retransmit_pending: bool,
    pub(crate) send_ack_pending: bool,
    pub(crate) pending_dupacks: u32,
    pub(crate) disconnect_pending: bool,
    pub(crate) reset_state: TcpState,
    pub(crate) delivered_time: f64,
}
```

这是**已有** `TcpConnectionCacheline0` 的字段增量，不另建 cacheline 类型、状态类型或锁。
连接构造时 `psh_pending=false`、`psh_sequence=TcpSeq::from(0)`、
`send_mss` 在首次有效 TCP options/MSS 计算后设定、
`limited_transmit=snd_nxt`、`cwnd_limited_sequence=snd_una`、
`deq_pending=false`、`burst_acked=0`、`retransmit_pending=false`、
`send_ack_pending=false`、`pending_dupacks=0`、`disconnect_pending=false`、
`reset_state=TcpState::Closed`、
`delivered_time=0.0`；只有 owner worker
写入，`CacheLineAlignMark` 保持原位置。第一次 TX_FLUSH
设置 pending，并以 `snd_una + tx_fifo.max_dequeue() - 1` 的 TCP 序号回绕运算保存
最后待 push 字节；已有 pending 时直接返回，不能每次 flush 重置边界。取 FIFO
可读量时通过当前 `&SessionWorker` 的 Session backlink 借用，不从全局 Main 二次借用。
`tcp_update_burst_snd_vars` 在 pending 时按当前可读量更新边界；
`push_header` 只在 `psh_sequence` 落入该段的 `[snd_nxt, snd_nxt + data_len)` 时置
PSH；它用 `available_bytes` 计算 `snd_nxt - snd_una + available_bytes` 并更新
cwnd-limited 判断，同时按 VPP 更新发送序号、RTT 采样和重传 timer。
不能继续让 `flush_data` 为空、或让所有数据段默认置 PSH。
来源：`tcp.c:1372-1380`、`tcp_output.c:316-329,917-1035`、
`tcp_types.h:115-125,490-499`。

TCP 不新增发送状态容器。`TcpMain` 仍持有固定 worker 槽；`TcpWorker` 仍持有
connection pool、cached options、临时 buffer index、ACK/dequeue/cleanup 队列、
timer wheel 和 `tco_next_node`。下列代码只展示已有记录的 TX 相关字段及需补齐的
方法，省略其余原有字段；`cacheline0/1/2` 位置不移动，`runtime` 不存进 TCP
worker。来源：VPP `tcp.h:76-122`、`tcp_output.c:300-329,1037-1058,1123-1270,
1672-1708,1804-2186`；Hammer `tcp/src/worker.rs:14-90`、
`tcp/src/lib.rs:139-178`。

```rust
// VPP tcp.h:76-122; tcp.c:1361-1401; tcp_output.c:1037-1058.
#[repr(C)]
pub struct TcpWorker {
    cacheline0: CacheLineAlignMark,
    connections: Pool<TcpConnection>,
    pending_deq_acked: Vec<u32>,
    pending_disconnects: Vec<u32>,
    pending_resets: Vec<u32>,
    time_us: f64,
    time_tstamp: u32,
    max_timers_per_loop: u32,
    pending_timers: FifoQueue<TcpTimerToken>,
    cacheline1: CacheLineAlignMark,
    pub(super) cached_opts: [u8; 40],
    tx_buffers: Vec<u32>,
    pending_cleanups: FifoQueue<TcpCleanupRequest>,
    tco_next_node: [SessionQueueNext; 2],
    timer_wheel: TimerWheel1t2w2048sl<u32>,
    // ... existing timer bookkeeping, lookup and protocol
    cacheline2: CacheLineAlignMark,
}

impl TcpConnection {
    // VPP tcp.c:1100-1164. Recovery/Closed returns zero for *ordinary*
    // Session FIFO TX. Before recovery, dupACK/SACK permits VPP's bounded
    // Limited Transmit; the result is rounded by current effective send_mss.
    #[inline]
    pub(crate) fn send_space(&self) -> u32;

    // VPP tcp_output.c:300-329. Recompute SACK/timestamp options,
    // effective MSS and receive window; write bytes to the existing
    // TcpWorker.cached_opts. If PSH is pending, refresh its FIFO boundary.
    pub(crate) fn update_burst_send_vars(
        &mut self, tx_fifo: &SvmFifo, cached_opts: &mut [u8; 40],
    );

    // VPP tcp.c:1372-1380. Repeated TX_FLUSH does not move the boundary.
    pub(crate) fn flush_data(&mut self, tx_fifo: &SvmFifo) {
        if self.psh_pending {
            return;
        }
        self.psh_pending = true;
        self.psh_sequence = self.snd_una.advance((tx_fifo.max_dequeue() as u32).wrapping_sub(1));
    }

    // VPP tcp_output.c:884-978. tx_segment's ordinary data branch chooses
    // PSH only when this sequence interval covers psh_sequence. The existing
    // TcpSegment writes directly into the first Buffer's reserved headroom.
    // commit_payload_tx is called exactly once for this new-data segment.
    #[inline(always)]
    pub(crate) fn push_one_header(&mut self, buffer: &mut Buffer, now: Instant) {
        let payload_len = buffer.current_len() + buffer.total_len_not_including_first();
        let capabilities = self.output_capabilities();
        let segment = self.tx_segment(payload_len, capabilities)
            .expect("Session queue supplies a sendable TCP connection");
        segment.write_to_buffer(buffer)
            .expect("Session queue reserved transport header headroom");
        self.commit_payload_tx(payload_len, now)
            .expect("new-data sequence/recovery state commits once");
    }

    // VPP tcp_cc.h:107-135; tcp_output.c:980-1035. This connection-local
    // marker is not a Session statistic or a second congestion controller.
    #[inline(always)]
    fn update_cwnd_limited(&mut self, max_dequeue: u32) {
        if self.cwnd_limited_sequence < self.snd_una {
            self.cwnd_limited_sequence = self.snd_una;
        }
        let cwnd = self.congestion.congestion_window();
        if cwnd > self.snd_wnd {
            return;
        }
        let outstanding = self.snd_nxt.raw().wrapping_sub(self.snd_una.raw());
        if max_dequeue >= cwnd || outstanding >= cwnd
            || (self.congestion.in_slow_start() && outstanding > cwnd / 2)
            || (cwnd.saturating_sub(outstanding) < self.send_mss
                && max_dequeue > outstanding)
        {
            self.cwnd_limited_sequence = self.snd_nxt;
        }
    }

    // VPP tcp_timer.h:108-125. At burst end, reset RTO when flight drains;
    // arm persist for a zero peer window, otherwise update the exact RTO
    // timer (with RACK adjustment when selected). Existing timer wheel
    // interval failures remain TcpNodeError::TimerUpdateFailed.
    #[inline(always)]
    fn retransmit_timer_update(
        &mut self, index: u32, wheel: &mut TimerWheel1t2w2048sl<u32>,
    ) -> RuntimeResult<()>;

    // VPP tcp.c:1434-1445. If pacing is enabled, refresh the existing
    // TransportConnection.pacer from BBR's current rate and measured RTT.
    fn tx_pacer_update(&mut self);
}

impl TcpWorker {
    // VPP tcp_output.c:1037-1058. Allocate one Buffer from runtime; on
    // allocation failure update the receive window and return. Otherwise
    // build ACK, then append (buffer, tco_next_node[family]) to Session's
    // paired pending columns. No AppWorker event is emitted here.
    pub(super) fn send_ack(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32,
    ) {
        let connection = self.connections.get(connection_index)
            .expect("ACK retains its TCP connection");
        let rx_available = sessions.session_from_handle(connection.base.session)
            .and_then(Session::rx_fifo)
            .expect("ACK retains Session RX FIFO").max_enqueue();
        let mut buffers = [0u32; 1];
        if runtime.buffer_alloc(&mut buffers) == 0 {
            self.connections.get_mut(connection_index)
                .expect("ACK retains its TCP connection")
                .set_rcv_wnd(rx_available);
            return; // TCP worker no_buffer; no Session Queue NoBuffer event.
        }
        let connection = self.connections.get_mut(connection_index)
            .expect("ACK retains its TCP connection");
        connection.set_rcv_wnd(rx_available);
        let local = connection.local().expect("established TCP has a local endpoint");
        let remote = connection.remote();
        let capabilities = connection.output_capabilities();
        let segment = connection.control_segment(
            local, remote, TcpSegmentFlags::ACK, None, capabilities,
        );
        segment.write_to_buffer(runtime.buffer_mut(buffers[0]))
            .expect("fresh control Buffer has TCP header space");
        let egress = hammer_core::buffer_opaque!(
            mut runtime.buffer_mut(buffers[0]) => TcpSecondaryOpaque
        ).egress_mut();
        egress.connection_index = connection_index;
        egress.worker_index = sessions.worker_index();
        egress.fib_index = connection.base.endpoint.fib_index();
        let next = self.tco_next_node[usize::from(!connection.remote().is_ipv4())];
        sessions.add_pending_tx_buffer(runtime, buffers[0], next);
    }

    // VPP tcp_output.c:1123-1270,1672-1708. Peek the *same Session TX FIFO*
    // at snd_nxt - snd_una; copy directly into final allocated Buffer(s),
    // add TCP header, enqueue to the existing tco_next_node. Return the
    // number of successfully packetized segments; no private payload Vec.
    fn transmit_unsent(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, burst_size: usize, available_bytes: u32,
    ) -> usize;

    // VPP tcp_output.c:1804-2065,2125-2152. Select SACK scoreboard,
    // byte-tracker/RACK or no-SACK recovery already owned by TcpConnection;
    // retransmit unacked FIFO ranges, then use remaining PRR/cwnd budget for
    // transmit_unsent. Reprogram retransmit when the burst cannot proceed.
    fn retransmit(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, burst_size: usize,
    ) -> usize;

    // VPP tcp_output.c:2070-2122. Preserve pending dupACK/SACK count and
    // max-burst behavior. Return VPP's attempted ACK count even if send_ack
    // cannot allocate a Buffer; only allocated ACKs enter Session pending.
    fn send_acks(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, burst_size: usize,
    ) -> usize;

    // VPP tcp_output.c:2154-2186. One custom-TX entry, not a second
    // session packetizer. Recovery may also send unsent FIFO bytes.
    pub(super) fn custom_tx(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, params: &TransportSendParams,
    ) -> usize {
        let retransmit = {
            let connection = self.connections.get_mut(connection_index)
                .expect("custom TX connection remains live");
            let pending = connection.recovery.in_recovery()
                && connection.retransmit_pending;
            if pending {
                connection.retransmit_pending = false;
            }
            pending
        };
        let packets = if retransmit {
            self.retransmit(runtime, sessions, connection_index,
                params.max_burst_size as usize)
        } else {
            0
        };
        let send_ack = {
            let connection = self.connections.get_mut(connection_index)
                .expect("custom TX connection remains live");
            let pending = connection.send_ack_pending;
            connection.send_ack_pending = false;
            pending && (packets == 0 || connection.pending_dupacks != 0)
        };
        if !send_ack {
            return packets;
        }
        if packets >= params.max_burst_size as usize {
            self.program_ack(runtime, sessions, connection_index, false);
            return packets;
        }
        packets + self.send_acks(runtime, sessions, connection_index,
            params.max_burst_size as usize - packets)
    }
}
```

`TcpConnection::send_space` 不是旧 `tx_payload_budget` 的别名：它不调用
`pacing_ready`、`next_send_delay` 或 Nagle；Session 统一应用通用 pacer。
它必须先在 recovery/Closed 返回零；否则取拥塞可用空间。收到 dupACK 或已有
SACK 字节但尚未进入 recovery 时，用 VPP `tcp_snd_space_inline` 的
`n_pkts = (SACK ? reorder - 1 : 2)` 限额和 `limited_transmit` 序号约束覆盖该空间，
再按 `send_mss` 舍入；对端窗口在 `send_params` 以
`snd_wnd - (snd_nxt - snd_una)` 独立截断。当前 recovery 没有暴露 dupACK/SACK
字节与 reorder 事实，迁移时由 TCP recovery owner 提供这些**连接私有事实**，
不在 service 添加 TCP flag 或基于 `tx_payload_budget` 猜值。正常非 TSO 发送将
`send_mss` 写进参数；若将来启用 TCP TSO，才按 VPP `tcp_session_cal_goal_size`
把 GSO goal size 写进参数，不把普通 MSS 默认为 GSO size。来源：VPP
`tcp.c:1100-1196`、`tcp_cc.h:107-135`。
`update_burst_send_vars` 和 `push_one_header` 操作 TCP 已有字段及 cached options，
不是在 service 增加 TCP 状态。`transmit_unsent`/`retransmit` 的 Buffer 只保留 index
到 `TcpWorker.tx_buffers` 或 Session pending，payload 均从 Session FIFO 读取。

当前 `TcpConnection::tx_segment` 在 established 路径无条件用
`ACK | PSH` 构造 `TcpSegment`（`connection.rs:1512-1515`）；这会抵消上述
PSH 边界设计。目标保留**已有** `TcpSegment` 作为 TCP output intent，由
connection 的现有构造路径根据 `psh_pending/psh_sequence` 为本段决定 PSH，
不在 Session 或 `TcpSegment::write_to_buffer` 里补改 flag。已有
`TcpSegment::write_to_buffer` 直接把 header 写入首 Buffer 的 headroom，
Session 已放入的 payload/chain 保持原位；`commit_payload_tx` 每段只执行一次，
不能由 Session 和 TCP 各推进一次 `snd_nxt` 或重复记录 recovery/timer。
来源：`tcp_output.c:884-1035`；Hammer `connection.rs:1487-1588`、
`segment.rs:104-129`。

```rust
// VPP tcp_output.c:890-930. Replace the existing unconditional ACK | PSH
// selection inside TcpConnection::tx_segment's established-data branch.
let start = self.tx_payload_sequence();
let end = start.advance(payload_len as u32);
let mut flags = TcpSegmentFlags::ACK;
if self.psh_pending && self.psh_sequence >= start && self.psh_sequence < end {
    flags.insert(TcpSegmentFlags::PSH);
}
let flags = self.output_flags(flags);
// The existing TcpSegment::new still constructs the data output intent.
// Its write_to_buffer writes directly into the final Buffer headroom.
```

VPP 的 `cached_opts[40]` 是 worker 的 burst option scratch；Hammer 仍保留该字段和
有效 MSS 的计算，但不能为了使用它先把完整 header 复制到临时 Buffer 再 copy-back。
现有 `TcpSegment`/`TcpSegmentHeader` 已能直接向最终 Buffer 写 options；普通数据
路径先复用这一能力。若后续要求缓存 options 字节，可在**同一**最终 Buffer 的
options 区写缓存，不新增 header owner 或第二次 payload copy。来源：
`tcp_output.c:300-329,890-930`；Hammer `tcp/src/segment.rs:82-129`。

TCP 的 `pending_deq_acked: Vec<u32>` **已经**在 `TcpWorker`，目标直接使用它。
在已有 `TcpConnectionCacheline0` 保存本 burst 的 `burst_acked: u32` 与
`deq_pending: bool`；首次 ACK 进 pending 向量，后续 ACK 累加字节，不重复排队。
TCP 输入先结束对具体 connection 的可变借用，再调用
`TcpWorker::program_dequeue(connection_index, bytes_acked)`；该方法自己同时修改
connection 的计数与 worker 向量，不在外部持有 connection 引用时重借 worker。
TCP 输入 node 在其一批 packet 处理结束、已有 Session RX enqueue event flush
之后且 disconnect 清理之前遍历该向量：清 pending 标志，
对有 ACK 字节的 connection 调用一次
`SessionWorker::tx_fifo_dequeue_drop(&connection.base, burst_acked)`，然后核对
FIFO 中仍保留的 `snd_nxt - snd_una`、必要时重新调度被 deschedule 的 Session，
更新重传 timer 与通用 pacer，最后清零 `burst_acked` 和向量。VPP 对应的
`delivered_time` 更新属于 TCP 已有的 delivery/CC owner，不塞入 Session。
当前 `established.rs`、`rcv_process.rs`、`syn_sent.rs` 的直接
`tx.drop_dequeue` 与逐 ACK `enqueue_ready` 均须收敛到这条 burst-end 路径；
TCP input 持有的 worker borrow 与 `&mut SessionWorker` 是两个 owner，调用 service
方法时不再经全局 Main 借第二份 Session worker。
来源：`tcp_input.c:494-545,1407-1413,2365-2371`、`session.c:738-749`。

```rust
impl TcpWorker {
    // VPP tcp_input.c:536-545. Coalesce ACKed bytes per connection for
    // this input burst; queue each connection index at most once.
    fn program_dequeue(&mut self, connection_index: u32, bytes_acked: u32);

    // VPP tcp_input.c:494-533,1407-1412,2365-2370. Called once after
    // the input burst, before leaving the TCP input node.
    fn handle_postponed_dequeues(&mut self, sessions: &mut SessionWorker);
}
```

`app_rx_event` 的 TCP 实现只处理零接收窗口恢复：若未发送过零窗口，立即返回，
不先修改 TCP 接收窗口。否则取 Session RX FIFO 的 size 和 producer 可入队空间，
空间不足阈值时对 **RX FIFO**
调用现有 dequeue-notification 请求，空间足够时按 `tcp_send_ack` 的路径申请
buffer、更新接收窗口、构造独立 ACK 并通过 service 的
`SessionWorker::add_pending_tx_buffer` 加入 TCP4/TCP6 output 的 pending 列。
`tcp_make_ack` 根据实际公告窗口更新 zero-window 标志；若 buffer 申请失败，
仍更新接收窗口后返回。VPP 在此处记 TCP worker 的 `no_buffer` 统计，
本 ADR 不引入先前排除的 stats，也不能错记为 Session Queue 的 `NoBuffer` node
counter。此处不返回 `SessionError::NotSupported` 或 recoverable `Result`。该入口不调用
`AppWorker::add_event`、不克隆 `TcpConnection` 试探 ACK，也不把 ACK 当普通
FIFO payload。RX FIFO size/空间读取在短借用中完成；需要 enqueue ACK 时先
结束该借用，再可变访问 `SessionWorker` 的 pending 列。
来源：`tcp.c:1382-1401`、`tcp_output.c:1037-1058`、
`session.h:917-975`。

```rust
// VPP tcp.c:1372-1401,1402-1431; tcp_output.c:917-930,1037-1058.
// TCP overrides the two optional service Transport hooks; this is the
// concrete plugin implementation, not another dispatch callback or node.
impl Transport<IpTransportEndpointConfig> for TcpMain {
    // VPP tcp.c:1171-1196; tcp_output.c:300-329. Update burst vars before
    // filling the caller's send-space, offset, MSS and flag fields.
    fn send_params(
        &self, runtime: &DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        params: &mut TransportSendParams,
    ) {
        let mut tcp = self.worker(runtime.thread_index())
            .expect("send params runs on the TCP owner worker");
        let TcpWorker { connections, cached_opts, .. } = &mut *tcp;
        let connection = connections.get_mut(connection_index)
            .expect("Session TX retains a live TCP connection");
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::tx_fifo)
            .expect("TCP data Session retains its TX FIFO");
        connection.update_burst_send_vars(fifo, cached_opts);
        let outstanding = connection.snd_nxt().wrapping_sub(connection.snd_una());
        params.send_mss = connection.send_mss as u16;
        params.send_space = connection.send_space()
            .min(connection.snd_wnd().saturating_sub(outstanding));
        params.tx_offset = outstanding;
        params.flags.deschedule = params.send_space == 0;
    }

    // Read the current Session TX FIFO through worker, then set the existing
    // connection's PSH pending flag and wrapping sequence boundary once.
    fn flush_data(&self, worker: &SessionWorker, connection_index: u32) {
        let mut tcp = self.worker(worker.worker_index())
            .expect("flush runs on the TCP owner worker");
        let connection = tcp.connection_mut(connection_index)
            .expect("TX_FLUSH retains a live TCP connection");
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::tx_fifo)
            .expect("TCP data Session retains its TX FIFO");
        connection.flush_data(fifo);
    }

    // Check zero-window history; request RX FIFO dequeue notification below
    // the VPP threshold, otherwise build an ACK directly in one allocated
    // Buffer and add it to SessionWorker's pending TCP4/TCP6 output.
    fn app_rx_event(
        &self, runtime: &mut DataPlaneMain,
        worker: &mut SessionWorker, connection_index: u32,
    ) {
        let mut tcp = self.worker(runtime.thread_index())
            .expect("RX event runs on the TCP owner worker");
        let connection = tcp.connection(connection_index)
            .expect("RX event retains a live TCP connection");
        if !connection.zero_receive_window_sent() {
            return;
        }
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::rx_fifo)
            .expect("TCP data Session retains its RX FIFO");
        let min_free = (fifo.size() / 8).clamp(4 << 10, 128 << 10);
        if fifo.max_enqueue() < min_free {
            fifo.want_deq_notification();
            return;
        }
        tcp.send_ack(runtime, worker, connection_index);
    }

    // VPP tcp_output.c:980-1035. Headerize all first buffers in this burst;
    // use available_bytes for cwnd-limited accounting and update timers.
    fn push_header(
        &self, runtime: &mut DataPlaneMain, worker: &SessionWorker,
        connection_index: u32,
        buffers: &[u32], available_bytes: u32,
    ) {
        let mut tcp = self.worker(runtime.thread_index())
            .expect("Session TX runs on the TCP owner worker");
        let now = tcp.last_timer_update;
        let TcpWorker { connections, timer_wheel, .. } = &mut *tcp;
        let connection = connections.get_mut(connection_index)
            .expect("Session TX retains its TCP connection");
        let outstanding = connection.snd_nxt.raw().wrapping_sub(connection.snd_una.raw());
        let max_dequeue = outstanding.checked_add(available_bytes)
            .expect("TCP outstanding and available FIFO bytes fit u32");
        for &index in buffers {
            let buffer = runtime.buffer_mut(index);
            connection.push_one_header(buffer, now);
            let egress = hammer_core::buffer_opaque!(mut buffer => TcpSecondaryOpaque)
                .egress_mut();
            egress.connection_index = connection_index;
            egress.worker_index = worker.worker_index();
            egress.fib_index = connection.base.endpoint.fib_index();
        }
        if buffers.is_empty() {
            return;
        }
        let fifo = worker.session(connection.base.session.session_index)
            .and_then(Session::tx_fifo)
            .expect("TCP data Session retains TX FIFO");
        assert!(fifo.max_dequeue() >= connection.snd_nxt.raw()
            .wrapping_sub(connection.snd_una.raw()) as usize,
            "TCP sent bytes remain in Session TX FIFO until ACK");
        connection.update_cwnd_limited(max_dequeue);
        connection.sync_payload_tx_timers(connection_index, timer_wheel, now)
            .expect("validated TCP timer interval remains representable");
    }

    // VPP tcp_output.c:1804-2065,2154-2186. Accept Connection only;
    // recovery may send unsent FIFO bytes too, followed by pending ACKs.
    // Return the frame-budget count, not the pending-buffer length.
    fn custom_tx(
        &self, runtime: &mut DataPlaneMain, worker: &mut SessionWorker,
        target: TransportTxTarget,
        params: &mut TransportSendParams,
    ) -> usize {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP registers only the peek TX entry");
        };
        self.worker(runtime.thread_index())
            .expect("custom TX runs on the TCP owner worker")
            .custom_tx(runtime, worker, connection_index, params)
    }
}
```

service 的 IO dispatch 在 Session 仍未 `TransportClosed` 的 RX event 上调用已注册
transport 的 `app_rx_event`；TX_FLUSH 只在普通 FIFO TX 入口调用 `flush_data`
一次，再继续同一 event 的 send params/custom TX/packetization。未提供 RX 行为的
transport 实现 no-op，不能返回 `NotSupported`。具体 TCP/UDP worker、PSH/ACK、
output next 都不移到 service。现有 `tcp_session_io` 内自行实现的 RX/TxFlush
分支在迁移后删除：RX 由 service 的同一 IO dispatch 调用注册的 TCP trait
入口，TxFlush 由共用 FIFO TX 算法在取 send params 前调用 trait。调用期间
只保存 connection index；`tx_get_transport` 取得的 `&Connection` 在任何
`flush_data`、`send_params`、`app_rx_event` 或 `push_header` 可变 TCP 操作前结束
借用，不能把该引用跨操作留在 `SessionTxContext`。TCP callback 获取一次
`TcpMain`，但不跨上述调用保留 `&Connection`。以上 trait 改动是实现前须批准的公开 API；
本 ADR 只设计，不改源码。

`SessionTxContext.send_params` 仍是本 worker 的可复用 scratch。调用
`custom_tx`（传 `&mut SessionWorker`）或 `send_params`（传 `&SessionWorker`）前，
用 `std::mem::take` 将这一个小值暂移到栈上，填入本轮 burst 后传
`&mut TransportSendParams`；回调结束立即写回 context，再运行依赖它的
`tx_set_dequeue_params`。这只移动发送参数，不拷贝 FIFO payload，且不会同时
构造 `&self` 与 `&mut self.tx_context.send_params` 的别名。
`push_header` 借 context 中首 Buffer index 的不可变切片及 `&SessionWorker`，
两者均为共享借用；TCP 独占修改的是另一个 owner 的 Buffer。

### TCP 时间与输出注册

VPP `session_queue_node_fn` 每轮先更新 Session 时间并调用 transport 的 time
subscriber；TCP 的顺序是处理到期 cleanup、推进 timer wheel、按
`max_timers_per_loop` 消费原样 timer token。Hammer 保留现有
`Transport::update_time(now, worker_index)` 签名：它只推进 TCP owner worker 的
clock/wheel 并把到期 token 放进已有 `pending_timers`。已有注册的
`tcp_session_update_time` 先调用 trait 更新 clock/收集到期 token，再以同一轮
`TcpWorker.time_origin` 转换出的同一单调 `Instant` 执行到期 cleanup，最后借当前
`runtime`/`SessionWorker` 派发 token；cleanup
取消的旧 token 必须由派发处按现有 pending/armed 位跳过。这样实际 cleanup 副作用
仍在 timer handler 之前，同时保留不可修改的 trait 签名；不得在 trait 与
subscriber 各推进一次 wheel，也不得遍历所有 timer 种类猜测到期项。VPP 的 cleanup 请求是按
`free_time` 排序的 FIFO，因而仅检查队头。Hammer 继续用现有
`TcpCleanupRequest.free_time: Instant`，与 TCP wheel 已有的单调时间基准一致，
不把 VPP 的 `f64` 表示机械搬进 Rust，也不增加第二个 cleanup 队列。
`session_queue_run_on_main_thread` 只针对 VPP 的 thread 0 数据面；Hammer 主线程不
跑数据面，不照搬该分支。来源：VPP `session_node.c:2033-2050`、
`tcp.c:1293-1369`；Hammer `tcp/src/lib.rs:543-562,856-911`、`tcp/src/worker.rs:36-40`。

```rust
// VPP tcp.c:1293-1369; session_node.c:2033-2050.
impl TcpWorker {
    // VPP tcp.c:1337-1359. Pop only due requests from the ordered FIFO;
    // no Session backlink means TCP-only cleanup, otherwise request the
    // service-owned Session deletion before TCP connection cleanup.
    fn handle_cleanups(
        &mut self, runtime: &DataPlaneMain, sessions: &mut SessionWorker, now: Instant,
    );

    // VPP tcp.c:1293-1335. Consume at most max_timers_per_loop exact
    // TcpTimerToken values, skip a reset/rearmed token, and keep the rest
    // in pending_timers for the next Session queue dispatch.
    fn dispatch_pending_timers(
        &mut self, runtime: &mut DataPlaneMain, sessions: &mut SessionWorker,
    ) -> Result<(), SessionError>;
}

// VPP tcp.c:1361-1369; session_node.c:2033-2050. Existing time subscriber.
fn tcp_session_update_time(
    runtime: &mut DataPlaneMain, sessions: &mut SessionWorker, now: f64,
) -> Result<(), SessionError> {
    let tcp = TCP_MAIN.get().expect("registered TCP time subscriber retains TCP Main");
    let worker_index = sessions.worker_index();
    <TcpMain as Transport<IpTransportEndpointConfig>>::update_time(
        tcp, now, worker_index,
    );
    {
        let mut worker = tcp.worker(runtime.thread_index())
            .expect("subscriber executes on the TCP owner worker");
        let origin_seconds = worker.time_origin_seconds
            .expect("update_time establishes the TCP worker time origin");
        let now = worker.time_origin
            .checked_add(Duration::from_secs_f64((now - origin_seconds).max(0.0)))
            .expect("TCP worker monotonic time remains representable");
        worker.handle_cleanups(runtime, sessions, now);
    }
    tcp.worker(runtime.thread_index())
        .expect("subscriber executes on the TCP owner worker")
        .dispatch_pending_timers(runtime, sessions)
}
```

TCP4/TCP6 output 各自编译 `session-queue -> tcp*-output` next，随后让
plugin-session 以**同一个** TCP protocol id 和各自 IP family 注册 Session type/TX
入口。`tco_next_node[0/1]` 是 TCP worker 的同一对 next；独立 ACK、重传、timer
control packet 和普通 FIFO TX 最终都走 service 的 pending buffer/next 列。
`session-queue` 初始 Disabled 且只由 Session enable 生命周期切换，TCP output
初始化不得在编译 next 后再次把它 Disabled。现有按 node name 查找 graph 的
注册阶段可保留一次，`bind_worker_graph` 不应每个 worker 再按名称发现同一个 edge。
来源：VPP `tcp.c:1590-1630,1743-1749`、`session.c:1819-1847,2196-2270`；
Hammer `tcp/src/output.rs:43-74`、`tcp/src/lib.rs:984-1026`、
`service/session/node.rs:91-103,138-153`。

```rust
// VPP tcp.c:1590-1630,1743-1749; session.c:1827-1847.
// These statements belong in each existing tcp4/tcp6 output init after
// registering that output node; no new graph init hook or bind helper.
let queue = runtime.nodes().node_by_name("session-queue")
    .expect("Session queue registers before TCP output graph init");
let next = SessionQueueNode::compile_output_next(runtime, queue, node)?;
IpSessionMain::global()?.register_transport(
    TCP_MAIN.get().expect("TCP Main initialized").protocol(),
    IpSessionFamily::Ip4, // Ip6 in tcp6 output init
    next,
    tcp_session_tx,
)?;
// Existing tcp_worker_init stores these two compiled next slots in
// tco_next_node; neither output init calls set_node_state(queue, Disabled).
```

## TCP 接收与 FIFO OOO segment

这部分更正 TCP 接收路径，不把 TX FIFO 的 peek/recovery 规则套到 RX。
TCP connection 持有 `rcv_nxt`、接收窗口和 SACK **公告状态**；Session 持有 RX FIFO，
`hammer-infra::svm::Fifo` 的 `OooSegment` pool/list 才持有未来字节的重组区间。
`ooo_enq_lookup`/`ooo_deq_lookup` 在 VPP 是 FIFO **chunk** 定位树，不能拿来索引
OOO segment；当前 Hammer `replace_ooo_segments` 对它们的插入/清空也要删除。
TCP 不再额外保存一份乱序 payload、`Vec` 重组队列或用 SACK block 代替 FIFO OOO segment。
SACK list 与 FIFO OOO list 可以同时存在：前者是发送给对端的 TCP 选项状态，
后者决定哪些字节何时对应用连续可读。来源：VPP
`tcp_input.c:1000-1101`、`svm_fifo.c:171-340,833-930`。

TCP 建立/接受的 Session 已关联 RX/TX FIFO 后、处理首个 data packet 前，按 VPP
`transport_fifos_init_ooo` 的时机确保 RX OOO enqueue chunk lookup 和 TX OOO dequeue
chunk lookup 可用。Hammer 的 `Fifo` 构造时已有 `ooo_segments` 和 chunk lookup，不复制一个 TCP
私有实例，也不在每包重建 lookup；现有 `enable_ooo` 只设置未被 OOO 入队读取的
`ooo_base`，**不等价于** VPP 的 lookup 初始化，不能把调用它当作完成接线。
目前 `write_at_without_tail_store` 仍从 tail chunk 线性扫描，现有 RbTree 还未
随 chunk 附着/回收维护。迁移时在 infra 的现有 chunk 操作中维护
`start_byte -> chunk offset`，OOO 写入按 producer-owned `ooo_enq_lookup` 定位；
不能把 segment 索引塞进去充当初始化。若构造期已完成这些映射，就无需另造
enable 调用。来源：`transport.c:1205-1210`、`svm_fifo.c:593-726,892-930`、
`tcp_input.c:1858,1883,2622`；Hammer `svm/fifo.rs:594-599,714-719,1257-1309,1597-1605`。

目标继续使用现有 `Fifo::enqueue_ooo`、`OooResult`、
`SessionWorker::enqueue_rx`、`RxDelivery`、`TcpConnection::accept_payload` 和
`TcpConnection::receive_payload`，不新增重组 owner。现有结果字段必须改为
FIFO 的真实结果：`OooResult.start/len` 表示本次插入/合并后 **newest OOO segment**
相对当前 producer tail 的起点和长度，而不是本次 Buffer 写入区间；完全被已有
OOO segment 覆盖、没有改变区间时 `start=None`。因此 `RxDelivery::OutOfOrder`
必须允许 `newest=None`，不能将没有新 SACK 区间的成功写入伪造成错误。跨 Buffer
链使用每段的 `offset + 已处理长度`；以**最后一次 FIFO OOO 插入**留下的 newest
标记为结果，不用 `min(start)..max(end)` 合成一个可能不存在的区间，也不保留
较早 chunk 的 stale newest。来源：
`svm_fifo.c:171-299,892-930`、`svm_fifo.h:630-662`、
`tcp_input.c:1051-1101`、`session.h:685-754,778-829`。

`ooos_list_head`/`ooos_newest` 当前是普通 `u32`，却在 `&Fifo` 下经 raw cast
改写；目标与 `ooo_segments` 一样用 `UnsafeCell<u32>` 表达同一 producer 独占写入
契约。`UnsafeCell` 不改变字段布局。`first_ooo_segment` 现返回池内 `&OooSegment`，
会把可被下一次 producer 插入/合并释放的引用暴露出去；目标仅返回 `Copy` 的
区间事实，或者由同一个 producer 操作的 `OooResult` 直接给 TCP，不跨调用保留
池元素借用。FIFO tail 仍只在按序补洞成功时 release 发布；OOO 数据和区间在
tail 前不能被 consumer 观察。来源：`svm_fifo.c:171-340,833-930`、
`svm_fifo.h:630-662`；Hammer `svm/fifo.rs:594-600,1569-1572,1604-1755`。

```rust
// VPP svm_fifo.c:171-299,892-930; svm_fifo.h:635-662.
// Existing infra result: only start/len semantics change; no new carrier.
pub struct OooResult {
    pub accepted: u32,
    pub delivered: u32,
    pub start: Option<u32>, // newest merged OOO segment, relative to producer tail
    pub len: u32,            // zero when start is None
}

impl Fifo {
    // OOO insert is all-or-nothing for this slice. It copies directly into
    // FIFO future storage, merges adjacent/overlapping OooSegment records,
    // leaves tail unchanged, and returns the merged segment just affected.
    pub fn enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, FifoError>;
}

// VPP session.h:778-829; tcp_input.c:1000-1101.
// Existing service outcome; change the existing variant, not add a second one.
pub enum RxDelivery {
    NotAccepted { rx_available: u32 },
    InOrder { accepted: NonZeroU32, promoted: u32, rx_available: u32 },
    OutOfOrder {
        accepted: NonZeroU32,
        newest: Option<(u32, NonZeroU32)>,
        rx_available: u32,
    },
}

impl SessionWorker {
    // In-order: enqueue at tail, include FIFO-promoted OOO bytes in
    // delivered count and queue one RX notification for this Session, even
    // when this enqueue writes zero bytes, as VPP queue_event does.
    // OOO: enqueue at offset, return last FIFO newest, queue no RX event.
    #[inline(always)]
    pub fn enqueue_rx(
        &mut self, runtime: &DataPlaneMain, handle: SessionHandle,
        buffer_index: u32, offset: u32,
    ) -> RxDelivery {
        let session = self.session_from_handle(handle)
            .expect("TCP input retains its owner-worker Session");
        let fifo = session.rx_fifo().expect("data Session retains RX FIFO");
        let mut accepted = 0u32;
        let mut promoted = 0u32;
        let mut processed = 0u32;
        let mut skip_promoted = 0usize;
        let mut newest = None;
        for buffer in runtime.chain(buffer_index) {
            let bytes = buffer.current();
            if offset == 0 {
                let skip = skip_promoted.min(bytes.len());
                skip_promoted -= skip;
                let bytes = &bytes[skip..];
                if !bytes.is_empty() {
                    let result = fifo.enqueue_ooo(0, bytes)
                        .expect("offset-zero FIFO enqueue cannot fail");
                    accepted = accepted.checked_add(result.accepted)
                        .expect("RX packet chain fits the FIFO length");
                    promoted = promoted.checked_add(result.delivered)
                        .expect("promoted OOO bytes fit the FIFO length");
                    skip_promoted += result.delivered as usize;
                    if result.accepted as usize != bytes.len() {
                        break; // FIFO full: keep any already accepted prefix.
                    }
                }
            } else if !bytes.is_empty() {
                let chunk_offset = offset.checked_add(processed)
                    .expect("validated TCP receive window bounds OOO offset");
                match fifo.enqueue_ooo(chunk_offset, bytes) {
                    Ok(result) => {
                        accepted = accepted.checked_add(result.accepted)
                            .expect("RX packet chain fits the FIFO length");
                        newest = result.start.and_then(|start| {
                            NonZeroU32::new(result.len).map(|length| (start, length))
                        }); // last insertion's newest, including None
                    }
                    Err(FifoError::OutOfOrderCapacityExceeded { .. }
                        | FifoError::SegmentExhausted) => break,
                    Err(FifoError::OutOfOrderLengthOutOfRange { .. }) => {
                        panic!("TCP packet chain length fits the FIFO offset")
                    }
                }
            }
            processed += u32::try_from(bytes.len())
                .expect("packet chain length fits FIFO offset");
        }
        let rx_available = u32::try_from(fifo.max_enqueue()).unwrap_or(u32::MAX);
        if offset == 0 {
            let session = self.session_mut(handle.session_index)
                .expect("RX Session remains live while TCP enqueues");
            if !session.flags.rx_event {
                session.flags.rx_event = true;
                self.sessions_to_enqueue.push(handle);
            }
        }
        let Some(accepted) = NonZeroU32::new(accepted) else {
            return RxDelivery::NotAccepted { rx_available };
        };
        if offset == 0 {
            RxDelivery::InOrder { accepted, promoted, rx_available }
        } else {
            RxDelivery::OutOfOrder { accepted, newest, rx_available }
        }
    }
}
```

下面是 `enqueue_ooo` 的核心替换代码。它只更新当前 FIFO 的 segment pool/list，
删除现有 `collect_ooo_segments`/`replace_ooo_segments` 及临时区间 `Vec`；
chunk lookup 保持独立，不随 segment 合并清空。`add_ooo_segment` 是 VPP
`ooo_segment_add` 对应的**私有** FIFO 原语，
不是 TCP/Session 的新容器。写 future bytes 失败时还没有发布 OOO 元数据；
成功时合并并返回真实 newest，`tail` 保持不变。

```rust
// VPP svm_fifo.c:171-340,892-930; svm/fifo_types.h:105-106.
// Existing Fifo fields change from u32 to UnsafeCell<u32>; repr stays u32.
// ooos_list_head: UnsafeCell<u32>, ooos_newest: UnsafeCell<u32>.
impl Fifo {
    fn add_ooo_segment(&self, tail: u32, offset: u32, length: u32)
        -> Option<(u32, u32)>
    {
        let start = tail.wrapping_add(offset);
        let mut end = start.wrapping_add(length);
        // SAFETY: only the FIFO producer mutates OOO metadata; the consumer
        // observes data only after a release store to tail.
        let segments = unsafe { &mut *self.ooo_segments.get() };
        let head = unsafe { &mut *self.ooos_list_head.get() };
        let newest = unsafe { &mut *self.ooos_newest.get() };

        let mut previous = OOO_SEGMENT_INVALID_INDEX;
        let mut current = *head;
        while current != OOO_SEGMENT_INVALID_INDEX
            && f_pos_lt(segments.get(current).expect("OOO link remains live").start, start)
        {
            previous = current;
            current = segments.get(current).expect("OOO link remains live").next;
        }
        // VPP ooo_segment_add: use the predecessor if the new bytes touch it;
        // otherwise use the first following segment if the bytes touch it.
        let target = if previous != OOO_SEGMENT_INVALID_INDEX
            && f_pos_leq(start, segments.get(previous).unwrap().start
                .wrapping_add(segments.get(previous).unwrap().length))
        {
            Some(previous)
        } else if current != OOO_SEGMENT_INVALID_INDEX
            && f_pos_leq(segments.get(current).unwrap().start, end)
        {
            Some(current)
        } else {
            None
        };
        let Some(index) = target else {
            let index = segments.insert(OooSegment {
                start, length, prev: previous, next: current,
            });
            if previous == OOO_SEGMENT_INVALID_INDEX {
                *head = index;
            } else {
                segments.get_mut(previous).unwrap().next = index;
            }
            if current != OOO_SEGMENT_INVALID_INDEX {
                segments.get_mut(current).unwrap().prev = index;
            }
            *newest = index;
            return Some((offset, length));
        };

        let old_start = segments.get(index).unwrap().start;
        let old_end = old_start.wrapping_add(segments.get(index).unwrap().length);
        if f_pos_lt(start, old_start) {
            segments.get_mut(index).unwrap().start = start;
        }
        if f_pos_lt(end, old_end) {
            end = old_end;
        }
        let mut changed = f_pos_lt(start, old_start) || f_pos_gt(end, old_end);
        loop {
            let next = segments.get(index).unwrap().next;
            if next == OOO_SEGMENT_INVALID_INDEX
                || !f_pos_leq(segments.get(next).unwrap().start, end)
            {
                break;
            }
            let segment = segments.remove(next).expect("OOO link remains live");
            let segment_end = segment.start.wrapping_add(segment.length);
            if f_pos_gt(segment_end, end) {
                end = segment_end;
            }
            segments.get_mut(index).unwrap().next = segment.next;
            if segment.next != OOO_SEGMENT_INVALID_INDEX {
                segments.get_mut(segment.next).unwrap().prev = index;
            }
            changed = true;
        }
        let merged_start = segments.get(index).unwrap().start;
        segments.get_mut(index).unwrap().length = end.wrapping_sub(merged_start);
        if !changed {
            return None; // VPP leaves ooos_newest invalid for a covered write.
        }
        *newest = index;
        Some((merged_start.wrapping_sub(tail), end.wrapping_sub(merged_start)))
    }

    pub fn enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, FifoError> {
        unsafe { *self.ooos_newest.get() = OOO_SEGMENT_INVALID_INDEX };
        if src.is_empty() {
            return Ok(OooResult { accepted: 0, delivered: 0, start: None, len: 0 });
        }
        if offset == 0 {
            let hdr = self.hdr;
            let head = unsafe { (*hdr).head.load(Ordering::Acquire) };
            let tail = unsafe { (*hdr).tail.load(Ordering::Relaxed) };
            if head == tail {
                self.prepare_empty_tail_chunk(tail);
            }
            let free = unsafe { (*hdr).size.saturating_sub(tail.wrapping_sub(head)) };
            let to_write = src.len().min(free as usize);
            let accepted = self.append_at_tail_without_tail_store(tail, &src[..to_write]) as u32;
            let delivered = if accepted == 0 {
                0
            } else {
                let next_tail = tail.wrapping_add(accepted);
                let promoted = self.promote_contiguous_from(next_tail);
                unsafe { (*hdr).tail.store(next_tail.wrapping_add(promoted), Ordering::Release) };
                promoted
            };
            return Ok(OooResult {
                accepted, delivered, start: None, len: 0,
            });
        }
        let length = u32::try_from(src.len()).map_err(|_| FifoError::OutOfOrderLengthOutOfRange {
            length: src.len(),
        })?;
        let available = self.max_enqueue();
        if offset as usize > available || src.len() > available - offset as usize {
            return Err(FifoError::OutOfOrderCapacityExceeded {
                end_offset: offset.wrapping_add(length), available,
            });
        }
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let start = tail.wrapping_add(offset);
        if self.write_at_without_tail_store(start, src) != src.len() {
            return Err(FifoError::SegmentExhausted);
        }
        let newest = self.add_ooo_segment(tail, offset, length);
        Ok(OooResult {
            accepted: length,
            delivered: 0,
            start: newest.map(|(start, _)| start),
            len: newest.map_or(0, |(_, len)| len),
        })
    }
}
```

`Fifo::enqueue` 现有实现先 release 发布中间 tail，再以临时 `Vec` 重建区间并第二次
发布 tail；目标让 offset-zero 的现有 `enqueue_ooo` 完成按序拷入与原位 OOO
收集，只 release 发布一次最终 tail。普通 `enqueue` 直接取其总交付量，service
接收路径则读取同一个 `OooResult` 中**准确的** `accepted/delivered`，不能用总量
`min(src.len())` 猜本次拷入量。`newest` 在每次按序 enqueue 的空间检查之前清空；
已交付的
OOO segment 从 pool/list 一起删除，chunk lookup 不变。下列代码替换现有
`promote_contiguous_from`、`collect_ooo_segments`、`replace_ooo_segments` 的热路径，
不增加另一种 FIFO 或分配中间 payload。来源：VPP `svm_fifo.c:308-340,833-877`。

```rust
// VPP svm_fifo.c:308-340,833-877. Producer alone mutates the OOO pool.
impl Fifo {
    fn promote_contiguous_from(&self, base: u32) -> u32 {
        let segments = unsafe { &mut *self.ooo_segments.get() };
        let head = unsafe { &mut *self.ooos_list_head.get() };
        let mut tail = base;
        while *head != OOO_SEGMENT_INVALID_INDEX {
            let index = *head;
            let segment = *segments.get(index).expect("OOO list index remains live");
            if f_pos_gt(segment.start, tail) {
                break;
            }
            *head = segment.next;
            if segment.next != OOO_SEGMENT_INVALID_INDEX {
                segments.get_mut(segment.next)
                    .expect("OOO next index remains live").prev = OOO_SEGMENT_INVALID_INDEX;
            }
            segments.remove(index).expect("OOO head remains live");
            let end = segment.start.wrapping_add(segment.length);
            if f_pos_gt(end, tail) {
                tail = end;
            }
        }
        tail.wrapping_sub(base)
    }
}
```

现有 `Fifo::enqueue` 复用 offset-zero 入口，不再保留第二份拷入/补洞循环：

```rust
// VPP svm_fifo.c:833-877. Existing public method, same return contract.
impl Fifo {
    pub fn enqueue(&self, src: &[u8]) -> usize {
        let result = self.enqueue_ooo(0, src)
            .expect("offset-zero FIFO enqueue cannot fail");
        result.accepted as usize + result.delivered as usize
    }
}
```

TCP input 顺序与 VPP `tcp46_established_inline` 一致：验证窗口/PAWS 和 ACK 之后，
才处理 payload，之后再判断 FIN；RX 的 `runtime`/`SessionWorker` 借用只在节点已有
owner 间传递，不重新从 global Main 借第二个 worker。`TcpConnection::accept_payload`
使用 TCP 序号判断：`seq_end <= rcv_nxt` 是旧包，DUPACK 而不 enqueue；
`seq < rcv_nxt < seq_end` 先从 **原 Buffer chain** 裁去旧前缀，剩余字节按序写；
`seq == rcv_nxt` 直接按序写；`seq > rcv_nxt` 则以 `seq - rcv_nxt` 为 offset
写入 Session RX FIFO OOO segment，并发送 DUPACK。裁剪不能把整段 payload 复制
到临时 `Vec`，Session FIFO 仍是唯一的 app/session 字节拷贝边界。来源：
`tcp_input.c:1147-1210,1361-1405`、`session.h:778-829`。

```rust
// VPP tcp_input.c:1000-1101,1149-1211; session.h:778-829.
impl TcpConnection {
    // Existing decision: return old-prefix trim and relative FIFO offset;
    // a fully old segment stays outside the Session enqueue path.
    #[inline]
    fn accept_payload(&self, packet: &TcpPacket) -> Option<(usize, u32)> {
        if packet.payload_len == 0 {
            return None;
        }
        let end = packet.sequence.advance(packet.payload_len as u32);
        if end <= self.rcv_nxt {
            return None;
        }
        if packet.sequence < self.rcv_nxt {
            return Some((packet.sequence.distance_to(self.rcv_nxt) as usize, 0));
        }
        Some((0, self.rcv_nxt.distance_to(packet.sequence)))
    }

    // Existing update: in-order accepted + FIFO-promoted bytes advance
    // rcv_nxt. OOO leaves rcv_nxt unchanged and updates SACK from FIFO's
    // newest merged interval; no new statistics field is required.
    #[inline]
    fn receive_payload(
        &mut self, sequence: TcpSeq, trim: u32, delivery: RxDelivery,
    ) {
        let sack_enabled = self.negotiated_options().sack;
        if trim != 0 {
            self.sack.set_duplicate(sack_enabled, sequence, self.rcv_nxt);
        }
        match delivery {
            RxDelivery::NotAccepted { .. } => {}
            RxDelivery::InOrder { accepted, promoted, .. } => {
                self.rcv_nxt = self.rcv_nxt.advance(accepted.get().wrapping_add(promoted));
                self.sack.update_range(sack_enabled,
                    self.rcv_nxt, self.rcv_nxt, self.rcv_nxt);
            }
            RxDelivery::OutOfOrder { newest: Some((start, length)), .. } => {
                let left = self.rcv_nxt.advance(start);
                let right = left.advance(length.get());
                self.sack.update_range(sack_enabled,
                    self.rcv_nxt, left, right);
            }
            RxDelivery::OutOfOrder { newest: None, .. } => {}
        }
    }
}
```

三个 data-bearing TCP input 分支使用同一段目标流程；下面保留原 Buffer 链，
只在每个 Buffer 上移动 `current_data/current_length`，不创建 payload `Vec` 或
第二份 OOO 存储。`packet.payload_len` 是解析出的总 data 长度，不能让尾随
非 payload 字节进入 FIFO；input frame 仍拥有整条链并在末尾释放。来源：VPP
`tcp_input.c:1000-1101,1149-1211,1394-1413,1897-1907,2260-2271`、
`session.h:685-829`。

```rust
// VPP tcp_input.c:1149-1211. Same branch in established/rcv_process/syn_sent.
assert_ne!(packet.payload_len, 0, "payload branch follows ACK processing");
let decision = tcp.connections.get(connection_index)
    .expect("input lookup retained TCP connection")
    .accept_payload(&packet);
let error = if let Some((trim, offset)) = decision {
    let mut prefix_left = packet.payload_offset + trim;
    let payload_len = packet.payload_len - trim;
    let mut payload_left = payload_len;
    let mut next = Some(buffer_index);
    while let Some(index) = next {
        let buffer = runtime.buffer_mut(index);
        let skip = prefix_left.min(buffer.current_len());
        buffer.advance(skip as isize);
        prefix_left -= skip;
        let keep = payload_left.min(buffer.current_len());
        buffer.truncate(keep).expect("TCP parser bounds payload within the chain");
        payload_left -= keep;
        next = buffer.next_buffer_slot();
    }
    assert_eq!(prefix_left, 0, "parsed TCP header remains inside the Buffer chain");
    assert_eq!(payload_left, 0, "parsed TCP payload remains inside the Buffer chain");

    let delivery = sessions.enqueue_rx(runtime, handle, buffer_index, offset);
    let received = match delivery {
        RxDelivery::NotAccepted { .. } => 0,
        RxDelivery::InOrder { accepted, .. }
        | RxDelivery::OutOfOrder { accepted, .. } => accepted.get() as usize,
    };
    tcp.connections.get_mut(connection_index)
        .expect("input retains TCP connection")
        .receive_payload(packet.sequence, trim as u32, delivery);
    tcp.program_ack(runtime, sessions, connection_index, offset != 0);
    if received == 0 {
        let connection = tcp.connections.get(connection_index)
            .expect("input retains TCP connection");
        if offset == 0 && connection.rcv_wnd < connection.send_mss {
            TcpNodeError::ZeroReceiveWindow
        } else {
            TcpNodeError::FifoFull
        }
    } else if received != payload_len {
        if offset == 0 {
            TcpNodeError::PartiallyEnqueued
        } else {
            TcpNodeError::FifoFull
        }
    } else if offset == 0 {
        TcpNodeError::Enqueued
    } else {
        TcpNodeError::EnqueuedOoo
    }
} else {
    tcp.program_ack(runtime, sessions, connection_index, true);
    TcpNodeError::SegmentOld
};
runtime.record_current_node_error(error)?;
// FIN follows payload processing; only seq_end == current rcv_nxt is accepted.
```

FIN 不能由旧 `receive_close_side` 在入队前用 `packet.sequence == rcv_nxt`
直接消费。`packet.sequence` 指向 payload 起点；同一个 packet 的 data 先写入
Session RX FIFO 并按**实际连续交付量**推进 `rcv_nxt`，随后才比较原 packet 的
`seq_end`（VPP `tcp_inlines.h:370-371` 定义为 `seq_number + data_len`，不含 FIN）。
已有 `process_fin_after_payload` 与 `receive_close_side` 应收敛到这个
顺序；不新建一份 FIN 处理状态。`FIN_WAIT_1` 还必须保留 VPP 的 `FINPNDG`
分支，`FIN_WAIT_2` 有未读 RX 字节时必须先 flush enqueue event，之后才向
Session 报告 transport closed。来源：`tcp_input.c:977-997,1361-1412,2260-2370`。

```rust
// VPP tcp_input.c:977-997,1361-1412,2260-2370.
// In established/receive-process, run this only after ACK and payload RX.
let fin_ready = packet.flags.contains(TcpSegmentFlags::FIN) && {
    let connection = tcp.connections.get(connection_index)
        .expect("validated TCP input retains the connection");
    packet.sequence.advance(packet.payload_len as u32) == connection.rcv_nxt
};
if fin_ready {
    let state = tcp.connections.get(connection_index)
        .expect("validated TCP input retains the connection").state();
    match state {
        TcpState::Established => {
            {
                let connection = tcp.connections.get_mut(connection_index)
                    .expect("FIN retains TCP connection");
                connection.rcv_nxt = connection.rcv_nxt.advance(1);
                connection.set_state(TcpState::CloseWait);
                // Existing WAITCLOSE timer is updated to closewait_time here.
            }
            tcp.program_ack(runtime, sessions, connection_index, false);
            tcp.program_disconnect(connection_index);
        }
        TcpState::FinWait1 => {
            let fin_pending = {
                let connection = tcp.connections.get_mut(connection_index)
                    .expect("FIN retains TCP connection");
                connection.rcv_nxt = connection.rcv_nxt.advance(1);
                let pending = connection.fin_pending();
                if pending {
                    connection.mark_fin_received();
                    // Remain FIN_WAIT_1; WAITCLOSE uses closewait_time.
                } else {
                    connection.set_state(TcpState::Closing);
                    // WAITCLOSE uses closing_time.
                }
                pending
            };
            if !fin_pending {
                tcp.program_ack(runtime, sessions, connection_index, false);
            }
        }
        TcpState::FinWait2 => {
            {
                let connection = tcp.connections.get_mut(connection_index)
                    .expect("FIN retains TCP connection");
                connection.rcv_nxt = connection.rcv_nxt.advance(1);
                connection.set_state(TcpState::TimeWait);
                // Reset existing timers; set WAITCLOSE to timewait_time here.
            }
            tcp.program_ack(runtime, sessions, connection_index, false);
            if sessions.session_from_handle(handle)
                .and_then(Session::rx_fifo)
                .expect("FIN retains Session RX FIFO").max_dequeue() != 0
            {
                sessions.flush_enqueue_events(runtime, tcp.protocol);
            }
            sessions.transport_closed(runtime, handle, connection_index)
                .expect("FIN_WAIT_2 transport retains its Session");
        }
        _ => { /* existing state-specific FIN handling remains TCP-owned */ }
    }
}
```

上面的 FIN 片段说明**执行位置和状态结果**；`fin_pending`、
`mark_fin_received` 表示现有 FIN pending 标志的读写，不批准为此新增公开
TCP helper。连接状态和 timer 在短作用域内更新；借用结束后才调用 worker
的 ACK、disconnect 或 service 通知。`SynRcvd`、
`CloseWait`、`Closing`、`LastAck`、`TimeWait` 的分支保持 VPP
`tcp_input.c:2275-2360` 的各自语义，不把所有 FIN 压成一个 close helper。

按序 enqueue 返回的 `accepted + promoted` 才是 `rcv_nxt` 增量。VPP
`tcp_input.c:1013-1029,1075-1076` 的 `bytes_in` 是独立统计：按序计本包实际写入量，
成功的 OOO 入队计本包 data_len，即使与已有 OOO segment 重叠；本 ADR 不为此
新增 Hammer stats。补洞使 FIFO tail 连续跨过 OOO segment 时，
应用只得到一次合并后的 RX 通知，并清理已送达的 SACK block。乱序 enqueue
不推进 `rcv_nxt`、不通知应用；SACK 启用时将 FIFO newest segment 的
`[rcv_nxt + start, rcv_nxt + start + len)` 加到 TCP SACK list，随后消费该
newest 标记；`newest=None` 表示不重复插入 SACK。VPP `tcp_rcv_fin` 拒绝
out-of-order FIN，因此不能将 FIN 作为 FIFO OOO payload 缓存。来源：
`tcp_input.c:977-1048,1051-1101,1203-1210`、`session.c:690-715`。

`established.rs`、`rcv_process.rs` 和 `syn_sent.rs` 的 data-bearing 分支共用上述
序号判定/FIFO 结果语义；目前后两者固定 offset 为零且绕过 `receive_payload`，
迁移时必须删除这种特例。`receive_open_reply` 对 SYN-ACK data 不能预先按
packet 长度推进 `rcv_nxt`，应先建立基于 SYN 消耗的接收序号，再按 FIFO 的
实际入队/补洞结果推进。现有 `TcpSackState` 保留为 TCP 选项状态，但不得据
packet 原始范围推断 FIFO 已合并的 OOO 范围；现有 `RxDelivery` 字段替换而不并存
两套协议。TCP input node 的 batch 尾部调用已有 `flush_enqueue_events`，
与 `session_main_flush_enqueue_events` 对齐；不能在每个 OOO packet 后通知 app。
来源：`tcp_input.c:1394-1412,1897-1907`、`session.c:690-715`。

这些 RX 分类落在 TCP input node 的 enum counter，而非 `SessionError`：
`Enqueued`/`EnqueuedOoo` 为已写入的观察计数，`PartiallyEnqueued` 为按序部分写入，
`FifoFull` 为 FIFO 空间/增长失败，`SegmentOld` 为全旧重传，
`ZeroReceiveWindow` 为接收窗口低于 MSS 且本次未写入。`OutOfOrderCapacityExceeded`
等 FIFO 容量结果在 packet 边界映射 `FifoFull`；TCP node 须比较
`accepted` 与本包 payload 长度，跨链部分入队不能误报完整入队，也不能把每包拥塞作为
`SessionError::RxOutOfOrderEnqueue` 向控制面返回；无效的 Session/worker 关联是
owner 不变量，窗口外 segment 由 TCP validation 自己分类。保留已有 node error
enum，仅为缺失的 VPP 类别补具体变体；实施前按仓库规则取得公开 enum 变更批准。
来源：`tcp_error.def:22-26,48,51`、`tcp_input.c:211-326,1000-1101,1149-1211`。

```rust
// VPP tcp_error.def:22-26,51; tcp_input.c:1000-1101,1149-1211.
// Target additions to existing TcpNodeError; its other variants remain.
pub enum TcpNodeError {
    Enqueued,
    EnqueuedOoo,
    FifoFull,
    PartiallyEnqueued,
    SegmentOld,
    ZeroReceiveWindow,
}
```

同步与 inline：TCP connection、SessionWorker 和 FIFO OOO metadata 都由当前
Data Worker 独占更新，FIFO consumer 只按 FIFO 自身的 release/acquire tail
发布/读取连续字节；OOO 未来字节不提前发布 tail，不另加锁、原子指针或
`volatile`。`session_enqueue_stream_connection` 在 VPP 是 `always_inline`，故
service 的短 `enqueue_rx` 分支可按现有热路径 `#[inline(always)]`；VPP
`tcp_segment_rcv`、`tcp_session_enqueue_data`/`ooo` 是普通 `static` 函数，
节点级 receive 分支不强制 inline。现有 `accept_payload`/`receive_payload`
是短连接局部操作，保留 `#[inline]`，不传播 `always_inline` 到整个 TCP node。
来源：`session.h:778`、`tcp_input.c:999-1003,1052-1055,1149-1152`、
`svm_fifo.c:833-930`。

### ACK、重传与 Session custom TX 接线代码

VPP `session_add_self_custom_tx_evt` 不只是“再排一个 TX”：它先设 Session 的
`CUSTOM_TX` flag，再利用 TX FIFO event bit 去重；已有 event bit 时由已排队
的 TX event 消费 custom 工作。ACK/dupACK 以 new list 优先投递，重传以 old
list 投递；transport 被 deschedule 时即使 event bit 已置仍须入队并清
descheduled。当前 `enqueue_ready` 没有这层语义，不能靠反复调用它代替。
service 不知道 TCP ACK 类型，只接收 priority 和 transport 已 deschedule 的事实。
来源：`session.c:172-200`、`tcp_output.c:1058-1089`、
`session_node.c:1496-1532`。

```rust
// VPP session_types.h:210-225; session.c:172-200.
// Uses the existing SessionFlags::custom_tx addition declared above.
impl SessionWorker {
    // New service operation required by the VPP Session event contract.
    // It does not accept a TCP connection or a callback.
    pub fn add_self_custom_tx_event(
        &mut self, runtime: &DataPlaneMain, handle: SessionHandle,
        priority: bool, descheduled: bool,
    ) {
        let enqueue = {
            let session = self.session_mut(handle.session_index)
                .expect("custom TX retains its Session on the owner worker");
            assert_ne!(session.load_state(), SessionState::TransportDeleted);
            if session.flags.custom_tx {
                return;
            }
            session.flags.custom_tx = true;
            session.tx_fifo().expect("custom TX retains TX FIFO").set_event()
                || descheduled
        };
        if !enqueue {
            return; // the existing TX event will observe custom_tx
        }
        let event = SessionEvent::from((SessionEventType::Tx, handle.session_index));
        if priority {
            self.allocate_new_event(event);
        } else {
            self.allocate_old_event(event);
        }
        if self.state() == SessionWorkerState::Interrupt {
            runtime.set_node_interrupt_pending(
                self.queue_node.expect("Session queue is installed"),
            ).expect("installed Session queue can be interrupted");
        }
    }

    // VPP session.c:204-221; transport.c:1112-1126. No FIFO event-bit
    // test here: the transport has already cleared DESCHED and checked
    // available bytes (or unset/rechecked the bit when there were none).
    pub fn reschedule_tx(&mut self, runtime: &DataPlaneMain, handle: SessionHandle) {
        assert_eq!(handle.worker_index, self.worker_index());
        self.allocate_new_event(SessionEvent::from((
            SessionEventType::Tx, handle.session_index,
        )));
        if self.state() == SessionWorkerState::Interrupt {
            runtime.set_node_interrupt_pending(
                self.queue_node.expect("Session queue is installed"),
            ).expect("installed Session queue can be interrupted");
        }
    }
}

// VPP tcp_output.c:1058-1089; tcp_input.c:1194-1208.
impl TcpWorker {
    fn program_ack(
        &mut self, runtime: &DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32, duplicate: bool,
    ) {
        let connection = self.connections.get_mut(connection_index)
            .expect("input retains the TCP connection");
        if !connection.send_ack_pending {
            sessions.add_self_custom_tx_event(runtime, connection.base.session,
                true, connection.base.is_descheduled());
            connection.send_ack_pending = true;
            connection.base.flags.descheduled = false;
        }
        if duplicate {
            connection.pending_dupacks = connection.pending_dupacks.saturating_add(1).min(255);
        }
    }

    // VPP tcp_output.c:1080-1089: retransmit uses the old Session event list.
    fn program_retransmit(
        &mut self, runtime: &DataPlaneMain, sessions: &mut SessionWorker,
        connection_index: u32,
    ) {
        let connection = self.connections.get_mut(connection_index)
            .expect("timer retains the TCP connection");
        if connection.retransmit_pending {
            return;
        }
        sessions.add_self_custom_tx_event(runtime, connection.base.session,
            false, connection.base.is_descheduled());
        connection.retransmit_pending = true;
        connection.base.flags.descheduled = false;
    }
}
```

`TcpWorker::custom_tx` 的既有 ADR 代码块消费这些 flag；它按 VPP
`tcp_session_custom_tx` 先完成 recovery 重传，再在余下 burst 内发 ACK/dupACK。
`tcp_send_acks` 必须保留“尝试 ACK 数量”与实际 pending Buffer 数的区别。
普通 Session TX 在 recovery/Closed 仍由 `send_params` 报零空间，不能绕开
custom TX 从旧 `tx_payload_budget` 单独发送。`SessionTxContext.send_params.max_burst_size`
由 Session queue 设定，TCP 不重置它。来源：`tcp.c:1128-1196`、
`tcp_output.c:2070-2186`、`session_node.c:1496-1553`。

```rust
// VPP tcp_input.c:494-545,932-975,1407-1413; session.c:738-749.
// In each TCP input branch, release the connection borrow before touching
// TcpWorker.pending_deq_acked; ACK/window/CC classification stays per packet.
let bytes_acked = {
    let TcpWorker { connections, timer_wheel, .. } = &mut *tcp;
    let connection = connections.get_mut(connection_index)
        .expect("input retains its TCP connection");
    // Target return: the existing ACK feedback's ac.bytes_acked; do not
    // derive it afterward from FIFO capacity or a second ACK interpretation.
    connection.receive_ack_with_timers(
        connection_index, timer_wheel, &packet, acknowledgment,
        advertised_window, sack_blocks, now,
    )?
};
if bytes_acked != 0 {
    tcp.program_dequeue(connection_index, bytes_acked);
}

impl TcpWorker {
    fn program_dequeue(&mut self, connection_index: u32, bytes_acked: u32) {
        let connection = self.connections.get_mut(connection_index)
            .expect("ACK retains its TCP connection");
        if !connection.deq_pending {
            self.pending_deq_acked.push(connection_index);
            connection.deq_pending = true;
        }
        connection.burst_acked = connection.burst_acked
            .checked_add(bytes_acked).expect("ACK burst does not exceed TX flight");
    }

    fn handle_postponed_dequeues(
        &mut self, runtime: &DataPlaneMain, sessions: &mut SessionWorker,
    ) -> RuntimeResult<()> {
        let mut timer_error = None;
        for &index in &self.pending_deq_acked {
            let connection = self.connections.get_mut(index)
                .expect("pending ACK retains TCP connection until burst end");
            connection.deq_pending = false;
            let acked = std::mem::take(&mut connection.burst_acked);
            if acked == 0 {
                continue;
            }
            if connection.snd_una == connection.snd_nxt {
                connection.delivered_time = self.time_us;
            }
            let dropped = sessions.tx_fifo_dequeue_drop(runtime, &connection.base, acked);
            assert_eq!(dropped, acked, "acked bytes remain in Session TX FIFO");
            let fifo = sessions.session_from_handle(connection.base.session)
                .and_then(Session::tx_fifo)
                .expect("ACK retains the TCP Session TX FIFO");
            assert!(fifo.max_dequeue() >= connection.snd_nxt.raw()
                .wrapping_sub(connection.snd_una.raw()) as usize);
            if connection.base.is_descheduled()
                && (connection.recovery.in_recovery() || connection.send_space() != 0)
            {
                connection.base.clear_descheduled();
                if fifo.max_dequeue() != 0 {
                    sessions.reschedule_tx(runtime, connection.base.session);
                } else {
                    fifo.unset_event();
                    if fifo.max_dequeue() != 0 && fifo.set_event() {
                        sessions.reschedule_tx(runtime, connection.base.session);
                    }
                }
            }
            if let Err(error) = connection.retransmit_timer_update(index, &mut self.timer_wheel) {
                if timer_error.is_none() {
                    timer_error = Some(error);
                }
            }
            connection.tx_pacer_update();
        }
        self.pending_deq_acked.clear(); // retain Vec capacity for next burst
        match timer_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    // VPP tcp_input.c:932-940. FIN and RST share the pending marker, so
    // the same connection is not inserted twice during one input burst.
    fn program_disconnect(&mut self, index: u32) {
        let connection = self.connections.get_mut(index)
            .expect("FIN retains TCP connection");
        if !connection.disconnect_pending {
            self.pending_disconnects.push(index);
            connection.disconnect_pending = true;
        }
    }

    // VPP tcp_input.c:135-149. SYN_SENT is handled immediately instead;
    // other states retain the pre-close reset state until burst end.
    fn program_reset(&mut self, index: u32) {
        let connection = self.connections.get_mut(index)
            .expect("RST retains TCP connection");
        if !connection.disconnect_pending {
            let state = connection.state();
            connection.reset_state = state;
            self.pending_resets.push(index);
            connection.disconnect_pending = true;
        }
    }

    fn handle_disconnects(
        &mut self, runtime: &DataPlaneMain, sessions: &mut SessionWorker,
    ) {
        for &index in &self.pending_disconnects {
            let connection = self.connections.get_mut(index)
                .expect("pending FIN retains TCP connection");
            connection.disconnect_pending = false;
            let handle = connection.base.session;
            let closed = connection.state() == TcpState::Closed;
            sessions.transport_closing(runtime, handle, index)
                .expect("owner Session accepts closing notification");
            if closed {
                sessions.transport_closed(runtime, handle, index)
                    .expect("closed transport notifies its Session once");
            }
        }
        self.pending_disconnects.clear();
        for &index in &self.pending_resets {
            let connection = self.connections.get_mut(index)
                .expect("pending reset retains TCP connection");
            connection.disconnect_pending = false;
            let handle = connection.base.session;
            // RST reception records the pre-close state before setting CLOSED.
            match connection.reset_state {
                TcpState::Established => {
                    sessions.transport_reset(runtime, handle, index)
                        .expect("established Session accepts reset");
                    sessions.transport_closed(runtime, handle, index)
                        .expect("reset transport also closes");
                }
                TcpState::CloseWait | TcpState::FinWait1 | TcpState::FinWait2
                    | TcpState::Closing | TcpState::LastAck => {
                    sessions.transport_closed(runtime, handle, index)
                        .expect("closing Session accepts transport close");
                }
                TcpState::SynRcvd => {
                    sessions.transport_deleted(runtime, handle, index)
                        .expect("unaccepted Session requests transport deletion");
                }
                TcpState::SynSent => {
                    unreachable!("SYN_SENT RST is handled on receipt, not queued")
                }
                TcpState::Closed | TcpState::TimeWait => {}
                TcpState::Listen => unreachable!("listener cannot enter pending reset"),
            }
        }
        self.pending_resets.clear();
    }
}
```

上述 per-packet ACK 示例将已有 `receive_ack_with_timers` 的返回值从 `()`
改为 `u32`，值就是本次被接受的 ACK 反馈的 `bytes_acked`。这只修改已有
TCP 私有方法，不新增 ACK carrier，也不让 service 重做 ACK/SACK 分类；
VPP `tcp_sack.h:127-147`、`tcp_input.c:875-892` 是来源。
`receive_ack_with_timers` 在目标中只保留
ACK/SACK、RTT、loss/CC 与不依赖最终 FIFO head 的 timer 处理；其现有
retransmit/pacer 更新必须从该方法移走，集中在 `handle_postponed_dequeues`。
借用时将 `connections` 与 `timer_wheel` 解构成不相交字段，不能一边持有
`tcp.connection_mut(...)` 一边再次借整个 `tcp`；不要求新 ACK carrier。
输入 frame 结尾的固定顺序是：

```rust
// VPP tcp_input.c:1407-1413,1926,2365-2370.
sessions.flush_enqueue_events(runtime, tcp.protocol);
let dequeue_error = tcp.handle_postponed_dequeues(runtime, sessions).err();
tcp.handle_disconnects(runtime, sessions);
if let Some(error) = dequeue_error {
    return Err(error);
}
```

`reset_state` 在收到 RST、把 connection 改为 `Closed` **之前**记录；
`SynSent` 不进入 `pending_resets`，在接收处立即给 service 的 connect 失败通知
`SessionError::Refused`，随后走现有 half-open TCP cleanup；`SynRcvd` 则在 burst
尾部请求 service 删除尚未向应用交付的 Session，不能发 established reset/closed。
此处 `transport_deleted` 对应 ADR-0040 已设计、当前尚未实现的 service 生命周期
入口；connect 失败通知亦须先在 service 完成，不能在 TCP 虚构 cleanup helper。
两项是实现前置缺口，不可通过跳过分支或错误复用 `transport_reset` 来宣称完成。
`handle_postponed_dequeues` 在每个 input frame 的 RX enqueue-event flush 之后执行，
`handle_disconnects` 最后执行。不能在单 packet 的 `receive_ack_with_timers`
内部一边 drop FIFO 一边逐 ACK 重排；该方法的 timer/pacer 更新也须延后到
burst-end，避免两条路径重复执行。来源：`tcp_input.c:494-545,106-135,
1407-1413,2365-2370`。

## FIFO TX 算法

```rust
impl SessionWorker {
    // VPP session_node.c:1239-1293. State check distinguishes peek from
    // dequeue; Accepting/TransportClosed with custom TX is not ordinary data.
    #[inline(always)]
    fn tx_not_ready(session: &Session, peek_data: bool) -> SessionTxReadiness;

    // VPP session_node.c:1276-1293. Peek always selects connection;
    // dequeue selects listener only for a Listening Session.
    #[inline(always)]
    fn tx_get_transport<'transport, T, E>(
        &self, transport: &'transport T, peek_data: bool,
    ) -> &'transport T::Connection where T: Transport<E>;

    // VPP session_node.c:1296-1435. Read FIFO consumer availability now;
    // apply tx_offset only for peek, then snd_space/MSS/frame budget.
    #[inline(always)]
    fn tx_set_dequeue_params<M, const DGRAM: bool>(
        &mut self, runtime: &DataPlaneMain, max_segments: u32, peek_data: bool,
    );

    // VPP session_node.c:1046-1071. Ordinary path copies directly from
    // session-owned FIFO into final Data Plane Buffer storage; DMA path borrows
    // readable FIFO segments and records transfers before packet publication.
    // VPP session_node.c:976-1044. DMA uses the same target buffer and FIFO
    // offset; these methods are inactive until runtime has generic DMA.
    #[inline(always)]
    fn tx_fill_dma_transfers(&mut self, runtime: &mut DataPlaneMain, buffer: u32) -> u32;

    #[inline(always)]
    fn tx_fill_dma_transfers_tail(
        &mut self, runtime: &mut DataPlaneMain, buffer: u32, len_to_dequeue: u32,
    ) -> u32;

    // VPP session_node.c:1046-1071. Ordinary copy or optional DMA dispatch.
    #[inline(always)]
    fn tx_copy_data(
        &mut self, runtime: &mut DataPlaneMain, buffer: u32, len_to_dequeue: u32,
    ) -> u32;

    #[inline(always)]
    fn tx_copy_data_tail(
        &mut self, runtime: &mut DataPlaneMain, buffer: u32, len_to_dequeue: u32,
    ) -> u32;

    // VPP session_node.c:1073-1157. Consumes exactly the selected segment's
    // remaining payload and connects chain headers using existing Buffer API.
    #[inline(always)]
    fn tx_fifo_chain_tail<M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, first: u32,
        remaining_buffers: &mut u16, peek_data: bool,
    );

    // VPP session_node.c:1159-1237. Set locally originated/zero error,
    // reserve TRANSPORT_MAX_HDRS_LEN, fill first buffer, then optional tail.
    #[inline(always)]
    fn tx_fill_buffer<M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, first: u32,
        remaining_buffers: &mut u16, peek_data: bool,
    );

    // VPP session_node.c:1437-1453. Unset FIFO event; recheck available
    // bytes against the transport offset; set event + head-old only if new
    // work appeared. Otherwise deschedule the owning connection.
    #[inline(always)]
    fn tx_maybe_reschedule<T, E>(
        &mut self, runtime: &DataPlaneMain, event_index: u32, transport: &T,
    )
    where T: Transport<E>;

    // VPP session_node.c:1456-1468. Ordinary TX and DMA pending lists.
    #[inline(always)]
    fn tx_add_pending_buffer(&mut self, buffer: u32, next: u16);

    // VPP session.h:1101-1115. Separate direct transport control packet API.
    #[inline(always)]
    pub fn add_pending_tx_buffer(&mut self, runtime: &DataPlaneMain, buffer: u32, next: u16);

    // VPP session_node.c:1472-1677. The shared peek/dequeue algorithm.
    // DGRAM is a registration-time specialization, never a stored TX mode.
    #[inline(always)]
    fn tx_fifo_read_and_send_i<T, E, M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime, event_index: u32,
        packets: &mut usize, peek_data: bool, transport: &T,
    ) -> SessionTxOutcome where T: Transport<E>;

    // VPP session_node.c:1680-1694. TCP chooses peek; FIFO bytes stay until ACK.
    fn tx_fifo_peek_and_send<T, E>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime, event_index: u32,
        packets: &mut usize, transport: &T,
    ) -> SessionTxOutcome where T: Transport<E>;

    // VPP session_node.c:1688-1694. Both ordinary and datagram dequeue
    // select this entry; M is used only for concrete datagram metadata.
    fn tx_fifo_dequeue_and_send<T, E, M, const DGRAM: bool>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime, event_index: u32,
        packets: &mut usize, transport: &T,
    ) -> SessionTxOutcome where T: Transport<E>;

    // VPP session.c:738-749. TCP ACK removes retained TX bytes through
    // Session ownership, including FIFO tuning and dequeue notification.
    pub fn tx_fifo_dequeue_drop<E, O>(
        &mut self, runtime: &DataPlaneMain,
        connection: &TransportConnection<E, O>, max_bytes: u32,
    ) -> u32 {
        let session = self.session_from_handle(connection.session)
            .expect("ACK retains its owner-worker Session");
        let fifo = session.tx_fifo().expect("TCP data Session retains TX FIFO");
        let dropped = fifo.drop_dequeue(max_bytes as usize) as u32;
        // Existing Session FIFO tuning runs here, after the actual drop.
        if fifo.needs_deq_notification(max_bytes as usize) {
            if let Some(index) = session.application_worker() {
                let application = ApplicationMain::global()
                    .expect("attached Session retains Application Main");
                let app_worker = unsafe { application.worker(index) }
                    .expect("attached Session retains AppWorker");
                let connectionless = matches!(
                    session.load_state(), SessionState::Listening | SessionState::Opened,
                );
                self.program_io_event(app_worker, session, SessionEventType::Tx, connectionless);
                self.program_app_worker(runtime, index);
            }
            // Existing FIFO subscriber list follows the same TX notification;
            // it is not another ACK dequeue or another application event.
        }
        dropped
    }
}
```

整批分配和双包流水是 `tx_fifo_read_and_send_i` 的同一函数体，不再回到当前的
单 buffer `tx_fifo_peek_and_send`。下面是完成 state/custom/send-params/pacer/
`tx_set_dequeue_params` 后的核心代码；`next` 已由注册的 Session type 查得，
`connection_index` 是 transport backlink，`ctx.left_to_send/max_dequeue` 已由前段
计算。`tx_fill_buffer` 直接从 Session FIFO 写最终 Buffer，并在 peek 路径只推进
本轮 offset。来源：VPP `session_node.c:1553-1677`。

```rust
// VPP session_node.c:1583-1677; inside tx_fifo_read_and_send_i.
let needed = self.tx_context.buffers_needed as usize;
self.tx_context.tx_buffers.resize(needed, 0);
let allocated = runtime.buffer_alloc(&mut self.tx_context.tx_buffers[..needed]);
if allocated != needed {
    runtime.buffer_free(&self.tx_context.tx_buffers[..allocated]);
    self.old_events.push_front(event_index);
    runtime.record_current_node_error(SessionQueueNodeError::NoBuffer)
        .expect("Session queue owns its NoBuffer counter");
    return SessionTxOutcome::NoBuffers;
}
if transport.is_tx_paced(runtime, connection_index) {
    transport.tx_pacer_update_bytes(runtime, connection_index,
        self.tx_context.max_len_to_send);
}
let mut remaining_buffers = allocated as u16;
let mut n_left = self.tx_context.segments_per_event;
self.tx_context.transport_pending_buffers.clear();
while n_left >= 4 {
    runtime.prefetch_write(self.tx_context.tx_buffers[(remaining_buffers - 3) as usize]);
    runtime.prefetch_write(self.tx_context.tx_buffers[(remaining_buffers - 4) as usize]);
    remaining_buffers -= 1;
    let first = self.tx_context.tx_buffers[remaining_buffers as usize];
    remaining_buffers -= 1;
    let second = self.tx_context.tx_buffers[remaining_buffers as usize];
    self.tx_fill_buffer::<M, DGRAM>(runtime, first, &mut remaining_buffers, peek_data);
    self.tx_fill_buffer::<M, DGRAM>(runtime, second, &mut remaining_buffers, peek_data);
    self.tx_context.transport_pending_buffers.extend([first, second]);
    self.tx_add_pending_buffer(first, next.slot());
    self.tx_add_pending_buffer(second, next.slot());
    n_left -= 2; // VPP's >=4 loop processes two packets, not four.
}
while n_left != 0 {
    if n_left > 1 {
        runtime.prefetch_write(self.tx_context.tx_buffers[(remaining_buffers - 2) as usize]);
    }
    remaining_buffers -= 1;
    let first = self.tx_context.tx_buffers[remaining_buffers as usize];
    self.tx_fill_buffer::<M, DGRAM>(runtime, first, &mut remaining_buffers, peek_data);
    self.tx_context.transport_pending_buffers.push(first);
    self.tx_add_pending_buffer(first, next.slot());
    n_left -= 1;
}
transport.push_header(runtime, self, connection_index,
    &self.tx_context.transport_pending_buffers, self.tx_context.max_dequeue);
if remaining_buffers != 0 {
    runtime.buffer_free(&self.tx_context.tx_buffers[..remaining_buffers as usize]);
}
*packets += self.tx_context.segments_per_event as usize;
assert_eq!(self.tx_context.left_to_send, 0);
// Then: old-list if this event was frame-limited, else unset/recheck FIFO
// event; only dequeue TX issues AppWorker dequeue notification.
```

`tx_fifo_read_and_send_i` 的执行顺序必须是：

1. 检查 Session state：peek 的 Ready 可发，Accepting 或 transport 已关闭但尚未
   Deleted 时仅允许 custom TX；未 Ready 的普通 peek defer，已 Deleted 丢弃。
   dequeue 仅在 transport 已 Deleted 或 FIFO 不存在时丢弃；它在 Listening 状态
   选 listener transport，其余状态选 connection。随后从
   `SessionMain.session_type_to_next` 取已注册 edge，计算
   `max_burst = SESSION_NODE_FRAME_SIZE - packets`。`TX_FLUSH` 先调用 `flush_data` 并把
   同一个 event 改为 TX，不产生第二个 TX event。
2. 如有 `SESSION_F_CUSTOM_TX`，先清 flag，再以
   `TransportTxTarget::Connection(connection_index)` 让具体 transport 用本轮剩余 burst
   执行 `custom_tx`，将返回的 frame-budget packet 数计入 `packets`（不以 pending
   buffer 增量替代）；关闭状态、耗尽预算、再次设置 custom flag
   的分支按 VPP 完成或放回 old list。custom TX 可能改变 event pool，后续只通过
   `event_index` 重新取得元素，不保留跨调用的元素引用。
3. 清 transport descheduled 状态，**即时**调用 `send_params`（TCP 会更新 MSS、窗口和
   `snd_nxt-snd_una` 的 peek offset）；按 `DESCHED`、`POSTPONE`、其他无发送空间分别
   deschedule、tail-old、head-old。若启用 pacing，按 bucket 限制并按 MSS 对齐；不足
   最小 burst 时 head-old。没有可发送 FIFO 数据时重置 pacer bucket 并执行 event
   unset/recheck，不生成 buffer。
4. 从 runtime 的默认 buffer 数据大小和 **140 字节 transport 最大 header 预留**计算
   首 buffer 容量、每后续 buffer 容量、每 MSS 所需链长以及整轮 `buffers_needed`。
   `max_len_to_send` 同时受 FIFO 可读量、offset、transport `snd_space` 和 frame 剩余
   packet 数约束。一次申请全部 buffer；不足就释放已申请的部分、event 放回 head-old，
   只增加 `NoBuffer` node counter，FIFO offset、消费位置和 pacer 扣款均不前进。
5. 申请成功后扣 pacer 字节预算；以 VPP `while n_left >= 4` 的双包流水循环填充，
   预取后续 buffer header，**每轮处理两个首 buffer**，最后 `while n_left != 0`
   处理尾包。每个 packet 的链尾取自同一批 buffer；预留 header 只在首 buffer，
   `Buffer::set_next_buffer`、`set_total_len_not_including_first` 表达 VPP chain flags。
   `transport_pending_buffers` 保存首 buffer index，不放自造 packet carrier。
6. 所有首 buffer 都填好后一次调用 `Transport::push_header`，参数为本轮首 buffer
   切片和实际 `max_dequeue`；具体 TCP 在该调用更新 sequence/重传 timer。释放任何
   未用的已分配 buffer，追加首 buffer 与相同数量的 output next，最后增加 packet
   计数。若本轮受 burst 截断，event 放 tail-old；否则 unset FIFO event、重检、
   必要时 head-old 或 deschedule。仅 dequeue 分支按实际消费字节触发
   `needs_deq_notification`/`session_dequeue_notify`；peek 的 ACK 后消费和通知仍由
   TCP 调用 service 的 `tx_fifo_dequeue_drop` 完成。重传读取用现有 `Session::tx_fifo`
   直接借用并 `peek`，不为纯借用另造 `session_tx_fifo_peek_bytes` helper。

普通 TX **确有一次** FIFO 到最终 packet buffer 的 payload copy，和 VPP 一致；不存在
中间 `Vec<u8>`、TCP 私有 payload copy 或 header 临时 buffer。`push_header` 只在
已有 buffer 的 headroom 中写 header。上述 `tx_copy_data` 不意味着把 FIFO 借用
直接挂进 packet chain；SVM FIFO 的生命周期和 Buffer Arena 的生命周期不同。

`session_tx_*` 签名与 VPP 的对应关系是：`session_worker_t *wrk` 变为
`SessionWorker` 的 `&mut self`；`wrk->ctx` 已是 self 的字段，不能再额外传一个
可变 `ctx`；`wrk->vm` 变成显式的 `&mut DataPlaneMain`；
`session_evt_elt_t *elt` 在 pool 可能因 custom TX 移动时变为同一元素的
`event_index`；`vlib_buffer_t *b` 变为现有 Buffer Pool 的 `u32` index，避免同时
借用整个 runtime 和其中一个 `&mut Buffer`。VPP 的 `u8 peek_data` 用 `bool`，
`int *n_tx_packets` 用 `&mut usize`，VFT 调用由额外的具体 `&T: Transport<E>`
单态化。除此之外，不给 helper 拆入 `fifo`、`offset`、`session`、buffer size
等 VPP 本来从 worker/context 取得的参数。`data0`/`data` 指向 `b` 内部，
Rust 由 `buffer` index 在 helper 内借目标 slice，不能与 `&mut Buffer` 同时外传。
`flush_pending_tx_buffers` 额外接收 `Frame`，仅因为当前
`DataPlaneMain::enqueue_to_next` 要求调用方提供 `&mut Frame`；它不再返回
pending 数量。queue 与 VPP 一样以自己的 `n_tx_packets` 为返回值。

`tx_fifo_dequeue_and_send` 同时覆盖普通 dequeue 与 VPP 的 datagram dequeue：
`const DGRAM` 和 metadata 范型 `M` 仅在注册时选择具体单态化入口，
不成为 Session/Transport/Context 的运行时 mode 字段。普通 dequeue 不读取 `M`；
datagram 的 `M` 是 plugin-session 拥有的 metadata。

## Datagram、Internal、DMA

```rust
impl SessionWorker {
    // VPP session_node.c:1696-1747. Custom/internal transport owns packet
    // generation; service owns burst, event lists and dequeue notification.
    fn tx_fifo_dequeue_internal<T, E>(
        &mut self, runtime: &mut DataPlaneMain, node: &mut NodeRuntime,
        event_index: u32, packets: &mut usize, transport: &T,
    ) -> usize where T: Transport<E>;

    // VPP session_node.c:1965-1973. Fan out paired slices in graph frames;
    // reset both Vec lengths only after graph accepts all of them.
    fn flush_pending_tx_buffers(
        &mut self, runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime, frame: &mut Frame,
    );
}
```

Datagram 不是 `connectionless` 布尔值的同义词。service 用现有
`SessionDatagramPrefix { data_length, data_offset }` 加 `size_of::<M>()` 和 GSO 字段
计算 record 长度；`M` 的 IP 实例及 endpoint 解释都留在 plugin-session。只有完整
record 才可发送；长度为零时按 VPP 丢弃该 header；未完整写入则留待下一轮。单包分段
受当前 record 剩余长度与 GSO size 约束，仅在该 datagram 不需 buffer chain 时合并
相等长度的后续完整 datagram。部分消费只覆写前缀的 `data_offset`，完整消费丢弃
整个 header + payload；需要连接 metadata 的 transport 从最终首 buffer 的预留区
取得该 record 的原始 metadata，不让 service 解码 IP。dequeue 通知的字节数包括
VPP 所计的 datagram header 长度；不可把 stream dequeue 规则直接套过来。
小 datagram 的合并扫描上限为 VPP 的 32 KiB，且只合并 payload 剩余长度相等、
header 和 payload 都已完整进入 FIFO 的 record。
datagram 走上述 `tx_fifo_dequeue_and_send` 的注册期具体化入口，复用共同的
窗口/预算、整批分配、双包流水、链尾和 pending fanout 算法，只在 record 解析与
FIFO 消费规则上分支；不复制第二套 packet scheduler。

```rust
// VPP tcp.c:1171-1196,1372-1380,1402-1431;
// tcp_output.c:980-1035,2154-2186; session.c:738-749.
// Concrete TCP entry registered for each IP Session type by plugin-session.
fn tcp_session_tx(
    worker: &mut SessionWorker,
    runtime: &mut DataPlaneMain,
    node: &mut NodeRuntime,
    event_index: u32,
    packets: &mut usize,
) -> SessionTxOutcome;
```

`tcp_session_tx` 是一个已注册的单态化 callback，不是第二个 TCP TX node；它取得
一次 TCP Main，再以 connection index 调用 service 的 peek 批处理；它**不能**
在进入 service 前保留 `RefMut<TcpWorker>` 或 `&Connection`。当前
`TcpMain::worker` 返回 `RefMut`，而 `Transport::send_params`、`push_header`、
`custom_tx` 等方法还要借同一个 TCP worker；跨 service 调用持有 guard 会在
第一个 trait 方法处触发重复可变借用。每个 trait 方法只在自己执行期间借
owner worker，释放 guard 后才允许下一次调用。来源：
`session_node.c:1494-1590,1652-1655`；Hammer `tcp/src/lib.rs:150-178,450-565`。
TCP 输入 burst 末尾才把 `connection.base` 借给
`SessionWorker::tx_fifo_dequeue_drop`，由 service 从 backlink 取得 Session 并决定
是否通知 AppWorker；TCP 自己仍负责 ACK 合并、timer、pacer 与重排。

Internal 路径不获取普通 FIFO send params，也不调用普通 FIFO `push_header`。它以
`TransportTxTarget::Session(handle)` 调用**同一个** `custom_tx` trait 方法，给出
不超过 `min(frame 剩余, TRANSPORT_PACER_MAX_BURST_PKTS)` 的预算；
`bytes_dequeued` 用于应用通知。custom flag 再次出现时 tail-old；否则按 VPP 清 FIFO
event、重检并可能 head-old。它保留为明确路径，即使当前没有插件使用；不以
`NotSupported` 假装这个 VPP 路径不存在。

DMA 是普通路径的可选实现，不改变 TX 语义。VPP `session.c:2119-2194` 和
`session_node.c:976-1071,1456-1468,2054-2063,2132-2145`：启用后从 FIFO
借 readable segments 向已分配 packet buffer 安排 DMA；本轮首 buffer/next 进入
`dma_transfers[dma_tail]`，提交 batch，completion 在所属 worker 将同一批成对移入
普通 pending 列，queue 后续轮次再 flush。ring 满或无法取得 batch 时本轮不消费
事件。当前 runtime 没有通用 DMA batch API，因此本次实施默认 `dma_enabled=false`
并用 VPP 的软件 fallback；不能让配置位开启一个没有 completion 的假 DMA 路径，
也不能在提交前提前扇出未完成的 packet。日后增加 runtime 通用 DMA 能力需独立批准，
不能在 service 私造 DMA engine。

## 初始化、同步、错误和 inline

`SessionMain::init` 构造固定 worker `Vec` 时初始化 TX scratch 与两个 pending 向量，
event queue/output edge/transport 单态化 TX 入口在 worker 开始处理该 Session 类型前
完成注册；cacheline mark 分隔 worker/context 的热字段。每次 TX 事件只重置本轮计数
与 scratch 向量长度，不重建 context，也不逐包查询全局 Main。

TX context、event lists、pending buffers 和 transport connection 都由同一个 Data
Worker 写，不需要 `SpinLock`、`RwLock`、`ArcSwap` 或 TX snapshot。跨 worker TX
只通过现有 Session event queue 投递；app 与 worker 之间的 FIFO head/tail、event
bit、dequeue notification 使用 SVM FIFO 的 acquire/release 协议。具体地，consumer
读 producer tail 使用 acquire、消费 head 使用 release；`set_event` 是 release
原子交换，`unset_event` 必须是 acquire 原子交换，然后重读 FIFO 可用量。
现有 `hammer-infra::svm::fifo::Fifo::unset_event` 是 release store，**尚未对齐**
`svm_fifo_unset_event`；TX 接入前在原 API 上修正这一原子契约，不新建第二套 event
bit，也不能用普通 bool 代替。Session state
继续使用现有 `AtomicU8` 的 acquire/release 读写表达 VPP 的 volatile 可见性；
`volatile` 不能替代 Rust 跨线程同步。注册或改 output edge 必须在启动前或
`WorkerBarrier` 内完成；barrier 是唯一的 graph publication 边界。DMA completion
必须回到 owner worker，不能从其他 worker 直接改 pending `Vec`。

```rust
// VPP session_node.c:925-949,1580-1587,2155-2159.
// Node counters, not control-plane Result or numeric retval.
pub enum SessionQueueNodeError {
    Tx,
    Timer,
    NoBuffer,
}
```

`NoData`（未 ready、窗口为零、pacer 等待、FIFO 不足或 datagram 未完整）和
`NoBuffers` 都是 TX outcome，不是 `SessionError`；仅后者增加 `NoBuffer` counter。
`Tx` 计数是成功 packet 数，`Timer` 保持 VPP 声明但本 TX 路径不自增。
`SessionTxOutcome::Ok` 只报告 VPP 的状态，`packets: &mut usize` 是唯一的
frame-budget 计数；TCP custom ACK 的返回值与 pending buffer 长度可能不同。
TCP 自己申请 ACK/重传 Buffer 失败时走 TCP worker 的资源路径，不记为
Session Queue `NoBuffer`。`TcpSegment::write_to_buffer` 在预留 140 字节 header
空间且 TCP options 不超过协议上限后失败，是 TCP output 的内部不变量：
按 connection/buffer 身份断言，不把每包 header 构造错误上翻为
`SessionError::TransportOpFailed`。来源：`tcp_output.c:884-1035,1037-1058`、
`session_node.c:1580-1587`。
缺少已注册 output edge、分配后的 FIFO 可读区间突变、链长度不匹配、
transport 返回超过 burst 的 packet 数，均是 owner 不变量，带 Session/worker
身份断言；不发明诸如 `MissingEgressEndpoints`、`TxModeUnsupported` 的恢复错误。
注册时 output graph 不存在则复用 `SessionQueueError::OutputMissing`，不接受
`u32::MAX` 哨兵进入热路径。外部配置与资源失败继续使用现有带 source 的
`SessionError`/`SessionQueueError`（VPP `session_types.h:519-561`）；不把 packet
失败翻译成 `i32`。

| 方法 | inline | VPP 依据 |
|---|---|---|
| `tx_not_ready`、`tx_get_transport`、`tx_set_dequeue_params`、`tx_fill_dma_transfers`/`tail`、`tx_copy_data`/`tail`、`tx_fifo_chain_tail`、`tx_fill_buffer`、`tx_maybe_reschedule` | `#[inline(always)]` | `session_node.c:976-1453` 的 `always_inline` |
| `tx_add_pending_buffer`、`add_pending_tx_buffer`、FIFO event 快路径 | `#[inline(always)]` | `session_node.c:1456-1468`、`session.h:1093-1115` |
| `tx_fifo_read_and_send_i` 共用读/发算法 | `#[inline(always)]` | `session_node.c:1472` 的 `always_inline` |
| peek、dequeue、internal 三个入口 | 不强制 inline | `session_node.c:1680-1747` 的普通函数；datagram 复用 dequeue，不造第四个入口 |
| `flush_pending_tx_buffers`、transport 注册、DMA setup/completion | 不标注 | `session_node.c:1965-1973`、`session.c:1827-1847,2119-2194` |
| `Transport::flush_data`、`app_rx_event` 及 TCP 实现 | 不强制 inline | `transport.h:86-88,183-191`、`tcp.c:1372-1401` 的 callback 非 `always_inline`；`transport_app_rx_evt` 包装虽为 `static inline`，Hammer 的单态化 dispatch 不需额外包装层 |
| TCP 批量 ACK 排队/释放与 `push_header` 外层 | 不强制 inline | `tcp_input.c:494-545` 的 `static` 函数、`tcp_output.c:980-1035` 的普通函数；内部 `tcp_push_one_header` 为 `always_inline`（`tcp_output.c:965-978`） |
| TCP 发送空间和 recovery FIFO 剩余量的小计算 | `#[inline]` | `tcp.c:1100-1164` 的 `static inline`、`tcp_output.c:1741-1746` 的 `static inline`；不把整个 `send_params` 强制内联 |
| TCP 逐首 buffer 写 header 的内层 | `#[inline(always)]` | `tcp_output.c:966-978` 的 `always_inline tcp_push_one_header`；外层批次循环不强制内联 |
| FIFO OOO 原位合并/收集、TCP custom TX 排队、ACK burst 排空、Session dequeue/drop 与 reschedule | 不强制 inline | `svm_fifo.c:171-340`、`session.c:172-221,738-749`、`tcp_input.c:494-545` 的普通函数；不因调用频繁擅自标 `always` |

### 实施前 API 决策

本 ADR 是目标设计，**不授权本次修改生产代码**。下表是实施时需逐项批准的新增或
变更公开 API；私有 FIFO/TCP 函数仅为本模块实现，不另发对外能力。没有列出的
新增包装类型、错误变体或兼容接口不属于本 ADR。

| 目标 API / owner | 现有接口为何不足 | 消费者与失败处置 |
|---|---|---|
| `SessionTxDispatch`、`SessionTxOutcome`、`SessionMain::register_transport` / service | 旧 `SessionIoDispatch` 合并 RX/TX、`TransportTxMode` 按事件动态选、output next 可为哨兵 | Session queue 按 Session type 调已注册的单态化 TCP 入口；注册失败复用 `SessionQueueError::OutputMissing`，运行期缺 edge 为 owner 断言 |
| `SessionWorker::add_self_custom_tx_event`、`reschedule_tx`、`tx_fifo_dequeue_drop` / service | `enqueue_ready` 不表达 custom flag/new-old 优先级；直接 FIFO drop 漏应用 TX/dequeue 通知 | TCP ACK、重传、burst 收尾；Session owner 保证 FIFO event 与 AppWorker 投递，缺 owner Session 为不变量 |
| `Transport` 的即时 `send_params`、`flush_data`、`app_rx_event`、`custom_tx`、批量 `push_header` / service trait，由 TCP 实现 | 当前单包 TX、空 `flush_data`/`custom_tx`、RX `NotSupported` 不能承载 VPP 接线 | TCP plugin 在 owner worker 单态化调用；无 RX hook 是成功 no-op；Buffer 不足是 node/worker 资源路径，非新 `SessionError` |
| `TransportTxTarget` / service | 同一 custom TX 入口在 VPP 注册期分别接 connection 或 internal Session；不能把 `void *`、IP 类型或运行时 `TxMode` 带进 service | 仅单态化入口构造；注册模式与 variant 不符为插件注册 bug，不返回数值错误 |
| `OooResult` 语义、`RxDelivery::OutOfOrder.newest: Option<_>`、`Fifo` 的 OOO 元数据原位操作 / infra 与 service | 当前每包区间与临时 `Vec` 不是 FIFO 合并结果；按序总写入量不能反推 copied/promoted，segment 索引污染 chunk lookup，普通 `u32` 下从 `&Fifo` 写 list 索引不合法 | offset-zero 复用既有 `OooResult` 分离 copied/promoted，chunk lookup 只服务 chunk；容量不足归 TCP input node `FifoFull`，不增控制面错误 |
| TCP existing connection/worker 的 PSH、ACK burst、reset state、timer/cleanup 字段与方法；现有 `receive_ack_with_timers` 返回 `bytes_acked: u32` / TCP plugin | 旧 packetized 路径直接 ACK/drop、默认 PSH，pending 列没有消费，timer/cleanup 分离；原 ACK 方法返回 `()` 丢掉 VPP `ac.bytes_acked` | TCP input/output owner 处理；只从已接受的 ACK 反馈向 worker pending 列传字节数，service 不分类 TCP ACK；timer wheel 的 Rust 可恢复失败保留已有 `TcpNodeError::TimerUpdateFailed`，必须清理 pending 队列后上报 |
| transport congestion 的 BBR 状态与 TCP recovery 的 dupACK/SACK/reorder 事实 / `hammer-service::transport::congestion::CongestionController` trait | 现有 `send_goal_size`/`tx_payload_budget` 不能重现 `tcp_cc_update_cwnd_limited` 的半窗口分支和 `tcp_snd_space_inline` 的 Limited Transmit | `TcpConnection` 直接持有 service 的 `BbrController`，通过 trait 静态分派；TCP 不再保留私有函数表或状态擦除层，CC 不进入 Session 调度 |

`session_transport_delete_request`/`session_stream_connect_notify` 的 Rust 对应入口在
ADR-0040 的 Session 生命周期范围内；本 ADR 的 SYN-RCVD/SYN-SENT RST 代码不能
替代这项前置工作，也不能用 `transport_reset` 冒充。FIFO tuning、TX FIFO
subscriber 通知与 AppWorker dequeue event 均由 service 在同一次
`tx_fifo_dequeue_drop` 调用内完成；代码片段中的注释是该方法的必做步骤，
不能在实施时省略。来源：VPP `session.c:548-567,602-625,657-680,738-749,753-805`、
`tcp_input.c:105-149,494-545`。

## 迁移顺序与验收

1. 移除 `TransportTxMode`、`TransportOptions.tx_mode`、`SessionTxContext.tx_mode`、
   `SessionMain.session_tx_modes` 和 `IpSessionMain::register_transport_type(tx_mode, ...)`；
   `init_tcp` 只分配一次 TCP protocol id、注册 RX/control/time；两个 output
   graph node 在现有 init 内各自编译 next 并注册对应 Session type/TX 入口；
   plugin-session 派生两个
   Session type。Session 保存不相互替代的 type/protocol，TCP worker init
   缓存相同的两个 next 供 control packet 使用。保留 `Transport` trait；
   不把 VPP 的 VFT 或整个 `session_tx_fns[TRANSPORT_TX_N_FNS]` 复制进 Hammer。
2. 修正 worker scratch/cacheline、成对 pending 向量、整批预算/分配、140-byte
   headroom、buffer chain 和双包流水循环。复用 `Buffer` 现有 chain API 与 SVM
   FIFO 的 `peek`/`dequeue`/`drop_dequeue`/event/notification API，并先修正
   `unset_event` 的 acquire 交换语义；不增加 payload 搬运容器。
3. 接通 TCP peek、`TX_FLUSH`、pacer、custom TX、RX 零窗口恢复和 dequeue ACK 通知；
   将现有 `tcp_session_io` 的 RX/TxFlush 分支收敛到 `Transport` trait，删除
   RX 的连接克隆和 `NotSupported` 返回，ACK 通过 Session pending buffer 扇出；
   `TcpConnection::tx_segment` 去掉默认 PSH，`push_header` 批量 headerize 后每段
   只提交一次 sequence/recovery 状态；恢复期 custom TX 从 Session FIFO 直接
   读取未发送数据。TCP 输入复用 `pending_deq_acked`，在 burst 末尾统一执行
   `SessionWorker::tx_fifo_dequeue_drop`、timer/pacer 更新与重新调度，删掉各输入
   node 的直接 `drop_dequeue` 和逐 ACK `enqueue_ready`。同一 TCP time subscriber
   处理到期 cleanup、唯一一次 wheel 更新和受限 timer 派发；FIN/RST pending 列
   在 input burst 尾部消费，握手 RST 等待 ADR-0040 的 Session 生命周期入口。
4. TCP RX 使用已有 FIFO `OooSegment`：让 `OooResult` 报告合并后的 newest，
   `RxDelivery::OutOfOrder` 允许无 newest；service 对 Buffer chain 保留 FIFO
   最后一次 OOO 插入的结果，不合成区间。`established`、`rcv_process` 和
   `syn_sent` 都在 FIFO 实际入队后推进 `rcv_nxt`，按旧包/重叠/未来/按序四种
   情况处理 DUPACK、SACK、FIN 与应用 RX event；移除 SYN-ACK data 的预推进。
   infra 的 OOO chunk lookup 在 chunk 附着/回收时维护，future 写入用它定位，
   segment pool/list 不得污染该 tree。
   FIFO 容量不足归 TCP input node enum counter，不上翻为 control-plane Result。
   再接 datagram/internal 分支；DMA 待 runtime 通用能力存在才启用，默认软件路径。
5. 验收需覆盖单事件多 MSS、首包与多 buffer 链、`n_left` 为 1/2/3/4/5 的循环
   边界、部分 buffer allocation、窗口/offset/pacer、custom TX 后重入、TX_FLUSH、
   FIFO event 竞争、dequeue 通知、重复 flush 的 PSH 边界、RX FIFO 阈值上下界、
   zero-window ACK 的 buffer 不足、recovery 中的新数据只由 custom TX 发送、
   ACK burst 合并释放/应用通知/重新调度、单协议双 Session type、TCP worker
   guard 不跨 service 重借、init/graph/worker 启动顺序、
   RX 旧包、前缀重叠、未来区间合并、重复 OOO 插入、跨 Buffer 链 OOO、
   按序补洞一次推进 `rcv_nxt`/通知 app、带 data 的 SYN-ACK 仅按实际入队推进、
   OOO FIN 被拒、FIFO 满/部分写入的 node 分类、SACK 取 FIFO merged newest、
   ACK 后零窗口 persist/RTO 切换、timer token 在 cleanup 后失效跳过、cleanup
   队头未来时间不越过、FIN/RST 同一 burst 去重及原状态通知、
   send params 不重复执行旧 pacing 门控、datagram 部分/完整 record、output next 4/6 和
   pending flush 的多 frame 扇出。当前已有部分实现，但 review 文档列出的
   Session/TCP/FIFO 缺口尚未全部修复；未运行编译、测试、静态检查或 CI。
