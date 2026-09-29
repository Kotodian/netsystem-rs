# ADR-0045: 单 Worker 大页 TUN/TCP/iperf3 测试

- 日期：2026-09-29
- 状态：CPU section 与 bitmap 核分配、单 worker 大页测试配置已实现；全路径大页与标准 iperf3 验收仍有前置缺口；测试未执行
- 前置：ADR-0039、ADR-0041、ADR-0043、ADR-0044
- 范围：Linux 同机 TUN、一个 Data Worker、HugeTLB Main Heap/Buffer、TCP 与 builtin iperf3 server

## 目标与边界

本测试先确认主机到 Hammer TUN 的 IPv4 双向路径、TCP 握手、Session/FIFO 数据接收以及 **Main Heap 和 packet Buffer** 的显式大页映射，再尝试标准 iperf3 客户端。它目前不能证明 Session MQ/FIFO 也使用大页。`[cpu].workers = 1` 指一个 **Data Worker**；主线程和其他辅助线程不计入该数。主机的 iperf3/socat 进程不是 Hammer Worker。

测试不添加 CLI、协议适配层、额外 listener、错误类型或测试专用实现。VPP 的 TUN、Session queue、TCP 和 builtin app 是数据路径与所有权的参照，不代表 VPP vperf 与标准 iperf3 互通。

| 依据 | 对本测试的意义 |
|---|---|
| `third_party/vpp/src/vnet/unix/tuntap.c:229-399,500-521` | TUN 文件就绪后把原始 IP 包送进 graph；Hammer 的主机侧地址仍须单独配置。 |
| `third_party/vpp/src/vnet/session/session_node.c:1472-1677,1858-1910` | Session queue 消费 IO/TX event，按 FIFO 字节生成 TCP 输出；不能仅凭 TCP accept 判定 TX 正常。 |
| `third_party/vpp/src/vnet/tcp/tcp_input.c:3006-3235`、`src/vnet/tcp/tcp_output.c:884-1035` | TCP input/状态分发和 output 是独立验收点。 |
| `third_party/vpp/src/vlib/buffer.c:749-770` | VPP Buffer 缺少大页时可以回退；本次测试必须显式选择大页并验证，不能把回退结果算作大页测试。 |
| `third_party/vpp/src/plugins/vperf/builtin/vperf_server.c:65-110` | builtin app 从 Session accept 获取数据路径；vperf 本身不是标准 iperf3 协议实现。 |
| `third_party/vpp/src/vnet/session/session.c:1733-1785` | worker MQ 使用 memfd segment，但不主动设置 huge-page 位；默认不要求 HugeTLB。 |
| `third_party/vpp/src/vnet/session/application.c:665-722`、`src/plugins/vperf/builtin/vperf_server.c:425-449` | builtin app 默认 private segment；只有显式 `USE_HUGE_PAGE` 才请求应用 segment 大页，vperf 默认未开启。 |
| `third_party/vpp/src/svm/ssvm.c:357-385` | private SSVM 按普通系统页映射；要强制 HugeTLB，不能只改变应用的 huge-page 标志而仍保留 private 后端。 |
| `third_party/vpp/src/vpp/conf/startup.conf:58-87`、`third_party/vpp/src/vlib/threads.c:215-299,1098-1204` | VPP 把主核、worker 数量/核列表、跳过核和相对 CPU 集合放在独立 `cpu` section；主核从 worker 可用集合中剔除，自动分配跳过 CPU 0（仅在无其他核时回退）。 |

## 测试配置

将下列内容用作 Hammer 的启动 TOML。测试地址使用 RFC 2544 基准网段，避免复用主机已有生产地址；若该网段在测试机上已有路由，先换成独立网段，并同时改主机地址与客户端目标地址。

```toml
plugins = ["tuntap", "iperf3"]

[memory]
main_heap_size = "512 MiB"
main_heap_page_size = "default-hugepage"

[cpu]
workers = 1

[worker.buffer]
slots_per_numa = 8192
frame_pool_size = 64
page_size = "default-hugepage"

[statseg]
socket_name = "/tmp/hammer-tun-tcp-iperf3.stats"

[plugin.tuntap]
enabled = true
name = "hammer0"
mtu = 1500
admin_up = true
ip4_address = "198.18.0.1/30"

[plugin.tcp]
congestion = "bbr"

[plugin.iperf3]
enable = true
namespace = 0
control_endpoint = "0.0.0.0:5201"
data_endpoint = "0.0.0.0:5202"
duration = "10s"
```

`[cpu]` 是运行时的 CPU 拓扑入口，不是测试专用字段。`workers` 与
`corelist-workers` 互斥：省略 `corelist-workers` 时，`workers` 指要创建的 Data Worker
数量；提供 `corelist-workers` 时，置位 CPU 的数量就是 worker 数量。运行时保存的是
`hammer_infra::bitmap::Bitmap`，不是一个保留重复项或输入顺序的 `Vec`；TOML 数组和 VPP
样式的范围字符串（例如 `"2-3,18-19"`）只是在配置边界写入 bitmap，重复 CPU 按 VPP
bitmap 语义合并。`main-core` 不属于 Data Worker，且不能和 worker bitmap 重叠。
`skip-cores` 在自动分配前从当前进程允许的 CPU bitmap 起始位置逐位清除指定数量；
`relative = true` 时，`main-core`/`corelist-workers` 使用该允许 bitmap 的序号，对应 VPP
的 `relative`。`[worker]` 只保留线程栈、空闲片、buffer、handoff、App Session 容量和
NUMA buffer 资源配置。

VPP 依据：`third_party/vpp/src/vlib/threads.c:230-279` 复制可用 CPU bitmap、跳过
`skip-cores` 并删除 main core；`:352-401` 对显式 coremask 做可用性校验，自动分配时
清除 CPU 0、优先选择第一个置位 CPU 并仅在非 relative 模式下把 CPU 0 作为最后回退；
`:1098-1162` 将 `corelist-*` 解析为 bitmap 并用 `clib_bitmap_count_set_bits` 决定线程数；
`:1235-1260` 将 relative corelist 映射回进程 affinity bitmap。Hammer 复用
`hammer_infra::Bitmap::{set,clear,is_set,first_set,iter_set,count_set}` 表达同一集合操作，
不再用 `HashSet` 或运行时 worker CPU `Vec` 模拟该 bitmap。

运行时字段与分配边界如下；配置反序列化只负责把数组/范围输入置入 bitmap，之后的
worker 数量、冲突检查、可用核删除和线程描述符遍历都直接消费该集合：

```rust
pub struct CpuConfig {
    pub workers: Option<usize>,
    pub main_core: Option<usize>,
    pub corelist_workers: Bitmap,
    pub skip_cores: usize,
    pub relative: bool,
}

impl CpuConfig {
    pub(crate) fn worker_count(&self) -> usize {
        self.corelist_workers
            .is_empty()
            .then(|| self.workers.unwrap_or(DEFAULT_WORKER_COUNT))
            .unwrap_or_else(|| self.corelist_workers.count_set())
    }
}
```

这里的 `Bitmap` 是 `hammer-infra::bitmap::Bitmap<usize>`；它不是配置字符串的缓存，也
不是按 worker 复制的索引表。显式 corelist 的物理核集合和自动选择结果都保持为 bitmap，
只有创建 `WorkerThread` 描述符时通过 `iter_set()` 得到稳定的升序核号。
`ThreadMain::worker_threads` 仍是连续的 `Vec<WorkerThread>`，对应 VPP 的
`vlib_worker_threads` 描述符向量；它保存线程记录而不是表达 CPU 集合，因此不改成 bitmap。

多 worker 的等价配置示例：

```toml
[cpu]
main-core = 1
corelist-workers = [2, 3]
```

自动分配示例（跳过前两个允许 CPU，并创建两个 worker）：

```toml
[cpu]
main-core = 2
workers = 2
skip-cores = 2
```

实现边界：`CpuConfig` 由 `hammer-runtime` 的早期 `cpu` config registration 安装；
`ThreadMain::configure` 只读取这份已验证配置，并在 worker 描述符创建前解析 CPU 集合。
`WorkerThread` 使用运行时默认调度策略。因此 Data Worker 数量和 CPU 归属在启动后均冻结，
不会在主循环或插件配置中再次解析。旧的 `[worker].count`、`[worker.cpu]`、
`[worker.scheduler]` 和 `app_core` 输入不再是配置面；`[worker]` 只接受资源字段。

`plugins` 仅列根插件；`tuntap` 依赖 `ip`，`iperf3` 依赖 `session` 和 `tcp`，后者再加载自己的依赖。插件 DSO 默认从 daemon 可执行文件旁加载；自定义目录使用 `HAMMER_PLUGIN_DIR`。`namespace = 0` 是现有默认 namespace，不需要另建一个 namespace。

显式的 `main_heap_page_size` 和 `worker.buffer.page_size` 分别要求 Main Heap 和 packet Buffer 使用 HugeTLB；缺页时应启动失败，不接受默认 Buffer 普通页回退。**这不是“所有 SVM 都用大页”配置**：当前 service Session worker MQ 创建时 `huge_page = false`；iperf3 attach 仅带 `ApplicationFlags::BUILTIN`，因此应用 RX MQ/FIFO segment 使用 private 后端及普通页。这个默认值对应上表的 VPP 路径，不是 `main_heap_page_size` 可以传递给 SVM 的隐式开关。`duration` 当前仅参与 iperf3 配置校验，实际传输时长以客户端命令为准。

如果测试目标是 **Session MQ、应用 RX MQ、FIFO 也全部强制 HugeTLB**，当前 TOML 和实现做不到，不能把本配置的结果称为“全路径大页”。后续需由 service Session owner 决定 worker MQ 的大页创建策略；iperf3 的 Application attach 则需同时选 builtin memfd 后端与 huge-page 选项，使应用 MQ/FIFO 使用可承载 HugeTLB 的 memfd。两处都应在创建失败时明确失败，不能回退到普通页。本 ADR 只记录这两个实施前置条件，不擅自新增配置字段、改变 VPP 默认值或修改代码。

## 执行顺序

在隔离的 Linux 测试机执行；需要 `/dev/net/tun`、`CAP_NET_ADMIN` 和足够的 HugeTLB 页。以下示例假设系统默认大页为 2 MiB。先记录原有 `vm.nr_hugepages`、`HugePages_Free`、Hugepagesize 与 NUMA 分布；若大页大小不是 2 MiB，应按实际大小重新计算页数，不照搬 512。512 个 2 MiB 页是供 512 MiB Main Heap、约 16 MiB packet Buffer 及余量使用的测试预算，不是产品默认值。仅在专用测试机上按需预留；已有配额高于 512 时不要降低它，测试后由管理员恢复原有配置。

```sh
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo
cat /proc/sys/vm/nr_hugepages
# 仅当默认大页是 2 MiB 且当前预留不足时，由管理员执行：
sudo sysctl -w vm.nr_hugepages=512
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo
```

1. `cargo build --workspace --locked` 构建 daemon 和需要的 DSO。确认 `target/debug/hammer` 旁存在 `libhammer_plugin_tuntap.so`、`libhammer_plugin_ip.so`、`libhammer_plugin_session.so`、`libhammer_plugin_tcp.so`、`libhammer_plugin_iperf3.so`。本文不执行构建。
2. 用上节 TOML 的实际文件路径启动 `sudo ./target/debug/hammer /absolute/path/to/startup.toml`。记录 PID 和启动日志；如使用 `HAMMER_PLUGIN_DIR`，确保提权后的进程仍收到该环境变量。启动失败时不继续网络测试，先区分 DSO 缺失、TUN 权限、大页不足和 listener 创建错误。
3. daemon 启动后，在**主机**执行以下命令。Hammer 接口是 `198.18.0.1/30`，主机端是 `198.18.0.2/30`；内核应把发往 `.1` 的流量路由到 `hammer0`。不要在 Hammer 配置里把两个地址都配到同一个接口。

```sh
sudo ip address replace 198.18.0.2/30 dev hammer0
ip -4 address show dev hammer0
ip -4 route get 198.18.0.1
ip -s link show dev hammer0
```

4. 验证大页与 worker 身份。以实际 PID 检查 `smaps`，其中应找到 Main Heap 和 packet Buffer 的 HugeTLB 映射（`VmFlags` 含 `ht`），并记录测试前后的系统空闲大页；仅检查配置或 `HugePages_Total` 不足以证明进程实际用了大页。`ps -T` 只用于辅助观察 CPU/线程，不以总线程数等于 1 作为验收条件。

```sh
hammer_pid=$(pgrep -n -x hammer)
ps -p "$hammer_pid" -o pid,args
sudo awk '/^[0-9a-f]+-[0-9a-f]+/ { region=$0 } /VmFlags:.* ht/ { print region; print }' "/proc/$hammer_pid/smaps"
ps -T -p "$hammer_pid" -o pid,tid,comm,psr
```

5. 先做数据路径测试：`socat` 从主机向 5202 单连接发送 8 MiB，当前 iperf3 data callback 直接消耗 RX FIFO 字节。这验证 TUN RX、IPv4/TCP、Session RX FIFO、应用消费及 ACK/TUN TX；它**不是**标准 iperf3 吞吐成绩。传输前后各记录 `ip -s link show dev hammer0` 和 daemon 错误日志；需要时用 `tcpdump -ni hammer0 'tcp port 5202'` 区分握手、数据和重传。

```sh
dd if=/dev/zero bs=64K count=128 status=none | socat -u - TCP:198.18.0.1:5202,connect-timeout=3
ip -s link show dev hammer0
```

6. 最后做标准客户端兼容性探测。单流、IPv4、10 秒是首个候选；不要先加并发、反向或 UDP。记录客户端的退出状态、首个错误和抓包阶段；只有完整参数协商、数据流和结果交换都成功，才报告 iperf3 吞吐量。

```sh
iperf3 -4 -c 198.18.0.1 -p 5201 -P 1 -t 10
```

结束时停止 daemon；确认它拥有的 TUN 被清理。仅撤销本次给主机添加的 `198.18.0.2/30`（若接口仍在），并由主机管理员恢复测试前的大页预留值。不要清理其他 TUN、路由、进程或 HugeTLB 预留。

## 验收与已知缺口

| 阶段 | 通过标准 | 失败时优先排查 |
|---|---|---|
| 启动 | 单 Data Worker 配置被接受；TUN、两个 listener 均初始化；Main Heap 与 Buffer 都可证实为 HugeTLB | 权限、同名 TUN、5201/5202 冲突、大页总量或 NUMA 节点不足；若这两类映射是普通页，**本阶段失败** |
| 全路径大页 | 仅在 Session MQ 与应用 MQ/FIFO 也有 HugeTLB 映射、且无普通页回退后通过 | 当前缺少可用配置与实现；**本 ADR 不将该项判为通过** |
| 主机路由 | `route get` 显示 `dev hammer0`、源地址 `.2`；TUN 双向计数有变化 | 主机已有更优路由、地址配反、接口未 UP |
| 5202 数据流 | 单 TCP 连接完成 8 MiB 发送并正常结束，TUN RX/TX 计数增加，无异常 reset/持续重传 | TCP accept、Session event、FIFO dequeue、ACK/output、IPv4 rewrite |
| 标准 iperf3 | 完整 control/data/results 交换且客户端正常退出，才记为协议通过 | 当前实现预期不能满足此项，见下文；不能用 5202 的裸 TCP 成功替代 |

当前插件的 control 监听 5201、data 监听 5202，配置还禁止两个端口相同；标准 iperf3 客户端的 control 和 data stream 连接同一个 `-p` 服务器端口。因此标准客户端的 data 连接不会到达现有 data listener。此外，当前 `ControlParser`/`on_rx` 只覆盖部分状态和单字节响应，没有完整的 iperf3 JSON 结果交换；`duration` 也未驱动服务端测试时钟。**本 ADR 不宣称标准 iperf3 端到端已经可通过**。将第 6 步视为暴露兼容性缺口的探测；补齐协议后再把它升为性能验收，并分别测单流、反向、多流及丢包/重传条件。

事实依据：`crates/hammer-plugins/app/iperf3/src/config.rs`、`src/main.rs`、`src/protocol.rs`；`crates/hammer-service/src/session/mod.rs`、`src/app.rs`；`crates/hammer-runtime/src/data_plane/buffer_pool.rs`；`crates/hammer-infra/src/mem/mod.rs`。标准客户端同端口行为对应 ESnet iperf3 的 `iperf_client_api.c::iperf_connect` 与 `iperf_tcp.c::iperf_tcp_connect` 均使用 `server_port`。其他运行表现仍待上述实测，不以编译成功代替。
