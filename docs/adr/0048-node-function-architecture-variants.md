# ADR-0048: Node Function 的架构变体注册与选择

Status: implemented (source review only; compilation, tests, CI and codegen validation not run)

Date: 2026-10-01

## 1. 范围和现状

本 ADR 设计 Graph Node function 与通用 graph fanout 函数的 CPU 架构
变体。`CLIB_MARCH_SFX` 是 VPP 在多架构编译时给同一个函数及其注册对象
生成不同符号的手段，不是 SIMD
宽度、每包调用的分发器或一种新 Node 类型。保留 Hammer 现有
`#[graph_node]`、`#[node_function(node = ...)]`、`RegistrationImage::node_functions`
与 `NodeMain`；不新增 service/plugin 私有注册表、Node VFT、CLI 或 TOML 开关。
`enqueue_to_next` 对齐 VPP 的普通 `CLIB_MARCH_FN` 路径，不伪装成 Node
candidate；二者共享 CPU 变体判定，但各自安装函数。

当前 `hammer-component-macros/src/lib.rs:894-1098` 为一个函数固定生成
`scalar/simd128/simd256/simd512` 四份；
`hammer-runtime/src/node.rs:1169-1207,1303-1329` 以 `simd_bytes` 最大值选择；
`runtime_simd.rs:6-18` 把 CPU 能力归结为一个向量宽度。这个模型把 AVX2
等同于完整 x86-64-v3，把 AVX-512F/BW 等同于完整 x86-64-v4，既不是
VPP 的变体身份，也不能表达 ARM CPU 型号优先级。`DataPlaneMain::for_worker`
从主线程克隆已选函数 (`data_plane/worker.rs:105-156`)；在异构 CPU 上，
主线程选出的专用函数可能在另一个 worker 上不安全。

VPP 依据如下，路径均相对仓库根目录：

| VPP 源码 | 语义 |
| --- | --- |
| `third_party/vpp/src/vppinfra/cpu.h:35-47` | `CLIB_MARCH_SFX` 为默认构建保留原符号、为变体构建附加后缀。 |
| `third_party/vpp/src/vlib/node.h:94-99,208-223` | `VLIB_NODE_FN` 给同一 Node 产生每变体的 function/registration，注册内容是函数与变体身份。 |
| `third_party/vpp/src/cmake/cpu.cmake:114-155,165-244,269-295` | 默认对象与额外变体对象分开编译；变体只有编译器支持且构建启用时才存在。x86 默认 x86-64-v2，另有 v3/v4；ARM 变体由目标 CPU/编译选项决定。 |
| `third_party/vpp/src/vppinfra/cpu.h:375-402,449-542` | 根据完整 CPU 能力或 ARM implementer/part 返回优先级；不支持的变体不参与选择。 |
| `third_party/vpp/src/vlib/node.c:259-292,347-368,604-632` | 初始化变体目录；Node 注册时选可用候选的最高优先级，或选显式配置且存在的候选。 |
| `third_party/vpp/src/vlib/node.c:874-901`、`node_init.c:36-96` | 手动指定变体会更新 Node 和各线程的 Node Runtime；这是控制面图变更，不是热路径探测。 |
| `third_party/vpp/src/vlib/main.c:833-917` | 分发直接调用已安装的 Node Runtime 函数。 |
| `third_party/vpp/src/vlib/buffer_funcs.c:10-173,388-415`、`buffer_node.h:353-392` | `enqueue_to_next` 是普通多架构函数，init 选择指针；每次调用经固定指针进入 bitmap/mask/compact 批处理，不属于 Node function inventory。 |

## 2. 边界与决策

* `hammer-component-macros` 只生成同一 Node body 的候选符号、目标特性标记、
  typed Frame trampoline 和静态声明；不检测运行 CPU。
* `hammer-runtime` 持有通用 `NodeVariant` 身份、注册清单与选择算法。插件
  仍在自己的 `RegistrationImage` 中声明候选，runtime 不含 TCP/IP/TUN 类型。
* `NodeMain` 的现有 worker-local Node function slot 是唯一执行入口。主线程
  建图时若已绑到固定 CPU，可选本线程能执行的函数；否则使用 baseline。
  Data Worker 在 CPU affinity 生效后、进入 worker-init 和 graph loop 前，
  对自己的 NodeMain 重选。refork 接收新图
  后、恢复调度前重选，包括后来注册的 Node。不得直接执行克隆自另一 CPU
  的专用函数。
* baseline 永远可用：有 `#[node_function]` 则用其 baseline 候选；没有则
  使用 `#[graph_node]` 原 `process`。专用候选缺席或不被本 CPU 支持时回退
  baseline；不把正常回退建模为错误或日志。Node function 注册及调用仍属于
  现有插件 image 生命周期，不能卸载正在被 Node Runtime 引用的插件映像。
* 此阶段不引入 VPP `node { default/per-node variant ... }` 的手动覆盖。
  若后续增加，必须使用现有 `WorkerBarrier` 停住 worker，再改主图与各 worker
  的函数槽并按现有 refork 规则发布；不能直接仿照 C 代码在运行中的 worker
  上写函数指针。手动覆盖还须逐 worker 检查 CPU 支持，不能只在 main 核检查。

下面的 Rust 块说明接口与所有权；注释标出对应的 VPP 来源。

```rust
// hammer-runtime::node；VPP vppinfra/cpu.h:35-47、vlib/node.h:94-99。
// 身份是 ISA/CPU 变体，不以 SIMD 字节数充当身份。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NodeVariant {
    Base,
    X86_64V3,
    X86_64V4,
}

impl NodeVariant {
    // VPP vppinfra/cpu.h:375-402、vlib/node.c:279-288。
    // 不支持时为 None；有值时为选择优先级。只在初始化/refork 时调用。
    #[inline]
    pub(crate) fn priority_on_current_cpu(self) -> Option<u8>;
}

// 沿用现有注册类型，不增加包裹类型或第二张函数表。
// VPP vlib/node.h:94-99,208-223。
pub struct NodeFunctionRegistration {
    node_name: &'static str,
    variant: NodeVariant,
    function: fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize,
    frame_args_size: (u16, u16, u16),
}

impl NodeFunctionRegistration {
    // 宏生成声明使用；替代现有 Simd<u8, LANES> 构造参数。
    pub const fn new(
        node_name: &'static str,
        variant: NodeVariant,
        function: fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize,
        frame_args_size: (u16, u16, u16),
    ) -> Self;
}
```

变体选择按 VPP 的“已编译候选 + 当前 CPU 支持 + 优先级”三个条件进行。
`None` 表示正常的 CPU 不支持；baseline 的优先级为 0，x86-64-v3/v4
沿用 VPP 的 45/95，不把选择优先级当作错误码或协议数值。
x86-64-v3 不能只探测 AVX2，还须覆盖 VPP `cpu.h:387-394` 的
AVX2/BMI1/BMI2/FMA/MOVBE/LZCNT/OSXSAVE，以及 Rust 候选额外启用的
F16C 与 OS 保存向量状态的前提；
v4 不能只探测 AVX-512F/BW，还须覆盖 DQ/VL/CD (`cpu.h:377-384`)。
运行时的支持判断必须覆盖候选实际启用的全部 CPU/OS features；
不支持的候选不能因为“SIMD 宽度够大”而被选中。预先用 `cfg` 排除
编译器/平台不能安全生成的候选，不能让它以 baseline 的机器码伪装为 v4。
在 x86 worker 上按绑核后的当前核执行 CPUID/XGETBV 能力检查，
不使用可能由其他核先初始化的进程级 CPU feature 缓存；未固定 CPU
亲和性的执行线程只能安装所有允许 CPU 都支持的 baseline。
Hammer baseline 的构建目标必须能在全部允许的 worker CPU 上执行；
VPP 的 x86-64-v2 默认编译目标是其构建选择，不能从 Hammer 的
`NodeVariant::Base` 名字推断出相同 `-march`。将整个 DSO 以仅主核支持的
`target-cpu=native` 构建，会使任何 per-worker 选择都无法提供安全回退。

```rust
// hammer-runtime::node；VPP vlib/node.c:259-292,347-368、main.c:833-917。
impl NodeMain {
    // 复用现有安装入口；在当前执行线程检测 CPU，最终只写本线程的
    // Node Runtime function slot。没有候选时保留 process。
    fn install_node_function<'a>(
        &self,
        node: NodeId,
        registrations: impl Iterator<Item = &'a NodeFunctionRegistration>,
        process: NodeProcessFn,
    ) -> RuntimeResult<()>;

    // Data Worker affinity 生效后以及每次 refork 后调用；不在热路径调用。
    // 从 GlobalMain 已发布的 registration image 借用候选，不复制插件函数表。
    fn select_node_functions<'a>(
        &self,
        registrations: impl Clone + Iterator<Item = &'a NodeFunctionRegistration>,
        cpu_pinned: bool,
    ) -> RuntimeResult<()>;
}

// VPP vlib/node.c:259-292；只看同名 Node 的已编译候选，比较
// priority_on_current_cpu() 的有效值，不支持的候选不参加。
// 相同 (node, variant) 的重复声明保留现有 typed duplicate 错误类别。
fn preferred_node_function<'a>(
    node_name: &str,
    allow_specialized: bool,
    registrations: impl Iterator<Item = &'a NodeFunctionRegistration>,
) -> RuntimeResult<Option<&'a NodeFunctionRegistration>>;
```

`select_node_functions` 只需现有 `NodeMain` 的 node name、原 `process`
及 `GlobalMain` 的注册清单；实现时在现有 `NodeRuntimeSlot` 内保存
`declared_process: NodeProcessFn`，使 `process` 始终只是当前选出的函数。
这是 worker-local 的一个字段，不是新注册表；不能在一个 worker 的已选
slot 上反推 baseline。每次选择前核对同一 Node 的所有候选具有一致的
Frame scalar/vector/aux layout；layout 不一致属于插件声明错误，
进入 packet dispatch 前断言并终止本次
加载，不能让某个变体用错误布局解释 Frame。不能给 `process` 加一次
运行时 CPU 分支；dispatch 仍只是调用固定的函数指针。
整批 Graph Node 注册前先用同一选择函数校验将要安装的候选，
使重复 `(node, variant)` 或 Frame layout 冲突在任何 Node 初始化前暴露；
插件扩图只预检尚未存在的 Node。预检不安装函数，也不按主线程的
CPU 能力代替 worker 的最终选择。

## 3. 宏、编译和现有 TCP 调用

继续只书写一个 typed Node body；宏复制 body，生成私有、互不冲突的
baseline/v3/v4 函数与声明。**TCP 业务代码不写 CPU/SIMD 宽度参数、
`_simd` 函数名或每变体的注册符号**。宏将同一个 Node 的全部静态声明
生成一个借用 slice；现有 `RegistrationImage::node_functions()` 展平
这些 slice 后仍向 `GlobalMain` 交付原来的注册项。group 只是 slice，
不增加包装 struct、函数表、`dyn` 或另一位注册 owner。这里须同步调整
`declare_plugin!`/`__declare_registration_image!` 的 `node_functions` 输入
与 `RegistrationImage` 的存储形状；现有插件的空列表不变。

```rust
// hammer-runtime::registration；VPP vlib/node.h:208-223：一 Node 多候选。
// 只改变现有 image 的 node_functions 字段及迭代，不增加第二张表。
struct RegistrationImage {
    node_functions: &'static [&'static [&'static NodeFunctionRegistration]],
    // 其余现有字段不变。
}

impl RegistrationImage {
    fn node_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static NodeFunctionRegistration> + '_;
}

// plugin/tcp；VPP vlib/node.h:208-223、vppinfra/cpu.h:35-47。
#[hammer_component_macros::node_function(node = Tcp4OutputNode)]
fn tcp4_output_node_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    tcp_output_node_process_frame::<true>(runtime, node_runtime, frame);
    frame.len()
}

// 宏生成一个 slice 项，plugin image 仅引用这一项；TCP6 同样处理。
// node_functions = [output::__TCP4_OUTPUT_NODE_FUNCTIONS, ...];
```

TCP output 的 `tcp_output_node_process_frame`、`tcp_output_next_for_index`、
`tcp_output_push_ipv4/ipv6`、`set_tcp_checksum` 与 reset 调用都删掉
`SIMD_BYTES` 参数和 `<1>`/`<SIMD_BYTES>` 实参；只保留 `IS_IP4` 这样的
协议维度 const 泛型。现有 `InternetChecksum<const SIMD_BYTES>` 导致
业务端显式选择机器宽度；同步将 infra 的 streaming checksum 收敛为
不带公开宽度参数的现有 `InternetChecksum` 类型，TCP/IP 使用同一接口。
infra 的 streaming `write` 采用不带公开宽度参数的可向量化分块算法，
并内联进宏生成的目标 ISA Node body；同一算法在普通 reset 路径按
baseline 编译，不作每包 CPU 探测。TCP output 到 checksum 的调用链
只在确实需要传播目标函数优化的短函数上使用 `#[inline(always)]`，
生成的 `#[target_feature]` Node body 本身不能标 `#[inline(always)]`。
这一选择必须用编译产物确认专用 Node 中的 checksum 真正采用了目标
ISA；若未内联或吞吐回退，实施不能以标量实现冒充完成。这个 infra
迁移只改 checksum 能力的实现/类型参数，不引入 TCP 专属 API、
TLS、额外全局 selector 或每包函数表分发。VPP `vnet/tcp/tcp_output.c:346-405`
同样由 TCP 准备伪首部事实、调用通用 checksum 能力，而非在 TCP API
中传一个 SIMD 宽度。

```rust
// hammer-infra::checksum；VPP tcp_output.c:346-405 对通用 ip_csum 的使用。
pub struct InternetChecksum {
    sum: u64,
    trailing_high: Option<u8>,
    // ISA 实现细节只留在 infra 内部。
}

impl Default for InternetChecksum {
    fn default() -> Self;
}

impl core::hash::Hasher for InternetChecksum {
    fn write(&mut self, bytes: &[u8]);
    fn finish(&self) -> u64;
}

// plugin/tcp；VPP tcp_output.c:346-405：参数只有协议/Buffer 事实。
pub(crate) fn tcp_output_push_ipv4(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv4Addr,
    dst: Ipv4Addr,
    total_len: u16,
    fib_index: u32,
) -> RuntimeResult<()>;
```

## 4. 通用 graph 函数：enqueue_to_next

VPP `buffer_funcs.c:10-173` 用 `enqueue_one` 的 bitmap、
`clib_mask_compare_u16` 与 `clib_compress_u32` 将同 next 的 Buffer index
直接追加到目的 Frame；容量不足时才暂存在一个 frame 大小的临时 index
数组，最多跨两个目的 Frame。`buffer_node.h:353-372` 的外层函数只是
取已选的普通 march 函数指针。Hammer `graph/fanout.rs:104-210` 已有
同名 public 方法与 bitmap 分组，不应新增一个业务层 `enqueue_next` API；
内部增设 baseline/v3/v4 函数候选，并将选出的普通函数保存在执行线程
自己的 `DataPlaneMain`。这不是 `NodeFunctionRegistration`，也不进入
插件 image 或 TCP。`enqueue_to_next_with_scalar` 是另一条带 scalar
的泛型帧接口，不假装是 VPP 的无 scalar 函数；它可复用 infra
的 mask/compact 能力，但本次不将其塞进无 scalar 函数指针签名。

```rust
// hammer-runtime::data_plane::main；VPP buffer_funcs.h:69-82、
// buffer_funcs.c:388-415。字段随 DataPlaneMain/worker 生命周期存在。
struct DataPlaneMain {
    enqueue_next: fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame, &[u16]),
    // 其余现有字段不变。
}

impl DataPlaneMain {
    // 保留现有公开签名；VPP buffer_node.h:353-372。
    #[inline(always)]
    pub fn enqueue_to_next<N: NodeNext>(
        &mut self,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
        nexts: &[N],
    );

    // 绑定 CPU 后、进入 worker-init/dispatch 前选择；未绑核 main 用 baseline。
    // VPP buffer_funcs.c:391-415；Hammer 以 worker-local slot 避免异构 CPU 越界。
    fn select_architecture_functions(&mut self, cpu_pinned: bool) -> RuntimeResult<()>;
}

// hammer-runtime::graph::fanout；VPP buffer_funcs.c:94-173。
fn enqueue_next_base(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
);

// x86 构建支持该目标 ISA 时才编译；安全入口只在已验证 CPU 上安装，
// 不向 node_function inventory 注册。VPP cpu.h:35-47、buffer_funcs.c:164-173。
#[cfg(target_arch = "x86_64")]
fn enqueue_next_x86_64_v3(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
) {
    // 函数指针安装前已核对当前 worker 的完整 CPU/OS 能力。
    unsafe { enqueue_next_x86_64_v3_body(runtime, node_runtime, frame, nexts) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,fma,f16c,lzcnt,movbe")]
unsafe fn enqueue_next_x86_64_v3_body(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
    nexts: &[u16],
);
```

同样生成 x86-64-v4 候选；是否可编译、能否被当前 CPU 执行以及优先级
复用第 2 节的判定，不能把 `hammer-infra::march_fn!` 当前的
进程级 `OnceLock` selector 用于这里：它只根据第一个调用线程选一次，
不能保证另一 Data Worker 的 CPU 支持。`DataPlaneMain` baseline 初始化
时填入 `enqueue_next_base`；worker affinity 设置成功后选本 worker 的
指针；图 refork 不重建该字段，CPU 迁移/重新绑定时必须重选。函数指针
只在每次 fanout 调用一次，不在每个 Buffer 上分发；字段 worker-owned，
不需要锁、原子、volatile 或额外同步。即使 Node function 采用 v4，
普通 fanout 函数仍独立选型，不把它错误地内联进每个业务 Node。
新增函数指针沿用 `DataPlaneMain` 的现有 cacheline 对齐；实施时检查
字段偏移，不为这一个指针新增 cacheline mark 或跨 worker 共享缓存。

真正的加速还依赖 infra。当前
`hammer-infra/src/mask_compare.rs:38-48` 的架构分支直接调用 scalar，
也没有 `clib_compress_u32` 对应的通用压缩能力。更新现有
`mask_compare_u16` 的架构内核，并在 infra 提供一个通用
`compress_u32`，由 fanout 在有足够空间时直接写目的 Frame，空间不足
时写临时 index 数组；这两者不含 Node/TCP 类型。两个函数的 `src`
和 `mask` 按有效 slice 长度处理，**不得**复制 VPP
`buffer_node.h:353-363` 在 `count` 之后仍可能读取的 C 指针契约。
Frame 内的 scalar/aux layout、Buffer 所有权、`put_next_frame` 顺序与
已存在的 `enqueue_to_next` 完全不变。仅复制 Buffer index，不复制包数据。

```rust
// hammer-infra::mask_compare；VPP buffer_funcs.c:34-45 的 generic 压缩事实。
// dst 至少容纳 mask 中置位的有效 src 元素，按 src 顺序写入；返回写入数。
pub fn compress_u32(dst: &mut [u32], src: &[u32], mask: &[u64]) -> usize;
```

VPP 的 `enqueue_to_next_with_aux`、`single_next`、thread handoff 也有
独立 march 函数 (`buffer_funcs.c:175-385`、`handoff.c:469-553`)；
本 ADR 不误称 Hammer 已实现这些全部入口。后续若扩展，逐一对应现有
Hammer 方法和数据布局，不用一个无类型万能函数表吞掉 scalar/aux。

Rust 的 `#[target_feature]` 是针对单个函数的 ISA 启用，不等于 VPP
CMake 对整个源文件分别传 `-march`/`-mtune`：被调用的普通函数未必随
Node body 重新编译。宏须把需要专门优化的代码保留在目标函数内，
业务短函数和 infra 的可向量化代码须被内联进目标函数；不能声称仅给
trampoline 加标记就得到完整 VPP 变体。`#[target_feature]` 标记加在专用
body 上；trampoline 只有在 CPU 资格检查后才调用；保留现有 Frame layout
校验与跨 DSO
panic abort。不能给 `#[target_feature]` body 标 `#[inline(always)]`；
小型不带特性的检测/优先级函数按 VPP 的 inline 意图使用 `#[inline]`，
Node dispatch 和一次性的 trampoline 不要求 `inline(always)`。

VPP AArch64 的 `octeontx2/thunderx2t99/cortexa72/neoverse*` 是带
`-mtune`/`-mcpu` 的 CPU 型号变体，不能把它们统称为 128-bit NEON。
本次设计的通用注册/选择机制可承载这些变体，但不把现有 `simd128`
重命名冒充 ARM 优化版。ARM 初期只产生 baseline；独立 per-function
目标 CPU 编译方案和 implementer/part 探测经验证后，再扩展 enum 与
编译候选。此限制在实施记录中保持显式，不声称已覆盖 VPP ARM 变体。

## 5. 初始化、同步和错误语义

初始化顺序：插件 image 先发布静态候选 -> main 构图时安装固定 CPU 可用
候选（未绑核则 baseline）-> 为 worker 克隆图 -> worker 绑定 CPU ->
worker 重选自己的 Node 和普通 fanout 函数 -> worker-init -> packet loop。
`NodeMain::refork` 后同一 worker 重选 Node 候选；普通 fanout 函数无需
因图 refork 重选。
main 不跑 dataplane 不等于可以给它安装当前 CPU 不支持的函数；
它仍可能执行普通图/Process 逻辑。候选静态只读，函数 slot 为
worker-owned 可变图状态；正常分发不需要锁、原子、volatile 或 fence。
控制面修改注册 image/图时继续使用现有 `WorkerBarrier` 与 refork；
不得另设 `OnceLock` 分发缓存、共享 `ArcSwap` 快照或 per-packet 探测。
这里的同步边界对应 VPP `node.c:874-901` 更新所有线程函数指针的意图，
但使用 Hammer 已有的主线程发布/worker refork 协议。

正常 CPU 不支持候选是选择条件，不是 `Error`。重复注册由注册/选择 owner
`hammer-runtime` 的 `RuntimeError::DuplicateNodeFunction { node, variant }`
表示；旧的 `hammer-core::DataPlaneError::DuplicateNodeFunction { simd_bytes }`
删除，core 不依赖 runtime 的变体身份；不新增数字 retval。
Frame layout 不一致、选出未支持 CPU 的函数、注册所指的插件
映像已卸载都是声明/生命周期不变量错误，必须在 dispatch 前拒绝或断言，
不能伪装成 packet error，也不能容忍到非法指令发生时才处理。
此 ADR 不增加每包 node error counter、不改变已有 packet error 语义。

## 6. 迁移与验证边界

实施时清理 `NodeFunctionRegistration::simd_bytes`、按宽度选最大值、
`DuplicateNodeFunction { simd_bytes }`、宏生成的 `simd128/256/512`
身份及 TCP image 的旧候选路径。TCP `output.rs`/`reset.rs` 的
`SIMD_BYTES` 泛型、`_simd` 名字与按宽度实例化 checksum 一起移除；
`hammer-infra::checksum::InternetChecksum` 不再向业务暴露宽度泛型。
保留 `runtime_simd` 中确实用于 Frame batch/Buffer 预取的宽度能力，
不把这些通用用途一并删除。
`NodeMain` 的普通 `process` fallback 与 plugin image 生命周期保持不变。

本 ADR 的公开面：新增通用 `NodeVariant`、用 variant
替换 `NodeFunctionRegistration::new` 的 SIMD token、把重复注册错误
移到 runtime 并携带 variant、将现有 `RegistrationImage::node_functions` 输入改为
宏生成的候选 slice、从 `InternetChecksum` 移除公开宽度参数，以及
在 infra 增加通用 `compress_u32`。`DataPlaneMain` 的 `enqueue_next`
只是私有 worker-local 函数字段，不向插件暴露新注册 API。
最终业务代码只有一个普通 TCP Node function 和一个普通 checksum 类型；
旧平铺候选清单/宽度泛型不再保留兼容入口。`select_node_functions` 是
runtime 内部方法，不向 plugin/service 增加 API。若要求 ARM 型号优化
或运行时手动切换，先单独确认编译与控制面接口，不通过本 ADR 顺手扩面。

实施后的验证矩阵（本次不运行）：

| 场景 | 验证条件 |
| --- | --- |
| 构建候选 | baseline 总存在；非 x86-64 目标没有 v3/v4 符号/注册；x86-64 编译器须支持声明的目标特性；每个 Node 在 image 中只占一个 slice 项。 |
| 选择 | mock CPU 能力依次选 Base/v3/v4；缺失候选时回退；重复 `(node, variant)` 返回具体错误；不同 Frame layout 不能发布。 |
| worker/refork | CPU affinity 生效后再选；异构 worker 可选不同候选；refork 后新旧 Node 均保留本 worker 的合法函数。 |
| graph fanout | baseline/v3/v4 的混合 next 分组、满 Frame/跨 Frame、partial tail 与现有结果相同；mask/compact 不读 slice 尾部；异构 worker 各用安全候选。 |
| 真正执行 | 同一输入分别强制 baseline 与受支持专用候选，结果、next、error counter 一致；不支持的候选从不执行。 |
| TCP/infra | TCP output/reset 无 `SIMD_BYTES`/`_simd`；两种 IP checksum 及跨 Buffer chain 的奇数字节与现有结果一致；专用 Node 的 checksum 汇编和吞吐不退回标量。 |
| 边界 | 插件卸载/加载、Graph Node fallback、Process Node 与已注册 Frame scalar 均不因候选增加而改变。 |

边界检查命令计划：`rg -n 'simd_bytes|SIMD128|SIMD256|SIMD512|NodeVariant|enqueue_next|compress_u32' crates/hammer-runtime crates/hammer-component-macros crates/hammer-infra crates/hammer-plugins/transport/tcp`。本次按任务要求不编译、不运行测试或 CI；因此 x86 专用候选是否正确生成、`inline(always)` 是否把 TCP checksum 纳入专用 Node 的机器码、跨 CPU 执行与吞吐均未验证，不能作为已证明的性能结论。
