# ADR-0015：Binary API Rust 定义与 Serde 编解码

状态：设计覆盖定义、codec、消息身份与表格式、API-init 和服务端分派。
按用户最新实施范围，#306 的代码只改 `hammer-ipc` 和它依赖的定义宏。
具体消息、Runtime、Service、协议插件及客户端后续重构，本次不改这些 crate，
不增加全局 `RefCell<ApiMain>`，不以这些 crate 的既有编译问题扩大任务。

任务：[GitHub #306](https://github.com/Kotodian/netsystem-rs/issues/306)。
Rust 类型及属性是唯一业务定义入口；不维护、生成或引入 `.api` 文件。
Vendored VPP 仅用于源码 review，不运行 vppapigen 或引入 generator 测试。

ADR-0017 修订消息表发布后的借用合同：API-init 发生在 ApiMain 安装后，范围/版本/注册表
使用主线程校验的 Cell/RefCell 与真实 Ref 借用；不从 OnceLock 伪造 &mut。本文旧的安装后
只读假设由其第 4/5 节取代。codec 本身仍不读取 TLS；唯一的 my_api_main selector 属于 ApiMain，
客户端 ID/请求/回调属于显式 Client，目标生成与迁移边界见 ADR-0017 第 8/9 节。

## 1. 设计结论与实施授权

具体消息拥有自己的字段和 Serde 实现，派生 `Api` 获得协议身份和 service。
具体 `Serializer` / `Deserializer` 直接借用输入输出切片；没有动态值树、
运行时 schema 解释器、trait object、TLS、锁或业务状态借用包装。

用户已要求按 VPP 实际错误分支定义行为、更新 issue、实现并 commit/push；
随后明确限定本次仅验证 IPC，撤回 Service、Runtime 和插件实施。
本文保留完整架构设计，明确区分本次 IPC 交付与后续接入，
旧稿中“仅定义层”“禁止全部测试”“任一 hook 错误终止启动”等描述由本文取代。

## 2. 源码依据

路径均相对于仓库根目录，结论只针对本仓库 vendored tree。

| 路径与符号 | 核对结果 |
| --- | --- |
| `third_party/vpp/src/tools/vppapigen/vppapigen.py:162`，`vla_is_last_check` | counted VLA、递归 VLA 和动态 string 必须在尾部；旧式 `[0]` 的 parser 行为另见错误清单 |
| 同文件 `:237`，`Using`；`:304`，`Define`；`:360`，`Enum` | alias CRC 输入为 `[]`；消息 CRC 使用原始 block；兼容 enum 尾项不参与 CRC |
| 同文件 `:1056`，判别 union 校验；`:1128`，`process`；`:1262`，`foldup_blocks` | discriminator/case 约束；file_crc 按原始声明顺序累计；消息 CRC 递归折叠依赖 |
| `third_party/vpp/src/tools/vppapigen/vppapigen_c.py:1241`，`NO_ENDIAN_CONVERSION` | `client_index` 不执行普通字段 endian 转换 |
| `third_party/vpp/src/vpp-api/python/vpp_papi/vpp_serializer.py:84`；`third_party/vpp/src/vppinfra/byte_order.h:143` | 整数大端，bool 非零为 true，f64 本机字节序 |
| `third_party/vpp/src/vlibapi/api_types.h:23`；`api_shared.c:999` | API string 为 u32 大端长度加原始 bytes，无强制 NUL 或 UTF-8 |
| `third_party/vpp/src/vlibapi/api_common.h:391`；`api_shared.c:715` | 消息记录直接按 ID 索引；只在越界时查找返回空；注册扩展默认记录 |
| `third_party/vpp/src/vlibapi/api_shared.c:883`、`:927`、`:955` | 范围分配、重复 name_crc 和查找缺失分别采用不同处理 |
| `third_party/vpp/src/vlibmemory/socket_api.c:486` | Socket 表使用 u16 ID + 64-byte name；导出复制最多 63 bytes |
| `third_party/vpp/src/vppinfra/serialize.c:132`；`serialize.h:186` | 共享内存表的 cstring 是紧凑长度加 bytes，wire 不含尾 NUL；ID 用专用紧凑整数 |
| `third_party/vpp/src/vlibmemory/vl_memory_msg_enum.h:18`；`memclnt.api` 声明顺序 | 固定 bootstrap ID 从 1 起，包含 autoreply 展开；Socket create 为 15/16，control ping 为 23/24，保留到 28 |
| `third_party/vpp/src/vlib/init.c:126`、`:306`；`third_party/vpp/src/vlibmemory/memclnt_api.c:326` | 缺失排序依赖警告跳过；调用前标记 call_once；hook 错误结束当前链，API Process 报告后继续 |

## 3. 层隔离契约

| Owner | 可调用、持有的内容 | 不可承担的职责 |
| --- | --- | --- |
| `hammer-component-macros::api` | Rust 声明解析、静态定义和 service 展开 | 不扫描兄弟源码，不分配运行时 ID，不实现客户端调用 |
| `hammer_ipc::binary_api::definition` | Api、Typedef、Block、Field、Service；常量 CRC 与集合校验 | 不解释消息 payload，不持有插件状态 |
| `hammer_ipc::binary_api::codec` | 借用 bytes、Serde trait、Array seed、原始字节 String | 不访问 registry、Socket、Session、WorkerBarrier 或领域操作 |
| `hammer_ipc::binary_api::api` | ApiMain 的 ID 范围、直接索引记录、两种名称索引、版本 | 不保存第二套 API-init inventory/called 集合，不拥有 Socket 生命周期 |
| `hammer_ipc::binary_api::table` | Socket 条目和共享内存表格式 | 不声明具体请求/回复，不创建 client registration，不执行全局 hookup |
| 后续 Runtime/Service/插件 owner | 生命周期 dispatcher、主线程分派、具体领域操作 | 不向通用 Runtime 塞入 ApiMain 或插件专属状态 |

本次边界检查：`cargo check -p hammer-ipc --all-targets`、
`cargo clippy -p hammer-ipc -p hammer-component-macros --all-targets --no-deps`、
`cargo fmt --all -- --check`、`git diff --check`。

## 4. 新增接口及必要性

这些接口在已授权的 IPC 实施范围内；Serde 本身不提供协议身份、前置 count
或消息注册表，现有 protobuf envelope 也不能替代这些功能。
不因接入全局状态再添加借用包装或新的同步 API。

| 接口 | Owner / 消费者 | 必要性 |
| --- | --- | --- |
| `Api: Serialize + for<'de> Deserialize<'de>` | definition / 消息声明、安装入口 | NAME、BLOCK、CRC、NAME_CRC、SERVICE、OPTIONS、FLAGS |
| `Typedef { NAME, BLOCK }` | definition / 嵌套领域类型与 derive | 跨 crate 读取静态协议定义，不让 proc macro 猜外部源码 |
| `Block::{Fields, Enum, Alias}`、`Field` | definition / 宏及手写 wire 类型 | 保留原始 CRC 输入和依赖；不是 Rust 内存布局 |
| `Service` | definition / 声明集合 | caller、reply、stream、stream_message、events；不增加第二套路由表 |
| `file_crc(&[Block]) -> u32` | definition / 模块 owner | 单个 derive 无法获知模块原始声明全集及顺序 |
| `validate_services(&[Option<Service>])` | definition / 模块 owner 的 const 校验 | 检查全集的 caller/reply 冲突 |
| `Serializer::new(&mut [u8])`、`Deserializer::new(&[u8])` | codec / 具体 Serde | 直接借用消息存储；`finish(self) -> usize` 返回已消费长度 |
| `serialize(&T, &mut [u8]) -> Result<usize, Error>`、`deserialize::<T>(&[u8]) -> Result<T, Error>` | codec / 消息边界 | 单次编解码入口 |
| `Array::<Vec<T>>::new(usize)`、`Array::<[T; N]>::new()` | codec / 含数组的 Deserialize | 通过 DeserializeSeed 提供已确定的元素数量 |
| `codec::String` | codec / 动态原始字节字段 | Rust UTF-8 String 无法表达 C API string 的所有合法字节 |
| `ApiMain`、`ApiMsgConfig`、`ApiMsgData`、`ApiMsgRange`、`ApiVersion` | api / 显式模块安装及未来分派 | 保存消息身份和执行策略；普通 `&self`/`&mut self` 访问 |
| `MessageTableEntry` | table / 消息表编解码 | 通用表条目，不包含具体请求、回复或 handler |

## 5. 错误契约

逐条错误清单见文末。只把 VPP 真实可失败的注册/查找结果映射到
`api::Error::{MessageRangeExists, MessageCountInvalid, MessageNameCrcMissing}`。
调用方分别处理重复范围、修正数量、确认目标身份是否安装；它们不是 wire retval。
`get_msg_ids` 的参数为 u16，负数无法进入；大于 1024 被拒绝，0 仍允许。

Serde trait 所需错误直接复用 `serde::de::value::Error`，通过 `codec::Error`
重导出。切片不足、不支持的 Serde 操作、数组数量不符等是本地库诊断，
不新增一套 Binary API 协议错误枚举，不从 `custom` 字符串恢复领域分类。
普通分配沿用进程分配 owner 的契约。

宏的坏声明通过 syn 编译诊断或编译期常量断言拒绝；不生成占位 CRC 或运行期 panic。
局部程序不变量可以断言。不得把解码失败、传输失败或 panic 变成 retval=0。

## 6. 字段 wire 契约

| 内容 | 编解码 |
| --- | --- |
| u8/u16/u32/u64、i8/i16/i32/i64 | 固定宽度大端，无 padding |
| `client_index: u32` | opaque 本机字节序；具体 Serde struct 使用字段名 `client_index` |
| context | 按声明整数正常编解码；解码再编码保持其 wire bytes，不额外分配 context |
| bool | 编码 0/1，读取任意非零为 true |
| f64 | 本机字节序，保持 VPP 行为及其跨大小端限制 |
| struct/tuple/newtype | 按声明顺序连接字段，不写名称、字段数或 tag |
| 固定数组 `[T; N]` | 恰好 N 个元素，无长度头；大数组可用 Array seed |
| 前置 count 数组 | count 在原有位置写一次，数组本身不再写长度 |
| 旧式尾数组 `[0]` | 具体 owner 确定固定元素大小，`deserialize_legacy` 按剩余 bytes 推导，检查整除及非零大小 |
| 定长 string | 全部 N bytes；不添加长度、不截掉 NUL 后的区域 |
| 动态 string | u32 大端字节长度 + 原始 bytes；允许非 UTF-8、嵌入 NUL，空值仍占 4 bytes |
| enum/enumflag | owner 用数值 Serde 保留声明整数宽度、未知值与未知 flag bits |
| 固定 union | owner 保留最大成员大小的原始存储；外层判别值选择转换分支，不增加内部 tag |

判别 union、开放数值枚举、前置 count 数组由具体类型显式实现 Serde。
`Api` derive 不重复生成 Serde，也不声称普通 Serde enum 的 variant index
就是协议数值。未知判别值保持原始 union bytes，不猜成员；首版拒绝 VLA union。
`discriminator/case` 的具体关系属于 owner 的 wire 实现，本版不提供通用属性生成器。

`f32`、i128/u128、char、Option、map、默认 tagged enum、deserialize_any、
自动忽略未知字段没有本格式布局，调用这些 Serde 方法返回本地诊断。
`is_human_readable` 为 false。

## 7. 数组、字符串与进度

SeqAccess 保存并恢复父结构剩余数量及字段名，包括子 visitor 返回错误时。
数组 count 不用于未经验证的大容量预分配；成功解码元素逐个进入最终 Vec。
数组遍历受剩余消息字节预算约束，零 wire 大小元素不属于准入范围。
固定数组解码保留普通 `[T; N]`，不引入 FixedList。

前置 count 可以与数组间隔其他字段。领域类型拥有私有 Vec，有界构造/替换
先检查数量能否写进声明整数，再修改对象；对外提供切片，count 从长度推导。
后续 IpRouteV2 的 n_paths/src/paths 及 Socket 回复表应遵守这一规则，
具体消息与 IP 插件本次不实施。

read/write 先检查本次访问的边界再更新位置。整个失败消息的输出不可发布，
但调用方的 scratch bytes 不保证回滚；输入进度也不做事务回滚。
通用解码不拒绝额外尾字节，`finish` 可以读取实际消费长度，
对应 VPP 分派的 `calc_size <= msg_len`。

## 8. 定义元数据与实际 Serde

`Block` 描述协议原始字段，包括 host 未单独保存的 count，不包含自动加入的消息 ID。
普通 Rust type alias 继承目标身份；独立协议 alias 可使用 `#[api(alias)]`
单字段 tuple struct。alias 的 CRC block 是空列表，不能按目标展开。

标准 Serde derive 适用于固定标量/嵌套结构。`api(length = "count")`、
`api(string)`、`api(legacy)` 只描述协议定义，不使标准 Serde 自动获得相应布局。
复杂类型应同时手写对应 Serde，必要时手写 Typedef/Api 的静态 block。
会更改布局的 Serde 属性不在 derive 准入范围内。

Rust 字段重命名与协议名称重命名分离。wire 上无字段名，但
`client_index` 的 opaque 例外要求具体 Serde 仍使用协议字段名。
manual_endian 也必须由 owner 明确提供正确 Serde，不能解释成默认小端或全部原样复制。

## 9. CRC 与模块输入

消息 CRC 从原始 block 的 Python repr 等价规范字节计算，随后按字段顺序
递归折叠被引用 typedef 的原始 block。不 hash Rust token，不折叠已完成的 CRC 数字。
enum 的兼容尾项排除在 CRC 外；alias 保持 `[]`。
NAME 默认为 snake_case，可显式指定；NAME_CRC 为 `name_` 加八位小写十六进制。

模块 owner 显式提交原始声明顺序的 `&[Block]` 给 file_crc。
包括该模块原始 typedef、enum、union、alias 和消息；不加入因 autoreply
生成的回复，不自行重复加入 import 的声明。服务关系、options、flags 不混入 block。
模块完整 Service 集合在 const 上调用 validate_services。

file_crc、单条消息 CRC、模块内编号、运行时 base+编号分别是不同事实。
模块安装使用 `get_msg_ids("module_<file_crc>", count)`，然后逐条安装消息、
NAME_CRC、版本和执行策略；derive 不分配运行时 ID。

## 10. ApiMain、表格式与 bootstrap

`ApiMain` 自己拥有 `Vec<ApiMsgData>`，直接按消息 ID 索引，扩展时补默认记录。
不使用 `Vec<Option<ApiMsgData>>`。记录内 name/handler 可以为空，对应 VPP 的空指针字段。
`get_msg_data` 只在 ID 越界时返回 None；范围内空记录仍是记录。
裸 name 索引与 NAME_CRC 索引独立，身份查找失败不退回裸 name。

范围重复/数量非法不修改分配状态；计数器按 VPP 的 u16 加法显式 wrapping，
不制造“ID 耗尽”协议错误。注册零 ID 警告并跳过；不同 handler 重注册警告后覆盖；
重复 NAME_CRC 警告并保留旧值。trace、replay、MP-safe 分别安装。

Socket 表条目为 `u16 index + [u8;64] name`。导出最多复制 63 bytes，
剩余区域为零；这忠实保留该 VPP 导出点的截断行为，没有 NameTooLong retval。
截断键不可以用于裸名称 fallback。具体 Socket 回复的 counted table 留给其 owner。

共享内存表独立编码：u32 大端 count，随后每项为紧凑 unsigned ID 和
紧凑字符串字节长度、字符串 bytes。wire 没有 NUL；C decoder 添加本地 NUL。
紧凑整数按低标志位选择 1/2/4 bytes，剩余值使用 marker=0 加 little-endian u64。
不能复用 Socket 条目布局。

固定 bootstrap 消息与动态模块安装属于后续协议 owner。IPC 不预置
control ping、Socket create/reply、固定 ID 常量、安装函数或具体 handler。
后续安装方决定 ApiMain 的首个动态 ID，再通过普通借用安装消息；
不能拿少量消息的 file_crc 冒充整个 memclnt 模块 CRC。

## 11. Service、autoreply 与生成选项

| 声明 | SERVICE |
| --- | --- |
| `returns = Reply` | 普通独立 reply |
| `returns = null` | Some(Service)，reply=None |
| `stream = Details` | 传统 stream：reply=details，stream_message=None |
| `returns = Reply, stream = Details` | 独立终结 reply + details |
| `returns = Reply, events(Event)` | 普通 reply 与相关事件列表 |
| 不声明关系 | SERVICE=None，与显式 null 不同 |

autoreply 显式指定生成的 Rust 类型名，在同模块生成同可见性独立回复，
字段为 id:u16、context:u32、retval:i32。回复有自己的身份，SERVICE=None。
复制请求已知 options，flags 不整体继承；允许请求不含 context，允许 autoreply+stream。
宏不执行业务，不构造成功结果，不生成 reply 方法、订阅或客户端等待逻辑。

returns 与 autoreply 互斥；null 不带 stream/events；events 非空且要求 reply，
不能与 stream 组合。目标路径由 Rust 类型系统要求 Api 能力；相同协议身份的
自回复在编译期拒绝。跨声明 caller/reply 冲突由完整集合校验。

| 选项 | 映射 |
| --- | --- |
| dont_trace | ApiMsgConfig::new 设置 traced=false |
| manual_print | 保存 flag；本实现本就不生成打印器 |
| manual_endian | 保存 flag，由具体 owner 的 Serde 确定转换 |
| autoendian | codec 已转换到 host；后续 dispatch 不再重复原地 swap |
| version/deprecated/status/vat_help | 保存已知元数据；owner 显式安装 ApiVersion |
| 字段 default | owner 的 Rust 构造/Default；不为截断输入补字段 |
| 字段 limit | owner 根据该字段真实协议实施；不解释为通用数值上限 |
| import | Rust 类型路径与 crate 依赖；不扫描或导入其他模块的 handlers |

本版没有另加 default/limit 的通用宏属性，也没有 JSON/打印/trace-replay 子系统。

## 12. API-init 与服务端分派：后续重构契约

以下是完整设计中的接入要求，不是本次 Runtime/Service 的已交付代码。
复用 InitFunction、RegistrationImage、GlobalMain 的 API-init inventory 与
callback identity/called bitmap。ADR-0017 第 4.1 节明确复用现有 #[init_function] 生成声明，
由所属 RegistrationImage.api_init_functions 选择 API-init 阶段，无须另加同义宏。
runs_before/runs_after 由公共机制处理；ApiMain 不保留第二份进度。

主线程 API Process 在底层 API 初始化成功、处理请求之前执行 API-init。
hook 在 owner 中借用 ApiMain 安装消息和策略；不能通过 Runtime registry
或新增全局 `RefCell<ApiMain>` 绕开最终所有权设计。本次只提供 ApiMain 的普通借用接口，不实施具体模块安装函数、全局借用链或启动调用方。

下面是 VPP 对齐目标；当前 runtime 遇缺失约束返回错误，ADR-0017 的 Process 用 ? 传播
API-init 错误停止启动，差异与实际接线见其第 4.1 节，不是下述行为已经实现。
缺失约束警告跳过；循环返回排序错误。调用前标记 call_once，hook 返回错误
停止当前链且保留标记。底层 socket/API 初始化失败退出 API Process；
API-init hook 错误报告后继续，不能合并成“一律终止整个启动”。

分派按消息 ID 找记录；未知 ID 或空 handler 警告丢弃。具体入口先完整解码
包含 ID 的消息，再执行业务；不得先切掉头部又按完整结构解码。
非 MP-safe 操作使用已有 WorkerBarrier，MP-safe 操作在串行主线程执行。
PluginMain 保持声明和函数代码映像寿命；跨 DSO ABI/unwind 边界随接入审查。
本次不修改旧 protobuf envelope、运行中的服务端分派或客户端。

## 13. 变更与删除清单

| Owner | 本次改动 |
| --- | --- |
| hammer-component-macros | Api/Typedef derive、api 属性、依赖重命名解析、autoreply、常量校验 |
| hammer-ipc | definition、codec、api、table；窄重导出；一个简单 API Serde 测试 |
| docs/adr | 完整设计、实际错误清单、实现范围和源码 review |
| Runtime/Service/IP 插件 | 不改；此前越界工作已撤回 |
| vppapigen、`.api` fixtures、Python oracle | 不引入 |

## 14. Review 与验证

源码 review 已核对类型布局、CRC 输入与递归折叠、直接 Vec 记录、
注册时覆盖/保留行为，以及两种消息表编码。
有意差异包括 Rust 显式声明和 service 关系、Serde 本地诊断、切片边界检查、
旧式尾数组/union 的首版准入限制；这些不是新增 VPP 协议错误。

只新增 `crates/hammer-ipc/tests/api_serde.rs`：声明一个简单 InterfaceState API，
序列化为确定的 7 bytes，再反序列化还原。没有 generator、compile-fail 或大型测试矩阵。
测试只在最终提交前运行，通过后立即提交。不运行 Runtime/Service/插件测试或本地 daemon/TUN。

本次验收只覆盖 IPC 库及其宏。未验证跨 DSO、全局 API-init、真实 Socket
注册和服务端调用链；它们属于后续重构，不能用当前简单 Serde 测试宣称完成。
最终检查结果随提交记录到 issue。

## 15. VPP 实际错误清单

下列后续层面的源码事实保留为架构依据，不意味着本次修改对应层。

### 1. 定义阶段

源码：`third_party/vpp/src/tools/vppapigen/vppapigen.py`。

| 编号 | 触发条件 | VPP 实际处理 | 源码行 |
| --- | --- | --- | --- |
| D1 | 重复注册同名 typedef/alias/enum/union 类型 | `KeyError`，不覆盖已有类型 | 31—37 |
| D2 | 未定义的字段类型；非法语法/输入结束 | `ParseError` | 578—579、985—1010 |
| D3 | 非 string 数组使用无长度的 `[]` | `ValueError` | 466—480 |
| D4 | string 被声明成普通标量 | `ValueError`，string 必须为数组声明 | 496—504 |
| D5 | counted array 指定的长度字段此前没有声明 | `ParseError` | 945—951 |
| D6 | VLA、含 VLA 的嵌套类型或动态 string 不在末尾 | `ValueError` | 162—189 |
| D7 | backwards_compatible 枚举项后又出现普通枚举项 | `ValueError` | 374—389 |
| D8 | enumflag 单个声明值有超过一个置位 bit | `TypeError`；0 不被该条件拒绝 | 402—413 |
| D9 | 普通 service 语法中 caller 与 reply 相同 | `ParseError`；完整集合检查也拒绝作为 reply 的 caller | 691—701、1166—1170 |
| D10 | service 的 caller 不存在 | `ValueError` | 1155—1160 |
| D11 | service 的非 null reply 不存在 | `ValueError` | 1161—1165 |
| D12 | service caller 同时被其他 service 当作 reply | `ValueError` | 1166—1170 |
| D13 | service 引用的 event 不存在 | `ValueError` | 1171—1176 |
| D14 | discriminator 引用不是此前可见的同级字段 | `ValueError` | 1077—1083 |
| D15 | discriminator 字段不是 Enum/EnumFlag | `ValueError` | 1085—1094 |
| D16 | discriminator 属性标在非 union 类型字段上 | `ValueError` | 1095—1102 |
| D17 | 带 discriminator 的 union 成员缺少 case | `ValueError` | 1103—1110 |
| D18 | case 名称不在判别枚举中 | `ValueError` | 1111—1117 |
| D19 | union 内重复 case 名称 | `ValueError` | 1118—1123 |

VPP 定义入口特有、Rust 显式声明入口不照搬的诊断：

- 字段名是 Python 关键字：`Field` 抛 `ValueError`（507）；这是 Python API 名称限制。
- `typeonly define`：parser 抛 `ParseError`，要求 typedef（776—780）；Rust 不提供该旧语法。
- 隐式后缀配对缺失：`_reply` 没有 caller、`_dump` 没有 details、`_details` 没有 dump/get、get 未声明 stream service、普通消息没有 reply/service，分别抛 `ValueError`（1187、1195、1202、1207、1214）。ADR 使用显式 service 引用，不能再用后缀强迫所有独立消息配对。
- 旧式 `[0]` 数组：parser 仅 warning，随后仍创建 Array（939—951），不是定义错误。
- `process` 的显式服务引用循环检查 reply/events，没有对 `stream_message` 做同样的存在性检查（1155—1176）。
- autoreply 请求缺少 context：`autoreply_block` 没有此错误；它直接生成 context/retval 回复字段（343—348）。

### 2. 字段编解码

源码：`third_party/vpp/src/vpp-api/python/vpp_papi/vpp_serializer.py`。这是 vendored Python serializer 的本地异常，不是 C API 错误码。

| 编号 | 触发条件 | 实际处理 | 源码行 |
| --- | --- | --- | --- |
| C1 | primitive 值不能按指定格式/宽度 pack，或 unpack 输入不足 | 由 Python `struct.pack/unpack_from` 抛 `struct.error`；字段 pack 外层可能再保留 cause 包装 | 115—125、666—672 |
| C2 | 固定 string 配置的 limit 为零；输入字符数超过已设 limit-1 | `VPPSerializerValueError` | 141—158 |
| C3 | 固定 u8 数组输入长度超过 N | `VPPSerializerValueError`；较短输入允许补零 | 225—244 |
| C4 | 固定 u8 数组解码不足 N bytes | `VPPSerializerValueError` | 246—252 |
| C5 | 普通固定数组编码元素数不等于 N | `VPPSerializerValueError` | 270—276 |
| C6 | 非空 counted array 的元素数不等于指定 count | `VPPSerializerValueError`；空输入分支提前返回，没有同样检查 | 310—319 |
| C7 | 旧式尾数组剩余 bytes 不能整除元素大小 | `VPPSerializerValueError` | 374—380 |
| C8 | union/struct 字段引用未知类型 | `VPPSerializerValueError` | 473—475、593—595 |
| C9 | alias 引用未知类型，或 alias 的数组长度为零 | `ValueError` | 517—522 |
| C10 | 传给 struct pack 的非 dict 实参缺少要求的字段 | `VPPSerializerValueError`；dict 缺少字段另走默认值行为 | 651—663 |
| C11 | 具体字段 pack 抛异常 | 包装为 `VPPSerializerValueError` 并通过 `raise ... from e` 保留 cause | 666—672 |
| C12 | 指定 options 时捕获到 IndexError | 转为 `VPPSerializerValueError("Options not supported...")`；源码只捕获 IndexError | 71—81 |

Python 文本的 ASCII 编码/解码异常是该客户端表示的限制；C API string 是原始字节，不据此给 Hammer 原始字节 String 新增 UTF-8/ASCII 协议错误。

### 3. 注册、查找和分派

源码简称：S=`third_party/vpp/src/vlibapi/api_shared.c`；M=`third_party/vpp/src/vlibmemory/memclnt_api.c`；H=`third_party/vpp/src/vlibapi/api_common.h`。

| 编号 | 触发条件 | VPP 实际处理 | 源码 |
| --- | --- | --- | --- |
| R1 | 消息范围名称重复 | warning，返回 `(u16)~0` 即 `0xffff`；不分配新范围 | S:897—902 |
| R2 | 范围数量 `n < 0 || n > 1024` | warning，返回 `0xffff` | S:905—911 |
| R3 | 注册消息 id=0 | warning，直接返回，不安装该消息；函数返回 void | S:727—735 |
| R4 | 已有非空 handler 被不同 handler 再注册 | warning，然后继续覆盖执行信息；不是拒绝安装 | S:740—748 |
| R5 | 重复 name_crc | warning，直接返回，保留旧映射；函数返回 void | S:935—940 |
| R6 | 按 name_crc 查不到消息 | 返回 u32 `~0` 即 `0xffffffff`，不按裸 name 回退 | S:955—966 |
| R7 | 按 ID 查找越界 | 返回空指针 | H:391—395 |
| R8 | 分派时记录不存在或 handler 为空 | warning，不执行 handler；若 free_it 则仍释放消息 | S:489、569—574 |
| R9 | calc_size 大于实际消息长度 | warning，跳过 handler；按 free_it 释放消息 | S:521—544、573—574 |
| R10 | calc_size_func 为空 | ASSERT；断言未启用时 warning，calc_size 保持 0，后续可能继续执行；不是 recoverable 错误返回 | S:517—544 |
| R11 | get_first_msg_id 查询的模块范围不存在 | 回复 retval=`-7`（`VNET_API_ERROR_INVALID_VALUE`），first_msg_id=`0xffff` | M:62—86 |
| R12 | get_first_msg_id 请求的 client_index 无有效 registration | 直接返回，不发送上述错误回复 | M:65—67 |

R11 是具体请求的协议 retval；R1/R2/R6 是进程内函数的失败哨兵。不能把它们当作同一种错误码，也不能给 R3/R4/R5/R8 新造通用 retval。

### 4. API-init

源码：`third_party/vpp/src/vlib/init.c` 与 `third_party/vpp/src/vlibmemory/memclnt_api.c`。

| 编号 | 触发条件 | VPP 实际处理 | 源码 |
| --- | --- | --- | --- |
| I1 | init_order/runs_before/runs_after 指向不存在的函数 | warning，跳过该约束，继续排序 | init.c:126—132、166—179 |
| I2 | 内部约束字符串 comma_split 失败 | 返回 `clib_error_t*`，内容为 `comma_split failed!`；是排序实现的内部表示路径 | init.c:157—158 |
| I3 | 约束无法形成完整顺序（例如环） | 返回 `clib_error_t*`，内容为 `Failed to find a suitable init function order!` | init.c:217—219 |
| I4 | 公共排序失败 | dispatcher 原样返回错误，不进入 hook 链 | init.c:306—307 |
| I5 | hook 返回错误 | 调用标记已在进入前记录；原样返回，停止当前链，不清除标记 | init.c:320—334 |
| I6 | API Process 收到上述 API-init 错误 | `clib_error_report`，随后继续初始化和消息处理；不据此退出整个 API Process | memclnt_api.c:342—351 |
| I7 | vl_sock_api_init 返回错误 | 报告并退出当前 API Process | memclnt_api.c:326—330 |
| I8 | vlib_api_init 返回负值 | warning 并退出当前 API Process | memclnt_api.c:333—336 |

I2 不要求 Rust 引入相同的逗号字符串中间表示或对应新错误。I5 的具体错误属于失败 hook 的操作，不包装成新的万能 HookFailure。

### 5. 本次不凭空新增的错误

| 原草案涉及的事项 | 本次源码核对结果 |
| --- | --- |
| Serde Error::custom、UnsupportedOperation、ArrayLengthMissing、ArrayElementsRemaining | VPP 没有这些 Serde 接口对应的协议错误；属于 Rust 库调用契约，不能列成新增 VPP 错误家族 |
| OutputTooShort | Python serializer 构造动态 bytes，没有调用方固定输出切片对应的容量错误；Rust 的切片边界检查不能包装成新协议 retval |
| TrailingBytes | S:541 使用 `calc_size <= msg_len`，该分派路径不拒绝额外尾部 bytes；Python VPPType.unpack 返回消费长度，也没有在该方法中统一拒绝尾部 bytes |
| count=0、消息 ID 加法耗尽 | S:905 只检查 n<0 或 n>1024；分配加法没有独立耗尽错误分支。不能声称 VPP 有相应类别；Rust 数值安全不靠制造协议错误保证 |
| autoreply 请求缺少 context | 没有该定义错误 |
| 所有同名消息/裸 name 重复都统一拒绝 | 无此统一行为；类型重复、range 重复、handler 重复和 name_crc 重复处理各不相同 |
| Socket 消息表 name_crc 超过 63 bytes | `socket_api.c:520` 调用 strncpy_s 且丢弃返回值；该导出调用点没有对应错误回复分支，不能凭空加 NameTooLong retval |
| 库的分配失败必须变成独立 Binary API 错误 | 上述注册与表导出调用点使用 vec/消息分配，没有逐项返回的 Binary API 分配错误；沿用分配 owner 的真实契约 |
| 返回 null 的 service、没有 SERVICE、未知 enum 数值、原始字符串非 UTF-8 | 不能仅因这些状态新造错误；前两项是合法声明状态，后两项按具体字段协议处理 |

此表不取消 Rust 的内存安全检查，也不要求复制 Python/C 的缺陷；它限定的是错误分类和协议行为的证据。业务 retval 必须追踪实际选定的业务 handler，本 issue 不预造通用业务错误表。
