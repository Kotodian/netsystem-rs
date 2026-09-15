# ADR-0018: VPP API-Segment Configuration, VPE API Main, and Show-Version

Status: proposed
Date: 2026-09-15

## Context

The current binary API implementation has the shared-memory transport authority
(`ApiMain`) and protocol business behavior in the same module. That is enough for
`memclnt_create_v2` and `control_ping`, but it does not provide the VPP split
needed for the next API surface:

- VPP parses the `api-segment` early configuration before mapping the primary
  API segment.
- VPP keeps API registrations and VPE-wide API state in `vpe_api_main`, while
  `api_main` owns message tables, mappings, queues, and registration lifetime.
- `show_version` is a VPE API message. Its handler reads build metadata and
  sends a typed reply; it is not an operation on the shared-memory transport
  owner.

The implementation must remain V2-only. It must preserve the existing async
Process and FileMain scheduling model, the thread-local `my_api_main` selector,
and the direct bootstrap installation of `control_ping` before ordinary
API-init traversal.

## VPP evidence

The design is based on the vendored VPP sources:

| Concern | VPP source and observed contract |
| --- | --- |
| Early API-segment configuration | `third_party/vpp/src/vpp/api/api.c`, `api_segment_config`, registered with `VLIB_EARLY_CONFIG_FUNCTION("api-segment")`; it accepts `prefix`, numeric or named `uid`/`gid`, `baseva`, `global-size`, `global-pvt-heap-size`, `api-pvt-heap-size`, and `api-size`. |
| Mapping after configuration | `third_party/vpp/src/vlib/main.c` and `third_party/vpp/src/vlibmemory/memory_api.c`; `map_api_segment_init` calls `vl_mem_api_init(am->region_name)` after early configuration. |
| VPE state | `third_party/vpp/src/vlibapi/api_helper_macros.h`; `vpe_api_main_t` contains VPE registration pools/hashes, `link_state_process_up`, and convenience pointers to `vlib_main_t` and `vnet_main_t`. |
| VPE ownership | `third_party/vpp/src/vnet/interface_api.c`; the process-global `vpe_api_main` is defined by the VPE API owner, not by the generic memory transport. |
| Message registration | `third_party/vpp/src/vpp/api/api.c`; `vpe_api_hookup` includes generated VPE declarations, allocates the VPE message-id range, and marks `show_version` thread-safe. |
| Show-version request/reply | `third_party/vpp/src/vpp/api/vpe.api`; request has `client_index` and `context`; reply has `context`, `retval`, `program[32]`, `version[32]`, `build_date[32]`, and `build_directory[256]`. |
| Handler behavior | `third_party/vpp/src/vpp/api/api.c`, `vl_api_show_version_t_handler`; it obtains build metadata, fills a reply, and sends it through the registration selected by the request. |
| Client entry point | `third_party/vpp/src/vpp-api/vapi/vapi_c_test.c` and `vapi_cpp_test.cpp`; the client allocates `show_version`, sends it, and consumes `show_version_reply` through the normal VAPI request/callback path. |

### 已核对的 VPP 实际调用链

以下路径均相对于 `third_party/vpp/`，行号来自本次本地源码读取。

| 顺序 | 实际调用点 | 已核对行为 |
| --- | --- | --- |
| 1 | `src/vlib/unix/main.c:731` → `src/vpp/api/api.c:265,381` | early config 调用 `api_segment_config`；数值/名字解析后调用 `vl_set_*`，尚未映射。 |
| 2 | `src/vlib/main.c:1919` → `src/vpp/api/api.c:247` | **直接调用** `vpe_api_init`，不是本文件中的 `VLIB_INIT_FUNCTION` 声明。保存 vm、vnet main，初始化订阅 hash，设置 `/vpe-api`，启用 API Process。 |
| 3 | `src/vlibmemory/memclnt_api.c:492` | `vl_mem_api_enable_disable` 仅改变 Process node 的 polling/disabled 状态，**不做映射或消息表注册**。 |
| 4 | `src/vlib/main.c:1925` → `src/vlibmemory/memory_api.c:1157` | `vlibmemory_init` 使用 global base/size/PVT/uid/gid 初始化根区域。该函数前面还 unlink 旧 SHM 路径，这是独立的启动策略，不能当作通用 attach 行为。 |
| 5 | `src/vlib/main.c:1931` → `src/vlibmemory/memory_api.c:541,480` | `map_api_segment_init` → `vl_mem_api_init` → `vl_map_shmem`；成功后安装 memclnt handler/name/CRC，设置队列通知和 primary region。 |
| 6 | `src/vlib/main.c:1943` | 此后才调用 VPP 的普通 init 注册链。 |
| 7 | `src/vlibmemory/memclnt_api.c:308,326,333` | API Process 开始执行，先做 socket init，再调用包含 control_ping 的 `vlib_api_init`（定义在 :182）。 |
| 8 | `src/vlibmemory/memclnt_api.c:342` → `src/vpp/api/api.c:227,244` | 遍历 `api_init_function_registrations`（call-once），调用 `vpe_api_hookup`；setup 和 MP-safe 设置属于此阶段。 |
| 9 | `src/tools/vppapigen/vppapigen_c.py:1730` | 生成 `setup_message_id_table`：使用 `my_api_main`，申请 module CRC 对应的 ID 范围，注册 name/CRC 和各消息 handler；返回 base ID。 |

**Hammer 适配决定：**按用户要求，service 的 `vpe_api_init` 用普通
`InitFunction` 承载，不在 runtime 写死 service 的直接调用。必须用初始化
依赖保持 owner 初始化 → 根/API 区域就绪 → Process bootstrap → API-init
hookup → 请求处理的顺序。不能把这个适配描述成 VPP 原生注册方式。
`vpe_api_hookup` 则保留真正独立的 API-init 阶段。

These references establish semantics and ownership. They do not require a C
layout clone or a Hammer identifier containing `vpp`.

## Decision

### 1. Add an early API-segment configuration value

The configuration owner parses the `api-segment` block before API mapping. The
parsed value is a concrete configuration record, not a field bag on `ApiMain`:

```rust
#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiSegmentConfig {
    pub region_name: String,
    pub root_path: PathBuf,
    pub uid: u32,
    pub gid: u32,
    pub global_base_va: usize,
    pub global_size: usize,
    pub global_private_heap_size: usize,
    pub api_private_heap_size: usize,
    pub api_size: usize,
}

impl Default for ApiSegmentConfig {
    fn default() -> Self {
        Self {
            region_name: "/vpe-api".to_owned(),
            root_path: PathBuf::new(),
            // SAFETY: these identity queries have no caller preconditions.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            global_base_va: 0x1_3000_0000,
            global_size: 64 << 20,
            global_private_heap_size: hammer_infra::svm::region::SVM_PVT_HEAP_SIZE,
            api_private_heap_size: hammer_infra::svm::region::SVM_PVT_HEAP_SIZE,
            api_size: 16 << 20,
        }
    }
}
```

字段没有 `Option`。配置块或某个键缺省时使用 `Default`；显式输入覆盖对应
默认值，非法输入报错，不退回默认值。空 `root_path` 明确表示不加共享内存
名字前缀；UID/GID 默认为启动进程身份。名字解析和大小单位解析由字段的
反序列化逻辑完成，结果仍然是这里的具体数值类型，不引入中间身份类型。

默认根区域大小和 API 区域大小分别为 64 MiB、16 MiB，私有堆大小复用
`SVM_PVT_HEAP_SIZE`（128 KiB）。依据是 VPP 的
`src/svm/svm_common.h:26,92`、`src/vlibapi/memory_shared.c:585`，以及 Hammer
现有 `ApiMain::root_region_config` 和 `memory_shared.rs` 的映射配置。
默认基址沿用 Hammer 的 `ApiMain::global_base_va`，不另选地址。VPP 的
`api_uid/api_gid = -1` 表示不改变所有者；这里在配置阶段取进程身份作为
具体默认值，不保留该哨兵。映射前验证地址、大小、对齐和数值转换。

配置解析先把默认值和覆盖值应用于现有 `ApiMain::set_api_uid`、
`set_api_gid`、`set_global_base_va`、`set_global_size`、
`set_global_pvt_heap_size`、`set_api_pvt_heap_size`、`set_api_size` 和
`set_api_region_name`；这些操作需要安装前的真实 `&mut ApiMain`。
`prefix` 的路径处理由 service 配置/启动代码完成，生成 root/API backing
路径。根区域复用 `SvmRegion`，API 区域复用已有接口：

```rust
// Existing hammer-ipc API; not a new wrapper.
pub unsafe fn map_shared_region(
    &self,
    root: &mut SvmRegion,
    path: &Path,
    is_server: bool,
) -> Result<(), MapError>;
```

删除草案中的 `map_primary_segment` 和 `ApiSegmentError`：目前没有证据
表明复用上述 owner 方法和既有错误类型不能完成映射。mapping 调用必须
处于独占初始化阶段；失败阻止 Process 进入收发循环。不得把 VPP 先创建
共享区域的事实写成“任何后续初始化失败都从未发布共享内存”；失败后的
已创建区域由启动 owner 按实际拥有关系释放。

The accepted configuration keys are VPP-compatible: `prefix`, `uid`, `gid`,
`baseva`, `global-size`, `global-pvt-heap-size`, `api-pvt-heap-size`, and
`api-size`. Human-readable sizes support the existing `M` and `G` forms and
plain bytes. Named uid/gid resolution is performed during configuration, before
the mapping attempt; the mapping owner never performs name lookup.

### 2. Add a VPE API owner

The VPE API owner lives in `hammer-service`, alongside the VPE/business API
behavior. `hammer-ipc` provides protocol declarations, codec, message-table
primitives, shared-memory mapping, registrations, and queue operations. The
service owner does not own shared-memory mappings, client pools, queue
allocation, or underlying message-id table storage:

```rust
pub struct VpeApiMain {
    /// The process-lifetime DataPlaneMain used by VPE API code.
    data_plane_main: &'static DataPlaneMain,
    net_main: &'static crate::net::NetMain,
}
```

`VpeApiMain` is a `hammer-service` process-global owner for VPE API state. Its installation is explicit and call-once, matching
the existing main installation path. `data_plane_main` is the direct
`&'static DataPlaneMain` equivalent of VPP's `vlib_main_t *vlib_main`.
Both fields are required references; there is no `Option` or raw-pointer
selector. `net_main` uses the existing `hammer_service::net::NetMain`, defined
at `crates/hammer-service/src/net/mod.rs:95`, and its existing `global()` at :115.
The VPE init hook runs after `net_main_init` (:804) and obtains that reference
with `NetMain::global()?`. The DataPlaneMain static-lifetime requirement remains
an implementation prerequisite documented below; it is not already provided
by the current init callback signature.
Callers do not obtain VPE business behavior by calling `ApiMain::global()`.

初始化通过现有注册链进行。`VpeApiMain` 没有 `init`、`new`、`install`、
`register_messages` 或 `show_version` 方法，也没有由 daemon 手动调用的
`init_vpe_api` 编排函数。

两个 hook 都由 `#[init_function]` 生成 `InitFunction`，所属的
`RegistrationImage` 列表决定阶段。下面是目标 hook 声明签名，函数体职责
在表中定义；普通 init 的静态 main 引用来源仍须满足下述生命周期条件：

```rust
// crates/hammer-service/src/vpe_api.rs — ordinary initialization hook
#[hammer_component_macros::init_function(
    name = "vpe_api_init", runs_after = ["net_main_init"]
)]
fn vpe_api_init(main: &mut DataPlaneMain) -> RuntimeResult<()>;

// Same module — API initialization hook, NOT an ordinary init callback.
#[hammer_component_macros::init_function(name = "vpe_api_hookup")]
fn vpe_api_hookup() -> RuntimeResult<()> {
    vpe::setup_message_id_table(ApiMain::current())?;
    Ok(())
}
```

| Hook | RegistrationImage 归属 | 函数体职责 |
| --- | --- | --- |
| `vpe_api_init` | `init_functions` | 在 service 内初始化并发布 `VpeApiMain`，建立 DataPlaneMain 和现有 NetMain 引用；初始化实际订阅状态并决定 API Process 启用。映射是后续独立步骤，不能把 enable 当成 map。这里不安装 VPE 消息表。 |
| `vpe_api_hookup` | `api_init_functions` | 调用 API 宏生成的 `vpe::setup_message_id_table`，分配 VPE 消息范围、安装 handler/name/CRC 和 MP-safe 属性。这里不初始化 VpeApiMain、不映射区域。 |

在 `crates/hammer-service/src/lib.rs` 的现有 image 声明中追加：

```rust
// Entries added to the existing lists; retain all other existing entries.
init_functions = [vpe_api::__INIT_FN_VPE_API_INIT];
api_init_functions = [vpe_api::__INIT_FN_VPE_API_HOOKUP];
```

排序和 call-once 复用 `GlobalMain` 及 `runtime::init::dispatch_init`。
`#[init_function]` 本身不决定 API 阶段；不能把 hookup 同时放入两个列表，
也不能由普通 init 直接调用它。`control_ping` 的直接 bootstrap 仍在
`run_api_init(main)` 遍历之前。

VPP 对应关系是 `src/vpp/api/api.c:247` 的 `vpe_api_init` 负责 main 引用、
订阅表、区域名字和 memory API enable；`:225` 的 `vpe_api_hookup` 单独
调用生成的 `setup_message_id_table`，由 `VLIB_API_INIT_FUNCTION` 注册。
订阅表仅在有相应订阅业务时引入，show-version 不新增空订阅池。

现有 Hammer `api_message_table!` 只生成固定 ID 表，不能声称它已经具备
VPE 动态范围能力。后续在这个宏内补动态范围声明和对应的
`setup_message_id_table(&ApiMain) -> Result<u16, api::Error>` 生成能力；复用
现有 `ApiMain::get_msg_ids`、`msg_config` 和 name/CRC 注册。这里的 `?`
对应生成的动态范围分配结果，普通 init 不调用这个 setup，也不手写
`setup_vpe_message_id_table` 包装。

`DataPlaneMain` 引用字段保留用户要求的 `&'static DataPlaneMain`。现有
`InitFunction.func` 只有 `fn(&mut DataPlaneMain)`，不能由这个短借用直接
得到静态引用。实现必须先落实 runtime owner 的静态共享引用和后续可变
访问的兼容性；仅让内存地址固定并不满足 Rust 借用规则。禁止在 hook 中
用 `transmute` 或裸指针延长生命周期。本 ADR 不把不存在的静态 main
访问器伪装成既有 API；这一运行时依赖属于后续实现前需完成的设计项。

### 3. Add the V2 show-version API

The messages follow `vpe.api` semantics and use owned Rust values. The handler
has the required safe generic shape at the dispatch boundary:

```rust
// crates/hammer-service/src/vpe_api.rs
#[derive(Clone, Copy, Debug, Api)]
#[api(name = "show_version", returns = ShowVersionReply,
      handler = show_version_handler)]
pub struct ShowVersion {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Debug, Api)]
#[api(name = "show_version_reply")]
pub struct ShowVersionReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    #[api(string)]
    pub program: [u8; 32],
    #[api(string)]
    pub version: [u8; 32],
    #[api(string)]
    pub build_date: [u8; 32],
    #[api(string)]
    pub build_directory: [u8; 256],
}

// Concrete business callback signature (body described below).
fn show_version_handler(request: ShowVersion);
// Reuse binary_api/api.rs:391's existing safe generic handler<T: Api>(T).

```

The concrete implementation is the safe value handler in `hammer-service`,
`fn show_version_handler(request: ShowVersion)`. It performs the complete
operation in that function: it reads build constants (VPP uses
`vpe_api_get_*` from `src/vpp/app/version.c:137,144,151`), looks up the
registration through the existing `ApiMain`
operation, constructs the owned reply, allocates and encodes it, queues it, and
frees the allocation only when queue insertion did not commit. There is no
`VpeApiMain::show_version` indirection and no reply-building method on
`ApiMain`; `ApiMain` supplies only allocation, registration lookup, and queue
access.

The reply uses fixed string arrays from `vpe.api:73`, not the codec's
length-prefixed API String. The payload sizes including the message ID are
10 bytes for the request and 362 bytes for the reply. Field serialization
must preserve those fixed widths, including the 256-byte array; the type
sketch does not claim the required Serde implementations already exist.
The handler zero-initializes each array and copies at most capacity minus one
bytes, following VPP's `strncpy` calls in `api.c:103`. A missing registration is an ordinary
request rejection and does not allocate a reply.

The VPE range marks `show_version` as MP-safe, matching the VPP hookup. The
handler still uses the existing dispatch barrier rules; MP-safe means it may
run without entering the serialized barrier, not that it may mutate shared
registration state.

### 4. Client surface

The V2 memory client does not retain one field per message type. It retains its
request bookkeeping, context counter, message-id mappings, registration handle,
and queue ownership. `show_version` is an ordinary typed request:

```rust
impl MemoryClient {
    pub async fn show_version(
        &mut self,
    ) -> Result<ShowVersionReply, MemoryClientError>;
}
```

The method allocates the generated request, assigns a context, sends it through
the existing request queue, and resolves the matching reply. It does not install
handlers or scan clients. Message declarations are installed by the VPE API
owner during API-init, while `control_ping` remains the direct bootstrap
message installed before that traversal.

## Initialization contract

1. Early config 从 `ApiSegmentConfig::default()` 开始解析覆盖项，得到
   完整具体值；验证和身份解析失败即停止启动。配置先应用于尚未安装的
   `ApiMain`，再发布供后续 hook 使用，避免从 `ApiMain::current()` 伪造
   `&mut ApiMain`。复用现有配置安装路径。
2. 普通 init 遍历调用 `vpe_api_init`，初始化 service 的 `VpeApiMain`；
   mapping 路径消费完整配置，建立区域并安装 `memclnt` bootstrap。
   所需区域名字在映射前确定，不在映射后再修改。普通 init 不做 VPE
   消息表 hookup，映射失败阻止进入请求处理阶段。
3. 沿用 `binary_api_clnt` 的 Process factory 启动路径：先直接执行
   `control::setup_message_id_table(api)`，再执行 `run_api_init(main)`。
4. API-init 遍历 service image 的 `vpe_api_hookup`，由它调用宏生成的
   `vpe::setup_message_id_table(api)`。message range、handler、name/CRC
   和 MP-safe 属性在此安装；注册错误向上传播，阻止请求循环启动。
5. 完成全部 API-init 后才开始接收请求，并允许 create-v2 发布完整消息表。
   保留现有 Tokio async Process、FileMain 和生命周期调度。

The reverse lifecycle is explicit: stop accepting API work, drain or reject
requests, release registrations and mapped regions through their owners, then
tear down the VPE owner. Static destruction is not used for shared-memory
resources.

## Lifecycle and ownership

After initialization, a client sends `show_version`; Process dispatch decodes an
owned request, calls the safe handler, and queues the typed reply through the
registration's input queue.

`ApiMain` owns mapping, allocation, message tables, client registrations, and
queue operations. `VpeApiMain` owns VPE API state and the two main references.
Build metadata is read directly by the handler, as in VPP; it does not require
an owned `BuildMetadata` field or a business method on VpeApiMain. The
memory client owns request correlation and client-side message-id mappings.
None of these owners introduces `owner_thread`, a second async scheduler, a
per-message client field, or a generic callback/helper registry.

## Rejected designs

- Putting `api-segment` parsing inside `ApiMain::new`; this would make an
  unvalidated transport object own startup configuration and would run too late.
- Adding VPE registration pools or build metadata to `ApiMain`; this repeats
  the ownership error that motivated this ADR.
- Calling VPE message-table setup from ordinary init, or treating show-version
  as a bootstrap exception. The VPE hookup belongs in `api_init_functions`;
  `control_ping` remains the direct pre-traversal bootstrap required by ADR-0017.
- Reintroducing `install_handlers`, `scan_clients`, per-message client options,
  or a closure-mediated state accessor.
- Replacing the existing async Process with a synchronous poll loop or a new
  thread-local protocol state container.

## Validation plan

Implementation is a separate change. Its final gate must include:

- configuration tests for the entirely omitted block, individual omitted keys,
  explicit overrides, numeric and named uid/gid, sizes, base address, and
  rejection before mapping;
- a real shared-memory integration test that sends `show_version` and checks
  all reply fields and context correlation;
- a real RegistrationImage lifecycle test confirming ordinary init initializes
  VpeApiMain without installing show-version, `control_ping` is installed before
  API-init traversal, and that traversal installs show-version exactly once;
- compile-time checking of the safe `fn<T: Api>(T)` dispatch and concrete
  `show_version_handler(ShowVersion)` signature;
- the existing IPC/infra test suites and the repository's final pre-commit test
  gate.

## Consequences

The binary API gains the VPP boundary needed for more VPE messages without
turning the transport owner into a business owner. Startup configuration becomes
explicit and testable, the VPE API has a stable owner for registrations and
main references, and `show_version` exercises the complete V2 request/reply path.
Future VPE messages extend the `hammer-service` `VpeApiMain` or a narrower
VPE-owned service module rather than adding fields and handlers to `ApiMain` or
the client. `hammer-service` may depend on `hammer-ipc`; the reverse dependency
is forbidden.
