# ADR-0049: CLI 目录、Unix 连接与主线程 Process

Status: implemented; validation deferred by issue #370

## 1. 结论和 VPP 依据

ADR-0051 adds a narrow synchronous `fn(&mut DataPlaneMain, Args)` command
form for non-MP-safe main-aware commands. The async Args-only form below remains
the default for commands that do not borrow the main during execution.

`CliMain` 是独立、非泛型的命令目录。`static` 只发布这个具体目录，不发布
`CliMain<T>`。每条命令的 `Args` 从 path 后的 `&str` 解析为拥有型结构体；
handler 只接收 `Args`，返回实现 `Display` 的输出值，不接收连接、输出句柄
或解析游标。`#[cli_command]` 只指定 `path`、`args` 和 VPP 本来就有的
help/`mp_safe` 元数据。宏为每个 `Args`、返回值和 async handler 生成
单态化启动函数，目录存统一的注册记录。`UnixCliMain` 独立拥有 listener、
连接池和 process-node 回收列；每条连接经 thread-zero `DataPlaneMain`
启动一个可复用的 Process Node。`UnixCliMain::init/global` 发布一个独立
全局 Main，其可变字段仍是 `Option`、`Pool`、`Vec` 的主线程借用，不在
listener 上用 `OnceLock`，不在 files 上加 `Mutex`。CLI fd 全部由 thread 0 的 `AsyncFileMain` 注册，
`File` 的异步读写直接提交 io_uring 操作，不经过 `FileMain` 的就绪回调。

| 依据 | 直接影响 |
| --- | --- |
| `third_party/vpp/src/vlib/cli.h:55-128` | 命令 path、回调、help、`is_mp_safe` 与 CLI 目录分开；回调消费 path 后剩余的 `unformat_input_t`。 |
| `third_party/vpp/src/vlib/cli.c:555-625,690-755,1416-1478` | 命令匹配、barrier、per-process output 绑定和注册。 |
| `third_party/vpp/src/vlib/unix/cli.c:120-221,443-470,2519-2650` | Unix file 保存输入/待写输出；process 执行输入；无输出命令也发 NUL 结束。 |
| `third_party/vpp/src/vlib/unix/cli.c:2705-2910,3164-3205` | 每个连接一个可复用的 process node；VPP 用 `clib_file_t` 回调把读入数据交给该 process。Hammer 保留连接/process 关系，I/O 实现改用 io_uring。 |
| `third_party/vpp/src/vlib/unix/cli.c:429-465,1265-1358,2970-3009` | `unix_cli_new_session_t` 仅记录 Telnet 字符模式连接的 file index 与提示符兜底 deadline；它不是通用 CLI 连接记录。 |
| `third_party/io-uring/src/opcode.rs:618-658,895-990,1032-1107` | 本仓库已有 `Accept`、`Read`/`Write`、`Send`/`Recv` SQE；它们是 Hammer 的实现工具，不是 VPP CLI 的 API。 |
| `third_party/vpp/src/vlib/unix/cli.c:3887-3923` | `wait <sec>` 确实会挂起 process；Rust 对应实际 `.await`。 |
| `third_party/vpp/src/vpp/app/vppctl.c:190-260,434-485` | AF_UNIX 客户端及非交互 NUL 确认；`hammerctl` 属于 `netsystem-client`。 |

`hammer-component-macros` 生成命令声明；`hammer-runtime::cli` 拥有目录、
命令结果格式化及错误；
`hammer-runtime::unix_cli` 拥有 Unix 前端；`hammer-runtime::process` 调度
连接 Future，`AsyncFileMain` 独占 thread-zero io_uring、`File` 池与在途 I/O。
CLI 不放进 `hammer-ipc`，也不把
`UnixCliMain` 放入 `GlobalMain`。此 ADR 不实现 telnet、pager 或 VPP
`function_arg`（它是注册期 opaque，不等于本 ADR 每次解析的 `Args`）。
因此也不引入 `unix_cli_new_session_t` 对应类型或
`unix_cli_new_session_process`：VPP 仅在 `cli_line_mode == 0` 的 Telnet
字符模式分支创建它，用来在协商未完成时限期发送初始提示符
（`vlib/unix/cli.c:2970-3009`）；本 ADR 的 AF_UNIX 命令连接直接按行
读取，接入后的连接状态由 `UnixCliFile` 持有。若以后加入 Telnet 字符
模式，再连同协商状态与提示符期限一起设计，而不是把 deadline 塞入所有
CLI 连接。

## 2. path + Args：命令返回输出值

path 匹配选择最长完整命令名。其后的文本保持原样，交给这条命令的
`Args::from_str(&str)`；解析必须验证全部剩余文本，未知词返回具体错误。
`Args` 拥有 Future 需要的值；不引入额外解析游标。handler 返回的值
实现 `Display`，宏在 Future 完成后用 formatter 生成一个 `String`；
空字符串就是零字节输出，非交互连接仍发送 NUL 完成标记。只有宏生成的
入口知道具体 `Args`、Future 和输出值类型，`CliMain` 不需要泛型、
`dyn`、`Any`、`PhantomData` 或一个没有实际所有权的泛型命令记录。

```rust
// hammer-runtime::cli; VPP: vlib/cli.h:60-100, cli.c:555-625,1416-1478.
// The macro monomorphizes this entry for Args, the handler Future and its
// Display result. No command callback receives a connection output object.
pub type CliCommandFn = fn(
    &mut DataPlaneMain,
    &str,
) -> Result<tokio::task::JoinHandle<Result<String, CliError>>, CliError>;

#[derive(Clone, Copy)]
pub struct CliCommandRegistration {
    pub path: &'static str,
    pub short_help: &'static str,
    pub long_help: &'static str,
    pub mp_safe: bool,
    pub start: CliCommandFn,
}

pub struct CliMain {
    commands: Vec<CliCommandRegistration>,
    command_index_by_path: std::collections::HashMap<&'static str, usize>,
}

impl CliMain {
    pub fn init() -> Result<&'static Self, CliError>;
    #[inline]
    pub fn global() -> &'static Self;
    pub fn register(&mut self, command: CliCommandRegistration) -> Result<(), CliError>;
    pub fn dispatch(
        &self,
        runtime: &mut DataPlaneMain,
        line: &str,
    ) -> Result<tokio::task::JoinHandle<Result<String, CliError>>, CliError>;
}
```

命令声明由 component 宏放入现有 image-bound registration inventory；插件
先装载，`CliMain::init` 在主线程逐条调用 `register`，验证重复 path、建立
目录，之后一次发布。目录随已加载镜像存活，执行期只读，因此不需要
`RwLock`。运行时卸载/新增 CLI 命令不属于本 ADR；若后来要求，必须先设计
注册与 DSO 代码生命周期，而不是偷偷给目录加锁。VPP 的构造器注册与初始化
参见 `vlib/cli.h:130-150`、`vlib/cli.c:1892-1914`。

```rust
// hammer-component-macros generated usage; VPP: vpp/app/version.c:39-132.
struct VersionArgs;

impl std::str::FromStr for VersionArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::UnexpectedArgument {
                argument: input.trim().to_owned(),
            });
        }
        Ok(Self)
    }
}

#[cli_command(
    path = "show version",
    args = VersionArgs,
    short_help = "show version",
    mp_safe = true,
)]
async fn show_version(_: VersionArgs) -> Result<String, CliError> {
    Ok(format!("hammer v{}\n", env!("CARGO_PKG_VERSION")))
}
```

本期仅实现 VPP `show version` 的基本查询，不实现可选的 `verbose`、
`cmdline` 分支，也不为 CLI 新增 argv 读取 API。版本取自构建包元数据，
命令不接受额外参数。宏要求
`Args: FromStr<Err = CliError>`、返回值 `R: Display`；生成的 starter
同步解析 `Args`，`spawn_local` 具体 async handler，完成后用
`format!("{value}")` 生成文本。handler 不接收输出对象，也不跨
`.await` 持有 `&mut DataPlaneMain`；同步 main 借用只能在启动阶段结束。
`show version` 没有等待事件，不应加虚假的 `.await`。

```rust
// VPP: vlib/unix/cli.c:3887-3923. Empty input selects one second;
// an explicit value is positive, <= one day, and has millisecond precision.
struct WaitArgs { duration: std::time::Duration }

#[cli_command(path = "wait", args = WaitArgs, mp_safe = false)]
async fn wait(args: WaitArgs) -> Result<String, CliError> {
    tokio::time::sleep(args.duration).await;
    Ok(format!("waited {:.3} sec.\n", args.duration.as_secs_f64()))
}
```

`wait` 展示真正的异步挂起：别的 File 和 Process 在等待时继续运行。
`.await` 不能把 CPU 密集计算自动变成非阻塞；此类命令需其 owner 的
独立执行能力和 Main Heap 线程初始化，不在主线程忙算。

## 3. 独立 UnixCliMain 与 formatter 输出

runtime 的 `init_function` 在 thread 0 发布全局 `UnixCliMain`，
`main_loop_exit_function` 负责关闭 listener 和删除 socket 路径。它直接拥有 listener 的 File 索引、
`Pool<UnixCliFile>` 与 process-node 回收列；**实际 `File` 由
`AsyncFileMain` 的池持有**。各可变字段用 `RefCell` 和 `Cell` 表达主线程
独占访问；`unsafe impl Sync` 仅允许全局发布，公开变更入口核对 thread 0。
`RefMut` 不跨 `.await`、barrier、File I/O 或未知回调。
daemon 不持有 `UnixCliMain`，`DataPlaneMain` 也不保存 CLI 专有字段；
worker 不持有 CLI。VPP：
`vlib/unix/cli.c:120-221,443-470,2853-2910`。

```rust
// hammer-runtime::unix_cli; VPP: vlib/unix/cli.c:120-221,443-470,
// 2519-2650,2705-2910. File indices refer to AsyncFileMain's pool.
pub struct UnixCliMain {
    listener: RefCell<Option<u32>>,
    socket_path: RefCell<Option<PathBuf>>,
    files: RefCell<Pool<UnixCliFile>>,
    unused_process_nodes: RefCell<Vec<NodeId>>,
    next_process_number: Cell<u32>,
}

static UNIX_CLI_MAIN: OnceLock<UnixCliMain> = OnceLock::new();

pub struct UnixCliFile {
    file_index: u32,
    process_node: NodeId,
}

impl UnixCliMain {
    pub fn init();
    #[inline]
    pub fn global() -> &'static Self;
    pub fn listen(&self, runtime: &mut DataPlaneMain, path: &Path) -> Result<(), CliError>;
    pub async fn accept(&self, main: &Rc<RefCell<DataPlaneMain>>) -> RuntimeResult<()>;
    async fn add_file(
        &self,
        main: &Rc<RefCell<DataPlaneMain>>,
        files: &Rc<RefCell<AsyncFileMain>>,
        descriptor: OwnedFd,
    ) -> RuntimeResult<()>;
    async fn reap_finished(&self, main: &Rc<RefCell<DataPlaneMain>>) -> RuntimeResult<()>;
    pub fn close_listener(&self, files: &Rc<RefCell<AsyncFileMain>>) -> RuntimeResult<()>;
}

#[init_function(name = "unix_cli_init")]
fn init_unix_cli(_: &mut DataPlaneMain) -> RuntimeResult<()>;

#[main_loop_exit_function(name = "unix_cli_exit")]
fn exit_unix_cli(runtime: &mut DataPlaneMain) -> RuntimeResult<()>;
```

宏在命令结果返回时调用 `Display` formatter，产生完整文本。非交互
Process 只处理一条命令：读完后写出文本和 NUL、flush、关闭；空输出也
发送 NUL。命令决定自身的换行，框架不补换行。没有输出通知队列或
第二条命令，所以一个 File 可以先供 `BufReader` 借用，再供
`BufWriter` 借用；不需要复制 File 或 fd。VPP：
`vlib/unix/cli.c:555-590,2638-2648,2750-2788`。

`listen` 把 AF_UNIX listener 交给 `AsyncFileMain::add`，accept Future
等待 File 的 `accept`，一次 completion 交出一个新 fd；成功后
`UnixCliMain::add_file` 为该连接复用已结束的 Process Node，或在 thread 0
动态注册新的 `unix-cli-process-N` node；然后创建 CLI 池项，将 fd 注册
为同一个 `AsyncFileMain` 的 File，并调用
`DataPlaneMain::start_process(node, unix_cli_process(...))`。新 node 注册仍
走现有 WorkerBarrier/refork；复用 node 不重新注册。VPP 的
`unix_cli_file_add` 正是先选 node、关联 file、再调用 `vlib_start_process`
（`vlib/unix/cli.c:2853-2910`）。连接 Process 从 `BufReader<&File>` 取得命令，
不再在 `UnixCliFile` 中另设输入 Vec 或 read-ready 回调；EOF、读写错误
让该 Process 退出。Process 关闭它注册的 File；下一次 accept 前
`reap_finished` 确认已完成任务并移除 CLI 池项；
Process Future 结束且 runtime 已观察并移除其运行记录后，node 才设为
Disabled 并进入 `unused_process_nodes`，不能在旧 Future 尚可 poll 时复用。
AsyncFileMain 在 CQE 确认前保留 fd 与操作缓冲区，不提前释放。以上都是
异步 File 操作，不注册
`read_ready`、`write_ready`、`error_ready` 回调。VPP 的连接/process
关系见 `vlib/unix/cli.c:2705-2910`；io_uring 是 Hammer 的实现选择。

## 4. 每连接一个 Process Node，执行体直接 await

现有 `Process<S, F>` 已是 Future，`F::Output = RuntimeResult<()>`
（`crates/hammer-runtime/src/process.rs:22-65`）。它是 node 的执行值，
不是另一种 CLI 连接类型；不定义 `UnixCliProcess` 结构体或独立 `run()`。
现有 `start_processes` 只启动静态声明的 Process Nodes，不能给后续接受
的每条连接启动动态 node。`DataPlaneMain::start_process` 是其运行时的
泛型对应：验证 thread 0、Process kind 与 node 未运行，将 node 置为
Polling，登记现有 Process 运行记录并启动连接 Future。Future 完成后
runtime 观察结果、移除记录；只有这时才能将 node 置为 Disabled 并交给
`UnixCliMain` 回收。当前 `run_main_until` 会暂时取出
`process_runtime`，必须调整其所有权，让 main loop 运行期间仍能在
thread-zero `LocalSet` 上启动新 Process；不能只把现有启动方法改名。

```rust
// hammer-runtime::DataPlaneMain; VPP: vlib/unix/cli.c:2853-2910,
// vlib/main.c:1297-1303. Reuses the existing Process<S, F> record.
impl DataPlaneMain {
    pub fn start_process<F>(&mut self, node: NodeId, future: F) -> RuntimeResult<()>
    where
        F: Future<Output = RuntimeResult<()>> + 'static;
}

// hammer-runtime::unix_cli; VPP: vlib/unix/cli.c:2711-2745,2853-2910.
// One invocation is the execution body of one registered Process Node.
async fn unix_cli_process(
    main: std::rc::Weak<std::cell::RefCell<DataPlaneMain>>,
    files: std::rc::Rc<std::cell::RefCell<AsyncFileMain>>,
    node: NodeId,
    file: u32,
) -> RuntimeResult<()>;
```

`UnixCliMain::add_file` 是唯一的连接启动点：从回收列取得 node 或动态
注册，建好 File 和 CLI 池项后构造 `unix_cli_process` Future，再调用
`runtime.start_process(node, future)`。动态名称随 graph node 存活，
不为每次复用重新分配。启动失败由 `add_file` 清理本次创建的连接资源，
不留下占用中的 CLI file；File 注册失败时，已取得的 node 也归还回收列。
连接 Future 异常结束而未关闭 File 时，`reap_finished` 在回收 node 前
移除该 File。

每连接的 Process Future 依次等待一条命令、命令 Future 和响应写入；io_uring CQE
唤醒相应 Future，**不把 completion 再转成 CLI read-ready 事件**。
它用 `AsyncBufReadExt::read_until(b'\n', ...)` 取得完整命令，短借 main
调用 `CliMain::dispatch`，释放借用后等待 handler 返回文本，再由连接
Process 写出文本或错误及完成标记并退出。等待 handler 时其他连接仍
可运行；本期不预读第二条命令，也不在等待 handler 时检测本连接 EOF。
**不增加 `ProcessYield`、CLI completion queue 或第二个 scheduler**。

```rust
// hammer-runtime::unix_cli; VPP: vlib/unix/cli.c:2519-2650,2705-2745,
// 2792-2827. CQE wakes this Process Future instead of signaling READ_READY.
// The registered File owns the fd. Its Rc keeps it alive after the short
// AsyncFileMain borrow ends; input and output borrow it sequentially.
let registered = files.borrow().file(file).expect("live CLI File");
let mut input = tokio::io::BufReader::new(registered.as_ref());

// No AsyncFileMain or DataPlaneMain borrow survives the await.
// read_until is cancellation-safe, unlike read_line.
let mut line = Vec::new();
let n_read = input.read_until(b'\n', &mut line).await
    .map_err(|source| RuntimeError::FileRead { source })?;
if n_read == 0 {
    return Ok(());
}
drop(input);

// Command parse/handler errors are rendered on this connection. Only task
// failure is a Process failure; handler code never receives an output object.
let command = match std::str::from_utf8(&line) {
    Ok(text) => {
        let text = text.trim_end_matches(&['\r', '\n'][..]);
        let main = main.upgrade().ok_or(RuntimeError::ServiceClosed)?;
        let mut runtime = main.borrow_mut();
        CliMain::global().dispatch(&mut runtime, text)
    }
    Err(source) => Err(CliError::InputEncoding { source }),
};
let text = match command {
    Ok(task) => match task.await {
        Ok(Ok(text)) => text,
        Ok(Err(error)) => format!("{error}\n"),
        Err(source) => return Err(RuntimeError::ProcessTaskJoin {
            node,
            source,
        }),
    },
    Err(error) => format!("{error}\n"),
};
let mut output = tokio::io::BufWriter::new(registered.as_ref());
output.write_all(text.as_bytes()).await
    .map_err(|source| RuntimeError::FileWrite { source })?;
output.write_all(&[0]).await
    .map_err(|source| RuntimeError::FileWrite { source })?;
output.flush().await
    .map_err(|source| RuntimeError::FileWrite { source })?;
```

`n_read == 0` 代表请求前 EOF，直接结束连接。`BufReader` 持有未成行
输入，`line` 只保存当前命令；去掉 LF/CRLF 后校验 UTF-8，再以 `&str`
交给 `Args::from_str`。一次响应写入包含命令文本及 NUL，flush 后
Process 退出。读写和 handler 错误的连接清理由 Process 完成；
AsyncFileMain 在 CQE 确认前保留在途 buffer 和 File。没有常驻输入、
输出队列、`Notify` 或同一连接的多命令调度。

`Process` 使用 thread-zero Tokio
`current_thread` executor；为了让 Future 可以安全持有 `Weak<RefCell<...>>`，
runtime 在这个 executor 的 `LocalSet` 上启动 node 的本地任务，去掉当前
`register_process` 无必要的 `Send` 约束。thread-zero main loop 的
`DataPlaneMain` 由一个 `Rc<RefCell<_>>` 拥有，daemon 与 CLI process
持引用/弱引用；每轮 graph/File/Process 同步操作只短借 `&mut`，任何
`RefMut<DataPlaneMain>` 都不能跨 `.await`。`Weak` 避免
`DataPlaneMain -> NodeMain -> Process task -> DataPlaneMain` 所有权环。
这是真正要修改的 runtime 所有权边界，不能靠 `ProcessYield` 掩盖。

动态 node 沿用 `Process::new` 和注册路径，由 `DataPlaneMain::start_process`
启动，不再为 CLI 读就绪扩展
`DataPlaneMain::process_events()`；该 API 是旧回调方案遗留设计，应从
本 ADR 删除。VPP process node 的复用/动态注册参见
`vlib/unix/cli.c:2705-2745,2853-2910`。

VPP `vlib_cli_input` 在当前 process 安装并恢复 output function
（`vlib/cli.c:690-755`）。Hammer 不照搬该回调式输出：命令返回值由宏
格式化，per-connection Process 按连接顺序追加并发送；命令不会收到
Unix file、裸 fd 或输出能力。
`mp_safe = false` 的 **同步 main 借用阶段**按
`vlib/cli.c:601-614` 进入 WorkerBarrier。不能在 barrier 内 `.await`：
async handler 只持有 `Args` 与自身取得的拥有型状态。若等待后仍需
修改 worker-visible 状态，必须再进入一个同步 main 阶段并单独取得 barrier；
不引入可跨 `.await` 借 `DataPlaneMain` 的签名。

## 5. 通用 File、AsyncFileMain 和错误边界

当前 `crates/hammer-runtime/src/file/mod.rs:48,787-838` 的 runtime
`File` 只是 `hammer-core::file::File<NodeMain, RuntimeError>` 的别名，
`AsyncFileMain` 只有复制的 wake fd，随后仍调用 `FileMain::poll_for_worker`；
`FileMain::read_some/write_some` (`:636-698`) 执行同步 syscall。
**这不是异步 File I/O。** 不定义第二个 CLI File，也不让新
`AsyncFileMain` 经由 `FILE_MAIN` 的 thread-zero shard。现有通用
`File<Context, Error>` 连同 callback ABI 从 core 迁至其真正的 I/O owner
`hammer-runtime::file`，保留这个 File 的既有字段/能力；现有宏生成的
`FileFunctions` 路径同步改引用，不保留两套 File 定义。这样 runtime
可以直接为原有 File 增加异步方法；当前类型别名无法在 runtime 实现
外部 `tokio::io::AsyncRead/AsyncWrite`（Rust 孤儿规则）。

`FileMain` 继续持有 worker 的 File、readiness poller 和 callback；
`AsyncFileMain` **直接持有 thread-zero 的 File 池**、独立 io_uring、
在途操作和 CQE 分派。一个 File 只属于其中一个 owner；CLI listener
和连接只登记在 AsyncFileMain，不设置 `polling_thread_index = 0` 再
进入 FileMain。此边界取代 ADR-0046 中 thread-zero adapter 的描述，
不改变它的 worker readiness 方案。
VPP `vlib/unix/cli.c:2750-2827,2853-2910` 提供连接、读写和 process
关系；io_uring 的 SQE/CQE 是 Hammer 的实现选择，不声称 VPP 使用它。

```rust
// Existing generic File, relocated rather than duplicated. VPP:
// vlib/unix/cli.c:2750-2827,2853-2910; vppinfra/file.h: clib_file_t.
// Linux operation source: third_party/io-uring/src/opcode.rs:618-658,
// 895-990,1032-1107. Borrowed Tokio buffers never become SQE pointers.
impl File<NodeMain, RuntimeError, AsyncFileRegistration> {
    pub async fn accept(&self) -> RuntimeResult<OwnedFd>;
}

// The existing File has Owner = () for FileMain; only the thread-zero
// specialization carries this private io_uring registration state.
struct AsyncFileRegistration {
    owner: std::rc::Weak<std::cell::RefCell<AsyncFileMain>>,
    index: std::cell::Cell<u32>,
    socket: bool,
    closed: std::cell::Cell<bool>,
    read: std::cell::Cell<Option<u32>>,
    write: std::cell::Cell<Option<u32>>,
    accept: std::cell::Cell<Option<u32>>,
}

impl tokio::io::AsyncRead for &File<NodeMain, RuntimeError, AsyncFileRegistration> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>>;
}

impl tokio::io::AsyncWrite for &File<NodeMain, RuntimeError, AsyncFileRegistration> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>>;
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<std::io::Result<()>>;
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<std::io::Result<()>>;
}

// hammer-runtime::file; VPP: vlib/unix/cli.c:2853-2910.
// Unlike the old adapter, this Main is the actual File and I/O owner.
pub struct AsyncFileMain {
    ring: io_uring::IoUring,
    completion_ready: std::rc::Rc<tokio::io::unix::AsyncFd<OwnedFd>>,
    submission_ready: std::rc::Rc<tokio::sync::Notify>,
    files: Pool<std::rc::Rc<File<NodeMain, RuntimeError, AsyncFileRegistration>>>,
    operations: Pool<Box<FileOperation>>,
}

// Private SQE/CQE ownership record; never a second File or CLI record.
struct FileOperation {
    file: std::rc::Rc<File<NodeMain, RuntimeError, AsyncFileRegistration>>,
    kind: FileOperationKind,
    waker: std::task::Waker,
}

enum FileOperationKind {
    Accept,
    Read { buffer: Vec<u8> },
    Write { buffer: Vec<u8>, offset: usize },
}

impl AsyncFileMain {
    pub fn init() -> RuntimeResult<std::rc::Rc<std::cell::RefCell<Self>>>;
    pub fn add(
        main: &std::rc::Rc<std::cell::RefCell<Self>>,
        fd: OwnedFd,
        description: String,
    ) -> RuntimeResult<u32>;
    #[inline]
    pub(crate) fn file(&self, index: u32)
        -> Option<std::rc::Rc<File<NodeMain, RuntimeError, AsyncFileRegistration>>>;
    pub fn remove(&mut self, index: u32) -> RuntimeResult<()>;
    pub async fn next_ready(main: std::rc::Rc<std::cell::RefCell<Self>>)
        -> RuntimeResult<usize>;
}
```

异步 owner 登记 File 时在原有 File 上设置私有的弱 owner 引用和槽位；
同步 File 没有异步 owner。两种 File 都独占各自的 `OwnedFd`；
thread-zero `AsyncFileMain` 的池、连接 Process 和在途操作只共享
`Rc<File>`，不在 fd 上添加 `Arc` 或 `Rc`。`file(index)` 复制同一注册
File 的 `Rc`，不复制 fd、不新增 File 类型。单个注册同时至多有一个
read 和一个 write 操作。Process 持有 File 跨 `.await`，却不持有对
`AsyncFileMain`、其池项或 `DataPlaneMain` 的借用；在同步 FileMain
拥有的 File 上启动异步操作是 owner 违反契约，局部断言而非恢复错误。

`AsyncRead::poll_read` 只在短暂 owner 借用中提交 read SQE，并把
`Waker` 留在操作记录，随后返回 `Pending`。SQE 指向 AsyncFileMain
操作池中稳定的 Vec，而**不是**调用方的 `ReadBuf<'_>`；CQE 到达后
把已读字节复制进新的 `ReadBuf`，若调用方缓冲较小，余量留待下次
poll。`BufReader` 负责 CLI 的跨读缓冲和行界限，不再让
`UnixCliFile` 复制一份常驻 input Vec。read 的零字节完成表示 EOF。

`AsyncWrite::poll_write` 将本次 `&[u8]` 拷入有界、由操作池持有的
稳定 Vec，**接受成功就返回 `Ready(n)`**；同一 File 的后续写在在途
操作完成前返回 `Pending`。短写以操作池中的同一 Vec 和 offset 继续
提交；`poll_flush` 只有在所有已接受字节取得成功 CQE 后才返回，
晚到的写错误从 `poll_flush` 或下一次写返回；`poll_shutdown` 先 flush
再关闭。不把调用者的临时 `&[u8]` 留给内核，也不将提交 SQE 误当成
实际发送完成。accept CQE 的新 fd
立即包装为 `OwnedFd`。这些操作使用 io_uring 的 `Accept`、`Recv`、
`Send`（socket send 带 `MSG_NOSIGNAL`）；通用非 socket fd 可用
`Read`/`Write`，不能把 socket 专属选项套到普通 fd。

这是真正的标准 `AsyncRead`/`AsyncWrite` 契约，因此连接 Process 的
输入/输出借用同一个 File，使用 `tokio::io::BufReader<&File>` /
`BufWriter<&File>`；非交互模式读一条命令后再写一次响应，不要求并发
借用。File 在两个操作和 Process 结束前由 `Rc<File>` 保持存活；
File 的 SQE staging 与 Tokio 缓冲层之间有明确复制。CLI 是控制面，
不把它宣传为零拷贝或把这个缓冲策略应用到 packet data path。

CQE 的 `user_data` 直接存 `FileOperation` 的 Pool 索引。操作槽位只在
对应原始 CQE 消费之后释放，取消请求的 CQE 使用独立标识并忽略；
因此同一个在途操作不可能被新操作复用槽位，不需要 generation。
完成结果写回 File 的异步注册状态再唤醒 Future。Future 取消只丢弃
接收方，不释放内核仍可能访问的 Vec；
`remove` 发出并提交取消请求后标记 File 关闭，原始操作的 CQE 到达后才释放
操作 buffer、复用槽位；取消请求自己的 CQE 使用独立标识，不释放原始
操作。实际 fd 在所有 File 能力和在途操作
的 `Rc<File>` 均释放后关闭。已关闭 File 的 `EAGAIN`/`EINTR` 完成不再
重投 SQE。SQ 满时先提交已有 SQE；若仍没有空位，返回
`WouldBlock`，不覆盖已经注册的 File。
`dispatch` 先批量提交待发 SQE、有限额地排空 CQE，然后同时等待
io_uring **自身 fd** 的可读通知和新提交通知；它克隆通知能力后才
`.await`，不持有 `RefMut<AsyncFileMain>`。不再复制 FileMain 的 wake
eventfd，也不调用 `poll_for_worker(0, ...)`；旧 thread-zero
`FileMode::Async` readiness adapter 随之退场。main loop 仍按其固定
Process/File 步骤选择这一个 `dispatch` Future，不在它里面执行 CLI
handler 或持有 WorkerBarrier。worker 的同步 File 轮询完全独立。
Linux 以 io_uring 实施；非 Linux 平台需要自己的异步 File backend，
不能静默退回 FileMain 回调并仍称为本 ADR 的 AsyncFileMain。

```rust
// hammer-runtime::cli; VPP: vlib/cli.c:572-625,690-730,1420-1478;
// vlib/unix/cli.c:2750-2827,3164-3205. No numeric domain errors.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("duplicate CLI command: {path}")]
    DuplicateCommand { path: &'static str },
    #[error("unknown CLI command: {input}")]
    UnknownCommand { input: String },
    #[error("unexpected CLI argument: {argument}")]
    UnexpectedArgument { argument: String },
    #[error("invalid CLI argument: {argument}")]
    InvalidArgument { argument: String },
    // Rust text boundary; VPP's unformat input is byte-oriented.
    #[error("CLI input is not UTF-8")]
    InputEncoding { #[source] source: std::str::Utf8Error },
    #[error("create CLI socket directory: {path}")]
    SocketDirectory { path: PathBuf, #[source] source: std::io::Error },
    #[error("bind CLI socket: {path}")]
    SocketBind { path: PathBuf, #[source] source: std::io::Error },
    #[error("configure CLI socket: {path}")]
    SocketConfigure { path: PathBuf, #[source] source: std::io::Error },
    #[error("remove CLI socket: {path}")]
    SocketRemove { path: PathBuf, #[source] source: std::io::Error },
    #[error("register CLI File")]
    FileRegister { #[source] source: Box<RuntimeError> },
    #[error("start CLI Process")]
    ProcessStart { #[source] source: Box<RuntimeError> },
    #[error("load CLI command declarations")]
    PluginCatalog { #[source] source: PluginError },
}

// The existing runtime error boundary retains CLI errors as a typed source.
pub enum RuntimeError {
    Cli(#[from] CliError),
    // Existing variants are unchanged.
}
```

命令参数错误属于这条连接，输出错误和 NUL 后结束；File 的 accept、
read、write CQE 负 errno 转为 `std::io::Error`，保留在已有
`RuntimeError::FileAccept/FileRead/FileWrite` source。`EAGAIN` 在 File
Future 内重新等待/提交，不作为 CLI 参数错误；task cancellation、panic、
连接 EOF 与 handler 返回的 `CliError` 分别处理，不用数字 retval 或
`to_string()` 做错误传递。`CliError` 的两个 runtime source 以 `Box`
打断与 `RuntimeError::Cli` 的递归类型；普通命令解析错误在当前连接内
处理，不导致 daemon 退出。

只给 `CliMain::global` 这种极短访问标记普通 `#[inline]`；
注册、格式化、socket I/O、barrier、Process poll 都不强制内联。
VPP `vlib/unix/cli.c:224-249` 的 `always_inline` 只用于极短的 pager/
file 清理，不是 CLI 全链路 `inline(always)` 的依据。

初始化顺序：Main Heap、加载插件声明、worker `FileMain`、thread-zero
`AsyncFileMain`、注册的 `CliMain::init` 与 `UnixCliMain::init`、启动静态
Process、daemon 在 thread 0 通过 `UnixCliMain::global().listen` 登记
AF_UNIX listener；每条连接再动态启动自己的 Process。listener 字段在
登记前为 `None`；daemon 默认启用本地 CLI，不把它接进 `GlobalMain`。
当前 thread-zero `DataPlaneMain` 的所有权转为 `Rc<RefCell<_>>` 是本 ADR
的明确共享 runtime API 改动；不是
可在旧的 `&mut DataPlaneMain` main loop 上凭空工作的局部 helper。

daemon 的 TOML 只配置监听路径，默认 `/run/hammer/cli.sock`：

```toml
[cli]
socket = "/run/hammer/cli.sock"
```

listener 在 thread 0 的 Process 启动阶段绑定；路径已有活动 listener
时拒绝覆盖，只有确认现存 Unix socket 已拒绝连接时才清除残留路径。
绑定成功后的配置或 File 注册失败也清除本次创建的路径；正常退出时
注册的 `unix_cli_exit` 在 main-loop exit 阶段移除本进程的 listener
和路径；即使主循环先报错，退出回调仍运行。VPP Unix CLI listener 的创建
与 File 登记见 `vlib/unix/cli.c:3164-3205`。

## 6. hammerctl 与实施门槛

`hammerctl` 在独立 `netsystem-client` 仓库连接 AF_UNIX
`SOCK_STREAM`，默认路径为 `/run/hammer/cli.sock`，`-s` 可覆盖。
本期只实现非交互模式：一次连接按行提交一条命令，以 NUL 标记完成
（空输出也有 NUL），客户端读到 NUL 后结束。没有命令时返回用法错误；
不读取 stdin、不提供交互提示符，也不启用 Telnet 字符模式或服务端提示符
协商。客户端处理短写、EOF、NUL 与文本同批到达，不显示 NUL。
它逐块输出 NUL 前的文本，不把整份响应积累到内存。
VPP：`vpp/app/vppctl.c:190-260,434-485` 和
`vlib/unix/cli.c:2638-2648`。服务端 `hammer-ipc` 仍只负责 Binary API。

本次实现将**现有** `File`/callback ABI 从 core 迁至 runtime 并同步宏引用；
由 AsyncFileMain 而非 FileMain 持有 thread-zero File、ring 和在途操作；
现有 File 实现 `AsyncRead`/`AsyncWrite` 与异步 accept；私有
`FileOperation` 持有内核在途缓冲区和 CQE/waker。main loop 改为
thread-zero `Rc<RefCell<DataPlaneMain>>`/`LocalSet` 所有权，增加动态
Process 注册、`DataPlaneMain::start_process`、完成后的 node 回收、
`CliError::InputEncoding` 与 `hammer-component-macros::cli_command`。
没有新增 CLI File、
`DataPlaneMain::process_events(node)` 或 thread-zero FileMain callback。
后续验证应覆盖：两个连接并发，一个 `wait`
挂起时另一个 `show version` 完成；每连接只执行一条命令、空输出 NUL、坏 Args、
短读写、断开/取消时在途 buffer 和 fd 的生命周期、CQE 后槽位复用、
CLI fd 只在主线程 AsyncFileMain，以及 barrier 不跨 await。
按 issue #370 的明确要求，本次未运行编译、测试或 CI；实现状态不代表
这些运行时行为已经验证。
