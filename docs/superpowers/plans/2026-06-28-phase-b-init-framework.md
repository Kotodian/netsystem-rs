# Phase 2 (B) — Init Framework + Engine Skeleton

## 决策汇总

| 项 | 决策 |
|---|---|
| 位置 | `hammer-runtime::init`（非 hammer-core，避免循环依赖） |
| 命名 | `Engine` / `EnginePool` / `spawn` / `run_init_functions` / `run_config_functions` |
| 泛型 | `Ordered` trait + `topological_order<T: Ordered>` + `dispatch_ordered<T: Ordered>` |
| 错误 | `InitError` enum + `thiserror`，`From<InitError> for CoreError` |
| 去重 | `DiGraphMap` 节点集 + O(n) 扫描，无 HashSet |
| infra 结构 | `EnginePool::engines` 用 `hammer_infra::vec::Vec<Engine>` |
| elog | 不加，留 Phase C/D |
| 测试 | 不加 test-only 生产方法，测试自行构造 |

---

## Task B-1: init 模块

**文件:**
- 修改: `crates/hammer-component-macros/src/lib.rs` — `::hammer_core::init::` → `::hammer_runtime::init::`（11 处 replaceAll）
- 修改: `crates/hammer-runtime/Cargo.toml` — 加 `linkme`、`petgraph`、`thiserror`
- 创建: `crates/hammer-runtime/src/engine.rs` — 最小 stub
- 创建: `crates/hammer-runtime/src/init.rs`
- 修改: `crates/hammer-runtime/src/lib.rs` — `pub mod engine; pub mod init;`

### init.rs

```rust
use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;

use hammer_core::error::{CoreError, HammerResult};

use crate::engine::Engine;

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("duplicate function name `{0}`")]
    DuplicateName(&'static str),
    #[error("`{name}` references unregistered dependency `{dep}`")]
    UnresolvedDependency { name: &'static str, dep: &'static str },
    #[error("dependency cycle: {cycle}")]
    Cycle { cycle: String },
}

impl From<InitError> for CoreError {
    fn from(err: InitError) -> Self {
        CoreError::internal(err.to_string())
    }
}

/// 拓扑排序接口。[`InitFunction`] / [`ConfigFunction`] 及所有 distributed slice 条目实现此 trait，
/// 共用 [`topological_order`] 排序逻辑。
pub trait Ordered {
    fn name(&self) -> &'static str;
    fn runs_before(&self) -> &'static [&'static str] { &[] }
    fn runs_after(&self) -> &'static [&'static str] { &[] }
}

pub struct InitFunction {
    pub name: &'static str,
    pub runs_before: &'static [&'static str],
    pub runs_after: &'static [&'static str],
    pub func: fn(&mut Engine) -> HammerResult<()>,
}

impl Ordered for InitFunction {
    fn name(&self) -> &'static str { self.name }
    fn runs_before(&self) -> &'static [&'static str] { self.runs_before }
    fn runs_after(&self) -> &'static [&'static str] { self.runs_after }
}

pub struct ConfigFunction {
    pub name: &'static str,
    pub func: fn(&mut Engine, &toml::Value) -> HammerResult<()>,
}

impl Ordered for ConfigFunction {
    fn name(&self) -> &'static str { self.name }
}

#[linkme::distributed_slice]
pub static INIT_FUNCTIONS: [InitFunction] = [..];

#[linkme::distributed_slice]
pub static CONFIG_FUNCTIONS: [ConfigFunction] = [..];

#[linkme::distributed_slice]
pub static EARLY_CONFIG_FUNCTIONS: [ConfigFunction] = [..];

#[linkme::distributed_slice]
pub static MAIN_LOOP_ENTER_FUNCTIONS: [InitFunction] = [..];

#[linkme::distributed_slice]
pub static MAIN_LOOP_EXIT_FUNCTIONS: [InitFunction] = [..];

#[linkme::distributed_slice]
pub static WORKER_INIT_FUNCTIONS: [InitFunction] = [..];

/// 对任何 `[T: Ordered]` 执行拓扑排序，返回索引执行顺序。
/// 重复名、未解析依赖、环均返回 [`InitError`]。
pub fn topological_order<T: Ordered>(items: &[T]) -> Result<Vec<usize>, InitError> {
    let mut graph = DiGraphMap::<&str, ()>::new();
    for item in items {
        graph.add_node(item.name());
    }
    // 重复检测：若节点数少于条目数，则存在重复名
    if graph.node_count() < items.len() {
        // O(n) 扫描定位首个重复名
        let mut seen = Vec::with_capacity(items.len());
        for item in items {
            if seen.contains(&item.name()) {
                return Err(InitError::DuplicateName(item.name()));
            }
            seen.push(item.name());
        }
        unreachable!("node_count < items.len() 意味着存在重复，但扫描未找到");
    }

    for item in items {
        let n = item.name();
        for dep in item.runs_after() {
            if !graph.contains_node(*dep) {
                return Err(InitError::UnresolvedDependency { name: n, dep });
            }
            graph.add_edge(*dep, n, ());
        }
        for before in item.runs_before() {
            if !graph.contains_node(*before) {
                return Err(InitError::UnresolvedDependency { name: n, dep: *before });
            }
            graph.add_edge(n, *before, ());
        }
    }

    let ordered = toposort(&graph, None)
        .map_err(|cycle| InitError::Cycle {
            cycle: cycle.node_id().to_string(),
        })?;

    let mut result = Vec::with_capacity(items.len());
    for name in ordered {
        let idx = items
            .iter()
            .position(|t| t.name() == name)
            .expect("toposort 节点必在 items 中");
        result.push(idx);
    }
    Ok(result)
}

fn dispatch<T: Ordered>(
    items: &[T],
    engine: &mut Engine,
    run: impl Fn(&T, &mut Engine) -> HammerResult<()>,
) -> HammerResult<()> {
    let order = topological_order(items)?;
    for idx in order {
        run(&items[idx], engine)?;
    }
    Ok(())
}

pub fn run_init_functions(engine: &mut Engine) -> HammerResult<()> {
    dispatch(&INIT_FUNCTIONS, engine, |f, e| (f.func)(e))
}

pub fn run_worker_init_functions(engine: &mut Engine) -> HammerResult<()> {
    dispatch(&WORKER_INIT_FUNCTIONS, engine, |f, e| (f.func)(e))
}

pub fn run_main_loop_enter(engine: &mut Engine) -> HammerResult<()> {
    dispatch(&MAIN_LOOP_ENTER_FUNCTIONS, engine, |f, e| (f.func)(e))
}

pub fn run_main_loop_exit(engine: &mut Engine) -> HammerResult<()> {
    dispatch(&MAIN_LOOP_EXIT_FUNCTIONS, engine, |f, e| (f.func)(e))
}

pub fn run_config_functions(
    engine: &mut Engine,
    early: bool,
    config: &toml::Value,
) -> HammerResult<()> {
    let functions = if early {
        &EARLY_CONFIG_FUNCTIONS[..]
    } else {
        &CONFIG_FUNCTIONS[..]
    };
    for func in functions {
        let section = config
            .get(func.name)
            .unwrap_or(&toml::Value::Table(toml::value::Table::new()));
        (func.func)(engine, section)?;
    }
    Ok(())
}
```

### engine.rs stub（B-1 编译依赖）

```rust
pub struct Engine;
```

### 测试（init.rs 尾部）

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graphmap::DiGraphMap;

    fn mock(
        specs: &[(&'static str, &'static [&'static str], &'static [&'static str])],
    ) -> Vec<InitFunction> {
        specs
            .iter()
            .map(|(name, after, before)| InitFunction {
                name,
                runs_after: after,
                runs_before: before,
                func: |_| Ok(()),
            })
            .collect()
    }

    #[test]
    fn orders_dependency_first() {
        let fns = mock(&[("a", &[], &["b"]), ("b", &["a"], &[])]);
        let order = topological_order(&fns).expect("topo");
        let names: Vec<&str> = order.iter().map(|i| fns[*i].name).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn missing_dep_errors() {
        let fns = mock(&[("a", &["ghost"], &[])]);
        let err = topological_order(&fns).expect_err("must fail");
        assert!(matches!(
            err,
            InitError::UnresolvedDependency { name: "a", dep: "ghost" }
        ));
    }

    #[test]
    fn cycle_errors() {
        let fns = mock(&[("a", &["b"], &[]), ("b", &["a"], &[])]);
        let err = topological_order(&fns).expect_err("must fail");
        assert!(matches!(err, InitError::Cycle { .. }));
    }

    #[test]
    fn no_deps_any_permutation() {
        let fns = mock(&[("x", &[], &[]), ("y", &[], &[]), ("z", &[], &[])]);
        let order = topological_order(&fns).expect("topo");
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn duplicate_name_errors() {
        let fns = mock(&[("a", &[], &[]), ("a", &[], &[])]);
        let err = topological_order(&fns).expect_err("must fail");
        assert!(matches!(err, InitError::DuplicateName("a")));
    }

    #[test]
    fn empty_slice_ok() {
        let fns: Vec<InitFunction> = Vec::new();
        let order = topological_order(&fns).expect("empty ok");
        assert!(order.is_empty());
    }

    #[test]
    fn init_error_converts_to_core_error() {
        let err = InitError::DuplicateName("foo");
        let core: CoreError = err.into();
        assert!(core.to_string().contains("duplicate"));
    }
}
```

### 验证

```
cargo build -p hammer-runtime
cargo test -p hammer-runtime init
```

### Commit

```
hammer-runtime(Feat): add init framework with generic topo sort

- Move InitFunction/ConfigFunction + 6 distributed slices to hammer-runtime::init
- (avoids circular dep: EngineMain lives in hammer-runtime, not hammer-core)
- Ordered trait + generic topological_order<T: Ordered> + dispatch<T>
- InitError enum (thiserror): DuplicateName/UnresolvedDependency/Cycle
- Dedup via DiGraphMap node_count, no std HashSet
- run_init_functions / run_worker_init_functions /
  run_main_loop_enter / run_main_loop_exit / run_config_functions
- Config dispatch resolves TOML section by function name
- Proc macros: ::hammer_core::init → ::hammer_runtime::init (11 sites)
- Add linkme, petgraph, thiserror deps to hammer-runtime
- Engine stub in engine.rs (filled by Task B-2)
```

---

## Task B-2: Engine + EnginePool

**文件:**
- 修改: `crates/hammer-runtime/src/engine.rs`（替换 stub）
- 修改: `crates/hammer-runtime/src/lib.rs`（re-export）

### engine.rs

```rust
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};

use hammer_adapter::DataPlaneRuntime;
use hammer_core::registry::RuntimeRegistry;

#[repr(align(64))]
pub struct Engine {
    pub thread_index: u32,
    pub numa_node: u32,
    pub main_loop_count: AtomicU32,
    pub runtime: DataPlaneRuntime,
    pub registry: Arc<RuntimeRegistry>,
    pub wait_at_barrier: Arc<AtomicU32>,
    pub workers_at_barrier: Arc<AtomicU32>,
    pub main_loop_exit_now: AtomicBool,
    pub main_loop_exit_status: Mutex<i32>,
}

impl Engine {
    pub fn new(runtime: DataPlaneRuntime, registry: Arc<RuntimeRegistry>) -> Self {
        Self {
            thread_index: 0,
            numa_node: 0,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            wait_at_barrier: Arc::new(AtomicU32::new(0)),
            workers_at_barrier: Arc::new(AtomicU32::new(0)),
            main_loop_exit_now: AtomicBool::new(false),
            main_loop_exit_status: Mutex::new(0),
        }
    }

    pub fn spawn(&self, index: u32) -> Self {
        Self {
            thread_index: index,
            numa_node: self.numa_node,
            main_loop_count: AtomicU32::new(0),
            runtime: self.runtime.clone(),
            registry: Arc::clone(&self.registry),
            wait_at_barrier: Arc::clone(&self.wait_at_barrier),
            workers_at_barrier: Arc::clone(&self.workers_at_barrier),
            main_loop_exit_now: AtomicBool::new(false),
            main_loop_exit_status: Mutex::new(0),
        }
    }
}

pub struct EnginePool {
    pub engines: hammer_infra::vec::Vec<Engine>,
    pub name: String,
    pub exec_path: String,
    pub argv: Vec<String>,
    pub startup_config: String,
}

impl EnginePool {
    pub fn new(main: Engine) -> Self {
        let mut engines = hammer_infra::vec::Vec::new();
        engines.push(main);
        Self {
            engines,
            name: String::new(),
            exec_path: String::new(),
            argv: Vec::new(),
            startup_config: String::new(),
        }
    }

    pub fn main_engine(&self) -> &Engine {
        &self.engines[0]
    }

    pub fn main_engine_mut(&mut self) -> &mut Engine {
        &mut self.engines[0]
    }

    pub fn worker_count(&self) -> usize {
        self.engines.len().saturating_sub(1)
    }

    pub fn engine(&self, index: usize) -> Option<&Engine> {
        self.engines.get(index)
    }

    pub fn engine_mut(&mut self, index: usize) -> Option<&mut Engine> {
        self.engines.get_mut(index)
    }
}
```

### 测试（engine.rs 尾部）

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use hammer_adapter::DataPlaneRuntime;
    use std::sync::Arc;
    use hammer_core::registry::RuntimeRegistry;

    fn test_engine() -> Engine {
        Engine::new(
            DataPlaneRuntime::with_buffer_capacity(64, 16),
            RuntimeRegistry::new(),
        )
    }

    #[test]
    fn spawn_shares_registry_and_resets_thread_index() {
        let main = test_engine();
        let worker = main.spawn(3);
        assert_eq!(worker.thread_index, 3);
        assert_eq!(main.thread_index, 0);
        assert!(Arc::ptr_eq(&main.registry, &worker.registry));
    }

    #[test]
    fn spawn_shares_barrier_arcs() {
        let main = test_engine();
        let worker = main.spawn(1);
        assert!(Arc::ptr_eq(&main.wait_at_barrier, &worker.wait_at_barrier));
        assert!(Arc::ptr_eq(&main.workers_at_barrier, &worker.workers_at_barrier));
    }

    #[test]
    fn spawn_resets_loop_count_and_exit_flag() {
        let main = test_engine();
        main.main_loop_count.store(42, std::sync::atomic::Ordering::Relaxed);
        main.main_loop_exit_now.store(true, std::sync::atomic::Ordering::Relaxed);
        let worker = main.spawn(1);
        assert_eq!(
            worker.main_loop_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(!worker.main_loop_exit_now.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn engine_pool_main_engine_at_index_zero() {
        let main = test_engine();
        let pool = EnginePool::new(main);
        assert_eq!(pool.worker_count(), 0);
        assert!(pool.engine(0).is_some());
        assert!(pool.engine(1).is_none());
    }
}
```

### lib.rs re-export

```rust
pub use engine::{Engine, EnginePool};
```

### 验证

```
cargo build -p hammer-runtime
cargo test -p hammer-runtime
```

### Commit

```
hammer-runtime(Feat): add Engine + EnginePool with spawn

- Engine: cache-line aligned per-thread struct absorbing DataPlaneRuntime
- EnginePool: singleton holding hammer_infra::Vec<Engine> (index 0 = main)
- Engine::spawn(index): clone runtime + registry + barrier arcs, reset counters
- EnginePool::new(main) / main_engine() / worker_count() / engine(idx)
- No test-only methods on production types; tests construct via Engine::new
```

---

## Task B-3: 迁移 CONTROL_INITS → #[init_function]

**文件:**
- 修改: `crates/hammer-service/src/packet_graph.rs` — 删 `CONTROL_INITS` + `init_control_planes`
- 修改: `crates/hammer-service/src/transport/mod.rs` — `#[init_function(name = "transport_init")]`
- 修改: `crates/hammer-service/src/transport/tcp/mod.rs` — `#[init_function(name = "tcp_init")]`
- 修改: `crates/hammer-service/src/net/lookup/mod.rs` — `#[init_function(name = "ip_init", runs_after = ["transport_init", "tcp_init"])]`
- 修改: `crates/hammer-service/src/service.rs` — `init_control_planes` → `run_init_functions`
- 修改: `crates/hammer-service/Cargo.toml` — 确认 `hammer-component-macros` dep

### 关键变更

三个 control init 函数原来签名为 `fn(&RuntimeRegistry) -> HammerResult<()>`，改为 `fn(&mut Engine) -> HammerResult<()>`，函数体从 `engine.registry` 取 registry：

```rust
// transport/mod.rs
#[hammer_component_macros::init_function(name = "transport_init")]
fn init_transport(engine: &mut hammer_runtime::Engine) -> HammerResult<()> {
    let reg = &engine.registry;
    init(reg)
}

// transport/tcp/mod.rs
#[hammer_component_macros::init_function(name = "tcp_init")]
fn init_tcp(engine: &mut hammer_runtime::Engine) -> HammerResult<()> {
    let reg = &engine.registry;
    init(reg)
}

// net/lookup/mod.rs
#[hammer_component_macros::init_function(name = "ip_init", runs_after = ["transport_init", "tcp_init"])]
fn init_ip(engine: &mut hammer_runtime::Engine) -> HammerResult<()> {
    let reg = &engine.registry;
    init(reg)
}
```

> ip_init 排在 transport/tcp 之后，因为 ip main 可能依赖它们已初始化。现有代码无显式排序——加 `runs_after` 保险。

**service.rs** 中 `new_with_event_subscribers`：

```rust
// 旧:
packet_graph::init_control_planes(&registry)?;

// 新:
let mut engine = hammer_runtime::Engine::new(
    hammer_runtime::new_worker_runtime(slot_capacity, slots),  // 需暴露
    Arc::clone(&registry),
);
hammer_runtime::init::run_init_functions(&mut engine)?;
```

> 注意 `new_worker_runtime` 当前在 `data_plane.rs` 是 `pub(crate)`，需改为 `pub` 或在 `Engine::new` 内调用。**推荐**：在 `hammer-runtime/src/lib.rs` `pub use data_plane::new_worker_runtime;`。

**packet_graph.rs** 删除 `CONTROL_INITS` 和 `init_control_planes`。保留 `SERVICE_GRAPH_NODES`。

### 验证

```
cargo test -p hammer-service
```

### Commit

```
hammer-service(Refactor): migrate CONTROL_INITS to #[init_function]

- Delete CONTROL_INITS distributed slice + init_control_planes
- Annotate init_transport / init_tcp / init_ip with #[init_function]
- service.rs: construct Engine, call run_init_functions instead of
  iterating CONTROL_INITS (keep old install_on_workers path for B phase)
- Expose new_worker_runtime from hammer-runtime
```

---

## Task B-4: fmt + clippy + workspace test

```
cargo test --workspace
cargo fmt --all
cargo clippy --workspace --all-targets
```

### Commit

```
project(Chore): Phase B complete — fmt + clippy clean, workspace tests pass
```
