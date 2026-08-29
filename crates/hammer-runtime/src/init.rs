use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::data_plane::DataPlaneMain;
use crate::error::RuntimeResult;
use crate::global_main::GlobalMain;
use hammer_stats::StatsMain;

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("duplicate function name `{0}`")]
    DuplicateName(&'static str),
    #[error("`{name}` references unregistered dependency `{dep}`")]
    UnresolvedDependency {
        name: &'static str,
        dep: &'static str,
    },
    #[error("dependency cycle: {cycle}")]
    Cycle { cycle: String },
}

pub trait Ordered {
    fn name(&self) -> &'static str;
    fn runs_before(&self) -> &'static [&'static str] {
        &[]
    }
    fn runs_after(&self) -> &'static [&'static str] {
        &[]
    }
}

/// Lifecycle registration collected from link images retained by PluginMain.
#[derive(Clone, Copy)]
pub struct InitFunction {
    pub name: &'static str,
    pub runs_before: &'static [&'static str],
    pub runs_after: &'static [&'static str],
    pub func: fn(&mut GlobalMain) -> RuntimeResult<()>,
}

/// Lifecycle registration executed once on each Data Worker.
///
/// Worker callbacks receive the owning [`DataPlaneMain`] directly. Global
/// registration and control authority remain with [`GlobalMain`].
#[derive(Clone, Copy)]
pub struct WorkerInitFunction {
    pub name: &'static str,
    pub runs_before: &'static [&'static str],
    pub runs_after: &'static [&'static str],
    pub func: fn(&mut DataPlaneMain) -> RuntimeResult<()>,
}

impl Ordered for WorkerInitFunction {
    fn name(&self) -> &'static str {
        self.name
    }
    fn runs_before(&self) -> &'static [&'static str] {
        self.runs_before
    }
    fn runs_after(&self) -> &'static [&'static str] {
        self.runs_after
    }
}

impl Ordered for InitFunction {
    fn name(&self) -> &'static str {
        self.name
    }
    fn runs_before(&self) -> &'static [&'static str] {
        self.runs_before
    }
    fn runs_after(&self) -> &'static [&'static str] {
        self.runs_after
    }
}

/// Serde configuration registration collected from link images.
///
/// Each owner receives the original startup document and deserializes its
/// declared section through a macro-generated, owner-local serde wrapper.
#[derive(Clone, Copy)]
pub struct ConfigFunction {
    pub name: &'static str,
    pub section: &'static str,
    pub runs_before: &'static [&'static str],
    pub runs_after: &'static [&'static str],
    pub func: fn(&str, &mut GlobalMain) -> RuntimeResult<()>,
}

impl Ordered for ConfigFunction {
    fn name(&self) -> &'static str {
        self.name
    }
    fn runs_before(&self) -> &'static [&'static str] {
        self.runs_before
    }
    fn runs_after(&self) -> &'static [&'static str] {
        self.runs_after
    }
}

pub fn topological_order<T: Ordered>(items: &[T]) -> Result<Vec<usize>, InitError> {
    let mut graph = DiGraphMap::<&str, ()>::new();
    for item in items {
        graph.add_node(item.name());
    }
    if graph.node_count() < items.len() {
        let mut seen = Vec::with_capacity(items.len());
        for item in items {
            if seen.contains(&item.name()) {
                return Err(InitError::DuplicateName(item.name()));
            }
            seen.push(item.name());
        }
        unreachable!("node_count < items.len() implies a duplicate but scan found none");
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
                return Err(InitError::UnresolvedDependency {
                    name: n,
                    dep: *before,
                });
            }
            graph.add_edge(n, *before, ());
        }
    }

    let ordered = toposort(&graph, None).map_err(|cycle| InitError::Cycle {
        cycle: cycle.node_id().to_string(),
    })?;

    let mut result = Vec::with_capacity(items.len());
    for name in ordered {
        let idx = items
            .iter()
            .position(|t| t.name() == name)
            .expect("toposort node must be in items");
        result.push(idx);
    }
    Ok(result)
}

fn dispatch_init(
    items: Vec<InitFunction>,
    called: &mut HashSet<&'static str>,
    engine: &mut GlobalMain,
) -> RuntimeResult<()> {
    let order = topological_order(&items)?;
    for index in order {
        let function = items[index];
        if called.contains(function.name) {
            continue;
        }
        called.insert(function.name);
        match catch_unwind(AssertUnwindSafe(|| (function.func)(engine))) {
            Ok(result) => result?,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
    Ok(())
}

fn dispatch_worker_init(
    items: Vec<WorkerInitFunction>,
    called: &mut HashSet<&'static str>,
    main: &mut DataPlaneMain,
) -> RuntimeResult<()> {
    let order = topological_order(&items)?;
    for index in order {
        let function = items[index];
        if called.contains(function.name) {
            continue;
        }
        called.insert(function.name);
        match catch_unwind(AssertUnwindSafe(|| (function.func)(main))) {
            Ok(result) => result?,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
    Ok(())
}

pub fn run_init_functions(engine: &mut GlobalMain) -> RuntimeResult<()> {
    let functions = engine.plugin_main().init_functions();
    let mut called = std::mem::take(&mut engine.called_init_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_init_functions = called;
    result
}

pub fn run_stats_registrations(engine: &GlobalMain) -> RuntimeResult<()> {
    let stats_main = StatsMain::global()?;
    for registration in engine.plugin_main().stats_registrations() {
        (registration.register)(stats_main)?;
        (registration.bind)(stats_main, &engine.registry)?;
    }
    Ok(())
}

pub fn run_worker_init_functions(
    main: &mut DataPlaneMain,
    functions: Vec<WorkerInitFunction>,
) -> RuntimeResult<()> {
    let functions = functions;
    let mut called = main.take_called_worker_init_functions();
    let result = dispatch_worker_init(functions, &mut called, main);
    main.restore_called_worker_init_functions(called);
    result
}

pub fn run_worker_exit_functions(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let functions = main.take_worker_exit_functions();
    let mut first_error = None;
    for function in functions {
        match catch_unwind(AssertUnwindSafe(|| function(main))) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub fn run_main_loop_enter(engine: &mut GlobalMain) -> RuntimeResult<()> {
    let functions = engine.plugin_main().main_loop_enter_functions();
    let mut called = std::mem::take(&mut engine.called_main_loop_enter_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_main_loop_enter_functions = called;
    result?;
    engine.main_loop_entered = true;
    Ok(())
}

pub fn run_main_loop_exit(engine: &mut GlobalMain) -> RuntimeResult<()> {
    let functions = engine.plugin_main().main_loop_exit_functions();
    let mut called = std::mem::take(&mut engine.called_main_loop_exit_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_main_loop_exit_functions = called;
    result
}

fn dispatch_config(
    items: Vec<ConfigFunction>,
    called: &mut HashSet<&'static str>,
    engine: &mut GlobalMain,
    document: &str,
) -> RuntimeResult<()> {
    let order = topological_order(&items)?;
    for index in order {
        let function = items[index];
        if called.contains(function.name) {
            continue;
        }
        match catch_unwind(AssertUnwindSafe(|| (function.func)(document, engine))) {
            Ok(result) => result?,
            Err(payload) => std::panic::resume_unwind(payload),
        }
        called.insert(function.name);
    }
    Ok(())
}

pub fn run_config_functions(
    engine: &mut GlobalMain,
    early: bool,
    document: &str,
) -> RuntimeResult<()> {
    let functions = engine.plugin_main().config_functions(early);
    if early {
        let mut called = std::mem::take(&mut engine.called_early_config_functions);
        let result = dispatch_config(functions, &mut called, engine, document);
        engine.called_early_config_functions = called;
        result
    } else {
        let mut called = std::mem::take(&mut engine.called_config_functions);
        let result = dispatch_config(functions, &mut called, engine, document);
        engine.called_config_functions = called;
        result
    }
}
