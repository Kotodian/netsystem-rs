use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::error::RuntimeResult;

use crate::engine::Engine;
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
    pub func: fn(&mut Engine) -> RuntimeResult<()>,
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
    pub func: fn(&str, &mut Engine) -> RuntimeResult<()>,
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
    engine: &mut Engine,
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

pub fn run_init_functions(engine: &mut Engine) -> RuntimeResult<()> {
    let functions = engine.plugin_main().init_functions();
    let mut called = std::mem::take(&mut engine.called_init_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_init_functions = called;
    result
}

pub fn run_stats_registrations(engine: &Engine, stats_main: &StatsMain) -> RuntimeResult<()> {
    for registration in engine.plugin_main().stats_registrations() {
        (registration.register)(stats_main)?;
    }
    Ok(())
}

pub fn run_worker_init_functions(engine: &mut Engine) -> RuntimeResult<()> {
    let functions = engine.worker_init_functions();
    let mut called = std::mem::take(&mut engine.called_worker_init_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_worker_init_functions = called;
    result
}

pub fn run_worker_exit_functions(engine: &mut Engine) -> RuntimeResult<()> {
    let functions = engine.take_worker_exit_functions();
    let mut first_error = None;
    for function in functions {
        match catch_unwind(AssertUnwindSafe(|| function(engine))) {
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

pub fn run_main_loop_enter(engine: &mut Engine) -> RuntimeResult<()> {
    let functions = engine.plugin_main().main_loop_enter_functions();
    let mut called = std::mem::take(&mut engine.called_main_loop_enter_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_main_loop_enter_functions = called;
    result?;
    engine.main_loop_entered = true;
    Ok(())
}

pub fn run_main_loop_exit(engine: &mut Engine) -> RuntimeResult<()> {
    let functions = engine.plugin_main().main_loop_exit_functions();
    let mut called = std::mem::take(&mut engine.called_main_loop_exit_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_main_loop_exit_functions = called;
    result
}

fn dispatch_config(
    items: Vec<ConfigFunction>,
    called: &mut HashSet<&'static str>,
    engine: &mut Engine,
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

pub fn run_config_functions(engine: &mut Engine, early: bool, document: &str) -> RuntimeResult<()> {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::RuntimeError;

    static WORKER_EXIT_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn record_worker_exit(_: &mut Engine) -> RuntimeResult<()> {
        WORKER_EXIT_CALLS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn mock(
        specs: &[(
            &'static str,
            &'static [&'static str],
            &'static [&'static str],
        )],
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
    fn worker_exit_callbacks_are_drained_once() {
        WORKER_EXIT_CALLS.store(0, Ordering::Relaxed);
        let mut engine = Engine::new(
            crate::DataPlaneRuntime::new(crate::DataPlaneRuntimeConfig::default()),
            crate::RuntimeRegistry::new(),
        );
        engine.register_worker_exit_function(record_worker_exit);

        run_worker_exit_functions(&mut engine).expect("worker exit callback");
        run_worker_exit_functions(&mut engine).expect("worker exit callback drain");

        assert_eq!(WORKER_EXIT_CALLS.load(Ordering::Relaxed), 1);
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
            InitError::UnresolvedDependency {
                name: "a",
                dep: "ghost"
            }
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
        let core: RuntimeError = err.into();
        assert!(matches!(
            core,
            RuntimeError::Init(InitError::DuplicateName("foo"))
        ));
    }
}
