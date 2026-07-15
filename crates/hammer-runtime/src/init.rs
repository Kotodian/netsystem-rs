use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use hammer_core::error::{CoreError, HammerResult};
use hammer_infra::map::FlatHashTable;

use crate::engine::Engine;
use hammer_infra::vec::Vec;

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

impl From<InitError> for CoreError {
    fn from(err: InitError) -> Self {
        CoreError::internal(err.to_string())
    }
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

/// Lifecycle registration collected from constructor-published link images.
#[derive(Clone, Copy)]
pub struct InitFunction {
    pub name: &'static str,
    pub runs_before: &'static [&'static str],
    pub runs_after: &'static [&'static str],
    pub func: fn(&mut Engine) -> HammerResult<()>,
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
    called: &mut FlatHashTable<&'static str, ()>,
    engine: &mut Engine,
) -> HammerResult<()> {
    let order = topological_order(&items)?;
    for index in order {
        let function = items[index];
        if called.get(&function.name).is_some() {
            continue;
        }
        called.insert(function.name, ());
        catch_unwind(AssertUnwindSafe(|| (function.func)(engine))).map_err(|_| {
            CoreError::internal(format!("init function `{}` panicked", function.name))
        })??;
    }
    Ok(())
}

pub fn run_init_functions(engine: &mut Engine) -> HammerResult<()> {
    let functions = crate::registration::init_functions();
    let mut called = std::mem::take(&mut engine.called_init_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_init_functions = called;
    result
}

pub fn run_worker_init_functions(engine: &mut Engine) -> HammerResult<()> {
    let functions = crate::registration::worker_init_functions();
    let mut called = std::mem::take(&mut engine.called_worker_init_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_worker_init_functions = called;
    result
}

pub fn run_main_loop_enter(engine: &mut Engine) -> HammerResult<()> {
    let functions = crate::registration::main_loop_enter_functions();
    let mut called = std::mem::take(&mut engine.called_main_loop_enter_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_main_loop_enter_functions = called;
    result?;
    engine.main_loop_entered = true;
    Ok(())
}

pub fn run_main_loop_exit(engine: &mut Engine) -> HammerResult<()> {
    let functions = crate::registration::main_loop_exit_functions();
    let mut called = std::mem::take(&mut engine.called_main_loop_exit_functions);
    let result = dispatch_init(functions, &mut called, engine);
    engine.called_main_loop_exit_functions = called;
    result
}

pub fn run_config_functions(engine: &mut Engine, early: bool) -> HammerResult<()> {
    let functions = crate::registration::config_functions(early);
    if early {
        let mut called = std::mem::take(&mut engine.called_early_config_functions);
        let result = dispatch_init(functions, &mut called, engine);
        engine.called_early_config_functions = called;
        result
    } else {
        let mut called = std::mem::take(&mut engine.called_config_functions);
        let result = dispatch_init(functions, &mut called, engine);
        engine.called_config_functions = called;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn orders_dependency_first() {
        let fns = mock(&[("a", &[], &["b"]), ("b", &["a"], &[])]);
        let order = topological_order(&fns).expect("topo");
        let names: Vec<&str> = order.iter().map(|i| fns[*i].name).collect();
        assert_eq!(names, hammer_infra::vec!["a", "b"]);
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
        let core: CoreError = err.into();
        assert!(core.to_string().contains("duplicate"));
    }
}
