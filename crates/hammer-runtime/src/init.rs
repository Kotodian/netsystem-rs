use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;

use hammer_core::error::{CoreError, HammerResult};

use crate::engine::Engine;

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

pub struct ConfigFunction {
    pub name: &'static str,
    pub func: fn(&mut Engine, &toml::Value) -> HammerResult<()>,
}

impl Ordered for ConfigFunction {
    fn name(&self) -> &'static str {
        self.name
    }
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
    let empty = toml::Value::Table(toml::value::Table::new());
    for func in functions {
        let section = config.get(func.name).unwrap_or(&empty);
        (func.func)(engine, section)?;
    }
    Ok(())
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
        let core: CoreError = err.into();
        assert!(core.to_string().contains("duplicate"));
    }
}
