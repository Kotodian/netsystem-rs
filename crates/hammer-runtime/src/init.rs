use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

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
    pub func: fn(&mut DataPlaneMain) -> RuntimeResult<()>,
    #[doc(hidden)]
    pub callback_index: &'static AtomicUsize,
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
    pub early: bool,
    pub func: fn(&str, Option<&mut DataPlaneMain>) -> RuntimeResult<()>,
    #[doc(hidden)]
    pub callback_index: &'static AtomicUsize,
}

const UNASSIGNED_CALLBACK_INDEX: usize = usize::MAX;
static NEXT_CALLBACK_INDEX: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn allocate_callback_index() -> usize {
    let index = NEXT_CALLBACK_INDEX.fetch_add(1, AtomicOrdering::Relaxed);
    assert_ne!(
        index, UNASSIGNED_CALLBACK_INDEX,
        "lifecycle callback index space is exhausted"
    );
    index
}

macro_rules! impl_callback_index {
    ($registration:ty) => {
        impl $registration {
            #[inline]
            pub(crate) fn callback_index(&self) -> Option<usize> {
                let index = self.callback_index.load(AtomicOrdering::Relaxed);
                (index != UNASSIGNED_CALLBACK_INDEX).then_some(index)
            }

            pub(crate) fn assign_callback_index(&self, index: usize) -> usize {
                match self.callback_index.compare_exchange(
                    UNASSIGNED_CALLBACK_INDEX,
                    index,
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                ) {
                    Ok(_) => index,
                    Err(assigned) => assigned,
                }
            }
        }
    };
}

impl_callback_index!(InitFunction);
impl_callback_index!(ConfigFunction);

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

impl<T: Ordered + ?Sized> Ordered for &T {
    fn name(&self) -> &'static str {
        (*self).name()
    }

    fn runs_before(&self) -> &'static [&'static str] {
        (*self).runs_before()
    }

    fn runs_after(&self) -> &'static [&'static str] {
        (*self).runs_after()
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
    items: &[&'static InitFunction],
    called: &mut hammer_infra::bitmap::Bitmap,
    main: &mut DataPlaneMain,
) -> RuntimeResult<()> {
    let order = topological_order(items)?;
    for index in order {
        let function = items[index];
        let callback_index = function
            .callback_index()
            .expect("lifecycle callback index is assigned during registration");
        if !called.set(callback_index) {
            continue;
        }
        (function.func)(main)?;
    }
    Ok(())
}

pub fn run_init_functions(global: &mut GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(
        &global.init_function_registrations,
        &mut global.init_functions_called,
        main,
    )
}

pub fn run_stats_registrations(plugins: &crate::PluginMain) -> RuntimeResult<()> {
    let stats_main = StatsMain::global()?;
    let mut result = Ok(());
    plugins.visit_images(|image| {
        if result.is_err() {
            return;
        }
        for registration in image.stats_registrations() {
            if let Err(error) = (registration.register)(stats_main) {
                result = Err(error);
                return;
            }
        }
    });
    result
}

pub fn run_worker_init_functions(
    main: &mut DataPlaneMain,
    functions: &[&'static InitFunction],
) -> RuntimeResult<()> {
    let mut called = std::mem::take(&mut main.worker_init_functions_called);
    let worker = main.thread_index();
    let mut result = Ok(());
    for function in functions {
        let callback_index = function
            .callback_index()
            .expect("worker-init callback index is assigned during registration");
        if !called.set(callback_index) {
            continue;
        }
        if let Err(source) = (function.func)(main) {
            result = Err(crate::RuntimeError::WorkerInitialization {
                worker,
                function: function.name,
                source: Box::new(source),
            });
            break;
        }
    }
    main.worker_init_functions_called = called;
    result
}

pub fn run_main_loop_enter(global: &mut GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(
        &global.main_loop_enter_function_registrations,
        &mut global.init_functions_called,
        main,
    )
}

pub fn run_main_loop_exit(global: &mut GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(
        &global.main_loop_exit_function_registrations,
        &mut global.init_functions_called,
        main,
    )
}

pub fn run_num_workers_change(
    global: &mut GlobalMain,
    main: &mut DataPlaneMain,
) -> RuntimeResult<()> {
    dispatch_init(
        &global.num_workers_change_function_registrations,
        &mut global.init_functions_called,
        main,
    )
}

pub fn run_api_init(global: &mut GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(
        &global.api_init_function_registrations,
        &mut global.init_functions_called,
        main,
    )
}

fn dispatch_config(
    items: &[&'static ConfigFunction],
    called: &mut hammer_infra::bitmap::Bitmap,
    mut main: Option<&mut DataPlaneMain>,
    early: bool,
    document: &str,
) -> RuntimeResult<()> {
    let selected: Vec<_> = items
        .iter()
        .copied()
        .filter(|function| function.early == early)
        .collect();
    let order = topological_order(&selected)?;
    for index in order {
        let function = selected[index];
        let callback_index = function
            .callback_index()
            .expect("config callback index is assigned during registration");
        if !called.set(callback_index) {
            continue;
        }
        (function.func)(document, main.as_deref_mut())?;
    }
    Ok(())
}

pub fn run_config_functions(
    global: &mut GlobalMain,
    main: Option<&mut DataPlaneMain>,
    early: bool,
    document: &str,
) -> RuntimeResult<()> {
    dispatch_config(
        &global.config_function_registrations,
        &mut global.init_functions_called,
        main,
        early,
        document,
    )
}
