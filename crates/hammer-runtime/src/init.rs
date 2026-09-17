use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use crate::data_plane::DataPlaneMain;
use crate::error::RuntimeResult;
use crate::global_main::GlobalMain;

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

/// Orders lifecycle registrations with VPP's `vlib_sort_init_exit_functions`
/// semantics (`vlib/init.c:63-199`): repeatedly take the first registered item
/// whose `runs_after` predecessors already ran, so items without a constraint
/// between them keep registration order — runtime image, then host images, then
/// plugins in load order.
pub fn topological_order<T: Ordered>(items: &[T]) -> Result<Vec<usize>, InitError> {
    let mut index_by_name = HashMap::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        if index_by_name.insert(item.name(), index).is_some() {
            return Err(InitError::DuplicateName(item.name()));
        }
    }

    let mut successors = vec![Vec::new(); items.len()];
    let mut predecessor_count = vec![0usize; items.len()];
    let mut constraints = HashSet::new();
    for (index, item) in items.iter().enumerate() {
        for dependency in item.runs_after() {
            let Some(predecessor) = index_by_name.get(dependency) else {
                return Err(InitError::UnresolvedDependency {
                    name: item.name(),
                    dep: dependency,
                });
            };
            if constraints.insert((*predecessor, index)) {
                successors[*predecessor].push(index);
                predecessor_count[index] += 1;
            }
        }
        for successor in item.runs_before() {
            let Some(successor) = index_by_name.get(successor) else {
                return Err(InitError::UnresolvedDependency {
                    name: item.name(),
                    dep: successor,
                });
            };
            if constraints.insert((index, *successor)) {
                successors[index].push(*successor);
                predecessor_count[*successor] += 1;
            }
        }
    }

    let mut ready: BinaryHeap<Reverse<usize>> = predecessor_count
        .iter()
        .enumerate()
        .filter(|(_, count)| **count == 0)
        .map(|(index, _)| Reverse(index))
        .collect();
    let mut order = Vec::with_capacity(items.len());
    while let Some(Reverse(index)) = ready.pop() {
        order.push(index);
        for &successor in &successors[index] {
            predecessor_count[successor] -= 1;
            if predecessor_count[successor] == 0 {
                ready.push(Reverse(successor));
            }
        }
    }

    if order.len() != items.len() {
        let blocked = (0..items.len())
            .find(|index| predecessor_count[*index] != 0)
            .expect("an incomplete order leaves a blocked registration");
        return Err(InitError::Cycle {
            cycle: items[blocked].name().to_string(),
        });
    }
    Ok(order)
}

fn dispatch_init(
    items: &[&'static InitFunction],
    global: &GlobalMain,
    main: &mut DataPlaneMain,
) -> RuntimeResult<()> {
    let order = topological_order(items)?;
    for index in order {
        let function = items[index];
        let callback_index = function
            .callback_index()
            .expect("lifecycle callback index is assigned during registration");
        if !global.mark_init_function_called(callback_index) {
            continue;
        }
        (function.func)(main)?;
    }
    Ok(())
}

pub fn run_init_functions(global: &GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(&global.init_function_registrations, global, main)
}

/// Runs the registration image against the not-yet-published stats owner.
///
/// Entry declarations and collector registrations both grow startup-only state,
/// so they run before `StatsMain::publish`; the `&mut` borrow is the guarantee
/// that no round or reader can observe the owner while the table grows.
pub(crate) fn run_stats_registrations(
    stats_main: &mut hammer_stats::StatsMain,
) -> RuntimeResult<()> {
    let plugins = crate::PluginMain::global()?;
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

pub fn run_main_loop_enter(global: &GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(&global.main_loop_enter_function_registrations, global, main)
}

pub fn run_main_loop_exit(global: &GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(&global.main_loop_exit_function_registrations, global, main)
}

pub fn run_num_workers_change(global: &GlobalMain, main: &mut DataPlaneMain) -> RuntimeResult<()> {
    dispatch_init(
        &global.num_workers_change_function_registrations,
        global,
        main,
    )
}

pub fn run_api_init(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let global = GlobalMain::global();
    dispatch_init(&global.api_init_function_registrations, global, main)
}

fn dispatch_config(
    items: &[&'static ConfigFunction],
    global: &GlobalMain,
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
        if !global.mark_init_function_called(callback_index) {
            continue;
        }
        (function.func)(document, main.as_deref_mut())?;
    }
    Ok(())
}

pub fn run_config_functions(
    global: &GlobalMain,
    main: Option<&mut DataPlaneMain>,
    early: bool,
    document: &str,
) -> RuntimeResult<()> {
    dispatch_config(
        &global.config_function_registrations,
        global,
        main,
        early,
        document,
    )
}
