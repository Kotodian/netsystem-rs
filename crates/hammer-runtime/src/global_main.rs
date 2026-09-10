use std::cell::UnsafeCell;
use std::sync::OnceLock;

use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::bitmap::Bitmap;

use crate::init::{ConfigFunction, InitFunction};
use crate::node::{NodeEntry, NodeFunctionRegistration};

pub(crate) static GLOBAL_MAIN: OnceLock<GlobalMain> = OnceLock::new();

#[repr(C)]
pub struct GlobalMain {
    cacheline0: CacheLineAlignMark,
    name: String,
    exec_path: String,
    argv: Vec<String>,
    startup_config: String,
    pub(crate) node_registrations: Vec<&'static NodeEntry>,
    pub(crate) node_function_registrations: Vec<&'static NodeFunctionRegistration>,
    pub(crate) init_function_registrations: Vec<&'static InitFunction>,
    pub(crate) main_loop_enter_function_registrations: Vec<&'static InitFunction>,
    pub(crate) main_loop_exit_function_registrations: Vec<&'static InitFunction>,
    pub(crate) worker_init_function_registrations: Vec<&'static InitFunction>,
    pub(crate) num_workers_change_function_registrations: Vec<&'static InitFunction>,
    pub(crate) api_init_function_registrations: Vec<&'static InitFunction>,
    pub(crate) config_function_registrations: Vec<&'static ConfigFunction>,
    init_functions_called: UnsafeCell<Bitmap>,
}

// SAFETY: registration lists and metadata are immutable after publication.
// The main thread is the only writer of lifecycle progress, and every access
// verifies that owner before dereferencing the UnsafeCell.
unsafe impl Sync for GlobalMain {}

const _: () = {
    assert!(core::mem::align_of::<GlobalMain>() == CACHE_LINE);
    assert!(core::mem::offset_of!(GlobalMain, cacheline0) == 0);
    assert!(core::mem::offset_of!(GlobalMain, name) == 0);
};

impl GlobalMain {
    pub fn new(name: String, exec_path: String, argv: Vec<String>, startup_config: String) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            name,
            exec_path,
            argv,
            startup_config,
            node_registrations: Vec::new(),
            node_function_registrations: Vec::new(),
            init_function_registrations: Vec::new(),
            main_loop_enter_function_registrations: Vec::new(),
            main_loop_exit_function_registrations: Vec::new(),
            worker_init_function_registrations: Vec::new(),
            num_workers_change_function_registrations: Vec::new(),
            api_init_function_registrations: Vec::new(),
            config_function_registrations: Vec::new(),
            init_functions_called: UnsafeCell::new(Bitmap::new()),
        }
    }

    pub(crate) fn global() -> &'static Self {
        GLOBAL_MAIN
            .get()
            .expect("GlobalMain is published before lifecycle dispatch")
    }

    #[inline]
    pub(crate) fn startup_config(&self) -> &str {
        &self.startup_config
    }

    pub(crate) fn register_node(&mut self, registration: &'static NodeEntry) {
        if !self
            .node_registrations
            .iter()
            .any(|current| std::ptr::eq(*current, registration))
        {
            self.node_registrations.push(registration);
        }
    }

    pub(crate) fn register_node_function(
        &mut self,
        registration: &'static NodeFunctionRegistration,
    ) {
        Self::push_declaration(&mut self.node_function_registrations, registration);
    }

    pub(crate) fn mark_init_function_called(&self, callback_index: usize) -> bool {
        crate::ensure_main_thread()
            .expect("global lifecycle progress belongs to the process main thread");
        // SAFETY: GlobalMain is published once, and only the verified process
        // main thread dispatches or mutates global lifecycle progress.
        unsafe { (&mut *self.init_functions_called.get()).set(callback_index) }
    }

    pub(crate) fn register_init(&mut self, registration: &'static InitFunction) {
        self.assign_init_callback_index(registration);
        Self::push_declaration(&mut self.init_function_registrations, registration);
    }

    pub(crate) fn register_main_loop_enter(&mut self, registration: &'static InitFunction) {
        self.assign_init_callback_index(registration);
        Self::push_declaration(
            &mut self.main_loop_enter_function_registrations,
            registration,
        );
    }

    pub(crate) fn register_main_loop_exit(&mut self, registration: &'static InitFunction) {
        self.assign_init_callback_index(registration);
        Self::push_declaration(
            &mut self.main_loop_exit_function_registrations,
            registration,
        );
    }

    pub(crate) fn register_worker_init(&mut self, registration: &'static InitFunction) {
        self.assign_init_callback_index(registration);
        Self::push_declaration(&mut self.worker_init_function_registrations, registration);
    }

    pub(crate) fn register_num_workers_change(&mut self, registration: &'static InitFunction) {
        self.assign_init_callback_index(registration);
        Self::push_declaration(
            &mut self.num_workers_change_function_registrations,
            registration,
        );
    }

    pub(crate) fn register_api_init(&mut self, registration: &'static InitFunction) {
        self.assign_init_callback_index(registration);
        Self::push_declaration(&mut self.api_init_function_registrations, registration);
    }

    pub(crate) fn register_config(&mut self, registration: &'static ConfigFunction) {
        self.assign_config_callback_index(registration);
        Self::push_declaration(&mut self.config_function_registrations, registration);
    }

    fn push_declaration<T>(registrations: &mut Vec<&'static T>, registration: &'static T) {
        if !registrations
            .iter()
            .any(|current| std::ptr::eq(*current, registration))
        {
            registrations.push(registration);
        }
    }

    fn assign_init_callback_index(&self, registration: &InitFunction) -> usize {
        if let Some(index) = registration.callback_index() {
            return index;
        }
        let existing = self
            .init_registrations()
            .find(|current| std::ptr::fn_addr_eq(current.func, registration.func))
            .and_then(InitFunction::callback_index);
        let index = existing.unwrap_or_else(crate::init::allocate_callback_index);
        let assigned = registration.assign_callback_index(index);
        assert_eq!(
            assigned, index,
            "lifecycle callback identity changed after registration"
        );
        assigned
    }

    fn assign_config_callback_index(&self, registration: &ConfigFunction) -> usize {
        if let Some(index) = registration.callback_index() {
            return index;
        }
        let existing = self
            .config_function_registrations
            .iter()
            .copied()
            .find(|current| std::ptr::fn_addr_eq(current.func, registration.func))
            .and_then(ConfigFunction::callback_index);
        let index = existing.unwrap_or_else(crate::init::allocate_callback_index);
        let assigned = registration.assign_callback_index(index);
        assert_eq!(
            assigned, index,
            "config callback identity changed after registration"
        );
        assigned
    }

    fn init_registrations(&self) -> impl Iterator<Item = &'static InitFunction> + '_ {
        self.init_function_registrations
            .iter()
            .chain(&self.main_loop_enter_function_registrations)
            .chain(&self.main_loop_exit_function_registrations)
            .chain(&self.worker_init_function_registrations)
            .chain(&self.num_workers_change_function_registrations)
            .chain(&self.api_init_function_registrations)
            .copied()
    }
}
