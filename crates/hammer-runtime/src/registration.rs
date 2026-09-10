//! Immutable executable registrations owned by one link image.
//!
//! `PluginMain` retains each image through the ABI-stable plugin root and
//! collects its runtime registrations after dependency-ordered loading. No DSO
//! load constructor, destructor, global registry, or synchronization is
//! involved. Session App and transport protocol capabilities are intentionally
//! absent: those are registered by their owning service/plugin authorities.

use crate::binary_api::BinaryApiMethodEntry;
use crate::error::RuntimeResult;
use crate::init::{ConfigFunction, InitFunction};
use crate::node::{NodeEntry, NodeFunctionRegistration};
use abi_stable::StableAbi;
use hammer_stats::StatsMain;

/// One static aggregate registration in a link image's stats catalog.
#[derive(Clone, Copy)]
pub struct StatsRegistration {
    pub name: &'static str,
    pub register: fn(&StatsMain) -> RuntimeResult<()>,
}

/// The existing registration catalog for one link image.
///
/// This is deliberately the only runtime registration carrier. It is opaque at
/// the root ABI boundary; only runtime's `PluginMain` accesses its inventories.
/// Session App and transport protocol registrations do not cross this boundary.
#[doc(hidden)]
#[repr(C)]
#[derive(StableAbi)]
#[sabi(unsafe_opaque_fields)]
pub struct RegistrationImage {
    init_functions: &'static [&'static InitFunction],
    config_functions: &'static [&'static ConfigFunction],
    main_loop_enter_functions: &'static [&'static InitFunction],
    main_loop_exit_functions: &'static [&'static InitFunction],
    worker_init_functions: &'static [&'static InitFunction],
    num_workers_change_functions: &'static [&'static InitFunction],
    api_init_functions: &'static [&'static InitFunction],
    graph_nodes: &'static [&'static NodeEntry],
    node_functions: &'static [&'static NodeFunctionRegistration],
    process_nodes: &'static [&'static NodeEntry],
    binary_api_methods: &'static [&'static BinaryApiMethodEntry],
    stats_registrations: &'static [&'static StatsRegistration],
}

impl RegistrationImage {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        init_functions: &'static [&'static InitFunction],
        config_functions: &'static [&'static ConfigFunction],
        main_loop_enter_functions: &'static [&'static InitFunction],
        main_loop_exit_functions: &'static [&'static InitFunction],
        worker_init_functions: &'static [&'static InitFunction],
        num_workers_change_functions: &'static [&'static InitFunction],
        api_init_functions: &'static [&'static InitFunction],
        graph_nodes: &'static [&'static NodeEntry],
        node_functions: &'static [&'static NodeFunctionRegistration],
        process_nodes: &'static [&'static NodeEntry],
        binary_api_methods: &'static [&'static BinaryApiMethodEntry],
    ) -> Self {
        Self::new_with_stats(
            init_functions,
            config_functions,
            main_loop_enter_functions,
            main_loop_exit_functions,
            worker_init_functions,
            num_workers_change_functions,
            api_init_functions,
            graph_nodes,
            node_functions,
            process_nodes,
            binary_api_methods,
            &[],
        )
    }

    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub const fn new_with_stats(
        init_functions: &'static [&'static InitFunction],
        config_functions: &'static [&'static ConfigFunction],
        main_loop_enter_functions: &'static [&'static InitFunction],
        main_loop_exit_functions: &'static [&'static InitFunction],
        worker_init_functions: &'static [&'static InitFunction],
        num_workers_change_functions: &'static [&'static InitFunction],
        api_init_functions: &'static [&'static InitFunction],
        graph_nodes: &'static [&'static NodeEntry],
        node_functions: &'static [&'static NodeFunctionRegistration],
        process_nodes: &'static [&'static NodeEntry],
        binary_api_methods: &'static [&'static BinaryApiMethodEntry],
        stats_registrations: &'static [&'static StatsRegistration],
    ) -> Self {
        Self {
            init_functions,
            config_functions,
            main_loop_enter_functions,
            main_loop_exit_functions,
            worker_init_functions,
            num_workers_change_functions,
            api_init_functions,
            graph_nodes,
            node_functions,
            process_nodes,
            binary_api_methods,
            stats_registrations,
        }
    }

    #[inline]
    pub(crate) fn init_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static InitFunction> + '_ {
        self.init_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn config_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static ConfigFunction> + '_ {
        self.config_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn worker_init_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static InitFunction> + '_ {
        self.worker_init_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn num_workers_change_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static InitFunction> + '_ {
        self.num_workers_change_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn api_init_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static InitFunction> + '_ {
        self.api_init_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn main_loop_enter_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static InitFunction> + '_ {
        self.main_loop_enter_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn main_loop_exit_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static InitFunction> + '_ {
        self.main_loop_exit_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn graph_nodes(&self) -> impl Clone + Iterator<Item = &'static NodeEntry> + '_ {
        self.graph_nodes.iter().copied()
    }

    #[inline]
    pub(crate) fn node_functions(
        &self,
    ) -> impl Clone + Iterator<Item = &'static NodeFunctionRegistration> + '_ {
        self.node_functions.iter().copied()
    }

    #[inline]
    pub(crate) fn process_nodes(&self) -> impl Clone + Iterator<Item = &'static NodeEntry> + '_ {
        self.process_nodes.iter().copied()
    }

    #[inline]
    pub(crate) fn binary_api_methods(
        &self,
    ) -> impl Clone + Iterator<Item = &'static BinaryApiMethodEntry> + '_ {
        self.binary_api_methods.iter().copied()
    }

    #[inline]
    pub(crate) fn stats_registrations(
        &self,
    ) -> impl Clone + Iterator<Item = &'static StatsRegistration> + '_ {
        self.stats_registrations.iter().copied()
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __declare_registration_image {
    () => {
        $crate::__declare_registration_image!(
            init_functions = [];
            config_functions = [];
            main_loop_enter_functions = [];
            main_loop_exit_functions = [];
            worker_init_functions = [];
            num_workers_change_functions = [];
            api_init_functions = [];
            graph_nodes = [];
            node_functions = [];
            process_nodes = [];
            binary_api_methods = [];
            stats_registrations = [];
        );
    };
    (
        init_functions = [$($init:path),* $(,)?];
        config_functions = [$($config:path),* $(,)?];
        main_loop_enter_functions = [$($enter:path),* $(,)?];
        main_loop_exit_functions = [$($exit:path),* $(,)?];
        worker_init_functions = [$($worker_init:path),* $(,)?];
        num_workers_change_functions = [$($num_workers_change:path),* $(,)?];
        api_init_functions = [$($api_init:path),* $(,)?];
        graph_nodes = [$($graph_node:path),* $(,)?];
        node_functions = [$($node_function:path),* $(,)?];
        process_nodes = [$($process_node:path),* $(,)?];
        binary_api_methods = [$($binary_api_method:path),* $(,)?];
        stats_registrations = [$($stats_registration:path),* $(,)?];
    ) => {
        static __HAMMER_REGISTRATION_IMAGE: $crate::__private::RegistrationImage =
            $crate::__private::RegistrationImage::new_with_stats(
                &[$(&$init),*],
                &[$(&$config),*],
                &[$(&$enter),*],
                &[$(&$exit),*],
                &[$(&$worker_init),*],
                &[$(&$num_workers_change),*],
                &[$(&$api_init),*],
                &[$(&$graph_node),*],
                &[$(&$node_function),*],
                &[$(&$process_node),*],
                &[$(&$binary_api_method),*],
                &[$(&$stats_registration),*],
            );
    };
    (
        init_functions = [$($init:path),* $(,)?];
        config_functions = [$($config:path),* $(,)?];
        main_loop_enter_functions = [$($enter:path),* $(,)?];
        main_loop_exit_functions = [$($exit:path),* $(,)?];
        worker_init_functions = [$($worker_init:path),* $(,)?];
        num_workers_change_functions = [$($num_workers_change:path),* $(,)?];
        api_init_functions = [$($api_init:path),* $(,)?];
        graph_nodes = [$($graph_node:path),* $(,)?];
        node_functions = [$($node_function:path),* $(,)?];
        process_nodes = [$($process_node:path),* $(,)?];
        binary_api_methods = [$($binary_api_method:path),* $(,)?];
    ) => {
        $crate::__declare_registration_image!(
            init_functions = [$($init),*];
            config_functions = [$($config),*];
            main_loop_enter_functions = [$($enter),*];
            main_loop_exit_functions = [$($exit),*];
            worker_init_functions = [$($worker_init),*];
            num_workers_change_functions = [$($num_workers_change),*];
            api_init_functions = [$($api_init),*];
            graph_nodes = [$($graph_node),*];
            node_functions = [$($node_function),*];
            process_nodes = [$($process_node),*];
            binary_api_methods = [$($binary_api_method),*];
            stats_registrations = [];
        );
    };
}
