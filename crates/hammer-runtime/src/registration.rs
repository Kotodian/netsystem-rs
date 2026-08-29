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
use crate::process::ProcessEntry;
use crate::registry::RuntimeRegistry;
use abi_stable::StableAbi;
use hammer_stats::{StatsMain, StatsResult};

/// One static aggregate registration in a link image's stats catalog.
#[derive(Clone, Copy)]
pub struct StatsRegistration {
    pub name: &'static str,
    pub register: fn(&StatsMain) -> StatsResult<()>,
    pub bind: fn(&StatsMain, &RuntimeRegistry) -> RuntimeResult<()>,
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
    init_functions: &'static [InitFunction],
    config_functions: &'static [ConfigFunction],
    early_config_functions: &'static [ConfigFunction],
    main_loop_enter_functions: &'static [InitFunction],
    main_loop_exit_functions: &'static [InitFunction],
    worker_init_functions: &'static [InitFunction],
    graph_nodes: &'static [NodeEntry],
    node_functions: &'static [NodeFunctionRegistration],
    process_nodes: &'static [ProcessEntry],
    binary_api_methods: &'static [BinaryApiMethodEntry],
    stats_registrations: &'static [StatsRegistration],
}

impl RegistrationImage {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        init_functions: &'static [InitFunction],
        config_functions: &'static [ConfigFunction],
        early_config_functions: &'static [ConfigFunction],
        main_loop_enter_functions: &'static [InitFunction],
        main_loop_exit_functions: &'static [InitFunction],
        worker_init_functions: &'static [InitFunction],
        graph_nodes: &'static [NodeEntry],
        node_functions: &'static [NodeFunctionRegistration],
        process_nodes: &'static [ProcessEntry],
        binary_api_methods: &'static [BinaryApiMethodEntry],
    ) -> Self {
        Self::new_with_stats(
            init_functions,
            config_functions,
            early_config_functions,
            main_loop_enter_functions,
            main_loop_exit_functions,
            worker_init_functions,
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
        init_functions: &'static [InitFunction],
        config_functions: &'static [ConfigFunction],
        early_config_functions: &'static [ConfigFunction],
        main_loop_enter_functions: &'static [InitFunction],
        main_loop_exit_functions: &'static [InitFunction],
        worker_init_functions: &'static [InitFunction],
        graph_nodes: &'static [NodeEntry],
        node_functions: &'static [NodeFunctionRegistration],
        process_nodes: &'static [ProcessEntry],
        binary_api_methods: &'static [BinaryApiMethodEntry],
        stats_registrations: &'static [StatsRegistration],
    ) -> Self {
        Self {
            init_functions,
            config_functions,
            early_config_functions,
            main_loop_enter_functions,
            main_loop_exit_functions,
            worker_init_functions,
            graph_nodes,
            node_functions,
            process_nodes,
            binary_api_methods,
            stats_registrations,
        }
    }

    #[inline]
    pub(crate) fn init_functions(&self) -> &'static [InitFunction] {
        self.init_functions
    }

    #[inline]
    pub(crate) fn config_functions(&self, early: bool) -> &'static [ConfigFunction] {
        if early {
            self.early_config_functions
        } else {
            self.config_functions
        }
    }

    #[inline]
    pub(crate) fn worker_init_functions(&self) -> &'static [InitFunction] {
        self.worker_init_functions
    }

    #[inline]
    pub(crate) fn main_loop_enter_functions(&self) -> &'static [InitFunction] {
        self.main_loop_enter_functions
    }

    #[inline]
    pub(crate) fn main_loop_exit_functions(&self) -> &'static [InitFunction] {
        self.main_loop_exit_functions
    }

    #[inline]
    pub(crate) fn graph_nodes(&self) -> &'static [NodeEntry] {
        self.graph_nodes
    }

    #[inline]
    pub(crate) fn node_functions(&self) -> &'static [NodeFunctionRegistration] {
        self.node_functions
    }

    #[inline]
    pub(crate) fn process_nodes(&self) -> &'static [ProcessEntry] {
        self.process_nodes
    }

    #[inline]
    pub(crate) fn binary_api_methods(&self) -> &'static [BinaryApiMethodEntry] {
        self.binary_api_methods
    }

    #[inline]
    pub(crate) fn stats_registrations(&self) -> &'static [StatsRegistration] {
        self.stats_registrations
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __declare_registration_image {
    () => {
        $crate::__declare_registration_image!(
            init_functions = [];
            config_functions = [];
            early_config_functions = [];
            main_loop_enter_functions = [];
            main_loop_exit_functions = [];
            worker_init_functions = [];
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
        early_config_functions = [$($early_config:path),* $(,)?];
        main_loop_enter_functions = [$($enter:path),* $(,)?];
        main_loop_exit_functions = [$($exit:path),* $(,)?];
        worker_init_functions = [$($worker_init:path),* $(,)?];
        graph_nodes = [$($graph_node:path),* $(,)?];
        node_functions = [$($node_function:path),* $(,)?];
        process_nodes = [$($process_node:path),* $(,)?];
        binary_api_methods = [$($binary_api_method:path),* $(,)?];
        stats_registrations = [$($stats_registration:path),* $(,)?];
    ) => {
        static __HAMMER_REGISTRATION_IMAGE: $crate::__private::RegistrationImage =
            $crate::__private::RegistrationImage::new_with_stats(
                &[$($init),*],
                &[$($config),*],
                &[$($early_config),*],
                &[$($enter),*],
                &[$($exit),*],
                &[$($worker_init),*],
                &[$($graph_node),*],
                &[$($node_function),*],
                &[$($process_node),*],
                &[$($binary_api_method),*],
                &[$($stats_registration),*],
            );
    };
    (
        init_functions = [$($init:path),* $(,)?];
        config_functions = [$($config:path),* $(,)?];
        early_config_functions = [$($early_config:path),* $(,)?];
        main_loop_enter_functions = [$($enter:path),* $(,)?];
        main_loop_exit_functions = [$($exit:path),* $(,)?];
        worker_init_functions = [$($worker_init:path),* $(,)?];
        graph_nodes = [$($graph_node:path),* $(,)?];
        node_functions = [$($node_function:path),* $(,)?];
        process_nodes = [$($process_node:path),* $(,)?];
        binary_api_methods = [$($binary_api_method:path),* $(,)?];
    ) => {
        $crate::__declare_registration_image!(
            init_functions = [$($init),*];
            config_functions = [$($config),*];
            early_config_functions = [$($early_config),*];
            main_loop_enter_functions = [$($enter),*];
            main_loop_exit_functions = [$($exit),*];
            worker_init_functions = [$($worker_init),*];
            graph_nodes = [$($graph_node),*];
            node_functions = [$($node_function),*];
            process_nodes = [$($process_node),*];
            binary_api_methods = [$($binary_api_method),*];
            stats_registrations = [];
        );
    };
}
