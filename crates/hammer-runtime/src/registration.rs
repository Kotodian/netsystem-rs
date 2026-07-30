//! Immutable executable registrations owned by one link image.
//!
//! `PluginMain` retains each image through the ABI-stable plugin root and
//! collects its registrations after dependency-ordered loading. No DSO load
//! constructor, destructor, global registry, or synchronization is involved.

use crate::app::{AppSessionProtocolEntry, SessionTransportRegistration};
use crate::binary_api::BinaryApiMethodEntry;
use crate::init::{ConfigFunction, InitFunction};
use crate::node::{NodeEntry, NodeFunctionRegistration};
use crate::process::ProcessEntry;
use abi_stable::StableAbi;

/// The existing registration catalog for one link image.
///
/// This is deliberately the only registration carrier. It is opaque at the
/// root ABI boundary; only runtime's `PluginMain` accesses its inventories.
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
    session_transports: &'static [SessionTransportRegistration],
    app_session_protocols: &'static [AppSessionProtocolEntry],
    binary_api_methods: &'static [BinaryApiMethodEntry],
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
        session_transports: &'static [SessionTransportRegistration],
        app_session_protocols: &'static [AppSessionProtocolEntry],
        binary_api_methods: &'static [BinaryApiMethodEntry],
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
            session_transports,
            app_session_protocols,
            binary_api_methods,
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
    pub(crate) fn session_transports(&self) -> &'static [SessionTransportRegistration] {
        self.session_transports
    }

    #[inline]
    pub(crate) fn app_session_protocols(&self) -> &'static [AppSessionProtocolEntry] {
        self.app_session_protocols
    }

    #[inline]
    pub(crate) fn binary_api_methods(&self) -> &'static [BinaryApiMethodEntry] {
        self.binary_api_methods
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
            session_transports = [];
            app_session_protocols = [];
            binary_api_methods = [];
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
        session_transports = [$($session_transport:path),* $(,)?];
        app_session_protocols = [$($app_session_protocol:path),* $(,)?];
        binary_api_methods = [$($binary_api_method:path),* $(,)?];
    ) => {
        static __HAMMER_REGISTRATION_IMAGE: $crate::__private::RegistrationImage =
            $crate::__private::RegistrationImage::new(
                &[$($init),*],
                &[$($config),*],
                &[$($early_config),*],
                &[$($enter),*],
                &[$($exit),*],
                &[$($worker_init),*],
                &[$($graph_node),*],
                &[$($node_function),*],
                &[$($process_node),*],
                &[$($session_transport),*],
                &[$($app_session_protocol),*],
                &[$($binary_api_method),*],
            );
    };
}
