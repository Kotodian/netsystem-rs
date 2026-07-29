//! Session-side TLS 1.3 plugin.

mod codec;
mod handshake;
mod record;

#[cfg(test)]
mod test_fixtures;

hammer_component_macros::declare_plugin!(
    name = "tls",
    load_after = [],
    init_functions = [],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
);
