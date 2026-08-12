//! Hammer HTTP plugin.
//!
//! This slice owns the synchronous HTTP/3 protocol primitives under
//! `http3::proto`, aligned with `third_party/vpp/src/plugins/http/http3/`
//! and `third_party/h3/h3/src/proto`, plus the VPP HTTP FIFO ABI codec under
//! `http_common` (message/header types and checked encode/decode for
//! publishing one request). It also declares the plugin descriptor, the
//! builtin HTTP Session App registration over QUIC sessions (mirroring VPP's
//! `http_transport_enable` attach of the static `http_app_cb_vft`,
//! http.c:1004-1063), and the main-thread HTTP listener authority with its
//! `start_listen`-only SessionTransport registration. The authority
//! bootstraps after QUIC/session init (VPP `http_transport_init`,
//! http.c:1867-1903), registers an inner Application listener, nests the
//! lower QUIC listen, and publishes the listener context in the O(1)
//! outer-listener slot map; stop_listen, HTTP3 engine dispatch, FIFO
//! transfer/publication, QPACK, and the Session App lifecycle are later
//! slices. `worker` owns the per-data-worker connection-context pool
//! primitive (VPP `http_worker_t::ctx_pool`); the listener authority
//! installs one worker per data worker through the `http_worker_init`
//! worker init function (ordered after session/QUIC worker init), mirroring
//! `QuicMain.workers`. The Session App callback table stays empty until
//! those slices own their lifecycle state.

mod http3;
mod http_app;
mod http_common;
mod listener;
mod worker;

#[cfg(test)]
mod http_app_tests;

hammer_component_macros::declare_plugin!(
    name = "http",
    load_after = ["quic"],
    init_functions = [listener::__INIT_FN_HTTP_TRANSPORT_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [listener::__INIT_FN_HTTP_WORKER_INIT],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
    session_transports = [listener::__SESSION_TRANSPORT_HTTP_TRANSPORT],
    session_apps = [http_app::HTTP_SESSION_APP],
    binary_api_methods = [],
);

pub use listener::HttpMain;
