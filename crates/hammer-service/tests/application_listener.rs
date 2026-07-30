use hammer_runtime::PluginMain;
use hammer_runtime::app::{
    APP_SESSION_POLICY_VERSION, AppSessionPolicy, AppSessionProtocolSelection,
};
use hammer_service::session::{ApplicationError, ApplicationMain};

#[hammer_component_macros::session_transport(name = "tcp", upper = "ordered-reliable-byte-stream")]
struct TcpTransport;

#[hammer_component_macros::session_transport(name = "quic", upper = "multiplexed-reliable-streams")]
struct QuicTransport;

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [];
    session_transports = [
        __SESSION_TRANSPORT_TCP_TRANSPORT,
        __SESSION_TRANSPORT_QUIC_TRANSPORT,
    ];
    app_session_protocols = [];
    binary_api_methods = [];
);

fn policy(transport: &str) -> AppSessionPolicy {
    AppSessionPolicy::new(
        APP_SESSION_POLICY_VERSION,
        transport,
        [AppSessionProtocolSelection::new("plaintext")],
    )
    .expect("App Session policy")
}

fn applications(capacity: usize) -> std::sync::Arc<ApplicationMain> {
    let mut plugins = PluginMain::default();
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    ApplicationMain::with_inventory(
        capacity,
        plugins.session_transports(),
        plugins.app_session_protocols(),
    )
}

#[test]
fn application_listener_owns_a_validated_immutable_session_policy() {
    let applications = applications(4);
    let application = applications.attach().expect("attach Application");
    let listener = applications
        .register_listener(application, &policy("tcp"))
        .expect("register Application listener");

    applications
        .remove_listener(application, listener)
        .expect("remove Application listener");
    assert!(matches!(
        applications.remove_listener(application, listener),
        Err(ApplicationError::ListenerMissing { listener: missing }) if missing == listener
    ));
}

#[test]
fn application_listener_rejects_incompatible_transport_semantics_before_publication() {
    let applications = applications(4);
    let application = applications.attach().expect("attach Application");

    assert!(matches!(
        applications.register_listener(application, &policy("quic")),
        Err(ApplicationError::SemanticsMismatch {
            lower: "quic",
            provides: "multiplexed-reliable-streams",
            upper: "plaintext",
            requires: "ordered-reliable-byte-stream",
        })
    ));
}

#[test]
fn application_listener_identity_is_owned_and_generation_checked() {
    let applications = applications(4);
    let first = applications.attach().expect("attach first Application");
    let listener = applications
        .register_listener(first, &policy("tcp"))
        .expect("register Application listener");
    let second_application = applications.attach().expect("attach second Application");

    assert!(matches!(
        applications.remove_listener(second_application, listener),
        Err(ApplicationError::ListenerNotOwned { application, listener: rejected })
            if application == second_application && rejected == listener
    ));

    applications
        .remove_listener(first, listener)
        .expect("remove first listener");
    let replacement = applications
        .register_listener(first, &policy("tcp"))
        .expect("register replacement listener");
    assert_ne!(listener, replacement);
}
