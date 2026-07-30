use hammer_infra::fifo::Fifo;
use hammer_runtime::app::{AppSessionProtocol, ORDERED_RELIABLE_BYTE_STREAM};
use hammer_runtime::{PluginError, PluginMain, RuntimeResult};

#[hammer_component_macros::session_transport(name = "tcp", upper = "ordered-reliable-byte-stream")]
struct TcpTransport;

#[hammer_component_macros::app_session_protocol(
    name = "test-protocol",
    lower = "ordered-reliable-byte-stream",
    upper = "ordered-reliable-byte-stream"
)]
struct TestProtocol;

impl AppSessionProtocol for TestProtocol {
    fn ingress(&mut self, _: &Fifo, _: &Fifo) -> RuntimeResult<(usize, usize)> {
        Ok((0, 0))
    }

    fn egress(&mut self, _: &Fifo, _: &Fifo) -> RuntimeResult<(usize, usize)> {
        Ok((0, 0))
    }
}

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
    session_transports = [__SESSION_TRANSPORT_TCP_TRANSPORT];
    app_session_protocols = [__APP_SESSION_PROTOCOL_TEST_PROTOCOL];
    binary_api_methods = [];
);

#[test]
fn component_protocol_registration_is_collected_by_plugin_main() {
    let mut plugins = PluginMain::default();
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);

    let registrations = plugins.app_session_protocols();

    let names = registrations
        .iter()
        .map(|entry| entry.registration().name())
        .collect::<Vec<_>>();

    assert!(names.contains(&"test-protocol"));
    let registration = plugins
        .app_session_protocol("test-protocol")
        .expect("resolve registered protocol")
        .registration();
    assert_eq!(registration.name(), "test-protocol");
    assert_eq!(registration.lower(), ORDERED_RELIABLE_BYTE_STREAM);
    assert_eq!(registration.upper(), ORDERED_RELIABLE_BYTE_STREAM);
}

#[test]
fn component_transport_registration_is_collected_by_plugin_main() {
    let mut plugins = PluginMain::default();
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);

    let transport = plugins
        .session_transport("tcp")
        .expect("resolve registered Transport");
    assert_eq!(transport.name(), "tcp");
    assert_eq!(transport.upper(), ORDERED_RELIABLE_BYTE_STREAM);
}

#[test]
fn protocol_lookup_reports_missing_and_duplicate_names() {
    let mut plugins = PluginMain::default();

    assert!(matches!(
        plugins.app_session_protocol("missing"),
        Err(PluginError::AppSessionProtocolMissing { name }) if name == "missing"
    ));

    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    assert!(matches!(
        plugins.app_session_protocol("test-protocol"),
        Err(PluginError::AppSessionProtocolDuplicate { name }) if name == "test-protocol"
    ));
}

#[test]
fn transport_lookup_reports_missing_and_duplicate_names() {
    let mut plugins = PluginMain::default();

    assert!(matches!(
        plugins.session_transport("missing"),
        Err(PluginError::SessionTransportMissing { name }) if name == "missing"
    ));

    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    assert!(matches!(
        plugins.session_transport("tcp"),
        Err(PluginError::SessionTransportDuplicate { name }) if name == "tcp"
    ));
}
