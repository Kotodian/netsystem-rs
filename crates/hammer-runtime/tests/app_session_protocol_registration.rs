use hammer_runtime::app::SessionAppContext;
use hammer_runtime::{DataWorkerId, Engine, PluginError, PluginMain, RuntimeResult};

#[allow(clippy::needless_pass_by_value)]
fn install(_: &mut Engine) -> RuntimeResult<()> {
    Ok(())
}

fn destroy(_: DataWorkerId, _: SessionAppContext) {}

static __SESSION_APP_TEST_APP: hammer_runtime::app::SessionAppRegistration =
    hammer_runtime::app::SessionAppRegistration::new("test-app", install, destroy);

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
    session_transports = [];
    session_apps = [__SESSION_APP_TEST_APP];
    binary_api_methods = [];
);

#[test]
fn component_session_app_registration_is_collected_by_plugin_main() {
    let mut plugins = PluginMain::default();
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);

    let registrations = plugins.session_apps();
    assert!(registrations.iter().any(|entry| entry.name() == "test-app"));

    let registration = plugins
        .session_app("test-app")
        .expect("resolve Session App");
    assert_eq!(registration.name(), "test-app");
}

#[test]
fn session_app_lookup_reports_missing_and_duplicate_names() {
    let mut plugins = PluginMain::default();

    assert!(matches!(
        plugins.session_app("missing"),
        Err(PluginError::SessionAppMissing { name }) if name == "missing"
    ));

    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    assert!(matches!(
        plugins.session_app("test-app"),
        Err(PluginError::SessionAppDuplicate { name }) if name == "test-app"
    ));
}
