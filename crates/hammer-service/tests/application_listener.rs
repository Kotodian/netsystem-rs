use hammer_runtime::app::{SessionAppId, SessionAppRegistration};
use hammer_service::session::{ApplicationError, ApplicationMain};

fn install_quic_session_app(_: &mut hammer_runtime::Engine) -> hammer_runtime::RuntimeResult<()> {
    Ok(())
}

fn destroy_quic_session_app(_: hammer_runtime::DataWorkerId, _: u64) {}

#[test]
fn application_listener_owns_a_validated_session_app_endpoint() {
    let applications = ApplicationMain::new(4);
    let application = applications.attach().expect("attach Application");
    let listener = applications
        .register_listener(application, None::<SessionAppId>, None)
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
fn application_listener_identity_is_owned_and_generation_checked() {
    let applications = ApplicationMain::new(4);
    let first = applications.attach().expect("attach first Application");
    let listener = applications
        .register_listener(first, None::<SessionAppId>, None)
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
        .register_listener(first, None::<SessionAppId>, None)
        .expect("register replacement listener");
    assert_ne!(listener, replacement);
}

#[test]
fn application_resolves_registered_session_app_identity() {
    let applications = ApplicationMain::with_session_apps(
        4,
        [SessionAppRegistration::new(
            "quic",
            install_quic_session_app,
            destroy_quic_session_app,
        )],
    );

    assert_eq!(
        applications
            .session_app_id("quic")
            .expect("resolve registered Session App"),
        SessionAppId::new(0)
    );
    assert!(matches!(
        applications.session_app_id("missing"),
        Err(ApplicationError::SessionAppMissing { name }) if name == "missing"
    ));
}
