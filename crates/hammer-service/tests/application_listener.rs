use hammer_runtime::app::{APP_SESSION_POLICY_VERSION, AppSessionPolicy};
use hammer_service::session::{ApplicationError, ApplicationMain};

#[test]
fn application_listener_owns_a_validated_immutable_session_policy() {
    let applications = ApplicationMain::new(4);
    let policy =
        AppSessionPolicy::new(APP_SESSION_POLICY_VERSION, []).expect("direct App Session policy");
    let application = applications.attach().expect("attach Application");
    let listener = applications
        .register_listener(application, &policy)
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
    let policy =
        AppSessionPolicy::new(APP_SESSION_POLICY_VERSION, []).expect("direct App Session policy");
    let first = applications.attach().expect("attach first Application");
    let listener = applications
        .register_listener(first, &policy)
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
        .register_listener(first, &policy)
        .expect("register replacement listener");
    assert_ne!(listener, replacement);
}
