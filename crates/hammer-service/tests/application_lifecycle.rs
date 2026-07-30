use hammer_service::session::ApplicationMain;

#[test]
fn local_registration_drop_detaches_the_application() {
    let applications = ApplicationMain::new(1);
    let registration = applications
        .register_local()
        .expect("register Local Application");
    let application = registration.application();

    assert!(
        applications
            .contains(application)
            .expect("resolve Local Application")
    );
    drop(registration);
    assert!(
        !applications
            .contains(application)
            .expect("observe Local Application detach")
    );
}

#[test]
fn detached_application_identity_cannot_resolve_a_replacement() {
    let applications = ApplicationMain::new(1);
    let removed = applications.attach().expect("attach first Application");
    applications
        .detach(removed)
        .expect("detach first Application");
    let replacement = applications.attach().expect("attach replacement");

    assert_ne!(removed, replacement);
    assert!(
        !applications
            .contains(removed)
            .expect("reject stale Application identity")
    );
    assert!(
        applications
            .contains(replacement)
            .expect("resolve replacement Application")
    );
}
