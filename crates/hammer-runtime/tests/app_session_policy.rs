use hammer_runtime::app::{
    APP_SESSION_POLICY_VERSION, AppSessionPolicy, AppSessionPolicyError,
    AppSessionProtocolSelection,
};

#[test]
fn policy_preserves_independent_transport_and_ordered_protocol_chain() {
    let policy = AppSessionPolicy::new(
        APP_SESSION_POLICY_VERSION,
        "tcp",
        [
            AppSessionProtocolSelection::with_id("tls", 41),
            AppSessionProtocolSelection::new("http2"),
        ],
    )
    .expect("valid application policy");

    assert_eq!(policy.transport(), "tcp");
    let selected = policy.protocols();
    assert_eq!(
        selected
            .iter()
            .map(AppSessionProtocolSelection::protocol)
            .collect::<Vec<_>>(),
        vec!["tls", "http2"]
    );
    assert_eq!(selected[0].id(), Some(41));
    assert_eq!(selected[1].id(), None);
}

#[test]
fn policy_rejects_an_unsupported_version() {
    let error = AppSessionPolicy::new(
        APP_SESSION_POLICY_VERSION + 1,
        "tcp",
        [AppSessionProtocolSelection::new("plaintext")],
    )
    .expect_err("unsupported policy version");

    assert!(matches!(
        error,
        AppSessionPolicyError::UnsupportedVersion { actual }
            if actual == APP_SESSION_POLICY_VERSION + 1
    ));
}
