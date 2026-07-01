#[test]
fn session_ooo_rx_path_does_not_allocate_payload_vec() {
    let source = include_str!("../src/session/app.rs");

    assert!(
        !source.contains("let mut bytes = Vec::new()"),
        "OOO RX must stream buffer-chain slices into the session FIFO without a payload Vec"
    );
    assert!(
        !source.contains("bytes.extend_from_slice"),
        "OOO RX must not gather payload bytes before FIFO enqueue"
    );
}

#[test]
fn session_rx_enqueue_reports_partial_delivery_without_claiming_full_accept() {
    let runtime_source = include_str!("../src/session/runtime.rs");
    let tcp_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        runtime_source.contains("accepted_len"),
        "SessionRxEnqueue should report accepted_len separately from delivered_len"
    );
    assert!(
        tcp_source.contains("enqueue.accepted_len"),
        "TCP established path must branch on exact accepted_len"
    );
}

#[test]
fn tcp_receive_window_is_refreshed_from_session_rx_fifo_capacity() {
    let connection_source = include_str!("../src/transport/tcp/connection.rs");
    let established_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        connection_source.contains("set_rcv_wnd"),
        "TcpConnection needs a narrow API for session-provided RX capacity facts"
    );
    assert!(
        established_source.contains("rx_available_len"),
        "established RX path must refresh advertised window from session RX capacity"
    );
}

#[test]
fn session_app_runtime_local_has_static_dispatch() {
    let app_source = include_str!("../src/session/app.rs");
    assert!(
        app_source.contains("impl SessionAppRuntime<Local>"),
        "SessionAppRuntime must have a Local impl block"
    );
    assert!(
        app_source.contains("impl SessionAppRuntime<Svm>"),
        "SessionAppRuntime must have an Svm impl block"
    );
    assert!(
        !app_source.contains("SessionBackendOps"),
        "SessionAppRuntime must not use SessionBackendOps trait"
    );
    assert!(
        !app_source.contains("Box<dyn"),
        "SessionAppRuntime must not use Box<dyn>"
    );
    assert!(
        app_source.contains("worker_index: usize"),
        "SessionAppRuntime must have worker_index field"
    );
    assert!(
        app_source.contains("seg: S"),
        "SessionAppRuntime must have seg field"
    );
}

#[test]
fn insert_session_with_id_is_generic_over_segment() {
    let runtime_source = include_str!("../src/session/runtime.rs");

    let fn_pos = runtime_source
        .find("fn insert_session_with_id")
        .expect("insert_session_with_id must be defined");
    let preceding = &runtime_source[..fn_pos];
    let last_impl = preceding
        .rfind("impl<")
        .expect("insert_session_with_id must live inside an impl block");
    let impl_block = &preceding[last_impl..];
    assert!(
        impl_block.contains("Seg: Segment") || impl_block.contains("Seg,"),
        "insert_session_with_id must be in a generic Seg impl block, not a Local-only block; got impl header: {impl_block:?}"
    );
    assert!(
        runtime_source.contains("create_app_session"),
        "insert_session_with_id must call SessionAppRuntime::create_app_session"
    );
    assert!(
        !runtime_source.contains("Box<dyn"),
        "SessionDriverRuntime must not use Box<dyn>"
    );
    assert!(
        !runtime_source.contains("SessionBackendOps"),
        "SessionDriverRuntime must not reference SessionBackendOps trait"
    );
    assert!(
        runtime_source.contains("new_svm"),
        "Svm path needs a new_svm constructor"
    );
}

#[test]
fn session_app_runtime_has_local_and_svm_impl_blocks() {
    let app_source = include_str!("../src/session/app.rs");

    assert!(
        app_source.contains("impl SessionAppRuntime<Local>"),
        "SessionAppRuntime must have a Local impl block with notify methods"
    );
    assert!(
        app_source.contains("impl SessionAppRuntime<Svm>"),
        "SessionAppRuntime must have an Svm impl block with notify methods"
    );
    assert!(
        app_source.contains(".fire()"),
        "SessionAppRuntime<Svm> must use fire() for wake calls"
    );
    assert!(
        !app_source.contains("Box<dyn"),
        "app.rs must not use Box<dyn>"
    );
}

#[test]
fn ensure_tcp_session_queue_dispatches_to_svm() {
    let tcp_mod_source = include_str!("../src/transport/tcp/mod.rs");

    assert!(
        tcp_mod_source.contains("new_svm"),
        "ensure_tcp_session_queue must use new_svm() for Svm backend"
    );
    assert!(
        tcp_mod_source.contains("new("),
        "ensure_tcp_session_queue must use new() for Local backend"
    );
}
