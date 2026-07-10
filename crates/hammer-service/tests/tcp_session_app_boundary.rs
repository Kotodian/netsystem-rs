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
fn session_rx_delivery_models_legal_outcomes() {
    let runtime_source = include_str!("../src/session/runtime.rs");
    let tcp_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        runtime_source.contains("enum RxDelivery"),
        "session runtime must model RX enqueue results with RxDelivery"
    );
    assert!(
        runtime_source.contains("struct OooSpan"),
        "session runtime must model OOO spans explicitly"
    );
    assert!(
        runtime_source.contains("NonZeroU32"),
        "RxDelivery accepted-byte facts must use non-zero domain values"
    );
    assert!(
        !runtime_source.contains("struct SessionRxEnqueue"),
        "old SessionRxEnqueue field bag should be removed"
    );
    assert!(
        runtime_source.contains("size_of::<RxDelivery>() <="),
        "session runtime must keep a size guard for the hot-path RxDelivery result"
    );
    assert!(
        tcp_source.contains("RxDelivery::"),
        "TCP established path must branch on RxDelivery outcomes"
    );
    assert!(
        !tcp_source.contains("enqueue.accepted_len"),
        "TCP established path must stop reading accepted_len from the old field bag"
    );
}

#[test]
fn established_rx_path_does_not_requery_session_rx_capacity() {
    let established_source = include_str!("../src/transport/tcp/established.rs");

    assert!(
        !established_source.contains("queue.rx_available_len(session_id)"),
        "established RX path must use RxDelivery rx_available facts instead of re-querying session RX capacity"
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
fn session_creation_is_generic_over_segment_and_tcp_owned() {
    let runtime_source = include_str!("../src/session/runtime.rs");
    let tcp_source = include_str!("../src/transport/tcp/mod.rs");

    let fn_pos = runtime_source
        .find("fn insert_session_with_transport")
        .expect("insert_session_with_transport must be defined");
    let preceding = &runtime_source[..fn_pos];
    let last_impl = preceding
        .rfind("impl<")
        .expect("insert_session_with_transport must live inside an impl block");
    let impl_block = &preceding[last_impl..];
    assert!(
        impl_block.contains("Seg: Segment") || impl_block.contains("Seg,"),
        "insert_session_with_transport must be generic over Seg; got impl header: {impl_block:?}"
    );
    assert!(
        runtime_source.contains("create_app_session"),
        "generic session creation must call SessionAppRuntime::create_app_session"
    );
    assert!(
        tcp_source.contains("fn insert_session_with_id"),
        "TCP must own its connection-aware session insertion entry point"
    );
    assert!(
        tcp_source.contains("transports.0.insert_connection"),
        "TCP session insertion must place connections in TcpWorker storage"
    );
    assert!(
        tcp_source.contains("self.insert_session_with_transport("),
        "TCP session insertion must delegate generic lifecycle creation to session runtime"
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
    assert!(
        !tcp_mod_source.contains("pub fn register_tcp_input<C, Seg>"),
        "TcpMain must not expose a generic segment that can disagree with the configured backend"
    );
}
