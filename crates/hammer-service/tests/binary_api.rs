use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, PluginError,
    PluginMain, RuntimeRegistry,
};
use hammer_service::binary_api::{
    BinaryApiClient, BinaryApiError, BinaryApiMain, BinaryApiReply, BinaryApiRequest,
    BinaryApiStatus,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static ECHO_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);

#[derive(Clone, PartialEq, Message)]
struct EchoRequest {
    #[prost(string, tag = "1")]
    text: String,
}

#[derive(Clone, PartialEq, Message)]
struct EchoReply {
    #[prost(string, tag = "1")]
    text: String,
    #[prost(string, tag = "2")]
    thread: String,
}

#[hammer_component_macros::binary_api(name = "test.echo")]
fn echo(request: EchoRequest) -> EchoReply {
    ECHO_BARRIER_SEEN.store(
        Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
        Ordering::SeqCst,
    );
    EchoReply {
        text: request.text,
        thread: format!("{:?}", std::thread::current().id()),
    }
}

#[derive(Clone, PartialEq, Message)]
struct PanicRequest {}

#[derive(Clone, PartialEq, Message)]
struct PanicReply {}

#[hammer_component_macros::binary_api(name = "test.panic")]
fn panic_method(_: PanicRequest) -> PanicReply {
    panic!("Binary API method panic")
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
    session_transports = [];
    session_apps = [];
    binary_api_methods = [__BINARY_API_ECHO, __BINARY_API_PANIC_METHOD];
);

struct CurrentEngine;

impl Drop for CurrentEngine {
    fn drop(&mut self) {
        Engine::uninstall_current();
    }
}

fn engine() -> Engine {
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity: 256,
        buffer_slots: 4,
        frame_slots: 4,
        ..DataPlaneBufferConfig::default()
    };
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers });
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());
    engine
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    engine
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    engine
}

fn socket_path() -> PathBuf {
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hammer-binary-api-{}-{sequence}.sock",
        std::process::id()
    ))
}

async fn call(stream: &mut tokio::net::UnixStream, request: BinaryApiRequest) -> BinaryApiReply {
    let request = request.encode_to_vec();
    stream
        .write_all(&(request.len() as u32).to_be_bytes())
        .await
        .expect("write request length");
    stream
        .write_all(&request)
        .await
        .expect("write request payload");

    let mut length = [0_u8; size_of::<u32>()];
    stream
        .read_exact(&mut length)
        .await
        .expect("read reply length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    stream
        .read_exact(&mut reply)
        .await
        .expect("read reply payload");
    BinaryApiReply::decode(reply.as_slice()).expect("decode Binary API reply")
}

#[tokio::test(flavor = "current_thread")]
async fn protobuf_methods_dispatch_on_the_main_thread_over_unix_socket() {
    let mut engine = engine();
    engine.install_current();
    let current = CurrentEngine;
    let path = socket_path();
    let main = Arc::new(BinaryApiMain::bind(&path, 64 * 1024).expect("bind Binary API"));
    let server = tokio::spawn(Arc::clone(&main).serve());
    let mut stream = tokio::net::UnixStream::connect(&path)
        .await
        .expect("connect Binary API");
    let expected_thread = format!("{:?}", std::thread::current().id());
    ECHO_BARRIER_SEEN.store(false, Ordering::SeqCst);

    let reply = call(
        &mut stream,
        BinaryApiRequest {
            context: 41,
            method: "test.echo".to_owned(),
            payload: EchoRequest {
                text: "hammer".to_owned(),
            }
            .encode_to_vec(),
        },
    )
    .await;
    assert_eq!(reply.context, 41);
    assert_eq!(reply.status, BinaryApiStatus::Ok as i32);
    let echo = EchoReply::decode(reply.payload.as_slice()).expect("decode echo reply");
    assert_eq!(echo.text, "hammer");
    assert_eq!(echo.thread, expected_thread);
    assert!(ECHO_BARRIER_SEEN.load(Ordering::SeqCst));

    let missing = call(
        &mut stream,
        BinaryApiRequest {
            context: 42,
            method: "test.missing".to_owned(),
            payload: Vec::new(),
        },
    )
    .await;
    assert_eq!(missing.context, 42);
    assert_eq!(missing.status, BinaryApiStatus::MethodMissing as i32);

    let invalid = call(
        &mut stream,
        BinaryApiRequest {
            context: 43,
            method: "test.echo".to_owned(),
            payload: vec![0xff],
        },
    )
    .await;
    assert_eq!(invalid.context, 43);
    assert_eq!(invalid.status, BinaryApiStatus::InvalidRequest as i32);

    let panicked = call(
        &mut stream,
        BinaryApiRequest {
            context: 44,
            method: "test.panic".to_owned(),
            payload: PanicRequest {}.encode_to_vec(),
        },
    )
    .await;
    assert_eq!(panicked.context, 44);
    assert_eq!(panicked.status, BinaryApiStatus::MethodPanicked as i32);

    drop(stream);

    server.abort();
    server.await.expect_err("server task aborted");
    drop(main);
    drop(current);
    drop(engine);
    assert!(!path.exists(), "Binary API socket must be removed on drop");
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_client_preserves_method_payload_context_and_status() {
    let mut engine = engine();
    engine.install_current();
    let current = CurrentEngine;
    let path = socket_path();
    let main = Arc::new(BinaryApiMain::bind(&path, 64 * 1024).expect("bind Binary API"));
    let server = tokio::spawn(Arc::clone(&main).serve());
    let client_path = path.clone();
    let expected_thread = format!("{:?}", std::thread::current().id());

    let (echo, missing) = tokio::task::spawn_blocking(move || {
        let mut client = BinaryApiClient::connect(client_path).expect("connect blocking client");
        let payload = client
            .call(
                "test.echo",
                &EchoRequest {
                    text: "hammer".to_owned(),
                }
                .encode_to_vec(),
            )
            .expect("call echo through blocking client");
        let echo = EchoReply::decode(payload.as_slice()).expect("decode echo payload");
        let missing = client
            .call("test.missing", &[])
            .expect_err("missing Binary API method is rejected");
        (echo, missing)
    })
    .await
    .expect("join blocking Binary API client");

    assert_eq!(echo.text, "hammer");
    assert_eq!(echo.thread, expected_thread);
    assert!(matches!(
        missing,
        BinaryApiError::ClientRejected { method, status }
            if method == "test.missing" && status == BinaryApiStatus::MethodMissing
    ));

    server.abort();
    server.await.expect_err("server task aborted");
    drop(main);
    drop(current);
    drop(engine);
    assert!(!path.exists(), "Binary API socket must be removed on drop");
}

#[test]
fn bind_reclaims_a_stale_socket_and_drop_removes_its_own_path() {
    let path = socket_path();
    let stale = std::os::unix::net::UnixListener::bind(&path).expect("bind stale socket");
    drop(stale);

    let main = BinaryApiMain::bind(&path, 64 * 1024).expect("replace stale Binary API socket");
    assert!(path.exists());
    drop(main);
    assert!(!path.exists(), "Binary API socket must be removed on drop");
}

#[test]
fn duplicate_method_registration_is_rejected() {
    let mut plugins = PluginMain::default();
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    plugins.register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);

    assert!(matches!(
        plugins.binary_api_method("test.echo"),
        Err(PluginError::BinaryApiMethodDuplicate { name }) if name == "test.echo"
    ));
}
