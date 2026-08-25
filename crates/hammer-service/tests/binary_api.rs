//! Binary API server seam: the `binary-api` Process Node serves the Unix
//! socket registered by `binary_api_init`, mirroring VPP's `vl_api_clnt_node`.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use hammer_runtime::RuntimeRegistry;
use hammer_runtime::config::Worker;
use hammer_runtime::{Engine, EnginePool, FILE_MAIN, FileMain, PluginError, PluginMain};
use hammer_service::binary_api::{
    BinaryApiClient, BinaryApiError, BinaryApiReply, BinaryApiRequest, BinaryApiStatus,
    DEFAULT_MAX_FRAME_BYTES,
};
use prost::Message;

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static BINARY_API_SERIAL: Mutex<()> = Mutex::new(());
static ECHO_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);

fn binary_api_serial() -> MutexGuard<'static, ()> {
    BINARY_API_SERIAL
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

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

#[derive(Clone, PartialEq, Message)]
struct LargeRequest {}

/// An 8 MiB reply: larger than any Unix socket receive buffer, so a client
/// that stops reading forces the server's write to block (VPP socket write
/// model: write interest stays armed until the drain completes).
#[derive(Clone, PartialEq, Message)]
struct LargeReply {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

#[hammer_component_macros::binary_api(name = "test.large")]
fn large(_: LargeRequest) -> LargeReply {
    LargeReply {
        data: vec![0xAB; 8 * 1024 * 1024],
    }
}

static MP_SAFE_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);
static MP_SAFE_CALL_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, PartialEq, Message)]
struct MpSafeRequest {
    #[prost(string, tag = "1")]
    text: String,
}

#[derive(Clone, PartialEq, Message)]
struct MpSafeReply {
    #[prost(string, tag = "1")]
    text: String,
    #[prost(string, tag = "2")]
    thread: String,
    #[prost(uint32, tag = "3")]
    sequence: u32,
}

#[hammer_component_macros::binary_api(name = "test.mp_safe", mp_safe)]
fn mp_safe_read(request: MpSafeRequest) -> MpSafeReply {
    MP_SAFE_BARRIER_SEEN.store(
        Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
        Ordering::SeqCst,
    );
    MpSafeReply {
        text: request.text,
        thread: format!("{:?}", std::thread::current().id()),
        sequence: MP_SAFE_CALL_COUNT.fetch_add(1, Ordering::SeqCst) + 1,
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
    session_transports = [];
    session_apps = [];
    binary_api_methods = [
        __BINARY_API_ECHO,
        __BINARY_API_PANIC_METHOD,
        __BINARY_API_LARGE,
        __BINARY_API_MP_SAFE_READ,
    ];
);

fn socket_path() -> PathBuf {
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hammer-binary-api-{}-{sequence}.sock",
        std::process::id()
    ))
}

fn stats_socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("hammer-stats-runtime-{}.sock", std::process::id()))
}

/// Builds an engine whose main loop has entered: the `binary_api_init`
/// function bound the socket at `path`, and the `binary-api` Process Node is
/// started with the given maximum frame size.
///
/// Socket removal is not asserted here after `drop(engine)`: the Data Worker
/// thread holds the registry (and with it the `BinaryApiMain` capability) for
/// its lifetime, so the capability drops only at process exit. Capability-drop
/// cleanup is asserted directly by
/// `bind_reclaims_a_stale_socket_and_drop_removes_its_own_path`.
fn engine_with_binary_api(
    path: &Path,
    max_frame_bytes: usize,
) -> (EnginePool, tokio::runtime::Runtime) {
    let engine = Engine::new_configured(RuntimeRegistry::new(), Worker::default())
        .expect("configure test engine");
    let runtime = main_runtime();
    let mut pool = EnginePool::new(engine, &runtime).expect("engine pool");
    pool.main_engine_mut()
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    pool.main_engine_mut()
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    let stats_path = stats_socket_path();
    let config = format!(
        "[stats]\nsocket_path = \"{}\"\n\n[binary_api]\nsocket_path = \"{}\"\nmax_frame_bytes = {}\n",
        stats_path.display(),
        path.display(),
        max_frame_bytes
    );
    pool.main_loop_enter(&[], &config, &runtime)
        .expect("enter main loop");
    (pool, runtime)
}

/// Uninstalls the current Engine before the test's engine drops, so the
/// main-thread thread-local never outlives its Engine.
struct CurrentEngine;

impl CurrentEngine {
    /// Records the engine's final address. `engine_with_binary_api` installs
    /// the helper frame's address (via `EnginePool::main_loop_enter`), which
    /// is dangling once the engine is returned by value and the frame pops,
    /// so the test must re-install after the move.
    fn install(engine: &mut Engine) -> Self {
        engine.install_current();
        CurrentEngine
    }
}

impl Drop for CurrentEngine {
    fn drop(&mut self) {
        Engine::uninstall_current();
    }
}

fn main_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build main runtime")
}

/// Runs the blocking Binary API client on a plain thread and bridges the
/// result through a oneshot. A server that never replies must fail the test
/// through the caller's deadline: a `spawn_blocking` client would instead
/// strand a tokio blocking-pool worker, which hangs `Runtime::drop` at
/// teardown. The stranded thread exits on its own once the engine drop
/// closes the listener, because the pending connection's peer vanishes.
async fn run_client<F, T>(work: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = tx.send(work());
    });
    rx.await.expect("client thread must send its result")
}

/// Opens a raw Binary API connection. The default Unix socket receive buffer
/// is far below the 8 MiB `test.large` reply, so a client that stops reading
/// forces the server's writes to block.
fn raw_client(path: &Path) -> io::Result<std::os::unix::net::UnixStream> {
    std::os::unix::net::UnixStream::connect(path)
}

/// Serializes one length-prefixed request frame (VPP `vac_write`).
fn write_frame(
    stream: &mut std::os::unix::net::UnixStream,
    method: &str,
    payload: &[u8],
) -> io::Result<()> {
    let frame = BinaryApiRequest {
        context: 1,
        method: method.to_owned(),
        payload: payload.to_vec(),
    }
    .encode_to_vec();
    stream.write_all(&(frame.len() as u32).to_be_bytes())?;
    stream.write_all(&frame)
}

/// Reads one length-prefixed reply frame (VPP `vac_read`).
fn read_reply(stream: &mut std::os::unix::net::UnixStream) -> io::Result<BinaryApiReply> {
    let mut length = [0_u8; size_of::<u32>()];
    stream.read_exact(&mut length)?;
    let mut payload = vec![0_u8; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut payload)?;
    Ok(BinaryApiReply::decode(payload.as_slice()).expect("decode Binary API reply"))
}

#[test]
fn binary_api_process_node_serves_one_request() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    let expected_thread = format!("{:?}", std::thread::current().id());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let client_path = path.clone();
            let (reply, barrier_seen) = run_client(move || {
                let mut client = BinaryApiClient::connect(client_path).expect("connect Binary API");
                ECHO_BARRIER_SEEN.store(false, Ordering::SeqCst);
                let payload = client
                    .call(
                        "test.echo",
                        &EchoRequest {
                            text: "hammer".to_owned(),
                        }
                        .encode_to_vec(),
                    )
                    .expect("call echo through the binary-api Process Node");
                let echo = EchoReply::decode(payload.as_slice()).expect("decode echo payload");
                (echo, ECHO_BARRIER_SEEN.load(Ordering::SeqCst))
            })
            .await;
            assert_eq!(reply.text, "hammer");
            assert_eq!(reply.thread, expected_thread);
            assert!(
                barrier_seen,
                "echo method must observe the worker barrier while dispatched"
            );
        })
        .await
        .expect("Binary API request completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn bind_reclaims_a_stale_socket_and_drop_removes_its_own_path() {
    let _serial = binary_api_serial();
    FILE_MAIN.get_or_init(|| FileMain::new().expect("create global FileMain"));
    let path = socket_path();
    let stale = std::os::unix::net::UnixListener::bind(&path).expect("bind stale socket");
    drop(stale);

    let main = hammer_service::binary_api::BinaryApiMain::bind(&path, 64 * 1024)
        .expect("replace stale Binary API socket");
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

#[test]
fn blocked_server_write_re_arms_when_the_client_resumes_reading() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let client_path = path.clone();
            let reply = run_client(move || {
                let mut client = raw_client(&client_path).expect("connect Binary API");
                write_frame(&mut client, "test.large", &[]).expect("write large request");
                // Hold the reply: the server's 8 MiB write must block on the
                // tiny receive buffer. Only a full drain disarms write
                // interest, so the reply must arrive once the client reads.
                std::thread::sleep(Duration::from_millis(500));
                read_reply(&mut client).expect("large reply after the write re-arms")
            })
            .await;
            assert_eq!(reply.status, BinaryApiStatus::Ok as i32);
            assert_eq!(reply.context, 1);
            let payload = LargeReply::decode(reply.payload.as_slice()).expect("decode large reply");
            assert_eq!(payload.data.len(), 8 * 1024 * 1024);
            assert!(payload.data.iter().all(|&byte| byte == 0xAB));
        })
        .await
        .expect("blocked write re-armed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn partial_frames_are_held_and_complete_frames_dispatch_in_order() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let long_text = format!("three{}", "x".repeat(8 * 1024));
            let expected_text = long_text.clone();
            let client_path = path.clone();
            let replies = run_client(move || {
                let frame3 = BinaryApiRequest {
                    context: 1,
                    method: "test.echo".to_owned(),
                    payload: EchoRequest {
                        text: long_text.clone(),
                    }
                    .encode_to_vec(),
                }
                .encode_to_vec();
                let mut client = raw_client(&client_path).expect("connect Binary API");
                write_frame(
                    &mut client,
                    "test.echo",
                    &EchoRequest {
                        text: "one".to_owned(),
                    }
                    .encode_to_vec(),
                )
                .expect("write first frame");
                write_frame(
                    &mut client,
                    "test.echo",
                    &EchoRequest {
                        text: "two".to_owned(),
                    }
                    .encode_to_vec(),
                )
                .expect("write second frame");
                // Begin a third frame and complete it only after the server
                // has processed the first chunk: the partial bytes must be
                // held until the rest arrives.
                client
                    .write_all(&(frame3.len() as u32).to_be_bytes())
                    .expect("write third prefix");
                client
                    .write_all(&frame3[..100])
                    .expect("write third partial");
                std::thread::sleep(Duration::from_millis(300));
                client
                    .write_all(&frame3[100..])
                    .expect("complete third frame");

                let mut texts = Vec::new();
                for _ in 0..3 {
                    let reply = read_reply(&mut client).expect("read reply");
                    assert_eq!(reply.status, BinaryApiStatus::Ok as i32);
                    texts.push(
                        EchoReply::decode(reply.payload.as_slice())
                            .expect("decode echo")
                            .text,
                    );
                }
                texts
            })
            .await;
            assert_eq!(
                replies,
                vec!["one".to_owned(), "two".to_owned(), expected_text]
            );
        })
        .await
        .expect("partial and burst frames completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn oversize_declared_frame_closes_the_client_and_releases_the_slot() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, 64 * 1024);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let client_path = path.clone();
            let text = run_client(move || {
                // Client A declares a frame far above the configured maximum:
                // the server closes the connection without reading the payload.
                let mut oversized = raw_client(&client_path).expect("connect oversize client");
                oversized
                    .write_all(&((1024 * 1024) as u32).to_be_bytes())
                    .expect("write oversize length prefix");
                let mut length = [0_u8; size_of::<u32>()];
                let read = oversized.read_exact(&mut length);
                assert!(
                    matches!(read, Err(error) if error.kind() == io::ErrorKind::UnexpectedEof),
                    "oversize client must be closed by the server"
                );

                // Client B is still served: the closed slot was released.
                let mut next = raw_client(&client_path).expect("connect next client");
                write_frame(
                    &mut next,
                    "test.echo",
                    &EchoRequest {
                        text: "after close".to_owned(),
                    }
                    .encode_to_vec(),
                )
                .expect("write echo after close");
                let reply = read_reply(&mut next).expect("reply after close");
                assert_eq!(reply.status, BinaryApiStatus::Ok as i32);
                EchoReply::decode(reply.payload.as_slice())
                    .expect("decode echo")
                    .text
            })
            .await;
            assert_eq!(text, "after close");
        })
        .await
        .expect("oversize close and slot release completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn partial_frame_then_disconnect_releases_the_slot() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let client_path = path.clone();
            let text = run_client(move || {
                // Client A leaves a partial frame behind and disconnects; the
                // server must close the slot on the peer's EOF.
                let partial = BinaryApiRequest {
                    context: 1,
                    method: "test.echo".to_owned(),
                    payload: vec![0x7F; 16 * 1024],
                }
                .encode_to_vec();
                let mut dropped = raw_client(&client_path).expect("connect dropping client");
                dropped
                    .write_all(&(partial.len() as u32).to_be_bytes())
                    .expect("write partial frame prefix");
                dropped
                    .write_all(&partial[..64])
                    .expect("write partial frame");
                drop(dropped);

                // A new client is served on the released slot.
                let mut next = raw_client(&client_path).expect("connect next client");
                write_frame(
                    &mut next,
                    "test.echo",
                    &EchoRequest {
                        text: "after disconnect".to_owned(),
                    }
                    .encode_to_vec(),
                )
                .expect("write echo after disconnect");
                let reply = read_reply(&mut next).expect("reply after disconnect");
                assert_eq!(reply.status, BinaryApiStatus::Ok as i32);
                EchoReply::decode(reply.payload.as_slice())
                    .expect("decode echo")
                    .text
            })
            .await;
            assert_eq!(text, "after disconnect");
        })
        .await
        .expect("disconnect cleanup completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn stalled_client_backpressure_does_not_starve_a_second_client() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(60), async {
            // Client A stops reading: six 8 MiB replies saturate the bounded
            // output budget (2 * max_frame_bytes per client) and the server's
            // writes block against the 4096-byte receive buffer.
            let a_path = path.clone();
            let (a_tx, a_rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                let mut a = raw_client(&a_path).expect("connect client A");
                for _ in 0..6 {
                    write_frame(&mut a, "test.large", &[]).expect("write large request");
                }
                std::thread::sleep(Duration::from_secs(1));
                let mut replies = Vec::with_capacity(6);
                for _ in 0..6 {
                    replies.push(read_reply(&mut a).expect("drain stalled replies"));
                }
                let _ = a_tx.send(replies);
            });

            // Client B is served while A is stalled and its output is bounded.
            let b_path = path.clone();
            let b_reply = run_client(move || {
                let mut b = raw_client(&b_path).expect("connect client B");
                write_frame(
                    &mut b,
                    "test.echo",
                    &EchoRequest {
                        text: "alive".to_owned(),
                    }
                    .encode_to_vec(),
                )
                .expect("write echo while A is stalled");
                read_reply(&mut b).expect("reply while A is stalled")
            })
            .await;
            assert_eq!(b_reply.status, BinaryApiStatus::Ok as i32);
            let echo = EchoReply::decode(b_reply.payload.as_slice()).expect("decode echo");
            assert_eq!(echo.text, "alive");

            // A's stalled queue drains once it resumes reading.
            let replies = a_rx.await.expect("client A drained");
            assert_eq!(replies.len(), 6);
            for reply in &replies {
                assert_eq!(reply.status, BinaryApiStatus::Ok as i32);
                assert_eq!(reply.context, 1);
                let payload =
                    LargeReply::decode(reply.payload.as_slice()).expect("decode large reply");
                assert_eq!(payload.data.len(), 8 * 1024 * 1024);
                assert!(payload.data.iter().all(|&byte| byte == 0xAB));
            }
        })
        .await
        .expect("stalled client drained within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn mp_safe_method_runs_on_the_main_thread_without_the_worker_barrier() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    let expected_thread = format!("{:?}", std::thread::current().id());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let client_path = path.clone();
            let (mp_safe, echo, mp_safe_barrier_seen, echo_barrier_seen) = run_client(move || {
                MP_SAFE_BARRIER_SEEN.store(false, Ordering::SeqCst);
                ECHO_BARRIER_SEEN.store(false, Ordering::SeqCst);
                let mut client = BinaryApiClient::connect(client_path).expect("connect Binary API");

                let read_payload = client
                    .call(
                        "test.mp_safe",
                        &MpSafeRequest {
                            text: "direct".to_owned(),
                        }
                        .encode_to_vec(),
                    )
                    .expect("call mp-safe method");
                let mp_safe = MpSafeReply::decode(read_payload.as_slice()).expect("decode mp-safe");

                let echo_payload = client
                    .call(
                        "test.echo",
                        &EchoRequest {
                            text: "barriered".to_owned(),
                        }
                        .encode_to_vec(),
                    )
                    .expect("call echo after mp-safe");
                let echo = EchoReply::decode(echo_payload.as_slice()).expect("decode echo");

                (
                    mp_safe,
                    echo,
                    MP_SAFE_BARRIER_SEEN.load(Ordering::SeqCst),
                    ECHO_BARRIER_SEEN.load(Ordering::SeqCst),
                )
            })
            .await;
            assert_eq!(mp_safe.text, "direct");
            assert_eq!(
                mp_safe.thread, expected_thread,
                "mp-safe handler must run on the Main Thread"
            );
            assert_eq!(
                mp_safe.sequence, 1,
                "mp-safe handler must run exactly once per request"
            );
            assert!(
                !mp_safe_barrier_seen,
                "mp-safe method must not observe the worker barrier"
            );
            assert_eq!(echo.text, "barriered");
            assert_eq!(echo.thread, expected_thread);
            assert!(
                echo_barrier_seen,
                "the default method must still observe the worker barrier after a read-only request"
            );
        })
        .await
        .expect("read-only and barriered requests completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn unknown_method_keeps_the_legacy_barriered_reply_and_dispatch_path() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let client_path = path.clone();
            let echo_barrier_seen = run_client(move || {
                ECHO_BARRIER_SEEN.store(false, Ordering::SeqCst);
                let mut client = BinaryApiClient::connect(client_path).expect("connect Binary API");
                let missing = client.call("test.does_not_exist", &[]);
                assert!(
                    matches!(
                        missing,
                        Err(BinaryApiError::ClientRejected {
                            status: BinaryApiStatus::MethodMissing,
                            ..
                        })
                    ),
                    "unknown method must reply MethodMissing, got: {missing:?}"
                );

                // A follow-up default request still dispatches under the
                // worker barrier: the unknown-method reply went through the
                // legacy barriered path and released it cleanly.
                let payload = client
                    .call(
                        "test.echo",
                        &EchoRequest {
                            text: "after missing".to_owned(),
                        }
                        .encode_to_vec(),
                    )
                    .expect("echo after unknown method");
                let _echo = EchoReply::decode(payload.as_slice()).expect("decode echo");
                ECHO_BARRIER_SEEN.load(Ordering::SeqCst)
            })
            .await;
            assert!(
                echo_barrier_seen,
                "a subsequent default request must still observe the worker barrier"
            );
        })
        .await
        .expect("unknown method and follow-up completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");
    drop(_engine_guard);
}

#[test]
fn process_node_shutdown_closes_the_listener_and_stops_dispatch() {
    let _serial = binary_api_serial();
    let path = socket_path();
    let (mut pool, runtime) = engine_with_binary_api(&path, DEFAULT_MAX_FRAME_BYTES);
    let _engine_guard = CurrentEngine::install(pool.main_engine_mut());

    // Serve one request so the node is running and owns the FileMain.
    pool.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let client_path = path.clone();
            let text = run_client(move || {
                let mut client = BinaryApiClient::connect(client_path).expect("connect Binary API");
                let payload = client
                    .call(
                        "test.echo",
                        &EchoRequest {
                            text: "warmup".to_owned(),
                        }
                        .encode_to_vec(),
                    )
                    .expect("call echo before shutdown");
                EchoReply::decode(payload.as_slice())
                    .expect("decode echo")
                    .text
            })
            .await;
            assert_eq!(text, "warmup");
        })
        .await
        .expect("warmup request completed within the deadline");
    })
    .expect("run process and control dispatchers");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("shutdown Process Nodes");

    // The listener is a process-global FileMain registration. Process Node
    // shutdown stops event consumption; Engine/capability teardown releases
    // the listener and its client descriptors.
    drop(_engine_guard);
    pool.close().expect("close Binary API engine pool");
    drop(pool);
    let client_path = path.clone();
    let error = std::os::unix::net::UnixStream::connect(&client_path)
        .expect_err("listener must close with the Process Node");
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
        ),
        "engine teardown must close the Binary API listener: {error}"
    );
}
