//! Control-plane `AsyncFileMain` seam.
//!
//! `AsyncFileMain` owns a plain `FileMain` and the Tokio `AsyncFd` created
//! from only that FileMain's duplicated backend wake descriptor. Managed
//! sockets remain FileMain-owned descriptors registered exclusively with
//! kqueue/io_uring; readiness is awaited on the Tokio main thread and
//! dispatched synchronously there, mirroring the data-worker wake order.

use std::io::Write;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Mutex;
use std::thread::ThreadId;
use std::time::Duration;

use hammer_infra::pool::Index;
use hammer_runtime::{
    AsyncFileMain, FILE_MAIN, File, FileFunctions, FileMain, NodeRuntime, RuntimeError,
    RuntimeResult,
};

/// Thread that observed each readiness dispatch, and the drained byte count.
static CALLBACK_TX: Mutex<Option<std::sync::mpsc::Sender<(ThreadId, u64)>>> = Mutex::new(None);

/// Each test drives a current-thread Tokio runtime; serializing keeps the
/// shared callback channel disjoint.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

fn set_nonblocking(fd: RawFd) {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(flags >= 0, "F_GETFL failed");
    let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    assert_eq!(result, 0, "F_SETFL failed");
}

fn drain_readiness(file: &File) -> u64 {
    let mut total = 0;
    let mut buffer = [0u8; 512];
    loop {
        // SAFETY: buffer is writable for its length; the File owns the
        // descriptor for the duration of this synchronous call.
        let n = unsafe { libc::read(file.fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if n > 0 {
            total += n as u64;
            continue;
        }
        if n == 0 {
            break;
        }
        match std::io::Error::last_os_error().kind() {
            std::io::ErrorKind::Interrupted => continue,
            _ => break,
        }
    }
    total
}

fn report_callback(_graph: &NodeRuntime, file: &mut File) -> RuntimeResult<()> {
    let drained = drain_readiness(file);
    if let Some(tx) = CALLBACK_TX.lock().expect("callback transmitter").as_ref() {
        let _ = tx.send((std::thread::current().id(), drained));
    }
    Ok(())
}

fn failing_callback(_graph: &NodeRuntime, _file: &mut File) -> RuntimeResult<()> {
    Err(RuntimeError::lifecycle(
        "AsyncFileMain readiness test",
        "injected File callback failure",
    ))
}

fn control_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("control-plane Tokio runtime")
}

fn test_file_main() -> &'static FileMain {
    FILE_MAIN.get_or_init(|| FileMain::new().expect("create global FileMain"))
}

/// Registers one readiness File on a socketpair and returns its Index and the
/// write end, which the caller keeps alive for the whole test.
fn register_readiness_file(
    file_main: &FileMain,
    description: &str,
    functions: FileFunctions,
) -> (Index, UnixStream) {
    let (read_end, write_end) = UnixStream::pair().expect("socketpair");
    set_nonblocking(read_end.as_raw_fd());
    let index = file_main
        .add(File::new(
            OwnedFd::from(read_end),
            description.to_owned(),
            0,
            functions,
        ))
        .expect("register File");
    (index, write_end)
}

#[test]
fn next_ready_dispatches_registered_files_on_the_tokio_main_thread() {
    let _serial = TEST_SERIAL.lock().expect("serialize runtime tests");
    let main_thread = std::thread::current().id();
    let (tx, rx) = std::sync::mpsc::channel();
    *CALLBACK_TX.lock().expect("callback transmitter") = Some(tx);

    let file_main = test_file_main();
    let (index, mut write_end) = register_readiness_file(
        file_main,
        "AsyncFileMain readiness test",
        FileFunctions {
            read: Some(report_callback),
            ..FileFunctions::default()
        },
    );
    write_end.write_all(b"x").expect("write readiness byte");

    // Keep the write end alive: closing the peer merges EVFILT_READ|EV_EOF,
    // which FileMain treats as an error-ready File with no error callback
    // and deletes without dispatch.
    let runtime = control_runtime();
    let (async_file_main, dispatched) = runtime.block_on(async move {
        let mut async_file_main = AsyncFileMain::new().expect("create AsyncFileMain");
        let dispatched = async_file_main
            .next_ready(&NodeRuntime::default())
            .await
            .expect("dispatch File readiness");
        (async_file_main, dispatched)
    });

    assert_eq!(dispatched, 1, "read callback dispatched exactly once");
    let (callback_thread, drained) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("File callback must fire through AsyncFileMain");
    assert_eq!(
        callback_thread, main_thread,
        "callback ran on the Tokio main thread"
    );
    assert_eq!(drained, 1, "readiness byte was drained by the callback");
    assert_eq!(
        async_file_main.file_main().file_event_counts(index),
        Some((1, 0, 0)),
        "File read callback dispatched exactly once"
    );
}

#[test]
fn next_ready_dispatches_repeated_wakes_without_reregistration() {
    let _serial = TEST_SERIAL.lock().expect("serialize runtime tests");
    let file_main = test_file_main();
    let (_index, mut write_end) = register_readiness_file(
        file_main,
        "repeated AsyncFileMain readiness test",
        FileFunctions {
            read: Some(report_callback),
            ..FileFunctions::default()
        },
    );
    write_end
        .write_all(b"x")
        .expect("write first readiness byte");

    let runtime = control_runtime();
    let dispatched = runtime.block_on(async move {
        let mut async_file_main = AsyncFileMain::new().expect("create AsyncFileMain");
        let first = async_file_main
            .next_ready(&NodeRuntime::default())
            .await
            .expect("first wake dispatch");
        write_end
            .write_all(b"x")
            .expect("write second readiness byte");
        let second = async_file_main
            .next_ready(&NodeRuntime::default())
            .await
            .expect("second wake dispatch");
        (first, second)
    });

    assert_eq!(
        dispatched,
        (1, 1),
        "each wake dispatched exactly once without re-registration"
    );
}

#[test]
fn next_ready_propagates_callback_errors() {
    let _serial = TEST_SERIAL.lock().expect("serialize runtime tests");
    let file_main = test_file_main();
    let (_index, mut write_end) = register_readiness_file(
        file_main,
        "failing AsyncFileMain readiness test",
        FileFunctions {
            read: Some(failing_callback),
            ..FileFunctions::default()
        },
    );
    write_end.write_all(b"x").expect("write readiness byte");

    let runtime = control_runtime();
    let error = runtime.block_on(async move {
        let mut async_file_main = AsyncFileMain::new().expect("create AsyncFileMain");
        async_file_main.next_ready(&NodeRuntime::default()).await
    });

    assert!(
        matches!(
            error,
            Err(RuntimeError::Lifecycle { stage, .. })
                if stage == "AsyncFileMain readiness test"
        ),
        "File callback error propagated out of next_ready"
    );
}

#[test]
fn async_file_main_requires_tokio_context() {
    let _serial = TEST_SERIAL.lock().expect("serialize runtime tests");
    let _ = test_file_main();
    // No Tokio runtime context on this test thread: adapter construction must
    // return an error rather than panic.
    assert!(
        AsyncFileMain::new().is_err(),
        "AsyncFileMain requires a Tokio runtime"
    );
}
