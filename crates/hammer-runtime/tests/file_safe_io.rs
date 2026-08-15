//! Safe nonblocking socket operations on FileMain-owned descriptors.
//!
//! These are the only socket primitives the Binary API server (or any
//! control-plane caller) uses: accept/read/write through a duplicated
//! descriptor, with a typed would-block/closed outcome instead of raw fd I/O.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_runtime::file::{FileFunctions, FileIoStatus, FileMain};

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn socket_path() -> PathBuf {
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hammer-runtime-file-safe-io-{}-{sequence}.sock",
        std::process::id()
    ))
}

fn listener(path: &PathBuf) -> UnixListener {
    let listener = UnixListener::bind(path).expect("bind safe-io test listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    listener
}

fn file_main() -> FileMain {
    FileMain::new().expect("create FileMain")
}

#[test]
fn accept_read_write_round_trip_through_registered_descriptors() {
    let path = socket_path();
    let mut main = file_main();
    let listener_index = main
        .add_listener(
            listener(&path),
            "safe-io listener".to_owned(),
            1,
            FileFunctions::default(),
        )
        .expect("register listener");

    assert_eq!(
        main.accept(
            listener_index,
            "safe-io client".to_owned(),
            2,
            FileFunctions::default()
        )
        .expect("accept with no pending connection"),
        None
    );

    let mut client = UnixStream::connect(&path).expect("connect client");
    let client_index = main
        .accept(
            listener_index,
            "safe-io client".to_owned(),
            2,
            FileFunctions::default(),
        )
        .expect("accept pending connection")
        .expect("accepted connection");

    client.write_all(b"hello").expect("write request");
    let mut buffer = [0_u8; 16];
    assert_eq!(
        main.read_some(client_index, &mut buffer)
            .expect("read request"),
        FileIoStatus::Progress(5)
    );
    assert_eq!(&buffer[..5], b"hello");

    main.write_some(client_index, b"world")
        .expect("write reply");
    let mut reply = [0_u8; 5];
    client.read_exact(&mut reply).expect("read reply");
    assert_eq!(&reply, b"world");

    assert_eq!(
        main.read_some(client_index, &mut buffer)
            .expect("read empty"),
        FileIoStatus::WouldBlock,
        "an idle socket must report would-block, not an error"
    );
}

#[test]
fn closed_peer_reads_and_writes_report_closed() {
    let path = socket_path();
    let mut main = file_main();
    let listener_index = main
        .add_listener(
            listener(&path),
            "safe-io listener".to_owned(),
            1,
            FileFunctions::default(),
        )
        .expect("register listener");
    let client = UnixStream::connect(&path).expect("connect client");
    let client_index = main
        .accept(
            listener_index,
            "safe-io client".to_owned(),
            2,
            FileFunctions::default(),
        )
        .expect("accept pending connection")
        .expect("accepted connection");

    drop(client);
    let mut buffer = [0_u8; 16];
    assert_eq!(
        main.read_some(client_index, &mut buffer)
            .expect("read after peer close"),
        FileIoStatus::Closed
    );
    assert_eq!(
        main.write_some(client_index, b"gone")
            .expect("write after peer close"),
        FileIoStatus::Closed
    );
}

#[test]
fn oversized_write_reports_progress_then_would_block_until_peer_reads() {
    let path = socket_path();
    let mut main = file_main();
    let listener_index = main
        .add_listener(
            listener(&path),
            "safe-io listener".to_owned(),
            1,
            FileFunctions::default(),
        )
        .expect("register listener");
    let mut client = UnixStream::connect(&path).expect("connect client");
    let client_index = main
        .accept(
            listener_index,
            "safe-io client".to_owned(),
            2,
            FileFunctions::default(),
        )
        .expect("accept pending connection")
        .expect("accepted connection");

    let payload = vec![0x5a_u8; 1 << 20];
    let mut sent = 0;
    let mut blocked = false;
    while sent < payload.len() {
        match main
            .write_some(client_index, &payload[sent..])
            .expect("write payload")
        {
            FileIoStatus::Progress(n) => sent += n,
            FileIoStatus::WouldBlock => {
                blocked = true;
                break;
            }
            FileIoStatus::Closed => panic!("live peer must not report Closed"),
        }
    }
    assert!(blocked, "a 1 MiB payload must outrun the socket buffer");

    // The peer drains the kernel buffer; every accepted byte arrives.
    let mut sink = vec![0_u8; 64 * 1024];
    let mut received = 0;
    while received < sent {
        match client.read(&mut sink) {
            Ok(0) => panic!("peer closed before draining the write buffer"),
            Ok(n) => received += n,
            Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(source) => panic!("drain failed: {source}"),
        }
    }
    assert_eq!(received, sent, "the peer must receive every accepted byte");
}
