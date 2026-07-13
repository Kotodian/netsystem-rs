#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use hammer_infra::pool::Index;
use hammer_runtime::{DataWorkerId, File, FileFunctions, FileMain};

struct RegisteredSocket {
    index: Index,
    raw_fd: RawFd,
    peer: Option<UnixStream>,
}

impl RegisteredSocket {
    fn register(
        files: &mut FileMain,
        worker: DataWorkerId,
        description: &str,
        private_data: u64,
        functions: FileFunctions,
    ) -> Self {
        let (registered, peer) = UnixStream::pair().expect("create socket pair");
        let fd = OwnedFd::from(registered);
        let raw_fd = fd.as_raw_fd();
        let index = files
            .add(File::new(
                Arc::new(fd),
                worker,
                description.to_owned(),
                private_data,
                functions,
            ))
            .expect("register socket File");
        Self {
            index,
            raw_fd,
            peer: Some(peer),
        }
    }

    fn make_readable(&mut self) {
        self.peer
            .as_mut()
            .expect("socket peer is open")
            .write_all(&[1])
            .expect("make socket readable");
    }

    fn close_peer(&mut self) {
        drop(self.peer.take());
    }
}

#[test]
fn deleted_file_index_does_not_resolve_after_pool_slot_reuse() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let first =
        RegisteredSocket::register(&mut files, worker, "first", 0, FileFunctions::default());

    assert!(files.delete(first.index).expect("delete first file"));
    assert!(files.get(first.index).is_none());

    let replacement = RegisteredSocket::register(
        &mut files,
        worker,
        "replacement",
        0,
        FileFunctions::default(),
    );

    assert_eq!(replacement.index.slot(), first.index.slot());
    assert_ne!(replacement.index.generation(), first.index.generation());
    assert!(files.get(first.index).is_none());
    assert_eq!(
        files
            .get(replacement.index)
            .expect("current file")
            .description(),
        "replacement"
    );
}

#[test]
fn readable_file_dispatches_on_its_polling_worker() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let mut socket = RegisteredSocket::register(
        &mut files,
        worker,
        "readable",
        41,
        FileFunctions {
            read: Some(|file| {
                file.set_private_data(file.private_data() + 1);
                Ok(())
            }),
            ..FileFunctions::default()
        },
    );

    socket.make_readable();
    assert_eq!(files.poll().expect("poll FileMain"), 1);

    let file = files.get(socket.index).expect("registered file");
    assert_eq!(file.polling_worker(), worker);
    assert_eq!(file.private_data(), 42);
    assert_eq!(file.read_events(), 1);
    assert_eq!(file.write_events(), 0);
    assert_eq!(file.error_events(), 0);
}

#[test]
fn readable_file_dispatches_across_repeated_readiness_cycles() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let mut socket = RegisteredSocket::register(
        &mut files,
        worker,
        "repeated readable",
        0,
        FileFunctions {
            read: Some(|file| {
                let mut byte = 0_u8;
                // SAFETY: `byte` is writable for one byte and File retains the
                // live descriptor for the duration of callback dispatch.
                let count = unsafe {
                    libc::read(
                        file.fd(),
                        std::ptr::from_mut(&mut byte).cast::<libc::c_void>(),
                        1,
                    )
                };
                assert_eq!(count, 1);
                file.set_private_data(file.private_data() + 1);
                Ok(())
            }),
            ..FileFunctions::default()
        },
    );

    socket.make_readable();
    assert_eq!(files.poll().expect("poll first readiness"), 1);
    socket.make_readable();
    assert_eq!(files.poll().expect("poll second readiness"), 1);

    let file = files.get(socket.index).expect("registered file");
    assert_eq!(file.private_data(), 2);
    assert_eq!(file.read_events(), 2);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_eventfd_readiness_dispatches_through_file_main() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    // SAFETY: eventfd returns a fresh descriptor or -1 with errno set.
    let raw_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    assert!(raw_fd >= 0, "create eventfd: {}", std::io::Error::last_os_error());
    // SAFETY: ownership of the fresh eventfd descriptor is transferred once.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    let index = files
        .add(File::new(
            Arc::new(fd),
            worker,
            "linux eventfd".to_owned(),
            0,
            FileFunctions {
                read: Some(|file| {
                    let mut value = 0_u64;
                    // SAFETY: `value` is writable for eight bytes and File
                    // retains the descriptor during callback dispatch.
                    let count = unsafe {
                        libc::read(
                            file.fd(),
                            std::ptr::from_mut(&mut value).cast::<libc::c_void>(),
                            std::mem::size_of::<u64>(),
                        )
                    };
                    assert_eq!(count, std::mem::size_of::<u64>() as isize);
                    file.set_private_data(value);
                    Ok(())
                }),
                ..FileFunctions::default()
            },
        ))
        .expect("register eventfd");

    let value = 7_u64;
    // SAFETY: `value` is readable for eight bytes and File owns the live fd.
    let count = unsafe {
        libc::write(
            raw_fd,
            std::ptr::from_ref(&value).cast::<libc::c_void>(),
            std::mem::size_of::<u64>(),
        )
    };
    assert_eq!(count, std::mem::size_of::<u64>() as isize);
    assert_eq!(files.poll().expect("poll eventfd readiness"), 1);

    let file = files.get(index).expect("registered eventfd");
    assert_eq!(file.private_data(), value);
    assert_eq!(file.read_events(), 1);
}

#[test]
fn write_interest_changes_without_replacing_file_index() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let socket = RegisteredSocket::register(
        &mut files,
        worker,
        "writable",
        0,
        FileFunctions {
            write: Some(|file| {
                file.set_private_data(7);
                Ok(())
            }),
            ..FileFunctions::default()
        },
    );

    assert!(
        !files
            .set_data_available_to_write(socket.index, true)
            .expect("enable write interest")
    );
    assert_eq!(files.poll().expect("poll FileMain"), 1);

    let file = files.get(socket.index).expect("same registered file");
    assert_eq!(file.private_data(), 7);
    assert_eq!(file.write_events(), 1);
}

#[test]
fn error_callback_runs_before_delete_closes_the_descriptor() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let mut socket = RegisteredSocket::register(
        &mut files,
        worker,
        "peer close",
        0,
        FileFunctions {
            error: Some(|file| {
                file.set_private_data(1);
                Ok(())
            }),
            ..FileFunctions::default()
        },
    );

    socket.close_peer();
    assert_eq!(files.poll().expect("poll peer close"), 1);
    let file = files.get(socket.index).expect("registered file");
    assert_eq!(file.private_data(), 1);
    assert_eq!(file.error_events(), 1);

    assert!(files.delete(socket.index).expect("delete file"));
    // SAFETY: F_GETFD only queries the descriptor number. EBADF proves the
    // last OwnedFd was released after readiness deletion.
    assert_eq!(unsafe { libc::fcntl(socket.raw_fd, libc::F_GETFD) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EBADF)
    );
}

#[test]
fn queued_event_for_deleted_generation_does_not_reach_reused_slot() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let mut stale = RegisteredSocket::register(
        &mut files,
        worker,
        "stale",
        0,
        FileFunctions {
            read: Some(|file| {
                file.set_private_data(1);
                Ok(())
            }),
            ..FileFunctions::default()
        },
    );
    stale.make_readable();
    assert!(files.delete(stale.index).expect("delete first file"));

    let current = RegisteredSocket::register(
        &mut files,
        worker,
        "current",
        0,
        FileFunctions {
            read: Some(|file| {
                file.set_private_data(2);
                Ok(())
            }),
            ..FileFunctions::default()
        },
    );

    assert_eq!(current.index.slot(), stale.index.slot());
    assert_ne!(current.index.generation(), stale.index.generation());
    assert_eq!(files.poll().expect("poll FileMain"), 0);
    let file = files.get(current.index).expect("replacement file");
    assert_eq!(file.private_data(), 0);
    assert_eq!(file.read_events(), 0);
}

#[test]
fn unhandled_error_deletes_file_and_closes_descriptor() {
    let worker = DataWorkerId::new(0);
    let mut files = FileMain::new(worker).expect("create FileMain");
    let mut socket = RegisteredSocket::register(
        &mut files,
        worker,
        "unhandled peer close",
        0,
        FileFunctions {
            read: Some(|_| Ok(())),
            ..FileFunctions::default()
        },
    );

    socket.close_peer();
    assert_eq!(files.poll().expect("poll peer close"), 0);
    assert!(files.get(socket.index).is_none());
    // SAFETY: F_GETFD only queries the descriptor number.
    assert_eq!(unsafe { libc::fcntl(socket.raw_fd, libc::F_GETFD) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EBADF)
    );
}
