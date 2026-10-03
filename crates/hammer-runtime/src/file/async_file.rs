//! Thread-zero io_uring File ownership and completion delivery.

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::OnceLock;
use std::task::{Context, Poll, Waker};

use hammer_infra::pool::Pool;
use io_uring::{IoUring, opcode, squeue, types};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Notify;

use super::{GenericFile, Readiness};
use crate::NodeMain;
use crate::error::{RuntimeError, RuntimeResult};

const READ_CAPACITY: usize = 8192;
const WRITE_CAPACITY: usize = 8192;
const CANCEL_TOKEN: u64 = u64::MAX;

pub(crate) struct AsyncFileRegistration {
    index: Cell<u32>,
    socket: Cell<bool>,
    closed: Cell<bool>,
    read: Cell<Option<u32>>,
    read_data: RefCell<Vec<u8>>,
    read_offset: Cell<usize>,
    read_eof: Cell<bool>,
    read_error: RefCell<Option<io::Error>>,
    write: Cell<Option<u32>>,
    write_result: RefCell<Option<io::Result<()>>>,
    accept: Cell<Option<u32>>,
    accept_result: RefCell<Option<io::Result<OwnedFd>>>,
    read_poll: Cell<Option<u32>>,
}

impl Default for AsyncFileRegistration {
    fn default() -> Self {
        Self {
            index: Cell::new(0),
            socket: Cell::new(false),
            closed: Cell::new(false),
            read: Cell::new(None),
            read_data: RefCell::new(Vec::new()),
            read_offset: Cell::new(0),
            read_eof: Cell::new(false),
            read_error: RefCell::new(None),
            write: Cell::new(None),
            write_result: RefCell::new(None),
            accept: Cell::new(None),
            accept_result: RefCell::new(None),
            read_poll: Cell::new(None),
        }
    }
}

pub(crate) type LocalFile = GenericFile<NodeMain, RuntimeError, AsyncFileRegistration>;

enum FileOperationKind {
    Accept,
    Read { buffer: Vec<u8> },
    Write { buffer: Vec<u8>, offset: usize },
    ReadPoll,
}

struct FileOperation {
    file: Rc<LocalFile>,
    kind: FileOperationKind,
    waker: Option<Waker>,
}

/// Main-thread File pool and the io_uring which owns its in-flight buffers.
pub struct AsyncFileMain {
    state: RefCell<AsyncFileState>,
}

struct AsyncFileState {
    ring: IoUring,
    completion_ready: Rc<AsyncFd<OwnedFd>>,
    submission_ready: Rc<Notify>,
    files: Pool<Rc<LocalFile>>,
    operations: Pool<Box<FileOperation>>,
    ready_files: Vec<(Rc<LocalFile>, Readiness)>,
}

static ASYNC_FILE_MAIN: OnceLock<AsyncFileMain> = OnceLock::new();

// SAFETY: the global is published once and every access is restricted to
// thread zero. Its RefCell arbitrates reentrant local tasks, not OS threads.
unsafe impl Send for AsyncFileMain {}
unsafe impl Sync for AsyncFileMain {}

impl AsyncFileMain {
    pub fn init() -> RuntimeResult<&'static Self> {
        crate::ensure_main_thread()?;
        let mut builder = IoUring::builder();
        builder.dontfork();
        let ring = builder
            .build(1024)
            .map_err(|source| RuntimeError::FilePollerIo {
                operation: "create main-thread io_uring",
                source,
            })?;
        // The ring fd itself is pollable when the completion queue is nonempty.
        let descriptor = unsafe { libc::fcntl(ring.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if descriptor < 0 {
            return Err(RuntimeError::FilePollerIo {
                operation: "duplicate main-thread io_uring descriptor",
                source: io::Error::last_os_error(),
            });
        }
        let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
        let completion_ready =
            AsyncFd::new(descriptor).map_err(|source| RuntimeError::FilePollerIo {
                operation: "register main-thread io_uring descriptor with Tokio",
                source,
            })?;
        assert!(
            ASYNC_FILE_MAIN
                .set(Self {
                    state: RefCell::new(AsyncFileState {
                        ring,
                        completion_ready: Rc::new(completion_ready),
                        submission_ready: Rc::new(Notify::new()),
                        files: Pool::new(),
                        operations: Pool::new(),
                        ready_files: Vec::new(),
                    }),
                })
                .is_ok(),
            "AsyncFileMain initializes once"
        );
        Ok(Self::global())
    }

    #[inline]
    pub fn global() -> &'static Self {
        crate::ensure_main_thread().expect("AsyncFileMain belongs to thread zero");
        ASYNC_FILE_MAIN
            .get()
            .expect("AsyncFileMain initializes before File use")
    }

    pub(crate) fn add(&self, file: LocalFile) -> RuntimeResult<u32> {
        crate::ensure_main_thread()?;
        let socket = is_socket(file.fd()).map_err(|source| RuntimeError::FilePollerIo {
            operation: "inspect main-thread File descriptor",
            source,
        })?;
        let functions = file.functions();
        assert!(functions.write.is_none(), "async File has no write-readiness callback");
        file.owner().socket.set(socket);
        let file = Rc::new(file);
        let mut state = self.state.borrow_mut();
        let index = state.files.insert(Rc::clone(&file));
        file.owner().index.set(index);
        if functions.read.is_some() || functions.error.is_some() {
            match state.enqueue(Rc::clone(&file), FileOperationKind::ReadPoll, None) {
                Ok(token) => file.owner().read_poll.set(Some(token)),
                Err(source) => {
                    drop(state.files.remove(index).expect("new File remains registered"));
                    return Err(RuntimeError::FilePollerIo {
                        operation: "register main-thread File read readiness",
                        source,
                    });
                }
            }
        }
        Ok(index)
    }

    #[inline]
    pub(crate) fn file(&self, index: u32) -> Option<Rc<LocalFile>> {
        crate::ensure_main_thread().expect("AsyncFileMain belongs to thread zero");
        self.state.borrow().file(index)
    }

    pub fn remove(&self, index: u32) -> RuntimeResult<()> {
        crate::ensure_main_thread()?;
        self.state.borrow_mut().remove(index)
    }
}

impl AsyncFileState {
    fn file(&self, index: u32) -> Option<Rc<LocalFile>> {
        self.files.get(index).cloned()
    }

    fn remove(&mut self, index: u32) -> RuntimeResult<()> {
        let file = self
            .files
            .get(index)
            .cloned()
            .ok_or(RuntimeError::FileIndexInvalid { index })?;
        let mut cancelled = false;
        for token in [
            file.owner().accept.get(),
            file.owner().read.get(),
            file.owner().write.get(),
            file.owner().read_poll.get(),
        ]
        .into_iter()
        .flatten()
        {
            let entry = opcode::AsyncCancel::new(u64::from(token))
                .build()
                .user_data(CANCEL_TOKEN);
            self.queue_entry(entry)
                .map_err(|source| RuntimeError::FilePollerIo {
                    operation: "cancel main-thread File operation",
                    source,
                })?;
            cancelled = true;
        }
        if cancelled {
            self.ring
                .submit()
                .map_err(|source| RuntimeError::FilePollerIo {
                    operation: "submit main-thread File cancellation",
                    source,
                })?;
        }
        file.owner().closed.set(true);
        drop(
            self.files
                .remove(index)
                .expect("validated File remains registered"),
        );
        self.submission_ready.notify_one();
        Ok(())
    }

    fn queue_entry(&mut self, entry: squeue::Entry) -> io::Result<()> {
        let mut queue = self.ring.submission();
        if queue.is_full() {
            drop(queue);
            self.ring.submit()?;
            queue = self.ring.submission();
        }
        // SAFETY: every pointer in an SQE refers to storage retained by an
        // operation until the matching CQE has been consumed.
        unsafe { queue.push(&entry) }.map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))
    }

    fn enqueue(
        &mut self,
        file: Rc<LocalFile>,
        kind: FileOperationKind,
        waker: Option<&Waker>,
    ) -> io::Result<u32> {
        let index = self.operations.insert(Box::new(FileOperation {
            file,
            kind,
            waker: waker.cloned(),
        }));
        if let Err(source) = self.queue_operation(index) {
            drop(
                self.operations
                    .remove(index)
                    .expect("new File operation remains owned"),
            );
            return Err(source);
        }
        self.submission_ready.notify_one();
        Ok(index)
    }

    fn queue_operation(&mut self, index: u32) -> io::Result<()> {
        let operation = self
            .operations
            .get(index)
            .expect("queued operation remains owned");
        let fd = types::Fd(operation.file.fd());
        let entry = match &operation.kind {
            FileOperationKind::Accept => {
                opcode::Accept::new(fd, std::ptr::null_mut(), std::ptr::null_mut())
                    .flags(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
                    .build()
            }
            FileOperationKind::ReadPoll => opcode::PollAdd::new(fd, libc::POLLIN as u32).build(),
            FileOperationKind::Read { buffer } if operation.file.owner().socket.get() => {
                opcode::Recv::new(fd, buffer.as_ptr().cast_mut(), buffer.len() as u32).build()
            }
            FileOperationKind::Read { buffer } => {
                opcode::Read::new(fd, buffer.as_ptr().cast_mut(), buffer.len() as u32)
                    .offset(u64::MAX)
                    .build()
            }
            FileOperationKind::Write { buffer, offset } if operation.file.owner().socket.get() => {
                opcode::Send::new(
                    fd,
                    unsafe { buffer.as_ptr().add(*offset) },
                    (buffer.len() - *offset) as u32,
                )
                .flags(libc::MSG_NOSIGNAL)
                .build()
            }
            FileOperationKind::Write { buffer, offset } => opcode::Write::new(
                fd,
                unsafe { buffer.as_ptr().add(*offset) },
                (buffer.len() - *offset) as u32,
            )
            .offset(u64::MAX)
            .build(),
        }
        .user_data(u64::from(index));
        self.queue_entry(entry)
    }

    fn update_waker(&mut self, index: u32, waker: &Waker) {
        if let Some(operation) = self.operations.get_mut(index) {
            let registered = operation
                .waker
                .as_mut()
                .expect("asynchronous File operation has a waker");
            if !registered.will_wake(waker) {
                *registered = waker.clone();
            }
        }
    }

    fn dispatch_completions(&mut self) -> RuntimeResult<usize> {
        let completions = self
            .ring
            .completion()
            .map(|entry| (entry.user_data(), entry.result()))
            .collect::<Vec<_>>();
        let count = completions.len();
        for (token, result) in completions {
            if token == CANCEL_TOKEN {
                continue;
            }
            let index = token as u32;
            if self.operations.get(index).is_none() {
                continue;
            }
            if result == -libc::EAGAIN || result == -libc::EINTR {
                if self
                    .operations
                    .get(index)
                    .expect("validated File operation")
                    .file
                    .owner()
                    .closed
                    .get()
                {
                    self.finish_operation(index, Err(io::ErrorKind::NotConnected.into()))?;
                    continue;
                }
                if let Err(source) = self.queue_operation(index) {
                    self.finish_operation(index, Err(source))?;
                } else {
                    self.submission_ready.notify_one();
                }
                continue;
            }
            let retry_write = if result > 0 {
                let operation = self
                    .operations
                    .get_mut(index)
                    .expect("validated File operation");
                if let FileOperationKind::Write { buffer, offset } = &mut operation.kind {
                    *offset += result as usize;
                    assert!(
                        *offset <= buffer.len(),
                        "send completion fits submitted data"
                    );
                    *offset < buffer.len()
                } else {
                    false
                }
            } else {
                false
            };
            if retry_write {
                if let Err(source) = self.queue_operation(index) {
                    self.finish_operation(index, Err(source))?;
                } else {
                    self.submission_ready.notify_one();
                }
                continue;
            }
            let result = if result < 0 {
                Err(io::Error::from_raw_os_error(-result))
            } else {
                Ok(result as usize)
            };
            self.finish_operation(index, result)?;
        }
        Ok(count)
    }

    fn finish_operation(&mut self, index: u32, result: io::Result<usize>) -> RuntimeResult<()> {
        let operation = self
            .operations
            .remove(index)
            .expect("completed File operation remains owned");
        let FileOperation { file, kind, waker } = *operation;
        let registration = file.owner();
        match kind {
            FileOperationKind::Accept => {
                registration.accept.set(None);
                let accepted = result.map(|fd| unsafe { OwnedFd::from_raw_fd(fd as i32) });
                registration.accept_result.replace(Some(accepted));
            }
            FileOperationKind::Read { mut buffer } => {
                registration.read.set(None);
                match result {
                    Ok(length) => {
                        buffer.truncate(length);
                        registration.read_data.replace(buffer);
                        registration.read_offset.set(0);
                        registration.read_eof.set(length == 0);
                    }
                    Err(source) => {
                        registration.read_error.replace(Some(source));
                    }
                }
            }
            FileOperationKind::Write { .. } => {
                registration.write.set(None);
                let result = match result {
                    Ok(0) => Err(io::Error::from(io::ErrorKind::WriteZero)),
                    Ok(_) => Ok(()),
                    Err(source) => Err(source),
                };
                registration.write_result.replace(Some(result));
            }
            FileOperationKind::ReadPoll => {
                registration.read_poll.set(None);
                if registration.closed.get() {
                    return Ok(());
                }
                let readiness = result.map_err(|source| RuntimeError::FilePollerIo {
                    operation: "poll main-thread File read readiness",
                    source,
                })?;
                if readiness & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as usize != 0 {
                    if file.functions().error.is_none() {
                        registration.closed.set(true);
                        drop(
                            self.files
                                .remove(registration.index.get())
                                .expect("ready File remains registered"),
                        );
                        return Ok(());
                    }
                    self.ready_files.push((Rc::clone(&file), Readiness::ERROR));
                } else if file.functions().read.is_some() {
                    self.ready_files.push((Rc::clone(&file), Readiness::READ));
                }
                let token = self
                    .enqueue(Rc::clone(&file), FileOperationKind::ReadPoll, None)
                    .map_err(|source| RuntimeError::FilePollerIo {
                        operation: "rearm main-thread File read readiness",
                        source,
                })?;
                registration.read_poll.set(Some(token));
            }
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

}

impl AsyncFileMain {
    pub async fn next_ready(&self) -> RuntimeResult<usize> {
        crate::ensure_main_thread()?;
        loop {
            let (completion_ready, submission_ready, count) = {
                let mut state = self.state.borrow_mut();
                state
                    .ring
                    .submit()
                    .map_err(|source| RuntimeError::FilePollerIo {
                        operation: "submit main-thread File operations",
                        source,
                    })?;
                let count = state.dispatch_completions()?;
                (
                    Rc::clone(&state.completion_ready),
                    Rc::clone(&state.submission_ready),
                    count,
                )
            };
            if count != 0 {
                return Ok(count);
            }
            tokio::select! {
                readiness = completion_ready.readable() => {
                    let mut guard = readiness.map_err(|source| RuntimeError::FilePollerIo {
                        operation: "await main-thread File completion",
                        source,
                    })?;
                    guard.clear_ready();
                }
                _ = submission_ready.notified() => {}
            }
        }
    }

    pub(crate) fn dispatch_ready(&self, graph: &mut NodeMain) {
        crate::ensure_main_thread().expect("AsyncFileMain belongs to thread zero");
        let mut ready_files = std::mem::take(&mut self.state.borrow_mut().ready_files);
        for (file, readiness) in ready_files.drain(..) {
            if file.owner().closed.get() {
                continue;
            }
            let functions = file.functions();
            let result = if readiness.contains(Readiness::ERROR) {
                file.record_error_event();
                functions.error.expect("error readiness has a callback")(graph, &file)
            } else {
                file.record_read_event();
                functions.read.expect("read readiness has a callback")(graph, &file)
            };
            if let Err(source) = result {
                tracing::error!(file = %file.description(), %source, "file callback error");
            }
        }
        self.state.borrow_mut().ready_files = ready_files;
    }
}

fn is_socket(fd: i32) -> io::Result<bool> {
    let mut socket_type = 0_i32;
    let mut length = std::mem::size_of::<i32>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            std::ptr::from_mut(&mut socket_type).cast(),
            &mut length,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let source = io::Error::last_os_error();
    if source.raw_os_error() == Some(libc::ENOTSOCK) {
        Ok(false)
    } else {
        Err(source)
    }
}

impl GenericFile<NodeMain, RuntimeError, AsyncFileRegistration> {
    pub async fn accept(&self) -> RuntimeResult<OwnedFd> {
        poll_fn(|context| self.poll_accept(context))
            .await
            .map_err(|source| RuntimeError::FileAccept { source })
    }

    fn poll_accept(&self, context: &mut Context<'_>) -> Poll<io::Result<OwnedFd>> {
        let registration = self.owner();
        if let Some(result) = registration.accept_result.borrow_mut().take() {
            return Poll::Ready(result);
        }
        if registration.closed.get() {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }
        let mut owner = AsyncFileMain::global().state.borrow_mut();
        if let Some(token) = registration.accept.get() {
            owner.update_waker(token, context.waker());
            return Poll::Pending;
        }
        let Some(file) = owner.file(registration.index.get()) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        match owner.enqueue(file, FileOperationKind::Accept, Some(context.waker())) {
            Ok(token) => {
                registration.accept.set(Some(token));
                Poll::Pending
            }
            Err(source) => Poll::Ready(Err(source)),
        }
    }
}

impl AsyncRead for &LocalFile {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let registration = self.owner();
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        {
            let data = registration.read_data.borrow();
            let offset = registration.read_offset.get();
            if offset < data.len() {
                let length = (data.len() - offset).min(buffer.remaining());
                buffer.put_slice(&data[offset..offset + length]);
                registration.read_offset.set(offset + length);
                return Poll::Ready(Ok(()));
            }
            if !data.is_empty() {
                drop(data);
                registration.read_data.borrow_mut().clear();
            }
        }
        if let Some(source) = registration.read_error.borrow_mut().take() {
            return Poll::Ready(Err(source));
        }
        if registration.read_eof.get() {
            return Poll::Ready(Ok(()));
        }
        if registration.closed.get() {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }
        let mut owner = AsyncFileMain::global().state.borrow_mut();
        if let Some(token) = registration.read.get() {
            owner.update_waker(token, context.waker());
            return Poll::Pending;
        }
        let Some(file) = owner.file(registration.index.get()) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        let capacity = buffer.remaining().min(READ_CAPACITY).max(1);
        match owner.enqueue(
            file,
            FileOperationKind::Read {
                buffer: vec![0; capacity],
            },
            Some(context.waker()),
        ) {
            Ok(token) => {
                registration.read.set(Some(token));
                Poll::Pending
            }
            Err(source) => Poll::Ready(Err(source)),
        }
    }
}

impl AsyncWrite for &LocalFile {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let registration = self.owner();
        if let Some(result) = registration.write_result.borrow_mut().take() {
            if let Err(source) = result {
                return Poll::Ready(Err(source));
            }
        }
        if registration.closed.get() {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        }
        let mut owner = AsyncFileMain::global().state.borrow_mut();
        if let Some(token) = registration.write.get() {
            owner.update_waker(token, context.waker());
            return Poll::Pending;
        }
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let Some(file) = owner.file(registration.index.get()) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        let length = buffer.len().min(WRITE_CAPACITY);
        match owner.enqueue(
            file,
            FileOperationKind::Write {
                buffer: buffer[..length].to_vec(),
                offset: 0,
            },
            Some(context.waker()),
        ) {
            Ok(token) => {
                registration.write.set(Some(token));
                Poll::Ready(Ok(length))
            }
            Err(source) => Poll::Ready(Err(source)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let registration = self.owner();
        if let Some(result) = registration.write_result.borrow_mut().take() {
            return Poll::Ready(result);
        }
        if let Some(token) = registration.write.get() {
            AsyncFileMain::global()
                .state
                .borrow_mut()
                .update_waker(token, context.waker());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(context)
    }
}
