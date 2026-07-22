use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;

use hammer_infra::segment::Svm;
use hammer_runtime::RuntimeResult;
use hammer_runtime::app::{SessionEventQueue, SessionEvt};
use thiserror::Error;
use tokio::io::unix::AsyncFd;

/// Failures while adapting an attached session to asynchronous app I/O.
#[derive(Debug, Error)]
pub enum RemoteAppSessionError {
    #[error("remote app session requires a signal-read descriptor")]
    SessionSignalMissing,
    #[error("failed to duplicate remote app session signal descriptor")]
    SessionSignalDuplicate {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register remote app session readiness")]
    SessionReadiness {
        #[source]
        source: std::io::Error,
    },
}

/// Cross-process async facade over an [`AppSession<Svm>`].
///
/// Uses [`tokio::io::unix::AsyncFd`] on the event-queue signal fd so the
/// app can await dataplane events without busy-polling the shared-memory
/// FIFOs.
pub struct RemoteAppSession {
    session: Arc<hammer_runtime::app::AppSession<Svm>>,
    evt_async_fd: AsyncFd<OwnedFd>,
}

impl RemoteAppSession {
    /// Wrap an existing cross-process session.
    ///
    /// Duplicates the event-queue signal-read fd so the [`AsyncFd`] has
    /// sole ownership of its descriptor. The original within the Session
    /// Message Queue is unaffected.
    ///
    /// # Panics
    ///
    /// Panics unless called while a Tokio runtime with I/O enabled is entered.
    pub fn new(
        session: Arc<hammer_runtime::app::AppSession<Svm>>,
    ) -> Result<Self, RemoteAppSessionError> {
        let read_fd = session
            .evt_q()
            .read_fd()
            .ok_or(RemoteAppSessionError::SessionSignalMissing)?;
        // SAFETY: F_DUPFD_CLOEXEC duplicates the live queue endpoint and
        // returns a fresh descriptor whose ownership transfers below.
        let duped = unsafe { libc::fcntl(read_fd, libc::F_DUPFD_CLOEXEC, 0) };
        if duped < 0 {
            return Err(RemoteAppSessionError::SessionSignalDuplicate {
                source: std::io::Error::last_os_error(),
            });
        }
        // SAFETY: fcntl returned a fresh descriptor and ownership transfers once.
        let owned = unsafe { OwnedFd::from_raw_fd(duped) };
        let evt_async_fd = AsyncFd::new(owned)
            .map_err(|source| RemoteAppSessionError::SessionReadiness { source })?;
        Ok(Self {
            session,
            evt_async_fd,
        })
    }

    /// App-side async receive. Blocks until the RX FIFO has at least one
    /// byte, copies up to `out.len()` bytes, and advances the FIFO head.
    pub async fn recv(&self, out: &mut [u8]) -> usize {
        loop {
            let read = self.session.rx_fifo().peek(0, out.len(), out);
            if read != 0 || out.is_empty() {
                self.session.rx_fifo().dequeue_drop(read);
                return read;
            }
            self.session.want_rx_notification();
            if self.session.rx_fifo().max_dequeue() != 0 {
                self.session.clear_rx_notification();
                continue;
            }
            self.wait_for_event().await;
            self.session.clear_rx_notification();
        }
    }

    /// App-side async send. Copies `bytes` into the TX FIFO, applying
    /// backpressure when the FIFO is full.
    pub async fn send_all(&self, bytes: &[u8]) -> RuntimeResult<usize> {
        let mut written = 0usize;
        while written < bytes.len() {
            let accepted = self.session.send_bytes(&bytes[written..])?;
            if accepted != 0 {
                written += accepted;
                continue;
            }
            self.session.want_tx_notification();
            if self.session.tx_fifo().max_enqueue() != 0 {
                self.session.clear_tx_notification();
                continue;
            }
            self.wait_for_event().await;
            self.session.clear_tx_notification();
        }
        Ok(written)
    }

    /// App-side async event receive. Waits for the next session event
    /// from the dataplane-side event queue.
    pub async fn next_event(&self) -> SessionEvt {
        loop {
            if let Some(evt) = self.session.evt_q().dequeue() {
                return evt;
            }
            self.wait_for_event().await;
        }
    }

    /// Wait for the [`AsyncFd`] to become readable, then drain the pipe
    /// signal byte. Returns once at least one event is pending in the
    /// event queue.
    async fn wait_for_event(&self) {
        loop {
            let guard = self
                .evt_async_fd
                .readable()
                .await
                .expect("RemoteAppSession AsyncFd readable");
            let fd = guard.get_inner().as_raw_fd();
            // Drain all signal bytes from the pipe so the next
            // edge-triggered wakeup is clean.
            let mut buf = [0u8; 64];
            loop {
                // SAFETY: buf is writable for its full length and AsyncFd owns
                // the live nonblocking descriptor for the duration of the read.
                let ret =
                    unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if ret <= 0 {
                    break;
                }
            }
            if !self.session.evt_q().is_empty() {
                return;
            }
        }
    }
}

impl AsRawFd for RemoteAppSession {
    fn as_raw_fd(&self) -> RawFd {
        self.evt_async_fd.as_raw_fd()
    }
}
