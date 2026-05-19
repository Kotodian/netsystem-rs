use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use btls::ssl::{
    Error as BtlsError, ErrorCode, HandshakeError, MidHandshakeSslStream, ShutdownResult, Ssl,
    SslRef, SslStream,
};
use hammer_core::error::{HammerError, HammerResult};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::fragment::FragmentedTcpStream;

pub(crate) struct BtlsClientStream {
    inner: SslStream<TokioTcpAdapter>,
}

impl BtlsClientStream {
    pub(super) fn ssl_mut(&mut self) -> &mut SslRef {
        self.inner.ssl_mut()
    }
}

pub(super) async fn connect(
    ssl: Ssl,
    stream: FragmentedTcpStream,
) -> HammerResult<BtlsClientStream> {
    let mut handshake = ssl.setup_connect(TokioTcpAdapter { stream });
    loop {
        match handshake.handshake() {
            Ok(inner) => return Ok(BtlsClientStream { inner }),
            Err(HandshakeError::WouldBlock(mut blocked)) => {
                wait_for_btls_error(&mut blocked).await?;
                handshake = blocked;
            }
            Err(HandshakeError::Failure(failed)) => {
                return Err(HammerError::internal(format!(
                    "btls tls handshake: {}",
                    failed.error()
                )));
            }
            Err(HandshakeError::SetupFailure(err)) => {
                return Err(HammerError::internal(format!("btls tls setup: {err}")));
            }
        }
    }
}

async fn wait_for_btls_error(
    handshake: &mut MidHandshakeSslStream<TokioTcpAdapter>,
) -> HammerResult<()> {
    let direction = retry_direction(handshake.error())
        .map_err(|err| HammerError::internal(format!("btls tls handshake readiness: {err}")))?;
    match direction {
        RetryDirection::Read => handshake
            .get_ref()
            .stream
            .readable()
            .await
            .map_err(|err| HammerError::internal(format!("btls tls read readiness: {err}"))),
        RetryDirection::Write => {
            if handshake.get_mut().stream.wait_fragment_delay().await {
                return Ok(());
            }
            handshake
                .get_ref()
                .stream
                .writable()
                .await
                .map_err(|err| HammerError::internal(format!("btls tls write readiness: {err}")))
        }
        RetryDirection::Eof => Err(HammerError::internal("btls tls handshake: unexpected EOF")),
    }
}

impl AsyncRead for BtlsClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match this.inner.ssl_read(buf.initialize_unfilled()) {
                Ok(0) => return Poll::Ready(Ok(())),
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(err) => {
                    if ready!(poll_btls_retry(&this.inner, cx, &err))? {
                        continue;
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl AsyncWrite for BtlsClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match this.inner.ssl_write(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(err) => {
                    if ready!(poll_btls_retry(&this.inner, cx, &err))? {
                        continue;
                    }
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "btls tls write after EOF",
                    )));
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().inner.get_mut().flush())
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match this.inner.shutdown() {
                Ok(ShutdownResult::Sent | ShutdownResult::Received) => {
                    return Pin::new(&mut this.inner.get_mut().stream).poll_shutdown(cx);
                }
                Err(err) => {
                    if ready!(poll_btls_retry(&this.inner, cx, &err))? {
                        continue;
                    }
                    return Pin::new(&mut this.inner.get_mut().stream).poll_shutdown(cx);
                }
            }
        }
    }
}

fn poll_btls_retry(
    stream: &SslStream<TokioTcpAdapter>,
    cx: &mut Context<'_>,
    err: &BtlsError,
) -> Poll<io::Result<bool>> {
    match retry_direction(err) {
        Ok(RetryDirection::Read) => match ready!(stream.get_ref().stream.poll_read_ready(cx)) {
            Ok(()) => Poll::Ready(Ok(true)),
            Err(err) => Poll::Ready(Err(err)),
        },
        Ok(RetryDirection::Write) => match ready!(stream.get_ref().stream.poll_write_ready(cx)) {
            Ok(()) => Poll::Ready(Ok(true)),
            Err(err) => Poll::Ready(Err(err)),
        },
        Ok(RetryDirection::Eof) => Poll::Ready(Ok(false)),
        Err(err) => Poll::Ready(Err(err)),
    }
}

fn retry_direction(err: &BtlsError) -> io::Result<RetryDirection> {
    match err.code() {
        ErrorCode::WANT_READ => Ok(RetryDirection::Read),
        ErrorCode::WANT_WRITE => Ok(RetryDirection::Write),
        ErrorCode::ZERO_RETURN => Ok(RetryDirection::Eof),
        ErrorCode::SYSCALL if err.io_error().is_none() => Ok(RetryDirection::Eof),
        _ => Err(io::Error::other(err.to_string())),
    }
}

enum RetryDirection {
    Read,
    Write,
    Eof,
}

struct TokioTcpAdapter {
    stream: FragmentedTcpStream,
}

impl Read for TokioTcpAdapter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.try_read(buf)
    }
}

impl Write for TokioTcpAdapter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.try_write_fragmented(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
