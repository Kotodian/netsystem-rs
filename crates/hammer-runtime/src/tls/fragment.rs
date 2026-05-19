use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hammer_core::config::TlsFragmentOptions;
use hammer_core::error::{HammerError, HammerResult};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep, sleep_until};

pub(crate) struct FragmentedTcpStream {
    inner: TcpStream,
    fragment: Option<TlsWriteFragmenter>,
}

impl FragmentedTcpStream {
    pub(super) fn new(inner: TcpStream, options: Option<TlsFragmentOptions>) -> HammerResult<Self> {
        Ok(Self {
            inner,
            fragment: options.map(TlsWriteFragmenter::new).transpose()?,
        })
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.try_read(buf)
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) fn try_write_fragmented(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(fragment) = self.fragment.as_mut() else {
            return self.inner.try_write(buf);
        };
        if !fragment.is_ready() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let write_len = fragment.write_len(buf.len());
        let written = self.inner.try_write(&buf[..write_len])?;
        fragment.after_write(written, buf.len());
        if fragment.is_done() {
            self.fragment = None;
        }
        Ok(written)
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) async fn wait_fragment_delay(&mut self) -> bool {
        let Some(fragment) = self.fragment.as_mut() else {
            return false;
        };
        fragment.wait_delay().await
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) async fn readable(&self) -> io::Result<()> {
        self.inner.readable().await
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) async fn writable(&self) -> io::Result<()> {
        self.inner.writable().await
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_ready(cx)
    }

    #[cfg(feature = "tls-utls-stream")]
    pub(super) fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_write_ready(cx)
    }
}

impl AsyncRead for FragmentedTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for FragmentedTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(fragment) = this.fragment.as_mut() else {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        };
        match fragment.poll_delay(cx) {
            Poll::Ready(()) => {}
            Poll::Pending => return Poll::Pending,
        }
        let write_len = fragment.write_len(buf.len());
        match Pin::new(&mut this.inner).poll_write(cx, &buf[..write_len]) {
            Poll::Ready(Ok(written)) => {
                fragment.after_write(written, buf.len());
                if fragment.is_done() {
                    this.fragment = None;
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

struct TlsWriteFragmenter {
    chunk_size: usize,
    sleep: Duration,
    delay: Option<Pin<Box<Sleep>>>,
    done: bool,
}

impl TlsWriteFragmenter {
    fn new(options: TlsFragmentOptions) -> HammerResult<Self> {
        Ok(Self {
            chunk_size: parse_fragment_size(&options.size)?,
            sleep: options.sleep,
            delay: None,
            done: false,
        })
    }

    fn write_len(&self, input_len: usize) -> usize {
        input_len.min(self.chunk_size)
    }

    fn after_write(&mut self, written: usize, input_len: usize) {
        if written == 0 {
            return;
        }
        if written < input_len {
            self.schedule_delay();
        } else {
            self.done = true;
        }
    }

    fn schedule_delay(&mut self) {
        if self.sleep.is_zero() {
            return;
        }
        self.delay = Some(Box::pin(sleep_until(Instant::now() + self.sleep)));
    }

    fn poll_delay(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(delay) = self.delay.as_mut() else {
            return Poll::Ready(());
        };
        match delay.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.delay = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }

    #[cfg(feature = "tls-utls-stream")]
    fn is_ready(&mut self) -> bool {
        let Some(delay) = self.delay.as_ref() else {
            return true;
        };
        delay.is_elapsed()
    }

    #[cfg(feature = "tls-utls-stream")]
    async fn wait_delay(&mut self) -> bool {
        let Some(delay) = self.delay.as_mut() else {
            return false;
        };
        delay.as_mut().await;
        self.delay = None;
        true
    }

    fn is_done(&self) -> bool {
        self.done
    }
}

fn parse_fragment_size(size: &str) -> HammerResult<usize> {
    let size = size.trim();
    if size.eq_ignore_ascii_case("tlshello") {
        return Ok(1);
    }
    if let Some((min, max)) = size.split_once('-') {
        let min = parse_fragment_size_number(min, size)?;
        let max = parse_fragment_size_number(max, size)?;
        if min > max {
            return Err(HammerError::config_validation(format!(
                "tls.fragment.size range minimum exceeds maximum: {size}"
            )));
        }
        return Ok(min);
    }
    parse_fragment_size_number(size, size)
}

fn parse_fragment_size_number(raw: &str, full: &str) -> HammerResult<usize> {
    let value = raw.trim().parse::<usize>().map_err(|_| {
        HammerError::config_validation(format!(
            "tls.fragment.size must be tlshello or a positive integer/range: {full}"
        ))
    })?;
    if value == 0 {
        return Err(HammerError::config_validation(
            "tls.fragment.size must be greater than zero",
        ));
    }
    Ok(value)
}
