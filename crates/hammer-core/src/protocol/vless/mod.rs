use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::error::{HammerError, HammerResult};
use crate::network::SocksAddr;

const VERSION: u8 = 0;
const ADDRESS_IPV4: u8 = 1;
const ADDRESS_DOMAIN: u8 = 2;
const ADDRESS_IPV6: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VlessCommand {
    Tcp = 1,
    Udp = 2,
}

pub fn encode_request(
    uuid: &[u8; 16],
    command: VlessCommand,
    destination: &SocksAddr,
    initial_payload: &[u8],
) -> HammerResult<Vec<u8>> {
    let mut out = Vec::with_capacity(24 + initial_payload.len());
    out.push(VERSION);
    out.extend_from_slice(uuid);
    out.push(0);
    out.push(command as u8);
    out.extend_from_slice(&destination.port.to_be_bytes());
    encode_address(destination, &mut out)?;
    out.extend_from_slice(initial_payload);
    Ok(out)
}

fn encode_address(destination: &SocksAddr, out: &mut Vec<u8>) -> HammerResult<()> {
    if let Some(domain) = destination.domain.as_deref() {
        let len = u8::try_from(domain.len()).map_err(|_| {
            HammerError::config_validation("vless destination domain must fit in 255 bytes")
        })?;
        out.push(ADDRESS_DOMAIN);
        out.push(len);
        out.extend_from_slice(domain.as_bytes());
        return Ok(());
    }

    match destination.host {
        std::net::IpAddr::V4(ip) => {
            out.push(ADDRESS_IPV4);
            out.extend_from_slice(&ip.octets());
        }
        std::net::IpAddr::V6(ip) => {
            out.push(ADDRESS_IPV6);
            out.extend_from_slice(&ip.octets());
        }
    }
    Ok(())
}

pub struct VlessStream<S> {
    inner: S,
    response: ResponseHeaderState,
}

impl<S> VlessStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            response: ResponseHeaderState::new(),
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> AsyncRead for VlessStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while !this.response.is_done() {
            match this.response.poll_read(&mut this.inner, cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for VlessStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

struct ResponseHeaderState {
    bytes: Vec<u8>,
    read: usize,
    done: bool,
}

impl ResponseHeaderState {
    fn new() -> Self {
        Self {
            bytes: vec![0; 2],
            read: 0,
            done: false,
        }
    }

    fn is_done(&self) -> bool {
        self.done
    }

    fn poll_read<S>(&mut self, stream: &mut S, cx: &mut Context<'_>) -> io::Result<Poll<()>>
    where
        S: AsyncRead + Unpin,
    {
        let target = self.bytes.len();
        let before = self.read;
        let mut buf = ReadBuf::new(&mut self.bytes[self.read..target]);
        match Pin::new(stream).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Err(err),
            Poll::Pending => return Ok(Poll::Pending),
        }
        let read_now = buf.filled().len();
        if read_now == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "vless response header eof",
            ));
        }
        self.read = before + read_now;
        if self.read == 2 && self.bytes.len() == 2 {
            if self.bytes[0] != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid vless response version",
                ));
            }
            let addon_len = self.bytes[1] as usize;
            self.bytes.resize(2 + addon_len, 0);
        }
        if self.read == self.bytes.len() {
            self.done = true;
        }
        Ok(Poll::Ready(()))
    }
}
