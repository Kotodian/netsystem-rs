use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use rand::{RngCore, rngs::OsRng};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::error::{HammerError, HammerResult};
use crate::network::SocksAddr;

pub mod reality;

pub const FLOW_XTLS_RPRX_VISION: &str = "xtls-rprx-vision";

const VERSION: u8 = 0;
const ADDRESS_IPV4: u8 = 1;
const ADDRESS_DOMAIN: u8 = 2;
const ADDRESS_IPV6: u8 = 3;
const ADDON_FIELD_FLOW: u32 = 1;
const PROTOBUF_WIRE_LENGTH_DELIMITED: u32 = 2;
const VISION_COMMAND_PADDING_CONTINUE: u8 = 0;
const VISION_COMMAND_PADDING_END: u8 = 1;
const VISION_COMMAND_PADDING_DIRECT: u8 = 2;
const VISION_FRAME_HEADER_LEN: usize = 5;
const VISION_MAX_BUFFER_SIZE: usize = 8192;
const VISION_MAX_FRAME_CONTENT_LEN: usize = VISION_MAX_BUFFER_SIZE - 21;
const VISION_SHORT_PADDING_RANGE: usize = 256;
const VISION_LONG_PADDING_RANGE: usize = 500;
const VISION_LONG_PADDING_THRESHOLD: usize = 900;

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
    VlessRequestBuilder::new(uuid, command, destination)
        .initial_payload(initial_payload)
        .encode()
}

pub struct VlessRequestBuilder<'a> {
    uuid: &'a [u8; 16],
    command: VlessCommand,
    destination: &'a SocksAddr,
    flow: Option<&'a str>,
    initial_payload: &'a [u8],
}

impl<'a> VlessRequestBuilder<'a> {
    pub fn new(uuid: &'a [u8; 16], command: VlessCommand, destination: &'a SocksAddr) -> Self {
        Self {
            uuid,
            command,
            destination,
            flow: None,
            initial_payload: &[],
        }
    }

    pub fn flow(mut self, flow: &'a str) -> Self {
        self.flow = Some(flow);
        self
    }

    pub fn optional_flow(mut self, flow: Option<&'a str>) -> Self {
        self.flow = flow;
        self
    }

    pub fn initial_payload(mut self, initial_payload: &'a [u8]) -> Self {
        self.initial_payload = initial_payload;
        self
    }

    pub fn encode(self) -> HammerResult<Vec<u8>> {
        let addons = encode_header_addons(self.flow);
        let addon_len = u8::try_from(addons.len()).map_err(|_| {
            HammerError::config_validation("vless request header addons exceed 255 bytes")
        })?;
        let mut out = Vec::with_capacity(24 + addons.len() + self.initial_payload.len());
        out.push(VERSION);
        out.extend_from_slice(self.uuid);
        out.push(addon_len);
        out.extend_from_slice(&addons);
        out.push(self.command as u8);
        out.extend_from_slice(&self.destination.port.to_be_bytes());
        encode_address(self.destination, &mut out)?;
        out.extend_from_slice(self.initial_payload);
        Ok(out)
    }
}

pub fn encode_udp_packet(payload: &[u8]) -> HammerResult<Vec<u8>> {
    let len = u16::try_from(payload.len())
        .map_err(|_| HammerError::config_validation("vless udp packet exceeds 65535 bytes"))?;
    let mut out = Vec::with_capacity(payload.len() + 2);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

pub async fn read_udp_packet<S>(stream: &mut S) -> HammerResult<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut len = [0_u8; 2];
    stream
        .read_exact(&mut len)
        .await
        .map_err(|err| HammerError::internal(format!("read vless udp packet length: {err}")))?;
    let mut payload = vec![0_u8; u16::from_be_bytes(len) as usize];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| HammerError::internal(format!("read vless udp packet payload: {err}")))?;
    Ok(payload)
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

fn encode_header_addons(flow: Option<&str>) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(flow) = flow {
        encode_len_delimited_field(ADDON_FIELD_FLOW, flow.as_bytes(), &mut out);
    }
    out
}

fn encode_len_delimited_field(field_number: u32, value: &[u8], out: &mut Vec<u8>) {
    let tag = (field_number << 3) | PROTOBUF_WIRE_LENGTH_DELIMITED;
    encode_varint(u64::from(tag), out);
    encode_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub struct VlessStream<S> {
    inner: S,
    response: ResponseHeaderState,
    body: VlessBodyCodec,
}

impl<S> VlessStream<S> {
    pub fn new(inner: S) -> Self {
        VlessStreamBuilder::new(inner).build()
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

pub struct VlessStreamBuilder<S> {
    inner: S,
    vision_uuid: Option<[u8; 16]>,
}

impl<S> VlessStreamBuilder<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            vision_uuid: None,
        }
    }

    pub fn vision(mut self, uuid: &[u8; 16]) -> Self {
        self.vision_uuid = Some(*uuid);
        self
    }

    pub fn build(self) -> VlessStream<S> {
        VlessStream {
            inner: self.inner,
            response: ResponseHeaderState::new(),
            body: match self.vision_uuid {
                Some(uuid) => VlessBodyCodec::Vision(VisionBodyCodec::new(uuid)),
                None => VlessBodyCodec::Plain,
            },
        }
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
        this.body.poll_read(&mut this.inner, cx, buf)
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
        let this = self.get_mut();
        this.body.poll_write(&mut this.inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.body.poll_flush(&mut this.inner, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.body.poll_shutdown(&mut this.inner, cx)
    }
}

enum VlessBodyCodec {
    Plain,
    Vision(VisionBodyCodec),
}

impl VlessBodyCodec {
    fn poll_read<S>(
        &mut self,
        stream: &mut S,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>>
    where
        S: AsyncRead + Unpin,
    {
        match self {
            Self::Plain => Pin::new(stream).poll_read(cx, buf),
            Self::Vision(vision) => vision.poll_read(stream, cx, buf),
        }
    }

    fn poll_write<S>(
        &mut self,
        stream: &mut S,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>
    where
        S: AsyncWrite + Unpin,
    {
        match self {
            Self::Plain => Pin::new(stream).poll_write(cx, buf),
            Self::Vision(vision) => vision.poll_write(stream, cx, buf),
        }
    }

    fn poll_flush<S>(&mut self, stream: &mut S, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        match self {
            Self::Plain => Pin::new(stream).poll_flush(cx),
            Self::Vision(vision) => vision.poll_flush(stream, cx),
        }
    }

    fn poll_shutdown<S>(&mut self, stream: &mut S, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        match self {
            Self::Plain => Pin::new(stream).poll_shutdown(cx),
            Self::Vision(vision) => vision.poll_shutdown(stream, cx),
        }
    }
}

struct VisionBodyCodec {
    uuid: [u8; 16],
    read: VisionReadDecoder,
    write_uuid: bool,
    write_padding: bool,
    padding_packets_left: u8,
    pending_write: Vec<u8>,
    pending_written: usize,
    pending_consumed: Option<usize>,
}

impl VisionBodyCodec {
    fn new(uuid: [u8; 16]) -> Self {
        Self {
            uuid,
            read: VisionReadDecoder::new(uuid),
            write_uuid: true,
            write_padding: true,
            padding_packets_left: 8,
            pending_write: Vec::new(),
            pending_written: 0,
            pending_consumed: None,
        }
    }

    fn poll_read<S>(
        &mut self,
        stream: &mut S,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>>
    where
        S: AsyncRead + Unpin,
    {
        if buf.remaining() == 0 || self.read.copy_output_to(buf) {
            return Poll::Ready(Ok(()));
        }

        loop {
            let mut tmp = [0_u8; VISION_MAX_BUFFER_SIZE];
            let mut read_buf = ReadBuf::new(&mut tmp);
            match Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
            let filled = read_buf.filled();
            if filled.is_empty() {
                self.read.finish_eof()?;
                self.read.copy_output_to(buf);
                return Poll::Ready(Ok(()));
            }
            self.read.push(filled)?;
            if self.read.copy_output_to(buf) {
                return Poll::Ready(Ok(()));
            }
        }
    }

    fn poll_write<S>(
        &mut self,
        stream: &mut S,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>
    where
        S: AsyncWrite + Unpin,
    {
        ready!(self.poll_flush_pending(stream, cx))?;
        if let Some(len) = self.pending_consumed.take() {
            return Poll::Ready(Ok(len));
        }
        if !self.write_padding {
            return Pin::new(stream).poll_write(cx, buf);
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let len = buf.len().min(VISION_MAX_FRAME_CONTENT_LEN);
        self.pending_write = self.encode_padding_frame(&buf[..len]);
        self.pending_written = 0;
        self.pending_consumed = Some(len);
        ready!(self.poll_flush_pending(stream, cx))?;
        let len = self
            .pending_consumed
            .take()
            .ok_or_else(|| io::Error::other("vless vision pending write lost consumed length"))?;
        Poll::Ready(Ok(len))
    }

    fn poll_flush<S>(&mut self, stream: &mut S, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        ready!(self.poll_flush_pending(stream, cx))?;
        Pin::new(stream).poll_flush(cx)
    }

    fn poll_shutdown<S>(&mut self, stream: &mut S, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        ready!(self.poll_flush_pending(stream, cx))?;
        Pin::new(stream).poll_shutdown(cx)
    }

    fn poll_flush_pending<S>(
        &mut self,
        stream: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        while self.pending_written < self.pending_write.len() {
            let written = ready!(
                Pin::new(&mut *stream).poll_write(cx, &self.pending_write[self.pending_written..])
            )?;
            if written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write vless vision frame",
                )));
            }
            self.pending_written += written;
        }
        self.pending_write.clear();
        self.pending_written = 0;
        Poll::Ready(Ok(()))
    }

    fn encode_padding_frame(&mut self, payload: &[u8]) -> Vec<u8> {
        let include_uuid = self.write_uuid;
        self.write_uuid = false;
        let command = self.next_padding_command(payload);
        let padding_len = random_padding_len(payload.len());
        let mut out = Vec::with_capacity(
            (if include_uuid { self.uuid.len() } else { 0 })
                + VISION_FRAME_HEADER_LEN
                + payload.len()
                + padding_len,
        );
        if include_uuid {
            out.extend_from_slice(&self.uuid);
        }
        out.push(command);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        out.extend_from_slice(&(padding_len as u16).to_be_bytes());
        out.extend_from_slice(payload);
        append_random_padding(&mut out, padding_len);
        out
    }

    fn next_padding_command(&mut self, payload: &[u8]) -> u8 {
        if self.padding_packets_left > 0 {
            self.padding_packets_left -= 1;
        }
        if self.padding_packets_left == 0 || looks_like_tls_application_data(payload) {
            self.write_padding = false;
            return VISION_COMMAND_PADDING_END;
        }
        VISION_COMMAND_PADDING_CONTINUE
    }
}

struct VisionReadDecoder {
    uuid: [u8; 16],
    input: Vec<u8>,
    output: Vec<u8>,
    mode: VisionReadMode,
}

impl VisionReadDecoder {
    fn new(uuid: [u8; 16]) -> Self {
        Self {
            uuid,
            input: Vec::new(),
            output: Vec::new(),
            mode: VisionReadMode::Probe,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.input.extend_from_slice(bytes);
        self.decode()
    }

    fn copy_output_to(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        let len = self.output.len().min(buf.remaining());
        if len == 0 {
            return false;
        }
        buf.put_slice(&self.output[..len]);
        self.output.drain(..len);
        true
    }

    fn finish_eof(&mut self) -> io::Result<()> {
        match self.mode {
            VisionReadMode::Probe | VisionReadMode::Direct => {
                self.output.extend_from_slice(&self.input);
                self.input.clear();
                self.mode = VisionReadMode::Direct;
                Ok(())
            }
            VisionReadMode::FrameHeader
            | VisionReadMode::FrameContent { .. }
            | VisionReadMode::FramePadding { .. } => {
                if self.input.is_empty() {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete vless vision frame",
                    ))
                }
            }
        }
    }

    fn decode(&mut self) -> io::Result<()> {
        loop {
            match self.mode {
                VisionReadMode::Probe => {
                    if self.input.len() < self.uuid.len() {
                        return Ok(());
                    }
                    if self.input[..self.uuid.len()] != self.uuid {
                        self.output.extend_from_slice(&self.input);
                        self.input.clear();
                        self.mode = VisionReadMode::Direct;
                        return Ok(());
                    }
                    self.input.drain(..self.uuid.len());
                    self.mode = VisionReadMode::FrameHeader;
                }
                VisionReadMode::FrameHeader => {
                    if self.input.len() < VISION_FRAME_HEADER_LEN {
                        return Ok(());
                    }
                    let command = self.input[0];
                    let content_len = u16::from_be_bytes([self.input[1], self.input[2]]) as usize;
                    let padding_len = u16::from_be_bytes([self.input[3], self.input[4]]) as usize;
                    self.input.drain(..VISION_FRAME_HEADER_LEN);
                    self.mode = VisionReadMode::FrameContent {
                        command,
                        content_len,
                        padding_len,
                    };
                }
                VisionReadMode::FrameContent {
                    command,
                    content_len,
                    padding_len,
                } => {
                    if self.input.len() < content_len {
                        return Ok(());
                    }
                    self.output.extend_from_slice(&self.input[..content_len]);
                    self.input.drain(..content_len);
                    if padding_len == 0 {
                        self.finish_frame(command)?;
                    } else {
                        self.mode = VisionReadMode::FramePadding {
                            command,
                            padding_len,
                        };
                    }
                }
                VisionReadMode::FramePadding {
                    command,
                    padding_len,
                } => {
                    if self.input.len() < padding_len {
                        return Ok(());
                    }
                    self.input.drain(..padding_len);
                    self.finish_frame(command)?;
                }
                VisionReadMode::Direct => {
                    self.output.extend_from_slice(&self.input);
                    self.input.clear();
                    return Ok(());
                }
            }
        }
    }

    fn finish_frame(&mut self, command: u8) -> io::Result<()> {
        match command {
            VISION_COMMAND_PADDING_CONTINUE => {
                self.mode = VisionReadMode::FrameHeader;
            }
            VISION_COMMAND_PADDING_END | VISION_COMMAND_PADDING_DIRECT => {
                self.output.extend_from_slice(&self.input);
                self.input.clear();
                self.mode = VisionReadMode::Direct;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown vless vision padding command",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum VisionReadMode {
    Probe,
    FrameHeader,
    FrameContent {
        command: u8,
        content_len: usize,
        padding_len: usize,
    },
    FramePadding {
        command: u8,
        padding_len: usize,
    },
    Direct,
}

fn random_padding_len(payload_len: usize) -> usize {
    let range = if payload_len > VISION_LONG_PADDING_THRESHOLD {
        VISION_LONG_PADDING_RANGE
    } else {
        VISION_SHORT_PADDING_RANGE
    };
    random_below(range)
}

fn random_below(limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    (OsRng.next_u32() as usize) % limit
}

fn append_random_padding(out: &mut Vec<u8>, len: usize) {
    if len == 0 {
        return;
    }
    let start = out.len();
    out.resize(start + len, 0);
    OsRng.fill_bytes(&mut out[start..]);
}

fn looks_like_tls_application_data(payload: &[u8]) -> bool {
    payload.len() >= 3 && payload[0] == 0x17 && payload[1] == 0x03 && payload[2] >= 0x03
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
