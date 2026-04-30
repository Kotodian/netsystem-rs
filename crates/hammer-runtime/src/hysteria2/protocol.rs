use std::io::{Cursor, Read};

use hammer_core::error::HammerError;
use http::HeaderMap;
use rand::{Rng, distributions::Alphanumeric};
use tokio::io::{AsyncRead, AsyncReadExt};

pub const URL_HOST: &str = "hysteria";
pub const URL_PATH: &str = "/auth";
pub const STATUS_AUTH_OK: u16 = 233;
pub const FRAME_TYPE_TCP_REQUEST: u64 = 0x401;
pub const MAX_ADDRESS_LENGTH: u64 = 2048;
pub const MAX_MESSAGE_LENGTH: u64 = 2048;
pub const MAX_PADDING_LENGTH: u64 = 4096;
pub const MAX_UDP_SIZE: usize = 4096;

const REQUEST_HEADER_AUTH: &str = "Hysteria-Auth";
const RESPONSE_HEADER_UDP_ENABLED: &str = "Hysteria-UDP";
const COMMON_HEADER_CC_RX: &str = "Hysteria-CC-RX";
const COMMON_HEADER_PADDING: &str = "Hysteria-Padding";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthRequest {
    pub auth: String,
    pub rx: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthResponse {
    pub udp_enabled: bool,
    pub rx: u64,
    pub rx_auto: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRequest {
    pub destination: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResponse {
    pub ok: bool,
    pub message: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRequestHeader {
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResponseHeader {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpMessage {
    pub session_id: u32,
    pub packet_id: u16,
    pub fragment_id: u8,
    pub fragment_total: u8,
    pub destination: String,
    pub payload: Vec<u8>,
}

impl UdpMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.header_size() + self.payload.len());
        out.extend_from_slice(&self.session_id.to_be_bytes());
        out.extend_from_slice(&self.packet_id.to_be_bytes());
        out.push(self.fragment_id);
        out.push(self.fragment_total);
        write_varint(&mut out, self.destination.len() as u64);
        out.extend_from_slice(self.destination.as_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, HammerError> {
        if input.len() < 8 {
            return Err(HammerError::internal("short UDP message"));
        }
        let session_id = u32::from_be_bytes(input[0..4].try_into().unwrap());
        let packet_id = u16::from_be_bytes(input[4..6].try_into().unwrap());
        let fragment_id = input[6];
        let fragment_total = input[7];
        let mut cursor = Cursor::new(&input[8..]);
        let destination = read_vstring(&mut cursor)?;
        let offset = 8 + cursor.position() as usize;
        Ok(Self {
            session_id,
            packet_id,
            fragment_id,
            fragment_total,
            destination,
            payload: input[offset..].to_vec(),
        })
    }

    #[inline]
    pub fn header_size(&self) -> usize {
        8 + varint_len(self.destination.len() as u64) + self.destination.len()
    }
}

pub fn auth_request_to_headers(headers: &mut HeaderMap, request: &AuthRequest) {
    headers.insert(REQUEST_HEADER_AUTH, request.auth.parse().unwrap());
    headers.insert(COMMON_HEADER_CC_RX, request.rx.to_string().parse().unwrap());
    headers.insert(COMMON_HEADER_PADDING, padding(256, 2048).parse().unwrap());
}

pub fn auth_request_from_headers(headers: &HeaderMap) -> AuthRequest {
    let auth = header_str(headers, REQUEST_HEADER_AUTH).to_owned();
    let rx = header_str(headers, COMMON_HEADER_CC_RX)
        .parse()
        .unwrap_or(0);
    AuthRequest { auth, rx }
}

pub fn auth_response_to_headers(headers: &mut HeaderMap, response: &AuthResponse) {
    headers.insert(
        RESPONSE_HEADER_UDP_ENABLED,
        response.udp_enabled.to_string().parse().unwrap(),
    );
    let rx = if response.rx_auto {
        "auto".to_owned()
    } else {
        response.rx.to_string()
    };
    headers.insert(COMMON_HEADER_CC_RX, rx.parse().unwrap());
    headers.insert(COMMON_HEADER_PADDING, padding(256, 2048).parse().unwrap());
}

pub fn auth_response_from_headers(headers: &HeaderMap) -> AuthResponse {
    let udp_enabled = header_str(headers, RESPONSE_HEADER_UDP_ENABLED)
        .parse()
        .unwrap_or(false);
    let rx = header_str(headers, COMMON_HEADER_CC_RX);
    AuthResponse {
        udp_enabled,
        rx: rx.parse().unwrap_or(0),
        rx_auto: rx == "auto",
    }
}

pub fn encode_tcp_request(destination: &str, payload: &[u8]) -> Vec<u8> {
    let padding = padding(64, 512);
    let mut out = Vec::new();
    write_varint(&mut out, FRAME_TYPE_TCP_REQUEST);
    write_vstring(&mut out, destination);
    write_varint(&mut out, padding.len() as u64);
    out.extend_from_slice(padding.as_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn decode_tcp_request(input: &[u8]) -> Result<TcpRequest, HammerError> {
    let mut cursor = Cursor::new(input);
    let frame_type = read_varint(&mut cursor)?;
    if frame_type != FRAME_TYPE_TCP_REQUEST {
        return Err(HammerError::internal(format!(
            "unsupported TCP frame type: {frame_type}"
        )));
    }
    let destination = read_vstring(&mut cursor)?;
    skip_padding(&mut cursor)?;
    let offset = cursor.position() as usize;
    Ok(TcpRequest {
        destination,
        payload: input[offset..].to_vec(),
    })
}

pub fn encode_tcp_response(ok: bool, message: &str, payload: &[u8]) -> Vec<u8> {
    let padding = padding(128, 1024);
    let msg = if message.len() > MAX_MESSAGE_LENGTH as usize {
        &message[..MAX_MESSAGE_LENGTH as usize]
    } else {
        message
    };
    let mut out = Vec::new();
    out.push(if ok { 0 } else { 1 });
    write_vstring(&mut out, msg);
    write_varint(&mut out, padding.len() as u64);
    out.extend_from_slice(padding.as_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn decode_tcp_response(input: &[u8]) -> Result<TcpResponse, HammerError> {
    if input.is_empty() {
        return Err(HammerError::internal("short TCP response"));
    }
    let mut cursor = Cursor::new(&input[1..]);
    let message = read_vstring_with_limit(&mut cursor, MAX_MESSAGE_LENGTH, true)?;
    skip_padding(&mut cursor)?;
    let offset = 1 + cursor.position() as usize;
    Ok(TcpResponse {
        ok: input[0] == 0,
        message,
        payload: input[offset..].to_vec(),
    })
}

pub async fn read_tcp_request_header<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<TcpRequestHeader, HammerError> {
    let frame_type = read_varint_async(reader).await?;
    if frame_type != FRAME_TYPE_TCP_REQUEST {
        return Err(HammerError::internal(format!(
            "unsupported TCP frame type: {frame_type}"
        )));
    }
    let destination = read_vstring_async(reader, MAX_ADDRESS_LENGTH, false).await?;
    skip_padding_async(reader).await?;
    Ok(TcpRequestHeader { destination })
}

pub async fn read_tcp_response_header<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<TcpResponseHeader, HammerError> {
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .await
        .map_err(|err| HammerError::internal(format!("read tcp response status: {err}")))?;
    let message = read_vstring_async(reader, MAX_MESSAGE_LENGTH, true).await?;
    skip_padding_async(reader).await?;
    Ok(TcpResponseHeader {
        ok: status[0] == 0,
        message,
    })
}

fn read_vstring(reader: &mut Cursor<&[u8]>) -> Result<String, HammerError> {
    read_vstring_with_limit(reader, MAX_ADDRESS_LENGTH, false)
}

fn read_vstring_with_limit(
    reader: &mut Cursor<&[u8]>,
    max: u64,
    allow_empty: bool,
) -> Result<String, HammerError> {
    let len = read_varint(reader)?;
    if (!allow_empty && len == 0) || len > max {
        return Err(HammerError::internal("invalid string length"));
    }
    let mut buf = vec![0; len as usize];
    Read::read_exact(reader, &mut buf)
        .map_err(|err| HammerError::internal(format!("read string: {err}")))?;
    String::from_utf8(buf).map_err(|err| HammerError::internal(format!("utf8 string: {err}")))
}

fn write_vstring(out: &mut Vec<u8>, value: &str) {
    write_varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

async fn read_vstring_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: u64,
    allow_empty: bool,
) -> Result<String, HammerError> {
    let len = read_varint_async(reader).await?;
    if (!allow_empty && len == 0) || len > max {
        return Err(HammerError::internal("invalid string length"));
    }
    let mut buf = vec![0; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|err| HammerError::internal(format!("read string: {err}")))?;
    String::from_utf8(buf).map_err(|err| HammerError::internal(format!("utf8 string: {err}")))
}

fn skip_padding(reader: &mut Cursor<&[u8]>) -> Result<(), HammerError> {
    let len = read_varint(reader)?;
    if len > MAX_PADDING_LENGTH {
        return Err(HammerError::internal("invalid padding length"));
    }
    let next = reader.position() + len;
    if next > reader.get_ref().len() as u64 {
        return Err(HammerError::internal("short padding"));
    }
    reader.set_position(next);
    Ok(())
}

async fn skip_padding_async<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(), HammerError> {
    let len = read_varint_async(reader).await?;
    if len > MAX_PADDING_LENGTH {
        return Err(HammerError::internal("invalid padding length"));
    }
    let mut padding = vec![0; len as usize];
    reader
        .read_exact(&mut padding)
        .await
        .map_err(|err| HammerError::internal(format!("read padding: {err}")))?;
    Ok(())
}

fn read_varint(reader: &mut Cursor<&[u8]>) -> Result<u64, HammerError> {
    let mut first = [0; 1];
    Read::read_exact(reader, &mut first)
        .map_err(|err| HammerError::internal(format!("read varint: {err}")))?;
    let prefix = first[0] >> 6;
    let len = 1usize << prefix;
    let mut bytes = [0; 8];
    bytes[0] = first[0] & 0x3f;
    if len > 1 {
        Read::read_exact(reader, &mut bytes[1..len])
            .map_err(|err| HammerError::internal(format!("read varint: {err}")))?;
    }
    Ok(u64::from_be_bytes(bytes) >> ((8 - len) * 8))
}

async fn read_varint_async<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64, HammerError> {
    let mut first = [0; 1];
    reader
        .read_exact(&mut first)
        .await
        .map_err(|err| HammerError::internal(format!("read varint: {err}")))?;
    let prefix = first[0] >> 6;
    let len = 1usize << prefix;
    let mut bytes = [0; 8];
    bytes[0] = first[0] & 0x3f;
    if len > 1 {
        reader
            .read_exact(&mut bytes[1..len])
            .await
            .map_err(|err| HammerError::internal(format!("read varint: {err}")))?;
    }
    Ok(u64::from_be_bytes(bytes) >> ((8 - len) * 8))
}

#[inline]
fn write_varint(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=63 => out.push(value as u8),
        64..=16_383 => {
            out.push(((value >> 8) as u8) | 0x40);
            out.push(value as u8);
        }
        16_384..=1_073_741_823 => {
            out.push(((value >> 24) as u8) | 0x80);
            out.push((value >> 16) as u8);
            out.push((value >> 8) as u8);
            out.push(value as u8);
        }
        _ => {
            out.push(((value >> 56) as u8) | 0xc0);
            out.push((value >> 48) as u8);
            out.push((value >> 40) as u8);
            out.push((value >> 32) as u8);
            out.push((value >> 24) as u8);
            out.push((value >> 16) as u8);
            out.push((value >> 8) as u8);
            out.push(value as u8);
        }
    }
}

#[inline]
fn varint_len(value: u64) -> usize {
    match value {
        0..=63 => 1,
        64..=16_383 => 2,
        16_384..=1_073_741_823 => 4,
        _ => 8,
    }
}

fn padding(min: usize, max: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(rand::thread_rng().gen_range(min..max))
        .map(char::from)
        .collect()
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}
