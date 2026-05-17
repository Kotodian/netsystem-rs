#![cfg(feature = "vless")]

use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hammer_core::SocksAddr;
use hammer_core::protocol::vless::{
    FLOW_XTLS_RPRX_VISION, VlessCommand, VlessRequestBuilder, VlessStream, VlessStreamBuilder,
    encode_request, encode_udp_packet, read_udp_packet,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

fn uuid() -> [u8; 16] {
    [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]
}

#[test]
fn encodes_tcp_request_header_and_initial_payload() {
    let destination = SocksAddr::domain(
        "example.com",
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        443,
    );

    let request =
        encode_request(&uuid(), VlessCommand::Tcp, &destination, b"hello").expect("request");

    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&uuid());
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&443_u16.to_be_bytes());
    expected.push(0x02);
    expected.push("example.com".len() as u8);
    expected.extend_from_slice(b"example.com");
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}

#[test]
fn builder_encodes_vision_flow_addon() {
    let destination = SocksAddr::domain(
        "example.com",
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        443,
    );

    let request = VlessRequestBuilder::new(&uuid(), VlessCommand::Tcp, &destination)
        .flow(FLOW_XTLS_RPRX_VISION)
        .initial_payload(b"hello")
        .encode()
        .expect("request");

    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&uuid());
    expected.push(18);
    expected.push(0x0a);
    expected.push(FLOW_XTLS_RPRX_VISION.len() as u8);
    expected.extend_from_slice(FLOW_XTLS_RPRX_VISION.as_bytes());
    expected.push(0x01);
    expected.extend_from_slice(&443_u16.to_be_bytes());
    expected.push(0x02);
    expected.push("example.com".len() as u8);
    expected.extend_from_slice(b"example.com");
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}

#[tokio::test]
async fn stream_strips_response_header_before_payload() {
    let (mut server, client) = tokio::io::duplex(64);
    tokio::spawn(async move {
        server.write_all(&[0x00, 0x00]).await.unwrap();
        server.write_all(b"reply").await.unwrap();
    });

    let mut stream = VlessStream::new(client);
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await.unwrap();

    assert_eq!(payload, b"reply");
}

#[tokio::test]
async fn vision_stream_decodes_padded_response_body() {
    let (mut server, client) = tokio::io::duplex(128);
    tokio::spawn(async move {
        server.write_all(&[0x00, 0x00]).await.unwrap();
        server.write_all(&uuid()).await.unwrap();
        server.write_all(&[0x01]).await.unwrap();
        server.write_all(&5_u16.to_be_bytes()).await.unwrap();
        server.write_all(&3_u16.to_be_bytes()).await.unwrap();
        server.write_all(b"reply").await.unwrap();
        server.write_all(&[0xaa, 0xbb, 0xcc]).await.unwrap();
    });

    let mut stream = VlessStreamBuilder::new(client).vision(&uuid()).build();
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await.unwrap();

    assert_eq!(payload, b"reply");
}

#[tokio::test]
async fn vision_stream_encodes_padded_request_body() {
    let (mut server, client) = tokio::io::duplex(4096);
    let mut stream = VlessStreamBuilder::new(client).vision(&uuid()).build();

    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();

    let mut prefix = [0_u8; 21];
    server.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix[..16], &uuid());
    assert_eq!(u16::from_be_bytes([prefix[17], prefix[18]]), 5);
    let padding_len = u16::from_be_bytes([prefix[19], prefix[20]]) as usize;

    let mut content = [0_u8; 5];
    server.read_exact(&mut content).await.unwrap();
    assert_eq!(&content, b"hello");

    let mut padding = vec![0_u8; padding_len];
    server.read_exact(&mut padding).await.unwrap();
}

#[tokio::test]
async fn vision_stream_does_not_duplicate_payload_after_pending_write() {
    let io = PartialPendingIo::default();
    let state = Arc::clone(&io.state);
    let mut stream = VlessStreamBuilder::new(io).vision(&uuid()).build();

    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();

    let written = state.lock().unwrap().written.clone();
    let payloads = decode_vision_payloads(&written);
    assert_eq!(payloads, vec![b"hello".to_vec()]);
}

#[test]
fn encodes_udp_packet_with_length_prefix() {
    let packet = encode_udp_packet(b"dns").expect("udp packet");

    assert_eq!(packet, [0x00, 0x03, b'd', b'n', b's']);
}

#[derive(Clone, Default)]
struct PartialPendingIo {
    state: Arc<Mutex<PartialPendingState>>,
}

#[derive(Default)]
struct PartialPendingState {
    written: Vec<u8>,
    calls: usize,
}

impl AsyncRead for PartialPendingIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for PartialPendingIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        state.calls += 1;
        match state.calls {
            1 => {
                let len = buf.len().min(3);
                state.written.extend_from_slice(&buf[..len]);
                Poll::Ready(Ok(len))
            }
            2 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => {
                state.written.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn decode_vision_payloads(mut bytes: &[u8]) -> Vec<Vec<u8>> {
    assert!(bytes.len() >= 16);
    assert_eq!(&bytes[..16], &uuid());
    bytes = &bytes[16..];

    let mut payloads = Vec::new();
    while !bytes.is_empty() {
        assert!(bytes.len() >= 5);
        let content_len = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
        let padding_len = u16::from_be_bytes([bytes[3], bytes[4]]) as usize;
        bytes = &bytes[5..];
        assert!(bytes.len() >= content_len + padding_len);
        payloads.push(bytes[..content_len].to_vec());
        bytes = &bytes[content_len + padding_len..];
    }
    payloads
}

#[tokio::test]
async fn reads_udp_packet_with_length_prefix() {
    let (mut server, mut client) = tokio::io::duplex(64);
    tokio::spawn(async move {
        server.write_all(&[0x00, 0x08]).await.unwrap();
        server.write_all(b"echo:dns").await.unwrap();
    });

    let payload = read_udp_packet(&mut client).await.expect("udp packet");

    assert_eq!(payload, b"echo:dns");
}
