#![cfg(feature = "outbound-vless")]

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Instant;
#[cfg(feature = "tls-utls")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "tls-utls")]
use aws_lc_rs::agreement;
use bytes::Bytes;
use hammer_adapter::{Network, OutboundManager as _, SocksAddr};
use hammer_core::config::{Outbound, OutboundKind, OutboundTlsOptions, VlessOutboundOptions};
#[cfg(feature = "tls-utls")]
use hammer_core::config::{
    RealityOptions, RealityPublicKey, RealityShortId, UtlsFingerprint, UtlsOptions,
};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::{DiscardWriter, Factory, Logger};
use hammer_core::protocol::vless::FLOW_XTLS_RPRX_VISION;
#[cfg(feature = "tls-utls")]
use hammer_core::protocol::vless::reality::{
    RealityClientVersion, derive_auth_key, seal_session_id,
};
use hammer_runtime::OutboundManager;
use rustls::pki_types::PrivateKeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

fn logger(id: &str) -> Logger {
    Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
}

fn vless_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        server: "127.0.0.1".to_owned(),
        server_port: port,
        uuid: [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ],
        flow: None,
        network: vec![Network::Tcp, Network::Udp],
        tls: OutboundTlsOptions::default(),
    }
}

fn vless_tls_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        tls: OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            ..Default::default()
        },
        ..vless_options(port)
    }
}

fn vless_vision_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        flow: Some(FLOW_XTLS_RPRX_VISION.to_owned()),
        tls: OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            ..Default::default()
        },
        ..vless_options(port)
    }
}

#[cfg(feature = "tls-utls")]
fn vless_utls_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        tls: OutboundTlsOptions {
            enabled: true,
            server_name: "localhost".to_owned(),
            insecure: true,
            utls: Some(UtlsOptions {
                fingerprint: UtlsFingerprint::Chrome,
            }),
            ..Default::default()
        },
        ..vless_options(port)
    }
}

#[cfg(feature = "tls-utls")]
fn vless_reality_utls_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        tls: OutboundTlsOptions {
            reality: Some(test_reality_options()),
            ..vless_utls_options(port).tls
        },
        ..vless_options(port)
    }
}

#[cfg(feature = "tls-utls")]
fn test_reality_options() -> RealityOptions {
    RealityOptions {
        public_key: RealityPublicKey(hex_bytes(
            "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f",
        )),
        short_id: RealityShortId(vec![0x0a, 0x0b]),
    }
}

fn vless_record_fragment_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        tls: OutboundTlsOptions {
            record_fragment: true,
            ..vless_tls_options(port).tls
        },
        ..vless_options(port)
    }
}

fn vless_tcp_only_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        network: vec![Network::Tcp],
        ..vless_options(port)
    }
}

fn vless_udp_only_options(port: u16) -> VlessOutboundOptions {
    VlessOutboundOptions {
        network: vec![Network::Udp],
        ..vless_options(port)
    }
}

#[test]
fn outbound_manager_registers_vless_outbound() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_options(443)),
        }],
    )
    .expect("outbound manager");

    let outbound = manager.get("vl").expect("vless outbound");
    assert_eq!(outbound.type_name(), "vless");
    assert_eq!(manager.default().unwrap().id(), "vl");
}

#[test]
fn outbound_manager_registers_vless_configured_networks() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_tcp_only_options(443)),
        }],
    )
    .expect("outbound manager");

    let outbound = manager.get("vl").expect("vless outbound");
    assert_eq!(outbound.networks(), &[Network::Tcp]);
}

#[tokio::test]
async fn vless_outbound_rejects_tcp_when_disabled_by_network() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_udp_only_options(9)),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 443);

    let err = match outbound.dial(Network::Tcp, destination, b"").await {
        Ok(_) => panic!("tcp dial accepted"),
        Err(err) => err,
    };

    assert!(
        err.to_string().contains("vless tcp is disabled by network"),
        "error = {err:?}"
    );
}

#[tokio::test]
async fn vless_outbound_rejects_udp_when_disabled_by_network() {
    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_tcp_only_options(443)),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");

    let err = match outbound.listen_packet().await {
        Ok(_) => panic!("udp listener accepted"),
        Err(err) => err,
    };

    assert!(
        err.to_string().contains("vless udp is disabled by network"),
        "error = {err:?}"
    );
}

#[tokio::test]
async fn vless_outbound_dials_tcp_and_strips_response_header() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let expected_request_len = 1 + 16 + 1 + 1 + 2 + 1 + 1 + "example.com".len() + "hello".len();
    let captured = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream.write_all(b"reply").await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::domain(
        "example.com",
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        443,
    );

    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial vless tcp");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"reply");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&443_u16.to_be_bytes());
    expected.push(0x02);
    expected.push("example.com".len() as u8);
    expected.extend_from_slice(b"example.com");
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}

#[tokio::test]
async fn vless_outbound_dials_tcp_over_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(tls_server_config().expect("server tls config")));
    let expected_request_len = 1 + 16 + 1 + 1 + 2 + 1 + 4 + "hello".len();
    let captured = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream.write_all(b"tls-reply").await.unwrap();
        stream.shutdown().await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_tls_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 8443);

    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial vless tls tcp");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"tls-reply");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&8443_u16.to_be_bytes());
    expected.push(0x01);
    expected.extend_from_slice(&[198, 51, 100, 7]);
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}

#[tokio::test]
async fn vless_outbound_dials_tcp_with_vision_body_codec() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(tls_server_config().expect("server tls config")));
    let expected_request_len = 1 + 16 + 1 + 18 + 1 + 2 + 1 + 1 + "example.com".len();
    let captured = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();

        let mut frame_prefix = [0_u8; 21];
        stream.read_exact(&mut frame_prefix).await.unwrap();
        assert_eq!(
            &frame_prefix[..16],
            &[
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]
        );
        assert_eq!(u16::from_be_bytes([frame_prefix[17], frame_prefix[18]]), 5);
        let padding_len = u16::from_be_bytes([frame_prefix[19], frame_prefix[20]]) as usize;
        let mut body = [0_u8; 5];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"hello");
        let mut padding = vec![0_u8; padding_len];
        stream.read_exact(&mut padding).await.unwrap();

        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream
            .write_all(&[
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ])
            .await
            .unwrap();
        stream.write_all(&[0x01]).await.unwrap();
        stream.write_all(&5_u16.to_be_bytes()).await.unwrap();
        stream.write_all(&0_u16.to_be_bytes()).await.unwrap();
        stream.write_all(b"reply").await.unwrap();
        stream.shutdown().await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_vision_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::domain(
        "example.com",
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        443,
    );

    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial vless vision tcp");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"reply");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(18);
    expected.push(0x0a);
    expected.push(FLOW_XTLS_RPRX_VISION.len() as u8);
    expected.extend_from_slice(FLOW_XTLS_RPRX_VISION.as_bytes());
    expected.push(0x01);
    expected.extend_from_slice(&443_u16.to_be_bytes());
    expected.push(0x02);
    expected.push("example.com".len() as u8);
    expected.extend_from_slice(b"example.com");
    assert_eq!(request, expected);
}

#[cfg(feature = "tls-utls")]
#[tokio::test]
async fn vless_outbound_dials_tcp_over_utls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(tls_server_config().expect("server tls config")));
    let expected_request_len = 1 + 16 + 1 + 1 + 2 + 1 + 4 + "hello".len();
    let captured = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream.write_all(b"utls-reply").await.unwrap();
        stream.shutdown().await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_utls_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 8443);

    let mut stream = outbound
        .dial(Network::Tcp, destination, b"hello")
        .await
        .expect("dial vless utls tcp");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"utls-reply");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&8443_u16.to_be_bytes());
    expected.push(0x01);
    expected.extend_from_slice(&[198, 51, 100, 7]);
    expected.extend_from_slice(b"hello");
    assert_eq!(request, expected);
}

#[cfg(feature = "tls-utls")]
#[tokio::test]
async fn vless_reality_over_utls_seals_client_hello_session_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let captured = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut record_header = [0_u8; 5];
        stream.read_exact(&mut record_header).await.unwrap();
        assert_eq!(record_header[0], 22, "first TLS record must be handshake");
        let record_len = usize::from(u16::from_be_bytes([record_header[3], record_header[4]]));
        let mut client_hello = vec![0_u8; record_len];
        stream.read_exact(&mut client_hello).await.unwrap();
        client_hello
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_reality_utls_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 8443);

    let _ = outbound.dial(Network::Tcp, destination, b"").await;
    let client_hello = tokio::time::timeout(Duration::from_secs(2), captured)
        .await
        .expect("client hello captured")
        .unwrap();

    assert_reality_session_id(&client_hello);
}

#[tokio::test]
async fn vless_outbound_dials_tcp_over_tls_with_record_fragment() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(tls_server_config().expect("server tls config")));
    let expected_request_len = 1 + 16 + 1 + 1 + 2 + 1 + 4;
    let captured = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream.write_all(b"record-fragment-reply").await.unwrap();
        stream.shutdown().await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_record_fragment_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 8443);

    let mut stream = outbound
        .dial(Network::Tcp, destination, b"")
        .await
        .expect("dial vless tls record fragment tcp");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert_eq!(response, b"record-fragment-reply");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(0x00);
    expected.push(0x01);
    expected.extend_from_slice(&8443_u16.to_be_bytes());
    expected.push(0x01);
    expected.extend_from_slice(&[198, 51, 100, 7]);
    assert_eq!(request, expected);
}

#[tokio::test]
async fn vless_outbound_sends_and_receives_udp_datagrams() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let expected_request_len = 1 + 16 + 1 + 1 + 2 + 1 + 4;
    let captured = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; expected_request_len];
        stream.read_exact(&mut request).await.unwrap();

        let mut len = [0_u8; 2];
        stream.read_exact(&mut len).await.unwrap();
        let mut payload = vec![0_u8; u16::from_be_bytes(len) as usize];
        stream.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload, b"dns");

        stream.write_all(&[0x00, 0x00]).await.unwrap();
        stream
            .write_all(&(b"echo:dns".len() as u16).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(b"echo:dns").await.unwrap();
        request
    });

    let manager = OutboundManager::from_options(
        logger("outbound"),
        "vl",
        &[Outbound {
            id: "vl".to_owned(),
            kind: OutboundKind::Vless(vless_options(server_addr.port())),
        }],
    )
    .expect("outbound manager");
    let outbound = manager.get("vl").expect("vless outbound");
    let destination = SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 53)), 53);
    let mut packet = outbound.listen_packet().await.expect("listen vless udp");

    packet
        .send_to(destination.clone(), Bytes::from_static(b"dns"))
        .await
        .expect("send vless udp");
    let got = packet.recv_from().await.expect("recv vless udp");

    assert_eq!(got.destination, destination);
    assert_eq!(got.payload.as_ref(), b"echo:dns");

    let request = captured.await.unwrap();
    let mut expected = Vec::new();
    expected.push(0x00);
    expected.extend_from_slice(&[
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]);
    expected.push(0x00);
    expected.push(0x02);
    expected.extend_from_slice(&53_u16.to_be_bytes());
    expected.push(0x01);
    expected.extend_from_slice(&[192, 0, 2, 53]);
    assert_eq!(request, expected);
}

fn tls_server_config() -> HammerResult<rustls::ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .map_err(|err| HammerError::internal(format!("generate certificate: {err}")))?;
    rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|err| HammerError::internal(format!("server tls versions: {err}")))?
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.into()],
        PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into()),
    )
    .map_err(|err| HammerError::internal(format!("server certificate: {err}")))
}

#[cfg(feature = "tls-utls")]
fn assert_reality_session_id(client_hello: &[u8]) {
    assert_eq!(client_hello[0], 1, "handshake message must be ClientHello");
    let client_random: [u8; 32] = client_hello[6..38].try_into().unwrap();
    let session_id_offset = 39;
    assert_eq!(
        client_hello[38], 32,
        "Reality requires a 32-byte session id"
    );
    let session_id = &client_hello[session_id_offset..session_id_offset + 32];
    let client_public_key = x25519_key_share_from_client_hello(client_hello);
    let mut aad = client_hello.to_vec();
    aad[session_id_offset..session_id_offset + 32].fill(0);

    let server_private_key = agreement::PrivateKey::from_private_key(
        &agreement::X25519,
        &hex_bytes::<32>("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb"),
    )
    .expect("server x25519 private key");
    let client_public_key =
        agreement::UnparsedPublicKey::new(&agreement::X25519, client_public_key);
    let auth_key = agreement::agree(
        &server_private_key,
        client_public_key,
        HammerError::internal("server/client x25519 agreement"),
        |shared_secret| {
            let shared_secret: &[u8; 32] = shared_secret.try_into().unwrap();
            derive_auth_key(shared_secret, &client_random)
        },
    )
    .expect("Reality auth key");

    let unix_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    let mut matched = false;
    for candidate_time in unix_time.saturating_sub(3)..=unix_time.saturating_add(3) {
        let expected = seal_session_id(
            &test_reality_options(),
            &auth_key,
            &client_random,
            &aad,
            RealityClientVersion::new(0, 1, 0),
            candidate_time,
        )
        .expect("Reality session id");
        if expected.as_bytes() == session_id {
            matched = true;
            break;
        }
    }

    assert!(
        matched,
        "ClientHello session id is not a valid Reality seal"
    );
}

#[cfg(feature = "tls-utls")]
fn x25519_key_share_from_client_hello(client_hello: &[u8]) -> &[u8] {
    let mut offset = 4 + 2 + 32;
    let session_id_len = usize::from(client_hello[offset]);
    offset += 1 + session_id_len;
    let cipher_suites_len = usize::from(u16::from_be_bytes([
        client_hello[offset],
        client_hello[offset + 1],
    ]));
    offset += 2 + cipher_suites_len;
    let compression_methods_len = usize::from(client_hello[offset]);
    offset += 1 + compression_methods_len;
    let extensions_len = usize::from(u16::from_be_bytes([
        client_hello[offset],
        client_hello[offset + 1],
    ]));
    offset += 2;
    let extensions_end = offset + extensions_len;
    while offset < extensions_end {
        let extension_type = u16::from_be_bytes([client_hello[offset], client_hello[offset + 1]]);
        let extension_len = usize::from(u16::from_be_bytes([
            client_hello[offset + 2],
            client_hello[offset + 3],
        ]));
        offset += 4;
        if extension_type == 51 {
            let mut share_offset = offset + 2;
            let share_end = offset + extension_len;
            while share_offset < share_end {
                let group = u16::from_be_bytes([
                    client_hello[share_offset],
                    client_hello[share_offset + 1],
                ]);
                let key_len = usize::from(u16::from_be_bytes([
                    client_hello[share_offset + 2],
                    client_hello[share_offset + 3],
                ]));
                share_offset += 4;
                if group == 0x001d && key_len == 32 {
                    return &client_hello[share_offset..share_offset + key_len];
                }
                share_offset += key_len;
            }
        }
        offset += extension_len;
    }
    panic!("ClientHello did not include an X25519 key share");
}

#[cfg(feature = "tls-utls")]
fn hex_bytes<const N: usize>(hex: &str) -> [u8; N] {
    assert_eq!(hex.len(), N * 2);
    let mut bytes = [0_u8; N];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(chunk[0]) << 4) | hex_nibble(chunk[1]);
    }
    bytes
}

#[cfg(feature = "tls-utls")]
fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex byte"),
    }
}
