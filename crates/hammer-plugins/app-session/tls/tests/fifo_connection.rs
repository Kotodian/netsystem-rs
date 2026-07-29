use std::sync::Arc;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle, SessionMsgQueue};
use hammer_service::session::{ProtocolChain, ProtocolChainIo};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};

use hammer_plugin_tls::Connection;

const FIFO_CAPACITY: usize = 128;

fn app_session(index: u32) -> Arc<AppSession> {
    let tx_events = Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("TX event queue"));
    Arc::new(
        AppSession::new_in_segment(
            Segment::local(4 * 1024),
            AppSessionConfig::new(FIFO_CAPACITY, 8),
            SessionHandle::new(index, 0),
            tx_events,
        )
        .expect("App Session"),
    )
}

fn tls_config(trust_server: bool) -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let certified =
        generate_simple_self_signed(["localhost".to_owned()]).expect("test certificate");
    let certificate = certified.cert.der().clone();
    let private_key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));

    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], private_key)
        .expect("server config");
    let mut roots = rustls::RootCertStore::empty();
    if trust_server {
        roots.add(certificate).expect("client trust root");
    }
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(client), Arc::new(server))
}

fn transfer_transport(source: &AppSession, destination: &AppSession) -> usize {
    let available = source.tx_fifo().max_dequeue();
    let accepted = source
        .tx_fifo()
        .peek_segments(0, available, |first, second| {
            let first_accepted = destination.enqueue_rx(first).expect("transport RX");
            if first_accepted != first.len() {
                return first_accepted;
            }
            first_accepted + destination.enqueue_rx(second).expect("transport RX")
        })
        .unwrap_or(0);
    source
        .drop_tx_acked(accepted)
        .expect("transport acknowledgement");
    accepted
}

fn drive<C, S>(
    client: &mut C,
    client_transport: &AppSession,
    server: &mut S,
    server_transport: &AppSession,
) -> usize
where
    C: ProtocolChainIo,
    S: ProtocolChainIo,
{
    let mut transferred = 0;
    client.egress().expect("client egress");
    transferred += transfer_transport(client_transport, server_transport);
    server.ingress().expect("server ingress");
    server.egress().expect("server egress");
    transferred += transfer_transport(server_transport, client_transport);
    client.ingress().expect("client ingress");
    transferred
}

fn receive(session: &AppSession, expected: &[u8]) -> bool {
    if session.rx_fifo().max_dequeue() < expected.len() {
        return false;
    }
    let matches = session
        .rx_fifo()
        .peek_segments(0, expected.len(), |first, second| {
            first == &expected[..first.len()] && second == &expected[first.len()..]
        })
        .unwrap_or(false);
    assert!(matches);
    assert_eq!(session.consume_rx(expected.len()), expected.len());
    true
}

#[test]
fn rustls_connection_handshakes_and_transfers_both_directions_through_small_fifos() {
    let (client_config, server_config) = tls_config(true);
    let client_transport = app_session(1);
    let client_application = app_session(2);
    let server_transport = app_session(3);
    let server_application = app_session(4);

    let client_tls = Connection::client(
        client_config,
        ServerName::try_from("localhost").expect("server name"),
        FIFO_CAPACITY,
    )
    .expect("client connection");
    let server_tls = Connection::server(server_config, FIFO_CAPACITY).expect("server connection");
    let mut client = ProtocolChain::new(
        Arc::clone(&client_application),
        client_tls,
        ProtocolChain::transport(Arc::clone(&client_transport)),
    );
    let mut server = ProtocolChain::new(
        Arc::clone(&server_application),
        server_tls,
        ProtocolChain::transport(Arc::clone(&server_transport)),
    );

    let mut request_received = false;
    for _ in 0..256 {
        drive(
            &mut client,
            &client_transport,
            &mut server,
            &server_transport,
        );
    }

    let request = b"request through rustls";
    assert_eq!(
        client_application.send_bytes(request).expect("request"),
        request.len()
    );
    for _ in 0..256 {
        drive(
            &mut client,
            &client_transport,
            &mut server,
            &server_transport,
        );
        if receive(&server_application, request) {
            request_received = true;
            break;
        }
    }
    assert!(request_received);
    assert_eq!(server_application.rx_fifo().max_dequeue(), 0);
    assert_eq!(client_application.tx_fifo().max_dequeue(), 0);

    let response = b"response through rustls";
    assert_eq!(
        server_application.send_bytes(response).expect("response"),
        response.len()
    );
    let mut response_received = false;
    for _ in 0..256 {
        drive(
            &mut client,
            &client_transport,
            &mut server,
            &server_transport,
        );
        if receive(&client_application, response) {
            response_received = true;
            break;
        }
    }
    assert!(response_received);
    assert_eq!(client_application.rx_fifo().max_dequeue(), 0);
    assert_eq!(server_application.tx_fifo().max_dequeue(), 0);
}

#[test]
fn certificate_error_leaves_the_alert_available_for_transport() {
    let (client_config, server_config) = tls_config(false);
    let client_transport = app_session(11);
    let client_application = app_session(12);
    let server_transport = app_session(13);
    let server_application = app_session(14);
    let client_tls = Connection::client(
        client_config,
        ServerName::try_from("localhost").expect("server name"),
        FIFO_CAPACITY,
    )
    .expect("client connection");
    let server_tls = Connection::server(server_config, FIFO_CAPACITY).expect("server connection");
    let mut client = ProtocolChain::new(
        client_application,
        client_tls,
        ProtocolChain::transport(Arc::clone(&client_transport)),
    );
    let mut server = ProtocolChain::new(
        server_application,
        server_tls,
        ProtocolChain::transport(Arc::clone(&server_transport)),
    );

    let mut certificate_rejected = false;
    for _ in 0..256 {
        client.egress().expect("client handshake records");
        transfer_transport(&client_transport, &server_transport);
        server.ingress().expect("server handshake ingress");
        server.egress().expect("server handshake records");
        transfer_transport(&server_transport, &client_transport);
        if client.ingress().is_err() {
            certificate_rejected = true;
            break;
        }
    }
    assert!(certificate_rejected);

    client.egress().expect("client TLS alert");
    assert_ne!(client_transport.tx_fifo().max_dequeue(), 0);
}
