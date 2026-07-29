use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::RuntimeResult;
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle, SessionMsgQueue};
use hammer_service::session::{
    AppSessionProtocol, ProtocolChain, ProtocolChainIo, ProtocolFifoAdvance,
};

const TLS_REQUEST_RECORD: &[u8] = b"encrypted HTTP request";
const HTTP_REQUEST: &[u8] = b"POST /session HTTP/1.1\r\n\r\nrequest";
const APPLICATION_REQUEST: &[u8] = b"request";
const APPLICATION_RESPONSE: &[u8] = b"response";
const HTTP_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\n\r\nresponse";
const TLS_RESPONSE_RECORD: &[u8] = b"encrypted HTTP response";

fn app_session(index: u32) -> Arc<AppSession> {
    let tx_events = Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("TX event queue"));
    Arc::new(
        AppSession::new_in_segment(
            Segment::local(4 * 1024),
            AppSessionConfig::new(64, 8),
            SessionHandle::new(index, 0),
            tx_events,
        )
        .expect("app session"),
    )
}

fn assert_fifo_message(fifo: &Fifo, message: &[u8]) {
    let observed = fifo.peek_segments(0, message.len(), |first, second| {
        let first_expected = &message[..first.len()];
        let second_expected = &message[first.len()..first.len() + second.len()];
        first == first_expected && second == second_expected
    });
    assert_eq!(observed, Some(true));
}

fn publish_protocol_message(source: &Fifo, destination: &Fifo, input: &[u8], output: &[u8]) {
    assert_fifo_message(source, input);

    let mut reservation = destination
        .reserve_write(output.len())
        .expect("destination FIFO reservation");
    let (first, second) = reservation.segments_mut();
    let first_length = first.len();
    first.copy_from_slice(&output[..first_length]);
    second.copy_from_slice(&output[first_length..first_length + second.len()]);
    assert_eq!(
        reservation
            .commit(output.len())
            .expect("destination FIFO publication"),
        output.len()
    );
    assert_eq!(source.dequeue_drop(input.len()), input.len());
}

#[test]
fn plaintext_exposes_the_existing_app_session_directly() {
    let session = app_session(1);
    let chain = ProtocolChain::plaintext(Arc::clone(&session));

    assert!(Arc::ptr_eq(chain.app_session(), &session));
}

struct TlsProtocol;

impl AppSessionProtocol for TlsProtocol {
    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<ProtocolFifoAdvance> {
        publish_protocol_message(
            lower_rx_fifo,
            upper_rx_fifo,
            TLS_REQUEST_RECORD,
            HTTP_REQUEST,
        );
        Ok(ProtocolFifoAdvance::new(
            TLS_REQUEST_RECORD.len(),
            HTTP_REQUEST.len(),
        ))
    }

    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<ProtocolFifoAdvance> {
        publish_protocol_message(
            upper_tx_fifo,
            lower_tx_fifo,
            HTTP_RESPONSE,
            TLS_RESPONSE_RECORD,
        );
        Ok(ProtocolFifoAdvance::new(
            HTTP_RESPONSE.len(),
            TLS_RESPONSE_RECORD.len(),
        ))
    }
}

struct HttpProtocol;

impl AppSessionProtocol for HttpProtocol {
    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<ProtocolFifoAdvance> {
        publish_protocol_message(
            lower_rx_fifo,
            upper_rx_fifo,
            HTTP_REQUEST,
            APPLICATION_REQUEST,
        );
        Ok(ProtocolFifoAdvance::new(
            HTTP_REQUEST.len(),
            APPLICATION_REQUEST.len(),
        ))
    }

    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<ProtocolFifoAdvance> {
        publish_protocol_message(
            upper_tx_fifo,
            lower_tx_fifo,
            APPLICATION_RESPONSE,
            HTTP_RESPONSE,
        );
        Ok(ProtocolFifoAdvance::new(
            APPLICATION_RESPONSE.len(),
            HTTP_RESPONSE.len(),
        ))
    }
}

#[test]
fn application_selects_tls_then_http_protocols() {
    let transport_session = app_session(10);
    let http_transport_session = app_session(11);
    let application_session = app_session(12);

    let plaintext = ProtocolChain::plaintext(Arc::clone(&transport_session));
    let tls = ProtocolChain::new(Arc::clone(&http_transport_session), TlsProtocol, plaintext);
    let mut chain = ProtocolChain::new(Arc::clone(&application_session), HttpProtocol, tls);

    assert!(Arc::ptr_eq(chain.app_session(), &application_session));

    assert_eq!(
        transport_session
            .enqueue_rx(TLS_REQUEST_RECORD)
            .expect("transport request"),
        TLS_REQUEST_RECORD.len()
    );
    chain.ingress().expect("TLS then HTTP ingress");
    assert_fifo_message(application_session.rx_fifo(), APPLICATION_REQUEST);
    assert_eq!(transport_session.rx_fifo().max_dequeue(), 0);
    assert_eq!(http_transport_session.rx_fifo().max_dequeue(), 0);

    assert_eq!(
        application_session
            .send_bytes(APPLICATION_RESPONSE)
            .expect("application response"),
        APPLICATION_RESPONSE.len()
    );
    chain.egress().expect("HTTP then TLS egress");
    assert_fifo_message(transport_session.tx_fifo(), TLS_RESPONSE_RECORD);
    assert_eq!(application_session.tx_fifo().max_dequeue(), 0);
    assert_eq!(http_transport_session.tx_fifo().max_dequeue(), 0);
}
